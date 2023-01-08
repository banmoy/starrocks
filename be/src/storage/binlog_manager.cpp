// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "binlog_manager.h"

#include "storage/binlog_builder.h"
#include "storage/binlog_util.h"
#include "storage/rowset/page_io.h"
#include "storage/tablet.h"
#include "util/crc32c.h"

namespace starrocks {

BinlogManager::BinlogManager(std::string path, int64_t max_file_size, int32_t max_page_size, KeysType keys_type,
                             CompressionTypePB compression_type, BinlogConfig* binlog_config)
        : _path(std::move(path)),
          _max_file_size(max_file_size),
          _max_page_size(max_page_size),
          _keys_type(keys_type),
          _compression_type(compression_type) {
    if (binlog_config == nullptr) {
        _binlog_enable = false;
        _binlog_ttl_second = INT64_MAX;
        _binlog_max_size = INT64_MAX;
    } else {
        _binlog_enable = binlog_config->binlog_enable;
        _binlog_ttl_second = binlog_config->binlog_ttl_second;
        _binlog_max_size = binlog_config->binlog_max_size;
    }
}

BinlogManager::~BinlogManager() {
    std::lock_guard lock(_meta_lock);
    if (_active_binlog_writer != nullptr) {
        _active_binlog_writer->close(true);
        _active_binlog_writer.reset();
    }
}

Status BinlogManager::init(RowsetVersionMap* rowset_version_map) {
    if (!_binlog_enable) {
        return Status::OK();
    }

    std::set<int64_t> binlog_file_ids;
    Status status = BinlogUtil::list_binlog_file_ids(_path, &binlog_file_ids);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to init binlog under " << _path << ", " << status;
        return status;
    }
    _next_file_id = 0;

    BinlogFileLoadFilter filter(INT64_MAX, INT64_MAX, rowset_version_map);
    std::vector<int64_t> useless_file_ids;
    // load binlog file metas from the largest file id to the smallest
    for (auto it = binlog_file_ids.rbegin(); it != binlog_file_ids.rend(); it++) {
        int64_t file_id = *it;
        std::string file_path = BinlogUtil::binlog_file_path(_path, file_id);
        StatusOr status_or = BinlogFileReader::load(file_id, file_path, filter);
        if (!status_or.ok()) {
            useless_file_ids.push_back(file_id);
            LOG(WARNING) << "Can't load binlog file " << file_path << ", " << status_or.status();
            continue;
        }
        std::shared_ptr<BinlogFileMetaPB> meta = status_or.value();
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        _binlog_file_metas[lsn] = meta;
        filter.reset(meta->start_version(), meta->start_seq_id(), rowset_version_map);

        RowsetId rowset_id;
        for (auto& rowset_id_pb : meta->rowsets()) {
            BinlogUtil::convert_pb_to_rowset_id(rowset_id_pb, &rowset_id);
            _rowset_count_map[rowset_id] += 1;
        }

        if (_next_file_id == 0) {
            _next_file_id = file_id + 1;
        }
    }
    for (auto& it : *rowset_version_map) {
        if (_rowset_count_map.count(it.second->rowset_id()) > 0) {
            _rowsets[it.second->rowset_id()] = it.second;
        }
    }

    status = delete_binlog_files(useless_file_ids);
    LOG_IF(WARNING, !status.ok()) << "Fail to delete useless binlog files when initializing, " << status;

    LOG(INFO) << "Init binlog manager successfully, load binlog files: " << _binlog_file_metas.size();

    return Status::OK();
}

Status BinlogManager::begin(int64_t version) {
    std::lock_guard lock(_ingestion_lock);
    if (_builder != nullptr) {
        std::string msg = fmt::format("Can't build binlog concurrently, running version {}, new version {}",
                                      _builder->version(), version);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    std::shared_lock meta_lock(_meta_lock);
    if (!_binlog_file_metas.empty()) {
        BinlogFileMetaPBSharedPtr file_meta = _binlog_file_metas.rbegin()->second;
        int64_t max_version = file_meta->end_version();
        if (max_version >= version) {
            std::string msg = fmt::format("Add duplicate version to binlog, max version {}, new version {}",
                                          max_version, version);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }

    std::shared_ptr<BinlogFileMetaPB> reused_file_meta;
    std::shared_ptr<BinlogFileWriter> reused_file_writer;
    if (_active_binlog_writer != nullptr) {
        reused_file_meta = _binlog_file_metas.rbegin()->second;
        reused_file_writer = _active_binlog_writer;
        _active_binlog_writer.reset();
    }
    _builder = std::make_unique<BinlogBuilder>(version, shared_from_this(), std::move(reused_file_meta),
                                               std::move(reused_file_writer));
    return Status::OK();
}

Status BinlogManager::append_rowset(const RowsetSharedPtr& rowset) {
    std::lock_guard lock(_ingestion_lock);
    Status status = _builder->append_rowset(rowset);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to append rowset to binlog, rowset " << rowset->rowset_id() << ", version "
                     << rowset->start_version() << ", " << status;
        return status;
    }

    return Status::OK();
}

void BinlogManager::publish(int64_t version) {
    std::lock_guard lock(_ingestion_lock);
    if (_builder == nullptr || _builder->version() != version) {
        return;
    }

    std::shared_ptr<BinlogBuildResult> result = _builder->build();
    _apply_build_result(result.get());
    _builder.reset();
}

void BinlogManager::abort(int64_t version) {
    std::lock_guard lock(_ingestion_lock);
    if (_builder == nullptr || _builder->version() != version) {
        return;
    }
    std::shared_ptr<BinlogBuildResult> result = _builder->abort();
    _apply_build_result(result.get());
    _builder.reset();
}

void BinlogManager::_apply_build_result(BinlogBuildResult* result) {
    std::unique_lock lock(_meta_lock);
    for (auto& meta : result->metas) {
        int128_t lsn = BinlogUtil::get_lsn(meta->start_version(), meta->start_seq_id());
        RowsetId rowset_id;
        // after binlog file is appended new data, should update the rowset meta
        // TODO get incremental rowsets more efficiently
        if (_binlog_file_metas.count(lsn)) {
            BinlogFileMetaPBSharedPtr old_file_meta = _binlog_file_metas[lsn];
            for (auto& rowset_id_pb : old_file_meta->rowsets()) {
                BinlogUtil::convert_pb_to_rowset_id(rowset_id_pb, &rowset_id);
                _rowset_count_map[rowset_id]--;
            }
        }

        _binlog_file_metas[lsn] = meta;
        for (auto& rowset_id_pb : meta->rowsets()) {
            BinlogUtil::convert_pb_to_rowset_id(rowset_id_pb, &rowset_id);
            _rowset_count_map[rowset_id] += 1;
        }
    }

    for (auto& it : result->rowsets) {
        _rowsets[it.first] = it.second;
    }

    _active_binlog_writer = std::move(result->active_writer);
}

StatusOr<std::shared_ptr<BinlogFileWriter>> BinlogManager::create_binlog_writer() {
    int64_t file_id = _next_file_id++;
    std::string file_path = BinlogUtil::binlog_file_path(_path, file_id);
    std::shared_ptr<BinlogFileWriter> binlog_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, _max_page_size, _compression_type);
    Status status = binlog_writer->init();
    if (status.ok()) {
        return binlog_writer;
    }
    LOG(WARNING) << "Fail to initialize binlog writer, file id " << file_id << ", file name " << file_path << ", "
                 << status;
    Status st = binlog_writer->close(false);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to close binlog writer, file id " << file_id << ", file name " << file_path << ", "
                     << status;
    }

    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(file_path))
    st = fs->delete_file(file_path);
    if (st.ok()) {
        LOG(INFO) << "Delete binlog file after creating failed " << file_path;
    } else {
        LOG(WARNING) << "Fail to delete binlog file after creating failed " << file_path << ", " << st;
    }

    return status;
}

bool BinlogManager::is_rowset_used(const RowsetId& rowset_id) {
    std::shared_lock lock(_meta_lock);
    return _rowset_count_map.count(rowset_id) >= 1;
}

void BinlogManager::delete_expired_binlog() {
    // TODO delete expired binlog
}

void BinlogManager::delete_excess_binlog() {
    // TODO delete some binlog oversized
}

void BinlogManager::delete_all_binlog() {
    std::unique_lock lock(_meta_lock);
    _clear_store();
}

StatusOr<BinlogFileMetaPBSharedPtr> BinlogManager::find_binlog_meta(int64_t version, int64_t seq_id) {
    std::shared_lock lock(_meta_lock);
    int128_t lsn = BinlogUtil::get_lsn(version, seq_id);
    auto upper = _binlog_file_metas.upper_bound(lsn);
    if (upper == _binlog_file_metas.begin()) {
        return Status::NotFound(strings::Substitute("Can't find file meta for version $0, seq_id $1", version, seq_id));
    }

    BinlogFileMetaPBSharedPtr file_meta;
    if (upper == _binlog_file_metas.end()) {
        file_meta = _binlog_file_metas.rbegin()->second;
    } else {
        file_meta = (--upper)->second;
    }

    if (file_meta->end_version() < version) {
        return Status::NotFound(strings::Substitute("Can't find file meta for version $0, seq_id $1", version, seq_id));
    }

    return file_meta;
}

StatusOr<std::shared_ptr<BinlogReader>> BinlogManager::create_reader(BinlogReaderParams& reader_params) {
    std::shared_lock lock(_meta_lock);
    int64_t reader_id = _next_reader_id++;
    return std::make_shared<BinlogReader>(shared_from_this(), reader_id, reader_params);
}

Status BinlogManager::delete_binlog_files(std::vector<std::int64_t>& file_ids) {
    if (file_ids.empty()) {
        return Status::OK();
    }

    int delete_fail_num = 0;
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_path))
    for (auto file_id : file_ids) {
        std::string file_path = BinlogUtil::binlog_file_path(_path, file_id);
        Status st = fs->delete_file(file_path);
        if (st.ok()) {
            VLOG(2) << "Delete binlog file " << file_path;
        } else {
            LOG(WARNING) << "Fail to delete binlog file " << file_path << ", " << st;
            delete_fail_num += 1;
        }
    }

    if (delete_fail_num > 0) {
        return Status::InternalError(
                fmt::format("Fail to delete all files, total {}, failed {}", file_ids.size(), delete_fail_num));
    }

    return Status::OK();
}

void BinlogManager::update_config(BinlogConfig* binlog_config) {
    std::unique_lock lock(_meta_lock);
    if (_binlog_enable && !binlog_config->binlog_enable) {
        _clear_store();
    } else if (!_binlog_enable && binlog_config->binlog_enable) {
        // switch to open
    }

    _binlog_enable = binlog_config->binlog_enable;
    _binlog_ttl_second = binlog_config->binlog_ttl_second;
    _binlog_max_size = binlog_config->binlog_max_size;
}

void BinlogManager::_clear_store() {
    if (_active_binlog_writer != nullptr) {
        Status st = _active_binlog_writer->close(false);
        LOG_IF(WARNING, !st.ok()) << "Fail to close file writer when clearing store, file path: "
                                  << _active_binlog_writer->file_path() << ", " << st;
        _active_binlog_writer.reset();
    }
    std::vector<int64_t> file_ids;
    for (auto& it : _binlog_file_metas) {
        file_ids.push_back(it.second->id());
    }
    Status st = delete_binlog_files(file_ids);
    LOG_IF(WARNING, !st.ok()) << "Fail to delete binlog files when clearing store, binlog storage: " << _path << ", "
                              << st;

    _binlog_file_metas.clear();
    _rowset_count_map.clear();
    _rowsets.clear();
    _binlog_readers.clear();
}

} // namespace starrocks