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

#include "storage/binlog_builder.h"

#include "util/filesystem_util.h"

namespace starrocks {

BinlogBuilder::BinlogBuilder(int64_t version, std::shared_ptr<BinlogManager> binlog_manager,
                             std::shared_ptr<BinlogFileMetaPB> reused_file_meta,
                             std::shared_ptr<BinlogFileWriter> reused_file_writer)
        : _version(version),
          _binlog_manager(binlog_manager),
          _reused_file_meta(reused_file_meta),
          _reused_file_writer(reused_file_writer) {}

Status BinlogBuilder::append_rowset(const RowsetSharedPtr& rowset) {
    // TODO sync parent dir after creating new file writer which will affect performance
    if (_reused_file_writer != nullptr) {
        _current_writer = _reused_file_writer;
        RETURN_IF_ERROR(_current_writer->begin(rowset->start_version(), rowset->rowset_id(), 0,
                                               rowset->creation_time() * 1000000));
    } else {
        ASSIGN_OR_RETURN(_current_writer, _binlog_manager->create_binlog_writer());
        _created_file_ids.push_back(_current_writer->file_id());
        RETURN_IF_ERROR(_current_writer->begin(rowset->start_version(), rowset->rowset_id(), 0,
                                               rowset->creation_time() * 1000000));
    }

    std::vector<SegmentSharedPtr>& segments = rowset->segments();
    for (int32_t seg_index = 0; seg_index < rowset->num_segments(); seg_index++) {
        int num_rows = segments[seg_index]->num_rows();
        if (num_rows == 0) {
            continue;
        }
        if (_current_writer == nullptr || _current_writer->file_size() > _binlog_manager->max_file_size()) {
            if (_current_writer != nullptr) {
                RETURN_IF_ERROR(_commit_current_writer(false, true));
            }
            ASSIGN_OR_RETURN(_current_writer, _binlog_manager->create_binlog_writer());
            _created_file_ids.push_back(_current_writer->file_id());
            RETURN_IF_ERROR(_current_writer->begin(rowset->start_version(), rowset->rowset_id(), _next_seq_id,
                                                   rowset->creation_time() * 1000000));
        }
        Status status = _current_writer->add_insert_range(seg_index, 0, num_rows);
        if (!status.ok()) {
            _abort_current_writer();
            LOG(WARNING) << "Fail to add_insert_range for rowset " << rowset->rowset_id() << ", segment index "
                         << seg_index << ", number of rows " << num_rows << ", version " << rowset->start_version()
                         << ", binlog writer " << _current_writer->file_path() << ", " << status;
            return status;
        }
    }

    if (rowset->num_rows() == 0) {
        Status status = _current_writer->add_empty();
        if (!status.ok()) {
            _abort_current_writer();
            LOG(WARNING) << "Fail to add_empty for rowset " << rowset->rowset_id() << ", version "
                         << rowset->start_version() << ", binlog writer " << _current_writer->file_path() << ", "
                         << status;
        }
        return status;
    }

    RETURN_IF_ERROR(_commit_current_writer(true, false));
    if (_current_writer->file_size() > _binlog_manager->max_file_size()) {
        // ignore the failure for the last writer, because all data has been committed
        Status status = _current_writer->close(true);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to close the last binlog writer after committed, file name: "
                         << _current_writer->file_path() << ", " << status;
        }
        _current_writer.reset();
    }

    if (rowset->num_rows() != 0) {
        _new_rowsets[rowset->rowset_id()] = rowset;
    }

    return Status::OK();
}

std::shared_ptr<BinlogBuildResult> BinlogBuilder::build() {
    std::shared_ptr<BinlogBuildResult> result = std::make_shared<BinlogBuildResult>();
    result->active_writer = std::move(_current_writer);
    result->metas = std::move(_new_metas);
    result->rowsets = std::move(_new_rowsets);
    return result;
}

std::shared_ptr<BinlogBuildResult> BinlogBuilder::abort() {
    std::shared_ptr<BinlogBuildResult> result = std::make_shared<BinlogBuildResult>();
    if (_reused_file_writer != nullptr) {
        if (_reused_file_writer->closed()) {
            Status status =
                    FileSystemUtil::resize_file(_reused_file_writer->file_path(), _reused_file_meta->file_size());
            if (!status.ok()) {
                // ignore failure because it does no affect already committed data
                LOG(WARNING) << "Failed to resize file, version " << _version << ", file id "
                             << _reused_file_writer->file_id() << ", file path " << _reused_file_writer->file_path()
                             << ", target size " << _reused_file_meta->file_size() << ", " << status;
            }
        } else {
            Status status = _reused_file_writer->reset(_reused_file_meta.get());
            if (!status.ok()) {
                // don't append file meta because the state of the writer is unknown
                _reused_file_writer->close(false);
                // ignore failure because it does no affect already committed data
                LOG(WARNING) << "Failed to reset file writer, version " << _version << ", file id "
                             << _reused_file_writer->file_id() << ", file path " << _reused_file_writer->file_path()
                             << ", " << status;
            } else {
                result->active_writer = _reused_file_writer;
            }
        }
    }

    if (!_created_file_ids.empty()) {
        Status status = _current_writer->close(false);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to close binlog writer when aborting committed " << _current_writer->file_path()
                         << ", " << status;
        }

        status = _binlog_manager->delete_binlog_files(_created_file_ids);
        LOG_IF(WARNING, !status.ok()) << "Fail to delete created binlog files when aborting, " << status;
    }

    return result;
}

Status BinlogBuilder::_commit_current_writer(bool end_of_rowset, bool close_writer) {
    Status status = _current_writer->commit(end_of_rowset);
    if (!status.ok()) {
        _abort_current_writer();
        LOG(WARNING) << "Fail to commit binlog writer " << _current_writer->file_path() << ", version " << _version
                     << ", " << status;
        return status;
    }
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    _current_writer->copy_file_meta(file_meta.get());
    if (close_writer) {
        status = _current_writer->close(true);
        if (!status.ok()) {
            LOG(WARNING) << "Fail to close binlog writer after committed " << _current_writer->file_path() << ", "
                         << status;
            return status;
        }
        _current_writer.reset();
    }
    _new_metas.push_back(file_meta);
    _next_seq_id = file_meta->end_seq_id() + 1;
    return Status::OK();
}

void BinlogBuilder::_abort_current_writer() {
    Status status = _current_writer->abort();
    if (!status.ok()) {
        // don't append file meta because the state of the writer is unknown
        _current_writer->close(false);
        LOG(WARNING) << "Fail to abort binlog writer " << _current_writer->file_path() << ", " << status;
    }
}

} // namespace starrocks