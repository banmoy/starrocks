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

#pragma once

#include <gutil/strings/substitute.h>

#include <cstdint>
#include <memory>
#include <unordered_set>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_writer.h"
#include "storage/binlog_reader.h"
#include "storage/binlog_util.h"
#include "storage/rowset/rowset.h"
#include "util/blocking_queue.hpp"

namespace starrocks {

struct BinlogConfig {
    int64_t version;
    bool binlog_enable;
    int64_t binlog_ttl_second;
    int64_t binlog_max_size;

    void update(const BinlogConfig& new_config) {
        update(new_config.version, new_config.binlog_enable, new_config.binlog_ttl_second, new_config.binlog_max_size);
    }

    void update(const TBinlogConfig& new_config) {
        update(new_config.version, new_config.binlog_enable, new_config.binlog_ttl_second, new_config.binlog_max_size);
    }

    void update(const BinlogConfigPB& new_config) {
        update(new_config.version(), new_config.binlog_enable(), new_config.binlog_ttl_second(),
               new_config.binlog_max_size());
    }

    void update(int64_t new_version, bool new_binlog_enable, int64_t new_binlog_ttl_second,
                int64_t new_binlog_max_size) {
        version = new_version;
        binlog_enable = new_binlog_enable;
        binlog_ttl_second = new_binlog_ttl_second;
        binlog_max_size = new_binlog_max_size;
    }

    void to_pb(BinlogConfigPB* binlog_config_pb) {
        binlog_config_pb->set_version(version);
        binlog_config_pb->set_binlog_enable(binlog_enable);
        binlog_config_pb->set_binlog_ttl_second(binlog_ttl_second);
        binlog_config_pb->set_binlog_max_size(binlog_max_size);
    }

    std::string to_string() {
        return strings::Substitute(
                "BinlogConfig={version=$0, binlog_enable=$1, binlog_ttl_second=$2, binlog_max_size=$3}", version,
                binlog_enable, binlog_ttl_second, binlog_max_size);
    }
};

class Tablet;
class BinlogBuildResult;
class BinlogBuilder;

using BinlogFileMetaPBSharedPtr = std::shared_ptr<BinlogFileMetaPB>;
using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

// Binlog records the change events when ingesting data to the table. The types of change events
// include INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE. For duplicate key table, there is
// only INSERT change event, and for primary key table, there are all types of change events.
// Each tablet will maintain its own binlog, and each change event has a unique, int128_t LSN
// (log sequence number). The LSN composites of an int64_t *version* and an int64_t *seq_id*.
// The *version* indicates which ingestion generates the change event, and it's same as the
// publish version for the ingestion. The *seq_id* is the sequence number of the change event in
// this ingestion. The information of these change events will be written to binlog files, and
// BinlogManager will manage these binlog files, including generating, reading, and deleting after
// expiration.
class BinlogManager : public std::enable_shared_from_this<BinlogManager> {
public:
    BinlogManager(std::string path, int64_t max_file_size, int32_t max_page_size, KeysType keys_type,
                  CompressionTypePB compression_type, BinlogConfig* binlog_config);

    ~BinlogManager();

    Status init(RowsetVersionMap* rowset_version_map);

    // The process to generate the binlog for an ingestion, and these methods will be protected
    // by TabletMeta#_meta_lock outside. See Tablet#add_inc_rowset
    // 1. begin: begin the process, and do some preparation. Go to next steps if return
    //    Status::OK, otherwise the process is finished with failure
    // 2. append_rowset: will be called if begin returns Status::OK, and generate binlog
    //    for duplicate key table. The binlog data is guaranteed to be persisted if return
    //    Status::OK. Note that the metas for the newly binlog are not applied to BinlogManager,
    //    and it's not visible for readers. Should call abort() if returns an error status
    // 3. publish: if append_rowset is successful, apply the metas for the newly binlog
    //    to the BinlogManager, and they are visible to readers. publish can be guaranteed
    //    to be successful because it only modifies in-memory structure (if there is no bugs).
    //    After publish, the whose process is finished successfully
    // 4. abort: stop the process. The cases to call abort()
    //    4.1 append_rowset failed
    //    4.2 append_rowset success, and before publish is called. In Tablet#add_inc_rowset,
    //        will abort the process if the rowset meta fails to save
    // Do not support to generate binlog for concurrent ingestion
    Status begin(int64_t version);
    Status append_rowset(const RowsetSharedPtr& rowset);
    void abort(int64_t version);
    void publish(int64_t version);

    StatusOr<std::shared_ptr<BinlogFileWriter>> create_binlog_writer();

    // Whether the rowset is used by the binlog.
    bool is_rowset_used(const RowsetId& rowset_id);

    // Delete expired and excess binlog
    void check_expiration_and_capacity();

    // Delete all of binlog
    void delete_all_binlog();

    void delete_unused_binlog_files();

    RowsetSharedPtr get_rowset(const RowsetId& rowset_id) {
        std::shared_lock lock(_meta_lock);
        return _rowsets.find(rowset_id)->second;
    }

    StatusOr<std::shared_ptr<BinlogReader>> create_reader(BinlogReaderParams& reader_params);

    void release_reader(int64_t reader_id) {
        std::shared_lock lock(_meta_lock);
        _binlog_readers.erase(reader_id);
    }

    // Find the meta of binlog file which may contain a given <version, seq_id>.
    // Return Status::NotFound if there is no such file.
    StatusOr<BinlogFileMetaPBSharedPtr> find_binlog_meta(int64_t version, int64_t seq_id);

    int32_t num_binlog_files() {
        std::shared_lock lock(_meta_lock);
        return _binlog_file_metas.size();
    }

    // This will be protected by TabletMeta#_meta_lock outside
    void update_config(BinlogConfig* binlog_config);

    bool binlog_enable() {
        std::shared_lock lock(_meta_lock);
        return _binlog_enable;
    }

    std::string& storage_path() { return _path; }

    int64_t max_file_size() { return _max_file_size; }

    // for testing
    std::pair<int64_t, int64_t> lowest_offset() {
        std::shared_lock lock(_meta_lock);
        if (_binlog_file_metas.empty()) {
            return std::make_pair(-1, -1);
        }
        auto meta = _binlog_file_metas.begin();
        return std::make_pair(meta->second->start_version(), meta->second->start_seq_id());
    }

    std::pair<int64_t, int64_t> highest_offset() {
        std::shared_lock lock(_meta_lock);
        if (_binlog_file_metas.empty()) {
            return std::make_pair(-1, -1);
        }
        auto meta = _binlog_file_metas.rbegin();
        return std::make_pair(meta->second->end_version(), meta->second->end_seq_id());
    }

    // for testing
    void install_metas(std::vector<BinlogFileMetaPBSharedPtr>& metas,
                       std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId>& rowsets);

    // For testing
    std::map<int128_t, BinlogFileMetaPBSharedPtr>& get_file_metas() { return _binlog_file_metas; }

    std::unordered_map<RowsetId, int32_t, HashOfRowsetId>& get_rowset_count() { return _rowset_count_map; }

    std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId>& get_rowsets() { return _rowsets; }

private:
    void _set_store_state(const Status& status);
    Status _check_store_state();
    void _apply_build_result(BinlogBuildResult* result);
    void _clear_store();

    // binlog storage directory
    std::string _path;
    int64_t _max_file_size;
    int32_t _max_page_size;
    KeysType _keys_type;
    CompressionTypePB _compression_type;

    // ensure no concurrent ingestion
    std::mutex _ingestion_lock;
    // builder for the current ingestion
    std::unique_ptr<BinlogBuilder> _builder;

    // protect following metas' read/write
    // TODO make lock fine-grained
    std::shared_mutex _meta_lock;
    std::atomic<int64_t> _next_file_id;
    bool _binlog_enable;
    int64_t _binlog_ttl_second;
    int64_t _binlog_max_size;

    // Whether error happens when init binlog in init(), or enable binlog
    // in update_config(), the flag will be cleared after disable binlog
    // in update_config(). The ingestion will fail if the flag is true
    std::atomic<bool> _error_state{false};
    std::string _error_msg;

    // mapping from start LSN(start_version, start_seq_id) of a binlog file to the file meta.
    // A binlog file with a smaller start LSN also has a smaller file id. The file with the
    // biggest start LSN is the meta of _active_binlog_writer if it's not null.
    std::map<int128_t, BinlogFileMetaPBSharedPtr> _binlog_file_metas;
    // the binlog file writer that can append data
    std::shared_ptr<BinlogFileWriter> _active_binlog_writer;
    // unused binlog files, and wait for deletion
    BlockingQueue<int64_t> _unused_binlog_files;

    // mapping from rowset id to the number of binlog files using it
    std::unordered_map<RowsetId, int32_t, HashOfRowsetId> _rowset_count_map;
    // mapping from rowset id to the Rowset used by binlog
    std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId> _rowsets;

    // statistics for disk usage
    int64_t _total_binlog_file_disk_size;
    int64_t _total_rowset_disk_size;

    // Allocate an id for each binlog reader
    // Guarded by _meta_lock
    int64_t _next_reader_id;
    // Mapping from the reader id to the readers.
    // Guarded by _meta_lock
    std::unordered_map<int64_t, BinlogReaderSharedPtr> _binlog_readers;
};

} // namespace starrocks
