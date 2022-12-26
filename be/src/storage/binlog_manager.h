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
#include "storage/rowset/rowset.h"

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

using BinlogFileMetaPBSharedPtr = std::shared_ptr<BinlogFileMetaPB>;
using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

// Binlog records the change events when loading data to the table. The types of change events
// include INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE. For duplicate key table, there is
// only INSERT change event, and for primary key table, there are all types of change events.
// Each tablet will maintain its own binlog, and each change event has a unique, int128_t LSN
// (log sequence number). The LSN composites of an int64_t *version* and an int64_t *seq_id*.
// The *version* indicates which load generates the change event, and it's same as the publish
// version for the load. The *seq_id* is the sequence number of the change event in this load.
// The information of these change events will be written to binlog files, and BinlogManager will
// manage these binlog files, including generating, reading, and deleting after expiration.
class BinlogManager : public std::enable_shared_from_this<BinlogManager> {
public:
    BinlogManager(std::string path, int64_t max_file_size, int32_t max_page_size, CompressionTypePB compression_type);

    ~BinlogManager();

    // Initialize the binlog.
    Status init();

    // Load single rowset. This method is corresponding to Tablet#laod_rowset
    void load_rowset(const RowsetSharedPtr& rowset);

    // Add a rowset only containing insert data (for duplicate key table)
    Status add_insert_rowset(RowsetSharedPtr rowset);

    // Delete expired binlog
    void delete_expired_binlog();

    // Delete some data to keep the binlog not too large
    void delete_excess_binlog();

    // Delete all of binlog
    void delete_all_binlog();

    // Whether the rowset is used by the binlog.
    bool is_rowset_used(const RowsetId& rowset_id);

    RowsetSharedPtr get_rowset(const RowsetId& rowset_id) {
        std::shared_lock lock(_meta_lock);
        return _rowsets.find(rowset_id)->second;
    }

    // Find the meta of binlog file which may contain a given <version, seq_id>.
    // Return Status::NotFound if there is no such file.
    StatusOr<BinlogFileMetaPBSharedPtr> find_binlog_file(int64_t version, int64_t seq_id);

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

    std::shared_ptr<BinlogReader> create_reader(BinlogReaderParams& reader_params) {
        std::shared_lock lock(_meta_lock);
        int64_t reader_id = _next_reader_id++;
        return std::make_shared<BinlogReader>(shared_from_this(), reader_id, reader_params);
    }

    int32_t num_binlog_files() {
        std::shared_lock lock(_meta_lock);
        return _binlog_file_metas.size();
    }

    std::string binlog_file_name(int32_t file_id) { return BinlogFileWriter::binlog_file_path(_path, file_id); }

private:
    friend class BinlogReader;

    int128_t _get_lsn(int64_t version, int64_t seq_id) { return (((int128_t)version) << 64) | seq_id; }

    StatusOr<std::shared_ptr<BinlogFileWriter>> _create_binlog_writer(int64_t file_id);

    Status _delete_binlog_files(std::vector<std::string>& file_names);

    void _update_metas_after_commit(RowsetSharedPtr new_rowset, std::vector<BinlogFileMetaPBSharedPtr>& new_file_metas);

    void _convert_rowset_id_pb(const RowsetIdPB& rowset_id_pb, RowsetId* rowset_id) {
        rowset_id->hi = rowset_id_pb.hi();
        rowset_id->mi = rowset_id_pb.mi();
        rowset_id->lo = rowset_id_pb.lo();
    }

    // ensure no concurrent ingestion
    std::mutex _write_lock;
    // protect meta read/write
    std::shared_mutex _meta_lock;

    // binlog storage directory
    std::string _path;
    int64_t _max_file_size;
    int32_t _max_page_size;
    CompressionTypePB _compression_type;

    // mapping from start LSN of a binlog file to the file meta. A binlog file
    // with a smaller start LSN also has a smaller file id. The file with the biggest
    // start LSN is the meta of _active_binlog_writer if it's not null.
    // Guarded by _meta_lock
    std::map<int128_t, BinlogFileMetaPBSharedPtr> _binlog_file_metas;
    std::shared_ptr<BinlogFileWriter> _active_binlog_writer;

    // mapping from rowset id to the number of binlog files using it
    // Guarded by _meta_lock
    std::unordered_map<RowsetId, int32_t, HashOfRowsetId> _rowset_count_map;
    // mapping from rowset id to the Rowset
    // Guarded by _meta_lock
    std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId> _rowsets;

    // Allocate an id for each binlog reader
    // Guarded by _meta_lock
    int64_t _next_reader_id;
    // Mapping from the reader id to the readers.
    // Guarded by _meta_lock
    std::unordered_map<int64_t, BinlogReaderSharedPtr> _binlog_readers;
};

} // namespace starrocks
