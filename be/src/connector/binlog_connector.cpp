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

#include "connector/binlog_connector.h"

#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks::connector {
using namespace vectorized;

DataSourceProviderPtr BinlogConnector::create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                                   const TPlanNode& plan_node) const {
    return std::make_unique<BinlogDataSourceProvider>(scan_node, plan_node);
}

// ================================

BinlogDataSourceProvider::BinlogDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _binlog_scan_node(plan_node.stream_scan_node.binlog_scan) {}

DataSourcePtr BinlogDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<BinlogDataSource>(this, scan_range);
}

// ================================

BinlogDataSource::BinlogDataSource(const BinlogDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(scan_range.binlog_scan_range) {}

Status BinlogDataSource::open(RuntimeState* state) {
    const TBinlogScanNode& binlog_scan_node = _provider->_binlog_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(binlog_scan_node.tuple_id);
    ASSIGN_OR_RETURN(_tablet, _get_tablet())
    std::shared_ptr<BinlogManager> binlog_manager = _tablet->binlog_manager();
    if (_scan_range.__isset.offset && _scan_range.offset.version > -1 && _scan_range.offset.lsn > -1) {
        _start_version = _scan_range.offset.version;
        _start_seq_id = _scan_range.offset.lsn;
        _max_version = _start_version + 1;
    } else {
        std::pair<int64_t, int64_t> pair = binlog_manager->lowest_offset();
        _start_version = pair.first;
        _start_seq_id = pair.second;

        pair = binlog_manager->highest_offset();
        _max_version = pair.first + 1;
    }
    LOG(INFO) << "Tablet id " << _tablet->tablet_uid() <<  ", start_version " << _start_version
              << ", start_seq_id " << _start_seq_id << ", max_version " << _max_version;
    BinlogReaderParams reader_params;
    reader_params.chunk_size = state->chunk_size();
    ASSIGN_OR_RETURN(reader_params.output_schema, _build_schema())
    _binlog_reader = binlog_manager->create_reader(reader_params);
    return Status::OK();
}

void BinlogDataSource::close(RuntimeState* state) {
    if (_binlog_reader != nullptr) {
        _binlog_reader->close();
        _binlog_reader.reset();
    }
}

Status BinlogDataSource::get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);
    _init_chunk(chunk, state->chunk_size());
    if (!_seek_reader) {
        _seek_reader = true;
        RETURN_IF_ERROR(_binlog_reader->seek(_start_version, _start_seq_id));
    }
    RETURN_IF_ERROR(_binlog_reader->get_next(chunk, _max_version));
    return Status::OK();
}

StatusOr<vectorized::VectorizedSchema> BinlogDataSource::_build_schema() {
    BinlogMetaFieldMap binlog_meta_map = build_binlog_meta_fields(_tablet->tablet_schema().num_columns());
    std::vector<uint32_t> data_columns;
    std::vector<uint32_t> meta_column_slot_index;
    vectorized::VectorizedFields meta_fields;
    int slot_index = -1;
    for (auto slot : _tuple_desc->slots()) {
        DCHECK(slot->is_materialized());
        slot_index += 1;
        int32_t index = _tablet->field_index(slot->col_name());
        if (index >= 0) {
            data_columns.push_back(index);
        } else if (slot->col_name() == BINLOG_OP) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_OP]);
        } else if (slot->col_name() == BINLOG_VERSION) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_VERSION]);
        } else if (slot->col_name() == BINLOG_SEQ_ID) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_SEQ_ID]);
        } else if (slot->col_name() == BINLOG_TIMESTAMP) {
            meta_column_slot_index.push_back(slot_index);
            meta_fields.emplace_back(binlog_meta_map[BINLOG_TIMESTAMP]);
        } else {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    }

    if (data_columns.empty()) {
        return Status::InternalError("failed to build binlog schema, no materialized data slot!");
    }

    const TabletSchema& tablet_schema = _tablet->tablet_schema();
    vectorized::VectorizedSchema schema =
            ChunkHelper::convert_schema_to_format_v2(tablet_schema, data_columns);
    for (int32_t i = 0; i < meta_column_slot_index.size(); i++) {
        uint32_t index = meta_column_slot_index[i];
        schema.insert(index, meta_fields[i]);
    }

    return schema;
}

Status BinlogDataSource::_mock_chunk(vectorized::Chunk* chunk) {
    int32_t num = _chunk_num.fetch_add(1, std::memory_order_relaxed);
    if (num >= 2) {
        return Status::EndOfFile(fmt::format("Has sent {} chunks", num));
    }
    chunk->get_column_by_index(0)->append_datum(Datum(num));
    chunk->get_column_by_index(1)->append_datum(Datum(num));
    chunk->get_column_by_index(2)->append_datum(Datum(num));
    int8_t op = num;
    chunk->get_column_by_index(3)->append_datum(Datum(op));
    int64_t version = num;
    chunk->get_column_by_index(4)->append_datum(Datum(version));
    int64_t seq_id = num;
    chunk->get_column_by_index(5)->append_datum(Datum(seq_id));
    int64_t timestamp = num;
    chunk->get_column_by_index(6)->append_datum(Datum(timestamp));
}

int64_t BinlogDataSource::raw_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_rows_read() const {
    return _rows_read_number;
}
int64_t BinlogDataSource::num_bytes_read() const {
    return _bytes_read;
}

int64_t BinlogDataSource::cpu_time_spent() const {
    return _cpu_time_ns;
}

StatusOr<TabletSharedPtr> BinlogDataSource::_get_tablet() {
    TTabletId tablet_id = _scan_range.tablet_id;
    std::string err;
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
    if (!tablet) {
        std::stringstream ss;
        ss << "failed to get tablet. tablet_id=" << tablet_id << ", reason=" << err;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    return tablet;
}

} // namespace starrocks::connector
