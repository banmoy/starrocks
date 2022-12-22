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
    return Status::OK();
}

void BinlogDataSource::close(RuntimeState* state) {
    // TODO close
}

Status BinlogDataSource::get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_cpu_time_ns);
    _init_chunk(chunk, 0);
    int32_t num = _chunk_num.fetch_add(1, std::memory_order_relaxed);
    if (num >= 2) {
        return Status::EndOfFile(fmt::format("Has sent {} chunks", num));
    }
    chunk->get()->get_column_by_index(0)->append_datum(Datum(num));
    chunk->get()->get_column_by_index(1)->append_datum(Datum(num));
    chunk->get()->get_column_by_index(2)->append_datum(Datum(num));
    int8_t op = num;
    chunk->get()->get_column_by_index(3)->append_datum(Datum(op));
    int64_t version = num;
    chunk->get()->get_column_by_index(4)->append_datum(Datum(version));
    int64_t seq_id = num;
    chunk->get()->get_column_by_index(5)->append_datum(Datum(seq_id));
    int64_t timestamp = num;
    chunk->get()->get_column_by_index(6)->append_datum(Datum(timestamp));
    return Status::OK();
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
    ;
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
