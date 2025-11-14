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

#include "storage/runtime_schema_manager.h"

#include "agent/master_info.h"
#include "runtime/client_cache.h"
#include "storage/metadata_util.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_load_schema(int64_t schema_id, int64_t tablet_id,
                                                                  int64_t txn_id, const TabletMetadataPtr& tablet_meta) {
    // TODO check schema cache and tablet metadata
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::LOAD);
    request.__set_schema_id(schema_id);
    request.__set_tablet_id(tablet_id);
    request.__set_txn_id(txn_id);
    TNetworkAddress master = get_master_address();
    return get_schema_from_fe(request, master);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_scan_schema(const TUniqueId& query_id, int64_t schema_id,
                                                                  int64_t tablet_id, const TNetworkAddress& fe_addr,
                                                                  const TabletMetadataPtr& tablet_meta) {
    // TODO check schema cache and tablet metadata
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::SCAN);
    request.__set_schema_id(schema_id);
    request.__set_query_id(query_id);
    request.__set_tablet_id(tablet_id);
    return get_schema_from_fe(request, fe_addr);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_schema_from_fe(const TGetRuntimeSchemaRequest& request, const TNetworkAddress& fe_addr) {
    TBatchGetRuntimeSchemaRequest batch_request;
    batch_request.__set_requests(std::vector<TGetRuntimeSchemaRequest>{request});
    TBatchGetRuntimeSchemaResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
        fe_addr.hostname, fe_addr.port,
            [&batch_request, &result](FrontendServiceConnection& client) {
                client->getRuntimeSchema(result, batch_request);
            },
            config::thrift_rpc_timeout_ms));
    if (result.results.empty()) {
        return Status::NotFound("Result is empty");
    }
    auto& single_result = result.results[0];
    Status status(single_result.status);
    if (!status.ok()) {
        return Status::InternalError("Failed to get runtime schema: " + status.to_string());
    }

    // TODO: get compression type from tablet metadata
    auto compression_type = TCompressionType::LZ4_FRAME;
    TabletSchemaPB schema_pb;
    RETURN_IF_ERROR(convert_t_schema_to_pb_schema(single_result.schema, compression_type, &schema_pb));
    TabletSchemaSPtr schema_ptr = TabletSchema::create(schema_pb);
    TabletSchemaCSPtr const_schema_ptr = schema_ptr;
    LOG(INFO) << "get_schema success, query_id: " << print_id(query_id) << ", schema_id: " << schema_id
                << ", db_id: " << db_id << ", table_id: " << table_id << ", tablet_id: " << tablet_id
                << ", schema_type: " << schema_type;
    return const_schema_ptr;
}


} // namespace starrocks