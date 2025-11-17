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

#include <fmt/format.h>

#include "agent/master_info.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "storage/metadata_util.h"
#include "storage/tablet_schema_map.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

// schema id -> schema
bthreads::singleflight::Group<int64_t, StatusOr<TabletSchemaCSPtr>> RuntimeSchemaManager::schema_group;

Status RuntimeSchemaManager::update_latest_schema_when_publish(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                               TabletMetadata* tablet_meta) {
    int64_t schema_id = op_write.has_schema_id() ? op_write.schema_id() : tablet_meta->schema().id();
    if (schema_id == tablet_meta->schema().id() || tablet_meta->historical_schemas().count(schema_id) > 0) {
        return Status::OK();
    }

    auto schema = GlobalTabletSchemaMap::Instance()->get(schema_id);
    if (schema == nullptr) {
        ASSIGN_OR_RETURN(schema, get_load_schema_from_fe(schema_id, tablet_meta->id(), txn_id));
    }

    DCHECK(schema != nullptr);
    // NOTE no need to update TabletMetadata::historical_schemas because FE forbids new FSE for legacy tables
    // whose segments does not contain struct column unique id
    if (schema->version() > tablet_meta->schema().schema_version()) {
        tablet_meta->mutable_schema()->Clear();
        schema->to_schema_pb(tablet_meta->mutable_schema());
    }
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_load_schema(int64_t schema_id, int64_t tablet_id, int64_t txn_id,
                                                                  const TabletMetadataPtr& tablet_meta) {
    auto schema = GlobalTabletSchemaMap::Instance()->get(schema_id);
    if (schema == nullptr) {
        const TabletMetadataPtr metadata =
                tablet_meta != nullptr
                        ? tablet_meta
                        : ExecEnv::GetInstance()->lake_tablet_manager()->get_latest_cached_tablet_metadata(tablet_id);
        if (metadata != nullptr) {
            if (schema_id == metadata->schema().id()) {
                schema = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema()).first;
            } else if (metadata->historical_schemas().count(schema_id) > 0) {
                schema = GlobalTabletSchemaMap::Instance()->emplace(metadata->historical_schemas().at(schema_id)).first;
            }
        }
    }
    if (schema != nullptr) {
        return schema;
    }
    return get_load_schema_from_fe(schema_id, tablet_id, txn_id);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_query_schema(const TUniqueId& query_id, int64_t schema_id,
                                                                   int64_t tablet_id, const TNetworkAddress& fe_addr,
                                                                   const TabletMetadataPtr& tablet_meta) {
    auto schema = GlobalTabletSchemaMap::Instance()->get(schema_id);
    if (schema == nullptr) {
        const TabletMetadataPtr metadata =
                tablet_meta != nullptr
                        ? tablet_meta
                        : ExecEnv::GetInstance()->lake_tablet_manager()->get_latest_cached_tablet_metadata(tablet_id);
        if (metadata != nullptr) {
            if (schema_id == metadata->schema().id()) {
                schema = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema()).first;
            } else if (metadata->historical_schemas().count(schema_id) > 0) {
                schema = GlobalTabletSchemaMap::Instance()->emplace(metadata->historical_schemas().at(schema_id)).first;
            }
        }
    }
    if (schema != nullptr) {
        return schema;
    }
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::QUERY);
    request.__set_schema_id(schema_id);
    request.__set_query_id(query_id);
    request.__set_tablet_id(tablet_id);
    return group_schema_from_fe(request, fe_addr);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_load_schema_from_fe(int64_t schema_id, int64_t tablet_id,
                                                                          int64_t txn_id) {
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::LOAD);
    request.__set_schema_id(schema_id);
    request.__set_tablet_id(tablet_id);
    request.__set_txn_id(txn_id);
    return group_schema_from_fe(request, get_master_address());
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::group_schema_from_fe(const TGetRuntimeSchemaRequest& request,
                                                                       const TNetworkAddress& fe_addr) {
    return schema_group.Do(request.schema_id, [&]() { return get_schema_from_fe(request, fe_addr); });
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_schema_from_fe(const TGetRuntimeSchemaRequest& request,
                                                                     const TNetworkAddress& fe_addr) {
    // TODO batch requests for different schemas
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
        // maybe the cluster is in upgrade, and the FE is still in old version
        if (status.message().find("Invalid method name 'getRuntimeSchema'") != std::string::npos) {
            return Status::NotFound(
                    "Maybe the cluster is in upgrade, and the FE is still in old version, and does not support the "
                    "thrift rpc");
        }
        return Status::InternalError("Failed to get runtime schema: " + status.to_string());
    }

    // TODO: get compression type from tablet metadata
    auto compression_type = TCompressionType::LZ4_FRAME;
    TabletSchemaPB schema_pb;
    RETURN_IF_ERROR(convert_t_schema_to_pb_schema(single_result.schema, compression_type, &schema_pb));
    TabletSchemaSPtr schema_ptr = TabletSchema::create(schema_pb);
    // PUT it in cache
    TabletSchemaCSPtr const_schema_ptr = GlobalTabletSchemaMap::Instance()->emplace(schema_ptr).first;
    LOG(INFO) << "get_schema success, query_id: " << print_id(request.query_id) << ", schema_id: " << request.schema_id
              << ", db_id: " << request.db_id << ", table_id: " << request.table_id
              << ", tablet_id: " << request.tablet_id << ", schema_type: " << static_cast<int>(request.schema_type);
    return const_schema_ptr;
}

} // namespace starrocks