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

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_load_write_schema(int64_t schema_id, int64_t tablet_id,
                                                                        int64_t txn_id,
                                                                        const TabletMetadataPtr& tablet_meta) {
    // TODO get schemas from
    // 1. global schema cache
    // 2. TabletManager::get_latest_cached_tablet_metadata
    // 3. FE
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::LOAD);
    request.__set_schema_id(schema_id);
    request.__set_tablet_id(tablet_id);
    request.__set_txn_id(txn_id);
    TNetworkAddress master = get_master_address();
    return get_schema_from_fe(request, master);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_load_publish_schema(const TxnLogPB_OpWrite& op_write,
                                                                          int64_t tablet_id, int64_t txn_id,
                                                                          const TabletMetadataPtr& tablet_meta) {
    DCHECK(tablet_meta != nullptr);
    int64_t rowset_schema_id = op_write.has_schema_id() ? op_write.schema_id() : tablet_meta->schema().id();
    // TODO get schema from
    // 1. global schema cache
    // 2. tablet_meta schema + historical schemas
    // 3. FE
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::LOAD);
    request.__set_schema_id(rowset_schema_id);
    request.__set_tablet_id(tablet_id);
    request.__set_txn_id(txn_id);
    TNetworkAddress master = get_master_address();
    return get_schema_from_fe(request, master);
}

void RuntimeSchemaManager::update_load_publish_schema(uint32_t rowset_id, const TabletSchemaCSPtr& rowset_schema,
                                                      TabletMetadata* tablet_meta) {
    auto rowset_schema_id = rowset_schema->id();
    bool new_schema = tablet_meta->schema().id() != rowset_schema_id &&
                      tablet_meta->historical_schemas().count(rowset_schema_id) <= 0;
    if (new_schema) {
        bool new_latest_schema = rowset_schema->schema_version() > tablet_meta->schema().schema_version();
        if (new_latest_schema) {
            // move rowsets with the current latest schema to historical schemas
            auto current_latest_schema_id = tablet_meta->schema().id();
            auto* rowset_to_schema = tablet_meta->mutable_rowset_to_schema();
            for (int i = 0; i < tablet_meta->rowsets_size(); i++) {
                auto rid = tablet_meta->rowsets(i).id();
                if (rowset_to_schema->count(rowset_id) <= 0 && rowset_id != rid) {
                    (*rowset_to_schema)[rid] = current_latest_schema_id;
                }
            }
            if (tablet_meta->historical_schemas().count(current_latest_schema_id) <= 0) {
                auto& item = (*tablet_meta->mutable_historical_schemas())[current_latest_schema_id];
                item.CopyFrom(tablet_meta->schema());
            }
            // no need to put rowset_id to historical
            tablet_meta->mutable_schema()->Clear();
            rowset_schema->to_schema_pb(tablet_meta->mutable_schema());
        } else {
            auto& item = (*tablet_meta->mutable_historical_schemas())[rowset_schema_id];
            rowset_schema->to_schema_pb(&item);
            (*tablet_meta->mutable_rowset_to_schema())[rowset_id] = rowset_schema_id;
        }
    } else {
        bool latest_schema = rowset_schema->schema_version() == tablet_meta->schema().schema_version();
        if (!latest_schema) {
            (*tablet_meta->mutable_rowset_to_schema())[rowset_id] = rowset_schema_id;
        }
    }
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_compaction_publish_schema(
        const TxnLogPB_OpCompaction& op_compaction, int64_t tablet_id, const std::vector<uint32_t>& input_rowsets_id,
        const TabletMetadata& tablet_meta) {
    if (op_compaction.has_schema_id()) {
        auto schema_id = op_compaction.schema_id();
        if (schema_id == tablet_meta.schema().id()) {
            return GlobalTabletSchemaMap::Instance()->emplace(tablet_meta.schema()).first;
        } else if (tablet_meta.historical_schemas().count(schema_id) > 0) {
            return GlobalTabletSchemaMap::Instance()->emplace(tablet_meta.historical_schemas().at(schema_id)).first;
        } else {
            return Status::InternalError(
                    fmt::format("output rowset schema id {} not found in tablet metadata", schema_id));
        }
    } else {
        // for compitible
        return ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(input_rowsets_id, &tablet_meta);
    }
}

void RuntimeSchemaManager::update_compaction_publish_schema(const std::vector<uint32_t>& input_rowsets_id,
                                                            std::optional<uint32_t> output_rowset_id,
                                                            const TabletSchemaCSPtr& output_rowset_schema,
                                                            TabletMetadata* tablet_meta) {
    if (tablet_meta->rowset_to_schema().empty()) {
        // TODO check output_rowset_schema id is equal to tablet_meta->schema().id()
        return;
    }

    for (int i = 0; i < input_rowsets_id.size(); i++) {
        tablet_meta->mutable_rowset_to_schema()->erase(input_rowsets_id[i]);
    }

    if (output_rowset_id.has_value() && output_rowset_schema->id() != tablet_meta->schema().id()) {
        // TODO check whether output_rowset_schema is in historical schemas
        tablet_meta->mutable_rowset_to_schema()->insert({output_rowset_id.value(), output_rowset_schema->id()});
    }

    std::unordered_set<int64_t> schema_id;
    for (auto& pair : tablet_meta->rowset_to_schema()) {
        schema_id.insert(pair.second);
    }

    for (auto it = tablet_meta->mutable_historical_schemas()->begin();
         it != tablet_meta->mutable_historical_schemas()->end();) {
        if (schema_id.find(it->first) == schema_id.end()) {
            it = tablet_meta->mutable_historical_schemas()->erase(it);
        } else {
            it++;
        }
    }
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_scan_schema(const TUniqueId& query_id, int64_t schema_id,
                                                                  int64_t tablet_id, const TNetworkAddress& fe_addr,
                                                                  const TabletMetadataPtr& tablet_meta) {
    // TODO check schema cache and tablet metadata
    // if tablet_meta, try to call TabletManager::get_latest_cached_tablet_metadata
    // need to update schema cache
    TGetRuntimeSchemaRequest request;
    request.__set_schema_type(TRuntimeSchemaType::SCAN);
    request.__set_schema_id(schema_id);
    request.__set_query_id(query_id);
    request.__set_tablet_id(tablet_id);
    return get_schema_from_fe(request, fe_addr);
}

StatusOr<TabletSchemaCSPtr> RuntimeSchemaManager::get_schema_from_fe(const TGetRuntimeSchemaRequest& request,
                                                                     const TNetworkAddress& fe_addr) {
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
    LOG(INFO) << "get_schema success, query_id: " << print_id(request.query_id) << ", schema_id: " << request.schema_id
              << ", db_id: " << request.db_id << ", table_id: " << request.table_id
              << ", tablet_id: " << request.tablet_id << ", schema_type: " << static_cast<int>(request.schema_type);
    return const_schema_ptr;
}

void RuntimeSchemaManager::update_alter_schema(const TabletSchemaPB& schema, TabletMetadata* tablet_meta) {
    auto current_latest_schema_id = tablet_meta->schema().id();
    auto* rowset_to_schema = tablet_meta->mutable_rowset_to_schema();
    for (int i = 0; i < tablet_meta->rowsets_size(); i++) {
        auto rid = tablet_meta->rowsets(i).id();
        if (rowset_to_schema->count(rid) <= 0) {
            (*rowset_to_schema)[rid] = current_latest_schema_id;
        }
    }
    if (tablet_meta->historical_schemas().count(current_latest_schema_id) <= 0) {
        auto& item = (*tablet_meta->mutable_historical_schemas())[current_latest_schema_id];
        item.CopyFrom(tablet_meta->schema());
    }
    tablet_meta->mutable_schema()->Clear();
    tablet_meta->mutable_schema()->CopyFrom(schema);
}

StatusOr<TabletSchemaSPtr> RuntimeSchemaManager::get_rowset_schema(const TabletMetadata& tablet_metadata,
                                                                  uint32_t rowset_id) {
    auto rowset_it = tablet_metadata.rowset_to_schema().find(rowset_id);
    if (rowset_it != tablet_metadata.rowset_to_schema().end()) {
        auto schema_it = tablet_metadata.historical_schemas().find(rowset_it->second);
        if (schema_it != tablet_metadata.historical_schemas().end()) {
            return GlobalTabletSchemaMap::Instance()->emplace(schema_it->second).first;
        } else {
            return Status::InternalError(fmt::format("can not find input rowset schema, id {}", rowset_it->second));
        }
    } else {
        return GlobalTabletSchemaMap::Instance()->emplace(tablet_metadata.schema()).first;
    }
}

} // namespace starrocks