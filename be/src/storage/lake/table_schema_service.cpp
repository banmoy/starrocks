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

#include "storage/lake/table_schema_service.h"

#include <fmt/format.h>

#include "agent/master_info.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/client_cache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/tablet_schema_map.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::lake {

StatusOr<TabletSchemaPtr> TableSchemaService::get_load_schema(const TableSchemaInfo& schema_info, int64_t txn_id,
                                                              const TabletMetadataPtr& tablet_meta) {
    int64_t schema_id = schema_info.schema_id;
    TabletSchemaPtr schema = _get_local_schema(schema_id, tablet_meta);
    if (schema != nullptr) {
        return schema;
    }

    TTableSchemaMeta schema_meta;
    schema_meta.__set_schema_id(schema_id);
    schema_meta.__set_db_id(schema_info.db_id);
    schema_meta.__set_table_id(schema_info.table_id);

    TGetTableSchemaRequest request;
    request.__set_schema_meta(schema_meta);
    request.__set_request_source(TTableSchemaRequestSource::LOAD);
    request.__set_txn_id(txn_id);
    request.__set_tablet_id(schema_info.tablet_id);

    TNetworkAddress coordinator_fe = get_master_address();
    auto status_or_schema = _get_remote_schema(request, coordinator_fe);
    if (!status_or_schema.is_not_supported()) {
        return status_or_schema;
    }
    // Only need to fallback for load, scan does compatiblily in LakeDataSource::get_tablet
    return _fallback_to_tablet_schema(schema_id, schema_info.tablet_id);
}

StatusOr<TabletSchemaPtr> TableSchemaService::get_scan_schema(const TableSchemaInfo& schema_info,
                                                              const TUniqueId& query_id,
                                                              const TNetworkAddress& coordinator_fe,
                                                              const TabletMetadataPtr& tablet_meta) {
    int64_t schema_id = schema_info.schema_id;
    TabletSchemaPtr schema = _get_local_schema(schema_id, tablet_meta);
    if (schema != nullptr) {
        return schema;
    }

    TTableSchemaMeta schema_meta;
    schema_meta.__set_schema_id(schema_id);
    schema_meta.__set_db_id(schema_info.db_id);
    schema_meta.__set_table_id(schema_info.table_id);

    TGetTableSchemaRequest request;
    request.__set_schema_meta(schema_meta);
    request.__set_request_source(TTableSchemaRequestSource::SCAN);
    request.__set_tablet_id(schema_info.tablet_id);
    request.__set_query_id(query_id);

    return _get_remote_schema(request, coordinator_fe);
}

TabletSchemaPtr TableSchemaService::_get_local_schema(int64_t schema_id, const TabletMetadataPtr& tablet_meta) {
    auto schema = _tablet_mgr->get_global_schema(schema_id);
    if (schema == nullptr && tablet_meta != nullptr) {
        const TabletSchemaPB* schema_pb = nullptr;
        if (schema_id == tablet_meta->schema().id()) {
            schema_pb = &tablet_meta->schema();
        } else {
            auto it = tablet_meta->historical_schemas().find(schema_id);
            if (it != tablet_meta->historical_schemas().end()) {
                schema_pb = &it->second;
            }
        }
        if (schema_pb != nullptr) {
            schema = GlobalTabletSchemaMap::Instance()->emplace(*schema_pb).first;
            _tablet_mgr->cache_global_schema(schema);
        }
    }
    return schema;
}

StatusOr<TabletSchemaPtr> TableSchemaService::_get_remote_schema(const TGetTableSchemaRequest& request,
                                                                 const TNetworkAddress& fe) {
    int32_t num_retries = 2;
    // should separate by FE for high availability
    GroupStrategy group_strategy = GroupStrategy::SCHEMA_AND_FE;
    TableSchemaService::GroupResultPtr group_result;
    for (int i = 0; i < num_retries; i++) {
        std::string group_key = _group_key(group_strategy, request, fe);
        group_result = _rpc_groups.Do(group_key, [&]() { return _send_rpc(request, fe); });
        auto& response = group_result->response;
        auto& leader = group_result->leader;
        if (response.ok()) {
            break;
        }

        if (response.is_thrift_rpc_error()) {
            if (response.message().find("Invalid method name 'getTableSchema'") != std::string::npos) {
                return Status::NotSupported(fmt::format("FE [{}:{}] haven't upgraded to support table schema service.",
                                                        fe.hostname, fe.port));
            } else {
                break;
            }
        }

        if (response.is_table_not_exists()) {
            break;
        }

        // TODO refine the group strategy when retry
        if (request.request_source == TTableSchemaRequestSource::LOAD) {
            if (leader.source == TTableSchemaRequestSource::LOAD) {
                if (request.txn_id == leader.txn_id) {
                    break;
                } else {
                    group_strategy = GroupStrategy::SCHEMA_AND_TXN;
                }
            } else if (leader.source == TTableSchemaRequestSource::SCAN) {
                group_strategy = GroupStrategy::SCHEMA_AND_TXN;
            } else {
                // unknown source, not retry
                break;
            }
        } else if (request.request_source == TTableSchemaRequestSource::SCAN) {
            if (leader.source == TTableSchemaRequestSource::LOAD) {
                group_strategy = GroupStrategy::SCHEMA_AND_QUERY;
            } else if (leader.source == TTableSchemaRequestSource::SCAN) {
                if (request.query_id == leader.query_id) {
                    break;
                } else {
                    group_strategy = GroupStrategy::SCHEMA_AND_QUERY;
                }
            } else {
                break;
            }
        }
    }
    return group_result->response;
}

TableSchemaService::GroupResultPtr TableSchemaService::_send_rpc(const TGetTableSchemaRequest& request,
                                                                 const TNetworkAddress& fe) {
    // TODO batch requests for different schemas in the future if needed
    TBatchGetTableSchemaRequest request_batch;
    request_batch.__set_requests(std::vector<TGetTableSchemaRequest>{request});

    TBatchGetTableSchemaResponse response_batch;
    Status status = ThriftRpcHelper::rpc<FrontendServiceClient>(
            fe.hostname, fe.port,
            [&request_batch, &response_batch](FrontendServiceConnection& client) {
                client->getTableSchema(response_batch, request_batch);
            },
            config::thrift_rpc_timeout_ms);

    TableSchemaService::GroupResultPtr result = std::make_shared<TableSchemaService::GroupResult>();
    result->leader.fe = fe;
    result->leader.source = request.request_source;
    if (request.request_source == TTableSchemaRequestSource::SCAN) {
        result->leader.query_id = request.query_id;
    } else if (request.request_source == TTableSchemaRequestSource::LOAD) {
        result->leader.txn_id = request.txn_id;
    }

    if (!status.ok()) {
        result->response = status;
        return result;
    }

    if (!response_batch.__isset.responses || response_batch.responses.empty()) {
        result->response = Status::InternalError("response from FE is empty");
        return result;
    }

    auto& response = response_batch.responses[0];
    status = Status(response.status);
    if (!status.ok()) {
        result->response = status;
        return result;
    }

    TabletSchemaPB schema_pb;
    status = convert_t_schema_to_pb_schema(response.schema, &schema_pb);
    if (!status.ok()) {
        result->response =
                Status::InternalError("Failed to convert thrift schema from FE to proto schema: " + status.to_string());
        return result;
    }
    TabletSchemaSPtr schema = TabletSchema::create(schema_pb);
    _tablet_mgr->cache_global_schema(schema);
    result->response = schema;
    return result;
}

std::string TableSchemaService::_group_key(GroupStrategy strategy, const TGetTableSchemaRequest& request,
                                           const TNetworkAddress& fe) {
    int64_t schema_id = request.schema_meta.schema_id;
    switch (strategy) {
    case GroupStrategy::SCHEMA_AND_FE:
        return fmt::format("f{}:{}:{}", schema_id, fe.hostname, fe.port);
    case GroupStrategy::SCHEMA_AND_QUERY:
        return fmt::format("q{}:{}:{}", schema_id, request.query_id.hi, request.query_id.lo);
    case GroupStrategy::SCHEMA_AND_TXN:
        return fmt::format("t{}:{}", schema_id, request.txn_id);
    default:
        return fmt::format("s{}", schema_id);
    }
}

StatusOr<TabletSchemaPtr> TableSchemaService::_fallback_to_tablet_schema(int64_t schema_id, int64_t tablet_id) {
    auto result = _tablet_mgr->get_tablet_schema_by_id(tablet_id, schema_id);
    if (result.ok()) {
        return result;
    } else if (result.is_not_found()) {
        LOG(WARNING) << "No schema file of id=" << schema_id << " for tablet=" << tablet_id;
        auto tablet_res = _tablet_mgr->get_tablet(tablet_id);
        if (tablet_res.ok()) {
            return tablet_res.value()->get_schema();
        } else {
            return tablet_res.status();
        }
    } else {
        return result.status();
    }
}

} // namespace starrocks::lake
