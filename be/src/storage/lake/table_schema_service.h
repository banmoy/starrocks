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

#include "common/statusor.h"
#include "gen_cpp/FrontendService.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "util/bthreads/single_flight.h"

namespace starrocks::lake {

class TabletManager;

/**
 * @brief Service for managing and retrieving table schemas in the shared-data mode.
 * 
 * This service handles schema retrieval requests for both load and scan operations.
 * It implements a multi-level caching strategy:
 * 1. Local cache: Checks if the schema is already available locally, including metadata cache and tablet metadata
 * 2. Remote fetch: Retrieves the schema from the Frontend (FE) via RPC if not found locally.
 * 3. Fallback: Falls back to tablet metadata if remote retrieval fails or is not supported.
 * 
 * It also uses SingleFlight to merge duplicate RPC requests for the same schema ID, reducing
 * pressure on the Frontend.
 */
class TableSchemaService {
public:
    struct TableSchemaInfo {
        int64_t schema_id;
        int64_t db_id;
        int64_t table_id;
        int64_t tablet_id;
    };

    TableSchemaService(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}
    ~TableSchemaService() = default;

    /**
     * @brief Retrieves the table schema for a load operation.
     * 
     * @param schema_info Basic information to identify the schema.
     * @param txn_id The transaction ID associated with the load.
     * @param tablet_meta Optional pointer to tablet metadata for local lookup.
     * @return A shared pointer to the TabletSchema on success, or an error status.
     */
    StatusOr<TabletSchemaPtr> get_load_schema(const TableSchemaInfo& schema_info, int64_t txn_id,
                                              const TabletMetadataPtr& tablet_meta = nullptr);

    /**
     * @brief Retrieves the table schema for a scan operation.
     * 
     * @param schema_info Basic information to identify the schema.
     * @param query_id The unique ID of the query initiating the scan.
     * @param coordinator_fe The network address of the coordinator FE to contact.
     * @param tablet_meta Pointer to tablet metadata for local lookup.
     * @return A shared pointer to the TabletSchema on success, or an error status.
     */
    StatusOr<TabletSchemaPtr> get_scan_schema(const TableSchemaInfo& schema_info, const TUniqueId& query_id,
                                              const TNetworkAddress& coordinator_fe,
                                              const TabletMetadataPtr& tablet_meta);

private:
    /**
     * @brief Represents the information of the actual RPC sent for a group of merged requests.
     * 
     * This structure reflects the details of the actual RPC call (e.g., target FE, request source,
     * query/txn ID) that was executed on behalf of a group of requests merged by SingleFlight.
     */
    struct LeaderRpcInfo {
        TNetworkAddress fe;                     
        TTableSchemaRequestSource::type source;
        // valid if request_source is SCAN
        TUniqueId query_id;
        // valid if request_source is LOAD
        int64_t txn_id;
    };

    /**
     * @brief Holds the result of a grouped schema retrieval request.
     */
    struct GroupResult {
        LeaderRpcInfo leader_info; 
        StatusOr<TabletSchemaPtr> response;
    };
    using GroupResultPtr = std::shared_ptr<GroupResult>;

    /**
     * @brief Strategies for grouping schema requests in SingleFlight.
     */
    enum class GroupStrategy {
        SCHEMA_AND_FE,    ///< Group by Schema ID and Frontend address.
        SCHEMA_AND_QUERY, ///< Group by Schema ID and Query ID.
        SCHEMA_AND_TXN    ///< Group by Schema ID and Transaction ID.
    };

    /**
     * @brief Attempts to retrieve the schema from local sources.
     * 
     * Checks the global metadata cache and the provided tablet metadata (if any).
     * 
     * @param schema_id The ID of the schema to retrieve.
     * @param tablet_meta Optional pointer to tablet metadata.
     * @return The TabletSchema if found locally, otherwise nullptr.
     */
    TabletSchemaPtr _get_local_schema(int64_t schema_id, const TabletMetadataPtr& tablet_meta);

    /**
     * @brief Retrieves the schema from a remote Frontend via RPC.
     * 
     * Uses SingleFlight to merge duplicate requests and handles retries with
     * different grouping strategies if necessary.
     * 
     * @param request The Thrift request to send.
     * @param fe The address of the target Frontend.
     * @return The retrieved TabletSchema or an error status.
     */
    StatusOr<TabletSchemaPtr> _get_remote_schema(const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Executes the actual RPC call to the Frontend.
     * 
     * This method is called by the SingleFlight mechanism.
     * 
     * @param request The Thrift request to send.
     * @param fe The address of the target Frontend.
     * @return A GroupResultPtr containing the response and leader key.
     */
    GroupResultPtr _send_rpc(const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Generates a unique string key for SingleFlight grouping.
     * 
     * @param strategy The grouping strategy to use.
     * @param request The request containing schema and ID information.
     * @param fe The target Frontend address.
     * @return A string key uniquely identifying the request group.
     */
    std::string _group_key(GroupStrategy strategy, const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Fallback mechanism to retrieve schema from local tablet files.
     * 
     * Used when remote retrieval fails or is not supported by the FE.
     * 
     * @param schema_id The ID of the schema to retrieve.
     * @param tablet_id The ID of the tablet to search in.
     * @return The TabletSchema if found, otherwise an error status.
     */
    StatusOr<TabletSchemaPtr> _fallback_to_tablet_schema(int64_t schema_id, int64_t tablet_id);

    TabletManager* _tablet_mgr;
    bthreads::singleflight::Group<std::string, GroupResultPtr> _rpc_groups;
};

} // namespace starrocks::lake