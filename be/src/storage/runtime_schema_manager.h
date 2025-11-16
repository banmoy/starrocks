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

#include "gen_cpp/FrontendService.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class RuntimeSchemaManager {
public:
    RuntimeSchemaManager();
    ~RuntimeSchemaManager();

    static StatusOr<TabletSchemaCSPtr> get_load_write_schema(int64_t schema_id, int64_t tablet_id, int64_t txn_id,
                                                             const TabletMetadataPtr& tablet_meta = nullptr);

    static StatusOr<TabletSchemaCSPtr> get_load_publish_schema(const TxnLogPB_OpWrite& op_write, int64_t tablet_id,
                                                               int64_t txn_id, const TabletMetadataPtr& tablet_meta);

    static void update_load_publish_schema(uint32_t rowset_id, const TabletSchemaCSPtr& rowset_schema,
                                           TabletMetadata* tablet_meta);

    static StatusOr<TabletSchemaCSPtr> get_compaction_publish_schema(const TxnLogPB_OpCompaction& op_compaction,
                                                                     int64_t tablet_id,
                                                                     const std::vector<uint32_t>& input_rowsets_id,
                                                                     const TabletMetadataPtr& tablet_meta);

    static void update_compaction_publish_schema(const std::vector<uint32_t>& input_rowsets_id,
                                                 std::optional<uint32_t> output_rowset_id,
                                                 const TabletSchemaCSPtr& output_rowset_schema,
                                                 TabletMetadata* tablet_meta);

    static StatusOr<TabletSchemaCSPtr> get_scan_schema(const TUniqueId& query_id, int64_t schema_id, int64_t tablet_id,
                                                       const TNetworkAddress& fe_addr,
                                                       const TabletMetadataPtr& tablet_meta = nullptr);

    static void update_alter_schema(const TabletSchemaPB& schema, TabletMetadata* tablet_meta);

private:
    static StatusOr<TabletSchemaCSPtr> get_schema_from_fe(const TGetRuntimeSchemaRequest& request,
                                                          const TNetworkAddress& fe_addr);
};

} // namespace starrocks