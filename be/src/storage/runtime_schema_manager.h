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

    static StatusOr<TabletSchemaCSPtr> get_load_schema(const PUniqueId& load_id, int64_t schema_id, int64_t db_id, int64 table_id, int64_t tablet_id);


    static StatusOr<TabletSchemaCSPtr> get_scan_schema(const TUniqueId& query_id, int64_t schema_id, int64_t db_id, int64 table_id, int64_t tablet_id,
                                                 const TNetworkAddress& coordinator,  const TabletMetadataPtr& tablet_meta = nullptr);

private:
    static StatusOr<TabletSchemaCSPtr> get_schema(const TUniqueId& query_id, int64_t schema_id, int64_t db_id, int64 table_id, int64_t tablet_id,
                                           TRuntimeSchemaType::type schema_type, const TNetworkAddress& coordinator, const TabletMetadataPtr& tablet_meta = nullptr);
};

} // namespace starrocks