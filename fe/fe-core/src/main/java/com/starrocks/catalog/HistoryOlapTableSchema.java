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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Optional;

public class HistoryOlapTableSchema {
    // index id -> schema info
    @SerializedName(value = "schemaInfoMap")
    private Map<Long, SchemaInfo> schemaInfoMap;

    public HistoryOlapTableSchema() {
    }

    public HistoryOlapTableSchema(Map<Long, SchemaInfo> schemaInfoMap) {
        this.schemaInfoMap = schemaInfoMap;
    }

    public Optional<SchemaInfo> getSchemaByIndexId(long indexId) {
        return Optional.ofNullable(schemaInfoMap.get(indexId));
    }

    public Optional<SchemaInfo> getSchemaBySchemaId(long schemaId) {
        for (SchemaInfo schemaInfo : schemaInfoMap.values()) {
            if (schemaId == schemaInfo.getId()) {
                return Optional.of(schemaInfo);
            }
        }
        return Optional.empty();
    }
}
