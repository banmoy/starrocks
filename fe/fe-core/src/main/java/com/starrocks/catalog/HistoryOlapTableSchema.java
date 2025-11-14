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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HistoryOlapTableSchema {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    // the history schema may be used by those transactions before this txn id
    @SerializedName(value = "txnIdThreshold")
    private long txnIdThreshold = -1;
    // schema id -> schema info
    @SerializedName(value = "schemaInfoMap")
    private Map<Long, SchemaInfo> schemaInfoMap;

    public HistoryOlapTableSchema() {
    }

    public HistoryOlapTableSchema(long dbId, long tableId, long txnIdThreshold, Map<Long, SchemaInfo> schemaInfoMap) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.txnIdThreshold = txnIdThreshold;
        this.schemaInfoMap = new HashMap<>();
    }

    public long getTxnIdThreshold() {
        return txnIdThreshold;
    }

    public Optional<SchemaInfo> getSchemaInfo(long schemaId) {
        return Optional.ofNullable(schemaInfoMap.get(schemaId));
    }
}
