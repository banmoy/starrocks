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

package com.starrocks.load.groupcommit;

import com.starrocks.load.streamload.StreamLoadKvParams;

import java.util.Objects;

public class LoadUniqueId {

    private final TableId tableId;
    private final StreamLoadKvParams params;

    public LoadUniqueId(TableId tableId, StreamLoadKvParams params) {
        this.tableId = tableId;
        this.params = params;
    }

    public TableId getTableId() {
        return tableId;
    }

    public StreamLoadKvParams getParams() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadUniqueId that = (LoadUniqueId) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, params);
    }
}
