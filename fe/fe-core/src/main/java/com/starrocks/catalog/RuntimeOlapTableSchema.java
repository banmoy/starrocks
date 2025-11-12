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

import com.starrocks.thrift.TTabletSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RuntimeOlapTableSchema {
    // schema id -> materialized index meta
    private final Map<Long, MaterializedIndexMeta> materializedIndexMetaMap;
    private final List<Index> indexes;
    private final Set<ColumnId> bloomFilterColumnNames;
    private final double bloomFilterFpp;
    public RuntimeOlapTableSchema(List<Index> indexes, Set<ColumnId> bloomFilterColumnNames, double bloomFilterFpp) {
        this.materializedIndexMetaMap = new HashMap<>();
        this.indexes = indexes;
        this.bloomFilterColumnNames = bloomFilterColumnNames;
        this.bloomFilterFpp = bloomFilterFpp;
    }

    public void addMaterializedIndexMeta(MaterializedIndexMeta materializedIndexMeta) {
        materializedIndexMetaMap.put(materializedIndexMeta.getSchemaId(), materializedIndexMeta);
    }


    public Optional<TTabletSchema> getRuntimeTabletSchema(long schemaId) {
        MaterializedIndexMeta indexMeta = materializedIndexMetaMap.get(schemaId);
        if (indexMeta == null) {
            return Optional.empty();
        }

        SchemaInfo schemaInfo = SchemaInfo.newBuilder()
                .setId(schemaId)
                .setVersion(indexMeta.getSchemaVersion())
                .setSchemaHash(indexMeta.getSchemaHash())
                .setKeysType(indexMeta.getKeysType())
                .setShortKeyColumnCount(indexMeta.getShortKeyColumnCount())
                .setStorageType(indexMeta.getStorageType())
                .addColumns(indexMeta.getSchema())
                .setSortKeyIndexes(indexMeta.getSortKeyIdxes())
                .setSortKeyUniqueIds(indexMeta.getSortKeyUniqueIds())
                .setIndexes(OlapTable.getIndexesBySchema(indexes, indexMeta.getSchema()))
                .setBloomFilterColumnNames(bloomFilterColumnNames)
                .setBloomFilterFpp(bloomFilterFpp)
                .build();
        return Optional.of(schemaInfo.toTabletSchema());
    }
}
