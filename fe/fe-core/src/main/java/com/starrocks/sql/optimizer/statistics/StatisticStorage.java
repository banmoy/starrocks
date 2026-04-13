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


package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface StatisticStorage {
    String CHANGES_ACTION_COLUMN = "__ACTION__";

    // partitionId: RowCount
    default Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
    }

    default void refreshTableStatistic(Table table, boolean isSync) {
    }

    default void refreshColumnStatistics(Table table, List<String> columns, boolean isSync) {
    }

    default void refreshMultiColumnStatistics(Long tableId, boolean isSync) {
    }

    default void refreshHistogramStatistics(Table table, List<String> columns, boolean isSync) {
    }

    /**
     * Overwrite the statistics of `targetPartition` with `sourcePartition`
     */
    default void overwritePartitionStatistics(long tableId, long sourcePartition, long targetPartition) {
    }

    default void updatePartitionStatistics(long tableId, long partition, long rows) {
    }

    default Statistics getChangesStatistics(Table table,
                                            Collection<Partition> partitions,
                                            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                            long fromVersion,
                                            long toVersion) {
        double baseRowCount = 0D;
        if (partitions != null && !partitions.isEmpty()) {
            Map<Long, Optional<Long>> tableStats = getTableStatistics(table.getId(), partitions);
            for (Partition partition : partitions) {
                Optional<Long> partitionRows = tableStats.getOrDefault(partition.getId(), Optional.empty());
                baseRowCount += partitionRows.orElseGet(partition::getRowCount);
            }
        }
        baseRowCount = Math.max(baseRowCount, 1D);

        long versionSpan = Math.max(1L, toVersion - fromVersion);
        double ratio = Math.max(0.001D, Math.min(0.3D, versionSpan * 0.02D));
        double changedKeys = Math.max(1D, baseRowCount * ratio);

        boolean isPrimaryKey = table instanceof OlapTable &&
                ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS;
        boolean isDuplicateKey = table instanceof OlapTable &&
                ((OlapTable) table).getKeysType() == KeysType.DUP_KEYS;

        double changesRowCount = changedKeys;
        if (isPrimaryKey) {
            changesRowCount = changedKeys * 1.5D;
        }
        double maxEstimatedRowCount = Math.max(0L, Config.statistic_max_changes_rows_estimate_value);
        changesRowCount = Math.max(1D, Math.min(maxEstimatedRowCount, changesRowCount));

        List<Map.Entry<ColumnRefOperator, Column>> entries = colRefToColumnMetaMap.entrySet().stream()
                .collect(Collectors.toList());
        List<String> nonActionColumnNames = entries.stream()
                .map(e -> e.getValue().getName())
                .filter(name -> !CHANGES_ACTION_COLUMN.equalsIgnoreCase(name))
                .collect(Collectors.toList());
        List<ColumnStatistic> baseColumnStats = getColumnStatistics(table, nonActionColumnNames);
        Map<String, ColumnStatistic> baseStatsByName = new LinkedHashMap<>();
        for (int i = 0; i < nonActionColumnNames.size(); i++) {
            baseStatsByName.put(nonActionColumnNames.get(i), baseColumnStats.get(i));
        }

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(changesRowCount);
        builder.setTableRowCountMayInaccurate(true);

        for (Map.Entry<ColumnRefOperator, Column> entry : entries) {
            ColumnRefOperator ref = entry.getKey();
            Column column = entry.getValue();
            String columnName = column.getName();

            if (CHANGES_ACTION_COLUMN.equalsIgnoreCase(columnName)) {
                double ndv = isDuplicateKey ? 1D : 2D;
                double min = isDuplicateKey ? 1D : -1D;
                builder.addColumnStatistic(ref, ColumnStatistic.builder()
                        .setMinValue(min)
                        .setMaxValue(1D)
                        .setNullsFraction(0D)
                        .setAverageRowSize(2D)
                        .setDistinctValuesCount(ndv)
                        .build());
                continue;
            }

            ColumnStatistic base = baseStatsByName.getOrDefault(columnName, ColumnStatistic.unknown());
            if (base.isUnknown()) {
                builder.addColumnStatistic(ref, base);
                continue;
            }

            double scaledNdv = Math.max(1D, Math.min(base.getDistinctValuesCount(), changesRowCount));
            if (isPrimaryKey && column.isKey()) {
                scaledNdv = Math.max(1D, Math.min(scaledNdv, changesRowCount));
            }

            double nullFraction = base.getNullsFraction();
            if (Double.isNaN(nullFraction)) {
                nullFraction = 0D;
            }
            nullFraction = Math.max(0D, Math.min(1D, nullFraction));

            builder.addColumnStatistic(ref, ColumnStatistic.buildFrom(base)
                    .setDistinctValuesCount(scaledNdv)
                    .setNullsFraction(nullFraction)
                    .build());
        }
        return builder.build();
    }

    ColumnStatistic getColumnStatistic(Table table, String column);

    List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns);

    /**
     * Return partition-level column statistics, it may not exist
     */
    default Map<Long, List<ColumnStatistic>> getColumnStatisticsOfPartitionLevel(Table table, List<Long> partitions,
                                                                                 List<String> columns) {
        return null;
    }

    default List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns) {
        return columns.stream().
                map(col -> ConnectorTableColumnStats.unknown()).collect(Collectors.toList());
    }

    default List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
        return getConnectorTableStatistics(table, columns);
    }

    default Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        return Maps.newHashMap();
    }
    
    default Map<String, Histogram> getConnectorHistogramStatistics(Table table, List<String> columns) {
        return Maps.newHashMap();
    }

    default Map<String, Histogram> getConnectorHistogramStatisticsSync(Table table, List<String> columns) {
        return getConnectorHistogramStatistics(table, columns);
    }

    default MultiColumnCombinedStatistics getMultiColumnCombinedStatistics(Long tableId) {
        return MultiColumnCombinedStatistics.EMPTY;
    }

    default void expireMultiColumnStatistics(Long tableId) {
    }

    default void expireHistogramStatistics(Long tableId, List<String> columns) {
    }

    default void expireTableAndColumnStatistics(Table table, List<String> columns) {
    }

    default void expireConnectorTableColumnStatistics(Table table, List<String> columns) {
    }

    default void refreshConnectorTableColumnStatistics(Table table, List<String> columns, boolean isSync) {
    }

    default void expireConnectorHistogramStatistics(Table table, List<String> columns) {
    }

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);
}
