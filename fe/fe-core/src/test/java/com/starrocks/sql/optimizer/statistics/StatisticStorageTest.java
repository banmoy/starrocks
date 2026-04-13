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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StatisticStorageTest extends PlanTestBase {
    private static final String DB_NAME = "test_changes_statistics";
    private static final String TABLE_NAME = "t_changes";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr.getCurrentState().setStatisticStorage(new InMemoryStatisticStorage());
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + TABLE_NAME + " (k1 int, v1 int) PRIMARY KEY(k1) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES('replication_num'='1');");
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
    }

    @Test
    public void testGetChangesStatisticsRespectsConfigCap() {
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Table table = db.getTable(TABLE_NAME);
        Partition partition = table.getPartitions().stream().findFirst().orElseThrow();

        storage.updatePartitionStatistics(table.getId(), partition.getId(), 1000);
        storage.addColumnStatistic(table, "k1", ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(1000)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(1000)
                .build());

        long previous = Config.statistic_max_changes_rows_estimate_value;
        try {
            Config.statistic_max_changes_rows_estimate_value = 400;
            Statistics statistics = storage.getChangesStatistics(
                    table,
                    List.of(partition),
                    ImmutableMap.of(),
                    1,
                    101);
            Assertions.assertEquals(400.0, statistics.getOutputRowCount(), 0.001);
        } finally {
            Config.statistic_max_changes_rows_estimate_value = previous;
        }
    }
}
