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

package com.starrocks.sql.analyzer;

import com.google.common.base.Joiner;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.RangeDistributionDesc;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.optimizer.rule.ivm.IvmRowIdDeriver;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class MaterializedViewAnalyzerTest {
    static StarRocksAssert starRocksAssert;
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        ConnectorPlanTestBase.mockAllCatalogs(starRocksAssert.getCtx(), temp.toURI().toString());

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());

        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view mv\n" +
                        "PARTITION BY k1\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh async\n" +
                        "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;");
    }

    @AfterAll
    public static void afterClass() {
        ConnectorPlanTestBase.dropAllCatalogs();
    }

    @Test
    public void testCreateIcebergTable1() throws Exception {
        {
            String mvName = "iceberg_parttbl_mv1";
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                            "COMMENT \"MATERIALIZED_VIEW\"\n" +
                            "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");
            Table mv = starRocksAssert.getTable("test", mvName);
            Assertions.assertTrue(mv != null);
            starRocksAssert.dropMaterializedView(mvName);
        }

        try {
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_bucket_mv1`\n" +
                            "COMMENT \"MATERIALIZED_VIEW\"\n" +
                            "PARTITION BY ts\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_bucket` as a;");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().
                    contains("Do not support create materialized view when base iceberg table partition transform "));
        }
    }

    @Test
    public void testCreateIcebergTable2() throws Exception {
        String mvName = "iceberg_parttbl_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                        "PARTITION BY date \n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");
        Table mv = starRocksAssert.getTable("test", mvName);
        Assertions.assertTrue(mv != null);
        Assertions.assertTrue(mv instanceof MaterializedView);
        PartitionInfo partitionInfo = ((MaterializedView) mv).getPartitionInfo();
        Assertions.assertTrue(partitionInfo.isListPartition());
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateIcebergTable2WithAdaptive() throws Exception {
        String mvName = "iceberg_parttbl_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                        "PARTITION BY date \n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "properties (\n" +
                        "'replication_num' = '1',\n" +
                        "'partition_refresh_strategy' = 'adaptive'" +
                        ") \n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");
        Table mv = starRocksAssert.getTable("test", mvName);
        Assertions.assertTrue(mv != null);
        Assertions.assertTrue(mv instanceof MaterializedView);
        PartitionInfo partitionInfo = ((MaterializedView) mv).getPartitionInfo();
        Assertions.assertTrue(partitionInfo.isListPartition());
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateIcebergTable3() {
        try {
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_bucket_mv1`\n" +
                            "PARTITION BY (id, data, date_trunc('year', ts))\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_year` as a;");
            Table mv = starRocksAssert.getTable("test", "iceberg_bucket_mv1");
            Assertions.assertTrue(mv != null);
            Assertions.assertTrue(mv instanceof MaterializedView);
            PartitionInfo partitionInfo = ((MaterializedView) mv).getPartitionInfo();
            Assertions.assertTrue(partitionInfo.isListPartition());
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        analyzeSuccess("refresh materialized view mv");
        Database testDb = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "mv");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        mv.setInactiveAndReason(AlterJobMgr.MANUAL_INACTIVE_MV_REASON);
        analyzeFail("refresh materialized view mv");
    }

    @Test
    public void testMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `mv1` (a comment \"a1\", b comment \"b2\", c)\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "DISTRIBUTED BY HASH(a) BUCKETS 12\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"compression\" = \"zstd\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT k1, k2, v1 from test.tbl1");
        {
            ShowResultSet showResultSet = ShowExecutor.execute((ShowStmt) analyzeSuccess("show full columns from mv1"),
                    starRocksAssert.getCtx());
            Assertions.assertEquals("[[a, date, , YES, YES, null, , , a1]," +
                            " [b, int, , YES, YES, null, , , b2]," +
                            " [c, int, , YES, YES, null, , , ]]",
                    showResultSet.getResultRows().toString());
        }
        {
            ShowResultSet showResultSet = ShowExecutor.execute((ShowStmt) analyzeSuccess("show create table mv1"),
                    starRocksAssert.getCtx());
            String result = showResultSet.getResultRows().toString();
            Assertions.assertTrue(result.contains("zstd"));
        }
    }

    @Test
    public void testCreateIncrementalMvStoresRewrittenIvmViewDef() {
        String sql = "create materialized view ivm_row_id_mv\n" +
                "distributed by hash(v1)\n" +
                "refresh incremental as\n" +
                "select v1 from tprimary";
        CreateMaterializedViewStatement statement = (CreateMaterializedViewStatement) analyzeSuccess(sql);

        Assertions.assertNotNull(statement.getIvmViewDef());
        Assertions.assertTrue(statement.getIvmViewDef().contains(IvmRowIdDeriver.DERIVED_ROW_ID_COLUMN_PREFIX));
        Assertions.assertTrue(statement.getQueryStatement().getQueryRelation().getColumnOutputNames().stream()
                .anyMatch(IvmRowIdDeriver::isDerivedRowIdColumnName));
    }

    @Test
    public void testCreateIncrementalMvPreservesWindowAliasAfterRewrite() {
        String sql = "create materialized view ivm_window_alias_mv\n" +
                "distributed by hash(sum_amount)\n" +
                "refresh incremental as\n" +
                "select sum(v2) over(partition by v1 order by pk rows between unbounded preceding and current row) " +
                "as sum_amount\n" +
                "from tprimary";
        CreateMaterializedViewStatement statement = (CreateMaterializedViewStatement) analyzeSuccess(sql);

        Assertions.assertTrue(statement.getIvmViewDef().contains("sum_amount"));
        Assertions.assertEquals("sum_amount", statement.getQueryStatement().getQueryRelation().getColumnOutputNames().get(0));
        Assertions.assertTrue(statement.getQueryStatement().getQueryRelation().getColumnOutputNames().stream()
                .anyMatch(IvmRowIdDeriver::isDerivedRowIdColumnName));
    }

    @Test
    public void testNondeterministicFunction() {
        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select  k1, k2, rand() from tbl1 group by k1, k2",
                "Materialized view query statement select item rand() not supported nondeterministic function.");

        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select k1, k2 from tbl1 group by k1, k2 union select k1, rand() from tbl1",
                "Materialized view query statement select item rand() not supported nondeterministic function.");

        analyzeFail("create materialized view mv partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                        "as select  k1, k2 from tbl1 where rand() > 0.5",
                "Materialized view query statement select item rand() not supported nondeterministic function.");
    }

    @Test
    public void testCreateMvWithNotExistResourceGroup() {
        String sql = "create materialized view mv\n" +
                "PARTITION BY k1\n" +
                "distributed by hash(k2) buckets 3\n" +
                "refresh async\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"resource_group\" = \"not_exist_rg\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;";
        Assertions.assertThrows(DdlException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql),
                "resource_group not_exist_rg does not exist.");
    }

    @Test
    public void testCreateMvWithWindowFunction() throws Exception {
        {
            String mvSql = "create materialized view window_mv_1\n" +
                    "partition by date_trunc('month', k1)\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (partition by date_trunc('month', k1) order by  k2)\n" +
                    "from tbl1 \n";
            starRocksAssert.useDatabase("test").withMaterializedView(mvSql);
        }

        {
            String mvSql = "create materialized view window_mv_2\n" +
                    "partition by k1\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (partition by k1 order by  k2)\n" +
                    "from tbl1 \n";
            starRocksAssert.useDatabase("test").withMaterializedView(mvSql);
        }

        {
            // Not supported
            String mvSql = "create materialized view window_mv_3\n" +
                    "partition by k1\n" +
                    "distributed by hash(k2)\n" +
                    "refresh manual\n" +
                    "as\n" +
                    "select \n" +
                    "\tk2, k1, row_number() over (order by  k2)\n" +
                    "from tbl1 \n";
            analyzeFail(mvSql, "Detail message: window function row_number ’s partition expressions" +
                    " should contain the partition column k1 of materialized view");
        }

        {
            starRocksAssert.withTable("CREATE TABLE t1_event(\n" +
                    "    order_root_id VARCHAR(255),\n" +
                    "    event_type VARCHAR(255),\n" +
                    "    sequence_timestamp datetime\n" + ")\n" +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\"replication_num\"=\"1\");");

            // Not supported
            String mvSql = "CREATE MATERIALIZED VIEW denorm_mv_root_order\n" +
                    "REFRESH ASYNC " +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\n" +
                    "  \"partition_refresh_number\" = \"4\")\n" +
                    "AS\n" +
                    "  SELECT\n" +
                    "    order_root_id, event_type, sequence_timestamp,\n" +
                    "    ROW_NUMBER() OVER (" +
                    "       PARTITION BY order_root_id " +
                    "       ORDER BY sequence_timestamp DESC) AS row_num\n" +
                    "  FROM t1_event";
            analyzeFail(mvSql, "Detail message: window function row_number ’s partition expressions " +
                    "should contain the partition column date_trunc('day', `test`.`t1_event`.`sequence_timestamp`) of" +
                    " materialized view.");

            // Not supported: with subquery
            mvSql = "CREATE MATERIALIZED VIEW denorm_mv_root_order\n" +
                    "REFRESH ASYNC \n" +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\n" + "  \"partition_refresh_number\" = \"4\")\n" +
                    "AS\n" +
                    "SELECT * from  (\n" +
                    "  SELECT\n" +
                    "    order_root_id, event_type, sequence_timestamp,\n" +
                    "    ROW_NUMBER() OVER (" +
                    "       PARTITION BY order_root_id " +
                    "       ORDER BY sequence_timestamp DESC) AS row_num\n" +
                    "  FROM t1_event) t\n" +
                    "ORDER BY sequence_timestamp;\n";
            analyzeFail(mvSql, "Detail message: window function row_number ’s partition expressions " +
                    "should contain the partition column date_trunc('day', `test`.`t1_event`.`sequence_timestamp`) " +
                    "of materialized view.");

            // Supported: with subquery
            mvSql = "CREATE MATERIALIZED VIEW denorm_mv_root_order\n" +
                    "REFRESH ASYNC \n" +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\n" + "  \"partition_refresh_number\" = \"4\")\n" +
                    "AS\n" +
                    "SELECT * from  (\n" +
                    "  SELECT\n" +
                    "    order_root_id, event_type, sequence_timestamp,\n" +
                    "    ROW_NUMBER() OVER (" +
                    "       PARTITION BY order_root_id, date_trunc('DAY', sequence_timestamp) " +
                    "       ORDER BY sequence_timestamp DESC) AS row_num\n" +
                    "  FROM t1_event) t\n" +
                    "ORDER BY sequence_timestamp;\n";
            analyzeSuccess(mvSql);

            // Not supported: with CTE
            mvSql = "CREATE MATERIALIZED VIEW denorm_mv_root_order\n" +
                    "REFRESH ASYNC \n" +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\n" + "  \"partition_refresh_number\" = \"4\")\n" +
                    "AS\n" +
                    " WITH cte1 AS (" +
                    "  SELECT\n" +
                    "    order_root_id, event_type, sequence_timestamp,\n" +
                    "    ROW_NUMBER() OVER (" +
                    "       PARTITION BY order_root_id " +
                    "       ORDER BY sequence_timestamp DESC) AS row_num\n" +
                    "  FROM t1_event) \n" +
                    "SELECT * FROM cte1 \n" +
                    "ORDER BY sequence_timestamp;\n";
            analyzeFail(mvSql, "Detail message: window function row_number ’s partition expressions " +
                    "should contain the partition column date_trunc('day', `test`.`t1_event`.`sequence_timestamp`) " +
                    "of materialized view.");

            // Supported: with CTE
            mvSql = "CREATE MATERIALIZED VIEW denorm_mv_root_order\n" +
                    "REFRESH ASYNC \n" +
                    "PARTITION BY date_trunc('day', sequence_timestamp)\n" +
                    "PROPERTIES(\n" + "  \"partition_refresh_number\" = \"4\")\n" +
                    "AS\n" +
                    " WITH cte1 AS (" +
                    "  SELECT\n" +
                    "    order_root_id, event_type, sequence_timestamp,\n" +
                    "    ROW_NUMBER() OVER (" +
                    "       PARTITION BY order_root_id, date_trunc('DAY', sequence_timestamp) " +
                    "       ORDER BY sequence_timestamp DESC) AS row_num\n" +
                    "  FROM t1_event) \n" +
                    "SELECT * FROM cte1 \n" +
                    "ORDER BY sequence_timestamp;\n";
            analyzeSuccess(mvSql);
        }
    }

    @Test
    public void testCreateMvBaseOnView() throws Exception {
        starRocksAssert.useDatabase("test")
                .withView("create view v1 as select date_trunc('month', k1) as kv1, k2 as kv2 from tbl1");

        analyzeSuccess("create materialized view mv1 partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                "as select kv1 as k1, kv2 as k2 from v1");

        starRocksAssert.useDatabase("test")
                .withView("create view v2(kv1, kv2) as select date_trunc('month', k1), k2 as vv from tbl1");

        analyzeSuccess("create materialized view mv2 partition by k1 distributed by hash(k2) buckets 3 refresh async " +
                "as select kv1 as k1, kv2 as k2 from v2");
    }

    @Test
    public void testGetQueryOutputIndices() {
        checkQueryOutputIndices(Arrays.asList(1, 2, 0, 3), "2,0,1,3", true);
        checkQueryOutputIndices(Arrays.asList(0, 1, 2, 3), "0,1,2,3", false);
        checkQueryOutputIndices(Arrays.asList(3, 2, 1, 0), "3,2,1,0", true);
        checkQueryOutputIndices(Arrays.asList(1, 2, 3, 0), "3,0,1,2", true);
        checkQueryOutputIndices(Arrays.asList(0, 1), "0,1", false);
    }

    @Test
    public void testGetQueryOutputIndicesIgnoreSyntheticColumns() {
        List<Pair<Column, Integer>> mvColumnPairs = Lists.newArrayList();
        mvColumnPairs.add(Pair.create(new Column("pk", IntegerType.BIGINT), -1));
        mvColumnPairs.add(Pair.create(new Column("v1", IntegerType.INT), 0));
        List<Integer> queryOutputIndices = MaterializedViewAnalyzer.getQueryOutputIndices(mvColumnPairs);
        Assertions.assertEquals(List.of(0), queryOutputIndices);
    }

    @Test
    public void testCreateMvAppendOlapIvmRowIdColumns() throws Exception {
        String sql = "create materialized view mv_row_id_append\n" +
                "refresh async\n" +
                "properties(\"refresh_mode\" = \"auto\")\n" +
                "as select v1 from tprimary";
        CreateMaterializedViewStatement statement = (CreateMaterializedViewStatement) analyzeSuccess(sql);
        Assertions.assertEquals(KeysType.PRIMARY_KEYS, statement.getKeysType());
        Assertions.assertEquals(List.of("__row_id_0_pk"), statement.getSortKeys());
        Assertions.assertEquals(List.of(), statement.getQueryOutputIndices());
        List<Column> mvColumns = statement.getMvColumnItems();
        Assertions.assertEquals(2, mvColumns.size());
        Assertions.assertEquals("__row_id_0_pk", mvColumns.get(0).getName());
        Assertions.assertTrue(mvColumns.get(0).isKey());
        Assertions.assertTrue(mvColumns.get(0).isHidden());
        Assertions.assertEquals("v1", mvColumns.get(1).getName());
    }

    private void checkQueryOutputIndices(List<Integer> inputs, String expect, boolean isChanged) {
        List<Pair<Column, Integer>> mvColumnPairs = Lists.newArrayList();
        for (Integer i : inputs) {
            mvColumnPairs.add(Pair.create(new Column("k1", IntegerType.INT), i));
        }
        List<Integer> queryOutputIndices = MaterializedViewAnalyzer.getQueryOutputIndices(mvColumnPairs);
        Assertions.assertTrue(queryOutputIndices.size() == mvColumnPairs.size());
        Assertions.assertEquals(Joiner.on(",").join(queryOutputIndices), expect);
        Assertions.assertEquals(IntStream.range(0, queryOutputIndices.size()).anyMatch(i -> i != queryOutputIndices.get(i)),
                isChanged);
    }

    @Test
    public void testReplicationNum() throws Exception {
        short defaultReplication = Config.default_replication_num;
        final String sql = "create materialized view mv1 refresh manual as " +
                "SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;";

        {
            Config.default_replication_num = 1;
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            starRocksAssert.getCtx());
            MaterializedViewAnalyzer.analyze(statementBase, starRocksAssert.getCtx());
            Assertions.assertEquals("1",
                    statementBase.getProperties().get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        }
        {
            Config.default_replication_num = 3;
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            starRocksAssert.getCtx());
            MaterializedViewAnalyzer.analyze(statementBase, starRocksAssert.getCtx());
            Assertions.assertEquals("3",
                    statementBase.getProperties().get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        }

        Config.default_replication_num = defaultReplication;
    }

    @Test
    public void testCreateMVCheckPartitionNameIgnoreCaseSensitive() {
        analyzeSuccess("create materialized view mv_hive_0 partition by str2date(L_SHIPDATE, '%Y%m%d') refresh manual as " +
                "SELECT l_partkey, L_SHIPDATE  FROM hive0.partitioned_db.lineitem_mul_par3 as a");
    }

    @Test
    public void testCreateMVForceRange() throws Exception {
        boolean oldEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = false;
        try {
            // set default config for async mvs
            UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());

            String sql = "create materialized view test.force_range_mv\n" +
                    "refresh async\n" +
                    "as select k1, k2, sum(v1) as total from tbl1 group by k1, k2;";

            // 1. Default: should NOT be range distribution if Config is false
            CreateMaterializedViewStatement stmt1 = (CreateMaterializedViewStatement) analyzeSuccess(sql);
            Assertions.assertFalse(stmt1.getDistributionDesc() instanceof RangeDistributionDesc);

            // 2. Set session variable to true: should be range distribution
            starRocksAssert.getCtx().getSessionVariable().setEnableRangeDistribution(true);
            try {
                CreateMaterializedViewStatement stmt2 = (CreateMaterializedViewStatement) analyzeSuccess(sql);
                Assertions.assertTrue(stmt2.getDistributionDesc() instanceof RangeDistributionDesc);
            } finally {
                starRocksAssert.getCtx().getSessionVariable().setEnableRangeDistribution(false);
            }

            // 3. Set Config to true: should be range distribution even if session variable is false
            Config.enable_range_distribution = true;
            CreateMaterializedViewStatement stmt3 = (CreateMaterializedViewStatement) analyzeSuccess(sql);
            Assertions.assertTrue(stmt3.getDistributionDesc() instanceof RangeDistributionDesc);

        } finally {
            Config.enable_range_distribution = oldEnableRangeDistribution;
        }
    }
}
