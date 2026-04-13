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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IVMBasedMvRefreshProcessorOlapTest extends MVTestBase {
    private static final String CREATE_T1 = "CREATE TABLE `t1` (\n" +
            "  `pk` bigint NOT NULL,\n" +
            "  `v1` int NOT NULL,\n" +
            "  `v2` int NOT NULL,\n" +
            "  `v3` int NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "PRIMARY KEY(`pk`)\n" +
            "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ");";

    private static final String CREATE_T2 = "CREATE TABLE `t2` (\n" +
            "  `pk` bigint NOT NULL,\n" +
            "  `v1` int NOT NULL,\n" +
            "  `v2` int NOT NULL,\n" +
            "  `v3` int NOT NULL\n" +
            ") ENGINE=OLAP\n" +
            "PRIMARY KEY(`pk`)\n" +
            "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ");";

    private final List<String> createdMVs = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
    }

    @BeforeEach
    public void setUpTables() throws Exception {
        createdMVs.clear();
        starRocksAssert.withTable(CREATE_T1);
        starRocksAssert.withTable(CREATE_T2);
    }

    @AfterEach
    public void tearDownObjects() throws Exception {
        for (String mvName : createdMVs) {
            connectContext.executeSql("drop materialized view if exists test." + mvName);
        }
        connectContext.executeSql("drop table if exists test.t1 force");
        connectContext.executeSql("drop table if exists test.t2 force");
    }

    @Test
    public void testIntersect() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (1, 10, 100, 1000), (3, 30, 300, 3000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_intersect_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1\n" +
                "INTERSECT\n" +
                "SELECT v1, v2, v3 FROM t2;");
        createdMVs.add("ivm_intersect_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_intersect_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,v3)",
                        "AS SELECT v1, v2, v3 FROM t1\n" +
                                "INTERSECT\n" +
                                "SELECT v1, v2, v3 FROM t2");

        starRocksAssert.refreshMV("refresh materialized view ivm_intersect_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");
        executeInsertSql("insert into t2 values (2, 20, 200, 2000)");

        MaterializedView mv = getMv("ivm_intersect_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_intersect_mv");
        assertThat(plan)
                .contains("INTERSECT", "LEFT ANTI JOIN", "UNION", "TABLE: ivm_intersect_mv");
    }

    @Test
    public void testExcept() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (2, 20, 200, 2000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_except_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1\n" +
                "EXCEPT\n" +
                "SELECT v1, v2, v3 FROM t2;");
        createdMVs.add("ivm_except_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_except_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,v3)",
                        "AS SELECT v1, v2, v3 FROM t1\n" +
                                "EXCEPT\n" +
                                "SELECT v1, v2, v3 FROM t2");

        starRocksAssert.refreshMV("refresh materialized view ivm_except_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");
        executeInsertSql("insert into t2 values (1, 10, 100, 1000)");

        MaterializedView mv = getMv("ivm_except_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_except_mv");
        assertThat(plan)
                .contains("EXCEPT", "LEFT ANTI JOIN", "UNION", "TABLE: ivm_except_mv");
    }

    @Test
    public void testFilter() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_filter_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1 WHERE v2 > 100;");
        createdMVs.add("ivm_filter_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_filter_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (__row_id_0_pk)",
                        "AS SELECT v1, v2, v3 FROM t1",
                        "WHERE v2 > 100");

        starRocksAssert.refreshMV("refresh materialized view ivm_filter_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");

        MaterializedView mv = getMv("ivm_filter_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_filter_mv");
        assertThat(plan)
                .contains("TABLE: ivm_filter_mv", "PREDICATES:", "v2 > 100");
    }

    @Test
    public void testAggregate() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 10, 200, 2000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_aggregate_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, sum(v2) AS sum_v2 FROM t1 GROUP BY v1;");
        createdMVs.add("ivm_aggregate_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_aggregate_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1)",
                        "AS SELECT v1, sum(v2) AS sum_v2 FROM t1 GROUP BY v1",
                        "sum_v2");

        starRocksAssert.refreshMV("refresh materialized view ivm_aggregate_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 10, 300, 3000)");

        MaterializedView mv = getMv("ivm_aggregate_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_aggregate_mv");
        System.out.println(plan);
        assertThat(plan)
                .contains("TABLE: ivm_aggregate_mv", "AGGREGATE");
    }

    @Test
    public void testJoin() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (1, 11, 101, 1001), (3, 30, 300, 3000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_join_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT t1.v1, t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.pk = t2.pk;");
        createdMVs.add("ivm_join_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_join_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (__row_id_0_pk,__row_id_1_pk)",
                        "AS SELECT t1.v1, t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.pk = t2.pk");

        starRocksAssert.refreshMV("refresh materialized view ivm_join_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 30, 300, 3000)");

        MaterializedView mv = getMv("ivm_join_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_join_mv");
        assertThat(plan)
                .contains("TABLE: ivm_join_mv", "HASH JOIN", "TABLE: t1", "TABLE: t2");
    }

    @Test
    public void testAggregateThenJoinOnGroupingKey() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 10, 200, 2001), (3, 20, 300, 3000)");
        executeInsertSql("insert into t2 values (1, 10, 101, 1001), (2, 20, 201, 2001), (3, 30, 301, 3001)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_agg_join_group_key_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "WITH w1 AS (\n" +
                "    SELECT v1, v2, max(v3) AS max_v3 FROM t1 GROUP BY v1, v2\n" +
                ")\n" +
                "SELECT w1.v1, w1.v2, w1.max_v3, t2.pk, t2.v3 FROM w1 JOIN t2 ON w1.v1 = t2.v1;");
        createdMVs.add("ivm_agg_join_group_key_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_agg_join_group_key_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,pk)",
                        "max(v3) AS max_v3",
                        "JOIN t2 ON w1.v1 = t2.v1");

        starRocksAssert.refreshMV("refresh materialized view ivm_agg_join_group_key_mv with sync mode");
        executeInsertSql("insert into t1 values (4, 10, 400, 4002)");
        executeInsertSql("insert into t2 values (4, 10, 401, 4003)");

        MaterializedView mv = getMv("ivm_agg_join_group_key_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_agg_join_group_key_mv");
        System.out.println(plan);
        assertThat(plan)
                .contains("TABLE: ivm_agg_join_group_key_mv", "HASH JOIN", "AGGREGATE", "TABLE: t1", "TABLE: t2");
    }

    @Test
    public void testAggregateThenJoinOnAggregateOutput() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 10, 200, 2001), (3, 20, 300, 3000)");
        executeInsertSql("insert into t2 values (1, 11, 101, 1000), (2, 21, 201, 2001), (3, 31, 301, 3001)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_agg_join_agg_output_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "WITH w1 AS (\n" +
                "    SELECT v1, v2, max(v3) AS max_v3 FROM t1 GROUP BY v1, v2\n" +
                ")\n" +
                "SELECT w1.v1, w1.v2, w1.max_v3, t2.pk, t2.v1 AS t2_v1 FROM w1 JOIN t2 ON w1.max_v3 = t2.v3;");
        createdMVs.add("ivm_agg_join_agg_output_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_agg_join_agg_output_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (v1,v2,pk)",
                        "max(v3) AS max_v3",
                        "JOIN t2 ON w1.max_v3 = t2.v3");

        starRocksAssert.refreshMV("refresh materialized view ivm_agg_join_agg_output_mv with sync mode");
        executeInsertSql("insert into t1 values (4, 30, 400, 4001)");
        executeInsertSql("insert into t2 values (4, 41, 401, 4001)");

        MaterializedView mv = getMv("ivm_agg_join_agg_output_mv");
        String plan = explainMVRefreshExecPlan(mv,
                "explain refresh materialized view ivm_agg_join_agg_output_mv");
        System.out.println(plan);
        assertThat(plan)
                .contains("TABLE: ivm_agg_join_agg_output_mv", "HASH JOIN", "AGGREGATE", "TABLE: t1", "TABLE: t2");
    }

    @Test
    public void testWindow() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 10, 200, 2000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_window_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1,\n" +
                "       sum(v2) OVER (PARTITION BY v1 ORDER BY v2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v2\n" +
                "FROM t1;");
        createdMVs.add("ivm_window_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_window_mv");
        System.out.println(showCreateSql);
        assertThat(showCreateSql)
                .contains("ORDER BY (__row_id_0_pk)",
                        "PARTITION BY v1 ORDER BY v2");

        starRocksAssert.refreshMV("refresh materialized view ivm_window_mv with sync mode");
        executeInsertSql("insert into t1 values (3, 10, 300, 3000)");

        MaterializedView mv = getMv("ivm_window_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_window_mv");
        assertThat(plan)
                .contains("TABLE: ivm_window_mv", "ANALYTIC");
    }

    @Test
    public void testUnionAll() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (3, 30, 300, 3000), (4, 40, 400, 4000)");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_union_all_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v1, v2, v3 FROM t1\n" +
                "UNION ALL\n" +
                "SELECT v1, v2, v3 FROM t2;");
        createdMVs.add("ivm_union_all_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_union_all_mv");
        assertThat(showCreateSql)
                .contains("ORDER BY (__row_id_0_pk,__row_id_1_child_index)",
                        "AS SELECT v1, v2, v3 FROM t1\n" +
                                "UNION ALL\n" +
                                "SELECT v1, v2, v3 FROM t2");

        starRocksAssert.refreshMV("refresh materialized view ivm_union_all_mv with sync mode");
        executeInsertSql("insert into t1 values (5, 50, 500, 5000)");
        executeInsertSql("insert into t2 values (6, 60, 600, 6000)");

        MaterializedView mv = getMv("ivm_union_all_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_union_all_mv");
        assertThat(plan)
                .contains("  7:Project\n" +
                                "  |  <slot 13> : 13: v1\n" +
                                "  |  <slot 14> : 14: v2\n" +
                                "  |  <slot 15> : 15: v3\n" +
                                "  |  <slot 16> : 16: pk\n" +
                                "  |  <slot 17> : 17: c_19\n" +
                                "  |  <slot 21> : CASE WHEN 18: __ACTION__ < 0 THEN 1 ELSE 0 END\n" +
                                "  |  \n" +
                                "  0:UNION",
                        "  5:Project\n" +
                                "  |  <slot 7> : 7: pk\n" +
                                "  |  <slot 8> : 8: v1\n" +
                                "  |  <slot 9> : 9: v2\n" +
                                "  |  <slot 10> : 10: v3\n" +
                                "  |  <slot 12> : 1\n" +
                                "  |  <slot 20> : 20: __ACTION__\n" +
                                "  |  \n" +
                                "  4:OlapScanNode",
                        "  2:Project\n" +
                                "  |  <slot 1> : 1: pk\n" +
                                "  |  <slot 2> : 2: v1\n" +
                                "  |  <slot 3> : 3: v2\n" +
                                "  |  <slot 4> : 4: v3\n" +
                                "  |  <slot 6> : 0\n" +
                                "  |  <slot 19> : 19: __ACTION__\n" +
                                "  |  \n" +
                                "  1:OlapScanNode")
                .contains("TABLE: ivm_union_all_mv", "UNION", "TABLE: t1", "TABLE: t2");
    }

    /**
     * CREATE MATERIALIZED VIEW mv15_non_overlap_window_agg
     * REFRESH MANUAL
     * DISTRIBUTED BY HASH(i3_1, i2_1) BUCKETS 192
     * ORDER BY (i3_1, i2_1)
     * PROPERTIES("refresh_mode"="INCREMENTAL")
     * AS
     * with w1 as (
     *     select
     *         *,
     *         max(i1_1) over(partition by s1_1 order by s2_1) as max_i1_1
     *     from t10
     * )
     * select
     *     i3_1, i2_1,
     *     sum(max_i1_1) as sum_max_i1_1,
     *     max(s1_1) as max_s1_1,
     *     max(s2_1) as max_s2_1,
     *     count(1) as cnt_1
     * from w1
     * group by i3_1, i2_1;
     * @throws Exception
     */
    @Test
    public void testWindowAgg() throws Exception {
        executeInsertSql("insert into t1 values (1, 10, 100, 1000), (2, 20, 200, 2000)");
        executeInsertSql("insert into t2 values (3, 30, 300, 3000), (4, 40, 400, 4000)");



        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_union_all_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "with w1 as (\n" +
                "    select \n" +
                "        *,\n" +
                "        max(v3) over(partition by v1 order by v2) as max_v3\n" +
                "    from t1\n" +
                ")\n" +
                "select \n" +
                "    v2, \n" +
                "    max(v1) as max_v2,\n" +
                "    count(max_v3) as cnt_1\n" +
                "from w1\n" +
                "group by v2;");
        createdMVs.add("ivm_union_all_mv");

        String showCreateSql = getShowCreateMaterializedView("ivm_union_all_mv");
        System.out.println(showCreateSql);

        starRocksAssert.refreshMV("refresh materialized view ivm_union_all_mv with sync mode");
        executeInsertSql("insert into t1 values (5, 50, 500, 5000)");
        executeInsertSql("insert into t2 values (6, 60, 600, 6000)");

        MaterializedView mv = getMv("ivm_union_all_mv");
        String plan = explainMVRefreshExecPlan(mv, "explain refresh materialized view ivm_union_all_mv");
        System.out.println(plan);
    }

    private String getShowCreateMaterializedView(String mvName) throws Exception {
        String showCreateSql = "show create materialized view test." + mvName + ";";
        ShowCreateTableStmt stmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateSql, connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, connectContext);
        List<List<String>> resultRows = showResultSet.getResultRows();
        Assertions.assertEquals(1, resultRows.size());
        return resultRows.get(0).get(1);
    }
}
