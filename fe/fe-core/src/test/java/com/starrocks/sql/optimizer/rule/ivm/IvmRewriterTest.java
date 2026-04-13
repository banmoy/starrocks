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

package com.starrocks.sql.optimizer.rule.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmRewriterTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableIncrementalRefreshMv(false);
    }

    @AfterEach
    public void tearDownObjects() throws Exception {
        connectContext.executeSql("drop materialized view if exists test.ivm_rewriter_pk_shuffle_mv");
        connectContext.executeSql("drop table if exists test.ivm_rewriter_t1 force");
    }

    @Test
    public void testDeriveTargetMvPkShuffleColumnsUsesMvPkOrder() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `ivm_rewriter_t1` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` int NOT NULL,\n" +
                "  `v2` int NOT NULL,\n" +
                "  `v3` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`ivm_rewriter_pk_shuffle_mv`\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"refresh_mode\" = \"incremental\")\n" +
                "AS\n" +
                "SELECT v3, v1, v2, sum(v3) AS sum_v3 FROM ivm_rewriter_t1 GROUP BY v1, v2, v3;");

        MaterializedView mv = getMv("ivm_rewriter_pk_shuffle_mv");
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator v3Ref = columnRefFactory.create("v3", IntegerType.INT, false);
        ColumnRefOperator v1Ref = columnRefFactory.create("v1", IntegerType.INT, false);
        ColumnRefOperator v2Ref = columnRefFactory.create("v2", IntegerType.INT, false);
        ColumnRefOperator sumV3Ref = columnRefFactory.create("sum_v3", IntegerType.BIGINT, false);

        List<ColumnRefOperator> shuffleColumns = IvmRewriter.deriveTargetMvPkShuffleColumns(mv, Map.of(
                v3Ref, mv.getOrderedOutputColumns(true).get(0),
                v1Ref, mv.getOrderedOutputColumns(true).get(1),
                v2Ref, mv.getOrderedOutputColumns(true).get(2),
                sumV3Ref, mv.getOrderedOutputColumns(true).get(3)));

        Assertions.assertEquals(List.of(v1Ref, v2Ref, v3Ref), shuffleColumns);
    }
}
