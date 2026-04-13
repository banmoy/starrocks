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

package com.starrocks.sql.spm;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.ivm.IvmRowIdDeriver;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class LogicalPlan2SQLBuilderTest extends PlanTestBase {
    @Test
    public void testJoinWithoutHint() throws Exception {
        String sql = "select t0.v1, t1.v4 from t0 join t1 on t0.v2 = t1.v5 where t0.v3 > 10";
        ExecPlan plan = getExecPlan(sql);
        LogicalPlan2SQLBuilder builder = new LogicalPlan2SQLBuilder();

        String newSql = builder.toSQL(plan.getLogicalPlan());
        assertContains(newSql, "SELECT v1, v4");
        assertContains(newSql, "INNER JOIN");
        assertContains(newSql, "ON v2 = v5");
        assertContains(newSql, "WHERE v3 > 10");
        assertNotContains(newSql, "[");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "INNER JOIN");
    }

    @Test
    public void testProjectAggTopN() throws Exception {
        String sql = "select v1 + 1 as x, count(*) as cnt from t0 group by v1 order by x limit 5";
        ExecPlan plan = getExecPlan(sql);
        LogicalPlan2SQLBuilder builder = new LogicalPlan2SQLBuilder();

        String newSql = builder.toSQL(plan.getLogicalPlan());
        assertContains(newSql, "SELECT");
        assertContains(newSql, "count()");
        assertContains(newSql, "GROUP BY v1");
        assertContains(newSql, "ORDER BY");
        assertContains(newSql, "LIMIT 5");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "AGGREGATE");
        assertContains(newPlan, "TOP-N");
    }

    @Test
    public void testWindow() throws Exception {
        String sql = "select v1, row_number() over(order by v2) as rn from t0";
        ExecPlan plan = getExecPlan(sql);
        LogicalPlan2SQLBuilder builder = new LogicalPlan2SQLBuilder();

        String newSql = builder.toSQL(plan.getLogicalPlan());
        assertContains(newSql, "row_number()OVER (ORDER BY v2 ASC");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "ANALYTIC");
    }

    @Test
    public void testUnionAll() throws Exception {
        String sql = "select v1 from t0 union all select v4 from t1";
        ExecPlan plan = getExecPlan(sql);
        LogicalPlan2SQLBuilder builder = new LogicalPlan2SQLBuilder();

        String newSql = builder.toSQL(plan.getLogicalPlan());
        assertContains(newSql, "UNION ALL");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "UNION");
    }

    @Test
    public void testPrimaryKeyJoinWithDerivedRowId() throws Exception {
        String sql = "select tp.v1, tp1.v3 "
                + "from tprimary tp join tprimary1 tp1 on tp.pk = tp1.pk1";
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        LogicalPlan logicalPlan = UtFrameUtils.getQueryLogicalPlan(connectContext, columnRefFactory, statement);
        OptimizerContext optimizerContext = OptimizerFactory.initContext(connectContext, columnRefFactory);

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(logicalPlan.getRoot(), optimizerContext);
        org.junit.jupiter.api.Assertions.assertTrue(result.success());
        org.junit.jupiter.api.Assertions.assertEquals(2, result.rootRowIdColumnRefs().size());

        List<ColumnRefOperator> outputs = new ArrayList<>(logicalPlan.getOutputColumn());
        outputs.addAll(result.rootRowIdColumnRefs());

        LogicalPlan2SQLBuilder builder = new LogicalPlan2SQLBuilder();
        String newSql = builder.toSQL(result.rewrittenRoot(), outputs);

        System.out.println(newSql);

        assertContains(newSql, "SELECT");
        assertContains(newSql, "v1");
        assertContains(newSql, "v3");
        assertContains(newSql, "pk");
        assertContains(newSql, "pk1");
        assertContains(newSql, "INNER JOIN");
        assertNotContains(newSql, "[");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "INNER JOIN");
    }
}
