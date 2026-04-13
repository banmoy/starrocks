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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmDeltaIntersectRuleTest {
    @Test
    public void testTransformIntersectDetail(@Mocked OlapTable leftTable,
                                             @Mocked OlapTable rightTable) {
        SetRewriteContext<LogicalIntersectOperator> rewriteContext =
                createIntersectRewriteContext(leftTable, rightTable, true);
        List<OptExpression> result =
                new IvmDeltaIntersectRule().transform(rewriteContext.deltaExpr(), rewriteContext.context());
        assertRewriteResult(result, rewriteContext.deltaExpr());
    }

    @Test
    public void testTransformIntersectDetailWithoutAction(@Mocked OlapTable leftTable,
                                                           @Mocked OlapTable rightTable) {
        SetRewriteContext<LogicalIntersectOperator> rewriteContext =
                createIntersectRewriteContext(leftTable, rightTable, false);
        List<OptExpression> result =
                new IvmDeltaIntersectRule().transform(rewriteContext.deltaExpr(), rewriteContext.context());
        assertRewriteResult(result, rewriteContext.deltaExpr());
    }

    private SetRewriteContext<LogicalIntersectOperator> createIntersectRewriteContext(OlapTable leftTable,
                                                                                      OlapTable rightTable,
                                                                                      boolean withAction) {
        new Expectations() {
            {
                leftTable.getBaseIndexMetaId();
                result = 1L;
                rightTable.getBaseIndexMetaId();
                result = 1L;
            }
        };

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator leftRef = columnRefFactory.create("c1", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("c2", IntegerType.INT, false);
        ColumnRefOperator leftRef2 = columnRefFactory.create("d1", IntegerType.INT, false);
        ColumnRefOperator rightRef2 = columnRefFactory.create("d2", IntegerType.INT, false);
        ColumnRefOperator actionRef = withAction ? columnRefFactory.create("__op", IntegerType.TINYINT, false) : null;

        Column leftColumn = new Column("c1", IntegerType.INT, false);
        Column rightColumn = new Column("c2", IntegerType.INT, false);
        Column leftColumn2 = new Column("d1", IntegerType.INT, false);
        Column rightColumn2 = new Column("d2", IntegerType.INT, false);

        OptExpression leftScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn, rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef, rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        OptExpression rightScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(leftRef2, leftColumn2, rightRef2, rightColumn2)),
                        Maps.newHashMap(Map.of(leftColumn2, leftRef2, rightColumn2, rightRef2)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build());

        LogicalIntersectOperator intersect = new LogicalIntersectOperator.Builder()
                .setOutputColumnRefOp(List.of(leftRef, rightRef))
                .setChildOutputColumns(List.of(List.of(leftRef, rightRef), List.of(leftRef2, rightRef2)))
                .build();
        OptExpression deltaIntersectExpr = OptExpression.create(
                new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(intersect, leftScan, rightScan));
        deriveLogicalProperty(deltaIntersectExpr);
        return new SetRewriteContext<>(columnRefFactory, context, deltaIntersectExpr);
    }

    private void assertRewriteResult(List<OptExpression> result, OptExpression deltaExpr) {
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(0).getOp() instanceof LogicalCTEProduceOperator);
        Assertions.assertTrue(rewritten.inputAt(0).inputAt(0).getOp() instanceof LogicalAggregationOperator);
        Assertions.assertTrue(rewritten.inputAt(0).inputAt(0).inputAt(0).getOp() instanceof LogicalUnionOperator);

        OptExpression finalUnion = rewritten.inputAt(1).inputAt(1).inputAt(1);
        Assertions.assertTrue(finalUnion.getOp() instanceof LogicalUnionOperator);
        Assertions.assertEquals(2, finalUnion.arity());
        Assertions.assertEquals(deltaExpr.getOutputColumns().getStream().count(),
                ((LogicalUnionOperator) finalUnion.getOp()).getOutputColumnRefOp().size());

        assertDiffBranch(finalUnion.inputAt(0));
        assertDiffBranch(finalUnion.inputAt(1));

        OptExpression oldProducer = rewritten.inputAt(1).inputAt(0).inputAt(0);
        Assertions.assertTrue(oldProducer.getOp() instanceof LogicalIntersectOperator);
        Assertions.assertTrue(oldProducer.inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertTrue(oldProducer.inputAt(0).inputAt(0).getOp() instanceof LogicalVersionOperator);

        OptExpression newProducer = rewritten.inputAt(1).inputAt(1).inputAt(0).inputAt(0);
        Assertions.assertTrue(newProducer.getOp() instanceof LogicalIntersectOperator);
        Assertions.assertTrue(newProducer.inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertTrue(newProducer.inputAt(0).inputAt(0).getOp() instanceof LogicalVersionOperator);
    }

    private void assertDiffBranch(OptExpression branch) {
        Assertions.assertTrue(branch.getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(branch.inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.LEFT_ANTI_JOIN, ((LogicalJoinOperator) branch.inputAt(0).getOp()).getJoinType());
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }

    private record SetRewriteContext<T>(ColumnRefFactory columnRefFactory,
                                        OptimizerContext context,
                                        OptExpression deltaExpr) {
    }
}
