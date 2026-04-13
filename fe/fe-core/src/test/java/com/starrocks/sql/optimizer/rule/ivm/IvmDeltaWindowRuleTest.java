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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmDeltaWindowRuleTest {
    @Test
    public void testTransformPartitionedWindow(@Mocked OlapTable table) {
        WindowRewriteContext rewriteContext = createWindowRewriteContext(table, true);
        List<OptExpression> result =
                new IvmDeltaWindowRule().transform(rewriteContext.deltaWindowExpr(), rewriteContext.context());
        assertRewriteResult(result, rewriteContext.columnRefFactory(), rewriteContext.deltaWindowExpr(),
                LogicalVersionOperator.VersionRefType.FROM_VERSION, LogicalVersionOperator.VersionRefType.TO_VERSION);
    }

    @Test
    public void testTransformPartitionedWindowWithoutActionColumn(@Mocked OlapTable table) {
        WindowRewriteContext rewriteContext = createWindowRewriteContext(table, false);
        List<OptExpression> result =
                new IvmDeltaWindowRule().transform(rewriteContext.deltaWindowExpr(), rewriteContext.context());
        assertRewriteResult(result, rewriteContext.columnRefFactory(), rewriteContext.deltaWindowExpr(),
                LogicalVersionOperator.VersionRefType.FROM_VERSION, LogicalVersionOperator.VersionRefType.TO_VERSION);
    }

    @Test
    public void testTransformRejectsWindowWithoutPartitionBy(@Mocked OlapTable table) {
        new Expectations() {
            {
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column orderColumn = new Column("v1", IntegerType.INT, true);
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(orderRef, orderColumn)),
                        Maps.newHashMap(Map.of(orderColumn, orderRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();

        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setOrderByElements(List.of(new Ordering(orderRef, true, true)))
                .setWindowCall(Map.of(windowRef, new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();
        OptExpression deltaWindowExpr = OptExpression.create(
                new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(window, OptExpression.create(scan)));
        deriveLogicalProperty(deltaWindowExpr);

        Assertions.assertTrue(new IvmDeltaWindowRule().transform(deltaWindowExpr, context).isEmpty());
    }

    private WindowRewriteContext createWindowRewriteContext(OlapTable table, boolean withActionColumn) {
        new Expectations() {
            {
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator partitionRef = columnRefFactory.create("k1", IntegerType.INT, false);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);
        ColumnRefOperator actionRef = withActionColumn
                ? columnRefFactory.create("__op", IntegerType.TINYINT, false)
                : null;

        Column partitionColumn = new Column("k1", IntegerType.INT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, true);
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(partitionRef, partitionColumn, orderRef, orderColumn)),
                        Maps.newHashMap(Map.of(partitionColumn, partitionRef, orderColumn, orderRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();

        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setPartitionExpressions(List.of(partitionRef))
                .setOrderByElements(List.of(
                        new Ordering(orderRef, true, true),
                        new Ordering(partitionRef, true, true)))
                .setEnforceSortColumns(List.of(
                        new Ordering(orderRef, true, true),
                        new Ordering(partitionRef, true, true)))
                .setWindowCall(Map.of(windowRef, new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();
        OptExpression deltaWindowExpr = OptExpression.create(
                new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(window, OptExpression.create(scan)));
        deriveLogicalProperty(deltaWindowExpr);
        return new WindowRewriteContext(columnRefFactory, context, deltaWindowExpr);
    }

    private void assertRewriteResult(List<OptExpression> result,
                                     ColumnRefFactory columnRefFactory,
                                     OptExpression deltaWindowExpr,
                                     LogicalVersionOperator.VersionRefType firstVersion,
                                     LogicalVersionOperator.VersionRefType secondVersion) {
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(1).getOp() instanceof LogicalUnionOperator);

        OptExpression unionExpr = rewritten.inputAt(1);
        Assertions.assertEquals(2, unionExpr.arity());
        Assertions.assertEquals(
                deltaWindowExpr.getOutputColumns().getColumnRefOperators(columnRefFactory),
                ((LogicalUnionOperator) unionExpr.getOp()).getOutputColumnRefOp());

        assertWindowBranch(unionExpr.inputAt(0), firstVersion);
        assertWindowBranch(unionExpr.inputAt(1), secondVersion);
    }

    private void assertWindowBranch(OptExpression branch, LogicalVersionOperator.VersionRefType versionRefType) {
        Assertions.assertTrue(branch.getOp() instanceof LogicalWindowOperator);
        Assertions.assertTrue(branch.inputAt(0).getOp() instanceof LogicalJoinOperator);
        OptExpression leftInput = branch.inputAt(0).inputAt(0);
        Assertions.assertTrue(leftInput.getOp() instanceof LogicalVersionOperator);
        Assertions.assertEquals(versionRefType, ((LogicalVersionOperator) leftInput.getOp()).getVersionRefType());
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }

    private record WindowRewriteContext(ColumnRefFactory columnRefFactory,
                                        OptimizerContext context,
                                        OptExpression deltaWindowExpr) {
    }
}
