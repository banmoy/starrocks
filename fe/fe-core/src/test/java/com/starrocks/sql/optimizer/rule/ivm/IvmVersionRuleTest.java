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
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
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

public class IvmVersionRuleTest {
    @Test
    public void testPushVersionThroughJoin(@Mocked OlapTable leftTable,
                                           @Mocked OlapTable rightTable) {
        mockBaseIndex(leftTable, rightTable);

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        OptExpression leftScan = newScan(factory, leftTable, "c1");
        OptExpression rightScan = newScan(factory, rightTable, "c2");
        OptExpression versionJoin = OptExpression.create(version(), OptExpression.create(
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), leftScan, rightScan));
        deriveLogicalProperty(versionJoin);

        List<OptExpression> result = new IvmVersionJoinRule().transform(versionJoin, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalJoinOperator);
        assertVersionChild(result.get(0).inputAt(0));
        assertVersionChild(result.get(0).inputAt(1));
    }

    @Test
    public void testPushVersionThroughAggregate(@Mocked OlapTable table) {
        mockBaseIndex(table);

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator groupKey = factory.create("k1", IntegerType.INT, false);
        OptExpression scan = newSingleColumnScan(table, groupKey);
        LogicalAggregationOperator aggregate =
                new LogicalAggregationOperator(AggType.GLOBAL, List.of(groupKey), Maps.newHashMap());
        OptExpression versionAgg = OptExpression.create(version(), OptExpression.create(aggregate, scan));
        deriveLogicalProperty(versionAgg);

        List<OptExpression> result = new IvmVersionAggregateRule().transform(versionAgg, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalAggregationOperator);
        assertVersionChild(result.get(0).inputAt(0));
    }

    @Test
    public void testPushVersionThroughWindow(@Mocked OlapTable table) {
        mockBaseIndex(table);

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator partitionRef = factory.create("k1", IntegerType.INT, false);
        ColumnRefOperator orderRef = factory.create("v1", IntegerType.INT, false);
        Column partitionColumn = new Column("k1", IntegerType.INT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, false);
        OptExpression scan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(partitionRef, partitionColumn, orderRef, orderColumn)),
                        Maps.newHashMap(Map.of(partitionColumn, partitionRef, orderColumn, orderRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setPartitionExpressions(List.of(partitionRef))
                .setOrderByElements(List.of(new Ordering(orderRef, true, true)))
                .setEnforceSortColumns(List.of(new Ordering(orderRef, true, true)))
                .setWindowCall(Map.of(factory.create("w1", IntegerType.BIGINT, false),
                        new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();
        OptExpression versionWindow = OptExpression.create(version(), OptExpression.create(window, scan));
        deriveLogicalProperty(versionWindow);

        List<OptExpression> result = new IvmVersionWindowRule().transform(versionWindow, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalWindowOperator);
        assertVersionChild(result.get(0).inputAt(0));
    }

    @Test
    public void testPushVersionThroughUnion(@Mocked OlapTable leftTable,
                                            @Mocked OlapTable rightTable) {
        mockBaseIndex(leftTable, rightTable);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator leftRef = factory.create("c1", IntegerType.INT, false);
        ColumnRefOperator rightRef = factory.create("c2", IntegerType.INT, false);
        ColumnRefOperator outputRef = factory.create("u1", IntegerType.INT, false);
        OptExpression versionUnion = OptExpression.create(version(), OptExpression.create(
                LogicalUnionOperator.builder()
                        .isUnionAll(true)
                        .setOutputColumnRefOp(List.of(outputRef))
                        .setChildOutputColumns(List.of(List.of(leftRef), List.of(rightRef)))
                        .build(),
                newSingleColumnScan(leftTable, leftRef),
                newSingleColumnScan(rightTable, rightRef)));
        deriveLogicalProperty(versionUnion);

        List<OptExpression> result = new IvmVersionUnionRule().transform(versionUnion, context);
        assertSetVersionRewrite(result, LogicalUnionOperator.class);
    }

    @Test
    public void testPushVersionThroughIntersect(@Mocked OlapTable leftTable,
                                                @Mocked OlapTable rightTable) {
        mockBaseIndex(leftTable, rightTable);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator leftRef = factory.create("c1", IntegerType.INT, false);
        ColumnRefOperator rightRef = factory.create("c2", IntegerType.INT, false);
        ColumnRefOperator outputRef = factory.create("i1", IntegerType.INT, false);
        OptExpression versionIntersect = OptExpression.create(version(), OptExpression.create(
                new LogicalIntersectOperator.Builder()
                        .setOutputColumnRefOp(List.of(outputRef))
                        .setChildOutputColumns(List.of(List.of(leftRef), List.of(rightRef)))
                        .build(),
                newSingleColumnScan(leftTable, leftRef),
                newSingleColumnScan(rightTable, rightRef)));
        deriveLogicalProperty(versionIntersect);

        List<OptExpression> result = new IvmVersionIntersectRule().transform(versionIntersect, context);
        assertSetVersionRewrite(result, LogicalIntersectOperator.class);
    }

    @Test
    public void testPushVersionThroughExcept(@Mocked OlapTable leftTable,
                                             @Mocked OlapTable rightTable) {
        mockBaseIndex(leftTable, rightTable);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator leftRef = factory.create("c1", IntegerType.INT, false);
        ColumnRefOperator rightRef = factory.create("c2", IntegerType.INT, false);
        ColumnRefOperator outputRef = factory.create("e1", IntegerType.INT, false);
        OptExpression versionExcept = OptExpression.create(version(), OptExpression.create(
                new LogicalExceptOperator.Builder()
                        .setOutputColumnRefOp(List.of(outputRef))
                        .setChildOutputColumns(List.of(List.of(leftRef), List.of(rightRef)))
                        .build(),
                newSingleColumnScan(leftTable, leftRef),
                newSingleColumnScan(rightTable, rightRef)));
        deriveLogicalProperty(versionExcept);

        List<OptExpression> result = new IvmVersionExceptRule().transform(versionExcept, context);
        assertSetVersionRewrite(result, LogicalExceptOperator.class);
    }

    private void assertSetVersionRewrite(List<OptExpression> result, Class<? extends LogicalSetOperator> setType) {
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(setType.isInstance(result.get(0).getOp()));
        Assertions.assertEquals(2, result.get(0).arity());
        assertVersionChild(result.get(0).inputAt(0));
        assertVersionChild(result.get(0).inputAt(1));
    }

    private void assertVersionChild(OptExpression versionExpr) {
        Assertions.assertTrue(versionExpr.getOp() instanceof LogicalVersionOperator);
        LogicalVersionOperator version = (LogicalVersionOperator) versionExpr.getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION, version.getVersionRefType());
        Assertions.assertTrue(versionExpr.inputAt(0).getOp() instanceof LogicalOlapScanOperator);
    }

    private LogicalVersionOperator version() {
        return new LogicalVersionOperator(LogicalVersionOperator.VersionRefType.FROM_VERSION);
    }

    private OptExpression newScan(ColumnRefFactory factory, OlapTable table, String columnName) {
        ColumnRefOperator ref = factory.create(columnName, IntegerType.INT, false);
        return newSingleColumnScan(table, ref);
    }

    private OptExpression newSingleColumnScan(OlapTable table, ColumnRefOperator ref) {
        Column column = new Column(ref.getName(), IntegerType.INT, ref.isNullable());
        return OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(ref, column)),
                        Maps.newHashMap(Map.of(column, ref)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
    }

    private void mockBaseIndex(OlapTable... tables) {
        new Expectations() {
            {
                for (OlapTable table : tables) {
                    table.getBaseIndexMetaId();
                    result = 1L;
                }
            }
        };
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }
}
