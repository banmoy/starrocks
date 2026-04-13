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
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmRowIdDeriverTest {
    @Test
    public void testUnionAllRewritesRowIdWithChildIndex(@Mocked OlapTable leftTable,
                                                        @Mocked OlapTable rightTable) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator leftPkRef = columnRefFactory.create("pk", IntegerType.INT, false);
        ColumnRefOperator leftValueRef = columnRefFactory.create("v1", IntegerType.INT, false);
        ColumnRefOperator rightPkRef = columnRefFactory.create("pk", IntegerType.INT, false);
        ColumnRefOperator rightValueRef = columnRefFactory.create("v1", IntegerType.INT, false);
        ColumnRefOperator unionValueRef = columnRefFactory.create("v1", IntegerType.INT, false);

        Column pkColumn = new Column("pk", IntegerType.INT, false);
        Column valueColumn = new Column("v1", IntegerType.INT, false);
        new Expectations() {
            {
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                leftTable.getKeyColumnsInOrder();
                result = List.of(pkColumn);
                leftTable.getBaseIndexMetaId();
                result = 1L;

                rightTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                rightTable.getKeyColumnsInOrder();
                result = List.of(pkColumn);
                rightTable.getBaseIndexMetaId();
                result = 1L;
            }
        };

        OptExpression leftScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftPkRef, pkColumn, leftValueRef, valueColumn)),
                        Maps.newHashMap(Map.of(pkColumn, leftPkRef, valueColumn, leftValueRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        OptExpression rightScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightPkRef, pkColumn, rightValueRef, valueColumn)),
                        Maps.newHashMap(Map.of(pkColumn, rightPkRef, valueColumn, rightValueRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());

        LogicalUnionOperator union = LogicalUnionOperator.builder()
                .isUnionAll(true)
                .setOutputColumnRefOp(List.of(unionValueRef))
                .setChildOutputColumns(List.of(List.of(leftValueRef), List.of(rightValueRef)))
                .build();
        OptExpression root = OptExpression.create(union, leftScan, rightScan);

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(root, context);
        Assertions.assertTrue(result.success());
        Assertions.assertEquals(2, result.rootRowIdColumnRefs().size());
        Assertions.assertEquals("__row_id_0_pk", result.rootRowIdColumnRefs().get(0).getName());
        Assertions.assertEquals("__row_id_1_child_index", result.rootRowIdColumnRefs().get(1).getName());

        OptExpression rewrittenUnionExpr = result.rewrittenRoot();
        if (rewrittenUnionExpr.getOp() instanceof LogicalProjectOperator) {
            rewrittenUnionExpr = rewrittenUnionExpr.inputAt(0);
        }
        Assertions.assertTrue(rewrittenUnionExpr.getOp() instanceof LogicalUnionOperator);
        LogicalUnionOperator rewrittenUnion = (LogicalUnionOperator) rewrittenUnionExpr.getOp();
        Assertions.assertEquals(3, rewrittenUnion.getOutputColumnRefOp().size());
        Assertions.assertEquals(3, rewrittenUnion.getChildOutputColumns().get(0).size());
        Assertions.assertEquals(3, rewrittenUnion.getChildOutputColumns().get(1).size());
        Assertions.assertTrue(rewrittenUnionExpr.inputAt(0).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(rewrittenUnionExpr.inputAt(1).getOp() instanceof LogicalProjectOperator);
    }

    @Test
    public void testWindowAppendsRowIdToOrderBy(@Mocked OlapTable table) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator partitionRef = columnRefFactory.create("k1", IntegerType.INT, false);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);

        Column partitionColumn = new Column("k1", IntegerType.INT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, true);
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getKeyColumnsInOrder();
                result = List.of(partitionColumn);
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

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
                .setOrderByElements(List.of(new Ordering(orderRef, true, true)))
                .setEnforceSortColumns(List.of(new Ordering(orderRef, true, true)))
                .setWindowCall(Map.of(windowRef, new CallOperator("row_number", IntegerType.BIGINT, List.of())))
                .build();
        OptExpression root = OptExpression.create(window, OptExpression.create(scan));

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(root, context);
        Assertions.assertTrue(result.success());
        Assertions.assertEquals(List.of(partitionRef), result.rootRowIdColumnRefs());

        LogicalWindowOperator rewrittenWindow = (LogicalWindowOperator) result.rewrittenRoot().getOp();
        Assertions.assertEquals(2, rewrittenWindow.getOrderByElements().size());
        Assertions.assertEquals(orderRef, rewrittenWindow.getOrderByElements().get(0).getColumnRef());
        Assertions.assertEquals(partitionRef, rewrittenWindow.getOrderByElements().get(1).getColumnRef());
        Assertions.assertEquals(2, rewrittenWindow.getEnforceSortColumns().size());
        Assertions.assertEquals(partitionRef, rewrittenWindow.getEnforceSortColumns().get(1).getColumnRef());
    }

    @Test
    public void testWindowWithoutPartitionByIsUnsupported(@Mocked OlapTable table) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator orderRef = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator windowRef = columnRefFactory.create("w1", IntegerType.BIGINT, false);
        Column orderColumn = new Column("v1", IntegerType.INT, true);
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                table.getKeyColumnsInOrder();
                result = List.of(new Column("k1", IntegerType.INT, false));
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

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

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(
                OptExpression.create(window, OptExpression.create(scan)), context);
        Assertions.assertFalse(result.success());
    }

    @Test
    public void testNullableRootRowIdIsEncodedInRewriteMode(@Mocked OlapTable leftTable,
                                                            @Mocked OlapTable rightTable) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator leftPkRef = columnRefFactory.create("left_pk", IntegerType.INT, false);
        ColumnRefOperator rightPkRef = columnRefFactory.create("right_pk", IntegerType.INT, false);
        ColumnRefOperator nullableLeftPkRef = columnRefFactory.create("nullable_left_pk", IntegerType.INT, true);
        ColumnRefOperator nullableRightPkRef = columnRefFactory.create("nullable_right_pk", IntegerType.INT, true);

        Column leftPkColumn = new Column("left_pk", IntegerType.INT, false);
        Column rightPkColumn = new Column("right_pk", IntegerType.INT, false);
        new Expectations() {
            {
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                leftTable.getKeyColumnsInOrder();
                result = List.of(leftPkColumn);
                leftTable.getBaseIndexMetaId();
                result = 1L;

                rightTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                rightTable.getKeyColumnsInOrder();
                result = List.of(rightPkColumn);
                rightTable.getBaseIndexMetaId();
                result = 1L;
            }
        };

        OptExpression leftScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftPkRef, leftPkColumn)),
                        Maps.newHashMap(Map.of(leftPkColumn, leftPkRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        OptExpression rightScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightPkRef, rightPkColumn)),
                        Maps.newHashMap(Map.of(rightPkColumn, rightPkRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());

        LogicalJoinOperator join = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.FULL_OUTER_JOIN)
                .setProjection(new Projection(Map.of(
                        nullableLeftPkRef, leftPkRef,
                        nullableRightPkRef, rightPkRef
                )))
                .build();

        IvmRowIdDeriver.Result result = IvmRowIdDeriver.deriveAndRewrite(
                OptExpression.create(join, leftScan, rightScan), context);
        Assertions.assertTrue(result.success());
        Assertions.assertNull(result.unsupportedReason());
        Assertions.assertEquals(1, result.rootRowIdColumnRefs().size());
        Assertions.assertEquals(TvrOpUtils.COLUMN_ROW_ID, result.rootRowIdColumnRefs().get(0).getName());
        Assertions.assertFalse(result.rootRowIdColumnRefs().get(0).isNullable());
        Assertions.assertTrue(result.rewrittenRoot().getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator rewrittenProject = (LogicalProjectOperator) result.rewrittenRoot().getOp();
        Assertions.assertTrue(rewrittenProject.getColumnRefMap().containsKey(result.rootRowIdColumnRefs().get(0)));
    }

    @Test
    public void testRewriteKeepsExistingEncodedRootRowId(@Mocked OlapTable leftTable,
                                                         @Mocked OlapTable rightTable) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator leftPkRef = columnRefFactory.create("left_pk", IntegerType.INT, false);
        ColumnRefOperator rightPkRef = columnRefFactory.create("right_pk", IntegerType.INT, false);
        ColumnRefOperator nullableLeftPkRef = columnRefFactory.create("nullable_left_pk", IntegerType.INT, true);
        ColumnRefOperator nullableRightPkRef = columnRefFactory.create("nullable_right_pk", IntegerType.INT, true);

        Column leftPkColumn = new Column("left_pk", IntegerType.INT, false);
        Column rightPkColumn = new Column("right_pk", IntegerType.INT, false);
        new Expectations() {
            {
                leftTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                leftTable.getKeyColumnsInOrder();
                result = List.of(leftPkColumn);
                leftTable.getBaseIndexMetaId();
                result = 1L;

                rightTable.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                rightTable.getKeyColumnsInOrder();
                result = List.of(rightPkColumn);
                rightTable.getBaseIndexMetaId();
                result = 1L;
            }
        };

        OptExpression leftScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftPkRef, leftPkColumn)),
                        Maps.newHashMap(Map.of(leftPkColumn, leftPkRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        OptExpression rightScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightPkRef, rightPkColumn)),
                        Maps.newHashMap(Map.of(rightPkColumn, rightPkRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());

        LogicalJoinOperator join = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.FULL_OUTER_JOIN)
                .setProjection(new Projection(Map.of(
                        nullableLeftPkRef, leftPkRef,
                        nullableRightPkRef, rightPkRef
                )))
                .build();

        IvmRowIdDeriver.Result rewritten = IvmRowIdDeriver.deriveAndRewrite(
                OptExpression.create(join, leftScan, rightScan), context);
        Assertions.assertTrue(rewritten.success());

        IvmRowIdDeriver.Result rewrittenAgain = IvmRowIdDeriver.deriveAndRewrite(
                rewritten.rewrittenRoot(), context);
        Assertions.assertTrue(rewrittenAgain.success());
        Assertions.assertEquals(rewritten.rootRowIdColumnRefs(), rewrittenAgain.rootRowIdColumnRefs());
        ColumnRefOperator encodedRowId = rewritten.rootRowIdColumnRefs().get(0);
        Assertions.assertTrue(rewrittenAgain.rewrittenRoot().getRowOutputInfo().getOutputColRefs().contains(encodedRowId));
    }
}
