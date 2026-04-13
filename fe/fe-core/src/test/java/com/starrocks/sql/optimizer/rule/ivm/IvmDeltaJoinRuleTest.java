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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmDeltaJoinRuleTest {
    @Test
    public void testTransformInnerJoin(@Mocked OlapTable leftTable,
                                       @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalUnionOperator);
        LogicalUnionOperator union = (LogicalUnionOperator) rewritten.getOp();
        Assertions.assertTrue(union.isUnionAll());
        Assertions.assertEquals(2, rewritten.arity());
        Assertions.assertTrue(union.getOutputColumnRefOp().contains(actionRef));

        OptExpression firstJoin = rewritten.inputAt(0);
        Assertions.assertTrue(firstJoin.getOp() instanceof LogicalJoinOperator);
        Assertions.assertTrue(firstJoin.inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(firstJoin.inputAt(1).getOp() instanceof LogicalVersionOperator);
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION,
                ((LogicalVersionOperator) firstJoin.inputAt(1).getOp()).getVersionRefType());

        OptExpression secondJoin = rewritten.inputAt(1);
        Assertions.assertTrue(secondJoin.getOp() instanceof LogicalJoinOperator);
        Assertions.assertTrue(secondJoin.inputAt(0).getOp() instanceof LogicalVersionOperator);
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION,
                ((LogicalVersionOperator) secondJoin.inputAt(0).getOp()).getVersionRefType());
        Assertions.assertTrue(secondJoin.inputAt(1).getOp() instanceof LogicalDeltaOperator);
    }

    @Test
    public void testTransformLeftOuterJoin(@Mocked OlapTable leftTable,
                                           @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);
        Assertions.assertEquals(2, result.get(0).arity());
        Assertions.assertTrue(result.get(0).inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.INNER_JOIN,
                ((LogicalJoinOperator) result.get(0).inputAt(0).inputAt(0).getOp()).getJoinType());
        Assertions.assertTrue(result.get(0).inputAt(1).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(result.get(0).inputAt(1).inputAt(0).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(result.get(0).inputAt(1).inputAt(0).inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.LEFT_ANTI_JOIN,
                ((LogicalJoinOperator) result.get(0).inputAt(1).inputAt(0).inputAt(0).getOp()).getJoinType());
    }

    @Test
    public void testTransformFullOuterJoin(@Mocked OlapTable leftTable,
                                           @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.FULL_OUTER_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);
        Assertions.assertEquals(3, result.get(0).arity());

        Assertions.assertTrue(result.get(0).inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.INNER_JOIN,
                ((LogicalJoinOperator) result.get(0).inputAt(0).inputAt(0).getOp()).getJoinType());

        Assertions.assertTrue(result.get(0).inputAt(1).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(result.get(0).inputAt(1).inputAt(0).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(result.get(0).inputAt(1).inputAt(0).inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.LEFT_ANTI_JOIN,
                ((LogicalJoinOperator) result.get(0).inputAt(1).inputAt(0).inputAt(0).getOp()).getJoinType());

        Assertions.assertTrue(result.get(0).inputAt(2).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(result.get(0).inputAt(2).inputAt(0).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(result.get(0).inputAt(2).inputAt(0).inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.RIGHT_ANTI_JOIN,
                ((LogicalJoinOperator) result.get(0).inputAt(2).inputAt(0).inputAt(0).getOp()).getJoinType());
    }

    @Test
    public void testTransformRightOuterJoin(@Mocked OlapTable leftTable,
                                            @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.RIGHT_OUTER_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);
        Assertions.assertEquals(2, result.get(0).arity());
    }

    @Test
    public void testTransformLeftAntiJoinUnsupported(@Mocked OlapTable leftTable,
                                                     @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_ANTI_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(0).getOp() instanceof LogicalCTEProduceOperator);
        Assertions.assertTrue(rewritten.inputAt(1).getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(1).inputAt(0).getOp() instanceof LogicalCTEProduceOperator);
        Assertions.assertTrue(rewritten.inputAt(1).inputAt(1).getOp() instanceof LogicalUnionOperator);
        OptExpression union = rewritten.inputAt(1).inputAt(1);
        Assertions.assertEquals(3, union.arity());
        Assertions.assertTrue(union.inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.LEFT_SEMI_JOIN, ((LogicalJoinOperator) union.inputAt(0).getOp()).getJoinType());
        Assertions.assertTrue(union.inputAt(0).inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(union.inputAt(0).inputAt(1).getOp() instanceof LogicalFilterOperator);
        Assertions.assertTrue(union.inputAt(1).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(union.inputAt(2).getOp() instanceof LogicalProjectOperator);
    }

    @Test
    public void testTransformLeftSemiJoinUnsupported(@Mocked OlapTable leftTable,
                                                     @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(0).getOp() instanceof LogicalCTEProduceOperator);
        Assertions.assertTrue(rewritten.inputAt(1).getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(rewritten.inputAt(1).inputAt(0).getOp() instanceof LogicalCTEProduceOperator);
        Assertions.assertTrue(rewritten.inputAt(1).inputAt(1).getOp() instanceof LogicalUnionOperator);
        OptExpression union = rewritten.inputAt(1).inputAt(1);
        Assertions.assertEquals(3, union.arity());
        Assertions.assertTrue(union.inputAt(0).getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(JoinOperator.LEFT_SEMI_JOIN, ((LogicalJoinOperator) union.inputAt(0).getOp()).getJoinType());
        Assertions.assertTrue(union.inputAt(0).inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(union.inputAt(0).inputAt(1).getOp() instanceof LogicalFilterOperator);
        Assertions.assertTrue(union.inputAt(1).getOp() instanceof LogicalProjectOperator);
        Assertions.assertTrue(union.inputAt(2).getOp() instanceof LogicalProjectOperator);
    }

    @Test
    public void testTransformRightSemiJoin(@Mocked OlapTable leftTable,
                                           @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.RIGHT_SEMI_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalCTEAnchorOperator);
    }

    @Test
    public void testTransformRightAntiJoin(@Mocked OlapTable leftTable,
                                           @Mocked OlapTable rightTable) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.RIGHT_ANTI_JOIN, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalCTEAnchorOperator);
    }

    private static void assertUnsupportedJoinRewrite(OlapTable leftTable,
                                                     OlapTable rightTable,
                                                     JoinOperator joinType) {
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
        ColumnRefOperator leftRef = columnRefFactory.create("l_id", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("r_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column leftColumn = new Column("l_id", IntegerType.INT, false);
        Column rightColumn = new Column("r_id", IntegerType.INT, false);
        LogicalOlapScanOperator leftScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, leftColumn)),
                        Maps.newHashMap(Map.of(leftColumn, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();
        LogicalOlapScanOperator rightScan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, rightColumn)),
                        Maps.newHashMap(Map.of(rightColumn, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build();

        OptExpression leftExpr = OptExpression.create(leftScan);
        OptExpression rightExpr = OptExpression.create(rightScan);
        BinaryPredicateOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftRef, rightRef);
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(joinType, onPredicate),
                leftExpr, rightExpr);
        OptExpression deltaJoinExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(deltaJoinExpr);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(deltaJoinExpr, context);
        Assertions.assertTrue(result.isEmpty());
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }
}
