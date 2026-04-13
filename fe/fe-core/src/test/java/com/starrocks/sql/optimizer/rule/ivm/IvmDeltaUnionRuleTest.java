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
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmDeltaUnionRuleTest {
    @Test
    public void testTransformUnionAll(@Mocked OlapTable leftTable,
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
        ColumnRefOperator leftRef = columnRefFactory.create("c1", IntegerType.INT, false);
        ColumnRefOperator rightRef = columnRefFactory.create("c1", IntegerType.INT, false);
        ColumnRefOperator unionRef = columnRefFactory.create("c1", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column column = new Column("c1", IntegerType.INT, false);
        OptExpression leftScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(leftTable,
                        Maps.newHashMap(Map.of(leftRef, column)),
                        Maps.newHashMap(Map.of(column, leftRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build());
        OptExpression rightScan = OptExpression.create(LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(rightTable,
                        Maps.newHashMap(Map.of(rightRef, column)),
                        Maps.newHashMap(Map.of(column, rightRef)),
                        null,
                        -1,
                        null))
                .setTableVersion(2L)
                .build());

        LogicalUnionOperator union = LogicalUnionOperator.builder()
                .isUnionAll(true)
                .setOutputColumnRefOp(List.of(unionRef))
                .setChildOutputColumns(List.of(List.of(leftRef), List.of(rightRef)))
                .build();
        OptExpression deltaUnionExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(union, leftScan, rightScan));
        deriveLogicalProperty(deltaUnionExpr);

        List<OptExpression> result = new IvmDeltaUnionRule().transform(deltaUnionExpr, context);
        Assertions.assertEquals(1, result.size());

        OptExpression rewritten = result.get(0);
        Assertions.assertTrue(rewritten.getOp() instanceof LogicalUnionOperator);
        LogicalUnionOperator rewrittenUnion = (LogicalUnionOperator) rewritten.getOp();
        Assertions.assertEquals(List.of(unionRef, actionRef), rewrittenUnion.getOutputColumnRefOp());
        Assertions.assertEquals(List.of(leftRef, actionRef), rewrittenUnion.getChildOutputColumns().get(0));
        Assertions.assertEquals(List.of(rightRef, actionRef), rewrittenUnion.getChildOutputColumns().get(1));
        Assertions.assertTrue(rewritten.inputAt(0).getOp() instanceof LogicalDeltaOperator);
        Assertions.assertTrue(rewritten.inputAt(1).getOp() instanceof LogicalDeltaOperator);
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }
}
