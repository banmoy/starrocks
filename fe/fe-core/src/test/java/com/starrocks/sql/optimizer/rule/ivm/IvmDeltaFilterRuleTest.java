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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IvmDeltaFilterRuleTest {
    @Test
    public void testTransformFilterWithoutProjection(@Mocked OlapTable table) {
        new Expectations() {
            {
                table.getBaseIndexMetaId();
                result = 1L;
            }
        };

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(columnRefFactory);
        ColumnRefOperator v1Ref = columnRefFactory.create("v1", IntegerType.INT, false);
        ColumnRefOperator v2Ref = columnRefFactory.create("v2", IntegerType.INT, false);
        ColumnRefOperator actionRef = columnRefFactory.create("__op", IntegerType.TINYINT, false);

        Column v1Column = new Column("v1", IntegerType.INT, false);
        Column v2Column = new Column("v2", IntegerType.INT, false);
        LogicalOlapScanOperator scan = LogicalOlapScanOperator.builder()
                .withOperator(new LogicalOlapScanOperator(table,
                        Maps.newHashMap(Map.of(v1Ref, v1Column, v2Ref, v2Column)),
                        Maps.newHashMap(Map.of(v1Column, v1Ref, v2Column, v2Ref)),
                        null,
                        -1,
                        null))
                .setTableVersion(1L)
                .build();

        LogicalFilterOperator filter = new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryType.GT, v2Ref,
                        com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createInt(100)));
        OptExpression deltaFilterExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(filter, OptExpression.create(scan)));
        deriveLogicalProperty(deltaFilterExpr);

        List<OptExpression> result = new IvmDeltaFilterRule().transform(deltaFilterExpr, context);
        Assertions.assertEquals(1, result.size());
        LogicalFilterOperator rewrittenFilter = (LogicalFilterOperator) result.get(0).getOp();
        Assertions.assertNotNull(rewrittenFilter.getProjection());
        Assertions.assertTrue(rewrittenFilter.getProjection().getColumnRefMap().containsKey(v1Ref));
        Assertions.assertTrue(rewrittenFilter.getProjection().getColumnRefMap().containsKey(v2Ref));
        Assertions.assertTrue(rewrittenFilter.getProjection().getColumnRefMap().containsKey(actionRef));
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        expression.deriveLogicalPropertyItself();
    }
}
