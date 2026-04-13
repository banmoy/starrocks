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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;

public class IvmDeltaOlapScanRule extends TransformationRule {
    public IvmDeltaOlapScanRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_OLAP_SCAN,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        if (!(scan.getTable() instanceof OlapTable olapTable)) {
            return List.of();
        }

        Long fromVersion = scan.getTableVersion();
        if (fromVersion == null) {
            return List.of();
        }
        long toVersion = IvmRuleUtils.getLatestVisibleVersion(olapTable);
        if (toVersion == fromVersion) {
            List<ColumnRefOperator> outputColumns =
                    input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
            return List.of(OptExpression.create(new LogicalValuesOperator(outputColumns, List.of())));
        }
        if (toVersion <= fromVersion) {
            return List.of();
        }

        Map<ColumnRefOperator, Column> newColRefToMeta = Maps.newHashMap(scan.getColRefToColumnMetaMap());
        Map<Column, ColumnRefOperator> newMetaToColRef = Maps.newHashMap(scan.getColumnMetaToColRefMap());
        ColumnRefOperator actionColumnRef = delta.getActionColumn();
        if (actionColumnRef != null) {
            Column actionMeta = new Column(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
            context.getColumnRefFactory().updateColumnRefToColumns(actionColumnRef, actionMeta, scan.getTable());
            newColRefToMeta.put(actionColumnRef, actionMeta);
            newMetaToColRef.put(actionMeta, actionColumnRef);
        }

        LogicalOlapScanOperator newScan = LogicalOlapScanOperator.builder()
                .withOperator(scan)
                .setTableVersion(null)
                .setChangesVersionRange(fromVersion, toVersion)
                .setColRefToColumnMetaMap(newColRefToMeta)
                .setColumnMetaToColRefMap(newMetaToColRef)
                .build();
        return List.of(OptExpression.create(newScan));
    }
}
