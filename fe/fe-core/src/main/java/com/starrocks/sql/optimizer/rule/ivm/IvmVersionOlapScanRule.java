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
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class IvmVersionOlapScanRule extends TransformationRule {
    public IvmVersionOlapScanRule() {
        super(RuleType.TF_OLAP_IVM_VERSION_OLAP_SCAN,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        Long resolvedVersion = resolveVersion(version, scan);
        if (resolvedVersion == null) {
            return List.of();
        }

        LogicalOlapScanOperator rewrittenScan = LogicalOlapScanOperator.builder()
                .withOperator(scan)
                .setTableVersion(resolvedVersion)
                .setChangesVersionRange(null, null)
                .build();

        List<ColumnRefOperator> outputColumns = new ArrayList<>(rewrittenScan.getColRefToColumnMetaMap().keySet());
        outputColumns.sort(Comparator.comparingInt(ColumnRefOperator::getId));
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator outputColumn : outputColumns) {
            projectMap.put(outputColumn, outputColumn);
        }

        return List.of(OptExpression.create(new LogicalProjectOperator(projectMap), OptExpression.create(rewrittenScan)));
    }

    private Long resolveVersion(LogicalVersionOperator version, LogicalOlapScanOperator scan) {
        if (version.getVersionRefType() == LogicalVersionOperator.VersionRefType.FROM_VERSION) {
            return scan.getTableVersion();
        }
        if (!(scan.getTable() instanceof OlapTable olapTable)) {
            return null;
        }
        return IvmRuleUtils.getLatestVisibleVersion(olapTable);
    }
}
