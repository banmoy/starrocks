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

package com.starrocks.sql.optimizer.rule.ivm.common;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class IvmRuleUtils {
    public static final String ACTION_COLUMN_NAME = StatisticStorage.CHANGES_ACTION_COLUMN;
    public static final Type ACTION_COLUMN_TYPE = IntegerType.TINYINT;
    public static final String IVM_RETRACT_HIDDEN_COLUMN_PREFIX = "__IVM_HIDDEN__";

    private IvmRuleUtils() {
    }

    public static boolean containsLogicalDelta(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_DELTA) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalDelta(child)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsLogicalVersion(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_VERSION) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalVersion(child)) {
                return true;
            }
        }
        return false;
    }

    public static long getLatestVisibleVersion(OlapTable table) {
        long maxVisibleVersion = 0;
        for (PhysicalPartition partition : table.getAllPhysicalPartitions()) {
            maxVisibleVersion = Math.max(maxVisibleVersion, partition.getVisibleVersion());
        }
        return maxVisibleVersion;
    }

    public static Optional<ColumnRefOperator> findActionColumn(OptExpression expression) {
        if (expression == null || expression.getOp() == null) {
            return Optional.empty();
        }
        if (expression.getOp() instanceof LogicalOlapScanOperator scan) {
            return scan.getColRefToColumnMetaMap().entrySet().stream()
                    .filter(entry -> isActionColumn(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalProjectOperator project) {
            return project.getColumnRefMap().keySet().stream()
                    .filter(IvmRuleUtils::isActionColumn)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalDeltaOperator delta) {
            if (delta.getActionColumn() != null) {
                return Optional.of(delta.getActionColumn());
            }
        }
        if (expression.getOp() instanceof LogicalAggregationOperator agg) {
            return agg.getGroupingKeys().stream()
                    .filter(IvmRuleUtils::isActionColumn)
                    .findFirst();
        }
        if (expression.getOp() instanceof LogicalJoinOperator join) {
            if (join.getJoinType().isLeftSemiJoin() && !expression.getInputs().isEmpty()) {
                return findActionColumn(expression.inputAt(0));
            }
            return Optional.empty();
        }
        if (expression.getOp() instanceof LogicalWindowOperator window) {
            Projection projection = window.getProjection();
            if (projection != null) {
                Optional<ColumnRefOperator> actionColumn = projection.getColumnRefMap().keySet().stream()
                        .filter(IvmRuleUtils::isActionColumn)
                        .findFirst();
                if (actionColumn.isPresent()) {
                    return actionColumn;
                }
            }
        }
        if (expression.getOp() instanceof LogicalFilterOperator filter) {
            Projection projection = filter.getProjection();
            if (projection != null) {
                return projection.getColumnRefMap().keySet().stream()
                        .filter(IvmRuleUtils::isActionColumn)
                        .findFirst();
            }
        }
        if (expression.getOp() instanceof LogicalCTEAnchorOperator) {
            if (expression.getInputs().size() >= 2) {
                return findActionColumn(expression.inputAt(1));
            }
            return Optional.empty();
        }
        if (expression.getOp() instanceof LogicalUnionOperator union) {
            return union.getOutputColumnRefOp().stream()
                    .filter(IvmRuleUtils::isActionColumn)
                    .findFirst();
        }
        if (expression.getInputs().size() == 1) {
            return findActionColumn(expression.inputAt(0));
        }
        return Optional.empty();
    }

    public static boolean isActionColumn(Column column) {
        return column != null && ACTION_COLUMN_NAME.equalsIgnoreCase(column.getName());
    }

    public static boolean isActionColumn(ColumnRefOperator columnRef) {
        return columnRef != null && ACTION_COLUMN_NAME.equalsIgnoreCase(columnRef.getName());
    }

    public static ColumnRefOperator createActionColumn(ColumnRefFactory factory, ColumnRefOperator parentActionColumn) {
        if (parentActionColumn != null) {
            return factory.create(parentActionColumn.getName(), parentActionColumn.getType(), false);
        }
        return factory.create(ACTION_COLUMN_NAME, ACTION_COLUMN_TYPE, false);
    }

    public static String count1StateColumnName(String aggOutputColumnName) {
        return helperColumnName("COUNT1", aggOutputColumnName);
    }

    public static String sumStateColumnName(String aggOutputColumnName) {
        return helperColumnName("SUM", aggOutputColumnName);
    }

    public static String countStateColumnName(String aggOutputColumnName) {
        return helperColumnName("COUNT", aggOutputColumnName);
    }

    public static String groupingKeyStateColumnName(int groupKeyIndex) {
        return IVM_RETRACT_HIDDEN_COLUMN_PREFIX + "_GROUP_KEY_" + groupKeyIndex;
    }

    public static boolean isIvmRetractHiddenColumn(Column column) {
        return column != null && isIvmRetractHiddenColumn(column.getName());
    }

    public static boolean isIvmRetractHiddenColumn(ColumnRefOperator columnRef) {
        return columnRef != null && isIvmRetractHiddenColumn(columnRef.getName());
    }

    public static boolean isIvmRetractHiddenColumn(String columnName) {
        return columnName != null && columnName.toUpperCase(Locale.ROOT).startsWith(IVM_RETRACT_HIDDEN_COLUMN_PREFIX);
    }

    private static String helperColumnName(String kind, String aggOutputColumnName) {
        return IVM_RETRACT_HIDDEN_COLUMN_PREFIX + "_" + kind + "_" + normalizeIdentifier(aggOutputColumnName);
    }

    private static String normalizeIdentifier(String name) {
        if (name == null) {
            return "col";
        }
        String lower = name.toLowerCase(Locale.ROOT);
        StringBuilder builder = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
                builder.append(c);
            } else {
                builder.append('_');
            }
        }
        if (builder.isEmpty()) {
            return "col";
        }
        final int maxLen = 48;
        if (builder.length() > maxLen) {
            return builder.substring(0, maxLen);
        }
        return builder.toString();
    }
}
