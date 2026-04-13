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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IvmDeltaWindowRule extends TransformationRule {
    public IvmDeltaWindowRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_WINDOW,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_WINDOW, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression windowExpr = input.inputAt(0);
        LogicalWindowOperator window = (LogicalWindowOperator) windowExpr.getOp();
        OptExpression child = windowExpr.inputAt(0);

        List<ColumnRefOperator> partitionKeys = extractPartitionKeys(window);
        if (partitionKeys.isEmpty()) {
            return List.of();
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        List<ColumnRefOperator> childOutputs = child.getOutputColumns().getColumnRefOperators(factory);
        if (partitionKeys.stream().anyMatch(key -> !childOutputs.contains(key))) {
            return List.of();
        }

        int cteId = context.getCteContext().getNextCteId();
        List<ColumnRefOperator> finalOutputs = input.getOutputColumns().getColumnRefOperators(factory);
        ColumnRefOperator actionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputsWithoutAction = finalOutputs.stream()
                .filter(output -> !output.equals(actionColumn))
                .toList();

        CloneInfo affectedPartitions = cloneChild(context, child, childOutputs, partitionKeys);
        LogicalAggregationOperator affectedPartitionsAgg =
                new LogicalAggregationOperator(AggType.GLOBAL, affectedPartitions.partitionKeys(), Maps.newHashMap());
        ColumnRefOperator affectedPartitionsActionColumn =
                IvmRuleUtils.createActionColumn(factory, actionColumn);
        OptExpression affectedPartitionsExpr = OptExpression.create(affectedPartitionsAgg,
                OptExpression.create(new LogicalDeltaOperator(false, affectedPartitionsActionColumn),
                        affectedPartitions.optExpression()));
        OptExpression affectedPartitionsProducer =
                OptExpression.create(new LogicalCTEProduceOperator(cteId), affectedPartitionsExpr);

        BranchInfo fromBranch = buildWindowBranch(context, cteId, windowExpr, childOutputs,
                finalOutputsWithoutAction, partitionKeys, affectedPartitions.partitionKeys(),
                LogicalVersionOperator.VersionRefType.FROM_VERSION, (byte) -1, actionColumn);
        BranchInfo toBranch = buildWindowBranch(context, cteId, windowExpr, childOutputs,
                finalOutputsWithoutAction, partitionKeys, affectedPartitions.partitionKeys(),
                LogicalVersionOperator.VersionRefType.TO_VERSION, (byte) 1, actionColumn);
        if (fromBranch == null || toBranch == null) {
            return List.of();
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputs,
                List.of(fromBranch.outputs(), toBranch.outputs()), true);
        OptExpression unionExpr =
                OptExpression.create(unionOperator, fromBranch.optExpression(), toBranch.optExpression());
        return List.of(OptExpression.create(new LogicalCTEAnchorOperator(cteId), affectedPartitionsProducer, unionExpr));
    }

    private BranchInfo buildWindowBranch(OptimizerContext context,
                                         int cteId,
                                         OptExpression windowExpr,
                                         List<ColumnRefOperator> childOutputs,
                                         List<ColumnRefOperator> finalOutputsWithoutAction,
                                         List<ColumnRefOperator> partitionKeys,
                                         List<ColumnRefOperator> affectedPartitionOutputs,
                                         LogicalVersionOperator.VersionRefType versionRefType,
                                         byte actionValue,
                                         ColumnRefOperator finalActionColumn) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        CloneInfo cloned = cloneWindow(context, windowExpr, childOutputs, finalOutputsWithoutAction, partitionKeys);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator output : cloned.childOutputs()) {
            projectMap.put(output, output);
        }
        ColumnRefOperator branchActionColumn = null;
        if (finalActionColumn != null) {
            branchActionColumn = factory.create(
                    finalActionColumn.getName(), finalActionColumn.getType(), finalActionColumn.isNullable());
            projectMap.put(branchActionColumn, ConstantOperator.createTinyInt(actionValue));
        }

        OptExpression snapshotExpr = OptExpression.create(new LogicalVersionOperator(versionRefType),
                OptExpression.create(new LogicalProjectOperator(projectMap), cloned.optExpression()));
        OptExpression semiJoinExpr = createLeftSemiJoin(
                factory, cteId, cloned.partitionKeys(), affectedPartitionOutputs, snapshotExpr);
        if (semiJoinExpr == null) {
            return null;
        }

        OptExpression branchWindowExpr = OptExpression.create(cloned.window(), semiJoinExpr);
        List<ColumnRefOperator> branchOutputs = new ArrayList<>(cloned.finalOutputsWithoutAction());
        if (branchActionColumn != null) {
            branchOutputs.add(branchActionColumn);
        }
        return new BranchInfo(branchWindowExpr, branchOutputs);
    }

    private OptExpression createLeftSemiJoin(ColumnRefFactory factory,
                                             int cteId,
                                             List<ColumnRefOperator> leftPartitionKeys,
                                             List<ColumnRefOperator> rightProducerOutputColumns,
                                             OptExpression leftChild) {
        if (leftPartitionKeys.size() != rightProducerOutputColumns.size()) {
            return null;
        }
        List<ColumnRefOperator> consumerOutputs = Lists.newArrayListWithCapacity(leftPartitionKeys.size());
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newHashMap();
        for (int i = 0; i < leftPartitionKeys.size(); i++) {
            ColumnRefOperator leftPartitionKey = leftPartitionKeys.get(i);
            ColumnRefOperator producerOutput = rightProducerOutputColumns.get(i);
            ColumnRefOperator consumerOutput = factory.create(
                    producerOutput.getName(), producerOutput.getType(), producerOutput.isNullable());
            consumerOutputs.add(consumerOutput);
            consumerMap.put(consumerOutput, producerOutput);
        }
        ScalarOperator onPredicate = buildSemiJoinPredicate(leftPartitionKeys, consumerOutputs);
        if (onPredicate == null) {
            return null;
        }
        OptExpression rightConsumer = OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap));
        return OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, onPredicate), leftChild, rightConsumer);
    }

    private ScalarOperator buildSemiJoinPredicate(List<ColumnRefOperator> leftPartitionKeys,
                                                  List<ColumnRefOperator> rightPartitionKeys) {
        if (leftPartitionKeys.size() != rightPartitionKeys.size()) {
            return null;
        }
        List<ScalarOperator> conjuncts = Lists.newArrayListWithCapacity(leftPartitionKeys.size());
        for (int i = 0; i < leftPartitionKeys.size(); i++) {
            conjuncts.add(
                    new BinaryPredicateOperator(BinaryType.EQ, leftPartitionKeys.get(i), rightPartitionKeys.get(i)));
        }
        return Utils.compoundAnd(conjuncts);
    }

    private List<ColumnRefOperator> extractPartitionKeys(LogicalWindowOperator window) {
        if (window.getPartitionExpressions().isEmpty()) {
            return List.of();
        }
        List<ColumnRefOperator> partitionKeys = Lists.newArrayListWithCapacity(window.getPartitionExpressions().size());
        for (ScalarOperator partitionExpr : window.getPartitionExpressions()) {
            if (!(partitionExpr instanceof ColumnRefOperator partitionKey)) {
                return List.of();
            }
            partitionKeys.add(partitionKey);
        }
        return partitionKeys;
    }

    private CloneInfo cloneChild(OptimizerContext context,
                                 OptExpression child,
                                 List<ColumnRefOperator> oldChildOutputs,
                                 List<ColumnRefOperator> oldPartitionKeys) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        OptExpression newChild = duplicator.duplicate(child);
        return new CloneInfo(newChild, duplicator.getMappedColumns(oldChildOutputs),
                duplicator.getMappedColumns(oldPartitionKeys), null, null);
    }

    private CloneInfo cloneWindow(OptimizerContext context,
                                  OptExpression windowExpr,
                                  List<ColumnRefOperator> oldChildOutputs,
                                  List<ColumnRefOperator> oldFinalOutputs,
                                  List<ColumnRefOperator> oldPartitionKeys) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        OptExpression duplicatedWindowExpr = duplicator.duplicate(windowExpr);
        List<ColumnRefOperator> duplicatedChildOutputs = duplicator.getMappedColumns(oldChildOutputs);
        List<ColumnRefOperator> duplicatedPartitionKeys = duplicator.getMappedColumns(oldPartitionKeys);
        List<ColumnRefOperator> duplicatedFinalOutputs = duplicator.getMappedColumns(oldFinalOutputs);
        return new CloneInfo(duplicatedWindowExpr.inputAt(0), duplicatedChildOutputs, duplicatedPartitionKeys,
                duplicatedWindowExpr.getOp().cast(), duplicatedFinalOutputs);
    }

    private record CloneInfo(OptExpression optExpression,
                             List<ColumnRefOperator> childOutputs,
                             List<ColumnRefOperator> partitionKeys,
                             LogicalWindowOperator window,
                             List<ColumnRefOperator> finalOutputsWithoutAction) {
    }

    private record BranchInfo(OptExpression optExpression, List<ColumnRefOperator> outputs) {
    }
}
