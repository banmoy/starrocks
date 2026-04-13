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
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
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

abstract class IvmDeltaSetOperatorRule extends TransformationRule {
    protected IvmDeltaSetOperatorRule(RuleType ruleType, OperatorType setOperatorType) {
        super(ruleType, Pattern.create(OperatorType.LOGICAL_DELTA)
                .addChildren(Pattern.create(setOperatorType)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression setExpr = input.inputAt(0);
        LogicalSetOperator setOperator = setExpr.getOp().cast();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputs = input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        List<ColumnRefOperator> detailOutputs = finalOutputs.stream()
                .filter(output -> !output.equals(actionColumn))
                .toList();
        if (detailOutputs.isEmpty() || setExpr.arity() < 2) {
            return List.of();
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        int affectedKeyCteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeyProducer =
                buildAffectedKeyProducer(context, actionColumn, affectedKeyCteId, setExpr, setOperator, detailOutputs);
        if (affectedKeyProducer == null) {
            return List.of();
        }

        OptExpression oldPart = buildVersionedSetOperator(context, affectedKeyCteId, detailOutputs,
                setExpr, setOperator, LogicalVersionOperator.VersionRefType.FROM_VERSION);
        OptExpression newPart = buildVersionedSetOperator(context, affectedKeyCteId, detailOutputs,
                setExpr, setOperator, LogicalVersionOperator.VersionRefType.TO_VERSION);
        if (oldPart == null || newPart == null) {
            return List.of();
        }

        int oldPartCteId = context.getCteContext().getNextCteId();
        int newPartCteId = context.getCteContext().getNextCteId();
        OptExpression oldProducer = OptExpression.create(new LogicalCTEProduceOperator(oldPartCteId), oldPart);
        OptExpression newProducer = OptExpression.create(new LogicalCTEProduceOperator(newPartCteId), newPart);

        DiffBranch plusPart = buildDiffBranch(
                factory, newPartCteId, oldPartCteId, detailOutputs, actionColumn, (byte) 1);
        DiffBranch minusPart = buildDiffBranch(
                factory, oldPartCteId, newPartCteId, detailOutputs, actionColumn, (byte) -1);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(
                finalOutputs, List.of(plusPart.outputs(), minusPart.outputs()), true);
        OptExpression unionExpr = OptExpression.create(unionOperator, plusPart.optExpression(), minusPart.optExpression());

        OptExpression newAnchor = OptExpression.create(new LogicalCTEAnchorOperator(newPartCteId), newProducer, unionExpr);
        OptExpression oldAnchor = OptExpression.create(new LogicalCTEAnchorOperator(oldPartCteId), oldProducer, newAnchor);
        return List.of(OptExpression.create(new LogicalCTEAnchorOperator(affectedKeyCteId), affectedKeyProducer, oldAnchor));
    }

    protected abstract LogicalSetOperator buildSetOperator(List<ColumnRefOperator> detailOutputs,
                                                           List<List<ColumnRefOperator>> childOutputColumns);

    private OptExpression buildAffectedKeyProducer(OptimizerContext context,
                                                   ColumnRefOperator parentActionColumn,
                                                   int cteId,
                                                   OptExpression setExpr,
                                                   LogicalSetOperator setOperator,
                                                   List<ColumnRefOperator> detailOutputs) {
        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();
        for (int i = 0; i < setExpr.arity(); i++) {
            ChildClone childClone = cloneChild(context, setExpr.inputAt(i), setOperator.getChildOutputColumns().get(i));
            ColumnRefOperator childActionColumn =
                    IvmRuleUtils.createActionColumn(context.getColumnRefFactory(), parentActionColumn);
            unionChildren.add(OptExpression.create(new LogicalDeltaOperator(false, childActionColumn),
                    childClone.optExpression()));
            unionChildOutputs.add(childClone.outputColumns());
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(detailOutputs, unionChildOutputs, true);
        OptExpression unionExpr = OptExpression.create(unionOperator, unionChildren);
        LogicalAggregationOperator distinctAgg =
                new LogicalAggregationOperator(AggType.GLOBAL, detailOutputs, Maps.newHashMap());
        return OptExpression.create(new LogicalCTEProduceOperator(cteId), OptExpression.create(distinctAgg, unionExpr));
    }

    private OptExpression buildVersionedSetOperator(OptimizerContext context,
                                                    int affectedKeyCteId,
                                                    List<ColumnRefOperator> detailOutputs,
                                                    OptExpression setExpr,
                                                    LogicalSetOperator setOperator,
                                                    LogicalVersionOperator.VersionRefType versionRefType) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        List<OptExpression> children = Lists.newArrayListWithCapacity(setExpr.arity());
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayListWithCapacity(setExpr.arity());
        for (int i = 0; i < setExpr.arity(); i++) {
            ChildClone childClone = cloneChild(context, setExpr.inputAt(i), setOperator.getChildOutputColumns().get(i));
            OptExpression versionExpr =
                    OptExpression.create(new LogicalVersionOperator(versionRefType), childClone.optExpression());
            OptExpression filteredExpr =
                    createLeftSemiJoin(factory, affectedKeyCteId, childClone.outputColumns(), detailOutputs, versionExpr);
            if (filteredExpr == null) {
                return null;
            }
            children.add(filteredExpr);
            childOutputColumns.add(childClone.outputColumns());
        }

        return OptExpression.create(buildSetOperator(detailOutputs, childOutputColumns), children);
    }

    private DiffBranch buildDiffBranch(ColumnRefFactory factory,
                                       int leftCteId,
                                       int rightCteId,
                                       List<ColumnRefOperator> detailOutputs,
                                       ColumnRefOperator actionColumn,
                                       byte actionValue) {
        CteConsumer left = createCteConsumer(factory, leftCteId, detailOutputs);
        CteConsumer right = createCteConsumer(factory, rightCteId, detailOutputs);
        ScalarOperator onPredicate = buildEqualityPredicate(left.outputColumns(), right.outputColumns());
        OptExpression antiJoinExpr = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_ANTI_JOIN, onPredicate),
                left.optExpression(), right.optExpression());

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newLinkedHashMap();
        List<ColumnRefOperator> projectOutputs =
                Lists.newArrayListWithCapacity(left.outputColumns().size() + (actionColumn == null ? 0 : 1));
        for (ColumnRefOperator output : left.outputColumns()) {
            projectMap.put(output, output);
            projectOutputs.add(output);
        }
        if (actionColumn != null) {
            ColumnRefOperator branchAction =
                    factory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
            projectMap.put(branchAction, ConstantOperator.createTinyInt(actionValue));
            projectOutputs.add(branchAction);
        }

        return new DiffBranch(OptExpression.create(new LogicalProjectOperator(projectMap), antiJoinExpr), projectOutputs);
    }

    private OptExpression createLeftSemiJoin(ColumnRefFactory factory,
                                             int cteId,
                                             List<ColumnRefOperator> leftOutputs,
                                             List<ColumnRefOperator> rightProducerOutputs,
                                             OptExpression leftChild) {
        CteConsumer rightConsumer = createCteConsumer(factory, cteId, rightProducerOutputs);
        ScalarOperator onPredicate = buildEqualityPredicate(leftOutputs, rightConsumer.outputColumns());
        if (onPredicate == null) {
            return null;
        }
        return OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, onPredicate), leftChild, rightConsumer.optExpression());
    }

    private CteConsumer createCteConsumer(ColumnRefFactory factory, int cteId, List<ColumnRefOperator> producerOutputs) {
        List<ColumnRefOperator> consumerOutputs = Lists.newArrayListWithCapacity(producerOutputs.size());
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newLinkedHashMap();
        for (ColumnRefOperator producerOutput : producerOutputs) {
            ColumnRefOperator consumerOutput =
                    factory.create(producerOutput.getName(), producerOutput.getType(), producerOutput.isNullable());
            consumerOutputs.add(consumerOutput);
            consumerMap.put(consumerOutput, producerOutput);
        }
        return new CteConsumer(OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap)), consumerOutputs);
    }

    private ScalarOperator buildEqualityPredicate(List<ColumnRefOperator> leftOutputs,
                                                  List<ColumnRefOperator> rightOutputs) {
        if (leftOutputs.size() != rightOutputs.size()) {
            return null;
        }
        if (leftOutputs.isEmpty()) {
            return ConstantOperator.TRUE;
        }
        List<ScalarOperator> predicates = new ArrayList<>(leftOutputs.size());
        for (int i = 0; i < leftOutputs.size(); i++) {
            predicates.add(new BinaryPredicateOperator(BinaryType.EQ, leftOutputs.get(i), rightOutputs.get(i)));
        }
        return Utils.compoundAnd(predicates);
    }

    private ChildClone cloneChild(OptimizerContext context, OptExpression child, List<ColumnRefOperator> oldOutputs) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        OptExpression newChild = duplicator.duplicate(child);
        return new ChildClone(newChild, duplicator.getMappedColumns(oldOutputs));
    }

    private record ChildClone(OptExpression optExpression, List<ColumnRefOperator> outputColumns) {
    }

    private record CteConsumer(OptExpression optExpression, List<ColumnRefOperator> outputColumns) {
    }

    private record DiffBranch(OptExpression optExpression, List<ColumnRefOperator> outputs) {
    }
}
