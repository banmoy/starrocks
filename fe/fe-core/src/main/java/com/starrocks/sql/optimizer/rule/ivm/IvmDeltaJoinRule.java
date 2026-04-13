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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class IvmDeltaJoinRule extends TransformationRule {
    public IvmDeltaJoinRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_JOIN, Pattern.create(OperatorType.LOGICAL_DELTA)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression joinExpr = input.inputAt(0);
        LogicalJoinOperator join = (LogicalJoinOperator) joinExpr.getOp();

        if (join.getJoinType().isRightSemiJoin()) {
            LogicalJoinOperator leftSemiJoin = LogicalJoinOperator.builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                    .build();
            joinExpr = OptExpression.create(leftSemiJoin, joinExpr.inputAt(1), joinExpr.inputAt(0));
            join = leftSemiJoin;
        } else if (join.getJoinType().isRightAntiJoin()) {
            LogicalJoinOperator leftAntiJoin = LogicalJoinOperator.builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                    .build();
            joinExpr = OptExpression.create(leftAntiJoin, joinExpr.inputAt(1), joinExpr.inputAt(0));
            join = leftAntiJoin;
        } else if (join.getJoinType().isRightOuterJoin()) {
            LogicalJoinOperator leftOuterJoin = LogicalJoinOperator.builder()
                    .withOperator(join)
                    .setJoinType(JoinOperator.LEFT_OUTER_JOIN)
                    .build();
            joinExpr = OptExpression.create(leftOuterJoin, joinExpr.inputAt(1), joinExpr.inputAt(0));
            join = leftOuterJoin;
        }
        if (join.getJoinType().isLeftSemiJoin()) {
            return transformLeftSemiJoin(input, context, joinExpr);
        }
        if (join.getJoinType().isLeftAntiJoin()) {
            return transformLeftAntiJoin(input, context, joinExpr);
        }
        if (join.getJoinType().isLeftOuterJoin()) {
            return transformLeftOuterJoin(input, context, joinExpr);
        }
        if (join.getJoinType().isFullOuterJoin()) {
            return transformFullOuterJoin(input, context, joinExpr);
        }
        if (!join.getJoinType().isInnerJoin()) {
            return List.of();
        }

        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = getJoinOutputsWithoutAction(finalOutputColumns, actionColumn);

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        BranchResult branch1 = buildBranch(factory, context, joinExpr,
                JoinOperator.INNER_JOIN, joinOutputColumns, actionColumn, true);
        BranchResult branch2 = buildBranch(factory, context, joinExpr,
                JoinOperator.INNER_JOIN, joinOutputColumns, actionColumn, false);
        if (!appendBranch(unionChildren, unionChildOutputs, branch1)
                || !appendBranch(unionChildren, unionChildOutputs, branch2)) {
            return List.of();
        }

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private List<OptExpression> transformFullOuterJoin(OptExpression input,
                                                       OptimizerContext context,
                                                       OptExpression joinExpr) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn == null) {
            return List.of();
        }

        ColumnRefSet originalLeftOutputs = joinExpr.inputAt(0).getOutputColumns();
        ColumnRefSet originalRightOutputs = joinExpr.inputAt(1).getOutputColumns();
        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = getJoinOutputsWithoutAction(finalOutputColumns, actionColumn);

        BranchResult innerBranch = buildOuterDeltaBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, JoinOperator.INNER_JOIN, null);
        BranchResult leftNullBranch = buildOuterDeltaBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, JoinOperator.LEFT_ANTI_JOIN, originalRightOutputs);
        BranchResult rightNullBranch = buildOuterDeltaBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, JoinOperator.RIGHT_ANTI_JOIN, originalLeftOutputs);
        if (innerBranch == null || leftNullBranch == null || rightNullBranch == null) {
            return List.of();
        }

        List<OptExpression> unionChildren = Lists.newArrayList(
                innerBranch.branchExpr(), leftNullBranch.branchExpr(), rightNullBranch.branchExpr());
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList(
                innerBranch.outputs(), leftNullBranch.outputs(), rightNullBranch.outputs());
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private List<OptExpression> transformLeftOuterJoin(OptExpression input,
                                                       OptimizerContext context,
                                                       OptExpression joinExpr) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn == null) {
            return List.of();
        }

        ColumnRefSet originalRightOutputs = joinExpr.inputAt(1).getOutputColumns();
        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = getJoinOutputsWithoutAction(finalOutputColumns, actionColumn);

        BranchResult innerBranch = buildOuterDeltaBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, JoinOperator.INNER_JOIN, null);
        BranchResult antiNullBranch = buildOuterDeltaBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, JoinOperator.LEFT_ANTI_JOIN, originalRightOutputs);
        if (innerBranch == null || antiNullBranch == null) {
            return List.of();
        }

        List<OptExpression> unionChildren = Lists.newArrayList(innerBranch.branchExpr(), antiNullBranch.branchExpr());
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList(innerBranch.outputs(), antiNullBranch.outputs());
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOperator, unionChildren));
    }

    private BranchResult buildOuterDeltaBranch(ColumnRefFactory factory,
                                               OptimizerContext context,
                                               OptExpression joinExpr,
                                               ColumnRefOperator actionColumn,
                                               List<ColumnRefOperator> joinOutputColumns,
                                               JoinOperator branchJoinType,
                                               ColumnRefSet nullSideOutputs) {
        DuplicatedJoin duplicatedJoin = duplicateJoin(factory, context, joinExpr);
        LogicalJoinOperator branchJoin = LogicalJoinOperator.builder()
                .withOperator((LogicalJoinOperator) duplicatedJoin.joinExpr().getOp())
                .setJoinType(branchJoinType)
                .build();
        OptExpression branchExpr = OptExpression.create(branchJoin,
                duplicatedJoin.joinExpr().inputAt(0), duplicatedJoin.joinExpr().inputAt(1));

        Map<ColumnRefOperator, ColumnRefOperator> nullOldToNew = Maps.newHashMap();
        if (nullSideOutputs != null) {
            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
            for (ColumnRefOperator output : joinOutputColumns) {
                ColumnRefOperator mappedOutput = duplicatedJoin.columnMapping().get(output);
                if (mappedOutput == null) {
                    return null;
                }
                if (nullSideOutputs.contains(output)) {
                    ColumnRefOperator newRef = factory.create(output.getName(), output.getType(), output.isNullable());
                    nullOldToNew.put(mappedOutput, newRef);
                    projectMap.put(newRef, ConstantOperator.createNull(output.getType()));
                } else {
                    projectMap.put(mappedOutput, mappedOutput);
                }
            }
            branchExpr = OptExpression.create(new LogicalProjectOperator(projectMap), branchExpr);
        }

        ColumnRefOperator branchAction = duplicateActionColumn(factory, actionColumn);
        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchAction, duplicatedJoin.columnMapping());
        if (outputs == null) {
            return null;
        }
        for (int i = 0; i < outputs.size(); i++) {
            ColumnRefOperator output = outputs.get(i);
            if (nullOldToNew.containsKey(output)) {
                outputs.set(i, nullOldToNew.get(output));
            }
        }
        OptExpression branchDelta = OptExpression.create(new LogicalDeltaOperator(false, branchAction), branchExpr);

        return new BranchResult(branchDelta, outputs);
    }

    private List<OptExpression> transformLeftSemiJoin(OptExpression input,
                                                      OptimizerContext context,
                                                      OptExpression joinExpr) {
        return transformLeftSemiAntiJoin(input, context, joinExpr, true);
    }

    private List<OptExpression> transformLeftAntiJoin(OptExpression input,
                                                      OptimizerContext context,
                                                      OptExpression joinExpr) {
        return transformLeftSemiAntiJoin(input, context, joinExpr, false);
    }

    private List<OptExpression> transformLeftSemiAntiJoin(OptExpression input,
                                                          OptimizerContext context,
                                                          OptExpression joinExpr,
                                                          boolean isSemiJoin) {
        ColumnRefFactory factory = context.getColumnRefFactory();
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn == null) {
            return List.of();
        }

        LogicalJoinOperator join = (LogicalJoinOperator) joinExpr.getOp();
        List<JoinKeyPair> joinKeyPairs = deriveJoinKeyPairs(join, joinExpr.inputAt(0), joinExpr.inputAt(1));
        if (joinKeyPairs.isEmpty()) {
            return List.of();
        }

        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = getJoinOutputsWithoutAction(finalOutputColumns, actionColumn);

        int kCteId = context.getCteContext().getNextCteId();
        KeysProducerResult kProducer = buildKeysProducer(factory, context, joinExpr, actionColumn, joinKeyPairs, kCteId);
        if (kProducer == null) {
            return List.of();
        }

        int flipCteId = context.getCteContext().getNextCteId();
        FlipProducerResult flipProducer =
                buildFlipProducer(factory, context, joinExpr, actionColumn, joinKeyPairs, kProducer, flipCteId);
        if (flipProducer == null) {
            return List.of();
        }

        BranchResult part1 = buildLeftSemiAntiPart1Branch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, joinKeyPairs, flipProducer, isSemiJoin);
        BranchResult part2 = buildLeftSemiAntiFlipBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, joinKeyPairs, flipProducer, true,
                isSemiJoin ? LogicalVersionOperator.toVersion() : LogicalVersionOperator.fromVersion(),
                isSemiJoin ? (byte) 1 : (byte) -1);
        BranchResult part3 = buildLeftSemiAntiFlipBranch(factory, context, joinExpr, actionColumn,
                joinOutputColumns, joinKeyPairs, flipProducer, false,
                isSemiJoin ? LogicalVersionOperator.fromVersion() : LogicalVersionOperator.toVersion(),
                isSemiJoin ? (byte) -1 : (byte) 1);
        if (part1 == null || part2 == null || part3 == null) {
            return List.of();
        }

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();
        if (!appendBranch(unionChildren, unionChildOutputs, part1)
                || !appendBranch(unionChildren, unionChildOutputs, part2)
                || !appendBranch(unionChildren, unionChildOutputs, part3)) {
            return List.of();
        }
        OptExpression unionExpr = OptExpression.create(
                new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true), unionChildren);

        // K and flip are materialized by CTE; other steps are inlined.
        OptExpression flipAnchor = OptExpression.create(new LogicalCTEAnchorOperator(flipCteId),
                flipProducer.producer(), unionExpr);
        OptExpression kAnchor = OptExpression.create(new LogicalCTEAnchorOperator(kCteId),
                kProducer.producer(), flipAnchor);
        return List.of(kAnchor);
    }

    /**
     * Build CTE K for left-semi delta rewrite:
     * distinct k from delta_left union all delta_right
     */
    private KeysProducerResult buildKeysProducer(ColumnRefFactory factory,
                                                 OptimizerContext context,
                                                 OptExpression joinExpr,
                                                 ColumnRefOperator actionColumn,
                                                 List<JoinKeyPair> joinKeyPairs,
                                                 int cteId) {
        DuplicatedJoin deltaJoin = duplicateJoin(factory, context, joinExpr);

        List<ColumnRefOperator> leftKeys = mapLeftJoinKeys(joinKeyPairs, deltaJoin.columnMapping());
        if (leftKeys.isEmpty() || leftKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }
        ColumnRefOperator leftAction = duplicateActionColumn(factory, actionColumn);
        OptExpression deltaLeft =
                OptExpression.create(new LogicalDeltaOperator(false, leftAction), deltaJoin.joinExpr().inputAt(0));

        List<ColumnRefOperator> rightKeys = mapRightJoinKeys(joinKeyPairs, deltaJoin.columnMapping());
        if (rightKeys.isEmpty() || rightKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }
        ColumnRefOperator rightAction = duplicateActionColumn(factory, actionColumn);
        OptExpression deltaRight =
                OptExpression.create(new LogicalDeltaOperator(false, rightAction), deltaJoin.joinExpr().inputAt(1));

        List<ColumnRefOperator> unionOutputKeys = createKeyRefs(factory, leftKeys);
        LogicalUnionOperator keyUnion =
                new LogicalUnionOperator(unionOutputKeys, Lists.newArrayList(leftKeys, rightKeys), true);
        OptExpression allKeys = OptExpression.create(keyUnion, deltaLeft, deltaRight);
        OptExpression distinctKeys =
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, unionOutputKeys, Maps.newHashMap()), allKeys);
        OptExpression producer = OptExpression.create(new LogicalCTEProduceOperator(cteId), distinctKeys);

        return new KeysProducerResult(cteId, unionOutputKeys, producer);
    }

    private FlipProducerResult buildFlipProducer(ColumnRefFactory factory,
                                                 OptimizerContext context,
                                                 OptExpression joinExpr,
                                                 ColumnRefOperator actionColumn,
                                                 List<JoinKeyPair> joinKeyPairs,
                                                 KeysProducerResult keys,
                                                 int cteId) {
        // c0: S_from join K then group by key count(*)
        DuplicatedJoin sFromJoin = duplicateJoin(factory, context, joinExpr);
        List<ColumnRefOperator> sFromKeys = mapRightJoinKeys(joinKeyPairs, sFromJoin.columnMapping());
        if (sFromKeys.isEmpty() || sFromKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        List<ColumnRefOperator> kForC0 = createKeyRefs(factory, sFromKeys);
        OptExpression kConsumeForC0 = createCteConsume(keys.cteId(), kForC0, keys.keyRefs());
        ScalarOperator c0On = buildEquiPredicate(sFromKeys, kForC0);
        if (c0On == null) {
            return null;
        }

        OptExpression sFrom = OptExpression.create(LogicalVersionOperator.fromVersion(), sFromJoin.joinExpr().inputAt(1));
        OptExpression c0Join =
                OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, c0On), sFrom, kConsumeForC0);
        ColumnRefOperator c0Ref = factory.create("__c0", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> c0AggMap = Maps.newHashMap();
        c0AggMap.put(c0Ref, createBuiltinCall(FunctionSet.COUNT, IntegerType.BIGINT, List.of(ConstantOperator.createInt(1))));
        OptExpression c0 = OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, sFromKeys, c0AggMap), c0Join);

        // cd: dS group by key sum(action)
        DuplicatedJoin dSJoin = duplicateJoin(factory, context, joinExpr);
        List<ColumnRefOperator> dSKeys = mapRightJoinKeys(joinKeyPairs, dSJoin.columnMapping());
        if (dSKeys.isEmpty() || dSKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        ColumnRefOperator dSAction = duplicateActionColumn(factory, actionColumn);
        OptExpression dS = OptExpression.create(new LogicalDeltaOperator(false, dSAction), dSJoin.joinExpr().inputAt(1));
        ColumnRefOperator cdRef = factory.create("__cd", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, CallOperator> cdAggMap = Maps.newHashMap();
        ScalarOperator dSActionAsBigint = new CastOperator(IntegerType.BIGINT, dSAction, true);
        cdAggMap.put(cdRef, createBuiltinCall(FunctionSet.SUM, IntegerType.BIGINT, List.of(dSActionAsBigint)));
        OptExpression cd = OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, dSKeys, cdAggMap), dS);

        // flip: K left join c0 left join cd, project key,c0,c1
        List<ColumnRefOperator> flipKeys = createKeyRefs(factory, keys.keyRefs());
        OptExpression kConsumeForFlip = createCteConsume(keys.cteId(), flipKeys, keys.keyRefs());
        ScalarOperator flipC0On = buildEquiPredicate(flipKeys, sFromKeys);
        ScalarOperator flipCdOn = buildEquiPredicate(flipKeys, dSKeys);
        if (flipC0On == null || flipCdOn == null) {
            return null;
        }
        OptExpression flipJoinC0 = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, flipC0On),
                kConsumeForFlip, c0);
        OptExpression flipJoinCd = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, flipCdOn),
                flipJoinC0, cd);

        ColumnRefOperator cnt0 = factory.create("__flip_c0", IntegerType.BIGINT, false);
        ColumnRefOperator cnt1 = factory.create("__flip_c1", IntegerType.BIGINT, false);
        ScalarOperator coalesceC0 = createBuiltinCall(FunctionSet.COALESCE, IntegerType.BIGINT,
                List.of(c0Ref, ConstantOperator.createBigint(0L)));
        ScalarOperator coalesceCd = createBuiltinCall(FunctionSet.COALESCE, IntegerType.BIGINT,
                List.of(cdRef, ConstantOperator.createBigint(0L)));
        ScalarOperator cnt1Expr = createBuiltinCall(FunctionSet.ADD, IntegerType.BIGINT,
                List.of(coalesceC0, coalesceCd));
        Map<ColumnRefOperator, ScalarOperator> flipProjectMap = Maps.newHashMap();
        for (ColumnRefOperator flipKey : flipKeys) {
            flipProjectMap.put(flipKey, flipKey);
        }
        flipProjectMap.put(cnt0, coalesceC0);
        flipProjectMap.put(cnt1, cnt1Expr);

        OptExpression flip = OptExpression.create(new LogicalProjectOperator(flipProjectMap), flipJoinCd);
        OptExpression producer = OptExpression.create(new LogicalCTEProduceOperator(cteId), flip);
        return new FlipProducerResult(cteId, flipKeys, cnt0, cnt1, producer);
    }

    private BranchResult buildLeftSemiAntiPart1Branch(ColumnRefFactory factory,
                                                      OptimizerContext context,
                                                      OptExpression joinExpr,
                                                      ColumnRefOperator actionColumn,
                                                      List<ColumnRefOperator> joinOutputColumns,
                                                      List<JoinKeyPair> joinKeyPairs,
                                                      FlipProducerResult flip,
                                                      boolean isSemiJoin) {
        DuplicatedJoin branchJoin = duplicateJoin(factory, context, joinExpr);
        LogicalJoinOperator branchJoinOp = (LogicalJoinOperator) branchJoin.joinExpr().getOp();
        List<ColumnRefOperator> branchRightKeys = mapRightJoinKeys(joinKeyPairs, branchJoin.columnMapping());
        if (branchRightKeys.isEmpty() || branchRightKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        ColumnRefOperator branchAction = duplicateActionColumn(factory, actionColumn);
        OptExpression dR = OptExpression.create(new LogicalDeltaOperator(false, branchAction), branchJoin.joinExpr().inputAt(0));

        FlipConsumeResult flipConsume = createFlipConsumeForBranch(factory, flip, branchRightKeys);
        ScalarOperator part1Predicate;
        if (isSemiJoin) {
            part1Predicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.GT, flipConsume.cnt0(), ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.GT, flipConsume.cnt1(), ConstantOperator.createBigint(0L)));
        } else {
            part1Predicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.EQ, flipConsume.cnt0(), ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.EQ, flipConsume.cnt1(), ConstantOperator.createBigint(0L)));
        }
        OptExpression part1Filter = OptExpression.create(new LogicalFilterOperator(part1Predicate), flipConsume.consume());

        LogicalJoinOperator semiJoin = LogicalJoinOperator.builder()
                .withOperator(branchJoinOp)
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .build();
        OptExpression part1 = OptExpression.create(semiJoin, dR, part1Filter);

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchAction, branchJoin.columnMapping());
        if (outputs == null) {
            return null;
        }
        return new BranchResult(part1, outputs);
    }

    private BranchResult buildLeftSemiAntiFlipBranch(ColumnRefFactory factory,
                                                     OptimizerContext context,
                                                     OptExpression joinExpr,
                                                     ColumnRefOperator actionColumn,
                                                     List<ColumnRefOperator> joinOutputColumns,
                                                     List<JoinKeyPair> joinKeyPairs,
                                                     FlipProducerResult flip,
                                                     boolean isUp,
                                                     LogicalVersionOperator versionOperator,
                                                     byte outAction) {
        DuplicatedJoin branchJoin = duplicateJoin(factory, context, joinExpr);
        LogicalJoinOperator branchJoinOp = (LogicalJoinOperator) branchJoin.joinExpr().getOp();
        List<ColumnRefOperator> branchRightKeys = mapRightJoinKeys(joinKeyPairs, branchJoin.columnMapping());
        if (branchRightKeys.isEmpty() || branchRightKeys.stream().anyMatch(Objects::isNull)) {
            return null;
        }

        ColumnRefOperator branchAction = duplicateActionColumn(factory, actionColumn);
        OptExpression leftVersion = OptExpression.create(versionOperator, branchJoin.joinExpr().inputAt(0));

        FlipConsumeResult flipConsume = createFlipConsumeForBranch(factory, flip, branchRightKeys);
        ScalarOperator predicate;
        if (isUp) {
            predicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.EQ, flipConsume.cnt0(), ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.GT, flipConsume.cnt1(), ConstantOperator.createBigint(0L)));
        } else {
            predicate = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.GT, flipConsume.cnt0(), ConstantOperator.createBigint(0L)),
                    new BinaryPredicateOperator(BinaryType.EQ, flipConsume.cnt1(), ConstantOperator.createBigint(0L)));
        }
        OptExpression filteredFlip = OptExpression.create(new LogicalFilterOperator(predicate), flipConsume.consume());

        LogicalJoinOperator semiJoin = LogicalJoinOperator.builder()
                .withOperator(branchJoinOp)
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .build();
        OptExpression joined = OptExpression.create(semiJoin, leftVersion, filteredFlip);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator output : joinOutputColumns) {
            ColumnRefOperator mappedOutput = branchJoin.columnMapping().get(output);
            if (mappedOutput == null) {
                return null;
            }
            projectMap.put(mappedOutput, mappedOutput);
        }
        projectMap.put(branchAction, ConstantOperator.createTinyInt(outAction));
        OptExpression projected = OptExpression.create(new LogicalProjectOperator(projectMap), joined);

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchAction, branchJoin.columnMapping());
        if (outputs == null) {
            return null;
        }
        return new BranchResult(projected, outputs);
    }

    private FlipConsumeResult createFlipConsumeForBranch(ColumnRefFactory factory,
                                                         FlipProducerResult flip,
                                                         List<ColumnRefOperator> branchRightKeys) {
        Map<ColumnRefOperator, ColumnRefOperator> consumeMap = Maps.newHashMap();
        for (int i = 0; i < branchRightKeys.size(); i++) {
            consumeMap.put(branchRightKeys.get(i), flip.keyRefs().get(i));
        }
        ColumnRefOperator cnt0 = factory.create("__branch_c0", IntegerType.BIGINT, false);
        ColumnRefOperator cnt1 = factory.create("__branch_c1", IntegerType.BIGINT, false);
        consumeMap.put(cnt0, flip.cnt0());
        consumeMap.put(cnt1, flip.cnt1());
        OptExpression consume = OptExpression.create(new LogicalCTEConsumeOperator(flip.cteId(), consumeMap));
        return new FlipConsumeResult(consume, cnt0, cnt1);
    }

    private List<ColumnRefOperator> createKeyRefs(ColumnRefFactory factory, List<ColumnRefOperator> inRefs) {
        List<ColumnRefOperator> refs = new ArrayList<>(inRefs.size());
        for (ColumnRefOperator inRef : inRefs) {
            refs.add(factory.create("__k", inRef.getType(), inRef.isNullable()));
        }
        return refs;
    }

    private OptExpression createCteConsume(int cteId,
                                           List<ColumnRefOperator> outputRefs,
                                           List<ColumnRefOperator> producerRefs) {
        Map<ColumnRefOperator, ColumnRefOperator> consumeMap = Maps.newHashMap();
        for (int i = 0; i < outputRefs.size(); i++) {
            consumeMap.put(outputRefs.get(i), producerRefs.get(i));
        }
        return OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumeMap));
    }

    private DuplicatedJoin duplicateJoin(ColumnRefFactory columnRefFactory,
                                         OptimizerContext context,
                                         OptExpression joinExpr) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(columnRefFactory, context);
        return new DuplicatedJoin(duplicator.duplicate(joinExpr), duplicator.getColumnMapping());
    }

    private BranchResult buildBranch(ColumnRefFactory columnRefFactory,
                                     OptimizerContext context,
                                     OptExpression joinExpr,
                                     JoinOperator branchJoinType,
                                     List<ColumnRefOperator> joinOutputColumns,
                                     ColumnRefOperator actionColumn,
                                     boolean isLeftDelta) {
        DuplicatedJoin duplicatedJoin = duplicateJoin(columnRefFactory, context, joinExpr);
        LogicalJoinOperator duplicatedJoinOp = (LogicalJoinOperator) duplicatedJoin.joinExpr().getOp();
        ColumnRefOperator branchActionColumn = duplicateActionColumn(columnRefFactory, actionColumn);
        OptExpression left;
        OptExpression right;
        if (isLeftDelta) {
            left = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                    duplicatedJoin.joinExpr().inputAt(0));
            right = OptExpression.create(LogicalVersionOperator.fromVersion(), duplicatedJoin.joinExpr().inputAt(1));
        } else {
            left = OptExpression.create(LogicalVersionOperator.toVersion(), duplicatedJoin.joinExpr().inputAt(0));
            right = OptExpression.create(new LogicalDeltaOperator(false, branchActionColumn),
                    duplicatedJoin.joinExpr().inputAt(1));
        }

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns,
                branchActionColumn, duplicatedJoin.columnMapping());
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                .withOperator(duplicatedJoinOp)
                .setJoinType(branchJoinType)
                .build();
        return new BranchResult(OptExpression.create(newJoin, left, right), outputs);
    }

    private List<JoinKeyPair> deriveJoinKeyPairs(LogicalJoinOperator join,
                                                 OptExpression leftChild,
                                                 OptExpression rightChild) {
        ScalarOperator onPredicate = join.getOnPredicate();
        if (onPredicate == null) {
            return List.of();
        }

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicate);
        ColumnRefSet leftColumns = leftChild.getOutputColumns();
        ColumnRefSet rightColumns = rightChild.getOutputColumns();
        List<BinaryPredicateOperator> eqPredicates = JoinHelper.getEqualsPredicate(leftColumns, rightColumns, conjuncts);
        if (eqPredicates.isEmpty() || eqPredicates.size() != conjuncts.size()) {
            return List.of();
        }

        List<JoinKeyPair> pairs = new ArrayList<>(eqPredicates.size());
        for (BinaryPredicateOperator eqPredicate : eqPredicates) {
            if (!(eqPredicate.getChild(0) instanceof ColumnRefOperator lhs)
                    || !(eqPredicate.getChild(1) instanceof ColumnRefOperator rhs)) {
                return List.of();
            }

            if (leftColumns.contains(lhs) && rightColumns.contains(rhs)) {
                pairs.add(new JoinKeyPair(lhs, rhs));
            } else if (leftColumns.contains(rhs) && rightColumns.contains(lhs)) {
                pairs.add(new JoinKeyPair(rhs, lhs));
            } else {
                return List.of();
            }
        }

        return pairs;
    }

    private List<ColumnRefOperator> mapLeftJoinKeys(List<JoinKeyPair> keyPairs,
                                                    Map<ColumnRefOperator, ColumnRefOperator> oldToNew) {
        return keyPairs.stream().map(JoinKeyPair::leftKey).map(oldToNew::get).collect(Collectors.toList());
    }

    private List<ColumnRefOperator> mapRightJoinKeys(List<JoinKeyPair> keyPairs,
                                                     Map<ColumnRefOperator, ColumnRefOperator> oldToNew) {
        return keyPairs.stream().map(JoinKeyPair::rightKey).map(oldToNew::get).collect(Collectors.toList());
    }

    private ScalarOperator buildEquiPredicate(List<ColumnRefOperator> leftKeys,
                                              List<ColumnRefOperator> rightKeys) {
        if (leftKeys.size() != rightKeys.size() || leftKeys.isEmpty()) {
            return null;
        }
        List<ScalarOperator> conjuncts = Lists.newArrayListWithCapacity(leftKeys.size());
        for (int i = 0; i < leftKeys.size(); i++) {
            if (leftKeys.get(i) == null || rightKeys.get(i) == null) {
                return null;
            }
            conjuncts.add(new BinaryPredicateOperator(BinaryType.EQ, leftKeys.get(i), rightKeys.get(i)));
        }
        return Utils.compoundAnd(conjuncts);
    }

    private boolean appendBranch(List<OptExpression> unionChildren,
                                 List<List<ColumnRefOperator>> unionChildOutputs,
                                 BranchResult branch) {
        if (branch == null || branch.outputs() == null) {
            return false;
        }
        unionChildren.add(branch.branchExpr());
        unionChildOutputs.add(branch.outputs());
        return true;
    }

    private ColumnRefOperator duplicateActionColumn(ColumnRefFactory columnRefFactory, ColumnRefOperator actionColumn) {
        if (actionColumn == null) {
            return null;
        }
        return columnRefFactory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
    }

    private List<ColumnRefOperator> getJoinOutputsWithoutAction(List<ColumnRefOperator> finalOutputColumns,
                                                                ColumnRefOperator actionColumn) {
        List<ColumnRefOperator> joinOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator outputColumn : finalOutputColumns) {
            if (actionColumn == null || outputColumn.getId() != actionColumn.getId()) {
                joinOutputColumns.add(outputColumn);
            }
        }
        return joinOutputColumns;
    }

    private List<ColumnRefOperator> deriveBranchOutputs(List<ColumnRefOperator> joinOutputColumns,
                                                        ColumnRefOperator actionColumn,
                                                        Map<ColumnRefOperator, ColumnRefOperator> oldToNewColumnMapping) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayListWithCapacity(joinOutputColumns.size() + (actionColumn == null ? 0 : 1));
        for (ColumnRefOperator output : joinOutputColumns) {
            ColumnRefOperator mappedOutput = oldToNewColumnMapping.get(output);
            if (mappedOutput == null) {
                return null;
            }
            outputs.add(mappedOutput);
        }
        if (actionColumn != null) {
            outputs.add(actionColumn);
        }
        return outputs;
    }

    private CallOperator createBuiltinCall(String fnName, Type returnType, List<ScalarOperator> args) {
        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(fnName, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return new CallOperator(fnName, returnType, args);
        }
        return new CallOperator(fnName, returnType, args, fn.copy());
    }

    private record DuplicatedJoin(OptExpression joinExpr,
                                  Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
    }

    private record JoinKeyPair(ColumnRefOperator leftKey, ColumnRefOperator rightKey) {
    }

    private record KeysProducerResult(int cteId,
                                      List<ColumnRefOperator> keyRefs,
                                      OptExpression producer) {
    }

    private record FlipProducerResult(int cteId,
                                      List<ColumnRefOperator> keyRefs,
                                      ColumnRefOperator cnt0,
                                      ColumnRefOperator cnt1,
                                      OptExpression producer) {
    }

    private record FlipConsumeResult(OptExpression consume,
                                     ColumnRefOperator cnt0,
                                     ColumnRefOperator cnt1) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }
}
