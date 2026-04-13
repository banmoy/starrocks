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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IvmDeltaAggregateRule extends TransformationRule {
    private static final byte DELETE_ACTION = -1;
    private static final byte INSERT_ACTION = 1;

    public IvmDeltaAggregateRule() {
        super(RuleType.TF_OLAP_IVM_DELTA_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.inputAt(0).getOp();
        OptExpression child = input.inputAt(0).inputAt(0);
        if (!isSupportedAggregate(agg)) {
            return List.of();
        }

        OptExpression optimized = tryRewriteByChangesAndMv(context, delta, agg, child);
        if (optimized != null) {
            return List.of(optimized);
        }

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        List<ColumnRefOperator> finalChildOutputs = child.getOutputColumns().getColumnRefOperators(columnRefFactory);
        List<ColumnRefOperator> finalGroupingKeys = agg.getGroupingKeys();

        if (finalGroupingKeys.stream().anyMatch(k -> !finalChildOutputs.contains(k))) {
            return List.of();
        }

        // Snapshot: Child -> Project -> Version
        SnapshotInfo toSnapshot = createSnapshotChild(context, finalGroupingKeys, child, finalChildOutputs,
                LogicalVersionOperator.VersionRefType.TO_VERSION, (byte) 1);
        SnapshotInfo fromSnapshot =
                createSnapshotChild(context, finalGroupingKeys, child, finalChildOutputs,
                        LogicalVersionOperator.VersionRefType.FROM_VERSION, (byte) -1);

        // DistinctAgg left semi join (Child -> DistinctAgg).
        SnapshotInfo affectedKeysChild = cloneChild(context, finalGroupingKeys, child, finalChildOutputs);
        List<ColumnRefOperator> affectedKeys = affectedKeysChild.groupingKeys;
        LogicalAggregationOperator affectedKeysOperator =
                new LogicalAggregationOperator(AggType.GLOBAL, affectedKeys, Maps.newHashMap());
        ColumnRefOperator affectedKeysActionColumn =
                IvmRuleUtils.createActionColumn(columnRefFactory, delta.getActionColumn());
        OptExpression affectedKeysOptExpr = OptExpression.create(affectedKeysOperator,
                OptExpression.create(new LogicalDeltaOperator(false, affectedKeysActionColumn), affectedKeysChild.optExpression));
        int cteId = context.getCteContext().getNextCteId();
        OptExpression affectedKeysProducer = OptExpression.create(new LogicalCTEProduceOperator(cteId), affectedKeysOptExpr);

        OptExpression toJoin = createLeftSemiJoin(columnRefFactory, cteId, affectedKeys, toSnapshot);
        OptExpression fromJoin = createLeftSemiJoin(columnRefFactory, cteId, affectedKeys, fromSnapshot);
        if (toJoin == null || fromJoin == null) {
            return List.of();
        }

        // Union All to/from left semi join.
        List<List<ColumnRefOperator>> unionChildrenOutputs = List.of(toSnapshot.outputColumns, fromSnapshot.outputColumns);
        List<ColumnRefOperator> unionOutputColumns = new ArrayList<>(finalChildOutputs);
        unionOutputColumns.add(delta.getActionColumn());
        OptExpression unionOptExpr = OptExpression.create(
                new LogicalUnionOperator(unionOutputColumns, unionChildrenOutputs, true),
                toJoin, fromJoin);

        // New Aggregation grouping on original grouping keys + action column, partition by original grouping keys.
        List<ColumnRefOperator> newGroupingKeys = new ArrayList<>(finalGroupingKeys);
        newGroupingKeys.add(delta.getActionColumn());
        LogicalAggregationOperator newAgg = LogicalAggregationOperator.builder()
                .withOperator(agg)
                .setGroupingKeys(newGroupingKeys)
                .setPartitionByColumns(finalGroupingKeys) // Grouping keys except actionColumn.
                .build();
        OptExpression newAggOptExpr = OptExpression.create(newAgg, unionOptExpr);
        OptExpression cteAnchor = OptExpression.create(new LogicalCTEAnchorOperator(cteId),
                affectedKeysProducer, newAggOptExpr);
        return List.of(cteAnchor);
    }

    /**
     * Fast path for root aggregate IVM refresh on PK MV: rewrite by merging {@code CHANGES} with current MV state.
     */
    private OptExpression tryRewriteByChangesAndMv(OptimizerContext context,
                                                   LogicalDeltaOperator delta,
                                                   LogicalAggregationOperator agg,
                                                   OptExpression child) {
        // This fast path is only valid for the root IVM aggregate refresh on primary-key MV.
        if (!delta.isRootDelta()) {
            return null;
        }
        MaterializedView targetMv = resolveTargetMv(context);
        if (targetMv == null || targetMv.getKeysType() != KeysType.PRIMARY_KEYS) {
            return null;
        }
        // Align aggregate input/grouping columns to the duplicated "changes" subtree.
        ColumnRefFactory factory = context.getColumnRefFactory();
        List<ColumnRefOperator> oldChildOutputs = child.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> finalGroupingKeys = agg.getGroupingKeys();
        if (finalGroupingKeys.stream().anyMatch(k -> !oldChildOutputs.contains(k))) {
            return null;
        }

        // DeltaChild.
        SnapshotInfo deltaChild = cloneChild(context, finalGroupingKeys, child, oldChildOutputs);

        // DeltaChild -> DeltaAggregate.
        ColumnRefOperator deltaActionColumn =
                factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        Map<ColumnRefOperator, CallOperator> deltaAggCalls = Maps.newHashMap();
        List<RetractableAggInfo> aggInfos = Lists.newArrayListWithCapacity(agg.getAggregations().size());
        Map<ColumnRefOperator, CallOperator> avgAggCalls = Maps.newHashMap();
        RetractableAggInfo totalCountInfo = null;
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
            String fnName = entry.getValue().getFnName().toLowerCase();
            if (FunctionSet.AVG.equals(fnName)) {
                avgAggCalls.put(entry.getKey(), entry.getValue());
                continue;
            }

            RetractableAggInfo info = buildRetractableAggInfo(
                    entry.getKey(), entry.getValue(), deltaChild.oldToNewMapping, deltaActionColumn, factory);
            if (info == null) {
                return null;
            }

            deltaAggCalls.put(info.deltaOutputRef, info.deltaAggCall);
            if (info.kind == AggKind.COUNT_ONE && totalCountInfo == null) {
                totalCountInfo = info;
            }
            aggInfos.add(info);
        }
        if (totalCountInfo == null) {
            return null;
        }
        LogicalAggregationOperator deltaAgg =
                new LogicalAggregationOperator(AggType.GLOBAL, deltaChild.groupingKeys, deltaAggCalls);
        LogicalDeltaOperator newDelta = new LogicalDeltaOperator.Builder()
                .withOperator(delta)
                .setRootDelta(false)
                .setActionColumn(deltaActionColumn)
                .build();
        OptExpression deltaAggOptExpr = OptExpression.create(deltaAgg, OptExpression.create(newDelta, deltaChild.optExpression));

        // MV Scan
        MvScanInfo mvScanInfo = buildMvScan(
                targetMv, finalGroupingKeys, aggInfos, factory, delta.getMvColumnMapping(), totalCountInfo);
        if (mvScanInfo == null) {
            return null;
        }

        // DeltaAggregate left outer join MV Scan on grouping keys.
        ScalarOperator joinOn = buildJoinOnByKeys(deltaChild.groupingKeys, mvScanInfo.groupingKeyRefs);
        if (joinOn == null) {
            return null;
        }
        OptExpression joinExpr = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, joinOn),
                deltaAggOptExpr, mvScanInfo.scanExpr);

        // Split delete/insert filter and project
        Map<ColumnRefOperator, ColumnRefOperator> deleteConsumerToProducerMap = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> insertConsumerToProducerMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> deleteProjectMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> insertProjectMap = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> deleteFinalToConsumerOutputMap = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> insertFinalToConsumerOutputMap = Maps.newHashMap();

        int mergeCteId = context.getCteContext().getNextCteId();
        ColumnRefOperator finalActionColumn = delta.getActionColumn();

        OptExpression deleteBranch = createSplit(deltaChild, aggInfos, mvScanInfo,
                finalGroupingKeys, finalActionColumn, factory, mergeCteId,
                (byte) -1, deleteConsumerToProducerMap, deleteProjectMap, deleteFinalToConsumerOutputMap);
        OptExpression insertBranch = createSplit(deltaChild, aggInfos, mvScanInfo,
                finalGroupingKeys, delta.getActionColumn(), factory, mergeCteId,
                (byte) 1, insertConsumerToProducerMap, insertProjectMap, insertFinalToConsumerOutputMap);
        OptExpression joinProducer = OptExpression.create(new LogicalCTEProduceOperator(mergeCteId), joinExpr);

        List<ColumnRefOperator> finalOutputs = Lists.newArrayList();
        finalOutputs.addAll(finalGroupingKeys);
        finalOutputs.addAll(agg.getAggregations().keySet());
        finalOutputs.add(finalActionColumn);
        List<ColumnRefOperator> deleteOutputs = finalOutputs.stream().map(deleteFinalToConsumerOutputMap::get).toList();
        List<ColumnRefOperator> insertOutputs = finalOutputs.stream().map(insertFinalToConsumerOutputMap::get).toList();
        List<List<ColumnRefOperator>> childOutputCols = List.of(deleteOutputs, insertOutputs);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(finalOutputs, childOutputCols, true);
        OptExpression unionChanges = OptExpression.create(unionOperator, deleteBranch, insertBranch);

        return OptExpression.create(new LogicalCTEAnchorOperator(mergeCteId), joinProducer, unionChanges);
    }

    private record SnapshotInfo(OptExpression optExpression, List<ColumnRefOperator> outputColumns,
                                List<ColumnRefOperator> groupingKeys, Map<ColumnRefOperator, ColumnRefOperator> oldToNewMapping) {
    }

    private SnapshotInfo cloneChild(OptimizerContext context,
                                    List<ColumnRefOperator> oldGroupingKeys,
                                    OptExpression child,
                                    List<ColumnRefOperator> oldOutputs) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(columnRefFactory, context);
        OptExpression newChildExpr = duplicator.duplicate(child);
        List<ColumnRefOperator> newOutputs = duplicator.getMappedColumns(oldOutputs);

        List<ColumnRefOperator> newGroupingKeys = Lists.newArrayListWithCapacity(oldGroupingKeys.size());
        for (ColumnRefOperator groupingKey : oldGroupingKeys) {
            ColumnRefOperator newGroupingKey = duplicator.getColumnMapping().get(groupingKey);
            newGroupingKeys.add(newGroupingKey);
        }

        return new SnapshotInfo(newChildExpr, newOutputs, newGroupingKeys, duplicator.getColumnMapping());
    }

    private SnapshotInfo createSnapshotChild(OptimizerContext context,
                                             List<ColumnRefOperator> groupingKeys,
                                             OptExpression child,
                                             List<ColumnRefOperator> oldOutputs,
                                             LogicalVersionOperator.VersionRefType versionRefType,
                                             byte actionValue) {
        ColumnRefFactory factory = context.getColumnRefFactory();

        // Child
        SnapshotInfo clonedChild = cloneChild(context, groupingKeys, child, oldOutputs);

        // Project
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator out : clonedChild.outputColumns) {
            projectMap.put(out, out);
        }
        ColumnRefOperator actionColumn = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        projectMap.put(actionColumn, ConstantOperator.createTinyInt(actionValue));
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);

        // Version
        LogicalVersionOperator versionOperator = new LogicalVersionOperator(versionRefType);

        // Child -> Project -> Version
        OptExpression optExpr =
                OptExpression.create(versionOperator, OptExpression.create(projectOperator, clonedChild.optExpression));
        List<ColumnRefOperator> snapshotOutputs = Lists.newArrayList(clonedChild.outputColumns);
        snapshotOutputs.add(actionColumn);
        return new SnapshotInfo(optExpr, snapshotOutputs, clonedChild.groupingKeys, clonedChild.oldToNewMapping);
    }

    private OptExpression createLeftSemiJoin(ColumnRefFactory columnRefFactory, int cteId,
                                             List<ColumnRefOperator> rightProducerOutputColumns, SnapshotInfo leftSnapshotInfo) {
        List<ColumnRefOperator> consumerOutputColumns = Lists.newArrayList();
        Map<ColumnRefOperator, ColumnRefOperator> consumerMap = Maps.newHashMap();
        for (ColumnRefOperator inputCol : rightProducerOutputColumns) {
            ColumnRefOperator outputCol = columnRefFactory.create(inputCol.getName(), inputCol.getType(), inputCol.isNullable());
            consumerMap.put(outputCol, inputCol);
            consumerOutputColumns.add(outputCol);
        }
        OptExpression rightConsumer = OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerMap));

        // (Snapshot) left semi join (AffectedKeysConsumer)
        ScalarOperator toOnPredicate = buildSemiJoinPredicate(leftSnapshotInfo.groupingKeys, consumerOutputColumns);
        if (toOnPredicate == null) {
            return null;
        }
        return OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, toOnPredicate),
                leftSnapshotInfo.optExpression,
                rightConsumer);
    }

    // left outer join -> consumer -> filter -> project -> union all
    private OptExpression createSplit(SnapshotInfo leftChild, List<RetractableAggInfo> leftAggInfos, MvScanInfo right,
                                      List<ColumnRefOperator> finalOutputGroupingKeys, ColumnRefOperator finalActionColumn,
                                      ColumnRefFactory factory, int cteId, byte actionValue,
                                      Map<ColumnRefOperator, ColumnRefOperator> consumerToProducerMap,
                                      Map<ColumnRefOperator, ScalarOperator> projectMap,
                                      Map<ColumnRefOperator, ColumnRefOperator> finalToConsumerMap) {
        for (int i = 0; i < leftChild.groupingKeys.size(); i++) {
            ColumnRefOperator producerOutput = leftChild.groupingKeys.get(i);
            ColumnRefOperator finalOutput = finalOutputGroupingKeys.get(i);

            ColumnRefOperator consumerOutput =
                    factory.create(producerOutput.getName(), producerOutput.getType(), producerOutput.isNullable());
            consumerToProducerMap.put(consumerOutput, producerOutput);
            projectMap.put(consumerOutput, consumerOutput);
            finalToConsumerMap.put(finalOutput, consumerOutput);
        }

        RetractableAggInfo consumerCount1 = null;
        for (RetractableAggInfo info : leftAggInfos) {
            ColumnRefOperator finalOutput = info.outputRef;

            RetractableAggInfo consumerInfo = info.cloneColumnRef(factory, consumerToProducerMap);
            ScalarOperator consumerCall = actionValue == DELETE_ACTION ?
                    consumerInfo.computeDeleteColumn(right, factory) :
                    consumerInfo.computeInsertColumn(right, factory);
            ColumnRefOperator consumerOutput =
                    factory.create(finalOutput.getName(), finalOutput.getType(), finalOutput.isNullable());
            projectMap.put(consumerOutput, consumerCall);
            finalToConsumerMap.put(finalOutput, consumerOutput);

            if (info.kind == AggKind.COUNT_ONE && consumerCount1 == null) {
                consumerCount1 = consumerInfo;
            }
        }

        if (consumerCount1 == null) {
            return null;
        }

        ColumnRefOperator consumerActionColumn =
                factory.create(finalActionColumn.getName(), finalActionColumn.getType(), finalActionColumn.isNullable());
        projectMap.put(consumerActionColumn, ConstantOperator.createTinyInt(actionValue));
        finalToConsumerMap.put(finalActionColumn, consumerActionColumn);

        ColumnRefOperator consumerCount0 =
                factory.create(right.totalCountRef.getName(), right.totalCountRef.getType(), right.totalCountRef.isNullable());
        consumerToProducerMap.put(consumerCount0, right.totalCountRef);
        ScalarOperator existCount = actionValue == DELETE_ACTION ?
                coalesceZero(consumerCount0) :
                addOperator(coalesceZero(consumerCount0), consumerCount1.deltaOutputRef, IntegerType.BIGINT);
        ScalarOperator existPredicate = new BinaryPredicateOperator(BinaryType.GT, existCount, ConstantOperator.createBigint(0));

        OptExpression consumer = OptExpression.create(new LogicalCTEConsumeOperator(cteId, consumerToProducerMap));
        return OptExpression.create(new LogicalProjectOperator(projectMap),
                OptExpression.create(new LogicalFilterOperator(existPredicate), consumer));
    }

    private boolean isSupportedAggregate(LogicalAggregationOperator agg) {
        if (!agg.getType().isGlobal()) {
            return false;
        }
        if (CollectionUtils.isEmpty(agg.getGroupingKeys()) || CollectionUtils.isEmpty(agg.getAggregations().entrySet())) {
            return false;
        }
        if (agg.getPredicate() != null) {
            return false;
        }
        return agg.getAggregations().values().stream().noneMatch(CallOperator::isDistinct);
    }

    private ScalarOperator buildSemiJoinPredicate(List<ColumnRefOperator> leftGroupingKeys,
                                                  List<ColumnRefOperator> rightGroupingKeys) {
        if (leftGroupingKeys.size() != rightGroupingKeys.size()) {
            return null;
        }
        List<ScalarOperator> joinConjuncts = Lists.newArrayListWithCapacity(leftGroupingKeys.size());
        for (int i = 0; i < leftGroupingKeys.size(); i++) {
            ColumnRefOperator leftKey = leftGroupingKeys.get(i);
            ColumnRefOperator rightKey = rightGroupingKeys.get(i);
            joinConjuncts.add(new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey));
        }
        return Utils.compoundAnd(joinConjuncts);
    }

    private ScalarOperator buildJoinOnByKeys(List<ColumnRefOperator> leftKeys, List<ColumnRefOperator> rightKeys) {
        if (leftKeys.size() != rightKeys.size()) {
            return null;
        }
        List<ScalarOperator> predicates = Lists.newArrayListWithCapacity(leftKeys.size());
        for (int i = 0; i < leftKeys.size(); i++) {
            predicates.add(new BinaryPredicateOperator(BinaryType.EQ, leftKeys.get(i), rightKeys.get(i)));
        }
        return Utils.compoundAnd(predicates);
    }

    private MaterializedView resolveTargetMv(OptimizerContext context) {
        if (!(context.getStatement() instanceof InsertStmt insertStmt)) {
            return null;
        }
        if (!insertStmt.isSystem() || !(insertStmt.getTargetTable() instanceof MaterializedView targetMv)) {
            return null;
        }
        return targetMv;
    }

    private RetractableAggInfo buildRetractableAggInfo(ColumnRefOperator oldOutput, CallOperator call,
                                                       Map<ColumnRefOperator, ColumnRefOperator> oldToNewMapping,
                                                       ColumnRefOperator actionColumn,
                                                       ColumnRefFactory columnRefFactory) {
        String fnName = call.getFnName().toLowerCase();
        if (call.isDistinct()) {
            return null;
        }

        ColumnRefOperator deltaRef = columnRefFactory.create("__delta_" + oldOutput.getName(), oldOutput.getType(), false);
        if (FunctionSet.COUNT.equals(fnName)) {
            ScalarOperator bigintAction = castOperator(actionColumn, IntegerType.BIGINT);
            if (isCountStarOrOne(call)) {
                return new RetractableAggInfo(oldOutput, AggKind.COUNT_ONE, deltaRef, sumCall(IntegerType.BIGINT, actionColumn));
            }
            if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ColumnRefOperator arg)) {
                return null;
            }
            ColumnRefOperator mappedArg = oldToNewMapping.get(arg);
            if (mappedArg == null) {
                return null;
            }

            ScalarOperator countExpr = nullToZero(mappedArg, bigintAction, IntegerType.BIGINT);
            return new RetractableAggInfo(oldOutput, AggKind.COUNT_COLUMN, deltaRef, sumCall(IntegerType.BIGINT, countExpr));
        }
        if (FunctionSet.SUM.equals(fnName)) {
            if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ColumnRefOperator arg)) {
                return null;
            }
            ColumnRefOperator mappedArg = oldToNewMapping.get(arg);
            if (mappedArg == null) {
                return null;
            }
            ScalarOperator typedAction = castOperator(actionColumn, mappedArg.getType());
            ScalarOperator scaled = multiplyOperator(mappedArg, typedAction, oldOutput.getType());
            return new RetractableAggInfo(oldOutput, AggKind.SUM_COLUMN, deltaRef, sumCall(oldOutput.getType(), scaled));
        }

        return null;
    }

    private MvScanInfo buildMvScan(MaterializedView targetMv, List<ColumnRefOperator> aggGroupingKeys,
                                   List<RetractableAggInfo> infos, ColumnRefFactory columnRefFactory,
                                   Map<ColumnRefOperator, Column> mvColumnMapping, RetractableAggInfo totalCountInfo) {
        Map<ColumnRefOperator, Column> colRefToMeta = Maps.newHashMap();
        Map<Column, ColumnRefOperator> metaToColRef = Maps.newHashMap();
        List<ColumnRefOperator> groupingColumns = Lists.newArrayListWithCapacity(aggGroupingKeys.size());
        for (ColumnRefOperator groupingKey : aggGroupingKeys) {
            Column mvColumn = mvColumnMapping.get(groupingKey);
            if (mvColumn == null) {
                return null;
            }
            ColumnRefOperator col = createScanColumnRef(columnRefFactory, targetMv, mvColumn, colRefToMeta, metaToColRef);
            groupingColumns.add(col);
        }

        for (RetractableAggInfo info : infos) {
            Column mvColumn = mvColumnMapping.get(info.outputRef);
            if (mvColumn == null) {
                return null;
            }
            info.mvColRef = createScanColumnRef(columnRefFactory, targetMv, mvColumn, colRefToMeta, metaToColRef);
        }
        ColumnRefOperator totalCountColumn = totalCountInfo.mvColRef;

        LogicalOlapScanOperator scan = new LogicalOlapScanOperator(targetMv, colRefToMeta, metaToColRef, null,
                Operator.DEFAULT_LIMIT, null, targetMv.getBaseIndexMetaId(), targetMv.getAllPartitionIds(), null,
                false, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), false, null);
        return new MvScanInfo(OptExpression.create(scan), groupingColumns, totalCountColumn);
    }

    private ColumnRefOperator createScanColumnRef(ColumnRefFactory factory, MaterializedView targetMv, Column column,
                                                  Map<ColumnRefOperator, Column> colRefToMeta,
                                                  Map<Column, ColumnRefOperator> metaToColRef) {
        if (metaToColRef.containsKey(column)) {
            return metaToColRef.get(column);
        }
        ColumnRefOperator ref = factory.create(column.getName(), column.getType(), column.isAllowNull());
        factory.updateColumnRefToColumns(ref, column, targetMv);
        colRefToMeta.put(ref, column);
        metaToColRef.put(column, ref);
        return ref;
    }

    private static boolean isCountStarOrOne(CallOperator call) {
        if (call.getArguments().isEmpty()) {
            return true;
        }
        if (call.getArguments().size() != 1 || !(call.getChild(0) instanceof ConstantOperator constant)) {
            return false;
        }
        Optional<ConstantOperator> asBigint = constant.castTo(IntegerType.BIGINT);
        return asBigint.isPresent() && !asBigint.get().isNull() && asBigint.get().getBigint() == 1L;
    }

    private static CallOperator sumCall(Type returnType, ScalarOperator arg) {
        return createBuiltinCall(FunctionSet.SUM, returnType, List.of(arg));
    }

    private static ScalarOperator coalesceZero(ScalarOperator input) {
        return new CaseWhenOperator(input.getType(), null,
                input, List.of(new IsNullPredicateOperator(input), zeroConstant(input.getType())));
    }

    private static ScalarOperator nullToZero(ScalarOperator nullableExpr, ScalarOperator nonNullExpr, Type type) {
        return new CaseWhenOperator(type, null,
                nonNullExpr, List.of(new IsNullPredicateOperator(nullableExpr), zeroConstant(type)));
    }

    private static ScalarOperator addOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.ADD, type, List.of(left, right));
    }

    private static ScalarOperator multiplyOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.MULTIPLY, type, List.of(left, right));
    }

    private static ScalarOperator divideOperator(ScalarOperator left, ScalarOperator right, Type type) {
        return createBuiltinCall(FunctionSet.DIVIDE, type, List.of(left, right));
    }

    private static ScalarOperator castOperator(ScalarOperator input, Type targetType) {
        if (input.getType().matchesType(targetType)) {
            return input;
        }
        return new CastOperator(targetType, input, true);
    }

    private static ScalarOperator avgFromSumCount(ScalarOperator sumExpr, ScalarOperator countExpr, Type type) {
        ScalarOperator safeCount = coalesceZero(countExpr);
        return new CaseWhenOperator(type, null, divideOperator(sumExpr, safeCount, type),
                List.of(new BinaryPredicateOperator(BinaryType.LE, safeCount, ConstantOperator.createBigint(0L)),
                        ConstantOperator.createNull(type)));
    }

    private static CallOperator createBuiltinCall(String fnName, Type returnType, List<ScalarOperator> args) {
        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(fnName, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return new CallOperator(fnName, returnType, args);
        }
        Function copied = fn.copy();
        copied = copied.updateArgType(argTypes);
        copied.setRetType(returnType);
        return new CallOperator(fnName, returnType, args, copied);
    }

    private static ConstantOperator zeroConstant(Type type) {
        Optional<ConstantOperator> casted = ConstantOperator.createBigint(0L).castTo(type);
        if (casted.isPresent()) {
            return casted.get();
        }
        return ConstantOperator.createInt(0);
    }

    private enum AggKind {
        COUNT_ONE,
        COUNT_COLUMN,
        SUM_COLUMN
    }

    private static final class RetractableAggInfo {
        private final ColumnRefOperator outputRef;
        private final AggKind kind;
        private final CallOperator deltaAggCall;
        private final ColumnRefOperator deltaOutputRef;
        private ColumnRefOperator mvColRef;

        private RetractableAggInfo(ColumnRefOperator outputRef, AggKind kind, ColumnRefOperator deltaOutputRef,
                                   CallOperator deltaAggCall) {
            this.outputRef = outputRef;
            this.kind = kind;
            this.deltaOutputRef = deltaOutputRef;
            this.deltaAggCall = deltaAggCall;
        }

        private RetractableAggInfo cloneColumnRef(ColumnRefFactory factory,
                                                  Map<ColumnRefOperator, ColumnRefOperator> newToOldMapping) {
            ColumnRefOperator newDeltaOutputRef =
                    factory.create(deltaOutputRef.getName(), deltaOutputRef.getType(), deltaOutputRef.isNullable());
            ColumnRefOperator newMvColRef = factory.create(mvColRef.getName(), mvColRef.getType(), mvColRef.isNullable());

            newToOldMapping.put(newDeltaOutputRef, deltaOutputRef);
            newToOldMapping.put(newMvColRef, mvColRef);

            RetractableAggInfo cloned = new RetractableAggInfo(outputRef, kind, newDeltaOutputRef, deltaAggCall);
            cloned.mvColRef = newMvColRef;

            return cloned;
        }

        private ScalarOperator computeDeleteColumn(MvScanInfo scanInfo, ColumnRefFactory factory) {
            return switch (kind) {
                case COUNT_ONE -> coalesceZero(mvColRef);
                case COUNT_COLUMN, SUM_COLUMN -> coalesceZero(mvColRef);
            };
        }

        private ScalarOperator computeInsertColumn(MvScanInfo scanInfo, ColumnRefFactory factory) {
            return switch (kind) {
                case COUNT_ONE -> addOperator(coalesceZero(mvColRef), deltaOutputRef, IntegerType.BIGINT);
                case COUNT_COLUMN -> addOperator(coalesceZero(mvColRef), deltaOutputRef, IntegerType.BIGINT);
                case SUM_COLUMN -> addOperator(coalesceZero(mvColRef), deltaOutputRef, outputRef.getType());
            };
        }
    }

    private record MvScanInfo(OptExpression scanExpr, List<ColumnRefOperator> groupingKeyRefs, ColumnRefOperator totalCountRef) {
    }

}
