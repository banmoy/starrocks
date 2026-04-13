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
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRowIdContext;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IvmRowIdDeriver {
    public static final String DERIVED_ROW_ID_COLUMN_PREFIX = "__row_id_";

    public record Result(boolean success, OptExpression rewrittenRoot, String unsupportedReason,
                         List<ColumnRefOperator> rootRowIdColumnRefs) {
    }

    private IvmRowIdDeriver() {
    }

    public static boolean isDerivedRowIdColumnName(String columnName) {
        return columnName != null && columnName.startsWith(DERIVED_ROW_ID_COLUMN_PREFIX);
    }

    public static Result deriveAndRewrite(OptExpression root, OptimizerContext optimizerContext) {
        IvmRowIdContext context = new IvmRowIdContext(optimizerContext.getColumnRefFactory());
        OptExpression rewritten = root.getOp().accept(new RewriteVisitor(context), root, null);
        if (!context.isSupported()) {
            return new Result(false, root, context.getUnsupportedReason().orElse("row-id derive failed"), List.of());
        }
        List<ColumnRefOperator> rootRowIds = context.getRowIds(root).orElse(List.of());
        if (rootRowIds.stream().anyMatch(ColumnRefOperator::isNullable)) {
            Optional<ColumnRefOperator> existingEncodedRootRowId = resolveEncodedRootRowId(rewritten, rootRowIds);
            if (existingEncodedRootRowId.isPresent()) {
                return new Result(true, rewritten, null, List.of(existingEncodedRootRowId.get()));
            }
            RootRowIdRewriteResult rewriteResult = rewriteNullableRootRowId(rewritten, rootRowIds, context);
            return new Result(true, rewriteResult.rewrittenRoot(), null, List.of(rewriteResult.encodedRowId()));
        }
        return new Result(true, rewritten, null, rootRowIds);
    }

    private record RootRowIdRewriteResult(OptExpression rewrittenRoot, ColumnRefOperator encodedRowId) {
    }

    private static RootRowIdRewriteResult rewriteNullableRootRowId(OptExpression root,
                                                                   List<ColumnRefOperator> rootRowIds,
                                                                   IvmRowIdContext context) {
        ColumnRefOperator encodedRootRowId = context.getColumnRefFactory()
                .create(TvrOpUtils.COLUMN_ROW_ID, buildEncodedRootRowId(rootRowIds).getType(), false);
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap(root.getRowOutputInfo().getColumnRefMap());
        projectMap.put(encodedRootRowId, buildEncodedRootRowId(rootRowIds));
        OptExpression rewrittenRoot = OptExpression.create(new LogicalProjectOperator(projectMap), root);
        context.putRowIds(root, List.of(encodedRootRowId));
        context.putRowIds(rewrittenRoot, List.of(encodedRootRowId));
        return new RootRowIdRewriteResult(rewrittenRoot, encodedRootRowId);
    }

    private static Optional<ColumnRefOperator> resolveEncodedRootRowId(OptExpression root,
                                                                       List<ColumnRefOperator> rootRowIds) {
        ScalarOperator encodedRootRowId = buildEncodedRootRowId(rootRowIds);
        return root.getRowOutputInfo().getColumnRefMap().entrySet().stream()
                .filter(entry -> TvrOpUtils.COLUMN_ROW_ID.equalsIgnoreCase(entry.getKey().getName())
                        || entry.getValue().equals(encodedRootRowId))
                .map(Map.Entry::getKey)
                .findFirst();
    }

    private static ScalarOperator buildEncodedRootRowId(List<ColumnRefOperator> rootRowIds) {
        int encodeRowIdVersion = TvrOpUtils.deduceEncodeRowIdVersionForScalarOperators(List.copyOf(rootRowIds));
        return TvrOpUtils.buildRowIdColumnOperator(encodeRowIdVersion, List.copyOf(rootRowIds));
    }

    private static class RewriteVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final IvmRowIdContext context;

        private RewriteVisitor(IvmRowIdContext context) {
            this.context = context;
        }

        @Override
        public OptExpression visit(OptExpression expression, Void context) {
            this.context.markUnsupported("unsupported operator for OLAP IVM row-id derive: "
                    + expression.getOp().getOpType());
            return expression;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression expression, Void context) {
            LogicalOlapScanOperator scan = (LogicalOlapScanOperator) expression.getOp();
            if (!(scan.getTable() instanceof OlapTable table)) {
                this.context.markUnsupported("only OlapTable is supported in OLAP IVM row-id derive");
                return expression;
            }
            if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
                this.context.markUnsupported("only PRIMARY KEY table is supported in OLAP IVM row-id derive");
                return expression;
            }
            List<Column> keyColumns = table.getKeyColumnsInOrder();
            if (keyColumns.isEmpty()) {
                this.context.markUnsupported("primary key column is missing in OLAP IVM row-id derive");
                return expression;
            }
            List<ColumnRefOperator> rowIds = keyColumns.stream()
                    .map(col -> getOrCreateKeyRef(scan, col))
                    .collect(Collectors.toList());

            Map<ColumnRefOperator, Column> newColRefToMeta = Maps.newHashMap(scan.getColRefToColumnMetaMap());
            Map<Column, ColumnRefOperator> newMetaToColRef = Maps.newHashMap(scan.getColumnMetaToColRefMap());
            boolean scanChanged = false;
            for (int i = 0; i < keyColumns.size(); i++) {
                Column keyColumn = keyColumns.get(i);
                ColumnRefOperator rowId = rowIds.get(i);
                if (!newColRefToMeta.containsKey(rowId)) {
                    newColRefToMeta.put(rowId, keyColumn);
                    scanChanged = true;
                }
                ColumnRefOperator existing = newMetaToColRef.get(keyColumn);
                if (existing == null || !existing.equals(rowId)) {
                    newMetaToColRef.put(keyColumn, rowId);
                    scanChanged = true;
                }
            }

            boolean projectionChanged = false;
            Projection newProjection = scan.getProjection();
            if (scan.getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> projectionMap =
                        Maps.newHashMap(scan.getProjection().getColumnRefMap());
                for (ColumnRefOperator rowId : rowIds) {
                    if (!projectionMap.containsKey(rowId)) {
                        projectionMap.put(rowId, rowId);
                        projectionChanged = true;
                    }
                }
                if (projectionChanged) {
                    newProjection = new Projection(
                            projectionMap,
                            Maps.newHashMap(scan.getProjection().getCommonSubOperatorMap()));
                }
            }

            if (!scanChanged && !projectionChanged) {
                putRowIds(expression, expression, rowIds);
                return expression;
            }

            LogicalOlapScanOperator.Builder builder = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setColRefToColumnMetaMap(newColRefToMeta)
                    .setColumnMetaToColRefMap(newMetaToColRef);
            if (projectionChanged) {
                builder.setProjection(newProjection);
            }
            OptExpression rewritten = OptExpression.create(builder.build());
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("filter must be unary in OLAP IVM row-id derive");
                return expression;
            }
            LogicalFilterOperator filter = (LogicalFilterOperator) expression.getOp();
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
            if (childRowIds == null || childRowIds.isEmpty()) {
                this.context.markUnsupported("filter child row-id is missing in OLAP IVM row-id derive");
                return expression;
            }
            List<ColumnRefOperator> rowIds = filter.getProjection() == null
                    ? childRowIds
                    : mapRowIdsThroughProjection(filter.getProjection().getColumnRefMap(), childRowIds);
            if (filter.getProjection() == null) {
                OptExpression rewritten = rewrittenChild == child ? expression : OptExpression.create(filter, rewrittenChild);
                putRowIds(expression, rewritten, rowIds);
                return rewritten;
            }

            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(filter.getProjection().getColumnRefMap());
            boolean projectionChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectionMap.containsKey(rowId)) {
                    projectionMap.put(rowId, childRowIds.get(i));
                    projectionChanged = true;
                }
            }
            if (!projectionChanged && rewrittenChild == child) {
                putRowIds(expression, expression, rowIds);
                return expression;
            }

            LogicalFilterOperator newFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setProjection(new Projection(
                            projectionMap,
                            Maps.newHashMap(filter.getProjection().getCommonSubOperatorMap())))
                    .build();
            OptExpression rewritten = OptExpression.create(newFilter, rewrittenChild);
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("project must be unary in OLAP IVM row-id derive");
                return expression;
            }
            LogicalProjectOperator project = (LogicalProjectOperator) expression.getOp();
            List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
            if (childRowIds == null || childRowIds.isEmpty()) {
                this.context.markUnsupported("project child row-id is missing in OLAP IVM row-id derive");
                return expression;
            }
            List<ColumnRefOperator> rowIds = mapRowIdsThroughProjection(project.getColumnRefMap(), childRowIds);

            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap(project.getColumnRefMap());
            boolean projectChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectMap.containsKey(rowId)) {
                    projectMap.put(rowId, childRowIds.get(i));
                    projectChanged = true;
                }
            }
            if (!projectChanged && rewrittenChild == child) {
                putRowIds(expression, expression, rowIds);
                return expression;
            }
            LogicalProjectOperator newProject = LogicalProjectOperator.builder()
                    .withOperator(project)
                    .setColumnRefMap(projectMap)
                    .build();
            OptExpression rewritten = OptExpression.create(newProject, rewrittenChild);
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("aggregate must be unary in OLAP IVM row-id derive");
                return expression;
            }
            LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
            if (agg.getGroupingKeys().isEmpty()) {
                this.context.markUnsupported("aggregate without group by is not supported in OLAP IVM row-id derive");
                return expression;
            }
            List<ColumnRefOperator> rowIds = agg.getGroupingKeys();
            OptExpression rewritten =
                    rewrittenChild == child ? expression : OptExpression.create(expression.getOp(), rewrittenChild);
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalWindow(OptExpression expression, Void context) {
            OptExpression child = expression.inputAt(0);
            OptExpression rewrittenChild = child.getOp().accept(this, child, null);
            if (expression.getInputs().size() != 1) {
                this.context.markUnsupported("window must be unary in OLAP IVM row-id derive");
                return expression;
            }
            LogicalWindowOperator window = (LogicalWindowOperator) expression.getOp();
            if (window.getPartitionExpressions().isEmpty()) {
                this.context.markUnsupported("window without partition by is not supported in OLAP IVM row-id derive");
                return expression;
            }
            if (window.getPartitionExpressions().stream().anyMatch(partition -> !(partition instanceof ColumnRefOperator))) {
                this.context.markUnsupported("window partition by expression must be column refs in OLAP IVM row-id derive");
                return expression;
            }

            List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
            if (childRowIds == null || childRowIds.isEmpty()) {
                this.context.markUnsupported("window child row-id is missing in OLAP IVM row-id derive");
                return expression;
            }

            boolean projectionChanged = false;
            Projection newProjection = window.getProjection();
            List<ColumnRefOperator> rowIds = newProjection == null
                    ? childRowIds
                    : mapRowIdsThroughProjection(newProjection.getColumnRefMap(), childRowIds);
            if (newProjection != null && rowIds != null && rowIds.size() == childRowIds.size()) {
                Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(newProjection.getColumnRefMap());
                for (int i = 0; i < rowIds.size(); i++) {
                    ColumnRefOperator rowId = rowIds.get(i);
                    if (!projectionMap.containsKey(rowId)) {
                        projectionMap.put(rowId, childRowIds.get(i));
                        projectionChanged = true;
                    }
                }
                if (projectionChanged) {
                    newProjection = new Projection(projectionMap, Maps.newHashMap(newProjection.getCommonSubOperatorMap()));
                }
            }
            boolean orderingChanged = false;
            List<Ordering> orderByElements = Lists.newArrayList(window.getOrderByElements());
            List<Ordering> enforceSortColumns = Lists.newArrayList(window.getEnforceSortColumns());
            for (ColumnRefOperator childRowId : childRowIds) {
                if (!containsOrderingColumn(orderByElements, childRowId)) {
                    orderByElements.add(new Ordering(childRowId, true, true));
                    orderingChanged = true;
                }
                if (!containsOrderingColumn(enforceSortColumns, childRowId)) {
                    enforceSortColumns.add(new Ordering(childRowId, true, true));
                    orderingChanged = true;
                }
            }

            if (!projectionChanged && !orderingChanged && rewrittenChild == child) {
                putRowIds(expression, expression, rowIds);
                return expression;
            }

            LogicalWindowOperator.Builder builder = LogicalWindowOperator.builder().withOperator(window);
            if (projectionChanged) {
                builder.setProjection(newProjection);
            }
            if (orderingChanged) {
                builder.setOrderByElements(orderByElements);
                builder.setEnforceSortColumns(enforceSortColumns);
            }
            OptExpression rewritten = OptExpression.create(builder.build(), rewrittenChild);
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalIntersect(OptExpression expression, Void context) {
            LogicalIntersectOperator intersect = (LogicalIntersectOperator) expression.getOp();
            List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(expression.arity());
            boolean changed = false;
            for (OptExpression child : expression.getInputs()) {
                OptExpression rewrittenChild = child.getOp().accept(this, child, null);
                rewrittenChildren.add(rewrittenChild);
                changed |= rewrittenChild != child;
            }
            if (expression.getInputs().size() < 2 || intersect.getOutputColumnRefOp().isEmpty()) {
                this.context.markUnsupported("intersect must have at least two outputs in OLAP IVM row-id derive");
                return expression;
            }
            OptExpression rewritten = changed ? OptExpression.create(expression.getOp(), rewrittenChildren) : expression;
            putRowIds(expression, rewritten, intersect.getOutputColumnRefOp());
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalExcept(OptExpression expression, Void context) {
            LogicalExceptOperator except = (LogicalExceptOperator) expression.getOp();
            List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(expression.arity());
            boolean changed = false;
            for (OptExpression child : expression.getInputs()) {
                OptExpression rewrittenChild = child.getOp().accept(this, child, null);
                rewrittenChildren.add(rewrittenChild);
                changed |= rewrittenChild != child;
            }
            if (expression.getInputs().size() < 2 || except.getOutputColumnRefOp().isEmpty()) {
                this.context.markUnsupported("except must have at least two outputs in OLAP IVM row-id derive");
                return expression;
            }
            OptExpression rewritten = changed ? OptExpression.create(expression.getOp(), rewrittenChildren) : expression;
            putRowIds(expression, rewritten, except.getOutputColumnRefOp());
            return rewritten;
        }

        @Override
        public OptExpression visitLogicalUnion(OptExpression expression, Void context) {
            ColumnRefFactory factory = this.context.getColumnRefFactory();
            LogicalUnionOperator union = (LogicalUnionOperator) expression.getOp();
            if (!union.isUnionAll()) {
                this.context.markUnsupported("only union all is supported in OLAP IVM row-id derive");
                return expression;
            }
            if (expression.getInputs().isEmpty()) {
                this.context.markUnsupported("union all must have children in OLAP IVM row-id derive");
                return expression;
            }

            List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(expression.arity());
            List<List<ColumnRefOperator>> newChildOutputs = Lists.newArrayListWithCapacity(expression.arity());
            List<ColumnRefOperator> firstChildRowIds = null;

            for (int i = 0; i < expression.arity(); i++) {
                OptExpression child = expression.inputAt(i);
                OptExpression rewrittenChild = child.getOp().accept(this, child, null);
                rewrittenChildren.add(rewrittenChild);

                List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
                if (childRowIds == null || childRowIds.isEmpty()) {
                    this.context.markUnsupported("union all child row-id is missing in OLAP IVM row-id derive");
                    return expression;
                }
                if (firstChildRowIds == null) {
                    firstChildRowIds = childRowIds;
                } else if (childRowIds.size() != firstChildRowIds.size()) {
                    this.context.markUnsupported("union all children must have the same row-id width in OLAP IVM row-id derive");
                    return expression;
                }
            }

            List<ColumnRefOperator> outputRowIds = Lists.newArrayList();
            int numChildRowIds = firstChildRowIds.size();
            for (int i = 0; i < numChildRowIds; i++) {
                ColumnRefOperator cRowId = firstChildRowIds.get(i);
                outputRowIds.add(factory.create(derivedRowIdColumnName(i, cRowId), cRowId.getType(), cRowId.isNullable()));
            }
            outputRowIds.add(factory.create(derivedChildIndexColumnName(numChildRowIds), IntegerType.INT, false));

            List<ColumnRefOperator> newUnionOutputs = Lists.newArrayList(union.getOutputColumnRefOp());
            newUnionOutputs.addAll(outputRowIds);

            for (int i = 0; i < expression.arity(); i++) {
                OptExpression child = expression.inputAt(i);
                OptExpression rewrittenChild = rewrittenChildren.get(i);
                List<ColumnRefOperator> childRowIds = this.context.getRowIds(child).orElse(null);
                if (childRowIds == null) {
                    this.context.markUnsupported("union all child row-id is missing in OLAP IVM row-id derive");
                    return expression;
                }

                List<ColumnRefOperator> oldChildOutputs = union.getChildOutputColumns().get(i);
                List<ColumnRefOperator> childOutputs = Lists.newArrayList(oldChildOutputs);

                Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
                for (ColumnRefOperator output : oldChildOutputs) {
                    projectMap.put(output, output);
                }
                for (ColumnRefOperator childRowId : childRowIds) {
                    ColumnRefOperator newChildRowId =
                            factory.create(childRowId.getName(), childRowId.getType(), childRowId.isNullable());
                    childOutputs.add(newChildRowId);
                    projectMap.put(newChildRowId, childRowId);
                }
                String childIndexName = derivedChildIndexColumnName(i);
                ColumnRefOperator childIndexRef = factory.create(childIndexName, IntegerType.INT, false);
                childOutputs.add(childIndexRef);
                if (!oldChildOutputs.contains(childIndexRef)) {
                    projectMap.put(childIndexRef, ConstantOperator.createInt(i));
                }

                rewrittenChildren.set(i, OptExpression.create(new LogicalProjectOperator(projectMap), rewrittenChild));
                newChildOutputs.add(childOutputs);
            }

            LogicalUnionOperator newUnion = LogicalUnionOperator.builder()
                    .withOperator(union)
                    .setOutputColumnRefOp(newUnionOutputs)
                    .setChildOutputColumns(newChildOutputs)
                    .build();
            OptExpression unionExpr = OptExpression.create(newUnion, rewrittenChildren);
            putRowIds(expression, unionExpr, outputRowIds);

            return unionExpr;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression expression, Void context) {
            OptExpression leftChild = expression.inputAt(0);
            OptExpression rightChild = expression.inputAt(1);
            OptExpression rewrittenLeft = leftChild.getOp().accept(this, leftChild, null);
            OptExpression rewrittenRight = rightChild.getOp().accept(this, rightChild, null);
            if (expression.getInputs().size() != 2) {
                this.context.markUnsupported("join must be binary in OLAP IVM row-id derive");
                return expression;
            }
            LogicalJoinOperator join = (LogicalJoinOperator) expression.getOp();
            JoinOperator joinType = join.getJoinType();
            if (!joinType.isInnerJoin() && !joinType.isCrossJoin()
                    && !joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin() && !joinType.isFullOuterJoin()
                    && !joinType.isLeftAntiJoin() && !joinType.isRightAntiJoin()
                    && !joinType.isLeftSemiJoin() && !joinType.isRightSemiJoin()) {
                this.context.markUnsupported(
                        "only inner/cross/left outer/right outer/full outer/left semi/right semi/left anti/right anti "
                                + "join is supported in OLAP IVM row-id derive");
                return expression;
            }

            List<ColumnRefOperator> leftRowIds = this.context.getRowIds(leftChild).orElse(null);
            List<ColumnRefOperator> rightRowIds = this.context.getRowIds(rightChild).orElse(null);
            if (leftRowIds == null || leftRowIds.isEmpty() || rightRowIds == null || rightRowIds.isEmpty()) {
                this.context.markUnsupported("join child row-id is missing in OLAP IVM row-id derive");
                return expression;
            }
            List<ColumnRefOperator> inputRowIds = getJoinInputRowIds(join.getJoinType(), leftRowIds, rightRowIds);
            List<ColumnRefOperator> rowIds = join.getProjection() == null
                    ? inputRowIds
                    : mapRowIdsThroughProjection(join.getProjection().getColumnRefMap(), inputRowIds);
            if (join.getProjection() == null) {
                OptExpression rewritten = rewrittenLeft == leftChild && rewrittenRight == rightChild
                        ? expression : OptExpression.create(join, rewrittenLeft, rewrittenRight);
                putRowIds(expression, rewritten, rowIds);
                return rewritten;
            }

            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap(join.getProjection().getColumnRefMap());
            boolean projectionChanged = false;
            for (int i = 0; i < rowIds.size(); i++) {
                ColumnRefOperator rowId = rowIds.get(i);
                if (!projectionMap.containsKey(rowId)) {
                    projectionMap.put(rowId, inputRowIds.get(i));
                    projectionChanged = true;
                }
            }
            if (!projectionChanged && rewrittenLeft == leftChild && rewrittenRight == rightChild) {
                putRowIds(expression, expression, rowIds);
                return expression;
            }

            LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                    .withOperator(join)
                    .setProjection(new Projection(
                            projectionMap,
                            Maps.newHashMap(join.getProjection().getCommonSubOperatorMap())))
                    .build();
            OptExpression rewritten = OptExpression.create(newJoin, rewrittenLeft, rewrittenRight);
            putRowIds(expression, rewritten, rowIds);
            return rewritten;
        }

        private boolean containsOrderingColumn(List<Ordering> orderings, ColumnRefOperator target) {
            return orderings.stream().anyMatch(ordering -> ordering.getColumnRef().equals(target));
        }

        private void putRowIds(OptExpression original, OptExpression rewritten, List<ColumnRefOperator> rowIds) {
            this.context.putRowIds(original, rowIds);
            if (rewritten != original) {
                this.context.putRowIds(rewritten, rowIds);
            }
        }

        private List<ColumnRefOperator> getJoinInputRowIds(JoinOperator joinType,
                                                           List<ColumnRefOperator> leftRowIds,
                                                           List<ColumnRefOperator> rightRowIds) {
            if (joinType.isLeftAntiJoin() || joinType.isLeftSemiJoin()) {
                return Lists.newArrayList(leftRowIds);
            }
            if (joinType.isRightAntiJoin() || joinType.isRightSemiJoin()) {
                return Lists.newArrayList(rightRowIds);
            }
            List<ColumnRefOperator> inputRowIds = Lists.newArrayListWithCapacity(leftRowIds.size() + rightRowIds.size());
            inputRowIds.addAll(leftRowIds);
            inputRowIds.addAll(rightRowIds);
            return inputRowIds;
        }

        private List<ColumnRefOperator> mapRowIdsThroughProjection(
                Map<ColumnRefOperator, ScalarOperator> projectionMap,
                List<ColumnRefOperator> inputRowIds) {
            List<ColumnRefOperator> outputRowIds = new ArrayList<>(inputRowIds.size());
            for (int i = 0; i < inputRowIds.size(); i++) {
                ColumnRefOperator input = inputRowIds.get(i);
                final int idx = i;
                ColumnRefOperator output = findOutputRef(projectionMap, input).orElseGet(
                        () -> this.context.getColumnRefFactory().create(
                                derivedRowIdColumnName(idx, input), input.getType(), input.isNullable()));
                outputRowIds.add(output);
            }
            return outputRowIds;
        }

        private String derivedRowIdColumnName(int idx, ColumnRefOperator input) {
            if (isDerivedRowIdColumnName(input.getName())) {
                return input.getName();
            }
            return DERIVED_ROW_ID_COLUMN_PREFIX + idx + "_" + input.getName();
        }

        private ColumnRefOperator getOrCreateKeyRef(LogicalOlapScanOperator scan, Column keyColumn) {
            ColumnRefOperator keyRef = scan.getColumnReference(keyColumn);
            if (keyRef != null) {
                return keyRef;
            }
            return this.context.getColumnRefFactory()
                    .create(keyColumn.getName(), keyColumn.getType(), keyColumn.isAllowNull());
        }

        private Optional<ColumnRefOperator> findOutputRef(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                          ColumnRefOperator input) {
            return columnRefMap.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(input))
                    .map(Map.Entry::getKey)
                    .findFirst();
        }
    }

    private static String derivedChildIndexColumnName(int idx) {
        return DERIVED_ROW_ID_COLUMN_PREFIX + idx + "_child_index";
    }
}
