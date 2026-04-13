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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonSyntaxException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.load.Load;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class IvmRewriter {
    private IvmRewriter() {
    }

    public static void rewrite(OptExpression tree, TaskContext rootTaskContext, TaskScheduler scheduler,
                               ColumnRefSet requiredColumns) {
        OptimizerContext optimizerContext = rootTaskContext.getOptimizerContext();
        if (!optimizerContext.getSessionVariable().isEnableOlapIVMRefresh()) {
            return;
        }

        OptExpression originalPlan = tree.getInputs().get(0);
        OptExpression ivmInput = originalPlan;

        ivmInput = bindBaseTableVersionForIvm(ivmInput, optimizerContext);

        MaterializedView targetMv = loadTargetMv(optimizerContext);
        Map<ColumnRefOperator, Column> mvColumnMapping = Maps.newHashMap();
        if (targetMv != null) {
            deriveLogicalProperty(ivmInput);
            mvColumnMapping = buildMvColumnMapping(ivmInput, optimizerContext, targetMv);
        }

        ColumnRefOperator actionColumn = optimizerContext.getColumnRefFactory()
                .create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
        tree.setChild(0, OptExpression.create(
                new LogicalDeltaOperator(true, actionColumn, mvColumnMapping), ivmInput));
        deriveLogicalProperty(tree);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.OLAP_IVM_DELTA_REWRITE_RULES);
        if (IvmRuleUtils.containsLogicalDelta(tree.getInputs().get(0))
                || IvmRuleUtils.containsLogicalVersion(tree.getInputs().get(0))) {
            tree.setChild(0, originalPlan);
            deriveLogicalProperty(tree);
            return;
        }

        OptExpression rewrittenRoot = tree.getInputs().get(0);
        deriveLogicalProperty(rewrittenRoot);
        if (isPrimaryKeyTargetMv(optimizerContext)) {
            rewrittenRoot = appendPkLoadOpColumn(rewrittenRoot, rootTaskContext, requiredColumns, targetMv, mvColumnMapping);
        }
        tree.setChild(0, rewrittenRoot);
        deriveLogicalProperty(tree);
    }

    private static OptExpression bindBaseTableVersionForIvm(OptExpression root, OptimizerContext optimizerContext) {
        MaterializedView targetMv = loadTargetMv(optimizerContext);
        if (targetMv == null || targetMv.getRefreshScheme() == null
                || targetMv.getRefreshScheme().getAsyncRefreshContext() == null) {
            return root;
        }

        Map<Long, Long> baseTableRefreshVersionMap = targetMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableVisibleVersionMap()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                .map(entry -> {
                    OptionalLong maxVersion = entry.getValue().values().stream()
                            .mapToLong(MaterializedView.BasePartitionInfo::getVersion)
                            .max();
                    if (maxVersion.isEmpty()) {
                        return null;
                    }
                    return Map.entry(entry.getKey(), maxVersion.getAsLong());
                })
                .filter(entry -> entry != null && entry.getValue() > 0L)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (baseTableRefreshVersionMap.isEmpty()) {
            return root;
        }

        return new BaseTableVersionBinder(baseTableRefreshVersionMap).visit(root, null);
    }

    private static MaterializedView loadTargetMv(OptimizerContext optimizerContext) {
        MvId mvId = parseTargetMvId(optimizerContext);
        if (mvId == null) {
            return null;
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        if (db == null) {
            return null;
        }
        if (!(db.getTable(mvId.getId()) instanceof MaterializedView mv)) {
            return null;
        }
        return mv;
    }

    private static boolean isMvRefreshPlan(OptimizerContext optimizerContext) {
        StatementBase statement = optimizerContext.getStatement();
        if (!(statement instanceof InsertStmt insertStmt)) {
            return false;
        }
        if (!insertStmt.isSystem()) {
            return false;
        }
        if (!(insertStmt.getTargetTable() instanceof MaterializedView targetMv)) {
            return false;
        }
        MvId targetMvId = parseTargetMvId(optimizerContext);
        if (targetMvId == null) {
            return false;
        }
        return targetMvId.getDbId() == targetMv.getDbId() && targetMvId.getId() == targetMv.getId();
    }

    private static boolean isPrimaryKeyTargetMv(OptimizerContext optimizerContext) {
        StatementBase statement = optimizerContext.getStatement();
        if (!(statement instanceof InsertStmt insertStmt)) {
            return false;
        }
        if (!(insertStmt.getTargetTable() instanceof MaterializedView targetMv)) {
            return false;
        }
        return targetMv.getKeysType() == KeysType.PRIMARY_KEYS;
    }

    private static OptExpression appendPkLoadOpColumn(OptExpression root, TaskContext rootTaskContext,
                                                      ColumnRefSet requiredColumns, MaterializedView targetMv,
                                                      Map<ColumnRefOperator, Column> mvColumnMapping) {
        ColumnRefOperator actionColumn = IvmRuleUtils.findActionColumn(root).orElse(null);
        if (actionColumn == null) {
            return root;
        }

        List<ColumnRefOperator> rootOutputColumns = root.getOutputColumns()
                .getColumnRefOperators(rootTaskContext.getOptimizerContext().getColumnRefFactory());
        boolean hasLoadOpColumn = rootOutputColumns.stream()
                .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()));
        if (hasLoadOpColumn) {
            return root;
        }

        ColumnRefOperator loadOpColumn = rootTaskContext.getOptimizerContext().getColumnRefFactory()
                .create(Load.LOAD_OP_COLUMN, IntegerType.TINYINT, false);
        ScalarOperator isDeleteAction = new BinaryPredicateOperator(BinaryType.LT, actionColumn,
                ConstantOperator.createTinyInt((byte) 0));
        ScalarOperator loadOpExpr = new CaseWhenOperator(IntegerType.TINYINT, null,
                ConstantOperator.createTinyInt((byte) 0),
                Lists.newArrayList(isDeleteAction, ConstantOperator.createTinyInt((byte) 1)));

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn)) {
                projectMap.put(outputColumn, outputColumn);
            }
        }
        projectMap.put(loadOpColumn, loadOpExpr);
        List<ColumnRefOperator> shuffleColumns = deriveTargetMvPkShuffleColumns(targetMv, mvColumnMapping);

        requiredColumns.union(loadOpColumn);
        rootTaskContext.getRequiredColumns().union(loadOpColumn);

        OptExpression projectExpr = OptExpression.create(new LogicalProjectOperator(projectMap), root);
        // DELETE must come first: __op=1 for DELETE, __op=0 for INSERT.
        List<Ordering> orderings = List.of(new Ordering(loadOpColumn, false, false));
        LogicalTopNOperator.Builder topNBuilder = LogicalTopNOperator.builder()
                .withOperator(
                        new LogicalTopNOperator(orderings, Operator.DEFAULT_LIMIT, Operator.DEFAULT_OFFSET, SortPhase.PARTIAL))
                .setOrderByElements(orderings)
                .setPerPipeline(true)
                .setShuffleColumns(shuffleColumns);
        LogicalTopNOperator topN = topNBuilder.build();
        return OptExpression.create(topN, projectExpr);
    }

    static List<ColumnRefOperator> deriveTargetMvPkShuffleColumns(MaterializedView targetMv,
                                                                  Map<ColumnRefOperator, Column> mvColumnMapping) {
        if (targetMv == null || mvColumnMapping == null || mvColumnMapping.isEmpty()) {
            return Lists.newArrayList();
        }

        List<ColumnRefOperator> pkShuffleColumns = Lists.newArrayList();
        for (Column keyColumn : targetMv.getKeyColumnsInOrder()) {
            ColumnRefOperator keyColumnRef = findMappedColumnRef(mvColumnMapping, keyColumn);
            if (keyColumnRef == null) {
                return Lists.newArrayList();
            }
            pkShuffleColumns.add(keyColumnRef);
        }
        return pkShuffleColumns;
    }

    private static ColumnRefOperator findMappedColumnRef(Map<ColumnRefOperator, Column> mvColumnMapping, Column targetColumn) {
        for (Map.Entry<ColumnRefOperator, Column> entry : mvColumnMapping.entrySet()) {
            Column mappedColumn = entry.getValue();
            if (mappedColumn != null && mappedColumn.getColumnId().equals(targetColumn.getColumnId())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private static MvId parseTargetMvId(OptimizerContext optimizerContext) {
        String strMvId = optimizerContext.getSessionVariable().getTvrTargetMvId();
        if (Strings.isNullOrEmpty(strMvId)) {
            return null;
        }
        try {
            return GsonUtils.GSON.fromJson(strMvId, MvId.class);
        } catch (JsonSyntaxException e) {
            return null;
        }
    }

    private static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }
        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

    private static Map<ColumnRefOperator, Column> buildMvColumnMapping(
            OptExpression root, OptimizerContext optimizerContext, MaterializedView targetMv) {
        List<ColumnRefOperator> outputColumns = root.getOutputColumns()
                .getColumnRefOperators(optimizerContext.getColumnRefFactory());
        List<Column> orderedOutputColumns = targetMv.getOrderedOutputColumns(true);
        if (outputColumns.size() != orderedOutputColumns.size()) {
            return Maps.newHashMap();
        }
        Map<ColumnRefOperator, Column> mapping = Maps.newHashMapWithExpectedSize(outputColumns.size());
        for (int i = 0; i < outputColumns.size(); i++) {
            mapping.put(outputColumns.get(i), orderedOutputColumns.get(i));
        }
        return mapping;
    }

    private static class BaseTableVersionBinder extends OptExpressionVisitor<OptExpression, Void> {
        private final Map<Long, Long> baseTableRefreshVersionMap;

        private BaseTableVersionBinder(Map<Long, Long> baseTableRefreshVersionMap) {
            this.baseTableRefreshVersionMap = baseTableRefreshVersionMap;
        }

        @Override
        public OptExpression visit(OptExpression expression, Void context) {
            List<OptExpression> rewrittenChildren = Lists.newArrayListWithCapacity(expression.getInputs().size());
            boolean hasChildChanged = false;
            for (OptExpression child : expression.getInputs()) {
                OptExpression rewrittenChild = child.getOp().accept(this, child, null);
                rewrittenChildren.add(rewrittenChild);
                hasChildChanged = hasChildChanged || rewrittenChild != child;
            }
            if (!hasChildChanged) {
                return expression;
            }
            return OptExpression.create(expression.getOp(), expression.getTvrMeta(), rewrittenChildren);
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression expression, Void context) {
            if (!(expression.getOp() instanceof LogicalOlapScanOperator scan)) {
                return expression;
            }
            Long fromVersion = baseTableRefreshVersionMap.get(scan.getTable().getId());
            if (fromVersion == null || fromVersion.equals(scan.getTableVersion())) {
                return expression;
            }
            LogicalOlapScanOperator rewrittenScan = LogicalOlapScanOperator.builder()
                    .withOperator(scan)
                    .setTableVersion(fromVersion)
                    .build();
            return OptExpression.create(rewrittenScan);
        }
    }
}
