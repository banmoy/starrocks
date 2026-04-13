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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.Expr2SQLPrinter;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// translate logical tree plan to SQL
public class LogicalPlan2SQLBuilder {
    private long tableId = 0;
    private long syntheticColumnId = -1;

    private final ExprSQLBuilder exprSQLBuilder = new ExprSQLBuilder();

    private final SQLBuilder planSQLBuilder = new SQLBuilder();

    public String toSQL(LogicalPlan logicalPlan) {
        return toSQL(logicalPlan.getRoot(), logicalPlan.getOutputColumn());
    }

    public String toSQL(OptExpression plan, List<ColumnRefOperator> outputColumn) {
        SQLRelation relation = plan.getOp().accept(planSQLBuilder, plan, null);
        List<Integer> outputIds = outputColumn.stream().map(ColumnRefOperator::getId).toList();
        List<Integer> selectIds = relation.selects.stream().map(Pair::getKey).toList();
        if (CollectionUtils.isEmpty(selectIds) || outputIds.equals(selectIds)) {
            return relation.toSQL(outputColumn);
        } else {
            String f = relation.toSQL();
            String select = outputColumn.stream()
                    .map(c -> relation.columnNames.get(c.getId()))
                    .collect(Collectors.joining(", "));
            return "SELECT " + select + " FROM (" + f + ") t" + (tableId++);
        }
    }

    private class SQLRelation {
        private final Map<Integer, String> columnNames = Maps.newHashMap();
        private List<String> cte = null;
        private List<Pair<Integer, String>> selects = List.of();
        private String from = "";
        private String where = "";
        private String groupBy = "";
        private String having = "";
        private String orderBy = "";
        private String limit = "";
        private String groupings = "";
        private String relationName = null;
        private List<String> reserveNames = null;
        private boolean assertRows = false;

        private String registerRef(Integer cid, String alias) {
            columnNames.put(cid, alias);
            return alias;
        }

        private String registerRef(Integer cid) {
            String ref = "c_" + cid;
            columnNames.put(cid, ref);
            return ref;
        }

        private String newAlias() {
            relationName = "t_" + (tableId++);
            return relationName;
        }

        private String getRelationAlias() {
            return relationName == null ? from : relationName;
        }

        private String toRelationSQL() {
            if (relationName == null) {
                return from;
            }
            if (assertRows) {
                return "ASSERT_ROWS (" + toSQL() + ") " + relationName;
            }
            return "(" + toSQL() + ") " + relationName;
        }

        private String toSQL() {
            return toSQL(Collections.emptyList());
        }

        private String toSQL(List<ColumnRefOperator> outputs) {
            StringBuilder sql = new StringBuilder();
            if (!CollectionUtils.isEmpty(cte)) {
                sql.append("WITH ");
                sql.append(String.join(", ", cte));
                sql.append(" ");
            }
            sql.append("SELECT ");
            if (CollectionUtils.isEmpty(outputs)) {
                sql.append(CollectionUtils.isEmpty(selects) ? "*" :
                        selects.stream().map(Pair::getValue).collect(Collectors.joining(", ")));
            } else {
                Map<Integer, String> temp = CollectionUtils.isEmpty(selects) ?
                        columnNames :
                        selects.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                sql.append(outputs.stream().map(p -> temp.get(p.getId())).collect(Collectors.joining(", ")));
            }

            sql.append(" FROM ");
            sql.append(from);
            if (!StringUtils.isBlank(where)) {
                sql.append(" WHERE ");
                sql.append(where);
            }
            if (!StringUtils.isBlank(groupBy)) {
                sql.append(" GROUP BY ");
                sql.append(groupBy);
            }
            if (!StringUtils.isBlank(having)) {
                sql.append(" HAVING ");
                sql.append(having);
            }
            if (!StringUtils.isBlank(orderBy)) {
                sql.append(" ORDER BY ");
                sql.append(orderBy);
            }
            if (!StringUtils.isBlank(limit)) {
                sql.append(" LIMIT ");
                sql.append(limit);
            }
            return sql.toString();
        }
    }

    private class SQLBuilder extends OptExpressionVisitor<SQLRelation, Void> {
        private final Map<Integer, String> cteColumnNames = Maps.newHashMap();
        private final Map<Integer, String> cteRelationNames = Maps.newHashMap();

        @Override
        public SQLRelation visit(OptExpression optExpression, Void context) {
            UnsupportedException.unsupportedException(
                    "LogicalPlan2SQLBuilder doesn't support: " + optExpression.getOp().getOpType());
            return null;
        }

        public SQLRelation process(OptExpression optExpression) {
            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public SQLRelation visitLogicalJoin(OptExpression optExpression, Void context) {
            SQLRelation left = process(optExpression.getInputs().get(0));
            SQLRelation right = process(optExpression.getInputs().get(1));

            SQLRelation joinRelation = new SQLRelation();
            LogicalJoinOperator join = optExpression.getOp().cast();

            if (StringUtils.equalsIgnoreCase(left.getRelationAlias(), right.getRelationAlias())) {
                left.newAlias();
                right.newAlias();
            }

            boolean columnConflicts = !Collections.disjoint(left.columnNames.values(), right.columnNames.values());
            if (!columnConflicts && left.reserveNames != null) {
                columnConflicts = !Collections.disjoint(left.reserveNames, right.columnNames.values());
            }
            if (!columnConflicts && right.reserveNames != null) {
                columnConflicts = !Collections.disjoint(right.reserveNames, left.columnNames.values());
            }

            if (columnConflicts) {
                left.columnNames.forEach((k, v) -> joinRelation.columnNames.put(k, left.getRelationAlias() + "." + v));
                right.columnNames.forEach(
                        (k, v) -> joinRelation.columnNames.put(k, right.getRelationAlias() + "." + v));
            } else {
                joinRelation.columnNames.putAll(left.columnNames);
                joinRelation.columnNames.putAll(right.columnNames);
            }
            joinRelation.from = left.toRelationSQL() + " " + join.getJoinType() + " " + right.toRelationSQL();
            if (join.getOnPredicate() != null) {
                joinRelation.from += " ON " + exprSQLBuilder.print(join.getOnPredicate(), joinRelation);
            }
            joinRelation.where = exprSQLBuilder.print(join.getPredicate(), joinRelation);

            if (join.getProjection() != null) {
                List<Integer> forces = columnConflicts ? Lists.newArrayList(joinRelation.columnNames.keySet()) :
                        Collections.emptyList();
                visitProjection(join.getProjection().getColumnRefMap(), joinRelation, forces);
            } else if (columnConflicts) {
                List<Pair<Integer, String>> selects = Lists.newArrayList();
                for (Integer key : joinRelation.columnNames.keySet()) {
                    selects.add(Pair.of(key,
                            joinRelation.columnNames.get(key) + " AS " + joinRelation.registerRef(key)));
                }
                joinRelation.selects = selects;
            }
            joinRelation.newAlias();
            return joinRelation;
        }

        @Override
        public SQLRelation visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator agg = optExpression.getOp().cast();
            SQLRelation childRelation = process(optExpression.inputAt(0));
            if (agg.getType().isLocal() || agg.getType().isDistinctLocal()) {
                for (var entry : agg.getAggregations().entrySet()) {
                    ColumnRefOperator key = entry.getKey();
                    CallOperator aggFn = entry.getValue();
                    String aggFnStr = exprSQLBuilder.print(aggFn, childRelation);
                    childRelation.registerRef(key.getId(), aggFnStr);
                }
                return childRelation;
            }

            SQLRelation aggRelation;
            List<Integer> aliasIds = Lists.newArrayList();
            List<Pair<Integer, String>> selects = Lists.newArrayList();

            if (!StringUtils.isEmpty(childRelation.groupings)) {
                aggRelation = childRelation;
                for (ColumnRefOperator groupBy : agg.getGroupingKeys()) {
                    aliasIds.add(groupBy.getId());
                    if (childRelation.columnNames.containsKey(groupBy.getId())) {
                        String groupByExpr = exprSQLBuilder.print(groupBy, childRelation);
                        String alias = aggRelation.registerRef(groupBy.getId());
                        selects.add(Pair.of(groupBy.getId(), groupByExpr + " AS " + alias));
                    }
                }
                aggRelation.groupBy = aggRelation.groupings;
            } else {
                aggRelation = new SQLRelation();
                aggRelation.from = childRelation.toRelationSQL();
                for (ColumnRefOperator groupBy : agg.getGroupingKeys()) {
                    Preconditions.checkState(childRelation.columnNames.containsKey(groupBy.getId()));
                    String groupByExpr = exprSQLBuilder.print(groupBy, childRelation);
                    String alias = aggRelation.registerRef(groupBy.getId());
                    selects.add(Pair.of(groupBy.getId(), groupByExpr + " AS " + alias));
                }
                aggRelation.groupBy = agg.getGroupingKeys().stream()
                        .map(groupBy -> exprSQLBuilder.print(groupBy, childRelation))
                        .collect(Collectors.joining(", "));
            }

            for (var entry : agg.getAggregations().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator aggFn = entry.getValue();
                String fn;
                if (childRelation.columnNames.containsKey(key.getId())) {
                    fn = exprSQLBuilder.print(key, childRelation);
                } else {
                    fn = exprSQLBuilder.print(aggFn, childRelation);
                }
                String alias = aggRelation.registerRef(key.getId());
                selects.add(Pair.of(key.getId(), fn + " AS " + alias));
                aliasIds.add(key.getId());
            }

            aggRelation.having = exprSQLBuilder.print(agg.getPredicate(), aggRelation);
            aggRelation.selects = selects;
            if (agg.getProjection() != null) {
                visitProjection(agg.getProjection().getColumnRefMap(), aggRelation, aliasIds);
            }
            aggRelation.newAlias();
            return aggRelation;
        }

        @Override
        public SQLRelation visitLogicalValues(OptExpression optExpression, Void context) {
            LogicalValuesOperator values = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = values.getRows().stream()
                    .map(l -> "(" + l.stream().map(v -> exprSQLBuilder.print(v, relation))
                            .collect(Collectors.joining(", ")) + ")").collect(Collectors.joining(", "));
            relation.from = "(VALUES " + relation.from + ") AS t";
            relation.from += "(" + values.getColumnRefSet().stream().map(c -> relation.registerRef(c.getId()))
                    .collect(Collectors.joining(", ")) + ")";

            if (values.getRows() != null) {
                if (values.getProjection() != null) {
                    visitProjection(values.getProjection().getColumnRefMap(), relation, Collections.emptyList());
                }
                relation.newAlias();
            }
            return relation;
        }

        private SQLRelation visitLogicalSet(OptExpression optExpression, String op) {
            LogicalSetOperator set = optExpression.getOp().cast();
            SQLRelation setRelation = new SQLRelation();

            set.getOutputColumnRefOp().forEach(c -> setRelation.registerRef(c.getId()));
            List<String> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                OptExpression child = optExpression.inputAt(i);
                SQLRelation relation = process(child);
                String childSQL = "";

                List<ColumnRefOperator> childOutputs = set.getChildOutputColumns().get(i);
                List<Pair<Integer, String>> childSelects = Lists.newArrayList();
                for (int index = 0; index < childOutputs.size(); index++) {
                    String alias = relation.columnNames.get(childOutputs.get(index).getId()) + " AS "
                            + setRelation.columnNames.get(set.getOutputColumnRefOp().get(index).getId());
                    childSelects.add(Pair.of(childOutputs.get(index).getId(), alias));
                }

                if (CollectionUtils.isEmpty(relation.selects)) {
                    relation.selects = childSelects;
                    childSQL += relation.toSQL();
                } else {
                    childSQL += "SELECT ";
                    childSQL += childSelects.stream().map(Pair::getValue).collect(Collectors.joining(", "));
                    childSQL += " FROM ";
                    childSQL += relation.toRelationSQL();
                }
                children.add(childSQL);
            }
            setRelation.from = "(" + String.join(" " + op + " ", children) + ") " + setRelation.newAlias();
            setRelation.newAlias();
            return setRelation;
        }

        @Override
        public SQLRelation visitLogicalUnion(OptExpression optExpression, Void context) {
            LogicalUnionOperator union = optExpression.getOp().cast();
            return visitLogicalSet(optExpression, union.isUnionAll() ? "UNION ALL" : "UNION");
        }

        @Override
        public SQLRelation visitLogicalExcept(OptExpression optExpression, Void context) {
            return visitLogicalSet(optExpression, "EXCEPT");
        }

        @Override
        public SQLRelation visitLogicalIntersect(OptExpression optExpression, Void context) {
            return visitLogicalSet(optExpression, "INTERSECT");
        }

        @Override
        public SQLRelation visitLogicalTableFunction(OptExpression optExpression, Void context) {
            LogicalTableFunctionOperator tableFunction = optExpression.getOp().cast();
            SQLRelation child = optExpression.getInputs().isEmpty() ? buildDummyRelation() : process(optExpression.inputAt(0));

            SQLRelation result = new SQLRelation();
            StringBuilder sb = new StringBuilder();
            sb.append(child.toRelationSQL()).append(", ");
            sb.append(tableFunction.getFn().functionName());
            sb.append("(").append(tableFunction.getFnParamColumnProject().stream()
                    .map(p -> exprSQLBuilder.print(p.second, child))
                    .collect(Collectors.joining(", "))).append(")");
            sb.append(" AS ").append(result.newAlias());
            sb.append("(").append(tableFunction.getFnResultColRefs().stream()
                    .map(ColumnRefOperator::getName)
                    .collect(Collectors.joining(", "))).append(")");

            result.from = sb.toString();
            result.columnNames.putAll(child.columnNames);
            for (ColumnRefOperator ref : tableFunction.getFnResultColRefs()) {
                result.columnNames.put(ref.getId(), result.registerRef(ref.getId(), ref.getName()));
            }
            return result;
        }

        @Override
        public SQLRelation visitLogicalLimit(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            LogicalLimitOperator limit = optExpression.getOp().cast();
            if (limit.isLocal()) {
                return child;
            }
            SQLRelation limitRelation;
            if (StringUtils.isEmpty(child.limit)) {
                limitRelation = child;
            } else {
                limitRelation = new SQLRelation();
                limitRelation.from = child.toRelationSQL();
                limitRelation.columnNames.putAll(child.columnNames);
                limitRelation.newAlias();
            }

            limitRelation.limit = limit.hasOffset() ? limit.getOffset() + ", " : "";
            limitRelation.limit += limit.getLimit();
            return limitRelation;
        }

        @Override
        public SQLRelation visitLogicalTopN(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            LogicalTopNOperator topN = optExpression.getOp().cast();
            if (!CollectionUtils.isEmpty(topN.getPartitionByColumns()) || !StringUtils.isEmpty(child.groupings)
                    || topN.getSortPhase() == SortPhase.PARTIAL) {
                return child;
            }

            SQLRelation relation;
            if (StringUtils.isEmpty(child.limit) && StringUtils.isEmpty(child.orderBy)) {
                relation = child;
            } else {
                relation = new SQLRelation();
                relation.from = child.toRelationSQL();
                relation.columnNames.putAll(child.columnNames);
                relation.newAlias();
            }

            relation.limit = topN.getOffset() > 0 ? topN.getOffset() + ", " : "";
            relation.limit += topN.hasLimit() ? topN.getLimit() : "";
            List<String> orderBys = Lists.newArrayList();
            for (Ordering orderDesc : topN.getOrderByElements()) {
                orderBys.add(exprSQLBuilder.print(orderDesc.getColumnRef(), relation) + " " + (orderDesc.isAscending() ?
                        "ASC" : "DESC"));
            }
            relation.orderBy = String.join(", ", orderBys);
            return relation;
        }

        @Override
        public SQLRelation visitLogicalAssertOneRow(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            LogicalAssertOneRowOperator assertOneRow = optExpression.getOp().cast();
            if (assertOneRow.getAssertion() != null && assertOneRow.getCheckRows() == 1) {
                Preconditions.checkState(child.columnNames.size() == 1);
                child.assertRows = true;
                if (CollectionUtils.isEmpty(child.selects)) {
                    List<Pair<Integer, String>> selects = Lists.newArrayList();
                    child.columnNames.forEach((k, v) -> selects.add(Pair.of(k, v)));
                    child.selects = selects;
                }
                child.newAlias();
                return child;
            }
            UnsupportedException.unsupportedException("LogicalPlan2SQLBuilder only supports ASSERT_ROWS <= 1");
            return null;
        }

        @Override
        public SQLRelation visitLogicalFilter(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            LogicalFilterOperator filter = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();
            relation.where = exprSQLBuilder.print(filter.getPredicate(), child);
            relation.columnNames.putAll(child.columnNames);
            if (filter.getProjection() != null) {
                visitProjection(filter.getProjection().getColumnRefMap(), relation, Collections.emptyList());
            }
            relation.newAlias();
            return relation;
        }

        @Override
        public SQLRelation visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            LogicalCTEAnchorOperator anchor = optExpression.getOp().cast();

            SQLRelation produce = process(optExpression.getInputs().get(0));
            produce.newAlias();
            cteRelationNames.put(anchor.getCteId(), produce.getRelationAlias());
            cteColumnNames.putAll(produce.columnNames);

            SQLRelation consume = process(optExpression.getInputs().get(1));
            if (consume.cte == null) {
                consume.cte = Lists.newArrayList();
            }
            consume.cte.add(produce.getRelationAlias() + " AS (" + produce.toSQL() + ")");
            return consume;
        }

        @Override
        public SQLRelation visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            LogicalCTEConsumeOperator consume = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = cteRelationNames.get(consume.getCteId());
            consume.getCteOutputColumnRefMap()
                    .forEach((k, v) -> relation.registerRef(k.getId(), cteColumnNames.get(v.getId())));
            relation.where = exprSQLBuilder.print(consume.getPredicate(), relation);
            if (consume.getProjection() != null) {
                visitProjection(consume.getProjection().getColumnRefMap(), relation, Collections.emptyList());
            }
            if (consume.getPredicate() != null || consume.getProjection() != null) {
                relation.newAlias();
            }
            return relation;
        }

        @Override
        public SQLRelation visitLogicalCTEProduce(OptExpression optExpression, Void context) {
            return process(optExpression.getInputs().get(0));
        }

        @Override
        public SQLRelation visitLogicalProject(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.inputAt(0));
            LogicalProjectOperator project = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();
            relation.columnNames.putAll(child.columnNames);
            visitProjection(project.getColumnRefMap(), relation, Collections.emptyList());
            relation.newAlias();
            return relation;
        }

        private void visitProjection(Map<ColumnRefOperator, ScalarOperator> project,
                                     SQLRelation relation,
                                     List<Integer> forceAlias) {
            if (project == null) {
                if (CollectionUtils.isEmpty(forceAlias)) {
                    return;
                }
                List<Pair<Integer, String>> selects = Lists.newArrayList();
                for (Integer key : relation.columnNames.keySet()) {
                    String v = relation.columnNames.get(key);
                    if (forceAlias.contains(key)) {
                        selects.add(Pair.of(key, v + " AS " + relation.registerRef(key)));
                    } else {
                        selects.add(Pair.of(key, v));
                    }
                }
                relation.selects = selects;
                return;
            }

            List<Integer> saveColumnIds = Lists.newArrayList();
            List<String> selects = Lists.newArrayList();
            List<String> alias = Lists.newArrayList();

            project.forEach((k, v) -> {
                saveColumnIds.add(k.getId());
                selects.add(exprSQLBuilder.print(v, relation));
            });

            project.forEach((k, v) -> {
                if (!k.equals(v) || forceAlias.contains(k.getId())) {
                    alias.add(" AS " + relation.registerRef(k.getId()));
                } else {
                    alias.add("");
                }
            });

            Preconditions.checkState(saveColumnIds.size() == selects.size());
            Preconditions.checkState(saveColumnIds.size() == alias.size());

            relation.selects = Lists.newArrayList();
            for (int i = 0; i < saveColumnIds.size(); i++) {
                relation.selects.add(Pair.of(saveColumnIds.get(i), selects.get(i) + alias.get(i)));
            }

            relation.columnNames.entrySet().removeIf(e -> !saveColumnIds.contains(e.getKey()));
        }

        @Override
        public SQLRelation visitLogicalWindow(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.inputAt(0));
            LogicalWindowOperator window = optExpression.getOp().cast();

            child.orderBy = "";

            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();

            String frame = "";
            if (!CollectionUtils.isEmpty(window.getPartitionExpressions())) {
                frame += "PARTITION BY " + window.getPartitionExpressions().stream()
                        .map(p -> exprSQLBuilder.print(p, child)).collect(Collectors.joining(", "));
                frame += " ";
            }
            if (!CollectionUtils.isEmpty(window.getOrderByElements())) {
                frame += "ORDER BY " + window.getOrderByElements().stream()
                        .map(o -> exprSQLBuilder.print(o.getColumnRef(), child) + " " + (o.isAscending() ? "ASC" :
                                "DESC")).collect(Collectors.joining(", "));
                frame += " ";
            }
            if (window.getAnalyticWindow() != null && !AnalyticWindow.DEFAULT_WINDOW.equals(
                    window.getAnalyticWindow())) {
                frame += ExprToSql.toSql(window.getAnalyticWindow());
            }
            frame = " OVER (" + frame + ")";

            List<Integer> analytics = Lists.newArrayList();
            relation.columnNames.putAll(child.columnNames);
            relation.selects = Lists.newArrayList();
            child.columnNames.forEach((k, v) -> relation.selects.add(Pair.of(k, v)));
            for (var entry : window.getWindowCall().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator value = entry.getValue();
                String analyticExpr = exprSQLBuilder.print(value, child) + maybeStripWindowFrame(value, frame);
                String alias = relation.registerRef(key.getId());
                relation.selects.add(Pair.of(key.getId(), analyticExpr + " AS " + alias));
                analytics.add(key.getId());
            }

            if (window.getProjection() != null) {
                visitProjection(window.getProjection().getColumnRefMap(), relation, analytics);
            }
            relation.newAlias();
            return relation;
        }

        @Override
        public SQLRelation visitLogicalRepeat(OptExpression optExpression, Void context) {
            SQLRelation relation = process(optExpression.getInputs().get(0));
            LogicalRepeatOperator repeat = optExpression.getOp().cast();

            SQLRelation groupingRelation = new SQLRelation();
            groupingRelation.from = relation.toRelationSQL();
            groupingRelation.columnNames.putAll(relation.columnNames);
            for (ColumnRefOperator grouping : repeat.getOutputGrouping()) {
                if ("GROUPING_ID".equals(grouping.getName())) {
                    continue;
                }
                Preconditions.checkState("GROUPING".equals(grouping.getName()));
                Preconditions.checkState(repeat.getGroupingsFnArgs().containsKey(grouping));
                String fn = "GROUPING(" + repeat.getGroupingsFnArgs().get(grouping).stream()
                        .map(p -> exprSQLBuilder.print(p, relation)).collect(Collectors.joining(", ")) + ")";
                groupingRelation.registerRef(grouping.getId(), fn);
            }
            List<String> groupings = Lists.newArrayList();
            for (var group : repeat.getRepeatColumnRef()) {
                groupings.add("(" + group.stream().map(c -> exprSQLBuilder.print(c, relation))
                        .collect(Collectors.joining(", ")) + ")");
            }
            groupingRelation.groupings = "GROUPING SETS(" + String.join(", ", groupings) + ")";
            return groupingRelation;
        }

        @Override
        public SQLRelation visitLogicalTableScan(OptExpression optExpression, Void context) {
            SQLRelation relation = new SQLRelation();
            LogicalScanOperator scan = optExpression.getOp().cast();
            relation.from = scan.getTable().getName();
            scan.getColRefToColumnMetaMap().forEach((k, v) -> relation.registerRef(k.getId(), v.getName()));
            relation.where = exprSQLBuilder.print(scan.getPredicate(), relation);
            if (scan.getProjection() != null) {
                visitProjection(scan.getProjection().getColumnRefMap(), relation, Collections.emptyList());
            }

            if (scan.getPredicate() != null || scan.getProjection() != null) {
                relation.newAlias();
                return relation;
            }
            relation.reserveNames = Lists.newArrayList();
            scan.getTable().getColumns().forEach(c -> relation.reserveNames.add(c.getName()));
            return relation;
        }

        private SQLRelation buildDummyRelation() {
            SQLRelation relation = new SQLRelation();
            int cid = (int) syntheticColumnId--;
            relation.from = "(VALUES (null)) AS t(" + relation.registerRef(cid) + ")";
            relation.newAlias();
            return relation;
        }

        private String maybeStripWindowFrame(CallOperator call, String frame) {
            String fn = call.getFnName();
            if (fn.equalsIgnoreCase("row_number")
                    || fn.equalsIgnoreCase("rank")
                    || fn.equalsIgnoreCase("dense_rank")
                    || fn.equalsIgnoreCase("cume_dist")
                    || fn.equalsIgnoreCase("percent_rank")
                    || fn.equalsIgnoreCase("ntile")) {
                int frameIndex = frame.indexOf("ROWS ");
                if (frameIndex > 0) {
                    return frame.substring(0, frameIndex).trim() + " )";
                }
            }
            return frame;
        }
    }

    private static class ExprSQLBuilder extends Expr2SQLPrinter<SQLRelation> {
        @Override
        public String print(ScalarOperator scalarOperator) {
            UnsupportedException.unsupportedException("LogicalPlan2SQLBuilder doesn't support: " + scalarOperator);
            return null;
        }

        @Override
        public String print(ScalarOperator scalarOperator, SQLRelation context) {
            if (scalarOperator == null) {
                return "";
            }
            return super.print(scalarOperator, context);
        }

        @Override
        public String visitVariableReference(ColumnRefOperator variable, SQLRelation context) {
            return context.columnNames.get(variable.getId());
        }

        @Override
        public String visitConstant(ConstantOperator literal, SQLRelation context) {
            return super.visitConstant(literal, context);
        }

        @Override
        public String visitCall(CallOperator call, SQLRelation context) {
            if (SPMFunctions.isSPMFunctions(call)) {
                List<String> children =
                        call.getChildren().stream().map(c -> print(c, context)).collect(Collectors.toList());
                return SPMFunctions.toSQL(call.getFnName(), children);
            }
            return super.visitCall(call, context);
        }
    }
}
