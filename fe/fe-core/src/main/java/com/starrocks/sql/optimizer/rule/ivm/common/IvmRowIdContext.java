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

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IvmRowIdContext {
    private final ColumnRefFactory columnRefFactory;
    private final Map<OptExpression, List<ColumnRefOperator>> rowIdsByExpr = Maps.newIdentityHashMap();
    private String unsupportedReason;

    public IvmRowIdContext(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public void putRowIds(OptExpression expression, List<ColumnRefOperator> rowIds) {
        rowIdsByExpr.put(expression, List.copyOf(rowIds));
    }

    public Optional<List<ColumnRefOperator>> getRowIds(OptExpression expression) {
        return Optional.ofNullable(rowIdsByExpr.get(expression));
    }

    public void markUnsupported(String reason) {
        if (unsupportedReason == null) {
            unsupportedReason = reason;
        }
    }

    public boolean isSupported() {
        return unsupportedReason == null;
    }

    public Optional<String> getUnsupportedReason() {
        return Optional.ofNullable(unsupportedReason);
    }
}
