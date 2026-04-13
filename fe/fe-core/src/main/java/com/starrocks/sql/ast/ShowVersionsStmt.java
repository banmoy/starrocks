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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.parser.NodePosition;

public class ShowVersionsStmt extends ShowStmt {
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName")
            .add("TableName")
            .add("PartitionName")
            .add("PhysicalPartitionId")
            .add("VisibleVersion")
            .add("BaseVersion")
            .build();

    private final TableName tableName;
    private Table table;

    public ShowVersionsStmt(TableName tableName, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public ImmutableList<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowVersionsStatement(this, context);
    }
}
