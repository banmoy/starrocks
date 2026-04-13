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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowVersionsStmt;

public class ShowVersionsStmtAnalyzer {
    public static void analyze(ShowVersionsStmt statement, ConnectContext context) {
        TableName tableName = statement.getTableName();
        tableName.normalization(context);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (!(table instanceof OlapTable)) {
            throw new SemanticException("Only support OlapTable for SHOW VERSIONS");
        }
        statement.setTable(table);
    }
}
