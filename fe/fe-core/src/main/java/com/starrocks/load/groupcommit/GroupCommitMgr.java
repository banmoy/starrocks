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

package com.starrocks.load.groupcommit;

import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GroupCommitMgr {

    private static final Logger LOG = LogManager.getLogger(GroupCommitMgr.class);

    private final ReentrantReadWriteLock lock;
    private final Map<TableId, TableGroupCommit> tableGroupCommitMap;
    private final ThreadPoolExecutor threadPoolExecutor;

    public GroupCommitMgr() {
        this.lock = new ReentrantReadWriteLock();
        this.tableGroupCommitMap = new ConcurrentHashMap<>();
        this.threadPoolExecutor = ThreadPoolManager
                .newDaemonCacheThreadPool(Config.group_commit_executor_threads_num, "group-commit-pool", true);
    }

    public TNetworkAddress getRedirectBe(String db, String table, HttpHeaders headers) {
        TableGroupCommit tableGroupCommit = getOrCreateTableGroupCommit(new TableId(db, table), headers);
        TNetworkAddress address = tableGroupCommit.getRedirectBe();
        LOG.debug("Redirect stream load for group commit, db: {}, table: {}, address: {}", db, table, address);
        return address;
    }

    public void notifyBeData(String dbName, String tableName, String beHost, String userLabel) {
        LOG.debug("Receive group commit notify, db: {}, table: {}, be: {}, userLabel: {}",
                dbName, tableName, beHost, userLabel);
        TableGroupCommit tableGroupCommit = getOrCreateTableGroupCommit(new TableId(dbName, tableName), null);
        if (tableGroupCommit != null) {
            tableGroupCommit.notifyBeData(beHost, userLabel);
        }
    }

    private TableGroupCommit getOrCreateTableGroupCommit(TableId tableId, HttpHeaders headers) {
        TableGroupCommit tableGroupCommit = tableGroupCommitMap.get(tableId);
        if (tableGroupCommit != null) {
            return tableGroupCommit;
        }
        if (headers == null) {
            return null;
        }

        lock.writeLock().lock();
        try {
            tableGroupCommit = tableGroupCommitMap.get(tableId);
            if (tableGroupCommit == null) {
                tableGroupCommit = new TableGroupCommit(tableId, headers, threadPoolExecutor);
                tableGroupCommit.init();
                tableGroupCommitMap.put(tableId, tableGroupCommit);
                LOG.info("Create table group commit, db: {}, table: {}", tableId.getDbName(), tableId.getTableName());
            }
        } finally {
            lock.writeLock().unlock();
        }
        return tableGroupCommit;
    }
}
