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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.LoadException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GroupCommitLoadExecutor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitLoadExecutor.class);

    // Initialized in constructor ==================================
    private final TableId tableId;
    private final String label;
    private final StreamLoadKvParams params;
    private final ConnectContext connectContext;
    private final Set<Long> coordinatorBeIds;
    private final int groupCommitIntervalMs;
    private final LoadExecuteCallback loadExecuteCallback;

    private final TimeTrace timeTrace;

    // Initialized in prepare() ==================================
    private long dbId;
    private long tblId;
    private StreamLoadInfo streamLoadInfo;

    // Initialized in beginTxn() ==================================
    private long txnId = -1;

    // Initialized in executeLoad() ==================================
    private List<TabletCommitInfo> tabletCommitInfo;
    private List<TabletFailInfo> tabletFailInfo;

    private final AtomicBoolean failure = new AtomicBoolean(false);

    public GroupCommitLoadExecutor(
            TableId tableId,
            String label,
            StreamLoadKvParams params,
            ConnectContext connectContext,
            Set<Long> coordinatorBeIds,
            int groupCommitIntervalMs,
            LoadExecuteCallback loadExecuteCallback) {
        this.tableId = tableId;
        this.label = label;
        this.params = params;
        this.connectContext = connectContext;
        this.coordinatorBeIds = coordinatorBeIds;
        this.groupCommitIntervalMs = groupCommitIntervalMs;
        this.loadExecuteCallback = loadExecuteCallback;
        this.timeTrace = new TimeTrace();
    }

    @Override
    public void run() {
        try {
            timeTrace.beginRunTimeMs.set(System.currentTimeMillis());
            prepare();
            beginTxn();
            executeLoad();
            commitAndPublishTxn();
            loadExecuteCallback.finishLoad(label);
        } catch (Throwable e) {
            failure.set(true);
            abortTxn();
        } finally {
            timeTrace.finishTimeMs.set(System.currentTimeMillis());
            loadExecuteCallback.finishLoad(label);
        }
    }

    public String getLabel() {
        return label;
    }

    public boolean containCoordinatorBe(long backendId) {
        return coordinatorBeIds.contains(backendId);
    }

    public boolean isActive() {
        if (failure.get()) {
            return false;
        }
        long joinPlanTimeMs = timeTrace.joinPlanTimeMs.get();
        return joinPlanTimeMs <= 0 || (System.currentTimeMillis() - joinPlanTimeMs < groupCommitIntervalMs);
    }

    private void prepare() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new UserException(String.format("Database %s does not exist", tableId.getDbName()));
        }
        dbId = db.getId();

        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(dbId, LockType.READ);
        try {
            table = GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(db.getFullName(), tableId.getTableName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        if (table == null) {
            throw new UserException(String.format("Table %s does not exsit", tableId.getDbName()));
        }
        tblId = table.getId();

        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        // set txnId in beginTxn
        streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(loadId, -1, params);
    }

    private void beginTxn() throws Exception {
        timeTrace.beginTxnTimeMs.set(System.currentTimeMillis());
        txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                dbId, Lists.newArrayList(tblId), label,
                TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING,
                streamLoadInfo.getTimeout(), streamLoadInfo.getWarehouseId());
        streamLoadInfo.setTxnId(txnId);
    }

    private void commitAndPublishTxn() throws Exception {
        timeTrace.commitTxnTimeMs.set(System.currentTimeMillis());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new UserException(String.format("Database [%s] does not exist when commit transaction", tableId.getDbName()));
        }

        long publishTimeoutMs =
                streamLoadInfo.getTimeout() * 1000L - (timeTrace.commitTxnTimeMs.get() - timeTrace.createTimeMs.get());
        StreamLoadTxnCommitAttachment txnCommitAttachment = new StreamLoadTxnCommitAttachment(
                0, 0, 0, 0, 0,
                0, 0, 0, 0, null);
        boolean publishSuccess = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                db, txnId, tabletCommitInfo, tabletFailInfo, publishTimeoutMs, txnCommitAttachment);
        if (!publishSuccess) {
            throw new UserException("Publish timeout");
        }
    }

    private void abortTxn() {
        if (txnId == -1) {
            return;
        }
        try {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    dbId, txnId, "failed to execute group commit");
        } catch (Exception e) {
            LOG.error("Failed to abort transaction {}", txnId, e);
        }
    }

    private void executeLoad() throws Exception {
        timeTrace.executePlanTimeMs.set(System.currentTimeMillis());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableId.getDbName());
        if (db == null) {
            throw new UserException(String.format("Database [%s] does not exist when executing load", tableId.getDbName()));
        }

        OlapTable table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            table = (OlapTable) db.getTable(tableId.getTableName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        if (table == null) {
            throw new UserException(String.format("Table [%s].[%s] does not exist when executing load",
                    tableId.getDbName(), tableId.getTableName()));
        }

        timeTrace.buildPlanTimeMs.set(System.currentTimeMillis());
        LoadPlanner loadPlanner = new LoadPlanner(-1, streamLoadInfo.getId(), txnId, db.getId(),
                tableId.getDbName(), table, streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(),
                streamLoadInfo.isPartialUpdate(), connectContext, null,
                streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                streamLoadInfo.getNegative(), coordinatorBeIds.size(),
                streamLoadInfo.getColumnExprDescs(), streamLoadInfo, label, streamLoadInfo.getTimeout());
        loadPlanner.setWarehouseId(streamLoadInfo.getWarehouseId());
        loadPlanner.setGroupCommit(groupCommitIntervalMs, coordinatorBeIds);
        loadPlanner.plan();

        Coordinator coord = new DefaultCoordinator.Factory().createStreamLoadScheduler(loadPlanner);
        try {
            QeProcessorImpl.INSTANCE.registerQuery(streamLoadInfo.getId(), coord);
            timeTrace.executePlanTimeMs.set(System.currentTimeMillis());
            coord.exec();

            timeTrace.joinPlanTimeMs.set(System.currentTimeMillis());
            int waitSecond = streamLoadInfo.getTimeout()
                    - (int) (System.currentTimeMillis() - timeTrace.createTimeMs.get()) / 1000;
            if (coord.join(waitSecond)) {
                Status status = coord.getExecStatus();
                if (!status.ok()) {
                    throw new LoadException(status.getErrorMsg());
                }
            } else {
                throw new LoadException("coordinator could not finished before job timeout");
            }
        } catch (Exception e) {
            throw new UserException(e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(streamLoadInfo.getId());
        }

        tabletCommitInfo = TabletCommitInfo.fromThrift(coord.getCommitInfos());
        tabletFailInfo = TabletFailInfo.fromThrift(coord.getFailInfos());
    }

    static class TimeTrace {
        AtomicLong createTimeMs;
        AtomicLong beginRunTimeMs = new AtomicLong(-1);
        AtomicLong beginTxnTimeMs = new AtomicLong(-1);
        AtomicLong buildPlanTimeMs = new AtomicLong(-1);
        AtomicLong executePlanTimeMs = new AtomicLong(-1);
        AtomicLong joinPlanTimeMs = new AtomicLong(-1);
        AtomicLong commitTxnTimeMs = new AtomicLong(-1);
        AtomicLong finishTimeMs = new AtomicLong(-1);

        public TimeTrace() {
            this.createTimeMs = new AtomicLong(System.currentTimeMillis());
        }
    }
}
