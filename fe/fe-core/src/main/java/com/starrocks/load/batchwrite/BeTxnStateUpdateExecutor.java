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

package com.starrocks.load.batchwrite;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.proto.PUpdateTransactionStateRequest;
import com.starrocks.proto.PUpdateTransactionStateResponse;
import com.starrocks.proto.TransactionStatePB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.PBackendService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionStateSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public class BeTxnStateUpdateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BeTxnStateUpdateExecutor.class);

    private final Executor rpcExecutor;
    // tnx id -> task
    private final ConcurrentMap<TaskId, Task> pendingTasks;

    public BeTxnStateUpdateExecutor(Executor rpcExecutor) {
        this.rpcExecutor = rpcExecutor;
        this.pendingTasks = new ConcurrentHashMap<>();
    }

    public void submitTask(String dbName, long txnId, long backendId) throws Exception {
        TaskId taskId = new TaskId(txnId, backendId);
        Task task = new Task(dbName, taskId);
        Task oldTask = pendingTasks.putIfAbsent(taskId, task);
        if (oldTask != null) {
            return;
        }
        boolean success = false;
        try {
            rpcExecutor.execute(() -> runTask(task));
            success = true;
            LOG.debug("Success to submit task, db: {}, txn_id: {}, backend_id: {}", dbName, txnId, backendId);
        } catch (Exception e) {
            LOG.error("Failed to submit task, db: {}, txn_id: {}, backend_id: {}", dbName, txnId, backendId, e);
            throw e;
        } finally {
            if (!success) {
                pendingTasks.remove(taskId);
            }
        }
    }

    private void runTask(Task task) {
        int numRetry = Config.merge_commit_be_txn_state_update_retry_times;
        for (int i = 0; i <= numRetry; i++) {
            UpdateResult result = updateTransactionState(task.dbName, task.taskId.txnId, task.taskId.backendId);
            if (result != UpdateResult.RETRY) {
                break;
            }
            try {
                Thread.sleep(Config.merge_commit_be_txn_state_update_retry_interval_ms);
            } catch (InterruptedException e) {
                LOG.error("Interrupted when retry to update transaction state, db: {}, txn_id: {}, backend id: {}",
                        task.dbName, task.taskId.txnId, task.taskId.backendId, e);
                break;
            }
        }
        pendingTasks.remove(task.taskId);
    }

    enum UpdateResult {
        SUCCESS,
        FAILED,
        RETRY
    }

    private UpdateResult updateTransactionState(String dbName, long txnId, long backendId) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ComputeNode computeNode = globalStateMgr.getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
        if (computeNode == null) {
            LOG.error("Can't find backend, db: {}, txn_id: {}, backend id: {}", dbName, txnId, backendId);
            return UpdateResult.FAILED;
        }
        if (!computeNode.isAlive()) {
            LOG.debug("Backend does not alive, db: {}, txn_id: {}, backend id: {}, address: {}",
                    dbName, txnId, backendId, computeNode.getAddress());
            return UpdateResult.RETRY;
        }
        TNetworkAddress address = new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.error("Can't find database, db: {}, txn_id: {}, backend id: {}", dbName, txnId, backendId);
            return UpdateResult.FAILED;
        }

        TransactionStateSnapshot state;
        try {
            state = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTxnState(db, txnId);
        } catch (Exception e) {
            LOG.error("Failed to get transaction state, db: {}, txn_id: {}, backend id: {}", dbName, txnId, backendId, e);
            return UpdateResult.FAILED;
        }
        try {
            TransactionStatePB statePB = new TransactionStatePB();
            statePB.setTxnId(txnId);
            statePB.setStatus(state.getStatus().toProto());
            statePB.setReason(state.getReason());
            PUpdateTransactionStateRequest request = new PUpdateTransactionStateRequest();
            request.setStates(Collections.singletonList(statePB));
            PBackendService service = BrpcProxy.getBackendService(address);
            Future<PUpdateTransactionStateResponse> future = service.updateTransactionState(request);
            future.get();
            return UpdateResult.SUCCESS;
        } catch (Exception e) {
            LOG.debug("Failed to get rpc result, db: {}, txn_id: {}, backend id: {}, address: {}",
                    dbName, txnId, backendId, address, e);
            return UpdateResult.RETRY;
        }
    }

    private static class TaskId {
        private final long txnId;
        private final long backendId;

        public TaskId(long txnId, long backendId) {
            this.txnId = txnId;
            this.backendId = backendId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TaskId taskId = (TaskId) o;
            return txnId == taskId.txnId && backendId == taskId.backendId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(txnId, backendId);
        }
    }

    private static class Task {
        final String dbName;
        final TaskId taskId;

        public Task(String dbName, TaskId taskId) {
            this.dbName = dbName;
            this.taskId = taskId;
        }
    }

}
