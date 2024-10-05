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

import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class IsomorphicLoad implements LoadExecuteCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IsomorphicLoad.class);

    private static final String LABEL_PREFIX = "group_commit_";

    private final long id;
    private final TableId tableId;
    private final String warehouseName;
    private final StreamLoadInfo streamLoadInfo;
    private final int groupCommitIntervalMs;
    private final int groupCommitParallel;
    private final ConnectContext connectContext;
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;
    private final Executor executor;
    private final Coordinator.Factory coordinatorFactory;
    private final ConcurrentHashMap<String, GroupCommitLoadExecutor> loadExecutorMap;
    private final ReentrantReadWriteLock lock;

    public IsomorphicLoad(
            long id,
            TableId tableId,
            String warehouseName,
            StreamLoadInfo streamLoadInfo,
            int groupCommitIntervalMs,
            int groupCommitParallel,
            ConnectContext connectContext,
            CoordinatorBackendAssigner coordinatorBackendAssigner,
            Executor executor) {
        this.id = id;
        this.tableId = tableId;
        this.warehouseName = warehouseName;
        this.streamLoadInfo = streamLoadInfo;
        this.groupCommitIntervalMs = groupCommitIntervalMs;
        this.groupCommitParallel = groupCommitParallel;
        this.connectContext = connectContext;
        this.coordinatorBackendAssigner = coordinatorBackendAssigner;
        this.executor = executor;
        this.coordinatorFactory = new DefaultCoordinator.Factory();
        this.loadExecutorMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public long getId() {
        return id;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getWarehouse() {
        return warehouseName;
    }

    public int getGroupCommitParallel() {
        return groupCommitParallel;
    }

    public int numRunningExecutors() {
        return loadExecutorMap.size();
    }

    public RequestCoordinatorBackendResult requestCoordinatorBackends() {
        TStatus status = new TStatus();
        List<ComputeNode> backends = null;
        try {
            backends = coordinatorBackendAssigner.getBackends(id);
            if (!backends.isEmpty()) {
                status.setStatus_code(TStatusCode.OK);
            } else {
                status.setStatus_code(TStatusCode.SERVICE_UNAVAILABLE);
                String errMsg = String.format(
                        "Can't find available backends, db: %s, table: %s, warehouse: %s, load id: %s",
                        tableId.getDbName(), tableId.getTableName(), warehouseName, id);
                status.setError_msgs(Collections.singletonList(errMsg));
                backends = null;
                LOG.error(errMsg);
            }
        } catch (Throwable throwable) {
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            String msg = String.format("Unexpected exception happen when getting backends, db: %s, table: %s, " +
                        "warehouse: %s, load id: %s, error: %s", tableId.getDbName(), tableId.getTableName(),
                                warehouseName, id, throwable.getMessage());
            status.setError_msgs(Collections.singletonList(msg));
            LOG.error("Failed to get backends, db: {}, table: {}, warehouse: {}, load id: {}",
                    tableId.getDbName(), tableId.getTableName(), warehouseName, id, throwable);
        }
        return new RequestCoordinatorBackendResult(status, backends);
    }

    public RequestLoadResult requestLoad(long backendId, String backendHost) {
        TStatus status = new TStatus();
        lock.readLock().lock();
        try {
            for (GroupCommitLoadExecutor loadExecutor : loadExecutorMap.values()) {
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, loadExecutor.getLabel());
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            for (GroupCommitLoadExecutor loadExecutor : loadExecutorMap.values()) {
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBackend(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, loadExecutor.getLabel());
                }
            }

            RequestCoordinatorBackendResult requestCoordinatorBackendResult = requestCoordinatorBackends();
            if (!requestCoordinatorBackendResult.isOk()) {
                return new RequestLoadResult(requestCoordinatorBackendResult.getStatus(), null);
            }

            Set<Long> backendIds = requestCoordinatorBackendResult.getResult().stream()
                    .map(ComputeNode::getId).collect(Collectors.toSet());
            if (!backendIds.contains(backendId)) {
                ComputeNode backend = GlobalStateMgr.getCurrentState()
                        .getNodeMgr().getClusterInfo().getBackendOrComputeNode(backendId);
                if (backend == null || !backend.isAvailable()) {
                    status.setStatus_code(TStatusCode.SERVICE_UNAVAILABLE);
                    status.setError_msgs(Collections.singletonList(
                            String.format("Backend [%s, %s] is not available", backendId, backendHost)));
                    return new RequestLoadResult(status, null);
                }
                backendIds.add(backendId);
            }

            String label = LABEL_PREFIX + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
            TUniqueId loadId = UUIDUtil.toTUniqueId(UUID.randomUUID());
            GroupCommitLoadExecutor loadExecutor = new GroupCommitLoadExecutor(
                    tableId, label, loadId, streamLoadInfo, groupCommitIntervalMs,
                    connectContext, backendIds, coordinatorFactory, this);
            loadExecutorMap.put(label, loadExecutor);
            try {
                executor.execute(loadExecutor);
            } catch (Exception e) {
                loadExecutorMap.remove(label);
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Collections.singletonList(e.getMessage()));
                return new RequestLoadResult(status, null);
            }
            status.setStatus_code(TStatusCode.OK);
            return new RequestLoadResult(status, label);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isActive() {
        return !loadExecutorMap.isEmpty();
    }

    @Override
    public void finishLoad(String label) {
        lock.writeLock().lock();
        try {
            loadExecutorMap.remove(label);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    GroupCommitLoadExecutor getLoadExecutor(String label) {
        return loadExecutorMap.get(label);
    }
}
