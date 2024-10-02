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
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

public class IsomorphicLoad implements LoadExecuteCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IsomorphicLoad.class);

    private final long id;
    private final TableId tableId;
    private final StreamLoadKvParams params;
    private final int groupCommitIntervalMs;
    private final int groupCommitParallel;
    private final ConnectContext connectContext;
    private final CoordinatorBeAssigner coordinatorBeAssigner;
    private final ThreadPoolExecutor executor;
    private final ConcurrentHashMap<String, GroupCommitLoadExecutor> loadExecutorMap;
    private final ReentrantReadWriteLock lock;

    public IsomorphicLoad(
            long id,
            TableId tableId,
            StreamLoadKvParams params,
            int groupCommitIntervalMs,
            int groupCommitParallel,
            ConnectContext connectContext,
            CoordinatorBeAssigner coordinatorBeAssigner,
            ThreadPoolExecutor executor) {
        this.id = id;
        this.tableId = tableId;
        this.params = params;
        this.groupCommitIntervalMs = groupCommitIntervalMs;
        this.groupCommitParallel = groupCommitParallel;
        this.connectContext = connectContext;
        this.coordinatorBeAssigner = coordinatorBeAssigner;
        this.executor = executor;
        this.loadExecutorMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public long getId() {
        return id;
    }

    public int getGroupCommitParallel() {
        return groupCommitParallel;
    }

    public String getWarehouse() {
        return params.getWarehouse().orElse(DEFAULT_WAREHOUSE_NAME);
    }

    public RequestCoordinatorBeResult requestCoordinatorBEs() {
        TStatus status = new TStatus();
        List<ComputeNode> backends = null;
        try {
            backends = coordinatorBeAssigner.requestBe(id);
            if (!backends.isEmpty()) {
                status.setStatus_code(TStatusCode.OK);
            } else {
                status.setStatus_code(TStatusCode.NOT_FOUND);
                status.setError_msgs(Collections.singletonList("Can't find available backends"));
                backends = null;
                LOG.warn("Group commit load can't find available coordinator BEs, id: {}, tableId: {}", id, tableId);
            }
        } catch (Throwable throwable) {
            status.setStatus_code(TStatusCode.UNKNOWN);
            status.setError_msgs(Collections.singletonList(throwable.getMessage()));
            LOG.error("Group commit load failed to request coordinator BEs, id: {}, tableId: {}", id, tableId, throwable);
        }
        return new RequestCoordinatorBeResult(status, backends);
    }

    public RequestLoadResult requestLoad(long backendId, String backendHost) {
        TStatus status = new TStatus();
        lock.readLock().lock();
        try {
            for (GroupCommitLoadExecutor loadExecutor : loadExecutorMap.values()) {
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBe(backendId)) {
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
                if (loadExecutor.isActive() && loadExecutor.containCoordinatorBe(backendId)) {
                    status.setStatus_code(TStatusCode.OK);
                    return new RequestLoadResult(status, loadExecutor.getLabel());
                }
            }

            RequestCoordinatorBeResult requestCoordinatorBeResult = requestCoordinatorBEs();
            if (!requestCoordinatorBeResult.isOk()) {
                return new RequestLoadResult(requestCoordinatorBeResult.getStatus(), null);
            }

            Set<Long> backendIds = requestCoordinatorBeResult.getResult().stream()
                    .map(ComputeNode::getId).collect(Collectors.toSet());
            if (!backendIds.contains(backendId)) {
                ComputeNode backend = GlobalStateMgr.getCurrentState()
                        .getNodeMgr().getClusterInfo().getBackend(backendId);
                if (backend == null || !backend.isAvailable()) {
                    status.setStatus_code(TStatusCode.NOT_FOUND);
                    status.setError_msgs(Collections.singletonList(
                            String.format("Backend [%s, %s] is invalid", backendId, backendHost)));
                    return new RequestLoadResult(requestCoordinatorBeResult.getStatus(), null);
                }
                backendIds.add(backendId);
            }

            String label = "group_commit_" + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
            GroupCommitLoadExecutor loadExecutor = new GroupCommitLoadExecutor(
                    tableId, label, params, connectContext, backendIds,
                    groupCommitIntervalMs, this);
            loadExecutorMap.put(label, loadExecutor);
            try {
                executor.execute(loadExecutor);
            } catch (Exception e) {
                loadExecutorMap.remove(label);
                status.setStatus_code(TStatusCode.UNKNOWN);
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

    // TODO add statistic about the load
    @Override
    public void finishLoad(String label) {
        lock.writeLock().lock();
        try {
            loadExecutorMap.remove(label);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
