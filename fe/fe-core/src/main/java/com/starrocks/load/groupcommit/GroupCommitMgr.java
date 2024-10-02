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
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class GroupCommitMgr extends FrontendDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitMgr.class);

    private final AtomicLong idGenerator;
    /** Protected by lock. get/put need read lock, and remove need write lock. */
    private final ConcurrentHashMap<LoadUniqueId, IsomorphicLoad> loadMap;
    private final ReentrantReadWriteLock lock;
    private final CoordinatorBeAssigner coordinatorBeAssigner;
    private final ThreadPoolExecutor threadPoolExecutor;

    public GroupCommitMgr() {
        super("group-commit-mgr", Config.group_commit_cleanup_check_interval_ms);
        this.idGenerator = new AtomicLong(0L);
        this.loadMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.coordinatorBeAssigner = new CoordinatorBeAssignerImpl();
        this.threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                        Config.group_commit_load_executor_threads_num, "group-commit-load", true);
    }

    @Override
    public synchronized void start() {
        super.start();
        this.coordinatorBeAssigner.start();
        LOG.info("Start group commit manager");
    }

    public RequestCoordinatorBeResult requestCoordinatorBEs(TableId tableId, StreamLoadKvParams params) {
        TStatus status = new TStatus(TStatusCode.OK);
        checkServiceAvailable(status);
        if (status.getStatus_code() != TStatusCode.OK) {
            return new RequestCoordinatorBeResult(status, null);
        }
        lock.readLock().lock();
        try {
            IsomorphicLoad load = getOrCreateTableGroupCommit(tableId, params, status);
            if (status.getStatus_code() != TStatusCode.OK) {
                return new RequestCoordinatorBeResult(status, null);
            }
            return load.requestCoordinatorBEs();
        } finally {
            lock.readLock().unlock();
        }
    }

    public RequestLoadResult requestLoad(
            TableId tableId, StreamLoadKvParams params, long backendId, String backendHost) {
        TStatus status = new TStatus(TStatusCode.OK);
        checkServiceAvailable(status);
        if (status.getStatus_code() != TStatusCode.OK) {
            return new RequestLoadResult(status, null);
        }
        lock.readLock().lock();
        try {
            IsomorphicLoad load = getOrCreateTableGroupCommit(tableId, params, status);
            if (status.getStatus_code() != TStatusCode.OK) {
                return new RequestLoadResult(status, null);
            }
            return load.requestLoad(backendId, backendHost);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Config.group_commit_cleanup_check_interval_ms);
        cleanup();
    }

    @VisibleForTesting
    void cleanup() {
        lock.writeLock().lock();
        try {
            List<Map.Entry<LoadUniqueId, IsomorphicLoad>> loads = loadMap.entrySet().stream()
                            .filter(entry -> !entry.getValue().isActive())
                            .collect(Collectors.toList());
            for (Map.Entry<LoadUniqueId, IsomorphicLoad> entry : loads) {
                loadMap.remove(entry.getKey());
                coordinatorBeAssigner.unregisterLoad(entry.getValue().getId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private IsomorphicLoad getOrCreateTableGroupCommit(TableId tableId, StreamLoadKvParams params, TStatus status) {
        LoadUniqueId uniqueId = new LoadUniqueId(tableId, params);
        IsomorphicLoad load = loadMap.get(uniqueId);
        if (load != null) {
            return load;
        }

        Integer groupCommitIntervalMs = params.getGroupCommitIntervalMs().orElse(null);
        Integer groupCommitParallel = params.getGroupCommitParallel().orElse(null);
        checkGroupCommitParameters(groupCommitIntervalMs, groupCommitParallel, status);
        if (status.getStatus_code() != TStatusCode.OK) {
            return null;
        }

        load = loadMap.computeIfAbsent(uniqueId, uid -> {
            long id = idGenerator.getAndIncrement();
            IsomorphicLoad newLoad = new IsomorphicLoad(
                    id, tableId, params, groupCommitIntervalMs, groupCommitParallel,
                    new ConnectContext(), coordinatorBeAssigner, threadPoolExecutor);
            coordinatorBeAssigner.registerLoad(id, newLoad.getWarehouse(), tableId, newLoad.getGroupCommitParallel());
            return newLoad;
        });
        return load;
    }

    private void checkServiceAvailable(TStatus status) {
        String errMsg = null;
        if (!Config.enable_group_commit) {
            errMsg = "Group commit does not enable. You can set configuration 'enable_group_commit' on FE leader";
        }
        if (!isRunning()) {
            errMsg = "Only FE leader can process group commit request, but current FE is not leader";
        }

        if (errMsg != null) {
            status.setStatus_code(TStatusCode.SERVICE_UNAVAILABLE);
            status.setError_msgs(Collections.singletonList(errMsg));
        }
    }

    private void checkGroupCommitParameters(Integer groupCommitIntervalMs, Integer groupCommitParallel, TStatus status) {
        if (groupCommitIntervalMs == null || groupCommitIntervalMs <= 0) {
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList("Group commit interval must be set positive"));
        }

        if (groupCommitParallel == null || groupCommitParallel <= 0) {
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList("Group commit parallel must be set positive"));
        }
    }
}
