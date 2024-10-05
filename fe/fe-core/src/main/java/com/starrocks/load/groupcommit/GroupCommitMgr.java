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
import com.starrocks.load.streamload.StreamLoadInfo;
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
import javax.annotation.Nullable;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

public class GroupCommitMgr extends FrontendDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitMgr.class);

    private final AtomicLong idGenerator;
    /** Protected by lock. get/put need read lock, and remove need write lock. */
    private final ConcurrentHashMap<LoadUniqueId, IsomorphicLoad> loadMap;
    private final ReentrantReadWriteLock lock;
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;
    private final ThreadPoolExecutor threadPoolExecutor;

    public GroupCommitMgr() {
        super("group-commit-mgr", Config.group_commit_cleanup_check_interval_ms);
        this.idGenerator = new AtomicLong(0L);
        this.loadMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.coordinatorBackendAssigner = new CoordinatorBackendAssignerImpl();
        this.threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                        Config.group_commit_load_executor_threads_num, "group-commit-load", true);
    }

    @Override
    public void start() {
        super.start();
        this.coordinatorBackendAssigner.start();
        LOG.info("Start group commit manager");
    }

    public int numLoads() {
        return loadMap.size();
    }

    public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            IsomorphicLoad load = getOrCreateTableGroupCommit(tableId, params, status);
            if (status.getStatus_code() != TStatusCode.OK) {
                return new RequestCoordinatorBackendResult(status, null);
            }
            return load.requestCoordinatorBackends();
        } finally {
            lock.readLock().unlock();
        }
    }

    public RequestLoadResult requestLoad(
            TableId tableId, StreamLoadKvParams params, long backendId, String backendHost) {
        TStatus status = new TStatus(TStatusCode.OK);
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
                coordinatorBackendAssigner.unregisterLoad(entry.getValue().getId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Nullable
    private IsomorphicLoad getOrCreateTableGroupCommit(
            TableId tableId, StreamLoadKvParams params, TStatus status) {
        LoadUniqueId uniqueId = new LoadUniqueId(tableId, params);
        IsomorphicLoad load = loadMap.get(uniqueId);
        if (load != null) {
            return load;
        }

        String warehouseName = params.getWarehouse().orElse(DEFAULT_WAREHOUSE_NAME);
        StreamLoadInfo streamLoadInfo;
        try {
            streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, params);
        } catch (Exception e) {
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    String.format("Failed to build stream load info, error: %s", e.getMessage())));
            return null;
        }

        Integer groupCommitIntervalMs = params.getGroupCommitIntervalMs().orElse(null);
        if (groupCommitIntervalMs == null || groupCommitIntervalMs <= 0) {
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "Group commit interval must be set positive, but is " + groupCommitIntervalMs));
            return null;
        }

        Integer groupCommitParallel = params.getGroupCommitParallel().orElse(null);
        if (groupCommitParallel == null || groupCommitParallel <= 0) {
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "Group commit parallel must be set positive, but is " + groupCommitParallel));
            return null;
        }

        load = loadMap.computeIfAbsent(uniqueId, uid -> {
            long id = idGenerator.getAndIncrement();
            IsomorphicLoad newLoad = new IsomorphicLoad(
                    id, tableId, warehouseName, streamLoadInfo, groupCommitIntervalMs, groupCommitParallel,
                    new ConnectContext(), coordinatorBackendAssigner, threadPoolExecutor);
            coordinatorBackendAssigner.registerLoad(id, newLoad.getWarehouse(), tableId, newLoad.getGroupCommitParallel());
            return newLoad;
        });
        return load;
    }
}
