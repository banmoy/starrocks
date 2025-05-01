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

package com.starrocks.load.mergecommit;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

/**
 * Manages merge commit operations.
 */
public class MergeCommitMgr extends FrontendDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitMgr.class);

    // An atomic counter used to generate unique ids for isomorphic merge commits.
    private final AtomicLong idGenerator;

    // A read-write lock to ensure thread-safe access to the loadMap.
    private final ReentrantReadWriteLock lock;

    // A concurrent map that stores IsomorphicMergeCommit instances, keyed by MergeCommitId.
    private final ConcurrentHashMap<MergeCommitId, IsomorphicMergeCommit> isomorphicMergeCommitMap;

    // An assigner that manages the assignment of coordinator backends.
    private final CoordinatorBackendAssigner coordinatorBackendAssigner;

    // A thread pool executor for executing merge commit tasks.
    private final ThreadPoolExecutor threadPoolExecutor;

    private final TxnStateDispatcher txnStateDispatcher;

    public MergeCommitMgr() {
        super("merge-commit-mgr", Config.merge_commit_gc_check_interval_ms);
        this.idGenerator = new AtomicLong(0L);
        this.isomorphicMergeCommitMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.coordinatorBackendAssigner = new CoordinatorBackendAssignerImpl();
        this.threadPoolExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                        Config.merge_commit_executor_threads_num, "merge-commit-load", true);
        this.txnStateDispatcher = new TxnStateDispatcher(threadPoolExecutor);
    }

    @Override
    public synchronized void start() {
        super.start();
        this.coordinatorBackendAssigner.start();
        LOG.info("Start merge commit manager");
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Config.merge_commit_gc_check_interval_ms);
        cleanupInactiveMergeCommit();
    }

    /**
     * Requests coordinator backends for the specified table and load parameters.
     *
     * @param tableId The ID of the table for which the coordinator backends are requested.
     * @param params The parameters for the stream load.
     * @return A RequestCoordinatorBackendResult containing the status of the operation and the coordinator backends.
     */
    public RequestCoordinatorBackendResult requestCoordinatorBackends(TableId tableId, StreamLoadKvParams params) {
        lock.readLock().lock();
        try {
            Pair<TStatus, IsomorphicMergeCommit> result = getOrCreateTableMergeCommit(tableId, params);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestCoordinatorBackendResult(result.first, null);
            }
            return result.second.requestCoordinatorBackends();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Requests a load operation for the specified table and load parameters.
     *
     * @param tableId The ID of the table for which the load is requested.
     * @param params The parameters for the stream load.
     * @param backendId The id of the backend where the request is from.
     * @param backendHost The host of the backend where the request is from.
     * @return A RequestLoadResult containing the status of the operation and the load result.
     */
    public RequestLoadResult requestLoad(
            TableId tableId, StreamLoadKvParams params, long backendId, String backendHost) {
        lock.readLock().lock();
        try {
            Pair<TStatus, IsomorphicMergeCommit> result = getOrCreateTableMergeCommit(tableId, params);
            if (result.first.getStatus_code() != TStatusCode.OK) {
                return new RequestLoadResult(result.first, null);
            }
            return result.second.requestLoad(backendId, backendHost);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Cleans up inactive merge commits to release resources.
     */
    @VisibleForTesting
    void cleanupInactiveMergeCommit() {
        lock.writeLock().lock();
        try {
            List<Map.Entry<MergeCommitId, IsomorphicMergeCommit>> loads = isomorphicMergeCommitMap.entrySet().stream()
                            .filter(entry -> !entry.getValue().isActive())
                            .collect(Collectors.toList());
            for (Map.Entry<MergeCommitId, IsomorphicMergeCommit> entry : loads) {
                isomorphicMergeCommitMap.remove(entry.getKey());
                coordinatorBackendAssigner.unregisterMergeCommit(entry.getValue().getId());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves or creates an IsomorphicMergeCommit instance for the specified table and parameters.
     *
     * @param tableId The ID of the table for which the merge commit is requested.
     * @param params The parameters for the stream load.
     * @return A Pair containing the status of the operation and the IsomorphicMergeCommit instance.
     */
    private Pair<TStatus, IsomorphicMergeCommit> getOrCreateTableMergeCommit(TableId tableId, StreamLoadKvParams params) {
        MergeCommitId uniqueId = new MergeCommitId(tableId, params);
        IsomorphicMergeCommit load = isomorphicMergeCommitMap.get(uniqueId);
        if (load != null) {
            return new Pair<>(new TStatus(TStatusCode.OK), load);
        }

        String warehouseName = params.getWarehouse().orElse(DEFAULT_WAREHOUSE_NAME);
        StreamLoadInfo streamLoadInfo;
        try {
            streamLoadInfo = StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    String.format("Failed to build stream load info, error: %s", e.getMessage())));
            return new Pair<>(status, null);
        }

        Integer mergeCommitIntervalMs = params.getMergeCommitIntervalMs().orElse(null);
        if (mergeCommitIntervalMs == null || mergeCommitIntervalMs <= 0) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "merge commit interval must be set positive, but is " + mergeCommitIntervalMs));
            return new Pair<>(status, null);
        }

        Integer mergeCommitParallel = params.getMergeCommitParallel().orElse(null);
        if (mergeCommitParallel == null || mergeCommitParallel <= 0) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INVALID_ARGUMENT);
            status.setError_msgs(Collections.singletonList(
                    "merge commit parallel must be set positive, but is " + mergeCommitParallel));
            return new Pair<>(status, null);
        }

        try {
            load = isomorphicMergeCommitMap.computeIfAbsent(uniqueId, uid -> {
                long id = idGenerator.getAndIncrement();
                IsomorphicMergeCommit newLoad = new IsomorphicMergeCommit(
                        id, tableId, warehouseName, streamLoadInfo, mergeCommitIntervalMs, mergeCommitParallel,
                        params, coordinatorBackendAssigner, threadPoolExecutor, txnStateDispatcher);
                coordinatorBackendAssigner.registerMergeCommit(id, newLoad.getWarehouseId(), tableId,
                        newLoad.getMergeCommitParallel());
                return newLoad;
            });
            LOG.info("Create merge commit, id: {}, {}, {}", load.getId(), tableId, params);
        } catch (Exception e) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Collections.singletonList(e.getMessage()));
            LOG.error("Failed to create merge commit for {}, params: {}", tableId, params, e);
            return new Pair<>(status, null);
        }

        return new Pair<>(new TStatus(TStatusCode.OK), load);
    }

    /**
     * Returns the number of merge commits currently managed.
     *
     * @return The number of merge commits.
     */
    public int numMergeCommit() {
        return isomorphicMergeCommitMap.size();
    }

    public CoordinatorBackendAssigner getCoordinatorBackendAssigner() {
        return coordinatorBackendAssigner;
    }
}
