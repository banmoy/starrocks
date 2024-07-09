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
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TableGroupCommit {

    private static final Logger LOG = LogManager.getLogger(TableGroupCommit.class);

    private final TableId tableId;
    private final HttpHeaders headers;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final ReentrantReadWriteLock lock;
    private final Map<String, GroupCommitTxn> runningTxns;
    private List<TNetworkAddress> candidateCoordinatorBEs;

    public TableGroupCommit(TableId tableId, HttpHeaders headers, ThreadPoolExecutor threadPoolExecutor) {
        this.tableId = tableId;
        this.headers = headers;
        this.threadPoolExecutor = threadPoolExecutor;
        this.lock = new ReentrantReadWriteLock();
        this.runningTxns = new HashMap<>();
        this.candidateCoordinatorBEs = new ArrayList<>();
    }

    public void init() {
        List<Long> nodeIds = new ArrayList<>();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(
                    WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            for (long nodeId : computeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodeIds.add(nodeId);
                }
            }
        } else {
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            nodeIds = systemInfoService.getAvailableBackends().stream()
                            .filter(ComputeNode::isAvailable)
                            .map(ComputeNode::getId)
                            .collect(Collectors.toList());
        }
        Collections.shuffle(nodeIds);

        int numBes = Math.min(Config.group_commit_num_candidate_bes, nodeIds.size());
        for (int i = 0; i < numBes; i++) {
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getBackendOrComputeNode(nodeIds.get(i));
            candidateCoordinatorBEs.add(new TNetworkAddress(node.getHost(), node.getHttpPort()));
        }
        LOG.info("Init group commit for table {}.{}, candidate BE: {}",
                tableId.getDbName(), tableId.getTableName(), candidateCoordinatorBEs);
    }

    public TNetworkAddress getRedirectBe() {
        lock.readLock().lock();
        try {
            if (candidateCoordinatorBEs.isEmpty()) {
                return null;
            }
            int index = ThreadLocalRandom.current().nextInt(candidateCoordinatorBEs.size());
            return candidateCoordinatorBEs.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void notifyBeData(String beHost) {
        lock.readLock().lock();
        try {
            for (GroupCommitTxn txn : runningTxns.values()) {
                if (txn.isOpen(beHost)) {
                    LOG.info("{} notify to have data, and this should belong to txn_id: {}", beHost, txn.label);
                    return;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            for (GroupCommitTxn txn : runningTxns.values()) {
                if (txn.isOpen(beHost)) {
                    LOG.info("{} notify to have data, and this should belong to txn_id: {}", beHost, txn.label);
                    return;
                }
            }

            Set<String> bes = getBes();
            bes.add(beHost);
            String label = "group_" + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
            GroupCommitLoadExecutor loadExecutor = new GroupCommitLoadExecutor(
                    this, tableId.getDbName(), tableId.getTableName(), label, headers, bes);
            long startTimeMs = System.currentTimeMillis();
            GroupCommitTxn groupCommitTxn = new GroupCommitTxn(label, startTimeMs,
                    startTimeMs + Config.group_commit_interval_ms, bes, loadExecutor);
            runningTxns.put(label, groupCommitTxn);
            threadPoolExecutor.execute(loadExecutor);
            LOG.info("Create group commit load for {}.{}, label: {}, BEs: {}, startTimeMs: {}, groupEndTimeMs: {}",
                    tableId.getDbName(), tableId.getTableName(), label, bes, startTimeMs, groupCommitTxn.groupEndTimeMs);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeLabel(String label) {
        lock.writeLock().lock();
        try {
            runningTxns.remove(label);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Set<String> getBes() {
        return candidateCoordinatorBEs.stream().map(TNetworkAddress::getHostname)
                .collect(Collectors.toSet());
    }

    public static class GroupCommitTxn {
        private final String label;
        private final long startTimeMs;
        private final long groupEndTimeMs;
        private final Set<String> coordinatorBEs;
        GroupCommitLoadExecutor loadExecutor;

        public GroupCommitTxn(String label, long startTimeMs, long groupEndTimeMs,
                              Set<String> coordinatorBEs, GroupCommitLoadExecutor loadExecutor) {
            this.label = label;
            this.startTimeMs = startTimeMs;
            this.groupEndTimeMs = groupEndTimeMs;
            this.coordinatorBEs = coordinatorBEs;
            this.loadExecutor = loadExecutor;
        }

        public boolean isOpen(String beHost) {
            if (!coordinatorBEs.contains(beHost)) {
                return false;
            }
            return System.currentTimeMillis() < groupEndTimeMs;
        }
    }
}
