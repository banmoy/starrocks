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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TableGroupCommit {

    private static final Logger LOG = LogManager.getLogger(TableGroupCommit.class);

    private final TableId tableId;
    private final HttpHeaders headers;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final ReentrantReadWriteLock lock;
    private final Map<String, GroupCommitLoadExecutor> runningLoads;
    private List<TNetworkAddress> candidateCoordinatorBEs;
    private List<TNetworkAddress> brpcAddresses;
    private final AtomicLong nextBeIndex;

    public TableGroupCommit(TableId tableId, HttpHeaders headers, ThreadPoolExecutor threadPoolExecutor) {
        this.tableId = tableId;
        this.headers = headers;
        this.threadPoolExecutor = threadPoolExecutor;
        this.lock = new ReentrantReadWriteLock();
        this.runningLoads = new HashMap<>();
        this.candidateCoordinatorBEs = new ArrayList<>();
        this.brpcAddresses = new ArrayList<>();
        this.nextBeIndex = new AtomicLong(0);
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
            brpcAddresses.add(new TNetworkAddress(node.getHost(), node.getBrpcPort()));
        }
        LOG.info("Init table group commit, db: {}, table: {}, candidate BEs: {}", tableId.getDbName(),
                tableId.getTableName(), candidateCoordinatorBEs);
    }

    public List<TNetworkAddress> getRedirectHttpAddresses() {
        return new ArrayList<>(candidateCoordinatorBEs);
    }

    public List<TNetworkAddress> getRedirectBrpcAddresses() {
        return new ArrayList<>(brpcAddresses);
    }

    public TNetworkAddress getRedirectBe() {
        // TODO protect candidateCoordinatorBEs if it changes
        if (candidateCoordinatorBEs.isEmpty()) {
            return null;
        }
        int index = (int) (nextBeIndex.incrementAndGet() % candidateCoordinatorBEs.size());
        return candidateCoordinatorBEs.get(index);
    }

    public void notifyBeData(String beHost, String userLabel) {
        lock.readLock().lock();
        try {
            for (GroupCommitLoadExecutor loadExecutor : runningLoads.values()) {
                if (loadExecutor.isActive(beHost)) {
                    LOG.info("Find active txn, db: {}, table: {}, label: {}, be: {}, leftActiveMs: {}," +
                            " user: {}", tableId.getDbName(), tableId.getTableName(), loadExecutor.getLabel(),
                            beHost, loadExecutor.leftActiveMs(), userLabel);
                    return;
                } else {
                    LOG.info("Inactive txn, db: {}, table: {}, label: {}, be: {}, user: {}",
                            tableId.getDbName(), tableId.getTableName(), loadExecutor.getLabel(), beHost, userLabel);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            for (GroupCommitLoadExecutor loadExecutor : runningLoads.values()) {
                if (loadExecutor.isActive(beHost)) {
                    LOG.info("Find active txn, db: {}, table: {}, label: {}, be: {},  leftActiveMs: {}," +
                            " user: {}", tableId.getDbName(), tableId.getTableName(), loadExecutor.getLabel(),
                            beHost, loadExecutor.leftActiveMs(), userLabel);
                    return;
                } else {
                    LOG.info("Inactive txn, db: {}, table: {}, label: {}, be: {}, user: {}",
                            tableId.getDbName(), tableId.getTableName(), loadExecutor.getLabel(), beHost, userLabel);
                }
            }

            Set<String> bes = getBes();
            bes.add(beHost);
            String label = "group_" + DebugUtil.printId(UUIDUtil.toTUniqueId(UUID.randomUUID()));
            GroupCommitLoadExecutor loadExecutor = new GroupCommitLoadExecutor(this, tableId.getDbName(),
                    tableId.getTableName(), label, headers, bes, Config.group_commit_interval_ms);
            runningLoads.put(label, loadExecutor);
            LOG.info("Create group commit load, db: {}, table: {}, label: {}, BEs: {}, activeTimeMs: {}, ",
                    tableId.getDbName(), tableId.getTableName(), label, bes, loadExecutor.getActiveTimeMs());
            threadPoolExecutor.execute(loadExecutor);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeLoad(String label) {
        lock.writeLock().lock();
        try {
            runningLoads.remove(label);
            LOG.info("Remove group commit load, db: {}, table: {}, label: {}", tableId.getDbName(),
                    tableId.getTableName(), label);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Set<String> getBes() {
        return candidateCoordinatorBEs.stream().map(TNetworkAddress::getHostname)
                .collect(Collectors.toSet());
    }
}
