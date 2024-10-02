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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

// Need balance when
// 1. nodes changed
// 2. unregister load
// there is no need to balance for register load because we allocate node from the least workload
public class CoordinatorBeAssignerImpl implements CoordinatorBeAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorBeAssignerImpl.class);

    private static final int MIN_CHECK_INTERVAL_MS = 1000;

    // load id -> LoadMeta
    private final ConcurrentHashMap<Long, LoadMeta> registeredLoadMetas;
    private final AtomicLong scheduleTaskIdAllocator;
    private final PriorityBlockingQueue<ScheduleTask> scheduleTaskQueue;
    private final ExecutorService executorService;

    private final AtomicLong pendingDetectUnavailableNodesTask;

    // warehouse -> scheduling meta of this warehouse.
    // It's only ead/write in the single schedule thread
    private final Map<String, WarehouseScheduleMeta> warehouseScheduleMetas;

    // TODO Loads that fail to schedule UNREGISTER_LOAD, and wait for garbage collection
    private final Map<Long, LoadMeta> loadWaitForGc;

    public CoordinatorBeAssignerImpl() {
        this.registeredLoadMetas = new ConcurrentHashMap<>();
        this.scheduleTaskIdAllocator = new AtomicLong(0);
        this.scheduleTaskQueue = new PriorityBlockingQueue<>(32, ScheduleTaskComparator.INSTANCE);
        this.executorService = ThreadPoolManager.newDaemonCacheThreadPool(
                1, "coordinator-be-assigner-scheduler", true);
        this.pendingDetectUnavailableNodesTask = new AtomicLong(0);
        this.warehouseScheduleMetas = new HashMap<>();
        this.loadWaitForGc = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        this.executorService.submit(this::runSchedule);
        LOG.info("Start cooridnator be assigner");
    }

    private void runSchedule() {
        while (true) {
            ScheduleTask task;
            try {
                int checkIntervalMs;
                if (warehouseScheduleMetas.isEmpty()) {
                    // There is no need to schedule periodically if there is no load, and
                    // it will be woken up by a REGISTER_LOAD task when receiving a new load
                    checkIntervalMs = Integer.MAX_VALUE;
                    LOG.info("Disable periodical schedule because there is no load");
                } else {
                    checkIntervalMs = Math.max(MIN_CHECK_INTERVAL_MS, Config.group_commit_be_assigner_schedule_interval_ms);
                    LOG.debug("Set schedule interval to {} ms", checkIntervalMs);
                }
                task = scheduleTaskQueue.poll(checkIntervalMs, TimeUnit.MILLISECONDS);
            } catch (Throwable throwable) {
                LOG.warn("Failed to poll schedule task queue", throwable);
                continue;
            }

            if (task == null) {
                long startTime = System.currentTimeMillis();
                try {
                    periodicalCheckNodeChangedAndBalance();
                    LOG.debug("Success to execute periodical schedule, cost: {} ms",
                            System.currentTimeMillis() - startTime);
                } catch (Throwable throwable) {
                    LOG.error("Failed to execute periodical schedule, cost: {} ms",
                            System.currentTimeMillis() - startTime, throwable);
                }
                continue;
            }

            long startTime = System.currentTimeMillis();
            LOG.info("Start to execute task, type: {}, task id: {}", task.getScheduleType(), task.getTaskId());
            try {
                task.getScheduleRunnable().run();
                LOG.info("Success to execute task, type: {}, task id: {}, cost: {} ms",
                        task.getScheduleType(), task.getTaskId(), System.currentTimeMillis() - startTime);
            } catch (Throwable throwable) {
                LOG.error("Failed to execute task, type: {}, task id: {}, cost: {} ms",
                        task.getScheduleType(), task.getTaskId(), System.currentTimeMillis() - startTime, throwable);
            } finally {
                task.finish();
                if (task.getScheduleType() == ScheduleType.DETECT_UNAVAILABLE_NODES) {
                    pendingDetectUnavailableNodesTask.decrementAndGet();
                }
            }
        }
    }

    @Override
    public void registerLoad(long loadId, String warehouseName, TableId tableId, int expectParallel) {
        LoadMeta loadMeta = registeredLoadMetas.computeIfAbsent(
                loadId, k -> new LoadMeta(loadId, warehouseName, tableId, expectParallel));
        LOG.info("Register load, load id: {}, warehouse: {}, {}, parallel: {}",
                loadId, warehouseName, tableId, expectParallel);
        try {
            syncScheduleOnRegisterLoad(loadMeta);
            LOG.info("Register load finish schedule, load id: {}", loadId);
        } catch (Exception e) {
            LOG.warn("Failed to wait schedule result when registering load, load id: {}", loadId, e);
        }

    }

    @Override
    public void unregisterLoad(long loadId) {
        LoadMeta meta = registeredLoadMetas.remove(loadId);
        if (meta != null) {
            asyncScheduleOnUnregisterLoad(meta);
            LOG.info("Unregister load, load id: {}, warehouse: {}, {}", loadId, meta.warehouseName, meta.tableId);
        }
    }

    @Override
    public List<ComputeNode> requestBe(long loadId) {
        LoadMeta meta = registeredLoadMetas.get(loadId);
        if (meta == null) {
            return null;
        }
        List<ComputeNode> nodes = meta.nodes;
        if (nodes == null) {
            return null;
        }

        // lazily initialized until find unavailable nodes
        List<ComputeNode> availableNodes = null;
        List<ComputeNode> unavailableNodes = null;
        // check available
        for (int i = 0; i < nodes.size(); i++) {
            ComputeNode node = nodes.get(i);
            if (!node.isAvailable()) {
                // initialize availableNodes when find the first unavailable node
                if (availableNodes == null) {
                    availableNodes = new ArrayList<>(nodes.subList(0, i));
                    unavailableNodes = new ArrayList<>();
                }
                unavailableNodes.add(node);
            } else {
                if (availableNodes != null) {
                    availableNodes.add(node);
                }
            }
        }
        if (availableNodes != null) {
            asyncScheduleOnDetectUnavailableNodes(meta, unavailableNodes);
        }

        return Collections.unmodifiableList(availableNodes == null ? nodes : availableNodes);
    }

    private void syncScheduleOnRegisterLoad(LoadMeta loadMeta) throws Exception {
        ScheduleTask task = new ScheduleTask(
                scheduleTaskIdAllocator.incrementAndGet(),
                ScheduleType.REGISTER_LOAD,
                () -> this.runScheduleOnRegisterLoad(loadMeta));
        boolean ret = scheduleTaskQueue.add(task);
        if (!ret) {
            LOG.error("Failed to submit schedule task {}, load id: {}, queue size: {}",
                    ScheduleType.REGISTER_LOAD, loadMeta.loadId, scheduleTaskQueue.size());
            throw new RejectedExecutionException(
                    String.format("Failed to submit schedule task %s, load id: %s", ScheduleType.REGISTER_LOAD, loadMeta.loadId));
        }
        LOG.info("Submit schedule task {}, load id: {}, task id: {}",
                ScheduleType.REGISTER_LOAD, loadMeta.loadId, task.getTaskId());

        // TODO add timeout
        task.wailFinish();
    }

    private void runScheduleOnRegisterLoad(LoadMeta loadMeta) {
        String warehouseName = loadMeta.warehouseName;
        WarehouseScheduleMeta warehouseMeta =
                warehouseScheduleMetas.computeIfAbsent(warehouseName, WarehouseScheduleMeta::new);

        Map<Long, LoadMeta> loadMetas = warehouseMeta.schedulingLoadMetas;
        long loadId = loadMeta.loadId;
        if (loadMetas.putIfAbsent(loadId, loadMeta) != null) {
            LOG.error("Register duplicate load id: {}, warehouse: {}, expect parallel: {}, nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseName, loadMeta.expectParallel, loadMeta.nodes);
            return;
        }

        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetaSet;
        if (nodeMetas.isEmpty()) {
            List<ComputeNode> nodes = getAvailableNodes(warehouseName);
            nodes.forEach(node -> nodeMetas.add(new NodeMeta(node)));
        }

        int needNodeNum  = Math.min(nodeMetas.size(), loadMeta.expectParallel);
        List<NodeMeta> changedNodeMetas = new ArrayList<>(needNodeNum);
        List<ComputeNode> selectedNodes = new ArrayList<>(needNodeNum);
        for (int i = 0; i < needNodeNum; i++) {
            NodeMeta nodeMeta = nodeMetas.pollFirst();
            if (nodeMeta == null) {
                // should not happen, just log an error message
                LOG.error("There is no enough node for load id: {}, num nodes: {}, need nodes: {}, selected nodes: {}",
                        loadId, changedNodeMetas.size(), needNodeNum, selectedNodes);
                break;
            }
            if (nodeMeta.loadIds.contains(loadId)) {
                // should not happen, just log an error message
                LOG.error("Node already contains load, node: {}, load id: {}", nodeMeta.node, loadId);
            } else {
                nodeMeta.loadIds.add(loadId);
                selectedNodes.add(nodeMeta.node);
            }
            changedNodeMetas.add(nodeMeta);
        }
        nodeMetas.addAll(changedNodeMetas);
        loadMeta.nodes = selectedNodes;
        LOG.info("Allocate nodes for load id: {}, warehouse: {}, {}, expect parallel: {}, actual parallel: {}, " +
                        "selected nodes: {}", loadMeta.loadId, loadMeta.warehouseName, loadMeta.tableId,
                loadMeta.expectParallel, needNodeNum, selectedNodes);
        logStat(warehouseMeta);
    }

    private void asyncScheduleOnUnregisterLoad(LoadMeta loadMeta) {
        ScheduleTask task = new ScheduleTask(
                scheduleTaskIdAllocator.incrementAndGet(),
                ScheduleType.UNREGISTER_LOAD,
                () -> this.runScheduleOnUnregisterLoad(loadMeta));
        boolean ret = scheduleTaskQueue.add(task);
        if (!ret) {
            loadWaitForGc.put(loadMeta.loadId, loadMeta);
            LOG.error("Failed to submit schedule task {}, load id: {}, queue size: {}",
                    ScheduleType.UNREGISTER_LOAD, loadMeta.loadId, scheduleTaskQueue.size());
        } else {
            LOG.info("Submit schedule task {}, load id: {}, task id: {}",
                    ScheduleType.UNREGISTER_LOAD, loadMeta.loadId, task.getTaskId());
        }
    }

    private void runScheduleOnUnregisterLoad(LoadMeta loadMeta) {
        String warehouseName = loadMeta.warehouseName;
        WarehouseScheduleMeta warehouseMeta = warehouseScheduleMetas.get(warehouseName);
        if (warehouseMeta == null) {
            LOG.error("Unregister a load from a non-exist warehouse, load id: {}, warehouse: {}, expect parallel: {}," +
                    " nodes: {}", loadMeta.loadId, loadMeta.warehouseName, loadMeta.expectParallel, loadMeta.nodes);
            return;
        }

        Map<Long, LoadMeta> loadMetas = warehouseMeta.schedulingLoadMetas;
        long loadId = loadMeta.loadId;
        if (loadMetas == null || !loadMetas.containsKey(loadId)) {
            LOG.error("Unregister a non-scheduling load, load id: {}, warehouse: {}, expect parallel: {}, nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseName, loadMeta.expectParallel, loadMeta.nodes);
            return;
        }

        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetaSet;
        List<NodeMeta> changedNodeMetas = new ArrayList<>();
        Iterator<NodeMeta> iterator = nodeMetas.iterator();
        // TODO make it more efficient without loop all nodes
        while (iterator.hasNext()) {
            NodeMeta nodeMeta = iterator.next();
            if (nodeMeta.loadIds.remove(loadId)) {
                changedNodeMetas.add(nodeMeta);
                iterator.remove();
            }
        }
        nodeMetas.addAll(changedNodeMetas);
        loadMetas.remove(loadId);
        warehouseMeta.needBalance.set(true);
        LOG.info("Deallocate nodes for load id: {}, warehouse: {}, {}, nodes: {}",
                loadMeta.loadId, loadMeta.warehouseName, loadMeta.tableId, loadMeta.nodes);
        logStat(warehouseMeta);
    }

    private void asyncScheduleOnDetectUnavailableNodes(LoadMeta loadMeta, List<ComputeNode> unavailableNodes) {
        // remove duplicated task, allowing one is being executed, and one is in the queue
        if (pendingDetectUnavailableNodesTask.incrementAndGet() > 3) {
            pendingDetectUnavailableNodesTask.decrementAndGet();
            return;
        }

        ScheduleTask task = new ScheduleTask(
                scheduleTaskIdAllocator.incrementAndGet(),
                ScheduleType.DETECT_UNAVAILABLE_NODES,
                () -> this.runScheduleOnDetectUnavailableNodes(loadMeta));
        boolean ret = scheduleTaskQueue.add(task);
        if (!ret) {
            pendingDetectUnavailableNodesTask.decrementAndGet();
            LOG.error("Failed to submit schedule task {}, load id {}, unavailable node ids: {}, queue size: {}",
                    ScheduleType.DETECT_UNAVAILABLE_NODES, loadMeta.loadId, unavailableNodes, scheduleTaskQueue.size());
        } else {
            LOG.info("Submit schedule task {}, load id: {}, task id: {}, pending task num: {}",
                    ScheduleType.DETECT_UNAVAILABLE_NODES, loadMeta.loadId, task.getTaskId(),
                    pendingDetectUnavailableNodesTask.get());
        }
    }

    private void runScheduleOnDetectUnavailableNodes(LoadMeta loadMeta) {
        WarehouseScheduleMeta warehouseMeta = warehouseScheduleMetas.get(loadMeta.warehouseName);
        if (warehouseMeta == null) {
            return;
        }
        runScheduleIfNodeChanged(warehouseMeta);
    }

    private void periodicalCheckNodeChangedAndBalance() {
        List<String> warehouseNames = new ArrayList<>(warehouseScheduleMetas.keySet());
        for (String name : warehouseNames) {
            WarehouseScheduleMeta warehouseMeta = warehouseScheduleMetas.get(name);
            if (warehouseMeta.schedulingLoadMetas.isEmpty()) {
                warehouseScheduleMetas.remove(warehouseMeta.warehouseName);
                LOG.info("Remove warehouse {} because there is no scheduling load", warehouseMeta.warehouseName);
            } else {
                runScheduleIfNodeChanged(warehouseMeta);
                doBalanceIfNeeded(warehouseMeta, Config.group_commit_balance_diff_ratio);
                if (LOG.isDebugEnabled()) {
                    logStat(warehouseMeta);
                }
            }
        }
    }

    private void runScheduleIfNodeChanged(WarehouseScheduleMeta warehouseMeta) {
        // 1. find the newest unavailable/available nodes
        Map<Long, ComputeNode> systemAvailableNodes =
                getAvailableNodes(warehouseMeta.warehouseName).stream()
                        .collect(Collectors.toMap(ComputeNode::getId, Function.identity()));
        Map<Long, NodeMeta> unavailableNodeMetas = new HashMap<>();
        Map<Long, NodeMeta> availableNodeMetas = new HashMap<>();
        for (NodeMeta nodeMeta : warehouseMeta.sortedNodeMetaSet) {
            ComputeNode node = nodeMeta.node;
            if (systemAvailableNodes.containsKey(node.getId())) {
                availableNodeMetas.put(node.getId(), nodeMeta);
            } else {
                unavailableNodeMetas.put(node.getId(), nodeMeta);
            }
        }
        boolean nodesChanged = !unavailableNodeMetas.isEmpty() || availableNodeMetas.size() < systemAvailableNodes.size();
        if (!nodesChanged) {
            LOG.debug("There is no node change for warehouse {}, num nodes: {}",
                    warehouseMeta.warehouseName, systemAvailableNodes.size());
            return;
        }
        List<ComputeNode> newAvailableNodes = new ArrayList<>();
        // add those new available nodes to the map
        for (ComputeNode node : systemAvailableNodes.values()) {
            if (!availableNodeMetas.containsKey(node.getId())) {
                availableNodeMetas.put(node.getId(), new NodeMeta(node));
                newAvailableNodes.add(node);
            }
        }

        LOG.info("Trigger schedule for node change, warehouse: {}, num new nodes: {}, num unavailable nodes: {}," +
                        " to add nodes: {}, to remove nodes: {}",
                warehouseMeta.warehouseName, newAvailableNodes.size(), unavailableNodeMetas.size(), newAvailableNodes,
                unavailableNodeMetas.values().stream().map(meta -> meta.node).collect(Collectors.toList()));

        // 2. Reallocate nodes to each load if
        //      1. the old node is unavailable
        //      2. there is new nodes to increase the parallel to the expected
        //    For each load
        //      a. calculate the new parallel as min(expectParallel, #availableNodes)
        //      b. remove the old unavailable nodes
        //      c. if the current number of allocated nodes not meets the new parallel,
        //         iterate from the node with the least number of loads, and choose
        //         those not containing this load id
        // The iterating order can allocate loads to the node with the least loads as
        // much as possible, but the result can be not balanced, and we will check
        // balance in balanceIfNeeded()
        long startTime = System.currentTimeMillis();
        NavigableSet<NodeMeta> sortedNodeMetaSet = warehouseMeta.sortedNodeMetaSet;
        sortedNodeMetaSet.clear();
        sortedNodeMetaSet.addAll(availableNodeMetas.values());
        // load id -> new node id set
        Map<Long, Set<Long>> changedLoadIds = new HashMap<>();
        for (LoadMeta loadMeta : warehouseMeta.schedulingLoadMetas.values()) {
            long loadId = loadMeta.loadId;
            int newParallel = Math.min(loadMeta.expectParallel, availableNodeMetas.size());
            Set<Long> allocatedNodeIds = new HashSet<>();
            boolean removeUnavailableNodes = false;
            for (ComputeNode node : loadMeta.nodes) {
                if (availableNodeMetas.containsKey(node.getId())) {
                    allocatedNodeIds.add(node.getId());
                } else {
                    removeUnavailableNodes = true;
                }
            }
            List<NodeMeta> changedNodeMetas = new ArrayList<>(newParallel - allocatedNodeIds.size());
            Iterator<NodeMeta> iterator = sortedNodeMetaSet.iterator();
            while (iterator.hasNext() && allocatedNodeIds.size() < newParallel) {
                NodeMeta nodeMeta = iterator.next();
                long nodeId = nodeMeta.node.getId();
                if (allocatedNodeIds.add(nodeId)) {
                    nodeMeta.loadIds.add(loadId);
                    changedNodeMetas.add(nodeMeta);
                    iterator.remove();
                }
            }
            sortedNodeMetaSet.addAll(changedNodeMetas);
            if (removeUnavailableNodes || !changedNodeMetas.isEmpty()) {
                changedLoadIds.put(loadId, allocatedNodeIds);
            }
        }

        // 3. update load metas
        for (Map.Entry<Long, Set<Long>> entry : changedLoadIds.entrySet()) {
            long loadId = entry.getKey();
            Set<Long> newNodeIds = entry.getValue();
            LoadMeta loadMeta = warehouseMeta.schedulingLoadMetas.get(loadId);
            List<ComputeNode> newNodes = new ArrayList<>(newNodeIds.size());
            newNodeIds.forEach(id -> newNodes.add(availableNodeMetas.get(id).node));
            List<ComputeNode> oldNodes = loadMeta.nodes;
            loadMeta.nodes = newNodes;
            LOG.info("Node change trigger reassign, load id: {}, warehouse: {}, {}, num old nodes: {}, " +
                            "num new nodes: {}, old nodes: {}, new nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseName, loadMeta.tableId,
                    oldNodes.size(), newNodes.size(), oldNodes, newNodes);
        }
        warehouseMeta.needBalance.set(true);
        LOG.info("Finish to schedule triggered by node change, warehouse: {}, num changed loads: {}, cost: {} ms",
                warehouseMeta.warehouseName, changedLoadIds.size(), System.currentTimeMillis() - startTime);
        logStat(warehouseMeta);
    }

    private void doBalanceIfNeeded(WarehouseScheduleMeta warehouseMeta, double expectDiffRatio) {
        if (!warehouseMeta.needBalance.compareAndSet(true, false)) {
            return;
        }

        double loadDiffRatio = calculateLoadDiffRatio(warehouseMeta.sortedNodeMetaSet);
        if (loadDiffRatio <= expectDiffRatio) {
            return;
        }

        LOG.info("Start to balance warehouse {}, expect load diff ratio: {}, current load diff ratio: {}",
                warehouseMeta.warehouseName, expectDiffRatio, loadDiffRatio);
        // TODO limit the number of loads in one balance cycle
        long startTime = System.currentTimeMillis();
        NavigableSet<NodeMeta> unfinishedNodeMetas = new TreeSet<>(warehouseMeta.sortedNodeMetaSet);
        NavigableSet<NodeMeta> finishedNodeMetas = new TreeSet<>(NodeMetaComparator.INSTANCE);
        int unfinishedParalell = unfinishedNodeMetas.stream().mapToInt(meta -> meta.loadIds.size()).sum();
        Set<Long> needUpdateLoadIds = new HashSet<>();
        while (!unfinishedNodeMetas.isEmpty()) {
            int expectMaxLoadNum = (unfinishedParalell + unfinishedNodeMetas.size() - 1) / unfinishedNodeMetas.size();
            NodeMeta largeNodeMeta = unfinishedNodeMetas.pollLast();
            Set<Long> largeLoadIds = largeNodeMeta.loadIds;
            List<NodeMeta> changedNodeMetas = new ArrayList<>();
            while (!unfinishedNodeMetas.isEmpty()) {
                NodeMeta smallNodeMeta = unfinishedNodeMetas.pollFirst();
                changedNodeMetas.add(smallNodeMeta);
                Set<Long> smallLoadIds = smallNodeMeta.loadIds;
                List<Long> changedLoadIds = new ArrayList<>();
                for (long loadId : largeLoadIds) {
                    if (largeLoadIds.size() - changedLoadIds.size() <= expectMaxLoadNum) {
                        break;
                    }
                    if (smallLoadIds.size() + changedLoadIds.size() >= expectMaxLoadNum) {
                        break;
                    }
                    if (!smallLoadIds.contains(loadId)) {
                        changedLoadIds.add(loadId);
                    }
                }
                changedLoadIds.forEach(largeLoadIds::remove);
                smallLoadIds.addAll(changedLoadIds);
                needUpdateLoadIds.addAll(changedLoadIds);
                if (largeLoadIds.size() <= expectMaxLoadNum) {
                    break;
                }
            }
            unfinishedNodeMetas.addAll(changedNodeMetas);
            finishedNodeMetas.add(largeNodeMeta);
            unfinishedParalell -= largeNodeMeta.loadIds.size();
            double diffRatio = calculateLoadDiffRatio(finishedNodeMetas, unfinishedNodeMetas);
            if (diffRatio <= expectDiffRatio) {
                break;
            }
        }
        warehouseMeta.sortedNodeMetaSet.clear();
        warehouseMeta.sortedNodeMetaSet.addAll(finishedNodeMetas);
        warehouseMeta.sortedNodeMetaSet.addAll(unfinishedNodeMetas);

        for (long loadId : needUpdateLoadIds) {
            LoadMeta loadMeta = warehouseMeta.schedulingLoadMetas.get(loadId);
            List<ComputeNode> newNodes = new ArrayList<>(loadMeta.expectParallel);
            for (NodeMeta nodeMeta : warehouseMeta.sortedNodeMetaSet) {
                if (nodeMeta.loadIds.contains(loadId)) {
                    newNodes.add(nodeMeta.node);
                }
            }
            List<ComputeNode> oldNodes = loadMeta.nodes;
            loadMeta.nodes = newNodes;
            LOG.info("Node change trigger reassign, load id: {}, warehouse: {}, {}, num old nodes: {}, " +
                            "num new nodes: {}, old nodes: {}, new nodes: {}",
                    loadMeta.loadId, loadMeta.warehouseName, loadMeta.tableId,
                    oldNodes.size(), newNodes.size(), oldNodes, newNodes);
        }
        LOG.info("Finish to balance warehouse {}, num changed loads: {}, cost: {} ms",
                warehouseMeta.warehouseName, needUpdateLoadIds.size(), System.currentTimeMillis() - startTime);
        logStat(warehouseMeta);
    }

    private double calculateLoadDiffRatio(NavigableSet<NodeMeta> nodeMetas) {
        if (nodeMetas.size() <= 1) {
            return 0;
        }

        int minLoadNum = nodeMetas.first().loadIds.size();
        int maxLoadNum = nodeMetas.last().loadIds.size();
        if (maxLoadNum <= 1) {
            return 0;
        }
        return (maxLoadNum - minLoadNum) / (double) maxLoadNum;
    }

    private double calculateLoadDiffRatio(NavigableSet<NodeMeta> left, NavigableSet<NodeMeta> right) {
        if (left.isEmpty()) {
            return calculateLoadDiffRatio(right);
        }

        if (right.isEmpty()) {
            return calculateLoadDiffRatio(left);
        }

        int minLoadNum = Math.min(left.first().loadIds.size(), right.first().loadIds.size());
        int maxLoadNum = Math.max(left.last().loadIds.size(), right.last().loadIds.size());
        if (maxLoadNum <= 1) {
            return 0;
        }
        return (maxLoadNum - minLoadNum) / (double) maxLoadNum;
    }

    private void logStat(WarehouseScheduleMeta warehouseMeta) {
        StringBuilder nodeStat = new StringBuilder();
        NavigableSet<NodeMeta> nodeMetas = warehouseMeta.sortedNodeMetaSet;
        Map<Long, LoadMeta> loadMetas = warehouseMeta.schedulingLoadMetas;
        int totalExpectParallel = loadMetas.values().stream().mapToInt(meta -> meta.expectParallel).sum();
        int totalActualParallel = loadMetas.values().stream().mapToInt(meta -> meta.nodes.size()).sum();
        int totalNodeLoadParallel = nodeMetas.stream().mapToInt(meta -> meta.loadIds.size()).sum();
        for (NodeMeta nodeMeta : nodeMetas) {
            if (nodeStat.length() > 0) {
                nodeStat.append(",");
            }
            ComputeNode node = nodeMeta.node;
            nodeStat.append("[");
            nodeStat.append(node.getId());
            nodeStat.append(",");
            nodeStat.append(node.getHost());
            nodeStat.append(",");
            nodeStat.append(nodeMeta.loadIds.size());
            nodeStat.append("]");
        }

        LOG.info("Statistics for warehouse {}, num nodes: {}, num loads: {}, total parallel expect/actual: {}/{}, " +
                        "node parallel: {}, load diff ratio actual: {}, load distribution [node id,host,#loads]: {}",
                warehouseMeta.warehouseName, nodeMetas.size(), loadMetas.size(), totalExpectParallel,
                totalActualParallel, totalNodeLoadParallel, calculateLoadDiffRatio(nodeMetas), nodeStat);

        if (LOG.isDebugEnabled()) {
            for (LoadMeta meta : loadMetas.values()) {
                LOG.debug("Load details, warehouse: {}, load id: {}, {}, parallel expect/actual: {}/{}, nodes: {}",
                        meta.warehouseName, meta.loadId, meta.tableId, meta.expectParallel,
                        meta.nodes.size(), meta.nodes);
            }
            for (NodeMeta nodeMeta : nodeMetas) {
                LOG.debug("Node details, warehouse: {}, node: {}, num load: {}, load ids: {}",
                        warehouseMeta.warehouseName, nodeMeta.node, nodeMeta.loadIds.size(), nodeMeta.loadIds);
            }
        }
    }

    private List<ComputeNode> getAvailableNodes(String warehouseName) {
        List<ComputeNode> nodes = new ArrayList<>();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseName);
            for (long nodeId : computeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodes.add(node);
                }
            }
        } else {
            nodes.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAvailableBackends());
        }
        return nodes;
    }

    enum ScheduleType {
        UNREGISTER_LOAD(0),
        DETECT_UNAVAILABLE_NODES(1),
        REGISTER_LOAD(2);

        // a larger number is with a higher priority
        private final int priority;

        ScheduleType(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    static class ScheduleTask {

        final long taskId;
        final ScheduleType scheduleType;
        final Runnable scheduleRunnable;
        final CompletableFuture<Void> future;

        public ScheduleTask(long taskId, ScheduleType scheduleType, Runnable scheduleRunnable) {
            this.taskId = taskId;
            this.scheduleType = scheduleType;
            this.scheduleRunnable = scheduleRunnable;
            this.future = new CompletableFuture<>();
        }

        public long getTaskId() {
            return taskId;
        }

        public ScheduleType getScheduleType() {
            return scheduleType;
        }

        public Runnable getScheduleRunnable() {
            return scheduleRunnable;
        }

        public void finish() {
            future.complete(null);
        }

        public void wailFinish() throws Exception {
            future.get();
        }
    }

    // a task is larger with a higher ScheduleType priority and a larger unique task id
    static class ScheduleTaskComparator implements Comparator<ScheduleTask> {

        static final ScheduleTaskComparator INSTANCE = new ScheduleTaskComparator();

        @Override
        public int compare(ScheduleTask task1, ScheduleTask task2) {
            int ret = Integer.compare(task1.getScheduleType().getPriority(), task2.getScheduleType().getPriority());
            if (ret != 0) {
                return ret;
            }

            // a smaller task id is with high priority
            return Long.compare(task2.getTaskId(), task1.getTaskId());
        }
    }

    static class NodeMeta  {
        ComputeNode node;
        Set<Long> loadIds;

        public NodeMeta(ComputeNode node) {
            this.node = node;
            this.loadIds = new HashSet<>();
        }
    }

    // a node is larger with a larger number of loads and a larger node id
    static class NodeMetaComparator implements Comparator<NodeMeta> {

        public static final NodeMetaComparator INSTANCE = new NodeMetaComparator();

        @Override
        public int compare(NodeMeta node1, NodeMeta node2) {
            int cmp = Integer.compare(node1.loadIds.size(), node2.loadIds.size());
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(node1.node.getId(), node2.node.getId());
        }
    }

    static class LoadMeta {
        final long loadId;
        final String warehouseName;
        final TableId tableId;
        final int expectParallel;
        // copy-on-write. only can be set in the schedule thread which is single
        volatile List<ComputeNode> nodes;

        public LoadMeta(long loadId, String warehouseName, TableId tableId, int expectParallel) {
            this.loadId = loadId;
            this.warehouseName = warehouseName;
            this.tableId = tableId;
            this.expectParallel = expectParallel;
            this.nodes = new ArrayList<>();
        }
    }

    static class WarehouseScheduleMeta {
        final String warehouseName;

        // A set of node metas sorted according to NodeMetaComparator
        final NavigableSet<NodeMeta> sortedNodeMetaSet;

        // load id -> LoadMeta. Loads that is being scheduled in nodeMetaMap. It may be not consistent
        // with CoordinatorBeAssignerImpl.registeredLoadMetas because registerLoad()/unregisterLoad()
        // will submit schedule tasks which will executed asynchronously
        final Map<Long, LoadMeta> schedulingLoadMetas;
        final AtomicBoolean needBalance;

        public WarehouseScheduleMeta(String warehouseName) {
            this.warehouseName = warehouseName;
            this.sortedNodeMetaSet = new TreeSet<>(NodeMetaComparator.INSTANCE);
            this.schedulingLoadMetas = new HashMap<>();
            this.needBalance = new AtomicBoolean(false);
        }
    }
}
