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
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.UtFrameUtils;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CoordinatorBackendAssignerTest extends GroupCommitTestBase {

    private CoordinatorBackendAssignerImpl assigner;

    @Before
    public void setup() {
        assigner = new CoordinatorBackendAssignerImpl();
        assigner.start();
    }

    @Test
    public void testRegisterLoad() {
        assigner.registerLoad(
                1L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        List<ComputeNode> nodes1 = assigner.getBackends(1);
        assertNotNull(nodes1);
        assertEquals(1, nodes1.size());
        assertEquals(1, nodes1.stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerLoad(
                2L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        List<ComputeNode> nodes2 = assigner.getBackends(2);
        assertNotNull(nodes2);
        assertEquals(2, nodes2.size());
        assertEquals(2, nodes2.stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerLoad(
                3L, "wh2", new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        List<ComputeNode> nodes3 = assigner.getBackends(3);
        assertNotNull(nodes3);
        assertEquals(4, nodes3.size());
        assertEquals(4, nodes3.stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());

        assigner.registerLoad(
                4L, "wh2", new TableId(DB_NAME_2, TABLE_NAME_2_2), 10);
        List<ComputeNode> nodes4 = assigner.getBackends(4);
        assertNotNull(nodes4);
        assertEquals(5, nodes4.size());
        assertEquals(5, nodes4.stream().map(ComputeNode::getId).collect(Collectors.toSet()).size());
    }

    @Test
    public void testUnRegisterLoad() {
        assigner.registerLoad(
                1L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        assigner.registerLoad(
                2L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        assigner.registerLoad(
                3L, "wh2", new TableId(DB_NAME_2, TABLE_NAME_2_1), 4);
        assigner.registerLoad(
                4L, "wh2", new TableId(DB_NAME_2, TABLE_NAME_2_2), 10);

        final AtomicLong expectNumScheduledTask = new AtomicLong(assigner.numScheduledTasks());
        assigner.unregisterLoad(1);
        assertNull(assigner.getBackends(1));
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(1, "wh1"));

        assigner.unregisterLoad(2);
        assertNull(assigner.getBackends(2));
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(2, "wh1"));

        assigner.unregisterLoad(3);
        assertNull(assigner.getBackends(3));
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(3, "wh2"));

        assigner.unregisterLoad(4);
        assertNull(assigner.getBackends(4));
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());
        assertFalse(containsLoadMeta(4, "wh2"));
    }

    @Test
    public void testDetectUnavailableNodesWhenGetBackends() {
        assigner.registerLoad(
                1L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_1), 1);
        assigner.registerLoad(
                2L, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_2), 2);
        assigner.registerLoad(
                3L, "wh1", new TableId(DB_NAME_2, TABLE_NAME_2_1), 3);
        assigner.registerLoad(
                4L, "wh1", new TableId(DB_NAME_2, TABLE_NAME_2_2), 4);

        assigner.disablePeriodicalScheduleForTest();
        List<ComputeNode> nodes = assigner.getBackends(1);
        assertEquals(1, nodes.size());
        ComputeNode notAliveNode = nodes.get(0);
        assertTrue(notAliveNode.isAvailable());
        notAliveNode.setAlive(false);
        assertFalse(notAliveNode.isAvailable());

        final AtomicLong expectNumScheduledTask = new AtomicLong(assigner.numScheduledTasks());
        assertTrue(assigner.getBackends(1).isEmpty());
        expectNumScheduledTask.incrementAndGet();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> assigner.numScheduledTasks() == expectNumScheduledTask.get());

        for (int i = 1; i <= 4; i++) {
            List<ComputeNode> newNodes = assigner.getBackends(i);
            assertEquals(i, newNodes.size());
            assertFalse(newNodes.stream().map(ComputeNode::getId)
                    .collect(Collectors.toSet())
                    .contains(notAliveNode.getId()));
        }
    }

    @Test
    public void testBalance() throws Exception {
        Set<Long> backendIds = new HashSet<>();
        for (int i = 1; i <= 100; i++) {
            assigner.registerLoad(
                    i, "wh1", new TableId(DB_NAME_1, TABLE_NAME_1_1), 4);
            List<ComputeNode> nodes = assigner.getBackends(i);
            assertEquals(4, nodes.size());
            nodes.forEach(node -> backendIds.add(node.getId()));
        }
        assertEquals(5, backendIds.size());
        assertTrue(assigner.currentLoadDiffRatio("wh1") < Config.group_commit_balance_diff_ratio);

        assigner.disablePeriodicalScheduleForTest();
        for (int i = 10006; i <= 10010; i++) {
            UtFrameUtils.addMockBackend(i);
        }
        backendIds.clear();
        assigner.periodicalCheckNodeChangedAndBalance();
        assertTrue(assigner.currentLoadDiffRatio("wh1") < Config.group_commit_balance_diff_ratio);
        for (int i = 1; i <= 100; i++) {
            List<ComputeNode> nodes = assigner.getBackends(i);
            assertEquals(4, nodes.size());
            nodes.forEach(node -> backendIds.add(node.getId()));
        }
        assertEquals(10, backendIds.size());

        for (int i = 10006; i <= 10010; i++) {
            UtFrameUtils.dropMockBackend(i);
        }
    }

    private boolean containsLoadMeta(long loadId, String warehouse) {
        CoordinatorBackendAssignerImpl.WarehouseScheduleMeta whMeta = assigner.getWarehouseMeta(warehouse);
        if (whMeta == null) {
            return false;
        }
        if (whMeta.schedulingLoadMetas.containsKey(loadId)) {
            return true;
        }
        for (CoordinatorBackendAssignerImpl.NodeMeta nodeMeta : whMeta.sortedNodeMetaSet) {
            if (nodeMeta.loadIds.contains(loadId)) {
                return true;
            }
        }
        return false;
    }
}
