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

import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.thrift.TStatusCode;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_FORMAT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_GROUP_COMMIT_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_GROUP_COMMIT_PARALLEL;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_WAREHOUSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GroupCommitMgrTest extends GroupCommitTestBase {

    private GroupCommitMgr groupCommitMgr;

    private TableId tableId1;
    private TableId tableId2;
    private TableId tableId3;
    private TableId tableId4;

    @Before
    public void setup() {
        groupCommitMgr = new GroupCommitMgr();
        groupCommitMgr.start();

        tableId1 = new TableId(DB_NAME_1, TABLE_NAME_1_1);
        tableId2 = new TableId(DB_NAME_1, TABLE_NAME_1_2);
        tableId3 = new TableId(DB_NAME_2, TABLE_NAME_2_1);
        tableId4 = new TableId(DB_NAME_2, TABLE_NAME_2_2);
    }

    @Test
    public void testRequestBackends() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "1000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "1");
            }});
        RequestCoordinatorBackendResult result1 =
                groupCommitMgr.requestCoordinatorBackends(tableId1, params1);
        assertTrue(result1.isOk());
        assertEquals(1, result1.getResult().size());
        assertEquals(1, groupCommitMgr.numLoads());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_FORMAT, "json");
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "1000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "1");
            }});
        RequestCoordinatorBackendResult result2 =
                groupCommitMgr.requestCoordinatorBackends(tableId1, params2);
        assertTrue(result2.isOk());
        assertEquals(1, result2.getResult().size());
        assertEquals(2, groupCommitMgr.numLoads());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "10000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result3 =
                groupCommitMgr.requestCoordinatorBackends(tableId2, params3);
        assertTrue(result3.isOk());
        assertEquals(4, result3.getResult().size());
        assertEquals(3, groupCommitMgr.numLoads());

        StreamLoadKvParams params4 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "10000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result4 =
                groupCommitMgr.requestCoordinatorBackends(tableId3, params4);
        assertTrue(result4.isOk());
        assertEquals(4, result4.getResult().size());
        assertEquals(4, groupCommitMgr.numLoads());
    }

    @Test
    public void testRequestLoad() {
        StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "100000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
            }});
        RequestLoadResult result1 = groupCommitMgr.requestLoad(
                tableId4, params, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result1.isOk());
        assertNotNull(result1.getResult());
        assertEquals(1, groupCommitMgr.numLoads());

        RequestLoadResult result2 = groupCommitMgr.requestLoad(
                tableId4, params, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(result1.getResult(), result2.getResult());
        assertEquals(1, groupCommitMgr.numLoads());
    }

    @Test
    public void testCheckParameters() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>());
        RequestCoordinatorBackendResult result1 =
                groupCommitMgr.requestCoordinatorBackends(tableId1, params1);
        assertFalse(result1.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result1.getStatus().getStatus_code());
        assertEquals(0, groupCommitMgr.numLoads());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "10000");
            }});
        RequestCoordinatorBackendResult result2 =
                groupCommitMgr.requestCoordinatorBackends(tableId2, params2);
        assertFalse(result2.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result2.getStatus().getStatus_code());
        assertEquals(0, groupCommitMgr.numLoads());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "10000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
                put(HTTP_WAREHOUSE, "no_exist");
            }});
        RequestCoordinatorBackendResult result3 =
                groupCommitMgr.requestCoordinatorBackends(tableId3, params3);
        assertFalse(result3.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result3.getStatus().getStatus_code());
        assertEquals(0, groupCommitMgr.numLoads());
    }

    @Test
    public void testCleanup() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "10000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result1 = groupCommitMgr.requestCoordinatorBackends(tableId1, params1);
        assertTrue(result1.isOk());
        assertEquals(1, groupCommitMgr.numLoads());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_GROUP_COMMIT_INTERVAL_MS, "100000");
                put(HTTP_GROUP_COMMIT_PARALLEL, "4");
            }});
        RequestLoadResult result2 = groupCommitMgr.requestLoad(
                tableId4, params2, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(2, groupCommitMgr.numLoads());

        groupCommitMgr.cleanup();
        assertEquals(1, groupCommitMgr.numLoads());
    }
}
