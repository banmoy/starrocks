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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/LoadAction.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class LoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    protected static final String GROUP_COMMIT = "group_commit";

    public LoadAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load",
                new LoadAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException, AccessDeniedException {
        try {
            executeWithoutPasswordInternal(request, response);
        } catch (DdlException e) {
            TransactionResult resp = new TransactionResult();
            resp.status = ActionStatus.FAILED;
            resp.msg = e.getClass() + ": " + e.getMessage();
            LOG.warn("Failed to execute executeWithoutPasswordInternal", e);

            sendResult(request, response, resp);
        }
    }

    public void executeWithoutPasswordInternal(BaseRequest request, BaseResponse response) throws DdlException,
            AccessDeniedException {

        // A 'Load' request must have 100-continue header
        if (!request.getRequest().headers().contains(HttpHeaders.Names.EXPECT)) {
            throw new DdlException("There is no 100-continue header");
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("No table selected.");
        }

        String label = request.getRequest().headers().get(LABEL_KEY);

        Authorizer.checkTableAction(ConnectContext.get().getCurrentUserIdentity(), ConnectContext.get().getCurrentRoleIds(),
                dbName, tableName, PrivilegeType.INSERT);

        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (request.getRequest().headers().contains(WAREHOUSE_KEY)) {
            warehouseName = request.getRequest().headers().get(WAREHOUSE_KEY);
        }

        TNetworkAddress redirectAddr;
        if (request.getRequest().headers().contains(GROUP_COMMIT)) {
            redirectAddr = GlobalStateMgr.getCurrentState().getGroupCommitMgr().getRedirectBe(
                    dbName, tableName, request.getRequest().headers());
            if (request.getRequest().headers().contains("meta")) {
                List<TNetworkAddress> httpAddresses =
                        GlobalStateMgr.getCurrentState().getGroupCommitMgr().getRedirectHttpAddresses(
                                dbName, tableName);
                String httpAddressesStr = "";
                if (httpAddresses != null && !httpAddresses.isEmpty()) {
                    StringJoiner joiner = new StringJoiner(";");
                    httpAddresses.forEach(addr -> joiner.add(addr.getHostname() + ":" + addr.getPort()));
                    httpAddressesStr = joiner.toString();
                }

                List<TNetworkAddress> brpcAddresses =
                        GlobalStateMgr.getCurrentState().getGroupCommitMgr().getRedirectBrpcAddresses(
                                dbName, tableName);
                String brpcAddressesStr = "";
                if (brpcAddresses != null && !brpcAddresses.isEmpty()) {
                    StringJoiner joiner = new StringJoiner(";");
                    brpcAddresses.forEach(addr -> joiner.add(addr.getHostname() + ":" + addr.getPort()));
                    brpcAddressesStr = joiner.toString();
                }
                sendResult(request, response, new GroupCommitBeMetas(httpAddressesStr, brpcAddressesStr));
                return;
            }

            if (redirectAddr == null) {
                throw new DdlException("Can't find redirect address for group commit");
            }
        } else {
            redirectAddr = selectRedirectAddr(warehouseName);
        }

        LOG.info("redirect load action to destination={}, db: {}, tbl: {}, label: {}, warehouse: {}",
                redirectAddr.toString(), dbName, tableName, label, warehouseName);
        redirectTo(request, response, redirectAddr);
    }

    private static TNetworkAddress selectRedirectAddr(String warehouseName) throws DdlException {
        // Choose a backend sequentially, or choose a cn in shared_data mode
        List<Long> nodeIds = new ArrayList<>();
        if (RunMode.isSharedDataMode()) {
            List<Long> computeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseName);
            for (long nodeId : computeIds) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAvailable()) {
                    nodeIds.add(nodeId);
                }
            }
            Collections.shuffle(nodeIds);
        } else {
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            nodeIds = systemInfoService.getNodeSelector().seqChooseBackendIds(1, false, false, null);
        }

        if (CollectionUtils.isEmpty(nodeIds)) {
            throw new DdlException("No backend alive.");
        }

        // TODO: need to refactor after be split into cn + dn
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
        if (node == null) {
            throw new DdlException("No backend or compute node alive.");
        }

        return new TNetworkAddress(node.getHost(), node.getHttpPort());
    }

    private static class GroupCommitBeMetas extends RestBaseResult {
        private String httpAddresses;
        private String brpcAddresses;

        public GroupCommitBeMetas(String httpAddresses, String brpcAddresses) {
            this.httpAddresses = httpAddresses;
            this.brpcAddresses = brpcAddresses;
        }
    }
}

