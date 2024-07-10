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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadParam;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionState;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.baidu.jprotobuf.pbrpc.client.DynamicProtobufRpcProxy.TIMEOUT_KEY;
import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;

public class GroupCommitLoadExecutor implements Runnable {

    private static final Logger LOG = LogManager.getLogger(GroupCommitLoadExecutor.class);

    private final TableGroupCommit groupCommit;
    private final String dbName;
    private final String tableName;
    private final String label;
    private final HttpHeaders headers;
    private final Set<String> candidateBes;
    private final long id;
    private final TUniqueId loadId;
    private final long timeoutMs;
    private long txnId = -1;
    private Coordinator coord;
    private long numRowsNormal;
    private long numRowsAbnormal;
    private long numRowsUnselected;
    private long numLoadBytesTotal;
    private String trackingUrl;
    private long beforeLoadTimeMs;
    private long endTimeMs;

    public GroupCommitLoadExecutor(
            TableGroupCommit groupCommit, String dbName, String tableName, String label,
            HttpHeaders headers, Set<String> candidateBes) {
        this.groupCommit = groupCommit;
        this.dbName = dbName;
        this.tableName = tableName;
        this.label = label;
        this.headers = headers;
        this.candidateBes = candidateBes;
        this.id = GlobalStateMgr.getCurrentState().getNextId();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        this.timeoutMs = getTimeoutMs();
    }

    @Override
    public void run() {
        try {
            beginTxn();
            executePlan();
        } catch (Exception e) {
            rollbackTxn();
            LOG.error("Failed to execute group commit load for {}.{}, label: {}, txnId: {}",
                    dbName, tableName, label, txnId, e);
        } finally {
            groupCommit.removeLabel(label);
        }
    }

    private void executePlan() throws Exception {
        beforeLoadTimeMs = System.currentTimeMillis();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        OlapTable table;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            table = (OlapTable) db.getTable(tableName);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        StreamLoadParam streamLoadParam = StreamLoadParam.parseHttpHeader(headers);
        StreamLoadInfo  streamLoadInfo = StreamLoadInfo.fromStreamLoadContext(
                loadId, txnId, (int) timeoutMs / 1000, streamLoadParam);
        LoadPlanner loadPlanner = new LoadPlanner(id, loadId, txnId, db.getId(), dbName, table,
                streamLoadInfo.isStrictMode(), streamLoadInfo.getTimezone(), streamLoadInfo.isPartialUpdate(),
                null, null, streamLoadInfo.getLoadMemLimit(), streamLoadInfo.getExecMemLimit(),
                streamLoadInfo.getNegative(), candidateBes.size(), streamLoadInfo.getColumnExprDescs(), streamLoadInfo,
                label, streamLoadInfo.getTimeout());
        loadPlanner.setWarehouseId(streamLoadInfo.getWarehouseId());
        loadPlanner.setCandidateBes(candidateBes);
        loadPlanner.setActiveTimeMs(Config.group_commit_interval_ms);
        loadPlanner.plan();

        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        coord = new DefaultCoordinator.Factory().createStreamLoadScheduler(loadPlanner);
        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, coord);
            coord.exec();

            int waitSecond = (int) ((deadlineMs - System.currentTimeMillis()) / 1000);
            if (coord.join(waitSecond)) {
                Status status = coord.getExecStatus();
                Map<String, String> loadCounters = coord.getLoadCounters();
                if (loadCounters == null || loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL) == null) {
                    throw new LoadException(ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg());
                }
                this.numRowsNormal = Long.parseLong(loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL));
                this.numRowsAbnormal = Long.parseLong(loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL));
                this.numRowsUnselected = Long.parseLong(loadCounters.get(LoadJob.UNSELECTED_ROWS));
                this.numLoadBytesTotal = Long.parseLong(loadCounters.get(LoadJob.LOADED_BYTES));

                if (numRowsNormal == 0) {
                    throw new LoadException(ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg());
                }

                if (coord.isEnableLoadProfile()) {
                    collectProfile();
                }

                this.trackingUrl = coord.getTrackingUrl();
                if (!status.ok()) {
                    throw new LoadException(status.getErrorMsg());
                }
            } else {
                throw new LoadException("coordinator could not finished before job timeout");
            }
        } catch (Exception e) {
            throw new UserException(e.getMessage());
        }

        List<TabletCommitInfo> commitInfos = TabletCommitInfo.fromThrift(coord.getCommitInfos());
        List<TabletFailInfo> failInfos = TabletFailInfo.fromThrift(coord.getFailInfos());
        endTimeMs = System.currentTimeMillis();
        StreamLoadTxnCommitAttachment txnCommitAttachment = new StreamLoadTxnCommitAttachment(
                beforeLoadTimeMs, beforeLoadTimeMs, beforeLoadTimeMs, beforeLoadTimeMs, endTimeMs,
                numRowsNormal, numRowsAbnormal, numRowsUnselected, numLoadBytesTotal, trackingUrl);
        boolean publishResult = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                db, txnId, commitInfos, failInfos, Config.group_commit_publish_time_ms, txnCommitAttachment);
        LOG.info("Finish group commit for {}.{}, label: {}, txn_id: {}, publish result: {}",
                dbName, tableName, label, txnId, publishResult);
    }

    public void collectProfile() {
        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - beforeLoadTimeMs;

        // For the usage scenarios of flink cdc or routine load,
        // the frequency of stream load maybe very high, resulting in many profiles,
        // but we may only care about the long-duration stream load profile.
        if (totalTimeMs < Config.stream_load_profile_collect_second * 1000) {
            LOG.info(String.format("Load %s, totalTimeMs %d < Config.stream_load_profile_collect_second %d)",
                    label, totalTimeMs, Config.stream_load_profile_collect_second));
            return;
        }

        RuntimeProfile profile = new RuntimeProfile("Load");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(loadId));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(beforeLoadTimeMs));

        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(System.currentTimeMillis()));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, dbName);

        Map<String, String> loadCounters = coord.getLoadCounters();
        if (loadCounters != null && loadCounters.size() != 0) {
            summaryProfile.addInfoString("NumRowsNormal", loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL));
            summaryProfile.addInfoString("NumLoadBytesTotal", loadCounters.get(LoadJob.LOADED_BYTES));
            summaryProfile.addInfoString("NumRowsAbnormal", loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL));
            summaryProfile.addInfoString("numRowsUnselected", loadCounters.get(LoadJob.UNSELECTED_ROWS));
        }
        ConnectContext session = ConnectContext.get();
        if (session != null) {
            SessionVariable variables = session.getSessionVariable();
            if (variables != null) {
                summaryProfile.addInfoString("NonDefaultSessionVariables", variables.getNonDefaultVariablesJson());
            }
        }
        profile.addChild(summaryProfile);
        if (coord.getQueryProfile() != null) {
            profile.addChild(coord.getQueryProfile());
        }
        ProfileManager.getInstance().pushProfile(null, profile);
    }

    private void beginTxn() throws Exception {
        long timeoutMs = getTimeoutMs();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        OlapTable table;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            table = (OlapTable) db.getTable(tableName);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        this.txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), label, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING, -1,
                timeoutMs / 1000, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    private void rollbackTxn() {
        if (txnId == -1) {
            return;
        }
        try {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                    db.getId(), txnId, "failed to execute group commit");
        } catch (Exception e) {
            LOG.error("Failed to abort transaction {}", txnId, e);
        }
    }

    private long getTimeoutMs() {
        long timeout = Optional.ofNullable(headers.get(TIMEOUT_KEY))
                .map(Long::parseLong)
                .orElse((long) Config.stream_load_default_timeout_second);
        return timeout * 1000L;
    }
}
