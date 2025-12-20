// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

package com.starrocks.load.streamload;

import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.AbstractTxnStateChangeCallback;
import com.starrocks.warehouse.LoadJobWithWarehouse;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for stream load tasks
 */
public abstract class AbstractStreamLoadTask extends AbstractTxnStateChangeCallback
        implements Writable, GsonPostProcessable, GsonPreProcessable, LoadJobWithWarehouse {

    public void beginTxnFromFrontend(TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public void beginTxnFromFrontend(int channelId, int channelNum, TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public void beginTxnFromBackend(TUniqueId requestId, String clientIp, long backendId, TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public TNetworkAddress tryLoad(int channelId, String tableName, TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException();
    }

    public TNetworkAddress executeTask(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public void prepareChannel(int channelId, String tableName, HttpHeaders headers, TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public void waitCoordFinishAndPrepareTxn(long preparedTimeoutMs, TransactionResult resp) {
        throw new UnsupportedOperationException();
    }

    public void commitTxn(HttpHeaders headers, TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException();
    }

    public void manualCancelTask(TransactionResult resp) throws StarRocksException {
        throw new UnsupportedOperationException();
    }

    public boolean checkNeedPrepareTxn() {
        throw new UnsupportedOperationException();
    }

    public boolean isDurableLoadState() {
        throw new UnsupportedOperationException();
    }

    public void cancelAfterRestart()  {
        throw new UnsupportedOperationException();
    }

    public void init() {
        throw new UnsupportedOperationException();
    }

    public abstract boolean checkNeedRemove(long currentMs, boolean isForce);

    // Common getters used by StreamLoadMgr
    public abstract long getId();
    public abstract String getLabel();
    public abstract String getDBName();
    public abstract long getDBId();
    public abstract long getTxnId();
    public abstract String getTableName();
    public abstract String getStateName();
    public abstract boolean isFinalState();
    public abstract long createTimeMs();
    public abstract long endTimeMs();
    public abstract long getFinishTimestampMs();
    public abstract String getStringByType();

    // =============== observability ===============

    // for information_schema.loads
    public abstract  List<TLoadInfo> toThrift();

    // for ShowExecutor.visitShowStreamLoadStatement and StreamLoadsLabelProcDir which are deprecated,
    // return empty by default
    public List<List<String>> getShowInfo() {
        return Collections.emptyList();
    }

    // for StreamLoadsProcDir which is deprecated, return empty by default
    public List<List<String>> getShowBriefInfo() {
        return Collections.emptyList();
    }

    // for information_schema.stream_loads which is deprecated, return empty by default
    public List<TStreamLoadInfo> toStreamLoadThrift() {
        return Collections.emptyList();
    }
}
