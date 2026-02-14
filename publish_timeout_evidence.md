# 事实证据文档：Transaction Publish Timeout 事件

> 本文档仅记录从日志、heap dump、线程栈、对象字段中采集到的原始事实信息，不包含推断性分析。

---

## 1. 日志记录

### 1.1 异常日志：MV 刷新失败（IllegalMonitorStateException）

- **时间**: 2026-02-11 21:17:41.562+08:00
- **线程**: starrocks-taskrun-pool-114 | tid=56318
- **级别**: WARN
- **位置**: TaskRun.executeTaskRun():423

```
2026-02-11 21:17:41.562+08:00 WARN (starrocks-taskrun-pool-114|56318) [TaskRun.executeTaskRun():423] Failed to execute task run, task_id: 308185, task_run_id: 019c4cd9-aebc-76f0-87de-a390f49850bd, failCount:1
com.starrocks.sql.common.DmlException: Refresh mv mv_5level_level_2 failed after 1 times, try lock failed: 0, error-msg : java.lang.IllegalMonitorStateException: Attempt to unlock lock, not locked by current locker
       at com.starrocks.common.util.concurrent.lock.LockManager.release(LockManager.java:301)
       at com.starrocks.common.util.concurrent.lock.Locker.release(Locker.java:109)
       at com.starrocks.common.util.concurrent.lock.Locker.unLockTablesWithIntensiveDbLock(Locker.java:370)
       at com.starrocks.common.util.concurrent.lock.Locker.unLockTableWithIntensiveDbLock(Locker.java:379)
       at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:1425)
       at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:996)
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:559)
[wrapped] com.starrocks.sql.common.DmlException: Expression add partition failed: Attempt to unlock lock, not locked by current locker, db: test_mv_async_db_040cc214_074c_11f1_a471_00163e0e489a, table: mv_5level_level_2
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:563)
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.syncAddOrDropPartitions(MVPCTRefreshRangePartitioner.java:153)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncPartitions(BaseMVRefreshProcessor.java:447)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncAndCheckPCTPartitions(BaseMVRefreshProcessor.java:772)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncAndCheckPCTPartitions(BaseMVRefreshProcessor.java:386)
       at com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor.getProcessExecPlan(MVPCTBasedRefreshProcessor.java:97)
       at com.starrocks.scheduler.MVTaskRunProcessor.doProcessTaskRun(MVTaskRunProcessor.java:311)
       at com.starrocks.scheduler.MVTaskRunProcessor.retryProcessTaskRun(MVTaskRunProcessor.java:278)
       at com.starrocks.scheduler.MVTaskRunProcessor.processTaskRun(MVTaskRunProcessor.java:193)
       at com.starrocks.scheduler.TaskRun.doExecuteTaskRun(TaskRun.java:455)
       at com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:415)
       at com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$1(TaskRunExecutor.java:66)
       at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768)
       at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
       at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
       at java.base/java.lang.Thread.run(Thread.java:840)
       at com.starrocks.scheduler.MVTaskRunProcessor.retryProcessTaskRun(MVTaskRunProcessor.java:298)
       at com.starrocks.scheduler.MVTaskRunProcessor.processTaskRun(MVTaskRunProcessor.java:193)
       at com.starrocks.scheduler.TaskRun.doExecuteTaskRun(TaskRun.java:455)
       at com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:415)
       at com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$1(TaskRunExecutor.java:66)
       at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768)
       at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
       at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
       at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: com.starrocks.sql.common.DmlException: Expression add partition failed: Attempt to unlock lock, not locked by current locker, db: test_mv_async_db_040cc214_074c_11f1_a471_00163e0e489a, table: mv_5level_level_2
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:563)
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.syncAddOrDropPartitions(MVPCTRefreshRangePartitioner.java:153)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncPartitions(BaseMVRefreshProcessor.java:447)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncAndCheckPCTPartitions(BaseMVRefreshProcessor.java:772)
       at com.starrocks.scheduler.mv.BaseMVRefreshProcessor.syncAndCheckPCTPartitions(BaseMVRefreshProcessor.java:386)
       at com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor.getProcessExecPlan(MVPCTBasedRefreshProcessor.java:97)
       at com.starrocks.scheduler.MVTaskRunProcessor.doProcessTaskRun(MVTaskRunProcessor.java:311)
       at com.starrocks.scheduler.MVTaskRunProcessor.retryProcessTaskRun(MVTaskRunProcessor.java:278)
       ... 8 more
Caused by: java.lang.IllegalMonitorStateException: Attempt to unlock lock, not locked by current locker
       at com.starrocks.common.util.concurrent.lock.LockManager.release(LockManager.java:301)
       at com.starrocks.common.util.concurrent.lock.Locker.release(Locker.java:109)
       at com.starrocks.common.util.concurrent.lock.Locker.unLockTablesWithIntensiveDbLock(Locker.java:370)
       at com.starrocks.common.util.concurrent.lock.Locker.unLockTableWithIntensiveDbLock(Locker.java:379)
       at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:1425)
       at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:996)
       at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:559)
       ... 15 more
```

**关键信息**:
- task_id: 308185
- task_run_id: 019c4cd9-aebc-76f0-87de-a390f49850bd
- MV 名称: mv_5level_level_2
- 数据库名: test_mv_async_db_040cc214_074c_11f1_a471_00163e0e489a
- failCount: 1
- 异常类型: IllegalMonitorStateException
- 异常位置: LockManager.release(LockManager.java:301)
- 释放锁的方法: Locker.unLockTablesWithIntensiveDbLock(Locker.java:370) → Locker.unLockTableWithIntensiveDbLock(Locker.java:379) → LocalMetastore.addPartitions(LocalMetastore.java:1425)

### 1.2 事务开始日志：txn 41505

- **时间**: 2026-02-11 21:17:43.718+08:00
- **线程**: starrocks-taskrun-pool-114 | tid=56318
- **级别**: INFO
- **位置**: DatabaseTransactionMgr.beginTransaction():192

```
2026-02-11 21:17:43.718+08:00 INFO (starrocks-taskrun-pool-114|56318) [DatabaseTransactionMgr.beginTransaction():192]
begin transaction: txn_id: 41505 with label insert_019c4cd9-b2f6-7d91-b3f0-adce27946c8b
from coordinator FE: 172.26.201.186, listner id: -1
```

**关键信息**:
- txn_id: 41505
- label: insert_019c4cd9-b2f6-7d91-b3f0-adce27946c8b
- coordinator: FE: 172.26.201.186
- 与 1.1 中的异常在同一线程 starrocks-taskrun-pool-114（tid=56318）
- 时间间隔: 距异常日志约 2.156 秒后

### 1.3 事务提交日志：txn 41505

- **时间**: 2026-02-11 21:17:43.757+08:00
- **线程**: starrocks-taskrun-pool-114 | tid=56318
- **级别**: INFO
- **位置**: DatabaseTransactionMgr.commitPreparedTransaction():547

```
2026-02-11 21:17:43.757+08:00 INFO (starrocks-taskrun-pool-114|56318) [DatabaseTransactionMgr.commitPreparedTransaction():547]
transaction:[TransactionState. txn_id: 41505, label: insert_019c4cd9-b2f6-7d91-b3f0-adce27946c8b,
db id: 307648, table id list: 308183, load id list: null, callback id: [-1, 308340],
coordinator: FE: 172.26.201.186, transaction status: COMMITTED,
error replicas num: 0, unknown replicas num: 0,
prepare time: 1770815863718, write end time: 1770815863753,
allow commit time: 1770815863753, commit time: 1770815863753, finish time: -1,
write cost: 35ms, reason: ,
attachment: com.starrocks.transaction.InsertTxnCommitAttachment@21808ed,
partition commit info:[], warehouse: 0] successfully committed
```

**关键信息**:
- txn_id: 41505
- db id: 307648
- table id: 308183
- callback id: [-1, 308340]
- transaction status: COMMITTED
- prepare time: 1770815863718
- commit time: 1770815863753
- write cost: 35ms
- attachment 类型: InsertTxnCommitAttachment
- partition commit info: 空（`[]`）

### 1.4 事务提交日志：txn 62316

- **时间**: 2026-02-12 11:06:54.375+08:00
- **线程**: thrift-server-pool-7 | tid=286
- **级别**: INFO
- **位置**: DatabaseTransactionMgr.commitPreparedTransaction():547

```
2026-02-12 11:06:54.375+08:00 INFO (thrift-server-pool-7|286) [DatabaseTransactionMgr.commitPreparedTransaction():547]
transaction:[TransactionState. txn_id: 62316, label: view_1770865614152,
db id: 335339, table id list: 335341, load id list: null, callback id: [335347],
coordinator: BE: 172.26.201.184, transaction status: COMMITTED,
error replicas num: 0, unknown replicas num: 0,
prepare time: 1770865614169, write end time: 1770865614369,
allow commit time: 1770865614369, commit time: 1770865614369, finish time: -1,
write cost: 200ms, reason: ,
attachment: com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment@7cb6d82c,
tabletCommitInfos size: 3,
partition commit info:[partitionId=335343, version=2, versionTime=0, isDoubleWrite=false,],
warehouse: 0] successfully committed
```

**关键信息**:
- txn_id: 62316
- db id: 335339
- table id: 335341
- callback id: [335347]
- coordinator: BE: 172.26.201.184
- transaction status: COMMITTED
- prepare time: 1770865614169
- commit time: 1770865614369
- write cost: 200ms
- attachment 类型: ManualLoadTxnCommitAttachment
- tabletCommitInfos size: 3
- partition commit info: partitionId=335343, version=2

### 1.5 Publish Timeout 告警日志

- **时间**: 2026-02-12 11:07:39.370+08:00
- **线程**: thrift-server-pool-7 | tid=286
- **级别**: WARN
- **位置**: FrontendServiceImpl.loadTxnCommitImpl():1317

```
2026-02-12 11:07:39.370+08:00 WARN (thrift-server-pool-7|286) [FrontendServiceImpl.loadTxnCommitImpl():1317]
txn 62316 publish timeout txn has not sent publish tasks yet,
maybe waiting previous txns on the same table(s) to finish, tableIds: 335341
```

**关键信息**:
- txn_id: 62316
- 超时原因: "txn has not sent publish tasks yet"
- tableIds: 335341
- 与 1.4 commit 日志时间差: 约 44.995 秒

---

## 2. Heap Dump 观察

### 2.1 TransactionState#3915（txn 62316）对象字段

来源: VisualVM heap dump 对象检查器

| 字段 | 值 |
|------|------|
| 对象实例 | com.starrocks.transaction.TransactionState#3915 |
| transactionId | 62316 |
| dbId | 335339 |
| label | java.lang.String#300841 : "view_1770865614152" |
| transactionStatus | TransactionStatus#4 : COMMITTED (ordinal=2) |
| sourceType | TransactionState$LoadJobSourceType#11 : BACKEND_STREAMING (ordinal=1) |
| tableIdList | java.util.ArrayList#147814 : 1 element |
| callbackIdList | java.util.ArrayList#147827 : 1 element |
| txnCoordinator | TransactionState$TxnCoordinator#3898 |
| requestId | com.starrocks.thrift.TUniqueId#2349 |
| txnLock | java.util.concurrent.locks.ReentrantReadWriteLock#17661 |
| errMsg | java.lang.String#3525769 |
| reason | java.lang.String#3525769 |
| computeResource | com.starrocks.epack.warehouse.cngroup.CNGroupResource#10578 |
| txnCommitAttachment | ManualLoadTxnCommitAttachment#110 |
| tabletIdToTTabletLocation | ConcurrentHashMap#20853 : 0 elements |
| tableToPartitionNameToPartition | ConcurrentHashMap#20852 : 0 elements |
| tableToCreatedPartitionNames | java.util.HashMap#139402 : 0 elements |
| loadedTblPartitionIndexes | java.util.HashMap#139400 : 1 element |
| publishVersionTasks | java.util.HashMap#139399 : **0 elements** |
| idToTableCommitInfos | java.util.HashMap#139393 : 1 element |
| tabletCommitInfos | java.util.HashSet#30724 : 3 elements |
| unknownReplicas | java.util.HashSet#30725 : 0 elements |
| errorReplicas | java.util.HashSet#30723 : 0 elements |
| isCreatePartitionFailed | AtomicBoolean#11557 : false |
| txnSpan | PropagatedSpan#1 |
| latch | CountDownLatch#3969 |
| finishChecker | null |
| finishState | null |
| traceParent | null |
| loadIds | null |
| lastErrTimeMs | 0 |
| warehouseId | 0 |
| preparedTimeoutMs | -1 |
| timeoutMs | 6000000 |
| callbackId | 335347 |
| allowCommitTimeMs | 1770865614369 |
| writeDurationMs | 200 |
| writeEndTimeMs | 1770865614369 |
| publishVersionFinishTime | -1 |
| publishVersionTime | -1 |
| **hasSendTask** | **false** |
| useCombinedTxnLog | true |
| newFinish | false |
| globalTransactionId | 404810764111577088 |
| finishTime | -1 |
| commitTime | 1770865614369 |
| preparedTime | 1770865614369 |
| prepareTime | 1770865614169 |

**注意**: `publishVersionTasks` 为空（0 elements），`hasSendTask = false`，`publishVersionFinishTime = -1`，`publishVersionTime = -1`。

### 2.2 DatabaseTransactionMgr（db 335339）状态

来源: VisualVM OQL 查询 `select x from com.starrocks.transaction.DatabaseTransactionMgr x where x.dbId == 335339`

| 字段 | 值 |
|------|------|
| dbId | 335339 |
| idToRunningTransactionState.size | 4 |
| 所有 running 事务状态 | 全部为 COMMITTED |
| 数据库下的表 | 仅 1 个表（table 335341） |
| transactionGraph.nodes.size | 4 |
| transactionGraph.nodesWithoutIns.size | **1** |

**说明**: 4 个 COMMITTED 事务全部属于 table 335341，但 `nodesWithoutIns` 仅 1 个，说明只有队首 1 个事务（txn 62316）具备被 publish 的条件，其余 3 个依赖它。

---

## 3. 线程栈信息

### 3.1 publish-version-daemon（tid=26）

- **线程状态**: WAITING
- **阻塞位置**: LockManager.lockAcquireSlowPath(LockManager.java:174)

```
"publish-version-daemon" daemon prio=5 tid=26 WAITING
    at java.lang.Object.wait(Native Method)
    at com.starrocks.common.util.concurrent.lock.LockManager.lockAcquireSlowPath(LockManager.java:174)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
       local variable: com.starrocks.common.util.concurrent.lock.LockType#3
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
    at com.starrocks.common.util.concurrent.lock.LockManager.lock(LockManager.java:142)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
       local variable: com.starrocks.common.util.concurrent.lock.LockType#3
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
    at com.starrocks.common.util.concurrent.lock.Locker.lock(Locker.java:93)
    at com.starrocks.common.util.concurrent.lock.Locker.lockTablesWithIntensiveDbLock(Locker.java:300)
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
       local variable: com.starrocks.common.util.concurrent.lock.LockType#3
    at com.starrocks.transaction.DatabaseTransactionMgr.finishTransaction(DatabaseTransactionMgr.java:1131)
       local variable: com.starrocks.transaction.DatabaseTransactionMgr#2667
       local variable: java.util.HashSet#179532
       local variable: com.starrocks.transaction.TransactionState#27109
       local variable: com.starrocks.catalog.Database#5940
       local variable: io.opentelemetry.api.trace.PropagatedSpan#1
       local variable: java.util.ArrayList#959775
       local variable: com.starrocks.common.util.concurrent.lock.Locker#18
    at com.starrocks.transaction.GlobalTransactionMgr.finishTransaction(GlobalTransactionMgr.java:601)
    at com.starrocks.transaction.PublishVersionDaemon.lambda$publishLakeTransactionAsync$7(PublishVersionDaemon.java:506)
    at com.starrocks.transaction.PublishVersionDaemon$$Lambda$1349+0x00002aabe9cb1740.accept(<unresolved>)
    at java.util.concurrent.CompletableFuture.uniAcceptNow(CompletableFuture.java:757)
       local variable: java.util.concurrent.CompletableFuture#100469
    at java.util.concurrent.CompletableFuture.uniAcceptStage(CompletableFuture.java:735)
    at java.util.concurrent.CompletableFuture.thenAccept(CompletableFuture.java:2182)
    at com.starrocks.transaction.PublishVersionDaemon.publishLakeTransactionAsync(PublishVersionDaemon.java:502)
    at com.starrocks.transaction.PublishVersionDaemon.publishVersionForLakeTableBatch(PublishVersionDaemon.java:437)
       local variable: com.starrocks.transaction.PublishVersionDaemon#1
       local variable: java.util.concurrent.ConcurrentHashMap$KeySetView#215
       local variable: java.util.concurrent.ConcurrentHashMap$KeySetView#216
       local variable: java.util.ArrayList$Itr#40
       local variable: java.util.ArrayList#959775
    at com.starrocks.transaction.PublishVersionDaemon.runAfterCatalogReady(PublishVersionDaemon.java:123)
    at com.starrocks.common.util.FrontendDaemon.runOneCycle(FrontendDaemon.java:78)
    at com.starrocks.common.util.Daemon.run(Daemon.java:98)
```

**局部变量关键信息**:
- Locker: Locker#18
- LockType: LockType#3
- 操作的事务: TransactionState#27109
- 操作的数据库: Database#5940
- 操作的 DatabaseTransactionMgr: DatabaseTransactionMgr#2667

### 3.2 auto-vacuum（tid=60）

- **线程状态**: WAITING
- **阻塞位置**: LockManager.lockAcquireSlowPath(LockManager.java:174)

```
"auto-vacuum" daemon prio=5 tid=60 WAITING
    at java.lang.Object.wait(Native Method)
    at com.starrocks.common.util.concurrent.lock.LockManager.lockAcquireSlowPath(LockManager.java:174)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
    at com.starrocks.common.util.concurrent.lock.LockManager.lock(LockManager.java:142)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
    at com.starrocks.common.util.concurrent.lock.Locker.lock(Locker.java:93)
    at com.starrocks.common.util.concurrent.lock.Locker.lockTablesWithIntensiveDbLock(Locker.java:300)
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
    at com.starrocks.lake.vacuum.AutovacuumDaemon.vacuumTable(AutovacuumDaemon.java:149)
       local variable: com.starrocks.lake.vacuum.AutovacuumDaemon#1
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: com.starrocks.common.util.concurrent.lock.Locker#13
    at com.starrocks.lake.vacuum.AutovacuumDaemon.runAfterCatalogReady(AutovacuumDaemon.java:102)
       local variable: com.starrocks.lake.vacuum.AutovacuumDaemon#1
       local variable: com.starrocks.catalog.Database#5940
    at com.starrocks.common.util.FrontendDaemon.runOneCycle(FrontendDaemon.java:78)
    at com.starrocks.common.util.Daemon.run(Daemon.java:98)
       local variable: com.starrocks.lake.vacuum.AutovacuumDaemon#1
```

**局部变量关键信息**:
- Locker: Locker#13
- LockType: LockType#4（在 lockAcquireSlowPath 层），LockType#4（在 lockTablesWithIntensiveDbLock 层）
- 操作的数据库: Database#5940
- 操作的表: LakeMaterializedView#370

### 3.3 database-quota-refresher（tid=27）

- **线程状态**: WAITING
- **阻塞位置**: LockManager.lockAcquireSlowPath(LockManager.java:174)

```
"database-quota-refresher" daemon prio=5 tid=27 WAITING
    at java.lang.Object.wait(Native Method)
    at com.starrocks.common.util.concurrent.lock.LockManager.lockAcquireSlowPath(LockManager.java:174)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#17
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
       local variable: com.starrocks.common.util.concurrent.lock.Locker#17
    at com.starrocks.common.util.concurrent.lock.LockManager.lock(LockManager.java:142)
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
    at com.starrocks.common.util.concurrent.lock.Locker.lock(Locker.java:93)
    at com.starrocks.common.util.concurrent.lock.Locker.lockTablesWithIntensiveDbLock(Locker.java:295)
       local variable: com.starrocks.common.util.concurrent.lock.Locker#17
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
    at com.starrocks.common.util.concurrent.lock.AutoCloseableLock.<init>(AutoCloseableLock.java:36)
    at com.starrocks.server.DatabaseQuotaRefresher.getUsedDataQuota(DatabaseQuotaRefresher.java:92)
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.lake.LakeMaterializedView#375
       local variable: com.starrocks.common.util.concurrent.lock.AutoCloseableLock#1
    at com.starrocks.server.DatabaseQuotaRefresher.updateAllDatabaseUsedDataQuota(DatabaseQuotaRefresher.java:69)
       local variable: com.starrocks.server.DatabaseQuotaRefresher#1
       local variable: com.starrocks.server.GlobalStateMgr#1
       local variable: java.util.ArrayList#847944
       local variable: java.util.ArrayList$Itr#39
       local variable: java.lang.Long#1193956
       local variable: com.starrocks.catalog.Database#5940
    at com.starrocks.server.DatabaseQuotaRefresher.runAfterCatalogReady(DatabaseQuotaRefresher.java:41)
       local variable: com.starrocks.server.DatabaseQuotaRefresher#1
    at com.starrocks.common.util.FrontendDaemon.runOneCycle(FrontendDaemon.java:78)
    at com.starrocks.common.util.Daemon.run(Daemon.java:98)
       local variable: com.starrocks.server.DatabaseQuotaRefresher#1
```

**局部变量关键信息**:
- Locker: Locker#17
- LockType: LockType#2（在 lockAcquireSlowPath 层），LockType#4（在 lockTablesWithIntensiveDbLock 层）
- 操作的数据库: Database#5940
- 操作的表: LakeMaterializedView#375

### 3.4 compaction-dispatch（tid=161）

- **线程状态**: WAITING
- **阻塞位置**: LockManager.lockAcquireSlowPath(LockManager.java:174)

```
"compaction-dispatch" daemon prio=5 tid=161 WAITING
    at java.lang.Object.wait(Native Method)
    at com.starrocks.common.util.concurrent.lock.LockManager.lockAcquireSlowPath(LockManager.java:174)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
    at com.starrocks.common.util.concurrent.lock.LockManager.lock(LockManager.java:142)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
       local variable: com.starrocks.common.util.concurrent.lock.LockGrantType#2
    at com.starrocks.common.util.concurrent.lock.Locker.lock(Locker.java:93)
    at com.starrocks.common.util.concurrent.lock.Locker.lockTablesWithIntensiveDbLock(Locker.java:295)
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
    at com.starrocks.sql.common.MetaUtils.isPhysicalPartitionExist(MetaUtils.java:219)
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.lake.LakeMaterializedView#375
       local variable: com.starrocks.common.util.concurrent.lock.Locker#19
    at com.starrocks.lake.compaction.CompactionScheduler.lambda$cleanPhysicalPartition$0(CompactionScheduler.java:282)
    at java.util.stream.ReferencePipeline$2$1.accept(ReferencePipeline.java:178)
       local variable: com.starrocks.lake.compaction.PartitionIdentifier#7001
    at java.util.HashMap$KeySpliterator.forEachRemaining(HashMap.java:1707)
    at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:509)
    at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:499)
    at java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:921)
    at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)
    at java.util.stream.ReferencePipeline.collect(ReferencePipeline.java:682)
    at com.starrocks.lake.compaction.CompactionScheduler.cleanPhysicalPartition(CompactionScheduler.java:283)
       local variable: com.starrocks.lake.compaction.CompactionScheduler#1
    at com.starrocks.lake.compaction.CompactionScheduler.runOneCycle(CompactionScheduler.java:112)
       local variable: com.starrocks.lake.compaction.CompactionScheduler#1
    at com.starrocks.common.util.Daemon.run(Daemon.java:98)
       local variable: com.starrocks.lake.compaction.CompactionScheduler#1
```

**局部变量关键信息**:
- Locker: Locker#19
- LockType: LockType#2（在 lockAcquireSlowPath 层），LockType#4（在 lockTablesWithIntensiveDbLock 层）
- 操作的数据库: Database#5940
- 操作的表: LakeMaterializedView#375

### 3.5 starrocks-taskrun-pool-114（tid=56318）

- **线程状态**: WAITING
- **阻塞位置**: LockManager.lockAcquireSlowPath(LockManager.java:174)

```
"starrocks-taskrun-pool-114" daemon prio=5 tid=56318 WAITING
    at java.lang.Object.wait(Native Method)
    at com.starrocks.common.util.concurrent.lock.LockManager.lockAcquireSlowPath(LockManager.java:174)
       local variable: com.starrocks.common.util.concurrent.lock.LockManager#1
       local variable: com.starrocks.common.util.concurrent.lock.Locker#6
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
       local variable: com.starrocks.common.util.concurrent.lock.Locker#6
    at com.starrocks.common.util.concurrent.lock.LockManager.lock(LockManager.java:142)
       local variable: com.starrocks.common.util.concurrent.lock.LockType#2
    at com.starrocks.common.util.concurrent.lock.Locker.lock(Locker.java:93)
    at com.starrocks.common.util.concurrent.lock.Locker.lockTablesWithIntensiveDbLock(Locker.java:295)
       local variable: com.starrocks.common.util.concurrent.lock.Locker#6
       local variable: com.starrocks.common.util.concurrent.lock.LockType#4
    at com.starrocks.statistic.StatisticsCollectionTrigger.prepareAnalyzeJobForLoad(StatisticsCollectionTrigger.java:289)
       local variable: com.starrocks.statistic.StatisticsCollectionTrigger#23
       local variable: com.starrocks.transaction.TableCommitInfo#19976
       local variable: com.starrocks.common.util.concurrent.lock.Locker#6
    at com.starrocks.statistic.StatisticsCollectionTrigger.process(StatisticsCollectionTrigger.java:152)
    at com.starrocks.statistic.StatisticsCollectionTrigger.triggerOnFirstLoad(StatisticsCollectionTrigger.java:122)
    at com.starrocks.statistic.StatisticUtils.triggerCollectionOnFirstLoad(StatisticUtils.java:186)
    at com.starrocks.listener.LoadJobStatsListener.onDMLStmtJobTransactionFinish(LoadJobStatsListener.java:58)
    at com.starrocks.listener.GlobalLoadJobListenerBus.lambda$onDMLStmtJobTransactionFinish$1(GlobalLoadJobListenerBus.java:63)
    at java.util.Spliterators$ArraySpliterator.forEachRemaining(Spliterators.java:992)
    at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:762)
    at com.starrocks.listener.GlobalLoadJobListenerBus.onDMLStmtJobTransactionFinish(GlobalLoadJobListenerBus.java:63)
    at com.starrocks.qe.StmtExecutor.handleDMLStmt(StmtExecutor.java:3253)
       local variable: com.starrocks.qe.StmtExecutor#191
       local variable: com.starrocks.qe.DmlType#5
       local variable: java.lang.String#40385
       local variable: java.lang.String#2291967
       local variable: java.lang.String#2291968
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.transaction.GlobalTransactionMgr#1
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: com.starrocks.transaction.TransactionState#27109
       local variable: java.lang.String#2291969
       local variable: com.starrocks.transaction.TransactionStatus#4
       local variable: java.lang.String#2291970
    at com.starrocks.load.InsertOverwriteJobRunner.executeInsert(InsertOverwriteJobRunner.java:389)
       local variable: com.starrocks.load.InsertOverwriteJobRunner#1
       local variable: com.starrocks.qe.ConnectContext$ScopeGuard#1
       local variable: com.starrocks.sql.plan.ExecPlan#147
    at com.starrocks.load.InsertOverwriteJobRunner.doLoad(InsertOverwriteJobRunner.java:181)
    at com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:161)
    at com.starrocks.load.InsertOverwriteJobRunner.transferTo(InsertOverwriteJobRunner.java:227)
       local variable: com.starrocks.load.InsertOverwriteJobState#3
    at com.starrocks.load.InsertOverwriteJobRunner.prepare(InsertOverwriteJobRunner.java:280)
       local variable: com.starrocks.load.InsertOverwriteJobRunner#1
       local variable: java.util.ArrayList#75822
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.common.util.concurrent.lock.Locker#4
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: java.util.ArrayList#75823
       local variable: com.starrocks.persist.InsertOverwriteStateChangeInfo#1
    at com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:158)
    at com.starrocks.load.InsertOverwriteJobRunner.run(InsertOverwriteJobRunner.java:146)
    at com.starrocks.load.InsertOverwriteJobMgr.executeJob(InsertOverwriteJobMgr.java:86)
       local variable: com.starrocks.load.InsertOverwriteJobMgr#1
       local variable: com.starrocks.qe.ConnectContext#424
       local variable: com.starrocks.qe.StmtExecutor#191
       local variable: com.starrocks.load.InsertOverwriteJob#1
       local variable: com.starrocks.load.InsertOverwriteJobRunner#1
    at com.starrocks.qe.StmtExecutor.handleInsertOverwrite(StmtExecutor.java:2706)
       local variable: com.starrocks.qe.StmtExecutor#191
       local variable: com.starrocks.sql.ast.InsertStmt#9
       local variable: com.starrocks.sql.ast.TableRef#14
       local variable: com.starrocks.catalog.Database#5940
       local variable: com.starrocks.common.util.concurrent.lock.Locker#5
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: com.starrocks.lake.LakeMaterializedView#370
       local variable: com.starrocks.load.InsertOverwriteJob#1
       local variable: com.starrocks.load.InsertOverwriteJobMgr#1
    at com.starrocks.qe.StmtExecutor.handleDMLStmt(StmtExecutor.java:2826)
    at com.starrocks.qe.StmtExecutor.handleDMLStmtWithProfile(StmtExecutor.java:2715)
       local variable: com.starrocks.sql.plan.ExecPlan#22
    at com.starrocks.scheduler.MVTaskRunProcessor.executePlan(MVTaskRunProcessor.java:353)
       local variable: com.starrocks.scheduler.MVTaskRunProcessor#1
       local variable: com.starrocks.sql.plan.ExecPlan#22
       local variable: com.starrocks.sql.ast.InsertStmt#9
       local variable: com.starrocks.qe.ConnectContext#424
       local variable: com.starrocks.qe.StmtExecutor#191
    at com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor.execProcessExecPlan(MVPCTBasedRefreshProcessor.java:122)
       local variable: com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor#1
       local variable: com.starrocks.scheduler.MvTaskRunContext#1
       local variable: com.starrocks.scheduler.mv.BaseMVRefreshProcessor$ProcessExecPlan#1
       local variable: com.starrocks.scheduler.MVTaskRunProcessor#1
       local variable: com.starrocks.sql.plan.ExecPlan#22
       local variable: com.starrocks.common.profile.TimeWatcher$ScopedTimer#339
       local variable: com.starrocks.sql.ast.InsertStmt#9
    at com.starrocks.scheduler.MVTaskRunProcessor.doProcessTaskRun(MVTaskRunProcessor.java:320)
       local variable: com.starrocks.scheduler.MVTaskRunProcessor#1
       local variable: com.starrocks.scheduler.MvTaskRunContext#1
       local variable: com.google.common.base.Stopwatch#381
       local variable: com.starrocks.scheduler.mv.BaseMVRefreshProcessor$ProcessExecPlan#1
    at com.starrocks.scheduler.MVTaskRunProcessor.retryProcessTaskRun(MVTaskRunProcessor.java:278)
       local variable: java.lang.String#284425
    at com.starrocks.scheduler.MVTaskRunProcessor.processTaskRun(MVTaskRunProcessor.java:193)
       local variable: com.starrocks.scheduler.MvTaskRunContext#1
       local variable: com.starrocks.sql.common.QueryDebugOptions#1
       local variable: com.starrocks.common.profile.Tracers$Mode#3
       local variable: com.starrocks.common.profile.Tracers$Module#8
       local variable: com.starrocks.qe.ConnectContext#424
       local variable: com.starrocks.sql.optimizer.QueryMaterializationContext#1
       local variable: com.starrocks.common.profile.TimeWatcher$ScopedTimer#340
    at com.starrocks.scheduler.TaskRun.doExecuteTaskRun(TaskRun.java:455)
       local variable: com.starrocks.scheduler.TaskRun#1
       local variable: com.starrocks.scheduler.MvTaskRunContext#1
       local variable: com.starrocks.common.profile.Timer#1
    at com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:415)
    at com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$1(TaskRunExecutor.java:66)
       local variable: com.starrocks.scheduler.TaskRun#1
       local variable: com.starrocks.scheduler.persist.TaskRunStatus#6473
    at java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
    at java.lang.Thread.run(Thread.java:840)
```

**局部变量关键信息**:
- 阻塞的 Locker: Locker#6
- 阻塞的 LockType: LockType#2（在 lockAcquireSlowPath 层），LockType#4（在 lockTablesWithIntensiveDbLock 层）
- handleDMLStmt 中的 TransactionState: TransactionState#27109
- handleDMLStmt 中的 DmlType: DmlType#5
- handleDMLStmt 中的 TransactionStatus: TransactionStatus#4
- handleDMLStmt 中的 Database: Database#5940
- handleDMLStmt 中的 Table: LakeMaterializedView#370
- handleInsertOverwrite 中的 Locker: Locker#5
- prepare 中的 Locker: Locker#4

**该线程中可见的全部 Locker 实例**:
- Locker#6: 在 prepareAnalyzeJobForLoad 中，当前正在等待
- Locker#5: 在 handleInsertOverwrite 中
- Locker#4: 在 InsertOverwriteJobRunner.prepare 中

---

## 4. LockManager 中的 MultiUserLock 状态

### 4.1 MultiUserLock#1 — key=307648（db 307648）

来源: LockManager.lockTables 数组，索引 [192]，HashMap#1416 中 key=Long#876438:307648

#### 4.1.1 firstWaiter

| 字段 | 值 |
|------|------|
| 实例 | LockHolder#10 |
| locker | Locker#14 |
| locker.lockerThread | com.starrocks.consistency.ConsistencyChecker#1 : consistency-checker |
| locker.threadName | java.lang.String#37865 : "consistency-checker" |
| locker.threadId | 23 |
| locker.waitingForRid | java.lang.Long#523919 : 307648 |
| locker.waitingForType | LockType#4 |
| locker.queryId | null |
| locker.lockRequestTimeMs | 1770815872055 |
| lockType | LockType#4 |
| lockAcquireTimeMs | 0 |
| refCount | 1 |

#### 4.1.2 firstOwner

| 字段 | 值 |
|------|------|
| 实例 | LockHolder#8 |
| locker | Locker#13 |
| locker.lockerThread | com.starrocks.lake.vacuum.AutovacuumDaemon#1 : auto-vacuum |
| locker.threadName | java.lang.String#55824 : "auto-vacuum" |
| locker.threadId | 60 |
| locker.waitingForRid | java.lang.Long#523921 : 308183 |
| locker.waitingForType | LockType#4 |
| locker.queryId | null |
| locker.lockRequestTimeMs | 1770815861112 |
| lockType | LockType#2 |
| lockAcquireTimeMs | 1770815861112 |
| refCount | 1 |

#### 4.1.3 otherOwners（1 element）

| 字段 | 值 |
|------|------|
| 实例 | LockHolder#9（在 HashSet#116420 → HashMap#552749 → Node#1227407） |
| locker | Locker#18 |
| locker.lockerThread | com.starrocks.transaction.PublishVersionDaemon#1 : publish-version-daemon |
| locker.threadName | java.lang.String#37874 : "publish-version-daemon" |
| locker.threadId | 26 |
| locker.waitingForRid | java.lang.Long#1198711 : 308183 |
| locker.waitingForType | LockType#3 |
| locker.queryId | null |
| locker.lockRequestTimeMs | 1770815863758 |
| lockType | LockType#1 |
| lockAcquireTimeMs | 1770815863758 |
| refCount | 1 |

#### 4.1.4 otherWaiters（13 elements）

展开的 elementData（java.lang.Object[]#491242，15 items，size=13）：

| 索引 | LockHolder 实例 | Locker | 线程 | 说明 |
|------|----------------|--------|------|------|
| [0] | LockHolder#14 | - | - | - |
| [1] | LockHolder#13 | - | - | - |
| [2] | LockHolder#12 | - | - | - |
| [3] | LockHolder#11 | - | - | - |
| [4] | LockHolder#16 | - | - | - |
| [5] | LockHolder#15 | - | - | - |
| **[6]** | **LockHolder#1** | **Locker#6** | **starrocks-taskrun-pool-114 (tid=56318)** | waitingForRid=Long#105618:307648, waitingForType=LockType#2, lockRequestTimeMs=1770830273281 |
| [7] | LockHolder#7 | - | - | - |
| [8] | LockHolder#6 | - | - | - |
| [9] | LockHolder#5 | - | - | - |
| [10] | LockHolder#4 | - | - | - |
| [11] | LockHolder#3 | - | - | - |
| [12] | LockHolder#2 | - | - | - |
| [13] | null | - | - | - |
| [14] | null | - | - | - |

**Locker#6 详细字段**（otherWaiters[6]）:

| 字段 | 值 |
|------|------|
| locker | Locker#6 |
| locker.lockerThread | java.lang.Thread#10097 : starrocks-taskrun-pool-114 |
| locker.threadName | java.lang.String#2532273 : "starrocks-taskrun-pool-114" |
| locker.threadId | 56318 |
| locker.waitingForRid | java.lang.Long#105618 : 307648 |
| locker.waitingForType | LockType#2 |
| locker.lockRequestTimeMs | 1770830273281 |
| lockType | LockType#2 |
| lockAcquireTimeMs | 0 |
| refCount | 1 |

### 4.2 MultiUserLock#2 — key=308183（table 308183）

来源: LockManager.lockTables 数组，索引 [215]，HashMap#1493 中 key=Long#1177140:308183

#### 4.2.1 firstWaiter

| 字段 | 值 |
|------|------|
| 实例 | LockHolder#18 |
| locker | Locker#13 |
| locker.lockerThread | com.starrocks.lake.vacuum.AutovacuumDaemon#1 : auto-vacuum |
| locker.threadName | java.lang.String#55824 : "auto-vacuum" |
| locker.threadId | 60 |
| locker.waitingForRid | java.lang.Long#523921 : 308183 |
| locker.waitingForType | LockType#4 |
| locker.queryId | null |
| locker.lockRequestTimeMs | 1770815861112 |
| lockType | LockType#4 |
| lockAcquireTimeMs | 0 |
| refCount | 1 |

#### 4.2.2 firstOwner

| 字段 | 值 |
|------|------|
| 实例 | LockHolder#17 |
| locker | **Locker#12** |
| locker.lockerThread | java.lang.Thread#10097 : **starrocks-taskrun-pool-114** |
| locker.threadName | java.lang.String#2532273 : "starrocks-taskrun-pool-114" |
| locker.threadId | **56318** |
| locker.queryId | null |
| locker.waitingForType | **null** |
| locker.waitingForRid | **null** |
| locker.lockRequestTimeMs | **1770815860557** |
| lockType | **LockType#3** |
| lockAcquireTimeMs | **1770815860557** |
| refCount | **1** |

#### 4.2.3 otherWaiters（1 element）

| 字段 | 值 |
|------|------|
| 实例 | ArrayList#843015 : 1 element |
| 内容 | （未完全展开） |

#### 4.2.4 otherOwners

| 字段 | 值 |
|------|------|
| 实例 | HashSet#156564 : 0 elements |

---

## 5. Locker 对象详细信息

### 5.1 Locker#12（table 308183 的 WRITE 锁持有者）

来源: VisualVM 对象检查器

| 字段 | 值 |
|------|------|
| 对象实例 | com.starrocks.common.util.concurrent.lock.Locker#12 |
| lockerThread | java.lang.Thread#10097 : starrocks-taskrun-pool-114 |
| threadName | java.lang.String#2532273 : "starrocks-taskrun-pool-114" |
| threadId | 56318 |
| queryId | null |
| lockRequestTimeMs | 1770815860557 |
| waitingForType | **null** |
| waitingForRid | **null** |

**引用关系（references）**:
- locker in com.starrocks.common.util.concurrent.lock.LockHolder#17
  - firstOwner in com.starrocks.common.util.concurrent.lock.MultiUserLock#2
    - value in java.util.HashMap$Node#1582837
      - [3] in java.util.HashMap$Node[]#6543 : 16 items
        - table in java.util.HashMap#1493 : 1 element

**说明**: Locker#12 仅被 MultiUserLock#2 的 firstOwner（LockHolder#17）引用，不出现在任何线程栈的局部变量中。`waitingForType = null` 且 `waitingForRid = null` 表示该 Locker 不在等待任何锁。

### 5.2 Locker#18（publish-version-daemon）

| 字段 | 值 |
|------|------|
| lockerThread | PublishVersionDaemon#1 : publish-version-daemon |
| threadName | "publish-version-daemon" |
| threadId | 26 |
| waitingForRid | Long#1198711 : 308183 |
| waitingForType | LockType#3 |
| lockRequestTimeMs | 1770815863758 |

### 5.3 Locker#13（auto-vacuum）

| 字段 | 值 |
|------|------|
| lockerThread | AutovacuumDaemon#1 : auto-vacuum |
| threadName | "auto-vacuum" |
| threadId | 60 |
| waitingForRid | Long#523921 : 308183 |
| waitingForType | LockType#4 |
| lockRequestTimeMs | 1770815861112 |

### 5.4 Locker#6（starrocks-taskrun-pool-114，等待中）

| 字段 | 值 |
|------|------|
| lockerThread | Thread#10097 : starrocks-taskrun-pool-114 |
| threadName | "starrocks-taskrun-pool-114" |
| threadId | 56318 |
| waitingForRid | Long#105618 : 307648 |
| waitingForType | LockType#2 |
| lockRequestTimeMs | 1770830273281 |

**注意**: Locker#6 和 Locker#12 拥有相同的 threadId (56318) 和相同的 lockerThread (Thread#10097)，但是不同的 Locker 对象实例。

---

## 6. TransactionState#27109 信息

来源: heap dump 对象检查，由用户确认

| 字段 | 值 |
|------|------|
| 对象实例 | com.starrocks.transaction.TransactionState#27109 |
| transactionId | 41505 |
| dbId | 307648 |
| table id | 308183 |
| transactionStatus | COMMITTED |

**关联**:
- `publish-version-daemon` 线程栈中 `finishTransaction` 的局部变量引用了该对象
- `starrocks-taskrun-pool-114` 线程栈中 `handleDMLStmt` 的局部变量引用了同一对象

---

## 7. LockType 实例映射

从 heap dump 中各处观察到的 LockType 实例编号与其含义的对应关系：

| 实例编号 | LockType | 说明 |
|---------|----------|------|
| LockType#1 | INTENTION_EXCLUSIVE | 数据库级意向排他锁 |
| LockType#2 | INTENTION_SHARED | 数据库级意向共享锁 |
| LockType#3 | WRITE | 表级写锁 |
| LockType#4 | READ | 表级读锁 |

---

## 8. 锁持有/等待关系汇总

基于 heap dump 中 MultiUserLock 和 Locker 对象的字段值整理：

### 8.1 db 307648 上的锁（MultiUserLock#1）

| 角色 | Locker | 线程 | 持有/等待的锁类型实例 | 状态 |
|------|--------|------|-------------------|------|
| firstOwner | Locker#13 | auto-vacuum (tid=60) | LockType#2 (IS) | 持有 |
| otherOwners | Locker#18 | publish-version-daemon (tid=26) | LockType#1 (IX) | 持有 |
| firstWaiter | Locker#14 | consistency-checker (tid=23) | LockType#4 (READ) | 等待 |
| otherWaiters[6] | Locker#6 | starrocks-taskrun-pool-114 (tid=56318) | LockType#2 (IS) | 等待 |
| otherWaiters[其他] | 其他 Locker | 其他线程 | 各类型 | 等待 |

### 8.2 table 308183 上的锁（MultiUserLock#2）

| 角色 | Locker | 线程 | 持有/等待的锁类型实例 | 状态 |
|------|--------|------|-------------------|------|
| firstOwner | Locker#12 | starrocks-taskrun-pool-114 (tid=56318) | LockType#3 (WRITE) | 持有 |
| firstWaiter | Locker#13 | auto-vacuum (tid=60) | LockType#4 (READ) | 等待 |
| otherWaiters | 1 element | （未完全展开） | - | 等待 |
| otherOwners | 0 elements | - | - | - |

---

## 9. 时间戳对照

所有时间戳来源于日志或 heap dump 中的 `lockRequestTimeMs` / `lockAcquireTimeMs` 字段。

| 时间戳（epoch ms） | 近似时间 | 来源 | 事件 |
|-------------------|---------|------|------|
| 1770815860557 | 2026-02-11 ~21:17:40.557 | Locker#12.lockRequestTimeMs / lockAcquireTimeMs | Locker#12 请求并获得 table 308183 WRITE 锁 |
| 1770815861112 | 2026-02-11 ~21:17:41.112 | Locker#13.lockRequestTimeMs / lockAcquireTimeMs (on db 307648) | auto-vacuum 获得 db 307648 IS 锁 |
| 1770815861112 | 2026-02-11 ~21:17:41.112 | Locker#13.lockRequestTimeMs (on table 308183) | auto-vacuum 请求 table 308183 READ 锁（未获得） |
| — | 2026-02-11 21:17:41.562 | 日志 | IllegalMonitorStateException 异常 |
| 1770815863718 | 2026-02-11 ~21:17:43.718 | 日志 / TransactionState#27109.prepareTime | txn 41505 begin |
| 1770815863753 | 2026-02-11 ~21:17:43.753 | TransactionState#27109.commitTime | txn 41505 commit |
| 1770815863758 | 2026-02-11 ~21:17:43.758 | Locker#18.lockRequestTimeMs / lockAcquireTimeMs (on db 307648) | publish-version-daemon 获得 db 307648 IX 锁 |
| 1770815863758 | 2026-02-11 ~21:17:43.758 | Locker#18.waitingForRid=308183 | publish-version-daemon 请求 table 308183 WRITE 锁（未获得） |
| 1770815872055 | 2026-02-11 ~21:17:52.055 | Locker#14.lockRequestTimeMs | consistency-checker 请求 db 307648 READ 锁（未获得） |
| 1770830273281 | 2026-02-11 ~25:11:13 (≈ 2026-02-12 01:11:13) | Locker#6.lockRequestTimeMs | starrocks-taskrun-pool-114 请求 db 307648 IS 锁（未获得） |
| 1770865614169 | 2026-02-12 ~11:06:54.169 | TransactionState#3915.prepareTime | txn 62316 prepare |
| 1770865614369 | 2026-02-12 ~11:06:54.369 | TransactionState#3915.commitTime | txn 62316 commit |

---

## 10. 关键对象实例跨引用

以下记录同一对象在不同位置出现的情况：

| 对象 | 出现位置 1 | 出现位置 2 | 说明 |
|------|-----------|-----------|------|
| TransactionState#27109 | publish-version-daemon 线程栈局部变量 | starrocks-taskrun-pool-114 线程栈 handleDMLStmt 局部变量 | 两个线程操作同一事务对象 |
| Database#5940 | publish-version-daemon 线程栈局部变量 | starrocks-taskrun-pool-114 线程栈多处局部变量 | 同一数据库对象 |
| LakeMaterializedView#370 | auto-vacuum 线程栈局部变量 | starrocks-taskrun-pool-114 线程栈局部变量 | 同一物化视图对象 |
| Thread#10097 | Locker#12.lockerThread | Locker#6.lockerThread | 同一线程对象，两个不同 Locker 实例 |
| Locker#13 | MultiUserLock#1.firstOwner (db 307648, 持有 IS) | MultiUserLock#2.firstWaiter (table 308183, 等待 READ) | auto-vacuum 的 Locker 在两个锁上有不同状态 |
| Locker#18 | MultiUserLock#1.otherOwners (db 307648, 持有 IX) | Locker#18.waitingForRid = 308183 | publish-version-daemon 持有 db 锁但等待 table 锁 |
