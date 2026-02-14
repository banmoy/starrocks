# RCA: Transaction Publish Timeout 根因分析

## 1. 问题现象

```
2026-02-12 11:07:39.370+08:00 WARN (thrift-server-pool-7|286) [FrontendServiceImpl.loadTxnCommitImpl():1317]
txn 62316 publish timeout txn has not sent publish tasks yet,
maybe waiting previous txns on the same table(s) to finish, tableIds: 335341
```

**表现**: 事务 62316（db 335339, table 335341）commit 成功后，无法完成 publish，报 "txn has not sent publish tasks yet"。该事务在 `TransactionGraph` 中排队等待前序事务完成，但前序事务始终无法被 publish。

## 2. 问题概述

- 事务 62316 本身并无异常，commit 成功后等待 publish 即可完成，但 publish 始终不来
- 根因在于另一个数据库（db 307648）中的 MV 刷新触发了锁泄漏和死锁（详见第 3 节），阻塞了 `publish-version-daemon` 线程
- `publish-version-daemon` 是单线程守护进程，负责 **所有数据库** 的事务 publish；该线程一旦被阻塞，全局所有事务 publish 全部停滞
- 因此，完全不相关的 db 335339 / table 335341 上的事务 62316 也受到波及，持续 14+ 小时无法 publish

## 3. 根因分析

### 3.1 直接原因：`publish-version-daemon` 被锁阻塞

Heap dump 中 `publish-version-daemon` 线程栈显示，它在处理 **TransactionState#27109**（txn 41505, db 307648, table 308183）时，尝试获取 table 308183 的 WRITE 锁，被阻塞在 `LockManager.lockAcquireSlowPath()`：

```
"publish-version-daemon" WAITING
    at LockManager.lockAcquireSlowPath()          // 等待 table 308183 的 WRITE 锁
    at Locker.lockTablesWithIntensiveDbLock()
    at DatabaseTransactionMgr.finishTransaction()  // TransactionState#27109
    at PublishVersionDaemon.publishLakeTransactionAsync()
```

### 3.2 锁持有关系：四方死锁

通过 heap dump 中 `LockManager.lockTables` 内的两个 `MultiUserLock` 实例分析：

| 锁资源 | MultiUserLock 实例 | 说明 |
|--------|-------------------|------|
| db 307648 | MultiUserLock#1 | 数据库级意向锁 |
| table 308183 | MultiUserLock#2 | 表级锁 |

#### MultiUserLock#1（db 307648）

| 角色 | Locker | 线程 | 锁类型 | 状态 |
|------|--------|------|--------|------|
| firstOwner | Locker#13 (auto-vacuum, tid=60) | auto-vacuum | INTENTION_SHARED | 已持有 |
| otherOwners | Locker#18 (publish-version-daemon, tid=26) | publish-version-daemon | INTENTION_EXCLUSIVE | 已持有 |
| firstWaiter | Locker#14 (consistency-checker, tid=23) | consistency-checker | READ | 等待中 |
| otherWaiters[6] | **Locker#6** (starrocks-taskrun-pool-114, tid=56318) | starrocks-taskrun-pool-114 | **INTENTION_SHARED** | **等待中** |
| otherWaiters[0-5,7-12] | 其他线程 | 各类后台线程 | 各种类型 | 等待中 |

#### MultiUserLock#2（table 308183）

| 角色 | Locker | 线程 | 锁类型 | 状态 |
|------|--------|------|--------|------|
| firstOwner | **Locker#12** (starrocks-taskrun-pool-114, tid=56318) | starrocks-taskrun-pool-114 | **WRITE** | **已持有** |
| firstWaiter | Locker#18 (auto-vacuum / publish-version-daemon, tid=60/26) | auto-vacuum | READ | 等待中 |
| otherWaiters | 1 个元素 | - | - | 等待中 |

#### 死锁环路

```
starrocks-taskrun-pool-114 (Locker#12)
    持有: table 308183 WRITE 锁
    等待: db 307648 INTENTION_SHARED 锁（Locker#6 在排队）
        ↓
db 307648 的 INTENTION_SHARED 被 fair lock 规则阻塞
    因为 firstWaiter (consistency-checker) 在等 READ 锁
    而 READ 与 INTENTION_EXCLUSIVE 冲突
    INTENTION_EXCLUSIVE 被 publish-version-daemon (Locker#18) 持有
        ↓
publish-version-daemon (Locker#18)
    持有: db 307648 INTENTION_EXCLUSIVE 锁
    等待: table 308183 WRITE 锁
    被 Locker#12 (starrocks-taskrun-pool-114) 持有的 WRITE 锁阻塞
        ↓
形成环路 → 死锁
```

### 3.3 从死锁环路中定位关键异常

回顾 3.2 节的死锁环路，`starrocks-taskrun-pool-114` 线程涉及两个 Locker 实例：

| Locker | 所在锁资源 | 角色 | 状态 |
|--------|-----------|------|------|
| Locker#12 (tid=56318) | MultiUserLock#2（table 308183） | firstOwner，持有 WRITE 锁 | 持有中 |
| Locker#6 (tid=56318) | MultiUserLock#1（db 307648） | otherWaiters[6]，等待 IS 锁 | 等待中 |

这里有两个关键异常：

**异常 1：锁序反转。** `lockTablesWithIntensiveDbLock` 的正常加锁顺序是 **先 db 意向锁，再 table 锁**。但 `starrocks-taskrun-pool-114` 的状态恰好相反 — 已持有 table 308183 WRITE 锁（Locker#12），却在等待 db 307648 INTENTION_SHARED 锁（Locker#6）。这种锁序反转在正常代码路径中不应出现，说明 Locker#12 持有的 table 锁并非来自当前正在执行的加锁操作。

**异常 2：同一线程使用两个不同的 Locker 实例。** Locker#12 和 Locker#6 的 `threadId` 都是 56318，但它们是不同的对象实例。检查线程调用栈，当前活跃的代码路径中引用的是 Locker#6 和 Locker#4，**Locker#12 并未出现在任何活跃的栈帧中**。

再看 Locker#12 自身的字段：

| 字段 | 值 | 含义 |
|------|------|------|
| waitingForType | null | 不在等待任何锁 |
| waitingForRid | null | 不在等待任何资源 |
| lockRequestTimeMs | 1770815860557 | 锁请求时间（早于 Locker#6） |

综合以上观察：
- **锁序反转** → table 锁不是当前操作获取的，而是之前遗留的
- **Locker#12 不在当前调用栈中** → 其创建者代码已执行完毕
- **Locker#12 不在等待任何锁** → 它的原始操作已结束
- **但 Locker#12 仍是 table 308183 WRITE 锁的 firstOwner** → 锁管理器仍认为它持有该锁

结论：Locker#12 是一个 **孤立的锁泄漏** — 其创建者操作已完成，但持有的 WRITE 锁未被正确释放。这不是两方互相争抢的典型死锁，而是一个泄漏的锁阻塞了正常的锁获取链路。

### 3.4 锁泄漏的根因：`LocalMetastore.addPartitions()` 的 SWAP 竞态条件

#### 3.4.1 触发异常的日志（smoking gun）

在 txn 41505 开始 **之前约 2 秒**，同一线程上发生了以下异常：

```
2026-02-11 21:17:41.562+08:00 WARN (starrocks-taskrun-pool-114|56318)
Failed to execute task run, task_id: 308185

java.lang.IllegalMonitorStateException:
  Attempt to unlock lock, not locked by current locker
    at LockManager.release(LockManager.java:301)
    at Locker.release(Locker.java:109)
    at Locker.unLockTablesWithIntensiveDbLock(Locker.java:370)
    at Locker.unLockTableWithIntensiveDbLock(Locker.java:379)
    at LocalMetastore.addPartitions(LocalMetastore.java:1425)
    at LocalMetastore.addPartitions(LocalMetastore.java:996)
    at MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:559)
```

#### 3.4.2 Bug 所在代码：`addPartitions` 中加锁与解锁使用不同的 table ID

`LocalMetastore.addPartitions` 方法采用两阶段加锁策略（READ → 释放 → WRITE）。在 WRITE 锁阶段，`try` 块内通过 `checkTable(db, tableName)` 按名称重新查找表对象并 **重赋值** 了 `olapTable` 变量，但 `finally` 块使用 `olapTable.getId()` 释放锁。当并发 DDL（如 `ALTER MV SWAP WITH`）改变了名称到表的映射时，加锁和解锁使用的 table ID 不一致。

关键代码（`LocalMetastore.java:1326-1438`，简化）：

```java
private void addPartitions(..., String tableName, ...) throws DdlException {
    OlapTable olapTable = checkTable(db, tableName);         // olapTable → 表A（table 308183）

    // 第一阶段：READ 锁
    locker.lockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);  // 锁 表A
    try {
        checkExistPartitionName = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, ...);
    } finally {
        locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);  // 解锁 表A
    }

    // *** 无锁间隙：此处发生并发 ALTER MV SWAP WITH 操作 ***

    // 第二阶段：WRITE 锁
    locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE);  // 锁 表A（olapTable 仍指向表A）
    try {
        olapTable = checkTable(db, tableName);  // !!! 按名称重新查表，SWAP 后 tableName 指向表B
                                                 // olapTable 被重赋值为 表B（不同 table ID）
        existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, ...);
        // ...
    } finally {
        locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
        //                                                 ^^^^^^^^^^^^^^^^
        //                                                 此时 olapTable.getId() = 表B ID
        //                                                 但 WRITE 锁加在 表A ID（308183）上
        //                                                 → IllegalMonitorStateException
    }
}
```

**触发条件**：在 `addPartitions` 的 READ 锁释放和 WRITE 锁获取之间的无锁间隙中，发生了改变表名称映射的操作 `ALTER MATERIALIZED VIEW mv_5level_level_2 SWAP WITH new_mv_level2`。SWAP 后 `mv_5level_level_2` 名称从表A 重定向到表B。

#### 3.4.3 锁泄漏机制

`unLockTablesWithIntensiveDbLock` 的执行顺序决定了锁泄漏方式：

```java
// Locker.java:359-376
public void unLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
    if (lockType == LockType.WRITE) {
        this.release(dbId, LockType.INTENTION_EXCLUSIVE);  // 步骤1：先释放 DB 意向锁
    } else {
        this.release(dbId, LockType.INTENTION_SHARED);
    }
    for (Long rid : tableListClone) {
        this.release(rid, lockType);                        // 步骤2：再释放表锁
    }
}
```

在本案例中：

1. **步骤1**：`release(db 307648, INTENTION_EXCLUSIVE)` → **成功**（dbId 正确）
2. **步骤2**：`release(表B_ID, WRITE)` → **失败**（表B 从未加锁）→ 抛出 `IllegalMonitorStateException`

结果：
- DB 意向锁（INTENTION_EXCLUSIVE）：已释放 ✓
- **表A（308183）的 WRITE 锁：泄漏**（永远不会被释放）

SWAP 后 `new_mv_level2` 名称指向表A（table 308183），因此后续对表 308183 的任何跨线程加锁操作都会永久阻塞。

#### 3.4.4 锁泄漏如何演变为死锁

锁泄漏发生在第一次尝试的 WRITE 锁阶段，而 SWAP 在此之前（READ 解锁与 WRITE 加锁之间的无锁间隙）就已经发生。第一次尝试失败后，MV 刷新框架在同一线程上继续执行后续任务。由于 SWAP 已经改变了表名映射，后续执行是在 SWAP 后的新状态下进行的。在此过程中，由于 `Locker.equals()` 基于 `threadId`，后续执行中创建的新 Locker 与泄漏的 Locker#12 被锁管理器视为同一 locker，对 table 308183 的锁请求被视为重入，**不会阻塞**，因此后续执行（包括 txn 41505 的 INSERT OVERWRITE 和 commit）得以正常推进。

问题出在 publish 等待及其后的回调阶段。`handleDMLStmt` 完成 txn 41505 的 commit 后，进入 publish 等待循环（`visibleWaiter.await`，参见 `StmtExecutor.java:3288-3306`），等待事务从 COMMITTED 变为 VISIBLE。在此期间：

1. `publish-version-daemon` 单线程守护进程拿到 txn 41505，调用 `publishLakeTransactionAsync`
2. 该方法遍历 `txnState.getIdToTableCommitInfos()`，对每个 `TableCommitInfo` 调用 `publishLakeTableAsync`，后者再遍历 `partitionCommitInfos` 调用 `publishPartition`（该方法需要获取 table READ 锁）
3. **但 txn 41505 的 `partition commit info` 为空**（参见 commit 日志：`partition commit info:[]`），这意味着 `partitionCommitInfos` 集合为空，`publishPartition` **从未被调用**，READ 锁也从未被请求
4. 空集合导致 `CompletableFuture.allOf(emptyArray)` 返回已完成的 future，`Stream.allMatch()` 对空流返回 `true`（vacuous truth），publish 阶段被判定为"成功"
5. `publishFuture` 立即完成（`uniAcceptNow` 同步执行），`publish-version-daemon` 直接进入 `finishTransaction`
6. `finishTransaction` 通过 `lockTablesWithIntensiveDbLock` **先获取 db 307648 的 INTENTION_EXCLUSIVE 锁（成功）**
7. 接着尝试获取 table 308183 的 WRITE 锁 — **被泄漏的 Locker#12 阻塞**
8. `publish-version-daemon` 卡住 → txn 41505 无法完成 finish → 永远不会变为 VISIBLE

> 注：如果 `partition commit info` 非空，`publishPartition` 会先尝试获取 table 308183 的 READ 锁，此时就会被 Locker#12 的 WRITE 锁阻塞，根本不会走到 `finishTransaction`。正是因为 partition commit info 为空，publish 阶段被跳过，daemon 线程才直接推进到需要 WRITE 锁的 `finishTransaction`，并在那里持有 db IX 锁的情况下卡住 — 这是死锁形成的关键前提。

publish 等待超时后（txnStatus 仍为 COMMITTED），`handleDMLStmt` 的 `finally` 块（`StmtExecutor.java:3378-3381`）执行，触发 `LoadJobStatsListener.onDMLStmtJobTransactionFinish`。该回调调用 `StatisticsCollectionTrigger.prepareAnalyzeJobForLoad`，后者需要通过 `lockTablesWithIntensiveDbLock` 获取 db 307648 的 INTENTION_SHARED 锁。

此时 db 307648 的锁状态（MultiUserLock#1）中，`publish-version-daemon` 已持有 INTENTION_EXCLUSIVE 锁（第 6 步获取），而 `consistency-checker` 作为 firstWaiter 在等待 READ 锁（与 IX 冲突）。由于 fair lock 规则，后到的 INTENTION_SHARED 请求必须排在 firstWaiter 之后，因此 Locker#6 被阻塞。

至此，`starrocks-taskrun-pool-114` 线程形成了 3.3 节中观察到的锁序反转状态：通过泄漏的 Locker#12 持有 table 308183 WRITE 锁，同时通过 Locker#6 等待 db 307648 IS 锁 — 而 `publish-version-daemon` 恰好反向依赖这两个锁，死锁环路闭合。

#### 3.4.5 完整的故障链

```
时间线：

T1 (~21:17:40) — MV 刷新第 1 次尝试，addPartitions 执行
├── READ 阶段：获取 table 308183（表A，MV "mv_5level_level_2"）READ 锁，检查分区后释放
│
├── *** 无锁间隙 ***
│   └── 21:17:40.459 ALTER MV mv_5level_level_2 SWAP WITH new_mv_level2 执行
│       └── "mv_5level_level_2" 名称从 表A（308183）重定向到 表B
│
├── WRITE 阶段：获取 table 308183（表A）WRITE 锁 ← Locker#12 持有此锁
│   ├── checkTable(db, "mv_5level_level_2") → 返回 表B（SWAP 后的表，不同 ID）
│   ├── 21:17:40.557 WARN "Duplicate partition name p20200615"（表B 已有该分区）
│   └── → DdlException
│
├── finally: unLockTableWithIntensiveDbLock(db.getId(), 表B.getId(), WRITE)
│   ├── release(db 307648, INTENTION_EXCLUSIVE) → 成功（dbId 正确）
│   └── release(表B_ID, WRITE) → IllegalMonitorStateException（表B 从未加锁）
│       └── ISE 作为 RuntimeException 传播
│           └── 表A（308183）WRITE 锁永远无法释放 ← 锁泄漏！
│
├── 21:17:41.562 MV 刷新第 1 次尝试失败

T2 (21:17:43.718) — MV 刷新后续执行，同一线程 starrocks-taskrun-pool-114
├── begin txn 41505（db 307648, table 308183）
├── 21:17:43.757 txn 41505 commit 成功，进入 COMMITTED 状态
│   注意：同线程 Locker.equals() 基于 threadId，新 Locker 与 Locker#12 被视为同一 locker
│   因此同线程的 WRITE 锁请求被视为重入，不会阻塞
│
├── handleDMLStmt 进入 publish 等待阶段（visibleWaiter.await）
│   ├── publish-version-daemon 拿到 txn 41505
│   │   ├── partition commit info 为空 → publishPartition 从未调用（不需要 READ 锁）
│   │   ├── CompletableFuture.allOf(空数组) → 立即完成，判定 publish "成功"
│   │   ├── 直接进入 finishTransaction（同步执行，uniAcceptNow）
│   │   ├── 获取 db 307648 IX 锁 → 成功
│   │   └── 获取 table 308183 WRITE 锁 → 被泄漏的 Locker#12 阻塞
│   └── txn 41505 无法 finish → 永不变为 VISIBLE → publish 等待超时
│
├── finally 块执行: onDMLStmtJobTransactionFinish（publish 成功或超时后才触发）
│   └── insertStmt.setOverwrite(false) 导致 DmlType = INSERT_INTO
│       └── 绕过了 dmlType != INSERT_OVERWRITE 的检查
│   └── 触发 StatisticsCollectionTrigger.prepareAnalyzeJobForLoad
│       └── lockTablesWithIntensiveDbLock(db 307648, ..., READ)
│           └── 需要先获取 db 307648 的 INTENTION_SHARED 锁
│           └── publish-version-daemon 已持有 db 307648 IX 锁
│           └── consistency-checker 作为 firstWaiter 等待 READ
│           └── fair lock 规则阻塞 IS 请求
│           └── Locker#6 进入 db 307648 的等待队列 ← 死锁形成！

T3 — 死锁确认
├── Locker#12 持有 table 308183 WRITE → 阻塞 publish-version-daemon
├── Locker#6 等待 db 307648 IS → 被 publish-version-daemon 的 IX 间接阻塞
├── publish-version-daemon 等待 table 308183 WRITE → 被 Locker#12 阻塞
└── 环路形成，所有事务 publish 停滞

T4 (14 小时后) — 连带影响
├── 2026-02-12 11:06:54 txn 62316（db 335339, table 335341）commit 成功
├── 2026-02-12 11:07:39 txn 62316 publish timeout
└── 报 "txn has not sent publish tasks yet"
```

## 4. 根因总结

| 层级 | 问题 | 说明 |
|------|------|------|
| **根本原因** | `addPartitions` 的 SWAP 竞态条件 | WRITE 锁阶段 `checkTable(db, tableName)` 重赋值了 `olapTable`，`finally` 块使用 SWAP 后的表B ID 释放锁，但锁实际加在表A ID 上 |
| **触发条件** | READ 解锁与 WRITE 加锁之间的无锁间隙 | 此间隙中并发执行了 `ALTER MV SWAP WITH`，改变了表名称到 table ID 的映射 |
| **直接后果** | `IllegalMonitorStateException` 导致锁泄漏 | `unLockTablesWithIntensiveDbLock` 先成功释放了 DB 意向锁，再对从未加锁的表B ID 执行 release 抛出 ISE；表A（308183）的 WRITE 锁永久残留 |
| **扩大因素** | 同线程重入 + publish 回调触发统计收集 | 泄漏锁的线程通过 `Locker.equals()` 重入，后续 commit 的 publish 超时后回调触发统计收集，请求 db IS 锁形成死锁 |
| **最终影响** | `publish-version-daemon` 全局阻塞 | 单线程 daemon 被死锁阻塞，所有数据库的事务 publish 全部停滞，持续 14+ 小时 |

## 5. 时间线还原

| 时间 | 事件 |
|------|------|
| 2026-02-11 21:17:40.459 | `ALTER MV mv_5level_level_2 SWAP WITH new_mv_level2` 执行，改变名称→表映射 |
| 2026-02-11 21:17:40.557 | `addPartitions` WRITE 锁阶段 `checkTable` 返回 SWAP 后的表B，报 "Duplicate partition" |
| 2026-02-11 21:17:41.562 | MV 刷新第 1 次尝试失败，`finally` 解锁表B ID 触发 `IllegalMonitorStateException`，表A（308183）WRITE 锁泄漏（Locker#12） |
| 2026-02-11 21:17:43.718 | MV 刷新后续执行（SWAP 后的新状态下），begin txn 41505 |
| 2026-02-11 21:17:43.757 | txn 41505 commit 成功，进入 COMMITTED 状态，`handleDMLStmt` 进入 publish 等待循环 |
| 2026-02-11 21:17:43.757+ | `publish-version-daemon` 拿到 txn 41505，partition commit info 为空 → 跳过 publishPartition → 直接进入 finishTransaction → 获取 db 307648 IX 锁成功 → 获取 table 308183 WRITE 锁被 Locker#12 阻塞 |
| publish 超时后 | `handleDMLStmt` finally 块触发 `onDMLStmtJobTransactionFinish` → 统计收集 → Locker#6 尝试获取 db 307648 IS 锁，被 fair lock 阻塞 → 死锁形成 |
| 2026-02-12 11:06:54.375 | txn 62316（db 335339, table 335341）commit 成功 |
| 2026-02-12 11:07:39.370 | txn 62316 publish timeout，报 "txn has not sent publish tasks yet" |
| 持续 | 所有数据库的事务 publish 全部停滞，持续约 14 小时+ |

## 6. 复现步骤

### 6.1 复现原理

在 `addPartitions` 的 READ 解锁和 WRITE 加锁之间注入可控等待点，在等待期间执行 `ALTER MATERIALIZED VIEW SWAP WITH` 改变表名称映射。

### 6.2 代码变更

**`Config.java`** — 添加 debug 配置项（运行时可动态修改，默认 0 不影响正常功能）：

```java
@ConfField(mutable = true, comment = "Debug: sleep ms before write lock in addPartitions, 0 means disabled")
public static long debug_sleep_before_addpartition_write_lock_ms = 0;
```

**`LocalMetastore.addPartitions`** — 在 READ 解锁后、WRITE 加锁前注入等待：

```java
} finally {
    locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);
}

// [DEBUG] Sleep to allow concurrent SWAP to happen between READ unlock and WRITE lock
while (Config.debug_sleep_before_addpartition_write_lock_ms > 0) {
    LOG.info("[DEBUG] waiting before write lock in addPartitions, db: {}, table: {} (id={}), interval={}ms",
            db.getFullName(), tableName, olapTable.getId(),
            Config.debug_sleep_before_addpartition_write_lock_ms);
    try {
        Thread.sleep(Config.debug_sleep_before_addpartition_write_lock_ms);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
    }
}

Preconditions.checkNotNull(distributionInfo);
```

**`fe.conf`** — 强制任务线程池单线程（用于验证同线程可重入行为）：

```properties
max_task_runs_threads_num = 1
```

### 6.3 复现步骤

#### 步骤 1：创建测试环境（Session 1）

```sql
CREATE DATABASE IF NOT EXISTS test_swap_bug_db;
USE test_swap_bug_db;

-- 基表，初始一个分区
CREATE TABLE base_table (
    dt DATE NOT NULL,
    id INT,
    val STRING
)
PARTITION BY RANGE(dt) (
    PARTITION p20200614 VALUES [('2020-06-14'), ('2020-06-15'))
)
DISTRIBUTED BY RANDOM
PROPERTIES ("replication_num" = "1");

INSERT INTO base_table VALUES ('2020-06-14', 1, 'a');

-- 创建 mv_test，刷新后只有 p20200614
CREATE MATERIALIZED VIEW mv_test
PARTITION BY dt
DISTRIBUTED BY RANDOM
REFRESH MANUAL
PROPERTIES ("replication_num" = "1", "session.insert_timeout" = "300")
AS SELECT dt, id, val FROM base_table;

REFRESH MATERIALIZED VIEW mv_test WITH SYNC MODE;

-- 基表加新分区
ALTER TABLE base_table ADD PARTITION p20200615 VALUES [('2020-06-15'), ('2020-06-16'));
INSERT INTO base_table VALUES ('2020-06-15', 2, 'b');

-- 创建 new_mv_test，刷新后有 p20200614 + p20200615
CREATE MATERIALIZED VIEW new_mv_test
PARTITION BY dt
DISTRIBUTED BY RANDOM
REFRESH MANUAL
PROPERTIES ("replication_num" = "1", "session.insert_timeout" = "300")
AS SELECT dt, id, val FROM base_table;

REFRESH MATERIALIZED VIEW new_mv_test WITH SYNC MODE;
```

此时：`mv_test`（表A）缺 p20200615，`new_mv_test`（表B）有 p20200614 + p20200615。

#### 步骤 2：触发 Bug（Session 1）

```sql
ADMIN SET FRONTEND CONFIG("debug_sleep_before_addpartition_write_lock_ms" = "1000");
REFRESH MATERIALIZED VIEW mv_test;
```

等 FE 日志出现 `[DEBUG] waiting before write lock` 后继续。

#### 步骤 3：SWAP 并放行（Session 2）

```sql
USE test_swap_bug_db;
ALTER MATERIALIZED VIEW mv_test SWAP WITH new_mv_test;
ADMIN SET FRONTEND CONFIG("debug_sleep_before_addpartition_write_lock_ms" = "0");
```

#### 步骤 4：验证

FE 日志中出现 `IllegalMonitorStateException`，复现成功。

同线程可重入验证（需 `max_task_runs_threads_num = 1`）：

```sql
-- 异步刷新 new_mv_test（单线程池，复用同一线程）
REFRESH MATERIALIZED VIEW new_mv_test;
-- 预期：不阻塞（同线程锁可重入）
```

#### 步骤 5：清理

```sql
ADMIN SET FRONTEND CONFIG("debug_sleep_before_addpartition_write_lock_ms" = "0");
DROP DATABASE test_swap_bug_db FORCE;
-- 重启 FE 释放泄漏锁，恢复 fe.conf 中 max_task_runs_threads_num
```

### 6.4 复现结果

| 验证项 | 预期 | 实际 |
|--------|------|------|
| mv_test 刷新失败 | `IllegalMonitorStateException` | 符合 |
| Duplicate partition 日志 | WARN `p20200615` 已存在 | 符合 |
| 同线程刷新 new_mv_test | 不阻塞（锁可重入） | 符合 |

## 7. 修复建议

此问题的本质是：手动 `lock`/`unlock` 配对时，`unlock` 使用的 resource ID 可能因中间代码对变量的重赋值而与 `lock` 时不一致。这不是 `addPartitions` 独有的模式——代码库中有 16+ 处文件使用类似的手动 `lockTableAndCheckDbExist` / `unLockTableWithIntensiveDbLock` 配对，都存在同类风险。因此修复方案需要从机制层面根本解决，而非逐个修补。

### 7.1 方案一：将 `checkTable` 的名称查找改为 ID 查找（仅针对本 case）

此方案**仅针对 `addPartitions` 这个具体 case**，不解决其他手动 `lock`/`unlock` 配对的通用风险。

**根因**：WRITE 锁阶段的 `checkTable(db, tableName)` 按名称查找表，当并发 SWAP 改变了名称映射后，返回的是不同的表对象，导致 `finally` 中 `olapTable.getId()` 与加锁时的 table ID 不一致。

**修复**：将名称查找改为 ID 查找。`checkTable(db, olapTable.getId())` 始终返回加锁时的那张表，无论是否发生 SWAP，`olapTable.getId()` 在 `finally` 中的值与加锁时一致，lock/unlock 配对永远正确。

```java
// ❌ Before：名称查找，SWAP 后返回不同的表
olapTable = checkTable(db, tableName);

// ✅ After：ID 查找，始终返回加锁时的表
olapTable = checkTable(db, olapTable.getId());
```

**优点**：改动极小（仅一行），不改变 try/finally 结构，不引入新抽象，语义更正确（操作的对象与加锁的对象始终一致）。

**局限性**：此方案仅修复 `addPartitions` 中因名称查找导致的变量重赋值问题。代码库中其他使用手动 `lock`/`unlock` 配对的 16+ 处文件，如果未来引入类似的名称查找重赋值模式，仍然存在同类风险。通用的机制性修复参见方案一和方案二。

### 7.2 方案二：使用已有的 AutoCloseableLock + try-with-resources

代码库已有 `AutoCloseableLock` 类，它在**构造时（加锁时）捕获 table ID**，在 `close()` 中用同一个 ID 释放。无论 `try` 块内如何重赋值变量，释放的始终是加锁时的 resource ID。

**当前的问题代码模式**：

```java
// ❌ 手动配对：unlock 使用的 olapTable.getId() 可能已被重赋值
locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE);
try {
    olapTable = checkTable(db, tableName);  // 可能重赋值为不同的 table
    // ...
} finally {
    locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
    //                                                ^^^^^^^^^^^^^^^^^ Bug!
}
```

**修复后的模式**：

```java
// ✅ AutoCloseableLock 在构造时绑定 table ID，close 时释放的是加锁时的 ID
long lockedTableId = olapTable.getId();
try (AutoCloseableLock ignored = new AutoCloseableLock(locker, db.getId(),
        List.of(lockedTableId), LockType.WRITE)) {
    olapTable = checkTable(db, tableName);  // 即使重赋值也不影响释放
    // ...
}
// AutoCloseableLock.close() 自动释放 lockedTableId 的锁
```

**局限性**：此方案依赖编码规范约束，无法阻止开发者绕过 `AutoCloseableLock` 继续使用手动 `lock`/`unlock` 配对。

### 7.3 方案三：lock 返回 LockGuard，从 API 层面消灭 unlock 传参错误

方案一依赖编码规范，无法彻底禁止误用。根本原因是当前 API 中 `lock` 和 `unlock` 是**分离的两次调用**，调用方需要在 `unlock` 时重新传入 resource ID。只要 `unlock` 接受参数，就无法阻止传错。

彻底的方案是：**`lock` 方法返回一个不透明的 `LockGuard`，`unlock` 只能通过 `LockGuard.close()` 完成，不接受任何参数。同时移除手动 unlock 的 public 方法。**

**重新设计的 API**：

```java
public class Locker {
    /**
     * 加锁并返回 LockGuard。调用方只能通过 guard.close() 释放锁，
     * 无需（也无法）传入 resource ID — 从 API 层面消灭传参错误。
     */
    public LockGuard lockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
        // ...existing lock logic...
        return new LockGuard(this, dbId, tableList, lockType);
    }

    // 移除 public 的 unLockTablesWithIntensiveDbLock 方法
    // unlock 只能通过 LockGuard.close() 触发
}

public class LockGuard implements AutoCloseable {
    private final Locker locker;
    private final Long dbId;
    private final List<Long> lockedTableIds;  // 加锁时绑定，final 不可变
    private final LockType lockType;

    @Override
    public void close() {
        // 释放的始终是加锁时的 resource ID，无论外部变量如何变化
        locker.doRelease(dbId, lockedTableIds, lockType);
    }
}
```

**调用方代码**：

```java
// ✅ lock 返回 guard，guard.close() 自动释放加锁时的 resource ID
// 调用方无法也不需要传入任何 ID — 从 API 层面保证正确性
try (LockGuard guard = locker.lockTablesWithIntensiveDbLock(
        db.getId(), List.of(olapTable.getId()), LockType.WRITE)) {
    olapTable = checkTable(db, tableName);  // 即使重赋值也不影响释放
    // ...
}
```

**关键设计要点**：
- `lockTablesWithIntensiveDbLock` 返回 `LockGuard`（`AutoCloseable`），不再返回 `void`
- **移除 `unLockTablesWithIntensiveDbLock` 等手动 unlock 的 public 方法**，unlock 只能通过 `LockGuard.close()` 触发
- `LockGuard` 在构造时捕获 `dbId` 和 `tableIds`，这些字段为 `final`，不可变
- 调用方使用 `try-with-resources`，既不需要也没有机会传入错误的 resource ID

## 8. 关键代码引用

| 文件 | 行号 | 说明 |
|------|------|------|
| `LocalMetastore.java` | 1326-1438 | `addPartitions` 方法：两阶段加锁（READ→WRITE）+ `checkTable` 重赋值导致 table ID 不一致 |
| `LocalMetastore.java` | 1393 | WRITE 锁获取：`lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE)` |
| `LocalMetastore.java` | 1399 | `olapTable = checkTable(db, tableName)` — SWAP 后重赋值为不同表 |
| `LocalMetastore.java` | 1432 | `finally` 中使用重赋值后的 `olapTable.getId()` 释放锁 — Bug 所在 |
| `Locker.java` | 359-376 | `unLockTablesWithIntensiveDbLock`：先释放 DB 意向锁再释放表锁，部分成功导致锁泄漏 |
| `LockManager.java` | 293-308 | `release()` 中 lock==null 时抛出 `IllegalMonitorStateException` |
| `Locker.java` | 508-517 | `equals()` 基于 threadId：使得同线程的新 Locker 可重入泄漏的锁 |
| `AutoCloseableLock.java` | 20-52 | 已有的 try-with-resources 锁封装，在构造时绑定 table ID |
| `TransactionState.java` | 1167-1176 | `getPublishTimeoutDebugInfo()` 消息来源 |
| `PublishVersionDaemon.java` | 506 | 单线程 publish 逻辑 |

详细的 Bug 分析参见：[rca_illegal_monitor_state_exception_mv_refresh.md](rca_illegal_monitor_state_exception_mv_refresh.md)
