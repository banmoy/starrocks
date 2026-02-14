# RCA: IllegalMonitorStateException during MV Refresh

## 1. 问题描述

### 1.1 现象

MV 刷新任务失败，FE 日志报错：

```
2026-02-11 21:17:41.562+08:00 WARN (starrocks-taskrun-pool-114|56318) [TaskRun.executeTaskRun():423]
Failed to execute task run, task_id: 308185, task_run_id: 019c4cd9-aebc-76f0-87de-a390f49850bd, failCount:1
com.starrocks.sql.common.DmlException: Refresh mv mv_5level_level_2 failed after 1 times, try lock failed: 0,
error-msg : java.lang.IllegalMonitorStateException: Attempt to unlock lock, not locked by current locker
    at com.starrocks.common.util.concurrent.lock.LockManager.release(LockManager.java:301)
    at com.starrocks.common.util.concurrent.lock.Locker.release(Locker.java:109)
    at com.starrocks.common.util.concurrent.lock.Locker.unLockTablesWithIntensiveDbLock(Locker.java:370)
    at com.starrocks.common.util.concurrent.lock.Locker.unLockTableWithIntensiveDbLock(Locker.java:379)
    at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:1425)
    at com.starrocks.server.LocalMetastore.addPartitions(LocalMetastore.java:996)
    at com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner.addRangePartitions(MVPCTRefreshRangePartitioner.java:559)
```

### 1.2 影响

- MV 刷新失败
- 表级 WRITE 锁泄漏，导致后续跨线程操作该表永久阻塞（需重启 FE 恢复）

### 1.3 环境

- 数据库：`test_mv_async_db_040cc214_074c_11f1_a471_00163e0e489a`
- MV 名称：`mv_5level_level_2`
- 时间：2026-02-11 21:17:40 ~ 21:17:41

## 2. 根因分析

### 2.1 Bug 所在代码

`LocalMetastore.addPartitions` 方法采用两阶段加锁策略（READ → 释放 → WRITE），在 WRITE 锁阶段按表名重新查找表对象后，`finally` 块使用了变更后的表 ID 释放锁，与加锁时的表 ID 不一致。

关键代码（简化）：

```java
private void addPartitions(..., String tableName, ...) throws DdlException {
    OlapTable olapTable = checkTable(db, tableName);         // olapTable → 表A

    // 第一阶段：READ 锁
    locker.lockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);  // 锁 表A
    try {
        checkExistPartitionName = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, ...);
    } finally {
        locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);  // 解锁 表A
    }

    // *** 无锁间隙：此处可发生并发 SWAP 操作 ***

    // 第二阶段：WRITE 锁
    locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE);  // 锁 表A（olapTable 仍指向表A）
    try {
        olapTable = checkTable(db, tableName);  // !!! 按名称重新查表，SWAP 后 tableName 指向表B
                                                 // olapTable 被重赋值为 表B
        existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, ...);
        // ...
    } finally {
        locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
        //                                                 ^^^^^^^^^^^^^^^^
        //                                                 此时 olapTable.getId() = 表B ID
        //                                                 但 WRITE 锁加在 表A ID 上
        //                                                 → IllegalMonitorStateException
    }
}
```

### 2.2 问题本质

`olapTable` 变量在 `try` 块内被 `checkTable(db, tableName)` 按名称重新赋值，而 `finally` 块使用 `olapTable.getId()` 释放锁。当并发 DDL 操作改变了名称到表的映射时，加锁和解锁使用的 table ID 不一致。

### 2.3 触发条件

在 `addPartitions` 的 READ 锁释放和 WRITE 锁获取之间的无锁间隙中，必须发生改变表名称映射的操作。已确认的触发场景：

- `ALTER MATERIALIZED VIEW mv_A SWAP WITH mv_B`

此操作将 `mv_A` 名称指向原 `mv_B` 的底层表（不同的 table ID），反之亦然。

## 3. 事件时间线

基于生产环境日志还原：

| 时间 | 事件 | 来源 |
|------|------|------|
| ~21:17:40 | MV 刷新开始，`addPartitions` 获取 READ 锁（表A ID），检查分区后释放 | 推断 |
| 21:17:40.459 | `ALTER MATERIALIZED VIEW mv_5level_level_2 SWAP WITH new_mv_level2` 执行成功 | Audit Log |
| ~21:17:40.5 | `addPartitions` 获取 WRITE 锁（表A ID），`checkTable("mv_5level_level_2")` 返回表B | 推断 |
| 21:17:40.557 | `Duplicate partition name p20200615`（表B 已有该分区）→ `DdlException` | FE WARN Log |
| 21:17:40.557 | `finally` 执行 `unlock(表B ID, WRITE)` → `IllegalMonitorStateException` | FE WARN Log |
| 21:17:41.562 | MV 刷新任务最终失败 | FE WARN Log |

### 3.1 关键日志证据

**Audit Log — SWAP 操作**

```
2026-02-11 21:17:40.459+08:00 [query]
|Stmt=ALTER MATERIALIZED VIEW mv_5level_level_2 SWAP WITH new_mv_level2;|State=OK|
```

**FE Log — 重复分区（证明 WRITE 锁内查到了 SWAP 后的表）**

```
2026-02-11 21:17:40.557+08:00 WARN (starrocks-taskrun-pool-114|56318)
[CatalogUtils.checkPartitionNameExistForAddPartitions():93]
Duplicate partition name p20200615, existed partition:partition_id: 307839; name: p20200615; ...
```

该 WARN 日志来自 `CatalogUtils.checkPartitionNameExistForAddPartitions`，且线程为 `starrocks-taskrun-pool-114|56318`（MV 刷新线程），证实是在 WRITE 锁阶段的第二次 `checkPartitionNameExistForAddPartitions` 调用中触发的。SWAP 前的表A 不存在 p20200615（否则 READ 阶段就会发现），说明 `checkTable(db, tableName)` 在 WRITE 锁内返回了 SWAP 后的表B。

## 4. 影响分析

### 4.1 直接影响

MV 刷新任务失败，抛出 `IllegalMonitorStateException`。

### 4.2 锁泄漏（二次影响）

`unLockTablesWithIntensiveDbLock` 的执行顺序：

```java
// 1. 先释放 DB 意向锁 → 成功（dbId 正确）
this.release(dbId, LockType.INTENTION_EXCLUSIVE);
// 2. 再释放表锁 → 失败（表B ID 从未加锁）
this.release(表B_ID, LockType.WRITE);  // → IllegalMonitorStateException
```

结果：

- DB 意向锁：已释放 ✓
- **表A 的 WRITE 锁：泄漏**（永远不会被释放）

SWAP 后 `new_mv_level2` 名称指向表A，因此后续对 `new_mv_level2` 的任何加锁操作都会受到影响：

| 场景 | 行为 | 原因 |
|------|------|------|
| 不同线程操作表A | 永久阻塞 | WRITE 锁与 READ/WRITE 请求冲突，timeout=0 无限等待 |
| 同线程操作表A | 不阻塞 | `Locker.equals` 基于 `threadId`，同线程视为同一 locker，锁可重入 |

需要重启 FE 才能释放泄漏的锁。

### 4.3 与 PR #51884 的关系

PR #51884 修复了 `Locker.tryLockTableWithIntensiveDbLock` 中一个冗余 `finally` 块导致的相同异常。该问题是 Locker 基础设施层面的 bug，而本问题是 `LocalMetastore.addPartitions` 业务逻辑层面的 bug。两者异常信息相同，但根因不同、触发路径不同。

## 5. 复现

### 5.1 复现原理

在 `addPartitions` 的 READ 解锁和 WRITE 加锁之间注入可控等待点，在等待期间执行 `ALTER MATERIALIZED VIEW SWAP WITH` 改变表名称映射。

### 5.2 代码变更

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

### 5.3 复现步骤

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

### 5.4 复现结果

| 验证项 | 预期 | 实际 |
|--------|------|------|
| mv_test 刷新失败 | `IllegalMonitorStateException` | ✓ 符合 |
| Duplicate partition 日志 | WARN `p20200615` 已存在 | ✓ 符合 |
| 同线程刷新 new_mv_test | 不阻塞（锁可重入） | ✓ 符合 |

## 6. 修复方案

### 6.1 核心思路

在 WRITE 锁阶段，`finally` 块必须使用加锁时的原始 table ID 释放锁，而非重赋值后的 table ID。同时在 `checkTable` 重赋值后检测表是否已被替换。

### 6.2 修复代码

```java
// 第二阶段：WRITE 锁
long lockedTableId = olapTable.getId();  // 保存加锁时的 table ID
if (!locker.lockTableAndCheckDbExist(db, lockedTableId, LockType.WRITE)) {
    throw new DdlException("db " + db.getFullName() + "(" + db.getId() + ") has been dropped");
}
Set<String> existPartitionNameSet = Sets.newHashSet();
try {
    olapTable = checkTable(db, tableName);
    // 检测表是否被 SWAP 或替换
    if (olapTable.getId() != lockedTableId) {
        throw new DdlException("Table " + tableName + " has been replaced (expected id="
                + lockedTableId + ", actual id=" + olapTable.getId() + "). Please retry.");
    }
    existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
    // ...
} finally {
    cleanExistPartitionNameSet(existPartitionNameSet, partitionNameToTabletSet);
    locker.unLockTableWithIntensiveDbLock(db.getId(), lockedTableId, LockType.WRITE);
    //                                                 ^^^^^^^^^^^^^^
    //                                                 始终使用加锁时的 ID
}
```

### 6.3 修复要点

1. **`finally` 用 `lockedTableId` 释放锁**：确保加锁和解锁 ID 一致，杜绝 `IllegalMonitorStateException` 和锁泄漏
2. **检测 table ID 变化**：`checkTable` 后如果 ID 不匹配，立即抛出 `DdlException`，避免在错误的表上执行 DDL
3. **向上抛出可重试异常**：MV 刷新框架有重试机制，下次刷新会获取到正确的表状态
