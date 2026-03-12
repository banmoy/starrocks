# MV 行数变为 0 问题分析报告

## 1. 异常日志分析

从日志中可以提取以下关键信息：

| 项目 | 值 |
|------|-----|
| 时间 | 2026-03-09 21:36:00.845Z |
| MV 名称 | `ta_source_activity_log_employee_mv` |
| 操作类型 | `INSERT OVERWRITE`（MV 刷新） |
| 错误类型 | S3 503 - "Please reduce your request rate" |
| 失败节点 | BE 172.31.175.175 |
| 失败原因 | 写 S3 对象时触发限流（Rate Limiting） |
| 事务结果 | **ABORTED**，耗时 42191ms |
| query_id | `9463fd68-1bfe-11f1-87aa-0a71f84af585` |
| job_id | `293714987` |
| 来源 | `PartitionBasedMvRefreshProcessor.refreshMaterializedView()` → `InsertOverwriteJobRunner` |

## 2. 从代码和堆栈实锤：INSERT OVERWRITE 本身不会导致数据丢失

### 2.1 堆栈精确定位失败点

从异常堆栈可以精确还原调用链：

```
run() [line 143]
  → handle() [line 155, state=PENDING]
    → prepare() [line 277]
      → transferTo(RUNNING) [line 224]
        → handle() [line 158, state=RUNNING]
          → doLoad() [line 174]
            → createTempPartitions() [line 176]  ✅ 成功
            → prepareInsert() [line 177]         ✅ 成功
            → executeInsert() [line 178]         ❌ 在这里抛出 DdlException
            → doCommit() [line 179]              ❌ 从未执行（关键！）
```

`executeInsert()` 内部调用 `stmtExecutor.handleDMLStmt()` (line 409)，而 `handleDMLStmt` 在 `coord.getExecStatus()` 不 OK 时抛出了 `DdlException`（line 2700-2701），错误信息就是 S3 503。

### 2.2 失败后的 GC 路径

`executeInsert()` 抛异常后，控制流是：

```
doLoad() 抛出 → handle() 抛出 → transferTo() 抛出 → prepare() 抛出
→ handle() 抛出 → run() 捕获异常
→ run() 调用 transferTo(OVERWRITE_FAILED)
  → handle() [state=FAILED] → gc(false)
```

`gc(false)` 的逻辑（InsertOverwriteJobRunner.java line 436-496）：

```java
// gc() 只做两件事：
// 1. 清理临时分区（tmpPartitionIds）
for (long pid : job.getTmpPartitionIds()) {
    targetTable.dropTempPartition(partition.getName(), true);  // 只 drop 临时分区
}
// 2. 写 OVERWRITE_FAILED 日志
GlobalStateMgr.getCurrentState().getEditLog().logInsertOverwriteStateChange(info);
```

**gc() 从不操作源分区（source partitions）。** 源分区的 drop 只发生在 `doCommit()` 里的 `replaceTempPartitions()` / `replacePartition()` 中，而 `doCommit()` 从未被执行。

### 2.3 结论：INSERT OVERWRITE 机制可以被排除

| 判断依据 | 说明 |
|----------|------|
| 堆栈证明 `doCommit()` 未执行 | 异常在 `executeInsert()` 抛出，`doCommit()` 是 `doLoad()` 中的下一行，从未到达 |
| `gc()` 只清理临时分区 | 源码明确只调用 `dropTempPartition()`，不碰源分区 |
| 事务状态为 ABORTED | 日志中 `TransactionState` 状态为 ABORTED，确认没有 commit |
| `doCommit()` 中的 `replaceTempPartitions()` | 这是唯一会 drop 源分区的地方，但它在 `doCommit()` 内部，没有被调用 |

**实锤：INSERT OVERWRITE 的临时分区交换机制没有被触发，不可能是 INSERT OVERWRITE 操作本身导致数据变为 0。**

### 2.4 在日志中验证的方法

如果要在 FE 日志中进一步实锤，可以搜索以下关键日志行：

```bash
# 如果 doCommit 被调用了，一定会打印这行日志（line 646）：
grep "overwrite job .* replace source partitions" fe.log

# 如果 gc 被调用了（说明是失败路径），会打印：
grep "insert overwrite job .* start to garbage collect" fe.log

# 如果找到了 gc 日志但没找到 replace 日志，实锤 doCommit 没被执行
```

针对这个具体的 job_id=293714987：

```bash
# 搜索该 job 的所有日志
grep "293714987" fe.log

# 预期能找到：
# 1. "insert overwrite job 293714987 start to garbage collect"   ← gc 被调用
# 2. "insert overwrite job:293714987 failed"                      ← 状态转为 FAILED
# 预期找不到：
# 1. "overwrite job 293714987 replace source partitions"          ← 说明 doCommit 没执行
```

## 3. 真正导致 MV 行数为 0 的嫌疑：Force Refresh

### 3.1 Force Refresh 的破坏性流程

代码位置：`PartitionBasedMvRefreshProcessor.syncPartitions()` (line 986-1029)

```java
if (mvRefreshParams.isForce() && !tentative) {
    // ① 先 drop 现有分区
    if (!mv.isPartitionedTable()) {
        mv.dropPartition(db.getId(), partitionName, false);     // 数据此刻已清空！
        localMetastore.buildNonPartitionOlapTable(db, mv, ...); // 重建空分区
    } else {
        for (String partName : toRefreshPartitions) {
            mvRefreshPartitioner.dropPartition(db, mv, partName);
        }
    }
}
// ② 之后才执行 syncAddOrDropPartitions → INSERT OVERWRITE
// 如果 INSERT OVERWRITE 失败，MV 就是空的
```

**关键时序**：

```
Force Refresh 路径：
  syncPartitions()
    → drop 现有分区           ← MV 数据变为 0（此操作不可回滚！）
    → 重建空分区
  refreshMaterializedView()
    → INSERT OVERWRITE        ← 如果这步失败，MV 就永远是 0 行
      → 创建临时分区
      → 写入数据到临时分区     ← S3 503 在这里发生
      → 交换分区               ← 未到达
```

### 3.2 非 Force Refresh 的安全流程

```
普通 Refresh 路径：
  syncPartitions()
    → 同步分区（不 drop 现有数据）
  refreshMaterializedView()
    → INSERT OVERWRITE
      → 创建临时分区
      → 写入数据到临时分区     ← 如果这步失败
      → gc() 只清理临时分区    ← 源数据不受影响
```

### 3.3 如何实锤是否是 Force Refresh

**方法 1：查询 task_runs 表（最直接）**

```sql
-- 获取 MV 的 ID
SELECT TABLE_ID FROM information_schema.materialized_views
WHERE TABLE_NAME = 'ta_source_activity_log_employee_mv';

-- 查看 3 月 9 日前后的所有刷新任务（替换 <mv_id>）
SELECT
    TASK_NAME,
    CREATE_TIME,
    FINISH_TIME,
    STATE,
    ERROR_MESSAGE,
    get_json_string(EXTRA_MESSAGE, '$.forceRefresh') AS is_force_refresh,
    get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS refresh_mode,
    get_json_string(EXTRA_MESSAGE, '$.mvPartitionsToRefresh') AS mv_partitions
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-<mv_id>'
  AND CREATE_TIME >= '2026-03-09 00:00:00'
ORDER BY CREATE_TIME DESC
LIMIT 50;
```

**如果 `is_force_refresh = true` 且 `STATE = FAILED`，就是实锤。**

**方法 2：搜索 FE 日志（更直接）**

```bash
# 搜索 force refresh 的 drop 日志
grep "force refresh, drop partitions" fe.log | grep "ta_source_activity_log_employee_mv"

# 如果找到了，时间在 3 月 9 日附近，说明确实做了 force refresh 并 drop 了分区
```

```bash
# 搜索这个 MV 相关的 INSERT OVERWRITE 失败
grep "ta_source_activity_log_employee_mv" fe.log | grep -E "failed|FAILED|error|ABORTED"
```

**方法 3：确认 MV 是否为非分区表**

```sql
SHOW CREATE MATERIALIZED VIEW ta_source_activity_log_employee_mv;
```

从日志中的 SQL 来看，SELECT 语句没有涉及分区键：
```sql
INSERT OVERWRITE `ta_source_activity_log_employee_mv`
SELECT ... FROM `analytics`.`employee`
```
这极可能是**非分区 MV**。对于非分区 MV，force refresh 会 drop 唯一的分区并重建空的，效果就是**全部数据归零**。

**方法 4：检查 audit log（如果开启了）**

```sql
-- 查看是否有人手动执行了 FORCE REFRESH
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE stmt LIKE '%REFRESH%ta_source_activity_log_employee_mv%'
  AND event_time >= '2026-03-08'
ORDER BY event_time;
```

## 4. 排查决策树（实锤流程）

```
Q1: task_runs 中是否有 forceRefresh=true 且 STATE=FAILED 的记录？
    ├── 是 → 实锤是 Force Refresh + INSERT 失败导致
    │         验证：FE 日志搜 "force refresh, drop partitions"
    │         结论：Force Refresh 先清空分区，INSERT 失败后数据无法恢复
    │
    └── 否 → Q2: task_runs 中最后一次 STATE=SUCCESS 的时间？
              ├── 很久之前 → Q3: MV 是否 active？
              │   ├── inactive → MV 可能因 schema 变更失活，数据过期被清理
              │   └── active → 检查是否有其他操作（DDL、TRUNCATE 等）
              │
              └── 最近有成功 → Q4: 最后成功的刷新查询计划中是否数据源为空？
                    ├── 是 → 问题在上游 employee 表数据
                    └── 否 → 需要更深层排查（FE 元数据损坏等）
```

## 5. 针对 doCommit() 非原子性的额外排除

有人可能怀疑 `replaceTempPartitions()` 的非原子性（先 drop 旧分区、再 add 新分区）导致问题。但可以排除：

| 排除理由 | 说明 |
|----------|------|
| `doCommit()` 未被调用 | 堆栈明确显示异常在 `executeInsert()`，`doCommit()` 是下一行 |
| 在写锁内执行 | `doCommit()` 获取了 TABLE WRITE LOCK，两步操作在同一个锁内，不会被并发打断 |
| 纯内存操作 | `replaceTempPartitions()` 是 FE 内存中的 metadata 操作，不涉及 I/O，不会因 S3 异常中断 |
| 只有 JVM crash 才可能打断 | 在 drop 旧分区和 add 新分区之间，只有 FE 进程 crash 才能中断，但这会触发 editlog replay 恢复 |

**结论：`doCommit()` 中的分区交换不是这次问题的原因。**

## 6. 需要确认的信息汇总

| 优先级 | 确认项 | 操作 | 目的 |
|--------|--------|------|------|
| **P0** | task_runs 中的 forceRefresh 字段 | 查 `information_schema.task_runs`（见上方 SQL） | **实锤是否 force refresh** |
| **P0** | FE 日志中的 "force refresh, drop partitions" | grep FE 日志 | **实锤 drop 分区操作是否发生** |
| **P1** | MV DDL（是否为非分区表） | `SHOW CREATE MATERIALIZED VIEW` | 确认 force refresh 的影响范围 |
| **P1** | MV 当前状态 | `SHOW MATERIALIZED VIEWS LIKE '...'` | 确认 is_active、last_refresh_state |
| **P2** | audit log 中是否有手动 FORCE REFRESH | 查 audit 表 | 确认触发方式 |
| **P2** | S3 限流时间段的其他 load 任务 | 查 `information_schema.loads` | 确认 S3 限流根因 |

## 7. 修复建议

### 短期修复
1. **手动刷新 MV**：`REFRESH MATERIALIZED VIEW ta_source_activity_log_employee_mv WITH SYNC MODE;`
2. **监控 S3 限流**：确认 S3 请求速率是否已恢复正常

### 长期改进
1. **避免使用 FORCE refresh**（除非必要），改用增量刷新
2. **MV 刷新失败告警**：监控 `information_schema.task_runs` 中的 FAILED 状态
3. **S3 限流防护**：考虑分散 load 任务时间、增加 S3 请求限额
4. **代码层面改进**：force refresh 应在 INSERT 成功后再 drop 旧分区（而非先 drop 再 INSERT），避免这种不一致窗口——这需要 StarRocks 源码改动
