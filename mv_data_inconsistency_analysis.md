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
| 来源 | `PartitionBasedMvRefreshProcessor.refreshMaterializedView()` → `InsertOverwriteJobRunner` |

错误发生在 `executeInsert()` 阶段（数据写入临时分区），尚未到达 `doCommit()`（分区交换）阶段。

## 2. INSERT OVERWRITE 机制分析

StarRocks MV 刷新使用 `INSERT OVERWRITE` 机制，流程如下：

```
1. createTempPartitions()  → 创建临时分区
2. prepareInsert()         → 将 INSERT 目标改为临时分区
3. executeInsert()         → 向临时分区写入数据  ← 此处失败
4. doCommit()              → 将临时分区与源分区交换（原子替换）
```

**正常失败路径**：如果 `executeInsert()` 失败，会进入 `gc()` 方法，只清理临时分区，源分区数据不受影响。也就是说，**单纯的 INSERT 失败不会导致 MV 数据丢失**。

## 3. 可能导致 MV 行数为 0 的场景

### 场景 A：Force Refresh + INSERT 失败（**最大嫌疑**）

代码位置：`PartitionBasedMvRefreshProcessor.syncPartitions()` (line 986-1029)

当 MV 刷新为 **force refresh** 模式时：
1. **先 drop 现有分区**（对于非分区表，drop 后立即 rebuild 空分区）
2. 然后才执行 INSERT OVERWRITE

如果 step 1 成功（分区已清空），但 step 2 的 INSERT OVERWRITE 因 S3 503 失败，MV 就会处于**空分区、0 行数据**的状态。

**关键代码**：
```java
if (mvRefreshParams.isForce() && !tentative) {
    // drop existing partitions for force refresh
    if (!mv.isPartitionedTable()) {
        mv.dropPartition(db.getId(), partitionName, false);  // 清空分区
        localMetastore.buildNonPartitionOlapTable(db, mv, ...);  // 重建空分区
    }
}
// 之后才执行 INSERT OVERWRITE，如果失败就会留下空分区
```

### 场景 B：doCommit() 中的非原子替换（可能性较低）

`replaceTempPartitions()` 的实现分两步：
1. Drop 所有源分区
2. Add 所有临时分区

如果在 step 1 和 step 2 之间发生异常（如 JVM crash），可能导致数据丢失。但这种情况在单次异常中发生概率较低。

### 场景 C：多次刷新失败累积（需要排查）

如果 MV 刷新任务被配置为持续重试，且每次 force refresh 都在 drop 后 INSERT 失败，最终 MV 会一直保持 0 行。

## 4. 排查步骤和需要确认的信息

### Step 1：确认 MV 当前状态

```sql
SHOW MATERIALIZED VIEWS LIKE 'ta_source_activity_log_employee_mv'\G
```

关注字段：
- `is_active` — MV 是否仍处于活跃状态
- `last_refresh_state` — 最后一次刷新是否成功
- `last_refresh_error_message` — 最后一次刷新错误信息
- `rows` — 当前行数
- `last_refresh_start_time` / `last_refresh_finished_time` — 最后刷新时间

### Step 2：查看刷新历史

```sql
-- 先获取 MV 的 table_id
SELECT TABLE_ID FROM information_schema.materialized_views
WHERE TABLE_NAME = 'ta_source_activity_log_employee_mv';

-- 查看刷新历史（替换 <mv_id>）
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
ORDER BY CREATE_TIME DESC
LIMIT 50;
```

**重点确认**：
- 数据变为 0 之前的最后一次成功刷新是什么时候
- 是否有 `forceRefresh=true` 的刷新任务
- 失败次数和频率

### Step 3：确认 MV 是否为分区表

```sql
SHOW CREATE MATERIALIZED VIEW ta_source_activity_log_employee_mv;
```

**关键点**：从日志中的 SQL 来看，这个 MV 是从 `analytics.employee` 表 SELECT 数据，没有看到分区键，**很可能是非分区 MV**。如果是非分区 MV，force refresh 会 drop 唯一的分区并重建空分区，INSERT 失败后就会是 0 行。

### Step 4：确认刷新触发方式

```sql
-- 查看 MV 的刷新策略
SHOW CREATE MATERIALIZED VIEW ta_source_activity_log_employee_mv;
```

关注：
- 是 `REFRESH ASYNC` 还是 `REFRESH MANUAL`
- 刷新间隔是多少
- 是否有人手动执行了 `REFRESH MATERIALIZED VIEW ... FORCE`

### Step 5：确认 S3 限流情况

```sql
-- 查看同时间段是否有大量 load 任务
SELECT * FROM information_schema.loads
WHERE CREATE_TIME BETWEEN '2026-03-09 21:00:00' AND '2026-03-09 22:00:00'
ORDER BY CREATE_TIME;
```

S3 503 "Please reduce your request rate" 说明该时间段 S3 请求过多。需确认：
- 是否有其他大批量 load/export 任务同时在跑
- 是否 S3 bucket 整体限流

### Step 6：确认数据是否可恢复

```sql
-- 尝试手动刷新 MV
REFRESH MATERIALIZED VIEW ta_source_activity_log_employee_mv WITH SYNC MODE;

-- 刷新后检查行数
SELECT COUNT(*) FROM ta_source_activity_log_employee_mv;
```

## 5. 根因判断

根据目前信息，**最可能的根因**是：

> **MV 的一次 force refresh 刷新操作中，先成功清空了 MV 的分区数据，但随后的 INSERT OVERWRITE 因 S3 限流（503）失败，导致 MV 被留在了"已清空但未重新填充"的状态，行数变为 0。**

支撑论据：
1. 日志明确显示 INSERT OVERWRITE 因 S3 503 失败并 ABORTED
2. INSERT OVERWRITE 的 `executeInsert()` 失败不会丢失原始数据（临时分区机制保护），但 force refresh 会在 INSERT OVERWRITE 之前先 drop 分区
3. 该 MV 查询的是 `analytics.employee` 全表，极可能是非分区 MV，force refresh 会导致整个 MV 数据被清空

## 6. 修复建议

### 短期修复
1. **手动刷新 MV**：`REFRESH MATERIALIZED VIEW ta_source_activity_log_employee_mv WITH SYNC MODE;`
2. **监控 S3 限流**：确认 S3 请求速率是否已恢复正常

### 长期改进
1. **避免使用 FORCE refresh**（除非必要），改用增量刷新
2. **MV 刷新失败告警**：监控 `information_schema.task_runs` 中的 FAILED 状态
3. **S3 限流防护**：考虑分散 load 任务时间、增加 S3 请求限额
4. **代码层面改进**：force refresh 应在 INSERT 成功后再 drop 旧分区（而非先 drop 再 INSERT），避免这种不一致窗口——这需要 StarRocks 源码改动

## 7. 需要你确认的信息汇总

| # | 确认项 | 目的 |
|---|--------|------|
| 1 | `SHOW MATERIALIZED VIEWS` 的输出 | 确认当前状态和最后刷新信息 |
| 2 | `information_schema.task_runs` 的刷新历史 | 确认是否有 force refresh 且失败 |
| 3 | MV 的 CREATE 语句 | 确认是否为非分区表、刷新策略 |
| 4 | 是否有人手动执行了 FORCE refresh | 确认触发方式 |
| 5 | 同时间段的其他 load 任务情况 | 确认 S3 限流原因 |
| 6 | 手动 REFRESH 后是否恢复 | 确认数据源是否正常 |
