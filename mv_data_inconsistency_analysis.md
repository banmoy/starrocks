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

## 2. 纯靠日志 100% 排除 INSERT OVERWRITE

INSERT OVERWRITE 的整个生命周期中，每个关键步骤都有**唯一对应的日志行**。通过检查这些日志行是否存在，可以 100% 判定 INSERT OVERWRITE 的执行走到了哪一步、是否触碰了源分区。

### 2.1 INSERT OVERWRITE 完整日志指纹图

下面是 INSERT OVERWRITE **每个代码路径**对应的唯一日志行（按执行时序排列）：

```
阶段               日志关键字                                         代码位置                           含义
─────────────────────────────────────────────────────────────────────────────────────────────────────────
[1] 创建 Job       "logCreateInsertOverwrite"                        StmtExecutor.java:2447             editlog 记录（FE 内部，不一定在 fe.log 中出现）
[2] prepare        "dynamic overwrite job {id} begin transaction"    InsertOverwriteJobRunner.java:266  仅 dynamic overwrite 才有
[3] executeInsert  "insert failed: {errMsg}"                         StmtExecutor.java:2700             ← INSERT 写数据失败才有此行
[4] executeInsert  "insert overwrite failed. error message:{}"       InsertOverwriteJobRunner.java:413  ← INSERT 写数据失败且 state=ERR 才有
[5] doCommit ★     "overwrite job {id} replace source partitions"    InsertOverwriteJobRunner.java:646  ← 只有 doCommit 被调用才有！
[6] doCommit ★     "dynamic overwrite job {id} replace"              InsertOverwriteJobRunner.java:700  ← 仅 dynamic overwrite + doCommit
[7] gc             "insert overwrite job {id} start to garbage collect" InsertOverwriteJobRunner.java:437 ← 只有失败走 gc 路径才有
[8] gc             "drop temp partition:{pid}"                       InsertOverwriteJobRunner.java:460  ← gc 清理临时分区
[9] 最终状态        "insert overwrite job:{id} failed"                InsertOverwriteJobRunner.java:162  ← 失败
[10] 最终状态       "insert overwrite job:{id} succeed"               InsertOverwriteJobRunner.java:166  ← 成功
[11] 事务 abort    "successfully rollback"                           DatabaseTransactionMgr.java:636    ← 事务回滚
[12] DML 失败       "failed to handle stmt [insert overwrite ...]"   StmtExecutor.java:2890             ← handleDMLStmt 的 catch 块
```

### 2.2 日志判定逻辑：只需 3 条 grep

拿到 FE 日志后，用 job_id `293714987` 过滤，**只需检查 3 个条件**：

#### 条件 A：doCommit 是否被调用过（判断分区交换是否发生）

```bash
grep "293714987" fe.log | grep "replace source partitions"
```

- **找到了** → doCommit 执行了，分区交换发生了，INSERT OVERWRITE 可能是原因
- **没找到** → doCommit 从未执行，**INSERT OVERWRITE 的分区交换 100% 没发生**

> 原理：`doCommit()` 在执行分区替换之前，**必定**先打印 line 646 的日志：
> ```java
> LOG.info("overwrite job {} replace source partitions:{} to tmp partitions:{}", job.getJobId(), ...);
> ```
> 这行日志在 `replaceTempPartitions()` / `replacePartition()` / `replaceMatchPartitions()` 之前，
> 没有这行日志 = `replaceTempPartitions()` 不可能被调用 = 源分区不可能被 drop。

#### 条件 B：gc 是否被调用（确认走了失败路径）

```bash
grep "293714987" fe.log | grep "start to garbage collect"
```

- **找到了** → 确认走了 gc 路径（gc 只清理临时分区，从不碰源分区）
- **没找到** → 异常（需要进一步排查）

#### 条件 C：最终状态确认

```bash
grep "293714987" fe.log | grep -E "job.*failed|job.*succeed"
```

- 找到 `"failed"` → 确认 INSERT OVERWRITE 以失败结束
- 找到 `"succeed"` → INSERT OVERWRITE 成功了（如果数据还是 0，说明是上游数据为空或其他原因）

### 2.3 100% 排除的判定标准

**当且仅当以下 3 个条件全部成立，可以 100% 排除 INSERT OVERWRITE 导致 MV 数据丢失**：

| 条件 | grep 命令 | 预期结果 |
|------|-----------|----------|
| ① doCommit 未执行 | `grep "293714987" fe.log \| grep "replace source partitions"` | **无匹配** |
| ② gc 已执行 | `grep "293714987" fe.log \| grep "start to garbage collect"` | **有匹配** |
| ③ 最终状态为 failed | `grep "293714987" fe.log \| grep "job.*failed"` | **有匹配** |

**三条都满足 → 实锤排除 INSERT OVERWRITE。** 逻辑闭环：
- ① 证明分区交换从未发生 → 源分区的 drop（在 `replaceTempPartitions()` 内部）不可能被执行
- ② 证明走了失败路径 → gc 只清理临时分区（`dropTempPartition`），源码中没有任何操作源分区的代码
- ③ 确认 job 以失败结束 → 没有后续的 doCommit 被延迟执行的可能

### 2.4 补充验证：从已有日志直接确认

实际上，你给出的日志**已经包含了部分实锤信息**：

**证据 1：事务 ABORTED（已有）**
```
transaction status: ABORTED, error replicas num: 0
```
事务 ABORTED = 数据没有 commit = 临时分区中的数据不会生效。

**证据 2：`handleDMLStmt` 抛异常（已有）**
```
[StmtExecutor.handleDMLStmt():2729] insert failed: 172.31.175.175: starlet err ...
```
这是 `handleDMLStmt` 中 `coord.getExecStatus()` 不 OK 时打印的（line 2700），此时 `executeInsert()` 还没返回。

**证据 3：`failed to handle stmt`（已有）**
```
[StmtExecutor.handleDMLStmt():2919] failed to handle stmt [insert overwrite ...]
```
这是 `handleDMLStmt` 的 catch 块（line 2890），说明整个 DML 执行异常退出。

**还缺的关键一条**：需要确认 `"overwrite job 293714987 replace source partitions"` 不存在。这条日志是唯一能打印在 "分区替换" 之前的日志，只有找不到它，才能 100% 排除。

### 2.5 如果 doCommit 确实执行了呢？（逆向分析）

假设 grep 找到了 `"replace source partitions"`，说明 doCommit 被调用了。这意味着：
1. `executeInsert()` 实际上成功了（尽管之前报错）
2. 进入了 `doCommit()` → `replaceTempPartitions()` / `replacePartition()`
3. 在 `replaceTempPartitions()` 中，先 drop 了源分区（line 3003-3006），再 add 临时分区（line 3010-3018）
4. 如果在两步之间发生异常（极端情况），可能导致数据丢失

但这种情况**和给出的日志矛盾**——日志明确显示事务 ABORTED，而 doCommit 需要事务已 commit 的数据才有意义。所以这种可能性可以提前排除。

## 3. 真正嫌疑：在 INSERT OVERWRITE 之外发生的分区 Drop

INSERT OVERWRITE 被排除后，MV 数据归零只可能发生在 INSERT OVERWRITE **之前**或**之外**。

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
// ② 之后才执行 syncAddOrDropPartitions → refreshMaterializedView → INSERT OVERWRITE
// 如果 INSERT OVERWRITE 失败，MV 就是空的
```

**关键：这个 drop 操作不在 INSERT OVERWRITE 的 Job 管理范围内，不受临时分区机制保护。**

### 3.2 Force Refresh 的日志指纹

```bash
# Force refresh 的唯一日志（line 990）
grep "force refresh, drop partitions" fe.log
```

如果在 MV 数据归零的时间窗口内找到了这条日志，结合 INSERT OVERWRITE 失败，就可以 100% 确认根因。

### 3.3 排查决策树

```
Step 1: grep "293714987" fe.log | grep "replace source partitions"
        ├── 找到 → INSERT OVERWRITE 的 doCommit 执行了（但和 ABORTED 事务矛盾，需深入分析）
        └── 没找到 → INSERT OVERWRITE 100% 排除
            │
            Step 2: grep "force refresh, drop partitions" fe.log  (时间范围过滤)
            ├── 找到 → Force Refresh drop 了分区 + INSERT 失败 → 根因确认
            └── 没找到 → Step 3: 检查其他可能
                ├── grep "DROP MATERIALIZED VIEW\|TRUNCATE\|ALTER.*DROP PARTITION" fe.audit.log
                ├── 查 information_schema.task_runs 的刷新历史
                └── 查 MV 是否 inactive
```

### 3.4 完整的一键排查脚本

```bash
#!/bin/bash
# 用法: ./check_mv_issue.sh <fe.log路径> <job_id>
# 示例: ./check_mv_issue.sh /path/to/fe.log 293714987

LOG_FILE=$1
JOB_ID=$2

echo "====== 1. 检查 INSERT OVERWRITE doCommit 是否执行（分区交换） ======"
result=$(grep "$JOB_ID" "$LOG_FILE" | grep "replace source partitions")
if [ -z "$result" ]; then
    echo "[PASS] doCommit 未执行。INSERT OVERWRITE 的分区交换没有发生。"
    echo "       → INSERT OVERWRITE 100% 排除为数据丢失原因。"
else
    echo "[ALERT] doCommit 被执行了！需要进一步分析："
    echo "$result"
fi

echo ""
echo "====== 2. 检查 gc 是否执行（确认走了失败路径） ======"
result=$(grep "$JOB_ID" "$LOG_FILE" | grep "start to garbage collect")
if [ -n "$result" ]; then
    echo "[PASS] gc 已执行，INSERT OVERWRITE 走了失败清理路径。"
    echo "$result"
else
    echo "[WARN] 未找到 gc 日志，需要检查 job 是否正常结束。"
fi

echo ""
echo "====== 3. 检查 INSERT OVERWRITE 最终状态 ======"
grep "$JOB_ID" "$LOG_FILE" | grep -E "job.*failed|job.*succeed"

echo ""
echo "====== 4. 检查事务状态 ======"
grep "$JOB_ID" "$LOG_FILE" | grep -E "ABORTED|successfully rollback|COMMITTED|VISIBLE"

echo ""
echo "====== 5. 检查是否有 Force Refresh（真正嫌疑） ======"
echo "--- 搜索 force refresh drop 日志 ---"
grep "force refresh, drop partitions" "$LOG_FILE" | tail -20

echo ""
echo "====== 6. 搜索该 Job 的所有日志（完整时间线） ======"
grep "$JOB_ID" "$LOG_FILE" | head -50
```

## 4. 针对 doCommit() 非原子性的额外排除

有人可能怀疑 `replaceTempPartitions()` 的非原子性（先 drop 旧分区、再 add 新分区）导致问题。但可以排除：

| 排除理由 | 说明 |
|----------|------|
| `doCommit()` 未被调用 | 日志中不存在 `"replace source partitions"` 即可 100% 确认 |
| 在写锁内执行 | `doCommit()` 获取了 TABLE WRITE LOCK，两步操作在同一个锁内，不会被并发打断 |
| 纯内存操作 | `replaceTempPartitions()` 是 FE 内存中的 metadata 操作，不涉及 I/O，不会因 S3 异常中断 |
| 只有 JVM crash 才可能打断 | 在 drop 旧分区和 add 新分区之间，只有 FE 进程 crash 才能中断，但这会触发 editlog replay 恢复 |

## 5. 需要确认的信息汇总

| 优先级 | 确认项 | 操作 | 目的 |
|--------|--------|------|------|
| **P0** | `"replace source partitions"` 是否存在 | `grep "293714987" fe.log \| grep "replace source partitions"` | **100% 判定 doCommit 是否执行** |
| **P0** | `"start to garbage collect"` 是否存在 | `grep "293714987" fe.log \| grep "start to garbage collect"` | **确认走了失败清理路径** |
| **P0** | `"force refresh, drop partitions"` 是否存在 | `grep "force refresh, drop partitions" fe.log` | **确认是否 Force Refresh drop 了分区** |
| **P1** | MV DDL（是否为非分区表） | `SHOW CREATE MATERIALIZED VIEW` | 确认 force refresh 的影响范围 |
| **P1** | task_runs 中的 forceRefresh 字段 | 查 `information_schema.task_runs` | 从元数据侧确认 |
| **P2** | audit log 中是否有手动 FORCE REFRESH | 查 audit 表 | 确认触发方式 |

## 7. 修复建议

### 短期修复
1. **手动刷新 MV**：`REFRESH MATERIALIZED VIEW ta_source_activity_log_employee_mv WITH SYNC MODE;`
2. **监控 S3 限流**：确认 S3 请求速率是否已恢复正常

### 长期改进
1. **避免使用 FORCE refresh**（除非必要），改用增量刷新
2. **MV 刷新失败告警**：监控 `information_schema.task_runs` 中的 FAILED 状态
3. **S3 限流防护**：考虑分散 load 任务时间、增加 S3 请求限额
4. **代码层面改进**：force refresh 应在 INSERT 成功后再 drop 旧分区（而非先 drop 再 INSERT），避免这种不一致窗口——这需要 StarRocks 源码改动
