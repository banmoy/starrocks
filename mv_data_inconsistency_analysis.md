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

### 2.2 重要澄清：293714987 不是 InsertOverwriteJob ID

从日志中出现 `293714987` 的两个位置：

```
[DefaultCoordinator.updateStatus():885] ... job id: 293714987, query id: 9463fd68-...
```
```
callback id: [-1, 293714987]
```

**`293714987` 是 `InsertLoadJob` ID**，不是 `InsertOverwriteJob` ID。它们是两个不同的东西：

| ID | 来源 | 含义 |
|----|------|------|
| `InsertOverwriteJob` ID | `GlobalStateMgr.getNextId()`（StmtExecutor.java:2436） | INSERT OVERWRITE 的外层 Job，管理 "创建临时分区→写入→交换" 的整个生命周期 |
| `InsertLoadJob` ID（293714987） | `LoadMgr.registerInsertLoadJob()`（StmtExecutor.java:2620-2634） | 内层写入阶段注册的 load job，用于 Coordinator 跟踪和监控 |

调用链：

```
MV refresh
  → handleDMLStmtWithProfile (外层)
    → handleDMLStmt
      → handleInsertOverwrite
        → InsertOverwriteJob 创建 (ID = getNextId(), 这个 ID 不在你的日志里)
        → InsertOverwriteJobRunner.run()
          → doLoad()
            → executeInsert()
              → handleDMLStmt (内层, 写入临时分区)
                → InsertLoadJob 注册 (ID = 293714987, 这个在你的日志里)
                → Coordinator 执行 (loadJobId = 293714987)
                → S3 503 失败 ← 异常从这里抛出
```

因此 `InsertOverwriteJobRunner` 的日志（如 `"overwrite job {id} replace source partitions"`）打印的是 **InsertOverwriteJob ID**，不是 `293714987`。用 `293714987` grep 不到 `InsertOverwriteJobRunner` 的日志。

### 2.3 正确的日志搜索方式

由于我们不知道 InsertOverwriteJob ID 是什么，需要用**其他信息**来关联。有以下几种方式：

#### 方式 1：用 query_id 搜索（最可靠）

日志中给出了 `query_id=9463fd68-1bfe-11f1-87aa-0a71f84af585`，这是 MV 刷新任务的 query ID，在整个执行链上下文中共享。

```bash
grep "9463fd68-1bfe-11f1-87aa-0a71f84af585" fe.log
```

这会返回该次 MV 刷新相关的**所有日志**，包括 `InsertOverwriteJobRunner` 的日志。

#### 方式 2：用 MV 表名搜索

```bash
grep "ta_source_activity_log_employee_mv" fe.log
```

#### 方式 3：直接搜索关键日志行（不依赖任何 ID）

```bash
# 搜索所有 INSERT OVERWRITE 的分区交换日志
grep "replace source partitions" fe.log
```

### 2.4 日志判定逻辑：3 步 100% 排除

#### 条件 A：doCommit 是否被调用过（核心判定）

```bash
# 搜索该 query_id 关联的所有日志中，是否有分区交换
grep "9463fd68-1bfe-11f1-87aa-0a71f84af585" fe.log | grep "replace source partitions"
```

- **没有匹配** → doCommit 从未执行，**INSERT OVERWRITE 的分区交换 100% 没发生**
- **有匹配** → doCommit 被调用了，需要进一步分析

> 原理：`doCommit()` 在执行分区替换之前，**必定**先打印 line 646 的日志：
> ```java
> LOG.info("overwrite job {} replace source partitions:{} to tmp partitions:{}", job.getJobId(), ...);
> ```
> 没有这行日志 = `replaceTempPartitions()` 不可能被调用 = 源分区不可能被 drop。

但注意：`"replace source partitions"` 日志打印的是 InsertOverwriteJob ID，不含 query_id。所以如果 grep query_id 搜不到，还需要**反向确认**——搜 gc 日志：

```bash
# 搜索该 query_id 关联的日志中，是否有 gc（失败清理）
grep "9463fd68-1bfe-11f1-87aa-0a71f84af585" fe.log | grep "start to garbage collect"
```

如果也搜不到（因为 gc 日志也是打印 InsertOverwriteJob ID），则需要换用方式 3：

```bash
# 搜索 3 月 9 日 21:35-21:37 时间窗口内的所有 overwrite 分区交换日志
grep "2026-03-09 21:3[5-7]" fe.log | grep "replace source partitions"
```

```bash
# 搜索同时间窗口的 gc 日志
grep "2026-03-09 21:3[5-7]" fe.log | grep "start to garbage collect"
```

```bash
# 搜索同时间窗口的 overwrite 成功/失败日志
grep "2026-03-09 21:3[5-7]" fe.log | grep "insert overwrite job.*failed\|insert overwrite job.*succeed"
```

#### 条件 B：gc 是否执行（确认走了失败路径）

```bash
grep "2026-03-09 21:3[5-7]" fe.log | grep "start to garbage collect"
```

- **找到了** → 走了 gc 路径（gc 只清理临时分区，从不碰源分区）

#### 条件 C：最终状态确认

```bash
grep "2026-03-09 21:3[5-7]" fe.log | grep "insert overwrite job.*failed\|insert overwrite job.*succeed"
```

- 找到 `"failed"` → INSERT OVERWRITE 以失败结束
- 找到 `"succeed"` → INSERT OVERWRITE 成功了

### 2.5 100% 排除的判定标准

**当且仅当以下 3 个条件全部成立，可以 100% 排除 INSERT OVERWRITE 导致 MV 数据丢失**：

| 条件 | grep 命令 | 预期结果 | 证明了什么 |
|------|-----------|----------|-----------|
| ① doCommit 未执行 | 时间窗口内搜 `"replace source partitions"` | **无匹配** | 分区交换从未发生，源分区 100% 没被碰 |
| ② gc 已执行 | 时间窗口内搜 `"start to garbage collect"` | **有匹配** | 走了 `gc()` 路径，gc 只清理临时分区 |
| ③ 状态为 failed | 时间窗口内搜 `"insert overwrite job.*failed"` | **有匹配** | Job 以失败结束，不存在延迟 doCommit 的可能 |

**三条都满足 → 实锤排除 INSERT OVERWRITE。**

### 2.6 从已有日志直接能确认的部分

你给出的日志**已经包含了部分证据**：

**证据 1：事务 ABORTED**
```
transaction status: ABORTED, error replicas num: 0
```
事务 ABORTED = 临时分区中的数据不会生效。而 `doCommit()` 中的 `replaceTempPartitions()` 是纯 FE 内存操作，不依赖事务状态。但是 `doCommit()` 是在 `executeInsert()` 之后调用的，`executeInsert()` 已经抛出了异常，控制流不可能到达 `doCommit()`。

**证据 2：`handleDMLStmt` 抛异常**
```
[StmtExecutor.handleDMLStmt():2729] insert failed: 172.31.175.175: starlet err ...
```
这是 `handleDMLStmt` 中 `coord.getExecStatus()` 不 OK 时打印的（line 2700）。这个 `handleDMLStmt` 是 `InsertOverwriteJobRunner.executeInsert()` 内部调用的（line 409）。异常会导致 `executeInsert()` 抛出，`doLoad()` 中下一行的 `doCommit()` 不会执行。

**证据 3：`failed to handle stmt`**
```
[StmtExecutor.handleDMLStmt():2919] failed to handle stmt [insert overwrite ...]
```
这是 `handleDMLStmt` catch 块（line 2890），确认 DML 执行异常退出。

**还缺一条确认**：需要搜 FE 日志确认 `"replace source partitions"` 不存在。这是唯一能从日志层面做到 100% 无死角排除的方式。

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
Step 1: 时间窗口内搜 "replace source partitions"
        ├── 找到 → INSERT OVERWRITE 的 doCommit 执行了（但和 ABORTED 事务矛盾，需深入分析）
        └── 没找到 → INSERT OVERWRITE 100% 排除
            │
            Step 2: 搜 "force refresh, drop partitions"（时间范围过滤）
            ├── 找到 → Force Refresh drop 了分区 + INSERT 失败 → 根因确认
            └── 没找到 → Step 3: 检查其他可能
                ├── grep "DROP MATERIALIZED VIEW\|TRUNCATE\|ALTER.*DROP PARTITION" fe.audit.log
                ├── 查 information_schema.task_runs 的刷新历史
                └── 查 MV 是否 inactive
```

### 3.4 完整的一键排查脚本

```bash
#!/bin/bash
# 用法: ./check_mv_issue.sh <fe.log路径> <query_id> <时间前缀>
# 示例: ./check_mv_issue.sh /path/to/fe.log 9463fd68-1bfe-11f1-87aa-0a71f84af585 "2026-03-09 21:3"
#
# 注意：日志中有两种 ID：
#   - InsertLoadJob ID (293714987): Coordinator/事务日志使用，可用 query_id 关联
#   - InsertOverwriteJob ID: InsertOverwriteJobRunner 日志使用，ID 值不在已知日志中
# 因此脚本同时用 query_id 和时间窗口两种方式搜索

LOG_FILE=$1
QUERY_ID=$2
TIME_PREFIX=$3

echo "====== 1. 检查 INSERT OVERWRITE doCommit 是否执行（分区交换） ======"
echo "--- 1a. 用 query_id 搜索 ---"
result=$(grep "$QUERY_ID" "$LOG_FILE" | grep "replace source partitions")
if [ -z "$result" ]; then
    echo "[INFO] 通过 query_id 未找到 doCommit 日志"
else
    echo "[ALERT] doCommit 被执行了！"
    echo "$result"
fi
echo "--- 1b. 用时间窗口搜索（因为 doCommit 日志用的是 InsertOverwriteJob ID，不含 query_id）---"
result=$(grep "$TIME_PREFIX" "$LOG_FILE" | grep "replace source partitions")
if [ -z "$result" ]; then
    echo "[PASS] 时间窗口内无 doCommit 日志 → INSERT OVERWRITE 分区交换未发生"
else
    echo "[ALERT] 时间窗口内找到 doCommit 日志，需确认是否属于该 MV："
    echo "$result"
fi

echo ""
echo "====== 2. 检查 gc 是否执行（确认走了失败路径） ======"
result=$(grep "$TIME_PREFIX" "$LOG_FILE" | grep "start to garbage collect")
if [ -n "$result" ]; then
    echo "[PASS] gc 已执行，INSERT OVERWRITE 走了失败清理路径"
    echo "$result"
else
    echo "[WARN] 未找到 gc 日志"
fi

echo ""
echo "====== 3. 检查 INSERT OVERWRITE 最终状态 ======"
grep "$TIME_PREFIX" "$LOG_FILE" | grep -E "insert overwrite job.*failed|insert overwrite job.*succeed"

echo ""
echo "====== 4. 检查事务状态（用 query_id）======"
grep "$QUERY_ID" "$LOG_FILE" | grep -E "ABORTED|successfully rollback|COMMITTED|VISIBLE"

echo ""
echo "====== 5. 检查是否有 Force Refresh（真正嫌疑） ======"
grep "force refresh, drop partitions" "$LOG_FILE" | tail -20

echo ""
echo "====== 6. 搜索 query_id 的所有日志（完整时间线） ======"
grep "$QUERY_ID" "$LOG_FILE" | head -50

echo ""
echo "====== 7. 搜索 MV 表名相关日志 ======"
grep "ta_source_activity_log_employee_mv" "$LOG_FILE" | grep "$TIME_PREFIX" | head -30
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
| **P0** | `"replace source partitions"` 是否存在 | 时间窗口 `"2026-03-09 21:3"` 内搜 fe.log | **100% 判定 doCommit 是否执行** |
| **P0** | `"start to garbage collect"` 是否存在 | 同上时间窗口搜 fe.log | **确认走了失败清理路径** |
| **P0** | `"force refresh, drop partitions"` 是否存在 | `grep "force refresh, drop partitions" fe.log` | **确认是否 Force Refresh drop 了分区** |
| **P1** | MV DDL（是否为非分区表） | `SHOW CREATE MATERIALIZED VIEW` | 确认 force refresh 的影响范围 |
| **P1** | task_runs 中的 forceRefresh 字段 | 查 `information_schema.task_runs` | 从元数据侧确认 |
| **P2** | audit log 中是否有手动 FORCE REFRESH | 查 audit 表 | 确认触发方式 |

### 附录：日志中各 ID 对照表

| 日志中的值 | 实际含义 | 出处 |
|-----------|---------|------|
| `293714987` | **InsertLoadJob ID**（内层 load job） | `LoadMgr.registerInsertLoadJob()` → `loadJob.getId()` |
| `9463fd68-1bfe-11f1-87aa-0a71f84af585` | **query_id**（MV 刷新任务的执行 ID） | `ConnectContext.getExecutionId()` |
| `36775388` | **transaction ID** | `GlobalTransactionMgr.beginTransaction()` |
| InsertOverwriteJob ID | **未在已有日志中出现**，需要从 fe.log 中搜索 | `GlobalStateMgr.getNextId()` |
| `insert_9463fd68-1bfe-11f1-87aa-0a71f84af585` | **事务 label** | 格式为 `insert_<query_id>` |

## 7. 修复建议

### 短期修复
1. **手动刷新 MV**：`REFRESH MATERIALIZED VIEW ta_source_activity_log_employee_mv WITH SYNC MODE;`
2. **监控 S3 限流**：确认 S3 请求速率是否已恢复正常

### 长期改进
1. **避免使用 FORCE refresh**（除非必要），改用增量刷新
2. **MV 刷新失败告警**：监控 `information_schema.task_runs` 中的 FAILED 状态
3. **S3 限流防护**：考虑分散 load 任务时间、增加 S3 请求限额
4. **代码层面改进**：force refresh 应在 INSERT 成功后再 drop 旧分区（而非先 drop 再 INSERT），避免这种不一致窗口——这需要 StarRocks 源码改动
