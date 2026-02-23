# Tablet Split 旧 Tablet 清理流程

## 概述

Tablet split 完成后，旧 tablet 的清理涉及三个层面：FE 元数据、StarManager shard 元数据、远程存储上的数据文件。三者的清理由不同组件在不同时机完成，且数据文件因新旧 tablet 间共享而需要特殊处理。

## 背景：Split 后的数据共享

Tablet split 是一个纯元数据操作，不复制数据文件。假设 tablet A 被 split 为 B 和 C：

- B 和 C 的 metadata 中 rowsets 引用的 segment 文件名与 A 完全相同
- split 发布时，`set_all_data_files_shared()` 将 A、B、C 三者 metadata 中的所有数据文件（segment、del、delvec、dcg、sstable）标记为 `shared=true`
- 这个 shared 标记持久化在远程存储的 tablet metadata protobuf 文件中

```
Split 前:
  A(v5): seg_001, seg_002  (shared_segments=[])

Split 后:
  A(v6): seg_001, seg_002  (shared_segments=[true, true])  ← 旧 tablet，即将被清理
  B(v6): seg_001, seg_002  (shared_segments=[true, true])  ← 新 tablet，range=[0, 500)
  C(v6): seg_001, seg_002  (shared_segments=[true, true])  ← 新 tablet，range=[500, 1000)
```

新 index 使用与旧 index 相同的 `shardGroupId`，因此旧 tablet 的 shard 和新 tablet 的 shard 处于同一个 shard group 内。

## 第一层：FE 元数据清理

### 触发时机

`SplitTabletJob` 的 CLEANING 阶段（`runCleaningJob`），在 split 发布后立即进入。

### 前置条件

等待所有在 split 发布前开始的事务完成（`isPreviousTransactionsFinished`），确保没有并发操作还在引用旧 tablet。

### 清理内容

`removeOldMaterializedIndexes()` 持有表写锁后：

1. 从 `PhysicalPartition.indexMetaIdToIndexIds` 的列表中移除旧 index id
2. 从 `PhysicalPartition.idToVisibleIndex` 中移除旧 `MaterializedIndex` 对象
3. 从 `TabletInvertedIndex` 中删除旧 tablet 的映射

### 效果

旧 tablet 从 FE 内存元数据中消失。查询、写入、autovacuum 等所有 FE 驱动的操作不再能看到旧 tablet。但旧 tablet 的 shard 仍注册在 StarManager 中，远程存储上的 metadata 和数据文件仍存在。

### 关键代码

```
SplitTabletJob.runCleaningJob()
  → isPreviousTransactionsFinished(endTransactionId, ...)   // 等待事务 drain
  → removeOldMaterializedIndexes()                          // 清理 FE 元数据
  → unregisterReshardingTablets()                           // 注销 resharding tablet 注册
  → setTableState(TABLET_RESHARD, NORMAL)                   // 恢复表状态
```

## 第二层：StarManager Shard 元数据清理

### 触发时机

`StarMgrMetaSyncer` 后台 daemon 周期性执行 `syncTableMetaAndColocationInfo()`。

### 发现冗余 shard

对每个表的每个 `MaterializedIndex`：

1. 从 StarManager 获取该 shard group 下的所有 shard ID
2. 遍历 FE 中 `materializedIndex.getTablets()` 的 tablet ID，从集合中逐一移除
3. 剩余的 shard ID 即为冗余 shard（旧 tablet）

由于新旧 index 使用相同的 `shardGroupId`，这个差集比对能准确找出旧 tablet。

### 清理流程

`dropTabletAndDeleteShard()` 执行两步：

1. **删除 tablet 数据**：按 shard 所在 BE 节点分组，发送 `DeleteTabletRequest` RPC（触发第三层清理）
2. **删除 shard 元数据**：调用 `starOSAgent.deleteShards()` 从 StarManager 移除 shard 注册信息

### 关键代码

```
StarMgrMetaSyncer.syncTableMetaAndColocationInfo()
  → syncTableMetaInternal(db, table, forceDeleteData=true)
    → listShard(groupId)                                    // 获取 StarManager 中的 shard
    → 对比 FE 中的 tablet ID，得到冗余 shard 集合
    → dropTabletAndDeleteShard(shardIds)                    // 删数据 + 删元数据
      → LakeService.deleteTablet(request)                   // RPC 到 BE
      → starOSAgent.deleteShards(shardToDelete)             // 删 StarManager 元数据
```

## 第三层：远程存储数据文件清理

### 触发时机

BE 收到 `DeleteTabletRequest` 后执行 `delete_tablets_impl()`。

### 共享文件的处理

删除旧 tablet A 时，对 shared 文件的处理贯穿三个环节：

**环节一：txn log 中的文件**

遍历 tablet A 的 txn log，对每个文件检查 `is_shared_segment()`：
- 非共享 → 删除
- 共享 → 跳过

**环节二：旧版本 metadata 中的 garbage 文件（compaction_inputs、orphan_files）**

共享的 garbage 文件被路由到 `dummy_shared_file_deleter`，其 `finish()` 从未被调用，等效于丢弃。

**环节三：最新版本 metadata 中的活跃 rowset**

对 tablet A 最新 metadata 中的每个数据文件：
- `!is_shared_segment(rowset, i) || allow_delete_shared_files` 为判断条件
- 对于非 file-bundling 的 tablet，`allow_delete_shared_files` = false
- 共享文件 → 条件不满足 → 跳过

**总结**：删除旧 tablet 时只清理非共享文件和 metadata 文件，共享的数据文件全部跳过。

### 清理的文件

| 文件类型 | 是否删除 |
|---------|---------|
| tablet metadata 文件（各版本） | 删除 |
| txn log 文件 | 删除 |
| txn log 中引用的非共享 segment | 删除 |
| txn log 中引用的共享 segment | 跳过 |
| 活跃 rowset 中的非共享 segment | 删除 |
| 活跃 rowset 中的共享 segment | 跳过 |

### 关键代码

```
delete_tablets_impl(tablet_mgr, root_dir, tablet_ids)
  → iterate txn logs
    → delete_files_under_txnlog()                           // 跳过 shared segment
  → iterate metadata versions
    → collect_garbage_files(..., &dummy_shared_file_deleter) // shared garbage 被丢弃
  → 处理 latest_metadata 的活跃 rowset
    → is_shared_segment() → true → 跳过
  → 删除 metadata 文件
```

## 共享数据文件的最终回收

共享文件由新 tablet 的 autovacuum 负责回收，而非旧 tablet 的删除流程。

### Autovacuum 的 tablet 范围

FE `AutovacuumDaemon` 通过 `partition.getLatestMaterializedIndices(VISIBLE)` 收集 tablet 列表，只包含新 tablet B 和 C，旧 tablet A 不在其中。

### 回收条件

当新 tablet B 和 C 都完成 compaction 后，共享文件从活跃 rowset 变为 `compaction_inputs`（garbage）。BE 端 `vacuum_tablet_metadata` 通过 `AsyncSharedFileDeleter` 处理：

1. `collect_garbage_files()`：从 B 和 C 的旧版本 metadata 的 `compaction_inputs` 中收集 shared 文件到候选集（`_pending_files`）
2. `collect_alive_shared_files()`：从 B 和 C 的最新 metadata 中收集仍在使用的 shared 文件到延迟集（`_delay_delete_files`）
3. `finish()`：只删除在候选集中但不在延迟集中的文件

### 时间线示例

```
T1: Split 完成，A 从 FE 移除
    B(v6): seg_001(shared), seg_002(shared)
    C(v6): seg_001(shared), seg_002(shared)

T2: B compaction → B(v7): seg_004
    seg_001, seg_002 进入 B 的 compaction_inputs

T3: Autovacuum [B, C]
    候选集 = {seg_001, seg_002}  (来自 B 的 garbage)
    延迟集 = {seg_001, seg_002}  (来自 C 的活跃 metadata)
    → 不删除

T4: C compaction → C(v7): seg_005
    seg_001, seg_002 进入 C 的 compaction_inputs

T5: Autovacuum [B, C]
    候选集 = {seg_001, seg_002}  (来自 B 和 C 的 garbage)
    延迟集 = {}                  (B 和 C 的最新 metadata 都不再引用)
    → 删除 seg_001, seg_002

T6: StarMgrMetaSyncer 删除 tablet A
    读取 A 的 metadata，shared 文件跳过（文件已在 T5 删除，跳过也无影响）
    删除 A 的 metadata 文件和 shard 元数据
```

### 安全性

- 旧 tablet A 已从 FE 移除，不会有任何查询访问它
- 新 tablet 的 vacuum 决策范围仅限于 FE 可见的活跃 tablet
- StarMgrMetaSyncer 删除 A 时对 shared 文件本身就跳过，不受文件是否已被删除的影响

## 三层清理总览

| 层面 | 执行者 | 时机 | 清理内容 |
|------|--------|------|---------|
| FE 元数据 | `SplitTabletJob.runCleaningJob()` | Split 完成后立即 | `PhysicalPartition`、`TabletInvertedIndex` 中的旧 index/tablet |
| StarManager 元数据 | `StarMgrMetaSyncer` 后台 | 周期性 | 比对发现冗余 shard，通过 RPC 触发 BE 删除，再删 shard 元数据 |
| 远程存储 - 非共享文件 | BE `delete_tablets_impl` | StarMgrMetaSyncer RPC 触发 | metadata 文件、txn log、非共享 segment |
| 远程存储 - 共享文件 | BE `vacuum_tablet_metadata` | 新 tablet autovacuum | 所有新 tablet compaction 后不再引用的 shared segment |
