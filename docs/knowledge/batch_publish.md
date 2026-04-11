# StarRocks 存算分离 Batch Publish 机制技术原理分析

## 1\. 概述

### 1.1 背景与动机

在 StarRocks 存算分离（Shared-Data）架构中，数据写入后需要经过 **publish** 阶段才能对查询可见。Publish 的核心操作是将事务产生的 txn log 应用到 tablet metadata 上，生成新版本的 metadata 并持久化到对象存储。

在高并发写入场景下，如果每个事务独立 publish，会导致：

- **RPC 开销大**：每个事务需要向每个相关 BE 节点发送独立的 publish RPC  
- **锁竞争频繁**：`finishTransaction` 需要持有表级写锁，频繁加解锁开销显著  
- **对象存储写放大**：每个事务产生一个独立的 tablet metadata 文件

**Batch publish** 将同一张表的多个连续事务合并为一次发布操作，从根本上减少 RPC 次数、锁竞争和元数据文件数量。

### 1.2 配置参数

| 参数 | 默认值 | 可动态修改 | 说明 |
| :---- | :---- | :---: | :---- |
| `lake_enable_batch_publish_version` | `true` | 是 | Batch publish 总开关 |
| `lake_batch_publish_min_version_num` | `1` | 是 | 触发 batch 的最小事务数 |
| `lake_batch_publish_max_version_num` | `10` | 是 | 单个 batch 的最大事务数 |

## 2\. 整体架构

### 2.1 端到端流程

```
PublishVersionDaemon.runAfterCatalogReady()            [FE 周期触发]
    │
    │  前置条件: lake_enable_batch_publish_version == true
    │            && RunMode.isSharedDataMode()
    │  注意: 进入 batch 分支后直接 return，本轮不再执行 single publish 分支
    │
    ▼
DatabaseTransactionMgr.getReadyToPublishTxnListBatch() [构建事务批次]
    │  ├─ TransactionGraph.getTxnsWithoutDependency()     找无前置依赖的事务
    │  ├─ TransactionGraph.getTxnsWithTxnDependencyBatch() 沿依赖链收集批次
    │  └─ 版本连续性校验 + 事务类型过滤
    │
    ▼
PublishVersionDaemon.publishVersionForLakeTableBatch() [分发批次]
    │
    ├─ batch.size() == 1 → publishLakeTransactionAsync()          [单事务路径]
    └─ batch.size() > 1  → publishLakeTransactionBatchAsync()     [批次路径]
          │
          ├─ 按 partition 分组 → PartitionPublishVersionData
          ├─ 每个 partition 并行执行 publishPartitionBatch()
          │     ├─ shadow index → Utils.publishLogVersionBatch()
          │     │     → BE RPC: publish_log_version_batch
          │     └─ normal index → Utils.publishVersionBatch()
          │     │                 / Utils.aggregatePublishVersion()
          │     │                 (由 enable_file_bundling 决定)
          │     │                 → BE RPC: publish_version
          │     │
          │     └─ isTxnStateBatchConsistent() 版本一致性检查
          │
          └─ 所有 partition 成功且一致性检查通过
               → finishTransactionBatch()                         [批量提交可见]
               → submitDeleteTxnLogJob()                          [FE 异步删除 txn log]
```

### 2.2 RPC 协议

FE 向 BE 发送两种 RPC，分别对应普通 index 和 shadow index（schema change 期间的临时 index）。以下仅列出与 batch publish 主流程直接相关的核心字段，完整定义参见 `gensrc/proto/lake_service.proto`。

**普通 index — `PublishVersionRequest`（核心字段）**：

```protobuf
message PublishVersionRequest {
    repeated int64 tablet_ids = 1;    // tablet 列表
    optional int64 base_version = 3;  // 基准版本（batch 首个事务的前一版本）
    optional int64 new_version = 4;   // 目标版本（batch 最后一个事务对应的版本）
    repeated TxnInfoPB txn_infos = 7; // batch 内所有事务的信息
    // 省略: txn_ids(deprecated), commit_time, timeout_ms,
    //       enable_aggregate_publish, resharding_tablet_infos 等
}
```

Batch 模式的关键体现：`txn_infos` 包含 N 个事务，`new_version = base_version + N`。

**Shadow index — `PublishLogVersionBatchRequest`（核心字段）**：

```protobuf
message PublishLogVersionBatchRequest {
    repeated int64 tablet_ids = 1;    // shadow index 的 tablet 列表
    repeated int64 versions = 3;      // 每个事务对应的 log version
    repeated TxnInfoPB txn_infos = 4; // 事务信息（与 versions 一一对应）
    // 省略: txn_ids(deprecated)
}
```

## 3\. FE 侧：事务批次构建

### 3.1 TransactionGraph — 依赖图模型

`TransactionGraph` 是事务批次选取的核心数据结构，维护事务间的写入依赖关系。

**数据模型**：

```java
class Node {
    long txnId;
    List<Long> writeTableIds;    // 该事务写入的表
    Set<Node> ins;               // 前置依赖（写同一张表的更早事务）
    Set<Node> outs;              // 后置依赖
}

// 全局状态
Map<Long, Node> nodes;              // txnId → Node
Set<Node> nodesWithoutIns;          // 无前置依赖的事务集合
Map<Long, Node> lastTableWriter;    // tableId → 最后写入该表的事务
```

**依赖建立规则**：当事务 B 和此前的事务 A 写同一张表时，B 依赖 A（`A → B`）。通过 `lastTableWriter` 追踪每张表最后的写入者来建立边。

### 3.2 批次选取算法

`getTxnsWithTxnDependencyBatch(minBatchSize, maxBatchSize, txnId)` 从一个无依赖事务出发，沿依赖链向后收集：

```
1. 从 txnId 对应的 Node 出发
2. 如果该事务写多张表（writeTableIds.size() > 1）→ 单独返回，不与其他事务 batch
3. 沿 outs 边向后遍历，只要后续事务也是单表写入就继续收集
4. 最多收集 maxBatchSize 个事务
5. 收集数量 < minBatchSize 时返回空（不够凑批）
```

### 3.3 事务类型过滤

图遍历得到候选事务后，`DatabaseTransactionMgr.getReadyToPublishTxnListBatch()` 还会进行进一步过滤。遇到以下情况时**截断 batch**：

| 排除条件 | 原因 |
| :---- | :---- |
| `tableInfo == null` | 表已被删除 |
| `sourceType == REPLICATION` | 跨集群复制的版本可能不连续 |
| `sourceType == DELETE` | 每个 delete predicate 需要独立版本保证 merge 顺序 |
| `prevVersion + 1 != currVersion` | 版本不连续（schema change 会占用版本号） |

### 3.4 可进入 Batch 的操作类型

需要注意：FE 侧的选批依据主要是**事务依赖关系、单表写入、事务来源类型（sourceType）、版本连续性**，而非直接按 op type 过滤。下表综合了 FE 选批规则和 BE apply 路径的实际约束：

| Op Type | 能否进入 Batch | 约束来源 | 说明 |
| :---- | :---: | :---: | :---- |
| **OpWrite** | 能 | — | 最常见的 batch 成员（stream load、insert、routine load、broker load、MV refresh 等） |
| **OpCompaction** | 能 | — | Compaction 事务不在 FE 排除列表中 |
| **OpParallelCompaction** | 能 | — | 同 OpCompaction |
| **OpSchemaChange** | 不能 | BE 路径约束 | Schema change 发生在新 tablet 的 `base_version=1` 时，BE 在 vtxn\_log 补放分支中断言 `base_version == 1 && txns.size() == 1`（仅该特定分支） |
| **OpAlterMetadata** | 不能 | BE 路径约束（PK 表） | PK 表 applier 中断言 `base_version + 1 == new_version`；非 PK 表 applier 无此断言，但 alter metadata 事务通常为单版本操作 |
| **OpReplication** | 不能 | FE 显式排除 | `sourceType == REPLICATION` 被 FE 截断 |

注：OpWrite 中携带 `delete_predicate` 的（即 `sourceType == DELETE`）也被 FE 显式排除。

一个 batch 内的典型组合为 OpWrite 和 OpCompaction/OpParallelCompaction 的混合。

### 3.5 并发控制

`PublishVersionDaemon` 维护两个集合实现批次路径与非批次路径的互斥：

- **`publishingTransactionIds`**：追踪正在被非批次路径发布的事务 ID  
- **`publishingLakeTransactionsBatchTableId`**：追踪正在被批次路径发布的表 ID

两个集合互相检查，保证同一张表/同一个事务不会被两条路径并发处理，应对 `lake_enable_batch_publish_version` 配置动态切换时的竞态。

### 3.6 Partition 级并行与 Finish 流程

`publishLakeTransactionBatchAsync()` 将 batch 内的事务按 partition 分组到 `PartitionPublishVersionData`，每个 partition 独立并行提交到线程池。所有 partition 都成功后，进入 finish 流程：

1. 调用 `finishTransactionBatch()`，在持有表级写锁的状态下：  
   - **一致性检查**：`isTxnStateBatchConsistent()` 验证 batch 内事务的 partition version 仍然连续，且首个事务的 version 等于 `partition.visibleVersion + 1`。如果检查失败（例如并发 schema change 导致版本跳跃），不会将事务置为 VISIBLE，直接返回  
   - 检查通过后，将所有事务状态设为 VISIBLE，持久化事务状态，触发 `afterVisible` 回调  
2. 调用 `submitDeleteTxnLogJob()` 异步下发 txn log 删除任务（best-effort）

**Publish RPC 成功不等于 FE 可见**——中间还有一致性检查这道关卡。

## 4\. BE 侧：Publish 执行机制

### 4.1 RPC 处理与任务分发

BE 收到 `publish_version` RPC 后：

```
LakeServiceImpl::publish_version():
  1. 参数校验（base_version, new_version, txn_infos）
  2. 为每个 tablet 创建一个发布任务
  3. 提交到 publish_version_thread_pool（ConcurrencyLimitedToken 限流）
  4. CountDownLatch 等待所有 tablet 完成
  5. 聚合 response（failed_tablets, compaction_scores）
```

每个 tablet 的发布任务调用核心函数：

```c
lake::publish_version(tablet_mgr, tablet_info, base_version, new_version, txns, skip_write);
```

### 4.2 核心流程 — `publish_version()`

```
publish_version(tablet_mgr, tablet_info, base_version, new_version, txns):

  ① 互斥检查
     tablet_txns.insert(tablet_id) — 同一 tablet 同时只允许一个 publish
     失败返回 ResourceBusy，FE 下次循环重试

  ② 不变式校验（仅 txns.size() > 1 时）
     CHECK_EQ(new_version, base_version + txns.size())

  ③ 缓存检查
     如果 new_version 的 metadata 已在 metacache → 直接返回（处理 FE 重试）

  ④ 基准版本优化
     cal_new_base_version() — 利用 PK index 缓存的版本跳过已 apply 的事务

  ⑤ 加载基准 metadata
     get_tablet_metadata(tablet_id, base_version)

  ⑥ 创建 TxnLogApplier + metadata 内存副本
     根据表类型选择：
       PRIMARY_KEYS → PrimaryKeyTxnLogApplier
       其他         → NonPrimaryKeyTxnLogApplier

  ⑦ 顺序 apply 每个事务的 txn log
     for i in [txn_offset, txns.size()):
       txn_log = load_txn_log(tablet_ids, txns[i])
       log_applier->apply(*txn_log)

  ⑧ 持久化
     log_applier->finish()

  ⑨ 异步清理
     delete_files_async(files_to_delete)
```

### 4.3 Tablet 间并发模型

```
              publish_version RPC（1 次调用）
                       │
                       ▼
        ┌──── publish_version_thread_pool ────┐
        │                                      │
┌───────┴───────┐  ┌───────┴───────┐  ┌──────┴───────┐
│   tablet_1    │  │   tablet_2    │  │   tablet_3   │
│               │  │               │  │              │
│ load base(v3) │  │ load base(v3) │  │ load base(v3)│
│ apply txn4    │  │ apply txn4    │  │ apply txn4   │
│ apply txn5    │  │ apply txn5    │  │ apply txn5   │
│ apply txn6    │  │ apply txn6    │  │ apply txn6   │
│ write meta(v6)│  │ write meta(v6)│  │ write meta(v6)│
└───────────────┘  └───────────────┘  └──────────────┘
        │                   │                  │
        └──────── latch.wait() ───────────────┘
                       │
                       ▼
                 返回 response
```

- **Tablet 间**：线程池并行  
- **Tablet 内**：txn log 严格按事务顺序串行 apply  
- **跨请求**：`tablet_txns` 全局并发集合保证同一 tablet 不被并发 publish

### 4.4 File Bundling 与 Metadata 持久化策略

前面 4.1-4.3 节描述的是默认的持久化路径：每个 tablet 各自将 metadata 写入对象存储上的独立文件。当 **file bundling**（`enable_file_bundling` / `table.isFileBundling()`）开启时，metadata 的持久化方式会发生根本性变化，但 txn log 的 apply 逻辑不受影响。

#### 默认路径 vs Aggregate Publish 路径

FE 在 `publishPartitionBatch()` 中根据 file bundling 配置选择持久化路径：

```java
boolean useAggregatePublish = table.isFileBundling();

if (!useAggregatePublish) {
    Utils.publishVersionBatch(...);       // 默认：FE 直接向各 BE 发送 publish_version
} else {
    Utils.aggregatePublishVersion(...);   // Aggregate：FE 选一个 aggregator 统一收集
}
```

两条路径的差异体现在 RPC 拓扑和 metadata 落盘方式上：

**默认路径**：FE 按 tablet 所在节点分组，向每个 BE 直接发送 `publish_version` RPC。每个 BE 独立完成 txn log apply 后，各自将 tablet metadata 写入对象存储。一个 partition 有 N 个 tablet，就产生 N 次对象存储写操作。

```
FE ──publish_version──→ BE-1 (tablet 1,2,3)  → 写 3 个 metadata 文件
   ──publish_version──→ BE-2 (tablet 4,5,6)  → 写 3 个 metadata 文件
```

**Aggregate 路径**：FE 选择一个 aggregator 节点，将所有 BE 的 publish 请求打包成一个 `aggregate_publish_version` RPC 发给它。Aggregator 再将请求拆分转发给各 BE。关键区别在于：转发的请求中 `enable_aggregate_publish=true`，这使得 BE 内部将 `skip_write_tablet_metadata` 置为 true——每个 tablet 完成 txn log apply 后，**不写对象存储，只将 metadata 缓存在内存中**，通过 response 返回给 aggregator。Aggregator 收集齐所有 tablet metadata 后，调用 `put_bundle_tablet_metadata()` 将同一 partition 的所有 tablet metadata **打包写入一个 bundle 文件**。

```
FE ──aggregate_publish_version──→ Aggregator
      Aggregator ──publish_version(skip_write=true)──→ BE-1 → 不写盘，返回 metadata
                 ──publish_version(skip_write=true)──→ BE-2 → 不写盘，返回 metadata
      Aggregator 收集所有 metadata → put_bundle_tablet_metadata() → 写 1 个 bundle 文件
```

#### `skip_write_tablet_metadata` 对 BE 内部的影响

这个标志贯穿了 `publish_version` 的整个执行路径，但它**只影响最终的持久化动作，不影响 apply 逻辑**：

- **TxnLog apply**（步骤 ①-⑦）：完全不受影响，txn log 的加载、apply、primary index 更新、delvec 生成等逻辑与默认路径完全一致  
- **Delvec 写盘**：`MetaFileBuilder::_finalize_delvec()` 不受此标志影响，delvec 文件照常写入对象存储  
- **Metadata 持久化**（步骤 ⑧）：`finish()` 中根据标志分支——false 时调用 `put_tablet_metadata()` 写对象存储，true 时调用 `cache_tablet_metadata()` 仅缓存到内存  
- **Txn log 删除**（BE 阶段）：只在 `!skip_write_tablet_metadata` 时才将 txn log 加入异步删除列表，aggregate 路径下 BE 不删除 txn log

#### Bundle 文件格式

`put_bundle_tablet_metadata()` 将同一 partition 的所有 tablet metadata 写入一个文件。由于同一 partition 的 tablet 通常共享相同的 schema，bundle 格式将 schema 提取出来去重存储，避免每个 tablet 各存一份完整的 schema 副本：

```
Bundle 文件布局:
┌───────────────────────────────────┐
│ tablet_meta_1 (schema 已剥离)     │  page 1
├───────────────────────────────────┤
│ tablet_meta_2 (schema 已剥离)     │  page 2
├───────────────────────────────────┤
│ ...                               │
├───────────────────────────────────┤
│ BundleTabletMetadataPB (header)   │  ← 去重 schema + page 索引
├───────────────────────────────────┤
│ header_size (8 bytes fixed LE)    │  footer
└───────────────────────────────────┘

BundleTabletMetadataPB:
  tablet_to_schema:  {tablet_id → schema_id}
  schemas:           {schema_id → TabletSchemaPB}  // 去重
  tablet_meta_pages: {tablet_id → {offset, size}}  // page 索引
```

每个 tablet 的 metadata 在序列化前会 `clear_schema()` 和 `clear_historical_schemas()`，schema 信息统一由 header 中的 `schemas` 字段持有。读取时通过 `tablet_to_schema` 映射还原。

#### 对 Batch Publish 整体效果的影响

File bundling 与 batch publish 是两个独立的优化维度，它们叠加时的效果：

- **Batch publish** 减少的是 FE 到 BE 的 **RPC 轮次**（N 个事务合并为 1 轮）和 FE 侧的**锁竞争**（1 次 finish 替代 N 次）  
- **File bundling** 减少的是 **对象存储写入次数**（N 个 tablet 的 metadata 合并为 1 次写入）和 **schema 重复存储**

两者叠加的最终效果：假设一个 partition 有 M 个 tablet，batch 包含 N 个事务：

| 维度 | 无优化 | 仅 Batch | 仅 File Bundling | 两者叠加 |
| :---- | :---- | :---- | :---- | :---- |
| FE→BE RPC 轮次 | N | 1 | N | 1 |
| 对象存储 metadata 写入次数 | N × M | M | N × 1 | **1** |
| FE 表级写锁次数 | N | 1 | N | 1 |

## 5\. TxnLog Apply 机制

### 5.1 TxnLog 结构

每个 `TxnLogPB` 包含以下互斥操作之一：

| 操作 | 含义 | 对 metadata 的影响 |
| :---- | :---- | :---- |
| OpWrite | 数据写入 | 追加新 rowset |
| OpCompaction | Compaction | 多个 rowset 替换为一个 |
| OpParallelCompaction | 并行 Compaction | 多子任务各自替换 rowset |
| OpSchemaChange | Schema Change | 重建 rowset 列表 |
| OpAlterMetadata | 元数据变更 | 修改 index/schema 属性 |
| OpReplication | 跨集群复制 | 替换或追加 rowset |

### 5.2 非主键表 Apply

#### OpWrite — 追加 Rowset

```c
Status apply_write_log(const TxnLogPB_OpWrite& op_write, int64_t txn_id) {
    update_metadata_schema(op_write, txn_id, _metadata, _tablet.tablet_mgr());
    auto rowset = _metadata->add_rowsets();          // 在 rowsets 列表尾部追加
    rowset->CopyFrom(op_write.rowset());             // 复制 segment 列表、行数等
    rowset->set_id(_metadata->next_rowset_id());     // 分配全局唯一 rowset id
    rowset->set_version(_new_version);               // 标记为 batch 目标版本
    _metadata->set_next_rowset_id(next + step);      // 推进 next_rowset_id
}
```

Batch 中每个 OpWrite 事务各自追加一个独立的 rowset，**不会合并**。

#### OpCompaction — 替换 Rowset

```
输入: input_rowsets = [R1, R2, R3], output_rowset = R_out

操作:
  1. 在 metadata.rowsets 中定位 [R1, R2, R3]（必须相邻）
  2. 将 [R1, R2, R3] 移入 metadata.compaction_inputs（标记待 GC）
  3. 用 R_out 替换 R1 的位置
  4. 删除 R2、R3 的位置
  5. 更新 cumulative_point

结果: rowsets = [..., R_out, ...]
```

#### OpParallelCompaction — 多子任务替换

`OpParallelCompaction` 包含 N 个 `OpCompaction` 子任务，每个子任务独立执行上述替换逻辑，最终生成 N 个 output rowset。

### 5.3 主键表 Apply

主键表比非主键表多两个关键组件：**Primary Index**（主键索引）和 **Delete Vector**（删除向量）。

#### OpWrite 的完整流程

```
publish_primary_key_tablet(op_write, txn_id, metadata, ...):

  1. 加载 rowset update state（upsert/delete 列表）

  2. 逐 segment 处理:
     for each segment in op_write.rowset:
       ├─ load_segment()        加载 segment 数据，解析 primary key
       ├─ rewrite_segment()     partial update 时重写 segment
       └─ index.upsert()        更新 primary index:
            ├─ 新 key → 插入 index
            └─ 已存在 key → 在旧 segment 的 delvec 标记删除

  3. 处理 del files（显式删除）:
     for each del_file:
       index.erase(key) → 在对应 segment 的 delvec 标记删除

  4. 生成 delvec（详见第 6 节）

  5. builder.apply_opwrite()    将新 rowset 添加到 metadata
     builder.append_delvec()    将 delvec 写入 builder 缓冲
```

#### Batch 模式下 Primary Index 的共享

整个 batch 共享同一个 `PrimaryKeyTxnLogApplier` 实例，其中：

- **Primary Index**：在首次 `apply_write_log` 或 `apply_compaction_log` 时懒加载，后续事务复用同一实例  
- **MetaFileBuilder**：整个 batch 共享，累积所有 delvec 变更

这保证了 batch 内事务的顺序语义——后面事务的 upsert 基于前面事务更新后的 index 做冲突判断。

### 5.4 finish() — 持久化

**非主键表**：

```c
Status finish() {
    _metadata->set_version(_new_version);
    if (_skip_write_tablet_metadata) {
        return tablet_mgr->cache_tablet_metadata(_metadata);  // aggregate 模式：仅缓存
    }
    return _tablet.put_metadata(_metadata);  // 默认：写入对象存储
}
```

**主键表**：

```c
Status finish() {
    _index_entry->value().commit(_metadata, &_builder);                // 提交 PK index 变更
    _builder.finalize(_max_txn_id, _skip_write_tablet_metadata);       // 写 delvec 文件 + metadata
    // finalize 内部：delvec 文件始终写盘；metadata 根据标志决定写盘还是仅缓存
}
```

两种表类型在 `finish()` 中都只执行**一次**持久化操作，将整个 batch 的所有变更一次性固化。当 file bundling 开启时（`_skip_write_tablet_metadata = true`），metadata 不直接写对象存储，而是缓存在内存中，后续由 aggregator 统一打包写入 bundle 文件（参见 4.4 节）。

## 6\. Delete Vector 处理（主键表）

### 6.1 设计原理

主键表中数据是 append-only 的。当新数据与旧数据 primary key 冲突时，通过在旧行所在 segment 的 delete vector 中标记 rowid 来实现逻辑删除：

```
Segment S0 (旧): [row0, row1, row2]
Segment S1 (新): [row0', row2']     ← key 与 row0, row2 相同
→ S0 的 delvec = {0, 2}             ← 标记 row0, row2 逻辑删除
```

### 6.2 核心数据结构

```c
class MetaFileBuilder {
    vector<uint8_t> _buf;                          // delvec 二进制缓冲区
    map<uint32_t, DelvecPagePB> _delvecs;          // rssid → {offset, size} in _buf
    map<uint32_t, DelVectorPtr> _segmentid_to_delvec;
};
```

整个 batch **共享一个 MetaFileBuilder 实例**，所有事务产生的 delvec 变更都累积在同一个 `_buf` 中。

### 6.3 单个 OpWrite 的 Delvec 生成

```
步骤 1: 初始化 new_deletes map
  为新 rowset 的每个 segment 预建空条目

步骤 2: Primary Index upsert
  新 key → 无删除
  已存在于 old_rssid → new_deletes[old_rssid].push_back(old_rowid)

步骤 3: 处理 del files
  index.erase(key) → new_deletes[rssid].push_back(rowid)

步骤 4: 生成最终 delvec
  for each (rssid, deleted_rowids) in new_deletes:
    if rssid 属于新 rowset:
      delvec = new DelVector(deleted_rowids)        // 新 segment 无旧 delvec
    else:
      old_delvec = get_del_vec(rssid, base_version, builder)
      new_delvec = old_delvec.merge(deleted_rowids) // 与旧 delvec 合并

步骤 5: 写入 builder 缓冲
  builder->append_delvec(delvec, rssid)
```

### 6.4 Batch 内事务间的 Delvec 串联

关键问题：**后一个事务如何看到前一个事务刚产生的 delvec？**

答案在 `get_del_vec()` 的查找顺序中：

```c
Status get_del_vec(tsid, version, builder, fill_cache, pdelvec) {
    if (builder != nullptr) {
        // 1. 优先从 builder 缓冲中查找（前面事务刚写入的 delvec）
        auto found = builder->find_delvec(tsid, pdelvec);
        if (*found) return OK;
    }
    // 2. 未命中则从持久化的 delvec 文件读取
    return get_del_vec_in_meta(tsid, version, fill_cache, pdelvec);
}
```

`builder->find_delvec()` 直接从内存缓冲 `_buf` 中反序列化 delvec。由于整个 batch 共享同一个 `MetaFileBuilder`，txn\_i 通过 `append_delvec` 写入 `_buf` 的 delvec，在 txn\_i+1 执行 `get_del_vec` 时能立即读到。

当同一 rssid 被多次 `append_delvec` 时，`_delvecs[rssid]` 被覆盖指向最新的 offset/size，旧数据留在 `_buf` 中但不再被引用。

### 6.5 具体示例

```
Batch: [txn4, txn5], base_version=3
R0.S0 已有 delvec = {5}

=== apply txn4 ===
  写入 key_B（已存在于 R0.S0 row=10）
  → new_deletes[R0.S0] = {10}
  → get_del_vec(R0.S0) → 从 delvec 文件读取旧 delvec {5}
  → merge → delvec = {5, 10}
  → builder.append_delvec({5,10}, R0.S0_rssid)

=== apply txn5 ===
  写入 key_C（已存在于 R0.S0 row=20）
  → new_deletes[R0.S0] = {20}
  → get_del_vec(R0.S0) → 先查 builder._delvecs → 命中！得到 {5, 10}
  → merge → delvec = {5, 10, 20}
  → builder.append_delvec({5,10,20}, R0.S0_rssid)  // 覆盖之前的条目

=== finish ===
  _finalize_delvec():
    写入一个 delvec 文件包含最终的 {5, 10, 20}
    metadata.delvec_meta.delvecs[R0.S0] = {version=6, ...}
```

### 6.6 Finalize 过程

`MetaFileBuilder::_finalize_delvec(version, txn_id)` 在 `finish()` 阶段一次性执行：

1. **更新已有条目**：遍历 `metadata.delvec_meta.delvecs`，如果 rssid 在 `_delvecs` 中有更新，替换其 offset/size/version  
2. **插入新条目**：`_delvecs` 中剩余的 rssid（新 segment）插入到 metadata  
3. **写 delvec 文件**：将 `_buf` 整体写入 `delvec_{txn_id}.dat`  
4. **清理旧文件**：不再被任何 rssid 引用的旧 delvec 文件记录移入 `orphan_files`

## 7\. Batch Publish 与单独 Publish 的差异

### 7.1 对象存储文件

以 3 个事务（txn4、txn5、txn6）、base\_version=3 为例：

|  | 单独 Publish | Batch Publish |
| :---- | :---- | :---- |
| Tablet metadata 文件 | v4, v5, v6（3 个） | v6（1 个） |
| Delvec 文件（PK 表） | 最多 3 个 | 1 个 |
| 中间版本是否存在 | v4、v5 存在 | v4、v5 不存在 |

### 7.2 Tablet Metadata 版本连续性

单独 publish 模式下，tablet metadata 的版本是连续递增的（v3 → v4 → v5 → v6），每个中间版本都有对应的 metadata 文件。

Batch publish 模式下，**metadata 版本不再连续**——对象存储上只有 v3 和 v6 的 metadata 文件，v4、v5 不存在。版本序列出现跳跃。

此外，`TabletMetadataPB` 的 schema 中**没有 `base_version` 字段**来记录它是基于哪个版本生成的：

```protobuf
message TabletMetadataPB {
    optional int64 id = 1;
    optional int64 version = 2;            // 自身版本号
    optional TabletSchemaPB schema = 3;
    repeated RowsetMetadataPB rowsets = 4;
    // ... 无 base_version / prev_version 字段
    optional int64 prev_garbage_version = 9; // 仅用于 GC，语义完全不同
}
```

这意味着：从一个 metadata 文件本身，**无法得知它是从哪个版本 apply 而来**，也无法区分它是由单独 publish（v5 → v6）还是 batch publish（v3 → v6）产生的。Base version 信息只能通过对象存储上实际存在哪些版本的 metadata 文件来间接推断。

### 7.3 Rowset 元数据

| 属性 | 单独 Publish | Batch Publish |
| :---- | :---- | :---- |
| Rowset 数量 | 相同 | 相同 |
| Rowset id | 相同 | 相同 |
| Rowset version | 各不相同（4, 5, 6） | 统一为 new\_version（6, 6, 6） |
| Segments 内容 | 相同 | 相同 |

Rowset `version` 字段的语义是 "该 rowset 在哪个 metadata version 中首次可见"。Batch publish 是原子的，metadata 从 v3 直接跳到 v6，因此所有 rowset 的 version 统一为 6。

#### 无法区分 Rowset 来源

`RowsetMetadataPB` 中没有字段标识 rowset 是由数据导入（OpWrite）还是 compaction（OpCompaction）产生的：

```protobuf
message RowsetMetadataPB {
    optional uint32 id = 1;
    optional bool overlapped = 2;
    repeated string segments = 3;
    optional int64 num_rows = 4;
    optional int64 data_size = 5;
    // ... 无 source_type / origin 字段
    optional uint32 max_compact_input_rowset_id = 9;  // 仅 PK 表 compaction 设置
}
```

唯一的间接线索是 `max_compact_input_rowset_id`（字段 9）：仅在**主键表的 compaction 路径**（`MetaFileBuilder::apply_opcompaction`）中赋值，记录 compaction 输入中最大的 rowset id，用于 PK index recover 时确定 rowset 逻辑顺序。

但这个字段的局限性很大：

| 场景 | `has_max_compact_input_rowset_id` | 能否判断来源 |
| :---- | :---: | :---: |
| PK 表 compaction 产生 | true | 能（compaction） |
| PK 表 load 产生 | false | 能（load） |
| 非 PK 表 compaction 产生 | false | **不能** |
| 非 PK 表 load 产生 | false | **不能** |

非主键表的 `apply_compaction_log_single_output` 直接操作 `metadata.rowsets`，不经过 `MetaFileBuilder`，不设置此字段。因此**非主键表中无法区分 rowset 是 load 还是 compaction 产生的**。

在 batch publish 场景下这一问题更加突出：batch 内 OpWrite 和 OpCompaction 产生的 rowset 全部标记为相同的 version，进一步丢失了来源信息。

### 7.5 Delete Vector 元数据（主键表）

| 属性 | 单独 Publish | Batch Publish |
| :---- | :---- | :---- |
| Delvec 最终内容 | 相同 | 相同 |
| Delvec version | 各 rssid 可能为 4/5/6 | 统一为 6 |
| version\_to\_file 条目 | 可能保留多个版本的文件引用 | 只有版本 6 的文件引用 |
| Delvec 文件数 | 每次 publish 一个 | 整个 batch 一个 |

### 7.6 Txn Log 清理机制

Txn log 的清理分为 **BE 执行阶段**和 **FE finish 阶段**两个环节，单独 publish 与 batch publish 的行为恰好互补：

| 阶段 | 单独 Publish | Batch Publish |
| :---- | :---- | :---- |
| **BE 执行阶段** | `txns.size() == 1` 时将 txn log 加入异步删除列表 | `txns.size() != 1` 时**不删除**，避免 batch/single 切换时丢失重试依据 |
| **FE finish 后** | `finishTransaction()` 后**不调用** `submitDeleteTxnLogJob` | `finishTransactionBatch()` 后**调用** `submitDeleteTxnLogJob` 异步下发删除任务 |
| **兜底** | vacuum 清理 | vacuum 清理 |

注：FE 的 `submitDeleteTxnLogJob` 是 best-effort 的异步操作（异常只打 warn log），不保证一定删除成功，vacuum 始终作为兜底机制。

BE 侧 batch 不删的原因（代码注释 `transactions.cpp:276-286`）：

假设 batch 包含 txn4, txn5。若 BE 成功 publish 后立即删除 txn log，但 FE 未收到 response，此时切换到 single 模式，FE 重新发布 txn4，但 txn log 已被删除，无法重新 apply。

### 7.7 其他差异

| 维度 | 单独 Publish | Batch Publish |
| :---- | :---- | :---- |
| `commit_time` | 最终值相同 | 最终值相同（取 batch 最后一个事务） |
| `gtid` | 最终值相同 | 最终值相同 |
| RPC 次数 | N 次（每事务一次） | 1 次（每 BE 节点） |
| 表级写锁 | N 次加解锁 | 1 次加解锁 |

### 7.8 正确性等价性

Batch publish 与单独 publish 的**最终数据逻辑完全一致**：

- Rowset 列表内容相同（数量、segment 文件、行数）  
- Delete vector 最终标记的删除行相同  
- Primary index 最终状态相同

差异仅在于版本号标记方式和元数据文件数量，不影响数据正确性和查询结果。

## 8\. 容错与幂等设计

### 8.1 FE 重试

| 场景 | 处理方式 |
| :---- | :---- |
| BE publish 成功但 FE 未收到 response | FE 下一轮重新发送 publish 请求 |
| BE metacache 中已有 new\_version metadata | 直接返回缓存结果，避免重复 apply |
| new\_version metadata 已在对象存储 | 读取并返回，同样避免重复 apply |

### 8.2 Batch ↔ Single 模式切换

| 场景 | 处理方式 |
| :---- | :---- |
| Single → Batch | 首个 txn log 可能已被 single 模式的 BE 阶段删除；BE 检查 base\_version+1 的 metadata 是否存在，存在则用作新 base 继续 apply |
| Batch → Single | BE 在多事务 publish 阶段不立即删除 txn log，保证切换到 single 模式后可以重新 apply；后续清理由 FE 异步删除任务或 vacuum 完成 |
| FE 两条路径并发保护 | `publishingTransactionIds` 与 `publishingLakeTransactionsBatchTableId` 互相检查 |

### 8.3 BE 侧容错

| 场景 | 处理方式 |
| :---- | :---- |
| 同一 tablet 并发 publish | `tablet_txns` 全局集合互斥，返回 ResourceBusy |
| PK index 异常 | `check_and_recover` 自动重建 primary index |
| Partial success（部分 partition 失败） | 返回 failed\_tablets，FE 整体重试 |
| Delvec 不一致 | 检测 `old + add != new`，触发 PK recover 流程 |

## 9\. 关键设计总结

1. **Metadata 纯内存操作**：整个 apply 过程中，`TabletMetadataPB` 只在内存中修改。所有变更在 `finish()` 时一次性持久化，batch 中任何事务 apply 失败不会产生中间状态。  
     
2. **PK Index 有状态共享**：整个 batch 共享同一个 primary index 实例。每个 OpWrite 的 upsert 在上一个 OpWrite 的基础上累积，保证 batch 内事务的顺序覆盖语义。  
     
3. **Delvec Builder 缓冲串联**：多个事务的 delvec 通过 `MetaFileBuilder._buf` 在内存中串联。`get_del_vec()` 优先从 builder 查找，实现 batch 内事务间 delvec 的可见性。  
     
4. **Txn Log 两阶段清理**：BE 在多事务 publish 阶段不立即删除 txn log，以保证 batch↔single 模式切换时的重试安全性；事务在 FE 端成功 finish 后，由 FE 异步下发删除任务（best-effort），vacuum 机制作为兜底。  
     
5. **Rowset Version 统一**：Batch 内所有 rowset 的 version 统一为 `new_version`，因为中间版本的 metadata 不存在，version 表示 "首次可见的版本" 而非 "产生该 rowset 的事务"。  
     
6. **Metadata 版本不连续且无 Base Version 记录**：Batch publish 产生的 metadata 版本会跳跃（如 v3 → v6），中间版本的 metadata 文件不存在于对象存储上。同时 `TabletMetadataPB` 不记录 base version，无法从 metadata 本身判断它是基于哪个版本 apply 而来，也无法区分是单独 publish 还是 batch publish 产生的。  
     
7. **Rowset 来源信息丢失**：`RowsetMetadataPB` 没有通用字段标识 rowset 是由数据导入还是 compaction 产生。唯一的间接线索 `max_compact_input_rowset_id` 仅覆盖主键表 compaction 场景，非主键表中完全无法区分。Batch publish 进一步加剧了这一问题——batch 内不同操作类型（OpWrite、OpCompaction）产生的 rowset 被标记为相同的 version，来源信息被进一步抹平。

## 10\. 实现边界与例外

1. 本文重点描述 shared-data 模式下的 batch publish 主流程，不覆盖 shared-nothing publish 机制。  
2. 文中对 RPC proto 仅摘录核心字段，完整字段定义以 `lake_service.proto` 为准。  
3. 文中关于 "每个 OpWrite 追加一个独立 rowset" 的描述，针对的是 batch 中不同事务的常规 apply 路径。对于 `load_ids_size() > 0` 的 multi-statement transaction，BE 可能走 `apply(TxnLogVector)` 路径，将同一事务内的多个 op\_write 合并为一个 rowset，rowset 形态会有所不同。  
4. 文中未展开 tablet resharding 等与 batch publish 正交的分支细节。

## 附录：参考实现位置

| 模块 | 文件路径 |
| :---- | :---- |
| FE Publish Daemon | `fe/fe-core/src/main/java/com/starrocks/transaction/PublishVersionDaemon.java` |
| FE 事务管理 | `fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java` |
| FE 事务依赖图 | `fe/fe-core/src/main/java/com/starrocks/transaction/TransactionGraph.java` |
| FE 批次数据容器 | `fe/fe-core/src/main/java/com/starrocks/transaction/TransactionStateBatch.java` |
| FE Partition 数据聚合 | `fe/fe-core/src/main/java/com/starrocks/lake/PartitionPublishVersionData.java` |
| FE Lake 工具类 | `fe/fe-core/src/main/java/com/starrocks/lake/Utils.java` |
| BE 事务 publish 核心 | `be/src/storage/lake/transactions.cpp` |
| BE TxnLog Applier | `be/src/storage/lake/txn_log_applier.cpp` |
| BE MetaFileBuilder | `be/src/storage/lake/meta_file.cpp` |
| BE PK UpdateManager | `be/src/storage/lake/update_manager.cpp` |
| BE Lake RPC 服务 | `be/src/service/service_be/lake_service.cpp` |
| BE TabletManager（bundle 写入） | `be/src/storage/lake/tablet_manager.cpp` |
| Proto 定义（RPC） | `gensrc/proto/lake_service.proto` |
| Proto 定义（类型） | `gensrc/proto/lake_types.proto` |

