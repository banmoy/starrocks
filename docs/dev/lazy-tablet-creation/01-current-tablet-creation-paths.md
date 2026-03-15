# 当前所有 Tablet 创建路径

## 概述

存算分离模式下，tablet 创建分 3 步：
1. **FE**: 通过 `StarOSAgent` 调用 StarManager 分配 shard ID（shard ID = tablet ID）
2. **FE**: 发送 `CreateReplicaTask` agent task 到 CN
3. **CN**: 构建 `TabletMetadataPB` 并写入对象存储

## 核心类

| 类 | 位置 | 职责 |
|----|------|------|
| `StarOSAgent` | `fe/fe-core/.../lake/StarOSAgent.java` | 封装 StarManager shard 创建 API |
| `LocalMetastore` | `fe/fe-core/.../server/LocalMetastore.java` | 中央 partition/tablet 创建逻辑 |
| `LakeTablet` | `fe/fe-core/.../lake/LakeTablet.java` | 存算分离 tablet（ID = shard ID）|
| `TabletTaskExecutor` | `fe/fe-core/.../task/TabletTaskExecutor.java` | 发送 `CreateReplicaTask` 到 CN |
| `LakeTableHelper` | `fe/fe-core/.../lake/LakeTableHelper.java` | 路由 lake 表的 alter/rollup |
| `lake::TabletManager` | `be/src/storage/lake/tablet_manager.cpp` | 在对象存储上创建 tablet metadata |

## StarOSAgent Shard 创建 API

| 方法 | 用途 | 生产代码调用方 |
|------|------|--------------|
| `createShardGroup(dbId, tableId, partitionId, indexId)` | 创建 shard group | `LocalMetastore.createPartition()`, `LakeTableRollupBuilder.build()` |
| `createShards(numShards, pathInfo, cacheInfo, groupId, matchShardIds, properties, computeResource)` | 创建 shard（tablet）| `LocalMetastore.createLakeTablets()`, `LakeTableRollupBuilder.build()`, `LakeTableAlterJobV2Builder.build()` |
| `createShardsForSplit(oldToNewShardIds, ...)` | tablet 分裂 | `SplitTabletJobFactory.createNewShards()` |
| `createShardsForMerge(newToOldShardIds, ...)` | tablet 合并 | `MergeTabletJobFactory.createNewShards()` |
| `createShardGroupForVirtualTablet()` | 存储卷虚拟 tablet | `SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` |
| `createShardWithVirtualTabletId(...)` | 指定 ID 创建 shard | `SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` |

## `new LakeTablet(...)` 构造点（生产代码）

| 文件 | 方法 | 上下文 |
|------|------|--------|
| `LocalMetastore.java:2248` | `createLakeTablets()` | 标准分区/tablet 创建 |
| `LakeTableAlterJobV2Builder.java:102` | `build()` | Schema change shadow tablets |
| `LakeTableRollupBuilder.java:111` | `build()` | Rollup/sync MV shadow tablets |
| `SplitTabletJobFactory.java:288` | `createMaterializedIndex()` | Tablet 分裂新 tablets |
| `MergeTabletJobFactory.java:372` | `createMaterializedIndex()` | Tablet 合并新 tablets |

---

## 完整的 19 条创建路径

### 第一类：通过 `LocalMetastore.createPartition()` → `createLakeTablets()`

共享的底层调用链：

```
LocalMetastore.createPartition()
  → StarOSAgent.createShardGroup()           // 每个 index
  → createLakeTablets()
    → StarOSAgent.createShards(bucketNum)     // 分配 shard ID
    → new LakeTablet(shardId)                 // 构建 FE 元数据
  （然后）
  buildPartitions()
    → TabletTaskExecutor.buildCreateReplicaTasks()
      → CreateReplicaTask (通过 Agent RPC 发到 CN)
        → lake::TabletManager::create_tablet()  // 写 metadata 到对象存储
```

| # | 功能 | FE 入口 |
|---|------|---------|
| 1 | CREATE TABLE（非分区） | `OlapTableFactory.java:818` |
| 2 | CREATE TABLE（分区） | `OlapTableFactory.java:856` |
| 3 | ADD PARTITION（ALTER TABLE） | `AlterJobExecutor.java:736` → `LocalMetastore.java:987` |
| 4 | 自动分区（INSERT/LOAD 触发）| `FrontendServiceImpl.java:2322` |
| 5 | 动态分区 | `DynamicPartitionScheduler.java:452` |
| 6 | TRUNCATE TABLE | `LocalMetastore.java:4776` |
| 7 | INSERT OVERWRITE（新建分区）| `InsertOverwriteJobRunner.java:348` |
| 8 | INSERT OVERWRITE（临时分区）| `InsertOverwriteJobRunner.java:416` |
| 9 | CREATE MATERIALIZED VIEW（非分区）| `LocalMetastore.java:3287` |
| 10 | MV PCT 刷新（新分区） | `MVPCTRefreshListPartitioner.java:523`, `MVPCTRefreshRangePartitioner.java:559` |
| 11 | ADD Sub-Partition（物理子分区）| `LocalMetastore.java:1706` |
| 12 | ALTER TABLE MERGE PARTITION | `MergePartitionJob.java:348` |
| 13 | ALTER TABLE OPTIMIZE | `OptimizeJobV2.java:198`, `OnlineOptimizeJobV2.java:210` |

### 第二类：Schema Change / Rollup（独立 builder）

| # | 功能 | FE 入口 | 特点 |
|---|------|---------|------|
| 14 | Schema Change（加列/删列/改列）| `LakeTableAlterJobV2Builder.java:89` | 使用 `matchShardIds` 做 placement 约束 |
| 15 | ADD ROLLUP / 同步物化视图 | `LakeTableRollupBuilder.java:76-111` | 每个 partition 创建 shardGroup + shards |

### 第三类：Tablet 重分片

| # | 功能 | FE 入口 | BE 路径 |
|---|------|---------|---------|
| 16 | Tablet Split | `SplitTabletJobFactory.java:313` | `publish_resharding_tablet → handle_splitting_tablet` |
| 17 | Tablet Merge | `MergeTabletJobFactory.java:400` | `publish_resharding_tablet → handle_merging_tablet` |

### 第四类：特殊路径

| # | 功能 | FE 入口 | 备注 |
|---|------|---------|------|
| 18 | 存储卷虚拟 Tablet | `SharedDataStorageVolumeMgr.java:724-729` | 用于跨集群复制，非标准数据 tablet |
| 19 | Restore | `LakeRestoreJob.java:302` | 走独立的 `LakeSnapshotLoader` 路径，不发 CreateReplicaTask |

---

## 发送 CreateReplicaTask 的 3 条代码路径

| 路径 | 文件 | 覆盖功能 | 是否走 TabletTaskExecutor |
|------|------|---------|--------------------------|
| `TabletTaskExecutor` | `TabletTaskExecutor.java:88-307` | #1-#13 全部功能 | 是 |
| `LakeTableSchemaChangeJob` | `LakeTableSchemaChangeJob.java:465-502` | #14 Schema Change | 否，独立发送逻辑 |
| `LakeRollupJob` | `LakeRollupJob.java:261-294` | #15 ADD ROLLUP | 否，独立发送逻辑 |

---

## BE 侧 Tablet 创建

| 路径 | 处理器 | 文件 | 使用场景 |
|------|--------|------|---------|
| CREATE agent task | `run_create_tablet_task()` → `lake_tablet_manager()->create_tablet()` | `agent_task.cpp:248-251`, `tablet_manager.cpp:209` | #1-#15 |
| Publish resharding (split) | `publish_resharding_tablet()` → `handle_splitting_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | #16 |
| Publish resharding (merge) | `publish_resharding_tablet()` → `handle_merging_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | #17 |
| Publish resharding (identical) | `publish_resharding_tablet()` → `handle_identical_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | 同 bucket 数的重分片 |
| Lake restore | `LakeSnapshotLoader::restore()` → `put_tablet_metadata()` | `lake_snapshot_loader.cpp:309` | #19 |
| 修复 metadata | `LakeServiceImpl::repair_tablet_metadata()` → `put_tablet_metadata()` | `lake_service.cpp:1917` | Admin 修复（非新建） |

---

## 不会创建 Tablet 的操作

| 操作 | 原因 |
|------|------|
| Clone | 存算分离用 replication 替代 |
| 存储介质迁移 | 不适用于 lake tablet |
| Compaction | 仅更新 metadata/rowset |
| REPLACE PARTITION | 仅交换 partition 元数据 |
| Publish version（正常） | 更新已有 tablet 的版本 |
| Repair tablet metadata | 重写已有 tablet 的 metadata |

---

## tablet_creation_optimization 现状

`lake_enable_tablet_creation_optimization`（默认 `false`）：

- **关闭时**：每个 tablet 一个 `CreateReplicaTask` → 每个 tablet 一个 metadata 文件
- **开启时**：同一 partition/index 下只发 **1 个** task → 写 1 个共享的 initial metadata 文件
  - CN 侧写入路径：`tablet_initial_metadata_location(tablet_id)` = `{metadata_root}/0000000000000000_0000000000000001.meta`
  - 其他 tablet 读取时通过 fallback 逻辑找到这个共享文件（`tablet_manager.cpp:539-548`）
- **无论开关**：都需要至少 `partitions × indexes` 次 CN RPC

## 验证方法论

本文档通过以下 8 个维度的穷举搜索验证完整性：
1. 所有 `StarOSAgent.createShards*()` / `createShardGroup*()` 的生产代码调用方
2. 所有 `new LakeTablet(...)` 构造点
3. 所有 `LocalMetastore.createPartition()` / `createLakeTablets()` / `createPhysicalPartition()` 调用方
4. 所有 `LocalMetastore.addPartitions()` / `addSubPartitions()` / `truncateTable()` 调用方
5. 所有 `createTempPartitionsFromPartitions()` / `PartitionUtils.createAndAddTempPartitionsForTable()` 调用方
6. 所有 `buildNonPartitionOlapTable()` 调用方
7. BE 侧所有 `lake::TabletManager::create_tablet()` 和 `put_tablet_metadata()` 调用方
8. BE 侧所有 `TABLET_TYPE_LAKE` 引用
