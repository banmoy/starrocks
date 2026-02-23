# 存算分离 DROP/TRUNCATE PARTITION 与 StarManager 交互原理

---

## 一、概述

在 StarRocks 存算分离架构下，分区的元数据管理分布在 **FE** 和 **StarManager** 两个系统中：

- **FE** 管理 Partition、PhysicalPartition、MaterializedIndex、Tablet 等逻辑元数据
- **StarManager** 管理 ShardGroup、Shard 等存储层元数据，并掌握数据在对象存储上的物理路径

两套元数据通过 `shardGroupId` 和 `shardId`（= `tabletId`）关联。任何涉及 Partition 生命周期变更的操作（DROP / TRUNCATE），都需要协调两侧的元数据一致性。

本文分析 DROP PARTITION 和 TRUNCATE PARTITION 两种操作与 StarManager 的交互原理，包括同步链路、异步回收机制以及兜底对账策略。

---

## 二、数据模型映射

理解交互原理的前提是理清两侧元数据的对应关系：

```
FE 元数据                           StarManager 元数据
─────────────                       ──────────────────
OlapTable                           (无直接对应)
  └─ Partition                      (无直接对应)
       └─ PhysicalPartition         (无直接对应)
            └─ MaterializedIndex ──── ShardGroup  (1:1)
                 └─ LakeTablet ───── Shard        (1:1)
```

**关键映射关系**：

- 一个 Partition 可包含多个 PhysicalPartition（子分区），每个 PhysicalPartition 包含 base index 和若干 rollup index
- 每个 **MaterializedIndex** 在 StarManager 中对应一个 **ShardGroup**
- 每个 **LakeTablet** 在 StarManager 中对应一个 **Shard**
- 创建 ShardGroup 时会携带 `dbId`、`tableId`、`partitionId`、`indexId` 四个 label，用于后续对账

因此，删除或替换一个 Partition，意味着需要清理该 Partition 下所有 MaterializedIndex 对应的 ShardGroup 及其下属的全部 Shard。

---

## 三、DROP PARTITION

### 3.1 设计原则

DROP PARTITION 采用 **"先断开引用，后异步清理"** 的策略：

1. **同步阶段**只做 FE 侧的逻辑元数据变更，不涉及任何 StarManager RPC，确保用户操作快速返回
2. **异步阶段**通过回收站机制逐步清理物理数据和 StarManager 元数据
3. **兜底机制**通过 StarMgrMetaSyncer 定期对账，处理异步清理失败的残留

### 3.2 同步阶段：FE 元数据变更

用户执行 `ALTER TABLE t DROP PARTITION p` 后，FE 的处理是纯内存 + EditLog 操作：

1. **校验**：表状态须为 NORMAL 或 TABLET_RESHARD；非 FORCE 模式下检查是否有进行中的事务
2. **写 EditLog**：`logDropPartitions` 持久化操作日志
3. **移入回收站**：对于 LakeTable，构造 `RecycleLakeRangePartitionInfo`（或 List/UnPartition 对应的子类），放入 `CatalogRecycleBin`
4. **FE 内存移除**：从 OlapTable 的内部数据结构中移除 Partition

此时 StarManager 对这次 DROP **完全不感知**——旧的 ShardGroup 和 Shard 元数据仍然存在。

> **为什么不同步删 StarManager 元数据？** 同步调用 StarManager RPC 会增加 DROP PARTITION 的延迟和失败风险。StarManager 是独立服务，网络调用可能超时或失败。将清理推迟到异步阶段可以保证用户操作的可靠性和响应速度。

### 3.3 异步阶段：回收站清理

`CatalogRecycleBin` 有一个后台守护线程，定时扫描过期的 Partition（过期时间由 `catalog_trash_expire_second` 控制）。对于 LakeTable 的 Partition，调用 `RecycleLakeRangePartitionInfo.delete()`，按顺序执行三步：

**Step 1 — 删除物理文件**

通过 `LakeTableHelper.removePartitionDirectory()`：
- 调用 `StarOSAgent.getShardInfo()` 从 StarManager **查询** Shard 的存储路径（`FilePathInfo`）
- 选择一个 BE/CN 节点，通过 BRPC 调用 `LakeService.dropTable()` 让其删除对象存储上的物理文件目录
- 如果路径被多个分区共享（`isSharedDirectory`），则跳过删除，避免误删

**Step 2 — 清理 FE 残余元数据**

调用 `onErasePartition()` 清理 FE 侧与 Partition 相关的残留信息（如 tablet 统计信息等）。

**Step 3 — 删除 StarManager 元数据**

通过 `LakeTableHelper.deleteShardGroupMeta()`：
- 收集该 Partition 下所有 MaterializedIndex 的 `shardGroupId`
- 调用 `StarOSAgent.deleteShardGroup(groupIds, cascade=true)`，级联删除 ShardGroup 及其下属的所有 Shard 元数据

`deleteShardGroup` 失败时仅打 warn 日志不抛异常，交由兜底机制处理。

**错误处理与重试**：
- 如果 `delete()` 返回 false（任何步骤失败），回收站会在 `FAIL_RETRY_INTERVAL`（60 秒）后重新调度
- 异步删除通过 `CompletableFuture` + 线程池（`ASYNC_REMOVE_PARTITION_EXECUTOR`）执行，不阻塞回收站主线程

### 3.4 流程总览

```
ALTER TABLE t DROP PARTITION p
    │
    ▼ (同步，无 StarMgr 交互)
  FE 元数据移除 + 写 EditLog + 旧 Partition 放入回收站
    │
    ▼ (异步，回收站后台线程)
  RecycleLakeRangePartitionInfo.delete()
    ├── getShardInfo()          ← 查询 StarMgr 获取存储路径
    ├── LakeService.dropTable() ← 通知 BE/CN 删物理文件
    ├── onErasePartition()      ← 清理 FE 残余元数据
    └── deleteShardGroup()      ← 删除 StarMgr 中的 ShardGroup + Shard
```

---

## 四、TRUNCATE PARTITION

### 4.1 设计原则

TRUNCATE 的语义是**清空分区数据但保留分区结构**。实现上采用 **"创建新分区替换旧分区"** 的策略——不是原地清空数据，而是创建一组全新的空 Partition 来替换旧的：

1. **同步阶段**需要与 StarManager 交互——为新 Partition 创建 ShardGroup 和 Shard
2. 旧 Partition 以 **不可恢复** 状态放入回收站
3. 异步清理路径与 DROP PARTITION 完全复用

### 4.2 同步阶段：创建新 Partition 并替换

整个过程分为"锁外创建"和"锁内替换"两个子阶段，以减少持锁时间。

#### 4.2.1 锁外创建新 Partition

在持有读锁收集待 truncate 的 Partition 信息后，释放锁，在锁外为每个旧 Partition 创建一个全新的替代品：

**RPC-1: 创建 ShardGroup**
- 对新 Partition 的每个 MaterializedIndex，调用 `StarOSAgent.createShardGroup(dbId, tableId, newPartitionId, indexMetaId)`
- StarManager 返回新的 `shardGroupId`

**RPC-2: 创建 Shard**
- 对每个 MaterializedIndex，调用 `StarOSAgent.createShards(bucketNum, pathInfo, cacheInfo, shardGroupId, ...)`
- StarManager 返回一组新的 `shardId`，每个 shardId 对应一个新的 LakeTablet

然后调用 `buildPartitions()` 初始化新 Partition 的 tablet 元数据。

#### 4.2.2 锁内原子替换

获取写锁后：
1. 校验表和分区在创建新 Partition 期间未发生变化（schema change、分区变更等）
2. 写 EditLog：`logTruncateTable`
3. 调用 `replacePartition()` 逐个替换：
   - 旧 Partition 从 OlapTable 的内存结构中移除
   - **旧 Partition 以 `recoverable=false` 放入回收站**——这意味着不可通过 RECOVER 恢复，回收站会尽快调度清理
   - 新 Partition 挂到 OlapTable 上，继承旧 Partition 的 range/list 定义、DataProperty 等
4. 旧 Tablet 从 TabletInvertedIndex 中移除并标记强制删除

### 4.3 异步阶段：旧 Partition 回收

与 DROP PARTITION 的异步清理完全一致，复用 `RecycleLakeRangePartitionInfo.delete()` 的三步流程：
1. 查询 StarManager 获取旧 Shard 存储路径
2. 通知 BE/CN 删除旧的物理文件
3. 从 StarManager 删除旧的 ShardGroup 和 Shard 元数据

由于 `recoverable=false`，回收站会跳过正常的过期等待时间，尽快执行清理。

### 4.4 流程总览

```
TRUNCATE TABLE t PARTITION(p)
    │
    ▼ (同步，有 StarMgr 交互)
  createShardGroup() × N  ← 为新 Partition 的每个 MaterializedIndex 创建 ShardGroup
  createShards() × N      ← 为每个 ShardGroup 创建 bucket 数个 Shard
  buildPartitions()        ← 初始化新 Partition 元数据
    │
    ▼ (同步，写锁内)
  EditLog.logTruncateTable()
  replacePartition()       ← 新 Partition 替换旧 Partition
    ├── 旧 Partition → RecycleBin (recoverable=false)
    └── 新 Partition 挂到 Table 上
    │
    ▼ (异步，回收站后台线程，与 DROP 复用)
  RecycleLakeRangePartitionInfo.delete()
    ├── getShardInfo()          ← 查询 StarMgr 获取旧路径
    ├── LakeService.dropTable() ← 通知 BE/CN 删旧物理文件
    ├── onErasePartition()      ← 清理 FE 残余元数据
    └── deleteShardGroup()      ← 删除 StarMgr 中的旧 ShardGroup + Shard
```

---

## 五、兜底机制：StarMgrMetaSyncer

无论是 DROP 还是 TRUNCATE，异步清理链路中对 StarManager 的 RPC 调用都可能失败。`StarMgrMetaSyncer` 作为一个定期运行的守护线程（间隔由 `star_mgr_meta_sync_interval_sec` 控制），提供最终一致性保障。

### 5.1 孤儿 ShardGroup 清理

**对账逻辑**：
1. 从 FE 收集所有存活 Partition（含回收站中的）的全部 `shardGroupId` 集合
2. 从 StarManager `listShardGroup()` 获取全量 ShardGroup
3. 差集即为"孤儿 ShardGroup"——StarManager 中存在但 FE 中已无引用
4. 对超过 `shard_group_clean_threshold_sec` 的孤儿 ShardGroup 执行清理

**安全守卫**（防止误删）：
- 检查对应的表是否正在做 Cluster Snapshot
- 检查 ShardGroup 是否在某个 Cluster Snapshot 信息中
- 检查 ShardGroup 是否被 Storage Volume 绑定为虚拟 group

**清理流程**：
1. `listShard(groupId)` 获取该 ShardGroup 下所有 Shard
2. 按 BE/CN 节点分组，发送 `DeleteTabletRequest` 删除物理数据
3. 调用 `deleteShards()` 从 StarManager 删除 Shard 元数据
4. 确认 ShardGroup 为空后，调用 `deleteShardGroup()` 删除 ShardGroup 本身

### 5.2 表内孤儿 Shard 清理

**场景**：同一个 ShardGroup 内，StarManager 中的 Shard 多于 FE 中的 Tablet（常见于 Schema Change 完成后遗留的旧 shadow shard）。

**对账逻辑**：
1. 遍历每张 LakeTable 的每个 MaterializedIndex
2. 对每个 ShardGroup，从 StarManager `listShard()` 获取全部 Shard
3. 减去 FE 中该 MaterializedIndex 的 Tablet 集合
4. 差集即为孤儿 Shard，执行删除

**跳过条件**（避免误删正在使用的 Shard）：
- 表正在做 Schema Change（`state != NORMAL`）
- 表开启了 Automatic Bucketing（Shard 数量可能正在动态变化）
- ShardGroup 刚发生过变更

### 5.3 孤儿 Worker 清理

与 Partition 操作无直接关系，但属于同一对账框架：清理 StarManager 中已注册但 FE 集群中已不存在的 Worker（BE/CN 节点）。

---

## 六、DROP 与 TRUNCATE 对比

| 对比维度 | DROP PARTITION | TRUNCATE PARTITION |
|---------|---------------|--------------------|
| **用户语义** | 删除分区（结构 + 数据） | 清空数据，保留分区结构 |
| **实现策略** | 移除旧分区 | 创建新分区 **替换** 旧分区 |
| **同步阶段 StarMgr 交互** | 无 | 有（`createShardGroup` + `createShards`） |
| **异步阶段 StarMgr 交互** | 有（`deleteShardGroup`） | 有（`deleteShardGroup`），与 DROP 完全复用 |
| **回收站行为** | `recoverable=true`（默认可恢复） | `recoverable=false`（不可恢复，尽快清理） |
| **RECOVER 支持** | 支持（非 FORCE 模式） | 不支持 |
| **同步阶段延迟来源** | 仅 FE 内存 + EditLog | FE 内存 + EditLog + StarMgr RPC（创建新资源） |
| **ShardGroup 净变化** | 减少（旧的被删） | 先增后减（创建新的，异步删旧的） |

### 为什么 TRUNCATE 不能原地清空？

存算分离架构下，数据以不可变的数据文件形式存储在对象存储上，每个 Shard 对应一组文件。"原地清空"意味着需要：
1. 删除 Shard 关联的所有数据文件
2. 重置 Shard 的版本和元数据状态

这比"创建新 Shard"更复杂且风险更高——新 Shard 天然处于空的初始状态，而重置旧 Shard 需要处理各种中间态。因此采用"替换"策略更简洁可靠。

---

## 七、关键配置参数

| 参数 | 默认值 | 说明 |
|------|-------|------|
| `catalog_trash_expire_second` | 86400（1 天） | 回收站中 Partition 的保留时间（DROP 的可恢复期） |
| `star_mgr_meta_sync_interval_sec` | 600（10 分钟） | StarMgrMetaSyncer 对账间隔 |
| `shard_group_clean_threshold_sec` | 86400（1 天） | 孤儿 ShardGroup 至少存活多久才会被清理 |
| `meta_sync_force_delete_shard_meta` | false | 对账时是否仅删元数据不删物理数据（紧急修复用） |

---

## 八、关键代码索引

| 模块 | 文件 | 关键方法 |
|------|------|---------|
| TRUNCATE 入口 | `LocalMetastore.java` | `truncateTable()` |
| DROP 入口 | `LocalMetastore.java` | `dropPartition()` |
| 创建 Partition | `LocalMetastore.java` | `createPartition()` |
| 创建 LakeTablet | `LocalMetastore.java` | `createLakeTablets()` |
| 分区替换 | `OlapTable.java` | `replacePartition(long dbId, Partition newPartition)` |
| Lake 回收站 | `RecycleLakeRangePartitionInfo.java` | `delete()` |
| 物理文件删除 | `LakeTableHelper.java` | `removePartitionDirectory()` |
| ShardGroup 元数据删除 | `LakeTableHelper.java` | `deleteShardGroupMeta()` |
| StarMgr 客户端 | `StarOSAgent.java` | `createShardGroup()`, `createShards()`, `deleteShardGroup()`, `deleteShards()` |
| 回收站调度 | `CatalogRecycleBin.java` | `erasePartition()` |
| 兜底对账 | `StarMgrMetaSyncer.java` | `deleteUnusedShardAndShardGroup()`, `syncTableMetaAndColocationInfo()` |
