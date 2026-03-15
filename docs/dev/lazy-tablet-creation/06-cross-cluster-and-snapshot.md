# 跨集群同步与 Cluster Snapshot 分析

## 一、跨集群同步（Cross-Cluster Replication）

### 当前流程

```
1. 目标集群先 CREATE TABLE（正常 tablet 创建流程 → 写 version 1 metadata）
2. FE 创建 LakeReplicationJob，分配 virtualTabletId 访问源集群存储
3. FE 发送 ReplicateSnapshotTask 到 CN
4. CN 执行 replicate_lake_remote_storage():
   4a. 读取目标 tablet metadata at target_visible_version     ← 依赖 version 1
   4b. 读取目标 tablet metadata at data_version              ← 依赖 version 1
   4c. 从源集群拷贝数据文件到目标存储
   4d. 写入 txn log（op_replication）
5. FE 提交事务 → publish_version
   5a. CN 读取 base_version 的 metadata                      ← 依赖 version 1
   5b. 应用 txn log，写入新版本 metadata
```

### 关键代码位置

**FE 侧：**
- `LakeReplicationJob.java:68-84` — `run()` 直接从 INITIALIZING → REPLICATING
- `LakeReplicationJob.java` — `sendReplicateLakeRemoteStorageTasks()` 发送任务
- `SharedDataStorageVolumeMgr.java:704-736` — `getOrCreateVirtualTabletId()` 创建虚拟 tablet 访问源集群

**BE 侧（3 处读取目标 tablet metadata）：**

| 位置 | 代码 | 读取的版本 |
|------|------|-----------|
| `lake_replication_txn_manager.cpp:110` | `target_tablet.get_metadata(target_visible_version)` | 首次=1 |
| `lake_replication_txn_manager.cpp:333` | `get_tablet_metadata(target_tablet_id, data_version)` | 首次=1 |
| `transactions.cpp:253` | `get_tablet_metadata(tablet_id, base_version)` | publish 时 base=1 |

### 去掉 CN 交互的影响

**会受影响。** 如果目标 tablet 由新 FE 创建（跳过了 CN 交互），version 1 metadata 不存在，
上述 3 处读取全部失败。

| 场景 | 影响 |
|------|------|
| 目标 tablet 由老 FE 创建（有 metadata） | **OK** |
| 目标 tablet 由新 FE 创建（无 metadata）+ 新 CN | **取决于新 CN 是否适配** |
| 目标 tablet 由新 FE 创建（无 metadata）+ 降级后老 CN | **失败** |

### Virtual Tablet 说明

`SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` 创建的虚拟 tablet
用于访问**源**集群的对象存储（通过 `build_starlet_uri(virtual_tablet_id, "")`），
**不是**目标 tablet。虚拟 tablet 不需要 version 1 metadata。

---

## 二、Cluster Snapshot

### 机制说明

存算分离**不支持**传统的 BACKUP/RESTORE，使用 Cluster Snapshot 替代。

**Cluster Snapshot 与传统 BACKUP/RESTORE 的根本区别：**
- 传统 BACKUP：复制数据文件 + tablet metadata 到备份仓库
- Cluster Snapshot：**只保存 FE image + StarMgr image**（元数据快照），数据文件和 tablet metadata 保持在原位

### 创建流程

`ClusterSnapshotJob` → `ClusterSnapshotJobScheduler`：

1. `INITIALIZING`：捕获 FE journal ID 和 StarMgr journal ID 的一致性快照点
2. `SNAPSHOTING`：触发 FE checkpoint + StarMgr checkpoint 生成 image
3. `UPLOADING`：上传 FE image + StarMgr image 到远端存储（storage volume）
4. `FINISHED`：完成

**关键特点：不读取也不写入对象存储上的 tablet metadata。**

### 恢复流程

`RestoreClusterSnapshotMgr`：

1. 从远端下载 FE image + StarMgr image 到本地
2. FE 加载 image 恢复所有 FE 元数据
3. StarMgr 加载 image 恢复所有 shard 信息
4. 更新 frontend / compute node / storage volume 配置
5. 数据文件和 tablet metadata 不移动不复制——还在原来的对象存储上

**恢复时不涉及 tablet metadata 的读写。**

### 当前不支持表级恢复

`RestoreClusterSnapshotMgr` 是整集群级操作：
- 配置文件（`cluster_snapshot.yaml`）只包含 `cluster_snapshot_path`、`frontends`、`compute_nodes`、`storage_volumes`
- 没有表级过滤选项
- FE 直接加载完整 image，无部分加载逻辑

### 去掉 CN 交互的影响

| 场景 | 影响 |
|------|------|
| **创建 Cluster Snapshot** | **不受影响** — 只保存 FE + StarMgr image |
| **同版本恢复** | **不受影响** — 新 CN 处理有/无 metadata 的 tablet |
| **老版本 Snapshot → 新版本集群** | **不受影响** — 所有 tablet 都有 metadata |
| **新版本 Snapshot → 老版本集群** | **有风险** — 新 FE 创建的空 tablet（version=1）无 metadata，老 CN 无法操作 |

### 新版本 Snapshot → 老版本集群的具体问题

恢复后 FE 元数据中记录 tablet `visibleVersion=1`（空表/空分区）：
- INSERT：`publish_version` 读 `base_version=1` → 不存在 → **失败**
- SELECT 空表：`get_tablet_metadata(tablet_id, 1)` → **失败**
- Schema Change：`get_tablet(new_tablet_id, 1)` → **失败**

**但：** 有数据的 tablet（version>1），metadata 是数据写入时 publish 创建的，
与初始 metadata 无关。**只有 `visibleVersion=1` 的空 tablet 有问题。**

### 未来表级恢复的影响

如果未来支持从 Cluster Snapshot 恢复单张表到已有集群：
- 恢复的表可能包含新版本创建的 tablet（无 version 1 metadata）
- 目标集群的 CN 必须是新版本才能正确处理
- 需要在恢复流程中检查版本兼容性

---

## 三、与 Vacuum 的交互

### 当前交互

Vacuum 通过 `ClusterSnapshotMgr.getVacuumRetainVersions()` 获取需保留的版本：

```java
// AutovacuumDaemon.java:257
vacuumRequest.retainVersions = clusterSnapshotMgr.getVacuumRetainVersions(
    db.getId(), table.getId(), partition.getParentId(), partition.getId());
```

Cluster Snapshot 记录每个 partition 的 `visibleVersion`，vacuum 不会删除这些版本的 metadata。

### 去掉 CN 交互的影响

如果 version 1 metadata 不存在，但 `retainVersions` 包含 1（因为 `visibleVersion=1`）：
- Vacuum 尝试保留 version 1 → 文件不存在 → **不报错**
  （`vacuum.cpp` 中 `ignore_not_found` 处理了缺失文件）

**不受影响。**

---

## 四、汇总

| 功能 | 是否受影响 | 核心原因 | 严重程度 |
|------|-----------|---------|---------|
| 跨集群复制 | **是** | CN 读取目标 tablet version 1 metadata | **高** |
| Cluster Snapshot 创建 | **否** | 不读取 tablet metadata | — |
| Cluster Snapshot 同版本恢复 | **否** | 新 CN 处理缺失 metadata | — |
| Cluster Snapshot 跨版本恢复（新→老） | **有风险** | 老 CN 无法处理缺失 metadata | **中**（仅空 tablet）|
| 未来表级恢复 | **有风险** | 同跨版本恢复 | **中** |
| Vacuum | **否** | `ignore_not_found` | — |
