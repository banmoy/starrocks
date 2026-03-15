# 受影响的所有代码路径

## 概述

去掉 CN 交互影响 8 大类别：
1. FE 侧发送 CreateReplicaTask 的代码（3 条路径）
2. BE 侧执行创建任务的代码（1 条主路径 + 4 件事）
3. BE 侧读取初始 tablet metadata 的下游消费者（13 条路径）← **最关键**
4. FE 侧依赖任务完成的代码（6 个方面）
5. 配置参数、监控指标、运维路径
6. 升降级兼容性
7. 跨集群同步
8. Cluster Snapshot

---

## Category 1: FE 发送方（3 条路径）

### 1.1 TabletTaskExecutor（共享路径）

**文件**：`fe/fe-core/src/main/java/com/starrocks/task/TabletTaskExecutor.java`

- `buildCreateReplicaTasks()`（line 229）构建 `CreateReplicaTask`
- `buildPartitionsSequentially()`（line 88）→ `sendCreateReplicaTasksAndWaitForFinished()`
- `buildPartitionsConcurrently()`（line 126）→ `sendCreateReplicaTasks()`
- 都由 `LocalMetastore.buildPartitions()`（line 1983）调用

**所有 `buildPartitions()` 的调用方**：

| 调用方 | 文件:行 | 对应的用户功能 |
|--------|---------|-------------|
| `OlapTableFactory.createTable()`（非分区） | `OlapTableFactory.java:820` | CREATE TABLE |
| `OlapTableFactory.createTable()`（分区） | `OlapTableFactory.java:861` | CREATE TABLE |
| `LocalMetastore.addPartitions()` | `LocalMetastore.java:1377` | ADD PARTITION, 自动分区, 动态分区, MV PCT 刷新, INSERT OVERWRITE, MERGE PARTITION |
| `LocalMetastore.addSubPartitions()` | `LocalMetastore.java:1749` | Add 物理子分区 |
| `LocalMetastore.buildNonPartitionOlapTable()` | `LocalMetastore.java:3345` | 非分区 MV |
| `LocalMetastore.truncateTable()` flow | `LocalMetastore.java:4856` | TRUNCATE TABLE |
| `LocalMetastore.createTempPartitionsFromPartitions()` | `LocalMetastore.java:5447` | INSERT OVERWRITE, OPTIMIZE, MERGE PARTITION |

### 1.2 LakeTableSchemaChangeJob（独立路径）

**文件**：`fe/fe-core/src/main/java/com/starrocks/alter/LakeTableSchemaChangeJob.java`
- `CreateReplicaTask` 构建：line 465–487
- 发送：`sendAgentTaskAndWait()` line 502
- **功能**：ALTER TABLE schema change

### 1.3 LakeRollupJob（独立路径）

**文件**：`fe/fe-core/src/main/java/com/starrocks/alter/LakeRollupJob.java`
- `CreateReplicaTask` 构建：line 261–281
- 发送：`sendAgentTaskAndWait()` line 294
- **功能**：CREATE MATERIALIZED VIEW / ADD ROLLUP

### 1.4 不受影响的路径

| 组件 | 原因 |
|------|------|
| `LakeRestoreJob.sendCreateReplicaTasks()` | 覆写为 no-op |
| `SchemaChangeJobV2` / `RollupJobV2` | 使用 `TABLET_TYPE_DISK`，仅 shared-nothing |
| `ReportHandler` / `TabletScheduler` | 仅 LocalTablet 的副本恢复 |

---

## Category 2: BE 执行方

### 2.1 Agent Task 分发

- `agent_server.cpp:493-495`: `HANDLE_TASK(TTaskType::CREATE, ..., run_create_tablet_task, ...)`
- `agent_task.cpp:220`: `run_create_tablet_task()` — 入口
- `agent_task.cpp:248-251`: 当 `tablet_type == TABLET_TYPE_LAKE` 时分发到 `lake_tablet_manager()->create_tablet()`

### 2.2 `lake::TabletManager::create_tablet()` 做的 4 件事

| 事项 | 代码位置 | 对象存储写入 |
|------|---------|-------------|
| 构建 `TabletMetadataPB` | `tablet_manager.cpp:211-265` | 无 |
| 创建 schema file | `tablet_manager.cpp:266-268` → `create_schema_file()` line 1200 | 写 `{tablet_root}/SCHEMA_{schema_id}` |
| 写入 metadata 到对象存储 | `tablet_manager.cpp:270-274` → `put_tablet_metadata()` line 307 | 写 `{tablet_root}/meta/{tablet_id}_{version}.meta` |
| 缓存 metadata/schema | `put_tablet_metadata()` line 315-321, `create_schema_file()` line 1211-1213 | 无（内存） |

### 2.3 任务完成回报

- `agent_task.cpp:265-270`: Lake tablet 只回报 `tablet_id`（无 path hash, 无 report version）
- `agent_task.cpp:94-96`: `unify_finish_agent_task()` 发送 `TFinishTaskRequest` 到 FE

---

## Category 3: BE 下游消费者（13 条路径）← **最关键**

这些代码期望对象存储上存在 version 1 的 tablet metadata。如果不再写入，全部需要适配。

| # | 消费者 | 文件:行 | 读取版本 | 严重程度 | 说明 |
|---|--------|---------|---------|---------|------|
| 1 | **Publish version（首次写入）** | `transactions.cpp:253` | base=1 | **致命** | 所有新 tablet 首次 INSERT/LOAD 失败 |
| 2 | **Schema change 执行** | `schema_change.cpp:373` | 显式 version=1 | **致命** | ALTER TABLE 对新 tablet 失败 |
| 3 | **查询执行（SELECT）** | `lake_connector.cpp:192` | visible version | **致命** | SELECT 空表失败（visible=1）|
| 4 | **Schema 解析** | `tablet_manager.cpp:1004,1007` | 通过 metadata | **致命** | 所有操作都需要 schema |
| 5 | **Tablet 重分片** | `tablet_reshard.cpp:879,1024,1191` | base 可能=1 | **高** | 新 tablet 首次 split/merge 失败 |
| 6 | **跨集群复制** | `lake_replication_txn_manager.cpp:110,333` | target version=1 | **高** | 向新 tablet 复制数据失败 |
| 7 | **主键表索引加载** | `lake_primary_index.cpp` | base version | **高** | PK 表首次操作失败 |
| 8 | **Compaction** | `tablet_parallel_compaction_manager.cpp:331` | task version | **中** | 通常 version>1 |
| 9 | **Vacuum / GC** | `vacuum.cpp:529` | version 1 | **中** | GC 行为变化 |
| 10 | **Version 1 fallback** | `tablet_manager.cpp:539-548` | kInitialVersion | **中** | `tablet_creation_optimization` 的 fallback 链 |
| 11 | **Metadata listing** | `tablet_manager.cpp:779-781` | initial metadata | **低** | Admin 场景 |
| 12 | **Meta reader** | `lake_meta_reader.cpp:44` | query version | **低** | information_schema 查询 |
| 13 | **Admin repair** | `lake_service.cpp:1917` | repair version | **低** | Admin 修复 |

---

## Category 4: FE 完成依赖（6 个方面）

### 4.1 MarkedCountDownLatch 同步等待

| 位置 | Latch 创建 | 等待方法 |
|------|-----------|---------|
| `TabletTaskExecutor.buildPartitionsConcurrently()` | line 140 | `waitForFinished()` line 182 |
| `TabletTaskExecutor.buildPartitionsSequentially()` | line 312 | line 314 |
| `LakeTableSchemaChangeJob.sendAgentTaskAndWait()` | line 328 | line 353 |
| `LakeRollupJob.sendAgentTaskAndWait()` | line 614 | line 627 |

### 4.2 AgentTaskQueue 管理

| 位置 | 操作 |
|------|------|
| `TabletTaskExecutor.sendCreateReplicaTasks()` line 332 | `addTaskList()` |
| `TabletTaskExecutor` finally block line 119/193 | `removeTask()` |
| `LakeTableSchemaChangeJob` line 338 | `removeBatchTask()` on failure |
| `LakeRollupJob` line 623 | `removeBatchTask()` on failure |
| `LeaderImpl.finishCreateReplica()` line 430 | `removeTask()` in finally |

### 4.3 Task 完成回调

`LeaderImpl.finishCreateReplica()`（line 383-431）：
- 成功：`countDownLatch`，更新 backend report version
- 失败：`countDownToZero` 标记错误
- 整个回调链去掉 CN 交互后成为死代码

### 4.4 完成后的状态转换

| 功能 | 完成后操作 |
|------|-----------|
| CREATE TABLE | `onCreate()` → `logCreateTable()` → 注册表 |
| ADD PARTITION | 锁表 → 添加分区 → `logAddPartition()` |
| TRUNCATE TABLE | `truncateTableInternal()` → 替换分区 → `logTruncateTable()` |
| Schema Change | `addShadowIndexToCatalog()` → `persistStateChange(WAITING_TXN)` |
| Rollup | `addRollupIndexToCatalog()` → `persistStateChange(WAITING_TXN)` |
| INSERT OVERWRITE | 继续写入临时分区 |
| MV refresh | 继续数据刷新 |

### 4.5 失败回滚

| 功能 | 回滚操作 |
|------|---------|
| CREATE TABLE | `deleteUselessTablets()` |
| ADD PARTITION | `deleteUselessTablets()` |
| TRUNCATE TABLE | `deleteUselessTablets(tabletIdSet)` |
| Schema Change / Rollup | `AlterCancelException` → job 取消 → 清理 task |

### 4.6 ConsistencyChecker

- `addCreatingTableId()` / `deleteCreatingTableId()` 包裹 `buildPartitions()`
- 创建期间跳过一致性检查，去掉 CN 交互后窗口缩短到几乎为零

---

## Category 5: 配置、监控、运维

### 配置参数（4 个变为无关）

| 参数 | 默认值 | 当前用途 |
|------|-------|---------|
| `tablet_create_timeout_second` | 10s | 单个 replica 创建超时 |
| `max_create_table_timeout_second` | 600s | 建表最大超时 |
| `create_table_max_serial_replicas` | 128 | 串行/并行阈值 |
| `lake_enable_tablet_creation_optimization` | false | 共享 initial metadata |

### 监控指标（2 个不再触发）

| 指标 | 文件 |
|------|------|
| `create_tablet_requests_total` | `starrocks_metrics.h:82` |
| `create_tablet_requests_failed` | `starrocks_metrics.h:83` |

### Thrift 定义（部分字段不再使用）

| 字段 | 位置 |
|------|------|
| `TCreateTabletReq.create_schema_file` | `AgentService.thrift:132` |
| `TCreateTabletReq.enable_tablet_creation_optimization` | `AgentService.thrift` |

### Admin 命令

| 命令 | 影响 |
|------|------|
| `ADMIN SHOW TABLET` | 需处理缺失的 initial metadata |
| `ADMIN REPAIR TABLE` | 修复逻辑需调整 |
| `ADMIN CHECK TABLET` | 可能报不一致 |

### 其他相关代码

- `LakeTableAsyncFastSchemaChangeJob`（line 124-126）：使用 `TabletMetadataUpdateAgentTaskFactory` 更新已有 tablet metadata，**不是** CreateReplicaTask 路径，但 schema file 创建模式需 review

---

## Category 6: 升降级兼容性

升级规范：**先升 CN，再升 FE**。降级规范：**先降 FE，再降 CN**。

核心不变量变化：老版本假定 DDL 完成后 version 1 metadata 存在于对象存储；新版本打破了这个假定。

### 升级场景

| 阶段 | 状态 | 风险 |
|------|------|------|
| Phase 1：CN 新 + FE 老 | 老 FE 仍发 `CreateReplicaTask`，新 CN 保留 handler 正常处理 | **低** |
| Phase 2：CN 新 + FE 新 | 新 FE 跳过 CN 交互，新 CN 必须处理缺失 metadata | **无**（目标态） |
| 混合 FE Leader/Follower | CN 已全部是新版本，edit log 回放不发 CN task | **低** |

关键洞察：CN 总是先于 FE 升级，所以当 FE（新或老）运行时，所有 CN 已是新版本。

### 降级场景

| 阶段 | 状态 | 风险 |
|------|------|------|
| Phase 1：FE 老 + CN 新 | 新 CN 同时支持有/无 metadata 的 tablet | **低**（修复窗口） |
| Phase 2：FE 老 + CN 老 | 老 CN 无法处理缺失 metadata 的 tablet | **高** |

Phase 2 中**只有新版本期间创建的 tablet 受影响**，老 FE 创建的 tablet 不受影响。

缓解：降级 Phase 1 期间（新 CN 还在运行）运行修复工具，为缺失 metadata 的 tablet 补写。

### 兼容性要素

| 方面 | 影响 |
|------|------|
| Edit log 格式（`CreateTableInfo`、`AddPartitionsInfoV2` 等） | **不变** — 不含 CN 交互信息 |
| FE image / checkpoint | **不变** |
| `TabletMetadataPB` protobuf | **必须向后兼容** — 新字段须 optional + 有默认值 |
| 对象存储产物（`{tablet_id}_1.meta`、`SCHEMA_{id}`） | **Breaking** — 可能不存在 |

### 必须保证

1. 新 CN **保留 `create_tablet` handler** — 升级/降级 Phase 1 使用
2. 新 CN **处理所有 13 条下游消费者的缺失 metadata 情况**
3. 降级需要修复工具或接受新 tablet 损坏

---

## Category 7: 跨集群同步

**会受影响。** 跨集群同步（`LakeReplicationJob`）要求目标 tablet 已存在且有 metadata。

BE 侧 `replicate_lake_remote_storage()` 有 3 处读取目标 tablet metadata：

| 位置 | 代码 | 首次同步时版本 |
|------|------|-------------|
| `lake_replication_txn_manager.cpp:110` | `target_tablet.get_metadata(target_visible_version)` | 1 |
| `lake_replication_txn_manager.cpp:333` | `get_tablet_metadata(target_tablet_id, data_version)` | 1 |
| `transactions.cpp:253` | publish 时读 `base_version` | 1 |

如果目标 tablet 由新 FE 创建（无 version 1 metadata），这 3 处全部失败。
新 CN 必须在 `replicate_lake_remote_storage()` 和 `publish_version()` 中适配缺失 metadata。

注：`SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` 创建的虚拟 tablet 用于访问**源**集群存储，不受影响。

---

## Category 8: Cluster Snapshot

存算分离不支持传统 BACKUP/RESTORE，使用 Cluster Snapshot。

**Cluster Snapshot 只保存 FE image + StarMgr image，不涉及对象存储上的 tablet metadata。**

| 场景 | 影响 |
|------|------|
| 创建 Cluster Snapshot | **不受影响** |
| 同版本恢复 | **不受影响** |
| 老版本 Snapshot → 新版本集群 | **不受影响** |
| 新版本 Snapshot → 老版本集群 | **有风险** — 新 FE 创建的空 tablet（version=1）无 metadata，老 CN 无法操作 |
| Vacuum 交互 | **不受影响** — `vacuum.cpp` 中 `ignore_not_found` 处理缺失文件 |

注意：当前 Cluster Snapshot **不支持表级恢复**（整集群操作）。
如果未来支持，恢复的表中可能包含无 metadata 的 tablet，目标集群 CN 须为新版本。
