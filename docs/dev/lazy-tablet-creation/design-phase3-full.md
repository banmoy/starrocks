# Phase 3: 完整设计 — 去掉存算分离 Tablet 创建时的 CN 交互

## 1. 方案概述

### 1.1 核心机制回顾

**Lazy Construction（按需构建）**：DDL 时不写初始 metadata，在 CN 首次需要某个 tablet 的 version 1 metadata 时，通过 FE RPC 获取信息并在 `get_tablet_metadata` 中按需构建它。

**一句话概括**：对启用了 lazy construction 的表，DDL 时跳过 `CreateReplicaTask`。在 `TabletManager::get_tablet_metadata` 的统一入口处增加 fallback：当 version 1 metadata 在对象存储上不存在时，CN 调用 FE 获取 tablet 的初始配置（schema + 表级配置 + GTID），按需构建 `TabletMetadataPB` 并仅缓存在内存中（不持久化到对象存储），对所有下游消费者透明。

### 1.2 整体架构变更

```
变更前：
FE (DDL) ──CreateReplicaTask──> CN ──put_tablet_metadata──> Object Storage
                                                               ↓
                             所有后续操作 ←──get_tablet_metadata──┘

变更后（lazy construction 启用的表）：
FE (DDL) ──[跳过 CreateReplicaTask]──> 完成

首次操作时：
CN ──get_tablet_metadata(tablet_id, 1)──> Object Storage: NotFound
  ↓
CN ──GetTabletInitialMetadata RPC──> FE: 返回 schema + 配置 + GTID
  ↓
CN ──构建 TabletMetadataPB──> 缓存在内存中
  ↓
返回给调用方 → 后续操作（如 publish）写出 version 2+ metadata 到对象存储
```

### 1.3 核心机制是否需要调整

**结论：不需要调整。** Phase 2 确定的核心机制在 Phase 3 的逐路径验证中保持正确，无需修改。

---

## 2. FE 侧变更

### 2.1 表级属性与灰度控制

| 文件 | 函数/位置 | 变更语义 |
|------|-----------|---------|
| `fe/fe-core/.../catalog/TableProperty.java` | 新增属性定义 | 新增 `lazy_tablet_creation` 表属性（默认值由版本策略决定：4.2+ 新建表默认 true，升级上来的老表默认 false） |
| `fe/fe-core/.../catalog/OlapTable.java` | 属性读取 | 新增 `isLazyTabletCreation()` 方法，用于 DDL 路径判断是否跳过 CreateReplicaTask |

### 2.2 CreateReplicaTask 发送路径的条件跳过

| 文件 | 函数 | 变更语义 |
|------|------|---------|
| `fe/fe-core/.../task/TabletTaskExecutor.java` | `buildCreateReplicaTasks()` 调用方 | 在 `buildPartitions()` 的调用链中，当表启用 lazy construction 时，**不调用** `buildCreateReplicaTasks()` 和 `sendCreateReplicaTasksAndWaitForFinished()` / `sendCreateReplicaTasks()`，直接返回空任务列表或跳过发送逻辑 |
| `fe/fe-core/.../task/TabletTaskExecutor.java` | `buildPartitionsSequentially()` / `buildPartitionsConcurrently()` | 当 `tasks` 为空（lazy construction 表）时，不执行 `sendCreateReplicaTasksAndWaitForFinished()`，直接完成 |
| `fe/fe-core/.../alter/LakeTableSchemaChangeJob.java` | `sendAgentTaskAndWait()` 调用处（约 line 465-502） | 在 PENDING 阶段创建 shadow tablet 时，若表启用 lazy construction，**不构建** `CreateReplicaTask`，**不调用** `sendAgentTaskAndWait()`，直接进入 WAITING_TXN |
| `fe/fe-core/.../alter/LakeRollupJob.java` | `sendAgentTaskAndWait()` 调用处（约 line 261-294） | 同上，创建 rollup shadow tablet 时，若表启用 lazy construction，跳过 CreateReplicaTask 发送 |

**实现要点**：
- `TabletTaskExecutor`：在 `buildCreateReplicaTasks()` 或更早的入口处，根据 `table.isLazyTabletCreation()` 判断。若为 true，返回空列表，上层 `sendCreateReplicaTasksAndWaitForFinished()` 收到空列表后直接 return（或等价逻辑）。
- `LakeTableSchemaChangeJob` / `LakeRollupJob`：在构建 task 前检查表属性，若启用则跳过整个 `CreateReplicaTask` 构建和发送，直接 `persistStateChange(WAITING_TXN)`。

### 2.3 新增 FE RPC：GetTabletInitialMetadata

| 文件 | 变更 |
|------|------|
| `gensrc/thrift/` 或对应 Thrift 定义文件 | 新增 `TGetTabletInitialMetadataRequest` / `TGetTabletInitialMetadataResponse`。Request 含 `tablet_id`；Response 含构建 `TabletMetadataPB` 所需的全部字段：`TTabletSchema`（或等价）、`enable_persistent_index`、`persistent_index_type`、`compaction_strategy`、`flat_json_config`、`range`、`gtid`、`compression_type`、`compression_level` 等 |
| `fe/fe-core/.../rpc/` 或 `FrontendService` 实现 | 新增 RPC handler `getTabletInitialMetadata()`。通过 `TabletInvertedIndex` 根据 `tablet_id` 查找 tablet → table → partition → index → `MaterializedIndexMeta`，组装 schema 和配置；从 `PhysicalPartition` 或 alter job 的持久化字段获取 `initialGtid`；返回 Response |

### 2.4 GTID 持久化

| 文件 | 函数/位置 | 变更语义 |
|------|-----------|---------|
| `fe/fe-core/.../catalog/PhysicalPartition.java` | 新增字段 | 新增 `@SerializedName("initialGtid")` 字段。`buildPartitions()` 中生成 GTID 后设置到此字段，随 edit log 持久化 |
| `fe/fe-core/.../server/LocalMetastore.java` | `buildPartitions()` | 在分配 GTID 后，设置到每个 `PhysicalPartition` 的 `initialGtid` |
| `fe/fe-core/.../alter/LakeTableSchemaChangeJob.java` | 新增字段 | 新增 `@SerializedName("shadowInitialGtid")`，在 PENDING 阶段为 shadow tablet 分配 GTID 并持久化 |
| `fe/fe-core/.../alter/LakeRollupJob.java` | 新增字段 | 同上，为 rollup shadow tablet 持久化 GTID |
| Split/Merge 路径 | 已有 | `SplitTabletJob` / `MergeTabletJob` 已有 `gtid` 字段，无需变更 |

### 2.5 CreateTabletOption 传递

| 文件 | 变更 |
|------|------|
| `fe/fe-core/.../server/LocalMetastore.java` | `buildPartitions()` 中构建 `CreateTabletOption` 时，根据 `table.isLazyTabletCreation()` 决定是否跳过 task。若跳过，不传入需要 CN 的 option，或直接短路 |
| `fe/fe-core/.../lake/LakeTableHelper.java` 或相关 | 确保 alter/rollup 路径能获取表的 `lazy_tablet_creation` 属性 |

---

## 3. CN/BE 侧变更

### 3.1 核心变更：get_tablet_metadata 中的 Lazy Construction Fallback

| 文件 | 函数 | 变更语义 |
|------|------|---------|
| `be/src/storage/lake/tablet_manager.cpp` | `get_tablet_metadata(const string& path, ...)` | 在现有 kInitialVersion fallback（line 539-548）之后，若 `metadata_or.status().is_not_found()` 且 `version == kInitialVersion` 且 `fs == nullptr`（本地 tablet，非跨集群复制的 source 读取），调用新增的 `construct_initial_metadata(tablet_id)` |
| `be/src/storage/lake/tablet_manager.cpp` | 新增 `construct_initial_metadata(int64_t tablet_id)` | 调用 FE RPC `GetTabletInitialMetadata(tablet_id)`；用返回的 `TTabletSchema` 调用已有 `convert_t_schema_to_pb_schema()` 构建 schema；组装完整 `TabletMetadataPB`（id, version=1, schema, enable_persistent_index, range, gtid 等）；**不**调用 `put_tablet_metadata()`；将结果缓存到 `_metacache`（按 path 和 latest key）；返回 metadata |
| `be/src/storage/lake/tablet_manager.cpp` | `get_tablet_metadata(int64_t tablet_id, int64_t version, ...)` | 该重载最终调用 path-based 重载（line 491-495），因此 lazy construction 自动覆盖。**无需单独修改** |

**关键约束**：
- `fs != nullptr` 时**不**触发 lazy construction，避免跨集群复制读取 source tablet 时错误调用本地 FE。
- 仅当 `version == kInitialVersion`（即 1）时触发。

### 3.2 新增 RPC 客户端与 SingleFlight

| 文件 | 变更 |
|------|------|
| `be/src/storage/lake/` 或 `be/src/rpc/` | 新增 `TabletInitialMetadataService` 或扩展现有 RPC 客户端，实现 `GetTabletInitialMetadata` 调用。复用 `TableSchemaService` 的 SingleFlight 分组逻辑：按 `(table_id, index_id)` 分组，一次 RPC 返回 table/index 级别共享信息；per-tablet 的 `range` 在 RPC 中按 tablet_id 返回映射 |
| `be/src/storage/lake/tablet_manager.cpp` | `construct_initial_metadata()` 内使用 SingleFlight，避免同一 table/index 下多 tablet 并发请求时重复 RPC |

### 3.3 Schema File 处理

Lazy construction **不创建** schema file。构建的 `TabletMetadataPB` 已包含完整 schema，缓存后消费者从 metadata 获取 schema。现有 schema fallback 链（cache → schema file → FE RPC → metadata）中，metadata 路径可满足需求。首次 publish 写出 version 2 后，若需 schema file 可由后续流程按需创建；对 version 1 的短暂生命周期，不创建 schema file 可简化实现。

### 3.4 MetadataIterator 例外处理

| 文件 | 函数 | 变更语义 |
|------|------|---------|
| `be/src/storage/lake/metadata_iterator.cpp` | `MetadataIterator<TabletMetadataPtr>::get_metadata_from_tablet_manager(path)` | 当 `is_tablet_initial_metadata(basename(path))` 为 true 时，改为调用 `_manager->get_tablet_metadata(_tablet_id, 1, false)`，从而触发 lazy construction。否则保持 `get_tablet_metadata(path, false)` |

**理由**：`list_tablet_metadata` 在 objects 为空时插入的 initial path 解析为 tablet_id=0，path-based `get_tablet_metadata` 无法获知实际 tablet_id，故需在迭代器中用 `_tablet_id` 路由到 `(tablet_id, version)` 重载。

### 3.5 create_tablet Handler 保留

| 文件 | 变更 |
|------|------|
| `be/src/agent/agent_task.cpp` | **不修改**。保留 `run_create_tablet_task()` 及对 `lake_tablet_manager()->create_tablet()` 的调用。用于：未启用 lazy construction 的表、升降级兼容（老 FE 仍发 task） |

---

## 4. 每条受影响路径的处理方式

以下按 04-affected-code-paths.md 的 8 个 Category 逐条说明。

### Category 1: FE 发送方（3 条路径）

| 路径 | 处理方式 | 说明 |
|------|---------|------|
| **1.1 TabletTaskExecutor** | 统一机制：条件跳过 | 当 `table.isLazyTabletCreation()` 为 true 时，`buildCreateReplicaTasks()` 返回空列表，或上层在调用前判断并跳过 `sendCreateReplicaTasksAndWaitForFinished()`。覆盖 #1-#13 全部功能（CREATE TABLE、ADD PARTITION、TRUNCATE、INSERT OVERWRITE、MV 等） |
| **1.2 LakeTableSchemaChangeJob** | 统一机制：条件跳过 | 在 PENDING 阶段，若表启用 lazy construction，不构建、不发送 CreateReplicaTask，直接 `persistStateChange(WAITING_TXN)` |
| **1.3 LakeRollupJob** | 统一机制：条件跳过 | 同上 |

**为什么被统一机制覆盖**：三条路径的共性都是"在 DDL 的某个阶段发送 CreateReplicaTask"。通过表级属性判断，启用时直接跳过，逻辑集中、无例外。

### Category 2: BE 执行方

| 路径 | 处理方式 | 说明 |
|------|---------|------|
| **2.1 Agent Task 分发** | 保留不变 | `run_create_tablet_task` 和 `create_tablet` handler 保留，用于未启用 lazy construction 的表及升降级 |
| **2.2 create_tablet() 四件事** | 启用 lazy construction 时不被调用 | FE 不发送 task，CN 不执行。首次需要 metadata 时由 `construct_initial_metadata()` 在内存中完成等价逻辑（构建、缓存），不写对象存储 |
| **2.3 任务完成回报** | 同上 | 无 task 则无回报 |

### Category 3: BE 下游消费者（13 条路径，按 04 文档；Phase 1 验证为 16 条直接消费者）

**统一机制**：所有消费者均通过 `get_tablet_metadata()` 或 `Tablet::get_metadata()`（内部调 `get_tablet_metadata`）读取 metadata。在 `get_tablet_metadata` 的 path-based 重载中增加 lazy construction fallback 后，**全部自动覆盖**。

| # | 消费者 | 文件:行 | 覆盖方式 |
|---|--------|---------|---------|
| 1 | Publish version | `transactions.cpp:253` | `get_tablet_metadata(tablet_id, base_version)` → base_version=1 时 fallback → lazy construction |
| 2 | Schema change 执行 | `lake/schema_change.cpp:373` | `get_tablet(new_tablet_id, 1)` → `get_tablet_metadata` → lazy construction |
| 3 | 查询执行 | `connector/lake_connector.cpp:192` | `get_tablet(tablet_id, version)` → visible_version=1 时 lazy construction |
| 4 | Schema 解析 | `tablet_manager.cpp:1004,1007` 等 | 通过 `get_tablet_metadata` 或 schema fallback 链，最终可触发 lazy construction |
| 5 | Tablet 重分片 | `tablet_reshard.cpp:879,1024,1191` | `get_tablet_metadata` → base_version=1 时 lazy construction |
| 6 | 跨集群复制（target） | `lake_replication_txn_manager.cpp:110,335` | `target_tablet.get_metadata(1)` 和 `get_tablet_metadata(target_tablet_id, 1)` → fs=nullptr → lazy construction |
| 7 | 主键表索引加载 | `update_manager.cpp:1439` → `lake_primary_index` | `get_tablet_metadata(tsid.tablet_id, meta_ver)` → meta_ver=1 时 lazy construction |
| 8 | Compaction | `tablet_parallel_compaction_manager.cpp:331` | 通常 version>1；若 task_version=1，lazy construction 覆盖 |
| 9 | Vacuum / GC | `vacuum.cpp:273,359,913` | `get_tablet_metadata` → version=1 时 lazy construction |
| 10 | Version 1 fallback | `tablet_manager.cpp:539-548` | 现有 kInitialVersion fallback 先执行；仍 NotFound 时由新增 lazy construction 处理 |
| 11 | Metadata listing | `tablet_manager.cpp:778-781` | **需单独处理**：`list_tablet_metadata` 在 objects 为空时插入 `tablet_initial_metadata_filename()`，path 解析为 tablet_id=0，无法触发 lazy construction。`MetadataIterator::get_metadata_from_tablet_manager()` 需判断：当 path 为 initial 路径时，调用 `get_tablet_metadata(_tablet_id, 1)` 而非 `get_tablet_metadata(path)`，从而走 lazy construction |
| 12 | Meta reader | `lake_meta_reader.cpp:44`（在 `be/src/storage/`） | 通过 `get_tablet_metadata` 读取，被覆盖 |
| 13 | Admin repair | `lake_service.cpp:1917` | 此为**写入**操作（`put_tablet_metadata`），非读取 version 1，**不受影响** |

**Phase 1 补充的消费者**（14-16）：
| 14 | Vacuum full | `vacuum_full.cpp:72` | `get_tablet_metadata(tablet_id, version)`，version 可能为 kInitialVersion → 覆盖 |
| 15 | Tablet retain info | `tablet_retain_info.cpp:27` | `get_tablet_metadata(tablet_id, version)` → 覆盖 |
| 16 | Lake delvec loader | `lake_delvec_loader.cpp:49,51` | `get_tablet_metadata` → 覆盖 |

**跨集群复制 source tablet**：`build_source_tablet_meta()` 使用 `get_tablet_metadata(path, false, 0, shared_src_fs)`，`fs != nullptr`。Lazy construction 条件要求 `fs == nullptr`，故**不触发**，正确——source 在源集群对象存储，本地 FE 无其元数据。

### Category 4: FE 完成依赖（6 个方面）

| 方面 | 处理方式 | 说明 |
|------|---------|------|
| 4.1 MarkedCountDownLatch | 条件跳过 | 当不发送 CreateReplicaTask 时，不创建 latch，不等待。DDL 直接完成 |
| 4.2 AgentTaskQueue | 条件跳过 | 无 task 则无 addTaskList/removeTask |
| 4.3 Task 完成回调 | 条件跳过 | 无 task 则 `finishCreateReplica` 不被调用 |
| 4.4 完成后的状态转换 | 不变 | CREATE TABLE、ADD PARTITION、Schema Change、Rollup 等的状态转换逻辑不变，仅完成时机提前（无需等 CN） |
| 4.5 失败回滚 | 调整 | 若 DDL 在"跳过 task"后失败，回滚只需清理 FE 侧元数据（如 `deleteUselessTablets()`），无需清理 CN task——因未发送 |
| 4.6 ConsistencyChecker | 不变 | `addCreatingTableId`/`deleteCreatingTableId` 仍包裹 `buildPartitions()`，窗口更短 |

### Category 5: 配置、监控、运维

| 项目 | 处理方式 |
|------|---------|
| `tablet_create_timeout_second` 等 4 个参数 | 对启用 lazy construction 的表**不再生效**（无 task）。保留供未启用的表使用。文档标注"对 lazy tablet creation 表无效" |
| `lake_enable_tablet_creation_optimization` | 新方案完全替代。可标记 deprecated，最终移除。Lazy construction fallback 链中保留对 shared initial metadata 的检查，兼容正在使用该优化的集群 |
| `create_tablet_requests_total` / `create_tablet_requests_failed` | 位于 shared-nothing `TabletManager`。Lake 路径不经过。对 lake 启用 lazy construction 后，lake create_tablet 调用减少，无新增指标需求。可选：新增 `lazy_tablet_metadata_construct_total` 计数 lazy construction 触发次数 |
| Thrift `create_schema_file` / `enable_tablet_creation_optimization` | 对 lazy construction 表不再使用；保留供兼容 |
| ADMIN SHOW TABLET / REPAIR / CHECK | `ADMIN SHOW TABLET`：若 tablet 无 version 1 文件，list 可能返回空或依赖 lazy construction 的缓存；需确保 list 逻辑与 `get_tablet_metadata` 一致。`ADMIN REPAIR`：修复逻辑针对已有 metadata 重写，不涉及新建；若需"补写 version 1"，可用预降级命令。`ADMIN CHECK`：可能对无 version 1 文件的 tablet 报不一致，需根据 lazy construction 语义调整或接受 |

### Category 6: 升降级兼容性

见第 5 节。

### Category 7: 跨集群同步

| 路径 | 处理方式 |
|------|---------|
| Target tablet 读取 version 1 | 通过 `get_tablet_metadata(target_tablet_id, 1, ..., nullptr)`，fs=nullptr，触发 lazy construction。**被统一机制覆盖** |
| Source tablet 读取 | `build_source_tablet_meta` 使用 `shared_src_fs`，fs!=nullptr，**不**触发 lazy construction。**正确** |
| Publish 到 target | 同 Category 3 #1，被覆盖 |

### Category 8: Cluster Snapshot

| 场景 | 处理方式 |
|------|---------|
| 创建 / 同版本恢复 / 老 Snapshot→新集群 | **不受影响** |
| 新 Snapshot→老版本集群 | 新 FE 创建的空 tablet 无 version 1 metadata。老 CN 无法处理。需在降级前执行预降级命令补写 metadata，或接受该场景不支持 |

---

## 5. 升降级方案

### 5.1 升级路径

| 阶段 | 状态 | FE 行为 | CN 行为 | 安全性 |
|------|------|---------|---------|--------|
| **Phase 0** | FE 老 + CN 老 | 发 CreateReplicaTask | 执行 create_tablet | 当前状态 |
| **Phase 1** | CN 新 + FE 老 | 仍发 CreateReplicaTask | 新 CN 保留 create_tablet handler，正常执行 | **安全**：行为与 Phase 0 一致 |
| **Phase 2** | CN 新 + FE 新 | 根据表属性决定是否跳过 CreateReplicaTask | lazy construction + create_tablet 双模式 | **安全**：目标态 |

**Phase 1 关键**：先升 CN，后升 FE。CN 新版本已包含 lazy construction fallback，但 FE 仍发 task，故 create_tablet 仍被调用，无行为变化。

### 5.2 降级路径

| 阶段 | 状态 | 分析 | 安全性 |
|------|------|------|--------|
| **Phase 1** | FE 新 → FE 老 | FE 降级后开始为所有 DDL 发 CreateReplicaTask | **安全**：新 DDL 创建的 tablet 有 version 1 |
| **Phase 2** | CN 新 → CN 4.1.x（含兼容补丁） | 4.2 时期创建的空 tablet：4.1.x CN 的 lazy construction fallback 处理 | **安全**：需 4.1.x 小版本提前加入 fallback 代码 |
| **Phase 3** | CN 4.1.x → CN 4.1.x 之前 | 老 CN 无法处理无 version 1 的 tablet | **需预降级**：执行 `ADMIN PREPARE DOWNGRADE` 补写 metadata |

### 5.3 预降级命令

**命令**：`ADMIN PREPARE DOWNGRADE;`

**作用**：扫描所有启用 lazy construction 的表的 tablet；对缺少 version 1 metadata 的 tablet（空 tablet），通过 CN 补写 version 1 metadata + schema file 到对象存储。

**实现要点**：FE 下发修复 task 到 CN，CN 对指定 tablet 执行与 `create_tablet` 等价的写入逻辑（或调用 `construct_initial_metadata` 后 `put_tablet_metadata`）。

### 5.4 数据迁移或修复工具

- **升级**：不需要。Edit log 格式不变，无数据迁移。
- **降级到 4.1.x**：若 4.1.x CN 含兼容代码，不需要。
- **降级到 4.1.x 之前**：需要预降级命令。无额外离线修复工具。

---

## 6. 配置和监控变更

### 6.1 配置参数

| 参数 | 变更 |
|------|------|
| `tablet_create_timeout_second` | 对 lazy construction 表无效。文档补充说明 |
| `max_create_table_timeout_second` | 同上 |
| `create_table_max_serial_replicas` | 同上 |
| `lake_enable_tablet_creation_optimization` | 标记 deprecated；lazy construction 完全替代其能力 |
| 新增 `lazy_tablet_creation` 表属性 | 支持 `CREATE TABLE ... PROPERTIES("lazy_tablet_creation" = "true/false")` 及 `ALTER TABLE ... SET("lazy_tablet_creation" = "true/false")` |

### 6.2 监控指标

| 指标 | 变更 |
|------|------|
| `create_tablet_requests_total` / `create_tablet_requests_failed` | 对 lake 路径，启用 lazy construction 后调用减少。无代码修改，行为自然变化 |
| 新增（可选）`lazy_tablet_metadata_construct_total` | Counter，记录 lazy construction 触发次数，便于观测 |
| 新增（可选）`get_tablet_initial_metadata_rpc_latency_us` | Histogram，记录 FE RPC 延迟 |

---

## 7. 自我检验

### 7.1 04-affected-code-paths.md 中的所有路径是否全部覆盖？

| Category | 路径 | 覆盖情况 |
|----------|------|---------|
| **1.1** | TabletTaskExecutor | ✅ 条件跳过 CreateReplicaTask |
| **1.2** | LakeTableSchemaChangeJob | ✅ 条件跳过 |
| **1.3** | LakeRollupJob | ✅ 条件跳过 |
| **2.1** | Agent Task 分发 | ✅ 保留，兼容 |
| **2.2** | create_tablet 四件事 | ✅ 启用时不调用；lazy construction 等价完成 |
| **2.3** | 任务完成回报 | ✅ 无 task 则无回报 |
| **3.1** | Publish version | ✅ get_tablet_metadata fallback |
| **3.2** | Schema change 执行 | ✅ 同上 |
| **3.3** | 查询执行 | ✅ 同上 |
| **3.4** | Schema 解析 | ✅ 同上 |
| **3.5** | Tablet 重分片 | ✅ 同上 |
| **3.6** | 跨集群复制 | ✅ target 覆盖；source 正确不触发 |
| **3.7** | 主键表索引加载 | ✅ 同上 |
| **3.8** | Compaction | ✅ 同上 |
| **3.9** | Vacuum / GC | ✅ 同上 |
| **3.10** | Version 1 fallback | ✅ 扩展为 lazy construction |
| **3.11** | Metadata listing | ✅ 同上 |
| **3.12** | Meta reader | ✅ 同上 |
| **3.13** | Admin repair | ✅ 不受影响（写入路径） |
| **4.1-4.6** | FE 完成依赖 | ✅ 条件跳过或不变 |
| **5** | 配置、监控、运维 | ✅ 已说明 |
| **6** | 升降级 | ✅ 已说明 |
| **7** | 跨集群同步 | ✅ 已说明 |
| **8** | Cluster Snapshot | ✅ 已说明 |

**结论：全部覆盖。**

### 7.2 方案中有多少处"例外处理"？每一处的理由是什么？

**2 处例外处理**：

1. **`get_tablet_metadata` 的 path-based 重载**：增加 lazy construction fallback。理由：统一入口，覆盖绝大部分消费者。
2. **`MetadataIterator::get_metadata_from_tablet_manager`**：当 path 为 initial 路径时，改用 `get_tablet_metadata(tablet_id, 1)`。理由：initial path 解析为 tablet_id=0，path-based 重载无法获知实际 tablet_id，迭代器持有 `_tablet_id`，需在此路由。

**无其他例外**：FE 侧的条件跳过是同一逻辑（表属性判断）的重复应用，非独立例外。2 处 < 3 处阈值。

### 7.3 升降级的每个中间状态是否安全？逐状态确认

| 状态 | 安全性 |
|------|--------|
| Phase 0：FE 老 + CN 老 | ✅ 当前生产状态 |
| Phase 1：CN 新 + FE 老 | ✅ 老 FE 发 task，新 CN 执行 create_tablet，行为一致 |
| Phase 2：CN 新 + FE 新 | ✅ 目标态，lazy construction 生效 |
| 降级 Phase 1：FE 老 + CN 新 | ✅ 老 FE 发 task，新 CN 仍可执行 create_tablet |
| 降级 Phase 2：FE 老 + CN 4.1.x（含兼容） | ✅ 4.2 创建的空 tablet 由 4.1.x fallback 处理 |
| 降级 Phase 3：FE 老 + CN 4.1.x 之前 | ⚠️ 需先执行预降级命令，否则新表不可用 |

**结论：除"降级到 4.1.x 之前"需预降级外，其余状态均安全。**

### 7.4 方案落地后系统是变简单了还是变复杂了？从哪些维度判断？

| 维度 | 判断 |
|------|------|
| **DDL 路径** | **变简单**：启用 lazy construction 的表，DDL 从 FE→CN→等待 变为 FE 直接完成，无 RPC、无超时、无 latch |
| **CN 职责** | **变简单**：CN 不再承担 DDL 时的 tablet 创建，向无状态计算节点演进 |
| **新增代码** | **略增**：~200 行（FE RPC handler、GTID 持久化、表属性、CN fallback）。集中在少数文件 |
| **双模式并存** | **过渡期略复杂**：需同时支持 lazy construction 与 create_tablet。长期当所有表启用后可移除 CreateReplicaTask 相关代码（~500+ 行） |
| **下游消费者** | **无变化**：消费者无需修改，通过统一入口自动受益 |
| **运维** | **变简单**：无需调 tablet 创建超时参数；降级到更早版本需预降级命令，属非常规场景 |

**综合判断：对启用 lazy construction 的表，系统显著变简单；整体在过渡期有轻微双模式复杂度，长期趋向更简单。**
