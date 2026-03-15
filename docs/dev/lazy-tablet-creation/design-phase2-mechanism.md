# Phase 2: 核心机制 — 去掉存算分离 Tablet 创建时的 CN 交互

## 1. 核心机制

### 整体方向

**Lazy Construction（按需构建）：DDL 时不写初始 metadata，在 CN 首次需要某个 tablet 的 version 1 metadata 时，通过 FE RPC 获取信息并在 `get_tablet_metadata` 中按需构建它。**

### 一句话概括

对启用了 lazy construction 的表，DDL 时跳过 `CreateReplicaTask`。在 `TabletManager::get_tablet_metadata` 的统一入口处增加一个 fallback：当 version 1 metadata 在对象存储上不存在时，CN 调用 FE 获取 tablet 的初始配置（schema + 表级配置 + GTID），按需构建 `TabletMetadataPB` 并仅缓存在内存中（不持久化到对象存储），对所有下游消费者透明。

### 核心机制描述

```
当前流程（DDL 时同步创建）：
FE (DDL) ──CreateReplicaTask──> CN ──put_tablet_metadata──> Object Storage
                                                               ↓
                             所有后续操作 ←──get_tablet_metadata──┘

新流程（lazy construction 开启的表）：
FE (DDL) ──[跳过 CreateReplicaTask]──> 完成

首次操作时（publish / query / schema change / replication）：
CN ──get_tablet_metadata(tablet_id, 1)──> Object Storage: NotFound
  ↓
CN ──GetTabletInitialMetadata RPC──> FE: 返回 schema + 配置 + GTID
  ↓
CN ──构建 TabletMetadataPB──> 缓存在内存中（不写对象存储）
  ↓
返回给调用方 → 后续操作（如 publish）写出 version 2+ metadata 到对象存储

注：通过表级属性控制是否启用 lazy construction（见 4.1 灰度控制策略）。
未启用的表仍走当前流程（发送 CreateReplicaTask）。
```

### 为什么只缓存不持久化

Version 1 metadata 是一个**空容器**（无 rowset、无数据），它唯一的用途是作为首次 publish 的 base。一旦 publish 完成，version 2 metadata 被写入对象存储，version 1 即被永久取代。

不持久化的理由：
1. **避免无谓的对象存储写入**：绝大多数场景中，建表后很快就会导入数据。首次 publish 会写 version 2 metadata，version 1 永远不需要从对象存储读回。
2. **更简单的实现**：省去并发写入协调、幂等性保证等复杂度。
3. **降级通过分层策略解决**：无缝降级到前一大版本（4.1.x）通过在 4.1.x CN 中提前加入 lazy construction fallback 代码保证；降级到更早版本通过预降级命令补写 metadata 保证（见 4.2 降级路径）。持久化不是解决降级的正确手段。

对于 CN 重启的情况：如果某个空表（version=1）被查询，lazy construction 触发 FE RPC → 构建 metadata → 缓存。CN 重启后缓存丢失，下一次查询再次触发 FE RPC。这个开销（~1-10ms）与现有的 `TableSchemaService` cache miss 行为一致，不是新模式。

### 为什么叫"lazy construction"而非"懒加载"

严格来说，这不是从某个已有存储位置"加载"数据，而是在首次需要时从 FE 的元数据**按需构建**。构建后仅存在于内存，直到首次 publish 产生的 version 2 metadata 被写入对象存储。

---

## 2. 为什么是这个方向

### 考虑过的备选方向

| # | 备选方向 | 概述 |
|---|---------|------|
| A | **Lazy Construction（选定）** | `get_tablet_metadata` fallback + FE RPC 按需构建 |
| B | 消除所有消费者对 version 1 的依赖 | 修改每个消费者使其不假设 version 1 存在 |
| C | FE 直接写对象存储 | DDL 时 FE 构建 protobuf 并写入 |
| D | FE 在每个请求中附带初始配置 | publish/query/schema change 请求携带配置 |
| E | 默认值 + 后续修正 | 用默认值构建 version 1，通过 ALTER META 修正 |
| F | StarManager 存储初始 metadata | 扩展 shard properties |

### 每个备选方向被排除的具体原因

**方向 B：消除所有消费者对 version 1 的依赖**

- 需要修改 16+ 条已知直接消费者路径 **【事实，依据：Phase 1 Plan 1.3 节，全局搜索 `get_tablet_metadata` 调用】**
- 每个消费者需要独立的"无 version 1"处理逻辑，本质是逐条打补丁
- 未来新增的 metadata 消费者需要记住"不能假设 version 1 存在"的约束——这是隐式契约，容易遗忘
- 设计标准明确拒绝："对每条下游消费者路径逐个加 fallback / 特殊处理——这不是设计，是打补丁"
- **排除原因：违反统一机制原则，修改面过广，未来维护成本高**

**方向 C：FE 直接写对象存储**

- FE 不知道 tablet 在对象存储上的实际路径——路径映射由 StarOS/Starlet 的 `LocationProvider` 管理，仅 CN 侧有 **【事实，依据：`StarletLocationProvider` 在 `be/src/storage/lake/starlet_location_provider.cpp`，FE 无等价物】**
- `convert_t_schema_to_pb_schema()` 只有 C++ 实现，FE 需要重写 Java 版本 **【事实，依据：`metadata_util.cpp:249-351`】**
- FE 需要引入对象存储写入能力——这是重大架构变更
- 设计标准明确拒绝："把 CN 做的事原样搬到 FE 或其他组件——这不是去掉依赖，是转移依赖"
- **排除原因：转移依赖，架构侵入性过大**

**方向 D：FE 在每个请求中附带初始配置**

- 需要修改 4+ 种 FE 请求类型：`TPublishVersionRequest`、`TScanRangeParams`、`TAlterTabletReqV2`、replication request **【推论：基于需要初始 metadata 的 4 个场景】**
- FE 需要在每个请求中判断"目标 tablet 是否需要初始配置"——增加 FE 逻辑复杂度
- 散落在多个代码路径中，不是统一机制
- 超过 3 处例外修改
- **排除原因：多处修改，散落逻辑，不满足统一机制要求**

**方向 E：默认值 + 后续修正**

- `enable_persistent_index`：默认 `false`。如果表设置为 `true`，首次 publish 不会构建 persistent index，导致后续主键表操作异常 **【事实，依据：`PrimaryKeyTxnLogApplier` 在 `txn_log_applier.cpp:318` 检查此字段】**
- `range`：对于 range 分布的主键表，`lake_persistent_index.cpp:541` 有 `RETURN_IF(!metadata->has_range(), Status::InternalError(...))` —— 无 range 直接报错 **【事实，依据：`lake_persistent_index.cpp:541`】**
- Publish 流程将 version 1 的所有字段复制到 version 2（`transactions.cpp:354`），错误的默认值会永久传播
- 设计标准明确拒绝："引入后台修复任务来弥补设计闭环的缺失"
- **排除原因：初始状态不正确，错误传播，需要额外修正机制**

**方向 F：StarManager 存储初始 metadata**

- StarManager 是 shard（存储资源）管理服务，不是 tablet metadata 存储服务——角色混淆
- 需要修改 StarManager 的 shard properties 格式，影响面广
- CN 读取 shard properties 的路径与 `get_tablet_metadata` 不同，需要额外集成
- 增加了 StarManager 的复杂度和职责
- **排除原因：跨越组件边界，增加系统复杂度**

### 为什么 Lazy Construction（方向 A）是正确选择

1. **单一变更点**：仅修改 `TabletManager::get_tablet_metadata` 中的 fallback 链。所有 16+ 条下游消费者路径自动覆盖，无需逐条修改。**【事实，依据：所有消费者最终都通过 `get_tablet_metadata` 的 path-based 重载（`tablet_manager.cpp:514-557`）读取 metadata】**

2. **对下游透明**：消费者拿到的 `TabletMetadataPB` 与 `create_tablet()` 写入的完全一致。无需区分"老 tablet"和"新 tablet"。

3. **面向未来**：未来新增的 metadata 消费者只要通过 `get_tablet_metadata` 读取，自动享受 fallback。不需要记住任何隐式约束。

4. **利用已有基础设施**：
   - `get_tablet_metadata` 已有 fallback 链（per-tablet → bundle → shared initial）**【事实，依据：`tablet_manager.cpp:539-548`】**
   - `TableSchemaService` 已有 SingleFlight、重试、缓存机制 **【事实，依据：`table_schema_service.cpp:77-141`】**
   - `TabletInvertedIndex` 可从 tablet_id 查到 table/partition/index **【事实，依据：`TabletInvertedIndex.java:92`】**

5. **真正消除 DDL 对 CN 的依赖**：DDL 只涉及 FE 和 StarManager，完全不需要 CN 参与。

---

## 3. 关键路径验证

### 3.0 前置说明：File bundling 和 Hash/Range 分布

**File bundling 下的 version 1 创建**：当 file bundling 开启时，FE 设置 `enableTabletCreationOptimization = true` **【事实，依据：`LocalMetastore.java:2017-2018`，`isFileBundling()` 导致 `enableTabletCreationOptimization = true`】**。此时 FE 只对每个 partition/index 的**第一个** tablet 发送 `CreateReplicaTask`（循环中有 `break`）**【事实，依据：`TabletTaskExecutor.java:305-307`】**。CN 将初始 metadata 写入共享路径 `{metadata_root}/0000000000000000_0000000000000001.meta`（`tablet_initial_metadata_location`）**【事实，依据：`tablet_manager.cpp:270-271`】**。由于 file bundling 下同 partition 所有 tablet 的 `real_location` 解析到同一物理目录 **【事实，依据：`StarletLocationProvider::real_location()` 通过 `ShardInfo.path_info.full_path()` 解析，同 partition 的 shard 共享 `PartitionFilePathInfo`（`OlapTable.java:3171-3174`）】**，其他 tablet 通过 `get_tablet_metadata` 中的 kInitialVersion fallback（`tablet_manager.cpp:539-548`）找到这个共享文件并 patch `tablet_id`。

`get_single_tablet_metadata()` 对 `version == kInitialVersion` 直接返回 NotFound **【事实，依据：`tablet_manager.cpp:642-643`】**，原因是 version 1 metadata 是普通的 `TabletMetadataPB`（非 `BundleTabletMetadataPB` 格式），由 path-based 重载中的 kInitialVersion fallback 处理，不走 bundle 解析路径。

**对 lazy construction 的影响**：lazy construction 的 fallback 位于现有 kInitialVersion fallback 之后。在 file bundling 下：如果共享初始 metadata 存在（老 FE 创建的 tablet），现有 fallback 直接命中，lazy construction 不触发；如果不存在（新 FE 跳过了 CreateReplicaTask），lazy construction 触发。两种情况天然兼容。

**Hash 分布 vs Range 分布**：差异仅体现在 `TabletMetadataPB.range` 字段：Hash/Random 分布不设置 range，Range 分布的每个 tablet 有独立的 range。FE RPC 通过 `Tablet.getRange()` 获取 per-tablet 的 range（已持久化）**【事实，依据：`Tablet.java:34` `@SerializedName("range")`】**。注意：Range 分布下将来可能在建表时一次性创建多个 tablet，每个 tablet 的 range 不同。RPC 设计需要支持返回 per-tablet 的 range 信息（见 Tradeoff 1 RPC 聚合粒度讨论）。

**DELETE SQL 和 Spark Load**：两者都不创建新 tablet，而是对已有 tablet 写入数据（DELETE 写 delete predicate，Spark Load 通过 PUSH_REQ 推送数据），最终通过标准的 commit → publish version 路径完成。Publish version 读取 base_version metadata 时，如果 base_version=1 则触发 lazy construction。**【事实，依据：`LakeDeleteJob.java:246-249` 走 `commitAndPublishTransaction`；`SparkLoadJob.java:594` 通过 `pushTask` 推送数据后走 publish 路径】**。两者均被统一机制覆盖。

### 3.1 新 tablet 的首次写入（publish version）

**场景**：`CREATE TABLE t1 ...`，然后 `INSERT INTO t1 VALUES (...)`

**当前流程**：
1. CREATE TABLE → FE 发 `CreateReplicaTask` → CN 写 version 1 metadata → 对象存储
2. INSERT → FE 发 publish version（base_version=1, new_version=2）→ CN 读 version 1 → 应用 txn log → 写 version 2

**新流程**：
1. CREATE TABLE → FE 创建 shard IDs（StarManager）、构建 `LakeTablet`、记录 edit log → **不发 CreateReplicaTask** → 完成
2. INSERT → FE 发 publish version（base_version=1, new_version=2）→ CN 执行 `publish_version()`
3. `publish_version()` → `get_tablet_metadata(tablet_id, 1, false)` **【依据：`transactions.cpp:252-253`】**
4. `get_tablet_metadata` fallback 链：
   - 缓存：未命中
   - 对象存储（per-tablet）：`{tablet_id}_0000000000000001.meta` → NotFound
   - Bundle metadata：对 kInitialVersion 直接跳过（version 1 不以 bundle 格式存储）**【事实，依据：`tablet_manager.cpp:642-643`】**
   - Shared initial metadata（`0000000000000000_0000000000000001.meta`）→ NotFound（新 FE 未创建此文件）
   - **新增 fallback**：`construct_initial_metadata(tablet_id)`
5. `construct_initial_metadata(tablet_id)`:
   - 调用 FE RPC `GetTabletInitialMetadata(tablet_id)` → 获得 schema + 全部配置
   - 构建 `TabletMetadataPB`（id=tablet_id, version=1, schema=..., enable_persistent_index=..., 等）
   - 缓存到内存（不写对象存储）
   - 返回 metadata
6. `publish_version()` 拿到 version 1 metadata → `new_metadata = make_shared<TabletMetadataPB>(*base_metadata)` → 应用 txn log → 写 version 2 到对象存储 → 完成

**性能影响**：首次 publish 增加 1 次 FE RPC（~1-10ms）。无额外对象存储写入（version 2 的写入与当前一致）。后续 publish 完全无影响。

**正确性**：version 1 metadata 包含完整、准确的 schema 和配置，与 `create_tablet()` 写入的一致。version 2 metadata 从 version 1 继承所有字段，行为无变化。

### 3.2 新 tablet 的首次查询（SELECT on empty table）

**场景**：`CREATE TABLE t1 ...`，然后 `SELECT * FROM t1`（无数据）

**新流程**：
1. FE 规划查询 → visible_version=1 → 发 scan 请求到 CN
2. CN `LakeDataSource::get_tablet()` → `tablet_manager->get_tablet(tablet_id, 1)` **【依据：`lake_connector.cpp:192`】**
3. `get_tablet(tablet_id, 1)` → `get_tablet_metadata(tablet_id, 1, ...)` → lazy construction → 缓存 → 返回
4. metadata 中 rowsets 为空 → 返回 0 行
5. Schema 获取：如果 FE 支持 fast schema evolution v2（`schema_key` 存在），schema 走 `TableSchemaService`（已有机制）；否则走 `tablet.get_schema()` 从 metadata 中获取 **【依据：`lake_connector.cpp:196-204`】**

**性能影响**：首次查询增加 1 次 FE RPC（~1-10ms）。CN 重启后缓存丢失则再次触发。后续查询（CN 未重启期间）完全无影响。

**正确性**：空表查询返回 0 行 + 正确的 schema。行为与当前一致。

### 3.3 Schema change 对新 tablet 的执行

**场景**：`CREATE TABLE t1 ...`，然后 `ALTER TABLE t1 ADD COLUMN ...`

Schema change 涉及两类 tablet 读取 version 1 metadata：
- **Shadow tablet**：ALTER TABLE PENDING 阶段创建的新 tablet，永远从 version 1 开始
- **Base tablet**：原始 tablet，ALTER TABLE 期间可能在 version 1（空表）

**新流程**：
1. ALTER TABLE → PENDING：FE 创建 shadow tablet 的 shard IDs → **不发 CreateReplicaTask** → 进入 WAITING_TXN
2. RUNNING：FE 发 `TAlterTabletReqV2` → CN `SchemaChangeHandler::do_process_alter_tablet()`
3. `_tablet_manager->get_tablet(new_tablet_id, 1)` → `get_tablet_metadata(new_tablet_id, 1, ...)` → lazy construction
4. `_tablet_manager->get_tablet(base_tablet_id, alter_version)` → 如果 alter_version=1 且 base tablet 也是新创建的 → lazy construction

**关键细节**：shadow tablet 的 schema 是新 schema（包含新增列）。FE 在 `LakeTableAlterJobV2Builder.build()` 中为 shadow tablet 创建 shard 并构建独立的 `MaterializedIndexMeta`。FE RPC 通过 tablet_id → index_id → `MaterializedIndexMeta` 获取正确的 schema。**【依据：`LakeTableAlterJobV2Builder.java:89-102`】**

**正确性**：shadow tablet 的 lazy construction 获取的是新 schema，base tablet 获取的是旧 schema。schema change 正确执行数据转换。

### 3.4 Lake Rollup（ADD ROLLUP / 同步物化视图）

**场景**：`CREATE MATERIALIZED VIEW mv1 AS SELECT ...` 对已有表创建 rollup index

Rollup 涉及 shadow tablet（rollup index 的新 tablet），类似 schema change 但有独立的构建路径。

**新流程**：
1. `LakeTableRollupBuilder.build()` 创建 shadow tablet 的 shard IDs + `MaterializedIndexMeta` **【依据：`LakeTableRollupBuilder.java:76-125`】** → **不发 CreateReplicaTask**
2. `LakeRollupJob` RUNNING 阶段：FE 发 `TAlterTabletReqV2` → CN 读取 shadow tablet 的 version 1 → lazy construction
3. FE RPC 通过 tablet_id → `TabletInvertedIndex` → rollup 的 index_id → rollup 的 `MaterializedIndexMeta` → 正确的 rollup schema

**关键细节**：rollup shadow tablet 的 schema 是 rollup 列定义，不同于 base index 的 schema。FE 在 `LakeTableRollupBuilder.build()` 中为每个 shadow tablet 关联了正确的 `rollupIndexMetaId`，且 `TabletInvertedIndex` 存储了 tablet → index 的映射。FE RPC 能正确返回 rollup schema。**【事实，依据：`LakeTableRollupBuilder.java:111-117`，`TabletInvertedIndex.java:92`】**

**正确性**：与 schema change 的 shadow tablet 处理完全一致。lazy construction 获取的 schema 是 rollup 定义的列集。

### 3.5 跨集群复制到新 tablet

**场景**：配置跨集群复制，目标集群上有新建的空表，源集群推送数据。

**新流程**：
1. FE 发起 replication → CN `LakeReplicationTxnManager::replicate_lake_remote_storage()`
2. 读取 source tablet metadata → **不受影响**
3. 读取 target tablet metadata：`target_tablet.get_metadata(target_visible_version)` **【依据：`lake_replication_txn_manager.cpp:110-111`】**
4. 如果 `target_visible_version == 1` → lazy construction → 返回 version 1 metadata
5. 基于 source + target metadata 构建 replicated metadata → 写入 target tablet

**为什么读取 source tablet metadata 不受影响**：

Source tablet 的 metadata 不通过本地集群的 `get_tablet_metadata(tablet_id, version)` 重载读取，而是通过 `build_source_tablet_meta()` 使用显式的源集群文件系统（`shared_src_fs`）和显式构造的路径（`src_meta_dir`）直接读取源集群对象存储 **【事实，依据：`lake_replication_txn_manager.cpp:244-257`，`build_source_tablet_meta` 调用 `get_tablet_metadata(src_tablet_meta_path, false, 0, shared_src_fs)` 使用 path-based 重载 + 外部文件系统】**。

Lazy construction 的 fallback 条件是 `version == kInitialVersion`，而 source tablet 读取的版本是 `src_visible_version`（源集群上的已 publish 版本），通常 > 1。即使 `src_visible_version == 1`（源集群是空表），lazy construction 的 FE RPC 会调用**本地** FE，而本地 FE 不持有源集群的 tablet 信息，因此需要确保 lazy construction 只在本地 tablet 读取（`fs == nullptr`）时触发。**【Phase 3 需确认 fallback 条件中增加 `fs == nullptr` 检查】**

**正确性**：target tablet 的 lazy construction 提供完整的 metadata，与 `create_tablet()` 写入的一致。source tablet 走独立的读取路径。replication 逻辑无需变化。

---

## 4. 升降级验证

### 前提

- 升级规范：先升 CN，后升 FE。降级规范：先降 FE，后降 CN。**【事实，依据：`04-affected-code-paths.md` Category 6】**
- 假设该功能在 **4.2** 版本发布
- 降级兼容性标准：**无缝降级到至少前一个大版本**（4.1.x），允许在 4.1.x 小版本中加兼容性代码；降级到更早版本允许通过预降级操作处理

### 4.1 升级路径与灰度控制

| 阶段 | 状态 | FE 行为 | CN 行为 | 分析 |
|------|------|---------|---------|------|
| **Phase 0** | FE 老 + CN 老 | 发 CreateReplicaTask | 执行 create_tablet | 当前状态 |
| **Phase 1** | FE 老 + CN 新 | 发 CreateReplicaTask（老 FE 不知道新能力） | 新 CN **保留** create_tablet handler，正常执行 | ✅ 天然兼容 |
| **Phase 2** | FE 新 + CN 新 | 根据表级开关决定是否跳过 CreateReplicaTask | lazy construction + create_tablet 双模式 | ✅ 目标态 |

**灰度控制策略**：

FE 通过**表级属性**控制是否使用 lazy construction：

| 表类型 | 默认行为 | 可配置 |
|--------|---------|--------|
| **4.2+ 新建的表** | 默认启用 lazy construction（跳过 CreateReplicaTask）| 可通过表属性关闭，回退到老行为 |
| **从旧版本升级上来的老表** | 默认不启用（保持发送 CreateReplicaTask）| 可通过 ALTER TABLE 按需开启 |

理由：
- 老表升级后默认保持老行为，**零风险**——行为与升级前完全一致
- 新表默认使用新机制，享受 DDL 性能提升
- 按需开启给用户和 SRE 控制权，出问题可逐表回退
- 表属性随 edit log 持久化，FE 重启后状态不丢失

**Phase 1 的关键**：新 CN 必须保留 `create_tablet` agent task handler。这不是额外的兼容层，而是新旧代码的自然共存——即使在 Phase 2，关闭了 lazy construction 的表仍然需要此 handler。

### 4.2 降级路径

**降级兼容性分层**：

| 降级目标 | 兼容级别 | 要求 |
|---------|---------|------|
| **4.1.x（前一大版本之后的小版本）** | **无缝降级** | 4.1.x 小版本中包含 CN 侧兼容性代码（lazy construction fallback） |
| **4.1.x 之前（更早版本）** | **预降级操作后可降** | 降级前执行兼容性命令，为受影响 tablet 补写 version 1 metadata + schema file |

#### 4.2.1 无缝降级到 4.1.x

**前提**：4.1.x 的某个小版本（如 4.1.5）CN 中提前加入 lazy construction fallback 代码。这段代码在 4.1.x 上是"休眠"的（因为 4.1.x FE 始终发送 CreateReplicaTask），但为 4.2 → 4.1.x 降级提供安全网。

| 阶段 | 状态 | 分析 |
|------|------|------|
| FE 4.2 → FE 4.1.x | FE 降级，开始为所有 DDL 发 CreateReplicaTask | 新 DDL 创建的 tablet 有 version 1 metadata → 安全 |
| CN 4.2 → CN 4.1.x | CN 降级到包含兼容性代码的 4.1.x | 4.2 时期创建的 tablet：已有数据的 → version 2+ 在 S3 → 安全；空 tablet → 4.1.x CN 的 lazy construction fallback 处理 → 安全 |

**4.1.x 兼容性代码的范围**：仅需在 CN 的 `get_tablet_metadata` 中加入 lazy construction fallback（~30-50 行）+ FE 中加入对应 RPC handler。这是一个小范围的向前兼容补丁。

#### 4.2.2 降级到 4.1.x 之前的版本

**操作步骤**：降级前执行预降级兼容性命令。

预降级命令的作用：
1. 扫描所有启用了 lazy construction 的表的 tablet
2. 对缺少 version 1 metadata 的 tablet（空 tablet），通过 CN 补写 version 1 metadata + schema file 到对象存储
3. 对已有 version 2+ 的 tablet，version 1 不是必需的（老 CN 可以从 version 2+ 工作），但如果需要严格兼容，也可补写

命令示例（Phase 3 详细设计）：
```sql
-- 为所有受影响的 tablet 补写 version 1 metadata
ADMIN PREPARE DOWNGRADE;
```

**为什么这不违反"不引入人工运维步骤"的设计标准**：设计标准针对的是**正常升降级路径**（前一大版本）的要求。降级到**更早版本**（跨越两个大版本）本身就不是常规操作，允许额外步骤是合理的。

### 4.3 升降级兼容性总结

```
4.2 (新版本)
  │
  │ 无缝降级（4.1.x CN 含兼容性代码）
  ▼
4.1.x（含兼容性补丁的小版本）
  │
  │ 需要预降级命令（ADMIN PREPARE DOWNGRADE）
  ▼
4.1.x 之前的版本
```

核心原因在于：**新 CN 同时支持两种模式**：
1. 收到 `CreateReplicaTask` → 执行 `create_tablet()` → 写 version 1 metadata（兼容老 FE 或关闭了 lazy construction 的表）
2. `get_tablet_metadata` 发现 version 1 不存在 → lazy construction → 缓存 version 1 metadata（lazy construction 开启的表）

两种模式不冲突，通过表级属性控制，灰度可控。

---

## 5. 关键 Tradeoff

### Tradeoff 1：首次操作增加 FE RPC

| 维度 | 判断 | 依据 |
|------|------|------|
| **延迟增量** | 1 次 FE RPC (~1-10ms) | TableSchemaService RPC 延迟参考 `g_schema_rpc_latency_us` 指标 |
| **触发频率** | 每个 tablet 在 CN 缓存有效期内一次 | 缓存命中后不再 RPC |
| **影响路径** | 首次 publish（最常见）、首次查询空表（较少）、首次 schema change（少） | 基于操作频率推断 |
| **是否可接受** | **是** | DDL 延迟降低数量级（从分钟级到毫秒级），首次操作增加 ~1-10ms，净收益巨大 |

**RPC 聚合粒度**：

同一 table/index 下的所有 tablet 共享相同的 schema 和表级配置。唯一的 per-tablet 差异是 `range` 字段（仅 range 分布时有意义，且每个 tablet range 不同）。

RPC 设计分两层：
1. **共享信息获取**：按 `(table_id, index_id)` 粒度。一次 RPC 返回该 table/index 的 schema + 全部表级配置（enable_persistent_index、compaction_strategy 等）。CN 侧 SingleFlight 按 `(table_id, index_id)` 分组去重。
2. **Per-tablet 信息（range）**：两种方式待 Phase 3 决定：
   - **方式 A**：RPC 接受一批 tablet_ids，返回 per-tablet 的 range 映射。适合首次 publish（同 partition 多 tablet 并发）。
   - **方式 B**：CN 侧从共享信息构建 metadata 时，range 字段留空。Range 分布表的持久化索引等路径如果需要 range，通过单独的 per-tablet RPC 补充。

理由选择 `(table_id, index_id)` 而非 `(partition_id, index_id)` 作为 SingleFlight 分组键：
- 同一 table 不同 partition 的 schema 和表级配置完全相同，无需区分 partition
- 首次建表后批量导入时，多个 partition 的 tablet 可能同时 publish，更粗的分组能合并更多请求

**RPC 目标 FE**：

参照已有的 `TableSchemaService` 模式 **【事实，依据：`table_schema_service.cpp:100,116`】**：
- **LOAD/Publish 路径**：发送到 FE Leader（`get_master_address()`），因为 publish 由 FE Leader 发起
- **SCAN/Query 路径**：发送到 coordinator FE（即发起查询的 FE），因为 scan 请求中已包含 coordinator 地址
- **Schema change / Replication 路径**：发送到 FE Leader

这与 `TableSchemaService` 完全一致，可复用其路由逻辑。

### Tradeoff 2：持久化 vs 纯内存缓存

| 选项 | 优点 | 缺点 |
|------|------|------|
| 持久化到对象存储 | CN 重启后不需重复 RPC | 每个 tablet 多一次 S3 写入（~10-50ms）；并发写入需协调；持久化仍不能完全解决降级问题 |
| **纯内存缓存（选定）** | 零额外 S3 IO；实现简单；首次 publish 后 version 2 即在 S3 上 | CN 重启后需重新 RPC；降级后空 tablet 缺少 version 1 |

**判断**：选择纯内存缓存。原因：
1. **降级通过分层策略解决，不依赖持久化**：无缝降级到前一大版本通过 4.1.x CN 兼容性代码保证；降级到更早版本通过预降级命令补写 metadata 保证（见 4.2 降级路径）。
2. **绝大多数场景中 version 1 不需要从 S3 读回**：建表后通常很快导入数据（首次 publish 写出 version 2），之后所有操作都基于 version 2+。version 1 metadata 的生命周期极短（从 lazy construction 到首次 publish），不值得为此做一次 S3 写入。
3. **CN 重启后的 FE RPC 开销可接受**：与 `TableSchemaService` 的 cache miss 行为一致（~1-10ms），且只影响空表的查询。有数据的表直接从 S3 读 version 2+。
4. **简化实现**：无需处理多 CN 并发写入同一 version 1 metadata 的协调问题。

### Tradeoff 3：新 FE RPC vs 扩展已有 TableSchemaService

| 选项 | 优点 | 缺点 |
|------|------|------|
| **新 RPC `GetTabletInitialMetadata`** | 语义清晰；不污染 schema 服务 | 新接口 + handler |
| **扩展 `TGetTableSchemaResponse`** | 复用 SingleFlight 基础设施 | 在 schema 响应中混入非 schema 字段 |

**判断**：倾向新 RPC，但具体实现可在 Phase 3 决定。核心要求是：
- 输入：`tablet_id`
- 输出：构建 version 1 `TabletMetadataPB` 所需的全部信息（schema + 配置字段）
- CN 侧实现可复用 `TableSchemaService` 的 SingleFlight/重试基础设施

### Tradeoff 4：GTID 在 version 1 中的值

**当前 GTID 分配机制**：

初始 metadata 的 GTID 通过 `GtidGenerator.nextGtid()` 分配，这是一个基于时间戳的全局唯一 ID（42bit 时间戳 + 8bit 集群 + 13bit 序列号）**【事实，依据：`GtidGenerator.java:23-28`】**。分配时机：
- `LocalMetastore.buildPartitions()` 中为整个 DDL 操作分配一个 GTID **【事实，依据：`LocalMetastore.java:2019`】**
- `LakeTableSchemaChangeJob` / `LakeRollupJob` 各自分配一个 GTID **【事实，依据：`LakeTableSchemaChangeJob.java:421`, `LakeRollupJob.java:218`】**
- 同一 DDL 中所有 tablet 共享同一个 GTID

**GTID 的语义定义**：

GTID 的语义是"产生此版本 metadata 的全局事务 ID"。在存算分离模式下，GTID 在 `TabletMetadataPB` 中记录产生此 metadata 的事务，用于 `expected_gtid` 缓存一致性校验（`tablet_manager.cpp:448,735`）。在存算一体模式下，GTID 还用于 `capture_consistent_rowsets(gtid)` 实现基于 GTID 的 MVCC 读取 **【事实，依据：`tablet.cpp:874-891` `_gtid_to_version_map`】**。对于 version 1，GTID 的语义是"创建此 tablet 的 DDL 操作的 ID"。

**判断：从 FE 获取正确的 GTID（选定）**

考虑到语义完整性和避免未来潜在问题，version 1 metadata 中应设置与当前 `create_tablet()` 一致的 GTID 值，而非默认 0。

**实现方式**：

1. **FE 持久化初始 GTID**：
   - 对于 `buildPartitions()` 路径（CREATE TABLE / ADD PARTITION / TRUNCATE TABLE 等）：在 `PhysicalPartition` 中新增 `@SerializedName("initialGtid")` 字段。`buildPartitions()` 生成 GTID 后，设置到每个 `PhysicalPartition` 中，随 edit log 持久化。**【可行性：`PhysicalPartition` 已有多个 `@SerializedName` 字段，新增字段向后兼容（默认值 0）】**
   - 对于 Schema Change / Rollup 路径：在 `LakeTableSchemaChangeJob` / `LakeRollupJob` 中新增 `@SerializedName` 字段持久化 shadow tablet 的初始 GTID。**【可行性：两个 job 类已有 `watershedGtid` 字段作为先例】**
   - 对于 Split / Merge 路径：`SplitTabletJob` / `MergeTabletJob` 已有 `@SerializedName("gtid")` **【事实，依据：`SplitTabletJob.java:69`, `MergeTabletJob.java:69`】**

2. **FE RPC 返回 GTID**：CN 请求 tablet 初始配置时，FE 通过 `tablet_id` → `TabletInvertedIndex` → `PhysicalPartition.getInitialGtid()`（或对应 job 的 GTID）返回。

3. **幂等性保证**：
   - GTID 一旦生成即持久化到 edit log，FE 重启后从 image/edit log 恢复，值不变
   - 同一 tablet 的多次 RPC 查询返回同一个 GTID（从持久化字段读取，非重新生成）
   - 多个 CN 并发请求同一 tablet 的初始 GTID，FE 返回相同值（只读查询，无竞争）

**当前代码中 version 1 GTID 的使用分析**（补充说明）：

虽然选择从 FE 获取正确的 GTID，但值得记录：当前读取 version 1 metadata 的代码路径均传入 `expected_gtid = 0`（不触发校验）：
- publish 路径：`get_tablet_metadata(tablet_id, base_version, false)` — expected_gtid 为 0 **【事实，依据：`transactions.cpp:252-253`】**
- query 路径：`get_tablet(tablet_id, version)` — expected_gtid 为 0 **【事实，依据：`tablet_manager.cpp:1285-1289`】**
- `cal_new_base_version` 中传入非 0 expected_gtid 的路径仅读 `index_version > base_version`，不读 version 1 **【事实】**

这意味着即使 GTID 为 0 也不会导致当前代码出错。选择从 FE 获取是出于语义完整性和防御性设计，而非修复现有 bug。

---

## 6. 未知项和风险

### 6.1 FE RPC 的具体接口设计（未知，Phase 3 解决）

FE 需要提供的信息：

| 字段 | 来源 | FE 是否已持久化 |
|------|------|----------------|
| `TabletSchemaPB` | `MaterializedIndexMeta` → schema → `convert_t_schema_to_pb_schema()` | ⚠️ FE 存的是 Java `SchemaInfo`/`TTabletSchema`，需转换为 protobuf |
| `enable_persistent_index` | `OlapTable.enablePersistentIndex()` | ✅ 已持久化 |
| `persistent_index_type` | `OlapTable.getPersistentIndexType()` | ✅ 已持久化 |
| `compaction_strategy` | `OlapTable.getCompactionStrategy()` | ✅ 已持久化 |
| `flat_json_config` | `TableProperty` | ✅ 已持久化 |
| `range` | `Tablet.getRange()` | ✅ 已持久化（`@SerializedName`）|
| `compression_type/level` | `MaterializedIndexMeta` → schema | ✅ 已持久化 |

**关键问题**：FE 需要将 Java 侧的 `TTabletSchema` 转换为 `TabletSchemaPB`。当前 `convert_t_schema_to_pb_schema()` 只有 C++ 实现。

两种可能的解决方式：
1. **FE 返回 `TTabletSchema`（Thrift），CN 侧调用已有的 `convert_t_schema_to_pb_schema()` 转换**——复用已有代码，CN 侧转换开销极低
2. **FE 实现 Java 版 protobuf 序列化**——需要新代码，但避免了 Thrift → protobuf 的二次转换

倾向方式 1，因为它完全复用已有代码路径，与当前 `CreateReplicaTask` 的 schema 传递方式一致（FE 传 Thrift `TTabletSchema` → CN 转为 protobuf）。

### 6.2 多 CN 并发 lazy construction

多个 CN 可能同时对同一 tablet 触发 lazy construction（例如不同 CN 同时查询同一空表）。由于只缓存不持久化：
- 每个 CN 独立调用 FE RPC → 独立构建 version 1 metadata → 独立缓存
- 无竞争条件，无需协调
- FE RPC 是只读操作，可安全并发

多 CN 各自缓存一份 version 1 metadata。这与 `TableSchemaService` 的行为一致——每个 CN 独立缓存 schema。

**风险等级**：无。纯内存缓存天然支持并发。

### 6.3 表或 partition 被删除后的 RPC 失败处理

当 lazy construction 触发 FE RPC 时，tablet 对应的表或 partition 可能已不存在（DROP TABLE / DROP PARTITION / TRUNCATE TABLE）。具体场景：

| 场景 | FE 侧状态 | RPC 行为 |
|------|----------|---------|
| DROP TABLE 后残留的 publish | `TabletInvertedIndex` 中 tablet 已删除 | RPC 返回 "tablet not found" |
| DROP PARTITION 后残留的 publish | 同上 | RPC 返回 "tablet not found" |
| TRUNCATE TABLE | 老 partition 被删除，新 partition 创建新 tablet ID | 老 tablet RPC 返回 "tablet not found"；新 tablet 正常 |
| TRUNCATE TABLE + 立即查询 | 新 partition 的新 tablet 需要 version 1 | lazy construction 正常（新 tablet 在 FE 中存在） |

处理方式：CN 将 FE RPC 的 "not found" 错误透传给调用方。这与当前 metadata 文件在 S3 上不存在时的行为一致——调用方（publish/query/etc）自身有处理 "metadata not found" 的逻辑。

**注意**：TRUNCATE TABLE 创建新 partition + 新 tablet ID，老 tablet ID 不会被复用。因此不存在"老 tablet 的 lazy construction 返回了新 tablet 的数据"的风险。**【事实，依据：`LocalMetastore.truncateTable()` 创建全新的 shard IDs（`createLakeTablets`），不复用老 ID】**

**风险等级**：低。错误透传，无需特殊逻辑。

### 6.4 FE Leader 切换期间的 RPC 可用性

如果 lazy construction 的 FE RPC 在 FE Leader 切换期间失败，CN 应重试。已有的 `TableSchemaService` 重试机制可以复用。

**风险等级**：低。复用已有机制。

### 6.5 Schema file 的处理（需 Phase 3 详细设计）

当前 `create_tablet` 在 `create_schema_file == true` 时写入独立的 schema file（`SCHEMA_{schema_id}`）。在 lazy construction 中，schema file 的创建时机和方式需要明确：
- 选项 A：lazy construction 时同时创建 schema file
- 选项 B：依赖已有的 schema fallback 链（cache → schema file → FE RPC → metadata 中的 schema）

这是实现细节，Phase 3 决定。

### 6.6 `tablet_creation_optimization` 的废弃策略

新方案完全替代 `lake_enable_tablet_creation_optimization`（该开关只是减少 CN RPC 次数，不消除 CN 依赖）。废弃策略：
- Phase 3 需要确定：是同步删除老代码，还是保留一段时间后删除
- lazy construction 的 fallback 链中已包含 shared initial metadata 的检查——天然兼容正在使用老优化的集群

---

## 自我检验

### Q1：这个方案是真正消除了依赖，还是换了个形式？

**对启用 lazy construction 的表，真正消除了 DDL 对 CN 的依赖。**

- DDL（CREATE TABLE, ADD PARTITION, ALTER TABLE 的 PENDING 阶段, TRUNCATE TABLE 等）完成时不需要任何 CN 参与
- 新增的 FE RPC 不在 DDL 路径上，而在首次数据操作路径上
- 首次数据操作（publish, query, schema change）本身就需要 FE 和 CN 同时在线——FE 发起操作，CN 执行。FE RPC 是 CN 对已在线的 FE 的回调，不是新引入的依赖
- 通过表级属性灰度控制，老表可保持老行为（发送 CreateReplicaTask），新表默认使用新机制

本质变化：对启用的表，DDL 的依赖图从 `FE → CN → Object Storage` 变为 `FE → StarManager`。CN 的参与推迟到真正需要 tablet 数据/元数据的时刻。

### Q2：首次写入、首次查询有没有性能退化？具体退化多少、是否可接受？

**首次操作有可测量但可接受的延迟增加。**

| 路径 | 当前延迟 | 新增延迟 | 总影响 |
|------|---------|---------|--------|
| DDL（CREATE TABLE 1000 分区 × 128 bucket） | 分钟级（发送 128,000 次 CreateReplicaTask）| **降低到毫秒级**（仅 StarManager 操作）| **极大改善** |
| 首次 publish（单个 tablet）| ~50-200ms（读 version 1 + 应用 txn log + 写 version 2）| +1-10ms（FE RPC，无额外 S3 写入）| 增加 ~1-5% |
| 首次查询空表（单个 tablet）| ~5-50ms（读 version 1 + 返回空结果）| +1-10ms（FE RPC）| 增加 ~10-50%（但绝对值仍在毫秒级）|
| 后续所有操作 | 无变化 | 无变化 | **零影响** |

DDL 延迟降低了 1-3 个数量级，首次 tablet 操作增加了 1-10ms（仅 FE RPC，无额外 S3 IO）。净收益极大。

注意：首次 publish 涉及多个 tablet（一个 partition 有 N 个 bucket），FE RPC 可通过 SingleFlight 按 (table_id, index_id) 合并，一次 RPC 返回 partition/index 级别的共享信息。因此批量首次 publish 的延迟增量接近单次 RPC 的水平而非 N 倍。**【推论，待 Phase 3 验证 SingleFlight 分组策略】**

### Q3：方案中有多少处"例外处理"？如果超过 3 处，方向是否需要重新审视？

**核心例外处理：1 处。**

唯一的例外处理是在 `get_tablet_metadata` 的 fallback 链末尾增加 lazy construction 逻辑。这是一个**统一的入口点**，覆盖所有下游消费者。

**不需要例外处理的路径**：
- 所有 16+ 条 Category 3 下游消费者 → 通过 `get_tablet_metadata` 统一覆盖
- FE 侧 3 条 CreateReplicaTask 发送路径 → 通过表级属性条件跳过（代码保留，供未启用 lazy construction 的表使用）
- Category 4 (FE 完成依赖) → 同上，条件跳过
- Category 5 (配置参数) → lazy construction 启用的表不再使用相关超时参数，但参数保留供未启用的表使用
- Category 7 (跨集群复制) → 通过 `get_tablet_metadata` 统一覆盖
- Category 8 (Cluster Snapshot) → 不涉及对象存储 tablet metadata → 无需变化

**1 处例外处理，远低于 3 处阈值。方向正确。**

### Q4：方案落地后系统是变简单了还是变复杂了？

**对启用 lazy construction 的表，DDL 路径显著变简单。整体系统在过渡期有双模式并存的复杂度，但长期趋向更简单。**

**保留的代码（过渡期双模式）**：
- FE 侧：`CreateReplicaTask` 构建/发送逻辑、`MarkedCountDownLatch` 等待、`AgentTaskQueue` 管理、`LeaderImpl.finishCreateReplica()` 回调——这些为未启用 lazy construction 的表继续服务
- CN 侧：`create_tablet` agent task handler——为上述表及升降级兼容继续服务
- 配置参数：`tablet_create_timeout_second` 等——为未启用的表继续服务

**新增的复杂度**：
- CN 侧：`get_tablet_metadata` 中 ~30-50 行 lazy construction fallback 逻辑
- FE 侧：1 个新 RPC handler（~100-150 行），组装 tablet 初始配置信息；`PhysicalPartition` / alter job 中新增 initialGtid 持久化字段
- FE 侧：表级属性控制逻辑 + DDL 路径的条件分支
- Thrift：1 个新 request/response 结构

**净效果**：
- 对新表（默认启用）：DDL 路径从 `FE → CN RPC → 等待 → 回调` 缩短为 `FE 直接完成`，用户体感 DDL 延迟大幅降低
- 系统新增约 ~200 行集中代码 + 表级属性，老代码保留供兼容
- 系统不变量放宽：从"DDL 完成时 version 1 metadata 必须存在于对象存储"变为"version 1 metadata 在首次需要时保证可构建"
- **长期**：当所有表都启用 lazy construction 后，`CreateReplicaTask` 相关代码可安全移除（~500+ 行）

**架构层面**：对启用的表，CN 不再承担 DDL 时的控制面职责，向纯粹的无状态计算节点更近了一步。
