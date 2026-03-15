# Phase 1: Plan — 去掉存算分离 Tablet 创建时的 CN 交互

## 一、文档验证结果

### 1.1 创建路径完整性（01-current-tablet-creation-paths.md）

**结论：文档准确，19 条路径和 3 条 CreateReplicaTask 发送路径均已验证。**

验证方法与结果：

| 验证维度 | 方法 | 结果 |
|----------|------|------|
| `StarOSAgent.createShards*()`/`createShardGroup*()` 生产代码调用方 | 全局搜索（排除测试代码） | 确认 9 个调用点，与文档一致 |
| `new LakeTablet(...)` 构造点 | 全局搜索 | 确认 5 个点：`LocalMetastore:2248`, `LakeTableAlterJobV2Builder:102`, `LakeTableRollupBuilder:111`, `SplitTabletJobFactory:288`, `MergeTabletJobFactory:372` |
| `LocalMetastore.buildPartitions()` 调用方 | 全局搜索 | 确认 7 个调用方（`OlapTableFactory` ×2, `addPartitions`, `addSubPartitions`, `buildNonPartitionOlapTable`, `truncateTable`, `createTempPartitionsFromPartitions`） |
| 3 条 CreateReplicaTask 路径 | 逐文件验证 | `TabletTaskExecutor:229`, `LakeTableSchemaChangeJob:465-502`, `LakeRollupJob:261-294` ——全部确认 |
| `LakeRestoreJob.sendCreateReplicaTasks()` 为 no-op | 阅读代码 | 确认（`LakeRestoreJob.java:112-116`，空方法体） |
| Split/Merge 不使用 CreateReplicaTask | 阅读代码 | 确认（使用 `StarOSAgent.createShardsForSplit/Merge` + BE 侧 `publish_resharding_tablet`） |

### 1.2 CN 执行逻辑（02-cn-interaction-details.md）

**结论：文档准确。**

- `TabletManager::create_tablet()` 的 4 件事在 `tablet_manager.cpp:209-274` 中全部验证：构建 TabletMetadataPB、创建 schema file（条件性）、写入对象存储、缓存。**【事实，依据：`tablet_manager.cpp:209-274`】**
- `convert_t_schema_to_pb_schema()` 确认只有 C++ 实现（`metadata_util.cpp:249-351`），FE 无 Java 等价物。**【事实】**
- `tablet_creation_optimization` 确认为 FE 配置（`Config.java:1062`），通过 request flag 传递给 BE。**【事实】**
- 回调处理 `LeaderImpl.finishCreateReplica()` 确认存在（`LeaderImpl.java:384-432`），lake tablet 只 countDown latch 不更新 replica info。**【事实】**

### 1.3 下游消费者（04-affected-code-paths.md Category 3）

**结论：文档基本准确，但有遗漏和一处不精确。**

**已验证的 13 条消费者**（行号因版本略有偏移，均确认存在且语义匹配）：

| # | 消费者 | 验证状态 | 备注 |
|---|--------|---------|------|
| 1 | Publish version | ✅ `transactions.cpp:251-253` | base_version=1 时致命 |
| 2 | Schema change | ✅ `schema_change.cpp:372-373` | 显式 version=1 |
| 3 | 查询执行 | ✅ `lake_connector.cpp:187-193`（在 `be/src/connector/`，非 `storage/lake/`） | visible=1 时致命 |
| 4 | Schema 解析 | ✅ `tablet_manager.cpp:1022-1035`（行号偏移） | 通过 version_hint 或 list |
| 5 | Tablet 重分片 | ✅ `tablet_reshard.cpp:879,1024,1191` | base 可能=1 |
| 6 | 跨集群复制 | ✅ `lake_replication_txn_manager.cpp:89,110,335`（多处行号偏移） | target version=1 |
| 7 | 主键表索引 | ⚠️ 间接消费者 | `lake_primary_index.cpp` 本身不直接调 `get_tablet_metadata`，metadata 由调用方传入（`update_manager.cpp:165`） |
| 8 | Compaction | ✅ `tablet_parallel_compaction_manager.cpp:331` | |
| 9 | Vacuum/GC | ✅ `vacuum.cpp:273,359,913`（多处位置） | |
| 10 | Version 1 fallback | ✅ `tablet_manager.cpp:539-548` | |
| 11 | Metadata listing | ✅ `tablet_manager.cpp:778-781` | |
| 12 | Meta reader | ✅ `lake_meta_reader.cpp:41-42`（在 `be/src/storage/`） | |
| 13 | Admin repair | ⚠️ 不精确 | `lake_service.cpp:1913-1918` 是**写入**操作（`put_tablet_metadata`），不是读取 version 1。FE 发送 metadata 给 BE 写入，BE 不从对象存储读取。应归类为"不受影响"或"需另行分析 FE 侧 repair 逻辑" |

**遗漏的消费者**（通过搜索所有 `get_tablet_metadata`/`get_tablet` 调用发现）：

| # | 消费者 | 文件 | 说明 |
|---|--------|------|------|
| 14 | Vacuum full | `vacuum_full.cpp:72` | 读取 version=kInitialVersion 的 metadata |
| 15 | Tablet retain info | `tablet_retain_info.cpp:27` | 遍历 retain_versions，可能包含 version 1 |
| 16 | Replication txn manager (非 lake 版) | `replication_txn_manager.cpp:63,183` | 使用 `get_tablet()` 和 `get_metadata(visible_version)` |
| 17 | Lake delvec loader | `lake_delvec_loader.cpp:49,51` | 按 version 加载 metadata |
| 18 | Update manager | `update_manager.cpp:1439` | 按 meta_ver 加载 metadata |

**【事实，依据：全局搜索 `get_tablet_metadata` 和 `get_tablet` 在 `be/src/storage/lake/` 及 `be/src/storage/` 下的调用】**

**影响评估**：新增的 5 条消费者中，#14 vacuum_full 和 #17 delvec_loader 可能在新 tablet 首次使用时被触发，严重程度为**中**；#15、#16、#18 大多在 tablet 已有数据后才会被调用，严重程度为**低到中**。

### 1.4 FE 完成依赖（04-affected-code-paths.md Category 4）

**结论：文档准确。**

| 验证项 | 结果 |
|--------|------|
| 4 个 MarkedCountDownLatch 点 | 全部确认 |
| Edit log 格式不含 CN 交互信息 | 确认（`CreateTableInfo` 只存 dbName/table/storageVolumeId，`AddPartitionsInfoV2` 只存 partition 元数据） |
| `LakeTableAsyncFastSchemaChangeJob` 使用 `TabletMetadataUpdateAgentTaskFactory` | 确认（`LakeTableAsyncFastSchemaChangeJob.java:124-126`）不走 CreateReplicaTask |
| ConsistencyChecker 包裹 buildPartitions | 确认（`LocalMetastore.java:2021-2036`） |

### 1.5 升降级兼容性（04-affected-code-paths.md Category 6）

**结论：分析准确，核心洞察正确。**

- "先升 CN 后升 FE"意味着当新 FE 开始跳过 CN 交互时，所有 CN 已为新版本——**正确**。**【事实】**
- Edit log 不含 CN 交互信息——**已验证**。
- 降级 Phase 2（FE 老 + CN 老）中新版本创建的 tablet 确实可能损坏——**推论正确**，因为老 CN 无处理缺失 metadata 的逻辑。

### 1.6 需要补充的内容

1. **Category 3 应增加 5 条遗漏的下游消费者**（见 1.3 节）
2. **#13 (Admin repair) 的分类应修正**：BE 侧是写入者不是读取者
3. **#7 (主键表索引) 应明确标注为间接消费者**，真正的直接消费者在 `update_manager.cpp`
4. **文件路径纠正**：`lake_connector.cpp` 在 `be/src/connector/` 而非 `be/src/storage/lake/`；`lake_meta_reader.cpp` 在 `be/src/storage/` 而非 `be/src/storage/lake/`

---

## 二、问题本质的理解

### 2.1 问题本质

存算分离模式下，tablet 创建的当前流程在对象存储上写入一个 version 1 的 `TabletMetadataPB` 文件作为 tablet 的"出生证明"。这个文件的内容本质上是：**空 tablet + schema + 少量配置参数**（没有 rowset，没有数据）。系统中所有后续操作（写入、查询、compaction、schema change 等）都以这个文件的存在作为隐式前提。

**问题的本质不是"谁来写这个文件"，而是"这个文件是否必须在 DDL 时存在"。**

当前的隐式契约是：DDL 完成 → 对象存储上存在 version 1 metadata。打破这个契约需要回答：系统在何时、如何获得使一个新 tablet 可用所需的信息？

### 2.2 核心挑战

1. **下游消费者的统一处理**：有 18+ 条代码路径依赖 version 1 metadata 的存在（文档 13 条 + 新发现 5 条），需要一个统一机制覆盖所有路径，而非逐条打补丁。**【这是设计标准的明确要求】**

2. **信息来源问题**：version 1 metadata 中的信息（schema、persistent_index 配置、compaction_strategy、flat_json_config、range 等）当前仅通过 `TCreateTabletReq` 从 FE 传递给 CN。如果去掉这个传递路径，下游需要从别的地方获得这些信息。

3. **升降级的自然兼容**：新 CN 必须同时处理有/无 version 1 metadata 的 tablet，且这种能力不能依赖额外的配置开关或运维步骤。

4. **首次操作的性能不可退化**：设计标准要求"核心读写路径的正确性和性能"优先级最高。首次写入（publish version）和首次查询不能因为缺少预写的 metadata 而显著变慢。

### 2.3 一个关键的架构观察

**所有下游消费者最终都通过 `TabletManager::get_tablet_metadata()` 访问 metadata。** 这个函数有多个重载，但最终都汇聚到 `get_tablet_metadata(path, cache_opts, ...)` 这个实现（`tablet_manager.cpp:514-558`）。现有的 `tablet_creation_optimization` 已经在此处（lines 539-548）实现了一个 fallback 机制：当特定 tablet 的 version 1 文件不存在时，尝试读取共享的 initial metadata 文件。

**【事实，依据：`tablet_manager.cpp:481-505` 的 `(tablet_id, version)` 重载 → 调用 path-based 重载 → lines 539-548 fallback】**

这意味着：如果能在 `get_tablet_metadata` 这个统一入口处解决"metadata 不存在"的问题，所有下游消费者都会自动受益。

### 2.4 另一个关键观察：TableSchemaService 已存在

BE 已有从 FE 获取 schema 的成熟机制：`TableSchemaService`（`table_schema_service.h/cpp`）。它支持 SingleFlight 请求合并、重试、缓存，被 LOAD 和 SCAN 路径使用。这意味着 CN 在运行时获取 schema 信息的通道已经建好。

**【事实，依据：`table_schema_service.cpp:77-141`，`get_schema_for_load/scan()` 方法】**

不过，TableSchemaService 目前只返回 `TabletSchemaPB`，不返回完整的 tablet-level 配置（persistent_index、compaction_strategy 等）。构建完整的 `TabletMetadataPB` 还需要这些信息。

**【事实，依据：`table_schema_service.cpp:336-344`，RPC 响应只包含 `TTabletSchema`】**

---

## 三、设计步骤规划

### Step 1：确定核心机制（第二阶段产出）

**目标**：确定一个统一的核心机制，解决"当 version 1 metadata 不存在时，系统如何让 tablet 可用"的问题。

关键决策点：
- 延迟物化（在首次需要时构建 metadata）vs. 提前物化（在 DDL 时由非 CN 组件写入）vs. 消除对初始 metadata 的需求
- 如果选择延迟物化：信息从哪里来、谁来执行、结果是否持久化
- 核心机制如何天然覆盖升降级场景

验收标准：用核心机制走通 4 个关键场景（首次写入、首次查询、schema change、跨集群复制）。

### Step 2：完整设计（第三阶段产出）

**目标**：基于核心机制，给出覆盖所有受影响路径的完整设计。

具体内容：
- FE 侧变更：哪些代码需要改、语义是什么
- CN/BE 侧变更：哪些代码需要改、语义是什么
- 8 个 Category 中每条路径的处理方式
- 升降级的每个阶段行为
- 配置和监控变更
- 精确到文件/函数级别

### Step 3：实现规划（如需要）

**目标**：给出分阶段实现的顺序和依赖关系，便于分 PR 推进。

---

## 四、初步方向思考

### 4.1 倾向的大方向：延迟物化（Lazy Materialization）

核心思路：不在 DDL 时写入 version 1 metadata，而是在 CN 首次需要某个 tablet 的 metadata 时，在 `get_tablet_metadata` 的统一入口处构建它。

为什么倾向这个方向：
- **统一性**：所有下游消费者都通过 `get_tablet_metadata` 访问 metadata，在此处加入 lazy materialization 逻辑可以一次覆盖全部消费者。符合设计标准"用一个统一机制处理所有受影响的路径"。
- **自然兼容性**：老 tablet 有 metadata → 正常读取；新 tablet 无 metadata → 触发 lazy materialization。CN 不需要区分老/新 tablet，行为自动适配。
- **系统变简单**：FE 侧删除 CreateReplicaTask 的构建和发送逻辑、latch 等待、AgentTaskQueue 管理、超时/重试；CN 侧减少一个 agent task handler。

关键待解问题：
- 构建完整 TabletMetadataPB 需要哪些信息、这些信息从哪里获取
- 是否需要新的 FE→CN RPC，或者能否复用/扩展现有的 TableSchemaService
- 物化的结果是否需要持久化到对象存储（以避免每次都重新构建）
- 首次操作的额外延迟是否可接受

### 4.2 备选方向

| 方向 | 简述 | 值得探索的原因 | 可能的问题 |
|------|------|--------------|-----------|
| **A: FE 直接写对象存储** | FE 在 DDL 时构建 TabletMetadataPB 并写入对象存储，完全跳过 CN | 消费者完全无感知，兼容性最好 | 设计标准明确排除"把 CN 的事搬到别处"；FE 需要对象存储写入能力和 C++ 的 schema 转换逻辑；本质是转移依赖而非消除 |
| **B: 消除对 version 1 的需求** | 修改所有消费者，使其不再假设 version 1 存在（如 publish 时直接创建 version 2） | 最彻底的解法 | 涉及 18+ 条路径的修改，每条都需要独立处理"没有 base version"的语义，本质上是逐条打补丁 |
| **C: StarManager 存储初始信息** | 在 StarManager 的 shard 元数据中存储 schema 和配置信息，CN 从 StarManager 获取 | 不需要 FE RPC | 增加了 StarManager 的职责和复杂度；StarManager 的 shard 元数据格式可能需要大改 |
| **D: 混合方案** | FE 只写 schema file 到对象存储（不写 metadata），CN 在首次需要时从 schema file + 确定性字段构建 metadata | 减少 FE 侧改动，schema 来源有保障 | FE 需要对象存储写入能力（但已有先例？需验证） |

### 4.3 对方向 A 的明确排除理由

设计标准写到：

> 不可接受的设计：把 CN 做的事原样搬到 FE 或其他组件——这不是去掉依赖，是转移依赖

方向 A 恰好落入此范畴。此外，它要求 FE 实现 `convert_t_schema_to_pb_schema` 的 Java 版本（或者 FE 直接写 protobuf 到对象存储），增加了 FE 的复杂度，且不解决"DDL 依赖外部组件可用性"的根本问题（改为依赖对象存储可用性，虽然对象存储通常更可靠）。

---

## 五、待讨论问题

### 5.1 需要确认的问题

1. **Tablet-level 配置信息的获取渠道**：构建完整的 `TabletMetadataPB` 除了 schema 外，还需要 `enable_persistent_index`、`persistent_index_type`、`compaction_strategy`、`flat_json_config`、`range`、`gtid` 等字段。现有的 `TableSchemaService` 只返回 schema。**问题：是否接受扩展 `TableSchemaService`（或新增 RPC）以返回这些信息？还是说有其他获取渠道？**

2. **物化结果是否持久化**：lazy materialization 构建的 metadata 是否写回对象存储？
   - 写回：后续访问不需要重复构建，性能更好；但增加了首次操作的写入延迟
   - 不写回：每次都从 FE 获取信息构建；更简单但性能可能不可接受
   - **我倾向于写回**，因为 metadata 后续在 publish、compaction 等路径中会被更新（版本递增），所以只需要构建一次

3. **范围确认：是否只针对存算分离内表？** 文档说"存算分离内表"，排除了外表。**确认：虚拟 tablet（`SharedDataStorageVolumeMgr` 的 `getOrCreateVirtualTabletId`）是否在 scope 内？** 虚拟 tablet 用于跨集群复制访问源集群存储，非标准数据 tablet。**【我倾向于排除虚拟 tablet，因为它的用途不同且不发 CreateReplicaTask】**

4. **Restore 路径**：`LakeRestoreJob` 的 `sendCreateReplicaTasks()` 已经是 no-op，但它通过 `LakeSnapshotLoader` 独立路径写入 metadata。这条路径是否在 scope 内？**【推论：不在 scope 内，因为 Restore 路径不涉及 CreateReplicaTask，且它有自己的 metadata 写入逻辑】**

### 5.2 识别到的关键 tradeoff

1. **首次操作延迟 vs. DDL 简洁性**

   延迟物化将 metadata 创建的开销从 DDL 时转移到首次操作时。对于 DDL（建表、加分区等）来说是纯收益。但对首次写入/查询来说，需要额外执行：(a) 发现 metadata 不存在 → (b) 从 FE 获取信息 → (c) 构建 metadata → (d) 可能写回对象存储。

   **关键问题**：这个额外延迟有多大？(b) 的 FE RPC 是主要开销，但 `TableSchemaService` 已有 SingleFlight 机制来合并并发请求。如果同一 partition 的多个 tablet 同时首次使用，RPC 可以合并。

   **我的判断**：首次操作增加约 1 次 FE RPC 的延迟（~1-10ms 量级，取决于网络），相比于对象存储写入延迟（~10-100ms）是小开销。且这个开销只发生一次（持久化后不再触发），可以接受。**【推论，需第二阶段验证】**

2. **统一机制的覆盖范围 vs. 例外情况**

   `get_tablet_metadata` 是大多数消费者的统一入口，但有些路径可能绕过它（如 `list_tablet_metadata`、`get_latest_cached_tablet_metadata`）。需要在第二阶段仔细验证是否所有消费者都能被统一机制覆盖。

   当前已知可能需要例外处理的路径：
   - **Metadata listing**（`list_tablet_metadata`）：当前已特殊处理 initial metadata（`tablet_manager.cpp:778-781`）
   - **Vacuum**：遍历版本时可能从高版本递减到 version 1
   - **Schema 解析**：有独立的 fallback 链（schema cache → schema file → FE RPC → tablet metadata）

   **如果例外超过 3 处，需要重新审视方向**——这是设计标准的要求。

3. **与现有 `tablet_creation_optimization` 的关系**

   `lake_enable_tablet_creation_optimization`（当前默认 OFF）已实现了一种"共享 initial metadata"机制。新方案与此机制的关系需要明确：
   - 替代？（新方案彻底取代 optimization）
   - 共存？（optimization 成为渐进路径）
   - 进化？（optimization 是新方案的前身，共享 initial metadata 文件的思路可复用）

   **我倾向于新方案完全替代** optimization，因为 optimization 仍然需要 CN 交互（只是减少了交互次数），而新方案的目标是完全消除 CN 交互。

---

## 六、验证工作台帐（Verification Ledger）

以下记录本阶段所有验证行为，标注事实/推论/未知：

| 编号 | 验证项 | 类型 | 依据/说明 |
|------|--------|------|----------|
| V1 | 19 条创建路径完整 | 事实 | 全局搜索 `createShards*`, `new LakeTablet`, `buildPartitions` 调用方 |
| V2 | 3 条 CreateReplicaTask 发送路径完整 | 事实 | 全局搜索 `CreateReplicaTask` 构造 + lake tablet 路径 |
| V3 | `create_tablet()` 做 4 件事 | 事实 | `tablet_manager.cpp:209-274` |
| V4 | `convert_t_schema_to_pb_schema` 仅 C++ | 事实 | `metadata_util.cpp:249-351`，FE 无等价物 |
| V5 | 下游消费者有 18+ 条（非 13 条） | 事实 | 全局搜索 `get_tablet_metadata`/`get_tablet` 调用 |
| V6 | Edit log 不含 CN 交互信息 | 事实 | `CreateTableInfo.java`, `AddPartitionsInfoV2.java` |
| V7 | 所有 `get_tablet_metadata(id, version)` 最终汇聚到 path-based 实现 | 事实 | `tablet_manager.cpp:481-505` → `514-558` |
| V8 | `TableSchemaService` 已存在且只返回 schema | 事实 | `table_schema_service.h/cpp` |
| V9 | 升级规范"先升 CN 后升 FE" | 事实 | `04-affected-code-paths.md` Category 6 |
| V10 | 首次操作额外延迟约 1 次 FE RPC | 推论 | 基于 V8 和 TableSchemaService 的 SingleFlight 机制 |
| V11 | 所有消费者都通过 `get_tablet_metadata` 访问 | 推论，需进一步验证 | 大部分已确认，但 listing、vacuum 等路径可能有例外 |
| V12 | 构建完整 TabletMetadataPB 需要 schema 之外的配置信息 | 事实 | `TabletMetadataPB` proto 定义 `lake_types.proto:186-228` |
| V13 | 获取 schema 之外配置信息的 RPC 渠道 | 未知 | 需第二阶段设计时确定 |
