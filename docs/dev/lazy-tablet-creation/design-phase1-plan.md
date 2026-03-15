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

**验证后的完整直接消费者清单**（行号因版本略有偏移，均确认存在且语义匹配）：

| # | 消费者 | 验证状态 | 备注 |
|---|--------|---------|------|
| 1 | Publish version | ✅ `transactions.cpp:251-253` | base_version=1 时致命 |
| 2 | Schema change | ✅ `schema_change.cpp:372-373` | 显式 version=1 |
| 3 | 查询执行 | ✅ `lake_connector.cpp:187-193`（在 `be/src/connector/`，非 `storage/lake/`） | visible=1 时致命 |
| 4 | Schema 解析 | ✅ `tablet_manager.cpp:1022-1035`（行号偏移） | 通过 version_hint 或 list |
| 5 | Tablet 重分片 | ✅ `tablet_reshard.cpp:879,1024,1191` | base 可能=1 |
| 6 | 跨集群复制（lake-to-lake） | ✅ `lake_replication_txn_manager.cpp:89,110,335`（多处行号偏移） | target version=1 |
| 7 | 跨集群复制（snapshot-based） | ✅ `replication_txn_manager.cpp:63,182` | `ReplicationTxnManager` 是 snapshot-based 跨集群复制路径，与 #6 的 `LakeReplicationTxnManager`（direct lake-to-lake）同属 `starrocks::lake` namespace，前者包含后者（`_lake_replication_txn_manager` 成员），line 182 `tablet.get_metadata(request.visible_version)` 可读 version 1 |
| 8 | Compaction | ✅ `tablet_parallel_compaction_manager.cpp:331` | |
| 9 | Vacuum/GC | ✅ `vacuum.cpp:273,359,913`（多处位置） | |
| 10 | Version 1 fallback | ✅ `tablet_manager.cpp:539-548` | |
| 11 | Metadata listing | ✅ `tablet_manager.cpp:778-781` | |
| 12 | Meta reader | ✅ `lake_meta_reader.cpp:41-42`（在 `be/src/storage/`） | |
| 13 | Vacuum full | ✅ `vacuum_full.cpp:72` | 读取 version=kInitialVersion 的 metadata |
| 14 | Tablet retain info | ✅ `tablet_retain_info.cpp:27` | 遍历 retain_versions，可能包含 version 1 |
| 15 | Lake delvec loader | ✅ `lake_delvec_loader.cpp:49,51` | 按 version 加载 metadata |
| 16 | Update manager | ✅ `update_manager.cpp:1439` | 按 meta_ver 加载 metadata，也是主键表索引加载的直接调用方（`update_manager.cpp:165` 传 metadata 给 `lake_primary_index.cpp`） |

**【事实，依据：全局搜索 `get_tablet_metadata` 和 `get_tablet` 在 `be/src/storage/lake/` 及 `be/src/storage/` 下的调用】**

**与原文档的差异**：
- **去掉原 #7（主键表索引）**：`lake_primary_index.cpp` 不直接调 `get_tablet_metadata`，metadata 由调用方传入。真正的直接消费者是 `update_manager.cpp`（现 #16）
- **去掉原 #13（Admin repair）**：`lake_service.cpp:1913-1918` 是**写入**操作（`put_tablet_metadata`），不是读取 version 1，不受影响
- **新增 #7（snapshot-based 跨集群复制）**、**#13-#16（vacuum_full、tablet_retain_info、delvec_loader、update_manager）**

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

1. **Category 3 下游消费者已重新整理**：去掉了间接消费者（主键表索引）和不受影响的路径（Admin repair），增加了遗漏的直接消费者，整理为 16 条完整清单（见 1.3 节）
2. **文件路径纠正**：`lake_connector.cpp` 在 `be/src/connector/` 而非 `be/src/storage/lake/`；`lake_meta_reader.cpp` 在 `be/src/storage/` 而非 `be/src/storage/lake/`

---

## 二、问题本质的理解

### 2.1 问题本质

存算分离模式下，tablet 创建的当前流程在对象存储上写入一个 version 1 的 `TabletMetadataPB` 文件作为 tablet 的"出生证明"。这个文件的内容本质上是：**空 tablet + schema + 少量配置参数**（没有 rowset，没有数据）。系统中所有后续操作（写入、查询、compaction、schema change 等）都以这个文件的存在作为隐式前提。

**问题的本质不是"谁来写这个文件"，而是"这个文件是否必须在 DDL 时存在"。**

当前的隐式契约是：DDL 完成 → 对象存储上存在 version 1 metadata。打破这个契约需要回答：系统在何时、如何获得使一个新 tablet 可用所需的信息？

### 2.2 核心挑战

1. **下游消费者的统一处理**：有 16 条已知直接消费者代码路径依赖 version 1 metadata 的存在，需要一个统一机制覆盖所有路径，而非逐条打补丁。**【这是设计标准的明确要求】**

2. **信息来源问题**：version 1 metadata 中的信息（schema、persistent_index 配置、compaction_strategy、flat_json_config、range 等）当前仅通过 `TCreateTabletReq` 从 FE 传递给 CN。如果去掉这个传递路径，下游需要从别的地方获得这些信息。

3. **升降级的自然兼容**：新 CN 必须同时处理有/无 version 1 metadata 的 tablet，且这种能力不能依赖额外的配置开关或运维步骤。**特别需要考虑：没有 version 1 metadata 的新表在降级到老版本后如何工作**——老 CN 不具备处理缺失 metadata 的能力，方案需要对此有明确的策略（接受降级后新表不可用、提供修复工具、或方案本身天然解决）。

4. **首次操作的性能不可退化**：设计标准要求"核心读写路径的正确性和性能"优先级最高。首次写入（publish version）和首次查询不能因为缺少预写的 metadata 而显著变慢。

5. **面向未来的扩展性**：
   - **新代码路径**：未来的新功能可能会引入新的 version 1 metadata 消费者。方案需要保证新代码不需要额外适配，即新增的 metadata 读取代码自然能工作，而非要求每个新功能开发者都记得处理"metadata 可能不存在"的情况。
   - **新 metadata 字段**：`TabletMetadataPB` 未来可能增加新字段。方案需要保证新字段能被正确纳入 lazy materialization 流程，而非每加一个字段就需要改一次 lazy materialization 逻辑。

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

**目标**：对比延迟物化（Lazy Materialization）和消除对 version 1 的需求两个方向，选定最终方向并确定具体的核心机制。

关键决策点：
- 两个方向在关键场景下的行为对比和优劣
- 配置信息获取渠道（开放探索）
- 如何保证未来新代码路径和新 metadata 字段的自然兼容

验收标准：用核心机制走通 4 个关键场景（首次写入、首次查询、schema change、跨集群复制），并验证升降级每个阶段的行为。

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

## 四、方向思考

### 4.1 首选方向：延迟物化（Lazy Materialization）

核心思路：不在 DDL 时写入 version 1 metadata，而是在 CN 首次需要某个 tablet 的 metadata 时，在 `get_tablet_metadata` 的统一入口处构建它。

倾向这个方向的理由：
- **统一性**：所有下游消费者都通过 `get_tablet_metadata` 访问 metadata，在此处加入 lazy materialization 逻辑可以一次覆盖全部消费者。符合设计标准"用一个统一机制处理所有受影响的路径"。
- **自然兼容性**：老 tablet 有 metadata → 正常读取；新 tablet 无 metadata → 触发 lazy materialization。CN 不需要区分老/新 tablet，行为自动适配。
- **系统变简单**：FE 侧删除 CreateReplicaTask 的构建和发送逻辑、latch 等待、AgentTaskQueue 管理、超时/重试；CN 侧减少一个 agent task handler。
- **面向未来**：新增的 metadata 读取代码自然走 `get_tablet_metadata`，不需要额外适配。

### 4.2 需对比的备选方向：消除对 version 1 的需求

核心思路：修改所有下游消费者，使其不再假设 version 1 metadata 存在。例如 publish 时直接创建 version 2（无需 base version），查询空表时不读取 metadata 等。

这个方向值得在第二阶段探索对比的原因：
- **最彻底**：从根本上消除"初始 metadata"的概念，系统中不再有"version 1 是特殊的"这一假设
- **无运行时开销**：不需要在首次操作时执行额外的 RPC 或构建逻辑
- **无降级问题**：如果消费者本身不依赖 version 1，降级后也不会因缺失 metadata 而出问题

当前的顾虑（需在第二阶段验证是否成立）：
- 涉及 16 条消费者路径的修改，可能本质上是逐条打补丁
- 每个新功能开发者需记住"不能假设 version 1 存在"的约束
- 部分路径（如 publish version 读 base_version）的语义改动可能较复杂

**第二阶段的任务**：将两个方向各走通关键场景（首次写入、首次查询、schema change、跨集群复制），比较优劣后选定。

### 4.3 排除的备选方向

| 方向 | 简述 | 排除理由 |
|------|------|---------|
| **FE 直接写对象存储** | FE 在 DDL 时构建 TabletMetadataPB 并写入 | 设计标准明确排除"转移依赖"；FE 需要 schema 转换逻辑和对象存储写入能力 |
| **StarManager 存储初始信息** | CN 从 StarManager 获取 schema 和配置 | 增加 StarManager 职责和复杂度，shard 元数据格式需要大改 |

---

## 五、已确认的约束和待解问题

### 5.1 已确认的约束

1. **范围**：仅针对存算分离内表。虚拟 tablet（`SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId`）不在 scope 内。Restore 路径不在 scope 内（`LakeRestoreJob.sendCreateReplicaTasks()` 已是 no-op，实际不支持 lake restore）。

2. **替代 `tablet_creation_optimization`**：新方案完全替代 `lake_enable_tablet_creation_optimization`。该优化仍需 CN 交互（只是减少次数），而新方案的目标是完全消除 CN 交互。落地后该配置参数及其相关的 shared initial metadata 逻辑可废弃。

3. **物化结果不持久化到对象存储**：lazy materialization 构建的 version 1 metadata 放入内存 cache，不写回对象存储。理由：version 1 metadata 在首次导入（publish version 2）后即被新版本替代，对象存储上只需要有 version 2+ 的 metadata。首次导入的 publish 流程本身会写出新版本的 metadata，所以 version 1 只需在内存中存在足够短的时间即可。

### 5.2 待解问题

1. **Tablet-level 配置信息的获取渠道**（开放问题）

   构建完整的 `TabletMetadataPB` 除了 schema 外，还需要 `enable_persistent_index`、`persistent_index_type`、`compaction_strategy`、`flat_json_config`、`range`、`gtid` 等字段。现有的 `TableSchemaService` 只返回 schema。

   需要在第二阶段探索的可能方案：
   - **扩展 FE RPC**：扩展 `TableSchemaService`（或新增 RPC）以返回完整的 tablet 初始化信息（schema + 配置），CN 在 lazy materialization 时调用
   - **首次写入路径传入配置**：FE 在发起首次写入（如 publish version）时，将配置信息作为请求参数传入，CN 用这些信息构建 metadata
   - **利用已有信息推导**：部分字段可能有确定性的默认值或可从其他已有信息源（如 schema 本身、shard properties）推导
   - 其他可能的渠道

   对比维度：对已有代码路径的侵入性、是否所有消费者（不仅是写入）都能覆盖、扩展性（新字段加入时的改动量）。这个问题的答案也可能因最终选定的方向（延迟物化 vs 消除对 version 1 的需求）而不同。

2. **首次操作延迟评估**

   延迟物化将 metadata 构建开销从 DDL 时转移到首次操作时。需要评估额外延迟的量级：
   - 如果用 FE RPC 获取信息，主要开销是 1 次 RPC（~1-10ms）
   - `TableSchemaService` 的 SingleFlight 机制可合并同 partition 多 tablet 的并发请求
   - 不写回对象存储，省去了对象存储写入延迟
   **【推论，需第二阶段验证】**

3. **统一机制的覆盖范围**

   `get_tablet_metadata` 是大多数消费者的统一入口，但有些路径可能绕过它（如 `list_tablet_metadata`、`get_latest_cached_tablet_metadata`）。需要在第二阶段验证是否所有 16 条消费者都能被统一机制覆盖。

   当前已知可能需要例外处理的路径：
   - **Metadata listing**（`list_tablet_metadata`）：当前已特殊处理 initial metadata（`tablet_manager.cpp:778-781`）
   - **Vacuum**：遍历版本时可能从高版本递减到 version 1
   - **Schema 解析**：有独立的 fallback 链（schema cache → schema file → FE RPC → tablet metadata）

   **如果例外超过 3 处，需要重新审视方向**——这是设计标准的要求。

4. **降级后新表的可用性**

   新版本期间创建的表没有 version 1 metadata。降级到老版本后，老 CN 无法处理缺失 metadata 的 tablet。需要在第二阶段明确策略：
   - 是否接受"降级后新表不可用，老表正常"——这取决于降级的 SLA 承诺
   - 是否需要在降级前提供修复工具（补写 version 1 metadata）——这与"不引入人工运维步骤"的设计标准存在张力
   - 方案本身是否能天然解决（如 lazy materialization 逻辑在新 CN 上运行时可以补写给老 CN 用）

---

## 六、验证工作台帐（Verification Ledger）

以下记录本阶段所有验证行为，标注事实/推论/未知：

| 编号 | 验证项 | 类型 | 依据/说明 |
|------|--------|------|----------|
| V1 | 19 条创建路径完整 | 事实 | 全局搜索 `createShards*`, `new LakeTablet`, `buildPartitions` 调用方 |
| V2 | 3 条 CreateReplicaTask 发送路径完整 | 事实 | 全局搜索 `CreateReplicaTask` 构造 + lake tablet 路径 |
| V3 | `create_tablet()` 做 4 件事 | 事实 | `tablet_manager.cpp:209-274` |
| V4 | `convert_t_schema_to_pb_schema` 仅 C++ | 事实 | `metadata_util.cpp:249-351`，FE 无等价物 |
| V5 | 下游直接消费者有 16 条 | 事实 | 全局搜索 `get_tablet_metadata`/`get_tablet` 调用，去除间接消费者和不受影响的路径 |
| V6 | Edit log 不含 CN 交互信息 | 事实 | `CreateTableInfo.java`, `AddPartitionsInfoV2.java` |
| V7 | 所有 `get_tablet_metadata(id, version)` 最终汇聚到 path-based 实现 | 事实 | `tablet_manager.cpp:481-505` → `514-558` |
| V8 | `TableSchemaService` 已存在且只返回 schema | 事实 | `table_schema_service.h/cpp` |
| V9 | 升级规范"先升 CN 后升 FE" | 事实 | `04-affected-code-paths.md` Category 6 |
| V10 | `ReplicationTxnManager` 和 `LakeReplicationTxnManager` 同属 `starrocks::lake` namespace | 事实 | `replication_txn_manager.h:56` `namespace starrocks::lake`，前者持有后者为成员（`_lake_replication_txn_manager`），前者是 snapshot-based 路径，后者是 direct lake-to-lake 路径 |
| V11 | 首次操作额外延迟约 1 次 FE RPC | 推论 | 基于 V8 和 TableSchemaService 的 SingleFlight 机制 |
| V12 | 所有消费者都通过 `get_tablet_metadata` 访问 | 推论，需进一步验证 | 大部分已确认，但 listing、vacuum 等路径可能有例外 |
| V13 | 构建完整 TabletMetadataPB 需要 schema 之外的配置信息 | 事实 | `TabletMetadataPB` proto 定义 `lake_types.proto:186-228` |
| V14 | 获取 schema 之外配置信息的渠道 | 未知 | 需第二阶段对比扩展 FE RPC vs 首次写入传入配置 |
