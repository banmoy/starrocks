# \[Proposal\] Point-in-time Query and Change Data Capture

---

## 1\. 摘要

**问题**：StarRocks 计划在存算分离内表上支持增量物化视图（IVM）。IVM 增量刷新依赖两项底层能力——**历史版本/时间点查询（Point-in-time Query, PITQ）** 和 **变更数据捕获（Change Data Capture, CDC）**。当前系统不具备这两项能力。

**长期产品价值**：PITQ 和 CDC 不仅服务于 IVM，也是 Time Travel 查询、流式计算、数据同步、合规审计等场景的基础能力。Snowflake、Databricks、Iceberg、BigQuery 等系统均已具备。支撑这些能力是 StarRocks 作为分析型数据库长期竞争力的一部分。

**本文目标**：PITQ 和 CDC 共同依赖表级 MVCC 能力，本文围绕 MVCC、PITQ、CDC 三条线，从 IVM 最小需求出发设计方案，并制定从 IVM 专用到通用能力的分阶段演进 roadmap。

---

## 2\. 动机

### 2.1 直接驱动：IVM 对底层能力的需求

增量物化视图（IVM）的核心目标是：当基表发生变更时，只计算 MV 需要更新的部分，避免全量重算。一次增量刷新需要以下输入：

- **Base version 数据**：MV 上次刷新时对应的基表数据快照  
- **Head version 数据**：基表当前最新版本的数据快照  
- **两个版本之间的变更（Delta）**：从 base 到 head 之间发生的行级数据变更

前两者需要**历史版本查询（PITQ）**，第三项需要**变更数据捕获（CDC）**。当前 StarRocks 存算分离内表只能读取表的最新状态，无法回到某个历史版本，也无法捕获两个版本之间的差异。

### 2.2 长期产品价值

PITQ 和 CDC 并非仅为 IVM 设计的临时能力，它们可以支撑多种高价值场景：

- **PITQ**：Time Travel（误操作恢复、报表复现、审计取证）、AI/ML 数据版本管理  
- **CDC**：流式计算对接（Flink / Spark Structured Streaming）、数据同步、合规审计、构建 SCD Type 2（拉链表）

**业界对标**：主流数据库和数据湖系统均已具备这两项能力。

| 系统 | 历史版本查询 | 变更数据捕获 |
| :---- | :---- | :---- |
| Snowflake | Time Travel（AT / BEFORE 语法，企业版可以保留 90 天） | Streams Object / CHANGES clause |
| Databricks (Delta Lake) | Time Travel（TIMESTAMP / VERSION AS OF，默认 7 天） | Change Data Feed (CDF) |
| Apache Iceberg | Snapshot-based Time Travel \+ Branch/Tag | Spark Procedures `create_changelog_view` |
| BigQuery | Time Travel (`FOR SYSTEM_TIME AS OF`，默认 7 天） | APPENDS / CHANGES 函数 |

StarRocks 补全这些基础能力，是构建长期产品竞争力的必要条件。

---

## 3\. 需求分析

本章分析 PITQ 和 CDC 在 IVM 与通用场景下的需求差异，明确 IVM 需要的能力边界。这些需求边界是后续所有方案设计的基础。

### 3.1 历史版本查询（PITQ）需求

Time Travel 是 PITQ 最完整的产品形态——以下 Time Travel 需求来自业界产品的调研总结（详见附录 A）。IVM 对 PITQ 的需求是 Time Travel 需求的子集：

| 维度 | IVM | Time Travel |
| :---- | :---- | :---- |
| **目标** | 查询数据内容 | 查询 \+ 恢复（含元数据） |
| **覆盖的操作** | DML。分区级 DDL 和 INSERT OVERWRITE 可低优支持，遇到降级全量刷新 | DML \+ 影响表结构和数据内容的 DDL (DROP/TRUNCATE PARTITION, Schema Change 等) |
| **Schema 语义** | 使用最新 schema | 语义上应使用历史 schema（业界实践不一） |
| **用户接口** | Java API（Analyze 阶段构建 Plan） | SQL（`FOR TIMESTAMP/VERSION AS OF`） |
| **保留时间** | 短窗口，取决于 MV refresh interval；best-effort，版本不可用时降级全量刷新 | 固定时间窗口，通常天级；必须严格遵循 |
| **保留版本** | 仅 MV 上次刷新对应的基表版本 | 时间窗口内所有版本 |

IVM 对 PITQ 的需求相比 Time Travel 显著简化：覆盖核心 DML 即可、不保留历史 schema、只需 Java API、可使用更轻量的保留策略。

### 3.2 变更数据捕获（CDC）需求

#### 3.2.1 基本概念

**变更类型（Change Type）**：每条变更携带一个类型标记——`INSERT`（新插入）、`DELETE`（被删除）、`UPDATE_BEFORE`（更新前的数据）、`UPDATE_AFTER`（更新后的数据）。

**Update 语义的两种表示**：一次 UPDATE 可以表示为 `UPDATE_BEFORE + UPDATE_AFTER`（保留完整更新语义），也可以表示为 `DELETE + INSERT`（更轻量，无法与"先删再插"区分）。例如将某行 `val` 从 10 改为 20：前者产出 `UPDATE_BEFORE(val=10)` \+ `UPDATE_AFTER(val=20)`；后者产出 `DELETE(val=10)` \+ `INSERT(val=20)`。

**Net Changes（净变更）**：将多个版本内同一行（按 ROW\_ID 标识）的变更合并为最小等价集。例如一行先 INSERT 再 UPDATE 再 DELETE，净效果是 0 条变更。

**顺序（Ordering）**：涉及三个层面——版本间是否按先后消费、同版本内行间是否有序、配对变更是否保证相邻。

#### 3.2.2 IVM 与通用 CDC 的需求差异

| 维度 | IVM | 通用 CDC（流计算、审计等） |
| :---- | :---- | :---- |
| **本质需求** | Net Changes，不需要中间变更 | 逐条行级变更，可能需要完整中间过程 |
| **覆盖的操作** | DML。分区级 DDL 和 INSERT OVERWRITE 可低优支持 | DML \+ 部分 DDL (DROP/TRUNCATE PARTITION 等) |
| **Update 语义** | DELETE \+ INSERT 即可 | 需要 UPDATE\_BEFORE \+ UPDATE\_AFTER（参见附录 E） |
| **消费粒度** | 以版本为粒度批量消费 | 可按版本或按 record 粒度 |
| **顺序保证** | 不需要（批处理，中间结果不可见） | 部分场景需严格有序 |
| **消费接口** | Java API | SQL 或 SDK（对接 Flink/Spark） |

#### 3.2.3 支持的表类型与操作范围

不同表类型对 CDC 的支持能力取决于表模型的存储特性：

| 表类型 | 支持的操作 | 变更类型 | 说明 |
| :---- | :---- | :---- | :---- |
| **明细表** | DML：仅 append。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT、DELETE（DDL 产生） | DELETE 操作生成变更成本高（需全表扫描），暂不支持 |
| **主键表** | DML：INSERT INTO、INSERT OVERWRITE、DELETE、UPDATE、各类 LOAD。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT、DELETE、UPDATE\_BEFORE、UPDATE\_AFTER | 能力最完整 |
| **聚合表** | DML：仅 append。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT（aggregate 语义）、DELETE（DDL 产生） | 增量 rowset 存储的是 aggregate 后的结果，可直接作为变更 |
| **更新表** | 不支持 | — | — |

**聚合表 CDC 在 IVM 中的使用示例**（来自 Applovin 实际场景）：

```
基表 (agg table): key1, key2, key3, val1 SUM
异步 MV: SELECT key1, key2, SUM(val1) GROUP BY 1, 2

导入数据：(1,1,1,1) (1,1,1,2) (2,2,2,1) (3,3,3,1) (2,2,2,2)
CHANGES：(1,1,1,3) (2,2,2,1) (3,3,3,1) (2,2,2,2)
// k=(1,1,1) 的两条导入在 rowset 中已聚合为 val1=3
// 底层原理：直接读取导入生成的增量 rowset，rowset 存储的就是部分 aggregate 后的结果
```

### 3.3 IVM 最小需求边界总结

本节总结的需求边界是后续第 4-6 章方案设计的基线。短期方案均围绕这些约束做最小化设计。

**PITQ**：

- 覆盖 DML（除 INSERT OVERWRITE）  
- 使用最新 schema，不需要保留历史 schema  
- Java API，不依赖 SQL 接口  
- 短窗口保留，MV 刷新完成后即可释放旧版本引用  
- 版本不可用时可降级全量刷新

**CDC**：

- 覆盖的操作同 PITQ  
- Update 使用 DELETE \+ INSERT 语义  
- 需要 Net Changes  
- 不需要保序  
- 以版本为粒度批量消费  
- Java API

**低优支持**（遇到时降级全量刷新）：DROP / TRUNCATE PARTITION、INSERT OVERWRITE。

---

## 4\. PITQ 和 CDC 的共同基础：MVCC 方案设计

PITQ 需要"读取表的某个历史版本"，CDC 需要"比较两个版本之间的差异"——两者都依赖系统对表的 MVCC 能力。MVCC 能力可以分为三部分：

- **多版本数据结构**：系统是否记录并保留了历史版本的元数据和数据，使得历史状态可被访问  
- **版本标识（Version Identity）**：如何唯一标识一个版本，使系统能精确引用"表在某个时间点的状态"  
- **版本保留（Retention）**：保留哪些版本，保留多久，如何清理

本章先定义基本概念并分析现状差距，然后分别给出面向 IVM 的短期方案和长期演进方向。

### 4.1 基本概念与现状

#### 4.1.1 Table State 与 Table Version

引入 Table State 和 Table Version 概念，当前系统中没有对应实现。

**Table State（表状态）** 是执行一次查询所需的最小信息集合，由两层构成：

- **Table Meta**：FE 管理并持久化的元数据，包括表定义（schema、分区、分布、索引等）、数据分片拓扑（Logical Partition → Physical Partition → Tablet 三级层次）和各 Physical Partition 的数据版本号（visible version，即该分区当前可读的最新版本号，单调递增）。查询规划阶段依赖这些信息进行 SQL 解析、分区裁剪与执行计划生成。  
- **Table Data**：对象存储上的 tablet metadata 文件（记录每个 tablet 在某个版本下包含哪些数据文件）和数据文件本身。查询执行阶段根据 Meta 中指定的版本号，从对象存储读取对应的数据。

**Table Version（表版本）** 标识表在某个时间点的完整状态。理论上，Table Version 映射为 Table Meta 的版本 \+ 各 Physical Partition 的 visible version。只要 Table State 的任何组件发生变化（DML、DDL 或系统操作），都应产生一个新的 Table Version。

为理解 Table Version 需要覆盖的范围，以下分类列出可能改变 Table State 的操作：

| 操作类型 | 示例 | 影响的 State 组件 |
| :---- | :---- | :---- |
| **DML** | INSERT INTO, DELETE, UPDATE, 各类 LOAD | 数据 |
| **DML（特殊）** | INSERT OVERWRITE | 数据分片拓扑 \+ 数据（新分区替换旧分区） |
| **DDL（不改拓扑）** | Fast Schema Change（加减列） | 表定义 |
| **DDL（改拓扑）** | DROP/TRUNCATE/MERGE PARTITION, 非 Fast Schema Change, 修改分桶数 | 表定义 \+ 数据分片拓扑 \+ 数据 |
| **系统操作** | Compaction, Tablet Reshard | 数据分片拓扑（逻辑等价，不改变查询结果） |

完整的操作分类详见附录 B。

#### 4.1.2 现状与差距

以下按多版本数据结构、版本标识、版本保留三个方向，对照 Time Travel、通用 CDC 等完整场景的需求，分析当前系统的能力和差距。

**多版本数据结构**

Table Data 已具备多版本——每个 Physical Partition 维护独立的 visible version，tablet metadata 按版本存储在对象存储上，通过指定 visible version 可以读取该版本对应的数据文件集合。但 Table Meta 不支持多版本——`OlapTable` 中的表定义、分片拓扑等元数据发生变更后直接原地更新，历史状态被覆盖。

由此产生两个差距：(1) 没有**表级版本数据结构**——Physical Partition visible version 是分区粒度的，而 PITQ 和 CDC 需要表级的一致性视图（所有分区在同一逻辑时间点的版本快照）；(2) Table Meta 无历史版本——schema change、drop partition 等操作后，历史版本依赖的 meta 不存在了。

差距 (2) 不是 PITQ/CDC 独有的问题。此前多个 feature 也遇到过元数据多版本需求（如 Fast Schema Change v2 将历史 schema 保存在 QueryPlan 和 SchemaChangeJob 中，Tablet Reshard 为用到的 materialized index 设计了多版本），但都是局部分散的，缺乏统一方案。

**版本标识**

系统中已有两个相关机制：Physical Partition visible version（分区级数据版本号，单调递增）和 GTID（Global Transaction ID，集群级单调递增的 64-bit 事务标识，内嵌毫秒级时间戳，可在 timestamp 和 GTID 之间转换）。但两者都无法直接作为 Table Version 的标识——前者是分区粒度不是表粒度，后者仅覆盖 DML 事务，无法标识 DDL 和系统操作产生的版本。

**版本保留**

当前有三套独立的清理机制：

- **Vacuum**（Auto vacuum / Full vacuum）：清理对象存储上不再需要的旧版本 tablet metadata 和数据文件，保留窗口由 `lake_autovacuum_grace_period_minutes` 等参数控制，主要保护正在执行中的查询（分钟级）  
- **CatalogRecycleBin**（FE 内的回收站机制）：清理 DROP / TRUNCATE PARTITION 产生的旧分区及其下属 Tablet，旧分区先暂存于回收站支持 RECOVER 恢复，过期后触发物理清理（删除对象存储数据文件和 StarManager 中的 Shard 元数据），保留时间由 `catalog_trash_expire_second` 控制（默认 1 天）  
- **StarMgrMetaSyncer**（FE 与 StarManager 之间的兜底对账机制）：定期比对两端元数据，清理 FE 中已不存在但 StarManager 中仍残留的 Shard 和数据文件，Tablet Reshard 依赖该机制

三套机制覆盖不同场景，但都缺少按"Table Version"保留的语义，也没有表级配置。此外，如果 Retention 时间长且 DML/DDL 频率高，FE 内存中保存所有历史元数据会面临显著的内存压力。

### 4.2 短期方案：面向 IVM 的最小 MVCC

IVM 的需求特点（详见 3.3）使得上述差距可以被绕过：

- IVM 只覆盖 DML，不需要支持 DDL 和系统操作 → **绕过 Table Meta 多版本**（不需要保留历史 schema 和数据分片拓扑，遇到拓扑变更直接断链降级）  
- IVM 只需 DML 版本标识 → **绕过 GTID 覆盖范围不完整的问题**（复用现有 GTID 即可）  
- IVM 保留窗口短且只保留 MV 引用的版本 → **绕过 FE 内存压力和长周期保留**（保留的版本数量与 MV 数量相关而非与 DML 频率相关，全内存可行）

基于这些简化，短期方案在三个维度的设计如下：

**多版本数据结构**（Table Meta 侧，Table Data 已有多版本无需改造）：

- 每个版本只需记录 `{ppId -> visibleVersion}` 映射，无需完整的 Meta 快照（当前按分区刷新的 MV 实际也会存储该映射）  
- 数据分片拓扑变更（DDL、reshard 等）触发断链，IVM 降级全量刷新

**版本标识**：

- `OlapTable` 增加一个表级的 `versionId` 字段（以 GTID 表示），每次 DML publish 后更新  
- 面向用户使用 timestamp，系统内部转换为 GTID，例如：  
  - "查询 t1 时刻的数据"：`max{gtid | timestamp(gtid) ≤ t1}`  
  - "查询 t1 到 t2 之间的变更"：`{gtid | t1 < timestamp(gtid) ≤ t2}`  
- IVM 直接记录 GTID 表示基表刷新的版本 (与当前 Iceberg IVM 兼容)

**版本保留**：

- 采用 **MV 订阅模式**而非固定时间窗口——只保留 MV 引用的版本，刷新成功后旧版本即可释放  
- **Table Meta**：只保留 MV 引用的 base version 的 `{ppId -> visibleVersion}` 映射，用于 PITQ 读取 base 快照。中间版本的 Meta 不保留——CDC 不需要中间版本的完整 Meta，只需要知道每个 Physical Partition 从 base 到 head 的 visible version 区间，这可以利用 visible version 连续递增的性质从 base 和 head 两个端点直接推导  
- **Table Data**：保留 base 到 head 之间所有中间版本的 tablet metadata 和数据文件，通过 `minRetainVersion` 阻止 Vacuum 清理。CDC 根据推导出的版本区间从这些文件中生成行级变更  
- 这种模式的代价是**牺牲了对任意版本的 PITQ 和 CDC**——只能查询 MV 引用的 base version 快照和 base→head 区间的变更，无法查询中间任意版本。这对 IVM 足够，通用 Time Travel 和 CDC 需要长期方案  
- P0 不支持 drop/truncate partition 后的历史查询，因此不需要修改 CatalogRecycleBin 和 StarMgrMetaSyncer；P1 支持时再适配

**示例**：假设表有两个 Physical Partition（pp1, pp2），MV 上次刷新后记录了 base version，之后经历了 3 次 DML：

```
base version:  {pp1: v3, pp2: v5}   ← MV 上次刷新记录的映射
     DML-1:    pp1 v3→v4
     DML-2:    pp2 v5→v6
     DML-3:    pp1 v4→v5
head version:  {pp1: v5, pp2: v6}   ← 当前最新
```

下次 IVM 刷新时：

- **PITQ** 读取 base version 数据（pp1@v3、pp2@v5），依赖 Meta 中保留的映射  
- **CDC** 读取 base 到 head 之间的变更（pp1 的 v3→v4→v5、pp2 的 v5→v6），依赖 Data 中保留的中间版本文件  
- 刷新成功后，head 成为新的 base，旧 base 映射可以释放，pp1 的 v3\~v4、pp2 的 v5 也可以被 Vacuum 清理

具体的数据结构和 Java API 设计参见附录 D。

### 4.3 长期方向：完整表级 MVCC

短期方案通过限定 IVM 场景绕过了主要挑战，但不支持任意历史版本查询和通用 CDC。要完整支持 Time Travel 等通用场景，需要补齐：

- **Table Meta 多版本**：保留历史表定义（schema）和历史数据分片拓扑，支持按历史 schema 查询  
- **GTID 扩展**：覆盖 DDL 和系统操作（每个操作分配一个 GTID），使 Table Version 标识能描述所有状态变更  
- **FE 内存压力与冷存储**：当前 FE 元数据全部驻留内存。天级保留窗口下，高频 DML/DDL 产生的历史版本元数据会带来显著的内存压力。多版本数据结构的设计需要同步考虑冷存储方案  
- **Vacuum/GC 策略**：引入表级 Retention 配置，协调 Vacuum、CatalogRecycleBin、StarMgrMetaSyncer 三套清理机制与版本保留的关系

上述改造涉及 FE 元数据管理、存储层 Vacuum、StarManager 对账等多个模块的协同，复杂度高，预计需要较长的落地周期，可在短期方案上渐进扩展。

### 4.4 小结

| 维度 | 短期（IVM 最小 MVCC） | 长期（完整表级 MVCC） |
| :---- | :---- | :---- |
| **多版本数据结构** | Table Meta 只记录 `{ppId -> visibleVersion}` 映射，不保留历史 schema 和数据分片拓扑；拓扑变更断链降级 | 完整的 Table Meta 多版本（历史 schema、数据分片拓扑），支持所有 DML \+ DDL |
| **版本标识** | 复用 GTID（仅覆盖 DML） | GTID 扩展覆盖 DDL 和系统操作 |
| **版本保留** | MV 订阅模式，只保留被引用的版本；Meta 保留端点，Data 保留中间版本 | 表级 Retention 配置，固定时间窗口保留所有版本 |
| **PITQ 能力** | 仅 MV 引用的 base version | 任意历史版本 |
| **CDC 能力** | 仅 MV base→head 区间 | 任意两个版本之间 |
| **复杂度** | 低，不涉及 Meta MVCC、RecycleBin/StarMgrMetaSyncer 改造 | 高，涉及多模块协同改造 |
| **落地周期** | 短期可交付 | 需要较长的落地周期 |

---

## 5\. PITQ：方案设计

第 4 章定义了 MVCC 机制（如何记录和保留版本）。本章在此基础上讨论 PITQ 的用户接口和查询流程——即如何利用 MVCC 能力完成历史版本查询。

### 5.1 用户接口

与 StarRocks 当前查询 Iceberg 表历史版本的语法一致，支持按 timestamp 和 version（GTID）两种方式指定：

```sql
-- 按 timestamp 查询
SELECT ... FROM t FOR TIMESTAMP AS OF '2026-02-21 10:30:00';

-- 按 version（GTID）查询
SELECT ... FROM t FOR VERSION AS OF 10;
```

- IVM 通过 Java API 在 Analyze 阶段注入版本信息构造 scan plan，不经过 SQL 层  
- Time Travel 等场景面向用户暴露上述 SQL 接口

### 5.2 查询流程

PITQ 的查询流程与当前查询流程的主要区别在于 FE 查询规划阶段——需要根据目标版本替换各分区的 visible version：

1. **版本定位**（新增）：FE 根据指定的 timestamp 或 GTID，定位目标 Table Version，获取该版本下各 Physical Partition 的 visible version 映射  
2. **构建历史查询表**（新增）：基于当前表的元数据，用目标版本的 visible version 覆盖各 Physical Partition 的版本号，移除目标版本中不存在的 partition。如果发现断链（partition 已被 drop 或 tablet 已 reshard），返回错误  
3. **按版本 scan**（修改）：规划 scan range 时，对每个 Physical Partition 使用目标版本的 visible version 而非当前最新版本  
4. **后续流程不变**：CN 按指定 version 从对象存储读取对应的 tablet metadata 和数据文件，执行 scan 并返回结果

简言之，PITQ 只改变了"读哪个版本"，CN 侧的读取和计算逻辑不需要修改。

### 5.3 MVCC 长短期方案下的差异

PITQ 的查询流程在短期和长期 MVCC 方案下**基本一致**——都是定位版本 → 构建历史 OlapTable → 按版本 scan。差异主要在 MVCC 层（第 4 章已讨论），对 PITQ 查询流程本身影响不大：

- **短期**：只能查询 MV 订阅的 DML 版本，使用最新 schema  
- **长期**：可查询任意历史版本（包括 DDL），使用历史 schema

---

## 6\. CDC：方案设计

CDC 的目标是捕获两个版本之间的行级数据变更。变更类型和 Net Changes 等基本概念已在 3.2.1 中定义，支持的表类型与操作范围详见 3.2.3。

### 6.1 CHANGES 数据格式

每条行级变更由**数据列**（与表的列一致，可只包含需要的列）和**元数据列**组成：

| 元数据列 | 类型 | 含义 |
| :---- | :---- | :---- |
| CHANGE\_TYPE | TINYINT | 变更类型编码：INSERT(0) / DELETE(1) / UPDATE\_BEFORE(2) / UPDATE\_AFTER(3) |
| ROW\_ID | BIGINT | 逻辑行标识，同一行的所有变更具有相同的 ROW\_ID |
| ROW\_VERSION | BIGINT | 产生变更的版本，配对的 UPDATE\_BEFORE / UPDATE\_AFTER 具有相同的 ROW\_VERSION |

**Row Tracking** 是存储层为每行数据维护的元信息，是生成上述元数据列的前提能力：

- **ROW\_ID**：逻辑行唯一标识。INSERT 时分配全局唯一值，UPDATE 后保持不变，DELETE 后不再复用。Net Changes 依赖 ROW\_ID 做同行变更合并  
- **ROW\_VERSION**：行版本。INSERT 时生成初始版本，UPDATE 后版本递增（不一定连续）。Update 配对和 Net Changes 排序依赖 ROW\_VERSION

Row Tracking 的具体生成方案、唯一性保证、存储格式等在单独文档中设计，本文假设存储层已具备此能力。

### 6.2 用户查询接口

**CHANGES 语法**——手动指定 offset，查询任意范围的 CHANGES，适用于 ad-hoc 查询和 debug 排查：

```sql
-- Option 1: CHANGES clause, 参考 SnowFlake, Spark
-- https://docs.snowflake.com/en/sql-reference/constructs/changes
-- https://issues.apache.org/jira/browse/SPARK-55668
SELECT * FROM tbl CHANGES FROM VERSION v1 TO v2;
SELECT * FROM tbl CHANGES FROM TIMESTAMP t1 TO t2;

-- Option 2: Table Function, 参考 BigQuery
-- https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/time-series-functions#changes
SELECT * FROM table_changes('tbl', t1, t2);
SELECT * FROM table_changes('tbl', v1, v2);
```

**STREAM 对象**——自动管理消费进度，用于命令式构建增量 ETL pipeline，通常配合 Task 调度框架定期触发。相比 IVM 声明式方式，STREAM 更复杂但也更灵活，适合 IVM 无法表达的复杂增量逻辑（参考 [Snowflake STREAM](https://docs.snowflake.com/en/sql-reference/sql/create-stream)）：

```sql
CREATE STREAM my_stream ON tbl;

-- 查询 offset 到最新版本之间的 CHANGES
SELECT * FROM my_stream;

-- DML 中消费 stream，执行成功后自动推进 offset
INSERT INTO target_tbl SELECT * FROM my_stream;
```

**SDK / RPC**——对接 Flink / Spark Structured Streaming，客户端通过 SDK 与 StarRocks 交互。

### 6.3 能力范围与约束

CDC 引擎本身按通用能力设计——Update 语义、Net Changes、排序都通过参数配置适配不同场景，不针对 IVM 做特殊裁剪。实际的能力边界来自 MVCC：短期 MVCC（4.2）只覆盖 DML，CDC 也只覆盖 DML；长期 MVCC（4.3）支持后，CDC 能力自然扩展。

接口上，先支持 CHANGES 语法——它是所有消费方式的基础；STREAM 对象和 SDK/RPC 后续按需支持。

| 维度 | 能力范围 |
| :---- | :---- |
| **操作类型** | INSERT / UPDATE / DELETE / 各类 LOAD；不含 INSERT OVERWRITE 和 DDL（受限于短期 MVCC） |
| **表类型** | 明细表（INSERT）、主键表（INSERT / DELETE / UPDATE\_BEFORE / UPDATE\_AFTER）、聚合表（INSERT，aggregate 后语义）；更新表不支持 |
| **消费模式** | 批量，以 version 为粒度 |
| **消费接口** | CHANGES 语法；STREAM 对象和 SDK/RPC 暂不支持 |
| **Update 语义** | 两种模式都支持，参数切换 |
| **Net Changes** | 可选能力，参数配置 |
| **排序** | 可选能力，参数配置 |

### 6.4 端到端流程

CDC 的链路分为 FE 和 CN 两个阶段：

```
┌─────────────────────────────────────────────────────────────┐
│ FE: 确定"读什么"                                           │
│   对比 old/new TableState，计算每个 tablet 的                │
│   version range，封装为 scan range 下发给 CN                │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ CN: 执行"怎么读"                                           │
│   每个 tablet 根据 version range 生成行级 changes，          │
│   可选经过 Net Changes 合并后输出                            │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
                    消费端处理 Changes
```

**FE 要解决的问题**：如何从两个 TableState 的 diff 中推导出每个 tablet 需要读取的版本区间，以及如何处理 partition 新增/删除、tablet reshard 等拓扑变化。

**CN 要解决的问题**：如何从 tablet 的版本区间中高效生成行级 changes。明细表和聚合表只有 append，直接读 delta rowset（导入产生的增量数据文件，包含一次导入写入的所有行）即可；**主键表是核心难点**——需要定位哪些行是 INSERT / DELETE / UPDATE，并从历史 segment 中读取旧值（DELETE 和 UPDATE\_BEFORE），列式存储下这意味着随机 IO。

### 6.5 FE 侧：确定每个 Tablet 的变更版本区间

FE 的任务是从两个 TableState（old 和 new）中推导出"哪些 tablet 有变更、每个 tablet 需要读哪个版本范围"，封装为 scan range 下发给 CN。

按 logical partition → physical partition 逐层比较 old 和 new 两个 TableState 中各 physical partition 的 visible version。version 相同则跳过，不同则该 partition 下所有 tablet 需要读取 `(oldVisibleVersion, newVisibleVersion]` 区间的变更。新增的 partition 从 version 0 开始读取；被删除的 partition 在短期方案中触发 fallback 全量刷新。

**示例**：一张 3 个 partition 的表，经历 2 次导入：

```
初始状态（oldState, versionId=100）:
  P1: PP1(visibleVersion=5)  → tablets [T1, T2]
  P2: PP2(visibleVersion=3)  → tablets [T3, T4]
  P3: PP3(visibleVersion=7)  → tablets [T5, T6]

第 1 次导入: 写入 P1 和 P2
第 2 次导入: 写入 P1

当前状态（newState, versionId=102）:
  P1: PP1(visibleVersion=8)  → tablets [T1, T2]    // 两次导入，version 5→8
  P2: PP2(visibleVersion=4)  → tablets [T3, T4]    // 一次导入，version 3→4
  P3: PP3(visibleVersion=7)  → tablets [T5, T6]    // 无变化
```

| Tablet | Version Range | 说明 |
| :---- | :---- | :---- |
| T1, T2 | (5, 8\] | P1 有变更，覆盖两次导入 |
| T3, T4 | (3, 4\] | P2 有变更，覆盖一次导入 |
| T5, T6 | 跳过 | P3 无变化 |

**Tablet Reshard**：如果 physical partition 在版本区间内发生了 reshard（系统自动调整分片数），同一个 physical partition 会存在多组 tablet，每组覆盖部分版本区间。FE 将两组版本区间都下发，CN 分别读取后在计算层合并：

```
PP1 在 version 6 发生 reshard:
  旧 tablets [T1, T2], 覆盖 version (5, 6]
  新 tablets [T1', T2'], 覆盖 version (6, 8]
```

### 6.6 CN 侧：从版本区间生成行级 Changes

#### 6.6.1 概述：两条互补的路径

CN 侧从 tablet 的版本区间中生成行级 changes，有两条互补的路径：

- **Delta Replay**：逐版本读取每次导入产生的增量文件（delta rowset），提取每次导入的变更，可选做 Net Changes 合并。  
- **Snapshot Diff**：不看中间过程，直接比较 old 和 new 两个版本的文件集合快照，用集合差运算得出 Net Changes。

|  | Delta Replay | Snapshot Diff |
| :---- | :---- | :---- |
| 输出 | 完整 changes，可进一步合并成 Net Changes | 仅 Net Changes |
| IO 模式 | 逐版本读取 | 只 scan 两个快照间差异文件 |
| 适合场景 | CDC 窗口短、delta 文件少 | CDC 窗口长、delta 文件多但 compaction 充分 |
| 适用表模型 | 所有 | 明细表、主键表 |

#### 6.6.2 Delta Replay

##### 6.6.2.1 明细表与聚合表

明细表和聚合表只有 append 操作，读取 `(V_old, V_new]` 范围内的 delta rowset，所有行标记为 INSERT。聚合表的 delta rowset 存储的是 aggregate 后的结果，CHANGES 也是聚合后的语义。

**如何找到 delta rowset**：从 tablet metadata 中找到版本范围内所有导入产生的 delta rowset。需要注意：compaction 产生的 rowset 只是合并不包含新数据，需要跳过；compaction 会合并旧 rowset，被合并掉的 delta rowset 需要回溯更早版本的 tablet metadata 查找；遇到 DELETE 操作产生的 rowset，返回特殊错误码给上层。

**示例**：查询 `(2, 6]` 范围的 changes

```
version 3: tablet metadata 包含 [rowset-2(v2, LOAD), rowset-3(v3, LOAD)]
version 4: tablet metadata 包含 [rowset-2(v2, LOAD), rowset-3(v3, LOAD), rowset-4(v4, LOAD)]
version 5: tablet metadata 包含 [rowset-5(v5, COMPACTION)]
           ← v5 发生 compaction，rowset-2 ~ rowset-4 被合并为 rowset-5
version 6: tablet metadata 包含 [rowset-5(v5, COMPACTION), rowset-6(v6, LOAD)]
```

从 `V_new=6` 的 tablet metadata 开始：rowset-6(LOAD) 在范围内读取；rowset-5(COMPACTION) 跳过；回溯到 version 4 的 tablet metadata，找到 rowset-3 和 rowset-4 读取。最终读取 rowset-3、rowset-4、rowset-6，所有行标记为 INSERT。

##### 6.6.2.2 主键表

主键表支持 INSERT / UPDATE / DELETE，是 Changes 生成的核心难点。需要解决两个子问题：**定位**（如何知道哪些行是 INSERT / DELETE / UPDATE）和**取值**（DELETE 和 UPDATE 的旧值散落在历史 segment 中，如何高效读取）。

三个候选方案的对比：

|  | 方案 A：导入时生成 Changelog | 方案 B：Delete Vector Diff (短期只考虑 IVM) | 方案 C：Changes Vector（长期推荐） |
| :---- | :---- | :---- | :---- |
| **思路** | PK Index 更新时读旧值，配对写入独立的 changelog 文件 | 查询时定位并读取旧值：delta rowset 中的行是新增或更新后的行，其它 segment 比较导入前后两个版本的 delete vector，新增被标记删除的行（差集）就是被删除或被更新的行 | 导入时在 PK Index 更新阶段记录 RoaringBitmap 标记每行 change type，和 delete vector 存在一起；查询时据此精确定位旧 segment 是 DELETE 还是 UPDATE\_BEFORE，新 segment 是 INSERT 还是 UPDATE\_AFTER |
| **导入开销** | 高（读取并持久化旧值） | 无 | 低（记录 bitmap） |
| **查询效率** | 高（顺序读 changelog） | 中（可合并多个导入对同一 segment 的读取，减少随机 IO） | 中（同样可跨版本合并同一 segment 的读取，减少随机 IO） |
| **能否区分 DELETE/UPDATE** | 能 | 不能——delete vector 只记录"行被标记删除"，要区分就必须拿被标记删除的行去新 segment 做 PK 匹配，复杂且代价高 | 能，无需额外 PK 匹配 |
| **额外存储** | changelog 文件（高频导入面临小文件问题；存储所有列的值，而查询可能只需部分列；Net Changes 场景下中间版本 changelog 最终被合并，提前生成是浪费） | 无 | 与 delete vector 存在一起，开销极小（变更信息自包含，不依赖跨 version diff） |

选择方案 C 的理由：(1) 导入性能优先，对导入影响极小；(2) 能区分 DELETE 和 UPDATE；(3) 查询可以跨版本合并同一 segment 的读取，减少随机 IO；(4) 实现复杂度低。

**Changes Vector 设计**

导入时 PK Index 更新过程中已经知道每行是 INSERT / UPDATE / DELETE，在这个时机记录三个轻量 bitmap（changes vector），查询时据此精确定位每行的变更类型和位置：

| Changes Vector | 记录位置 | 含义 |
| :---- | :---- | :---- |
| **delete\_type\_vector** | 旧 segment | 被 DELETE 操作删除的行（区别于 UPDATE 导致的删除） |
| **before\_type\_vector** | 旧 segment | 被 UPDATE 的行（旧值位置） |
| **after\_type\_vector** | 新 segment | UPDATE 结果写入的行（新值位置），其余行为 INSERT |

三者的关系：旧 segment 的 `delete vector diff` \= `delete_type_vector` ∪ `before_type_vector`（互斥）；新 segment 中的行 \= INSERT 行 ∪ UPDATE\_AFTER 行（由 `after_type_vector` 区分）。

**示例**：一次导入的 changes vector 生成

```
导入前（version 5）:
  Segment S0: rows [A=1, B=2, C=3, D=4]    delete_vector = {}

导入操作:
  UPDATE A SET val=10    (A 原值 1)
  DELETE B               (B 原值 2)
  INSERT E val=5         (新行)

导入后（version 6）:
  S0: delete_vector={A,B}, delete_type_vector[v6]={B}, before_type_vector[v6]={A}
  S1(新): rows [A=10, E=5], after_type_vector[v6]={A}
```

查询 version 6 的 changes 时：读 S0 中被 `delete_type_vector` 和 `before_type_vector` 标记的行（A 和 B 一起读取），B → DELETE，A → UPDATE\_BEFORE；读 S1 全量顺序读取，A 在 `after_type_vector` 中 → UPDATE\_AFTER，E 不在 → INSERT。

**跨版本合并优化**：CDC 窗口覆盖多个版本时，同一旧 segment 可能被多个版本的 changes vector 引用。将同一 segment 上所有版本的 bitmap 合并（OR）后一次批量读取，再按原始 bitmap 拆分回各版本的 changes，将多次独立 IO 降为一次。

```
CDC 窗口 (5, 8]，S0 被三个版本引用：
  version 6: before_type_vector = {A}
  version 7: delete_type_vector = {C}
  version 8: before_type_vector = {D}

逐版本读取 → 3 次独立 IO
合并后读取 → merged = {A} ∪ {C} ∪ {D} = {A, C, D}，1 次批量读取
           → 再按原始 vector 拆分：A → v6 UPDATE_BEFORE, C → v7 DELETE, D → v8 UPDATE_BEFORE
```

##### 6.6.2.3 Net Changes

**动机**

对于主键表，当 CDC 窗口覆盖多个版本时，同一行可能有多条变更。Net Changes 将同一 `row_id` 下的多条变更合并为最小等价变更，减少下游处理量。

以 IVM 为例，表 A(id, score) 与表 B(id, name) 做 JOIN，MV 定义为 `SELECT id, name, score FROM A JOIN B ON A.id = B.id`。如果 A 的 id=1 行经历两次 UPDATE（score: 1→2→3）：

```
不合并（原始 changes）:
  A changes: -(1, 1), +(1, 2), -(1, 2), +(1, 3)           ← 4 条
  IVM: 每条 change 都需 join B，4 次 MV 更新

Net Changes 合并后:
  A changes: -(1, 1), +(1, 3)                               ← 2 条
  IVM: 2 次 MV 更新，结果等价
```

###### 

合并规则

对每个 `row_id`，根据 `row_version` 确定最早变更类型（first\_type）和最晚变更类型（last\_type），然后按规则合并。变更类型编码：`0` \= INSERT，`1` \= DELETE，`2` \= UPDATE\_BEFORE，`3` \= UPDATE\_AFTER。

**规则 1（单条变更）**：若某个 `row_id` 下只有一条变更（INSERT 或 DELETE），原样输出。

**规则 2-5（多条变更合并）**：

| \# | first\_type | last\_type | 输出 | 语义 |
| :---- | :---- | :---- | :---- | :---- |
| 2 | INSERT (0) | UPDATE\_AFTER (3) | 1 条 INSERT（最终值） | 新建后被更新，等价于直接以最终值插入 |
| 3 | INSERT (0) | DELETE (1) | 0 条 | 新建后被删除，变更相互抵消 |
| 4 | UPDATE\_BEFORE (2) | UPDATE\_AFTER (3) | 2 条：BEFORE（原始值）+ AFTER（最终值） | 多次更新合并为一次 |
| 5 | UPDATE\_BEFORE (2) | DELETE (1) | 1 条 DELETE（原始值） | 先更新后删除，等价于直接删除 |

规则 4 和 5 中，输出记录的 `row_version` 统一使用 `max_ver`，确保配对的 UPDATE\_BEFORE / UPDATE\_AFTER 具有相同版本。

**实现**

Net Changes 的实现分为两层——存储层做廉价快筛，计算层做精确兜底：

- **存储层（per-segment XOR）**：对同一个 segment，将 CDC 窗口内所有版本的 changes vector 合并为两个 bitmap——**enter**（INSERT ∪ after\_type\_vector，即"进入"该 segment 的行）和 **leave**（delete\_type\_vector ∪ before\_type\_vector，即"离开"该 segment 的行）。对两者做 XOR：同时出现在 enter 和 leave 中的行是可抵消的中间态，从 changes vector 中移除，后续不再读取。  
    
- **计算层（窗口函数兜底）**：Compaction 会将多个 segment 合并为新 segment，导致某些行的"进入"和"离开"分散在不同 segment 上，存储层的 segment 内 XOR 无法发现这些跨 segment 的配对。计算层通过窗口函数对所有 segment 输出的 changes 做全局 Net Changes 合并，利用表按 `ROW_ID` 分桶的特性避免全局 shuffle（`PARTITION BY row_id` 与分桶键一致，可本地执行）。

存储层以极低成本（bitmap 运算）减少大部分数据量，计算层保证无论是否发生 compaction 结果都正确。跨 segment 的抵消本质上是按 `row_id` 做 group by——这是一个聚合操作，放在计算层而非存储层是因为存储层的执行模型是 per-segment 并行扫描，引入全局聚合会破坏并行性并重复实现计算层已有的能力。

完整的三次导入推演（覆盖全部 4 种 change type、含 compaction 对比）详见附录 F。SQL 实现见附录 C。

#### 6.6.3 Snapshot Diff

Snapshot Diff 是与 Delta Replay 互补的中期路径，适用于高频导入 \+ 长 CDC 窗口的场景。

Delta Replay 逐版本读取 delta rowset。当 CDC 窗口内累积了大量小文件时（如每秒导入 1 次，MV 每小时刷新 1 次 → 3,600 个 delta rowset），文件打开和元数据解析的固定开销累积起来远大于有效数据量。

Snapshot Diff 换一种策略：不逐版本读取，直接比较 old 和 new 两个版本的文件集合快照，做集合差运算。两个版本共有的文件直接跳过（内容完全一样），只读 old-only 文件（标记为 DELETE 侧）和 new-only 文件（标记为 INSERT 侧）。IO 效率提升来自两点：文件打开次数从数千个小文件降为少量 compaction 后的大文件；有效 IO 量与实际变更量成正比而非与导入次数成正比。

```
Delta Replay（逐文件）:
  old ──→ [delta_v1] [delta_v2] ... [delta_v3600] ──→ new
          逐个读取 3,600 个小文件

Snapshot Diff:
  old snapshot: {S1, S2, S3}
  new snapshot: {S2, S3, S_merged}       ← S_merged 合并了 S1 + delta 文件
  共有文件 {S2, S3} → 跳过
  old-only: {S1} → DELETE 侧,  new-only: {S_merged} → INSERT 侧
```

**Carry-over Row 问题**：compaction 重新组织了文件布局，某些从未修改的行会因为从 old 的 S1 搬迁到 new 的 S\_merged 而被误判为变更。解决方式：利用计算层 anti-join 消除 carry-over row——同一 `(row_id, row_version)` 在两侧都出现说明该行未变化，anti-join 天然将其排除。表按 `ROW_ID` 分桶，anti-join 可走 colocated join，本地执行零 shuffle：

```sql
-- carry-over row 过滤（概念性 SQL）
SELECT 'DELETE' AS change_type, o.*
FROM TableChangesScan(side='old') o
LEFT ANTI JOIN TableChangesScan(side='new') n
  ON o.row_id = n.row_id AND o.row_version = n.row_version
UNION ALL
SELECT 'INSERT' AS change_type, n.*
FROM TableChangesScan(side='new') n
LEFT ANTI JOIN TableChangesScan(side='old') o
  ON n.row_id = o.row_id AND n.row_version = o.row_version
```

执行层可进一步将双 anti-join 融合为单遍 symmetric diff。

**适用范围与局限**：

| 条件 | 说明 |
| :---- | :---- |
| **适用表模型** | 明细表、主键表（compaction 不改变行内容） |
| **不适用** | 聚合表（compaction 会 aggregate 行，无法还原原始变更） |
| **前提条件** | 只需 Net Changes、Update 使用 DELETE \+ INSERT 模式（适合 IVM） |
| **compaction 要求** | old 和 new 版本都需经过充分 compaction 才能发挥效果 |

#### 6.6.4 并行 Scan

CDC scan 在三个层面支持并行，前述多项设计选择为此提供了基础：

| 并行层面 | 并行方式 | 关键设计支撑 |
| :---- | :---- | :---- |
| **Tablet 间** | FE 将不同 tablet 的 scan range 分配到不同 CN 节点，tablet 之间完全独立 | FE 按 tablet 粒度下发独立 scan range（6.5） |
| **Segment 间** | 同一 tablet 内多个 segment 可并行读取。Changes Vector 自包含于 segment，不需要跨 segment 做 PK 匹配 | Changes Vector 自包含设计；跨版本 bitmap 合并减少旧 segment 调度次数；存储层 XOR 是 per-segment 操作 |
| **Segment 内** | 同一 segment 按行范围切分并行读取，RoaringBitmap 支持按行范围做子集查询 | 列式存储天然支持；无需全局协调 |

此外，Net Changes 的计算层窗口函数利用表按 `ROW_ID` 分桶的特性，可本地执行，无需跨节点 shuffle。

### 6.7 增量数据统计信息

IVM 在决定刷新策略时需要知道增量数据的规模决定刷新模式。统计信息获取方式有两种：

- **统计信息查询**：从 tablet metadata 收集（删除行数、新增行数、文件数等）

```sql
-- 查询两个版本之间的增量统计信息（示意）
SELECT partition_id, added_rows, deleted_rows, delta_files, delta_bytes
FROM table_changes_stats('tbl', v1, v2);
```

- **导入结果附带**：需要考虑 FE 如何存储

为了简化，可以先用查询方式

### 6.8 小结

CDC 方案围绕"FE 确定读什么、CN 执行怎么读"的两阶段链路展开。

**数据格式**：数据列 \+ 三个元数据列（CHANGE\_TYPE / ROW\_ID / ROW\_VERSION），依赖存储层 Row Tracking 能力。

**FE 侧**：逐分区比较 old/new TableState 的 visible version，推导每个 tablet 的版本区间下发给 CN。利用 visible version 连续递增的性质不需要中间版本 Meta；Tablet Reshard 场景下发多组版本区间。

**CN 侧**：两条互补路径。

| 路径 | 适用场景 | 核心机制 |
| :---- | :---- | :---- |
| **Delta Replay** | CDC 窗口短、delta 文件少 | 逐版本读取 delta rowset 生成变更；明细表/聚合表直接标记 INSERT；主键表通过 Changes Vector 定位变更类型和旧值位置 |
| **Snapshot Diff** | CDC 窗口长、delta 文件多但 compaction 充分 | 比较两版本快照文件差异，只 scan 差异文件；anti-join 消除 carry-over row |

**并行能力**：Tablet 间、Segment 间、Segment 内三级并行。

**其它**：增量统计信息供 IVM 决定刷新策略；接口上先支持 CHANGES 语法，IVM 通过 Java API 直接构建 Plan；CDC 引擎按通用能力设计，当前受限于短期 MVCC 仅覆盖 DML，MVCC 扩展后能力自然扩展。

---

## 7\. Roadmap

### Phase 1：IVM 最小集

**目标**：以最小改动支撑 IVM 增量刷新，快速交付。

**MVCC**：

| 模块 | 范围 | 优先级 |
| :---- | :---- | :---- |
| **Table Version** | 基于 GTID，仅记录 DML。`OlapTable` 增加表级 `versionId`，MV 订阅模式管理版本生命周期 | P0 |
| **Vacuum 协同** | `minRetainVersion` 保护窗口内版本的 tablet metadata 和数据文件不被清理 | P0 |
| **断链降级** | partition drop/truncate、tablet reshard、INSERT OVERWRITE 等拓扑变更触发断链，IVM 回退全量刷新 | P0 |

**PITQ**：

| 模块 | 范围 | 优先级 |
| :---- | :---- | :---- |
| **历史版本查询** | 仅 MV 引用的版本可查；使用最新 schema；提供 SQL API 用来 用于 debug 和问题排查 | P0 |

**CDC**：

| 模块 | 范围 | 优先级 |
| :---- | :---- | :---- |
| **Row Tracking** | 存储层为每行维护 ROW\_ID \+ ROW\_VERSION，支撑 CDC 元数据列和 Net Changes 合并 **注：Delta Replay 方案，可以不用考虑 COMPACTION 后 ROW\_VRESION 继承** | P0 |
| **Delta Replay** | 明细表/聚合表读取 delta rowset；主键表通过 Delete vector diff 精确定位变更类型和旧值位置；Net Changes 通过存储层 XOR 快筛 \+ 计算层窗口函数兜底合并多版本变更 | P0 |
| **增量统计** | 增量数据统计信息收集 | P0 |
| **CHANGES 语法** | 基础 CDC SQL 接口，用于 debug 和问题排查 | P1 |

**假设与约束**：

- 仅针对存算分离（Cloud-Native）表  
- 不涉及已删除 Tablet 的保留  
- 不修改现有 CatalogRecycleBin 和 StarMgrMetaSyncer 机制

### Phase 2：增强能力

**目标**：扩展 PITQ 和 CDC 的操作覆盖范围，优化高频导入场景性能。

**MVCC**：

| 模块 | 范围 |
| :---- | :---- |
| **拓扑变更版本保留** | 协调 CatalogRecycleBin 保留、StarMgrMetaSyncer 延迟清理旧 Tablet，支持 drop/truncate partition、tablet reshard 后的版本保留 |
| **版本可观测** | `SHOW HISTORY FOR TABLE t`——展示版本历史、操作类型、保留策略等运维信息 |

**PITQ**：

| 模块 | 范围 |
| :---- | :---- |
| **拓扑变更后历史查询** | 支持 drop/truncate partition、tablet reshard 后的历史版本查询，不再触发断链降级 |

**CDC**：

| 模块 | 范围 |
| :---- | :---- |
| **拓扑变更后变更捕获** | 支持 drop/truncate partition、tablet reshard 后的变更捕获，不再触发断链降级 |
| **Snapshot Diff** | 高频导入 \+ 长 CDC 窗口场景，基于两版本快照做集合差直接产出 Net Changes |

### Phase 3：通用能力

**目标**：完整的 Time Travel 产品能力和通用 CDC 能力，与业界对齐。

**MVCC**：

| 模块 | 范围 |
| :---- | :---- |
| **Meta MVCC** | 统一的元数据多版本机制——保留历史 schema、数据分片拓扑，支持按历史 schema 查询 |
| **长周期保留** | 表级 Retention 配置，天级保留 \+ 历史元数据冷存储 |

**PITQ**：

| 模块 | 范围 |
| :---- | :---- |
| **Time Travel SQL** | `SELECT ... FROM t FOR TIMESTAMP/VERSION AS OF`，面向用户的 SQL 接口 |

**CDC**：

| 模块 | 范围 |
| :---- | :---- |
| **STREAM 对象** | 自动管理消费进度的 CDC 对象，配合 Task 调度框架构建增量 ETL pipeline |
| **流式对接** | SDK/RPC 对接 Flink/Spark Structured Streaming |

### 结论

* Phase 1 IVM 最小集 在 v4.2 发布，6月份 code freeze，只进企业版

---

## 附录

### 附录 A：业界 Time Travel 产品能力对比

| 维度 | Snowflake | Databricks (Delta Lake) | Apache Iceberg | BigQuery |
| :---- | :---- | :---- | :---- | :---- |
| **时间点查询语法** | `AT(TIMESTAMP|OFFSET|STATEMENT)` / `BEFORE` | `TIMESTAMP AS OF` / `VERSION AS OF` / `@ts` / `@vN` | `TIMESTAMP AS OF` / `VERSION AS OF` / Branch / Tag | `FOR SYSTEM_TIME AS OF` |
| **默认保留时长** | 1 天（Enterprise+ 最大 90 天） | 7 天（受 VACUUM 控制） | 5 天（需显式 `expire_snapshots`） | 7 天（不可延长） |
| **查询使用的 Schema** | 当前 schema | 默认当前 schema（Column Mapping 下可用历史 schema） | Snapshot/Tag→历史 schema；Branch→当前 schema | 当前 schema |
| **数据恢复** | UNDROP \+ 零拷贝 CLONE \+ CTAS | RESTORE（同表回滚）+ CLONE \+ MERGE | rollback\_to\_snapshot \+ set\_current\_snapshot \+ cherrypick | Copy \+ Table Snapshot（零拷贝只读）+ Table Clone |
| **CDC 机制** | CHANGES clause（无状态）+ Streams（有状态事务性推进） | Change Data Feed（需显式启用，不可追溯） | `create_changelog_view`（支持 net\_changes / compute\_updates） | APPENDS / CHANGES 函数（无状态） |
| **CDC 元数据列** | METADATA$ACTION / METADATA$ISUPDATE / METADATA$ROW\_ID | \_change\_type / \_commit\_version / \_commit\_timestamp | \_change\_type / \_change\_ordinal / \_commit\_snapshot\_id | \_CHANGE\_TYPE |
| **支持的 DML/DDL** | INSERT / UPDATE / DELETE / MERGE / TRUNCATE | INSERT / UPDATE / DELETE / MERGE / INSERT OVERWRITE / TRUNCATE | INSERT / INSERT OVERWRITE / UPDATE / DELETE / MERGE / TRUNCATE | INSERT / UPDATE / DELETE / MERGE / TRUNCATE |

### 附录 B：Table State 变更操作完整分类

| 操作 | 影响的 State 组件 | 层面 |
| :---- | :---- | :---- |
| INSERT INTO | 数据 | 用户 |
| INSERT OVERWRITE | 数据分片 \+ 数据 | 用户 |
| DELETE FROM / UPDATE | 数据 | 用户 |
| STREAM LOAD / BROKER LOAD / ROUTINE LOAD | 数据 | 用户 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN (Fast SC) | 表定义 | 用户 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN (非 Fast SC) | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| ALTER TABLE ORDER BY | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| CREATE/DROP INDEX | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| ALTER TABLE ADD PARTITION | 表定义 \+ 数据分片 | 用户 |
| ALTER TABLE DROP PARTITION | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| TRUNCATE TABLE / PARTITION | 数据分片 \+ 数据 | 用户 |
| 表达式分区合并 | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| ALTER TABLE DISTRIBUTED BY (修改桶数) | 表定义 \+ 数据分片 \+ 数据 | 用户 |
| ALTER TABLE RENAME / SET / MODIFY COMMENT | 表定义 | 用户 |
| Compaction | 数据 | 系统 |
| Tablet Reshard | 数据分片 | 系统 |
| Physical Partition 新增 | 数据分片 | 系统 |

Table State 组件说明：**表定义** \= schema、分区定义、分布定义、索引等（FE）；**数据分片** \= Physical Partition、Tablet 实例（FE）；**数据** \= 对象存储上的数据文件。**系统操作**不改变用户可见的数据或定义，仅改变物理组织。

### 附录 C：Net Changes 示例与 SQL 实现

以下 SQL 是用于说明 Net Changes 合并逻辑的概念性示意，不是最终执行实现。合并规则定义见 6.6.2.3。

原始变更数据：

```
row_id | row_version | change_type | val
-------|-------------|-------------|----
     1 |           1 |     0 (INS) |  10
     2 |           1 |     0 (INS) |  10
     2 |           3 |     2 (BEF) |  10
     2 |           3 |     3 (AFT) |  20
     3 |           1 |     0 (INS) |  10
     3 |           5 |     1 (DEL) |  10
     4 |           2 |     2 (BEF) |  10
     4 |           2 |     3 (AFT) |  20
     4 |           4 |     2 (BEF) |  20
     4 |           4 |     3 (AFT) |  30
     5 |           2 |     2 (BEF) |  10
     5 |           2 |     3 (AFT) |  20
     5 |           5 |     1 (DEL) |  20
     6 |           3 |     1 (DEL) |  50
```

Net Changes 输出：

```
row_id | row_version | change_type | val  | 命中规则
-------|-------------|-------------|------|----------
     1 |           1 |     0 (INS) |  10  | #1 单条 INSERT
     2 |           3 |     0 (INS) |  20  | #2 <INS, AFT> → 以最终值插入
       |             |             |      | (row_id=3 命中规则 #3，抵消无输出)
     4 |           4 |     2 (BEF) |  10  | #4 <BEF, AFT> → 从原始值更新到最终值
     4 |           4 |     3 (AFT) |  30  | #4
     5 |           5 |     1 (DEL) |  10  | #5 <BEF, DEL> → 携带原始值删除
     6 |           3 |     1 (DEL) |  50  | #1 单条 DELETE
```

#### 

窗口函数：利用表按 `row_id` 分桶的特性，所有 `PARTITION BY row_id` 可本地执行，无需全局 shuffle。

```sql
WITH base AS (
    SELECT
        *,
        MIN(row_version) OVER (PARTITION BY row_id) AS min_ver,
        MAX(row_version) OVER (PARTITION BY row_id) AS max_ver,
        COUNT(*)         OVER (PARTITION BY row_id) AS cnt
    FROM changes
),
classified AS (
    SELECT
        *,
        MIN(CASE WHEN row_version = min_ver THEN change_type END)
            OVER (PARTITION BY row_id) AS first_type,
        MAX(CASE WHEN row_version = max_ver THEN change_type END)
            OVER (PARTITION BY row_id) AS last_type
    FROM base
)
SELECT
    row_id,
    CASE WHEN cnt > 1 THEN max_ver ELSE row_version END AS row_version,
    CASE
        WHEN first_type = 0 AND last_type = 3 THEN 0
        WHEN first_type = 2 AND last_type = 1 THEN 1
        ELSE change_type
    END AS change_type,
    val
FROM classified
WHERE
    cnt = 1
    OR (first_type = 0 AND last_type = 3
        AND row_version = max_ver AND change_type = 3)
    OR (first_type = 2 AND last_type = 3
        AND row_version = min_ver AND change_type = 2)
    OR (first_type = 2 AND last_type = 3
        AND row_version = max_ver AND change_type = 3)
    OR (first_type = 2 AND last_type = 1
        AND row_version = min_ver AND change_type = 2)
;
```

### 附录 D：最小 MVCC Java API

本附录帮助理解 4.2 短期方案的数据结构和使用方式。

**1\. 版本标识**

```java
public class OlapTable {
    // 表级版本 ID，以 GTID 表示，每次 DML publish 后更新
    @SerializedName(value = "versionId")
    private long versionId = -1;
}
```

#### 

**2\. Table State：一个版本下的表状态快照**

Table State 记录某个版本下各 Physical Partition 的 visible version。 IVM 最小方案不保留历史 schema 和数据分片拓扑，Partition 信息和 tablet id 从当前 `OlapTable` 获取。

```java
public class TableState {
    private final long tableId;
    private final long versionId;                                        // GTID
    private final Map<Long, LogicalPartitionState> partitionStates;      // partitionId -> state
}

public class LogicalPartitionState {
    private final long id;
    private final String name;
    private final Map<Long, PhysicalPartitionState> physicalPartitionStates;
}

public class PhysicalPartitionState {
    private final long id;
    private final long materializedIndexMetaId;   // 用于检测 tablet reshard
    private final long visibleVersion;
}
```

#### 

**3\. Delta State：两个版本之间的差异**

Delta State 由 base 和 head 两个 TableState 对比推导得出（利用 visible version 连续递增的性质）， 描述每个 Physical Partition 的增量版本区间，CDC 据此生成行级变更。

```java
public class DeltaState {
    private final long tableId;
    private final Map<Long, LogicalPartitionDeltaState> partitionDeltaStates;
}

public class LogicalPartitionDeltaState {
    private final long id;
    private final String name;
    private final Map<Long, PhysicalPartitionState> newPartitions;       // 仅在 head 中（新增分区）
    private final Map<Long, PhysicalPartitionState> droppedPartitions;   // 仅在 base 中（删除分区）
    private final Map<Long, PhysicalPartitionDeltaState> changedPartitions; // 两者都有，version 不同
}

public class PhysicalPartitionDeltaState {
    private final long id;
    // reshard 后 delta 可能跨多个 materialized index，
    // 每个覆盖部分版本区间，如 (1, 4], (4, 10], (10, 20]
    private final List<MaterializedIndexDeltaState> indexDeltaStates;
}

public class MaterializedIndexDeltaState {
    private final long metaIndexId;
    private final long startVersion;   // 不包含
    private final long endVersion;     // 包含
}
```

#### 

**4\. 版本管理：MV 订阅模式**

MV 通过订阅/取消订阅管理版本生命周期。只有被订阅的版本才保留，未被订阅的可释放。

```java
public class TableVersionKeeper {
    // 安全兜底：超时后无论是否被订阅都清理
    private volatile long maxKeepTimeMs;
    // version id -> table state
    private final Map<Long, TableState> savedStates;
    // 引用计数：version id -> 订阅数
    private final Map<Long, AtomicLong> subscribedStates;

    // MV 刷新前调用：保存当前最新 state，增加引用计数，返回 version id
    public Optional<Long> subscribeLatestTableState() { ... }

    // MV 刷新成功后调用：释放旧 base version，引用计数归零后 state 可被清理
    public void unsubscribeTableState(long versionId) { ... }

    public TableState getTableState(long versionId) { ... }

    // 定期清理超过 maxKeepTimeMs 的版本
    public void removeExpiredTableState() { ... }
}
```

**5\. 查询辅助**

```java
public class TableStateUtils {
    // PITQ：根据历史 TableState 构建可查询的 OlapTable
    // 从当前 catalogTable 复制，然后用 tableState 中的 visible version 覆盖，
    // 移除 tableState 中不存在的 partition。
    // 如果发现 partition 已被 drop 或 tablet 已 reshard（断链），返回 empty
    public static Optional<OlapTable> buildOlapTable(
            TableState tableState, OlapTable catalogTable) { ... }

    // CDC：对比 base 和 head 两个 TableState，推导出 DeltaState
    // 利用 visible version 连续递增的性质，不需要中间版本的 Meta
    public static Optional<DeltaState> computeDeltaState(
            TableState oldState, TableState newState, OlapTable catalogTable) { ... }
}
```

#### 

**6\. IVM 刷新流程**

串联上述 API 的完整使用流程：

```java
// 1. 获取上次刷新记录的 base version
long baseVersionId = refreshContext.getBaseVersionId(baseTableInfo);

// 2. 订阅当前最新版本作为 head（同时也是下次刷新的 base）
long headVersionId = tableVersionKeeper.subscribeLatestTableState().get();

// 3. 获取 base 和 head 的 Table State
TableState baseState = tableVersionKeeper.getTableState(baseVersionId);
TableState headState = tableVersionKeeper.getTableState(headVersionId);
OlapTable catalogTable = getCurrentCatalogTable();

// 4. PITQ：构建 base version 的查询 OlapTable
Optional<OlapTable> baseTable = TableStateUtils.buildOlapTable(baseState, catalogTable);
if (baseTable.isEmpty()) {
    // 断链（partition drop/reshard 等），降级全量刷新
    fallbackToFullRefresh();
    return;
}

// 5. CDC：推导 base→head 的 DeltaState
DeltaState delta = TableStateUtils.computeDeltaState(baseState, headState, catalogTable).get();

// 6. 执行增量刷新
//    - scan baseTable 读取 base version 数据
//    - scan delta 范围内的 CHANGES 生成行级变更
//    - 增量计算 ΔMV，应用到 MV 表

// 7. 刷新成功后，释放旧 base，head 成为下次刷新的 base
tableVersionKeeper.unsubscribeTableState(baseVersionId);
refreshContext.setBaseVersionId(baseTableInfo, headVersionId);
```

### 附录 E：Flink CDC Update 语义对比

以下通过一个 Flink 流处理场景说明两种 Update 语义对下游系统的影响差异。

**场景说明**：StarRocks 主键表（订单表）的变更通过 Flink CDC 消费，在 Flink 中与维度表做 Lookup Join（即用变更行的 `user_id` 实时查询维度表获取用户名），然后将关联结果写入 Redis（KV 存储，按 `order_id` 做 key 的 upsert/delete）。

**场景**：`order_id=1` 的 `amount` 从 100 改为 200，`user_id=1` 对应 `name="张三"`。

**方案 A：UPDATE\_BEFORE \+ UPDATE\_AFTER**

```
CDC 产出:     -U(1, u1, 100)          +U(1, u1, 200)
Lookup Join:  -U(1, "张三", 100)      +U(1, "张三", 200)
Redis:                                SET order:1 {张三, 200}
```

Redis 操作 1 次 SET。数据始终可见，从 `{张三, 100}` 原子变为 `{张三, 200}`。

**方案 B：DELETE \+ INSERT**

```
CDC 产出:     -D(1, u1, 100)          +I(1, u1, 200)
Lookup Join:  -D(1, "张三", 100)      +I(1, "张三", 200)
Redis:        DEL order:1             SET order:1 {张三, 200}
```

Redis 操作 1 次 DEL \+ 1 次 SET。DEL 和 SET 之间存在时间窗口，`order:1` 短暂不存在。

| 对比维度 | 方案 A (UPDATE) | 方案 B (DELETE+INSERT) |
| :---- | :---- | :---- |
| Redis 写入次数 | 1 | 2 |
| 数据连续性 | 始终可见 | 短暂消失 |
| Dashboard 查询 | 平滑更新 | 可能闪烁（查到空值） |
| Redis QPS | N | 2N |

方案 B 在高并发场景下，下游 `GET order:1` 有概率拿到 `nil`。对于 IVM 场景（批处理，所有变更处理完才原子可见）这不是问题，但对于流式实时同步场景需要注意。

### 附录 F：Net Changes 两层过滤完整推演

本附录通过一个覆盖全部 4 种 change type、跨 3 次导入的例子，演示存储层 XOR \+ 计算层窗口函数的完整过滤流程。合并规则定义见 6.6.2.3。

**初始状态**：S0 是已有 segment，包含行 {A, B}。

**Version 3（第一次导入，创建 S1）**：更新 A 和 B，插入 C D E F。

```
S0:  before_type_vector[v3] = {A, B}           ← A, B 旧值标记
S1:  rows = {A, B, C, D, E, F}
     after_type_vector[v3]  = {A, B}            ← A, B 是 UPDATE_AFTER
     隐含 INSERT            = {C, D, E, F}      ← 新行
```

**Version 5（第二次导入）**：更新 B，删除 D。

```
S1:  before_type_vector[v5] = {B}               ← B 旧值离开 S1
     delete_type_vector[v5] = {D}               ← D 被删除
S2:  rows = {B'}, after_type_vector[v5] = {B}   ← B 的新值落到 S2
```

**Version 7（第三次导入）**：更新 A，删除 F。

```
S1:  before_type_vector[v7] = {A}               ← A 旧值离开 S1
     delete_type_vector[v7] = {F}               ← F 被删除
S3:  rows = {A'}, after_type_vector[v7] = {A}   ← A 的新值落到 S3
```

S1 的 changes vector 汇总（4 种 change type 全部出现）：

```
             ┌─ v3 创建 ─┐   ┌── v5 ──┐   ┌── v7 ──┐
             AFTER INSERT   BEFORE DEL   BEFORE DEL
      Row A:  ✓                           ✓
      Row B:  ✓              ✓
      Row C:       ✓
      Row D:       ✓                ✓
      Row E:       ✓
      Row F:       ✓                            ✓
```

**Step 1：存储层 XOR（无 compaction）**

合并 S1 在 CDC 窗口内所有版本的 bitmap，做 XOR：

```
enter（进入 S1）= after ∪ insert = {A, B, C, D, E, F}
leave（离开 S1）= before ∪ delete = {A, B, D, F}

         A  B  C  D  E  F
enter:   1  1  1  1  1  1
leave:   1  1  0  1  0  1
                ↓ XOR
result:  0  0  1  0  1  0   →  surviving = {C, E}，cancelled = {A, B, D, F}
```

逐行分析：

| 行 | 进入(v3) | 离开 | 抵消? | 效果 |
| :---- | :---- | :---- | :---: | :---- |
| A | AFTER | BEFORE(v7) | yes | 中间态消除；原始 BEFORE 在 S0，最终 AFTER 在 S3，留给计算层 |
| B | AFTER | BEFORE(v5) | yes | 中间态消除；原始 BEFORE 在 S0，最终 AFTER 在 S2，留给计算层 |
| C | INSERT | — | no | 无离开，直接通过 |
| D | INSERT | DELETE(v5) | yes | 生了又死了，完全抵消（规则 \#3），0 条输出 |
| E | INSERT | — | no | 同 C，直接通过 |
| F | INSERT | DELETE(v7) | yes | 完全抵消（规则 \#3），0 条输出 |

存储层效果：**S1 从 6 行降至 2 行**（C 和 E），减少 67% 的读取量。

**Step 2：Compaction 如何导致漏网**

假设 v5 和 v7 之间发生 compaction，S1 \+ S2 合并为 S\_merged：

```
时间线：
  v3           v5          compaction        v7
  ├── S1 创建 ──┤── S2 创建 ──┤── S1+S2→S_merged ──┤── S3 创建 ──┤
```

v7 时，A 和 F 的数据已在 S\_merged 中，变更记录在 S\_merged 上而非 S1：

```
S1 的 XOR（只经历 v3 + v5，v7 的变更不在 S1 了）:
  enter = {A, B, C, D, E, F}
  leave = {B, D}                ← 只有 v5，没有 v7
  cancelled = {B, D}
  surviving = {A, C, E, F}     ← A 和 F 没能被抵消！

S_merged 的 XOR:
  enter = {}                    ← compaction 不产生 changes vector
  leave = {A, F}                ← v7 的 BEFORE(A) + DELETE(F)
  cancelled = {}                ← enter 为空，无法抵消
```

对比无 compaction 的情况：

|  | 无 compaction | 有 compaction |
| :---- | :---- | :---- |
| 存储层消除行数 | 4 行（A, B, D, F） | 2 行（B, D） |
| 传给计算层行数 | 6 行 | 10 行 |
| 最终结果正确性 | correct | correct（计算层兜底） |

A 和 F 因为"进入"和"离开"分散在不同 segment（S1 vs S\_merged），存储层 XOR 看不到配对，**漏网**了。

**Step 3：计算层窗口函数兜底**

以漏网的 **F** 为例，计算层收到：

```
row_id=F, row_version=v3, change_type=0(INSERT)   ← 来自 S1
row_id=F, row_version=v7, change_type=1(DELETE)    ← 来自 S_merged
```

窗口函数计算：`cnt=2, first_type=INSERT(0), last_type=DELETE(1)` → **规则 \#3**，0 条输出。存储层没消掉的，计算层消掉了。

以漏网的 **A** 为例，计算层收到 4 条：

```
row_id=A, v3, BEFORE(2), old_val    ← S0
row_id=A, v3, AFTER(3),  mid_val    ← S1（漏网）
row_id=A, v7, BEFORE(2), mid_val    ← S_merged（漏网）
row_id=A, v7, AFTER(3),  new_val    ← S3
```

窗口函数：`first_type=BEFORE(2), last_type=AFTER(3)` → **规则 \#4**，输出 `BEFORE(old_val) + AFTER(new_val)`，两次 UPDATE 合并为一次，中间态被 WHERE 过滤。

**最终输出**（无论是否发生 compaction，结果相同）：

```
  A: BEFORE(old) + AFTER(new)    ← 规则 #4，两次更新合并为一次
  B: BEFORE(old) + AFTER(new)    ← 规则 #4，两次更新合并为一次
  C: INSERT(val)                 ← 规则 #1，直接通过
  D: (无输出)                    ← 规则 #3，INSERT+DELETE 抵消
  E: INSERT(val)                 ← 规则 #1，直接通过
  F: (无输出)                    ← 规则 #3，INSERT+DELETE 抵消
```

