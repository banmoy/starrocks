# [DISCUSSION] 存算分离内表历史版本查询与变更数据捕获

> **文档状态**：讨论稿，面向跨团队方向收敛  
> **目标读者**：存储、元数据、查询、MV、架构、产品等团队  
> **约定**：文中以 `[已定]` `[待讨论]` `[暂不展开]` 标注内容状态

---

## 1. 核心摘要

**问题**：StarRocks 计划在存算分离内表上支持增量物化视图（IVM）。IVM 增量刷新依赖两项底层能力——**历史版本查询（Point-in-time Query, PITQ）** 和 **变更数据捕获（Change Data Capture, CDC）**。当前系统不具备这两项能力。

**战略价值**：PITQ 和 CDC 不仅服务于 IVM，也是 Time Travel 查询、流式计算对接、数据同步、合规审计等场景的基础能力。Snowflake、Databricks、Iceberg、BigQuery 等系统均已具备。支撑这些能力是 StarRocks 作为分析型数据库长期竞争力的一部分。

**推荐方向**：从 IVM 所需的最小能力集出发，分阶段向通用能力演进。

- **短期（Phase 1）**：面向 IVM 的最小实现——仅覆盖 DML，使用最新 schema，短窗口保留，遇到数据分片拓扑变更操作降级全量刷新。
- **中期（Phase 2）**：扩展支持更多操作（drop/truncate partition、tablet reshard、INSERT OVERWRITE），提供 CDC SQL 接口。
- **长期（Phase 3）**：完整的 Time Travel（Meta MVCC、任意版本查询、天级保留）和通用 CDC（完整 UPDATE 语义、流式消费接口）。

**本文目标**：对齐问题理解、明确需求边界、分析方案空间、收敛推荐方向、制定分阶段 roadmap，并标注需要团队讨论的关键决策点。

---

## 2. 问题定义与动机

### 2.1 直接驱动：IVM 对底层能力的需求

增量物化视图（IVM）的核心目标是：当基表发生变更时，只计算 MV 需要更新的部分，避免全量重算。一次增量刷新需要以下输入：

- **Base version 数据**：MV 上次刷新时对应的基表数据快照
- **Head version 数据**：基表当前最新版本的数据快照
- **两个版本之间的变更（Delta）**：从 base 到 head 之间发生的行级数据变更

用业界通用术语来描述，前者需要**历史版本查询（Point-in-time Query, PITQ）**，后者需要**变更数据捕获（Change Data Capture, CDC）**。

当前 StarRocks 存算分离内表不具备这两项能力——系统只能读取表的最新状态，无法回到某个历史版本，也无法捕获两个版本之间的差异。具体的技术障碍将在第 4 章（Table Version）中分析。

### 2.2 长期产品价值

PITQ 和 CDC 并非仅为 IVM 设计的临时能力。从更广的产品视角看，这两项能力可以支撑多种高价值场景：

**PITQ 能够支撑的场景**：
- **Time Travel**：用户按时间戳或版本号读取表的历史状态，用于误操作恢复、报表复现、审计取证
- **AI / ML 数据版本管理**：为模型训练和推理提供可复现的历史数据快照

**CDC 能够支撑的场景**：
- **流式计算对接**：将表的增量变更推送给 Flink / Spark Structured Streaming
- **数据同步**：将内表变更同步到外部系统
- **合规审计**：记录数据的完整变更历史
- **SCD Type 2（缓慢变化维度）**：在维度表中保留历史版本行而非覆盖更新，使事实表可以关联到变更发生时的维度状态，常见于数仓维度建模

**业界对标**：主流数据库和数据湖系统均已具备这两项能力。

| 系统 | 历史版本查询 | 变更数据捕获 |
|:-----|:-----------|:-----------|
| Snowflake | Time Travel（AT / BEFORE 语法，企业版可以保留 90 天） | Streams Object / CHANGES clause |
| Databricks (Delta Lake) | Time Travel（TIMESTAMP / VERSION AS OF，默认 7 天） | Change Data Feed (CDF) |
| Apache Iceberg | Snapshot-based Time Travel + Branch/Tag | Spark Procedures `create_changelog_view` |
| BigQuery | Time Travel (`FOR SYSTEM_TIME AS OF`，默认 7 天） | APPENDS / CHANGES 函数 |

这些能力在上述系统中是增量 ETL、流计算、数据恢复等产品的基础。StarRocks 补全这些基础能力，是构建长期产品竞争力的必要条件。

### 2.3 思路：分层思考、分阶段落地

完整支持 Time Travel 和通用 CDC 涉及较大的系统改造（元数据多版本、保留策略、冷存储等），无法在短期内完成。但 IVM 的需求是 Time Travel 和通用 CDC 需求的**严格子集**（详见第 3 章分析），可以用更轻量的方案先满足。

因此本文采用以下思路：

1. **先定义问题全貌**：不仅为 IVM 考虑，也覆盖更广的场景需求，确保技术方案有清晰的演进路径
2. **再裁剪 IVM 最小集**：基于 IVM 的特点做需求裁剪，设计复杂度可控的第一阶段方案
3. **关键原则——短期方案不阻塞长期演进**：IVM 最小实现中的数据结构和接口设计，应可以自然扩展到 Time Travel 和通用 CDC

---

## 3. 需求分析

本章分别分析 PITQ 和 CDC 在不同场景下的需求差异，明确 IVM 需要的能力边界。

### 3.1 历史版本查询（PITQ）需求

Time Travel 是 PITQ 最完整的产品形态——以下 Time Travel 需求来自业界产品（Snowflake、Databricks、Iceberg、BigQuery）的调研总结，具体参见附录 A。IVM 对 PITQ 的需求是 Time Travel 需求的子集。以下对比两者的差异：

| 维度 | IVM | Time Travel |
|:-----|:----|:------------|
| **目标** | 查询数据，主要关注数据内容 | 查询 + 恢复，关注元数据 + 数据（schema、分区定义等） |
| **覆盖的操作** | DML（INSERT / DELETE / UPDATE / 各类 LOAD）。分区级 DDL（DROP / TRUNCATE PARTITION）和 INSERT OVERWRITE 不一定适合增量刷新，可低优支持，遇到可以考虑降级全量刷新 | DML + 影响表结构和数据的 DDL（schema change、分区变更等） |
| **Schema 语义** | 使用最新 schema 查询历史数据——MV 刷新目标是与基表最新状态保持一致 | 语义上应使用历史版本对应的 schema（业界实践不一：Spark + Iceberg 默认使用历史 schema，Snowflake 使用当前 schema） |
| **用户接口** | Java API——参考当前 Iceberg IVM 实现，查询 Analyze 阶段构造历史版本 scan plan | SQL 接口——`FOR TIMESTAMP/VERSION AS OF`，用户通过时间戳/ VERSION ID 指定要读取的版本 |
| **保留时间** | 短窗口，取决于 MV refresh interval（实时场景通常很短）；可以 best-effort，版本提前释放时 MV 可降级全量刷新 | 固定时间窗口，通常天级（Databricks 默认 7 天，Snowflake Standard 默认 1 天）；严格遵循，否则用户可能无法恢复数据 |
| **保留版本** | 只需保留 MV 上次刷新对应的基表版本 | 保留时间窗口内所有 DML + DDL 产生的版本，支持查询任意版本 |

**总结**：IVM 对 PITQ 的需求相比 Time Travel，在以下方面有显著简化：

- 覆盖核心 DML 即可，不支持的操作可降级全量刷新
- 不需要保留历史 schema 等元数据
- 只需 Java API，不需要 SQL 接口
- 可使用更轻量的保留策略，降低维护成本

### 3.2 变更数据捕获（CDC）需求

CDC 的核心目标是捕获两个版本之间的行级数据变更。

#### 3.2.1 基本概念

**变更类型（Change Type）**：每条变更携带一个类型标记——

- `INSERT`：新插入的行
- `DELETE`：被删除的行
- `UPDATE_BEFORE`：更新操作产生的两条变更之一，表示更新前的数据
- `UPDATE_AFTER`：更新操作产生的两条变更之一，表示更新后的数据

**Update 语义的两种表示方式**：一次 UPDATE 可以表示为 `UPDATE_BEFORE + UPDATE_AFTER`（保留完整的更新语义），也可以表示为 `DELETE + INSERT`（更轻量，不需要将更新前后的数据进行关联）。例如将某行 `val` 从 10 改为 20：前者产出 `UPDATE_BEFORE(val=10)` + `UPDATE_AFTER(val=20)` 两条变更；后者产出 `DELETE(val=10)` + `INSERT(val=20)` 两条变更。两种表示在语义上不同——前者明确标识了这是一次更新，后者无法与"先删再插一条新行"区分。

**Net Changes（净变更）**：将多个版本内同一行（按 ROW_ID 标识）的变更合并为最小等价集——应用 Net Changes 后的最终状态与应用全部原始变更后的状态一致。例如一行先 INSERT 再 UPDATE 再 DELETE，净效果是 0 条变更（相互抵消）。

**顺序（Ordering）**：消费 CHANGES 时是否保持特定顺序，涉及三个层面——(1) 一次消费多个版本时，版本之间是否按版本号先后消费；(2) 同一版本内，不同行之间是否有序；(3) 同一行的配对变更（如 `UPDATE_BEFORE` 和 `UPDATE_AFTER`）是否保证相邻且有序。

#### 3.2.2 IVM 与通用 CDC 的需求差异

不同消费场景对 CDC 的需求存在差异：

| 维度 | IVM | 通用 CDC（流计算、审计等）|
|:-----|:----|:--------------------------|
| **本质需求** | 两个版本之间的净变更（Net Changes），不需要逐条的中间变更 | 逐条行级变更，可能需要完整的中间过程，比如审计 |
| **覆盖的操作** | DML（INSERT / DELETE / UPDATE / 各类 LOAD）。类似 PITQ，分区级 DDL 和 INSERT OVERWRITE 可低优支持，遇到时降级全量刷新 | DML + 部分 DDL（DROP/TRUNCATE PARTITION 等，以 DELETE 变更体现） |
| **Update 语义** | DELETE + INSERT 即可，不需要将 BEFORE/AFTER 关联 | 可能需要 UPDATE_BEFORE + UPDATE_AFTER 完整语义（参见附录 E 示例） |
| **消费粒度** | 保证事务语义，以版本为粒度批量消费，可以一次消费多个版本的变更 | 可能需要按版本逐条消费 |
| **顺序保证** | 不需要——IVM 本质是批处理，中间结果不可见，对顺序依赖弱 | 一些场景需要严格有序，否则影响正确性，比如 Flink 没有事务机制，以 record 为粒度消费 |
| **消费接口** | Java API——参考当前 Iceberg IVM 实现，在 MV 刷新的 Analyze 阶段通过 API 获取版本信息并构造增量 SCAN plan，不使用 SQL API | SQL：CHANGES 语句（指定任意版本区间的无状态查询）、STREAM 对象（自动管理消费位点，每次只返回上次消费之后的变更）；SDK/RPC（对接 Flink/Spark） |

#### 3.2.3 支持的表类型与操作范围

不同表类型对 CDC 的支持能力不同，这取决于表模型的存储特性：

| 表类型 | 支持的操作 | 变更类型 | 说明 |
|:------|:----------|:--------|:-----|
| **明细表** | DML：仅 append 类型。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT、DELETE（DDL 产生） | DELETE 操作生成变更的成本高（需全表扫描），暂不支持 |
| **主键表** | DML：INSERT INTO、INSERT OVERWRITE、DELETE、UPDATE、各类 LOAD。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER | 能力最完整 |
| **聚合表** | DML：仅 append。DDL：TRUNCATE TABLE/PARTITION、DROP PARTITION | INSERT（aggregate 语义，变更是聚合后的结果）、DELETE（DDL 产生） | 增量 rowset 存储的是 aggregate 后的结果，可直接作为变更使用 |
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

基于上述分析，IVM 对 PITQ 和 CDC 的最小需求如下（均为 `[已定]`）：

**PITQ**：
- 覆盖 DML（除 INSERT OVERWRITE）
- 使用最新 schema，不需要保留历史 schema
- Java API（Analyze 阶段构造增量 scan plan），不需要 SQL 接口
- 短窗口保留，MV 刷新完成后即可释放旧版本引用
- 版本不可用时可降级全量刷新

**CDC**：
- 覆盖的操作同 PITQ
- Update 使用 DELETE + INSERT 语义
- 需要 Net Changes
- 不需要保序
- 以版本为粒度批量消费
- Java API

**低优支持**（遇到时可以降级全量刷新）：
- DROP / TRUNCATE PARTITION、INSERT OVERWRITE。

---

## 4. PITQ 和 CDC 的共同基础：MVCC 方案设计

PITQ 需要"读取表的某个历史版本"，CDC 需要"比较两个版本之间的差异"——两者都依赖系统对表的**MVCC**能力。MVCC 能力可以分为三部分：

- **多版本数据结构**：系统是否记录并保留了历史版本的元数据和数据，使得历史状态可被引用和访问
- **版本标识（Version Identity）**：如何唯一标识一个版本，使用户或系统能够精确引用"表在某个时间点的状态"
- **版本保留（Retention）**：历史版本保留多久、何时可以被清理，既要避免过早清理导致历史版本不可用，也要避免无限保留导致存储膨胀

本章先定义基本概念，然后沿这三个方向分析现状、差距和方案。

### 4.1 基本概念

**Table State（表状态）**：执行一次查询所需的最小信息集合，由两层构成：

- **Table Meta**：FE 管理并持久化的元数据，包括表定义（schema、分区、分布、索引等）、数据分片拓扑（Logical Partition → Physical Partition → Tablet 三级层次）和各 Physical Partition 的数据版本号（visible version）。查询规划阶段依赖这些信息进行 SQL 解析、分区裁剪与执行计划生成。
- **Table Data**：对象存储上的 tablet metadata 文件和数据文件。查询执行阶段根据 Meta 中指定的版本号，从对象存储读取对应的数据。

**Table Version（表版本）**：Table State 的版本号，标识表在某个时间点的完整状态。理论上，Table Version 映射为 Table Meta 的版本 + 各 Physical Partition 的 visible version。只要 Table State 的任何组件发生变化（DML、DDL 或系统操作），都应产生一个新的 Table Version。

> Table State 和 Table Version 新引入的概念，当前系统中没有对应实现。

为理解 Table Version 需要覆盖的范围，以下分类列出所有可能改变 Table State 的操作：

| 操作类型 | 示例 | 影响的 State 组件 |
|:--------|:-----|:-----------------|
| **DML** | INSERT INTO, DELETE, UPDATE, 各类 LOAD | 数据 |
| **DML（特殊）** | INSERT OVERWRITE | 数据分片拓扑 + 数据（新分区替换旧分区） |
| **DDL（不改拓扑）** | Fast Schema Change（加减列） | 表定义 |
| **DDL（改拓扑）** | DROP/TRUNCATE/MERGE PARTITION, 非 Fast Schema Change, 修改分桶数 | 表定义 + 数据分片拓扑 + 数据 |
| **系统操作** | Compaction, Tablet Reshard | 数据分片拓扑（逻辑等价，不改变查询结果） |

> 完整的操作分类详见附录 B。

### 4.2 现状与差距

#### 4.2.1 多版本数据结构

**现状**：
- Table Data 已具备多版本——每个 Physical Partition 维护独立的 visible version（单调递增），tablet metadata 按版本存储在对象存储上，通过指定 visible version 可以读取该版本对应的数据文件集合
- Table Meta 不支持多版本——`OlapTable` 中的表定义、分片拓扑等元数据发生变更后直接原地更新，历史状态被覆盖或丢弃

**差距**：
1. 没有表级版本数据结构——Physical Partition visible version 是分区粒度的，一张表可能包含多个分区，各分区版本独立推进，PITQ 和 CDC 需要一个**表级的一致性视图**（所有分区在同一个逻辑时间点的版本快照）
2. Table Meta 无历史版本——schema change、drop partition 等操作后，查询历史版本所依赖的 meta（schema、分区定义、tablet 集合等）不存在了

> 第 2 点不是 PITQ/CDC 独有的问题。此前多个 feature 也遇到过元数据多版本需求（如 Fast Schema Change v2 将历史 schema 保存在 QueryPlan 和 SchemaChangeJob 中，Tablet Reshard 为用到的 materialized index 设计了多版本），但都是局部分散的，缺乏统一方案。

#### 4.2.2 版本标识

**现状**：系统中已有两个与版本标识相关的机制：
- Physical Partition visible version：分区级的数据版本号，单调递增，但是分区粒度，无法直接标识"表在某个时间点的完整状态"
- GTID（Global Transaction ID）：64-bit ID，标识一次成功提交的 DML 事务，在集群范围内单调递增，内嵌毫秒级时间戳，可以在 timestamp 和 GTID 之间转换，但仅覆盖 DML 事务，不覆盖 DDL 和系统操作

**差距**：两者都无法直接作为 Table Version 的标识：
- Physical Partition visible version 是分区粒度，不是表粒度
- GTID 是事务粒度且仅覆盖 DML，无法标识 DDL 和系统操作产生的版本

需要一种机制将 Table Version 与可检索的标识关联，使系统能根据标识（timestamp 或 ID）定位到具体的 Table Version。

#### 4.2.3 版本保留

**现状**：历史版本数据结构存在之后，还需要确保在需要的时间内不被清理。当前有三套独立的清理机制：
- **Vacuum（Auto vacuum / Full vacuum）**：负责清理对象存储上不再需要的旧版本 tablet metadata 和数据文件。触发场景是日常的 DML 和 Compaction 产生的版本淘汰。保留窗口由 `lake_autovacuum_grace_period_minutes` 等参数控制，主要保护正在执行中的查询（分钟级）
- **CatalogRecycleBin**：负责清理 DROP / TRUNCATE PARTITION 产生的旧分区及其下属 Tablet。旧分区先暂存于回收站（支持 RECOVER 恢复），过期后触发物理清理——包括删除对象存储上的数据文件和 StarManager 中的 Shard 元数据。保留时间由 `catalog_trash_expire_second` 控制（默认 1 天）
- **StarMgrMetaSyncer**：兜底对账机制，定期比对 FE 和 StarManager 的元数据，清理 FE 中已不存在但 StarManager 中仍残留的 Shard 和数据文件。处理的是上述两种机制未能及时清理的残留（如 RecycleBin 清理失败、Tablet Reshard 后的旧 Tablet 等）

**差距**：
- 三套清理机制覆盖不同的场景，都缺少按“Table Version”保留的语义
- Retention 时间段，假设假设 FE 内存能存放所有历史元数据（包括 StarManager）
- 均是 FE 级别的 Retention 配置，没有表级配置

### 4.3 长期方案：完整的表级 MVCC

为支持任意历史版本、支持更多 DDL、天级别甚至更长的 retention 等能力，需要补齐完整的表级 MVCC 能力。主要挑战：

**多版本数据结构**：
- 补齐 Table Meta 多版本——保留历史表定义（schema）和历史数据分片拓扑，支持按历史 schema 查询
- FE 内存压力——当前 FE 元数据全部驻留内存，天级保留的历史版本元数据需要冷存储方案。另外高频导入场景下每次 DML 都产生一个 Table Version，Time Travel 场景下保留所有版本本身也会带来内存压力

**版本标识**：
- 将 GTID 扩展覆盖 DDL 和系统操作（每个操作分配一个 GTID），使 Table Version 标识能描述所有状态变更

**版本保留**：
- 引入表级 Retention 配置
- retention 策略与版本保留协同

上述挑战涉及 FE 元数据管理、存储层 Vacuum、StarManager 对账等多个模块的协同改造，复杂度高，预计需要较长的落地周期。因此需要考虑面向 IVM 的最小方案（见 4.4），长期逐步演进。

### 4.4 短期方案：面向 IVM 的最小 MVCC

IVM 场景的需求简化（详见 3.3）使得短期可以绕过 4.3 中的大部分挑战，用最小改动实现可用的 MVCC：

IVM 场景的需求特点使得短期可以绕过 4.3 中的主要挑战：

- IVM 只覆盖 DML，不需要支持 DDL 和系统操作 → **绕过 Table Meta 多版本**（不需要保留历史 schema 和数据分片拓扑，遇到数据分片拓扑变更直接断链降级）
- IVM 只需 DML 版本标识 → **绕过 GTID 覆盖范围不完整的问题**（复用现有 GTID 即可）
- IVM 保留窗口短且只保留 MV 引用的版本 → **绕过 FE 内存压力和长周期保留**（保留的版本数量与 MV 数量相关而非与 DML 频率相关，全内存可行）

基于这些简化，短期方案在三个维度的设计如下：

**多版本数据结构**（Table Meta 侧，Table Data 已有多版本无需改造）：
- 每个版本只需记录 `{ppId -> visibleVersion}` 映射，无需完整的 Meta 快照 (当前按分区刷新的 MV 实际也会存储该映射)
- 数据分片拓扑变更（DDL、reshard 等）触发断链，IVM 降级全量刷新

**版本标识**：
- `OlapTable` 增加一个表级的 `versionId` 字段（以 GTID 表示），每次 DML publish 后更新
- 面向用户使用 timestamp，系统内部转换为 GTID，例如：
  - "查询 t1 时刻的数据"：`max{gtid | timestamp(gtid) ≤ t1}`
  - "查询 t1 到 t2 之间的变更"：`{gtid | t1 < timestamp(gtid) ≤ t2}`
- IVM 直接记录 GTID 表示基表刷新的版本，与当前 Iceberg 

**版本保留**：
- 采用 **MV 订阅模式**而非固定时间窗口——只保留 MV 引用的版本，刷新成功后旧版本即可释放
- **Table Meta**：只保留 MV 引用的 base version 的 `{ppId -> visibleVersion}` 映射，用于 PITQ 读取 base 快照。中间版本的 Meta 不保留——因为 CDC 不需要中间版本的完整 Meta，只需要知道每个 Physical Partition 从 base 到 head 的 visible version 区间，这可以利用 visible version 连续递增的性质从 base 和 head 两个端点直接推导
- **Table Data**：保留 base 到 head 之间所有中间版本的 tablet metadata 和数据文件，通过 `minRetainVersion` 阻止 Vacuum 清理。CDC 根据推导出的版本区间从这些文件中生成行级变更
- 这种模式的代价是**牺牲了对任意版本的 PITQ 和 CDC**——只能查询 MV 引用的 base version 快照和 base→head 区间的变更，无法查询中间任意版本的快照或任意两个版本之间的变更。这对 IVM 足够，通用 Time Travel 和 CDC 需要长期方案（4.3）
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
- 刷新成功后，head 成为新的 base，旧 base 映射可以释放，pp1 的 v3~v4、pp2 的 v5 也可以被 Vacuum 清理

> 具体的数据结构和 Java API 设计参见附录 D。

### 4.5 版本可观测

支持通过 SQL 查看当前保留的版本信息，便于运维和问题排查：

```sql
SHOW HISTORY FOR TABLE t;
```

可以输出如下字段：

| 字段 | 说明 |
|:-----|:-----|
| `timestamp` | 版本产生的时间 |
| `version_id` | Table Version ID（GTID） |
| `operation_type` | 产生该版本的操作类型（如 INSERT、DELETE、LOAD 等） |
| `retention_policy` | subscribe / time-based |

### 4.6 小结

| 维度 | 短期（IVM 最小 MVCC） | 长期（完整表级 MVCC） |
|:-----|:---------------------|:---------------------|
| **多版本数据结构** | Table Meta 只记录 `{ppId -> visibleVersion}` 映射，不保留历史 schema 和数据分片拓扑；数据分片拓扑变更断链降级 | 完整的 Table Meta 多版本（历史 schema、数据分片拓扑），支持所有 DML + DDL |
| **版本标识** | 复用 GTID（仅覆盖 DML），`OlapTable` 增加 `versionId` | GTID 扩展覆盖 DDL 和系统操作 |
| **版本保留** | MV 订阅模式，只保留被引用的版本；Meta 保留端点，Data 保留中间版本 | 表级 Retention 配置，固定时间窗口保留所有版本 |
| **PITQ 能力** | 仅 MV 引用的 base version | 任意历史版本 |
| **CDC 能力** | 仅 base→head 区间 | 任意两个版本之间 |
| **复杂度** | 低，不涉及 Meta MVCC、RecycleBin/StarMgrMetaSyncer 改造 | 高，涉及多模块协同改造 |
| **落地周期** | 短期可交付 | 需要较长的落地周期 |

短期方案通过限定 IVM 场景（仅 DML、短窗口、可降级）绕过了长期方案的主要挑战，以牺牲对任意版本的 PITQ 和 CDC 能力为代价换取快速落地。长期方案在短期基础上渐进扩展，两者数据结构兼容。

---

## 5. PITQ：方案设计

第 4 章定义了 MVCC 机制（如何记录和保留版本）。本章在此基础上讨论 PITQ 的用户接口和查询流程——即如何利用 MVCC 能力完成历史版本查询。

### 5.1 用户接口

与 StarRocks 当前查询 Iceberg 表历史版本的语法一致，支持按 timestamp 和 version（GTID）两种方式指定：

```sql
-- 按 timestamp 查询
SELECT ... FROM t FOR TIMESTAMP AS OF '2026-02-21 10:30:00';

-- 按 version（GTID）查询
SELECT ... FROM t FOR VERSION AS OF 10;
```

可通过 `SHOW HISTORY FOR TABLE t`（参见 4.5）查看当前保留的版本列表，获取可用的 timestamp 和 version id。

- IVM 通过 Java API 在 Analyze 阶段注入版本信息构造 scan plan，不经过 SQL 层（参见 3.1）
- 长期 Time Travel 场景面向用户暴露上述 SQL 接口

### 5.2 查询流程

PITQ 的查询流程与当前查询流程的主要区别在于 FE 查询规划阶段——需要根据目标版本替换各分区的 visible version：

1. **版本定位**（新增）：FE 根据指定的 timestamp 或 GTID，定位目标 Table Version，获取该版本下各 Physical Partition 的 visible version 映射
2. **构建历史查询表**（新增）：基于当前表的元数据，用目标版本的 visible version 覆盖各 Physical Partition 的版本号，移除目标版本中不存在的 partition。如果发现断链（partition 已被 drop 或 tablet 已 reshard），返回错误
3. **按版本 scan**（修改）：规划 scan range 时，对每个 Physical Partition 使用目标版本的 visible version 而非当前最新版本
4. **后续流程不变**：CN 按指定 version 从对象存储读取对应的 tablet metadata 和数据文件，执行 scan 并返回结果

简言之，PITQ 只改变了"读哪个版本"，CN 侧的读取和计算逻辑不需要修改。

### 5.3 MVCC 长短期方案下的差异

PITQ 的查询流程在短期和长期 MVCC 方案下**基本一致**——都是定位版本 → 构建历史 OlapTable → 按版本 scan。差异主要在 MVCC 层（第 4 章已详细讨论），对 PITQ 查询流程本身影响不大：

- **短期**：只能查询 MV 订阅的 DML 版本，使用最新 schema
- **长期**：可查询任意历史版本（包括 DDL），使用历史 schema

---

## 6. CDC：方案设计

### 6.1 CHANGES 数据格式与 Row Tracking

变更类型和 Net Changes 的概念已在 3.2.1 中介绍。本节补充 CDC 方案设计所需的数据格式和存储层前提。

**CHANGES 数据格式**：每条行级变更由**数据列**（与表的列一致，可只包含需要的列）和**元数据列**组成：

| 元数据列 | 类型 | 含义 |
|:--------|:-----|:-----|
| CHANGE_TYPE | TINYINT | 变更类型：INSERT / DELETE / UPDATE_BEFORE / UPDATE_AFTER |
| ROW_ID | BIGINT | 逻辑行标识，同一行的所有变更具有相同的 ROW_ID |
| ROW_VERSION | BIGINT | 产生变更的版本，配对的 UPDATE_BEFORE / UPDATE_AFTER 具有相同的 ROW_VERSION |

**Row Tracking（行追踪）**：存储层为每行数据维护的元信息，是生成上述元数据列的前提能力。

- **ROW_ID**：逻辑行唯一标识。INSERT 时生成全局唯一 ID，UPDATE 后保持不变
- **ROW_VERSION**：行的版本。INSERT 时生成初始版本，UPDATE 后版本增加（不一定连续），具体可用 partition version 或 GTID 表示

### 6.2 核心问题

CDC 的技术难度因表类型而异：

- **明细表、聚合表、只有 INSERT 的主键表**：CHANGES 就是导入产生的增量 rowset，直接顺序 scan 文件即可读取，无特殊挑战
- **主键表有 UPDATE / DELETE**：这是核心难点。UPDATE 产生的旧值（BEFORE）和 DELETE 的旧值需要从历史 segment 中读取。列式存储下，按 ROW_ID 定位旧值意味着随机 IO，在高频小导入场景下可能产生大量随机读

### 6.3 推荐方案：查询时生成 + bitmap vector 辅助 `[已定]`

整体上存在两种方案思路：

| 方案 | 思路 | 优点 | 缺点 |
|:-----|:-----|:-----|:-----|
| **方案 1：导入时生成** | 主键表更新 primary index 时拿到旧 rssid，读取旧值，配对写入 changelog | 利用 PK 已有机制配对 BEFORE/AFTER；查询读取 changelog 效率高 | 读取旧值影响导入性能；changelog 增加存储和维护开销；Net Changes 场景不一定需要保留每次导入的完整 changes |
| **方案 2：查询时生成** | 导入时增加轻量元信息辅助定位变更行，旧值读取推迟到查询时 | 不影响导入性能；无额外存储开销；查询时可攒批读取旧值提高效率；运行时 filter/project | 查询 scan 效率不如直接读 changelog；增加少量元数据开销 |

**推荐方案 2** `[已定]`。核心理由：

1. **导入性能优先**：实时导入场景对延迟和抖动敏感，IVM 本身是异步消费方，对查询延迟的容忍度更高
2. **维护复杂度低**：不引入额外的 changelog 文件生命周期管理

**具体机制**（主键表）：

导入时增加以下 bitmap vector 元数据：
- 旧 segment 的 **delta delete bitmap**：记录本次导入新增的删除标记（当前只记录累积的全量 delete vector，无法知道 delta）
- 旧 segment 的 **update_before bitmap**：标记哪些行是因为 UPDATE 被删除的（区别于 DELETE 操作的删除）
- 新 segment 的 **update_after bitmap**：标记哪些行是 UPDATE 产生的新值（区别于 INSERT 的新行）

查询时，根据 delta rowset 和这些 bitmap vector 推导出每个 segment 需要读取的行及其 change type。关键特性：

- **每个 version 的 changes 只依赖自己 version 的 tablet metadata**，不需要跨 tablet 比对。这意味着即使发生了 tablet reshard，changes 仍然可以正确生成
- segment 之间、segment 内部都可以并行 scan，提高 IO 效率

### 6.4 Update 语义 `[已定]`

支持两种模式，通过配置切换：

- **DELETE + INSERT**：轻量模式，不需要关联 BEFORE 和 AFTER。IVM 使用此模式
- **UPDATE_BEFORE + UPDATE_AFTER**：完整模式，保留 UPDATE 语义。流式计算场景可能需要

> 两种模式在不同下游场景的差异参见附录 E（以 Flink 同步到 Redis 为例）。

### 6.5 Net Changes `[已定]`

**核心思想**：对每个 ROW_ID，根据其最早变更（first_type）和最晚变更（last_type）的类型组合，确定合并后的净输出。

**合并规则概要**（5 条规则）：

| # | first_type | last_type | 输出 | 语义 |
|:--|:-----------|:----------|:-----|:-----|
| 1 | 仅单条变更 | — | 原样输出 | 无需合并 |
| 2 | INSERT | UPDATE_AFTER | 1 条 INSERT（最终值） | 新建后被更新，等价于直接以最终值插入 |
| 3 | INSERT | DELETE | 0 条 | 新建后被删除，变更相互抵消 |
| 4 | UPDATE_BEFORE | UPDATE_AFTER | 2 条：BEFORE（原始值）+ AFTER（最终值） | 多次更新合并为一次 |
| 5 | UPDATE_BEFORE | DELETE | 1 条 DELETE（原始值） | 先更新后删除，等价于直接删除 |

> 完整规则定义、示例数据和 SQL 实现见附录 C。

**实现位置：计算层** `[已定]`

Net Changes 在计算层通过窗口函数完成，利用表按 ROW_ID 分桶的特性避免全局 shuffle（所有 `PARTITION BY row_id` 与分桶键一致，可本地执行）。

选择在计算层而非存储层做的理由：
- 存储层首要任务是并行 scan 提高 IO 效率，在并行 scan 基础上支持保序和合并会引入额外复杂度，不一定比计算层更高效
- 计算层实现更灵活——天然处理 tablet reshard 后同一 row 的 changes 来自不同 tablet 的场景（此时不能用 local shuffle，但 reshard 频率低，可接受）
- 不同场景对 Net Changes 的需求不同（IVM 需要，审计不需要），在计算层做更容易按需选择

### 6.6 排序 `[已定]`

存储层 scan 返回的 CHANGES 数据在以下维度上**无序**：
- Row 之间无序
- 同一 version + row 下的 UPDATE_BEFORE / UPDATE_AFTER 之间无序，且不保证相邻
- 一次消费多个 version 时，version 之间无序

如果下游需要保序（如流式计算场景），在计算层按 `(row_version, row_id, change_type)` 排序即可。IVM 作为批处理不需要保序。

### 6.7 小文件优化思路 `[待讨论]`

**场景**：高频实时导入 + MV 刷新间隔较大（如小时级），两次刷新之间会产生大量小文件。

**思路**：在 IVM 只需要 Net Changes 且 UPDATE = DELETE + INSERT 的假设下，可以直接比较 old version 和 new version 的文件——new version 经过 compaction 后小文件数量少，scan 效率高。对于 `(ROW_ID, ROW_VERSION)` 相同的行（carry-over row，在新旧版本中都存在且未变化），通过 ROW_VERSION deduplication 过滤。

**局限**：聚合表 compaction 后数据已 aggregate，无法通过 snapshot diff 获取变更。

### 6.8 系统接口

**存储层接口**：

```
TabletChangesReader : ChunkIterator
  输入：读取的列、predicates、version range [start_version, end_version)
  输出：Chunk（数据列 + 元数据列 CHANGE_TYPE / ROW_ID / ROW_VERSION）
```

**与查询层对接**：
- Plan 节点：`OlapChangesScanNode`
- 执行层：`ConnectorScanNode` / `ConnectorScanOperator`（ConnectorType = `OLAP_CHANGES`）+ `OlapChangesDataSource`
- 如果 tablet 支持并行读取 CHANGES，可以有多个实例并行 scan，每个负责一部分

**用户接口（长期）** `[暂不展开]`：

```sql
-- 按 timestamp 或 version 查询变更
SELECT * FROM tbl CHANGES FROM VERSION v1 TO v2;
SELECT * FROM tbl CHANGES FROM TIMESTAMP t1 TO t2;

-- STREAM 对象，自动管理消费进度
CREATE STREAM stream ON tbl;
SELECT * FROM stream;
INSERT INTO target_tbl SELECT * FROM stream;

-- SDK/RPC 对接 Flink/Spark（PULL 模式）
```

**增量数据统计信息** `[待讨论]`：
- 动态查询方式：从 tablet metadata 收集（删除行数、新增行数、文件数等）
- 导入结果附带方式：在 FE 缓存，可先不持久化

---

## 7. 分阶段 Roadmap

### Phase 1：IVM 最小集（短期）

**目标**：以最小改动支撑 IVM 增量刷新，快速交付。

| 模块 | 范围 | 优先级 |
|:-----|:-----|:------|
| **Table Version** | 基于 GTID，仅记录 DML。TableVersionHistory 短窗口内存存储 + EditLog 持久化 | P0 |
| **PITQ** | 仅 MV 引用的版本可查；最新 schema；Java API | P0 |
| **CDC** | 仅 MV 引用范围 `[oldVersion, headVersion)` 的 CHANGES；DELETE + INSERT 语义 | P0 |
| **Row Tracking** | 存储层为每行维护 ROW_ID + ROW_VERSION | P0 |
| **Net Changes** | 计算层窗口函数实现 | P0 |
| **Vacuum 协同** | `minRetainVersion` 保护窗口内版本 | P0 |
| **断链降级** | partition drop/truncate、tablet reshard、INSERT OVERWRITE 等触发断链，IVM 回退全量刷新 | P0 |

**假设与约束**：
- 仅针对存算分离（Cloud-Native）表
- 不涉及已删除 Tablet 的保留
- 不修改现有 CatalogRecycleBin 和 StarMgrMetaSyncer 机制

### Phase 2：增强能力（中期）

**目标**：扩展 PITQ 和 CDC 的操作覆盖范围，提供 CDC SQL 接口。

| 模块 | 范围 |
|:-----|:-----|
| **PITQ** | 支持 drop/truncate partition、tablet reshard 后的历史版本查询——协调 CatalogRecycleBin 保留、StarMgrMetaSyncer 延迟清理旧 Tablet |
| **CDC** | 支持 INSERT OVERWRITE |
| **小文件优化** | 高频导入场景基于 snapshot diff 的 CDC 优化 |
| **CDC SQL 接口** | CHANGES 语句（`SELECT ... FROM tbl CHANGES FROM VERSION v1 TO v2`） |
| **增量统计** | 增量数据统计信息收集与暴露 |

### Phase 3：通用能力（长期）

**目标**：完整的 Time Travel 产品能力和通用 CDC 能力，与业界对齐。

| 模块 | 范围 |
|:-----|:-----|
| **Meta MVCC** | 统一的元数据多版本机制——保留历史 schema、数据分片拓扑，支持按历史 schema 查询 |
| **长周期保留** | 天级保留 + 历史元数据冷存储 |
| **Time Travel SQL** | `SELECT ... FROM t FOR TIMESTAMP AS OF <ts>` |
| **数据恢复** | Copy 恢复 → CLONE → ROLLBACK，分期引入 |
| **完整 CDC 语义** | UPDATE_BEFORE + UPDATE_AFTER 完整模式 |
| **STREAM 对象** | 自动管理消费进度的 CDC 对象 |
| **流式对接** | SDK/RPC 对接 Flink/Spark Structured Streaming（PULL 模式） |
| **版本可观测** | `SHOW HISTORY FOR TABLE t`——展示版本历史、操作类型、统计信息 |

---

## 8. 待讨论与待验证问题

以下问题在当前阶段无法单方面确定，需要跨团队讨论或技术验证。

### 8.1 Phase 1 范围

| # | 问题 | 背景 | 选项 |
|:--|:-----|:-----|:-----|
| 1 | INSERT OVERWRITE 是否纳入 Phase 1？ | INSERT OVERWRITE 涉及分区替换（数据分片拓扑变更），支持复杂度较高。但部分用户场景频繁使用 INSERT OVERWRITE | A. 不纳入，遇到时降级全量刷新<br>B. 纳入，需额外处理分区替换逻辑 |
| 2 | 聚合表 CDC 是否纳入 Phase 1？ | 聚合表 CDC 逻辑相对简单（直接读增量 rowset），且有真实用户场景（Applovin） | A. 纳入<br>B. 延后到 Phase 2 |

### 8.2 CDC 技术验证

| # | 问题 | 背景 |
|:--|:-----|:-----|
| 3 | bitmap vector 的存储开销是否可接受？ | 主键表每次导入增加 delta delete bitmap、update_before bitmap、update_after bitmap，需要评估在高频导入场景下的额外存储和 IO 开销 |
| 4 | 查询时读取旧值的性能是否满足 IVM 需求？ | 列式存储下按 ROW_ID 定位旧值可能产生随机 IO，需要在实际场景中验证性能 |
| 5 | Net Changes 计算层实现的性能？ | 窗口函数 + local shuffle 的开销需要在大数据量下验证 |

### 8.3 长期方向

| # | 问题 | 背景 |
|:--|:-----|:-----|
| 6 | 长期 Meta MVCC 的技术路线？ | 当前多个 feature 都有元数据多版本需求但各自 workaround。需要讨论统一方案的形态——是基于现有 OlapTable 扩展，还是引入独立的元数据版本存储 |
| 7 | 历史元数据冷存储方案？ | FE 内存压力是长期痛点。可行方案包括使用内表作为冷存储、使用对象存储等 |
| 8 | Row Tracking 的具体实现方案？ | ROW_ID 的全局唯一性保证、ROW_VERSION 的具体取值（partition version vs GTID）等实现细节需要存储团队确认 |

---

## 附录

### 附录 A：业界 Time Travel 产品能力对比

| 维度 | Snowflake | Databricks (Delta Lake) | Apache Iceberg | BigQuery |
|:-----|:----------|:-----------------------|:---------------|:---------|
| **时间点查询语法** | `AT(TIMESTAMP\|OFFSET\|STATEMENT)` / `BEFORE` | `TIMESTAMP AS OF` / `VERSION AS OF` / `@ts` / `@vN` | `TIMESTAMP AS OF` / `VERSION AS OF` / Branch / Tag | `FOR SYSTEM_TIME AS OF` |
| **默认保留时长** | 1 天（Enterprise+ 最大 90 天） | 7 天（受 VACUUM 控制） | 5 天（需显式 `expire_snapshots`） | 7 天（不可延长） |
| **查询使用的 Schema** | 当前 schema | 默认当前 schema（Column Mapping 下可用历史 schema） | Snapshot/Tag→历史 schema；Branch→当前 schema | 当前 schema |
| **数据恢复** | UNDROP + 零拷贝 CLONE + CTAS | RESTORE（同表回滚）+ CLONE + MERGE | rollback_to_snapshot + set_current_snapshot + cherrypick | Copy + Table Snapshot（零拷贝只读）+ Table Clone |
| **CDC 机制** | CHANGES clause（无状态）+ Streams（有状态事务性推进） | Change Data Feed（需显式启用，不可追溯） | `create_changelog_view`（支持 net_changes / compute_updates） | APPENDS / CHANGES 函数（无状态） |
| **CDC 元数据列** | METADATA$ACTION / METADATA$ISUPDATE / METADATA$ROW_ID | _change_type / _commit_version / _commit_timestamp | _change_type / _change_ordinal / _commit_snapshot_id | _CHANGE_TYPE |
| **支持的 DML/DDL** | INSERT / UPDATE / DELETE / MERGE / TRUNCATE | INSERT / UPDATE / DELETE / MERGE / INSERT OVERWRITE / TRUNCATE | INSERT / INSERT OVERWRITE / UPDATE / DELETE / MERGE / TRUNCATE | INSERT / UPDATE / DELETE / MERGE / TRUNCATE |

### 附录 B：Table State 变更操作完整分类

| 操作 | 影响的 State 组件 | 层面 |
|:-----|:-----------------|:-----|
| INSERT INTO | 数据 | 用户 |
| INSERT OVERWRITE | 数据分片 + 数据 | 用户 |
| DELETE FROM / UPDATE | 数据 | 用户 |
| STREAM LOAD / BROKER LOAD / ROUTINE LOAD | 数据 | 用户 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN (Fast SC) | 表定义 | 用户 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN (非 Fast SC) | 表定义 + 数据分片 + 数据 | 用户 |
| ALTER TABLE ORDER BY | 表定义 + 数据分片 + 数据 | 用户 |
| CREATE/DROP INDEX | 表定义 + 数据分片 + 数据 | 用户 |
| ALTER TABLE ADD PARTITION | 表定义 + 数据分片 | 用户 |
| ALTER TABLE DROP PARTITION | 表定义 + 数据分片 + 数据 | 用户 |
| TRUNCATE TABLE / PARTITION | 数据分片 + 数据 | 用户 |
| 表达式分区合并 | 表定义 + 数据分片 + 数据 | 用户 |
| ALTER TABLE DISTRIBUTED BY (修改桶数) | 表定义 + 数据分片 + 数据 | 用户 |
| ALTER TABLE RENAME / SET / MODIFY COMMENT | 表定义 | 用户 |
| Compaction | 数据 | 系统 |
| Tablet Reshard | 数据分片 | 系统 |
| Physical Partition 新增 | 数据分片 | 系统 |

> Table State 组件说明：**表定义** = schema、分区定义、分布定义、索引等（FE）；**数据分片** = Physical Partition、Tablet 实例（FE）；**数据** = 对象存储上的数据文件。**系统操作**不改变用户可见的数据或定义，仅改变物理组织。

### 附录 C：Net Changes 合并规则

#### 规则定义

变更类型编码：`0` = INSERT，`1` = DELETE，`2` = UPDATE_BEFORE，`3` = UPDATE_AFTER。配对的 UPDATE_BEFORE 和 UPDATE_AFTER 共享相同的 `row_version`。

对每个 `row_id`，根据 `row_version` 确定：
- **first_type**：最小 `row_version` 处的变更类型，取 `MIN(change_type)`
- **last_type**：最大 `row_version` 处的变更类型，取 `MAX(change_type)`

**规则 1（单条变更）**：若某个 `row_id` 下只有一条变更（只可能是 INSERT 或 DELETE），原样输出。

**规则 2-5（多条变更合并）**：

| # | first_type | last_type | 输出 | 语义 |
|:--|:-----------|:----------|:-----|:-----|
| 2 | INSERT (0) | UPDATE_AFTER (3) | 1 条 `(row_id, max_ver, INSERT, max_after_val)` | 新建后被更新，净效果等价于以最终值直接插入 |
| 3 | INSERT (0) | DELETE (1) | 0 条 | 新建后被删除，变更相互抵消 |
| 4 | UPDATE_BEFORE (2) | UPDATE_AFTER (3) | 2 条：`BEFORE(min_before_val)` + `AFTER(max_after_val)` | 多次更新合并，净效果等价于从原始值更新到最终值 |
| 5 | UPDATE_BEFORE (2) | DELETE (1) | 1 条 `(row_id, max_ver, DELETE, min_before_val)` | 先更新后删除，净效果等价于直接删除，val 携带原始值 |

> 规则 4 和 5 中，输出记录的 `row_version` 统一使用 `max_ver`，确保配对的 UPDATE_BEFORE / UPDATE_AFTER 具有相同版本。

#### 示例

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

#### SQL 实现（窗口函数方案）

利用表按 `row_id` 分桶的特性，所有 `PARTITION BY row_id` 可本地执行，无需全局 shuffle。

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

本附录帮助理解 4.4 短期方案的数据结构和使用方式。

#### 1. 版本标识

```java
public class OlapTable {
    // 表级版本 ID，以 GTID 表示，每次 DML publish 后更新
    @SerializedName(value = "versionId")
    private long versionId = -1;
}
```

#### 2. Table State：一个版本下的表状态快照

Table State 记录某个版本下各 Physical Partition 的 visible version。
IVM 最小方案不保留历史 schema 和数据分片拓扑，Partition 信息和 tablet id 从当前 `OlapTable` 获取。

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

#### 3. Delta State：两个版本之间的差异

Delta State 由 base 和 head 两个 TableState 对比推导得出（利用 visible version 连续递增的性质），
描述每个 Physical Partition 的增量版本区间，CDC 据此生成行级变更。

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

#### 4. 版本管理：MV 订阅模式

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

#### 5. 查询辅助

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

#### 6. IVM 刷新流程

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

**方案 A：UPDATE_BEFORE + UPDATE_AFTER**

```
CDC 产出:     -U(1, u1, 100)          +U(1, u1, 200)
Lookup Join:  -U(1, "张三", 100)      +U(1, "张三", 200)
Redis:                                SET order:1 {张三, 200}
```

Redis 操作 1 次 SET。数据始终可见，从 `{张三, 100}` 原子变为 `{张三, 200}`。

**方案 B：DELETE + INSERT**

```
CDC 产出:     -D(1, u1, 100)          +I(1, u1, 200)
Lookup Join:  -D(1, "张三", 100)      +I(1, "张三", 200)
Redis:        DEL order:1             SET order:1 {张三, 200}
```

Redis 操作 1 次 DEL + 1 次 SET。DEL 和 SET 之间存在时间窗口，`order:1` 短暂不存在。

| 对比维度 | 方案 A (UPDATE) | 方案 B (DELETE+INSERT) |
|:--------|:---------------|:---------------------|
| Redis 写入次数 | 1 | 2 |
| 数据连续性 | 始终可见 | 短暂消失 |
| Dashboard 查询 | 平滑更新 | 可能闪烁（查到空值） |
| Redis QPS | N | 2N |

> 方案 B 在高并发场景下，下游 `GET order:1` 有概率拿到 `nil`。对于 IVM 场景（批处理，所有变更处理完才原子可见）这不是问题，但对于流式实时同步场景需要注意。
