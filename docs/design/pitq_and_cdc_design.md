[Proposal] Point-in-time Query and Change Data Capture

---

## 1. 摘要

**问题**：StarRocks 计划在存算分离内表上支持增量物化视图（IVM）。IVM 增量刷新依赖两项底层能力——**历史版本查询（Point-in-time Query, PITQ）** 和 **变更数据捕获（Change Data Capture, CDC）**。当前系统不具备这两项能力。

**长期产品价值**：PITQ 和 CDC 不仅服务于 IVM，也是 Time Travel 查询、流式计算、数据同步、合规审计等场景的基础能力。Snowflake、Databricks、Iceberg、BigQuery 等系统均已具备。支撑这些能力是 StarRocks 作为分析型数据库长期竞争力的一部分。

**本文目标**：PITQ 和 CDC 共同依赖表级 MVCC 能力，本文围绕 MVCC、PITQ、CDC 三条线，从 IVM 最小需求出发设计方案，并制定从 IVM 专用到通用能力的分阶段演进 roadmap。

---

## 2. 动机

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
| **用户接口** | Java API——Analyze 阶段构建 Plan | SQL 接口——`FOR TIMESTAMP/VERSION AS OF`，用户通过时间戳/ VERSION ID 指定要读取的版本 |
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
| **Update 语义** | DELETE + INSERT 即可，不需要将 BEFORE/AFTER 关联 | 需要 UPDATE_BEFORE + UPDATE_AFTER 完整语义（参见附录 E 示例） |
| **消费粒度** | 保证事务语义，以版本为粒度批量消费，可以一次消费多个版本的变更 | 可以按版本批量或按 record 粒度消费 |
| **顺序保证** | 不需要——IVM 本质是批处理，中间结果不可见，对顺序依赖弱 | 一些场景需要严格有序，否则影响正确性，比如流计算场景|
| **消费接口** | Java API——Analyze 阶段构建 Plan | SQL 或 SDK（对接 Flink/Spark） |

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

基于上述分析，IVM 对 PITQ 和 CDC 的最小需求如下：

**PITQ**：
- 覆盖 DML（除 INSERT OVERWRITE）
- 使用最新 schema，不需要保留历史 schema
- Java API，不依赖 SQL 接口
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
- 假设 FE 内存能存放所有历史元数据（包括 StarManager），如果 Retention 时间长，并且 DML/DDL 频率高，内存压力大 (**重要**)
- 均是 FE 级别的 Retention 配置，没有表级配置

### 4.3 长期方案：完整的表级 MVCC (Time Travel)

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
- Time Travel 场景面向用户暴露上述 SQL 接口

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

CDC（Change Data Capture）的目标是捕获两个版本之间的行级数据变更。变更类型和 Net Changes 等基本概念已在 3.2.1 中定义，支持的表类型与操作范围详见 3.2.3。

### 6.1 CHANGES 数据格式

每条行级变更由**数据列**（与表的列一致，可只包含需要的列）和**元数据列**组成：

| 元数据列 | 类型 | 含义 |
|:--------|:-----|:-----|
| CHANGE_TYPE | TINYINT | 变更类型编码：INSERT(0) / DELETE(1) / UPDATE_BEFORE(2) / UPDATE_AFTER(3) |
| ROW_ID | BIGINT | 逻辑行标识，同一行的所有变更具有相同的 ROW_ID |
| ROW_VERSION | BIGINT | 产生变更的版本，配对的 UPDATE_BEFORE / UPDATE_AFTER 具有相同的 ROW_VERSION |

**Row Tracking（行追踪）**：存储层为每行数据维护的元信息，是生成上述元数据列的前提能力。

- **ROW_ID**：逻辑行唯一标识。INSERT 时分配全局唯一值，UPDATE 后保持不变，DELETE 后不再复用。Net Changes 依赖 ROW_ID 做同行变更合并
- **ROW_VERSION**：行版本。INSERT 时生成初始版本，UPDATE 后版本递增（不一定连续）。Update 配对和 Net Changes 排序依赖 ROW_VERSION

> Row Tracking 的具体生成方案、唯一性保证、存储格式等在单独文档中设计，本文假设存储层已具备此能力。

### 6.2 用户查询接口

**CHANGES 语法**

- 手动指定 offset，查询任意范围的 CHANGES
- 适用于 ad-hoc 查询、debug 排查

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

**STREAM 对象**

- 自动管理消费进度，用于命令式构建增量 ETL pipeline，通常需要配合 Task 调度框架定期触发。相比 IVM 声明式方式，STREAM 更复杂但也更灵活，适合 IVM 无法表达的复杂增量逻辑
- 参考 Snowflake STREAM https://docs.snowflake.com/en/sql-reference/sql/create-stream

```sql
CREATE STREAM my_stream ON tbl;

-- 查询 offset 到最新版本之间的 CHANGES
SELECT * FROM my_stream;

-- DML 中消费 stream，执行成功后自动推进 offset
INSERT INTO target_tbl SELECT * FROM my_stream;
```

**SDK / RPC**
- 对接 Flink / Spark Stuctured Streaming，客户端通过 SDK 与 StarRocks 交互。

### 6.3 能力范围与约束

CDC 引擎本身按通用能力设计——Update 语义（DELETE+INSERT / UPDATE_BEFORE+UPDATE_AFTER）、Net Changes、排序都通过参数配置适配不同场景，不需要针对 IVM 做特殊裁剪。

实际的能力边界来自 MVCC：短期 MVCC（4.4）只覆盖 DML，因此 CDC 也只覆盖 DML，IVM 遇到不支持场景降级全量刷新；长期 MVCC（4.3）支持后，CDC 能力自然扩展到 DDL 和任意版本区间。

接口上，先支持 CHANGES 语法——它是所有消费方式的基础，目前只用于 debug 和问题排查；STREAM 对象和 SDK/RPC 暂不支持，后面按需支持。

| 维度 | 能力范围 |
|:-----|:--------|
| **操作类型** | INSERT / UPDATE / DELETE / 各类 LOAD；不含 INSERT OVERWRITE 和 DDL（受限于短期 MVCC） |
| **表类型** | 明细表（INSERT）、主键表（INSERT / DELETE / UPDATE_BEFORE / UPDATE_AFTER）、聚合表（INSERT，aggregate 后语义）；更新表不支持 |
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

**CN 要解决的问题**：如何从 tablet 的版本区间中高效生成行级 changes。明细表和聚合表只有 append，直接读 delta rowset 即可；**主键表是核心难点**——需要定位哪些行是 INSERT / DELETE / UPDATE，并从历史 segment 中读取旧值（DELETE 和 UPDATE_BEFORE），列式存储下这意味着随机 IO。

### 6.5 FE 侧：确定每个 Tablet 的变更版本区间

FE 的任务是从两个 TableState（old 和 new）中推导出"哪些 tablet 有变更、每个 tablet 需要读哪个版本范围"，然后封装为 scan range 下发给 CN。

**Diff 思路**：按 logical partition → physical partition 逐层比较 old 和 new 两个 TableState 中各 physical partition 的 visible version。version 相同则跳过，version 不同则该 partition 下所有 tablet 需要读取 `(oldVisibleVersion, newVisibleVersion]` 区间的变更。新增的 partition 从 version 0 开始读取；被删除的 partition 在短期方案中触发 fallback 全量刷新。

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

Diff 结果：

| Tablet | Version Range | 说明 |
|:-------|:-------------|:-----|
| T1, T2 | (5, 8] | P1 有变更，覆盖两次导入的 3 个 version |
| T3, T4 | (3, 4] | P2 有变更，覆盖一次导入 |
| T5, T6 | 跳过 | P3 无变化 |

**Tablet Reshard**：如果 physical partition 在版本区间内发生了 reshard，同一个 physical partition 会存在多组 tablet，每组覆盖部分版本区间：

```
PP1 在 version 6 发生 reshard:
  旧 tablets [T1, T2], 覆盖 version (5, 6]
  新 tablets [T1', T2'], 覆盖 version (6, 8]
```

FE 将两组版本区间都下发，CN 分别读取后在计算层合并。

### 6.6 CN 侧：从版本区间生成行级 Changes

#### 6.6.1 明细表与聚合表

**明细表和聚合表**只有 append 操作，读取 `(V_old, V_new]` 范围内的 delta rowset，所有行标记为 INSERT。聚合表的 delta rowset 存储的是 aggregate 后的结果，CHANGES 也是聚合后的语义。

**如何找到 delta rowset**：

- 目标：从 tablet metadata 中找到 `(V_old, V_new]` 范围内所有导入产生的 delta rowset
- **跳过 compaction**：compaction 产生的 rowset 不包含新数据，只是合并，需要跳过
- **回溯查找**：compaction 会合并旧 rowset，因此 `V_new` 对应的 tablet metadata 可能不包含所有版本的 delta rowset。被 compaction 合并掉的 delta rowset 需要查找更早版本的 tablet metadata，最差情况需要遍历 `(V_old, V_new]` 范围内所有版本的 tablet metadata
- **遇到 DELETE 降级**：如果遇到 DELETE 操作产生的 rowset，返回特殊错误码给上层

**示例**：查询 `(2, 6]` 范围的 changes

```
version 3: tablet metadata 包含 [rowset-2(v2, LOAD), rowset-3(v3, LOAD)]
version 4: tablet metadata 包含 [rowset-2(v2, LOAD), rowset-3(v3, LOAD), rowset-4(v4, LOAD)]
version 5: tablet metadata 包含 [rowset-5(v5, COMPACTION)]
           ← v5 发生 compaction，rowset-2 ~ rowset-4 被合并为 rowset-5
version 6: tablet metadata 包含 [rowset-5(v5, COMPACTION), rowset-6(v6, LOAD)]
```

从 `V_new=6` 的 tablet metadata 开始：
- rowset-6(v6, LOAD) → 在范围内，读取
- rowset-5(v5, COMPACTION) → 在范围内，但是 COMPACTION 跳过
- 回溯到 version 4 的 tablet metadata，找到 rowset-3(v3, LOAD) 和 rowset-4(v4, LOAD) → 读取
- 找到所有 delta rowset，不需要回溯 version 3 tablet metadata，中止

最终读取 rowset-3、rowset-4、rowset-6，所有行标记为 INSERT。

#### 6.6.2 主键表

主键表支持 INSERT / UPDATE / DELETE，是 Changes 生成的核心难点。需要解决两个子问题：**定位**（如何知道哪些行是 INSERT / DELETE / UPDATE）和**取值**（DELETE 和 UPDATE 的旧值散落在历史 segment 中，如何高效读取）。

##### 方案对比

**方案 A：导入时生成 Changelog**
- 思路：PK Index 更新时读旧值，配对写入独立的 changelog 文件
- 优点：查询效率高（顺序读 changelog）
- 缺点：
  - 读取并持久化旧值影响导入性能
  - changelog 增加存储和维护开销，高频导入还会面临小文件问题
  - Net Changes 场景中间版本 changelog 最终被合并，提前生成是浪费
  - 存储所有列的值，而查询可能只需要部分列

**方案 B：Delete Vector Diff**
- 思路：查询时定位并读取旧值，Delta rowset 中的行是新增或更新后的行，其它 segment 比较导入前后两个版本的 delete vector，新增被标记删除的行（差集）就是被删除或被更新的行
- 优点：
  - 无导入开销，概念简单，不需要额外存储
  - 查询时合并多个导入对同一个 segment delete 的读取，减少随机 IO
- 缺点：
  - **无法区分 DELETE 和 UPDATE**——delete vector 只记录"行被标记删除"，无法判断是 DELETE 还是 UPDATE 导致的旧行标记删除
  - 要区分就必须拿被标记删除的行去新 segment 做 PK 匹配，复杂且代价高

**方案 C：Changes Vector（推荐）**
- 思路
  - 导入时在 PK Index 更新阶段记录 RoaringBitmap 标记每行 change type，和 delete vector 存在一起
  - 查询时据此精确定位旧 segment 是 delete 还是 update_before，新 segment 是 insert 还是 update_after
- 优点：
  - 可以区分 DELETE 和 UPDATE，无需额外的 PK 匹配
  - 导入增加的额外开销极低
  - 查询时合并多个导入对同一个 segment delete 的读取，减少随机 IO
  - 变更信息自包含，不依赖跨 version diff
- 缺点：scan 效率略低于方案 A

**选择方案 C** 的理由：

1. 导入性能优先，对导入影响小
2. 能区分 DELETE 和 UPDATE
3. 查询可以跨版本合并 segment delete，减少随机 IO
3. 实现复杂度低

##### Changes Vector 示例

**核心思路**：导入时 PK Index 更新过程中已经知道每行是 INSERT / UPDATE / DELETE，在这个时机顺便记录三个轻量 bitmap（changes vector），查询时据此精确定位每行的变更类型和位置：

| Changes Vector | 记录位置 | 含义 |
|:-------------|:--------|:-----|
| **delete_type_vector** | 旧 segment | 被 DELETE 操作删除的行（区别于 UPDATE 导致的删除） |
| **before_type_vector** | 旧 segment | 被 UPDATE 的行（旧值位置） |
| **after_type_vector** | 新 segment | UPDATE 结果写入的行（新值位置），其余行为 INSERT |

三者的关系：

- 旧 segment 的 `delete vector diff` = `delete_type_vector` ∪ `before_type_vector`（互斥）
- 新 segment 中的行 = INSERT 行 ∪ UPDATE_AFTER 行（由 `after_type_vector` 区分）

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

查询 version 6 的 changes 时：

- 读 S0 中被 `delete_type_vector[v6]` 和 `before_type_vector[v6]` 标记的行（A 和 B 一起读取），根据所在的 vector 设置 CHANGE_TYPE：B → DELETE，A → UPDATE_BEFORE
- 读 S1 全量顺序读取（A 和 E 一起读取），根据 `after_type_vector[v6]` 设置 CHANGE_TYPE：A 在 vector 中 → UPDATE_AFTER，E 不在 → INSERT

**跨版本合并优化**：CDC 窗口覆盖多个版本时，同一旧 segment 可能被多个版本的 changes vector 引用，逐版本读取会产生多次独立 IO。优化方式：将同一 segment 上所有版本的 bitmap 合并（OR）后一次批量读取，再按原始 bitmap 拆分回各版本的 changes。

```
CDC 窗口 (5, 8]，S0 被三个版本引用：
  version 6: before_type_vector = {A}
  version 7: delete_type_vector = {C}
  version 8: before_type_vector = {D}

逐版本读取 → 3 次独立 IO
合并后读取 → merged = {A} ∪ {C} ∪ {D} = {A, C, D}，1 次批量读取 S0 的 A, C, D 三行
           → 再按原始 vector 拆分：A → v6 UPDATE_BEFORE, C → v7 DELETE, D → v8 UPDATE_BEFORE
```

---

#### 6.6.3 Net Changes

##### 6.6.3.1 Motivation

对于主键表，当 CDC 窗口覆盖多个版本时，同一行可能有多条变更（如先 UPDATE 再 UPDATE）。如果不合并，下游需要逐条处理所有中间变更，数据量大且计算浪费。Net Changes 将同一 `row_id` 下的多条变更合并为最小等价变更。

**示例**：以 IVM 为例，表 A(id, score) 与表 B(id, name) 做 JOIN，MV 定义为 `SELECT id, name, score FROM A JOIN B ON A.id = B.id`。A 的 id=1 行经历两次 UPDATE（score: 1→2→3）：

```
不合并（原始 changes）:
  A changes: -(1, 1), +(1, 2), -(1, 2), +(1, 3)           ← 4 条
  IVM 处理: 每条 change 都需要 join B → -(1, n, 1), +(1, n, 2), -(1, n, 2), +(1, n, 3)
  顺序 merge into MV                                       ← 4 次 MV 更新

Net Changes 合并后:
  A changes: -(1, 1), +(1, 3)                               ← 2 条
  IVM 处理: -(1, n, 1), +(1, n, 3)
  顺序 merge into MV                                       ← 2 次 MV 更新，结果相同
```

Net Changes 将 4 条中间变更合并为 2 条，IVM 的 join 计算量和 MV 更新次数减半，结果等价。


##### 6.6.3.2 合并规则

对每个 `row_id`，根据 `row_version` 确定最早变更类型（first_type）和最晚变更类型（last_type），然后按规则合并。变更类型编码：`0` = INSERT，`1` = DELETE，`2` = UPDATE_BEFORE，`3` = UPDATE_AFTER。

**规则 1（单条变更）**：若某个 `row_id` 下只有一条变更（只可能是 INSERT 或 DELETE），原样输出。

**规则 2-5（多条变更合并）**：

| # | first_type | last_type | 输出 | 语义 |
|:--|:-----------|:----------|:-----|:-----|
| 2 | INSERT (0) | UPDATE_AFTER (3) | 1 条 INSERT（最终值） | 新建后被更新，等价于直接以最终值插入 |
| 3 | INSERT (0) | DELETE (1) | 0 条 | 新建后被删除，变更相互抵消 |
| 4 | UPDATE_BEFORE (2) | UPDATE_AFTER (3) | 2 条：BEFORE（原始值）+ AFTER（最终值） | 多次更新合并为一次 |
| 5 | UPDATE_BEFORE (2) | DELETE (1) | 1 条 DELETE（原始值） | 先更新后删除，等价于直接删除 |

> 规则 4 和 5 中，输出记录的 `row_version` 统一使用 `max_ver`，确保配对的 UPDATE_BEFORE / UPDATE_AFTER 具有相同版本。


##### 6.6.3.3 实现

**原理概述**：Net Changes 的实现分为两层——存储层做廉价快筛，计算层做精确兜底。

- **存储层**：对同一个 segment，将 CDC 窗口内所有版本的 changes vector 合并为两个 bitmap——**enter**（insert ∪ after_type_vector，即"进入"该 segment 的行）和 **leave**（delete_type_vector ∪ before_type_vector，即"离开"该 segment 的行）。对两者做 XOR：同时出现在 enter 和 leave 中的行说明"进了又出了"，属于可抵消的中间态，从 changes vector 中移除，后续不再读取。
- **计算层**：Compaction 会将多个 segment 合并为新 segment，导致某些行的"进入"和"离开"分散在不同 segment 上，存储层的 segment 内 XOR 无法发现这些配对，形成**漏网**。计算层通过窗口函数对所有 segment 输出的 changes 做全局 Net Changes 合并（合并规则见 6.6.3.2），利用表按 `ROW_ID` 分桶的特性避免全局 shuffle（所有 `PARTITION BY row_id` 与分桶键一致，可本地执行）。

**为什么在计算层而非存储层兜底**：跨 segment 的抵消本质上是按 `row_id` 做 group by——需要等所有 segment 扫完、将同一行散落在不同 segment 的变更聚到一起、再应用合并规则。这是一个聚合操作，而存储层的执行模型是 per-segment 并行扫描，引入全局聚合会破坏并行性并重复实现计算层已有的能力。计算层天然适合做这件事，且因表按 `ROW_ID` 分桶，窗口函数可本地执行，无网络开销。

两层结合：存储层以极低成本减少大部分数据量，计算层保证无论是否发生 compaction 结果都正确。

---

**完整示例**

以下通过一个覆盖全部 4 种 change type、跨 3 次导入的例子，演示两层过滤的完整流程。

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
|:--|:---------|:-----|:-----:|:-----|
| A | AFTER | BEFORE(v7) | ✓ | 中间态消除；原始 BEFORE 在 S0，最终 AFTER 在 S3，留给计算层 |
| B | AFTER | BEFORE(v5) | ✓ | 中间态消除；原始 BEFORE 在 S0，最终 AFTER 在 S2，留给计算层 |
| C | INSERT | — | ✗ | 无离开，直接通过 |
| D | INSERT | DELETE(v5) | ✓ | 生了又死了，完全抵消（规则 #3），0 条输出 |
| E | INSERT | — | ✗ | 同 C，直接通过 |
| F | INSERT | DELETE(v7) | ✓ | 完全抵消（规则 #3），0 条输出 |

存储层效果：**S1 从 6 行降至 2 行**（C 和 E），减少 67% 的读取量。

**Step 2：Compaction 如何导致漏网**

假设 v5 和 v7 之间发生 compaction，S1 + S2 合并为 S_merged：

```
时间线：
  v3           v5          compaction        v7
  ├── S1 创建 ──┤── S2 创建 ──┤── S1+S2→S_merged ──┤── S3 创建 ──┤
```

v7 时，A 和 F 的数据已在 S_merged 中，变更记录在 S_merged 上而非 S1：

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

| | 无 compaction | 有 compaction |
|:--|:-------------|:-------------|
| 存储层消除行数 | 4 行（A, B, D, F） | 2 行（B, D） |
| 传给计算层行数 | 6 行 | 10 行 |
| 最终结果正确性 | ✓ | ✓（计算层兜底） |

A 和 F 因为"进入"和"离开"分散在不同 segment（S1 vs S_merged），存储层 XOR 看不到配对，**漏网**了。

**Step 3：计算层窗口函数兜底**

以漏网的 **F** 为例，计算层收到：

```
row_id=F, row_version=v3, change_type=0(INSERT)   ← 来自 S1
row_id=F, row_version=v7, change_type=1(DELETE)    ← 来自 S_merged
```

窗口函数计算：`cnt=2, first_type=INSERT(0), last_type=DELETE(1)` → **规则 #3**，0 条输出。存储层没消掉的，计算层消掉了。

以漏网的 **A** 为例，计算层收到 4 条：

```
row_id=A, v3, BEFORE(2), old_val    ← S0
row_id=A, v3, AFTER(3),  mid_val    ← S1（漏网）
row_id=A, v7, BEFORE(2), mid_val    ← S_merged（漏网）
row_id=A, v7, AFTER(3),  new_val    ← S3
```

窗口函数：`first_type=BEFORE(2), last_type=AFTER(3)` → **规则 #4**，输出 `BEFORE(old_val) + AFTER(new_val)`，两次 UPDATE 合并为一次，中间态被 WHERE 过滤。

**最终输出**（无论是否发生 compaction，结果相同）：

```
  A: BEFORE(old) + AFTER(new)    ← 规则 #4，两次更新合并为一次
  B: BEFORE(old) + AFTER(new)    ← 规则 #4，两次更新合并为一次
  C: INSERT(val)                 ← 规则 #1，直接通过
  D: (无输出)                    ← 规则 #3，INSERT+DELETE 抵消
  E: INSERT(val)                 ← 规则 #1，直接通过
  F: (无输出)                    ← 规则 #3，INSERT+DELETE 抵消
```

SQL 实现见附录 C。

---

#### 6.6.4 并行 Scan

大批量导入或高频导入场景下，CDC 窗口内累积的数据量和文件数可能很大；主键表还需要额外读取旧 segment 中的历史值。如果 CDC scan 无法充分并行，就会成为增量刷新链路的瓶颈，抵消 IVM 相对全量刷新的优势。

CDC scan 在三个层面支持并行，前述多项设计选择为此提供了基础：

| 并行层面 | 并行方式 | 关键设计支撑 |
|:---------|:---------|:------------|
| **Tablet 间** | FE 将不同 tablet 的 scan range 分配到不同 CN 节点，tablet 之间完全独立 | FE 按 tablet 粒度下发独立 scan range（6.5） |
| **Segment 间** | 同一 tablet 内，多个 segment 可并行读取。主键表涉及新旧两类 segment，Changes Vector 自包含于 segment，不需要跨 segment 做 PK 匹配，新旧 segment 之间也无依赖 | Changes Vector 自包含设计（6.6.2）；跨版本 bitmap 合并减少旧 segment 调度次数（6.6.2）；Net Changes 存储层 XOR 是 per-segment 操作，不引入全局依赖（6.6.3） |
| **Segment 内** | 同一 segment 按行范围切分并行读取，changes vector 的 RoaringBitmap 结构支持按行范围做子集查询，每个并行实例独立判断 CHANGE_TYPE | 列式存储天然支持；RoaringBitmap 可按行范围切分，无需全局协调 |

此外，Net Changes 的计算层窗口函数利用表按 `ROW_ID` 分桶的特性，可本地执行，无需跨节点 shuffle。

---

#### 6.6.5 小文件优化：Snapshot Diff

##### 问题

6.6.3 的标准 CDC 路径是逐版本、逐 segment 读取 changes vector 生成变更。在高频导入 + 长 CDC 窗口的场景下，这种方式面临 IO 放大问题：

```
场景：每秒导入 1 次，MV 每小时刷新 1 次
  → CDC 窗口内 3,600 个 delta rowset（每次导入产生 1 个）
  → 逐文件读取：3,600 次文件打开 + bitmap 解析 + 行读取
```

每个 delta rowset 可能只有几百行，但文件打开和元数据解析的固定开销不可忽略，累积起来 IO 开销远大于实际有效数据量。

##### 思路：从"逐文件追变更"到"两个快照做 Diff"

核心观察：IVM 只需要 Net Changes（不需要中间过程），而 new 版本经过 compaction 后文件少、数据紧凑。因此可以换一种策略——不逐文件读取 delta rowset，直接比较 old 和 new 两个版本的快照，用集合差运算得出变更：

```
标准路径（逐文件）:
  old ──→ [delta_v1] [delta_v2] ... [delta_v3600] ──→ new
          逐个读取 3,600 个小文件，生成 changes，再做 Net Changes

Snapshot Diff 路径:
  old snapshot
       ↕ diff
  new snapshot (compacted, 少量大文件)
  → 直接产出 Net Changes
```

**前提条件**：只需 Net Changes、Update 使用 DELETE + INSERT 模式（适合 IVM 场景）。

##### Carry-over Row 问题

Snapshot diff 会产生**假变更**——某些行在 old 和 new 中完全一样（从未被修改），但因为 compaction 重新组织了文件布局，同一行在 old 里属于 segment S1、在 new 里属于 S_merged，diff 时会误判为变更。

```
示例：行 X 从未修改（row_version 始终为 v2）

  old snapshot: S1 包含 X (row_id=X, row_version=v2, val=10)
  new snapshot: S_merged 包含 X (row_id=X, row_version=v2, val=10)  ← compaction 搬迁

  Snapshot diff 输出:
    old 侧: -(X, v2, 10)    ← DELETE（误判）
    new 侧: +(X, v2, 10)    ← INSERT（误判）

  实际上 X 没有任何变化，这就是 carry-over row
```

**解决方式**：在计算层通过 `(row_id, row_version)` 去重。同一 `row_id` 若在 old 和 new 中 `row_version` 相同，说明该行未变化，过滤掉：

```sql
-- carry-over row 过滤（概念性 SQL）
SELECT * FROM (
    SELECT *, COUNT(*) OVER (PARTITION BY row_id, row_version) AS dup_cnt
    FROM snapshot_diff
)
WHERE dup_cnt = 1   -- 只保留不重复的行，即真正的变更
```

##### 适用范围与局限

| 条件 | 说明 |
|:-----|:-----|
| **适用表模型** | 明细表、主键表（compaction 不改变行内容）|
| **不适用** | 聚合表（compaction 会 aggregate 行，snapshot 中是聚合后的结果，无法还原原始变更）|
| **compaction 要求** | old 和 new 版本都需经过充分 compaction 才能发挥效果；若 new 版本尚未 compaction，小文件依然多，diff 效率不高 |

##### 与标准 CDC 路径的关系

两种路径互补，可根据场景选择：

| | 标准路径（changes vector） | Snapshot Diff |
|:--|:--------------------------|:-------------|
| 输出 | 完整 changes（支持所有 change type） | 仅 Net Changes |
| IO 模式 | 逐文件顺序读取 | 两次全量 scan（old + new） |
| 最优场景 | CDC 窗口短、delta 文件少 | CDC 窗口长、delta 文件多但 compaction 充分 |
| 适用表模型 | 所有 | 明细表、主键表 |

### 6.7 增量数据统计信息

- **动态查询方式**：从 tablet metadata 收集（删除行数、新增行数、文件数等）
- **导入结果附带方式**：在 FE 缓存，可不持久化

### 6.8 小结

CDC 方案围绕"FE 确定读什么、CN 执行怎么读"的两阶段链路展开，核心设计选择与权衡如下：

| 维度 | 设计选择 | 说明 |
|:-----|:---------|:-----|
| **数据格式** | 数据列 + 三个元数据列（CHANGE_TYPE / ROW_ID / ROW_VERSION） | 依赖存储层 Row Tracking 能力，支撑 Update 语义区分和 Net Changes 合并 |
| **FE 侧** | 逐分区比较 old/new 两个 TableState 的 visible version，推导每个 tablet 的版本区间 | 利用 visible version 连续递增的性质，不需要中间版本的 Meta；Tablet Reshard 场景下发多组版本区间 |
| **CN 侧——明细表/聚合表** | 直接读取 delta rowset，所有行标记为 INSERT | 实现简单，需回溯历史 tablet metadata 跳过 compaction rowset |
| **CN 侧——主键表** | Changes Vector 方案（导入时记录三个轻量 bitmap） | 在导入性能、操作类型区分能力和查询效率之间取得平衡；优于 Changelog（导入开销大）和 Delete Vector Diff（无法区分 DELETE/UPDATE） |
| **Net Changes** | 存储层 XOR 快筛 + 计算层窗口函数兜底 | 存储层以极低成本减少大部分数据量；计算层利用表按 ROW_ID 分桶的特性本地执行，保证无论是否发生 compaction 结果都正确 |
| **小文件优化** | Snapshot Diff（两版本全量快照做集合差） | 适用于高频导入 + 长 CDC 窗口 + 充分 compaction 的场景，与标准路径互补；通过 `(row_id, row_version)` 去重消除 carry-over row |
| **并行能力** | Tablet 间、Segment 文件间、Segment 内部三级并行 | Changes Vector 的 bitmap 结构天然支持按行范围切分，无需全局协调 |
| **用户接口** | 先支持 CHANGES 语法，STREAM 对象和 SDK/RPC 后续按需支持 | CHANGES 是所有消费方式的基础；IVM 通过 Java API 直接构建 Plan |
| **能力边界** | 受限于短期 MVCC，仅覆盖 DML；Update 语义和 Net Changes 通过参数配置 | CDC 引擎本身按通用能力设计，不针对 IVM 做特殊裁剪；MVCC 扩展后能力自然扩展 |

---

## 7. Roadmap

### Phase 1：IVM 最小集（短期）

**目标**：以最小改动支撑 IVM 增量刷新，快速交付。

**MVCC**：

| 模块 | 范围 | 优先级 |
|:-----|:-----|:------|
| **Table Version** | 基于 GTID，仅记录 DML。`OlapTable` 增加表级 `versionId`，MV 订阅模式管理版本生命周期（4.4） | P0 |
| **Vacuum 协同** | `minRetainVersion` 保护窗口内版本的 tablet metadata 和数据文件不被清理 | P0 |
| **断链降级** | partition drop/truncate、tablet reshard、INSERT OVERWRITE 等数据分片拓扑变更触发断链，IVM 回退全量刷新 | P0 |

**PITQ**：

| 模块 | 范围 | 优先级 |
|:-----|:-----|:------|
| **历史版本查询** | 仅 MV 引用的版本可查；使用最新 schema；Java API（5.2） | P0 |

**CDC**：

| 模块 | 范围 | 优先级 |
|:-----|:-----|:------|
| **明细表/聚合表** | 读取 delta rowset，跳过 compaction rowset，回溯历史 tablet metadata（6.6.1） | P0 |
| **主键表** | Changes Vector 方案：导入时记录三个轻量 bitmap（delete_type_vector / before_type_vector / after_type_vector），查询时精确定位变更类型和旧值位置；支持跨版本 bitmap 合并减少 IO（6.6.2） | P0 |
| **Row Tracking** | 存储层为每行维护 ROW_ID + ROW_VERSION，支撑 CDC 元数据列和 Net Changes 合并（6.1） | P0 |
| **Net Changes** | 存储层 per-segment XOR 快筛 + 计算层窗口函数兜底，利用表按 ROW_ID 分桶本地执行（6.6.3） | P0 |
| **CHANGES 语法** | 基础 CDC SQL 接口，用于 debug 和问题排查（6.2、6.3） | P1 |

**假设与约束**：
- 仅针对存算分离（Cloud-Native）表
- 不涉及已删除 Tablet 的保留
- 不修改现有 CatalogRecycleBin 和 StarMgrMetaSyncer 机制

### Phase 2：增强能力（中期）

**目标**：扩展 PITQ 和 CDC 的操作覆盖范围，优化高频导入场景性能。

**MVCC**：

| 模块 | 范围 |
|:-----|:-----|
| **拓扑变更版本保留** | 协调 CatalogRecycleBin 保留、StarMgrMetaSyncer 延迟清理旧 Tablet，支持 drop/truncate partition、tablet reshard 后的版本保留 |
| **版本可观测** | `SHOW HISTORY FOR TABLE t`——展示版本历史、操作类型、保留策略等信息（4.5） |

**PITQ**：

| 模块 | 范围 |
|:-----|:-----|
| **拓扑变更后历史查询** | 支持 drop/truncate partition、tablet reshard 后的历史版本查询，不再触发断链降级 |

**CDC**：

| 模块 | 范围 |
|:-----|:-----|
| **拓扑变更后变更捕获** | 支持 drop/truncate partition、tablet reshard 后的变更捕获，不再触发断链降级 |
| **Snapshot Diff** | 高频导入 + 长 CDC 窗口场景下，基于两版本快照做集合差直接产出 Net Changes，避免逐文件读取小 delta rowset 的 IO 放大；通过 `(row_id, row_version)` 去重消除 carry-over row（6.6.5） |
| **增量统计** | 增量数据统计信息收集与暴露（6.7） |

### Phase 3：通用能力（长期）

**目标**：完整的 Time Travel 产品能力和通用 CDC 能力，与业界对齐。

**MVCC**：

| 模块 | 范围 |
|:-----|:-----|
| **Meta MVCC** | 统一的元数据多版本机制——保留历史 schema、数据分片拓扑，支持按历史 schema 查询（4.3） |
| **长周期保留** | 表级 Retention 配置，天级保留 + 历史元数据冷存储 |

**PITQ**：

| 模块 | 范围 |
|:-----|:-----|
| **Time Travel SQL** | `SELECT ... FROM t FOR TIMESTAMP/VERSION AS OF`，面向用户的 SQL 接口（5.1） |

**CDC**：

| 模块 | 范围 |
|:-----|:-----|
| **STREAM 对象** | 自动管理消费进度的 CDC 对象，配合 Task 调度框架构建增量 ETL pipeline（6.2） |
| **流式对接** | SDK/RPC 对接 Flink/Spark Structured Streaming（PULL 模式） |

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

### 附录 C：Net Changes 示例与 SQL 实现

> 合并规则定义见 6.6.3。

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
