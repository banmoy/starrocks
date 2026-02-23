## StarRocks Time Travel Proposal（Shared-Data / 存算分离）

### 1. 背景与用户价值

Time Travel 的目标是提供**时间点查询（point-in-time query）**能力：让用户能够以"时间戳 / 版本号"读取表在历史某个提交点的历史版本，并以可控的保留策略保存这些可回溯点，从而支撑数据恢复、审计与可复现分析。

- **误操作恢复（数据工程 / DBA）**
  - 误删、误更新、写入坏数据后，需要快速找回"出问题前"的数据状态。
- **报表复现（分析师）**
  - 上周/昨天生成的报表与今天不一致，需要复现当时的输入数据口径以定位差异来源。
- **审计取证（合规）**
  - 需要证明某个时间点的数据状态，并可重复跑出同样结果（提交点一致性）。
- **增量物化视图 IVM**
  - IVM 的共同前提是 **Table 多版本读取**：需要能够读取基表在 `[v_from, v_to]` 的任意版本（至少能读到 base/head 两端），并在需要时读取期间 changes（Δ）。

### 2. 产品调研

| 维度 | Snowflake | Databricks (Delta Lake) | Spark + Apache Iceberg | BigQuery |
|---|-----------|------------------------|----------------------|----------|
| **典型用户场景** | 误操作恢复、数据备份复制、变更分析；Stream + CHANGES 实现增量 ELT | 快照隔离、数据修复、时序查询、分析/ML 复现、归档、生产表实验 | 审计合规（Tag）、WAP 验证、误操作回滚、增量 CDC、可复现分析 | 误操作恢复、报表复现/审计、长期快照归档、开发/测试克隆、增量变更追踪 |
|**支持的 DML 与 DDL** | **DML**：INSERT / UPDATE / DELETE / MERGE。**DDL**：TRUNCATE | **DML**：INSERT / UPDATE / DELETE (PARTITION) / MERGE / INSERT OVERWRITE。**DDL**：TRUNCATE TABLE / SCHEMA EVOLUTION。| **DML**：INSERT / INSERT OVERWRITE / UPDATE / DELETE (PARTITION) / MERGE。**DDL**：TRUNCATE TABLE / SCHEMA EVOLUTION。 | **DML**：INSERT / UPDATE / DELETE (PARTITION) / MERGE。**DDL** TRUNCATE TABLE |
| **时间点查询语法** | `AT(TIMESTAMP\|OFFSET\|STATEMENT => ...)` / `BEFORE(STATEMENT => ...)` | `TIMESTAMP AS OF <expr>` / `VERSION AS OF <v>` / `@ts` / `@vN` | `TIMESTAMP AS OF` / `VERSION AS OF` / `FOR SYSTEM_TIME\|VERSION AS OF` / Branch·Tag 语法 | `FOR SYSTEM_TIME AS OF <timestamp_expr>` |
| **默认/最大保留时长** | 默认 1 天；Standard 最大 1 天，Enterprise+ 最大 **90 天** + 7 天 Fail-safe | 有效默认 **7 天**（受 VACUUM 控制）；无硬性最大上限，但官方不建议长期归档 | 默认 **5 天**（`max-snapshot-age-ms`）；无硬性上限，需显式 `expire_snapshots` | 默认 **7 天**，最大 7 天 + 7 天 Fail-safe（不可查询，仅 Google 客服恢复） |
| **查询使用的 Schema** | 当前 schema | 历史 schema | **Snapshot ID/Timestamp/Tag → 历史 schema**；**Branch → 当前 schema**；通过唯一列 ID 保证演进正确性 | 当前 schema |
| **数据恢复能力** | **UNDROP** ✅（表/Schema/DB）、**零拷贝 CLONE** ✅、CTAS ✅；不支持**同表回滚** | **RESTORE（同表回滚）** ✅（生成新版本，可逆）、Deep/Shallow **CLONE** ✅、INSERT/MERGE 恢复 ✅；**UNDROP** ❌ | **rollback_to_snapshot/timestamp** ✅、**set_current_snapshot** ✅、**cherrypick** ✅、CTAS ✅；**UNDROP** ❌ | Copy 恢复 ✅（bq cp / CTAS）、**Table Snapshot**（零拷贝只读）✅、**Table Clone**（零拷贝可写）✅；**同表 ROLLBACK** ❌、**UNDROP** ❌（需先 copy） |

### 3. 产品定义与范围

先对通用场景的 Time Travel 进行定义，然后在 3.6 中讨论增量物化视图的特殊性

#### 3.1 Time Travel 定义

Time Travel 允许用户访问**过去一段时间**内、**任意一个历史版本**所对应的数据状态。

- **适用对象**：普通内表与异步物化视图；同步物化视图本质是内表的 Rollup Index，内表历史版本也包含了同步 MV 历史状态。
- **版本生成**：当表发生**DML** 或**影响查询结果的 DDL** 时，会形成新的版本，例如：
  - **DML**：`INSERT`、`INSERT OVERWRITE`、`DELETE`、`UPDATE`，以及各类导入（`STREAM LOAD`/`BROKER LOAD`/`ROUTINE LOAD` 等）。
  - **部分影响查询结果的 DDL**：`TRUNCATE TABLE/PARTITION`、`ALTER TABLE ADD/DROP PARTITION`、以及会改变数据可见性或查询语义的表结构变更（如新增/删除列等）。
  - **暂不纳入 `DROP TABLE`**：`DROP TABLE` 属于对象删除，涉及回收站/元数据生命周期等额外复杂度，本期 Time Travel 暂不覆盖；误删表的恢复场景由 `RECOVER`（Recycle Bin）机制覆盖。
- **版本定位**：可以用 **timestamp** 来指定，系统会解析到不晚于该时间点的最近一个版本；若超出保留范围则报错。
  - **对外接口简化**：Time Travel 面向用户侧只暴露 **timestamp**（不提供按 `version` 精确定位），以减少概念负担，提升易用性。
  - **内部可使用更具体的 version**：在系统内部实现中（例如增量物化视图版本推进），可以使用与物理实现相关的 version 标识，进行细粒度控制；该部分不作为用户侧语义的一部分，具体见技术实现部分。
- **时间点查询 Schema 语义**：使用时间点的历史 schema，提供严格的业务一致性语义。

#### 3.2 核心能力

Time Travel 的核心能力由三部分组成：

- **历史版本保留**
  - 通过保留策略决定历史版本的可访问窗口。
- **时间点查询**
  - 支持按 **timestamp** 访问历史版本：`FOR TIMESTAMP AS OF <timestamp>`（解析到不晚于该时间点的最近一次提交点）。
- **数据恢复**
  - **复制恢复到新表（Copy）**：`CTAS` 或 `INSERT INTO SELECT ... FOR ...`（简单通用，但需要数据复制）。
  - **零拷贝恢复到新表（CLONE）**：从指定历史版本克隆出一张新表（仅复制元数据，数据文件共享；实现复杂度高，可分期引入）。
  - **同表回滚（ROLLBACK）**：把表回滚到指定历史版本（单表内操作、速度最快；实现复杂度高，可分期引入）。

支持优先级：
- **P0**：历史版本保留 + 时间点查询 + 复制恢复（Copy），基础能力，增量物化视图高优需求。
- **P1/P2+**：零拷贝恢复到新表（CLONE）、同表回滚（ROLLBACK）等恢复能力，非高优需求，并且实现复杂度高。

#### 3.3 用户接口定义

##### 3.3.1 历史版本保留

- **表粒度保留策略配置**

```sql
-- 建表时指定
CREATE TABLE t (
    -- columns...
)
PROPERTIES ("history_retention" = "30 days");

-- 动态打开
ALTER TABLE t SET ('history_retention' = '30 days');

-- 动态关闭
ALTER TABLE t SET ('history_retention' = '0');
```

- **历史版本可观测性**

```sql
SHOW HISTORY FOR TABLE t;
```

输出字段：`timestamp`、`version`（内部标识）、`operation_type`、`details`（`txn_id`、`rows_changed` 等）等。

##### 3.3.2 时间点查询

- **按 timestamp 访问历史版本**

```sql
SELECT ... FROM t FOR TIMESTAMP AS OF '2026-02-21 10:30:00';
SELECT * FROM t FOR TIMESTAMP AS OF now();
SELECT * FROM t FOR TIMESTAMP AS OF minutes_sub(now(), 10);
```

##### 3.3.3 数据恢复

- **复制恢复到新表（Copy，P0）**

```sql
CREATE TABLE t_recovered AS
SELECT * FROM t FOR TIMESTAMP AS OF '2026-02-21 10:30:00';

-- 或者：先建表再写入
INSERT INTO t_recovered
SELECT * FROM t FOR TIMESTAMP AS OF '2026-02-21 10:30:00';
```

- **数据零拷贝恢复到新表（CLONE，P1/P2+，待定）**

```sql
RESTORE TABLE t FOR TIMESTAMP AS OF '2026-02-21 10:30:00' TO TABLE t_recovered;
```

- **同表回滚（ROLLBACK，P1/P2+，待定）**

```sql
RESTORE TABLE t TO TIMESTAMP AS OF '2026-02-21 10:30:00';
```

#### 3.4 与 Cluster Snapshot 的区别

Time Travel 与 Cluster Snapshot 都能保留并恢复表的历史状态，但解决的是不同层级的问题，二者互补而非替代。

| 对比维度 | Cluster Snapshot | Time Travel |
|---|---|---|
| **问题域** | 基础设施层面的灾备（Disaster Recovery） | 数据逻辑层面的历史查询与精细恢复 |
| **典型场景** | 集群不可用、FE 元数据损坏、机房/存储故障 | 业务误删/误更新、写入脏数据、审计与报表复现、排障定位 |
| **保留范围** | 集群级对象集合（如 catalog / database / table / 权限 / tasks 等） | 表（table）数据状态的历史版本 |
| **保留粒度** | 集群（全有全无） | 表级 |
| **触发方式** | 定时器驱动：周期性生成（例如 ~10 分钟） | 操作驱动：DML 与部分影响查询结果的 DDL 提交后自然产生历史版本 |
| **保留策略** | 通常只保留最新 1 个恢复点（新覆盖旧） | 需要保留**过去一段时间**内的所有历史版本 |
| **保留成本** | 低：数据 zero-copy，但 FE 元数据需持久化到对象存储 | 极低：基于 MVCC，完全 zero-copy |
| **可用性** | 高：数据和FE元数据借助对象存储高可用/高持久性提升灾备可靠性 | 有限：只有数据在对象存储上，**FE 元数据仍在本地**；主要价值在于轻量表级在线回溯 |
| **恢复粒度** | 集群或单个表 | 表 |
| **恢复点精度** | 稀疏（~10 分钟一个，且仅保留最新 1 个） | 稠密（每个操作都是恢复点，保留期内均可回溯） |
| **恢复复杂度** | 集群粒度复杂，表粒度简单（单条 SQL） | 简单（单条 SQL） |
| **恢复前预览** | 弱：难以做到"先查后恢复"的逐表预览验证 | 强：可先 `SELECT ... FOR TIMESTAMP AS OF ...` 验证再恢复 |
| **操作者** | DBA / 运维团队 | 数据工程师 / 分析师 / DBA |

#### 3.5 增量物化视图（IVM）需求特点

**IVM 的场景特征：**

1. **只需要两个版本**：IVM 增量刷新读取基表的 base version（MV 上次刷新对应的基表版本）与 head version（当前最新版本），并计算二者之间的变更（Δ）。从读取历史版本的角度，两次刷新之间的版本不需要保留。（注：计算变更（Change Data Capture）可能需要保留中间版本，该需求在 CDC 设计中讨论，不在本节范围内。）
2. **实时刷新**：IVM 目标是秒级刷新延迟，大部分情况下保留小时级的历史版本应该足够。
3. **始终使用最新 schema**：IVM 按最新 schema 执行增量计算，不需要保留历史 schema。
4. **天然具备 fallback 能力**：IVM 同时支持增量与全量刷新，只有增量刷新依赖历史版本读取。当历史版本不可用时，可自动退化为全量刷新，对用户透明。

**基于上述特征，IVM 需求特点（待讨论）：**

1. **更轻量的保留策略**：IVM 不需要天级别的保留窗口，小时级足够；也不需要保留窗口内的所有历史版本，只需保留 MV 依赖的个别版本即可，从而降低历史版本的保留开销。
2. **可以优先支持 DML （除 INSERT OVERWRITE）**：DDL（如 TRUNCATE TABLE/PARTITION、DROP PARTITION、重写数据的 Schema Change 等）支持 Time Travel 实现复杂度高（参考 4.2.2），但实时场景下发生频率低，作为折衷这些 DDL 发生后可退化为全量刷新，从而降低 Time Travel 首期的实现范围。

### 4. 技术方案

本节聚焦 3.2 节定义的 P0 产品能力——**历史版本保留**、**时间点查询**与**复制恢复（Copy）**——的技术实现方案。P1/P2+ 的零拷贝恢复（CLONE）和同表回滚（ROLLBACK）在此基础上扩展，不在本节范围内。

#### 4.1 关键概念

本节统一 shared-data 下与 Time Travel 相关的关键术语，作为后续讨论的基础。其中 **Table State** 和 **Table Version** 是本设计新引入的内部概念，用于精确描述系统状态与版本语义，不面向用户暴露。

- **Table State（表状态）**：描述表的逻辑和物理结构，包含一个查询需要的所有信息，由 FE 管理的 **Table Meta** 与对象存储上的 **Table Data** 共同构成。影响 Table State 的操作（DML、DDL、系统操作等）的完整分类参见附录 A。
  - **Table Meta**：表定义（表属性、schema、分区定义、分桶方式、索引/rollup 定义等）、**数据分片拓扑**、**数据版本（Visible Version）**。查询规划阶段依赖这些信息进行 SQL 解析、类型推导、分区裁剪与执行计划生成。
  - **Table Data**：对象存储上的 tablet metadata 与数据文件。
- **数据分片拓扑**：查询与执行依赖的物理对象层级为 `Partition → PhysicalPartition → MaterializedIndex → Tablet`：
  - `Partition`：用户可见的逻辑分区。
  - `PhysicalPartition`：系统内部维护的分区实例；一个逻辑分区可对应多个物理分区。
  - `MaterializedIndex`：base/rollup index，决定某个 Tablet 集合的数据布局。
  - `Tablet`：最小数据分片单元，tablet metadata 与数据文件均存储在对象存储上。
  - 改变拓扑的操作包括：ADD/DROP PARTITION、TRUNCATE、INSERT OVERWRITE、Schema Change、Reshard（Tablet Split/Merge）等。DML 和 Compaction 不改变拓扑，仅推进数据版本或重组数据文件。
- **数据版本（Visible Version）**：每个 `PhysicalPartition` 维护单调递增的 `visibleVersion`。它是数据可见性与一致性的核心锚点：在 `visibleVersion = v` 下，所属 Tablets 读取与版本 `v` 对齐的 tablet metadata，从而定位该版本引用的数据文件集合。
- **Table Version**：Table State 的表级版本号，覆盖所有 State 变更（DML、DDL、系统操作如 Compaction / Reshard 等）。实现上可映射为 Table Meta 的版本 + 各 PhysicalPartition 的 visible version。Time Travel 的目标即保留并可读历史时刻的 Table State——用 Table Version 标识历史恢复点：系统根据用户指定的 timestamp 定位到具体的 Table Version，系统据此取回该版本对应的 Table State。其中 Compaction / Reshard 等系统操作不改变数据内容与查询语义，仅改变物理组织；`SHOW HISTORY` 展示完整的 Table Version 历史，通过 `operation_type` 区分操作类型。
- **Tablet 调度**：Tablet → CN 的分配关系由 StarOS 管理，属于运行时状态，用于把 SCAN 任务路由到可服务的 CN。它不构成 Table State，也不需要历史化——Time Travel 只要求"能读到历史版本的数据"，不要求"按历史时刻的 CN 分配执行"。

> 参考：`docs/design/cloud_native_table_concepts_state_version_snapshot.md` 对 Table State / Table Version / Table Snapshot 的统一定义；代码结构可参考 `OlapTable` / `PhysicalPartition`。

#### 4.2 思路与挑战

##### 4.2.1 总体思路

通过 MVCC 实现 zero-copy，时间点查询核心流程分为三步：

1. **定位目标 Table Version**：将 `FOR TIMESTAMP AS OF <ts>` 解析为不晚于该时间点的最近一个 Table Version（即 `timestamp → Table Version` 映射），取回该版本对应的：
   - **历史表定义**：schema、分区定义、分布、索引/rollup 定义等，用于 SQL 解析与优化（按历史 schema 执行）。
   - **历史数据分片拓扑**：该 Table Version 对应的 Partition / PhysicalPartition / MaterializedIndex / Tablet 集合。
   - **历史数据版本**：每个 PhysicalPartition 在该 Table Version 下的目标可见版本 `hist_visible_version`，保证所有相关 Tablets 在 `hist_visible_version` 下的 tablet metadata 与数据文件可达、可读。
2. **Tablet 调度**：基于当前时刻的 Tablet → CN 映射将 SCAN 任务路由到可服务的 CN（直接使用查询时刻的分配即可），前提是 StarManager 中仍保留这些历史 Tablets 对应的 Shard 元数据。
3. **按版本读取**：CN 按 `hist_visible_version` 读取对象存储上的 tablet metadata 和数据文件，返回历史时刻的查询结果。

##### 4.2.2 技术挑战

基于上述信息需求与当前 shared-data 的能力现状，以下是 Time Travel 面临的主要技术挑战：

1. **缺少 Table Version 语义实现**
   当前系统中没有一个实现可以满足 Table Version 语义：支持 timestamp 到 Table Version 的映射，以及表达一致性的 Table State。虽然 Data 部分 PhysicalPartition 有各自的 visibleVersion，但涉及多个 PhysicalPartition 时，需保证各 `hist_visible_version` 形成一致的 point-in-time 视图。现有 GTID 仅覆盖 DML，不覆盖 DDL 和 Reshard 等，且无法从 GTID 反查 `hist_visible_version`，无法作为 Table Version 的实现。

2. **Table Meta 缺少多版本机制**
   当前 Table Meta 在数据结构 `OlapTable` 中维护，发生变更后直接原地更新或替换，只保留最新状态，不保留历史版本，因此没有机制能回答"时间 T 时刻的表定义和数据分片是什么"。

3. **数据分片拓扑变更带来的额外复杂度**
   对于无拓扑变更的操作（DML、Compaction），Tablet 持续存在，Time Travel 只需保留旧版本数据文件即可按版本读取。但拓扑变更操作（DROP/TRUNCATE PARTITION、Reshard、非 Fast Schema Change 等）会替换或删除 Tablet 本身，带来不同的挑战——历史查询需要访问当前已不存在的 Tablet。这要求在保留窗口内额外保留：(1) 历史数据分片拓扑（知道历史时刻有哪些 Tablet）；(2) StarManager 中旧 Tablet 的 Shard 元数据或 StarManager 支持 on-demand 调度（历史 Tablet 可调度）；(3) 对象存储上旧 Tablet 的 tablet metadata 和数据文件（历史数据可读取）。

4. **Vacuum/GC 保留策略扩展**
   现有 vacuum 需要考虑 Time Travel 保留策略 `history_retention`，避免保留窗口内数据被清理。

5. **FE 元数据内存压力**
   当前 FE 元数据全部驻留内存。若为 Time Travel 保留天级别的历史元数据版本，内存开销可能显著增长，需要考虑历史元数据的存储与淘汰策略。

#### 4.3 可选方案

基于 4.2.2 的技术挑战，本节提出两个方案，分别基于不同的目标和场景假设：

**方案一 面向 IVM 的最小实现** ：
- 目标：以最小改动支撑 IVM 增量刷新的历史版本读取需求，快速交付
- 假设
  - 支持 DML（INSERT / DELETE / UPDATE / 各类 LOAD），以及不涉及 Tablet 删除的 DDL（如 ADD PARTITION、Fast Schema Change）。
  - 涉及 Tablet 删除或替换的操作（TRUNCATE / DROP PARTITION / INSERT OVERWRITE / 非 Fast Schema Change / Reshard 等）发生后，该操作之前的版本不可读，IVM 回退到全量刷新。
  - 使用最新 schema，不需要保留历史 schema。
  - 保留窗口短（小时级）。

**方案二 通用 Time Travel**
- 目标：支持完整的 Time Travel 场景，与其它产品能力对齐
- 假设：参考第 3 节产品定义

方案一是方案二的子集——方案二可在方案一的基础上渐进扩展。两个方案在各技术挑战维度上的区别：

| 技术挑战 | 方案一（IVM 最小实现） | 方案二（通用 Time Travel） |
|---|---|---|
| **1. Table Version 语义** | 需要保证多个 PhysicalPartition 的 visible version 一致（point-in-time 视图）| 除 visible version 一致性外，还需保证表定义、数据分片拓扑等在同一 Table Version 下一致 |
| **2. Table Meta 多版本** | 仅需 PhysicalPartition visible version 的多版本；当前 Meta 其余部分直接可用 | 需要所有 Table Meta 的多版本——表定义（schema）、数据分片拓扑、visible version 等均需多版本 |
| **3. 数据分片拓扑变更** | 涉及 Tablet 删除/替换的操作直接标记版本链断裂，IVM 降级全量刷新 | 保留历史拓扑、StarManager Shard 元数据延迟清理、对象存储上旧 Tablet 数据保留 |
| **4. Vacuum/GC 保留策略** | 适配 `history_retention`，延长数据文件保留；不涉及已删除 Tablet 的保留 | 数据文件、已删除 Tablet 的 Shard 元数据等均需协调保留与淘汰 |
| **5. FE 元数据内存压力** | 低——仅增加 PhysicalPartition visible version 历史版本，小时级窗口） | 高-需关注天级保留，考虑历史 Meta 的存储与淘汰策略（按需加载、冷数据下沉等） |

##### 4.3.1 方案一：面向 IVM 的最小实现


##### 4.3.2 方案二：通用 Time Travel


#### 附录

##### A. Table State 变更操作分类

参考 `docs/design/cloud_native_table_concepts_state_version_snapshot.md` 2.2 节——按 DML、DDL、系统操作、物化视图操作分类列出了所有可能影响 Table State 的操作，标注了每项操作影响的 State 组件（表定义 / 数据分片 / 数据）及所属层面（用户 / 系统）。
