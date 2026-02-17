# Cloud-Native Table Concepts: State, Version 与 Snapshot

---

## 一、概述

StarRocks 存算分离（Cloud-Native）架构下，多个功能需要"保留和访问表的历史状态"：

- **Cluster Snapshot**：集群级灾难恢复，隐含包含所有表在快照时刻的完整状态
- **Time Travel**：查询表的历史数据、恢复到历史状态
- **Change Data Capture (CDC)**：捕获两个历史状态之间的数据变更，支持增量数据处理

为避免各功能各自解读导致概念分裂，本文档定义以下三个基础概念，供上层功能设计统一引用：

- **Table State**：哪些信息构成一张表的完整状态
- **Table Version**：State 的版本，每次 State 变更产生一个新的 Version
- **Table Snapshot**：将某个 Version 的 State 持久化为可靠的恢复点

> **定位说明**：以上概念是系统技术层面的定义，面向内部研发，用于模块设计与实现对齐。用户侧产品文档和 SQL 语义描述可能采用不同表述。

---

## 二、Table State

### 2.1 定义

Table State 是表的一致性状态，描述了一张表的逻辑结构和物理结构，包含了读写表所需的必要信息。它由 **Table Meta**（FE 管理并持久化的元数据）和 **Table Data**（对象存储上的数据文件）两层构成：

**Table Meta**（FE 管理并持久化）：
- **表定义**：用户通过 DDL 声明的表逻辑结构，包括 Schema、分区、分布、索引、排序键、表属性等
- **同步物化视图定义**：Rollup Index 的 MV 定义
- **异步物化视图属性**：异步 MV 特有的 Refresh 定义/状态、基表关联、Query Rewrite 配置等属性。这些属性仅对异步 MV 表适用，普通表无此组件
- **数据分片**：系统基于表定义自动创建的物理拓扑对象，包括 Physical Partition 实例和 Tablet 实例等

**Table Data**（对象存储上的数据文件）：
- **数据**：实际存储的行数据及内嵌索引（Bitmap Index 数据、Bloom Filter 数据、Short Key Index、行的物理排列等）
- **同步物化视图数据**：Rollup 数据文件，与 base table 共享版本号，在同一事务中原子更新

> 同步物化视图虽然在产品层面与 base table 是两个对象，但物理结构上同步 MV 是 base table 的 Rollup Index，无法独立存在，因此其定义/拓扑（Meta）和数据（Data）均纳入 base table 的 Table State。

表正常运行还依赖 **Tablet 调度**（StarOS 管理的 Tablet 到 CN 的分配关系），它属于运行时状态，与表的逻辑和物理结构无关，不纳入 Table State。

上述组件分为两个层面：

**用户层面**——用户直接定义、操作或感知的信息，关乎功能正确性和用户预期：
- 表定义、数据、同步物化视图、异步物化视图特有属性

**系统层面**——系统内部管理的信息，用户不直接操作，关乎技术实现：
- 数据分片

以下逐项说明各组件的具体内容。

#### 2.1.1 表定义

用户通过 DDL 声明的表逻辑结构，是用户与系统之间的"契约"，存储在 FE 中：

| 组件 | 说明 |
|------|------|
| **Schema** | 列名、列类型、列顺序、nullable 属性、默认值 |
| **分区定义** | PARTITION BY 声明的逻辑分区方案及各分区的范围/列表值（区别于数据分片中系统自动管理的 Physical Partition） |
| **分布定义** | 分布策略（Hash/Random）及桶数 |
| **索引定义** | Bitmap Index、Bloom Filter 等辅助索引的定义（哪些列上有什么索引） |
| **排序键定义** | 表模型（PRIMARY/UNIQUE/AGGREGATE/DUPLICATE KEY）及排序列顺序 |
| **表属性** | 配置项，如 `enable_persistent_index`、`compression` 等 |

> 注：索引和排序键的**数据部分**（实际的索引数据、行的物理排列）包含在数据文件中（见 2.1.3）。此处讨论的是其**元数据定义**——即 FE 中记录的"哪些列上有索引""排序列是什么"等信息。查询时 Planner 基于这些元数据定义进行优化决策。

#### 2.1.2 数据分片

系统基于表定义自动创建和管理的运行时物理对象，是逻辑定义到物理存储的桥梁。用户不直接感知，存储在 FE 中：

| 组件 | 说明 |
|------|------|
| **Physical Partition 实例** | 一个逻辑分区可包含多个 Physical Partition，数量可能是动态的（如 Random 分桶下会动态新增），各有独立版本号 |
| **Tablet 实例** | 每个 Physical Partition 下按分桶策略划分为多个 Tablet，是数据存储的最小分片单元。Range 分布下可被系统 Split/Merge（Reshard）导致 Tablet ID 变化；非 Fast Schema Change 需要重写数据，整个 Physical Partition 及其下属 Tablet 都会被替换 |

#### 2.1.3 数据

表中实际存储的行数据，以文件形式存储在对象存储中。

数据文件中同时内嵌了索引数据（Bitmap Index 数据、Bloom Filter 数据）和物理排序（Short Key Index、行的排列顺序）。

#### 2.1.4 同步物化视图

基于 base table 创建的 Rollup Index，兼具元数据和数据两个层面：

- **元数据**（Meta）：MV 的定义（聚合列、排序方式等），以及独立的数据分片拓扑（Physical Partition、Tablet 等），存储在 FE
- **数据**（Data）：Rollup 数据文件，存储在对象存储中，与 base table 共享版本号，在同一事务中原子更新

同步 MV 不改变 base table 的查询结果，但查询优化器可自动选择使用 sync MV 加速查询。

#### 2.1.5 异步物化视图特有属性

异步物化视图本质上是一张独立的内表，其 Table State（表定义、数据分片、数据）与普通表一致。此处仅讨论异步 MV 额外附加的特有属性（均属 Meta 层，存储在 FE）——这些属性仅对异步 MV 适用，普通表不包含此组件：

| 组件 | 说明 |
|------|------|
| **Refresh 定义** | 刷新策略（ASYNC / MANUAL）、刷新间隔、刷新范围 |
| **Refresh 状态** | 上次刷新时间、各分区的刷新状态与新鲜度 |
| **基表关联关系** | 派生的查询定义（`AS SELECT ...`）、依赖的基表列表 |
| **Query Rewrite 配置** | 是否参与透明查询改写 |

### 2.2 State 变更操作

以下将所有可能影响 Table State 的操作按类型分类。组件名对应 2.1 定义，括号标注所属层（M = Table Meta，D = Table Data）。

2.2.1 ~ 2.2.2 为用户层面操作；2.2.3 为系统层面操作，改变 State 的物理结构但数据逻辑等价；2.2.4 为物化视图操作。所有操作均产生新的 Table Version（见第三节）。

#### 2.2.1 DML 操作

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| INSERT INTO | 数据 (D) | 用户 | 新增行 |
| INSERT OVERWRITE | 数据分片 (M) + 数据 (D) | 用户 | 新分区替换旧分区 |
| DELETE FROM ... WHERE | 数据 (D) | 用户 | 删除行 |
| UPDATE ... SET ... WHERE | 数据 (D) | 用户 | 修改行 |
| STREAM LOAD / BROKER LOAD / ROUTINE LOAD | 数据 (D) | 用户 | 批量导入 |

#### 2.2.2 DDL 操作

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| ALTER TABLE ADD/DROP/MODIFY COLUMN（Fast Schema Change） | 表定义 (M) | 用户 | 仅修改列定义，不重写数据 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN（非 Fast Schema Change） | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 重写数据并替换 Tablet |
| ALTER TABLE ORDER BY | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 重写数据并替换 Tablet |
| CREATE/DROP INDEX | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 重写数据并替换 Tablet |
| ALTER TABLE SET bloom_filter_columns | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | Bloom Filter 索引变更，重写数据并替换 Tablet |
| ALTER TABLE ... DISTRIBUTED BY (修改桶数) | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 变更分桶数，重写数据并替换 Tablet，数据逻辑等价 |
| ALTER TABLE ADD PARTITION | 表定义 (M) + 数据分片 (M) | 用户 | 新增逻辑分区，创建对应 Physical Partition 和 Tablet |
| ALTER TABLE DROP PARTITION | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 删除逻辑分区及其数据和分片 |
| 表达式分区合并 | 表定义 (M) + 数据分片 (M) + 数据 (D) | 用户 | 合并多个表达式分区为一个，重写数据并替换 Tablet |
| TRUNCATE TABLE | 数据分片 (M) + 数据 (D) | 用户 | 新 Tablet 替换旧 Tablet |
| TRUNCATE PARTITION | 数据分片 (M) + 数据 (D) | 用户 | 新 Tablet 替换旧 Tablet |
| ALTER TABLE RENAME | 表定义 (M) | 用户 | 变更表名 |
| ALTER TABLE SET ('key' = 'value') | 表定义 (M) | 用户 | 改变表定义，但不影响数据查询结果。`bloom_filter_columns` 属于索引定义变更，见上方 |
| ALTER TABLE MODIFY COMMENT | 表定义 (M) | 用户 | 改变表定义，但不影响数据查询结果 |

> **DROP TABLE / DROP DATABASE**：DROP 销毁的是表对象本身，而非变更表的某个 State 组件，不归入上述常规 State 变更操作。

#### 2.2.3 系统操作

以下操作可由系统自动触发，也可通过 DDL 手动触发，但不改变用户可见的数据或定义：

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| Compaction | 数据 (D) | 系统 | 物理文件合并，数据逻辑等价 |
| Tablet Reshard | 数据分片 (M) | 系统 | Range 分布下 Tablet 分裂/合并，数据逻辑等价 |
| Physical Partition 新增 | 数据分片 (M) | 系统 | Random 分桶写扩展，不改变逻辑数据 |

#### 2.2.4 物化视图操作

**同步物化视图**：

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| CREATE MATERIALIZED VIEW (sync) | 同步 MV 定义 (M) + 同步 MV 数据 (D) | 用户 | 新增 sync MV 定义及数据 |
| DROP MATERIALIZED VIEW (sync) | 同步 MV 定义 (M) + 同步 MV 数据 (D) | 用户 | 删除 sync MV 定义及数据 |
| 基表 DML / Partition 变更等 | 同步 MV 数据 (D) | 用户 | Rollup Index 随基表同步变化，在同一事务中原子更新 |

**异步物化视图**：

异步 MV 作为独立内表，其数据和表定义遵循与普通表相同的 State 规则（2.2.1 ~ 2.2.3）。以下仅涉及 MV 特有属性的操作：

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| REFRESH（手动或自动触发） | 数据 (D) + 异步 MV 属性 (M) | 用户 | 本质上是 DML 操作，更新数据并更新 Refresh 状态 |
| ALTER MATERIALIZED VIEW ... REFRESH | 异步 MV 属性 (M) | 用户 | 修改刷新策略 |


### 2.3 不同功能关注的 State 变更

不同功能对 State 变更的关注范围不同：

- **Cluster Snapshot**：关注用户层面和系统层面的所有变更，需要完整捕获某一时刻的 Table State（Meta + Data）
- **Time Travel / CDC**：主要关注用户层面的变更，尤其是数据相关的变更（DML、影响数据的 DDL）。Time Travel 用于查询历史数据或恢复到历史状态，CDC 用于捕获两个版本之间的增量数据差异

---

## 三、Table Version

Table Version 是 Table State 的版本，覆盖 Table Meta 与 Table Data 的变化。只要 Table State 的任一组件发生变化——无论是用户操作（DDL、DML）还是系统后台物理变化（Compaction、Tablet Reshard、Physical Partition 动态新增等）——都会产生一个新的 Version。因此，Version 可能在没有用户显式操作的情况下推进。

当前系统已在不同层面具备版本能力：

**数据层**——Table Data 已具备多版本能力，涉及两个粒度的版本号：

- **Physical Partition visible version**：FE 为每个 Physical Partition 维护的单调递增版本号，表示该分区数据的可见版本（事务 publish 后推进），是数据可见性与一致性的核心口径。
- **Tablet metadata version**：Tablet 粒度的元数据文件版本，记录该 Tablet 在特定版本下引用的数据文件集合。各 Tablet 的 metadata version 需与所属 Physical Partition 的 visible version 对齐，从而定位该版本的完整数据。

**事务层**——系统存在 **GTID（Global Transaction ID）**，用于标识一次成功提交的 DML 事务，在集群范围内提供单调递增的事务序列口径。但 GTID 仅覆盖 DML 事务，无法覆盖所有 Table State 变更（如 TRUNCATE PARTITION 等 DDL 和系统后台物理变化）。

Table Version 是在上述机制之上定义的**表级版本号**，覆盖所有 State 变更，将某个一致点的 Table Meta 与 Table Data 统一标识为一个编号。实现上，一个 Table Version 可映射为：Table Meta 的版本 + 各 Physical Partition 的 visible version（即其下各 Tablet 数据对齐到的 metadata version）。当一次事务提交推动某张表相关的 Physical Partition visible version 推进时，该表的 Table Version 也随之推进。

目前 Table Meta 尚不支持 MVCC，因此 Table State 目前还无法实现表级 MVCC。

---

## 四、Table Snapshot

Table Snapshot 用于把某个 **Table Version** 固化为一个**能可靠引用的恢复点**。为实现"可靠引用"，系统可能需要对该版本执行额外的持久化与保留动作。

以 **Cluster Snapshot** 为例：它隐含包含所有表在快照时刻对应的 Table Snapshot。Cluster Snapshot 额外将该时刻的 **Table Meta**持久化到对象存储，而 **Table Data** 仍复用对象存储中已有的数据文件。
