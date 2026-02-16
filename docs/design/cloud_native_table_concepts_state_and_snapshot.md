# Cloud-Native Table Concepts: State 与 Snapshot

---

## 一、概述

StarRocks 存算分离（Cloud-Native）架构下，多个功能需要"保留和访问表的历史状态"：

- **Cluster Snapshot**：集群级灾难恢复，隐含包含所有表在快照时刻的完整状态
- **Time Travel**：查询表的历史数据、恢复到历史状态
- **Change Data Capture (CDC)**：捕获两个历史状态之间的数据变更，支持增量数据处理

这些功能共同依赖两个基础概念：

- **Table State**：定义表的"状态"包含什么——哪些信息构成了一张表的完整状态
- **Table Snapshot**：表示某一时刻的 Table State——如何捕获、存储、访问特定时刻的状态实例

明确这两个概念的定义，可以避免各功能各自解读导致的概念分裂、实现重复和行为不一致。本文档建立统一的基础定义，供上层功能设计引用。

---

## 二、Table State

### 2.1 定义

Table State 描述了一张表的逻辑结构和物理结构，Table State 由以下内容组成：

- **表定义**：用户通过 DDL 声明的表逻辑结构，包括 Schema、分区、分桶、索引、排序键、表属性等
- **数据分片**：系统基于表定义自动创建的物理对象，包括 Physical Partition 实例和 Tablet 实例等
- **数据**：实际存储的行数据，以 Tablet Meta 和 Segment 等文件的形式存储在对象存储中
- **同步物化视图**：基于 base table 创建的 Rollup Index，包含 MV 定义和 Rollup 数据。虽然产品层面 MV 和 base table 是两个对象，但物理结构上同步 MV 是 base table 的一部分，无法独立存在，因此纳入 base table 的 Table State
- **异步物化视图特有属性**：异步 MV 特有的 Refresh 定义、基表关联、Query Rewrite 配置等属性

表正常运行还依赖 **Tablet 调度**（StarOS 管理的 Tablet 到 CN 的分配关系），但它与表的结构无关，不属于 Table State。

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
| **分布定义** | DISTRIBUTED BY 声明的分布策略（Hash/Random/Range）及桶数 |
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

- **元数据**：MV 的定义（聚合列、排序方式等），以及独立的数据分片和 Tablet 调度，存储在 FE
- **数据**：Rollup 数据，与 base table 共享版本号，在同一事务中原子更新

同步 MV 不改变 base table 的查询结果，但查询优化器可自动选择使用 sync MV 加速查询。

#### 2.1.5 异步物化视图特有属性

异步物化视图本质上是一张独立的内表，其数据、Schema、分区等遵循与普通表相同的规则。此处仅讨论异步 MV 作为物化视图特有的属性：

| 组件 | 说明 |
|------|------|
| **Refresh 定义** | 刷新策略（ASYNC / MANUAL）、刷新间隔、刷新范围 |
| **Refresh 状态** | 上次刷新时间、各分区的刷新状态与新鲜度 |
| **基表关联关系** | 派生的查询定义（`AS SELECT ...`）、依赖的基表列表 |
| **Query Rewrite 配置** | 是否参与透明查询改写 |

### 2.2 State 变更操作

以下将所有可能影响 Table State 的操作按类型分类。

2.2.1 ~ 2.2.2 为用户层面操作，改变 Table State；2.2.3 为系统内部操作，仅改变系统层面信息，不改变用户可见的 State；2.2.4 为物化视图操作。

#### 2.2.1 DML 操作

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| INSERT INTO | 数据 | 用户 | 新增行 |
| INSERT OVERWRITE | 数据 + 数据分片 | 用户 | 新分区替换旧分区 |
| DELETE FROM ... WHERE | 数据 | 用户 | 删除行 |
| UPDATE ... SET ... WHERE | 数据 | 用户 | 修改行 |
| STREAM LOAD / BROKER LOAD / ROUTINE LOAD | 数据 | 用户 | 批量导入 |

#### 2.2.2 DDL 操作

| 操作 | 影响的 State 组件 | 层面 | 说明 |
|------|-------------------|:----:|------|
| ALTER TABLE ADD/DROP/MODIFY COLUMN（Fast Schema Change） | 表定义 | 用户 | 仅修改列定义，不重写数据 |
| ALTER TABLE ADD/DROP/MODIFY COLUMN（非 Fast Schema Change） | 表定义 + 数据 + 数据分片 | 用户 | 重写数据并替换 Tablet |
| ALTER TABLE ORDER BY | 表定义 + 数据 + 数据分片 | 用户 | 重写数据并替换 Tablet |
| CREATE/DROP INDEX | 表定义 + 数据 + 数据分片 | 用户 | 重写数据并替换 Tablet |
| ALTER TABLE SET bloom_filter_columns | 表定义 + 数据 + 数据分片 | 用户 | Bloom Filter 索引变更，重写数据并替换 Tablet |
| ALTER TABLE ... DISTRIBUTED BY (修改桶数) | 表定义 + 数据 + 数据分片 | 用户 | 变更分桶数，重写数据并替换 Tablet，数据逻辑等价 |
| ALTER TABLE ADD PARTITION | 表定义 + 数据分片 | 用户 | 新增逻辑分区，创建对应 Physical Partition 和 Tablet |
| ALTER TABLE DROP PARTITION | 表定义 + 数据 + 数据分片 | 用户 | 删除逻辑分区及其数据和分片 |
| 表达式分区合并 | 表定义 + 数据 + 数据分片 | 用户 | 合并多个表达式分区为一个，重写数据并替换 Tablet |
| TRUNCATE TABLE | 数据 + 数据分片 | 用户 | 新 Tablet 替换旧 Tablet |
| TRUNCATE PARTITION | 数据 + 数据分片 | 用户 | 新 Tablet 替换旧 Tablet |
| ALTER TABLE RENAME | 表定义 | 用户 | 变更表名 |
| ALTER TABLE SET ('key' = 'value') | 表定义（表属性） | 用户 | 改变表定义，但不影响数据查询结果。`bloom_filter_columns` 属于索引定义变更，见上方 |
| ALTER TABLE MODIFY COMMENT | 表定义 | 用户 | 改变表定义，但不影响数据查询结果 |

> **DROP TABLE / DROP DATABASE**：DROP 销毁的是表对象本身，而非变更表的某个 State 组件，不归入上述常规 State 变更操作。

#### 2.2.3 系统操作

以下操作可由系统自动触发，也可通过 DDL 手动触发，但不改变用户可见的数据或定义：

| 操作 | 影响的组件 | 层面 | 说明 |
|------|-----------|:----:|------|
| Compaction | 数据 | 系统 | 物理文件合并，数据逻辑等价 |
| Tablet Reshard | 数据分片 | 系统 | Range 分布下 Tablet 分裂/合并，数据逻辑等价 |
| Physical Partition 新增 | 数据分片 | 系统 | Random 分桶写扩展，不改变逻辑数据 |

#### 2.2.4 物化视图操作

**同步物化视图**：

| 操作 | 影响的组件 | 层面 | 说明 |
|------|-----------|:----:|------|
| CREATE MATERIALIZED VIEW (sync) | 同步物化视图 | 用户 | 新增 sync MV 定义及数据 |
| DROP MATERIALIZED VIEW (sync) | 同步物化视图 | 用户 | 删除 sync MV 定义及数据 |
| 基表 DML / Partition 变更等 | 同步物化视图（数据 + 数据分片） | 用户 | Rollup Index 随基表同步变化，在同一事务中原子更新 |

**异步物化视图**：

异步 MV 作为独立内表，其数据和表定义遵循与普通表相同的 State 规则（2.2.1 ~ 2.2.3）。以下仅涉及 MV 特有属性的操作：

| 操作 | 影响的组件 | 层面 | 说明 |
|------|-----------|:----:|------|
| REFRESH（手动或自动触发） | 数据 + Refresh 状态 | 用户 | 本质上是 DML 操作，遵循通用数据变更规则，同时更新 Refresh 状态 |
| ALTER MATERIALIZED VIEW ... REFRESH | 异步物化视图特有属性 | 用户 | 修改刷新策略 |


---

## 三、Table Snapshot

### 3.1 定义

Table Snapshot 表示某一时刻的 Table State。每次 State 变更都隐含产生一个新的 Snapshot，但该 Snapshot 不一定被持久化为物理实体（见 3.2）。

### 3.2 物理实体

Snapshot 的物理实体是将某个 Snapshot 持久化后的具体物理对象集合。物理实体的生成策略和组成方式由具体功能的实现决定：

- **Cluster Snapshot**：Table Snapshot 是 Cluster Snapshot 的组成部分。系统每隔一段时间生成一次，期间可能发生多次 State 变更，只有触发快照的那个时刻对应的 Snapshot 会被持久化。物理实体由对象存储上的 FE Image + Edit Log + 数据文件组成。
- **Time Travel**：每次 State 变更（如一次导入）都会生成一个 Snapshot，通常通过多版本机制来表示。

