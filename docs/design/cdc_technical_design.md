# Change Data Capture (CDC) for IVM — 技术设计文档

> 本文档是 [pitq_and_cdc.pdf](pitq_and_cdc.pdf) / [change_data_capture_proposal_shared_data.md](change_data_capture_proposal_shared_data.md) 中 CDC 部分的完善版本，面向研发 review。
> 聚焦于技术方案的问题定义、设计考量与取舍、关键原理，不涉及所有实现细节。

---

## 目录

1. [问题定义与设计目标](#1-问题定义与设计目标)
2. [基本概念与语义约定](#2-基本概念与语义约定)
3. [消费接口](#3-消费接口)
4. [端到端流程总览](#4-端到端流程总览)
5. [Version Range 计算（FE 侧）](#5-version-range-计算fe-侧)
6. [Changes 生成机制（CN 侧）](#6-changes-生成机制cn-侧)
7. [Compaction 交互](#7-compaction-交互)
8. [Scan 架构与并行模型](#8-scan-架构与并行模型)
9. [Net Changes](#9-net-changes)
10. [边界场景与 Fallback](#10-边界场景与-fallback)
11. [小文件优化](#11-小文件优化)
12. [总结与 Roadmap](#12-总结与-roadmap)

---

## 1. 问题定义与设计目标

### 1.1 问题

增量物化视图（IVM）刷新时需要获取基表两个版本之间的**行级数据变更**（插入、更新、删除），即 Change Data Capture（CDC）。当前 StarRocks 内表不具备这一能力：

- **明细表 / 聚合表**：只支持 append，新增数据存储在 delta rowset 中，理论上可以通过读取 delta rowset 获取 INSERT 变更，但目前没有提供相应的读取接口。
- **主键表**：支持 INSERT / UPDATE / DELETE，但变更信息散落在 segment 和 delete vector 中，没有统一的变更捕获机制。尤其是 UPDATE 和 DELETE 产生的"旧值"，需要从历史 segment 中读取，缺少高效的定位手段。

### 1.2 目标

- **短期**：为 IVM 提供 CDC 能力，覆盖核心 DML（INSERT / UPDATE / DELETE / 各类 LOAD），不含 INSERT OVERWRITE 和 DDL。
- **长期**：扩展到 Time Travel、流计算、审计合规、数据同步等场景。

### 1.3 设计约束

| 约束 | 原因 |
|------|------|
| **写入性能优先** | 实时导入场景对延迟和抖动敏感，CDC 机制不能显著影响导入路径 |
| **批量消费** | IVM 本质是批处理，以 version 为粒度消费，不需要逐行流式推送 |
| **IVM-first** | 短期只需覆盖 IVM，接口和实现可以做相应裁剪 |
| **可扩展** | 数据结构和接口设计预留向 Time Travel、流计算等场景扩展的能力 |

---

## 2. 基本概念与语义约定

本节定义 CDC 的基础语义，是后续所有技术方案讨论的前提。

### 2.1 变更类型（Change Type）

| 类型 | 编码 | 含义 |
|------|------|------|
| INSERT | 0 | 新插入一行 |
| DELETE | 1 | 删除一行 |
| UPDATE_BEFORE | 2 | 更新前的旧值 |
| UPDATE_AFTER | 3 | 更新后的新值 |

### 2.2 Update 语义：两种表示模式

UPDATE 产生的变更有两种表示方式，适用于不同场景，实现成本也不同：

**模式 A：UPDATE_BEFORE + UPDATE_AFTER**

- 保留完整的 UPDATE 语义，消费端可以区分"更新"和"删除+插入"
- 适用场景：流计算（Flink sink 到 upsert 系统时可以跳过 BEFORE 只写 AFTER）、审计
- 实现成本高：需要将 BEFORE 和 AFTER 进行关联配对，主键表导入时 PK Index 知道旧行的 rssid，但要读取旧值并与新值配对

**模式 B：DELETE + INSERT**

- 将 UPDATE 拆成独立的 DELETE（旧值）和 INSERT（新值），不需要显式配对
- 适用场景：IVM 只关心集合差（delta），不需要 UPDATE 语义
- 实现成本低：主键表只需要根据 delete vector diff 生成 DELETE，新 segment 生成 INSERT，不需要 old/new segment 之间做 PK 映射

**设计决策**：两种模式都支持，通过参数配置。IVM 使用模式 B（DELETE + INSERT），降低实现和计算成本。

> 关于两种模式对下游影响的具体分析，参见 [change_data_capture_proposal_shared_data.md Appendix A](change_data_capture_proposal_shared_data.md#appendix-a-flink-update-change-type)。

### 2.3 顺序性

**存储层输出无序**，具体表现为：

- Row 之间无序
- 同一 row 的 UPDATE_BEFORE / UPDATE_AFTER 之间无序，且不保证相邻
- 一次消费多个 version 时，version 之间无序

**设计理由**：

- 存储层首要任务是并行 scan 以提升 IO 吞吐。Version 之间、同一 version 的文件之间、同一文件内部都可能并行 scan。在并行基础上支持保序会引入额外复杂度且不一定比计算层高效。
- 不同场景对顺序要求不同，在计算层按需排序更灵活：
  - **IVM**：批处理，version 之间和 BEFORE/AFTER 之间都不需要有序。Net Changes 会对同一 row 的 changes 排序合并，但这是相对宽松的局部排序。
  - **流计算**（Flink 等）：以 record 为粒度处理，中间结果对下游可见，需要严格按 `(row_version, change_type)` 排序，由计算层保证。

### 2.4 消费粒度

**批量模式，以 version 为粒度**：可以指定读取某个版本或连续几个版本的变更，但不能只读取某个版本的部分变更。这与 IVM 的批处理语义一致。

### 2.5 CHANGES 组成

```
CHANGES = 数据列 + 元数据列
```

- **数据列**：与表的列一致，可以只包含需要的列（支持 projection）
- **元数据列**：

| 名称 | 类型 | 含义 |
|------|------|------|
| CHANGE_TYPE | TINYINT | 变更类型编码（0-3） |
| ROW_ID | BIGINT | 逻辑行标识，同一行的所有变更具有相同的 ROW_ID |
| ROW_VERSION | BIGINT | 产生变更的版本，配对的 UPDATE_BEFORE/UPDATE_AFTER 共享相同 ROW_VERSION |

### 2.6 Row Tracking（前置依赖）

CDC 依赖存储层为每行维护的两个属性：

- **ROW_ID**：逻辑行唯一标识。INSERT 时分配全局唯一值，UPDATE 后保持不变，DELETE 后不再复用。Net Changes 依赖 ROW_ID 做同行变更合并。
- **ROW_VERSION**：行版本。INSERT 时生成初始版本，UPDATE 后版本递增（不一定连续），row 之间的 version 没有关系。Update 配对和 Net Changes 排序依赖 ROW_VERSION。

Row Tracking 的具体生成方案、唯一性保证、存储格式等在单独文档中设计，本文假设存储层已具备此能力。

### 2.7 支持的表类型

| 表类型 | 支持的 DML | 变更类型 | 说明 |
|--------|-----------|---------|------|
| 明细表 | append | INSERT | 直接读 delta rowset |
| 主键表 | INSERT / UPDATE / DELETE / 各类 LOAD | INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER | 核心复杂度所在 |
| 聚合表 | append | INSERT（aggregate 语义） | 读 delta rowset，数据是 aggregate 后的结果 |
| 更新表 | 不支持 | — | — |

---

## 3. 消费接口

在深入技术实现前，先介绍 CDC 的使用方式，建立"CDC 长什么样"的直观认知。

### 3.1 SQL 查询接口

面向 Ad-hoc 查询，指定 timestamp 或 version 范围：

```sql
-- Option 1: CHANGES clause (类似 Snowflake / Spark)
SELECT * FROM tbl CHANGES FROM VERSION v1 TO v2;
SELECT * FROM tbl CHANGES FROM TIMESTAMP t1 TO t2;

-- Option 2: Table Function
SELECT * FROM table_changes('tbl', v1, v2);
```

### 3.2 STREAM 对象

自动管理消费 offset，类似 Snowflake Stream：

```sql
CREATE STREAM my_stream ON tbl;

-- 查询 offset 到最新版本之间的 CHANGES
SELECT * FROM my_stream;

-- DML 中消费 stream，执行成功后自动推进 offset
INSERT INTO target_tbl SELECT * FROM my_stream;
```

### 3.3 SDK / RPC（对接 Flink / Spark）

- **PULL 模式**：客户端通过 SDK 发起查询，指定消费范围，CN 执行特殊 scan plan，通过 RPC 返回数据。类似当前 connector scan。
- **PUSH 模式**：客户端订阅，服务端有新 CHANGES 自动推送。实时性更高，暂不考虑。

### 3.4 IVM 内部 Java API

IVM 不经过 SQL 层，在 Analyze 阶段通过 Java API 直接获取版本信息并注入 plan：

```java
// 获取上次 refresh 的版本
long oldVersionId = asyncRefreshContext.baseTableInfoTvrDeltaMap.get(tableInfo);
TableState oldState = tableVersionKeeper.getTableState(oldVersionId);

// 订阅当前最新版本
long newVersionId = tableVersionKeeper.subscribeLatestTableState().get();
TableState newState = tableVersionKeeper.getTableState(newVersionId);

// 计算 delta
DeltaState delta = TableStateUtils.computeDeltaState(oldState, newState, catalogTable);
```

---

## 4. 端到端流程总览

CDC 的完整链路如下，后续章节逐一展开各环节。

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              MV Refresh 触发                                │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ① FE: 获取 old/new TableState                                [§5]          │
│    从 TableVersionKeeper 获取上次 refresh 的 oldState 和当前 newState        │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ② FE: Diff TableState，计算 Version Range                    [§5]          │
│    对比 old/new State，按 partition → physical partition 逐层 diff           │
│    输出每个 tablet 的 version range (oldVisibleVer, newVisibleVer]           │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ③ FE: 构造 OlapTableChangesScanNode                          [§8]          │
│    将 tablet id + version range 封装到 scan range，下发给 CN                │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ④ CN: 每个 Tablet 生成 Changes                                [§6]          │
│    - 明细表/聚合表：读 delta rowset，标记 INSERT                            │
│    - 主键表：根据 bitmap vector 定位 INSERT/DELETE/UPDATE，                  │
│      读取新值（顺序读）和旧值（攒批读）                                      │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ⑤ CN: Net Changes（可选）                                     [§9]          │
│    计算层按 row_id 合并多版本变更，输出最小等价 changes                       │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ⑥ IVM 增量计算算子消费 Changes                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Version Range 计算（FE 侧）

FE 负责确定"哪些 tablet 有变更、每个 tablet 需要读哪个版本范围的 changes"，这是连接 PITQ（版本管理）和 CDC（变更扫描）的桥梁。

### 5.1 输入与输出

- **输入**：
  - `oldState`：上次 MV refresh 时快照的 TableState（从 `TableVersionKeeper` 获取）
  - `newState`：当前最新的 TableState
  - `catalogTable`：当前 catalog 中的 `OlapTable`（用于获取 partition/tablet 的物理拓扑）
- **输出**：每个需要 scan 的 tablet 的 version range `(oldVisibleVersion, newVisibleVersion]`

### 5.2 Diff 逻辑

按 logical partition → physical partition 逐层比较 oldState 和 newState：

```
对 newState 中的每个 LogicalPartition:
    如果 partition 不在 oldState 中:
        → 新增 partition，该 partition 下所有 tablet 读取 (0, newVisibleVersion]
    否则，对比该 partition 下每个 PhysicalPartition:
        如果 physicalPartition.visibleVersion 不同:
            → version 前进，该 PP 下所有 tablet 读取 (oldVisibleVer, newVisibleVer]
        如果 visibleVersion 相同:
            → 无变化，跳过

对 oldState 中存在但 newState 中不存在的 partition:
    → 被删除的 partition，记录到 DeltaState.droppedPartitions
    → IVM 最小化实现中，触发 fallback 全量刷新
```

### 5.3 示例

一张 3 个 partition 的表，经历 2 次导入：

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
|--------|--------------|------|
| T1, T2 | (5, 8] | P1 有变更，覆盖两次导入的 3 个 version |
| T3, T4 | (3, 4] | P2 有变更，覆盖一次导入 |
| T5, T6 | 跳过 | P3 无变化 |

### 5.4 Tablet Reshard 场景

如果 physical partition 发生了 tablet reshard（数据在新旧 tablet 之间重新分布），同一个 physical partition 会存在多组 materialized index，每组覆盖部分 version range。

```
PP1 在 version 6 发生 reshard:
  旧 index (metaId=A): tablets [T1, T2], 覆盖 version (5, 6]
  新 index (metaId=B): tablets [T1', T2'], 覆盖 version (6, 8]
```

FE 需要将这两组 index 的 version range 都下发，CN 分别读取后在计算层合并。这也是 `MaterializedIndexDeltaState` 数据结构中 `List<MaterializedIndexDeltaState>` 的设计意图——支持一个 physical partition 内多段 version range 的覆盖。

### 5.5 下发结构

FE 将计算好的信息封装到 `OlapTableChangesScanNode` 的 scan range 中：

```
ScanRange {
    tablet_id: long
    start_version: long  // excluding
    end_version: long    // including
}
```

每个 CN 节点接收分配给自己的 tablet scan range，独立执行 changes 生成。

---

## 6. Changes 生成机制（CN 侧）

CN 拿到 tablet + version range `(V_old, V_new]` 后，需要输出这个范围内的所有行级 changes。这是整个 CDC 最核心的技术环节。

### 6.1 问题分解

**明细表 / 聚合表**：只有 append 操作，`(V_old, V_new]` 范围内的 delta rowset 就是全部 INSERT 变更，直接顺序读取即可，实现简单。

**主键表**：支持 INSERT / UPDATE / DELETE，是核心难点。问题可以分解为两个子问题：

1. **定位（Locate）**：哪些行是 INSERT、哪些是 DELETE、哪些是 UPDATE，以及 UPDATE 的 BEFORE/AFTER 配对
2. **取值（Fetch）**：INSERT 和 UPDATE_AFTER 的值在新 segment 中可以顺序读取；但 DELETE 和 UPDATE_BEFORE 的旧值需要从历史 segment 中读取，列式存储下随机读取代价高

### 6.2 方案对比

以下讨论主键表的 changes 生成方案。

#### 方案 A：导入时生成 Changelog

**思路**：导入时利用 PK Index 更新过程拿到被删除/被更新行的旧 rssid（rowset segment id），立即读取旧值，将新旧值配对后写入独立的 changelog 文件。查询时直接读取 changelog。

- 优点：查询效率高（顺序读 changelog）；利用 PK Index 已有机制配对 BEFORE/AFTER
- 缺点：
  - **影响导入性能**：每次导入额外随机读旧值 + 写 changelog，对实时导入延迟影响大
  - **存储开销高**：changelog 需要存储所有列的完整旧值，即使查询只需要部分列
  - **维护复杂**：changelog 文件的生命周期管理、与 compaction 的交互等引入额外复杂度
  - 对于 Net Changes 场景，中间版本的 changelog 最终会被合并掉，提前生成是浪费

#### 方案 B：Delete Vector Diff

**思路**：查询时比较 `V_old` 和 `V_new` 时刻的 delete vector，差集即为该版本范围内被删除或被更新的行。新增的行从 delta rowset 读取。

- 优点：无导入开销，无额外存储，实现概念简单
- 缺点：
  - **无法区分 DELETE 和 UPDATE**：delete vector 只记录"该行被标记删除"，无法判断是真正的 DELETE 操作还是 UPDATE 导致的旧行标记删除。要区分二者就必须拿被标记删除的行去新 segment 中做 PK 匹配，等于在查询时重新做一遍 PK 关联，复杂且代价高

#### 方案 C：Bitmap Vector（采纳）

**思路**：导入时，在 PK Index 更新阶段，顺便将每行的 change type 记录为轻量的 bitmap vector，写入 segment 元数据。查询时根据 bitmap vector 精确定位每行的变更类型和位置。

- 优点：
  - **导入边际成本极低**：PK Index 更新时本来就知道每行的 change type（insert/delete/update），只需额外记录几个 bitmap，不需要读旧值
  - **存储开销极低**：每行 1~2 bit（Roaring Bitmap 压缩后更小）
  - **查询时精确定位**：知道每个 segment 中哪些行是什么变更类型，可以批量读取，把"不知道读哪里的随机 IO"变成"已知位置的攒批读取"
  - **自包含**：每个 version 的变更元信息自包含在该 version 的 segment 元数据中，不依赖跨 version / 跨 tablet 的 diff
- 缺点：scan 效率相对方案 A（预生成 changelog）略低

**选择方案 C 的核心理由**：

1. **写入性能优先**：方案 A 影响导入路径，方案 C 几乎零开销
2. **方案 B 无法区分 DELETE 和 UPDATE**：这是方案 B 的根本缺陷
3. **攒批优化空间**：方案 C 的 bitmap vector 支持跨版本合并（详见 6.4），查询性能可接受

### 6.3 主键表当前存储结构回顾

理解方案 C 需要先回顾主键表的存储机制：

```
Tablet
├── Segment 0 (rowset version 1)    ← 历史 segment
│   ├── 数据列（columnar）
│   └── Delete Vector: bitmap 标记哪些行已被后续版本删除/更新
├── Segment 1 (rowset version 2)    ← 历史 segment
│   ├── 数据列
│   └── Delete Vector
├── Segment 2 (rowset version 5)    ← 新导入的 segment
│   └── 数据列
└── PK Index: primary_key → (segment_id, row_offset)
```

每次导入时，PK Index 更新流程：

1. 对新 segment 的每一行，查 PK Index
2. 如果 PK 已存在（旧行）→ 这是一个 **UPDATE**：标记旧 segment 中该行为 deleted（更新 delete vector），新行写入新 segment
3. 如果 PK 不存在 → 这是一个 **INSERT**：新行写入新 segment，PK Index 新增条目
4. 对 DELETE 操作 → 标记旧 segment 中该行为 deleted

**关键观察**：在步骤 2-4 中，PK Index 已经知道了每行的 change type。方案 C 就是在这个时机把信息记录下来。

### 6.4 Bitmap Vector 方案详解

导入时，除了更新 delete vector，额外记录以下 bitmap vector：

| Bitmap Vector | 记录位置 | 含义 |
|--------------|---------|------|
| **delete_change_type_bitmap** | 旧 segment | 在该 version 中被 DELETE 操作删除的行（真正的 DELETE，不是 UPDATE 导致的） |
| **update_before_bitmap** | 旧 segment | 在该 version 中被 UPDATE 的行（即 UPDATE_BEFORE 的位置） |
| **update_after_bitmap** | 新 segment | 在该 version 中作为 UPDATE 结果写入的行（即 UPDATE_AFTER 的位置），其余行为 INSERT |

三者的关系：
- 旧 segment 的 `delete vector diff` = `delete_change_type_bitmap` ∪ `update_before_bitmap`（互斥）
- 新 segment 中的行 = INSERT 行 ∪ UPDATE_AFTER 行（由 `update_after_bitmap` 区分）
- `update_before_bitmap` 和 `update_after_bitmap` 构成 UPDATE 的配对关系

#### 示例：一次导入的 bitmap vector 生成

```
导入前状态（version 5）:
  Segment S0: rows [A=1, B=2, C=3, D=4]    delete_vector = {}

导入操作:
  UPDATE A SET val=10    (A 原值 1)
  DELETE B               (B 原值 2)
  INSERT E val=5         (新行)

导入后（version 6）:
  Segment S0: rows [A=1, B=2, C=3, D=4]
    delete_vector = {A, B}               ← A 被 UPDATE，B 被 DELETE
    delete_change_type_bitmap[v6] = {B}  ← 真正的 DELETE
    update_before_bitmap[v6] = {A}       ← UPDATE 的旧值位置

  Segment S1 (新): rows [A=10, E=5]
    update_after_bitmap[v6] = {A}        ← UPDATE 的新值位置
    (E 不在 bitmap 中，因此是 INSERT)
```

查询 version 6 的 changes 时：
- 读 S0 的 `delete_change_type_bitmap[v6]` → B 行：`(B, DELETE, val=2)`
- 读 S0 的 `update_before_bitmap[v6]` → A 行：`(A, UPDATE_BEFORE, val=1)`
- 读 S1 的 `update_after_bitmap[v6]` → A 行：`(A, UPDATE_AFTER, val=10)`
- 读 S1 中不在 `update_after_bitmap` 的行 → E 行：`(E, INSERT, val=5)`

#### 跨版本合并优化

当 CDC 窗口 `(V_old, V_new]` 覆盖多个版本时，多个版本可能在**同一个旧 segment** 上都记录了 delete/update_before bitmap。

```
version 6: S0 的 update_before_bitmap = {A}        → 需要读 S0 的 A 行
version 7: S0 的 delete_change_type_bitmap = {C}    → 需要读 S0 的 C 行
version 8: S0 的 update_before_bitmap = {D}         → 需要读 S0 的 D 行
```

如果逐版本读取，会对 S0 发起 3 次独立的读取请求。优化方式：

**将同一个 segment 上的所有版本的 bitmap 合并（OR），一次批量读取**：

```
merged_bitmap = {A} ∪ {C} ∪ {D} = {A, C, D}
→ 一次读取 S0 的 A, C, D 三行
→ 再根据各版本的原始 bitmap 拆分回各版本的 changes
```

这将多次"不确定位置的随机读"转化为"已知位置的单次批量读"，显著减少 IO 次数。此优化在 Scan 架构（§8）中进一步展开。

### 6.5 明细表

只有 append 操作，直接读取 `(V_old, V_new]` 范围内的 delta rowset，所有行标记为 INSERT。

### 6.6 聚合表

同样只有 append，读取 delta rowset。注意 rowset 中存储的是 aggregate 后的结果，因此 CHANGES 也是聚合后的语义。

示例（Applovin 真实场景）：

```
基表: key1, key2, key3, val1 SUM
导入: (1,1,1,1) (1,1,1,2) (2,2,2,1) (3,3,3,1) (2,2,2,2)

delta rowset 存储（aggregate 后）:
(1,1,1,3) (2,2,2,1) (3,3,3,1) (2,2,2,2)

CHANGES 输出（INSERT 语义）:
(1,1,1,3) (2,2,2,1) (3,3,3,1) (2,2,2,2)
```

---

## 7. Compaction 交互

Compaction 是影响 CDC 正确性的关键因素，需要确保 CDC 窗口内的变更信息在 compaction 后仍然可用。

### 7.1 Compaction 对 Changes 的影响

主键表 compaction 会将多个 segment 合并为一个新 segment，过程中：
- 旧 segment 被合并后逻辑删除
- 新 segment 只包含最新版本的活跃行
- 旧 segment 上的 delete vector 和 bitmap vector 如果跨越了 CDC 窗口，可能影响变更信息

### 7.2 方案 C 的兼容性

Bitmap vector 方案天然兼容 compaction，原因在于 **bitmap vector 记录的是每个 version 对 segment 的增量操作，而不是 segment 之间的 diff**：

- Compaction 生成新 segment 时，PK Index 同样会执行 `try_replace` 流程，新 segment 的 bitmap vector 正确记录了这次 compaction 的变更类型
- 旧 segment 上已记录的 bitmap vector 在 compaction 完成前仍然有效
- Compaction 完成后，如果 CDC 窗口的 `V_old` 在 compaction 之前，旧 segment 的 bitmap vector 仍可用于生成该版本范围的 changes

### 7.3 保留策略

关键约束：**CDC 窗口 `(V_old, V_new]` 引用的 segment 和 bitmap vector 不能被 vacuum 清理掉**。

这与 PITQ 的 `TableVersionKeeper` 订阅机制统一——只要某个版本被 MV subscribe，该版本依赖的元数据和数据文件就不会被清理。具体的保留策略与 PITQ 部分设计一致。

---

## 8. Scan 架构与并行模型

### 8.1 Scan Plan 结构

```
OlapTableChangesScanNode
  ├── scan_ranges: [{tablet_id, start_version, end_version}, ...]
  ├── output_columns: [数据列...] + [CHANGE_TYPE, ROW_ID, ROW_VERSION]
  └── options: {update_semantic: DELETE_INSERT | UPDATE_BEFORE_AFTER, net_changes: bool}
```

BE/CN 侧对应 `OlapChangesDataSource`（通过 `ConnectorScanOperator`，`ConnectorType = OLAP_CHANGES`），每个 tablet 的 scan range 由一个或多个 `OlapChangesDataSource` 实例处理。

### 8.2 单 Tablet Scan 流程（主键表）

对于一个 tablet，version range `(V_old, V_new]`，scan 分为两个阶段：

**阶段 1：收集 bitmap vector，按 segment 聚合**

```
对 (V_old, V_new] 中每个 version V_i:
    收集 V_i 在各旧 segment 上的 delete_change_type_bitmap 和 update_before_bitmap
    收集 V_i 的新 segment 的 update_after_bitmap

按旧 segment 聚合:
    对每个被引用的旧 segment S:
        merged_read_bitmap[S] = ∪ (所有版本在 S 上的 delete + update_before bitmap)
```

**阶段 2：读取数据，生成 changes**

```
并行读取旧 segment:
    对每个被引用的旧 segment S:
        使用 merged_read_bitmap[S] 一次批量读取所有被引用的行
        根据各版本的原始 bitmap，拆分为各版本的 DELETE 和 UPDATE_BEFORE changes

并行读取新 segment:
    对每个 (V_old, V_new] 范围内的新 segment:
        全量顺序读取
        根据 update_after_bitmap 区分 UPDATE_AFTER 和 INSERT
```

### 8.3 并行模型

并行发生在多个维度：

| 维度 | 并行方式 | 说明 |
|------|---------|------|
| Tablet 间 | 不同 CN 节点并行 | FE 按 tablet 分配到不同 CN |
| Version 间 | 同一 tablet 内多个 version 的新 segment 并行读取 | 新 segment 之间独立 |
| Segment 间 | 同一 version 的多个旧 segment 并行读取 | 旧 segment 之间独立 |
| Segment 内 | 列式存储按 column 并行 | 利用 columnar 存储特性 |

**旧值批量读取是性能关键**：通过 bitmap 合并机制（§6.4），将多次随机读合并为单次批量读，每个旧 segment 只读一次。这是方案 C 查询性能可接受的重要保证。

### 8.4 Project / Filter 下推

- **Project（列裁剪）**：数据列支持只读取需要的列，减少 IO。元数据列（CHANGE_TYPE, ROW_ID, ROW_VERSION）根据上层是否需要来决定是否输出。
- **Filter**：
  - `CHANGE_TYPE` filter：可以下推到 scan 层，直接跳过不需要的变更类型。例如 IVM 使用 DELETE+INSERT 模式时可以跳过 UPDATE_BEFORE/UPDATE_AFTER
  - `ROW_VERSION` filter：可以下推用于缩小 version 范围
  - 数据列 filter：需要注意不能影响 Net Changes 的正确性（如果启用 Net Changes，filter 应在 Net Changes 之后）

---

## 9. Net Changes

### 9.1 动机

当 CDC 窗口覆盖多个版本时，同一行可能有多条变更（insert → update → update → delete）。直接交给下游处理所有原始 changes，存在两个问题：

- **数据量大**：中间版本的变更最终被后续变更覆盖，下游处理后结果相同但计算量增大
- **IVM 语义适配**：IVM 只需要知道"两个快照之间的集合差"（delta），不需要中间过程

Net Changes 将同一 `row_id` 下的多条变更合并为最小等价变更。

### 9.2 合并规则

对每个 `row_id`，根据 `row_version` 确定 `first_type`（最小版本的变更类型）和 `last_type`（最大版本的变更类型），然后按规则合并：

| # | first_type | last_type | 输出 | 语义 |
|---|-----------|-----------|------|------|
| 1 | 单条变更 | — | 原样输出 | 只有 INSERT 或 DELETE |
| 2 | INSERT(0) | UPDATE_AFTER(3) | 1 条 INSERT（最终值） | 新建后更新 → 以最终值直接插入 |
| 3 | INSERT(0) | DELETE(1) | 0 条 | 新建后删除 → 相互抵消 |
| 4 | UPDATE_BEFORE(2) | UPDATE_AFTER(3) | 2 条：BEFORE(原始值) + AFTER(最终值) | 多次更新 → 一次更新 |
| 5 | UPDATE_BEFORE(2) | DELETE(1) | 1 条 DELETE（原始值） | 更新后删除 → 直接删除 |

> 规则 4/5 中输出的 `row_version` 统一使用 `max_ver`，确保配对的 UPDATE_BEFORE/UPDATE_AFTER 版本一致。

### 9.3 为什么在计算层做

Net Changes 在计算层而非存储层完成，原因：

1. **Tablet Reshard**：reshard 后同一 row 的不同版本 changes 可能来自不同 tablet，存储层无法跨 tablet 合并，计算层可以通过 shuffle by `row_id` 处理
2. **与并行 scan 解耦**：存储层专注并行 IO 效率，Net Changes 是纯计算逻辑
3. **灵活性**：不同场景对 Net Changes 的需求不同（IVM 需要，审计不需要），在计算层可通过参数控制

### 9.4 计算实现

利用窗口函数，`PARTITION BY row_id` 与分桶键一致时可 local shuffle 避免全局 shuffle：

```sql
WITH base AS (
    SELECT *,
        MIN(row_version) OVER (PARTITION BY row_id) AS min_ver,
        MAX(row_version) OVER (PARTITION BY row_id) AS max_ver,
        COUNT(*)         OVER (PARTITION BY row_id) AS cnt
    FROM changes
),
classified AS (
    SELECT *,
        MIN(CASE WHEN row_version = min_ver THEN change_type END)
            OVER (PARTITION BY row_id) AS first_type,
        MAX(CASE WHEN row_version = max_ver THEN change_type END)
            OVER (PARTITION BY row_id) AS last_type
    FROM base
)
SELECT row_id,
    CASE WHEN cnt > 1 THEN max_ver ELSE row_version END AS row_version,
    CASE
        WHEN first_type = 0 AND last_type = 3 THEN 0
        WHEN first_type = 2 AND last_type = 1 THEN 1
        ELSE change_type
    END AS change_type,
    val
FROM classified
WHERE cnt = 1
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

> 注意：tablet reshard 场景下同一 row 的 changes 可能来自不同 tablet，此时不能用 local shuffle，需要全局 shuffle by `row_id`。但 reshard 频率低，可以接受。

---

## 10. 边界场景与 Fallback

IVM 最小化实现中，部分场景暂不支持增量 CDC，触发 **fallback 到全量刷新**。

### 10.1 Fallback 条件

| 场景 | 原因 | 处理 |
|------|------|------|
| **INSERT OVERWRITE** | 语义上替换整个 partition 的数据，旧 partition 进入 CatalogRecycleBin，CDC 无法简单表示 | Fallback 全量刷新 |
| **DROP / TRUNCATE PARTITION** | 旧 partition 的元数据和数据可能已被 vacuum，无法生成 DELETE changes | Fallback 全量刷新 |
| **Tablet Reshard** | 虽然数据结构上可支持（多段 MaterializedIndex），但 vacuum 机制尚未适配 | Fallback 全量刷新（P1 支持） |
| **Schema Change（非 fast）** | 数据被重写到新 tablet，历史版本不可访问 | Fallback 全量刷新 |
| **CDC 窗口内的版本已被 vacuum** | `TableVersionKeeper` 的版本已过期释放 | Fallback 全量刷新 |

### 10.2 Fallback 机制

IVM 刷新流程中，以下检查点会触发 fallback：

1. `TableVersionKeeper.getTableState(oldVersionId)` 返回空 → 版本已过期
2. `TableStateUtils.buildOlapTable()` 返回空 → partition/tablet 已被 drop/reshard
3. `TableStateUtils.computeDeltaState()` 发现 dropped partitions → partition 被删除

Fallback 不影响正确性——MV 回退到全量刷新，只是消耗更多资源。

---

## 11. 小文件优化

### 11.1 问题

高频实时导入 + MV refresh 间隔较大（如小时级）的场景下，CDC 窗口内会积累大量小文件（每次导入一个 delta rowset）。逐文件 scan 的 IO 开销高。

### 11.2 优化思路

**前提**：只需 Net Changes，且 Update 使用 DELETE+INSERT 模式（IVM 场景适用）。

**思路**：不逐文件读取 delta rowset 生成 changes，而是直接比较 old 和 new 两个版本的文件。new 版本经过 compaction 后文件数量少、数据紧凑，比较效率远高于逐文件 scan。

```
old version (V_old): compacted segments [S0_old, S1_old]
new version (V_new): compacted segments [S0_new, S1_new]

比较 new - old，差异即为 Net Changes
```

### 11.3 Carry-over Row 去重

直接比较 old/new 版本文件时，存在 **carry-over row** 问题：某些行在 old 和 new 中都存在且未变化，但由于 compaction 重新组织了文件，可能出现在 diff 结果中。

解决方式：在计算层通过 `(row_id, row_version)` 去重——如果同一 `row_id` 在 old 和 new 中的 `row_version` 相同，说明该行未变化，过滤掉。

### 11.4 限制

- **聚合表不适用**：compaction 后会 aggregate，无法还原出增量变更
- 需要 old 和 new 版本都经过充分 compaction 才能发挥效果

---

## 12. 总结与 Roadmap

### 12.1 短期（P0）— IVM 最小可行

| 能力 | 范围 |
|------|------|
| DML 覆盖 | INSERT / UPDATE / DELETE / 各类 LOAD（不含 INSERT OVERWRITE） |
| 表类型 | 明细表、主键表、聚合表 |
| Update 语义 | DELETE + INSERT（IVM 模式） |
| Net Changes | 支持 |
| Row Tracking | 支持 |
| Version Range | 只能查询被 MV 引用的范围（依赖 TableVersionKeeper subscribe） |
| 边界场景 | DROP/TRUNCATE PARTITION、Tablet Reshard、Schema Change → Fallback 全量刷新 |

### 12.2 短期（P1）

| 能力 | 范围 |
|------|------|
| DROP/TRUNCATE PARTITION | 适配 CatalogRecycleBin 保留策略，生成 DELETE changes |
| Tablet Reshard | 适配 vacuum 机制，支持多段 MaterializedIndex 的 changes 读取 |
| 小文件优化 | 基于 compaction 后的文件比较 |
| UPDATE_BEFORE + UPDATE_AFTER | 支持完整 UPDATE 语义（面向流计算场景） |

### 12.3 长期

| 能力 | 范围 |
|------|------|
| 任意版本查询 | 元数据多版本数据结构设计（与 PITQ / Time Travel 统一） |
| SQL 接口 | `CHANGES` clause / Table Function |
| STREAM 对象 | 自动 offset 管理 |
| Flink / Spark 集成 | SDK / RPC 对接 |
| PUSH 模式 | 服务端主动推送 |
