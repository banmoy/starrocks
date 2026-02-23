# GTID 与 Time Travel 版本定位能力分析

## 背景

Time Travel 技术挑战之一是 `timestamp → Table Version` 映射不存在（参见 `time_travel_proposal_shared_data.md` 4.2.2）。本文分析已有的 GTID 机制能否满足这一需求。

## GTID 机制概述

### 结构

GTID（Global Transaction ID）为 64-bit ID，结构如下：

```
|-- 1bit --|-- 42bit --|-- 8bit --|-- 13bit --|
|    0     | timestamp |  cluster |  sequence |
```

- **timestamp**（42 bits）：自 epoch（2020-01-01 00:00:00 UTC）起的毫秒数
- **cluster**（8 bits）：集群 ID，当前为 0
- **sequence**（13 bits）：同一毫秒内的序列号（最多 8192/ms）

### 生成

FE `GtidGenerator.nextGtid()` 在事务 COMMIT 时生成，写入 `TransactionState.globalTransactionId`。时钟回退时使用上次时间戳保证单调性。

辅助方法 `GtidGenerator.getGtid(timestamp)` 可从任意时间戳 O(1) 构造对应的 GTID（sequence = 0）。

### 存储

- **FE**：`TransactionState` 持久化到 journal（`@SerializedName("gtid")`）
- **BE**：Publish version 时写入 `TabletMetadataPB.gtid` 字段（对象存储上的 tablet metadata 文件）

> 参考代码：`fe/.../transaction/GtidGenerator.java`、`fe/.../transaction/TransactionState.java`、`be/src/storage/lake/transactions.cpp`、`gensrc/proto/lake_types.proto`

### 覆盖范围

GTID **仅覆盖 DML 事务**。`cloud_native_table_concepts_state_version_snapshot.md` 明确指出：

> GTID 仅覆盖 DML 事务，无法覆盖所有 Table State 变更（如 TRUNCATE PARTITION 等 DDL 和系统后台物理变化）。

以下操作**没有** GTID：DDL（TRUNCATE、DROP PARTITION、Schema Change）、Compaction、Reshard。

## Shared-Nothing 下的 GTID → Version 能力

Shared-nothing BE 每个 tablet 在内存中维护 `_gtid_to_version_map`（`std::map<int64_t, int64_t>`），提供 GTID 到 tablet version 的映射：

```cpp
// be/src/storage/tablet.cpp
Status Tablet::capture_consistent_rowsets(const int64_t gtid, vector<RowsetSharedPtr>* rowsets) {
    auto it = _gtid_to_version_map.upper_bound(gtid);
    if (it != _gtid_to_version_map.begin()) {
        --it;
        version = it->second;
    }
    // ... use version to get rowsets
}
```

这是因为 shared-nothing BE 有状态——tablet 常驻内存，map 随 edit log 持续更新。

## Shared-Data 下的 GTID → Version 能力

Shared-data 下**不存在**等价的定位路径：

1. **CN 无状态**：CN 不持有 tablet 常驻状态，没有 `_gtid_to_version_map`。
2. **Tablet metadata 按 version 存储**：对象存储上的 metadata 文件以 version 编号命名，GTID 仅是 `TabletMetadataPB` 内的一个字段。从 GTID 反查 version 需要遍历 metadata 文件，代价不可接受。
3. **查询路径由 FE 驱动**：当前 shared-data 的查询路径是 FE 将 `visibleVersion` 下发给 CN，CN 据此从对象存储取 metadata 文件并读数据。CN 不参与版本定位。

因此在 shared-data 下，**版本定位必须在 FE 侧完成**。

## 能否满足 Time Travel 的 timestamp → Table Version 需求

### 不能独立满足

| 维度 | 要求 | GTID 现状 | 差距 |
|------|------|-----------|------|
| 覆盖范围 | 所有 State 变更（DML + DDL + 系统操作） | 仅 DML 事务 | DDL / Compaction / Reshard 无 GTID |
| 定位粒度 | 表级 Table Version | Per-transaction，一个 GTID 可能涉及多表 | 无表级版本语义 |
| 定位端 | FE 侧（需在查询规划阶段完成） | BE/CN 侧无 shared-data 映射 | FE 无 `timestamp → version` 索引 |
| Meta 回溯 | 历史表定义 + 历史拓扑 | 不涉及 Meta | 完全不覆盖 |

### 可复用的价值

1. **时间编码**：42-bit timestamp 内嵌于 GTID，`getGtid(timestamp)` 可 O(1) 构造，为 timestamp → 版本排序提供基础设施。
2. **单调递增序列**：集群级单调性可作为 Table Version 的排序基础。
3. **Tablet metadata 审计**：GTID 已写入 `TabletMetadataPB`，可用于关联分析和一致性校验。

## 结论

GTID 可作为 Table Version 的**候选实现基础**（复用其时间编码和单调性），但需要：

1. **扩展覆盖范围**：为 DDL 和系统操作（Compaction / Reshard）也生成版本标识，或建立独立的 Table Version 序列。
2. **建立 FE 侧映射**：为每个表维护可检索的 `(timestamp / Table Version, {pp_id → visibleVersion})` 映射，支持 FE 在查询规划阶段完成版本定位。
3. **配合 Meta 历史化**：版本定位只是第一步，还需要 Meta 历史化机制来提供历史表定义和拓扑（参见 `time_travel_proposal_shared_data.md` 4.2.3 设计空间分析）。
