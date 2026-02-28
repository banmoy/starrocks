# StarRocks CDC (Change Data Capture) 研究报告

> 日期: 2026-02-28
> 目的: 分析 StarRocks 中 CDC 的现有实现、架构设计，并探讨增强方向

---

## 1. 概述

StarRocks 在 CDC 方面有两个维度:

| 维度 | 说明 | 现状 |
|------|------|------|
| **CDC 数据写入 (Inbound)** | 将外部数据源的变更数据实时同步到 StarRocks | ✅ 成熟，多种工具支持 |
| **CDC 数据输出 (Outbound)** | 将 StarRocks 内部的数据变更事件输出给下游消费 | ⚠️ 有内部 Binlog 基础设施，但未对外开放 |

---

## 2. Inbound CDC — 将变更数据写入 StarRocks

### 2.1 支持的外部 CDC 工具

| 工具 | 数据源 | 特性 |
|------|--------|------|
| **Flink CDC 3.0** | MySQL, PostgreSQL, Oracle, SQL Server, TiDB | Schema 变更同步, 全库同步, 分库分表合并, Exactly-once |
| **Flink CDC 2.x + SMT** | 同上 | 需要 SMT 同步 schema |
| **Debezium + Kafka** | PostgreSQL, MySQL, Oracle, SQL Server | 通过 Kafka Connector 的 `AddOpFieldForDebeziumRecord` Transform |
| **Canal** | MySQL (binlog) | Canal → Kafka → Routine Load |
| **CloudCanal (BladePipe)** | MySQL, Oracle, PostgreSQL, SQL Server, TiDB, Hana, PolarDB, Db2 | 商业工具 |
| **Maxwell** | MySQL | JSON 格式 CDC 事件 |
| **SeaTunnel** | 多种 | Exactly-once 语义 |

### 2.2 StarRocks 原生加载方式对 CDC 的支持

所有加载方式都通过 `__op` 字段支持 UPSERT 和 DELETE 操作（仅限 Primary Key 表）:

```
__op = 0 或 'upsert' → UPSERT (INSERT 或 UPDATE)
__op = 1 或 'delete' → DELETE
```

| 加载方式 | 支持格式 | CDC 操作 |
|---------|---------|----------|
| **Stream Load** | CSV, JSON | UPSERT, DELETE via `__op` |
| **Broker Load** | CSV, JSON, Parquet, ORC | UPSERT, DELETE via SET clause |
| **Routine Load** | JSON, CSV, Avro (Kafka/Pulsar) | UPSERT, DELETE via column mapping |
| **Flink Connector** | - | UPSERT, DELETE, 部分更新, 条件更新 |
| **Kafka Connector** | JSON, Avro | 通过 Debezium Transform 添加 `__op` |

### 2.3 Primary Key 表 — CDC 的核心基础

Primary Key 表是 StarRocks 支持 CDC 写入的核心表类型:

- **UPSERT**: 基于主键的 Insert 或 Update (Delete+Insert 策略)
- **DELETE**: 基于主键删除 (`__op=1`)
- **部分更新**: `partial_update=true`，支持行模式和列模式
- **条件更新**: `merge_condition` 参数，防止旧数据覆盖新数据
- **主键索引**: HashMap 映射 PK → (rowset_id, segment_id, rowid)
- **DelVector**: 每个 segment 文件存储删除标记

---

## 3. Outbound CDC — StarRocks 内部 Binlog 机制

### 3.1 架构总览

StarRocks 已经内建了一套完整的 Binlog 基础设施，主要用于**物化视图 (MV) 增量刷新**:

```
┌──────────────────────────────────────────────────────────────────────┐
│                         FE (Frontend)                                │
│  BinlogManager: 管理 binlog 元数据、可用性追踪、版本协调              │
│  BinlogScanNode: 查询优化器中的 binlog 扫描节点                      │
│  LogicalBinlogScanOperator: 逻辑优化器算子                           │
│  BinlogConsumeStateVO: MV 消费状态跟踪                               │
└─────────────────────────────┬────────────────────────────────────────┘
                              │ Thrift RPC
┌─────────────────────────────▼────────────────────────────────────────┐
│                         BE (Backend)                                  │
│  BinlogManager: 每个 tablet 的 binlog 生命周期管理                    │
│  BinlogBuilder: 在数据写入时构建 binlog 条目                          │
│  BinlogReader: 读取变更事件                                           │
│  BinlogFileWriter/Reader: 底层 binlog 文件读写                        │
│  BinlogConnector: 查询执行引擎的 binlog 数据源                        │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Binlog 配置

**表级别属性** (通过 `ALTER TABLE ... SET` 设置):

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `binlog_enable` | false | 启用/禁用 binlog |
| `binlog_ttl_second` | 1800 (30分钟) | Binlog 保留时间 |
| `binlog_max_size` | Long.MAX_VALUE | Binlog 最大总大小 |
| `binlog_version` | - | 配置版本号 (用于协调) |

**BE 级别配置**:

| 参数 | 说明 |
|------|------|
| `binlog_file_max_size` | 单个 binlog 文件最大大小 |
| `binlog_page_max_size` | 单个 page 最大大小 |

### 3.3 Binlog 文件格式

```
┌──────────────────────┬───────┬────────┬───────┬─────────────────────────┐
│   File Header        │ Page  │ ...... │ Page  │ File Footer [optional]  │
│ (BinlogFileHeaderPB) │       │        │       │   (BinlogFileMetaPB)    │
└──────────────────────┴───────┴────────┴───────┴─────────────────────────┘

每个 Page:
┌─────────────────────────┬──────────────────────────────────────────────────┐
│ Page Header             │              Page Content                        │
│ (PageHeaderPB)          │ LogEntry | LogEntry | ... | LogEntry             │
│ - version               │ (LogEntryPB: INSERT_RANGE, UPDATE, DELETE, EMPTY)│
│ - compress_type         │                                                  │
│ - start_seq_id          │                                                  │
│ - end_seq_id            │                                                  │
│ - timestamp_in_us       │                                                  │
└─────────────────────────┴──────────────────────────────────────────────────┘
```

### 3.4 日志条目类型 (LogEntryPB)

| 类型 | Protobuf 定义 | 说明 |
|------|--------------|------|
| `INSERT_RANGE_PB` | `InsertRangePB {file_id, start_row_id, num_rows}` | 批量插入 |
| `UPDATE_PB` | `UpdatePB {before_file_id, before_row_id, after_file_id, after_row_id}` | 更新 (生成 UPDATE_BEFORE + UPDATE_AFTER) |
| `DELETE_PB` | `DeletePB {file_id, row_id}` | 删除 |
| `EMPTY_PB` | - | 无数据变更的写入 |

### 3.5 LSN (Log Sequence Number) 系统

```cpp
struct BinlogLsn {
    uint128_t lsn;  // 高64位: version (publish version), 低64位: seq_id
};
```

LSN 唯一标识每个变更事件，用于 seek 和消费进度跟踪。

### 3.6 变更事件元数据列

每个变更事件包含以下元数据:

| 列名 | 类型 | 说明 |
|------|------|------|
| `_binlog_op` | TINYINT | 操作类型: INSERT(0), UPDATE_BEFORE(1), UPDATE_AFTER(2), DELETE(3) |
| `_binlog_version` | BIGINT | 生成变更事件的版本号 |
| `_binlog_seq_id` | BIGINT | 版本内的序列号 |
| `_binlog_timestamp` | BIGINT | 变更时间戳 (微秒) |

### 3.7 Binlog 生命周期管理

**写入流程** (在 Tablet 写入时):

```
begin_ingestion()  →  BinlogBuilder 构建  →  precommit_ingestion()  →  commit_ingestion()
       |                                            |
       | (构建失败)                                  | (RowsetMeta 持久化失败)
       v                                            v
  abort_ingestion()                           delete_ingestion()
```

**文件状态机**:

```
Alive (活跃，可服务读请求)
  ↓ (过期或超容量)
Wait Reader (等待读者完成)
  ↓ (无活跃读者)
Unused (可删除)
  ↓
Deleted (物理删除)
```

**崩溃恢复**: 从高版本到低版本恢复，使用 `min_valid_lsn` 和 `sorted_valid_versions` 验证数据完整性。

### 3.8 查询执行集成

Binlog 已经集成到 StarRocks 的查询优化器和执行引擎:

- `LogicalBinlogScanOperator` → 逻辑算子
- `StreamScanImplementationRule` → 物理算子转换规则
- `BinlogScanNode` → 物理扫描节点
- `BinlogConnector` / `BinlogDataSource` → Pipeline 执行引擎数据源
- 支持 Stream Pipeline (用于 MV 增量刷新) 和 Non-Stream Pipeline

### 3.9 物化视图增量刷新

Binlog 的主要消费者是 MV 增量刷新:

- `BinlogConsumeStateVO` 跟踪每个 tablet 的消费进度
- `MVMaintenanceTask` 使用 binlog 进行增量更新
- 通过 `StreamEpochManager` 管理消费 epoch

### 3.10 当前限制

1. **仅支持 Duplicate Key 表**: Binlog 目前主要支持 Duplicate Key 表 (Primary Key 表支持可能在开发中)
2. **仅跟踪增量 rowset**: 不跟踪 base compaction 结果
3. **版本连续性要求**: 恢复需要连续版本
4. **未对外暴露**: 没有 SQL 语法或 API 供用户直接消费 binlog
5. **保留时间有限**: 默认 TTL 仅 30 分钟

---

## 4. 增强 CDC 支持的可能方向

### 方向 A: 暴露 Binlog 为外部可消费的 CDC 流

**思路**: 在现有 binlog 基础设施上构建外部消费接口

**需要的工作**:

1. **SQL 语法**: 添加类似 `SUBSCRIBE TO CHANGES ON table` 的语法
2. **HTTP/gRPC API**: 提供 binlog 消费的 API 端点
3. **Kafka 发布**: 将变更事件发布到 Kafka topic
4. **Primary Key 表支持**: 扩展 binlog 支持到 Primary Key 表
5. **Schema Evolution**: 处理 schema 变更时的 binlog 兼容性
6. **增强保留策略**: 支持更长的保留时间和基于消费者进度的保留

**优点**: 复用现有基础设施，实现成本相对较低
**挑战**: Primary Key 表的 binlog 支持、分布式协调、消费者管理

### 方向 B: 基于 WAL 的 CDC

**思路**: 在 Write-Ahead Log 层面捕获变更

**需要的工作**:

1. 实现或利用 WAL 机制
2. WAL 到 CDC 事件的转换层
3. 分布式 WAL 聚合

**优点**: 低延迟，与存储引擎紧密集成
**挑战**: 实现复杂度高，WAL 格式可能随版本变化

### 方向 C: 增强 Flink CDC 反向集成

**思路**: 开发 StarRocks 作为 Flink CDC 的 Source Connector

**需要的工作**:

1. 实现 Flink CDC Source Connector for StarRocks
2. 利用现有 binlog 机制提供变更流
3. 支持全量快照 + 增量变更

**优点**: 复用 Flink CDC 生态系统，社区接受度高
**挑战**: 需要对外暴露 binlog 消费 API

### 方向 D: 基于触发器/物化视图的 CDC

**思路**: 利用 MV 机制捕获变更并转发

**需要的工作**:

1. 创建特殊类型的 MV 作为 CDC sink
2. MV 刷新时将变更发送到外部系统
3. 支持 Kafka、HTTP webhook 等 sink 类型

**优点**: 利用现有 MV 增量刷新机制
**挑战**: 增加 MV 系统复杂度

---

## 5. 关键源代码文件索引

### FE (Java)

| 文件路径 | 说明 |
|---------|------|
| `fe/fe-core/src/main/java/com/starrocks/binlog/BinlogManager.java` | Binlog 生命周期管理 |
| `fe/fe-core/src/main/java/com/starrocks/binlog/BinlogConfig.java` | Binlog 配置 |
| `fe/fe-core/src/main/java/com/starrocks/planner/BinlogScanNode.java` | Binlog 物理扫描节点 |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/stream/LogicalBinlogScanOperator.java` | 逻辑优化器算子 |
| `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/BinlogConsumeStateVO.java` | MV 消费状态 |
| `fe/fe-core/src/main/java/com/starrocks/common/util/PropertyAnalyzer.java` | 属性解析 (binlog 配置) |

### BE (C++)

| 文件路径 | 说明 |
|---------|------|
| `be/src/storage/binlog_manager.h/cpp` | 核心 binlog 管理 |
| `be/src/storage/binlog_builder.h/cpp` | Binlog 条目构建 |
| `be/src/storage/binlog_reader.h/cpp` | Binlog 变更事件读取 |
| `be/src/storage/binlog_file_writer.h/cpp` | Binlog 文件写入 |
| `be/src/storage/binlog_file_reader.h/cpp` | Binlog 文件读取 |
| `be/src/storage/binlog_util.h/cpp` | 工具类和 BinlogLsn |
| `be/src/connector/binlog_connector.h/cpp` | 查询执行引擎 binlog 连接器 |

### Protobuf/Thrift 定义

| 文件路径 | 说明 |
|---------|------|
| `gensrc/proto/binlog.proto` | Binlog 文件格式定义 |
| `gensrc/thrift/AgentService.thrift` | TBinlogConfig |
| `gensrc/thrift/Types.thrift` | TBinlogOffset |
| `gensrc/thrift/PlanNodes.thrift` | TBinlogScanRange, TBinlogScanNode |

---

## 6. 总结与建议

StarRocks 已经具备了相当完整的内部 binlog 基础设施，主要服务于 MV 增量刷新。如果要增强 CDC 支持，**方向 A (暴露 Binlog)** 和 **方向 C (Flink CDC Source Connector)** 是最务实的选择:

1. **短期**: 扩展 binlog 支持到 Primary Key 表，增加 binlog 保留策略
2. **中期**: 开发 Binlog 消费 API (HTTP/gRPC)，支持外部消费者
3. **长期**: 开发 Flink CDC Source Connector for StarRocks，融入 CDC 生态

关键前提是需要先解决 Primary Key 表的 binlog 支持问题，因为 CDC 场景中 Primary Key 表是最常用的表类型。
