# StarRocks Time Travel 产品需求文档（PRD）大纲

> **适用架构**：存算分离（Shared-Data / Cloud-Native）
> **相关文档**：
> - [产品设计](../../.cursor/plans/time_travel_product_design_b9ad5bc6.plan.md)
> - [Cluster Snapshot 与 Time Travel 差异分析](./time_travel_vs_cluster_snapshot_analysis.md)
> - [Cloud-Native Table State 与 Snapshot 概念](./cloud_native_table_concepts_state_and_snapshot.md)

---

## 一、概述与背景

### 1.1 项目背景

- StarRocks 存算分离架构的发展现状
- 数据保护能力现状：RECOVER（RecycleBin）、Backup/Restore、Cluster Snapshot
- 现有机制在"表级数据恢复"和"历史数据查询"方面的空白
- 竞品（Snowflake、Databricks/Delta Lake）已将 Time Travel 作为标配能力

### 1.2 项目目标

- 一句话定义：为存算分离架构下的 Cloud-Native Table 提供表级时间旅行查询与数据恢复能力
- 核心价值命题：让用户对数据的时间维度拥有完整掌控力——观察、恢复、审计、重现

### 1.3 目标用户

| 用户角色 | 核心需求 |
|---------|---------|
| 数据工程师 | 误操作恢复、ETL 验证 |
| 数据分析师 | 报表重现、跨时间点对比 |
| DBA | 问题定位、表/库恢复 |
| 合规官员 | 数据审计、合规取证 |
| 数据科学家 | ML 训练数据复现、Point-in-Time 特征拼接 |

### 1.4 非目标（Scope 限定）

- 存算一体架构不在本期范围
- 外部 Catalog 表（Iceberg/Delta Lake/Hudi）的 Time Travel 由各自 Connector 支持，不在本 PRD 范围
- ASOF JOIN 语法为后续阶段独立 PRD
- Change Data Capture（CDC / 增量变更追踪）为独立项目

---

## 二、需求概览与分层

### 2.1 需求分层模型

```
Tier 1: 时间点查询（Point-in-Time Query）—— 核心能力
Tier 2: 数据恢复（Data Recovery）—— 高价值场景
Tier 3: 生命周期管理（Lifecycle Management）—— 成本与运维
```

### 2.2 交付阶段规划

| 阶段 | 能力 | 优先级 |
|------|------|--------|
| 第一期 | Tier 1 时间点查询 + 保留策略配置 + RESTORE + UNDROP | P0-P1 |
| 第二期 | 跨时间点 JOIN + 分区级 RESTORE | P2 |
| 第三期 | 存储成本可视化 + Fail-safe 安全网 | P3 |

### 2.3 与现有功能的关系

- 与 Cluster Snapshot 的互补定位（引用差异分析文档）
- 对存算分离模式下 RECOVER 功能的替代关系
- 与 Backup/Restore 的区别

---

## 三、Tier 1：时间点查询

### 3.1 用户场景

- 场景 1：数据验证——ETL 前后数据对比
- 场景 2：报表重现——历史报表数字复现
- 场景 3：问题定位——定位某时刻的异常数据
- 场景 4：审计合规——证明特定时间点的数据状态
- 场景 5：ML 训练数据复现——用版本号精确回溯训练集

### 3.2 SQL 语法规格

#### 3.2.1 FOR TIMESTAMP AS OF

```sql
SELECT ... FROM table_name FOR TIMESTAMP AS OF timestamp_expr;
```

- 语法定义与 EBNF
- `timestamp_expr` 支持的表达式类型：字符串字面量、函数表达式、算术表达式（`current_timestamp() - INTERVAL '2' HOUR`）
- 与 WHERE / GROUP BY / ORDER BY / LIMIT 等子句的组合规则
- 在子查询、CTE、VIEW 中的行为
- 与已有 Iceberg Time Travel 语法的一致性要求

#### 3.2.2 FOR VERSION AS OF

```sql
SELECT ... FROM table_name FOR VERSION AS OF version_number;
```

- 版本号的定义与获取方式
- 版本号与时间戳的映射关系

#### 3.2.3 相对时间查询（语法糖）

```sql
SELECT ... FROM table_name FOR TIMESTAMP AS OF current_timestamp() - INTERVAL '2' HOUR;
```

- 支持的 INTERVAL 单位
- 表达式求值时机（查询规划阶段一次性求值）

#### 3.2.4 跨时间点 JOIN（第二期）

```sql
SELECT ... FROM table_a
JOIN table_b FOR TIMESTAMP AS OF timestamp_expr AS alias
ON join_condition;
```

- 同一张表的当前版本与历史版本 JOIN
- 不同表的不同时间点 JOIN
- 与普通 JOIN 的语义差异

### 3.3 语义与行为定义

#### 3.3.1 版本解析规则

- **时间戳解析**：查找目标时间戳**之前或等于**该时间的最近一个已提交版本
- **版本号解析**：精确匹配指定版本号；不存在则报错
- **表不存在**：如果表在目标时间点尚未创建，报错
- **表已被 DROP**：在 UNDROP 保留期内，仍可对已 DROP 的表进行 Time Travel 查询（需明确定义行为）

#### 3.3.2 时区处理

- 时间戳按 session timezone 解析
- 内部以 UTC 存储与比较
- 跨时区查询的行为

#### 3.3.3 事务隔离

- Time Travel 查询与并发写入的隔离级别
- 查询一致性保证（快照读）

### 3.4 限制与约束

- 仅支持 Cloud-Native Table（存算分离架构）
- 不支持系统表、临时表
- DDL 变更后的 Schema 演化行为定义（历史版本使用历史 Schema 还是当前 Schema）
- 查询只读，不可在 Time Travel 结果集上执行 DML

---

## 四、Tier 2：数据恢复

### 4.1 用户场景

- 场景 1：误 DELETE / UPDATE 数据
- 场景 2：误 DROP TABLE
- 场景 3：误 DROP DATABASE
- 场景 4：ETL 写入脏数据需要回滚
- 场景 5：TRUNCATE TABLE 后恢复

### 4.2 RESTORE TABLE

#### 4.2.1 语法

```sql
-- 按时间戳恢复
ALTER TABLE table_name RESTORE TO TIMESTAMP timestamp_expr;

-- 按版本号恢复
ALTER TABLE table_name RESTORE TO VERSION version_number;

-- 分区级恢复（第二期）
ALTER TABLE table_name PARTITION (partition_spec)
RESTORE TO TIMESTAMP timestamp_expr;
```

#### 4.2.2 语义与行为

- RESTORE 创建一个新版本，内容等同于目标历史版本（非覆盖历史）
- RESTORE 操作本身可被后续 Time Travel 查询到（可审计、可逆）
- 原子性保证：要么全部成功，要么全部失败
- RESTORE 期间的并发读写行为
- RESTORE 后物化视图的刷新策略

#### 4.2.3 Schema 演化处理

- 如果当前 Schema 与目标版本的 Schema 不同，RESTORE 的行为定义
- 新增列如何处理（填充默认值 / NULL）
- 删除列如何处理
- 类型变更如何处理
- 不兼容变更的处理策略

#### 4.2.4 分区级 RESTORE（第二期）

- 仅恢复指定分区的数据
- 跨分区一致性考量
- 分区定义变更时的行为

### 4.3 UNDROP TABLE / DATABASE

#### 4.3.1 语法

```sql
UNDROP TABLE [db_name.]table_name;
UNDROP DATABASE db_name;
```

#### 4.3.2 语义与行为

- DROP 后对象进入"回收站"状态
- 保留期内可通过 UNDROP 恢复到 DROP 前的最新状态
- 同名冲突处理：如果已有同名对象，UNDROP 报错
- 保留期外不可恢复
- UNDROP 后物化视图、权限、调度任务的恢复行为
- 与现有 RECOVER 命令的关系和迁移策略

### 4.4 查看历史版本

#### 4.4.1 语法

```sql
SHOW HISTORY FOR TABLE [db_name.]table_name [LIMIT n];
```

#### 4.4.2 输出字段

| 字段 | 说明 |
|------|------|
| version | 版本号 |
| timestamp | 版本创建时间 |
| operation | 产生该版本的操作类型（INSERT/DELETE/UPDATE/DDL/RESTORE 等） |
| rows_affected | 影响的行数（如果可获取） |

---

## 五、Tier 3：生命周期管理

### 5.1 保留策略

#### 5.1.1 三层保留模型

```
活跃区（当前版本）→ Time Travel 区（可查询的历史版本）→ 安全网 Fail-safe（仅管理员可恢复）→ 永久删除
```

#### 5.1.2 保留策略配置

- 系统级默认值（FE Config）
- 数据库级别覆盖
- 表级别覆盖
- 级联继承规则：`表设置 > 数据库设置 > 系统默认值`

#### 5.1.3 保留期参数

| 参数 | 默认值 | 范围 | 级别 | 说明 |
|------|--------|------|------|------|
| `default_time_travel_retention` | 7d | 0-365d | 系统 | 全局默认保留期 |
| `time_travel_retention` | 继承上级 | 0-365d | 数据库/表 | 对象级保留期 |
| `time_travel_failsafe_retention` | 7d | 固定 | 系统 | 安全网保留期（不可修改） |

#### 5.1.4 保留期变更行为

- 增大保留期：立即生效，已有历史版本按新策略保留
- 缩小保留期：立即生效，超出新保留期的历史版本在下一次 GC 时清理
- 设为 0：关闭 Time Travel，不再保留历史版本

### 5.2 存储成本可视化（第三期）

#### 5.2.1 表级存储信息

```sql
SHOW TIME TRAVEL STORAGE FOR TABLE table_name;
```

输出：当前数据大小、历史版本数据大小、总大小、最早可用版本/时间戳

#### 5.2.2 数据库级存储信息

```sql
SHOW TIME TRAVEL STORAGE FOR DATABASE db_name;
```

### 5.3 手动清理

```sql
ALTER TABLE table_name PURGE VERSIONS BEFORE timestamp_expr;
```

- 仅在紧急释放空间时使用
- 不可逆操作，需明确确认
- 不影响 Fail-safe 区数据

### 5.4 安全网 Fail-safe（第三期）

- Time Travel 保留期到期后，数据进入 Fail-safe 期
- Fail-safe 期内仅 ADMIN 可执行恢复操作
- Fail-safe 期固定 7 天，不可修改
- Fail-safe 期到期后数据永久删除

---

## 六、权限与安全

### 6.1 权限模型

| 操作 | 所需权限 |
|------|---------|
| Time Travel 查询 (`SELECT ... FOR TIMESTAMP/VERSION AS OF`) | 表的 SELECT 权限 |
| RESTORE TABLE | 表的 ALTER + INSERT 权限 |
| UNDROP TABLE | 数据库的 CREATE TABLE 权限 |
| UNDROP DATABASE | CREATE DATABASE 权限 |
| 修改保留策略 | 表/数据库的 ALTER 权限 |
| 手动清理 (PURGE) | 表的 ALTER + DROP 权限 |
| 查看存储信息 | 表的 SELECT 或 ALTER 权限 |
| Fail-safe 恢复 | ADMIN 权限 |

### 6.2 审计日志

- RESTORE / UNDROP / PURGE 等关键操作需记录审计日志
- 审计日志内容：操作者、操作时间、操作类型、目标对象、目标版本/时间戳

---

## 七、错误处理

### 7.1 错误分类与错误信息

| 错误场景 | 错误码 | 错误信息模板 |
|---------|--------|------------|
| 查询时间超出保留期 | 待定 | `Time travel query failed. The requested timestamp '{ts}' is beyond the retention period. The earliest available timestamp for table '{tbl}' is '{earliest_ts}' (retention: {retention}). To extend retention, run: ALTER TABLE {tbl} SET ('time_travel_retention' = '...');` |
| 指定版本不存在 | 待定 | `Version {ver} does not exist for table '{tbl}'. Use SHOW HISTORY FOR TABLE {tbl} to see available versions.` |
| 表在目标时间点不存在 | 待定 | `Table '{tbl}' did not exist at timestamp '{ts}'. The table was created at '{create_ts}'.` |
| UNDROP 同名冲突 | 待定 | `Cannot undrop table '{tbl}': a table with the same name already exists. Rename or drop the existing table first.` |
| RESTORE Schema 不兼容 | 待定 | `Cannot restore table '{tbl}' to version {ver}: schema is incompatible. {detail}` |
| 不支持的表类型 | 待定 | `Time travel is not supported for {table_type} tables. Only cloud-native tables support time travel.` |
| 保留期配置无效 | 待定 | `Invalid time_travel_retention value '{val}'. Valid range: 0 to 365 days.` |

### 7.2 错误信息设计原则

- 明确说明问题原因
- 提供可操作的修复建议（如 ALTER TABLE ... SET 命令）
- 包含当前有效的约束信息（保留期、最早可用时间等）

---

## 八、配置参数

### 8.1 FE 配置参数

| 参数 | 默认值 | 是否动态 | 说明 |
|------|--------|---------|------|
| `default_time_travel_retention` | `7d` | 是 | 全局默认 Time Travel 保留期 |
| `time_travel_failsafe_retention` | `7d` | 否 | 安全网保留期，固定不可修改 |

### 8.2 表/数据库属性

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `time_travel_retention` | 继承上级 | 对象级 Time Travel 保留期 |

### 8.3 与现有 Vacuum 参数的关系

- `lake_autovacuum_grace_period_minutes`：Vacuum 宽限期，保护执行中的查询
- `lake_autovacuum_max_previous_versions`：Vacuum 最大保留版本数
- Time Travel 保留期独立于 Vacuum 参数，两者取 max 值
- 实际保留期 = `max(vacuum_grace_period, time_travel_retention)`

---

## 九、可观测性

### 9.1 Metrics

| Metric 名称 | 类型 | 说明 |
|-------------|------|------|
| `time_travel_query_total` | Counter | Time Travel 查询总次数 |
| `time_travel_query_failed_total` | Counter | Time Travel 查询失败次数（按错误类型分标签） |
| `time_travel_restore_total` | Counter | RESTORE 操作总次数 |
| `time_travel_restore_duration_ms` | Histogram | RESTORE 操作耗时分布 |
| `time_travel_undrop_total` | Counter | UNDROP 操作总次数 |
| `time_travel_historical_storage_bytes` | Gauge | 历史版本占用存储空间 |
| `time_travel_retained_versions` | Gauge | 保留的历史版本数量 |

### 9.2 日志

- Time Travel 查询的关键路径日志（DEBUG 级别）
- RESTORE / UNDROP 操作的操作日志（INFO 级别）
- 版本清理（GC）的执行日志（INFO 级别）

### 9.3 系统表 / Information Schema

- 考虑在 `information_schema` 中新增 Time Travel 相关的系统视图
- 展示各表的 Time Travel 配置、最早可用版本、存储占用等信息

---

## 十、性能与资源

### 10.1 性能要求

| 指标 | 要求 |
|------|------|
| Time Travel 查询延迟 | 与同表当前版本查询相比，额外开销 < 10%（版本解析开销） |
| RESTORE 操作延迟 | 表级 RESTORE（元数据操作）应在秒级完成 |
| UNDROP 操作延迟 | 秒级完成 |
| SHOW HISTORY 延迟 | 秒级完成 |
| 版本解析延迟 | 时间戳/版本号到具体版本的映射 < 100ms |

### 10.2 资源影响

- **对象存储**：历史版本数据文件延迟删除导致的额外存储开销
- **FE 内存**：历史版本元数据的内存占用
- **GC 负载**：版本清理的 CPU 和 I/O 开销
- 需提供存储开销的估算公式或经验数据

### 10.3 可扩展性

- 大量历史版本（365 天保留）下的元数据管理性能
- 高并发 Time Travel 查询场景的资源消耗
- 大表（TB 级）RESTORE 的资源需求

---

## 十一、兼容性

### 11.1 SQL 兼容性

- 与 SQL:2011 标准的 temporal query 对齐程度
- 与已有 Iceberg/Delta Lake Time Travel 语法的一致性
- 与 Snowflake Time Travel 语法的差异说明

### 11.2 版本兼容性

- 升级兼容：从不支持 Time Travel 的版本升级后的行为
- 降级兼容：降级到不支持 Time Travel 的版本后对历史版本数据的处理
- FE/BE 混合版本部署时的行为

### 11.3 与现有功能的兼容

- 物化视图（同步 / 异步）：Time Travel 查询是否穿透 MV、RESTORE 后 MV 刷新策略
- 数据导入（Stream Load / Broker Load / Routine Load）：导入事务与版本产生的关系
- Compaction：Compaction 不改变逻辑数据，但影响物理文件组织，需确保 Time Travel 查询结果不受影响
- Schema Change（Fast / 非 Fast）：Schema 变更后历史版本的查询行为
- Tablet Reshard：Reshard 后历史版本的查询路径

---

## 十二、Schema 演化与 Time Travel 交互

### 12.1 问题定义

当表的 Schema 在 Time Travel 保留期内发生变更，查询历史版本时需要处理 Schema 不匹配的问题。

### 12.2 需要定义的行为

| Schema 变更类型 | Time Travel 查询行为 |
|----------------|---------------------|
| ADD COLUMN（Fast Schema Change） | 历史版本中新增列返回 NULL / 默认值 |
| DROP COLUMN（Fast Schema Change） | 历史版本中已删除列是否可查询、如何查询 |
| MODIFY COLUMN（类型变更） | 历史版本中使用原始类型还是新类型 |
| RENAME COLUMN | 历史版本中使用原始列名还是新列名 |
| ADD/DROP/MODIFY COLUMN（非 Fast，重写数据） | 新旧 Tablet 共存时的版本解析 |
| ALTER TABLE ORDER BY（排序键变更） | 数据逻辑等价，透明处理 |

### 12.3 设计原则

- 明确"历史版本使用历史 Schema"还是"历史版本按当前 Schema 展示"的策略选择
- 参考 Iceberg Schema Evolution 的 column ID 机制
- 参考 Delta Lake 的 Schema 演化处理方式

---

## 十三、AI/ML 场景支持

### 13.1 Point-in-Time 特征拼接

- 使用 `FOR TIMESTAMP AS OF` 获取标签时刻的特征值
- 结合 ASOF JOIN 实现 point-in-time 正确的特征 JOIN（后续阶段）
- 作为轻量级 Feature Store 的定位

### 13.2 训练数据版本化

- 使用 `FOR VERSION AS OF` 精确复现训练数据集
- 与 MLflow 等实验追踪系统的集成模式（记录版本号/时间戳）

### 13.3 AI Agent 时间感知查询

- Text-to-SQL 场景下 Time Travel 语法的生成
- AI Agent 使用 Time Travel 进行时间推理查询

---

## 十四、测试策略

### 14.1 单元测试

- FE：SQL 解析、查询规划、版本解析、权限检查
- BE：历史版本数据读取、Segment 文件访问

### 14.2 集成测试

- SQL 集成测试（`test/` 框架）
  - 基本 Time Travel 查询（TIMESTAMP AS OF / VERSION AS OF）
  - RESTORE 操作与验证
  - UNDROP 操作与验证
  - 保留策略配置与继承
  - Schema 变更后的 Time Travel 查询
  - 跨时间点 JOIN
  - 错误场景覆盖
  - 权限测试

### 14.3 性能测试

- Time Travel 查询 vs 普通查询的延迟对比
- RESTORE 操作的延迟测试（不同表大小）
- 大量历史版本下的查询性能
- 存储开销测试

### 14.4 兼容性测试

- 升级/降级场景
- 与物化视图、数据导入、Compaction 等功能的交互测试

---

## 十五、用户文档计划

### 15.1 新增文档

| 文档 | 路径 | 说明 |
|------|------|------|
| Time Travel 概述 | `docs/en/using_starrocks/time_travel/overview.md` | 功能介绍、适用场景、快速入门 |
| 时间点查询 | `docs/en/using_starrocks/time_travel/point_in_time_query.md` | FOR TIMESTAMP/VERSION AS OF 语法与示例 |
| 数据恢复 | `docs/en/using_starrocks/time_travel/data_recovery.md` | RESTORE、UNDROP 语法与示例 |
| 生命周期管理 | `docs/en/using_starrocks/time_travel/lifecycle_management.md` | 保留策略配置、存储管理 |
| SQL 参考 | `docs/en/sql-reference/` | 各新增 SQL 语法的参考页面 |

### 15.2 更新文档

| 文档 | 说明 |
|------|------|
| FE 配置参考 | 新增 Time Travel 相关配置参数 |
| BE 配置参考 | 如有新增 BE 参数 |
| Metrics 参考 | 新增 Time Travel 相关监控指标 |
| 权限参考 | 新增 Time Travel 相关权限说明 |

### 15.3 中英文同步

- 所有新增文档需同时提供 `docs/en/` 和 `docs/zh/` 版本

---

## 十六、交付里程碑

### 第一期（P0-P1）

- [ ] `FOR TIMESTAMP AS OF` 查询
- [ ] `FOR VERSION AS OF` 查询
- [ ] 相对时间查询支持
- [ ] `SHOW HISTORY FOR TABLE`
- [ ] `ALTER TABLE ... RESTORE TO TIMESTAMP/VERSION`
- [ ] `UNDROP TABLE / DATABASE`
- [ ] `time_travel_retention` 保留策略配置（系统/库/表三级）
- [ ] 错误信息体系
- [ ] 权限模型实现
- [ ] 基础 Metrics
- [ ] 单元测试与集成测试
- [ ] 用户文档（英文 + 中文）

### 第二期（P2）

- [ ] 跨时间点 JOIN
- [ ] 分区级 RESTORE
- [ ] 更多 Schema 演化场景支持
- [ ] 性能优化

### 第三期（P3）

- [ ] `SHOW TIME TRAVEL STORAGE` 存储成本可视化
- [ ] `ALTER TABLE ... PURGE VERSIONS BEFORE` 手动清理
- [ ] Fail-safe 安全网机制
- [ ] `information_schema` 系统视图

---

## 十七、风险与待决事项

### 17.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 长保留期下 FE 元数据内存膨胀 | FE OOM / GC 压力 | 评估元数据内存占用模型，必要时引入元数据分级存储 |
| 大量历史版本文件导致对象存储 LIST 性能下降 | 查询延迟增加 | 优化文件组织结构，引入版本索引 |
| Schema 演化与 Time Travel 的复杂交互 | 查询结果不符合预期 | 明确定义每种 Schema 变更的行为，充分测试 |
| RESTORE 大表的性能 | 恢复时间过长 | 评估 RESTORE 是否仅需元数据操作即可完成 |

### 17.2 开放问题（待讨论决定）

| 编号 | 问题 | 候选方案 | 状态 |
|------|------|---------|------|
| Q1 | Time Travel 查询使用历史 Schema 还是当前 Schema？ | A) 历史 Schema（保真）B) 当前 Schema（易用）C) 可配置 | 待定 |
| Q2 | DROP COLUMN 后历史版本中该列是否仍可查询？ | A) 可查询（需保留列元数据）B) 不可查询（列被删除即不可见） | 待定 |
| Q3 | RESTORE 到 Schema 不兼容版本时的行为？ | A) 报错拒绝 B) 尽力恢复 + 警告 C) 允许指定策略 | 待定 |
| Q4 | 安全网（Fail-safe）的恢复入口如何设计？ | A) 复用 RESTORE 语法 + 管理员权限 B) 专用管理命令 C) 系统过程 | 待定 |
| Q5 | `time_travel_retention = 0` 时已有的历史版本如何处理？ | A) 立即全部清理 B) 渐进式清理 C) 保留到原保留期到期 | 待定 |
| Q6 | 存算分离模式下是否完全废弃 RECOVER 命令？ | A) 完全替代 B) 保留但标记 deprecated C) 共存 | 待定 |

---

## 附录

### A. 术语表

| 术语 | 定义 |
|------|------|
| Table State | 表的完整状态，包括表定义、数据分片、数据、同步物化视图等（详见概念文档） |
| Table Snapshot | 某一时刻的 Table State 实例 |
| Version | 每次 State 变更产生的单调递增编号 |
| Time Travel 保留期 | 历史版本数据保持可查询/可恢复的时间窗口 |
| Fail-safe | Time Travel 保留期到期后的安全缓冲期，仅管理员可恢复 |
| Vacuum | 清理过期数据文件的后台进程 |

### B. 竞品参考

| 竞品 | 参考特性 |
|------|---------|
| Snowflake | AT/BEFORE 语法、1-90 天保留、Fail-safe 7 天、UNDROP |
| Delta Lake | TIMESTAMP AS OF / VERSION AS OF、RESTORE、DESCRIBE HISTORY |
| Iceberg | Time Travel 查询语法（StarRocks 已支持外部 Iceberg 表） |

### C. 相关设计文档索引

| 文档 | 说明 |
|------|------|
| [产品设计](../../.cursor/plans/time_travel_product_design_b9ad5bc6.plan.md) | Time Travel 产品设计全文 |
| [Cluster Snapshot 差异分析](./time_travel_vs_cluster_snapshot_analysis.md) | Time Travel 与 Cluster Snapshot 的定位对比 |
| [Table State 与 Snapshot 概念](./cloud_native_table_concepts_state_and_snapshot.md) | 统一的 Table State / Snapshot 基础概念定义 |
