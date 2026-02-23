## Time Travel 产品调研

> 本文档从 [time_travel_proposal_shared_data.md](time_travel_proposal_shared_data.md) 拆分而来，包含完整的产品调研内容。

### 2.1 调研维度说明

| # | 维度 | 定义 |
|---|------|------|
| 1 | 典型用户场景 | 官方文档中明确列出的 Time Travel 典型使用场景 |
| 2 | 时间点查询语法 | SQL 语法及扩展（`AS OF`、`BEFORE`、`VERSION AS OF` 等） |
| 3 | 版本定位方式 | 支持的定位手段：timestamp、version/snapshot ID、偏移量、语句 ID 等 |
| 4 | 默认 / 最大保留时长 | 默认保留窗口、可配置的最大保留上限、付费版本差异 |
| 5 | 保留策略粒度 | 能否按 table / schema / database / account 级别分别设置 |
| 6 | 适用对象 | 表（managed/external）、视图、MV、数据库/Schema、Stream 等 |
| 7 | Schema 演进与 Time Travel 的交互 | 查询历史版本时使用当前 schema 还是历史 schema；schema change 后能否回溯 |
| 8 | 数据恢复能力 | UNDROP、CLONE/RESTORE 到历史版本、同表 ROLLBACK |
| 9 | 变更历史可观测性 | 查看表变更记录的方式（CHANGES clause、DESCRIBE HISTORY、audit log 等） |
| 10 | 增量 / CDC 读取能力 | 能否读取两个版本之间的 delta（inserts/updates/deletes） |
| 11 | 存储与成本模型 | 历史版本的存储方式（copy-on-write / MVCC）、额外存储计费 |
| 12 | 与流式 / MV / 下游集成 | Time Travel 与 Streams、MV、CDC connector 等的联动 |
| 13 | 限制与注意事项 | 已知限制（DDL 类型、临时表、瞬态表、外部表等） |
| 14 | 支持的 DML 与 DDL | 哪些 DML / DDL 操作会生成可 Time Travel 的版本；DDL 对历史版本的影响 |

### 2.2 各产品调研详情

#### 2.2.1 Snowflake

> 信息来源：`docs.snowflake.com` — Understanding & using Time Travel, AT | BEFORE, CHANGES, CREATE … CLONE, UNDROP TABLE, Introduction to Streams

**1. 典型用户场景**

Snowflake 官方文档将 Time Travel 定位为以下三大核心场景：

- **误操作恢复**：恢复被意外或有意删除的对象（表、Schema、Database），支持 UNDROP 及零拷贝 CLONE 到历史时间点。
- **数据备份与复制**：从过去的关键时间点复制数据，用于创建开发/测试环境副本。
- **数据使用与变更分析**：分析指定时间段内数据的使用和操纵情况，支持审计与合规。

此外，通过 Streams + CHANGES clause 机制，Snowflake 将 Time Travel 与增量数据处理（CDC / ELT pipeline）紧密结合。

**2. 时间点查询语法**

Snowflake 通过 `AT | BEFORE` 子句实现时间点查询，语法紧跟 `FROM` 子句中的表名之后：

```sql
-- AT: 包含指定时间点的变更
SELECT * FROM my_table AT(TIMESTAMP => '2024-06-05 12:30:00'::TIMESTAMP_LTZ);

-- AT + OFFSET: 相对当前时间的秒数偏移
SELECT * FROM my_table AT(OFFSET => -60*5);

-- AT + STATEMENT: 按查询 ID 定位
SELECT * FROM my_table AT(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-- BEFORE: 指定语句完成之前的时间点
SELECT * FROM my_table BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-- AT + STREAM: 以 Stream 当前偏移量为起点（仅用于 CHANGES 及 CREATE STREAM）
SELECT * FROM my_table CHANGES(INFORMATION => DEFAULT) AT(STREAM => 'my_stream');
```

**3. 版本定位方式**

| 定位方式 | 说明 |
|----------|------|
| `TIMESTAMP => <ts>` | 精确时间戳（需显式 CAST 为 TIMESTAMP / TIMESTAMP_LTZ / TIMESTAMP_NTZ / TIMESTAMP_TZ），最小精度为毫秒 |
| `OFFSET => -N` | 相对当前时间的秒数偏移，N 可以是整数或算术表达式（如 `-30*60`） |
| `STATEMENT => <query_id>` | 指定 DML/TCL/SELECT 语句的 query ID；query ID 必须在 14 天内执行过 |
| `STREAM => '<name>'` | 使用已有 Stream 的当前偏移量作为定位点（仅 CHANGES 子句和 CREATE STREAM） |

`AT` 关键字的语义为**包含**指定参数对应的变更；`BEFORE` 关键字的语义为**不包含**指定语句完成后的变更（即"该语句完成前一刻"）。

**4. 默认 / 最大保留时长**

| 版本 | 默认保留 | 最大保留 | 说明 |
|------|---------|---------|------|
| Standard Edition | 1 天 | 1 天 | 可设为 0（关闭 Time Travel） |
| Enterprise Edition+ | 1 天 | **90 天**（permanent 对象） | transient / temporary 表最大仍为 1 天 |

- 通过 `DATA_RETENTION_TIME_IN_DAYS` 参数控制，可在 account / database / schema / table 级别设置。
- `MIN_DATA_RETENTION_TIME_IN_DAYS`（account 级）可强制最小保留，实际保留 = `MAX(DATA_RETENTION_TIME_IN_DAYS, MIN_DATA_RETENTION_TIME_IN_DAYS)`。
- 保留期结束后数据进入 **Fail-safe**（额外 7 天，仅 Snowflake 内部可恢复，用户不可访问）。

**5. 保留策略粒度**

Snowflake 支持**四级层次化继承**的保留策略：

| 级别 | 参数 | 说明 |
|------|------|------|
| Account | `DATA_RETENTION_TIME_IN_DAYS` | 全局默认值，需 ACCOUNTADMIN 角色 |
| Database | `DATA_RETENTION_TIME_IN_DAYS` | 建库时或 ALTER 设置，未设置则继承 Account |
| Schema | `DATA_RETENTION_TIME_IN_DAYS` | 建 Schema 时或 ALTER 设置，未设置则继承 Database |
| Table | `DATA_RETENTION_TIME_IN_DAYS` | 建表时或 ALTER 设置，未设置则继承 Schema |

此外，Account 级 `MIN_DATA_RETENTION_TIME_IN_DAYS` 可设置下限，保证所有对象至少保留指定天数。

**6. 适用对象**

| 对象类型 | Time Travel 查询 | CLONE | UNDROP | 说明 |
|----------|:---:|:---:|:---:|------|
| Permanent Table | ✅ | ✅ | ✅ | 核心支持对象 |
| Transient Table | ✅ | ✅ | ✅ | 最大保留 1 天，无 Fail-safe |
| Temporary Table | ✅ | ✅ | ✅ | 最大保留 1 天，会话结束即消失 |
| Non-materialized View | ✅ | — | — | 使用当前 schema |
| Dynamic Table | ✅ | ✅ | ✅ | 支持 Stream |
| Iceberg Table (Snowflake-managed) | ✅ | ✅ | ✅ | 支持 CLONE、Stream |
| Hybrid Table | ⚠️ | ⚠️ | ❌ | 仅支持 AT+TIMESTAMP，不支持 BEFORE/OFFSET/STATEMENT |
| Schema | — | ✅ | ✅ | 递归 CLONE 所有子对象 |
| Database | — | ✅ | ✅ | 递归 CLONE 所有子对象 |
| External Table | ❌ | ❌ | ❌ | 不支持 Time Travel |
| Materialized View | ❌ | — | — | Stream 不可追踪 MV 变更 |

**7. Schema 演进与 Time Travel 的交互**

- **使用当前（最新）schema**：查询历史数据时，返回结果使用表的**当前 schema 定义**（包括列名、默认值等），而非历史时间点的 schema。
- 例如：若在查询的历史时间点之后新增了一列，查询历史数据时该列也会出现在结果中（值为 NULL 或默认值）。
- 同样适用于 non-materialized view 的 Time Travel 查询。
- **Clone 行为**：CLONE 时，数据使用指定历史时间点的快照，但元数据（注释、clustering key 等）使用**执行 CLONE 语句时**的当前值。

**8. 数据恢复能力**

| 恢复方式 | 语法 | 说明 |
|----------|------|------|
| **UNDROP** | `UNDROP TABLE <name>;` | 恢复最近一次 DROP 的版本；若同名表已存在则报错，需先 RENAME |
| **零拷贝 CLONE 到历史版本** | `CREATE TABLE t_clone CLONE t AT(TIMESTAMP => <ts>);` | 创建历史版本的零拷贝克隆，支持 table/schema/database 级别 |
| **复制恢复（CTAS）** | `CREATE TABLE t_copy AS SELECT * FROM t AT(TIMESTAMP => <ts>);` | 传统数据复制方式 |
| **同表回滚** | 不直接支持 | 需通过 CLONE + SWAP 或 RENAME 模拟 |

UNDROP 依赖 Time Travel 保留期：对象必须在 `DATA_RETENTION_TIME_IN_DAYS` 窗口内才可恢复。支持按 table ID 恢复特定版本（当存在多个同名已删除表时）。

**9. 变更历史可观测性**

| 机制 | 说明 |
|------|------|
| `SHOW <objects> HISTORY` | 显示已删除对象列表，包含 `dropped_on`、`retention_time` 等列 |
| `CHANGES` clause | 查询两个时间点之间的变更元数据（不消费偏移量），支持 `DEFAULT`（全量 delta）和 `APPEND_ONLY` 模式 |
| Streams 查询 | 查询 Stream 返回 `METADATA$ACTION`（INSERT/DELETE）、`METADATA$ISUPDATE`（是否为 UPDATE 的一部分）、`METADATA$ROW_ID`（行唯一标识） |
| `SHOW TABLES` | `retention_time` 列显示当前保留天数 |
| `SHOW STREAMS` | `stale_after` 列显示 Stream 预计过期时间 |

**10. 增量 / CDC 读取能力**

Snowflake 提供两种互补的增量读取机制：

**(a) CHANGES clause（无状态、只读）**

```sql
SELECT * FROM t1
  CHANGES(INFORMATION => DEFAULT)
  AT(TIMESTAMP => $ts1)
  END(TIMESTAMP => $ts2);
```

- `DEFAULT` 模式：返回完整 delta（inserts + updates + deletes），通过对比 inserted/deleted 行计算净变更。
- `APPEND_ONLY` 模式：仅返回新增行，性能更优。
- 不消费偏移量，多次查询可返回相同结果。
- 需先启用 change tracking（`ALTER TABLE ... SET CHANGE_TRACKING = TRUE`）或创建 Stream。

**(b) Streams（有状态、事务性推进）**

```sql
CREATE STREAM s1 ON TABLE t1;
-- 查询变更
SELECT * FROM s1;
-- 在 DML 中消费后偏移量自动推进
INSERT INTO target SELECT * FROM s1;
```

- **Standard stream**：跟踪 INSERT/UPDATE/DELETE + TRUNCATE，返回行级 delta。
- **Append-only stream**：仅跟踪 INSERT，性能更优。
- **Insert-only stream**：用于 external table / externally-managed Iceberg table。
- Stream 偏移量仅在 DML 事务中消费后推进，单纯 SELECT 不推进。
- 支持 repeatable read 隔离级别。

**11. 存储与成本模型**

| 方面 | 说明 |
|------|------|
| **存储方式** | MVCC：数据修改时，Snowflake 保留修改前的数据版本 |
| **Time Travel 存储** | 历史版本在保留期内占用额外存储，按标准存储费率计费 |
| **Fail-safe 存储** | 保留期结束后进入 Fail-safe（7 天），仍占用存储 |
| **CLONE 成本** | 零拷贝：CLONE 不产生额外存储，直到对克隆进行修改 |
| **Stream 存储** | Stream 本身仅存储偏移量，几乎不占空间；但可能延长源表保留期（最多额外 14 天，由 `MAX_DATA_EXTENSION_TIME_IN_DAYS` 控制） |
| **变更追踪列** | 启用 change tracking 后，表增加隐藏列存储元数据，占用少量额外存储 |

**12. 与流式 / MV / 下游集成**

| 集成能力 | 说明 |
|----------|------|
| **Streams** | 一等公民 CDC 对象，自动追踪 DML 变更，偏移量事务性推进 |
| **Tasks** | 可由 Stream 数据可用性触发（`SYSTEM$STREAM_HAS_DATA`），形成 Stream → Task 的 ELT pipeline |
| **Dynamic Tables** | 支持创建 Stream，与 Time Travel 配合 |
| **CHANGES clause** | 轻量级无状态 CDC，适用于临时性增量读取 |
| **Materialized Views** | 不可通过 Stream 追踪 MV 变更；MV 本身的保留期由父对象决定 |
| **Data Sharing** | Stream 可以创建在 shared table 上，但不延长源表保留期 |

Snowflake 的 Stream + Task 组合是其增量数据处理的核心模式：Stream 提供 CDC 能力，Task 提供调度能力，二者结合构成完整的增量 ELT pipeline。

**13. 限制与注意事项**

| 限制 | 说明 |
|------|------|
| **External Table** | 不支持 Time Travel 查询、CLONE、UNDROP |
| **Hybrid Table** | AT 仅支持 TIMESTAMP 参数；不支持 BEFORE；不支持 UNDROP |
| **Transient / Temporary Table** | 最大保留 1 天；transient 无 Fail-safe |
| **CTE** | AT/BEFORE 不能直接用于 CTE 引用，但可以在 WITH 子句内部的查询中使用 |
| **UNDROP 同名冲突** | 若同名对象已存在，UNDROP 失败，需先 RENAME |
| **容器删除与子对象保留** | DROP DATABASE/SCHEMA 时，子对象的保留期被容器覆盖（不单独生效） |
| **重建对象** | CREATE OR REPLACE 会丢弃历史，导致关联 Stream 失效（stale） |
| **Schema Change** | 查询使用当前 schema，历史数据中不兼容的 schema 变更可能导致语义偏差 |
| **STATEMENT 参数** | query ID 必须在 14 天内，超期需改用 TIMESTAMP |
| **TIMESTAMP 精度** | 最小精度为毫秒 |
| **Fail-safe** | 保留期外用户不可自行恢复，仅 Snowflake 内部操作 |

#### 2.2.2 Databricks (Delta Lake)

> 信息来源：`docs.databricks.com` — Work with table history, RESTORE, Clone a table, Use Delta Lake change data feed, Update table schema, Delta table properties reference

**1. 典型用户场景**

Databricks 官方文档列出以下 Time Travel 核心使用场景：

- **快速变化表的快照隔离（Snapshot Isolation）**：为一组查询提供一致的历史快照，避免并发写入干扰分析结果。
- **数据修复（Fixing Mistakes）**：误删、误更新后，通过 `RESTORE` 或 `SELECT ... TIMESTAMP AS OF` 恢复到正确状态。
- **复杂时序查询（Temporal Queries）**：例如计算过去 7 天的新增用户数——将当前表与 7 天前的快照做差值：

```sql
SELECT
  (SELECT count(distinct userId) FROM my_table)
  -
  (SELECT count(distinct userId) FROM my_table TIMESTAMP AS OF date_sub(current_date(), 7))
AS new_customers;
```

- **分析 / 报表 / ML 模型复现**：审计与合规场景下需要重新跑出某个历史时间点的分析结果；ML 场景下需要归档精确训练数据集以复现模型。
- **数据归档**：使用 `CLONE` 定期保存表在某个时间点的完整状态，用于合规或灾备。
- **生产表的短期实验**：`SHALLOW CLONE` 一张生产表做实验，不影响生产工作负载。

**2. 时间点查询语法**

Delta Lake 提供两种 SQL 语法风格：

*标准 AS OF 语法：*

```sql
-- 按时间戳查询
SELECT * FROM people10m TIMESTAMP AS OF '2018-10-18T22:15:12.013Z';

-- 按版本号查询
SELECT * FROM people10m VERSION AS OF 123;
```

*@ 内联语法（紧凑格式）：*

```sql
-- 时间戳格式 yyyyMMddHHmmssSSS
SELECT * FROM people10m@20190101000000000;

-- 版本号前缀 v
SELECT * FROM people10m@v123;
```

`timestamp_expression` 可以是任何能转换为 timestamp 的表达式，包括：
- 日期字符串：`'2018-10-18'`
- 带时区的 timestamp 字符串：`cast('2018-10-18 13:36:32 CEST' as timestamp)`
- ISO 8601 字符串：`'2018-10-18T22:15:12.013Z'`
- 日期函数：`date_sub(current_date(), 1)`
- 时间间隔：`current_timestamp() - interval 12 hours`

**限制**：`timestamp_expression` 和 `version` 均不支持子查询。

**3. 版本定位方式**

| 定位方式 | 语法 | 说明 |
|---------|------|------|
| Timestamp | `TIMESTAMP AS OF <expr>` | 任何可转换为 timestamp 的表达式 |
| Version 号 | `VERSION AS OF <version>` | long 类型，从 `DESCRIBE HISTORY` 获取 |
| @ 内联 Timestamp | `table@yyyyMMddHHmmssSSS` | 精确到毫秒的紧凑格式 |
| @ 内联 Version | `table@v<N>` | 字母 v 前缀 + 版本号 |

不支持 OFFSET（秒数偏移）、STATEMENT（语句 ID）、STREAM 等 Snowflake 特有的定位方式。

**4. 默认 / 最大保留时长**

Delta Lake 的 Time Travel 保留由两个独立参数控制：

| 参数 | 默认值 | 作用 |
|------|--------|------|
| `delta.deletedFileRetentionDuration` | **7 天** | 控制被删除的数据文件保留时长；`VACUUM` 会清理超出此窗口的文件 |
| `delta.logRetentionDuration` | **30 天** | 控制事务日志（操作历史）的保留时长 |

**实际可用的 Time Travel 窗口取决于 VACUUM**：即使日志保留 30 天，如果 `VACUUM` 按默认 7 天清理了数据文件，则超过 7 天的版本将无法查询。因此**有效默认窗口为 7 天**。

要延长 Time Travel 窗口，必须**同时调大**两个参数：

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.logRetentionDuration' = '30 days'
);
```

无硬性最大保留上限，但存储成本随保留时长线性增长。Databricks **不建议**将 Time Travel 作为长期数据归档方案。

在 Databricks Runtime 18.0 及以上版本中，请求超出 `deletedFileRetentionDuration` 的版本时，查询会被直接阻断（而非返回不完整数据）。

**5. 保留策略粒度**

| 级别 | 方式 | 说明 |
|------|------|------|
| Table | `ALTER TABLE ... SET TBLPROPERTIES` | 每张表独立设置 |
| Session 默认值 | `spark.databricks.delta.properties.defaults.*` | 为新建表设置默认值，不影响已有表 |
| Schema / Database / Account | — | **不支持** |

与 Snowflake 四级继承模型不同，Delta Lake 仅支持表级和 Session 默认值两个层次。

**6. 适用对象**

| 对象类型 | 支持 Time Travel | 备注 |
|---------|:---:|------|
| Delta 托管表（Managed Table） | ✅ | 完整支持 |
| Delta 外部表（External Table） | ✅ | 需要事务日志和数据文件均可访问 |
| Streaming Table | ✅ | 支持 Time Travel 查询 |
| Materialized View | ❌ | 不支持 Time Travel 查询 |
| Clone 表 | ✅ | 历史独立于源表，版本号与时间戳不可互通 |
| 非 Delta 格式表（Parquet / CSV 等） | ❌ | 无事务日志，不支持 |

**7. Schema 演进与 Time Travel 的交互**

- **默认语义**：Time Travel 查询使用**当前（最新）schema** 解析 SQL。
- **加列（ADD COLUMN）**：安全兼容——历史版本中不存在的列返回 NULL。
- **Column Mapping 模式下的非加法变更**（rename / drop / type change）：
  - Databricks Runtime 12.2 LTS 及以上：批量读取使用**查询目标版本的 schema**；但如果查询的版本范围跨越了非加法 schema 变更，查询失败。
  - 更低版本：不支持读取发生过列重命名/删除的表的历史数据。
- **Schema 变更终止流式读取**：任何 schema 更新都会终止正在读取该表的 Structured Streaming 作业，需要手动重启。
- **Change Data Feed 限制**：启用 Column Mapping 的表发生非加法 schema 变更后，CDF 的流式读取在较低版本中不可用；批量 CDF 读取不能跨越 schema 变更的版本范围。

**8. 数据恢复能力**

| 恢复方式 | 语法 | 说明 |
|----------|------|------|
| **RESTORE（同表回滚）** | `RESTORE TABLE t TO TIMESTAMP AS OF <ts>;` | 生成新版本将表状态回滚到指定时间点（可逆操作） |
| **RESTORE（按版本）** | `RESTORE TABLE t TO VERSION AS OF <v>;` | 按版本号回滚 |
| **深拷贝 CLONE** | `CREATE TABLE t2 CLONE t VERSION AS OF 15;` | 复制数据 + 元数据，独立于源表 |
| **浅拷贝 CLONE** | `CREATE TABLE t2 SHALLOW CLONE t TIMESTAMP AS OF '2019-01-01';` | 仅复制元数据，数据文件引用源表 |
| **查询恢复（INSERT）** | `INSERT INTO t SELECT * FROM t TIMESTAMP AS OF <ts> WHERE ...;` | 选择性恢复特定行 |
| **查询恢复（MERGE）** | `MERGE INTO t USING t TIMESTAMP AS OF <ts> source ON ... WHEN MATCHED THEN UPDATE SET *;` | 选择性回滚匹配行 |

RESTORE 返回执行指标：`table_size_after_restore`、`num_removed_files`、`num_restored_files`、`removed_files_size`、`restored_files_size`。

CLONE 注意事项：
- 深拷贝独立于源表，但创建开销大。
- 浅拷贝创建快但依赖源表数据文件；源表 `VACUUM` 后浅拷贝可能失效（`FileNotFoundException`）。
- Clone 表的历史与源表完全独立。

**无 UNDROP TABLE**：Delta Lake / Databricks 没有内置的 `UNDROP TABLE` 机制。表删除后需要依赖外部备份或提前 CLONE 归档。

**9. 变更历史可观测性**

```sql
DESCRIBE HISTORY table_name;          -- 完整历史（按时间倒序）
DESCRIBE HISTORY table_name LIMIT 1;  -- 仅最近一次操作
```

返回字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | long | 表版本号 |
| `timestamp` | timestamp | 提交时间 |
| `userId` / `userName` | string | 操作者 |
| `operation` | string | 操作类型（WRITE / DELETE / UPDATE / MERGE / RESTORE / CLONE / TRUNCATE / OPTIMIZE / VACUUM 等） |
| `operationParameters` | map | 操作参数（如 predicates、mode 等） |
| `operationMetrics` | map | 操作指标（如 `numFiles`、`numOutputRows`、`numDeletedRows` 等，按操作类型不同而不同） |
| `readVersion` | long | 操作读取的基准版本 |
| `isolationLevel` | string | 隔离级别（WriteSerializable 等） |
| `isBlindAppend` | boolean | 是否为追加写入 |
| `userMetadata` | string | 用户自定义提交元数据 |

此外，Databricks Catalog Explorer 提供可视化的 History 选项卡。

**10. 增量 / CDC 读取能力**

Delta Lake 通过 **Change Data Feed (CDF)** 提供增量读取能力：

*启用方式（需显式开启，默认关闭）：*

```sql
-- 建表时启用
CREATE TABLE student (id INT, name STRING, age INT)
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 已有表启用
ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**重要**：仅记录启用后的变更，**不能追溯**历史变更。

*增量读取元数据列：*

| 列名 | 类型 | 值 |
|------|------|-----|
| `_change_type` | String | `insert` / `update_preimage` / `update_postimage` / `delete` |
| `_commit_version` | Long | 包含该变更的版本号 |
| `_commit_timestamp` | Timestamp | 提交时间 |

*批量读取（Batch）：*

```sql
-- 按版本范围（起止均包含）
SELECT * FROM table_changes('tableName', 0, 10);

-- 按时间范围
SELECT * FROM table_changes('tableName', '2021-04-21 05:45:46', '2021-05-21 12:00:00');

-- 从某个版本到最新
SELECT * FROM table_changes('tableName', 0);
```

*流式读取（Structured Streaming）：*

```python
spark.readStream
  .option("readChangeFeed", "true")
  .option("startingVersion", 76)
  .table("source_table")
```

默认行为：首次启动时返回最新快照作为 INSERT 事件，之后仅返回增量变更。支持 `startingVersion` / `startingTimestamp` 指定起点。

CDF 记录的变化数据受 `VACUUM` 保留策略约束，超出保留窗口的变化数据被自动清理。

**11. 存储与成本模型**

| 方面 | 说明 |
|------|------|
| **事务日志** | `_delta_log/` 目录下有序、追加写入的 JSON 文件，每个文件记录一个版本的 add/remove 文件操作；每 10 个事务生成 Parquet 格式 checkpoint 加速读取 |
| **数据存储模型** | Copy-on-Write：UPDATE / DELETE / MERGE 不做原地修改，而是重写受影响的 Parquet 文件 → 在日志中记录 add（新文件）和 remove（旧文件） |
| **MVCC** | 读者看到查询启动时的一致快照；每个版本是一组文件引用的集合 |
| **Time Travel 成本** | 历史版本利用旧文件自然保留，`VACUUM` 清理前几乎零额外存储成本 |
| **VACUUM 清理** | `VACUUM table RETAIN n HOURS` 物理删除超出保留窗口的旧数据文件；一旦执行，对应版本的 Time Travel 永久失效 |
| **CDF 额外成本** | 启用 Change Data Feed 后，部分操作生成独立的 change data 文件，带来少量额外存储开销 |
| **CLONE 成本** | 深拷贝：需复制全部数据文件；浅拷贝：仅复制元数据，零额外存储（但依赖源表文件） |

**12. 与流式 / MV / 下游集成**

| 集成能力 | 说明 |
|----------|------|
| **Structured Streaming + CDF** | 核心增量处理模式：下游 Streaming 作业通过 `readChangeFeed` 读取 Delta 表的增量变更，支持 exactly-once 语义 |
| **Streaming Table** | Databricks 的 Streaming Table 支持 Time Travel 查询 |
| **Materialized View** | **不支持** Time Travel 查询；MV 的增量刷新基于 Row Tracking 机制而非 Time Travel |
| **CLONE 保留流元数据** | 深拷贝同步源表的 stream 和 COPY INTO 元数据，流式写入可在 clone 表上从中断处继续；浅拷贝不保留此元数据 |
| **Schema 变更与流的交互** | 任何 schema 更新终止正在读取的 Structured Streaming 作业，需手动重启 |

**13. 限制与注意事项**

| 限制项 | 详情 |
|-------|------|
| **有效 Time Travel 窗口受限于 VACUUM** | 默认 7 天；`VACUUM` 后对应版本永久不可恢复 |
| **DBR 18.0+ 阻断过期查询** | 请求版本超出 `deletedFileRetentionDuration` 时直接报错，不返回不完整数据 |
| **不建议用于长期归档** | 官方明确建议仅用过去 7 天的数据做 Time Travel |
| **MV 不支持 Time Travel** | Materialized View 无法按版本/时间戳查询 |
| **Column Mapping + 非加法 Schema 变更** | 发生 rename/drop 列后，CDF 和 Time Travel 在部分版本组合/流式场景下失效 |
| **CDF 需显式启用且不可追溯** | 仅记录启用后的变更，无法读取启用前的历史变更 |
| **Clone 表历史独立** | Clone 表的版本号 / 时间戳与源表不互通 |
| **浅拷贝依赖源表** | 源表 `VACUUM` 后浅拷贝可能失效（`FileNotFoundException`） |
| **Timestamp / Version 不支持子查询** | `AS OF` 后不能使用子查询 |
| **Schema 更新终止流** | 任何 schema 变更导致正在读取的 Structured Streaming 作业失败，需手动重启 |
| **无 UNDROP TABLE** | 表删除后无内置恢复机制，需依赖外部备份或提前 CLONE |

#### 2.2.3 Spark + Apache Iceberg

> 信息来源：`iceberg.apache.org/docs/latest` — Spark Queries (Time Travel), Spark Procedures (Snapshot Management / CDC), Configuration, Evolution, Maintenance, Branching and Tagging, Spark DDL, Iceberg Table Spec (Snapshots / Snapshot References / Retention Policy)

Apache Iceberg 是一种开源表格式（table format），通过 MVCC（Multi-Version Concurrency Control）和 Snapshot 机制提供 Time Travel 能力。Iceberg 本身定义了表格式规范（spec），由 Spark、Flink、Trino 等引擎提供查询和写入能力。以下以 Spark + Iceberg 为主要参考。

**1. 典型用户场景**

Iceberg 官方文档中 Time Travel 的典型使用场景：

- **审计与合规**：通过 Tag 保留重要历史快照（如每周/每月/每年快照），满足审计留痕需求。例如创建 `EOW-01`（周末快照，保留 7 天）、`EOM-01`（月末快照，保留 180 天）、`EOY-2023`（年末快照，永久保留）。
- **数据工程验证（Write-Audit-Publish）**：在独立 Branch 上执行写入并验证数据质量，验证通过后通过 `fast_forward` 将 Branch 合并到 main，实现"先验后发"工作流。
- **误操作恢复（Rollback）**：通过 `rollback_to_snapshot` 或 `rollback_to_timestamp` 将表回滚到历史状态。
- **增量处理与 CDC**：通过 `create_changelog_view` 读取两个快照之间的变更（inserts/deletes/updates），用于下游 ETL 增量处理。
- **可复现分析**：通过 snapshot ID 或 timestamp 精确读取历史版本数据。

**2. 时间点查询语法**

Iceberg 提供丰富的 SQL 与 DataFrame API 进行时间点查询（Spark 3.3+）：

```sql
-- 按 timestamp 查询
SELECT * FROM prod.db.table TIMESTAMP AS OF '1986-10-26 01:21:00';
SELECT * FROM prod.db.table FOR SYSTEM_TIME AS OF '1986-10-26 01:21:00';

-- 按 snapshot ID 查询
SELECT * FROM prod.db.table VERSION AS OF 10963874102873;
SELECT * FROM prod.db.table FOR SYSTEM_VERSION AS OF 10963874102873;

-- 按 branch / tag 名称查询
SELECT * FROM prod.db.table VERSION AS OF 'audit-branch';
SELECT * FROM prod.db.table VERSION AS OF 'historical-snapshot';

-- branch / tag 表示法（namespace 语法）
SELECT * FROM prod.db.table.`branch_audit-branch`;
SELECT * FROM prod.db.table.`tag_historical-snapshot`;

-- Unix timestamp（秒）
SELECT * FROM prod.db.table TIMESTAMP AS OF 499162860;
```

DataFrame API：

```scala
// 按 timestamp（毫秒）
spark.read.option("as-of-timestamp", "499162860000").format("iceberg").load("path/to/table")
// 按 snapshot ID
spark.read.option("snapshot-id", 10963874102873L).format("iceberg").load("path/to/table")
// 按 tag
spark.read.option("tag", "historical-snapshot").format("iceberg").load("path/to/table")
// 按 branch
spark.read.option("branch", "audit-branch").format("iceberg").load("path/to/table")
```

**3. 版本定位方式**

| 定位方式 | 说明 |
|---|---|
| `TIMESTAMP AS OF <ts>` | 解析到指定时间点或之前最近的 snapshot |
| `VERSION AS OF <snapshot_id>` | 64-bit long 型唯一标识，精确定位到某一快照 |
| `VERSION AS OF '<branch_name>'` | 定位到该 Branch 的 HEAD snapshot |
| `VERSION AS OF '<tag_name>'` | 定位到 Tag 引用的特定 snapshot |
| Unix timestamp（秒） | 与 `TIMESTAMP AS OF` 等价，以秒为单位的整数 |
| `branch_<name>` / `tag_<name>` | Namespace 语法直接引用 branch/tag |

注意：当 branch/tag 名称与 snapshot ID 的字符串表示相同时，snapshot ID 优先匹配。

**4. 默认 / 最大保留时长**

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `history.expire.max-snapshot-age-ms` | 432000000（5 天） | 表及所有 Branch 上快照的默认最大保留时长 |
| `history.expire.min-snapshots-to-keep` | 1 | 表及所有 Branch 上至少保留的快照数量 |
| `history.expire.max-ref-age-ms` | `Long.MAX_VALUE`（永久） | Branch/Tag 引用本身的默认最大保留时长 |
| main Branch | — | 永不过期 |

- **无硬性最大上限**：保留时长由用户自行配置，无付费版本差异（开源项目）。
- **需显式触发过期**：Iceberg 不自动删除历史 snapshot，需调用 `expire_snapshots` 存储过程；否则 snapshot 及关联数据文件会无限积累。
- **Branch/Tag 独立保留策略**：可为每个 Branch/Tag 单独设置保留参数，在 `expire_snapshots` 时分别评估。

**5. 保留策略粒度**

| 粒度 | 支持情况 | 说明 |
|---|---|---|
| Table 级 | ✅ | 通过 table properties 配置 |
| Branch 级 | ✅ | 每个 Branch 可设置 `min-snapshots-to-keep`、`max-snapshot-age-ms`、`max-ref-age-ms` |
| Tag 级 | ✅ | 每个 Tag 可设置 `max-ref-age-ms`（`RETAIN n DAYS`） |
| Database / Catalog 级 | ❌ | 不支持 |

DDL 示例：

```sql
-- 创建 Branch 并设置保留策略
ALTER TABLE prod.db.sample CREATE BRANCH `audit-branch`
  AS OF VERSION 1234 RETAIN 30 DAYS
  WITH SNAPSHOT RETENTION 3 SNAPSHOTS 2 DAYS;

-- 创建 Tag 并设置保留 365 天
ALTER TABLE prod.db.sample CREATE TAG `historical-tag`
  AS OF VERSION 1234 RETAIN 365 DAYS;

-- 修改 Branch 引用和保留策略
ALTER TABLE prod.db.sample REPLACE BRANCH `audit-branch`
  AS OF VERSION 4567 RETAIN 60 DAYS;
```

**6. 适用对象**

| 对象类型 | Time Travel | 说明 |
|---|:---:|---|
| Iceberg Managed Table | ✅ | 完整支持（SQL + DataFrame API） |
| Metadata Table（history/snapshots/manifests 等） | ✅ | 可对 metadata table 做 time travel 查询 |
| Iceberg View（view spec） | ❌ | View 本身无 snapshot 概念，不支持 time travel |
| 非 Iceberg 外部表 | ❌ | 需先通过 `migrate` / `snapshot` 过程转换为 Iceberg 格式 |

**7. Schema 演进与 Time Travel 的交互**

Iceberg 的 schema 演进是**仅元数据操作**（不重写数据文件），通过唯一的列 ID 追踪每个列，保证演进的独立性和正确性：

| 查询方式 | 使用的 Schema |
|---|---|
| 按 snapshot ID 查询 | **历史快照的 schema**（snapshot's schema） |
| 按 timestamp 查询 | **历史快照的 schema**（snapshot's schema） |
| 按 tag 查询 | **历史快照的 schema**（snapshot's schema） |
| 按 branch 名称查询 | **表的当前 schema**（table's schema） |

```sql
-- 假设表从 (id, data, col) 演进为 (id, data, new_col)

-- 按 branch 查询 → 当前 schema (id, data, new_col)
SELECT * FROM db.table VERSION AS OF 'test_branch';
-- 结果: id=1, data='a', new_col=NULL

-- 按 snapshot ID 查询 → 历史 schema (id, data, col)
SELECT * FROM db.table VERSION AS OF 8109744798576441359;
-- 结果: id=1, data='a', col=1.0
```

Iceberg schema 演进保证：
1. 列重排不影响列值关联
2. 列更新不影响其他列的值
3. 删除列不影响其他列的值
4. 新增列不会读取已有列的数据（通过唯一 ID 保证）

**8. 数据恢复能力**

| 恢复方式 | 说明 |
|---|---|
| **`rollback_to_snapshot`** | 回滚到指定 snapshot ID，要求目标 snapshot 是当前 snapshot 的祖先 |
| **`rollback_to_timestamp`** | 回滚到指定时间点对应的 snapshot |
| **`set_current_snapshot`** | 设置为指定 snapshot ID 或 Branch/Tag 引用（不要求祖先关系） |
| **`cherrypick_snapshot`** | 从非祖先 snapshot 中拣选变更（仅支持 append 和 dynamic overwrite） |
| **`snapshot` 过程** | 创建一个新 Iceberg 表引用源表的数据文件（轻量复制，用于测试验证） |
| **CTAS 复制恢复** | `CREATE TABLE t_new AS SELECT * FROM t VERSION AS OF ...` |
| **UNDROP TABLE** | ❌ 不支持（`DROP TABLE` 后无回收站机制） |

```sql
-- 回滚到指定 snapshot
CALL catalog_name.system.rollback_to_snapshot('db.sample', 1);

-- 回滚到指定时间点
CALL catalog_name.system.rollback_to_timestamp('db.sample', TIMESTAMP '2021-06-30 00:00:00.000');

-- 设置当前 snapshot（不限于祖先）
CALL catalog_name.system.set_current_snapshot('db.sample', 1);

-- Cherry-pick 变更
CALL catalog_name.system.cherrypick_snapshot('my_table', 1);

-- Branch fast-forward
CALL catalog_name.system.fast_forward('my_table', 'main', 'audit-branch');
```

**9. 变更历史可观测性**

Iceberg 通过丰富的 **metadata table** 提供变更历史查询：

| 元数据表 | 说明 |
|---|---|
| `table.history` | Snapshot 切换记录：`made_current_at`、`snapshot_id`、`parent_id`、`is_current_ancestor` |
| `table.snapshots` | 所有有效 snapshot 详情：`committed_at`、`snapshot_id`、`parent_id`、`operation`（append/delete/overwrite/replace）、`summary`（含文件级统计） |
| `table.metadata_log_entries` | 元数据文件变更记录：`timestamp`、`file`、`latest_snapshot_id`、`latest_schema_id`、`latest_sequence_number` |
| `table.refs` | Branch / Tag 引用：`name`、`type`、`snapshot_id`、`max_reference_age_in_ms`、`min_snapshots_to_keep`、`max_snapshot_age_in_ms` |
| `ancestors_of` 过程 | 报告指定 snapshot 的祖先 snapshot 链 |

```sql
SELECT * FROM prod.db.table.history;
SELECT * FROM prod.db.table.snapshots;
SELECT * FROM prod.db.table.refs;
CALL spark_catalog.system.ancestors_of('db.tbl');
```

Snapshot summary 中包含 `operation` 字段及文件级统计（`added-records`、`total-records`、`added-data-files` 等）。

**10. 增量 / CDC 读取能力**

Iceberg 提供两种增量读取机制：

**(a) DataFrame API 增量读取（仅 append 操作）**

```scala
spark.read
  .format("iceberg")
  .option("start-snapshot-id", "10963874102873")
  .option("end-snapshot-id", "63874143573109")
  .load("path/to/table")
```

限制：仅支持 `append` 操作产生的增量数据，不支持 `replace`、`overwrite`、`delete`。Spark SQL 不支持此语法。

**(b) `create_changelog_view` 过程（完整 CDC）**

```sql
CALL spark_catalog.system.create_changelog_view(
  table => 'db.tbl',
  options => map('start-snapshot-id','1','end-snapshot-id', '2'),
  identifier_columns => array('id', 'name')
);

SELECT * FROM tbl_changes;
SELECT * FROM tbl_changes WHERE _change_type = 'INSERT' AND id = 3 ORDER BY _change_ordinal;
```

Changelog view 包含 CDC 元数据列：
- `_change_type`：`INSERT` / `DELETE` / `UPDATE_BEFORE` / `UPDATE_AFTER`
- `_change_ordinal`：变更顺序
- `_commit_snapshot_id`：变更所属 snapshot ID

高级特性：
- `net_changes => true`：去除中间状态，仅输出净变更
- `compute_updates => true`：通过 identifier columns 识别 update 前后镜像（pre/post update images）
- 自动去除 copy-on-write 产生的 carry-over 行（可通过 `SparkChangelogTable` 查看原始 changelog）

**(c) 原始 Changelog 表**

```sql
SELECT * FROM spark_catalog.db.tbl.changes;
```

**11. 存储与成本模型**

| 方面 | 说明 |
|---|---|
| **版本化机制** | MVCC：每次写入生成新 snapshot，数据文件不可变 |
| **更新策略** | Copy-on-Write（默认）或 Merge-on-Read（format-version 2+） |
| **历史版本存储** | Zero-copy：旧 snapshot 引用已有的数据文件，仅新增/修改的文件占用额外空间 |
| **元数据开销** | Manifest file + Manifest list per snapshot（Avro 格式），通常远小于数据文件 |
| **额外计费** | 无额外计费机制（开源，存储成本取决于底层 S3/HDFS/GCS） |
| **垃圾回收** | 需显式调用 `expire_snapshots` 删除过期 snapshot 及不再引用的数据文件 |
| **孤儿文件清理** | `remove_orphan_files` 过程清理未被元数据引用的文件（默认保留 3 天） |
| **元数据清理** | 可通过 `write.metadata.delete-after-commit.enabled=true` 自动清理旧 metadata JSON 文件（默认保留最近 100 个版本） |

**12. 与流式 / MV / 下游集成**

| 集成方式 | 说明 |
|---|---|
| **Spark Structured Streaming** | 支持 Iceberg 作为流式 source 和 sink |
| **Flink** | 支持 Flink CDC 读写、Branch 读写 |
| **WAP（Write-Audit-Publish）** | 在 audit branch 上写入 → 验证 → `fast_forward` 合并到 main |
| **Changelog View** | `create_changelog_view` 生成 CDC 视图，可直接 feed 下游 ETL |
| **物化视图** | Iceberg 本身无内建 MV 支持；依赖引擎层（如 Spark / StarRocks / Trino）实现 |
| **Branch 协作** | 不同工作流可在独立 Branch 上并行写入，互不干扰 |
| **表复制** | `rewrite_table_path` 过程支持跨存储系统的表元数据路径重写（配合文件拷贝工具实现完整表复制） |

**13. 限制与注意事项**

| 限制 | 说明 |
|---|---|
| **增量读取** | DataFrame API 增量读取仅支持 `append` 操作；完整 CDC 需使用 `create_changelog_view` |
| **UNDROP TABLE** | 不支持，`DROP TABLE` 后无回收站机制 |
| **Snapshot 不自动过期** | 必须显式调用 `expire_snapshots`，否则数据和元数据无限积累 |
| **Branch/Tag 名称冲突** | 名称与 snapshot ID 字符串相同时，`VERSION AS OF` 优先匹配 snapshot ID |
| **Schema 行为不一致** | Branch 查询使用当前 schema，snapshot ID/timestamp/tag 查询使用历史 schema |
| **Spark 版本要求** | SQL time travel 语法需要 Spark 3.3+；Spark 3.0 及更早版本 DataFrameReader 的 option 被静默忽略 |
| **Format-version 要求** | 行级更新/删除（delete files）需 format-version 2+；format-version 1 仅支持 append 和 overwrite |
| **Rollback vs Set** | `rollback_to_snapshot` 要求祖先关系；`set_current_snapshot` 无此限制但风险更高 |
| **元数据膨胀** | 高频写入（流式）产生大量 metadata JSON，需配置自动清理或定期维护 |
| **孤儿文件保留期** | `remove_orphan_files` 默认 3 天保留期，缩短可能误删进行中写入的文件 |
| **Cherry-pick 限制** | 仅支持 append 和 dynamic overwrite snapshot 的 cherry-pick |
| **Metadata table 跨 snapshot** | "all" 前缀的 metadata table（如 `all_data_files`）可能对同一文件产生多行（跨 snapshot 引用） |


#### 2.2.4 BigQuery

> 信息来源：`cloud.google.com/bigquery/docs` — Time Travel, FOR SYSTEM_TIME AS OF, Table Snapshots, Table Clones, Change History, Change Data Capture, Materialized Views, INFORMATION_SCHEMA

BigQuery 是 Google Cloud 的全托管云端数仓，内置 Time Travel 与 Fail-safe 两层数据保留机制，以列式存储块版本化方式实现历史数据访问。

**1. 典型用户场景**

BigQuery 官方文档列出的 Time Travel 典型场景：

- **误操作恢复**：查询被更新或删除的数据、恢复被删除/过期的表或数据集。
- **报表复现 / 审计**：通过 `FOR SYSTEM_TIME AS OF` 读取某个历史时间点的表状态，用于复现当时数据口径。
- **表快照长期保留**：通过 Table Snapshot 将 Time Travel 窗口内的数据固化为长期只读快照，用于合规归档（突破 7 天限制）。
- **表克隆用于开发/测试**：通过 Table Clone 基于历史版本创建轻量级可写副本，用于沙盒分析、开发测试。
- **增量变更追踪**：通过 `APPENDS` / `CHANGES` 函数读取表在指定时间范围内的增量变更行，用于增量 ETL、表副本维护。

**2. 时间点查询语法**

BigQuery 使用标准 SQL 的 `FOR SYSTEM_TIME AS OF` 子句：

```sql
-- 查询 1 小时前的历史版本
SELECT * FROM mydataset.mytable
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- 查询绝对时间点
SELECT * FROM mydataset.mytable
  FOR SYSTEM_TIME AS OF '2017-01-01 10:00:00-07:00';

-- 替换表后查询替换前的版本
SELECT * FROM books FOR SYSTEM_TIME AS OF before_replace_timestamp;

-- DML 操作前的版本（通过 JOBS_TIMELINE 获取 job start timestamp）
SELECT * FROM books FOR SYSTEM_TIME AS OF JOB_START_TIMESTAMP;
```

注意：`FOR SYSTEM_TIME AS OF` 中 `timestamp_expression` 的默认时区是 `America/Los_Angeles`（而非 timestamp 字面量默认的 `UTC`）。

**3. 版本定位方式**

| 定位方式 | 语法 | 说明 |
|---------|------|------|
| **Timestamp（SQL）** | `FOR SYSTEM_TIME AS OF <timestamp_expr>` | 支持绝对时间戳、`CURRENT_TIMESTAMP()` 偏移、变量等 |
| **Unix 毫秒时间戳（CLI/API）** | `tableid@<epoch_ms>` | 用于 `bq cp` 命令和 API 的 time decorator |
| **相对偏移（CLI）** | `tableid@-<offset_ms>` | 如 `@-3600000` 表示 1 小时前 |
| **最早可用版本（CLI）** | `tableid@0` | 返回 Time Travel 窗口内最早的数据 |

BigQuery **不支持**按 version ID / snapshot ID 定位，只能通过 timestamp 定位。单条查询不能同时引用同一张表的两个不同版本（当前版本 + 历史版本，或两个不同历史版本均不允许）。

**4. 默认 / 最大保留时长**

| 配置 | 值 |
|------|-----|
| **默认 Time Travel 窗口** | 7 天 |
| **最小 Time Travel 窗口** | 2 天 |
| **最大 Time Travel 窗口** | 7 天 |
| **Fail-safe 期** | 额外 7 天（不可配置、不可查询、仅通过 Google 客服恢复） |

缩短 Time Travel 窗口可在物理存储计费模型下节省存储费用（逻辑计费模型下无差异）。无付费版本差异——所有 BigQuery 用户均可使用 2–7 天的 Time Travel。

**5. 保留策略粒度**

| 级别 | 说明 |
|------|------|
| **Project** | 可通过 DDL 设置项目级默认 Time Travel 窗口（`default_time_travel_hours`） |
| **Dataset** | 每个 dataset 可单独设置 Time Travel 窗口（2–7 天），覆盖项目默认值 |
| **Table** | **不支持**按表单独设置，表继承所属 dataset 的 Time Travel 窗口 |

已删除的表使用删除时刻生效的窗口时长，后续对 dataset 窗口的修改不影响已删除表的可恢复期。

**6. 适用对象**

| 对象类型 | Time Travel 查询 | Snapshot / Clone | 恢复删除 | 说明 |
|---------|:---:|:---:|:---:|------|
| 标准表（BASE TABLE） | ✅ | ✅ | ✅ | 核心支持对象 |
| 分区表 | ✅ | ✅ | ✅ | 与标准表一致 |
| Table Clone | ✅ | ✅ | ✅ | 作为标准表处理 |
| Table Snapshot | ❌ | — | — | 快照本身不支持 Time Travel |
| 逻辑视图（VIEW） | ❌ | ❌ | ❌ | 不能对视图做 Time Travel，不能直接恢复删除的视图 |
| 物化视图（MV） | ❌ | ❌ | ❌ | 删除后不可恢复，必须重建 |
| 外部表（External） | ❌ | ❌ | — | 不支持（Iceberg 外部表可通过 `FOR SYSTEM_TIME AS OF` 访问 Iceberg 快照） |
| 临时表 | ❌ | ❌ | ❌ | Session / Script / Cached result tables 均不支持 |
| 数据集（Dataset） | — | — | ✅ | 支持恢复删除的数据集（在 Time Travel 窗口内） |

**7. Schema 演进与 Time Travel 的交互**

BigQuery 在 Time Travel 查询时使用**当前 schema** 解析 SQL：

- 使用 `CREATE OR REPLACE TABLE` 替换表后，仍可通过 `FOR SYSTEM_TIME AS OF` 查询替换前的数据——即使新表 schema 不同（官方文档有明确示例）。
- 列式存储块的版本化机制意味着 schema 变更后旧版本的存储块仍保留。
- 官方文档未详细说明 schema 不兼容变更（如列类型变更、列删除等）时的精确行为，但 `CREATE OR REPLACE TABLE` 的场景说明历史版本保留了完整的历史 schema 信息。

**8. 数据恢复能力**

| 恢复方式 | 语法 / 方法 | 说明 |
|---------|------------|------|
| **复制恢复到新表** | `bq cp tableid@<timestamp> new_table` 或 `CREATE TABLE ... AS SELECT ... FOR SYSTEM_TIME AS OF ...` | 通过 time decorator 或 SQL 复制历史版本到新表 |
| **Table Snapshot（零拷贝只读）** | `CREATE SNAPSHOT TABLE ... CLONE ... FOR SYSTEM_TIME AS OF ...` | 从 Time Travel 窗口内任意时间点创建零拷贝只读快照，可永久保留 |
| **Table Clone（零拷贝可写）** | `CREATE TABLE ... CLONE ... FOR SYSTEM_TIME AS OF ...` | 基于历史版本创建独立可写表 |
| **恢复删除的表** | `bq cp tableid@<epoch_ms> new_table` | 不能直接查询已删除的表，必须先通过 copy 恢复 |
| **恢复删除的数据集** | Google Cloud Console / API | 在 Time Travel 窗口内恢复 |

关键限制：
- **不支持同表 ROLLBACK**：没有原生 `ROLLBACK TABLE TO TIMESTAMP` 语法。
- **不能查询已删除表**：即使在 Time Travel 窗口内，也必须先 copy 恢复后才能查询。
- Fail-safe 期间的数据不可直接查询或自行恢复，只能通过 Google 客服紧急恢复。
- 恢复表时不会复制 tags 和分区信息。

**9. 变更历史可观测性**

| 机制 | 说明 |
|------|------|
| **`APPENDS` 函数** | `SELECT * FROM APPENDS(TABLE t, <start_ts>, <end_ts>)` — 返回指定时间范围内追加的所有行 |
| **`CHANGES` 函数** | `SELECT * FROM CHANGES(TABLE t, <start_ts>, <end_ts>)` — 返回所有变更行（含 INSERT / UPDATE / DELETE / TRUNCATE），需启用 `enable_change_history = TRUE` |
| **`INFORMATION_SCHEMA.TABLE_STORAGE`** | 查看 `time_travel_physical_bytes`、`fail_safe_physical_bytes`、`deleted` 状态、`table_deletion_time`、`table_deletion_reason` 等 |
| **`INFORMATION_SCHEMA.TABLES`** | `snapshot_time_ms`（Clone/Snapshot 基准时间）、`is_change_history_enabled` 等 |
| **`INFORMATION_SCHEMA.JOBS`** | 作业历史：DML 操作时间和详情 |
| **Cloud Audit Logs** | 审计日志记录所有 BigQuery API 调用和 DDL/DML 操作 |

**10. 增量 / CDC 读取能力**

BigQuery 提供两层增量读取能力：

**(a) Change History 函数（`APPENDS` / `CHANGES`，无状态）**

```sql
-- 读取最近 1 小时内追加的行
SELECT * FROM APPENDS(TABLE mydataset.mytable,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL);

-- 读取最近 1 小时内所有变更（需启用 enable_change_history）
SELECT * FROM CHANGES(TABLE mydataset.mytable,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), NULL);
```

`CHANGES` 返回 `_CHANGE_TYPE` 列（`INSERT` / `UPDATE_AFTER` / `UPDATE_BEFORE` / `DELETE`），可区分变更类型。需先 `ALTER TABLE ... SET OPTIONS (enable_change_history = TRUE)` 启用。

**(b) CDC 写入（Storage Write API `_CHANGE_TYPE` 伪列）**

写入侧 CDC 能力：通过 Storage Write API 流式写入时设置 `_CHANGE_TYPE`（`UPSERT` / `DELETE`），BigQuery 自动应用 upsert/delete 语义（需声明 Primary Key）。支持 `_CHANGE_SEQUENCE_NUMBER` 伪列控制顺序。

**不支持**：BigQuery 没有类似 Snowflake Streams 的持久化增量消费对象。`APPENDS` / `CHANGES` 是无状态函数调用，需调用方自行维护消费位点。

**11. 存储与成本模型**

| 方面 | 说明 |
|------|------|
| **存储方式** | 列式存储块版本化：修改 cell 时创建受影响存储块的新版本，旧版本保留用于 Time Travel |
| **版本化粒度** | 存储块级（非 cell 级）：存储块大小自适应，修改一个 cell 可能导致整个块被版本化 |
| **Time Travel 存储（物理计费）** | 单独计费，按 active physical storage 费率 |
| **Time Travel 存储（逻辑计费）** | 包含在基础费率中，不额外计费 |
| **Fail-safe 存储** | 物理计费下单独计费；逻辑计费下包含在基础费率中 |
| **Snapshot / Clone 成本** | 零拷贝：仅当基表中与快照/克隆共享的数据被修改或删除时才产生额外费用 |
| **`enable_change_history` 成本** | 启用后存储变更元数据，产生额外存储和计算费用 |

**12. 与流式 / MV / 下游集成**

| 集成能力 | 说明 |
|----------|------|
| **物化视图（MV）** | 支持自动增量刷新；不支持对 MV 做 Time Travel / Snapshot；删除基表后 MV 无法查询，恢复基表后须重建 MV |
| **Table Snapshot** | 可基于 Time Travel 时间点创建零拷贝只读快照，用于长期保留超过 7 天的历史状态 |
| **Table Clone** | 可基于 Time Travel 时间点创建零拷贝可写克隆，独立于基表 |
| **Change History** | `APPENDS` / `CHANGES` 函数可用于增量 ETL，将增量变更同步到外部系统 |
| **CDC 写入** | Storage Write API 支持从外部 CDC 源（Datastream for MySQL/PostgreSQL/Oracle）实时同步到 BigQuery |
| **Streaming Buffer** | 处于 write-optimized storage 中的数据不被 Snapshot / Clone 包含 |
| **CDC + MV 限制** | CDC-enabled 表的基表不能在与其 MV 相同的查询中被引用 |

**13. 限制与注意事项**

| 限制 | 说明 |
|------|------|
| **最大 Time Travel 窗口** | 7 天，不可延长（可通过 Table Snapshot 突破） |
| **Fail-safe 不可直接访问** | 额外 7 天，不可查询，仅通过 Google 客服恢复 |
| **外部表不支持** | 外部表不支持 Time Travel（Iceberg 外部表走 Iceberg 自身 snapshot） |
| **临时表不支持** | Session / Script / Cached result tables 均不支持 |
| **VIEW / MV 不支持** | 不能对 VIEW 或 MV 做 Time Travel、Snapshot、Clone |
| **不能查询已删除表** | 即使在 Time Travel 窗口内也不能直接查询，须先 copy 恢复 |
| **同一查询不能引用同表的不同版本** | 单条 SQL 中不能同时引用当前版本和历史版本 |
| **无同表 ROLLBACK** | 没有原生 `ROLLBACK TABLE TO TIMESTAMP` 功能 |
| **Table 级别不可配置** | Time Travel 窗口只能在 project / dataset 级别配置 |
| **恢复不保留分区信息** | 从历史版本恢复表时不会复制分区方案和 tags |
| **PIVOT / UNPIVOT 限制** | 不能直接与 `FOR SYSTEM_TIME AS OF` 组合（需子查询） |
| **Row-level access** | 有行级访问策略的表，仅 `bigquery.admin` 角色可访问历史数据 |
| **`CHANGES` 需显式启用** | 使用 `CHANGES` 函数需设置 `enable_change_history = TRUE`，产生额外费用 |
| **Streaming Buffer 数据** | Snapshot / Clone 不包含 Streaming Buffer 中的数据 |

### 2.3 能力对比总结

| # | 维度 | Snowflake | Databricks (Delta Lake) | Spark + Apache Iceberg | BigQuery |
|---|------|-----------|------------------------|----------------------|----------|
| 1 | **典型用户场景** | 误操作恢复、数据备份复制、变更分析；Stream + CHANGES 实现增量 ELT | 快照隔离、数据修复、时序查询、分析/ML 复现、归档、生产表实验 | 审计合规（Tag）、WAP 验证、误操作回滚、增量 CDC、可复现分析 | 误操作恢复、报表复现/审计、长期快照归档、开发/测试克隆、增量变更追踪 |
| 2 | **时间点查询语法** | `AT(TIMESTAMP\|OFFSET\|STATEMENT => ...)` / `BEFORE(STATEMENT => ...)` | `TIMESTAMP AS OF <expr>` / `VERSION AS OF <v>` / `@ts` / `@vN` | `TIMESTAMP AS OF` / `VERSION AS OF` / `FOR SYSTEM_TIME\|VERSION AS OF` / Branch·Tag 语法 | `FOR SYSTEM_TIME AS OF <timestamp_expr>` |
| 3 | **版本定位方式** | Timestamp、Offset（秒）、Statement ID（query ID）、Stream offset | Timestamp、Version 号（long）；**不支持** Offset / Statement / Stream | Timestamp、Snapshot ID（64-bit long）、Branch 名称、Tag 名称、Unix timestamp（秒） | Timestamp（SQL）、Unix ms（CLI/API）、相对偏移 ms（CLI）；**不支持** version/snapshot ID |
| 4 | **默认/最大保留时长** | 默认 1 天；Standard 最大 1 天，Enterprise+ 最大 **90 天** + 7 天 Fail-safe | 有效默认 **7 天**（受 VACUUM 控制）；无硬性最大上限，但官方不建议长期归档 | 默认 **5 天**（`max-snapshot-age-ms`）；无硬性上限，需显式 `expire_snapshots` | 默认 **7 天**，最大 7 天 + 7 天 Fail-safe（不可查询，仅 Google 客服恢复） |
| 5 | **保留策略粒度** | **Account → Database → Schema → Table** 四级继承 + `MIN_DATA_RETENTION_TIME_IN_DAYS` 下限 | **Table** 级 + Session 默认值；不支持 Schema/Database/Account 级 | **Table** 级 + **Branch/Tag** 级独立保留策略；不支持 Database/Catalog 级 | **Project → Dataset** 级；Table 继承 Dataset，不支持 Table 级单独设置 |
| 6 | **适用对象** | Permanent/Transient/Temporary 表 ✅、View ✅（只读）、Dynamic Table ✅、Iceberg Table ✅；External Table ❌、MV ❌ | Delta Managed/External 表 ✅、Streaming Table ✅、Clone 表 ✅；MV ❌、非 Delta 格式 ❌ | Iceberg Managed Table ✅、Metadata Table ✅；View ❌、非 Iceberg 外部表 ❌ | 标准表 ✅、分区表 ✅、Table Clone ✅；Snapshot 本身 ❌、View ❌、MV ❌、External ❌、临时表 ❌ |
| 7 | **Schema 演进与 TT 交互** | 使用**当前 schema**；新增列在历史版本返回 NULL | 默认使用**当前 schema**；Column Mapping 模式下批量读可用历史 schema；非加法变更可能阻断跨版本查询 | **Snapshot ID/Timestamp/Tag → 历史 schema**；**Branch → 当前 schema**；通过唯一列 ID 保证演进正确性 | 使用**当前 schema**；`CREATE OR REPLACE TABLE` 后仍可查询替换前数据 |
| 8 | **数据恢复能力** | **UNDROP** ✅（表/Schema/DB）、**零拷贝 CLONE** ✅、CTAS ✅；**同表回滚**需 CLONE+SWAP 模拟 | **RESTORE（同表回滚）** ✅（生成新版本，可逆）、Deep/Shallow **CLONE** ✅、INSERT/MERGE 恢复 ✅；**UNDROP** ❌ | **rollback_to_snapshot/timestamp** ✅、**set_current_snapshot** ✅、**cherrypick** ✅、CTAS ✅；**UNDROP** ❌ | Copy 恢复 ✅（bq cp / CTAS）、**Table Snapshot**（零拷贝只读）✅、**Table Clone**（零拷贝可写）✅；**同表 ROLLBACK** ❌、**UNDROP** ❌（需先 copy） |
| 9 | **变更历史可观测性** | `SHOW ... HISTORY`、`CHANGES` clause、Streams 查询元数据列 | `DESCRIBE HISTORY`（返回 version/timestamp/operation/operationMetrics 等） | Metadata tables（`history`/`snapshots`/`metadata_log_entries`/`refs`）、`ancestors_of` | `APPENDS`/`CHANGES` 函数、`INFORMATION_SCHEMA`（TABLE_STORAGE/TABLES/JOBS）、Cloud Audit Logs |
| 10 | **增量/CDC 读取能力** | **CHANGES clause**（无状态，DEFAULT/APPEND_ONLY 模式）+ **Streams**（有状态事务性推进，Standard/Append-only/Insert-only 类型） | **Change Data Feed (CDF)**：批量 `table_changes()` + 流式 `readChangeFeed`；需**显式启用**且**不可追溯**历史 | DataFrame API 增量读（仅 append）+ **`create_changelog_view`**（完整 CDC，支持 net_changes/compute_updates） | **APPENDS**（仅追加行）/ **CHANGES**（全类型，需启用 `enable_change_history`）：无状态函数，需调用方自行维护消费位点 |
| 11 | **存储与成本模型** | MVCC；历史版本按标准费率计费 + 7 天 Fail-safe；CLONE 零拷贝；Stream 延长源表保留（最多 +14 天） | Copy-on-Write；VACUUM 前几乎零额外成本；CDF 少量额外开销；Deep CLONE 需复制文件，Shallow CLONE 零拷贝 | MVCC + CoW/MoR；zero-copy 引用旧文件；无额外计费（开源，取决于底层 S3/HDFS）；需显式 GC | 列式存储块版本化；物理计费下 TT 存储单独计费，逻辑计费下包含；Snapshot/Clone 零拷贝 |
| 12 | **与流式/MV/下游集成** | **Stream + Task**：一等公民 CDC→ELT pipeline；Dynamic Table 支持 Stream；MV 不可追踪变更 | **Structured Streaming + CDF**：核心增量模式；Schema 变更终止流；MV 不支持 TT | **Spark/Flink Streaming**；**WAP** 工作流（Branch→fast_forward）；**Changelog View**；无内建 MV | MV 支持自动增量刷新但**不支持 TT**；Table Snapshot/Clone；APPENDS/CHANGES 函数；CDC 写入（Storage Write API） |
| 13 | **限制与注意事项** | External Table 不支持；Hybrid Table 仅部分支持；UNDROP 同名冲突；Schema Change 可能语义偏差；STATEMENT 14 天有效 | 有效窗口受 VACUUM 限制（默认 7 天）；MV 不支持；CDF 需显式启用不可追溯；无 UNDROP；Schema 更新终止流 | Snapshot 不自动过期需手动 GC；Branch/Tag 名冲突优先匹配 snapshot ID；Schema 行为不一致；无 UNDROP；元数据膨胀 | 最大 7 天不可延长；外部/临时/View/MV 不支持；不能查询已删除表；无同表 ROLLBACK；单查询不能引用同表不同版本 |
| 14 | **支持的 DML 与 DDL** | **DML**：INSERT / UPDATE / DELETE / MERGE / TRUNCATE 均生成版本。**分区**：Snowflake 采用自动 micro-partition，无用户可操作的 DROP / TRUNCATE PARTITION 语法。**DDL**：CREATE OR REPLACE 丢弃历史；DROP TABLE 可 UNDROP；ALTER 不生成数据版本 | **DML**：INSERT / UPDATE / DELETE / MERGE / INSERT OVERWRITE / TRUNCATE 均生成新版本。**分区**：Delta Lake **不支持** ALTER TABLE DROP / ADD PARTITION 等分区管理语法；分区裁剪靠 DELETE + WHERE 条件实现，按普通 DML 生成版本。**DDL**：CREATE / SET TBLPROPERTIES / OPTIMIZE / RESTORE / CLONE 均生成新版本；VACUUM 记入日志 | **DML**：INSERT / INSERT OVERWRITE / UPDATE / DELETE / MERGE / TRUNCATE 均生成新 Snapshot。**分区**：INSERT OVERWRITE 按 dynamic partition overwrite 语义替换受影响分区，生成 overwrite 类型 Snapshot；Partition evolution（分区方案变更）仅更新 metadata 不生成 Snapshot。**DDL**：Schema evolution 仅更新 metadata 不生成 Snapshot | **DML**：INSERT / UPDATE / DELETE / MERGE / TRUNCATE + 流式写入 / 批量加载均被跟踪。**分区**：支持 ALTER TABLE DROP PARTITION 删除分区，被 CHANGES 函数跟踪（"Individual table partition deletions"）；删除的分区数据在 TT 窗口内可恢复。**DDL**：CREATE OR REPLACE 生成新版本旧版本仍可 TT 查询；DROP TABLE 在窗口内可恢复 |

### 2.4 对 StarRocks 的启示

结合 StarRocks 存算分离架构与增量 MV 需求，从以上调研中提炼以下要点：

**1. 语法设计：采用业界主流 `FOR TIMESTAMP AS OF` 语法**

四款产品均以 timestamp 作为最基本、最通用的版本定位方式。StarRocks 面向用户仅暴露 timestamp 的决策与 BigQuery 一致，降低概念负担；内部实现可参考 Iceberg 的 snapshot ID 做细粒度版本管理（用于增量 MV 版本推进等）。

**2. 保留策略：表级配置 + 合理默认值**

- Snowflake 的四级继承最灵活但复杂；Databricks 和 Iceberg 以表级为主。StarRocks 首期按**表级**配置保留策略即可满足需求，后续可扩展到 Database 级继承。
- 默认保留时长建议参考业界 5–7 天的共识（Iceberg 5 天、Databricks/BigQuery 7 天），结合存算分离 MVCC zero-copy 的低成本优势，可适当设长。

**3. Schema 语义：首期采用"当前 schema"策略**

Snowflake、Databricks、BigQuery 均默认使用当前 schema，仅 Iceberg 在 snapshot ID/tag 场景使用历史 schema。StarRocks 首期采用当前 schema 符合增量 MV 需求（MV 始终基于最新 schema 执行），且实现复杂度低。当历史版本与当前 schema 不兼容时直接报错即可。

**4. 数据恢复：Copy 优先，CLONE/ROLLBACK 分期引入**

- 四款产品均支持 CTAS/Copy 恢复（最简单通用）。
- Databricks 的 RESTORE（同表回滚）和 Snowflake 的零拷贝 CLONE 用户体验最优，但实现复杂度高。StarRocks 首期聚焦 Copy 恢复（P0），CLONE 和 ROLLBACK 作为 P1/P2 分期引入。

**5. 增量/CDC 读取：为增量 MV 奠定基础**

- Snowflake 的 Streams（有状态 + 事务性偏移推进）与 Iceberg 的 `create_changelog_view`（支持 net_changes / compute_updates）是增量 MV 最直接的参考模型。
- StarRocks 存算分离架构天然具备 MVCC 多版本能力，可在此基础上实现类似 CHANGES clause 的无状态增量读取，为增量 MV 提供 `[v_from, v_to]` 范围的 delta 数据。
- CDF/CHANGES 的"需显式启用"设计（Databricks、BigQuery）在 StarRocks 中可简化为"开启 Time Travel 即自动具备增量读取能力"，降低用户配置负担。

**6. 存储成本：存算分离 MVCC 的天然优势**

- StarRocks 存算分离架构中，数据文件存储在对象存储上，MVCC 机制下历史版本天然 zero-copy（类似 Iceberg）。相比 Snowflake/BigQuery 的额外计费模型，StarRocks 的 Time Travel 存储成本主要取决于过期 GC 策略，几乎没有额外开销。
- 需要实现可靠的过期清理机制（类似 Iceberg `expire_snapshots` / Delta `VACUUM`），避免历史数据无限积累。

**7. 限制与取舍**

- **MV 不支持 Time Travel 是业界共识**：四款产品均不支持对 MV 做 Time Travel 查询，StarRocks 可沿用此设计。
- **DROP TABLE 不纳入首期**：Snowflake 的 UNDROP 虽便利但引入元数据生命周期管理复杂度，StarRocks 已有 RECOVER（Recycle Bin）机制覆盖误删场景，首期不纳入 Time Travel 范畴。
- **External Table 不支持 Time Travel** 也是业界共识；StarRocks 通过 Iceberg/Hudi 等 connector 读取外部表时，由外部表格式自身的版本管理机制提供 Time Travel 能力。
