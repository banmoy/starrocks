## StarRocks Change Data Capture Proposal（Shared-Data / 存算分离）

### 1. 背景与用户价值

增量物化视图（IVM）刷新时需要查询历史版本、当前版本以及两个版本之间的变更，需要 StarRocks 内表具有捕获数据变更的能力，即 Change Data Capture（CDC）。除了 IVM，其它 CDC 典型应用场景如下
- 
- 

本文档初步探讨 StarRocks CDC 的产品和技术路径设计。


#### 前提

- "version" 如何定义和标识
- CDC 数据湖一起考虑


### 2. 产品调研

### 3. 目标

- 讨论 StarRocks CDC 产品能力，确保覆盖 IVM 场景需求，其它场景有哪些
- 讨论实现路径，短期要能支持 IVM 场景

### 3. 产品定义

CDC 允许捕获行级变更，并可以消费

#### 3.0 前提

- 存储层支持 Row Lineage，每一行数据包含如下信息
  - ROW_ID (BIGINT): 逻辑行唯一标识，INSERT 生成全局唯一 row id，UPDTAE 后 row id 保持不变
  - ROW_VERSION (BIGINT): 数据的版本，INSERT 生成初始版本 (不一定从 0 开始)，UPDATE 后版本增加 (不一定连续)，row 之间的 verison 没有关系，具体实现可以单独讨论，比如可以用 partition version 或 gtid 表示

TODO: 举例说明如何使用，比如增量 MV 等

- history_retention
  - 需要读取的变更在保留周期内

#### 3.1 概念

**CHANGES**：DML 以及部分 DDL（比如 DROP PARTITION）产生的**行级数据变更（包括插入、更新和删除）**，包括变更前后的数据，以及变更类型等元数据
**Change Data Capture (CDC)**：捕获 CHANGES 的动作

> CHANGE 和 CDC 是其它系统中的常用概念


##### 3.1.1 变更类型 (Change Type)

变更类型
- INSERT: 插入数据
- DELETE: 删除数据
- UPDATE_BEFORE: 更新操作生成两条变更，这个表示更新前的数据
- UPDATE_AFTER: 更新操作生成两条变更，这个表示更新后的数据

某些场景下 UPDATE 可以表示成 DELETE+INSERT，比如增量物化视图，见 3.4
- 前者可以保持语义的完整性，和 DELETE 区分开，在一些只需要 UPDATE_AFTER 的场景下可以跳过 UPDATE_BEFORE ，比如同步到外部 kv 系统，只需要 overwrite。但需要将 BEFORE 和 AFTER 进行关联，因此生成成本要高
- 后者适用于不需要 UPDATE 完整语义的场景，比如增量MV，优点是不需要将 BEFORE 和 AFTER 进行关联，生成变更的成本低，比如主键表只需要根据 old segment delete vector diff 以及 new segment 即可生成全部变更，不需要 old 和 new segment 之间 PK 进行映射。

##### 3.1.2 CHANGES 组成

 CHANGES = 数据列 + 元数据列
  - 数据列：与表的列一致，可以只包含需要的列
  - 元数据列

| 名称 | 类型 | 含义 |
| :--- | :--- | :--- |
| CHNAGE_TYPE | TINYINT | 变更类型: INSERT、DELETE、UPDATE_BEFORE、UPDATE_AFTER<br>备注: 如果配置了 UPDATE = INSERT + DELETE，可以去掉 UPDATE_BEFORE/UPDATE_AFTER |
| ROW_ID | BIGINT | 逻辑行标识，同一行的所有变更具有相同的 row id |
| ROW_VERSION | BIGINT | 产生变更的 row version，配对的 UPDATE_BEFORE/UPDATE_AFTER 具有相同的 version |

##### 3.2 支持的表类型

| 表类型 | 支持的操作 | 变更类型 |
| :----- | :--------- | :------- |
| 明细表 | - DML: 只支持 append 类型，DELETE 成本高，暂不考虑<br>- DDL: TRUNCATE TABLE/PARTITION、DROP PARTITION | - INSERT<br>- DELETE（DDL 产生） |
| 主键表 | - DML: `INSERT INTO`、`INSERT OVERWRITE`、`DELETE`、`UPDATE`，以及各类导入（`STREAM LOAD`/`BROKER LOAD`/`ROUTINE LOAD` 等）<br>- DDL: TRUNCATE TABLE/PARTITION、DROP PARTITION | - INSERT<br>- DELETE<br>- UPDATE_BEFORE<br>- UPDATE_AFTER |
| 聚合表 | - DML: 只支持 append，其它成本高<br>- DDL: TRUNCATE TABLE/PARTITION、DROP PARTITION | - INSERT（append 产生，aggregate 语义，对变更聚合后的结果，见下面示例）<br>- DELETE（DDL 产生） |
| 更新表 | - 不支持 | - 不支持 |

聚合表 CHANES 在 IVM 中的使用示例，Applovin 真实场景，基表 agg table，async mv 进行上卷
  ```
  base agg table: key1, key2, key3, val1 sum
  async mv: select key1, key2, sum(val1) group by 1, 2

  // 导入数据：k=(1,1,1) 和 (2,2,2) 分别有两条
  (1, 1, 1, 1) (1, 1, 1, 2) (2, 2, 2, 1) (3, 3, 3, 1), (2, 2, 2, 2)
  
  // CHANGES：k=(1,1,1) 聚合后保留 1 条变更，k=(2,2,2) 保留 2 条原始变更
  // 底层原理：直接读取导入生成的增量 rowset 生成变更，rowset 存储的是 aggregate 后的结果
  (1, 1, 1, 3) (2, 2, 2, 1) (3, 3, 3, 1), (2, 2, 2, 2)
  ```


#### 3.2 CHANGES 管理

每个 Tablet 生成并维护自己的 changes，保证同一个 row 的变更由同一个 tablet 生成，消费也是

#### 3.3 消费

#### 3.3.1 消费粒度

以版本为粒度进行消费，可以指定读取某个版本或连续几个版本的变更，但不能只读取某个版本的部分变更

#### 3.3.2 用户接口

- 指定 timestamp 或 version 查询 (具体语法待定)
```sql

-- Option 1: Table Function 
SELECT * FROM table_changes("tbl", ts1, ts2);
SELECT * FROM table_changes("tbl", v1, v2);

-- Option 2: CHANGES clause similar to Snowflake
SELECT * FROM tbl CHANGES FROM VERSION v1 to v2;
SELECT * FROM tbl CHANGES FROM TIMESTAMP v1 to v2;
```

- 定义 `STREAM` 对象，可以查询并自动管理 offset，类似 Snowflake、云器等系统，
```sql

-- 初始 offset 1
CREATE STREAM stream ON tbl;

-- 查询 offset 到最新版本之间的 CHANGES，[offset 1, now]
SELECT * FROM stream;

-- DML 中查询 streamm，执行成功后自动更新 offset 到最新位置
INSERT INTO target_tbl SELECT * FROM stream;


SELECT * FROM stream;
```

- 对接 Flink/Spark structured streaming，使用 SDK 通过 RPC 与 CN 进行数据传输
  - PULL 模式：类似当前 connector SCAN，客户端发一个特殊查询，指定要消费的范围，服务端执行一个特殊 plan，通过 RPC 返回给客户端
  - PUSH 模式：客户端发起订阅，服务端有新的 CHANGES 自动推送给客户端，实时性更高，暂不考虑

#### 3.3.3 消费行为

- Net Changes
- 语义：将多个导入的 CHANGES 合并成最终的，比如
  - insert, update, delete = empty
  - update (1 -> 2), update (2 -> 4) = before 1 + after 4
  - insert 1, update (1 -> 3) = insert 3
 - 场景
  - IVM 只需要最终的集合差，不需要看到中间变化，不影响结果并且减少增量计算的数据量
  - 审计等可能需要中间的每一个变化


- Update Semantic
 - 语义：表示成 UPDATE_BEFORE + UPDATE_AFTER 还是 DELETE + INSERT
  - 前者可以保留完整的 UPDATE 语义，一些使用场景可以据此进行优化，比如 Flink 同步数据到 upsert 语义的外部系统(比如KV)，可以将 update_before 过滤掉，提高效率
  - 后者不需要将 update 前后的数据进行显示变更，生成方案有更多选择，成本上也不同等方面有优势，对于 IVM 来说够用
 - UPDATE_BEFORE + UPDATE_AFTER 在哪里生成

- Projection 以及 Filter PushDown 


#### 3.4 增量物化视图需求特点

 * Net Changes
 * UPDATE 可以表示成 DELETE + INSERT
 * DDL (drop/truncate partition相关) 可以低优支持，批处理适合全量刷新

#### 3.5 系统内部接口

- 存储层 TabletChangesReader
  - 输入
    - 读取的列
    - predicates
    - changes 相关参数，比如 [start_version, end_version)
  - 输出
    - Chunk: 数据列 + 元数据列
```c++
class TablTabletChangesReader final : public ChunkIterator {
  Status get_next(Chunk* chunk);
}
```

- 与查询层对接
  - Plan: OlapChangesScanNode，比如 OlapChangesScanNode -> IVM
  - Execution
    - ConnectorScanNode/ConnectorScanOperator: ConnectorType = OLAP_CHANGES
    - OlapChangesDataSource
      - 通过 TabletChangesReader 读取 tablet 指定范围 CHANGES
      - 如果 tablet 支持并行读取 CHANGES，可能有多个实例并行 SCAN，每个负责其中的一部分。并行 scan 涉及到同一个 row 多个 changes 的 order 语义，可以根据使用场景选择，详见消费行为。 对于明细表/聚合表只支持 append 比较简单，可以按文件并行生成

### 4. 技术方案

#### 问题定义

导入产生的 insert 或 update_after 变更可以通过新增的 rowset 读取，适用于明细表、聚合表只允许 append，以及只有 insert 的主键表，但如果主键表有 update_before/delete 需要从 segment 查询旧值 (列出可能的问题)
- 完整的 update 语义，需要维护 update_before 和 update_after 的映射，IVM 场景可以弱化
- 列式存储，随机 IO，尽可能攒批来减少
- 其它？


#### 可选方案

1. 导入时生成，主键表更新 primary index 能够拿到delete/update_before旧的 rssid，根据 rssid 读取旧值，新旧值配对写到 change log，查询时读取 change log 
  - 优点：利用 pk 已有机制配对 before 和 after， 查询读取 change log 效率高
  - 缺点：读取旧值影响导入性能，change log 增加存储和维护开销
2. 导入时增加一些轻量的元信息辅助定位 delete/update_before，从 segment 读取旧值移到查询时来做
  - 优点：不影响导入，没有额外存储开销，查询时如果涉及多个导入变更，可以攒批来查询旧值，提高效率
  - 缺点：影响查询

抉择：导入性能要优先于 IVM 等场景，采用方案2的思路

##### UPDATE = DELETE + INSERT

##### 整体来看分为两种

划分维度
- 生成时机: 导入 vs 读取
- 存储机制: log-based vs snapshot diff

共同问题
- 导入速度 > 消费速度，产生很多小文件
- 

##### UPDATE = UPDATE_BEFOER + UPDATE_AFTER


#### Net Changes

##### 问题

如下算子需要对多个 changes 进行合并
* select * from t  保留最 old 的 delete, 以及最新的 insert, 需要根据 row version 排序
* join / group by 合并减少计算开销，尤其是 join
* 有些情况不需要 net changes，比如 group by sum, agg 自然合并了，不需要单独做



- 顺序
 - 语义：下游算子收到同一个 row 的所有 changes 的顺序
  - 跨多个导入消费时，是否需要 row version 先后，分布式存储，无法保证 tablet 之间也有序
  - 对于 UPDATE，是否需要 update_before/update_after 相邻，比如生成 debezium 格式，要求比较高除了存储层出来的时候要顺序，后续所有的数据流都要报这
  - 在流式系统里需要，可能输出中间结果，通过顺序确保一致，IVM 本质还是批处理，所有变更处理完才原子可见，暂时不提供顺序保证，如果需要可以在计算层按照 <commit_version, row_id, change_type> 排序
 - 谁来保证
  - 存储层，同一个 row 的 changes 必须从同一个并发 (OlapChangesDataSource) 按顺序产生，下游可以基于此进行优化，比如
  - 计算层，同一个 row 的 changes 可能从多个并发产生，无法保序，计算层按需根据 (row_id, row_version, change_type) 排序


反馈：如果小文件太多，可以返回报错，重试另一种方案


文件多 + 数据量大时，需要并行
1. scan 并行，不同version之间并行，文件之间并行，同一个文件内部并行
2. 合并并行，不同 row 之间合并并行

这一套做下来基本就是当前查询的 IO+计算 模型

###### How

- 存储层
- 计算层
  - 存储层逐行


* 计算层
* 存储层
允许 false changes


计算引擎层通用可以把 iceberg, delta 的同时做掉


存储层可以增加两个 bimtap vector
- 每个 old segment 的 diff delete bitmap -> 快速读取 delete changes
- 每个 old segment 的 update_before bitmap -> 快速读取 update changes
- 每个 new segment 的 diff update_after bitmap
- 如果不加，需要先读取老的值，在读取旧的值，并行能力不好 
- 跨多个导入的情况，还可以把同一个 segment 和 delete 合并、insert 合并，并取差集

- 高频导入小文件优化
  - 导入速度高于
  - 直接比较文件，对于 (rowId, rowVersion) 相同的可以过滤掉

- 存储层计算 net changes
  - 
  - 并发
    - 大导入数据量大，单节点

- 大导入，并且和小导入混合
  - 可能问题，并发 net changes，
  - 在 tablet 层做窗口函数效果不好 

#### 附录


##### Appendix A: Flink UPDATE CHANGE TYPE

- 场景设定

```sql
-- StarRocks 主键表 CDC 源
CREATE TABLE sr_orders (
  order_id INT, user_id INT, amount INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'starrocks-cdc');

-- 维表
CREATE TABLE dim_users (
  user_id INT, name STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('connector' = 'jdbc', 'url' = '...');

-- KV Sink（Redis，有主键，upsert 模式）
CREATE TABLE order_details (
  order_id INT, user_name STRING, amount INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'redis');

INSERT INTO order_details
SELECT o.order_id, d.name, o.amount
FROM sr_orders o
JOIN dim_users FOR SYSTEM_TIME AS OF o.proc_time AS d
  ON o.user_id = d.user_id;
```

**业务操作**：order_id=1 的 amount 从 100 改成 200（user_id=1 对应 name="张三"）。

- 方案 A：StarRocks CDC 提供 UPDATE_BEFORE + UPDATE_AFTER

```
CDC 产出:        -U(1, u1, 100)          +U(1, u1, 200)
                      │                        │
Lookup Join:    -U(1, "张三", 100)       +U(1, "张三", 200)
                      │                        │
           DropUpdateBefore                    │
                      │                        │
                   [丢弃 ✓]                    │
                                               ▼
Redis:                                   SET order:1 {张三, 200}
```

**Redis 操作**：1 次 SET

**效果**：数据**始终可见**，从 `{张三, 100}` 原子地变成 `{张三, 200}`。

- 方案 B：StarRocks CDC 提供 DELETE + INSERT

```
CDC 产出:        -D(1, u1, 100)          +I(1, u1, 200)
                      │                        │
Lookup Join:    -D(1, "张三", 100)       +I(1, "张三", 200)
                      │                        │
           DropUpdateBefore                    │
                      │                        │
                [拦不住 -D ✗]                  │
                      ▼                        ▼
Redis:         DEL order:1              SET order:1 {张三, 200}
```

**Redis 操作**：1 次 DEL + 1 次 SET

**效果**：DEL 和 SET 之间存在一个**时间窗口**，order:1 **不存在**。

- 用户可见的差异

| | 方案 A (-U/+U) | 方案 B (-D/+I) |
|--|--|--|
| **Redis 写入次数** | 1 | 2 |
| **数据连续性** | 始终可见 | 短暂消失 |
| **Dashboard 查询** | 平滑更新 | 闪烁（查到空值） |
| **下游 EXIST 检查** | 始终 true | 可能 false |
| **Redis QPS** | N | 2N |

如果你的业务有一个服务在循环 `GET order:1`，方案 A 永远拿到有效值，方案 B 有概率拿到 `nil`。在高并发场景下这不是小概率事件。


##### Appendix B: CDC Net Changes 合并规则

###### 目标

将同一个 `row_id` 下的多条 CDC 变更合并为**最小等价的 net changes**，使下游消费者应用 net changes 后得到的最终状态，与应用全部原始变更后的状态一致。

###### 首末变更类型判定

变更类型：`0` = INSERT，`1` = DELETE，`2` = UPDATE_BEFORE，`3` = UPDATE_AFTER。配对的 UPDATE_BEFORE 和 UPDATE_AFTER 共享相同的 `row_version`。

对每个 `row_id`，根据 `row_version` 确定两个关键属性：

- **first_type**：最小 `row_version` 处的变更类型，取该版本下 `MIN(change_type)`。
  - `0`（INSERT）：该行在本批次内新建。
  - `1`（DELETE）：该行在本批次内被删除（仅当只有单条变更时出现）。
  - `2`（UPDATE_BEFORE）：该行在本批次之前已存在，首次变更为更新。

- **last_type**：最大 `row_version` 处的变更类型，取该版本下 `MAX(change_type)`。
  - `0`（INSERT）：该行在本批次内新建（仅当只有单条变更时出现）。
  - `1`（DELETE）：该行在本批次结束时已被删除。
  - `3`（UPDATE_AFTER）：该行在本批次结束时仍存在，持有更新后的值。

> 当只有单条变更时，first_type 和 last_type 相同，只可能为 INSERT (0) 或 DELETE (1)。

###### 合并规则

**规则 1：仅有单条变更**

若某个 `row_id` 下只有一条变更记录（只可能是 INSERT 或 DELETE），原样输出，无需合并。

**规则 2–5：多条变更合并**

若某个 `row_id` 下有多条变更记录，根据 first_type 和 last_type 的组合进行合并：

| # | first_type | last_type | 输出 | 语义说明 |
|---|-----------|-----------|------|---------|
| 2 | INSERT (0) | UPDATE_AFTER (3) | 1 条：`(row_id, max_ver, INSERT, max_after_val)` | 新建后被更新，净效果等价于以最终值直接插入 |
| 3 | INSERT (0) | DELETE (1) | 0 条 | 新建后被删除，变更相互抵消 |
| 4 | UPDATE_BEFORE (2) | UPDATE_AFTER (3) | 2 条：`(row_id, max_ver, UPDATE_BEFORE, min_before_val)` + `(row_id, max_ver, UPDATE_AFTER, max_after_val)` | 经历一次或多次更新，净效果等价于从原始值直接更新到最终值 |
| 5 | UPDATE_BEFORE (2) | DELETE (1) | 1 条：`(row_id, max_ver, DELETE, min_before_val)` | 先被更新后被删除，净效果等价于直接删除，val 携带原始值 |

> **版本对齐**：规则 4 和 5 中，输出记录的 `row_version` 统一使用 `max_ver`，确保配对的 UPDATE_BEFORE / UPDATE_AFTER 具有相同版本，符合标准 CDC 语义。

###### 示例

可以把 CHANGES 看做如下的明细表

```sql
CREATE TABLE changes (
    row_id INT,
    row_version INT,
    change_type INT,
    val INT
) DUPLICATE KEY(row_id)
DISTRIBUTED BY HASH(row_id) BUCKETS 5;
```

| 列名 | 说明 |
|------|------|
| `row_id` | 逻辑行标识。INSERT 时分配，后续 UPDATE 继承，DELETE 后不再复用。 |
| `row_version` | 产生该变更的版本号（DML 序列号），每个 DML 对应一个版本。 |
| `change_type` | 变更类型 |
| `val` | 变更携带的数据。 |

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

Net changes 输出：

```
row_id | row_version | change_type | val  | 命中规则
-------|-------------|-------------|------|----------
     1 |           1 |     0 (INS) |  10  | #1 单条 INSERT
     2 |           3 |     0 (INS) |  20  | #2 <INS, AFT> → 以最终值插入
                                           |    （row_id=3 命中规则 #3，抵消无输出）
     4 |           4 |     2 (BEF) |  10  | #4 <BEF, AFT> → 从原始值更新到最终值
     4 |           4 |     3 (AFT) |  30  | #4
     5 |           5 |     1 (DEL) |  10  | #5 <BEF, DEL> → 携带原始值删除
     6 |           3 |     1 (DEL) |  50  | #1 单条 DELETE
```

###### SQL 实现

以下提供两种 SQL 实现方案，均利用表按 `row_id` 分桶的特性避免全局 shuffle。

- 方案一：窗口函数

使用两层 CTE，通过窗口函数在每个 `row_id` 分区内完成分类和过滤。

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
    -- 规则 1：单条变更，原样输出
    cnt = 1
    -- 规则 2：<INSERT, UPDATE_AFTER> → 输出 max_ver 的 after，type 改为 INSERT
    OR (first_type = 0 AND last_type = 3
        AND row_version = max_ver AND change_type = 3)
    -- 规则 3：<INSERT, DELETE> → 不输出
    -- 规则 4：<UPDATE_BEFORE, UPDATE_AFTER> → 输出 min_ver 的 before + max_ver 的 after
    OR (first_type = 2 AND last_type = 3
        AND row_version = min_ver AND change_type = 2)
    OR (first_type = 2 AND last_type = 3
        AND row_version = max_ver AND change_type = 3)
    -- 规则 5：<UPDATE_BEFORE, DELETE> → 输出 min_ver 的 before，type 改为 DELETE
    OR (first_type = 2 AND last_type = 1
        AND row_version = min_ver AND change_type = 2)
;
```

**特点**：无需 JOIN，全部在窗口函数内完成。所有 `PARTITION BY row_id` 与分桶键一致，可本地执行。

- 方案二：聚合 + JOIN

先通过 `GROUP BY` 聚合得到每个 `row_id` 的分类信息，再 JOIN 回原表选取目标记录。

`first_type` 和 `last_type` 的计算利用编码技巧：将 `(row_version, change_type)` 编码为单个整数 `row_version * 4 + change_type`（change_type 取值为 0–3，因此乘 4 可保证无歧义）。对编码值取 `MIN` 即得首条变更的 `(min_ver, first_type)`，取 `MAX` 即得末条变更的 `(max_ver, last_type)`。

```sql
WITH stats AS (
    SELECT
        row_id,
        MIN(row_version) AS min_ver,
        MAX(row_version) AS max_ver,
        COUNT(*)         AS cnt,
        MIN(CAST(row_version AS BIGINT) * 4 + change_type) % 4 AS first_type,
        MAX(CAST(row_version AS BIGINT) * 4 + change_type) % 4 AS last_type
    FROM changes
    GROUP BY row_id
)
SELECT
    c.row_id,
    CASE WHEN s.cnt > 1 THEN s.max_ver ELSE c.row_version END AS row_version,
    CASE
        WHEN s.first_type = 0 AND s.last_type = 3 THEN 0
        WHEN s.first_type = 2 AND s.last_type = 1 THEN 1
        ELSE c.change_type
    END AS change_type,
    c.val
FROM stats s
JOIN changes c ON s.row_id = c.row_id
WHERE
    s.cnt = 1
    OR (s.first_type = 0 AND s.last_type = 3
        AND c.row_version = s.max_ver AND c.change_type = 3)
    OR (s.first_type = 2 AND s.last_type = 3
        AND c.row_version = s.min_ver AND c.change_type = 2)
    OR (s.first_type = 2 AND s.last_type = 3
        AND c.row_version = s.max_ver AND c.change_type = 3)
    OR (s.first_type = 2 AND s.last_type = 1
        AND c.row_version = s.min_ver AND c.change_type = 2)
;
```

**特点**：只需一个 CTE + 一次 JOIN。`GROUP BY row_id` 和 `JOIN ON row_id` 均与分桶键一致，可分别利用本地聚合和 bucket join 避免全局 shuffle。

- 两种方案对比

| | 方案一：窗口函数 | 方案二：聚合 + JOIN |
|---|---|---|
| 表扫描次数 | 1 次 | 2 次（聚合 + JOIN 各一次） |
| 是否需要 JOIN | 否 | 是（bucket join） |
| 中间数据量 | 窗口函数附加列传播到每行 | 聚合结果只有 `row_id` 粒度，较小 |
| 可读性 | 窗口函数嵌套较深 | 聚合逻辑与选取逻辑分离，较清晰 |
| shuffle 风险 | 低（仅 PARTITION BY） | 低（GROUP BY + JOIN 均匹配分桶键） |