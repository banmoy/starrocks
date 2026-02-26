## StarRocks Change Data Capture Proposal（Shared-Data / 存算分离）

### 1. 背景与用户价值

增量物化视图（IVM）刷新时需要查询历史版本、当前版本以及两个版本之间的变更，需要 StarRocks 内表具有捕获数据变更的能力，即 Change Data Capture（CDC）。除了 IVM，其它 CDC 典型应用场景如下
- 
- 

本文档初步探讨 StarRocks CDC 的产品和技术路径设计。

### 2. 产品调研

### 3. 目标

- 讨论 StarRocks CDC 产品能力，确保覆盖 IVM 场景需求，其它场景有哪些
- 讨论实现路径，短期要能支持 IVM 场景

### 3. 产品定义

CDC 允许捕获行级变更，并可以消费


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
| ROW_ID | BIGINT | 逻辑行标识，用于 row tracking，UPDATE 前后的变更具有相同的 row_id。前提是表支持并开启了 row_id 功能 |
| GTID (optional) | BIGINT | 生成变更的 global transaction id，全局唯一单调递增，可以对跨导入的相同 ROW （同一 ROW_ID）定序，如果不依赖 |

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

#### 3.2 管理

```
CREATE TABLE t (
    -- columns...
)
PROPERTIES ("history_retention" = "30 days");

-- 动态打开
ALTER TABLE t SET ('history_retention' = '30 days');

-- 动态关闭
ALTER TABLE t SET ('history_retention' = '0');
```

#### 3.3 消费

##### Net Changes

##### 3.3.1 “哪些变更”如何表达

两种方式
- **指定时间范围**：我要消费 t1 和 t2 之间的变更
- **执行**






#### 3.3 用户接口定义

#### 3.4 增量物化视图需求特点

 * UPDATE 可以表示成 DELETE + INSERT
 * DDL 可以低优支持，批处理适合全量刷新


### 4. 技术方案

#### 4.1 关键概念


#### 4.2 思路与挑战

##### 4.2.1 总体思路

##### 4.2.2 技术挑战

* 
* 聚合表需要保留导入的 rowset，不能 compaction，从而产出 insert
* global transaction id 
  * 存储层需要加一列来 inheritance global transaction id，否则重写或 compaction 会打乱
  * DDL 如果要支持 CHANGES，也需要分配一个 gtid

#### 4.3 可选方案

##### 4.3.1 方案一：面向 IVM 的最小实现

Net Changes，非严格的语义
- 文件对比，伪变更
- log-based，


##### 4.3.2 方案二：通用 Time Travel


#### 附录