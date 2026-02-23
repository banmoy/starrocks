1. 背景与问题定义
Incremental View Maintenance (IVM)
- 基表：R（这里以一个基表举例，实际为 n 个基表 $$R_1, R_2, ..., R_n$$）
- 物化视图：MV（基于上述基表）
- 时间点：R 已更新到 v10，而 MV 仍停留在基于$$R_{v1}$$的结果（MV@v1）
- 目标：利用可用状态（$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，以及可选的算子中间状态），只计算 MV 需要更新的部分 ΔMV，并把变更 ΔMV 应用到 MV 表，使其到达 MV@v10。


2. 目标
总目标
相对基表的刷新延迟（从基表变更提交成功到 MV 对外可见对应版本为止）P99 ≤ 1s。

详细目标
1. 正确性：增量刷新结果与 full refresh 等价。
2. 性能：在下面条件满足的前提下，实现刷新延迟 P99≤1s。
  1. 基表在 1s 内完成了导入任务，即写入的数据量在 1s 内完成。
  2. 集群负载压力较小，即有足够的资源完成本次 MV 刷新。
3. 可运维：具备清晰的作业可观测性、可诊断性、可回退机制。
4. 可演进：从“易落地、覆盖面广”的方案起步，逐步增强到支持更多算子/更高性能。

3. 三种实现路线的共同需求
3.1 Table 多版本读取

对于 IVM 依赖的每个 table，需要能够读取 [v1, v10] 任意版本的数据（metadata + data）。其中：
- v1 (base version)：MV 当前可见版本所基于的输入 table 版本。
- v10 (head version)：依赖 table 的当前可见的最新表版本。

3.2 Changes (Delta)
给定 v1 (base version) 和 v10 (head version)，能够读取 table 在 [v1, v10] 之间发生变化的行 changes，经过 IVM 计算后得到 ΔMV，并将 ΔMV 消费到 MV table 中。

格式
产生与消费 Changes 行的格式：(columns..., ROW_ID, ACTION)。
它包含了额外的两列 ROW_ID, ACTION：为了支持基表与 MV 表的回撤删除 (retractable)。
- ACTION
  - ACTION ∈ {INSERT, DELETE}。
  - UPDATE 拆分为：(old_row, DELETE) + (new_row, UPDATE)。
- ROW_ID
  - ROW_ID 相关内容详见下面的《3.3 ROW_ID》。

产生
给定 v1 (base version) 和 v10 (head version)，返回 [v1, v10] 期间发生的 changes 行。
暂时不支持明细表的删除操作：因为明细表产生带有 DELETE 的 changes 的性能非常差，在实时刷新场景也并不常见。
- 后续也可以支持明细表的删除操作，因为产生带有删除操作的 changes 也并不复杂，只是产生 changes 时会扫描全表、再应用 rowset 中的谓词。但是 IVM 计算量相比 full refresh 会少很多。

消费
将 IVM 计算得到的 MV 的 changes ΔMV 应用到 MV table 中。
- 对一个 table 的同一个事务中，同时有 DELETE 和 INSERT。
- 接口的形式？MERGE INTO 或者 Apply Changes
  - 如果是 MERGE INTO，需要支持复杂的匹配条件，而非简单的是否与主键 matched。所以，本质上底层还是需要一个 Apply Changes 接口，然后约定其中一列是 ACTION。
  - 目前 PK 表已经支持了：导入时可以指定 op 列。
- 如何根据 ROW_ID 快速找到 MV table 中需要更新的那一行？
  - 具体见下面的《3.4 MV 表模型》

3.3 ROW_ID
为什么需要 ROW_ID
基表更新一行后，会影响目标 MV 表中的若干行。所以，需要一种机制，能够根据基表的这一行，识别出来目标 MV 表中需要更新的是哪些行。
也就是一种 identity 的机制，对于基表的一行的 identity，经过整个 MV 计算后，能够确定性地得到 MV table 中对应行的 identity。这个 identity 我们在这里称为 ROW_ID (逻辑上的 ROW_ID)。

约束要求
- 基表输出的 ROW_ID：表级别全局唯一。
- 算子输出的 ROW_ID：
  - 输入 ROW_ID 不同，输出 ROW_ID 不同；
  - 血缘稳定、可重放。


ROW_ID 生成规则
基表输出 ROW_ID
- 主键表：直接使用 Primary Key 作为 ROW_ID。
  - 【TODO】是否使用相同的 ROW_ID 格式
- 明细表：需要一个机制，能产生全局唯一的 ROW_ID。StarRocks 全局 Row ID 设计方案

算子输出 ROW_ID
- Linear Operator：output ROW_ID = input ROW_ID
  - Linear Operator$$f$$满足 $$\Delta f(R) = f(\Delta R)$$。
推导：$$f(R_{v1} + \Delta R) = f(R_{v1}) + f(\Delta R)$$，即 $$\Delta f(R) = f(R_{v1}+\Delta R) - f(R_{v1}) = f(\Delta R)$$。
- Group-by Aggregate：Hash(group_by_keys)
- Window Function：output ROW_ID = input ROW_ID
- UNION ALL：Hash(input ROW_ID, child_index)，child_index 指第 0/1/2... 个 child。
- Join：Hash(R1.ROW_ID, R2.ROW_ID, …, Rn.ROW_ID)
  - Hash(Hash(r1.row_id, r2.row_id), r3.row_id)


【新增】注意，上述的 Hash 不是必须的。
- 我们可以设置一个策略的，当 ROW_ID 的列数、bytes 超过阈值时，再使用 Hash 来降低 ROW_ID 的长度。

对用户不暴露。

Hash 碰撞策略
因为算子计算 ROW_ID 使用了 Hash 方法，理论上是会有碰撞的。提供两档策略：
- 默认不检测碰撞
  - 使用 >=160-bit 的 ROW_ID，碰撞概率极低。$$p \approx 3.42\times 10^{-19}$$，概率小到远低于硬件位翻转、介质不可恢复读错等更常见的风险（概率约为 $$10^{-16}$$）。
- 可选检测策略：消费 Changes 时检测并 fallback
  - 检测策略
    - INSERT：要求 MV 中不存在相同 ROW_ID
    - DELETE：要求 MV 中存在相同 ROW_ID
  - 发现碰撞：增量刷新失败 → fallback 到全量刷新。
  - 谁来检测：Query 层来检测，在 Plan 中应用 changes 前与 MV 做 join。
  - 【问题】存储层的 merge 逻辑 tricky，需要测试。
  - 【问题】中间算子有冲突。
- 配置开关（session variable）：默认关闭检测，线上可按需开启。

ROW_ID 格式
- 对于 table
  - 明细表：详见 StarRocks 全局 Row ID 设计方案
  - 主键表：直接使用 Primary Key。
- 对于算子
  - 固定长度的 binary，例如 160-bit。

3.4 MV 表模型
目前 MV 的表模型默认是明细表而非主键模型，因为除了 Root 是 Group-by Aggregate 以外的场景，无法定义主键。
对于 IVM，问题在于如何根据 changes ΔMV 中的 ROW_ID 快速找到要删除的行，并把它标记为删除？

选择一：把 MV 改为主键模型（ROW_ID 作为主键）✅
- 实现
  - 对于 Root 是 Group-by Aggregate，并且 group-by keys 能作为主键：使用 group-by keys 作为主键。
  - 其余情况：使用 ROW_ID 作为主键。
- 优点：DELETE/UPDATE 定位简单，apply changes 友好。
- 问题：用户不能自己随意更改 MV 的分区/分桶。
  - 【TODO】主键包含 ROW_ID，但是可以有其他主键列。
    - 分区分桶
      - 分桶键需要包含 ROW_ID。
      - 分区键可以不包含 ROW_ID，而是使用其他主键。
    - Range

PK 不支持 Binary，需要改成 Varchar。


选择二：沿用明细表，增加 predicate-rowset 软删除
- 实现
  - 每次 refresh 在所有 tablet 增加一个 predicate rowset，predicate 为 ROW_ID in (…)。
  - 对 ROW_ID 建 bitmap index 加速过滤。
- 问题
  - 因为 MV 依然还是明细表，MV 无法再生成 Delta，无法使用 MV on MV。
  - 性能也无法保证。

3.5 Plan 固化与确定性约束
避免因为两次刷新的 MV plan 不同，导致 ROW_ID 结果不确定，需要对 physical plan 进行固化。

【TODO】预期路线一不需要固化。
plan manager

【问题】固化下来的plan如果随着数据变化不再是最优了，要怎么办？



3.6 高频导入压力
基表与 MV 的秒级更新，会对导入带来压力
- 事务压力：短时间内大量事务提交。
- BE 存储压力：每次导入会产生一个 version/rowset。

3.7 可观测性
每次 refresh 作业输出：
- 输入版本范围：R@v_from → R@v_to
- 输出版本：MV@v_to
- plan、latency、profile、执行的资源消耗信息
- fallback 原因：cost 超阈值、hash collision、非支持算子、执行失败等

4. 三条实现路线
接下来讨论的三条实现路线针对的是 Incremental View Maintenance 部分。即使用可用状态（$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，以及可选的算子中间状态），如何计算得到 ΔMV。


根据状态维护方式、算子计算增量方式，可以分为如下三种路线。
1. 状态维护方式
  1. Non-stateful： 只使用$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$。
  2. Stateful：除了 $$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，中间算子可以存储和使用 state。
2. 计算增量方式
  1. SQL 改写：在 BE 不实现专门的增量算子，将增量算子逻辑转化为普通算子可以表示的形式。例如 $$\Delta(R \Join S) = \Delta R \Join S + R' \Join \Delta S$$，将一个 hash join 转为两个 hash join。
  2. 增量算子：在 BE 实现专门的增量算子，输入 $$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，输出 delta。
状态维护 \ 增量方式
2.1 SQL 改写
2.2 增量算子
1.1 Non-stateful

路线一：Non-stateful SQL Base

-
通常没有这种实现方式

1.2 Stateful
路线二：Stateful SQL Base

路线三：Stateful Operator


4.1 路线一：Non-stateful SQL Base
把增量计算表达为普通算子组合，并且输入只使用$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$。

增量算子表示方法
表示为普通算子
将增量算子转化为多个普通算子组合的形式。
- 线性算子：不需要处理，增量算子等于普通算子。
  - $$f$$满足 $$\Delta f(R) = f(\Delta R)$$。
推导：$$f(R_{v1} + \Delta R) = f(R_{v1}) + f(\Delta R)$$，即 $$\Delta f(R) = f(R_{v1}+\Delta R) - f(R_{v1}) = f(\Delta R)$$。
- Join 算子：$$\Delta R \Join S + R \Join \Delta S + \Delta R \Join \Delta S$$以及变体。
- 其余算子：$$\Delta f(R) = -f(R_{v1}⋉_{affected\_keys}\Delta R) + f(R_{v10}⋉_{affected\_keys}\Delta R)$$。
  - 在旧版 R 与新版 R 上读取受影响的 key (affected_key) 的所有行，重新计算算子$$f$$的结果，减掉旧结果，加上新结果。
  - affected_key 指每种算子用于数据分区的 key，例如 aggregation 的 group by，window function 的 partition by、except 的所有列、intersect 的所有列。

举两个例子，对于下面的 Aggregation MV 示例，转化为的普通算子组合如下图所示。
SELECT k, count(1) as cnt FROM R GROUP BY k;


图中的 +R@v10、-R@v1、ΔR 是指：
- +R@v10：把 R@v10 的所有行都读出来，每一行增加一列 action=+1。
- -R@v1：把 R@v1 的所有行都读出来，每一行增加一列 action=-1。
- ΔR：R@v1 到 R@v10 的 changes，带有额外的两列 ROW_ID 和 action。

【新增】对于下面的 Inner Join MV 示例，转化为的普通算子组合如下图所示。
SELECT r_c1, r_c2, s_c1, s_c2
FROM R inner join S
ON r_c1 = s_c1;

$$\Delta(R \Join S)  \\ \ \ \ \ = \Delta R \Join S_1 + R_1 \Join \Delta S + \Delta R \Join \Delta S
\\ \ \ \ \ = \Delta R \Join S_1 + R_{10} \Join \Delta S $$



Root 算子的优化
此外，如果一个算子是 root 算子，那么它还可以使用 MV table 本身作为额外的输入。所以，对于 [IVM] 增量算子 Delta 推导中增量算子的 state 与算子输出格式相同（或者增加隐藏列）的增量算子，也可以支持或性能更好的表示形式。包括：
1. Non-group by aggregation：可以支持。
2. Group by aggregation：Linear 和 Decomposable 的聚合函数可以有性能更好的表示形式。
3. 其余场景：对于不能使用 1 和 2 的场景，也可以做一个优化：不去读 v1 重新经过该算子计算，而是直接读 MV@v1。【新增】

例如，上面的例子可以表示为如下形式：
#2 Linear Group by aggregation
COUNT(1) GROUP BY k


#3 其余场景【新增】
MIN(c) GROUP BY k



Append-only 时的优化
如果确认本次刷新所有基表都没有删除（包括 update 也不行），那么可以对算子表示形式进行简化。包括：
- Aggregation
  - Linear 与 Decomposable Aggregation：直接更新 PK Table 就可以了。
  - MIN/MAX Aggregation：也是 linear 的了。
例如，上面的例子可以表示为如下形式：



算子支持情况
支持的算子
- 线性算子
  - 标量表达式
  - Filter
  - Lateral 行转列
  - UNION ALL
- 带有 on 等值谓词的 Join
- 有 Group By 的 Aggregate
- Non-group by aggregation：只支持作为 root 算子的情况。
- 有 Partition By 的 Window Function 
- 集合算子
  - UNION ALL (线性算子)
  - INTERSECT
  - EXCEPT
- 特殊的算子
  - current_date、current_timestamp

不支持的算子
- Non-Group By Aggregate：不支持不是 root 算子的情况。
- Non-Partition By Window Function
- TopN/Limit：建议 MV 定义时去掉，查询时再加上。
- 不带有 on 等值谓词的 Join
- 非确定性表达式：例如 random

Plan Rewrite 机制
Plan Rewrite
- 在根节点插入 Delta 算子（逻辑上引入 Δ）。
- 一组 physical rewrite rules 将 Δ 逐步下推到 Scan
- 在 Scan 处落到：
  - 读取 ΔR（Changes）
  - 必要时读取 R@v1、R@v10 的受影响 key 子集。

举个例子，对于下面的 MV，经过下图的步骤进行改写。
-- MV
SELECT r_c1, r_c2, s_c1_sum, s_c2
FROM R INNER JOIN (
    SELECT MAX(s_c1) s_c1_max, s_c2 FROM S GROUP BY s_c2
) ON r_c1 = s_c1_sum;
Delta_R join S + R@v10 join Delta_S
Step 1



Step 2


Step 3


Step 4



Incremental vs. full refresh 策略
增量不一定是最优的，需要一个策略来决定使用增量刷新还是全量刷新。
- 做 plan cost 对比：full refresh plan vs incremental plan。
- 启发式阈值：例如 incremental 读取数据量估计不超过 10%。

实现难度
实现难度适中。主要改动在于 FE 的 planner 中，对 plan 改写、固化、决策 Incremental vs. full refresh。

优缺点
优点
- 实现难度适中。
- 对于基本的 SPJG (SELECT-PROJECT-JOIN-GROUP-BY) 场景基本可以支持。

缺点
- 对于复杂 plan，基表可能读取大量数据。
下面是一个具体的例子，因为 join key 不包含 group by key，所以当更新了 R 产生了 ΔR 后，需要计算 ΔR join agg(S)agg(S) 的存在导致 runtime filter  无法应用到 scan(S) 上，需要要读全量数据。
- 对于低基数 key，基表可能读取大量数据。
  因为需要在新版与旧版基表上读取 Delta 涉及的 group by key/partition by key 的全量数据。
对于上述两种 case，可以建议用户手动将一个 MV 拆分为多个 MV，即用 table 保存中间算子的状态。

-- MV
SELECT r_c1, r_c2, s_c1_sum, s_c2
FROM R INNER JOIN (
    SELECT MAX(s_c1) s_c1_max, s_c2 FROM S GROUP BY s_c2
) ON r_c1 = s_c1_max;



4.2 路线二：Stateful SQL Base
在路线 1 的基础上：把增量计算表达为普通算子组合，并且输入使用$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$；
增加：中间算子可以存储和使用 state，state 用普通 table 来表示。（本质上，是对路线一种建议用户手动拆分为多个 MV 的自动化。）

增量算子表示方法
仍然使用“将增量算子转化为多个普通算子组合的形式”，但是允许某些算子维护 operator state，以减少反复重算范围。
- 输入为 $$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$：
  - 转化方式同路线一。
- 输入为$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，以及 operator state：
  - 详见 [IVM] 增量算子 Delta 推导。

Plan Rewrite 机制
在 stateful operator 处“拆分为多个 SQL”
原因
- 每个 stateful operator 既要更新自己的 state table，又要输出 Δ 给下游，只能断为两个 SQL。
如何做
- Plan Rewrite：与路线 1 类似，但会在选定 stateful 算子处切断 plan 为多个 sub-plan。
- 生成 DAG：每个子计划产出 Δ、更新 state table、供下游消费。

下面是一个具体的例子。
-- MV
SELECT r_c1, r_c2, s_c1_sum, s_c2
FROM R INNER JOIN (
    SELECT MAX(s_c1) s_c1_max, s_c2 FROM S GROUP BY s_c2
) ON r_c1 = s_c1_max;




选择哪些算子做 stateful 的策略
问题：stateful vs non-stateful 未必总是更快，选择哪些算子做 stateful？
针对路线一的两个短板场景，来做 stateful 算子。
- 对于复杂 plan，基表可能读取大量数据。
例如对于 (select sum(c1) as k from R gorup by k1) join S using(k)，因为 join key 不包含 group by key，所以当更新了 S 产生了 ΔS 后，需要计算 ΔS join agg(R)，agg(R) 没有办法把 runtime filter 应用到 scan(R) 上，需要要读全量数据。
- 对于低基数 key，基表可能读取大量数据。
  因为需要在新版与旧版基表上读取 Delta 涉及的 group by key/partition by key 的全量数据。

调度与 frontiers
调度
问题：拆分为多个 plan 后，如何调度？
方案：拆分后按依赖关系的拓扑序执行。

MV 内多表 frontiers（可见性边界）
将 MV 看做一个 pipeline：一个 MV 需要同时维护多个 state table 和 目标 MV table，并且拆分为了多个有依赖关系的子计划。
- frontiers：维护每个 state table / MV Table 的“最新可见版本”。
- 逐级刷新：每次在 MV 的 pipeline 中从已经刷新完成的 state table 开始执行。这样的好处：
  - 在下游子计划刷新失败后，不用回滚上游已经刷新的子计划。
  - 不需要支持多表事务。

实现难度
难度更大：在“路线一、Non-stateful SQL Base”的基础上，需要增加：
- 决策使用 stateful operator 的位置。
  - 新的 stateful operator 的增量算法推导。
- 维护和调度一个 MV 由多个子计划构成的 pipeline。

优缺点
优点
- 复杂 plan、低基数 key 场景性能显著优于路线一。
- 可以逐步支持路线 1 难支持的算子（通过引入 state 缓解重算）

缺点
- 一次刷新 MV 需要更新的 table 数量很多 → 更加依赖对高频导入的优化。
- 存储更高，因为需要存储  state table。

4.3 路线三：Stateful Operator（BE 原生增量算子 + 内部状态）
增量算子表示方法
直接使用 BE 原生实现的增量算子，可以依赖以下两种输入方式：
- 增量算子输入为 $$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$。
- 增量算子输入为$$R_{v1}, R_{v10}, \Delta R = R_{v10} - R_{v1}$$，以及 operator state。

Plan Rewrite 机制
与“二、Stateful SQL Base”类似。
虽然不需要拆分为多个 plan，但是依然需要决策哪些算子需要 stateful。

Operator State 机制（关键难点）
State 使用 operator 内部状态存储，而非普通 table。
- 持久化：文件存储格式追加式多版本 state
  - 带有 timestmap，按 key 排序的 LSM：（key, value, time, action）
- 一致性边界
  - frontier：frontier 之前时间点的 state 数据才可以被 compaction。
  - 推进 frontier：只有整个 MV 刷新成功才会更新 frontier。来处理这种情况：新的一批 Delta，前面算子计算 state 成功了，后面算子计算 state 失败。
- 容错
  - 需要多副本存储 state，或者存储在 remote 对象存储中，或者直接使用 iceberg 格式。
  - 以及副本均衡。

实现难度
三种路线中，难度最大，需要实现一套 operator state 的机制。

优缺点
优点
- 复杂算子（window/topk/semi/anti/outer）性能更好，因为路线二用“拆成复杂计划”表达，plan 复杂且需要重新读取 state table。
问题
- 相比路线二的复杂度陡升，收益是否足够大？

5. 三条路线对比

倾向的 POC 实现路径：从路线一迭代至路线二。
1. 先实现路线一 Non-stateful SQL Base：基于我们已有的 IVM 进行完善，支持内表，支持上述算子，支持基表的 Append-only → Retractable。
2. 再逐步引入路线二 Stateful SQL Base。

6. POC 计划
POC mock
- ACTION：先在 table 中增加一列 action (+1/-1) 来模拟 ACTION。
- ROW_ID：先使用自增列模拟明细表的 ROW_ID。
- 应用 Changes：因为 Incremental MV 改用主键表，所以直接在主键表上 update/insert，用 action=0 来表示删除。

POC 实现计划
1. 实现和测试路线一 Non-stateful SQL Base：2~3 周
  1. Plan 固化。
  2. Plan Rewrite
    1. Plan Rewrite 框架。
    2. 每个增量算子的实现方式。
    3. ROW_ID 传播、计算、冲突检测。
2. 实现和测试路线二 Stateful SQL Base：2~3 周
  1. Plan Rewrite
    1. Stateful 增量算子的实现方式。
    2. 选择哪些算子做 stateful 的策略。
    3. 拆分为多个 sub plan。
  2. 维护和调度一个 MV 的 pipeline。

7. 相关文档
1. [RFC] [2025-07-09] Incremental Materialized View Refresh
2. StarRocks 4.1 增量物化视图 (Incremental Materialized View）：最小化刷新代价
3. Incremental Materialized View Benchmark
4. Incremental Materialized View Maintenance 调研
5. IVM 算子实现和演进路径讨论
6. 增量物化视图：SQL 改写方式的完整算子覆盖与正确性证明
7. StarRocks 全局 Row ID 设计方案
8. IVM POC 实现路线思维导图
9. [IVM] 增量算子 Delta 推导
10. [IVM] Snowflake Dynamic Table Test
11. [IVM] Stateful SQL IVM 手写 SQL 测试
12. Support Time-travel && Branch && Tag support in Share-Data
13. RFC: MV Rolling Partition Refresh
