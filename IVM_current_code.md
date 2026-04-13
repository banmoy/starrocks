# IVM 当前基于 SQL 改写的代码逻辑（FE 为主）

本文基于现有代码（主要在 FE）梳理 IVM（Incremental View Materialization / Incremental View Maintenance）的实现逻辑，覆盖整体流程、模块划分、关键数据结构与详细逻辑。重点描述“基于 SQL 改写 + TVR（Time Varying Relation）”的刷新链路。

---

## 1. 总体流程（从创建到刷新执行）

1) **创建/修改 MV 时的 SQL 改写与能力判定**
- 入口：`MaterializedViewAnalyzer` 在解析 CREATE MV（或 ALTER MV）时，调用 `IVMAnalyzer` 对 MV 定义 SQL 进行“可增量”判定与改写。
- 结果：若改写成功，生成 IVM 专用 SQL（`ivmViewDef`），并将 MV 的 `currentRefreshMode` 设置为 `INCREMENTAL` 或保持 `AUTO`；若改写失败且 refreshMode 为 AUTO，则回退为 PCT（全量/分区刷新）。

2) **元数据落盘**
- `LocalMetastore` 把 `ivmViewDef` 存入 `MaterializedView.ivmDefineSql`，并持久化 `currentRefreshMode`、`encodeRowIdVersion`。

3) **触发刷新任务与调度**
- `MVRefreshProcessorFactory` 按 `MaterializedView.getCurrentRefreshMode()` 选择处理器：
  - `INCREMENTAL` -> `MVIVMBasedRefreshProcessor`
  - `AUTO` -> `MVHybridBasedRefreshProcessor`（先尝试 IVM，失败后切到 PCT）
  - `FULL/PCT` -> `MVPCTBasedRefreshProcessor`

4) **IVM 计划生成与执行**
- `MVIVMBasedRefreshProcessor` 计算基表“版本增量”（TVR delta），生成带 TVR 版本范围的 Insert-Select 计划。
- 执行时开启 `enable_ivm_refresh`，触发优化器 TVR 规则改写，最终在执行阶段仅处理增量数据。

5) **事务提交后版本元信息更新**
- `IVMInsertLoadTxnCallback` 在事务提交前/后把 `tempBaseTableInfoTvrDeltaMap` 合并到 `baseTableInfoTvrVersionRangeMap`，完成“增量版本推进”。

---

## 2. 模块划分与职责

### 2.1 SQL 分析与改写
**核心文件**
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/ast/CreateMaterializedViewStatement.java`

**职责**
- 判定 SQL 是否可用于 IVM（限制 join、aggregation、subquery、window 等）。
- 对聚合进行 **AggState** 改写，新增 `__ROW_ID__` 与 `__AGG_STATE_*` 列。
- 生成 IVM 专用 SQL（`ivmViewDef`）并写回 statement。
- 计算 `encodeRowIdVersion`，保证行 ID 编码在 MV 生命周期内稳定。

### 2.2 MV 元数据与持久化
**核心文件**
- `fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedView.java`
- `fe/fe-core/src/main/java/com/starrocks/server/LocalMetastore.java`

**职责**
- 保存 `ivmDefineSql`、`currentRefreshMode`、`encodeRowIdVersion`。
- `getMVQueryDefinedSql()` 在 IVM 有改写 SQL 时优先使用改写 SQL。
- `getIVMTaskDefinition()` 生成 IVM 插入语句 `INSERT INTO <mv> <ivmDefineSql>`。
- `AsyncRefreshContext` 存储 TVR 版本范围（增量进度），以及临时 delta map。

### 2.3 IVM 刷新执行器
**核心文件**
- `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/ivm/MVIVMBasedRefreshProcessor.java`
- `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/hybrid/MVHybridBasedRefreshProcessor.java`
- `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVRefreshProcessorFactory.java`

**职责**
- 计算基表变化版本范围（TVR delta），生成 insert plan。
- 生成增量计划并执行，更新刷新元数据。
- AUTO 模式下，失败回退到 PCT 刷新。

### 2.4 优化器 TVR 改写
**核心文件**
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/QueryOptimizer.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tvr/TvrTableScanRule.java`
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tvr/common/TvrOpUtils.java`

**职责**
- 在 `enable_ivm_refresh` 时开启 `RuleSet.TVR_REWRITE_RULES`。
- 将普通 table scan 转化为 “from-snapshot/to-snapshot” 的 TVR 双分支扫描。
- 构建 `ROW_ID` / `AGG_STATE` 相关表达式与比较谓词。

### 2.5 事务回调与增量版本推进
**核心文件**
- `fe/fe-core/src/main/java/com/starrocks/load/loadv2/IVMInsertLoadTxnCallback.java`
- `fe/fe-core/src/main/java/com/starrocks/load/loadv2/InsertLoadTxnCallbackFactory.java`

**职责**
- 在 IVM 刷新 insert 提交后，推进 MV 的基表版本快照记录。
- 清理 temp 版本 map，写 edit log。

---

## 3. SQL 改写详细逻辑（IVMAnalyzer）

**入口**：`IVMAnalyzer.rewrite(refreshMode)`

1) **刷新模式判定**
- 如果 refreshMode 不是 `INCREMENTAL` 或 `AUTO`，直接 `Optional.empty()`。
- `getRefreshMode()` 读取 `properties[mv_refresh_mode]`，若未指定，代码返回 `PCT`（注意注释写“Default to INCREMENTAL”，但实现返回 `PCT`）。

2) **语法结构限制**
- 仅支持 `SelectRelation` 或 `UnionRelation`（`UNION ALL` 且子查询必须是纯 select）。
- 不支持：`SubqueryRelation`、Window、Order By、带 Aggregate 的 Union 子查询。
- Join 限制：只允许 `INNER`、`CROSS`。
- 基表类型限制：仅支持 `ICEBERG`、`PAIMON`。

3) **Aggregate 改写**（核心）
- 若存在 aggregate：
  - 必须有 `GROUP BY`。
  - 将原 aggregate `f(x)` 替换为：
    - 计算阶段：`f_combine(x)` 作为中间聚合列。
    - 输出阶段：`f_state_merge(__AGG_STATE_*)` 作为原输出表达式的替代。
  - 对 always-non-nullable 函数，输出包裹 `CASE WHEN f_state_merge IS NULL THEN <default> ELSE f_state_merge END`。
- 代码位置：
  - `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java:276`（`checkAggregate`）
  - `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java:352`（`buildIntermediateAggregateFunc`）
  - `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java:380`（`buildStateMergeFuncExpr`）
- 中间状态类型来源：
  - 一个聚合函数的中间状态类型由 `AggregateFunction::intermediateType` 确定，每个聚合函数的具体中间状态类型详见
    `FunctionSet::initAggregateBuiltins`。
- state 相关函数说明：
  ```sql
  avg(c1)

  -- scalar function: translate c1 into `avg`'s intermediate state
  avg_state(c1)
  -- scalar function: merge two intermediate states into an intermediate result
  avg_state_union(s1, s2)
  -- scalar function: merge two intermediate states into final result
  avg_state_merge(s1, s2)

  -- aggregate function: translate args into intermediate state and union the intermedate state into the final intermediate state
  avg_combine(c1)
  ```
- 示例（`MIN(x)`）：
  - MV 定义 SQL：
    ```sql
    CREATE MATERIALIZED VIEW mv AS
    SELECT k, MIN(x) FROM t GROUP BY k;
    ```
  - MV Table 定义 SQL（逻辑结构示意）：
    ```sql
    CREATE TABLE mv (
      __ROW_ID__ VARCHAR,
      k <type_of_k>,
      min_x <result_type_of_min_x>,
      __AGG_STATE_min_x <agg_state_type>
    ) PRIMARY KEY(__ROW_ID__);
    ```
  - MV 刷新 SQL（IVM 逻辑形态示意）：
    ```sql
    INSERT INTO `mv`
    SELECT
      d.__ROW_ID__,
      d.k,
      min_state_merge(min_state_union(d.__AGG_STATE_min_x, s.__AGG_STATE_min_x)) AS min_x,
      min_state_union(d.__AGG_STATE_min_x, s.__AGG_STATE_min_x) AS __AGG_STATE_min_x
    FROM (
      SELECT
        FROM_BINARY(ENCODE_ROW_ID(k), 'encode64') AS __ROW_ID__,
        k,
        min_combine(x) AS __AGG_STATE_min_x
      FROM t /* TVR delta */
      GROUP BY k
    ) d
    LEFT JOIN mv s
    ON d.__ROW_ID__ = s.__ROW_ID__;
    ```

4) **新增列**
- 在 SELECT 列表最前加入 `__ROW_ID__`：
  - 由 `TvrOpUtils.buildRowIdFuncExpr(encodeRowIdVersion, groupByExprs)` 生成。
- 在 SELECT 列表末尾加入每个聚合的 `__AGG_STATE_*` 列。
- 输出表达式列表同步替换（Substitution）。

5) **可撤回 Sink 判定**
- 一旦有聚合改写，`markRetractableSink()` 置位，结果中 `needRetractableSink=true`。
- 该标记用于后续 `MaterializedViewAnalyzer` 中将 `keysType` 强制设为 `PRIMARY_KEYS`。

6) **编码行 ID 版本**
- `encodeRowIdVersion` 通过 `TvrOpUtils.deduceEncodeRowIdVersion()` 根据 group by 类型/长度推导。
- 写入 `CreateMaterializedViewStatement`，最终固化到 `MaterializedView.encodeRowIdVersion`。

---

## 4. MV 创建/修改路径的 IVM 逻辑

### 4.1 CREATE MV 分析
**文件**：`MaterializedViewAnalyzer`

关键逻辑：
- 对原 SQL 先做 `Analyzer.analyze()` 和 nondeterministic 函数检查。
- 设置：
  - `inlineViewDef`（规范 SQL）、`simpleViewDef`、`originalViewDefineSql`。
- 如果 refreshMode 为 `INCREMENTAL` 或 `AUTO`：
  - 调用 `IVMAnalyzer.rewrite()`。
  - 若改写成功：
    - 重新分析 rewritten SQL；设置 `ivmViewDef`。
    - 若 `needRetractableSink=true`，将 `keysType` 改为 `PRIMARY_KEYS`。
    - `currentRefreshMode` 设置为 IVMAnalyzer 返回的 `currentRefreshMode`。
  - 若改写失败且模式为 AUTO：回退到 `PCT`。

### 4.2 ALTER MV 修改刷新模式
**文件**：`AlterMVJobExecutor`

关键逻辑：
- 将要改为 `INCREMENTAL`/`AUTO` 时，重新用 `IVMAnalyzer` 校验 SQL。
- 如果 MV 原本不是 IVM/AUTO，则拒绝变更（仅允许 IVM/AUTO 类型 MV 之间切换）。

### 4.3 元数据落盘
**文件**：`LocalMetastore`
- 把 `CreateMaterializedViewStatement.ivmViewDef` 写入 `MaterializedView.ivmDefineSql`。
- 写入 `currentRefreshMode`、`encodeRowIdVersion`。

---

## 5. IVM 刷新执行详细逻辑

### 5.1 刷新入口与处理器选择
**文件**：`MVRefreshProcessorFactory`
- 根据 `MaterializedView.currentRefreshMode` 选择 IVM / Hybrid / PCT 处理器。

### 5.1.1 异步刷新触发与任务调度
**文件**：`TaskBuilder`
- ASYNC 刷新模式下，如果指定了 interval，会生成周期任务（`PERIODICAL`）并设置 `TaskSchedule`：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/TaskBuilder.java:195`
  ```java
  IntervalLiteral intervalLiteral = asyncRefreshSchemeDesc.getIntervalLiteral();
  long period = ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getLongValue();
  TimeUnit timeUnit = TimeUtils.convertUnitIdentifierToTimeUnit(intervalLiteral.getUnitIdentifier().getDescription());
  TaskSchedule taskSchedule = new TaskSchedule(startTime, period, timeUnit);
  task.setSchedule(taskSchedule);
  task.setType(Constants.TaskType.PERIODICAL);
  ```
- 如果没有 interval，则使用 `EVENT_TRIGGERED`（事件触发型任务）：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/TaskBuilder.java:191`

### 5.1.2 刷新任务执行入口
**文件**：`MVTaskRunProcessor`
- 任务执行时创建 refresh processor，并驱动执行：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/MVTaskRunProcessor.java:148`
  ```java
  this.mvRefreshProcessor = MVRefreshProcessorFactory.INSTANCE.newProcessor(db, mv, mvTaskRunContext, mvMetricsEntity);
  ```
- 执行入口在 `processTaskRun`：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/MVTaskRunProcessor.java:185`

### 5.1.3 基表变更检测与跳过逻辑
**文件**：`MVIVMBasedRefreshProcessor` / `BaseMVRefreshProcessor`
- IVM 侧：若所有 base table 的 TVR delta 为空则跳过刷新：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/ivm/MVIVMBasedRefreshProcessor.java:117`
  ```java
  boolean isTaskRunSkipped = snapshotBaseTables.values().stream()
          .map(snapshotInfo -> (TvrTableSnapshotInfo) snapshotInfo)
          .map(TvrTableSnapshotInfo::getTvrSnapshot)
          .allMatch(TvrVersionRange::isEmpty);
  if (isTaskRunSkipped) {
      return new ProcessExecPlan(Constants.TaskRunState.SKIPPED, null, null);
  }
  ```
- PCT 侧：检查 base table partition 变化，若变化则重试同步：
  - `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/BaseMVRefreshProcessor.java:760`
  ```java
  if (checkPCTBaseTablePartitionChange()) {
      ...
      continue;
  }
  ```

### 5.2 IVM 刷新过程（MVIVMBasedRefreshProcessor）

**核心流程**：
1) **只支持 complete refresh**：否则报错。
2) **计算基表 delta**：
   - 读取 MV 的 `AsyncRefreshContext.baseTableInfoTvrVersionRangeMap`。
   - 通过 `getBaseTableChangedVersionRange()` 获取每个 base table 的增量 TVR delta。
   - 增量为空则跳过刷新。

3) **delta 计算细节**
- `getMaxBaseTableChangedDelta()`：
  - 获取当前表的 TVR snapshot（`MetadataMgr.getCurrentTvrSnapshot`）。
  - 如果 MV 尚无记录，则从 `MIN -> current`。
  - 如果无变化，返回 empty delta。
- `getBaseTableChangedVersionRange()`：
  - 校验基表类型（仅 ICEBERG/PAIMON）。
  - 校验 `TvrTableDeltaTrait` 是否 append-only；若非 append-only 且 refreshMode=INCREMENTAL，则抛异常；AUTO 模式会记录并后续可能切换 PCT。
  - `mv_max_rows_per_refresh/mv_max_bytes_per_refresh` 时，使用 `getBaseTableChangedDeltaAdaptive()` 拆分 delta，并可能生成下一次 task。

4) **生成 IVM Insert-Select 计划**
- 构造 `InsertStmt`：`mv.getIVMTaskDefinition()` => `INSERT INTO <mv> <ivmDefineSql>`。
- 在 buildInsertPlan 时，为所有 `TableRelation` 绑定对应 `TvrVersionRange`。
- 启用 session 变量：
  - `enableIVMRefresh = true`
  - `tvrTargetMvid = <mvId>`

5) **执行与元数据更新**
- 执行前把 `tempMvTvrVersionRangeMap` 写入 `AsyncRefreshContext.tempBaseTableInfoTvrDeltaMap`。
- 执行完成后调用 `updatePCTMeta()` 更新 PCT 元数据（用于 MV rewrite，非 IVM 核心逻辑）。

### 5.3 AUTO 模式（Hybrid）
**文件**：`MVHybridBasedRefreshProcessor`
- `AUTO` 先尝试 IVM；异常则切 PCT。
- 切换到 PCT 时，复用 IVM 的 delta 计算，把版本范围填入临时 map，并在最后一个 taskRun 更新到 `baseTableInfoTvrVersionRangeMap`。

---

## 6. 优化器 TVR 改写路径

### 6.1 TVR 规则触发
**文件**：`QueryOptimizer`
- 当 `SessionVariable.enableIVMRefresh = true` 时，应用 `RuleSet.TVR_REWRITE_RULES`。

### 6.2 TvrTableScanRule
**文件**：`TvrTableScanRule`

逻辑：
- 仅支持 IVM 允许的表类型。
- scan operator 必须带 `TvrTableDeltaTrait` 且 append-only。
- 将一个 scan 拆为 `fromSnapshot` 和 `toSnapshot` 两个版本扫描，并封装到 `TvrOptMeta` 中。

### 6.3 TvrOpUtils
**文件**：`TvrOpUtils`

功能：
- `__ROW_ID__` 编码：
  - `encode_sort_key` / `encode_fingerprint_sha256` 的选择由 `encodeRowIdVersion` 决定。
- 提供 `AGG_STATE` 相关函数名推导，以及 `state_union` 构造。
- `buildRowIdEqBinaryPredicateOp()` 等辅助方法用于 TVR 差异合并条件构建。

---

## 7. 事务回调与版本推进

### 7.1 回调注册
**文件**：`InsertLoadTxnCallbackFactory`
- 只有当 `enableIVMRefresh` 且目标表是 MV 时，返回 `IVMInsertLoadTxnCallback`。

### 7.2 IVMInsertLoadTxnCallback

关键流程：
- `beforeCommitted()`：
  - 读取 `AsyncRefreshContext.tempBaseTableInfoTvrDeltaMap`。
  - 合并为 `baseTableInfoTvrDeltaMap`（commit 目标）。
- `afterCommitted()`：
  - 写回 `AsyncRefreshContext.baseTableInfoTvrVersionRangeMap`。
  - 清理 temp map。
  - 记录 refresh scheme 更新日志。

---

## 8. 关键数据结构

### 8.1 IVM Analyzer 结果
- `IVMAnalyzeResult`：
  - `queryStatement`：改写后的 SQL AST。
  - `needRetractableSink`：是否需要可撤回 sink（当前主要由 aggregate 触发）。
  - `currentRefreshMode`：最终决定的刷新模式。

### 8.2 TVR 相关结构（`fe/fe-core/src/main/java/com/starrocks/common/tvr/*`）
- `TvrVersion`：表示版本号，支持 `MIN/MAX`。
- `TvrVersionRange`：`from/to` 版本范围抽象。
- `TvrTableDelta`：表示增量范围，含 `fromSnapshot` / `toSnapshot`。
- `TvrTableSnapshot`：单点快照（`from=MIN`，`to=snapshot`）。
- `TvrTableDeltaTrait`：描述 delta 的可撤回性与统计信息（append-only 关键）。
- `TvrDeltaStats`：增量的 rows / file size 统计，用于拆分 delta。

### 8.3 IVM Refresh 过程结构
- `TvrTableSnapshotInfo`：继承 `PCTTableSnapshotInfo`，额外保存 `TvrVersionRange`。
- `MaterializedView.AsyncRefreshContext`：
  - `baseTableInfoTvrVersionRangeMap`：已提交的基表版本范围。
  - `tempBaseTableInfoTvrDeltaMap`：一次刷新事务内的增量范围缓存。

### 8.4 Session 变量
- `SessionVariable.enableIVMRefresh`：控制是否启用 TVR 规则。

---

## 9. 重要限制与当前行为总结

- **仅支持 ICEBERG / PAIMON** 基表类型。
- **Join 仅支持 INNER / CROSS**。
- **不支持**：Window、Order By、子查询（带可撤回输入）、Union 非 ALL 或带聚合。
- 聚合必须有 `GROUP BY`。
- IVM 的 insert 语句为 `INSERT INTO`（非 overwrite）。
- AUTO 模式下如 IVM 失败会回退 PCT。

---

## 10. 代码入口索引（便于二次阅读）

- SQL 改写：`fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java`
- CREATE MV 分析：`fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java`
- MV 元数据落盘：`fe/fe-core/src/main/java/com/starrocks/server/LocalMetastore.java`
- IVM 刷新处理器：`fe/fe-core/src/main/java/com/starrocks/scheduler/mv/ivm/MVIVMBasedRefreshProcessor.java`
- AUTO 混合处理器：`fe/fe-core/src/main/java/com/starrocks/scheduler/mv/hybrid/MVHybridBasedRefreshProcessor.java`
- TVR 扫描改写：`fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tvr/TvrTableScanRule.java`
- TVR 表达式工具：`fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/tvr/common/TvrOpUtils.java`
- 事务回调：`fe/fe-core/src/main/java/com/starrocks/load/loadv2/IVMInsertLoadTxnCallback.java`
