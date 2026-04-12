# 存算分离 OlapTable Scan 下推机制

## 0. 问题与方案概述

### 问题

在存算分离（Shared-Data）架构下，数据存储在远端对象存储（如 S3、HDFS）上，计算节点通过网络读取数据。与本地磁盘相比，远端读取的延迟和带宽成本显著增大。如果一个查询需要扫描大量数据但实际只用到其中一小部分，未经优化的全量扫描会浪费大量网络 IO 和 CPU。

核心挑战：**如何在查询执行的尽早阶段，以尽低的代价排除无关数据，使最终传给上层算子的数据量最小化。**

### 方案

StarRocks 在查询规划（FE）和查询执行（BE）两个阶段实施多层下推：

- **FE 查询规划阶段**：在优化器中通过规则变换，将谓词、投影、聚合、Limit 等信息尽可能推入 Scan 节点。同时进行分区裁剪和分桶裁剪，减少需要扫描的 tablet 数量。生成 Runtime Filter 描述，关联到 probe 侧 Scan 节点。
- **FE → BE 序列化**：通过 Thrift 协议将下推信息（TLakeScanNode、TScanRangeLocations、TRuntimeFilterDescription 等）发送给 BE。
- **BE 查询执行阶段**：Lake TabletReader 在打开 segment 之前利用 segment 元数据过滤跳过整个 segment；SegmentIterator 依次使用 Short Key Index、Zone Map、Bitmap Index、Bloom Filter Index、GIN 倒排索引、向量索引等进行行级过滤；最后在 chunk 读取阶段进行谓词求值、Runtime Filter 过滤和 Late Materialization。

整个下推管线形成一个**漏斗**：每一层过滤掉一部分数据，越靠前的层代价越低、过滤越粗；越靠后的层代价越高、过滤越精确。

```
  ┌──────────────────────────────────────────────────┐
  │          全量数据（所有分区、所有 tablet）          │  ← 原始数据
  └──────────────────────────────────────────────────┘
        ↓ FE: 分区裁剪 + 分桶裁剪
  ┌────────────────────────────────────────┐
  │     选中的 tablet（大幅减少）           │
  └────────────────────────────────────────┘
        ↓ BE: Segment 元数据过滤
  ┌──────────────────────────────────┐
  │     选中的 segment                │
  └──────────────────────────────────┘
        ↓ BE: Short Key / Zone Map / Bitmap / Bloom Filter
  ┌────────────────────────────┐
  │   索引过滤后的行范围        │
  └────────────────────────────┘
        ↓ BE: 谓词求值 + Runtime Filter
  ┌──────────────────────┐
  │  谓词过滤后的行        │
  └──────────────────────┘
        ↓ BE: Late Materialization（仅读取所需列）
  ┌────────────────┐
  │  最终输出 chunk  │  ← 传给上层算子
  └────────────────┘
```

### 涉及的表类型

本文档仅涉及存算分离模式下的 OlapTable，包括：

| 表模型 | KeysType | 特性 |
|--------|----------|------|
| 明细表 | DUP_KEYS | 允许重复 key，无聚合 |
| 聚合表 | AGG_KEYS | 相同 key 的 value 列按聚合函数合并 |
| 主键表 | PRIMARY_KEYS | 按主键去重，支持 Delete Vector |

---

## 1. 概念模型

### 1.1 架构总览

```
┌─────────────────────────────── FE (Java) ────────────────────────────────┐
│                                                                           │
│  SQL Text                                                                 │
│    │                                                                      │
│    ▼                                                                      │
│  Parser / Analyzer                                                        │
│    │                                                                      │
│    ▼                                                                      │
│  Optimizer (CBO/RBO)                                                      │
│    ├─ PushDownPredicateScanRule     ── 谓词下推                           │
│    ├─ PartitionPruneRule            ── 分区裁剪                           │
│    ├─ OptDistributionPruner         ── 分桶裁剪                           │
│    ├─ LimitPruneTabletsRule         ── Limit 裁剪 tablet                  │
│    ├─ PruneScanColumnRule           ── 列裁剪                             │
│    ├─ PreAggregateTurnOnRule        ── 预聚合决策                         │
│    ├─ PushDownAggToMetaScanRule     ── 聚合下推到 MetaScan                │
│    ├─ PushDownLimitDirectRule       ── Limit 下推                         │
│    ├─ PushDownTopNBelowUnionRule    ── TopN 下推                          │
│    └─ RuntimeFilter 关联            ── 生成并关联到 probe Scan 节点        │
│    │                                                                      │
│    ▼                                                                      │
│  Physical Plan → OlapScanNode.toThrift()                                  │
│    │                                                                      │
│    ▼                                                                      │
│  TLakeScanNode + TScanRangeLocations + TRuntimeFilterDescription          │
│                                                                           │
└────────────────────────────── Thrift RPC ─────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────── BE (C++) ─────────────────────────────────┐
│                                                                           │
│  FragmentExecutor → Pipeline                                              │
│    │                                                                      │
│    ▼                                                                      │
│  OlapChunkSource                                                          │
│    ├─ _init_reader_params()   ── 谓词分类 (pushdown / non-pushdown)       │
│    └─ _init_olap_reader()     ── 创建 lake::TabletReader                  │
│         │                                                                 │
│         ▼                                                                 │
│  lake::TabletReader                                                       │
│    ├─ get_segment_iterators()                                             │
│    │    ├─ ZonemapPredicatesRewriter  ── 谓词重写为 zone map 可用形式     │
│    │    └─ Rowset::read()                                                 │
│    │         ├─ SegmentMetadataFilter::may_contain()  ── segment 元数据裁剪│
│    │         └─ load_segments() → SegmentIterator                         │
│    │                                                                      │
│    ▼                                                                      │
│  SegmentIterator::_init_internal()   ── 索引过滤阶段                      │
│    ├─ 1. _get_row_ranges_by_rowid_range()   ── Rowid 范围                 │
│    ├─ 2. _get_row_ranges_by_keys()          ── Short Key Index            │
│    ├─ 3. _apply_tablet_range()              ── Tablet 范围                 │
│    ├─ 4. [条件] _apply_del_vector()         ── Delete Vector (前置)       │
│    ├─ 5. _apply_bitmap_index()              ── Bitmap Index               │
│    ├─ 6. _get_row_ranges_by_zone_map()      ── Zone Map                   │
│    ├─ 7. _get_row_ranges_by_bloom_filter()  ── Bloom Filter Index         │
│    ├─ 8. _apply_inverted_index()            ── GIN 倒排索引               │
│    ├─ 9. [条件] _apply_del_vector()         ── Delete Vector (后置)       │
│    ├─10. _get_row_ranges_by_vector_index()  ── 向量索引                   │
│    └─11. _apply_data_sampling()             ── 数据采样                    │
│    │                                                                      │
│    ▼                                                                      │
│  SegmentIterator::_do_get_next()     ── Chunk 读取与谓词求值               │
│    ├─ _predicate_evaluate()                                               │
│    │    ├─ Non-Expression 谓词                                            │
│    │    ├─ Expression 谓词                                                │
│    │    └─ Runtime Filter 求值                                            │
│    ├─ Delete Predicate 过滤                                               │
│    └─ Late Materialization (延迟物化非谓词列)                              │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### 1.2 术语定义

| 术语 | 定义 |
|------|------|
| Partition | OlapTable 的一级数据划分，按分区键（通常为时间列）将数据分成多个独立的存储单元 |
| Tablet | Partition 内的二级数据划分，按分桶键（Hash/Random）将数据分成多个 tablet，是 BE 调度和并行扫描的基本单位 |
| Rowset | Tablet 内的一次数据导入产生的数据集合，由一个或多个 Segment 文件组成 |
| Segment | Rowset 中的物理文件，内部按列存储，包含数据页、索引页和统计信息 |
| Page | Segment 内列数据的物理存储单位，每个 Page 包含固定数量的行和对应的 Zone Map 统计 |
| Zone Map | 每个 Page / Segment 记录的列最小值和最大值，用于快速判断谓词是否可能命中 |
| Short Key Index | 基于排序键前 N 列的前缀索引，用于快速定位 key 范围对应的行区间 |
| Bitmap Index | 为列的每个不同值建立 Roaring Bitmap 索引，适合低基数列的等值/IN 过滤 |
| Bloom Filter Index | 概率型数据结构，可以确定一个值"一定不在"某个 Page 中 |
| GIN (Generalized Inverted Index) | 倒排索引，支持文本列的子串和短语匹配 |
| Delete Vector | 主键表特有，标记已删除行的 Roaring Bitmap，通过 `LakeDelvecLoader` 从元数据服务异步加载 |
| Delete Predicate | 通过 DELETE 语句产生的删除条件，存储在 Rowset 元数据中，扫描时过滤匹配行 |
| Runtime Filter | 查询执行时由 Join 的 build 侧动态构建的过滤器（Bloom Filter / Min-Max / IN），推送给 probe 侧 Scan 节点 |
| TopN Filter | 特殊的 Runtime Filter，由 ORDER BY + LIMIT 动态构建，按排序键过滤 Scan 节点 |
| Late Materialization | 先只读取谓词列进行过滤，确定需要的行之后再读取其余列，减少不必要的列数据读取 |
| PredicateTree | BE 中谓词的树形表示，支持 AND/OR 组合，叶节点为 `ColumnPredicate` |
| SparseRange | BE 中表示行号集合的数据结构，索引过滤的结果用它表示，各层过滤通过交集操作逐步缩小范围 |
| TLakeScanNode | 存算分离 OlapTable 的 Thrift Scan 节点定义，区别于存算一体的 TOlapScanNode |
| SegmentMetadataFilter | 存算分离特有的优化：在加载 segment footer 之前，利用 segment 级别的 sort_key_min/max 统计信息跳过整个 segment |

### 1.3 关键协调标志生命周期

以下标志在 FE 设置、通过 Thrift 传递、在 BE 消费：

| 标志 | FE 设置方 | Thrift 字段 | BE 消费方 | 作用 |
|------|----------|-------------|----------|------|
| isPreAggregation | `PreAggregateTurnOnRule` | `TLakeScanNode.is_pre_aggregation` | `TabletReader` | 控制 AGG_KEYS/UNIQUE_KEYS 表是否在 scan 层做预聚合 |
| enableColumnExprPredicate | SessionVariable | `TLakeScanNode.enable_column_expr_predicate` | `SegmentIterator` | 控制是否启用列表达式谓词下推 |
| enablePruneColumnAfterIndexFilter | SessionVariable | `TLakeScanNode.enable_prune_column_after_index_filter` | `SegmentIterator` | 控制索引过滤后是否裁剪列 |
| enableGinFilter | SessionVariable | `TLakeScanNode.enable_gin_filter` | `SegmentIterator` | 控制是否启用 GIN 倒排索引 |
| enableGlobalLateMaterialization | SessionVariable | `TLakeScanNode.enable_global_late_materialization` | `SegmentIterator` | 控制全局延迟物化 |
| enableTopnFilterBackPressure | SessionVariable | `TLakeScanNode.enable_topn_filter_back_pressure` | `OlapScanOperator` | 控制 TopN Filter 背压机制 |
| outputAscHint | FE Planner (TopN Filter) | `TLakeScanNode.output_asc_hint` | `SegmentIterator` | 提示 scan 输出按排序键升序/降序 |

---

## 2. FE 侧下推（查询规划阶段）

### 2.1 分区裁剪

分区裁剪在查询规划最早期执行，通过评估 WHERE 子句中涉及分区键的谓词，排除不可能包含匹配数据的分区。这直接减少了需要扫描的 tablet 总数。

**核心类与方法：**

- `PartitionPruneRule.transform()` — 优化器规则入口
- `OptOlapPartitionPruner.prunePartitions()` — 分区裁剪主逻辑
- `RangePartitionPruner` / `ListPartitionPruner` — 不同分区类型的裁剪器
- `ColumnFilterConverter.convertColumnFilter()` — 将谓词转换为 `PartitionColumnFilter`

**算法流程：**

1. `PushDownPredicateScanRule` 将 WHERE 谓词推入 `LogicalOlapScanOperator`
2. `LogicalScanOperator.buildColumnFilters()` 调用 `ColumnFilterConverter.convertColumnFilter()`，将谓词解析为每列的 `PartitionColumnFilter`（提取上界、下界、IN 列表）
3. `PartitionPruneRule` 根据分区类型选择裁剪器：
   - **Range 分区**：将列过滤器与分区范围做交集，排除不相交的分区
   - **List 分区**：将列过滤器与分区值列表做匹配，排除无匹配值的分区
   - **表达式分区**：对分区表达式求值来判断
4. 裁剪结果写入 `LogicalOlapScanOperator.selectedPartitionIds`
5. `prunePartitionPredicates()` 进一步识别已被分区范围完全覆盖的谓词（如分区键 = 常量），这些谓词在后续不再需要在 BE 侧重复求值，存入 `prunedPartitionPredicates`

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `enable_rbo_table_prune` | `true` | 启用基于规则的分区裁剪 |
| `enable_cbo_table_prune` | `true` | 启用基于代价的分区裁剪 |
| `enable_expr_prune_partition` | `true` | 启用基于表达式求值的分区裁剪 |

### 2.2 分桶裁剪

在确定需要扫描的分区后，进一步根据分桶键上的等值谓词裁剪 tablet。

**核心类与方法：**

- `OptDistributionPruner` — 分桶裁剪主逻辑
- `LimitPruneTabletsRule` — 基于 LIMIT 的 tablet 裁剪

**算法流程：**

`OptDistributionPruner` 针对 Hash 分桶：
1. 检查谓词中是否包含分桶键的等值条件（如 `bucket_key = 'value'`）
2. 计算等值条件对应的 hash 值
3. 确定该 hash 值映射到的 tablet
4. 只保留命中的 tablet，排除其他 tablet

`LimitPruneTabletsRule` 针对 DUP_KEYS 表的 `SELECT * FROM t LIMIT n` 场景：
1. 检查条件：仅 DUP_KEYS 表、无 WHERE 谓词
2. 按 tablet 累加行数，直到行数总和超过 LIMIT 值
3. 只保留累加所需的前若干个 tablet

裁剪结果写入 `LogicalOlapScanOperator.selectedTabletId`。

### 2.3 谓词下推

将 WHERE 子句中的谓词从 Filter 算子下推到 Scan 算子内部，使 BE 在读取数据时直接应用过滤。

**核心类与方法：**

- `PushDownPredicateScanRule.transform()` — 将 Filter → Scan 模式中的谓词下推到 Scan
- `ScalarOperatorRewriter` — 谓词重写（常量折叠、范围提取等）
- `ScalarRangePredicateExtractor` — 范围谓词优化

**谓词重写链：**

```
原始谓词
  → ReduceCastRule（消除不必要的类型转换）
  → NormalizePredicateRule（标准化谓词形式）
  → FoldConstantsRule（常量折叠）
  → SimplifiedPredicateRule（简化谓词）
  → ScalarRangePredicateExtractor（合并范围谓词）
  → 最终 ScalarOperator
```

**OR 谓词下推：**

OR 谓词需要特殊处理。启用后，OR 条件可以下推到 BE 的 `PredicateTree` 中以 `CompoundNode<OR>` 表示，BE 的索引过滤（Zone Map、Bitmap Index 等）支持对 OR 节点求值。

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `enable_pushdown_or_predicate` | `true` | 启用 OR 谓词下推 |
| `max_pushdown_or_predicates` | `32` | 单列 OR 条件数上限 |
| `max_pushdown_conditions_per_column` | `-1`（无限制） | 单列 IN 条件数上限 |

### 2.4 列裁剪

只读取查询实际需要的列，避免读取不参与计算的列数据。

**核心类与方法：**

- `PruneScanColumnRule.transform()` — 列裁剪规则
- `OlapScanNode.toThrift()` — 将未使用列信息序列化

**算法流程：**

1. `PruneScanColumnRule` 分析上层算子（Project、Aggregate、Join 等）对列的引用
2. 计算 Scan 节点实际需要输出的列集合 = 上层引用列 ∪ 谓词使用列
3. 更新 `LogicalOlapScanOperator.colRefToColumnMetaMap` 为所需列子集

**Thrift 序列化（TLakeScanNode）：**

- `unused_output_column_name`：仅在谓词中使用但不需要输出的列名列表。BE 在谓词求值后可以跳过这些列的物化。仅在表没有 DELETE 操作时设置（`OlapScanNode.toThrift()` line 1109: `if (!olapTable.hasDelete())`）。
- `column_access_paths`：复杂类型（JSON、STRUCT、ARRAY）的子字段访问路径，用于子字段级别的列裁剪。通过 `TColumnAccessPath` 递归描述访问路径（ROOT → FIELD → KEY/INDEX 等）。

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `enable_filter_unused_columns_in_scan_stage` | `true` | 在 scan 阶段过滤未使用列 |
| `enable_prune_column_after_index_filter` | `true` | 索引过滤后裁剪列 |
| `enable_count_star_optimization` | `true` | `COUNT(*)` 优化：不需要读取任何列数据 |

### 2.5 预聚合下推

对于 AGG_KEYS 表，在 scan 层按排序键对相同 key 的行做预聚合，减少输出行数。

**核心类与方法：**

- `PreAggregateTurnOnRule.rewrite()` — 遍历物理计划树，决定是否开启预聚合

**决策逻辑（`PreAggregateTurnOnRule` lines 143-191）：**

仅对 AGG_KEYS / UNIQUE_KEYS 表生效。需同时满足：
1. 所有谓词只涉及 key 列
2. 所有 GROUP BY 列都是 key 列
3. 上层聚合函数与 schema 定义的聚合函数匹配

如果不满足条件，`isPreAggregation = false`，并记录 `turnOffReason` 用于 EXPLAIN 输出。

**Thrift 序列化：**

- `TLakeScanNode.is_pre_aggregation`（boolean）

**BE 行为：**

- `true`：TabletReader 使用 `AggregateIterator` 包装 segment iterator，在读取时对相同 key 的行做聚合
- `false`：直接输出所有行，聚合由上层算子完成

### 2.6 聚合下推到 MetaScan

将简单聚合函数（COUNT、MIN、MAX）下推到 segment 元数据层，无需读取实际数据。

**核心类与方法：**

- `PushDownAggToMetaScanRule.transform()` — 将 Aggregation → Project → Scan 模式转换为 MetaScan

**适用场景：**

- `SELECT COUNT(*) FROM t`
- `SELECT MIN(col), MAX(col) FROM t`
- 无 WHERE 子句或谓词已被分区裁剪完全覆盖

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `enable_rewrite_simple_agg_to_meta_scan` | `true` | 启用聚合下推到 MetaScan |

### 2.7 Limit / TopN 下推

#### Limit 下推

`PushDownLimitDirectRule` 将 LIMIT 约束从上层算子推向 Scan 方向。这主要体现为 tablet 裁剪（见 2.2 节 `LimitPruneTabletsRule`）。

#### TopN 下推

`PushDownTopNBelowUnionRule` 将 ORDER BY + LIMIT（TopN）推过 UNION 等算子，使每个子查询分支都带上 TopN 约束。

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `cbo_push_down_topn_limit` | `1000` | TopN 下推的 LIMIT 阈值（LIMIT > 此值则不下推） |
| `cbo_push_down_distinct_limit` | `4096` | DISTINCT + LIMIT 下推阈值 |

### 2.8 Runtime Filter 生成与关联

Runtime Filter 是查询执行时动态构建的过滤器，由 Join 的 build 侧生成，推送给 probe 侧的 Scan 节点，在数据读取阶段提前过滤不匹配的行。

**核心类与方法：**

- `RuntimeFilterDescription` — Runtime Filter 描述（filter ID、build/probe 表达式、类型）
- `PlanNode.addProbeRuntimeFilter()` — 将 filter 关联到 Scan 节点

**Runtime Filter 类型：**

| 类型 | 枚举值 | 来源 | 作用 |
|------|--------|------|------|
| Join Filter | `JOIN_FILTER` | Hash Join build 侧 | Bloom Filter / IN List / Min-Max 过滤 |
| TopN Filter | `TOPN_FILTER` | ORDER BY + LIMIT | 按排序键动态收紧过滤范围 |
| Agg IN Filter | `AGG_IN_FILTER` | Aggregation distinct 值 | IN 列表过滤 |

**Join Filter 决策逻辑（`RuntimeFilterDescription.canProbeUse()`）：**

- 检查 probe 侧基数是否小于 build 侧最小尺寸阈值
- 考虑 BROADCAST/SHUFFLE/COLOCATE 等 Join 模式
- 尊重 `globalRuntimeFilterProbeMinSelectivity` 选择率阈值

**TopN Filter 特殊处理：**

TopN Filter 关联到 Scan 节点后，设置 `outputAscHint` 提示 BE 按排序键顺序输出。这使 Scan 可以与 TopN 协同工作——随着 TopN 缓冲区填满，过滤阈值动态收紧。

**背压机制（TLakeScanNode 专属字段）：**

当 TopN Filter 开启背压模式（`topn_filter_back_pressure_mode > 0`），Scan 节点在积累一定行数后主动降速，等待 TopN Filter 更新阈值。通过以下字段控制：
- `back_pressure_max_rounds`：最大背压轮次
- `back_pressure_num_rows`：触发背压的行数阈值
- `back_pressure_throttle_time`：每轮节流等待时间（纳秒）
- `back_pressure_throttle_time_upper_bound`：节流时间上限

**Runtime Filter 布局策略（`TRuntimeFilterLayoutMode`）：**

| 模式 | 含义 |
|------|------|
| SINGLETON | 单个未分区的 filter |
| PIPELINE_SHUFFLE | Pipeline 级别 shuffle |
| GLOBAL_SHUFFLE_2L / 1L | 全局 shuffle（两级/一级合并） |
| PIPELINE_BUCKET / PIPELINE_BUCKET_LX | Bucket 级别 filter（带/不带 local exchange） |
| GLOBAL_BUCKET_2L / 2L_LX / 1L | 全局 bucket 级别 filter |

**关键配置：**

| SessionVariable | 默认值 | 作用 |
|----------------|--------|------|
| `enable_global_runtime_filter` | `true` | 主开关 |
| `enable_topn_runtime_filter` | `true` | 启用 TopN Filter |
| `enable_join_runtime_filter_push_down` | `true` | 启用 Join Runtime Filter 存储层下推 |
| `global_runtime_filter_build_max_size` | `64 MB` | build 侧 filter 最大尺寸 |
| `global_runtime_filter_build_min_size` | `128 KB` | build 侧 filter 最小尺寸 |
| `global_runtime_filter_wait_timeout` | `20 ms` | filter 到达超时 |
| `runtime_filter_scan_wait_time` | `20 ms` | Scan 侧等待 filter 的时间 |
| `enable_dynamic_prune_scan_range` | `true` | 启用动态 scan range 裁剪 |

**Thrift 序列化：**

Runtime Filter 在 `PlanNode.treeToThriftHelper()` 中序列化到 `TPlanNode.probe_runtime_filters`（不在 TLakeScanNode 内部），以 `TRuntimeFilterDescription` 列表的形式传递给 BE。

---

## 3. FE → BE 序列化（Thrift 接口）

### 3.1 TLakeScanNode

存算分离 OlapTable 使用 `TLakeScanNode`（`TPlanNodeType.LAKE_SCAN_NODE`），在 `OlapScanNode.toThrift()` 中通过 `olapTable.isCloudNativeTableOrMaterializedView()` 判断。

**序列化字段一览（`OlapScanNode.toThrift()` lines 1082-1132）：**

| 字段 | 类型 | 来源 | 作用 |
|------|------|------|------|
| `is_pre_aggregation` | bool | PreAggregateTurnOnRule | 预聚合开关 |
| `sort_key_column_names` | list\<string\> | 表 schema | 排序键列名 |
| `rollup_name` | string | selectedIndexMetaId | 选中的物化视图/Rollup 名 |
| `sql_predicates` | string | conjuncts | WHERE 谓词的 SQL 文本（仅 profiling 用） |
| `sort_column` | string | Planner | 排序列 |
| `enable_column_expr_predicate` | bool | SessionVariable | 列表达式谓词开关 |
| `enable_prune_column_after_index_filter` | bool | SessionVariable | 索引过滤后列裁剪开关 |
| `enable_gin_filter` | bool | SessionVariable | GIN 倒排索引开关 |
| `dict_string_id_to_int_ids` | map\<i32,i32\> | 低基数优化 | 字典编码列的全局 ID 映射 |
| `unused_output_column_name` | list\<string\> | PruneScanColumnRule | 仅参与谓词但不输出的列 |
| `bucket_exprs` | list\<TExpr\> | Planner | 分桶表达式 |
| `column_access_paths` | list\<TColumnAccessPath\> | PruneScanColumnRule | 复杂类型子字段访问路径 |
| `next_uniq_id` | i32 | 表 schema | JSON 扁平化路径 ID 上限 |
| `sorted_by_keys_per_tablet` | bool | Planner | 每个 tablet 内按排序键有序 |
| `output_chunk_by_bucket` | bool | Planner | 按 bucket 输出 chunk |
| `enable_global_late_materialization` | bool | SessionVariable | 全局延迟物化开关 |
| `output_asc_hint` | bool | TopN Filter | scan 输出升序提示 |
| `schema_key` | string | 表 schema | schema 版本标识 |
| `enable_topn_filter_back_pressure` | bool | SessionVariable | TopN 背压开关 |
| `back_pressure_*` | 多个字段 | SessionVariable | 背压参数 |

**注意：TLakeScanNode 与 TOlapScanNode 的差异**

以下字段仅在 TOlapScanNode（存算一体）中设置，TLakeScanNode 中**不设置**：
- `columns_desc`（列描述）
- `schema_id`
- `max_parallel_scan_instance_num`
- `partition_order_hint`
- `vector_search_options`
- `sample_options`
- `use_pk_index`

### 3.2 TScanRangeLocations

分区裁剪和分桶裁剪的结果不显式出现在 TLakeScanNode 中，而是隐含在 `TScanRangeLocations` 列表里——每个元素对应一个需要扫描的 tablet。

**`addScanRangeLocations()`（OlapScanNode.java lines 553-709）的存算分离路径：**

1. 通过 `WarehouseManager.getAllComputeNodeIdsAssignToTablets()` 获取 tablet 到计算节点的映射
2. 调用 `tablet.getQueryableReplicas()` 时传入 `computeResource` 参数，选择与当前 warehouse 关联的副本
3. 构建 `TInternalScanRange`，包含 tablet_id、partition_id、version 等

**TInternalScanRange 关键字段：**

| 字段 | 作用 |
|------|------|
| `tablet_id` | Tablet 标识 |
| `partition_id` | 所属分区 ID |
| `version` | 数据版本（一致性保证） |
| `row_count` | Tablet 行数估算（供 BE 调度参考） |
| `bucket_sequence` | 分桶序号（用于按桶输出） |
| `fill_data_cache` | 是否填充数据缓存 |
| `skip_page_cache` / `skip_disk_cache` | 缓存控制 |

### 3.3 TRuntimeFilterDescription

在 `TPlanNode.probe_runtime_filters` 中序列化。

**关键字段：**

| 字段 | 作用 |
|------|------|
| `filter_id` | 唯一标识 |
| `build_expr` / `plan_node_id_to_target_expr` | build/probe 表达式 |
| `has_remote_targets` | 是否跨 fragment |
| `bloom_filter_size` | Bloom Filter 大小 |
| `build_join_mode` | BROADCAST / PARTITIONED 等 |
| `layout` | TRuntimeFilterLayout（分布策略） |
| `is_asc` / `is_nulls_first` / `limit` | TopN Filter 参数 |

---

## 4. BE 侧下推（执行阶段）

### 4.1 Pipeline 入口：OlapChunkSource

**谓词初始化（`OlapChunkSource::_init_reader_params()` lines 259-358）：**

1. 从 `_scan_ctx->conjuncts_manager()` 获取谓词
2. 提取 Runtime Filter 谓词：`get_runtime_filter_predicates()`
3. **谓词分类**——将谓词分为可下推和不可下推两类：
   ```cpp
   pred_tree.root().partition_copy(
       [parser](const auto& node) { return parser->can_pushdown(node); },
       &pushdown_pred_root, &non_pushdown_pred_root);
   ```
4. 可下推谓词存入 `_params.pred_tree`，由 TabletReader / SegmentIterator 处理
5. 不可下推谓词存入 `_non_pushdown_pred_tree`，在 chunk 输出后由 OlapChunkSource 应用

**Lake TabletReader 创建（`_init_olap_reader()` lines 531-622）：**

对于存算分离 tablet，创建 `lake::TabletReader`，传入从 morsel 预获取的 rowsets。

### 4.2 Lake TabletReader

**初始化流程（`lake::TabletReader`）：**

1. `prepare()` — 从 `TabletMetadataPB` 加载 tablet schema，获取 rowsets
2. `open()` — 调用 `get_segment_iterators()` 创建 segment iterator

**`get_segment_iterators()`（lines 333-441）关键步骤：**

1. `init_predicates()` — 初始化谓词
2. `init_delete_predicates()` — 从 `RowsetMetadataPB.delete_predicate()` 解析删除谓词
3. `ZonemapPredicatesRewriter::rewrite_predicate_tree()` — 将谓词重写为 zone map 可用的形式（`pred_tree_for_zone_map`）
4. 对每个 Rowset 调用 `rowset->read(schema, rs_opts)`

**Iterator 组装策略（`init_collector()` lines 529-692）：**

| KeysType | 组装方式 |
|----------|---------|
| DUP_KEYS | `new_union_iterator()`（无需排序/去重，直接合并） |
| AGG_KEYS | `new_merge_iterator()` → `new_aggregate_iterator()`（归并排序 + 聚合） |
| PRIMARY_KEYS | `new_union_iterator()`（依赖 Delete Vector 去重） |

### 4.3 Segment 元数据过滤（存算分离特有）

这是存算分离区别于存算一体的关键优化。在加载 segment footer 之前，利用 segment 级别的排序键统计信息跳过整个 segment。

**触发条件（`Rowset::read()` lines 342-363）：**

```
enable_lake_segment_metadata_filter = true
  AND segment_metas 非空
  AND pred_tree_for_zone_map 非空
  AND tablet_schema 有效
```

**算法（`SegmentMetadataFilter::may_contain()` lines 89-114）：**

1. 从 `SegmentMetadataPB` 获取 `sort_key_min` 和 `sort_key_max`（composite tuple）
2. 使用 `MetadataPruner` visitor 遍历谓词树：
   - **限制**：仅对**第一个排序键列**生效。因为 composite tuple 中非首列的值不是独立的逐列 min/max，不能直接用于逐列过滤
   - 对于 AND 节点：任一子节点可裁剪 → 整个 AND 可裁剪
   - 对于 OR 节点：所有子节点可裁剪 → 整个 OR 才可裁剪
3. 调用 `ColumnPredicate::zone_map_filter()` 判断谓词与 min/max 范围是否相交
4. 不相交的 segment 加入 `skip_segment_idxs`，不再加载

**统计指标：**
- `stats->segment_metadata_filtered`：被跳过的行数
- `stats->segments_metadata_filtered`：被跳过的 segment 数

**配置：**

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `enable_lake_segment_metadata_filter` | `true` | 主开关 |

### 4.4 索引过滤阶段

进入 `SegmentIterator::_init_internal()` 后，按**严格固定顺序**执行以下索引过滤。注释明确指出："the calling order matters, do not change unless you know why."（line 870）。

每一步都将过滤结果与当前 `_scan_range`（`SparseRange` 类型）做交集，逐步缩小需要读取的行范围。

#### 4.4.1 Rowid Range 过滤

**方法：** `_get_row_ranges_by_rowid_range()`

应用外部指定的 rowid 范围约束（来自 `SegmentReadOptions.rowid_range_option`），用于并行 scan 的 morsel 分配——每个 scanner 只负责 segment 中的一个 rowid 子范围。

#### 4.4.2 Short Key Index（前缀索引）

**方法：** `_get_row_ranges_by_keys()`

利用排序键前缀索引定位 key 范围对应的行区间。

**算法：**
1. 根据 `TabletReaderParams` 中的 `start_key` / `end_key`，构建 `SeekRange`
2. 对每个 SeekRange，通过 `_lookup_ordinal()` 在 Short Key Index 中做二分查找，定位起止行号
3. 将行号范围加入结果集

**适用场景：**
- 谓词涉及排序键前缀列的范围查询（`>=`, `<=`, `BETWEEN`）
- 等值查询（`=`）当 `enable_short_key_for_one_column_filter = true` 时

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `enable_short_key_for_one_column_filter` | `false` | 启用单列等值查询使用 Short Key Index |

#### 4.4.3 Tablet Range 过滤

**方法：** `_apply_tablet_range()`

应用 tablet 的分桶键范围约束，过滤掉不属于当前 tablet key 范围的行。用于多 tablet 扫描时排除跨 tablet 边界的数据。

#### 4.4.4 Delete Vector（条件位置）

**方法：** `_apply_del_vector()`

仅对**主键表**（PRIMARY_KEYS）生效。从 `LakeDelvecLoader` 异步加载的 Delete Vector 中获取已删除行的 Roaring Bitmap，从 `_scan_range` 中减去。

**执行位置由配置控制：**

| BE Config | 默认值 | 效果 |
|-----------|--------|------|
| `apply_del_vec_after_all_index_filter` | `true` | Delete Vector 在所有索引过滤之后应用 |
| （设为 false） | — | Delete Vector 在 Bitmap Index 之前应用 |

默认在所有索引过滤之后应用的原因：索引过滤可能已经大幅缩小了 `_scan_range`，此时再应用 Delete Vector 可以减少 bitmap 交集运算的开销。

#### 4.4.5 Bitmap Index

**方法：** `_apply_bitmap_index()`

**核心类：** `BitmapIndexReader`、`BitmapIndexEvaluator`

**算法：**
1. 为每个有 Bitmap Index 的谓词列创建 `BitmapIndexIterator`
2. `BitmapIndexEvaluator` 遍历 `PredicateTree`，对每个叶节点调用 `ColumnPredicate::seek_bitmap_dictionary()` 和 `ColumnPredicate::bitmap_filter()` 获取匹配的 Roaring Bitmap
3. AND 节点：子节点 bitmap 做交集；OR 节点：子节点 bitmap 做并集
4. 将结果与 `_scan_range` 做交集

**支持的谓词类型：** `=`, `!=`, `IN`, `NOT IN`

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `enable_index_bitmap_filter` | `true` | 主开关 |
| `enable_bitmap_index_memory_page_cache` | `true` | Bitmap Index 内存缓存 |

#### 4.4.6 Zone Map（页级）

**方法：** `_get_row_ranges_by_zone_map()`

**核心类：** `ZoneMapIndexReader`、`ZoneMapFilterEvaluator`

**算法：**
1. 收集 Delete Predicate 按列分组为 OR 条件
2. 使用 `ZoneMapFilterEvaluator` 遍历 `PredicateTree`：
   - 对每个谓词列，读取该列每个 Page 的 Zone Map（min, max, has_null）
   - 调用 `ColumnPredicate::zone_map_filter(ZoneMapDetail)` 判断 Page 是否可能包含匹配行
   - AND 节点：任一子节点排除该 Page → 该 Page 被排除
   - OR 节点：所有子节点排除该 Page → 该 Page 才被排除
3. 将 Page 粒度的结果转换为行号范围，与 `_scan_range` 做交集

**两级 Zone Map 与执行位置：**
- **Segment 级**（`sort_key_min/max`）：在 4.3 节的 `SegmentMetadataFilter` 中使用，**发生在 segment footer 加载之前**，仅对排序键首列生效。这是存算分离特有的优化层
- **Page 级**（每个 Page 的逐列 min/max）：在本步骤（`_get_row_ranges_by_zone_map`）使用，**发生在 segment 打开之后**，对所有有 Zone Map 的列生效。这是所有存储路径共享的通用优化

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `enable_index_page_level_zonemap_filter` | `true` | 页级 Zone Map 过滤开关 |
| `enable_index_segment_level_zonemap_filter` | `true` | 段级 Zone Map 过滤开关 |
| `enable_zonemap_index_memory_page_cache` | `true` | Zone Map 索引内存缓存 |
| `enable_string_prefix_zonemap` | `true` | 字符串列使用前缀 Zone Map |

#### 4.4.7 Bloom Filter Index

**方法：** `_get_row_ranges_by_bloom_filter()`

**核心类：** `BloomFilterIndexReader`

**算法：**
1. `BloomFilterSupportChecker` 检查谓词是否支持 Bloom Filter 评估
2. 对每个 Page，读取该列的 Bloom Filter
3. 测试谓词值是否"可能存在"于该 Page
4. 确定"一定不存在"的 Page 从 `_scan_range` 中排除

**支持的谓词类型：** `=`, `IN`, `IS NULL`

**两种 Bloom Filter：**
- **标准 Bloom Filter**：适用于任意值类型
- **NGRAM Bloom Filter**：专用于字符串前缀/子串匹配

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `enable_index_bloom_filter` | `true` | 主开关 |

#### 4.4.8 GIN 倒排索引

**方法：** `_apply_inverted_index()`

**核心类：** `InvertedIndexIterator`、`InvertedReader`

倒排索引支持文本列的子串和短语匹配。为每个有 GIN 索引的谓词列创建 `InvertedIndexIterator`，执行倒排索引查询获取匹配行号集合。

| 控制标志 | 来源 | 默认值 |
|----------|------|--------|
| `enable_gin_filter` | SessionVariable → TLakeScanNode | `true` |

#### 4.4.9 向量索引（ANN）

**方法：** `_get_row_ranges_by_vector_index()`

**核心类：** `VectorIndexReader`（需编译时开启 `WITH_TENANN`）

用于近似最近邻搜索。根据 `VectorSearchOption` 中的查询向量和参数（top-K、距离阈值等），返回最相似的行号集合。

**注意：** TLakeScanNode 当前**不设置** `vector_search_options`（仅 TOlapScanNode 设置），因此向量索引在存算分离路径下可能不可用。

#### 4.4.10 数据采样

**方法：** `_apply_data_sampling()`

根据 `TTableSampleOptions` 中的采样参数（按 Block 或按 Page），随机选择部分数据块，其余从 `_scan_range` 中排除。

**注意：** TLakeScanNode 当前**不设置** `sample_options`（仅 TOlapScanNode 设置），因此数据采样在存算分离路径下可能不可用。

### 4.5 谓词重写与分类

索引过滤完成后，在进入 chunk 读取之前：

#### 4.5.1 谓词重写

**方法：** `_rewrite_predicates()`（line 903）

利用 segment 的字典编码信息，将字符串类型的谓词重写为字典码比较，避免字符串解码开销。如果 `enable_join_runtime_filter_pushdown = true`，Runtime Filter 谓词也会被重写。

#### 4.5.2 谓词分类

**方法：** `_init_column_predicates()`（lines 1360-1372）

将谓词树分为三类：

```
完整谓词树
  ├─ Index-only 谓词 → 丢弃（已被索引完全求值，无需重复计算）
  └─ 需要求值的谓词
       ├─ Non-Expression 谓词 → _non_expr_pred_tree
       │   （IN, BETWEEN, =, <, >, IS NULL 等简单谓词，可在列迭代器级别高效求值）
       └─ Expression 谓词 → _expr_pred_tree
           （复杂表达式谓词，需要完整 chunk 物化后求值）
```

另外，Runtime Filter 谓词单独存储在 `_runtime_filter_preds` 中。

### 4.6 Chunk 读取与谓词求值

**方法：** `_do_get_next()`（lines 2041-2200）

#### 4.6.1 Late Materialization 路径

当 Late Materialization 开启时（决策见 4.7 节）：

**第一阶段：逐列谓词求值**

1. `_predicate_evaluate_late_materialize_read_first_column()`
   - 只读取第一个谓词列的数据
   - 应用该列的 Non-Expression 谓词
   - 应用该列的 Expression 谓词
   - 产生 selection 向量（标记哪些行通过了过滤）

2. `_evaluate_late_materialize_read_other_columns()`
   - 对谓词列 2 到 N，依次：
     - 根据上一步的 selection 向量，通过 rowid 随机读取（fetch_values_by_rowid）
     - 应用该列的 compound 谓词（`_filter_by_compound_and_predicates()`）
     - 在 compound 谓词之后应用该列的 Runtime Filter（`_evaluate_col_runtime_filters()`）
     - 更新 selection 向量

**第二阶段：物化非谓词列**

3. `_finish_late_materialization()`
   - 根据最终 selection 向量确定的行，通过 rowid 读取所有非谓词列

#### 4.6.2 非 Late Materialization 路径

1. 一次性读取所有列的 chunk 数据
2. `_filter_by_non_expr_predicates()` — 应用 Non-Expression 谓词和 Runtime Filter
3. `_filter_by_expr_predicates()` — 应用 Expression 谓词

#### 4.6.3 Runtime Filter 在谓词求值中的位置

Runtime Filter 在以下两个位置被求值：

**位置 1：compound 谓词之后（Late Materialization 路径）**

在 `_filter_by_compound_and_predicates()`（lines 2479-2521）中：
1. 先求值 compound 谓词（`compound_and_predicates_evaluate()`）
2. 然后求值该列的 Runtime Filter（`_evaluate_col_runtime_filters()`）

**位置 2：Non-Expression 谓词之后（非 Late Materialization 路径）**

在 `_filter_by_non_expr_predicates()`（lines 2523-2549）中：
1. 先求值 `_non_expr_pred_tree`
2. 然后求值 `_runtime_filter_preds`

#### 4.6.4 Delete Predicate 过滤

在所有谓词求值完成后（lines 2139-2164）：
1. 检查 chunk 是否有 delete state 且存在 delete predicates
2. 对匹配 delete predicate 的行标记为删除
3. 从 selection 向量中排除

### 4.7 Late Materialization 决策

**决策方法：** `_init_context()`（line 905）

Late Materialization 的核心思想：先只读取谓词列做过滤，确定需要的行后再通过 rowid 随机读取其余列。当过滤率高（大部分行被过滤掉）时，避免了不必要列的读取。

**决策依据：**

| 配置 | 默认值 | 含义 |
|------|--------|------|
| `late_materialization_ratio` (BE Config) | `10` | 0 = 禁用, 1000 = 始终启用, 其他 = 基于选择率决策 |
| `enable_predicate_col_late_materialize` (SessionVariable) | `true` | 谓词列级别延迟物化 |
| `enable_global_late_materialization` (SessionVariable) | `true` | 全局延迟物化（FE 侧控制，通过 TLakeScanNode 传递） |

### 4.8 Runtime Filter 存储层下推与采样策略

Runtime Filter 不仅在 chunk 级别求值（4.6.3 节），还可以下推到存储层参与 Zone Map 过滤。

**Zone Map 级别（`_try_to_update_ranges_by_runtime_filter()`）：**

在 chunk 读取阶段（`do_get_next()` 中，而非 `_init_internal()`），每次读取新 chunk 之前调用此方法，检查 Runtime Filter 是否已从 Join build 侧异步到达。若已到达，则利用 Runtime Filter 的 min/max 统计对 Zone Map 进行过滤——如果某个 Page 的 Zone Map 范围与 Runtime Filter 的 min/max 范围不相交，整个 Page 可以被跳过。之所以不在 segment 初始化阶段执行，是因为 Runtime Filter 可能在 segment 初始化之后才就绪，需要在每次 chunk 读取时动态检查并增量更新 `_scan_range`。

**采样策略（`RuntimeFilterPredicates`）：**

Runtime Filter 的谓词求值使用采样机制优化执行顺序：

| 阶段 | 行为 |
|------|------|
| INIT | 收集可用的 Runtime Filter |
| SAMPLE | 在样本数据上评估各 filter 的选择率 |
| NORMAL | 按选择率从高到低排序执行 filter |

每隔 16 个 chunk 重新评估选择率，动态调整执行顺序。

| BE Config | 默认值 | 作用 |
|-----------|--------|------|
| `predicate_sampling_trigger_selectivity_threshold` | `0.2` | 首个谓词列选择率高于此阈值时触发采样 |

---

## 5. 端到端示例

### 场景

存算分离集群，明细表（DUP_KEYS）定义如下：

```sql
CREATE TABLE orders (
    dt DATE,
    city_id INT,
    order_id BIGINT,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    detail JSON
) ENGINE=OLAP
DUPLICATE KEY(dt, city_id, order_id)
PARTITION BY RANGE(dt) (
    PARTITION p20240101 VALUES [('2024-01-01'), ('2024-01-02')),
    PARTITION p20240102 VALUES [('2024-01-02'), ('2024-01-03')),
    ...
    PARTITION p20240115 VALUES [('2024-01-15'), ('2024-01-16'))
)
DISTRIBUTED BY HASH(city_id) BUCKETS 4
PROPERTIES("storage_type" = "column_with_row");
```

- 15 个分区，每个分区 4 个 tablet，共 60 个 tablet
- 排序键：`(dt, city_id, order_id)`
- 分桶键：`city_id`
- `status` 列有 Bitmap Index
- `amount` 列有 Bloom Filter Index

执行查询：

```sql
SELECT o.city_id, SUM(o.amount)
FROM orders o JOIN dim_city d ON o.city_id = d.id
WHERE o.dt = '2024-01-15'
  AND o.status = 'PAID'
  AND o.amount > 100.00
GROUP BY o.city_id
ORDER BY SUM(o.amount) DESC
LIMIT 10;
```

### FE 规划阶段

**1. 分区裁剪**

`PartitionPruneRule` → `OptOlapPartitionPruner.prunePartitions()`：
- 谓词 `dt = '2024-01-15'` 与 Range 分区 `[2024-01-15, 2024-01-16)` 匹配
- 结果：只保留 `p20240115` 分区（1 个分区，4 个 tablet）
- 谓词 `dt = '2024-01-15'` 被加入 `prunedPartitionPredicates`（不再传给 BE 重复求值）

从 60 个 tablet 裁剪到 4 个——**减少 93%**。

**2. 谓词下推**

`PushDownPredicateScanRule`：
- `status = 'PAID'` 和 `amount > 100.00` 下推到 `LogicalOlapScanOperator.predicate`
- `dt = '2024-01-15'` 已被分区裁剪覆盖，从 conjuncts 中移除

**3. 列裁剪**

`PruneScanColumnRule`：
- 上层需要：`city_id`（GROUP BY + JOIN）、`amount`（SUM）
- 谓词需要：`status`、`amount`
- 不需要读取：`dt`（已裁剪）、`order_id`、`detail`
- `status` 加入 `unused_output_column_name`（仅参与谓词，不需要输出）

**4. 预聚合决策**

`PreAggregateTurnOnRule`：DUP_KEYS 表，`isPreAggregation = false`。

**5. Runtime Filter**

Join `o.city_id = d.id`：
- dim_city 是小维表（BROADCAST Join）
- 生成 JOIN_FILTER：`RuntimeFilterDescription(filter_id=1, build_expr=d.id, probe_expr=o.city_id)`
- 关联到 orders 的 Scan 节点

ORDER BY ... LIMIT 10：
- 生成 TOPN_FILTER：`RuntimeFilterDescription(filter_id=2, type=TOPN_FILTER, is_asc=false, limit=10)`
- 关联到 orders 的 Scan 节点，设置 `outputAscHint = false`（降序）

**6. Thrift 序列化**

生成 `TLakeScanNode`：
```
is_pre_aggregation = false
enable_column_expr_predicate = true
enable_prune_column_after_index_filter = true
enable_gin_filter = true
enable_global_late_materialization = true
unused_output_column_name = ["status"]
sorted_by_keys_per_tablet = true
output_asc_hint = false
```

生成 4 个 `TScanRangeLocations`（对应 p20240115 分区的 4 个 tablet）。

### BE 执行阶段

以其中一个 tablet 为例（假设包含 3 个 rowset，共 5 个 segment，每个 segment 约 10 万行）。

**1. Lake Segment 元数据过滤**

`SegmentMetadataFilter::may_contain()` 对每个 segment 的 `sort_key_min/max` 检查：
- 排序键首列是 `dt`，谓词 `dt = '2024-01-15'` 已被分区裁剪，不在 BE 谓词中
- 剩余谓词 `status = 'PAID'` 和 `amount > 100.00` 不涉及排序键首列
- 结果：所有 5 个 segment 都保留（无法基于这些谓词做 segment 级过滤）

**2. SegmentIterator 索引过滤**（以 segment_0 为例，10 万行）

初始 `_scan_range = [0, 100000)`

| 步骤 | 方法 | 效果 | 剩余行数 |
|------|------|------|---------|
| Rowid Range | `_get_row_ranges_by_rowid_range()` | 无外部约束 | 100,000 |
| Short Key Index | `_get_row_ranges_by_keys()` | 无排序键前缀谓词 | 100,000 |
| Tablet Range | `_apply_tablet_range()` | 无跨 tablet 边界 | 100,000 |
| Bitmap Index | `_apply_bitmap_index()` | `status = 'PAID'` 命中 Bitmap Index，排除 status != 'PAID' 的行 | ~20,000 |
| Zone Map | `_get_row_ranges_by_zone_map()` | `amount > 100.00` 排除 max(amount) <= 100.00 的 Page | ~15,000 |
| Bloom Filter | `_get_row_ranges_by_bloom_filter()` | `amount > 100.00` 在 Bloom Filter 不支持范围谓词，无额外过滤 | ~15,000 |
| Delete Vector | `_apply_del_vector()` | DUP_KEYS 表无 Delete Vector | ~15,000 |

**3. 谓词分类**

- Index-only：（无，Bitmap Index 不完全精确）
- Non-Expression 谓词：`status = 'PAID'`, `amount > 100.00`
- Expression 谓词：（无）
- Runtime Filter：`city_id IN (runtime_bloom_filter_from_dim_city)`

**4. Chunk 读取与谓词求值**（Late Materialization 路径）

假设 chunk_size = 4096：

第一个 chunk（4096 行）：
1. 只读取 `status` 列 → 应用 `status = 'PAID'` → 约 800 行通过
2. 根据 selection 读取 `amount` 列 → 应用 `amount > 100.00` → 约 500 行通过
3. 读取 `city_id` 列 → 应用 Runtime Filter `city_id IN bloom_filter` → 约 400 行通过（实际过滤效果取决于 dim_city 表的基数和 Join 选择率）
4. 读取剩余输出列的最终数据（Late Materialization）
5. 输出 400 行给上层算子

**5. 最终结果**

上层算子（Aggregate → Sort → Limit）处理过滤后的数据，返回 Top 10 城市。

---

## 6. 配置参考

### 6.1 FE SessionVariable

| 变量名 | 默认值 | 作用 | 可见性 |
|--------|--------|------|--------|
| `enable_column_expr_predicate` | `true` | 列表达式谓词下推 | INVISIBLE |
| `enable_prune_column_after_index_filter` | `true` | 索引过滤后列裁剪 | INVISIBLE |
| `enable_gin_filter` | `true` | GIN 倒排索引 | 可见 |
| `enable_global_late_materialization` | `true` | 全局延迟物化 | 可见 |
| `enable_global_late_materialization_cost_based` | `true` | 基于代价的延迟物化决策 | 可见 |
| `enable_pushdown_or_predicate` | `true` | OR 谓词下推 | INVISIBLE |
| `max_pushdown_or_predicates` | `32` | 单列 OR 条件数上限 | INVISIBLE |
| `max_pushdown_conditions_per_column` | `-1` | 单列 IN 条件数上限（-1=无限制） | 可见 |
| `enable_global_runtime_filter` | `true` | Runtime Filter 主开关 | 可见 |
| `enable_topn_runtime_filter` | `true` | TopN Filter | 可见 |
| `enable_join_runtime_filter_push_down` | `true` | Join RF 存储层下推 | INVISIBLE |
| `global_runtime_filter_build_max_size` | `64 MB` | RF build 侧最大尺寸 | INVISIBLE |
| `global_runtime_filter_build_min_size` | `128 KB` | RF build 侧最小尺寸 | INVISIBLE |
| `global_runtime_filter_wait_timeout` | `20 ms` | RF 全局等待超时 | INVISIBLE |
| `runtime_filter_scan_wait_time` | `20 ms` | Scan 侧 RF 等待时间 | INVISIBLE |
| `enable_dynamic_prune_scan_range` | `true` | 动态 scan range 裁剪 | 可见 |
| `cbo_push_down_topn_limit` | `1000` | TopN 下推的 LIMIT 阈值 | 可见 |
| `cbo_push_down_distinct_limit` | `4096` | DISTINCT LIMIT 下推阈值 | 可见 |
| `enable_count_star_optimization` | `true` | COUNT(*) 优化 | INVISIBLE |
| `enable_filter_unused_columns_in_scan_stage` | `true` | Scan 阶段过滤未使用列 | 可见 |
| `enable_predicate_col_late_materialize` | `true` | 谓词列级别延迟物化 | 可见 |
| `enable_rewrite_simple_agg_to_meta_scan` | `true` | 聚合下推到 MetaScan | 可见 |
| `enable_push_down_pre_agg_with_rank` | `true` | Rank 相关预聚合下推 | 可见 |
| `enable_rbo_table_prune` | `true` | 基于规则的分区裁剪 | 可见 |
| `enable_cbo_table_prune` | `true` | 基于代价的分区裁剪 | 可见 |
| `enable_expr_prune_partition` | `true` | 表达式分区裁剪 | 可见 |
| `topn_filter_back_pressure_mode` | `0` | TopN 背压模式（0=关闭） | 可见 |

### 6.2 BE Config

| 配置名 | 默认值 | 作用 |
|--------|--------|------|
| `enable_lake_segment_metadata_filter` | `true` | 存算分离 segment 元数据过滤 |
| `enable_index_page_level_zonemap_filter` | `true` | 页级 Zone Map 过滤 |
| `enable_index_segment_level_zonemap_filter` | `true` | 段级 Zone Map 过滤 |
| `enable_index_bitmap_filter` | `true` | Bitmap Index 过滤 |
| `enable_index_bloom_filter` | `true` | Bloom Filter Index 过滤 |
| `enable_short_key_for_one_column_filter` | `false` | 单列 Short Key Index |
| `enable_string_prefix_zonemap` | `true` | 字符串前缀 Zone Map |
| `apply_del_vec_after_all_index_filter` | `true` | Delete Vector 应用位置 |
| `enable_pindex_filter` | `true` | 主键索引 Bloom Filter |
| `enable_json_flat_remain_filter` | `true` | JSON 扁平化剩余字段过滤 |
| `late_materialization_ratio` | `10` | 延迟物化比率（0=禁用, 1000=始终） |
| `enable_bitmap_index_memory_page_cache` | `true` | Bitmap Index 内存缓存 |
| `enable_zonemap_index_memory_page_cache` | `true` | Zone Map 索引内存缓存 |
| `enable_ordinal_index_memory_page_cache` | `true` | Ordinal 索引内存缓存 |
| `predicate_sampling_trigger_selectivity_threshold` | `0.2` | 谓词采样触发选择率阈值 |

### 6.3 统计指标（OlapReaderStatistics）

| 指标 | 含义 |
|------|------|
| `segment_metadata_filtered` | 被 SegmentMetadataFilter 跳过的行数 |
| `segments_metadata_filtered` | 被 SegmentMetadataFilter 跳过的 segment 数 |
| `rows_key_range_filtered` | 被 Short Key Index 过滤的行数 |
| `rows_stats_filtered` | 被 Zone Map 过滤的行数 |
| `rows_bitmap_index_filtered` | 被 Bitmap Index 过滤的行数 |
| `rows_bf_filtered` | 被 Bloom Filter Index 过滤的行数 |
| `rows_del_vec_filtered` | 被 Delete Vector 过滤的行数 |
| `runtime_stats_filtered` | 被 Runtime Filter 过滤的行数 |
| `segment_init_ns` | Segment 初始化耗时 |
| `bitmap_index_filter_timer` | Bitmap Index 过滤耗时 |
| `zone_map_filter_ns` | Zone Map 过滤耗时 |
| `bf_filter_ns` | Bloom Filter 过滤耗时 |
| `vec_cond_ns` | 向量化谓词求值耗时 |
| `rf_cond_evaluate_ns` | Runtime Filter 求值耗时 |

---

## 7. 结构不变量

### 索引过滤顺序不可变

`SegmentIterator::_init_internal()` 中的索引过滤顺序由代码注释明确保护（"the calling order matters, do not change unless you know why"）。变更顺序可能导致：
- Bitmap Index 在 Zone Map 之前执行会增加 Bitmap 查询的范围，降低效率
- Delete Vector 的位置影响后续索引过滤的 `_scan_range` 大小

### TLakeScanNode 与 TOlapScanNode 的互斥

`OlapScanNode.toThrift()` 通过 `isCloudNativeTableOrMaterializedView()` 在两个代码分支中二选一。一个 TPlanNode 只会包含 `lake_scan_node` 或 `olap_scan_node` 之一，由 `TPlanNodeType` 枚举区分。

### Runtime Filter 在 PlanNode 层序列化

Runtime Filter 不在 TLakeScanNode / TOlapScanNode 内部，而在 `TPlanNode.probe_runtime_filters` 中。这是因为 Runtime Filter 是跨算子的协调机制，由 `PlanNode` 基类统一管理。

### Delete Vector 仅对 PRIMARY_KEYS 生效

Delete Vector 加载和应用的前置条件是 `options.is_primary_keys`。DUP_KEYS 和 AGG_KEYS 表不使用 Delete Vector，其删除通过 Delete Predicate 实现。

### SegmentMetadataFilter 仅对排序键首列生效

`MetadataPruner` 明确检查 `sort_key_idxes[0] == column_id`，非首列的 composite tuple 值不是独立的逐列 min/max，不能用于谓词评估。这是一个实现限制，意味着如果查询谓词不涉及排序键首列，SegmentMetadataFilter 无法过滤任何 segment。
