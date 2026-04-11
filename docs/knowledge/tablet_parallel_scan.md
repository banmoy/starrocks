# 存算分离内表 Tablet 内并行切分机制

> **导读**：本文档分四层展开：
> - **问题与方案概览**（Section 1）：tablet 数少于 pipeline_dop 时并行度退化的问题及解决思路
> - **概念模型**（Section 2）：Morsel、MorselQueue、MorselQueueFactory、ChunkSource 等核心概念
> - **切分算法与判定**（Sections 3-4）：tablet 如何按 RowID 或 ShortKey 切分为子区间，以及切分判定逻辑
> - **调度与辅助机制**（Sections 5-6）：切出的子区间如何分发到各 Driver，以及 EOS 追踪、IO 调度

## 1. 问题与方案概览

Pipeline 执行引擎中，每个 ScanOperator 实例绑定一个 Driver，pipeline_dop 决定了 Driver 数量。基本调度单元是 Morsel——每个 tablet 对应一个 Morsel。

**核心矛盾**：

1. **tablet 数少于 Driver 数**：当 tablet 数量远小于 pipeline_dop 时，多数 Driver 无法获取 Morsel，实际并行度退化。
2. **单 tablet 数据量大**：当 tablet 数少于 pipeline_dop 且单个 tablet 包含大量数据时，一个 Driver 串行读取整个 tablet 成为瓶颈，需要多个 Driver 并行读取同一 tablet 的不同区间来加速。（设置 `tablet_internal_parallel_mode = FORCE_SPLIT` 时，即使 tablet 数足够也可强制切分。）

**解决思路**：将一个 Morsel（整 tablet）拆成多个子区间 Morsel，使更多 Driver 有工作可做。存算分离内表通过 ConnectorScanNode → LakeDataSourceProvider 路径扫描，使用**切分调度**：在 IO task 内部由 TabletReader 预计算所有子区间，然后注入到各 Driver 的队列，各 Driver 并行读取不同子区间。

**架构背景**：存算分离架构下，FE planner 将 lake 表的 scan 映射到 ConnectorScanNode（复用 connector 框架的 DataSource 抽象），而非 OlapScanNode。存算一体内表走 OlapScanNode，使用不同的切分调度接入（见 Section 8 与 OlapScan 路径的差异），但共享切分算法。

> 注：BE 侧存在 Shared Scan 机制（多个 Driver 共享 chunk buffer，IO 产出轮询分发），但 FE 自 3.5 版本起硬编码 `SessionVariable.isEnableSharedScan()` 返回 false（原因：与 event-based scheduling 不兼容），因此该路径当前不可触发，本文档不做展开。

---

## 2. 概念模型

### 2.1 核心组件

```
FE 下发 scan range
        │
        ▼ 每个 tablet 对应一个 Morsel
   MorselQueue（持有 Morsel 的队列）
        │
        ▼
   MorselQueueFactory（决定如何分配 MorselQueue 给各 Driver）
        │
        ▼
   IndividualMorselQueueFactory
   （每个 Driver 1 个 DynamicMorselQueue，支持运行时注入新 morsel）
        │
        ▼
   Driver 从 queue 取 morsel → 创建 ChunkSource → 提交 IO task
                                                       │
                                                       ▼
                                                  BalancedChunkBuffer (kDirect)
                                                       │
                                                       ▼
                                                  Driver pull_chunk()
```

**术语定义**：

| 术语 | 含义 |
|------|------|
| **Morsel** | 扫描工作的调度单元。原始 Morsel 对应一个 tablet；Split Morsel 对应 tablet 的一个子区间 |
| **Split Morsel** | 携带子区间描述的 Morsel。Physical 切分产出的携带 `RowidRangeOption`（精确 rowid 区间），Logical 切分产出的携带 `ShortKeyRangesOption`（short-key 近似区间） |
| **MorselQueue** | 持有 Morsel 的队列。存算分离表使用 DynamicMorselQueue（支持运行时追加新 morsel） |
| **MorselQueueFactory** | 决定如何将 MorselQueue 分配给各 Driver。存算分离表使用 IndividualMorselQueueFactory（每个 Driver 独立 queue） |
| **ChunkSource** | 由 Morsel 创建，绑定到一个 IO task，负责从 tablet 的指定区间读取数据并写入 ChunkBuffer |
| **ChunkBuffer** | 缓冲 IO task 产出的 chunk。kDirect 策略下每个 Driver 只消费自己的产出 |

### 2.2 切分调度的数据流

存算分离表的切分调度采用**外部注入式**——切分不在 morsel queue 层完成，而是在 IO task 内部由 TabletReader 预计算，然后注入回 Driver 的队列：

1. 原始 Morsel 分配给某个 Driver
2. Driver 的 IO task 中，TabletReader 一次性预计算所有子区间 → 立即返回 EOF
3. ChunkSource 收到 EOF → 将子区间包装为 Split Morsel → `append_morsels()` 注入到各 Driver 的 DynamicMorselQueue
4. 各 Driver 从自己的 queue 取 Split Morsel → 并行读取不同子区间

### 2.3 `has_more_from_split` 语义

DynamicMorselQueue 上的 boolean flag，核心作用：**阻止 ScanOperator 在 split morsel 注入前过早判定完成**。

当为 true 时，`MorselQueue::has_more()` 返回 true → `ScanOperator::is_finished()` 不退出——即使当前 queue 已空。

生命周期：

1. Pipeline 构建时设置 `has_more_from_split = true`
2. 运行时，每个原始 morsel 完成切分后调用 `IndividualMorselQueueFactory::mark_split_source_morsel_finished()`，原子递减计数器
3. 所有原始 morsel 切分完成后（计数器归零），设置所有 queue 的 `has_more_from_split = false`
4. `ScanOperator::is_finished()` 可正常判定完成

---

## 3. 切分算法

Physical 切分和 Logical 切分是两种 tablet 内切分算法，封装在 `PhysicalSplitMorselQueue` / `LogicalSplitMorselQueue` 中，通过 `try_get()` 按需产出 split morsel。存算分离表和存算一体表共用这两种算法。

### 3.1 Physical vs Logical 选择

`_could_split_tablet_physically()`

```
Physical 切分条件 (基于 RowID 精确切分):
  keys_type == PRIMARY_KEYS
  keys_type == DUP_KEYS
  keys_type == UNIQUE_KEYS && is_preaggregation
  keys_type == AGG_KEYS    && is_preaggregation

不满足 → Logical 切分 (基于 ShortKey 近似切分)
```

原因：Physical 切分要求不同子区间可独立读取、无需合并/聚合。聚合表或 unique 表（merge-on-read）只有 is_preaggregation=true 时才能跳过聚合。

### 3.2 Physical 切分：基于 RowID 的精确切分

**数据模型**：

```
Tablet
  └── Rowset 0
  │     ├── Segment 0  (rows: 0 ~ 999999)
  │     ├── Segment 1  (rows: 0 ~ 999999)
  │     └── ...
  └── Rowset 1
        └── ...
```

遍历 tablet 的每个 rowset 的每个 segment，利用 segment 的 short-key index 确定每个 seek range 对应的 rowid 区间，然后按 `splitted_scan_rows` 切分出子区间。

**具体示例**：1 个 tablet、200 万行、dop=4、Physical 切分。此处 `splitted_scan_rows` 取 500,000 便于演示切分过程（实际值由 Section 4 的公式计算，约为 262,144）。tablet 有 1 个 rowset、2 个 segment（各 100 万行）。`try_get()` 被调用 4 次：

```
Segment 0 (1M rows):
  split morsel 1: rowid [0, 500000)
  split morsel 2: rowid [500000, 1000000)

Segment 1 (1M rows):
  split morsel 3: rowid [0, 500000)
  split morsel 4: rowid [500000, 1000000)
```

4 个 Driver 各拿到一个 split morsel，并行读取不同的 rowid 区间。实际使用 `splitted_scan_rows = 262,144` 时会产出约 8 个 split morsel，由 4 个 Driver 分摊。

**核心状态**（`PhysicalSplitMorselQueue`）：

```cpp
size_t _tablet_idx, _rowset_idx, _segment_idx;  // 当前位置
SparseRange<> _segment_scan_range;        // 当前 segment 的 rowid 区间集合
SparseRangeIterator<> _segment_range_iter; // 迭代器
size_t _num_segment_rest_rows;            // 当前 segment 剩余行数
```

**Morsel 产出流程**（`PhysicalSplitMorselQueue::try_get()` → `_try_get_split_from_single_tablet()`）：

`try_get()` 全程持有 `std::mutex`。存算分离表的切分调度在单个 IO task 内串行调用 `try_get()`，无并发争抢。

```
1. 初始化 RowidRangeOption（空）
2. 循环直到 num_taken_rows >= splitted_scan_rows：
   a. segment 未初始化或已耗尽 → _next_segment() + _init_segment()
   b. 从 _segment_range_iter 取出 splitted_scan_rows 行
   c. 尾部优化：segment 剩余行数 < splitted_scan_rows → 一次全部消费
   d. rowid_range->add(rowset, segment, range, is_first_split_of_segment)
   e. tablet 整体已耗尽 → 提前返回
3. 封装为 PhysicalSplitScanMorsel(RowidRangeOptionPtr) 返回
```

由于尾部优化，实际 morsel 大小可达 `2 * splitted_scan_rows - 1` 行。

**_init_segment()** 流程：解析 seek range → 加载 rowset/segment index（`rowset->load()`、`segment->load_index()`）→ 用 `_lower_bound_ordinal` / `_upper_bound_ordinal` 计算 rowid 区间 → 构建 SparseRange 迭代器。存算分离表用 `lake::TabletReader::parse_seek_range()`。

**错误处理**：存算分离表的切分调度采用 **graceful fallback**——TabletReader 预计算中任何 `try_get()` 失败时清空 `_split_tasks`，退回非切分模式继续读取（见 Section 5.2）。

### 3.3 Logical 切分：基于 ShortKey 的近似切分

**适用场景**：聚合表或 unique 表且 `is_preaggregation = false`（需要 merge-on-read）。

**核心思路**：选择 tablet 中**最大 rowset** 的 short-key index 作为切分参考，将 block 级别的 short-key 区间分配给不同的 morsel。其他 rowset 仍参与 merge，但读取范围被 short-key range 限定。

**核心状态**（`LogicalSplitMorselQueue`）：

```cpp
BaseRowset* _largest_rowset;              // 最大 rowset
SegmentGroupPtr _segment_group;           // 最大 rowset 的 segment 集合
int64_t _sample_splitted_scan_blocks;     // 每个 morsel 目标 block 数
```

**Morsel 产出流程**（`LogicalSplitMorselQueue::try_get()`）：

```
1. tablet 未初始化 → _init_tablet()
2. 循环直到 num_taken_blocks >= _sample_splitted_scan_blocks：
   a. 创建 lower_bound → 计算 STEP → 推进 → 创建 upper_bound
   b. 有效区间 → 添加为 ShortKeyRangeOption
   c. seek_range 耗尽 → 推进（单个 morsel 可跨 seek_range 边界）
3. 封装为 LogicalSplitScanMorsel(ShortKeyRangesOptionPtr) 返回
```

**自适应步进逻辑**（STEP 计算的三个特殊处理）：

1. **Duplicate short-key fallback**：upper bound 与 lower bound short-key 相同时，退化为每次推进 `_sample_splitted_scan_blocks / 4` 个 block
2. **尾部 block 均分**：最后一个 seek_range 剩余略大于目标值时，当前和下一个 morsel 平分
3. **跨 seek_range morsel**：block 数不够时继续从下一个 seek_range 取

> 源码中有详细示例注释（`LogicalSplitMorselQueue::try_get()` 内，搜索 "For example, assume that"）。

**_init_tablet()** 关键细节：
- `_create_segment_group()` 对 overlapped rowset 只取最大 segment（可能导致 morsel 大小不均匀）
- `_sample_splitted_scan_blocks = splitted_scan_rows × segment_group.num_blocks() / tablet_num_rows`（分母取 `max(1, tablet, largest_rowset, segment_group)` 行数，防御元数据不一致）

---

## 4. 切分判定逻辑

`LakeDataSourceProvider::_could_tablet_internal_parallel()`（存算一体表的 OlapScanNode 版本逻辑相同）：

```
前置拒绝:
  · use_pk_index = true → false（点查不需要并行）
  · !force_split && num_total_scan_ranges >= pipeline_dop → false（tablet 够多）

计算:
  num_table_rows = Σ tablet.num_rows()
  splitted_scan_rows = max_splitted_scan_bytes / estimated_scan_row_bytes
                       clamp to [min_splitted_scan_rows, max_splitted_scan_rows]
  scan_dop = num_table_rows / splitted_scan_rows
             clamp to [1, pipeline_dop]

判定:
  force_split → true
  scan_dop >= pipeline_dop  → true
  scan_dop >= min_scan_dop(默认 4)  → true
  otherwise                 → false
```

**具体示例（续）**：1 个 tablet、200 万行、`max_splitted_scan_bytes = 512MB`、`estimated_scan_row_bytes = 2048`。`splitted_scan_rows = 512MB / 2048 = 262144`，clamp 到 `[16384, 1048576]` → `262144`。`scan_dop = 2000000 / 262144 = 7`，clamp 到 `[1, 4]` → `4`。`scan_dop(4) >= pipeline_dop(4)` → 启用切分，产出 `2000000 / 262144 ≈ 8` 个 split morsel 分配给 4 个 Driver。

> 注：如果只有 100 万行，`scan_dop = 1000000 / 262144 = 3`，`scan_dop(3) < min_scan_dop(4)` 且 `< pipeline_dop(4)` → 不启用切分。

`estimated_scan_row_bytes` 是基于查询输出 schema 的**未压缩行大小估算**：对每个 slot 取 `slot_size() + type_estimated_overhead_bytes()`，不考虑压缩/编码/列裁剪。对于压缩率高的宽表可能导致切分粒度偏细。

`scan_dop` 通过 `MorselQueueFactory::size()` 传递到 `decompose_to_pipeline()`，直接决定创建的 Driver 数量。

---

## 5. 切分调度

存算分离内表通过 ConnectorScanNode → LakeDataSourceProvider 路径扫描。切分在运行时 IO task 内部由 `lake::TabletReader::open()` 预计算，然后通过外部注入分发到各 Driver。

### 5.1 Pipeline 构建

`LakeDataSourceProvider::convert_scan_range_to_morsel_queue()` 根据切分判定（Section 4）创建 DynamicMorselQueue 并设置 `has_more_from_split = true`。走 IndividualMorselQueueFactory 路径：`uniform_distribute_morsels()` 将原始 morsel 轮询分配到 per-driver DynamicMorselQueue（含初始无 morsel 的 Driver 也创建空 queue）。

> 注：`ScanNode::convert_scan_range_to_morsel_queue_factory()` 内部有一个 5 条件判定决定走 SharedMorselQueueFactory 还是 IndividualMorselQueueFactory（涉及 `always_shared_scan`、`enable_shared_scan`、`scan_dop`、queue 类型、morsel 数量 vs io_parallelism），完整逻辑见该方法源码。DynamicMorselQueue 属于 DYNAMIC 类型，满足 IndividualMorselQueueFactory 的条件。

### 5.2 TabletReader 预计算

当 Driver 拿到**原始 morsel**（`_split_context == nullptr`）时，`LakeConnectorChunkSource::open()` 传入 `need_split=true`。

`lake::TabletReader::open()` 内部：

1. 检查 `_rowsets.empty()` → 拒绝切分，fallback
2. 检查 `tablet_num_rows < splitted_scan_rows * lake_tablet_rows_splitted_ratio(1.5)` → 拒绝切分，fallback（防止小 tablet 数据倾斜）
3. 创建 PhysicalSplitMorselQueue 或 LogicalSplitMorselQueue（复用 Section 3 切分算法）
4. `try_get()` 循环预计算**所有** split → `_split_tasks[]`
5. 任何 `try_get()` 失败 → 清空 `_split_tasks`，**graceful fallback** 到非切分模式
6. `TabletReader::do_get_next()` 立即返回 EOF（不读数据）

**具体示例（续）**：1 tablet、200 万行、dop=4、Physical 切分。TabletReader 预计算约 8 个 split task（各约 25 万行的 RowidRangeOption），立即返回 EOF。

### 5.3 Split Morsel 注入与分发

`ConnectorChunkSource::buffer_next_batch_chunks_blocking()` 收到 EOF 时：

1. `get_split_tasks()` 提取预计算的 split
2. 将每个 split task 包装为带 `_split_context` 的 ScanMorsel
3. `scan_op->append_morsels()` → `IndividualMorselQueueFactory::next_driver_seq()` 轮询选择目标 Driver → 注入到该 Driver 的 DynamicMorselQueue 头部（优先消费）
4. `mark_split_source_morsel_finished()` 递减计数器（归零时设置 `has_more_from_split = false`）

**具体示例（续）**：8 个 split morsel 通过 `next_driver_seq()` 轮询分配到 Driver-0 ~ Driver-3，每个 Driver 得到 2 个。

### 5.4 Split Morsel 消费

当 Driver 拿到 **split morsel**（`_split_context != nullptr`）时：

- 设置 `rowid_range_option` 或 `short_key_ranges_option`
- `lake::TabletReader(need_split=false)` — 不再切分（递归防护：`_split_context != nullptr` 阻止再次触发切分）
- 正常读取子区间数据

### 5.5 端到端数据流

```
┌─────────────────── Pipeline 构建 ───────────────────┐
│  LakeDataSourceProvider::convert_scan_range_to_...() │
│       → DynamicMorselQueue (has_more_from_split=true)│
│  ScanNode::convert_scan_range_to_morsel_queue_factory│
│       → IndividualMorselQueueFactory (per-driver)    │
└──────────────────────────────────────────────────────┘

┌─────────────── 运行时：原始 morsel ─────────────────┐
│  Driver-0 拿到原始 morsel (_split_context == nullptr)│
│     │                                                │
│     ▼                                                │
│  [IO Thread] lake::TabletReader::open(need_split)    │
│     → 预计算所有 split → _split_tasks[]              │
│     → 立即返回 EOF                                   │
│     │                                                │
│     ▼                                                │
│  ConnectorChunkSource 收到 EOF                       │
│     → get_split_tasks() 提取 split                   │
│     → append_morsels() 注入 IndividualMorselQueue    │
│       Factory → next_driver_seq() 轮询分发           │
│     → mark_split_source_morsel_finished()            │
└──────────────────────────────────────────────────────┘

┌────────── 运行时：split morsel 并行消费 ────────────┐
│  Driver-0  Driver-1  Driver-2  Driver-3              │
│     │         │         │         │                  │
│     ▼         ▼         ▼         ▼                  │
│  从各自 DynamicMorselQueue 取 split morsel           │
│     │         │         │         │                  │
│     ▼         ▼         ▼         ▼                  │
│  [IO Thread] TabletReader(need_split=false)           │
│  使用 _split_context 的 rowid_range/short_key_ranges │
│  → 读取子区间数据 → chunk_buffer.put [kDirect]       │
│     │         │         │         │                  │
│     ▼         ▼         ▼         ▼                  │
│  pull_chunk() 从各自 sub_buffer 消费                 │
└──────────────────────────────────────────────────────┘
```

---

## 6. 辅助机制

### 6.1 TicketChecker：Split Morsel 的 Tablet 级 EOS 追踪

切分将一个 tablet 拆成多个 morsel，可能被不同 Driver 消费。下游算子需要知道整个 tablet 的所有 split morsel 都处理完毕。

`TicketChecker` 通过原子计数实现：

```
per tablet_id 的 Ticket (int64_t):
  |--all_ready_bit(1)--|--unused(3)--|--leave_count(30)--|--enter_count(30)--|
  bit 63                bits 60-62    bits 30-59          bits 0-29

注：源码注释（ticket_checker.h:44）写 "not_used(1bit)"，与实际不符。

enter(tablet_id, is_last_split): enter_count++，最后一个 split 设 all_ready_bit
leave(tablet_id): leave_count++，返回 (all_ready && enter_count == leave_count)
```

`ScanOperator::_should_emit_eos()` 中，只有 `_ticket_checker->leave()` 返回 true 才对外发出 EOS。

### 6.2 Unplug 调度策略

`ScanOperator::has_output()` 实现类似 Linux Block 层 Unplug 算法。前置检查：TopN filter back pressure — `ORDER BY ... LIMIT N` 的 TopN runtime filter 收敛后抑制产出。

Unplug 主逻辑：

```
        _unpluging ─── chunk_number > 0 → true; == 0 → false, 进入 plug 模式
        chunk_number >= threshold → _unpluging = true
        plug 模式:
           buffer_full? → 有 chunk 就消费
           IO task 全在运行 → 等待
           morsel queue 非空 → 拉起 IO task
           chunk_source 有 next_chunk → 继续扫描
           fallback → 有 chunk 就消费
```

`_buffer_unplug_threshold()` = clamp(buffer_capacity / dop / **2**, 1, **kIOTaskBatchSize=64**)

> 注：源码注释写 "/4" 和 "16"，与实现不符。以实现为准。

### 6.3 通知机制

IO task 完成时通知 Driver，有两条路径：

- **broadcast**（通知所有 Driver）：共享 OlapScanContext 时 `need_notify_all()` 可返回 true
- **单 Driver**（仅通知当前 Driver）：存算分离表的切分调度（per-driver context）走此路径

`active_inputs_empty_event()` 使用 compare-and-swap 实现 one-shot 语义。

### 6.4 IO Task 并发控制

每个 ScanOperator 最多 `available_pickup_morsel_count()` 个并发 IO task（默认 4）。`ConnectorScanOperator` override 实现自适应数量。

**有序扫描约束**：`sorted_by_keys_per_tablet = true` 时强制返回 1，同时影响 factory 选择阈值。

调度逻辑（`_try_to_trigger_next_scan()`）：

```
0. total_cnt = available_pickup_morsel_count()
1. running_io_tasks >= limit → 返回
2. unpluging 且 buffer 达阈值 → 返回
3. LIMIT 快速退出：任何空闲 chunk_source 已 reach_limit() → 返回
4. 遍历 chunk_source slot → 运行中跳过、有 next_chunk 立即触发、空闲加入调度队列
5. morsel_queue 就绪 → 对空闲 slot 调用 _pickup_morsel()
```

---

## 7. 关键配置参数

**BE 配置**（`config.h`，运行时可调）：

| 参数 | 含义 | 默认值 |
|------|------|--------|
| `tablet_internal_parallel_max_splitted_scan_bytes` | 每个 split morsel 的目标字节数 | 536870912 (512 MB) |
| `tablet_internal_parallel_min_splitted_scan_rows` | 切分行数下界 | 16384 |
| `tablet_internal_parallel_max_splitted_scan_rows` | 切分行数上界 | 1048576 |
| `tablet_internal_parallel_min_scan_dop` | 启用切分的最小 scan_dop | 4 |
| `io_tasks_per_scan_operator` | 每个 ScanOperator 的最大 IO 并发数 | 4 |
| `lake_tablet_rows_splitted_ratio` | TabletReader 切分行数阈值倍率 | 1.5 |

**Session 变量**（`SessionVariable.java`）：

| 参数 | 含义 | 默认值 | 备注 |
|------|------|--------|------|
| `enable_lake_tablet_internal_parallel` | 启用 tablet 内并行切分（存算分离） | true | 存算分离模式下通过 `RunMode.isSharedDataMode()` 路由到此变量 |
| `enable_tablet_internal_parallel` | 启用 tablet 内并行切分（存算一体） | true | |
| `tablet_internal_parallel_mode` | AUTO / FORCE_SPLIT | auto | INVISIBLE 变量，内部调试用 |

---

## 8. 与 OlapScan 路径的差异

存算一体内表走 OlapScanNode，使用不同的切分调度接入（内部产出式）。切分算法（Section 3）和判定逻辑（Section 4）完全共用，差异仅在调度层：

| 维度 | 存算分离（ConnectorScan） | 存算一体（OlapScan） |
|------|---|---|
| 切分时机 | IO task 内，TabletReader 一次性预计算 | pipeline 构建阶段，`SplitMorselQueue::try_get()` 按需切分 |
| MorselQueueFactory | IndividualMorselQueueFactory（per-driver DynamicMorselQueue） | SharedMorselQueueFactory（所有 Driver 共享 SplitMorselQueue） |
| 分发方式 | 外部注入：`append_morsels()` + `next_driver_seq()` 轮询 | 内部产出：Driver 竞争 `try_get()` |
| 并发模型 | `try_get()` 在单 IO task 内串行调用，无争抢 | `try_get()` 全程持 mutex，多 Driver 串行竞争；首次 segment 加载有 I/O 阻塞 |
| 附加阈值 | `lake_tablet_rows_splitted_ratio = 1.5` | 无 |
| 错误处理 | graceful fallback（退回非切分模式） | fail-stop（queue 永久耗竭） |
| 完成判定 | `mark_split_source_morsel_finished()` 归零 | morsel queue 空 |
| OlapScanContext | per-driver（独立 context） | 共享（`SharedMorselQueueFactory::is_shared()=true` → 1 个 context → broadcast 通知可触发） |

---

## 9. 结构性不变量

以下不变量由远端代码隐式保证，维护时需确保不被打破。

| 不变量 | 防止什么问题 | 保证机制 |
|--------|-------------|---------|
| **OlapScan / ConnectorScan 由 ScanNode 类型互斥** | 同一 tablet 被双重切分 | FE planner 决定 OlapScanNode vs ConnectorScanNode，不存在运行时交叉 |
| **切分调度递归防护** | 对 split morsel 再次触发切分 | `_split_context == nullptr` 检查：原始 morsel 触发切分，split morsel 直接读取 |
| **QueryCache 与 OlapScan SplitMorselQueue 互斥** | `unget()` 无锁写数据竞争；AR_SKIP ticket 泄漏 | QueryCache 要求 `scan_ranges_per_driver_seq` 非空 → IndividualMorselQueueFactory → `_lane_arbiter == nullptr` on Split path |
