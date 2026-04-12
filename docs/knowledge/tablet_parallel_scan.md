# 存算分离内表 Tablet 内并行切分机制

> **导读**：
> - **Section 1-2**：问题背景和概念模型——建立理解后续内容所需的术语和全局图景
> - **Section 3-4**：切分算法和判定逻辑——tablet 如何被切成子区间、什么条件下触发
> - **Section 5**：切分调度——从 pipeline 构建到 split morsel 并行消费的完整流程（**主线在此结束**）
> - **Section 6-9**：辅助机制、配置参数、与存算一体路径的差异、结构性不变量——参考性内容，按需阅读

## 1. 问题与方案概览

### 1.1 问题

Pipeline 执行引擎中，每个 ScanOperator 绑定一个 Driver，pipeline_dop 决定 Driver 数量。每个 tablet 对应一个扫描任务（称为 Morsel）。当 tablet 数量远小于 pipeline_dop 时，多数 Driver 分不到 Morsel，实际并行度退化。同时，如果单个 tablet 数据量很大，一个 Driver 串行读取整个 tablet 也会成为瓶颈。

### 1.2 核心思路

将一个 Morsel（整 tablet）拆成多个子区间，分配给多个 Driver 并行读取。这个过程分三步完成：

1. **切**：用切分算法（Physical 或 Logical）将 tablet 按行数目标拆成若干子区间描述
2. **分**：将这些子区间描述分发到各 Driver 的工作队列
3. **读**：各 Driver 从自己的队列取出子区间，并行读取对应的数据

### 1.3 架构背景

存算分离架构下，FE planner 将 lake 表的 scan 映射到 ConnectorScanNode（复用 connector 框架的 DataSource 抽象）。本文档描述的切分调度即基于此路径。

---

## 2. 概念模型

### 2.1 调度骨架

先回答四个基础问题，建立整体图景：

1. **系统调度的对象是什么？** 是 morsel——一个可被 Driver 执行的扫描任务。初始状态下每个 tablet 对应一个原始 morsel。
2. **这些任务放在哪里？** 放在 MorselQueue（队列）里，Driver 从中取出 morsel 执行。
3. **多个 Driver 各该用哪个 queue，morsel 怎么分进去？** 由 MorselQueueFactory（策略层）决定。它不是普通的"创建对象工厂"，而是组织 queue 拓扑并分配 morsel 的策略。
4. **存算分离路径具体采用什么策略？** IndividualMorselQueueFactory 给每个 Driver 一个 DynamicMorselQueue；原始 morsel 先被分配进去，运行时切出来的 split morsel 也继续注入。

**调度骨架图**（先看通用结构，再看本路径的具体实例化）：

```
通用结构：

  scan ranges → 原始 morsels（待分配的工作项）
                      │
                      ▼
              MorselQueueFactory
              （决定如何组织 per-driver queues，并把 morsels 分配进去）
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
    Driver 0 queue  Driver 1 queue  Driver 2 queue ...
          │           │           │
          ▼           ▼           ▼
    Driver 取出执行  Driver 取出执行  Driver 取出执行

存算分离路径的具体实例化：

  原始 morsels
        │
        ▼
  IndividualMorselQueueFactory
    · 初始分配原始 morsels
    · 后续注入 split morsels
        │
        ├── Driver 0 → DynamicMorselQueue
        ├── Driver 1 → DynamicMorselQueue
        ├── Driver 2 → DynamicMorselQueue
        └── Driver 3 → DynamicMorselQueue
                │
                ▼
        Driver 取 morsel → ChunkSource → IO task → ChunkBuffer → 消费
```

**术语定义**：

| 术语 | 含义 |
|------|------|
| **原始 morsel** | 扫描工作的调度单元，对应一个完整 tablet |
| **split morsel** | 携带子区间描述的 morsel，对应 tablet 的一个切片。Physical 切分的携带 `RowidRangeOption`，Logical 切分的携带 `ShortKeyRangesOption` |
| **split task** | 切分器在 TabletReader 内部产出的子区间描述。收到 EOF 后被包装为 split morsel 才能被 Driver 调度（见 Section 5.3） |
| **MorselQueue** | 装 morsel 的队列。本路径使用 DynamicMorselQueue（支持运行时追加新 morsel） |
| **MorselQueueFactory** | 决定 morsel 如何分配给各 Driver 的策略层。本路径使用 IndividualMorselQueueFactory（每个 Driver 独立队列） |
| **ChunkSource** | 由 morsel 创建，绑定到一个 IO task，负责从 tablet 的指定区间读取数据 |
| **ChunkBuffer** | 缓冲 IO task 产出的 chunk。kDirect 策略下每个 Driver 只消费自己的产出 |

### 2.2 切分、分发、消费：三类角色的接力

Section 1.2 提到的"切、分、读"三步，在实现中由三类角色接力完成。**三者不是并列的消费队列，而是前后衔接的职责分工**：切分器负责算出子区间，分发逻辑负责把子区间分给目标 Driver，运行时队列负责保存并供 Driver 消费。

| 角色 | 职责 | 具体对象 | Driver 是否直接消费 |
|------|------|---------|-------------------|
| **切分器** | 计算 tablet 的子区间（split task） | `PhysicalSplitMorselQueue` / `LogicalSplitMorselQueue` | 否（临时工具，用完即丢） |
| **分发逻辑** | 将 split task 包装为 split morsel，轮询注入各 Driver 队列 | `ConnectorChunkSource` + `IndividualMorselQueueFactory` | 否（协调层） |
| **运行时队列** | 存储 morsel，供 Driver 按需取用 | `DynamicMorselQueue`（每个 Driver 一个） | 是 |

各角色的详细说明：

**切分器**（PhysicalSplitMorselQueue / LogicalSplitMorselQueue）：
- 持有 tablet 的 rowset/segment 元数据，每次调用 `try_get()` 现场计算一个子区间描述（split task）
- 在 TabletReader 内部创建，预计算完所有 split task 后即丢弃

**分发逻辑**（ConnectorChunkSource + IndividualMorselQueueFactory）：
- 将切分器产出的 split task 包装为可调度的 split morsel（赋予 `_split_context`）
- 通过 `append_morsels()` + `next_driver_seq()` 轮询注入到各 Driver 的 DynamicMorselQueue

**运行时队列**（DynamicMorselQueue）：
- 每个 Driver 一个，内部是 deque，生命周期贯穿整个 scan
- `append_morsels()` 在头部插入（split morsel 优先消费）
- `try_get()` 从头部弹出供 Driver 使用

三者的协作流程：

```
[切] TabletReader::open() 内部
     创建切分器 → 循环 try_get() → 收集所有 split task
     切分器用完即丢
              │
              ▼
[分] ConnectorChunkSource 收到 EOF
     split task → 包装为 split morsel → append_morsels()
     轮询注入各 Driver 的 DynamicMorselQueue
              │
              ▼
[读] 各 Driver 从自己的 DynamicMorselQueue 取 split morsel
     → 并行读取不同子区间
```

### 2.3 `has_more_from_split`：防止提前退出

DynamicMorselQueue 上的 boolean flag。当为 true 时，即使队列已空，`ScanOperator::is_finished()` 也不会判定完成——因为还有 split morsel 尚未注入。

生命周期：pipeline 构建时设为 true → 每个原始 morsel 完成切分后原子递减计数器 → 所有原始 morsel 切分完成（计数器归零）时设为 false → scan 可正常结束。

---

## 3. 切分算法

本节描述切分器如何将 tablet 划分为子区间。切分器的 `try_get()` 每次产出一个子区间描述（split task）；这些 split task 随后会在 Section 5.3 被包装为可调度的 split morsel。

系统提供两种切分算法，分别适用于不同的表类型。两者的核心区别在于**切分依据不同**：

- **Physical 切分**：直接按 rowid 精确划分行区间。每个子区间独立读取，不需要跨区间合并。适用于不需要聚合/去重的表（主键表、明细表，或开启 preaggregation 的聚合/唯一表）。
- **Logical 切分**：按 short-key 索引的 block 边界近似划分 key 区间。各子区间仍需结合所有 rowset 做 merge-on-read，但读取范围被 key range 限定。适用于需要 merge-on-read 的聚合表或唯一表。

选择条件（`_could_split_tablet_physically()`）：

```
Physical 切分:
  keys_type == PRIMARY_KEYS 或 DUP_KEYS
  或 (UNIQUE_KEYS 或 AGG_KEYS) 且 is_preaggregation = true

否则 → Logical 切分
```

### 3.1 Physical 切分

遍历 tablet 的每个 rowset 的每个 segment，利用 segment 的 short-key index 确定每个 seek range 对应的 rowid 区间，然后按 `splitted_scan_rows` 切分出子区间。

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

**具体示例**：1 个 tablet、200 万行、dop=4。此处 `splitted_scan_rows` 取 500,000 便于演示（实际值由 Section 4 的公式计算，约为 262,144）。tablet 有 1 个 rowset、2 个 segment（各 100 万行）。切分器的 `try_get()` 被调用 4 次：

```
Segment 0 (1M rows):
  split task 1: rowid [0, 500000)
  split task 2: rowid [500000, 1000000)

Segment 1 (1M rows):
  split task 3: rowid [0, 500000)
  split task 4: rowid [500000, 1000000)
```

这 4 个 split task 经过 Section 5.3 的包装后成为 split morsel，分配给 4 个 Driver 并行读取。实际使用 `splitted_scan_rows = 262,144` 时会产出约 8 个 split task，由 4 个 Driver 分摊。

**核心状态**（`PhysicalSplitMorselQueue`）：

```cpp
size_t _tablet_idx, _rowset_idx, _segment_idx;  // 当前遍历位置
SparseRange<> _segment_scan_range;        // 当前 segment 的 rowid 区间集合
SparseRangeIterator<> _segment_range_iter; // 区间迭代器
size_t _num_segment_rest_rows;            // 当前 segment 剩余行数
```

**产出流程**（`PhysicalSplitMorselQueue::try_get()`）：

```
1. 初始化 RowidRangeOption（空）
2. 循环直到 num_taken_rows >= splitted_scan_rows：
   a. segment 未初始化或已耗尽 → _next_segment() + _init_segment()
   b. 从 _segment_range_iter 取出 splitted_scan_rows 行
   c. 尾部优化：segment 剩余行数 < splitted_scan_rows → 一次全部消费
   d. rowid_range->add(rowset, segment, range, is_first_split_of_segment)
   e. tablet 整体已耗尽 → 提前返回
3. 封装为一个 rowid 子区间描述（split task）返回
```

由于尾部优化，单个子区间覆盖的行数可达 `2 * splitted_scan_rows - 1`。

`try_get()` 全程持有 `std::mutex`。存算分离表的切分调度在单个 IO task 内串行调用，无并发争抢。

**`_init_segment()` 流程**：解析 seek range → 加载 rowset/segment index → 用 `_lower_bound_ordinal` / `_upper_bound_ordinal` 计算 rowid 区间 → 构建 SparseRange 迭代器。存算分离表用 `lake::TabletReader::parse_seek_range()`。

**错误处理**：存算分离表的切分调度采用 graceful fallback——TabletReader 预计算中任何 `try_get()` 失败时清空 `_split_tasks`，退回非切分模式继续读取（见 Section 5.2）。

### 3.2 Logical 切分

选择 tablet 中**最大 rowset** 的 short-key index 作为切分参考，将 block 级别的 short-key 区间划分为不同的子区间描述。其他 rowset 仍参与 merge，但读取范围被 short-key range 限定。

**核心状态**（`LogicalSplitMorselQueue`）：

```cpp
BaseRowset* _largest_rowset;              // 最大 rowset
SegmentGroupPtr _segment_group;           // 最大 rowset 的 segment 集合
int64_t _sample_splitted_scan_blocks;     // 每个子区间的目标 block 数
```

**产出流程**（`LogicalSplitMorselQueue::try_get()`）：

```
1. tablet 未初始化 → _init_tablet()
2. 循环直到 num_taken_blocks >= _sample_splitted_scan_blocks：
   a. 创建 lower_bound → 计算 STEP → 推进 → 创建 upper_bound
   b. 有效区间 → 添加为 ShortKeyRangeOption
   c. seek_range 耗尽 → 推进（单个子区间可跨 seek_range 边界）
3. 封装为一个 short-key 子区间描述（split task）返回
```

**自适应步进逻辑**（STEP 计算的三个特殊处理）：

1. **Duplicate short-key fallback**：upper bound 与 lower bound short-key 相同时，退化为每次推进 `_sample_splitted_scan_blocks / 4` 个 block
2. **尾部 block 均分**：最后一个 seek_range 剩余略大于目标值时，当前和下一个子区间平分
3. **跨 seek_range 子区间**：block 数不够时继续从下一个 seek_range 取

> 源码中有详细示例注释（`LogicalSplitMorselQueue::try_get()` 内，搜索 "For example, assume that"）。

**`_init_tablet()` 关键细节**：
- `_create_segment_group()` 对 overlapped rowset 只取最大 segment（可能导致子区间大小不均匀）
- `_sample_splitted_scan_blocks = splitted_scan_rows × segment_group.num_blocks() / tablet_num_rows`（分母取 `max(1, tablet, largest_rowset, segment_group)` 行数，防御元数据不一致）

---

## 4. 切分判定逻辑

并非所有查询都会触发切分。`LakeDataSourceProvider::_could_tablet_internal_parallel()` 根据数据量和配置判定是否值得切分：

```
前置拒绝:
  · use_pk_index = true → 不切分（点查不需要并行）
  · !force_split && num_total_scan_ranges >= pipeline_dop → 不切分（tablet 够多，不需要额外并行）

计算:
  num_table_rows = Σ tablet.num_rows()
  splitted_scan_rows = max_splitted_scan_bytes / estimated_scan_row_bytes
                       clamp to [min_splitted_scan_rows, max_splitted_scan_rows]
  scan_dop = num_table_rows / splitted_scan_rows
             clamp to [1, pipeline_dop]

判定:
  force_split → 切分
  scan_dop >= pipeline_dop  → 切分
  scan_dop >= min_scan_dop(默认 4)  → 切分
  otherwise                 → 不切分
```

**具体示例（续）**：1 个 tablet、200 万行、`max_splitted_scan_bytes = 512MB`、`estimated_scan_row_bytes = 2048`。`splitted_scan_rows = 512MB / 2048 = 262,144`（在 `[16384, 1048576]` 范围内）。`scan_dop = 2,000,000 / 262,144 ≈ 7`，clamp 到 4。`scan_dop(4) >= pipeline_dop(4)` → 启用切分，预计产生约 8 个子区间描述（split task）；这些描述随后会在 Section 5 中被包装并分配给 4 个 Driver。

> 注：如果只有 100 万行，`scan_dop = 3`，不满足 `>= pipeline_dop(4)` 也不满足 `>= min_scan_dop(4)` → 不切分。

`estimated_scan_row_bytes` 是基于查询输出 schema 的**未压缩行大小估算**（对每个 slot 取 `slot_size() + type_estimated_overhead_bytes()`），不考虑压缩/编码/列裁剪。对于压缩率高的宽表可能导致切分粒度偏细。

`scan_dop` 通过 `MorselQueueFactory::size()` 传递到 `decompose_to_pipeline()`，直接决定创建的 Driver 数量。

---

## 5. 切分调度

本节描述从 pipeline 构建到 split morsel 并行消费的完整流程。这是存算分离内表切分机制的**主线**。

### 5.1 Pipeline 构建：准备 per-driver 队列

`LakeDataSourceProvider::convert_scan_range_to_morsel_queue()` 根据切分判定（Section 4）创建 DynamicMorselQueue 并设置 `has_more_from_split = true`。走 IndividualMorselQueueFactory 路径：`uniform_distribute_morsels()` 将原始 morsel 轮询分配到 per-driver DynamicMorselQueue（含初始无 morsel 的 Driver 也创建空 queue）。

> 注：`ScanNode::convert_scan_range_to_morsel_queue_factory()` 内部有一个 5 条件判定决定 Factory 类型，完整逻辑见该方法源码。DynamicMorselQueue 属于 DYNAMIC 类型，满足 IndividualMorselQueueFactory 的条件。

### 5.2 切：TabletReader 预计算 split task

当 Driver 拿到**原始 morsel**（`_split_context == nullptr`）时，IO task 中 `LakeConnectorChunkSource::open()` 传入 `need_split=true`。

`lake::TabletReader::open()` 内部创建切分器并预计算所有子区间：

1. 检查 `_rowsets.empty()` → 拒绝切分，fallback
2. 检查 `tablet_num_rows < splitted_scan_rows * lake_tablet_rows_splitted_ratio(1.5)` → 拒绝切分，fallback（防止小 tablet 数据倾斜）
3. 创建 PhysicalSplitMorselQueue 或 LogicalSplitMorselQueue（复用 Section 3 切分算法）
4. 循环调用 `try_get()` 预计算**所有** split → 收集为内部的 `_split_tasks[]`
5. 任何 `try_get()` 失败 → 清空 `_split_tasks`，**graceful fallback** 到非切分模式
6. 切分器用完即丢。`TabletReader::do_get_next()` 立即返回 EOF（不读数据）

**具体示例（续）**：1 tablet、200 万行、dop=4、Physical 切分。TabletReader 预计算约 8 个 split task（各约 25 万行的 RowidRangeOption），立即返回 EOF。

### 5.3 分：从 split task 到 split morsel

TabletReader 产出的 `_split_tasks[]` 是内部的子区间描述（包含 RowidRangeOption 或 ShortKeyRangesOption），还不是可以被 Driver 调度的 morsel。需要经过一步**包装和分发**，将它们转变为可调度的 split morsel 并注入各 Driver 的队列。

`ConnectorChunkSource::buffer_next_batch_chunks_blocking()` 收到 TabletReader 的 EOF 后执行这一步：

1. `get_split_tasks()` 从 TabletReader 提取 `_split_tasks[]`
2. 将每个 split task 包装为带 `_split_context` 的 ScanMorsel——此时 split task 变为 split morsel，可以被 Driver 从 DynamicMorselQueue 中取出并调度执行
3. `scan_op->append_morsels()` → `IndividualMorselQueueFactory::next_driver_seq()` 轮询选择目标 Driver → 注入到该 Driver 的 DynamicMorselQueue 头部（优先消费）
4. `mark_split_source_morsel_finished()` 递减计数器（归零时设置 `has_more_from_split = false`）

**具体示例（续）**：8 个 split morsel 通过 `next_driver_seq()` 轮询分配到 Driver-0 ~ Driver-3，每个 Driver 得到 2 个。

### 5.4 读：Split Morsel 并行消费

当 Driver 拿到 **split morsel**（`_split_context != nullptr`）时：

- 从 `_split_context` 中取出 `rowid_range_option` 或 `short_key_ranges_option`
- `lake::TabletReader(need_split=false)` — 不再触发切分（递归防护：`_split_context != nullptr` 阻止再次进入 Section 5.2 的预计算流程）
- 正常读取子区间数据

### 5.5 端到端数据流

```
┌──────────────── Pipeline 构建 ─────────────────┐
│  每个 Driver 分到一个 DynamicMorselQueue        │
│  原始 morsel 轮询分配到各 Driver 的队列         │
│  has_more_from_split = true                     │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│ [切] Driver-0 拿到原始 morsel                   │
│      → IO task: TabletReader::open(need_split)  │
│      → 切分器预计算所有 split task               │
│      → 立即返回 EOF                              │
│                                                  │
│ [分] ChunkSource 收到 EOF                        │
│      → split task 包装为 split morsel            │
│      → append_morsels() 轮询注入各 Driver 队列  │
│      → mark_split_source_morsel_finished()       │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│ [读] Driver-0  Driver-1  Driver-2  Driver-3     │
│       │         │         │         │           │
│       ▼         ▼         ▼         ▼           │
│   从各自 DynamicMorselQueue 取 split morsel     │
│       │         │         │         │           │
│       ▼         ▼         ▼         ▼           │
│   IO task: TabletReader(need_split=false)        │
│   使用 split_context 的子区间描述读取数据       │
│       │         │         │         │           │
│       ▼         ▼         ▼         ▼           │
│   chunk → ChunkBuffer → Driver pull_chunk()     │
└─────────────────────────────────────────────────┘
```

---

> 以下为辅助机制、配置参数和扩展参考，不影响对主线流程的理解，按需阅读。

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

存算一体内表走 OlapScanNode，使用不同的切分调度接入。切分算法（Section 3）和判定逻辑（Section 4）完全共用，差异仅在调度层：

| 维度 | 存算分离（ConnectorScan） | 存算一体（OlapScan） |
|------|---|---|
| 切分时机 | IO task 内，TabletReader 一次性预计算 | pipeline 构建阶段，切分器的 `try_get()` 按需切分 |
| MorselQueueFactory | IndividualMorselQueueFactory（per-driver DynamicMorselQueue） | SharedMorselQueueFactory（所有 Driver 共享切分器） |
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
