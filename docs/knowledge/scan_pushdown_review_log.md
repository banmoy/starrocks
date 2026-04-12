# 存算分离 OlapTable Scan 下推机制 Review Log

Date: 2026-04-12

Target: docs/knowledge/scan_pushdown.md

---

## Round 1

**[Review]** Date: 2026-04-12

### P1-1: Bloom Filter 支持的谓词类型错误（Section 4.4.7, line 662）

文档声称："`=`, `IN`, `IS NOT NULL`"

代码实际：
- `=` → `ColumnEqPredicate::support_original_bloom_filter() = true`（`column_predicate_cmp.cpp:527`）✓
- `IN` → `ColumnInPredicate::support_original_bloom_filter() = true`（`column_in_predicate.cpp:142`）✓
- `IS NULL` → `ColumnIsNullPredicate::support_original_bloom_filter() = true`（`column_null_predicate.cpp:93`）— **实际支持但未列出**
- `IS NOT NULL` → `ColumnNotNullPredicate` 无 `support_original_bloom_filter()` override，默认 `false` — **实际不支持但被列出**

**修复**：将 line 662 从 "`=`, `IN`, `IS NOT NULL`" 改为 "`=`, `IN`, `IS NULL`"。

### P1-2: Runtime Filter Zone Map 下推的执行时机错误（Section 4.8, line 806）

文档声称："在 segment 初始化阶段，Runtime Filter 的 min/max 统计可用于 Zone Map 过滤"

代码实际：`_try_to_update_ranges_by_runtime_filter()` 在 `_do_get_next()` 中被调用（`segment_iterator.cpp` lines 1984, 2004, 2021），发生在 **chunk 读取阶段**，而非 segment 初始化阶段。这在架构上有明确原因：Runtime Filter 从 Join build 侧异步到达，在 `_init_internal()` 执行时可能尚未就绪，因此必须在每次 chunk 读取时动态检查。

**修复**：将 "在 segment 初始化阶段" 改为 "在 chunk 读取阶段（`_do_get_next()` 中）"，并补充说明原因——Runtime Filter 可能在 segment 初始化之后才从 build 侧到达，因此需要在每次读取时动态检查并增量应用 Zone Map 过滤。

### P2-1: 部分代码行号偏移

| 引用位置 | 文档值 | 实际值 | 偏差 |
|---------|--------|--------|------|
| `init_collector()`（Section 4.2） | lines 529-660 | lines 529-692 | 尾行偏移 32 |
| `PreAggregateTurnOnRule`（Section 2.5） | "lines 143-190" | `visitPhysicalOlapScan` lines 143-191 | 尾行偏移 1 |
| `_init_reader_params()`（Section 4.1） | lines 259-357 | lines 259-358 | 尾行偏移 1 |

不影响逻辑理解，属于 polish 级别。

### P2-2: 端到端示例数值不一致（Section 5, line 951）

索引过滤步骤表中，Bloom Filter 步骤剩余 ~15,000 行，下一步 Delete Vector 显示 14,000 行。但文本明确说 "DUP_KEYS 表无 Delete Vector"——该步骤不应产生任何过滤。剩余行数应保持 ~15,000。

**修复**：将 Delete Vector 行的剩余行数从 `14,000` 改为 `~15,000`。

### P2-3: "漏斗"核心隐喻缺少可视化（Section 0, line 19）

文档在 line 19 引入"漏斗"比喻——这是全文的组织核心——但仅用一句话描述。Section 1.1 的架构图是代码调用图，不是漏斗形数据流图。读者无法直观看到"宽输入 → 逐层过滤 → 窄输出"的形状以及各层的预期过滤效果。

**建议**：在 Section 0 "方案"之后或 Section 1.1 开头增加一个漏斗形数据流图，标注各过滤层名称和预期数据量缩减趋势。

### 已确认正确的关键声明

以下声明经代码验证均正确，无需修改：

- 全部 14 个 SessionVariable 默认值（Section 6.1）
- 全部 15 个 BE Config 默认值（Section 6.2）
- SegmentIterator 索引过滤顺序（Section 4.4, 11 步顺序完全匹配 `segment_iterator.cpp:883-899`）
- SegmentMetadataFilter 仅对排序键首列生效（`segment_metadata_filter.cpp:44`）
- OlapChunkSource 谓词分类逻辑（`olap_chunk_source.cpp:314`）
- Iterator 组装策略（DUP/PRIMARY → union, AGG/UNIQUE → merge+aggregate）
- Late Materialization 三阶段路径
- 谓词三分类（index-only / non-expression / expression, `segment_iterator.cpp:1360-1372`）
- Runtime Filter 每 16 chunk 重新评估选择率（`segment_iterator.cpp:1375`）
- TLakeScanNode 与 TOlapScanNode 字段差异
- 所有 SessionVariable getter 均返回存储值，无硬编码覆盖，无死路径

---

**[Response]** Date: 2026-04-12

### 评估方法

对每条 P1 级意见启动独立代码验证 agent，核对源码后决定是否接受。P2 级意见直接评估合理性。

### 评估结果

| 编号 | 评估 | 验证依据 | 行动 |
|------|------|---------|------|
| P1-1 | **接受** | `ColumnIsNullPredicate::support_original_bloom_filter()` 返回 `true`（`column_null_predicate.cpp:93`）；`ColumnNotNullPredicate` 无 override，继承基类 `false` | 已将 `=, IN, IS NOT NULL` 改为 `=, IN, IS NULL` |
| P1-2 | **接受** | `_try_to_update_ranges_by_runtime_filter()` 仅在 `do_get_next()` 中被调用（`segment_iterator.cpp:1984,2004,2021`），不在 `_init_internal()` 中 | 已修正描述为"chunk 读取阶段"，补充异步到达原因说明 |
| P2-1 | **接受** | 行号偏移确实存在 | 已修正 3 处行号 |
| P2-2 | **接受** | Bloom Filter 不支持范围谓词，无额外过滤；DUP_KEYS 无 Delete Vector | 已将 Delete Vector 行剩余行数从 14,000 改为 ~15,000 |
| P2-3 | **接受** | 漏斗可视化确实能增强核心隐喻的直观性 | 已在 Section 0 方案之后增加 ASCII 漏斗数据流图 |

### 具体修改

1. **Section 4.4.7 line 662**：`=, IN, IS NOT NULL` → `=, IN, IS NULL`
2. **Section 4.8 line 806-807**：重写为"在 chunk 读取阶段（`do_get_next()` 中）"，补充 Runtime Filter 异步到达的原因说明
3. **Section 4.2**：`init_collector()` 行号从 529-660 改为 529-692
4. **Section 2.5**：`PreAggregateTurnOnRule` 行号从 143-190 改为 143-191
5. **Section 4.1**：`_init_reader_params()` 行号从 259-357 改为 259-358
6. **Section 5 索引过滤表**：Bloom Filter 行修正为"无额外过滤，~15,000"；Delete Vector 行从 14,000 改为 ~15,000
7. **Section 0**：在漏斗描述之后增加 ASCII 漏斗数据流图

---

## Round 2

**[Review]** Date: 2026-04-12

### 修复验证

逐条验证 Round 1 全部 5 条修复：

| 编号 | 验证位置 | 结果 |
|------|---------|------|
| P1-1 | line 688: `=`, `IN`, `IS NULL` | ✓ 正确 |
| P1-2 | line 833: "在 chunk 读取阶段（`do_get_next()` 中，而非 `_init_internal()`）"，含异步到达原因说明 | ✓ 正确 |
| P2-1 | lines 302/507/538: 行号分别修正为 143-191, 259-358, 529-692 | ✓ 正确 |
| P2-2 | line 977: Delete Vector 剩余行数改为 ~15,000 | ✓ 正确 |
| P2-3 | lines 21-45: 新增 ASCII 漏斗数据流图，展示全量数据 → 分区/分桶裁剪 → Segment 元数据过滤 → 索引过滤 → 谓词/RF → Late Materialization → 最终输出 | ✓ 正确 |

### 结论

全部修复正确，无新引入问题。文档满足交付标准：

- 准确性：全部声明与当前源码一致
- 概念自包含：所有术语先定义后使用
- 架构上下文：Section 0 + 1.1 提供充分背景
- 认知坡度：层次递进，无跳跃
- 范围匹配：内容与标题一致
- 示例：Section 5 端到端示例贯穿关键环节
- 可视化：漏斗数据流图 + 架构调用图
- 活性：无已禁用路径被描述为活跃

Review 完成。

---
