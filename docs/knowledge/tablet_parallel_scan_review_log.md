# Tablet Parallel Scan Design Review Log

Date: 2026-04-11

Target: `docs/design/tablet_parallel_scan.md`
Review goal: Verify factual accuracy against current source code, assess completeness, identify structural issues.

---

## Round 1

**[Review]** Date: 2026-04-11

### Summary

The document is well-structured and provides a solid end-to-end walkthrough of the tablet parallel scan mechanism. The decision flow diagrams, ASCII data flow illustrations, and the Section 11 comparison table are particularly effective. However, cross-referencing against the current codebase reveals several factual errors, meaningful omissions, and a systemic line-number drift problem.

Issues are ordered by severity: factual errors that could mislead a reader first, then omissions that affect understanding, then maintenance/structural concerns.

### Issue 1: Section 8 — `_buffer_unplug_threshold()` formula is wrong

**Severity: High (factual error)**

The document states:

> `_buffer_unplug_threshold()` = clamp(buffer_capacity / dop / **4**, 1, **16**)

The actual implementation at `scan_operator.cpp:140-143`:

```cpp
size_t ScanOperator::_buffer_unplug_threshold() const {
    size_t threshold = buffer_capacity() / _dop / 2;  // divisor is 2, not 4
    threshold = std::max<size_t>(1, std::min<size_t>(kIOTaskBatchSize, threshold));
    return threshold;
}
```

Where `kIOTaskBatchSize = 64` (defined at `scan_operator.h:119`).

**Actual formula**: `clamp(buffer_capacity / dop / 2, 1, 64)`

Both the divisor (2 vs 4) and the upper bound (64 vs 16) are wrong. The document appears to have copied the stale comment at `scan_operator.cpp:165` which also says "/4" — but the comment and the implementation disagree, and the implementation is the source of truth.

Additionally, the Section 8 flow chart describes `has_output()` logic. The flow chart's branch `buffer_full? → NO, all IO tasks running → false` is an oversimplification. The actual code at `scan_operator.cpp:192` checks `is_running_all_io_tasks()` which compares against `_io_tasks_per_scan_operator`, not "all IO tasks running" in the global sense. The phrasing could mislead a reader into thinking it's checking a global condition.

### Issue 2: Section 7 — TicketChecker bit layout has wrong unused bit count

**Severity: Medium (factual error)**

The document states:

> `|--all_ready_bit(1)--|--unused(1)--|--leave_count(30)--|--enter_count(30)--|`

This sums to 62 bits, but `int64_t` is 64 bits. From `ticket_checker.h:45-48`:

```cpp
static constexpr int64_t ALL_READY_BIT = 1L << 63;       // bit 63
static constexpr int64_t ENTER_COUNT_BITS = (1L << 30) - 1;   // bits 0-29
static constexpr int LEAVE_COUNT_SHIFT = 30;
static constexpr int64_t LEAVE_COUNT_BITS = ((1L << 30) - 1) << LEAVE_COUNT_SHIFT;  // bits 30-59
```

Actual layout: `|--all_ready(1)--|--unused(3)--|--leave_count(30)--|--enter_count(30)--|`

Bits 60-62 are unused — 3 bits, not 1. Note: the code comment at `ticket_checker.h:44` also says "not_used(1bit)", so the document copied a stale code comment. The document should fix it and ideally note the code comment discrepancy so that gets fixed too.

### Issue 3: Line number references are systematically drifted

**Severity: Medium (maintenance)**

Nearly all `morsel.cpp` line references are off by 100+ lines, indicating the document was written against an older version of the code. Key examples:

| Document reference | Actual location | Delta |
|---|---|---|
| `PhysicalSplitMorselQueue::try_get()（morsel.cpp:577）` | morsel.cpp:460 | -117 |
| `_try_get_split_from_single_tablet()（morsel.cpp:515）` | morsel.cpp:395 | -120 |
| `_init_segment()（morsel.cpp:604）` | morsel.cpp:596 | -8 |
| `LogicalSplitMorselQueue::try_get()（morsel.cpp:813）` | morsel.cpp:691 | -122 |
| `_init_tablet()（morsel.cpp:1011）` | morsel.cpp:942 | -69 |
| `DynamicMorselQueue::append_morsels（morsel.cpp:1070）` | morsel.cpp:1069 | -1 |

References to `olap_scan_node.cpp` and `scan_node.cpp` are more stable (within a few lines), likely because those files changed less.

**Recommendation**: Use `ClassName::method_name()` as the primary anchor, with line numbers as supplementary hints only. Alternatively, pin references to git commits so drift is expected and explicit.

### Issue 4: Section 3.3 — Physical Split tail optimization not described

**Severity: Medium (omission)**

The `_try_get_split_from_single_tablet()` implementation at `morsel.cpp:434-438` contains an important optimization:

```cpp
if (_num_segment_rest_rows < _splitted_scan_rows) {
    // If there are too few rows left in the segment, take them all this time.
    _segment_range_iter.next_range(_splitted_scan_rows, &taken_range);
    _num_segment_rest_rows = 0;
}
```

When a segment's remaining rows are less than `_splitted_scan_rows`, all remaining rows are consumed in the current morsel rather than creating a tiny tail morsel. This avoids the overhead of an extra IO task for a negligible amount of data.

The document's Section 3.3 flow description lists "如果 tablet 整体已耗尽，提前返回" but doesn't mention this per-segment tail absorption. This is worth documenting because it means the actual morsel size can be up to `2 * _splitted_scan_rows - 1` rows — nearly double the target — which affects concurrency planning reasoning.

### Issue 5: Section 4.4 — LogicalSplit adaptive stepping logic under-described

**Severity: Medium (omission)**

The document summarizes the LogicalSplit morsel production loop as a simplified flow, but the actual implementation (`morsel.cpp:700-811`) has three non-trivial behaviors that are only alluded to:

1. **Duplicate short-key fallback**: When the upper bound short-key equals the lower bound, the iterator advances by `_sample_splitted_scan_blocks/4` at a time (`morsel.cpp:766-770`) instead of the full step. This prevents a morsel from covering more data than intended when many blocks share the same short-key prefix.

2. **Tail-block sharing**: When the last seek_range has slightly more blocks than `_sample_splitted_scan_blocks` remaining, the current and next morsel split the remainder equally (`morsel.cpp:775-778`). This prevents the last morsel from being tiny.

3. **Cross-seek-range morsel**: A single morsel can span the end of one seek_range and the beginning of the next (`morsel.cpp:792-798`). The document's Section 4.4 step f mentions "推进到下一个 seek_range" but doesn't emphasize that this happens **within** the same morsel.

The code has a detailed comment with examples (morsel.cpp:715-748) that illustrates these behaviors. Incorporating a similar example into the document would significantly improve understanding.

### Issue 6: Section 4.4 — `_create_segment_group` overlapped rowset handling not mentioned

**Severity: Low-Medium (omission)**

`LogicalSplitMorselQueue::_create_segment_group()` at `morsel.cpp:917-929`:

```cpp
if (rowset->is_overlapped()) {
    segments.emplace_back(_find_largest_segment(rowset));
} else {
    segments = rowset->get_segments();
}
```

When the largest rowset has overlapping segments, only the largest segment is used as the short-key reference. For non-overlapped rowsets, all segments participate. The document says "构建最大 rowset 的 segment 集合" without this distinction.

This matters because for overlapped rowsets, the block count used for split planning comes from a single segment, which may not be representative of the full rowset. This could lead to significantly uneven morsel sizes.

### Issue 7: Section 4.4 step 5 — `_sample_splitted_scan_blocks` formula imprecise

**Severity: Low-Medium (imprecision)**

The document says:

> `_sample_splitted_scan_blocks = splitted_scan_rows × total_blocks / tablet_num_rows`

The actual code at `morsel.cpp:977-980`:

```cpp
const auto tablet_num_rows = std::max<int64_t>({1, static_cast<int64_t>(_tablets[_tablet_idx]->num_rows()),
                                                _largest_rowset->num_rows(), _segment_group->num_rows()});
_sample_splitted_scan_blocks = _splitted_scan_rows * _segment_group->num_blocks() / tablet_num_rows;
```

Two inaccuracies:
- "total_blocks" is actually `_segment_group->num_blocks()` — blocks in the **largest rowset's segment group** only, not all rowsets' blocks.
- `tablet_num_rows` is `max(1, tablet.num_rows(), largest_rowset.num_rows(), segment_group.num_rows())`, not simply the tablet's row count. This defensive max() guards against metadata inconsistencies.

### Issue 8: Section 9 — `_try_to_trigger_next_scan` omits `reach_limit()` early return

**Severity: Low (omission)**

The document's Section 9 description of `_try_to_trigger_next_scan()` omits the `reach_limit()` early return at `scan_operator.cpp:349-353`:

```cpp
for (int i = 0; i < _io_tasks_per_scan_operator; i++) {
    if (!_is_io_task_running[i] && _chunk_sources[i] != nullptr && _chunk_sources[i]->reach_limit()) {
        return Status::OK();
    }
}
```

When any idle chunk source has hit the scan limit (from SQL `LIMIT` clause), no new IO tasks are submitted. This is the mechanism that prevents over-reading in LIMIT queries. Worth mentioning since LIMIT is a very common use case.

### Issue 9: Section 9 — `total_cnt` source not explained

**Severity: Low (imprecision)**

The document says "遍历所有 chunk_source slot" in step 3, implying the loop count is `io_tasks_per_scan_operator`. The actual code uses `total_cnt = available_pickup_morsel_count()` which for `ScanOperator` defaults to `_io_tasks_per_scan_operator` (`scan_operator.h:88`), but `ConnectorScanOperator` overrides it with adaptive IO task sizing (`connector_scan_operator.cpp:406`).

The loop's second pass (`morsel.cpp:370`) uses `size < total_cnt` as the bound. This means the number of new morsels picked up is limited by `available_pickup_morsel_count()`, not a fixed count. This is a minor extensibility point that the document could note, as it affects understanding of external table scan behavior.

### Issue 10: Structural — Section 2.4 flow chart could clarify decision semantics

**Severity: Low (clarity)**

The Section 2.4 flow chart presents 5 conditions as sequential YES/NO checks, each YES leading to `SharedMorselQueueFactory`. The actual code is a single conjunctive `if` statement (`scan_node.cpp:182-183`):

```cpp
if (!always_shared_scan() && !enable_shared_scan && scan_dop > 1 &&
    is_fixed_or_dynamic_morsel_queue && morsel_queue->num_original_morsels() <= io_parallelism) {
    // IndividualMorselQueueFactory
} else {
    // SharedMorselQueueFactory
}
```

The flow chart's semantics are correct (any one condition being true → Shared), but presenting them as sequential checks could imply short-circuit ordering matters. A brief note like "以下条件任一成立则进入 SharedMorselQueueFactory" would clarify.

---

**[Response]** Date: 2026-04-11

All 10 issues verified against current source code. Positions:

### Issue 1: Agreed, will fix

Confirmed `scan_operator.cpp:141` uses `/2` and `min(kIOTaskBatchSize=64, ...)`. Document's `/4` and `16` both wrong — copied from stale comment at `scan_operator.cpp:165`. Will correct formula and note the code comment discrepancy.

### Issue 2: Agreed, will fix

Confirmed bit layout: ALL_READY_BIT at bit 63, leave_count at bits 30-59, enter_count at bits 0-29 → bits 60-62 are 3 unused bits. Document and code comment both say 1 unused bit. Will fix document to 3 bits.

### Issue 3: Agreed, will adopt recommendation

Confirmed systematic drift (e.g., `try_get()` doc says 577, actual 460; `_try_get_split_from_single_tablet()` doc says 515, actual 395). Will switch to `ClassName::method_name()` as primary anchor, demote line numbers to supplementary hints.

### Issue 4: Agreed, will add

Confirmed tail optimization at `morsel.cpp:434-438`. Will add to Section 3.3, noting morsel size can reach up to `2 * splitted_scan_rows - 1`.

### Issue 5: Agreed, will add

All three behaviors confirmed: duplicate short-key fallback (`morsel.cpp:766-770`), tail-block sharing (`morsel.cpp:775-778`), cross-seek-range morsel. Will expand Section 4.4 with these details and reference the code's inline example at `morsel.cpp:715-748`.

### Issue 6: Agreed, will add

Confirmed `_create_segment_group()` at `morsel.cpp:919-923` selects only the largest segment for overlapped rowsets. Will add to Section 4.4's `_init_tablet()` description.

### Issue 7: Agreed, will fix

Confirmed `morsel.cpp:977-979`: uses `_segment_group->num_blocks()` (not all rowsets' blocks), and `tablet_num_rows = max(1, tablet.num_rows(), largest_rowset.num_rows(), segment_group.num_rows())`. Will correct the formula.

### Issue 8: Agreed, will add

Confirmed `reach_limit()` early return at `scan_operator.cpp:349-353`. Will add to Section 9 as a step before the main scheduling loop.

### Issue 9: Partially agreed, will note briefly

Confirmed `available_pickup_morsel_count()` default and `ConnectorScanOperator` override. Since document focuses on internal tables, will add a one-line note mentioning the extensibility point without expanding on connector logic.

### Issue 10: Agreed, will add clarifying note

Will add "以下条件任一成立则进入 SharedMorselQueueFactory" before the flow chart in Section 2.4.

---

## Round 2

**[Review]** Date: 2026-04-11

### Summary

Round 1 addressed factual accuracy and omissions. This round focuses on deeper concerns: concurrency semantics, error handling, cross-component interactions, and conceptual gaps that affect a reader's ability to reason about the system's behavior under pressure.

### Issue 11: SplitMorselQueue holds mutex during I/O operations

**Severity: High (design concern, should be documented)**

Both `PhysicalSplitMorselQueue::try_get()` and `LogicalSplitMorselQueue::try_get()` acquire a `std::mutex` for their entire duration. Inside this mutex, the following I/O operations can occur:

**Physical Split** (`_try_get_split_from_single_tablet` → `_init_segment`):
```cpp
// morsel.cpp:625 — inside mutex
RETURN_IF_ERROR(rowset->load());
// morsel.cpp:641 — inside mutex
RETURN_IF_ERROR(segment->load_index());
```

**Logical Split** (`try_get` → `_init_tablet`):
```cpp
// morsel.cpp:973 — inside mutex
RETURN_IF_ERROR(_largest_rowset->load());
// morsel.cpp:974 → _create_segment_group → morsel.cpp:926
RETURN_IF_ERROR(segment->load_index());
```

Since all drivers share one SplitMorselQueue via SharedMorselQueueFactory, this means:

1. When Driver-0 triggers `_init_segment()` or `_init_tablet()` for the first time on a new rowset/tablet, all other drivers are blocked on the mutex waiting for the I/O to complete.
2. Only one driver can produce a split morsel at a time — despite the goal being to increase parallelism.

The serialization is transient (subsequent calls to `try_get()` for the same segment/rowset don't repeat the I/O), but the document claims split "真正增加了 IO 并行度" (Section 11) without noting this serialization bottleneck during metadata loading. For tablets with many rowsets or cold segment metadata, this can cause noticeable startup latency.

The document should describe the concurrency model of SplitMorselQueue: a single mutex serializes morsel production, the I/O is amortized over the tablet's segments, and the actual read parallelism is achieved later when different drivers run their IO tasks on different sub-ranges concurrently.

### Issue 12: `_unget_morsel` is not concurrency-safe under SharedMorselQueueFactory

**Severity: Medium (potential data race, should be documented)**

`MorselQueue::unget()` writes `_unget_morsel` without any lock (`morsel.cpp:275-277`):

```cpp
void MorselQueue::unget(MorselPtr&& morsel) {
    _unget_morsel = std::move(morsel);
}
```

But `SplitMorselQueue::try_get()` reads `_unget_morsel` inside its mutex:

```cpp
StatusOr<MorselPtr> PhysicalSplitMorselQueue::try_get() {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_unget_morsel != nullptr) {
        return std::move(_unget_morsel);
    }
```

With SharedMorselQueueFactory, multiple drivers share the same SplitMorselQueue. The `unget()` is called from `ScanOperator::_pickup_morsel()` at `scan_operator.cpp:559` when the query cache lane arbiter returns `AR_BUSY`:

```cpp
if (acquire_result == query_cache::AR_BUSY) {
    _morsel_queue->unget(std::move(morsel));
    return Status::OK();
}
```

Scenario: Driver-A calls `unget()` (no lock) while Driver-B is inside `try_get()` (holding mutex but reading `_unget_morsel`). This is a data race on `_unget_morsel` — concurrent read and write of a non-atomic `MorselPtr`.

Worse scenario: Driver-A and Driver-C both call `unget()` concurrently — the second write overwrites the first, losing a morsel. Since `_inc_split()` was already called for both morsels inside `try_get()`, the TicketChecker's `enter_count` would be higher than the eventual `leave_count`, causing the tablet's EOS to never be emitted.

**Practical impact**: This only triggers when Split + SharedMorselQueueFactory + QueryCache lane arbiter are all active simultaneously. This may be a rare combination, but the document should note the concurrency constraint: `unget()` assumes single-writer semantics, which holds for IndividualMorselQueueFactory but not for SharedMorselQueueFactory.

### Issue 13: Query Cache interaction with Split morsels not discussed

**Severity: Medium (conceptual gap)**

`ScanOperator::_pickup_morsel()` (`scan_operator.cpp:554-600`) contains a non-trivial query cache interaction loop that directly affects split morsel lifecycle:

1. **Lane probing** (`AR_PROBE`): The cache operator probes whether the tablet is already cached. If hit, the morsel may be entirely skipped — `try_get()` is called again to get the next morsel.
2. **Delta rowsets** (`AR_IO`): For cache misses with partial staleness, the morsel's rowsets are replaced with delta rowsets (`morsel->set_delta_rowsets()`). This changes what the TabletReader actually reads.
3. **Lane busy** (`AR_BUSY`): The morsel is returned to the queue via `unget()` (Issue 12).
4. **Lane skip** (`AR_SKIP`): The morsel is discarded and a new one is obtained.

The document's Section 7 discusses TicketChecker as the EOS coordination mechanism for query cache, but doesn't mention the morsel-pickup phase where cache probing can skip, redirect, or return morsels. This is important because:

- A split morsel that gets skipped (`AR_PROBE` hit or `AR_SKIP`) still had `_inc_split()` called during `try_get()`. The corresponding `leave()` must still happen via the EOS chunk path, or the TicketChecker would stall. The mechanism for this — how a skipped morsel's ticket is reconciled — is not explained.

Actually, wait: skipping happens in `_pickup_morsel` AFTER `try_get()` returns the morsel. If `AR_SKIP` → `try_get()` again, the previous morsel is simply dropped (its `MorselPtr` goes out of scope). But `_inc_split()` was called for it inside the SplitMorselQueue's `try_get()`. The ticket checker has an extra `enter_count` with no corresponding `leave_count`. This would cause the tablet's EOS to never be emitted, unless there's a compensating mechanism I'm not seeing.

This is either a bug or there's a subtle invariant ensuring `AR_SKIP` cannot occur with SplitMorselQueue. The document should clarify this interaction.

### Issue 14: Error handling in split path — fail-stop semantics not documented

**Severity: Low-Medium (missing documentation)**

When `_init_segment()` fails in `PhysicalSplitMorselQueue::_try_get_split_from_single_tablet()` (`morsel.cpp:420-423`):

```cpp
if (auto status = _init_segment(); !status.ok()) {
    // Morsel_queue cannot generate morsels after errors occurring.
    _tablet_idx = _tablets.size();
    return status;
}
```

The queue is permanently exhausted (`_tablet_idx = _tablets.size()`), and the error propagates up through `try_get()` → `_pickup_morsel()` → `_try_to_trigger_next_scan()` → `pull_chunk()`.

Similarly for `LogicalSplitMorselQueue::_init_tablet()`, though the error path is through the `RETURN_IF_ERROR` macro chain.

The design choice is **fail-stop**: any segment/rowset loading failure aborts the entire scan, even if other tablets could still be scanned successfully. This is a reasonable choice (partial results are usually unacceptable for SQL queries), but the document doesn't mention error semantics at all. A brief note on error handling would help readers understand the resilience model.

### Issue 15: `scan_dop` determines final scan driver count — connection not explicit

**Severity: Low-Medium (conceptual gap)**

The document carefully explains how `scan_dop` is computed in Section 2.2 (clamped to `[1, pipeline_dop]` based on row estimates). But it doesn't explicitly state what `scan_dop` **controls** in the SharedMorselQueueFactory path.

At `scan_node.cpp:195`:
```cpp
return std::make_unique<pipeline::SharedMorselQueueFactory>(std::move(morsel_queue), scan_dop);
```

`SharedMorselQueueFactory::size()` returns `scan_dop`, which determines the number of scan operators (and thus drivers) created by the pipeline framework. This means:

- If `scan_dop = 3` and `pipeline_dop = 8`, only 3 drivers participate in the scan, not 8.
- The split mechanism increases morsel count to feed these 3 drivers, not to feed all 8.

The document's Section 1 motivating example ("3 个 tablet、pipeline_dop = 8 → 只有 3 个 Driver 有数据可读") implies that split solves this by enabling all 8 drivers. But if `scan_dop` computes to, say, 5, then only 5 drivers scan. The document should clarify that split increases **effective parallelism up to `scan_dop`**, not necessarily up to `pipeline_dop`.

Exception: when `enable_shared_scan = true` and `use_default_dop_when_shared_scan = true` (scan_node.cpp:193), `scan_dop` is overridden to `pipeline_dop`. But the Split path doesn't use shared scan, so this override doesn't apply.

### Issue 16: Notification mechanism — two-path dispatch not fully described

**Severity: Low (omission)**

Section 5.6 describes `need_notify_all()` and the broadcast notification. But the actual notification mechanism has two paths (`scan_operator.h:108-115`):

```cpp
auto defer_notify(NotifyAll notify_all) {
    return DeferOp([this, notify_all]() {
        if (notify_all()) {
            _source_factory()->observes().notify_source_observers();  // broadcast to all drivers
        } else {
            _observable.notify_source_observers();  // notify only this driver
        }
    });
}
```

When `need_notify_all()` returns false (the common case for non-shared-scan), only the current driver's observer is notified. The document describes only the broadcast path, giving the impression that IO task completion always wakes all drivers. The single-driver path is the default behavior for the Split path (where `shared_scan = false`).

Additionally, `active_inputs_empty_event()` (`olap_scan_context.h:139-144`) uses a compare-and-swap that makes it a **one-shot event**:

```cpp
bool active_inputs_empty_event() {
    if (!_active_inputs_empty.load(std::memory_order_acquire)) {
        return false;
    }
    bool val = true;
    return _active_inputs_empty.compare_exchange_strong(val, false);
}
```

This means the "all active inputs are empty" notification fires exactly once. If multiple IO tasks complete near-simultaneously, only the first one triggers the broadcast. This is intentional (one broadcast is sufficient to wake all drivers), but the one-shot semantics are a subtle design detail worth documenting.

---

**[Response]** Date: 2026-04-11

### Issue 11: Agreed, will document concurrency model

Confirmed: `try_get()` holds `_mutex` throughout, including `rowset->load()` and `segment->load_index()` I/O. Will add a note to Section 3.3/4.4 about the mutex-serialized morsel production, and qualify Section 11's "真正增加了 IO 并行度" — the parallelism is in the subsequent IO tasks reading different sub-ranges, not in morsel production itself.

### Issue 12: Disagreed on the bug, agreed on documenting the invariant

Verified structural invariant: `_lane_arbiter` is only set when QueryCache is enabled. QueryCache requires `scan_ranges_per_driver_seq` to be non-empty (`fragment_executor.cpp:542-543`: if empty → `set_enable_cache(false)`). When `scan_ranges_per_driver_seq` is non-empty, `convert_scan_range_to_morsel_queue_factory()` takes the else branch (`scan_node.cpp:197`) → `IndividualMorselQueueFactory`, never `SharedMorselQueueFactory` with `SplitMorselQueue`.

Therefore `_lane_arbiter == nullptr` when using SplitMorselQueue — the `unget()` path is never reached. The data race is theoretical, not practical. Will document this structural invariant (QueryCache and SplitMorselQueue are mutually exclusive paths).

### Issue 13: Disagreed on the bug, agreed on documenting the invariant

Same structural invariant as Issue 12. When `_lane_arbiter == nullptr`, the entire cache interaction loop (`scan_operator.cpp:554-600`) is skipped. AR_SKIP cannot occur with SplitMorselQueue — no ticket leak. Will note that the lane arbiter / query cache interaction is structurally excluded from the Split path.

### Issue 14: Agreed, will add brief note

Confirmed fail-stop semantics: `_tablet_idx = _tablets.size()` permanently exhausts the queue on error. Will add a brief error-handling note to Section 3.

### Issue 15: Agreed, will clarify

Confirmed: `SharedMorselQueueFactory::size()` returns `scan_dop`, and `decompose_to_pipeline()` uses this as `dop` (`olap_scan_node.cpp:908`). Split increases effective parallelism up to `scan_dop`, not necessarily `pipeline_dop`. Will fix Section 1's motivating example and add explicit explanation of what `scan_dop` controls.

### Issue 16: Partially agreed

The two-path dispatch and one-shot CAS semantics are worth documenting. However, the review's claim that "non-shared-scan common case is single-driver notification" is inaccurate for the Split path: `shared_morsel_queue=true` (`morsel_queue_factory->is_shared()=true` for `SharedMorselQueueFactory`) → 1 shared `OlapScanContext` → `only_one_observer()=false` → broadcast path can trigger via `active_inputs_empty_event()`. Will document the two-path dispatch with accurate description of when each path activates.

---

## Round 3

**[Review]** Date: 2026-04-11

### Summary

This round addresses the developer's pushbacks on Issues 12, 13, and 16 from Round 2, and proposes a documentation recommendation emerging from the analysis.

### Issue 12/13 Follow-up: Correction accepted — structural invariant confirmed

I verified the claimed invariant through the full code path:

1. `fragment_executor.cpp:542-543` — `scan_ranges_per_driver_seq.empty()` → `set_enable_cache(false)`. QueryCache requires per-driver scan ranges.
2. `scan_node.cpp:197-224` — when `scan_ranges_per_driver_seq` is non-empty, the else branch always returns `IndividualMorselQueueFactory` (line 221) or `BucketSequenceMorselQueueFactory` (line 218), never `SharedMorselQueueFactory`.
3. `scan_node.cpp:208` — `queue->set_has_more_from_split(false)` is explicitly called in this path, disabling split even if a SplitMorselQueue was created.
4. `pipeline_driver.cpp:190-216` — `_lane_arbiter` is only set when cache is enabled.

**Conclusion**: QueryCache and SplitMorselQueue + SharedMorselQueueFactory are structurally mutually exclusive. `_lane_arbiter == nullptr` when using the Split path. The `unget()` data race (Issue 12) and AR_SKIP ticket leak (Issue 13) are structurally impossible in current code.

My original analysis was narrowly correct about the raw concurrency hazard but failed to account for the higher-level structural constraint. The code relies on an implicit invariant rather than a local safety property — this is exactly the kind of non-local reasoning that a design document should make explicit. Documenting this invariant (as already agreed) will prevent future maintainers from breaking it unknowingly.

### Issue 16 Follow-up: Correction accepted

Verified the notification chain for the Split path:

1. `olap_scan_node.cpp:909` — `shared_morsel_queue = morsel_queue_factory->is_shared()` → `true` for SharedMorselQueueFactory.
2. `olap_scan_context.h:196` — `_contexts(shared_morsel_queue ? 1 : dop)` → 1 shared OlapScanContext.
3. `olap_scan_operator.cpp:99` — each driver calls `_ctx->attach_observer(state, observer())` on the shared context.
4. `olap_scan_context.h:138` — `only_one_observer()` returns false when multiple drivers are attached.

My original claim in Issue 16 — "non-shared-scan common case is single-driver notification" — was incorrect for the Split path. The Split path uses SharedMorselQueueFactory → `shared_morsel_queue=true` → 1 shared context → multiple observers → broadcast path is available. The single-driver path only applies when `IndividualMorselQueueFactory` is used (where each driver has its own context with exactly one observer).

The corrected picture: for the Split path, `need_notify_all()` can return true via `!only_one_observer() && active_inputs_empty_event()` OR `has_full_events()`. The broadcast occurs when the last active input detaches (once, via CAS) or when the buffer limiter has full events.

### Issue 17 (new): Recommend documenting the three structural exclusion invariants

**Severity: Low (documentation recommendation)**

The Round 2 exchange surfaced three non-obvious structural invariants that protect the system's correctness but are not documented anywhere:

| Invariant | What it prevents | Code evidence |
|---|---|---|
| QueryCache requires `scan_ranges_per_driver_seq` non-empty | SplitMorselQueue + QueryCache lane arbiter co-occurring | `fragment_executor.cpp:542`, `scan_node.cpp:208` |
| Split + SharedScan are mutually exclusive | Dual parallelism mechanisms with conflicting assumptions | `scan_node.cpp:191` (`set_has_more_from_split(false)`) |
| Split path with SharedMorselQueueFactory → shared OlapScanContext | Misunderstanding notification behavior for Split | `olap_scan_context.h:196`, `olap_scan_node.cpp:909` |

The first invariant was proposed in Issues 12/13. The second is already well-documented in Section 1. The third emerged from Issue 16.

A brief "Structural Invariants" or "Non-obvious Constraints" section listing these (with code pointers) would help future maintainers understand which code paths are guaranteed not to co-occur and why. This is especially valuable because these invariants are enforced by distant code (e.g., FE scan range distribution → BE factory selection), not by local assertions.

---

**[Response]** Date: 2026-04-11

### Issue 12/13 Follow-up: Acknowledged

No further action needed — the structural invariant is already documented in Section 6.1 of the design document.

### Issue 16 Follow-up: Acknowledged

No further action needed — Section 5.6 already reflects the corrected notification behavior for all three scenarios (shared scan / split / independent).

### Issue 17: Agreed, will add consolidated section

The three invariants are currently scattered across Section 1, Section 5.6, and Section 6.1. Will add a Section 12 "Structural Invariants" that consolidates them with code pointers, making non-local constraints visible at a glance.

---

## Round 4

**[Review]** Date: 2026-04-11

### Summary

Final round. Rounds 1-3 covered correctness, concurrency, and structural invariants. This round addresses the configuration reference (Section 10) and a few remaining gaps in the decision flow narrative.

### Issue 18: Section 10 — all default values are listed as "-", but they all have concrete defaults

**Severity: Medium (factual omission)**

Section 10's table lists "-" for every default value. The actual defaults from `config.h` and `SessionVariable.java`:

| Parameter | Document default | Actual default | Source |
|---|---|---|---|
| `tablet_internal_parallel_max_splitted_scan_bytes` | - | 536870912 (512 MB) | `config.h:1063` |
| `tablet_internal_parallel_min_splitted_scan_rows` | - | 16384 | `config.h:1059` |
| `tablet_internal_parallel_max_splitted_scan_rows` | - | 1048576 | `config.h:1061` |
| `tablet_internal_parallel_min_scan_dop` | - | 4 | `config.h:1067` |
| `io_tasks_per_scan_operator` | 4 | 4 | `config.h:1166` (correct) |
| `use_default_dop_when_shared_scan` | - | true | `config.h:1055` |
| `enable_shared_scan` (session) | false | false | `SessionVariable.java:1225` (correct) |
| `enable_tablet_internal_parallel` (session) | true | true | `SessionVariable.java:1214` (correct) |
| `tablet_internal_parallel_mode` | AUTO / FORCE_SPLIT | "auto" | `SessionVariable.java:1222` |

Only `io_tasks_per_scan_operator`, `enable_shared_scan`, and `enable_tablet_internal_parallel` have their defaults filled in. The rest show "-". These are the most important parameters for understanding the system's default behavior — for example, the default `min_scan_dop = 4` means split won't activate unless the estimated row count produces at least 4 parallel tasks, which directly affects the threshold example in Section 1.

### Issue 19: Section 10 — missing `enable_lake_tablet_internal_parallel` session variable

**Severity: Medium (omission for a document titled "存算分离内表")**

The document's title is "存算分离内表 Tablet 并行 Scan 机制", but it doesn't mention the `enable_lake_tablet_internal_parallel` session variable which specifically controls the feature for shared-data mode.

From `SessionVariable.java:6206-6209`:

```java
if (RunMode.isSharedDataMode()) {
    tResult.setEnable_tablet_internal_parallel(enableLakeTabletInternalParallel);
} else {
    tResult.setEnable_tablet_internal_parallel(enableTabletInternalParallel);
}
```

In shared-data mode, `enable_lake_tablet_internal_parallel` (default: `true`) is used instead of `enable_tablet_internal_parallel`. These are independent session variables — a user could disable `enable_tablet_internal_parallel` while leaving the lake variant enabled, or vice versa. The document should mention this distinction since it directly controls whether the mechanisms described in Sections 2-4 are activated in the target environment (shared-data).

### Issue 20: Section 2.2 — `estimated_scan_row_bytes` is used but not explained

**Severity: Low-Medium (clarity gap)**

The `_could_tablet_internal_parallel()` formula in Section 2.2 uses `estimated_scan_row_bytes`:

> `splitted_scan_rows = max_splitted_scan_bytes / estimated_scan_row_bytes`

But the document doesn't explain what this value is or how it's computed. From `olap_scan_node.cpp:843-847`:

```cpp
for (const auto& slot : slots) {
    size_t field_bytes = std::max<size_t>(slot->slot_size(), 0);
    field_bytes += type_estimated_overhead_bytes(slot->type().type);
    _estimated_scan_row_bytes += field_bytes;
}
```

It's a schema-based estimate: the sum of each column's `slot_size()` plus a per-type overhead. This does NOT account for compression, encoding efficiency, or column pruning in the storage layer — it's an uncompressed row size estimate based on the query's output schema.

This matters because the estimate directly controls split granularity: a wide table with many VARCHAR columns will have a large `estimated_scan_row_bytes`, producing smaller `splitted_scan_rows` and thus more split morsels. If the data is heavily compressed, the actual IO per morsel will be much less than 512 MB, potentially creating too-fine splits.

A brief note on how `estimated_scan_row_bytes` is derived and its limitations would help readers reason about when the split granularity might be suboptimal.

### Issue 21: Section 2.1 — pre-split morsel sorting not mentioned

**Severity: Low (omission)**

Before the split decision in `convert_scan_range_to_morsel_queue()`, morsels are sorted in two cases (`olap_scan_node.cpp:425-443`):

1. **`partition_order_hint`** — sorts morsels by `partition_id` in ascending or descending order. This is used for ordered partition scans.
2. **`output_chunk_by_bucket`** — sorts morsels by `owner_id` (tablet_id). This is used for bucket-ordered output.

These sorts happen BEFORE the SplitMorselQueue is created, meaning the order of `_morsels[]` in the SplitMorselQueue is determined by these sorts. Since `SplitMorselQueue::_tablet_idx` advances sequentially through `_morsels[]`, the sort order determines which tablets are split first.

For the `partition_order_hint` case, this ensures that ordered scans process partitions in the requested order even with split morsels. The document doesn't mention this, which could lead to confusion about whether split preserves partition ordering.

### Issue 22: `tablet_internal_parallel_mode` is an INVISIBLE session variable

**Severity: Low (context)**

The document lists `tablet_internal_parallel_mode` with values `AUTO / FORCE_SPLIT` in Section 10 without noting that this is an INVISIBLE session variable (`SessionVariable.java:1221`, `flag = VariableMgr.INVISIBLE`). This means it's not shown by `SHOW VARIABLES` and is intended as an internal tuning knob, not a user-facing configuration.

This context helps readers understand that FORCE_SPLIT is a debugging/testing tool rather than a production configuration option.

---

**[Response]** Date: 2026-04-11

All 5 issues verified against source. Positions:

### Issue 18: Agreed, will fill in defaults

Confirmed all defaults from `config.h` and `SessionVariable.java`. Will fill in concrete values.

### Issue 19: Agreed, will add

Confirmed `enableLakeTabletInternalParallel` at `SessionVariable.java:1217` (default true), and `SessionVariable.java:6206-6209` routing logic. Will add to Section 10 with explanation of the shared-data mode routing.

### Issue 20: Agreed, will add brief note

Confirmed schema-based estimate at `olap_scan_node.cpp:844-847`. Will add a note to Section 2.2 explaining the derivation and its limitations (no compression/encoding awareness).

### Issue 21: Agreed, will add

Confirmed pre-split sorting at `olap_scan_node.cpp:425-443`. Will add to Section 2.1.

### Issue 22: Agreed, will annotate

Confirmed `VariableMgr.INVISIBLE` at `SessionVariable.java:1221`. Will mark in Section 10 table.

---

## Round 5

**[Review]** Date: 2026-04-11

### Summary

Rounds 1-4 reviewed the document's content against the OlapScan code path. This round examines the lake ConnectorScan path and finds a second split mechanism that the document's scoping doesn't account for.

### Issue 23: Lake ConnectorScan has a reader-level split mechanism — undocumented and unscoped

**Severity: High (scoping gap for a document titled "存算分离内表")**

The document describes the morsel-queue-level split (SplitMorselQueue → SharedMorselQueueFactory) as THE tablet parallel scan mechanism. But for lake (shared-data) tables accessed via the ConnectorScan path, there is a **second, independent split mechanism** inside `lake::TabletReader::open()` (`lake/tablet_reader.cpp:135-215`).

**How it works**:

At `lake_connector.cpp:458-465`, when a LakeConnectorChunkSource opens the reader:

```cpp
bool need_split = _provider->could_split() && _split_context == nullptr;
// ...
ASSIGN_OR_RETURN(_reader,
    _tablet.new_reader(std::move(child_schema), need_split, _provider->could_split_physically(), ...));
```

When `need_split=true`, `lake::TabletReader::open()` does the following:

1. Creates a SplitMorselQueue **inside the reader** (`tablet_reader.cpp:169-178`)
2. Applies a secondary row threshold: `tablet_num_rows < splitted_scan_rows * lake_tablet_rows_splitted_ratio(1.5)` → rejects split (`tablet_reader.cpp:158`)
3. Calls `try_get()` in a loop to pre-compute ALL split tasks into `_split_tasks` (`tablet_reader.cpp:187-215`)
4. On error, falls back to non-split mode (`tablet_reader.cpp:194-196`)

**Key differences from the morsel-queue-level split**:

| Aspect | Morsel-queue-level (documented) | Reader-level (undocumented) |
|---|---|---|
| Where | `SplitMorselQueue::try_get()` during morsel pickup | `lake::TabletReader::open()` during IO task |
| Granularity | One IO task per split morsel | All splits pre-computed within one IO task |
| Parallelism | Different drivers process different splits | Same driver processes all splits sequentially |
| Secondary threshold | None | `lake_tablet_rows_splitted_ratio = 1.5` |
| Error handling | Fail-stop (queue exhausted) | Graceful fallback to non-split |
| Trigger | `_split_context == nullptr` (original morsel) | Morsel-queue-level split already produced `_split_context != nullptr` |

The mutual exclusion condition `_split_context == nullptr` at `lake_connector.cpp:458` ensures the two mechanisms don't overlap — if a morsel already came from the morsel-queue-level split (has `_split_context`), the reader-level split is skipped.

**Why this matters**: The document's title is "存算分离内表 Tablet 并行 Scan 机制". A reader encountering this document for lake tablet splitting would get an incomplete picture. They might assume that if `_could_tablet_internal_parallel()` returns false (e.g., small table), no splitting occurs. In fact, the ConnectorScan path can still trigger reader-level splitting for the same table.

**Recommendation**: Either:
- (a) Add a Section 0 scoping note that explicitly limits the document to the OlapScan path's morsel-queue-level split, and references the reader-level split as "out of scope, see `lake/tablet_reader.cpp`", or
- (b) Add a Section covering the reader-level split as the "third mechanism" alongside morsel-queue-level split and shared scan.

### Issue 24: Section 10 — missing `lake_tablet_rows_splitted_ratio` config

**Severity: Medium (missing config for a lake-focused document)**

`lake_tablet_rows_splitted_ratio` (default: 1.5, `config.h:1070`) is the secondary split threshold used by `lake::TabletReader`. When `tablet_num_rows < splitted_scan_rows * 1.5`, the reader rejects splitting to avoid data skew between tablets.

This config directly affects splitting behavior for lake tablets and is defined immediately after the other split-related configs in `config.h:1069-1070`:

```cpp
// Only the num rows of lake tablet less than lake_tablet_rows_splitted_ratio * splitted_scan_rows,
// than the lake tablet can be splitted.
CONF_mDouble(lake_tablet_rows_splitted_ratio, "1.5");
```

Even if the reader-level split (Issue 23) is scoped out, this config also interacts with the `_init_segment`/`_init_tablet` path since `splitted_scan_rows` is a shared parameter. It should be listed in Section 10.

### Issue 25: Section 8 — TopN filter back pressure omitted from `has_output()` description

**Severity: Low (omission)**

Section 8's `has_output()` flow chart starts with the unplug logic. But the actual code at `scan_operator.cpp:159-161` has a TopN filter check before the unplug logic:

```cpp
if (!_morsel_queue->empty() && _topn_filter_back_pressure && _topn_filter_back_pressure->should_throttle()) {
    return false;
}
```

When TopN runtime filters (e.g., from `ORDER BY ... LIMIT N` queries) are active, the scan operator is throttled to avoid producing rows that will ultimately be filtered out. This can completely suppress `has_output()` even when the morsel queue has work and the buffer has capacity.

This is relevant because it can mask the unplug behavior described in Section 8. A reader debugging a "scan operator not producing output" scenario would not find this check in the document.

---

**[Response]** Date: 2026-04-11

### Issue 23: Agreed, will add scoping note (option a)

Confirmed reader-level split in `lake::TabletReader::open()` (`lake/tablet_reader.cpp:135-215`): creates SplitMorselQueue inside the reader, pre-computes all split tasks, with graceful fallback on error. The two mechanisms are mutually exclusive via `_split_context == nullptr` check at `lake_connector.cpp:458`.

Choosing option (a): add a scoping note at the document top. Reader-level split belongs to the ConnectorScan path's internal implementation and operates at a different architectural layer (inside a single IO task vs. across drivers). Expanding to cover it would blur the document's focus on the pipeline scheduling-level mechanism.

### Issue 24: Agreed, will add

Confirmed `lake_tablet_rows_splitted_ratio` default 1.5 at `config.h:1070`. Will add to Section 10.

### Issue 25: Agreed, will add

Confirmed TopN filter back pressure at `scan_operator.cpp:159-161`. Will add to Section 8's `has_output()` flow chart.

---

## Round 6

**[Review]** Date: 2026-04-11

### Summary

Final round. One remaining interaction between sorted scan and the split mechanism, plus a closing assessment.

### Issue 26: Section 9 — `_sorted_by_keys_per_tablet` forces `io_tasks_per_scan_operator = 1`

**Severity: Low-Medium (interaction not documented)**

`OlapScanNode::io_tasks_per_scan_operator()` (`olap_scan_node.h:95-100`):

```cpp
int io_tasks_per_scan_operator() const override {
    if (_sorted_by_keys_per_tablet) {
        return 1;
    }
    return starrocks::ScanNode::io_tasks_per_scan_operator();
}
```

When FE sets `sorted_by_keys_per_tablet = true` (for ordered scans requiring in-key-order output per tablet), each ScanOperator is limited to 1 concurrent IO task regardless of the config. This affects two things:

1. **IO parallelism per driver**: Section 9's description of "最多同时运行 `io_tasks_per_scan_operator` 个 IO task（默认 4）" is misleading — for sorted scans the effective value is 1.

2. **Factory selection threshold**: At `scan_node.cpp:172`:
   ```cpp
   int io_parallelism = scan_dop * io_tasks_per_scan_operator();
   ```
   With `io_tasks_per_scan_operator() = 1`, `io_parallelism = scan_dop`, making `num_original_morsels() <= io_parallelism` more likely to be true, which increases the chance of choosing `IndividualMorselQueueFactory` over `SharedMorselQueueFactory`. This changes the split path from internal-production (SplitMorselQueue) to the external-injection path (DynamicMorselQueue), which may not be the intended behavior for sorted scans.

The document should note this override as a constraint on Section 9's IO concurrency description.

### Closing Assessment

Across 6 rounds and 26 issues, the review covered:

- **Factual accuracy**: 2 formula errors (unplug threshold, TicketChecker bits), line number drift, config defaults
- **Completeness**: reader-level split in lake ConnectorScan, pre-split sorting, tail optimizations, adaptive stepping, overlapped rowsets, TopN back pressure, sorted scan interaction
- **Concurrency**: mutex-under-IO in SplitMorselQueue, notification two-path dispatch, one-shot CAS semantics
- **Architecture**: QueryCache ↔ Split mutual exclusion, scan_dop as effective driver count, structural invariant documentation
- **Configuration**: 3 missing configs, missing lake-specific session variable, INVISIBLE variable annotation

The document is well-written and provides strong coverage of the core mechanism. The most impactful fixes are: (1) the unplug threshold formula (Issue 1), (2) the scoping note for reader-level split (Issue 23), (3) filling in config defaults (Issue 18), and (4) adding the structural invariants section (Issue 17).

---

**[Response]** Date: 2026-04-11

### Issue 26: Agreed, will add

Confirmed `OlapScanNode::io_tasks_per_scan_operator()` at `olap_scan_node.h:95-100`: `_sorted_by_keys_per_tablet` forces return 1. This affects both Section 9's IO concurrency description and Section 2.4's factory selection threshold (`io_parallelism = scan_dop * 1`). Will add a note to Section 9.

### Closing Assessment: Acknowledged

6 rounds, 26 issues — all addressed. The review significantly improved factual accuracy, completeness, and architectural clarity of the document.

---

## Round 7

**[Review]** Date: 2026-04-11

### Issue 27: Reader-level split 不应作为 scoping note 排除，应作为完整机制展开

**Severity: High (文档结构性问题)**

#### 问题

当前文档开头用一段 scoping note 将 reader-level split 排除在外，仅给出 `lake/tablet_reader.cpp` 的代码指引。但文档标题是"存算分离内表 Tablet 并行 Scan 机制"——这个标题承诺的是**机制级别的完整叙述**，而非某一条代码路径的局部说明。

Reader-level split 不是一个独立的、可以"参见另一篇文档"的旁支功能。它与 morsel-queue-level split **共享同一套切分算法**（Physical/Logical Split）、**共享同一套配置参数**（`splitted_scan_rows`、`min_scan_dop` 等）、**共享同一组决策判定逻辑**（`_could_tablet_internal_parallel`、`_could_split_tablet_physically`），只是在 pipeline 调度层的接入方式不同。将它排除在外，读者会：

1. 误以为 Sections 3-4 的 Physical/Logical Split 算法只有一个消费者（SplitMorselQueue）
2. 不理解 Section 2.4 的"外部注入式"为什么要描述（现有文档中没有任何实际实现走这条路径）
3. 无法回答"存算分离表走 ConnectorScan 路径时 tablet 内并行如何工作"这个核心问题

#### 分析：两个机制的共性与差异

通过代码验证，两个机制的完整对比：

**共享的部分（算法层）**：

| 共享组件 | 说明 |
|---------|------|
| `_could_tablet_internal_parallel()` | 判定逻辑完全相同：行数估算 → splitted_scan_rows → scan_dop → min_scan_dop 阈值。lake 版本在 `LakeDataSourceProvider` 中实现，使用 `lake_tablet_manager()->get_tablet_num_rows()` 获取行数 |
| `_could_split_tablet_physically()` | Physical/Logical 选择逻辑相同 |
| `PhysicalSplitMorselQueue` / `LogicalSplitMorselQueue` | 切分算法完全复用。reader-level split 在 `lake::TabletReader::open()` 中创建 SplitMorselQueue，调用 `try_get()` 循环预计算所有 split task |
| 配置参数 | `splitted_scan_rows`、`min_scan_dop` 等参数共享 |

**差异的部分（调度层）**：

| 维度 | Morsel-queue-level split | Reader-level split |
|------|-------------------------|--------------------|
| **代码路径** | OlapScanNode → SplitMorselQueue → SharedMorselQueueFactory | ConnectorScanNode → LakeDataSourceProvider → DynamicMorselQueue → IndividualMorselQueueFactory |
| **切分时机** | Pipeline 构建阶段，SplitMorselQueue 在 `try_get()` 时按需切分 | IO task 内部，`lake::TabletReader::open()` 一次性预计算全部 split |
| **morsel 产出** | 内部产出：SplitMorselQueue::try_get() 直接返回 split morsel | 外部注入：TabletReader 产出 `_split_tasks` → ConnectorChunkSource 在 EOF 时提取 → `append_morsels()` 注入 DynamicMorselQueue |
| **Driver 分发** | SharedMorselQueueFactory：所有 Driver 竞争同一个 SplitMorselQueue | IndividualMorselQueueFactory：`next_driver_seq()` 轮询分配到 per-driver DynamicMorselQueue |
| **附加阈值** | 无 | `lake_tablet_rows_splitted_ratio = 1.5`：TabletReader 在 open() 时检查 `tablet_num_rows < splitted_scan_rows * 1.5`，不满足则拒绝 split，graceful fallback |
| **错误处理** | Fail-stop：`_tablet_idx = _tablets.size()` 永久耗尽队列 | Graceful fallback：`_split_tasks.clear(); _need_split = false; return init_collector()` 退回非 split 模式 |
| **完成协调** | morsel queue 空即完成 | `mark_split_source_morsel_finished()` 原子计数器归零 + `has_more_from_split = false` |
| **互斥保证** | `_split_context == nullptr` → reader-level split 跳过已 split 的 morsel | 同左 |

**关键发现：Section 2.4 的"外部注入式"描述正是 reader-level split 的基础设施**。当前文档详细描述了 `IndividualMorselQueueFactory::append_morsels()`、`next_driver_seq()`、`mark_split_source_morsel_finished()` 三个协调方法，但没有给出任何实际的消费者——这使得 Section 2.4 的后半部分读起来像是抽象的架构描述而非实际代码路径的解释。Reader-level split 就是那个缺失的消费者。

#### Reader-level split 的完整生命周期

从代码验证的执行流：

```
1. Pipeline 构建阶段
   LakeDataSourceProvider::convert_scan_range_to_morsel_queue()
     → _could_tablet_internal_parallel()  [共享判定逻辑]
     → _could_split = true, _could_split_physically = ?
     → DynamicMorselQueue + has_more_from_split = true
     → IndividualMorselQueueFactory (per-driver DynamicMorselQueue)

2. 运行时：首次 IO task（原始 morsel，_split_context == nullptr）
   ConnectorChunkSource → LakeDataSource::open()
     → lake::TabletReader(need_split=true, could_split_physically)
     → TabletReader::open():
        a. 检查 _rowsets.empty() → 拒绝 split
        b. 检查 tablet_num_rows < splitted_scan_rows * 1.5 → 拒绝 split
        c. 创建 PhysicalSplitMorselQueue 或 LogicalSplitMorselQueue
        d. try_get() 循环预计算所有 split → _split_tasks[]
        e. 任何 try_get() 失败 → 清空 _split_tasks，fallback 到非 split
     → TabletReader::do_get_next() 立即返回 EOF

3. 运行时：EOF 时提取 split tasks
   ConnectorChunkSource::buffer_next_batch_chunks_blocking()
     → 收到 EOF
     → _data_source->get_split_tasks(&split_tasks)
     → 包装为带 _split_context 的 ScanMorsel
     → scan_op->append_morsels() 注入 IndividualMorselQueueFactory
     → _report_split_source_morsel_finished_once()

4. 运行时：后续 IO task（split morsel，_split_context != nullptr）
   ConnectorChunkSource → LakeDataSource::open()
     → _split_context != nullptr → 设置 rowid_range_option 或 short_key_ranges_option
     → lake::TabletReader(need_split=false)  [不再 split]
     → 正常读取子区间数据
```

#### 建议的文档结构调整

不是简单追加一个 Section，而是围绕"tablet 内并行 scan"的统一主线重组：

**核心思路**：Physical/Logical Split 是**算法层**，morsel-queue-level 和 reader-level 是**调度层的两种接入方式**。文档应先讲算法，再讲两种接入。

建议结构：

1. **问题定义**（现 Section 1，保持）
2. **Split 算法：Physical Split 与 Logical Split**（现 Sections 3-4，前提到算法层）
   - 这两个算法被两种调度接入共用，应作为独立的算法描述
3. **调度接入一：Morsel-queue-level split（OlapScan 路径）**
   - 决策流程（现 Section 2）
   - SplitMorselQueue + SharedMorselQueueFactory
   - 内部产出式：try_get() 按需切分
4. **调度接入二：Reader-level split（ConnectorScan 路径）**
   - 决策流程（LakeDataSourceProvider::_could_tablet_internal_parallel）
   - lake::TabletReader::open() 预计算
   - 外部注入式：ConnectorChunkSource → append_morsels → DynamicMorselQueue
   - 附加阈值：lake_tablet_rows_splitted_ratio
   - 互斥保证：_split_context == nullptr
   - Graceful fallback 错误处理
5. **Shared Scan**（现 Section 5，保持）
6. **端到端数据流**（现 Section 6，增加 reader-level split 路径图）
7. **TicketChecker / Unplug / IO Task 控制**（现 Sections 7-9，保持）
8. **配置参数 / 机制对比 / 结构性不变量**（现 Sections 10-12，对比表增加 reader-level split 列）

这个结构的好处：
- 算法层（Physical/Logical Split）只讲一次，两种接入各自引用
- 两种调度接入的差异清晰对比，读者不会混淆
- Section 2.4 的"外部注入式"描述有了具体消费者，不再悬空
- Section 11 的对比表从二元变为三元，覆盖完整

---

**[Response]** Date: 2026-04-11

### Issue 27: Agreed, will restructure

The argument is correct on all three points:

1. "外部注入式" in Section 2.4 describes `append_morsels`/`next_driver_seq`/`mark_split_source_morsel_finished` without a concrete consumer — reader-level split is that consumer.
2. Physical/Logical Split algorithms are shared by both scheduling paths — describing them under "morsel-queue-level split" implies they belong to that path alone.
3. The document title promises "存算分离内表" coverage, but ConnectorScan is the primary path for lake tables. Scoping it out contradicts the title.

Will restructure per the proposed plan: algorithm layer first (Physical/Logical Split), then two scheduling integration points (morsel-queue-level + reader-level), updating the comparison table to three-way.

---

## Round 8

**[Review]** Date: 2026-04-11

### Summary

重组后的文档结构清晰，算法层与调度层分离合理，三路对比表覆盖完整。本轮聚焦准确性验证和面向新读者的可读性。

### Issue 28: Section 5.5 / Section 11 — `_split_context` 互斥描述有事实错误

**Severity: High (factual error)**

Section 5.5 写道：

> morsel-queue-level split 产出的 morsel 带有 `_split_context`（由 SplitMorselQueue 内部设置），reader-level split 检测到 `_split_context != nullptr` 时跳过。

Section 11 结构性不变量表：

> **两种 Split 调度互斥** → `_split_context == nullptr` 检查：reader-level split 跳过已有 split_context 的 morsel

代码验证：**`SplitMorselQueue` 从不设置 `_split_context`**。搜索 `set_split_context` 的全部调用点：

- `connector_scan_operator.cpp:916`：ConnectorChunkSource 包装 reader-level split task 时设置
- `connector_scan_operator.cpp:656`：ConnectorChunkSource 将 split_context 传递给 data source 时设置

两处均在 ConnectorScan 路径内。OlapScan 路径的 `PhysicalSplitScanMorsel` / `LogicalSplitScanMorsel` 通过类成员 `_rowid_range_option` / `_short_key_ranges_option` 携带切分信息，不使用 `_split_context`。

**两种 Split 调度的互斥机制不是 `_split_context`，而是 ScanNode 类型**：一个 tablet 要么通过 OlapScanNode 扫描（morsel-queue-level），要么通过 ConnectorScanNode 扫描（reader-level），由 FE planner 在 plan 阶段决定，不存在运行时交叉。

`_split_context == nullptr` 的真实语义是 **reader-level split 的内部递归防护**：
- 原始 morsel（`_split_context == nullptr`）→ 触发 reader-level split → 产出带 `_split_context` 的 split morsel
- Split morsel（`_split_context != nullptr`）→ 直接使用 split_context 读取，不再 split

应修正为：
1. Section 5.5 标题改为"递归防护"而非"互斥保证"
2. Section 11 表中删除"两种 Split 调度互斥"这一行，或改为"ScanNode 类型互斥：FE planner 决定 OlapScanNode vs ConnectorScanNode"

### Issue 29: Section 1 互斥讨论过早，引用未定义概念

**Severity: Medium (readability)**

Section 1 在介绍三种机制的表格之后立即展开互斥讨论，引用了多个此时尚未介绍的概念：

- `IndividualMorselQueueFactory`（Section 4-5 才引入）
- `append_morsels(driver_seq, morsels)`（Section 5.3 才解释）
- `mark_split_source_morsel_finished()`（Section 5.3 才解释）
- `SharedMorselQueueFactory`（Section 4.2 才引入）
- `set_has_more_from_split(false)`（Section 4.3 才引入）

一个不了解机制的读者在 Section 1 会碰到大量无法理解的术语。

**建议**：Section 1 仅保留互斥的**结论**（一句话："Split 与 Shared Scan 互斥，代码层面由 `set_has_more_from_split(false)` 保证"），将三条原因的详细解释移至 Section 6（Shared Scan）之后或 Section 11（结构性不变量）中。此时读者已经了解了所有涉及的组件。

### Issue 30: Section 1 互斥原因 #1 只涵盖 reader-level split，遗漏 morsel-queue-level split

**Severity: Medium (incomplete)**

当前互斥原因 #1：

> reader-level split 的外部注入式分发需要 per-driver queue（`IndividualMorselQueueFactory`），通过 `append_morsels(driver_seq, morsels)` 注入。`SharedMorselQueueFactory` 只有一个共享 queue，没有 per-driver 分发能力。

这只解释了 reader-level split 与 shared scan 的冲突。Morsel-queue-level split 与 shared scan 的冲突原因不同：SplitMorselQueue 的类型是 PHYSICAL_SPLIT / LOGICAL_SPLIT（不属于 FIXED/DYNAMIC），直接进入 SharedMorselQueueFactory 的 else 分支，此时强制 `set_has_more_from_split(false)` 关闭外部注入。

如果保留互斥讨论在 Section 1，应分别说明两种 split 各自与 shared scan 的冲突点。

### Issue 31: 缺少文档导读

**Severity: Medium (readability)**

文档约 640 行，覆盖 3 种机制、2 套算法、2 种调度接入、多个辅助机制。对于不了解机制的读者，缺少入口引导。

**建议**：在 Section 1 之前或之内增加简短导读，例如：

> 本文档分三层展开：
> - **算法层**（Section 2）：tablet 如何按 RowID 或 ShortKey 切分为子区间
> - **调度层**（Sections 3-5）：切分决策如何做出，split morsel 如何分发到 Driver
> - **辅助机制**（Sections 6-8）：Shared Scan、EOS 追踪、IO 调度
>
> 如果只关心存算分离表的主路径，重点阅读 Section 2（算法）+ Section 5（Reader-level Split）。

### Issue 32: Section 10 对比表使用 plaintext 对齐，阅读困难

**Severity: Low (readability)**

Section 10 的三路对比表使用空格对齐的 plaintext 格式。由于三列内容长度差异大，实际渲染时列对不齐。转为 Markdown 表格可提升可读性。当前表有 12 行对比维度，可拆分为 2-3 个子表（分发模型 / IO 行为 / 完成协调），每个更紧凑。

---

## Round 9

**[Review]** Date: 2026-04-11

### Summary

Round 8 提出了若干独立的概念清晰度问题。本轮不再逐点补丁，而是从"一个不了解机制的读者如何建立理解"出发，系统性地分析文档的认知路径缺陷，给出一个完整的修改方案。

### 问题诊断：读者认知路径的断裂

一个读者理解 tablet 并行 scan 需要依次建立五层认知：

```
Layer 0: 架构背景 — OlapScanNode vs ConnectorScanNode 分别用在什么场景
Layer 1: 问题     — tablet 数 < dop 导致并行度退化
Layer 2: 概念模型 — Morsel / MorselQueue / MorselQueueFactory / ChunkSource 是什么、如何协作
Layer 3: 算法     — 一个 tablet 如何被切成子区间（Physical / Logical Split）
Layer 4: 调度     — 切出的子区间如何分发给各 Driver（两种路径）
Layer 5: 辅助     — EOS 追踪、完成判定、IO 控制、互斥约束
```

当前文档的认知路径：

```
Section 1: Layer 1 → 直接跳到 Layer 4 的三路概览 → 跳到 Layer 5 的互斥细节
Section 2: Layer 3
Section 3: Layer 4 的前半（判定逻辑）
Section 4-5: Layer 4 的后半（调度实现）
Section 6: Layer 4 的第三条路（Shared Scan）
Section 7-8: Layer 5
```

**Layer 0 和 Layer 2 完全缺失。** 读者在没有概念框架的情况下被要求理解三种机制的对比和互斥。

### Issue 33: 缺少架构背景 — 读者不知道为什么有两条 scan 路径

**Severity: High**

文档在 Section 1 的表格中写道：

| Morsel-queue-level Split | OlapScanNode | 存算一体内表 |
| Reader-level Split | ConnectorScanNode | 存算分离内表（主路径） |

但从不解释**为什么**存算分离表走 ConnectorScanNode。一个读者会立即产生疑问：
- OlapScanNode 不是也支持 lake 表的 `parse_seek_range` 吗？（Section 2.2 里提到了 `lake::TabletReader::parse_seek_range()`）
- 如果 OlapScanNode 能处理 lake 表，为什么还需要另一条路径？

这个背景不需要长篇大论，但至少需要一句话解释：存算分离架构下，FE planner 将 lake 表的 scan 映射到 ConnectorScanNode（复用 connector 框架的 DataSource 抽象），而非 OlapScanNode。两条路径在 tablet 并行 scan 的层面使用不同的调度接入，但共享切分算法。

### Issue 34: 缺少概念模型 — 术语在定义前使用

**Severity: High**

以下术语在 Section 1-2 中密集出现，但从未被定义：

| 术语 | 首次出现 | 实际解释 |
|------|---------|---------|
| Morsel | Section 1 第 1 行 | "一个 tablet 的扫描任务"——6 个字，不够 |
| Split morsel | Section 1 表格 | 从未明确定义 |
| MorselQueue | Section 1 表格 | 从未定义 |
| MorselQueueFactory | Section 1 互斥讨论 | 从未定义两层模型 |
| SharedMorselQueueFactory | Section 1 互斥讨论 | Section 4.2 才出现 |
| IndividualMorselQueueFactory | Section 1 互斥讨论 | Section 5.1 才出现 |
| ChunkSource | Section 7.1 | 从未定义 |
| has_more_from_split | Section 4.3 / 5.1 | 散布三处，无完整语义 |
| 内部产出式 / 外部注入式 | Section 1 "接入方式" | Section 4.3 / 5.3 才有含义 |

一个不了解机制的读者在 Section 1 就会碰壁。

**应在算法之前增加概念模型 Section**，用一段话 + 一张图建立：

```
扫描工作的调度单元是 Morsel：

  FE 下发 scan range → 每个 tablet 对应一个 Morsel
                          │
                          ▼
                     MorselQueue（持有 Morsel 的队列）
                          │
                          ▼
                     MorselQueueFactory（决定如何把 MorselQueue 分配给 Driver）
                     ┌─────────────────┬──────────────────────┐
                     │                 │                      │
              SharedMorselQueue   IndividualMorselQueue   (per-driver queues)
              Factory（共享一个    Factory（每个 Driver
              queue，Driver 竞争） 一个 queue）
                     │                 │
                     ▼                 ▼
                  Driver 从 queue 取 morsel → 创建 ChunkSource → 提交 IO task
                                                                     │
                                                                     ▼
                                                              ChunkBuffer → chunk 供消费

Tablet 并行 scan 的核心思路：将一个 Morsel（整 tablet）拆成多个 Split Morsel
（子区间），使更多 Driver 有工作可做。

Split Morsel 携带子区间描述：
  · Physical Split → RowidRangeOption（精确 rowid 区间）
  · Logical Split  → ShortKeyRangesOption（short-key 近似区间）
```

这个模型建立后，后续所有 Section 的术语都有了着陆点。特别是：
- "内部产出式"可以在图上标注为"SplitMorselQueue 自身产出 split morsel"
- "外部注入式"可以标注为"TabletReader 产出 split → 注入到 DynamicMorselQueue"
- `has_more_from_split` 的语义可以一次性完整说明："当此 flag 为 true 时，MorselQueue::has_more() 返回 true，ScanOperator 不会判定完成——因为还有 split morsel 尚未注入。所有原始 morsel 的 split 完成后，flag 被设为 false，scan 才能正常结束。"

### Issue 35: Section 1 信息密度过高，混合了问题、方案总览和实现细节

**Severity: High (readability)**

当前 Section 1 在 30 行内完成了：
1. 问题定义（3 tablets, dop=8）
2. scan_dop 的注释（一段技术细节）
3. 三种机制的表格（方案总览）
4. 互斥的三条原因（实现细节）
5. `set_has_more_from_split(false)` 的代码级保证

一个新读者在这 30 行内经历了从"问题是什么"到"代码怎么保证互斥"的认知跨度。

**建议拆分为**：

- **Section 1: 问题与方案概览**（仅 Layer 1）
  - 问题：tablet 数 < dop
  - 方案思路：一句话说明 split 和 shared scan 的核心区别
  - 指向后续 section 的路标（不展开细节）
  - 不出现任何 Factory/Queue 类名

- **Section 2: 概念模型**（Layer 2，新增）
  - 上述 Morsel → MorselQueue → Factory → Driver 的图
  - Split Morsel 的定义
  - 两种分发模式的一句话对比
  - `has_more_from_split` 的完整语义

- **Section 3: Split 算法**（Layer 3，现 Section 2，保持）

- **Section 4: Split 判定**（现 Section 3，保持）

- **Section 5-6: 两种调度路径**（现 Section 4-5，保持）

- **Section 7: Shared Scan**（现 Section 6，保持）

- **互斥约束**：移入 Section 11 结构性不变量（在所有机制介绍完之后）

### Issue 36: "Split" 一词需要消歧

**Severity: Medium**

"Split" 在文档中同时指代五个层次：

1. 一般动词：把 tablet 切成子区间
2. Physical Split / Logical Split：两种切分算法
3. Morsel-queue-level split / Reader-level split：两种调度路径
4. SplitMorselQueue：一个类
5. "Split 与 Shared Scan 互斥"：指所有 split 机制

读者在不同 section 遇到 "split" 时不知道指哪个层次。

**建议**：
- 算法层统一称"**切分算法**"（Physical 切分 / Logical 切分）
- 调度层统一称"**split 调度**"（morsel-queue-level / reader-level）
- "Split 与 Shared Scan 互斥"改为"**tablet 内并行切分**与 Shared Scan 互斥"
- 避免在散文中单独使用 "split" 指代不确定的层次

### Issue 37: 缺少贯穿全文的具体示例

**Severity: Medium**

Section 1 给了 "3 tablets, dop=8" 的例子说明问题，但之后再也没有回到这个例子。读者无法将抽象机制映射到具体行为。

**建议**：选一个具体场景（例如 1 个 tablet、100 万行、dop=4、Physical Split、splitted_scan_rows=25万），从判定逻辑开始串联到产出 4 个 split morsel → 分发到 4 个 Driver → 并行读取 4 个 RowID 区间。这个例子只需要在几个关键节点上各用 2-3 行标注，不需要单独一节。

### Issue 38: Section 5.5 `_split_context` 互斥描述仍有事实错误

**Severity: High (factual, 重申 Issue 28)**

Section 5.5 和 Section 11 的描述未修正（见 Round 8 Issue 28 的详细分析）。

核心纠正：
- `_split_context` 仅存在于 ConnectorScan 路径，是 reader-level split 的**内部递归防护**，不是两种 split 调度的互斥机制
- 两种 split 调度的互斥由 ScanNode 类型保证（FE planner 决定），不是运行时检查
- Section 5.5 标题应从"互斥保证"改为"递归防护"
- Section 11 表中"两种 Split 调度互斥 → `_split_context == nullptr`"应改为 "Reader-level split 递归防护 → `_split_context == nullptr` 阻止对 split morsel 再次切分"

### 总结：建议的修改清单

按优先级排序：

| 优先级 | 改动 | 影响范围 |
|--------|------|---------|
| P0 | 修复 Section 5.5 / Section 11 的 `_split_context` 事实错误 | Section 5.5, Section 11 |
| P0 | 新增概念模型 Section（Morsel → Queue → Factory → Driver 图 + 术语定义 + `has_more_from_split` 完整语义） | 新 Section，插入 Section 1 与 Section 2 之间 |
| P0 | 增加一句架构背景：存算分离表走 ConnectorScanNode 的原因 | Section 1 或概念模型 Section |
| P1 | 拆分 Section 1：问题 + 方案概览（无类名），互斥细节后移到结构性不变量 | Section 1, Section 11 |
| P1 | "Split" 消歧：算法层 = "切分算法"，调度层 = "split 调度" | 全文术语 |
| P2 | 增加贯穿示例（1 tablet、100万行、dop=4 的端到端演练） | 散布在几个关键 section |
| P2 | Section 10 plaintext 对比表转 Markdown 表格 | Section 10 |

---

**[Response]** Date: 2026-04-11

All issues in Round 8 and Round 9 verified. Positions:

### Issue 28/38: Agreed, factual error — will fix

`_split_context` is only set in ConnectorScan path (connector_scan_operator.cpp:916, 656). SplitMorselQueue never sets it. The two split scheduling paths are mutually exclusive by ScanNode type (FE planner decides OlapScanNode vs ConnectorScanNode), not by runtime `_split_context` check. Will rename Section 5.5 from "互斥保证" to "递归防护", fix Section 11 invariant table.

### Issues 29, 30, 31, 33, 34, 35: Agreed — will restructure front matter

Will implement the Round 9 proposal:
- New Section 1: Problem + solution overview (no class names)
- New Section 2: Concept model (Morsel → Queue → Factory → Driver diagram, term definitions, `has_more_from_split` semantics)
- Architecture background: one sentence on why lake tables use ConnectorScanNode
- Mutual exclusion details moved to structural invariants section (after all mechanisms introduced)

### Issue 32: Agreed, will convert to Markdown table

### Issue 36: Agreed, will disambiguate

Will use "切分算法" for algorithm layer, "split 调度" for scheduling layer, "tablet 内并行切分" for the general concept.

### Issue 37: Agreed, will add running example

Will thread a concrete example (1 tablet, 1M rows, dop=4, Physical Split) through key sections.

---

## Round 10

**[Review]** Date: 2026-04-11

### Closing

9 轮 review，38 个 issue，覆盖：

- **准确性**（Rounds 1-2, 5, 8）：公式错误、bit 布局、`_split_context` 事实错误、config 默认值、line number drift
- **完整性**（Rounds 1, 4-5）：尾部优化、自适应步进、overlapped rowset、reader-level split、lake 专属配置、TopN back pressure、sorted scan 约束
- **并发与架构**（Rounds 2-3）：mutex-under-IO、unget 竞争、QueryCache 互斥不变量、通知机制
- **概念清晰度与可读性**（Rounds 8-9）：概念模型缺失、术语未定义先用、Section 1 过载、"Split" 歧义、架构背景缺失、贯穿示例

所有 issue 已得到回应，但 Round 9 的 P0/P1 改动尚未落地到文档中。

### Issue 39: 文档当前不可交付——Round 9 的结构性改动是交付前置条件

**Severity: Blocker**

以一个不了解机制的新人视角逐段检验当前文档：

**Section 1（第 1-28 行）**：读者在第 11 行遇到 `MorselQueueFactory::size()`（未定义），第 17 行遇到 "Morsel-queue-level Split"（未解释含义），第 23 行遇到 `IndividualMorselQueueFactory`、`append_morsels(driver_seq, morsels)`（未定义）。读者此时没有任何概念框架，无法理解这些术语指什么、为什么重要。**结论：新人在 Section 1 就会放弃阅读。**

**Section 2（第 31-181 行）**：讲切分算法，本身写得好。但第 33 行说"被 morsel-queue-level 和 reader-level 两种调度接入共用"——读者不知道这两种接入是什么，因为它们要到 Section 4-5 才介绍。第 81 行提到"morsel-queue-level split"和"SharedMorselQueueFactory"但读者仍不知道这些是什么。**结论：算法内容好，但依赖未建立的概念。**

**Section 4-5（第 217-346 行）**：调度路径的实现细节。但读者不知道为什么存算分离表走 ConnectorScanNode（Section 5 第 269 行直接说"存算分离内表通过 ConnectorScanNode → LakeDataSourceProvider 路径扫描"，没有解释为什么）。**结论：读者能理解 how，但不理解 why。**

**Section 5.5（第 343-346 行）**：`_split_context` 互斥描述有事实错误（Issue 28/38，已 agreed 但未修正）。**结论：会误导读者。**

Round 9 提出的 P0 改动（概念模型 Section、架构背景、`_split_context` 修正）直接解决上述每一个断裂点。这些不是锦上添花，是读者能否读下去的前提。

**交付标准**：文档可交付给新人阅读的最低条件是：
1. ✅ 准确性——除 Section 5.5 外已修正
2. ❌ 概念自包含——术语在使用前有定义
3. ❌ 架构背景——读者知道为什么有两条路径
4. ❌ 认知坡度平缓——从问题到方案到实现，不跳跃

Round 9 的 P0 项全部落地后，条件 2-4 才能满足。

---

**[Response]** Date: 2026-04-11

### Issue 39: All 4 delivery conditions now met

Round 9 的 P0/P1 改动已在 Round 8/9 response 后的文档重写中全部落地。Issue 39 引用的行号（11/17/23/33/81/269/343）对应旧文档结构，新文档已重组。

逐条验证：

1. **准确性** — `_split_context` 已改为"递归防护"（Section 6.5），Section 11 不变量表修正为"ScanNode 类型互斥" + "递归防护"。
2. **概念自包含** — 新增 Section 2 概念模型：组件图 + 9 个术语定义 + 两种 split 调度对比 + `has_more_from_split` 完整生命周期。Section 1 不再出现任何 Factory/Queue 类名。
3. **架构背景** — Section 1 末尾增加一段：存算分离 FE planner 映射 lake 表到 ConnectorScanNode 的原因。
4. **认知坡度** — 导读 → 问题概览（无类名）→ 概念模型（术语定义）→ 切分算法 → 判定 → 调度 → Shared Scan → 辅助 → 配置 → 对比 → 不变量。新读者在遇到任何技术术语前都有定义。

---

## Round 11

**[Review]** Date: 2026-04-11

### Summary

重写后的文档在结构和可读性上有质的提升：导读、概念模型、术语定义、`has_more_from_split` 完整语义、架构背景、贯穿示例、Markdown 对比表、互斥后移——Round 9 的 P0/P1 改动全部到位。以下是验证过程中发现的剩余问题。

### Issue 40: 贯穿示例在 Section 3.2 和 Section 4 之间自相矛盾

**Severity: Medium (readability)**

Section 3.2（line 133）：

> 假设 1 个 tablet、**100 万行**、dop=4、Physical 切分、**`splitted_scan_rows = 250,000`**。
> → 产出 4 个 split morsel

Section 4（line 239）：

> 1 个 tablet、100 万行、`max_splitted_scan_bytes = 512MB`、`estimated_scan_row_bytes = 2048`。
> `splitted_scan_rows = 512MB / 2048 = 262,144`。
> `scan_dop = 1,000,000 / 262,144 = 3`，clamp 到 [1, 4] → 3。
> **scan_dop(3) < min_scan_dop(4) → 不启用切分。**

读者跟着贯穿示例走到 Section 4 会发现：Section 3.2 假设切分已经发生（用了一个凑整的 `splitted_scan_rows`），但用实际默认配置计算后 100 万行根本不会触发切分。然后 Section 4 说 200 万行才行，但 Section 3.2 的 4 个 split morsel 示例是基于 100 万行的。

**建议**：让示例全程一致。两种修法：
- (a) Section 3.2 改用 200 万行（与 Section 4 的"200 万行启用切分"对齐），4 个 split morsel 各 50 万行
- (b) Section 3.2 保留 100 万行但标注"此处仅演示切分算法，假设判定已通过"，Section 4 显式指出 100 万行不满足阈值

### Issue 41: 端到端数据流图被移除

**Severity: Medium (regression)**

旧版 Section 7 有三张端到端 ASCII 数据流图，分别展示 morsel-queue-level split、reader-level split、shared scan 的完整生命周期。这些图是理解三条路径最直观的可视化入口——尤其 reader-level split 的"原始 morsel → EOF → get_split_tasks → append_morsels → split morsel 消费"流程，纯文字描述远不如图清晰。

重写后这些图全部消失，导读（line 9）指向 Section 6 阅读 reader-level split，但读者在 Section 6 只看到分步骤文字描述。

**建议**：恢复端到端数据流图作为独立 Section（可放在 Section 7 Shared Scan 之后），或至少在 Section 5/6 各嵌入一张简化版流程图。旧版的图质量很好，不需要重写，直接恢复即可。

### Issue 42: MorselQueueFactory 的完整选择逻辑被简化为一句话

**Severity: Low (信息丢失，但对新人首次阅读影响不大)**

旧版 Section 4.2 有一个 5 条件流程图，展示 `convert_scan_range_to_morsel_queue_factory()` 的完整决策逻辑：

```
always_shared_scan()? / enable_shared_scan? / scan_dop == 1? /
type ∉ {FIXED, DYNAMIC}? / num_morsels > io_parallelism?
→ 任一 YES → SharedMorselQueueFactory
→ 全部 NO  → IndividualMorselQueueFactory
```

重写后 Section 5.1 只有一句："SplitMorselQueue 的类型是 PHYSICAL_SPLIT 或 LOGICAL_SPLIT（不属于 FIXED/DYNAMIC），进入 SharedMorselQueueFactory 路径"。其他 4 个条件（`enable_shared_scan`、`always_shared_scan`、`scan_dop==1`、`num_morsels > io_parallelism`）被省略。

这对新人首次阅读影响不大（他们只需知道 SplitMorselQueue → SharedMorselQueueFactory），但对需要理解边界情况的开发者是信息丢失。

**建议**：在 Section 5.1 增加一个折叠注释或脚注，指出完整的 5 条件判定逻辑和代码位置（`ScanNode::convert_scan_range_to_morsel_queue_factory()`），不展开不影响主线阅读。

### 交付评估

逐项验证 Round 10 Issue 39 提出的交付标准：

| 标准 | 状态 | 说明 |
|------|------|------|
| 准确性 | ✅ | `_split_context` 修正为递归防护；公式、bit 布局、config 默认值均已修正 |
| 概念自包含 | ✅ | Section 2 概念模型提供完整术语定义，`has_more_from_split` 有完整生命周期 |
| 架构背景 | ✅ | Section 1 末尾解释存算分离表走 ConnectorScanNode 的原因 |
| 认知坡度 | ✅ | 导读 → 问题 → 概念 → 算法 → 判定 → 调度 → 辅助，术语使用前均已定义 |
| 贯穿示例 | ⚠️ | 示例存在但 Section 3.2 与 Section 4 数值矛盾（Issue 40） |
| 可视化 | ❌ | 端到端数据流图被移除（Issue 41） |

### Issue 43: "Morsel-queue-level split" / "Reader-level split" 命名面向实现，不面向读者

**Severity: High (concept clarity)**

这两个名字基于"切分发生在代码哪一层"——morsel queue 层 vs TabletReader 层。但读者此时不知道 morsel queue 和 TabletReader 在架构中处于什么位置，这些名字对他们没有语义。

更根本的问题是：读者需要理解的不是"有两种 split 调度"，而是**"我的存算分离表走的是哪条路径、它怎么工作"**。当前文档把两种调度并列为对等概念（Section 5、Section 6），读者必须同时理解两条路径才能建立心智模型。但实际上存算分离表只走 reader-level 这一条，另一条跟读者无关。

### Issue 44: 文档标题是"存算分离内表"，但用大量篇幅介绍存算一体路径

**Severity: High (scope)**

按当前结构，读者的阅读路径是：

```
Section 1: 问题 + 三种方案（OK）
Section 2: 概念模型（OK）
Section 3: 切分算法（OK，存算分离表用得到）
Section 4: 切分判定（OK）
Section 5: Morsel-queue-level split（存算一体路径，存算分离不走这条）  ← 为什么要读这个？
Section 6: Reader-level split（存算分离的实际路径）
Section 7: Shared Scan
...
```

读者在 Section 5 花时间理解了一种他永远用不到的机制（SharedMorselQueueFactory + SplitMorselQueue 竞争模型），然后到 Section 6 发现存算分离表走的是完全不同的路径（IndividualMorselQueueFactory + DynamicMorselQueue + 外部注入）。这不仅浪费注意力，还会产生困惑："这两种调度到底什么关系？我应该关注哪个？"

**事实**：存算分离内表通过 ConnectorScanNode → LakeDataSourceProvider 扫描。切分调度只有一条路径。OlapScanNode 的 morsel-queue-level split 调度是存算一体表的机制，与本文档的目标读者无关。

### 建议：以存算分离表的切分调度为主线，OlapScan 路径降级为对比注释

重组后的结构：

```
Section 1: 问题与方案概览（保持，但三种方案的表格只保留与存算分离相关的两种：
           切分调度 + Shared Scan。OlapScan 变体在脚注提及）
Section 2: 概念模型（保持）
Section 3: 切分算法（保持，Physical / Logical 共享）
Section 4: 切分判定（保持，判定逻辑共享）
Section 5: 切分调度（现 Section 6 的内容，改为主 Section）
           · pipeline 构建：DynamicMorselQueue + IndividualMorselQueueFactory
           · TabletReader 预计算
           · split morsel 注入与分发
           · split morsel 消费
           · 递归防护
           · 端到端数据流图
Section 6: Shared Scan（保持）
Section 7: 辅助机制（保持）
Section 8: 配置（保持）
Section 9: 对比 / 不变量（简化：切分调度 vs Shared Scan 二路对比。
           增加一个"与 OlapScanNode 路径的差异"注释框，
           说明存算一体表走 SplitMorselQueue + SharedMorselQueueFactory，
           共享算法但调度接入不同，2-3 行即可）
```

**这样改的好处**：

1. **消除命名困惑**：不再需要 "Morsel-queue-level split" / "Reader-level split" 这对并列概念。存算分离表的切分调度就是"切分调度"，不需要限定词。OlapScan 的变体只在对比注释中出现，用"OlapScan 路径使用不同的调度接入"一句话带过。

2. **读者不再被迫理解两条路径**：Section 5 直接讲存算分离表怎么工作，没有"先读一个你用不到的 Section 5，再读你实际用的 Section 6"的认知负担。

3. **文档更短**：当前 Section 5（morsel-queue-level，14 行）+ Section 10 对比表中的 morsel-queue-level 列 → 全部可以压缩为对比注释中的 3-5 行。

4. **对比表简化**：从三路对比（morsel-queue-level / reader-level / shared scan）简化为二路对比（切分调度 vs shared scan）+ OlapScan 差异注释。三路对比表本身就是命名困惑的来源。

**OlapScan 路径的信息不会丢失**，只是降级：从 co-equal section 降为"与 OlapScan 路径的差异"对比注释。需要了解 OlapScan 路径的读者可以从注释中获得关键差异点（SplitMorselQueue 作为 pipeline queue / SharedMorselQueueFactory / Driver 竞争模型），然后直接读代码。

### 交付评估（更新）

| 标准 | 状态 | 说明 |
|------|------|------|
| 准确性 | ✅ | Round 1-8 的事实错误均已修正 |
| 概念自包含 | ✅ | Section 2 概念模型到位 |
| 架构背景 | ✅ | Section 1 解释了 ConnectorScanNode 的原因 |
| 认知坡度 | ⚠️ | Section 5 (OlapScan) 是存算分离读者不需要的认知负担（Issue 43/44）|
| 贯穿示例 | ⚠️ | Section 3.2 与 Section 4 数值矛盾（Issue 40）|
| 可视化 | ❌ | 端到端数据流图被移除（Issue 41）|
| 范围聚焦 | ❌ | 存算一体路径与存算分离路径并列，不符合标题承诺的范围（Issue 44）|

**Issue 40-41（示例矛盾 + 数据流图）+ Issue 43-44（命名 + 范围聚焦）修复后可交付。**

---

**[Response]** Date: 2026-04-12

### Issue 40: Fixed — 示例统一为 200 万行

Section 3.2 改为 200 万行 / 2 segment(各 100 万行) / `splitted_scan_rows = 500,000` → 4 个 split morsel。Section 4 示例与之一致：200 万行 → `scan_dop = 4` → 启用切分。增加 100 万行不触发的反例作为注释。

### Issue 41: Fixed — 恢复端到端数据流图

Section 5.5 包含三段式 ASCII 图：Pipeline 构建 → 原始 morsel 预计算 → split morsel 并行消费。

### Issue 42: Fixed — 增加脚注

Section 5.1 增加注释，指出 `convert_scan_range_to_morsel_queue_factory()` 内有 5 条件判定逻辑。

### Issue 43: Fixed — 消除命名困惑

"Morsel-queue-level split" / "Reader-level split" 命名不再作为并列概念出现。存算分离表的切分调度在文档中直接称为"切分调度"（Section 5），OlapScan 变体仅在 Section 8 对比表中出现。

### Issue 44: Fixed — 范围聚焦到存算分离

原 Section 5（morsel-queue-level split）从独立 Section 降级为 Section 8"与 OlapScan 路径的差异"对比表。原 Section 6（reader-level split）升级为 Section 5"切分调度"（主 Section）。Section 2 概念模型精简为只展示存算分离表的组件图。

同时基于对 FE 代码的验证（`SessionVariable.isEnableSharedScan()` 自 3.5 版本起硬编码返回 false），移除了 Shared Scan 相关内容：

- 原 Section 6（Shared Scan 机制）整节删除
- Section 1 改为一句注释说明 BE 侧存在 Shared Scan 但 FE 自 3.5 起禁用
- 配置参数表删除 `use_default_dop_when_shared_scan` 和 `enable_shared_scan`
- 对比表从"切分调度 vs Shared Scan"改为"存算分离 vs 存算一体"路径差异（Section 8）
- 结构性不变量删除"切分调度与 Shared Scan 互斥"（不可触发的约束无需记录）

最终文档结构（9 个 Section）：

```
Section 1: 问题与方案概览
Section 2: 概念模型
Section 3: 切分算法（Physical / Logical）
Section 4: 切分判定逻辑
Section 5: 切分调度（存算分离主路径）
Section 6: 辅助机制（TicketChecker / Unplug / 通知 / IO 控制）
Section 7: 关键配置参数
Section 8: 与 OlapScan 路径的差异
Section 9: 结构性不变量
```

---

## Round 12

**[Review]** Date: 2026-04-12

### Summary

文档经过 Round 9 的结构性重组和 Round 11 的范围聚焦后，质量有质的飞跃。以存算分离表为单一主线、OlapScan 降级为对比表、Shared Scan 降级为注释——认知路径清晰，读者不再被迫理解无关路径。

逐段以新人视角验证可读性：

- **导读 → Section 1**：无类名、无术语，纯问题描述 + 方案概述 + 架构背景 ✓
- **Section 2 概念模型**：图只展示存算分离路径的组件，术语表简洁，`has_more_from_split` 有完整生命周期 ✓
- **Section 3 切分算法**：Physical/Logical 讲解清晰，贯穿示例 200 万行 ✓
- **Section 4 切分判定**：公式 + 示例 + 反例（100万行不触发）✓
- **Section 5 切分调度**：5 个子节 + 端到端数据流图，从 pipeline 构建到 split morsel 消费完整覆盖 ✓
- **Section 6-9**：辅助机制、配置、OlapScan 差异、不变量——参考性内容 ✓

### Issue 45: Section 1 第二条矛盾描述："即使 tablet 数足够"的场景在默认路径下不会触发切分

**Severity: Low**

Section 1 line 16：

> **单 tablet 数据量大**：即使 tablet 数足够，单个 tablet 包含大量数据时，一个 Driver 串行读取整个 tablet 成为瓶颈，需要多个 Driver 并行读取同一 tablet 的不同区间来加速。

但 Section 4 的判定逻辑：

> `!force_split && num_total_scan_ranges >= pipeline_dop → false（tablet 够多）`

即当 tablet 数 >= dop 时，默认不启用切分。"即使 tablet 数足够"的场景在默认路径下不存在（除非 `FORCE_SPLIT`）。

建议删除第二条或改为："当 `tablet_internal_parallel_mode = FORCE_SPLIT` 时，即使 tablet 数足够也可强制切分，用于单 tablet 数据量过大的场景"。

### Issue 46: 贯穿示例在 Section 3.2 和 Section 4 之间 splitted_scan_rows 不一致

**Severity: Low**

Section 3.2 使用 `splitted_scan_rows = 500,000`（凑整）→ 4 个 split morsel。Section 4 用实际配置推导出 `splitted_scan_rows = 262,144` → 约 8 个 split morsel。同一个 200 万行 tablet 在两个 section 产出不同数量的 split。

建议 Section 3.2 标注"此处 `splitted_scan_rows` 取整数便于演示，实际值由 Section 4 的公式计算"，或统一使用 262,144。

### 交付评估

| 标准 | 状态 |
|------|------|
| 准确性 | ✅ |
| 概念自包含 | ✅ |
| 架构背景 | ✅ |
| 认知坡度 | ✅ |
| 范围聚焦 | ✅ |
| 贯穿示例 | ✅（minor inconsistency in Issue 46）|
| 可视化 | ✅（端到端数据流图已恢复）|

**文档可交付。** Issue 45-46 为 Low severity 的 polish 项，不阻塞交付。

---

**[Response]** Date: 2026-04-12

### Issue 45: Fixed

Section 1 第二条矛盾修正为"当 tablet 数少于 pipeline_dop 且单 tablet 数据量大时"，并在括号中注明 FORCE_SPLIT 可强制触发。

### Issue 46: Fixed

Section 3.2 增加标注"此处 `splitted_scan_rows` 取 500,000 便于演示，实际值由 Section 4 公式计算（约 262,144）"，并在示例末尾说明实际会产出约 8 个 split morsel。

12 轮 review、46 个 issue，全部处理完毕。

---

## Round 13

**[Review]** Date: 2026-04-12

### Summary

本轮 review 不校验代码事实，只从**文档结构、叙事逻辑和新手可读性**出发重新审视当前版本。整体上，文档已经具备较好的导读、术语表、贯穿示例和主路径数据流图，主链路也基本完整；但仍有几处结构性问题会影响第一次接触该机制的读者建立稳定心智模型。最主要的问题不是“信息不够”，而是**实现细节进入过早、主线与旁支权重不均、以及 DynamicMorselQueue 与 PhysicalSplitMorselQueue / LogicalSplitMorselQueue 的职责关系没有先讲透**。

以下问题按优先级排序。

### Issue 47: Section 1 仍然过早进入实现细节，读者在建立概念前就被类名和路径细节打断

**Severity: P1（新手阅读阻塞）**

Section 1 在刚说明“tablet 数少会导致并行度退化”之后，就立即进入 `ConnectorScanNode`、`LakeDataSourceProvider`、`IO task`、`TabletReader`、Driver 队列注入等实现细节。此时读者虽然知道“为什么要做 tablet 内并行”，但还没有充分掌握 `Morsel`、`MorselQueueFactory`、`DynamicMorselQueue`、`Split Morsel` 等基本词汇，容易在开头就失去跟进能力。

问题不在于这些信息不重要，而在于**出现时机太早**。对于新人，Section 1 更适合只做三件事：

1. 问题是什么
2. 解决思路是什么
3. 这篇文档会按什么层次展开

建议将实现类名和 Connector/Olap 路径差异延后到 Section 2 或 Section 5 中再展开。开头更适合放一张极简图：`一个 tablet -> 切成多个 split morsel -> 分给多个 Driver`。

### Issue 48: DynamicMorselQueue 与 PhysicalSplitMorselQueue / LogicalSplitMorselQueue 的关系仍未被“一句话讲透”

**Severity: P1（核心概念不清）**

这是当前版本中最影响新手理解的点之一。文档已经单独列出了三种 queue 的定义和协作图，但仍没有在最前面给出一个对读者最关键的总括：**谁负责切、谁负责分、谁负责给 Driver 消费**。

当前写法会让读者产生几个典型疑问：

- `PhysicalSplitMorselQueue / LogicalSplitMorselQueue` 既然也叫 queue，Driver 会不会直接从这里取任务？
- `DynamicMorselQueue` 被称为“容器/分发器”，那跨 Driver 的“分给谁”到底是它做的，还是外部逻辑做的？
- `split task` 和 `split morsel` 是同一层对象，还是前后转换的两个阶段？

文档真正想表达的是一条接力链，但这条链现在分散在多个小节中，读者需要自己拼：

- `PhysicalSplitMorselQueue / LogicalSplitMorselQueue`：临时切分器，只负责**计算** split
- 外部分发逻辑：负责把计算结果**分配**到目标 Driver
- `DynamicMorselQueue`：每个 Driver 的运行时队列，只负责**存和取**

建议在 Section 2.2 开头先加一段非常短的总纲，例如：

> 三者不是并列的消费队列，而是前后接力的三个角色：  
> `PhysicalSplitMorselQueue / LogicalSplitMorselQueue` 负责算出 split；  
> 外部分发逻辑负责把 split 分给目标 Driver；  
> `DynamicMorselQueue` 负责保存这些 morsel，并供各 Driver 消费。

然后再接现在的术语细节和协作图。这样读者先抓住“职责边界”，后面再看实现会顺很多。

### Issue 49: `Physical` vs `Logical` 的介绍仍偏实现条件导向，概念模型没有先立住

**Severity: P1（认知层次顺序不佳）**

Section 3 已经比早期版本清晰很多，但对第一次阅读的人来说，`Physical` 和 `Logical` 仍然更像“代码分支条件”，而不是两种不同的切分思想。

当前阅读体验是：先看到 keys type、`is_preaggregation` 等条件，再逐渐明白为什么要有两种切法。  
对新手更友好的顺序应该是：

1. 先用一句话定义两种切法的思想差异  
   - `Physical`：按精确 rowid 切，目标是把数据精确拆开给多人读  
   - `Logical`：按 short-key 近似切，在不能精确独立读取时尽量获得并行
2. 再解释为什么某些表可以用 `Physical`，某些只能用 `Logical`
3. 最后给实现条件

建议把“为什么有两种切分算法”提到条件表之前，让读者先有抽象模型，再接受实现细节。

### Issue 50: 主线在 Section 5 已经闭环，但 Section 6 和后续参考信息仍然略显抢戏

**Severity: P1（主次权重不均）**

对于“让新手理解存算分离内表的 tablet 内并行切分”这个目标，真正的主路径在：

`Section 1 -> Section 2 -> Section 3 -> Section 4 -> Section 5`

到这里其实已经完成了“问题 -> 概念 -> 算法 -> 判定 -> 调度”的完整闭环。  
但 Section 6 以及后续配置、对比、不变量等内容虽然有价值，却会让一部分新手在读完主路径后又重新掉进运行时细节和边界条件，注意力开始发散。

这类内容不需要删除，但建议在结构上再做一次“主线已结束”的显式提示。例如在 Section 5 末尾加一句：

> 到这里已经覆盖了存算分离表切分调度的主路径。后文主要补充完成判定、调度细节、配置和与 OlapScan 路径的差异。

这样读者能明确知道“后面是扩展阅读，不是继续理解主链路所必须的前置知识”。

### Issue 51: 术语已经有定义，但正文中仍存在局部漂移，尤其是 `split task` / `split morsel`

**Severity: P2（可读性）**

当前版本的术语表是有效的，但在 Section 5 的叙事中，以下几组词仍然切换较快：

- 原始 morsel / split morsel
- split task / split morsel
- 切分器 / 分发器 / 运行时队列
- 预计算所有 split / 外部注入 / 立即返回 EOF

其中最容易让读者停顿的是 `split task` 与 `split morsel` 的关系。现在文档暗含的是：

1. `TabletReader` 预计算出内部的 split task 描述
2. `ChunkSource` 收到 EOF 后，把这些描述包装成可调度的 split morsel
3. split morsel 再被注入各 Driver 的 `DynamicMorselQueue`

建议在 Section 5.2 和 Section 5.3 之间加一段桥接说明，把这层对象转换明确说出来，减少读者回跳术语表的次数。

### Issue 52: 开头关于旁路机制和路径差异的说明虽然已经收敛，但仍建议继续弱化

**Severity: P2（节奏优化）**

当前版本已经比旧版克制很多，但在文档前部仍然较早出现“另一条路径”“差异”“禁用的机制”等旁路说明。对于第一次阅读的用户，这类信息会让人过早意识到“还有很多例外和岔路”，削弱主线推进感。

建议原则：

- 开头只讲主路径必须知道的事实
- 路径差异、不可触发路径、历史兼容信息尽量放在正文讲完之后

即便保留，也建议以一两句注释带过，而不要在前几屏里占据过多注意力。

### Issue 53: 某些小节的信息密度仍偏高，适合先加一句“本节只需要记住什么”

**Severity: P2（阅读负担）**

`Section 2.2`、`Section 3.2/3.3`、`Section 6` 中个别小节，仍然是读者刚进入就看到状态字段、流程步骤、细节分支。对于维护者这很好，但新手更需要先得到一句结论，再进入细节。

建议在这类小节前补一句非常短的总结句，例如：

- “这一节只需要记住：Physical/LogicalSplitMorselQueue 负责切，DynamicMorselQueue 负责存和发。”
- “这一节只需要记住：TicketChecker 解决的是‘一个 tablet 被拆成多个 split 后，什么时候才能真正 EOS’。”

这种一句话结论能明显降低首次阅读的阻塞感。

### Closing Assessment

当前版本已经**可读、可交付**，比前几个版本有明显提升；本轮问题主要集中在“如何进一步让新人第一次读就读顺”。其中最值得优先处理的是：

1. Section 1 继续去实现细节化（Issue 47）
2. 用一句总纲彻底讲清三类 queue 的职责接力关系（Issue 48）
3. 先建立 `Physical / Logical` 的概念模型，再讲选择条件（Issue 49）
4. 在主路径结束处显式告诉读者“后文为扩展阅读”（Issue 50）

如果这几项处理到位，文档对新手的友好度还会再上一个台阶。

---

**[Response]** Date: 2026-04-12

Round 13 的 7 个 issue 全部同意，已直接重写相关段落落实到文档。以下为各 issue 对应的修改：

### Issue 47: Fixed — Section 1 收敛为纯问题+思路

Section 1 拆为 1.1（问题）、1.2（核心思路）、1.3（架构背景）。1.2 提出"切—分—读"三步作为全文叙事骨架，不出现任何 MorselQueue/Factory 类名。`ConnectorScanNode`、`LakeDataSourceProvider` 等实现路径延后到 1.3 架构背景（一句话）和 Section 5 正文。Shared Scan 注释从 Section 1 正文移至 1.3 末尾脚注。

### Issue 48: Fixed — Section 2.2 重写为"切—分—存"接力模型

Section 2.2 开头新增总纲段落，明确三者是接力关系而非并列的消费队列：切分器负责"切"、外部分发逻辑负责"分"、DynamicMorselQueue 负责"存和给 Driver 消费"。然后再接角色详解和协作流程图。协作图也改用"[切]→[分]→[读]"标签，与 Section 1.2 的三步叙事对应。

### Issue 49: Fixed — Section 3 先讲思想差异再讲选择条件

Section 3 开头重写为两段话：先解释 Physical（按 rowid 精确划分，子区间独立读取）和 Logical（按 short-key 近似划分，仍需 merge-on-read）的思想差异和适用直觉，然后才给出 `keys_type` / `is_preaggregation` 选择条件。

### Issue 50: Fixed — 主线结束后显式提示

Section 5.5 端到端数据流图之后、Section 6 之前增加一行分隔提示："以下为辅助机制、配置参数和扩展参考，不影响对主线流程的理解，按需阅读。"

### Issue 51: Fixed — split task 与 split morsel 的桥接说明

Section 5.3 开头新增桥接段落：TabletReader 产出的 `_split_tasks[]` 是内部子区间描述（split task），ChunkSource 收到 EOF 后将它们包装为可调度的 split morsel（赋予 `_split_context`），然后注入 DynamicMorselQueue。明确了从 split task 到 split morsel 的转换时机和语义。

### Issue 52: Fixed — 旁路信息弱化

Section 1 中 Shared Scan 和 OlapScan 路径差异的说明进一步收敛。Shared Scan 降为 1.3 末尾的一句脚注。OlapScan 差异降为 1.3 中"存算一体内表走 OlapScanNode，差异见 Section 8"一句带过。

### Issue 53: Fixed — 关键小节增加一句话结论

Section 2.2 开头、Section 3 开头、Section 6.1 开头各增加一句总结性引导句（如"三者不是并列的消费队列，而是前后接力"、"两种切分算法的核心区别在于切分依据不同"、"TicketChecker 解决的是：一个 tablet 被拆成多个 split 后，什么时候才能对外发出 EOS"），降低首次阅读的阻塞感。

---

## Round 14

**[Review]** Date: 2026-04-12

### Summary

本轮继续只看**结构、术语和新手可读性**，不做代码事实校验。Round 13 提出的主要问题已经基本落实：Section 1 明显收敛，Section 5 主线更完整，`split task -> split morsel` 的桥接也已经出现。当前版本已经可交付，但再读一遍后，仍有几处“读者会短暂停下来重新想一遍”的点，主要集中在 **Section 2.1/2.2 的概念表达** 和 **`split task` / `split morsel` 的概念边界**。

### Issue 54: Section 2.2 仍然混用了“类型”和“角色”，会让读者重新困惑一次

**Severity: P1（概念边界仍不够稳）**

Section 2.2 的标题和开头写的是：

> `切—分—存：三种 MorselQueue 的接力`

> `Section 1.2 提到的"切、分、读"三步，在实现中由三种 MorselQueue 子类型接力完成`

但正文列出的其实是三类**角色**，不是三种 `MorselQueue` 子类型：

1. `PhysicalSplitMorselQueue / LogicalSplitMorselQueue`（queue 子类型）
2. `ConnectorChunkSource + IndividualMorselQueueFactory`（分发逻辑，不是 queue）
3. `DynamicMorselQueue`（queue 子类型）

这会让第一次阅读的人又回到一个熟悉的疑问：  
“你到底是在按**类层次**讲，还是在按**职责分工**讲？”

这类困惑在这个位置代价比较高，因为 Section 2 本来就是用来“钉牢概念模型”的。如果这里再次混淆“类型”和“角色”，读者后面读 Section 5 时会反复在脑中纠正。

**建议**：

- 把标题从“**三种 MorselQueue 的接力**”改成“**三类角色的接力**”或“**切分、分发、消费三类角色**”
- 第一段也改成“由三类角色接力完成”，不要再说“三种 MorselQueue 子类型”
- 如果希望保留类型信息，建议单独加一个 3 列小表：

| 角色 | 具体对象 | Driver 是否直接消费 |
|---|---|---|
| 切分器 | `PhysicalSplitMorselQueue` / `LogicalSplitMorselQueue` | 否 |
| 分发逻辑 | `ConnectorChunkSource` + `IndividualMorselQueueFactory` | 否 |
| 运行时队列 | `DynamicMorselQueue` | 是 |

这样“按职责理解”和“按代码对象落位”就不会打架。

### Issue 55: `split task` 与 `split morsel` 的关系比之前清楚了，但仍然没有完全统一

**Severity: P1（对象生命周期仍有轻微摇摆）**

Round 13 后，Section 5.3 已经明确写出：

1. `TabletReader` 先产出 `_split_tasks[]`
2. EOF 后再包装成 split morsel
3. 注入 `DynamicMorselQueue`

这一段是明显改进。  
但 Section 3 的算法描述仍然会让读者感觉“切分器是不是直接返回 split morsel”：

- Section 2.2 写的是：切分器 `try_get()` 现场计算一个子区间，封装为 **split task** 返回
- Section 3.1 / 3.2 写的是：`try_get()` 封装为 `PhysicalSplitScanMorsel` / `LogicalSplitScanMorsel` 返回

对已经熟悉代码的人，这可能只是两个视角；但对新手来说，这会造成对象层级轻微摇摆：

- 算法层到底产出的是“子区间描述”
- 还是已经产出“可调度的 morsel”

**建议**二选一，保持全文统一：

1. 要么在 Section 3 明确说：  
   “这里从算法层视角描述 `try_get()` 的产出，重点是子区间描述本身；在存算分离主路径中，这些描述随后会在 Section 5.3 被包装成可调度的 split morsel。”

2. 要么在 Section 3 避免过早使用 `PhysicalSplitScanMorsel` / `LogicalSplitScanMorsel` 这类看起来已经是“最终调度对象”的名字，统一先叫“子区间描述”。

现在文档已经接近讲清，但还差这最后一步“术语完全收口”。

### Issue 56: Section 1.3 仍然稍早把读者注意力拉向“另一条路径”和“不可触发机制”

**Severity: P2（节奏问题）**

Section 1.3 现在已经比旧版克制很多，但对一个完全不懂机制的新读者来说，下面这两类信息仍然偏早出现：

- “存算一体走 OlapScanNode，差异见 Section 8”
- “Shared Scan 机制当前不可触发，本文不做展开”

这些内容都不是错，但它们会在读者刚进入文档主线时，提前制造两个分叉：

1. 还有另一条我暂时不懂的路径
2. 还有一个我不需要学但需要先知道不存在的机制

这会轻微削弱 Section 1 应有的线性推进感。

**建议**：

- Section 1.3 只保留一句最必要的架构背景：为什么存算分离表走 `ConnectorScanNode`
- `OlapScan` 的差异和 `Shared Scan` 的禁用说明，尽量后移到 Section 8 附近，或作为文末注释出现

这不是必须修改的 blocker，但会让开头更干净。

### Issue 57: Section 2.1 中 `MorselQueue` / `MorselQueueFactory` 出现多次，但层次关系还不够清晰

**Severity: P1（概念模型入口仍有卡点）**

Section 2.1 的组件图和术语表已经比旧版清楚很多，但对新手来说，这里仍然存在一个小的“第一眼卡顿”：`MorselQueue` 和 `MorselQueueFactory` 都在图里出现，也都在术语表里再次出现，但两者的**层次关系**还没有被一句话说透。

当前读者很容易出现这样的阅读感受：

1. 看到 `MorselQueue`：知道它是“持有 morsel 的队列”
2. 紧接着看到 `MorselQueueFactory`：知道它“决定如何将队列分配给各 Driver”
3. 但脑中仍然会停一下想：  
   - `Factory` 是在创建 queue，还是在分配 queue？  
   - `DynamicMorselQueue` 和 `MorselQueue` 是实例关系、子类型关系，还是两个不同层级？  
   - 我到底应该把 `MorselQueueFactory` 理解成“队列管理器”还是“Driver 分发策略”？

也就是说，当前 2.1 已经有定义，但还缺一句把关系串起来的人话，例如：

> 可以把 `MorselQueue` 理解为“装任务的盒子”，把 `MorselQueueFactory` 理解为“决定这些盒子如何分给各 Driver 的规则/工厂”。  
> 在存算分离路径中，最终每个 Driver 会拿到一个 `DynamicMorselQueue`，它是 `MorselQueue` 的一种具体实现。

**建议**：

- 在组件图下方、术语表之前加一两句“关系说明”，专门讲清 `MorselQueue`、`MorselQueueFactory`、`DynamicMorselQueue` 的上下层关系
- 术语表中的 `MorselQueueFactory` 定义也可以再收窄一点，突出它是“分配/组织 queue 给 Driver 的策略层”，而不是普通意义上的“创建对象工厂”
- 如果想更直观，可以在图中把这三者排成“抽象层 -> 策略层 -> 具体实现层”的视觉关系，避免读者只看到三个名字但看不出层次

### Closing Assessment

这一版已经进入“可以交付，只剩最后几处概念收口”的阶段。相比 Round 13，这次剩下的问题更集中，也更容易改：

1. 把 Section 2.2 从“类型描述”彻底改成“角色描述”（Issue 54）
2. 把 `split task` / `split morsel` 的对象边界彻底统一（Issue 55）
3. 再轻微弱化 Section 1 的旁路信息（Issue 56）
4. 在 Section 2.1 用一句关系说明彻底讲清 `MorselQueue` / `MorselQueueFactory` / `DynamicMorselQueue` 的层次（Issue 57）

如果这几项处理掉，整篇文档对第一次阅读的工程师会更”顺”，不太需要中途停下来重新翻前文确认概念。

---

**[Response]** Date: 2026-04-12

### Issue 54: Fixed — Section 2.2 从”类型描述”改为”角色描述”

标题改为”切分、分发、消费：三类角色的接力”。开头总纲明确”三者不是并列的消费队列，而是前后衔接的职责分工”。新增角色-对象-消费关系三列表格，让”按职责理解”和”按代码对象落位”互不打架。

### Issue 55: Fixed — split task / split morsel 术语收口

Section 3 开头新增一段：”本节描述切分器如何将 tablet 划分为子区间。切分器的 `try_get()` 每次产出一个子区间描述（split task）；这些 split task 随后会在 Section 5.3 被包装为可调度的 split morsel。” Section 3.1 的示例图中 “split morsel 1/2/3/4” 改为 “split task 1/2/3/4”，并在图后标注”经过 Section 5.3 包装后成为 split morsel”。术语表中新增 split task 条目。

### Issue 56: Fixed — Section 1.3 旁路信息后移

Section 1.3 只保留一句：存算分离架构下 FE 将 lake 表映射到 ConnectorScanNode。OlapScan 差异和 Shared Scan 禁用说明从 Section 1 移除（OlapScan 差异仍在 Section 8，Shared Scan 注释已在前几轮降级为文末参考）。

### Issue 57: Fixed — Section 2.1 增加层次关系说明

组件图下方、术语表之前新增一段：”MorselQueue 是'装任务的盒子'，MorselQueueFactory 是'决定这些盒子如何分给各 Driver 的策略'。在存算分离路径中，Factory 为每个 Driver 创建一个 DynamicMorselQueue。” 术语表中 MorselQueueFactory 的定义收窄为”将队列分配给各 Driver 的策略层”。

---

## Round 15

**[Review]** Date: 2026-04-12

### Summary

Round 14 的主要修改已经落实，Section 2 的概念入口现在顺很多，`MorselQueue` / `MorselQueueFactory` / `DynamicMorselQueue` 的关系也基本讲清了。本轮继续只看**表达一致性和新手认知连续性**，不做代码事实校验。当前剩下的问题已经不多，主要是：**“split task / split morsel”虽然大体统一了，但 Section 3 和 Section 4 里还有几处残留表述，会让读者觉得对象边界又轻微滑回去了。**

### Issue 58: Section 3 已经把算法层产物改成 `split task`，但产出流程最后一步仍写成返回 `PhysicalSplitScanMorsel` / `LogicalSplitScanMorsel`

**Severity: P1（术语收口未完成）**

Round 14 已经把 Section 3 的总纲、示例和术语表改成了“算法层产出的是 split task，随后在 Section 5.3 包装为 split morsel”。这个方向是对的。  
但 Section 3.1 / 3.2 的“产出流程”最后一步仍然写成：

- `封装为 PhysicalSplitScanMorsel(RowidRangeOptionPtr) 返回`
- `封装为 LogicalSplitScanMorsel(ShortKeyRangesOptionPtr) 返回`

这会把读者重新拉回到一个刚刚才被解决的问题上：  
“所以切分器到底直接返回的是 split task，还是已经是一个 morsel？”

也就是说，文档的**段落级叙事**已经统一了，但**步骤级措辞**还没有一起更新，导致对象边界又轻微滑回去了。

**建议**：

- 把这两处最后一步改成与前文一致的说法，例如：
  - `封装为一个 rowid 子区间描述（split task）返回`
  - `封装为一个 short-key 子区间描述（split task）返回`
- 如果你仍然想保留代码里的具体类型名，建议在括号中降级注明：
  - `（代码中对应 PhysicalSplitScanMorsel，随后在 Section 5.3 的主路径中视作 split task 被继续包装/分发）`

当前的问题不是“术语错”，而是**文档内部在两个视角之间来回切换**，新手会再次停顿。

### Issue 59: Section 4 的示例已经站在“判定逻辑”层，但结尾直接跳到“产出约 8 个 split morsel 分配给 4 个 Driver”，略微抢跑了 Section 5

**Severity: P2（认知层次轻微前跳）**

Section 4 本质上是在回答两个问题：

1. 会不会切分
2. 大概切多细

但现在示例的收尾是：

> 启用切分，产出约 8 个 split morsel 分配给 4 个 Driver。

这句话本身不难懂，但它已经提前跳到了 Section 5 的“包装与分发”视角。对已经熟悉全文结构的人没问题；对第一次阅读的人，会有一个小小的层次前跳：

- Section 4 本来刚算出 `scan_dop` 和 `splitted_scan_rows`
- 结果一句话直接说到了“split morsel 分配给 Driver”
- 可真正“什么时候从 split task 变成 split morsel、怎么分到 Driver”是 Section 5 才讲的

**建议**：

- 把 Section 4 结尾收敛为算法/判定视角，例如：
  - `启用切分，预计会产生约 8 个子区间描述；这些描述随后会在 Section 5 中被包装并分配给 4 个 Driver。`

这样既保留数量感，又不会抢跑主线后半段。

### Issue 60: Section 2.1 的组件图本身信息量不足，没承担起解释 `MorselQueue -> MorselQueueFactory` 关系的职责

**Severity: P1（概念模型图仍不够直观）**

Round 14 已经在组件图下方加了“层次关系”文字说明，这对文字读者有帮助；但这里的核心问题其实不只是“缺一句解释”，而是**图本身没有把层次关系画出来**。当前图里：

```text
MorselQueue
   ↓
MorselQueueFactory
   ↓
每个 Driver 一个 DynamicMorselQueue
```

这三行对于熟悉代码的人可以脑补出含义，但对新手来说，箭头语义并不清楚：

- `MorselQueue -> MorselQueueFactory` 是“包含关系”、“创建关系”、“配置关系”还是“处理流程”？
- `MorselQueueFactory` 为什么会出现在 `MorselQueue` 的下游？它看起来像是在“消费 queue”，而不是“决定 queue 如何分给 Driver”
- `DynamicMorselQueue` 是从 `MorselQueueFactory` 里“创建出来”的，还是只是被 `Factory` 选择/分配的一种具体队列？

也就是说，**现在的图形结构仍然在误导读者把这三者理解成一条线性数据流**，但它们其实更接近：

- `MorselQueue`：抽象的任务容器
- `MorselQueueFactory`：决定这些容器如何组织/分配给 Driver 的策略层
- `DynamicMorselQueue`：在当前路径下采用的一种具体 per-driver 队列实现

这类关系更适合画成“抽象层 -> 策略层 -> 具体实现/分配结果”，而不是单纯一条竖线。

**建议**：

1. 不要只靠图下文字补救，直接重画 Section 2.1 组件图。
2. 把图改成能表达“层次”和“作用”的结构，例如：

```text
FE 下发 scan range
        │
        ▼
原始 morsel
        │
        ▼
MorselQueue（抽象：装 morsel 的队列）

MorselQueueFactory（策略：决定队列如何分给 Driver）
        │
        └── 在存算分离路径中：
            每个 Driver 一个 DynamicMorselQueue
                    │
                    ▼
            Driver 从自己的 queue 取 morsel
                    │
                    ▼
            ChunkSource → IO task → ChunkBuffer
```

3. 如果希望更清楚，还可以在图里显式标注箭头含义，比如：
   - `抽象概念`
   - `分配策略`
   - `当前路径采用的具体实现`
4. 图画清楚之后，图下那段“盒子/策略”文字说明反而可以缩短，因为图本身就已经承担了解释职责。

当前 2.1 的问题已经不再是“术语没定义”，而是**图没有把最重要的关系直观表达出来，导致读者必须靠读图下文字来补理解**。对于概念入口图来说，这说明图本身还不够强。

### Issue 61: 从第一性原理看，Section 2.1 不该先摆类名，而应先回答“系统在调度什么、谁在分配什么”

**Severity: P1（2.1 的呈现顺序仍未命中最核心问题）**

Issue 60 说的是“图不够直观”，但再往前追一层，Section 2.1 的真正问题其实是：**它现在仍然是从类名出发组织内容，而不是从调度问题本身出发。**

对一个第一次读这篇文档的人来说，Section 2.1 最应该先回答的不是：

- 什么叫 `MorselQueue`
- 什么叫 `MorselQueueFactory`
- 什么叫 `DynamicMorselQueue`

而是这 4 个更基础的问题：

1. **系统真正调度的对象是什么？**  
   是 `morsel`，也就是一个可被 Driver 执行的扫描任务。本文场景下，初始状态通常是“每个 tablet 一个原始 morsel”。

2. **这些任务放在哪里，Driver 从哪里拿？**  
   放在 queue 里，所以 `MorselQueue` 的本质是“装 morsel 的容器”。

3. **多个 Driver 各该用哪个 queue，原始 morsel 怎么分进去，后续 split morsel 又怎么再塞进去？**  
   这是 `MorselQueueFactory` 负责的。所以它的本质不是普通意义上的“对象工厂”，而是“组织 queue 拓扑并决定 morsel 分配规则的策略层”。

4. **当前这条存算分离路径具体采用什么策略？**  
   使用 `IndividualMorselQueueFactory`，给每个 Driver 一个 `DynamicMorselQueue`；原始 morsel 先被分配进去，运行时切出来的 split morsel 也继续被注入进去。

也就是说，`原始 morsels` 和 `MorselQueueFactory` 的关系不是“上下游对象”，而是：

- `原始 morsels` 是 **被分配的输入工作项**
- `MorselQueueFactory` 是 **决定这些工作项如何落到各 Driver queue 中的策略层**

这也是为什么当前图里把

```text
MorselQueue
   ↓
MorselQueueFactory
   ↓
DynamicMorselQueue
```

画成一条线，会天然让读者误解为“前后处理链”。

**建议不要继续修补现有图，而是重写 2.1 的呈现顺序**：

#### 建议结构

1. **先讲调度骨架，不急着抛具体类名**

```text
scan range
   ↓
原始 morsels（初始扫描任务，每个 tablet 一个）
   ↓
MorselQueueFactory（决定这些任务如何分给 Driver）
   ↓
各 Driver 自己的 MorselQueue
   ↓
Driver 取出 morsel 执行
```

这张图先让读者明白：

- `morsel` 是被调度的对象
- `queue` 是存放这些对象的地方
- `factory` 是决定怎么分配这些对象和这些 queue 的策略层

2. **再讲本文这条路径的具体实例化**

```text
FE 下发 scan ranges
        │
        ▼
原始 morsels（每个 tablet 一个）
        │
        ▼
IndividualMorselQueueFactory
  - 初始分配原始 morsels
  - 后续注入 split morsels
        │
        ├── Driver 0 -> DynamicMorselQueue
        ├── Driver 1 -> DynamicMorselQueue
        ├── Driver 2 -> DynamicMorselQueue
        └── Driver 3 -> DynamicMorselQueue
```

这样 `DynamicMorselQueue` 的位置就自然了：  
它不是“Factory 的下一个节点”，而是“当前策略下每个 Driver 实际拿到的具体 queue 实现”。

3. **最后再给术语表**

术语表应该是“已有图景后的查词工具”，而不是让读者靠词表反推系统关系。

#### 一句话结论

Section 2.1 最该改的不是某一句定义，而是**整节的建模顺序**：

- 先讲“被调度的对象是什么”
- 再讲“对象放哪”
- 再讲“谁决定怎么分”
- 最后讲“当前路径里具体是哪种 factory、哪种 queue”

如果按这个顺序重做，`原始 morsels` 和 `MorselQueueFactory` 的关系就会自然显现，不需要读者自己猜箭头是什么意思。

---

**[Response]** Date: 2026-04-12

### Issue 58: Fixed

Section 3.1 产出流程最后一步改为"封装为一个 rowid 子区间描述（split task）返回"，Section 3.2 改为"封装为一个 short-key 子区间描述（split task）返回"。与 Section 3 开头的总纲和术语表一致。

### Issue 59: Fixed

Section 4 示例结尾改为"预计产生约 8 个子区间描述（split task）；这些描述随后会在 Section 5 中被包装并分配给 4 个 Driver"。收敛到判定层视角，不再抢跑分发层。

### Issue 60 + 61: Fixed — Section 2.1 重写

按 Round 15 建议的建模顺序重写 Section 2.1：

1. 先用 4 个问答建立调度骨架（被调度的对象是什么 → 放哪 → 谁决定怎么分 → 本路径具体用什么），不急着抛类名
2. 给出两层组件图：通用结构（morsel → Factory → per-Driver queue → 执行）+ 存算分离路径的具体实例化（IndividualMorselQueueFactory → per-Driver DynamicMorselQueue），Factory 不再画成 MorselQueue 的"下游节点"，而是作为策略层决定 queue 拓扑
3. 术语表放在图之后，作为"已有图景后的查词工具"

### Closing Assessment

---

这一版文档已经非常接近“术语完全闭环”的状态了。现在剩下的问题都不是结构性大问题，而是**最后几句跨层表述没有完全同步更新**：

1. Section 3 的流程步骤还残留“直接返回 morsel”的表述（Issue 58）
2. Section 4 的示例结尾略微提前跳到了 Section 5 的分发层（Issue 59）
3. Section 2.1 的图还没有真正把 `MorselQueue` / `MorselQueueFactory` / `DynamicMorselQueue` 的关系画清楚（Issue 60）
4. Section 2.1 的呈现顺序仍应从“调度骨架”而不是“类名定义”出发（Issue 61）

如果这几处一起收掉，文档的概念入口和对象链路都会更稳定：前面先从调度骨架看懂“系统在调度什么、谁在分配什么”，再落到 `queue / factory / DynamicMorselQueue` 的具体实现；后面则能顺着 `split task -> split morsel -> Driver 消费` 一路读下去。

---

## Round 16

**[Review]** Date: 2026-04-12

### Summary

Round 15 的方向是对的，而且这次 `Section 2.1` 的重写已经明显好于之前版本：它终于先回答“系统在调度什么、谁在分配什么”，再落到类名和当前路径实例化。`split task -> split morsel` 的主链也基本稳定了。  
本轮继续只看**表达一致性与图示是否真正承担解释职责**，不做代码事实校验。当前还剩两处比较小但真实的残留问题：**算法层术语还有几句没完全收口**，以及 **`2.1` 的通用图里，Factory 如何把原始 morsels 落到 per-driver queues 这一步仍然有点隐。**

### Issue 62: Section 3 已经切到“算法层视角”，但仍残留几处 `morsel` 表述，会轻微打破 `split task -> split morsel` 的分层

**Severity: P2（术语一致性残留）**

Round 15 已经把 Section 3 的主叙事改成：

- 算法层产出的是 `split task`
- Section 5.3 才把它们包装成 `split morsel`

这个主线已经正确。  
但 Section 3 里还有几处零散表述仍沿用“morsel”视角，例如：

- `实际 morsel 大小可达 ...`
- `将 block 级别的 short-key 区间分配给不同的 morsel`
- `当前和下一个 morsel 平分`
- `可能导致 morsel 大小不均匀`

这些句子单独看没问题，但放在现在已经明确分层的文档里，会让读者有一点点“刚刚不是说这里还在 split task 层吗？”的迟滞感。

**建议**：

- 在 Section 3 中，凡是还处在算法层、尚未进入 Section 5.3 包装/分发语境的地方，尽量统一改成：
  - `子区间描述`
  - `split task`
  - 或中性表达如 `子区间大小`

例如：

- `实际 morsel 大小可达 ...` → `单个子区间描述覆盖的行数可达 ...`
- `分配给不同的 morsel` → `划分为不同的子区间描述`
- `当前和下一个 morsel 平分` → `当前和下一个子区间描述平分`

这是一个纯表达层的小收口，但会让“算法层 vs 调度层”的边界更稳定。

### Issue 63: Section 2.1 的“通用结构图”已经比之前清楚，但 `Factory` 如何把原始 morsels 放进 per-driver queues 仍然有点隐

**Severity: P2（图示仍可更强）**

当前 2.1 的通用图是：

```text
scan ranges → 原始 morsels（每个 tablet 一个）
                    │
                    ▼
            MorselQueueFactory（策略：决定 morsel 如何分给各 Driver）
                    │
                    ▼
            各 Driver 的 MorselQueue → Driver 取 morsel 执行
```

这已经比旧图好很多，因为它至少把 `原始 morsels` 放到了 `Factory` 上游。  
但如果从“图本身就能解释关系”的标准看，这里还差半步：**图里仍然没有显式表现出“Factory 负责把原始 morsels 分配进 per-driver queues”这个动作本身。**

也就是说，读者现在大概率能理解：

- `Factory` 在原始 morsels 和 per-driver queues 之间

但还不一定能第一眼看清：

- `Factory` 不只是“决定谁用哪个 queue”
- 还负责把原始 morsels **落入这些具体 queue**
- 后续 split morsels 也会沿着同一分配框架被再次注入

**建议**：

- 把通用图再轻微增强成“策略 + 落位动作”更明确的形态，例如：

```text
scan ranges
   ↓
原始 morsels（待分配的工作项）
   ↓
MorselQueueFactory（决定如何组织 per-driver queues，并把 morsels 分配进去）
   ↓
Driver 0 queue   Driver 1 queue   Driver 2 queue ...
      ↓               ↓               ↓
   Driver 取出执行   Driver 取出执行   Driver 取出执行
```

或者把 `Factory` 画成侧边控制框，而不是主干节点，这样“它是策略层，不是数据流节点”会更强。

当前版本已经不会让人误解成 `MorselQueue -> MorselQueueFactory -> DynamicMorselQueue` 的线性链了；这一条建议的目标是让图再往前走一步，从“能读懂”提升到“第一眼就懂”。

### Closing Assessment

这一版其实已经进入 polish 阶段了。Round 15 解决的是大的建模顺序问题，Round 16 剩下的是两类更小的收口：

1. Section 3 中仍有少量“算法层里沿用 morsel 说法”的残留（Issue 62）
2. Section 2.1 的通用图还可以再明确一点，把“Factory 将原始 morsels 落到 per-driver queues”的动作画出来（Issue 63）

如果这两处也一起收掉，前面的概念入口会更顺，后面的对象链条也会更稳。

---

**[Response]** Date: 2026-04-12

### Issue 62: Fixed

Section 3 中 4 处残留的 "morsel" 算法层表述统一改为中性术语：
- "实际 morsel 大小可达" → "单个子区间覆盖的行数可达"
- "分配给不同的 morsel" → "划分为不同的子区间描述"
- "当前和下一个 morsel 平分" → "当前和下一个子区间平分"
- "导致 morsel 大小不均匀" → "导致子区间大小不均匀"

### Issue 63: Fixed

Section 2.1 通用图增强：Factory 从单线箭头改为分叉结构（`┌──┼──┐`），显式画出"把 morsels 分配进 per-driver queues"的落位动作。Factory 的标注也从"策略：决定如何分给各 Driver"补充为"决定如何组织 per-driver queues，并把 morsels 分配进去"。

---

## Round 17

**[Review]** Date: 2026-04-12

### Summary

Round 16 之后，文档已经基本进入最终 polish 阶段。`Section 2.1` 的调度骨架图现在足够直观，`Factory` 如何把原始 morsels 分配到 per-driver queues 也已经能从图里直接看出来。`Section 3` 的大部分算法层表述也已经切回 `split task / 子区间` 视角。  
本轮继续只看**表达一致性**，不做代码事实校验。整体上我没有再看到新的结构性问题，只剩下一个很小但明确的术语残留。

### Issue 64: Section 3.2 还残留 3 处算法层里的 `morsel` 表述，没有和上一轮一起收口

**Severity: P2（最后的术语清理）**

Round 16 已经把 Section 3 中多数“算法层里使用 morsel 说法”的表述改掉了，但 `Section 3.2 Logical 切分` 仍然有 3 处残留：

1. 核心状态里的注释：

```cpp
int64_t _sample_splitted_scan_blocks;     // 每个 morsel 目标 block 数
```

2. 产出流程里的步骤说明：

```text
seek_range 耗尽 → 推进（单个 morsel 可跨 seek_range 边界）
```

3. 自适应步进逻辑的小标题：

```text
跨 seek_range morsel
```

这些说法单独看都合理，但放在当前已经明确分层的文档里，仍然会轻微破坏“Section 3 只讲 split task / 子区间描述，Section 5 才讲 split morsel”的稳定视角。

**建议**：

- `每个 morsel 目标 block 数` → `每个子区间描述的目标 block 数`
- `单个 morsel 可跨 seek_range 边界` → `单个子区间描述可跨 seek_range 边界`
- `跨 seek_range morsel` → `跨 seek_range 子区间描述`

这已经是非常小的 polish 项了，但如果要把术语视角真正做到从头到尾完全一致，这 3 处最好也一起收掉。

### Closing Assessment

当前版本已经没有新的结构性问题。Round 17 只剩这一处术语残留：

1. `Section 3.2` 还有 3 处算法层里的 `morsel` 用词没改完（Issue 64）

这处修完之后，我认为这篇文档在”结构、主线、概念模型、新手可读性、术语一致性”这几个维度上就基本完整了。

---

**[Response]** Date: 2026-04-12

### Issue 64: Fixed

Section 3.2 的 3 处残留改完：
- `每个 morsel 目标 block 数` → `每个子区间的目标 block 数`
- `单个 morsel 可跨 seek_range 边界` → `单个子区间可跨 seek_range 边界`
- `跨 seek_range morsel` → `跨 seek_range 子区间`

17 轮 review、64 个 issue，全部处理完毕。

---

## Round 18

**[Review]** Date: 2026-04-12

### Summary

基于 Round 17 的回复再次通读当前文档，本轮继续只看**结构、逻辑主线、新手可读性和术语一致性**，不做代码事实校验。

这一轮**未发现新的可操作问题**。

重点复核结果：

- `Section 2.1` 已经真正改成“调度骨架 -> 当前路径实例化 -> 术语表”的顺序，且图中能够直接看出 `MorselQueueFactory` 将原始 morsels 分配到 per-driver queues。
- `Section 2.2` 的“角色”与“类型”边界现在稳定，不再把分发逻辑误写成一种 queue。
- `Section 3` 已经稳定在算法层视角，`split task -> split morsel` 的对象链条没有再出现明显摇摆。
- `Section 4` 没有再提前跳入分发层。
- `Section 5` 仍然是清晰的主线闭环，后文也明确标注为参考性内容。

### Closing Assessment

以“第一次接触该机制的工程师”为目标读者来判断，当前版本已经满足：

1. 概念入口清晰
2. 主线顺序稳定
3. 术语前后一致
4. 图和文字能够互相支撑，而不是互相补锅

在”文档结构、逻辑是否清晰连贯、能否让新手看懂”这个 review 目标下，我这里没有新的 review finding 了。

---

**[Response]** Date: 2026-04-12

Acknowledged. 18 轮 review、64 个 issue，全部处理完毕。文档交付。

---
