# 存算一体 BE FD 数量过多 —— 根因分析

## 现象

- `lsof` 观察到 fd 暴涨时，增加的文件主要是 `index.l0.xxx` 类型
- 过一段时间 fd 恢复正常，这些 `index.l0` 文件从 `lsof` 中消失
- 示例：apply version 112 时出现 `index.l0.112.0`，apply 结束后该 fd 消失

## 根因：PersistentIndex 缓存淘汰 + 并发 apply 导致 L0 文件 fd 瞬时暴涨

### 核心链路

apply_rowset_commit 的主要流程：

```
tablet_updates.cpp::_apply_rowset_commit()
  ├── index_cache.get_or_create(tablet_id)  // 从缓存获取或创建 PrimaryIndex
  ├── index.load(&_tablet)                  // 加载 PersistentIndex（打开 L0 文件 fd）
  ├── index.prepare(version)
  ├── index.upsert()/erase()               // 写 WAL，追加到 L0 文件
  ├── index.commit()                        // 可能创建新的 L0 文件（snapshot/flush）
  ├── index.on_commited()                   // 清理过期文件
  └── index_cache.release(index_entry)      // 释放回缓存（fd 保持打开）
```

### L0 文件 fd 的生命周期

1. **打开时机**：`ShardByLengthMutableIndex::load()` 中以 `WritableFile`（可写）模式打开 L0 文件

```cpp
// be/src/storage/persistent_index.cpp ~line 2316
WritableFileOptions wblock_opts;
wblock_opts.mode = FileSystem::MUST_EXIST;
ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, index_file_name));
```

2. **持有期间**：`_index_file`（WritableFile）一直被 `ShardByLengthMutableIndex` 持有，用于 WAL 追加写入

3. **关闭时机**：仅在以下场景关闭：
   - `PrimaryIndex` 从 `DynamicCache` 中被淘汰（`unload()`），触发 `PersistentIndex` 析构
   - commit 时触发 kSnapshot 或 kFlush+reload，旧 `_index_file` 被替换
   - `PrimaryIndex::unload_without_lock()` → `_persistent_index.reset()` → 析构链

### FD 暴涨的直接原因

**apply 线程池并发 + PrimaryIndex 缓存未命中 → 大量 L0 文件同时打开**

具体分析：

1. **apply 线程池并发度 = CPU 核数**（默认 `transaction_apply_worker_count = 0`，即等于 CPU 核数）
   - 64 核机器 → 最多 64 个 tablet 同时 apply

2. **PrimaryIndex 缓存有内存上限**（`DynamicCache`，基于 `update_memory_limit_percent`）
   - 当主键表 tablet 数量多时，缓存无法容纳所有 PrimaryIndex
   - 部分 PrimaryIndex 被淘汰（`unload()`），关闭 L0 fd

3. **批量 publish 触发大量并发 apply**
   - FE 批量下发 publish version 任务
   - 大量 tablet 需要同时 apply
   - 之前被淘汰的 PrimaryIndex 需要重新 load（重新打开 L0 文件）
   - 同时 apply 的 tablet 都持有各自的 L0 fd

4. **apply 结束后 fd 恢复**
   - apply 完成后，PrimaryIndex release 回缓存
   - 缓存内存超限 → 淘汰旧的 PrimaryIndex → `unload()` → 关闭 L0 fd
   - fd 数量回落

### 时序示意

```
时间线 ──────────────────────────────────────────────────────>

Tablet A:  [load L0 (fd+1)] ─── apply ─── [release to cache]
Tablet B:  [load L0 (fd+1)] ─── apply ─── [release to cache]
Tablet C:    [load L0 (fd+1)] ── apply ─── [release to cache]
...
Tablet N:      [load L0 (fd+1)] apply ─── [release to cache]

fd 数量:   ████████████████████████████████  ← 暴涨
                                                  ↓ 缓存淘汰 → unload → fd 关闭
fd 数量:   ████                               ← 恢复正常
```

### 关键代码路径

| 文件 | 位置 | 说明 |
|------|------|------|
| `tablet_updates.cpp:~1380` | `index_cache.get_or_create(tablet_id)` | 获取或创建 PrimaryIndex |
| `primary_index.cpp:~1188` | `_persistent_index->load_from_tablet(tablet)` | 加载 PersistentIndex |
| `persistent_index.cpp:~5287` | `status = load(index_meta)` | 从 meta 加载 index，打开 L0 文件 |
| `persistent_index.cpp:~2316` | `_index_file = fs->new_writable_file(...)` | **实际打开 L0 文件 fd** |
| `persistent_index.cpp:~1987` | `_shards[i]->append_wal(... _index_file ...)` | WAL 追加写入，使用 L0 fd |
| `persistent_index.cpp:~3746` | `PersistentIndex::on_commited()` | apply 结束后的清理 |
| `tablet_updates.cpp:~1854` | `index_cache.release(index_entry)` | 释放回缓存，fd 保持打开 |
| `primary_index.cpp:~1112` | `_persistent_index.reset()` | 缓存淘汰时关闭 fd |
| `update_manager.cpp:~102` | `ThreadPoolBuilder("update_apply").set_max_threads(CpuInfo::num_cores())` | apply 线程池大小 |

### 为什么是 L0 文件而不是 L1/L2

- L0 文件以 **WritableFile** 模式打开（可读写），fd 一直由 `_l0->_index_file` 持有
- L1/L2 文件以 **RandomAccessFile** 模式打开，也由 `ImmutableIndex::_file` 持有
- **但** L0 是每个 PersistentIndex 都一定有的（每个主键 tablet 一个），而 L1/L2 不一定有
- 因此 L0 fd 数量 ≈ 缓存中的 PrimaryIndex 数量 + 正在 apply 的 tablet 数量

## 影响因素

| 因素 | 影响 | 当前默认值 |
|------|------|-----------|
| 主键表 tablet 数量 | tablet 越多，并发 apply 越多 | - |
| CPU 核数 | 决定 apply 线程池大小 | `num_cores` |
| `transaction_apply_worker_count` | 限制并发 apply 数 | 0（= CPU 核数） |
| PrimaryIndex 缓存容量 | 缓存越小，淘汰越频繁，每次 apply 需要重新 load | 基于 `update_memory_limit_percent` |
| `update_cache_expire_sec` | 缓存过期时间 | 360s（6分钟） |
| 批量 publish 的 tablet 数 | 一次性触发的 apply 数量 | 取决于业务导入频率 |

## FD 暴涨量估算

在最坏情况下：

```
瞬时 fd 增量 ≈ min(并发 apply 线程数, 需要 apply 的 tablet 数) × (1 L0 fd + L1 fd + L2 fd)
```

典型场景：
- 64 核机器，1000 个主键表 tablet 同时需要 apply
- 每个 PersistentIndex 有 1 个 L0 + 1 个 L1 = 2 个 fd
- 瞬时 fd 增量 ≈ 64 × 2 = 128 个（仅来自 apply 线程）
- 加上缓存中已有的 index fd → 总量可能数千

如果缓存频繁淘汰（内存不足），且导入频率高：
- 每轮 publish 都需要重新 load → fd 频繁开关
- 多轮 publish 重叠时 fd 可能进一步叠加

## 排查建议

```bash
# 1. 查看当前 BE 进程 fd 总数
ls /proc/<be_pid>/fd | wc -l

# 2. 统计 index.l0 相关 fd 数量
ls -la /proc/<be_pid>/fd/ 2>/dev/null | grep 'index.l0' | wc -l

# 3. 查看 PrimaryIndex 缓存大小
curl http://<be_host>:8040/metrics 2>/dev/null | grep update_primary_index_num

# 4. 查看 apply 线程池状态
curl http://<be_host>:8040/metrics 2>/dev/null | grep update_apply

# 5. 查看 fd 软硬限制
curl http://<be_host>:8040/metrics 2>/dev/null | grep process_fd_num
```
