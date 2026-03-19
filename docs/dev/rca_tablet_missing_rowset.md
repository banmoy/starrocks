# RCA: 主键表 Tablet 迁移导致 Rowset Meta 丢失，BE 重启后报 Missing Rowset

主键表 tablet 在短时间内经历两次磁盘迁移（A→B→A）时，由于 GC 与迁移之间的竞态条件，新 tablet 的元数据会被误删。BE 重启前 tablet 依赖内存正常工作，**重启后 tablet 不可用且无法自动恢复**。

## 摘要

| 项目 | 说明 |
|------|------|
| 现象 | BE 重启后 tablet 加载失败，报错 `tablet init missing rowset` |
| 触发条件 | 同一 tablet 短时间内来回迁移（磁盘 A→B→A），且 GC 在两次迁移之间清除了旧 tablet 的 RocksDB 元数据但尚未从 `_shutdown_tablets` 队列中移除 |
| 根因 | `TabletUpdates::clear_meta()` 按 `tablet_id` 清除元数据，不区分 `tablet_uid`，导致同磁盘上新 tablet 的 rowset meta 被误删 |
| 影响范围 | 仅影响**存算一体模式下的主键表**；受影响 tablet 在 BE 重启前正常（依赖内存），**重启后不可用且无法自动恢复** |

## 问题描述

BE 重启后，部分主键表 tablet 加载失败，日志报错：

```
tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129]
  #pending:0 all: active:0,1 missing:0,1
```

含义是：tablet 8199056 的 TabletMetaPB 记录了 version 129 引用 rowset {0, 1}，但在 RocksDB 中找不到对应的 rowset meta。自动修复（`TabletUpdates::_purge_versions_to_fix_rowset_missing_inconsistency`）因只有一个 version 无法 purge，修复失败，tablet 不可用。

重启前 tablet 工作正常——因为活跃 tablet 的 rowset 信息已加载到内存中，不依赖 RocksDB 读取。只有重启时才会从 RocksDB 重新加载元数据，此时才暴露 rowset meta 丢失的问题。

## 影响与排查

### 影响范围

- **表类型**：仅主键表（Primary Key Table）受影响，非主键表的 `_remove_tablet_meta` 走不同的清理路径
- **部署模式**：仅存算一体模式（Shared-Nothing），存算分离模式不涉及本地磁盘迁移
- **触发场景**：同一 tablet 短时间内在两块磁盘之间来回迁移（A→B→A），且 GC 恰好在两次迁移之间执行

### 如何判断是否命中此 Bug

**第一步**：搜索 BE 日志中的报错关键词：

```bash
grep "tablet init missing rowset" be.WARNING
```

**第二步**：若找到匹配，提取 tablet_id，检查该 tablet 是否在短时间内经历了来回迁移：

```bash
grep "storage migrate.*tablet_id=<tablet_id>" be.INFO
```

若同一 tablet 在数分钟内出现两次迁移记录，且目标磁盘形成 A→B→A 的模式，则大概率命中此 Bug。

### 规避与恢复

- **规避**：避免对同一 tablet 短时间内触发来回迁移；升级到包含修复的版本
- **恢复**：受影响 tablet 无法自动恢复，需通过 `ADMIN SET REPLICA STATUS` 将对应副本标记为 bad，由 FE 调度副本补齐

## 根因分析

### 迁移的正常流程

tablet 的元数据存储在所在磁盘的 RocksDB 中，核心包括 **TabletMetaPB**（tablet 基本信息，含 tablet_id、tablet_uid、版本号、引用的 rowset 列表等）和 **Rowset Meta**（每个 rowset 的详细信息）。TabletMetaPB 相当于"目录"，rowset meta 才是实际内容，**两者缺一不可**。

磁盘迁移通过 `TabletManager::create_tablet_from_meta_snapshot` 实现，分三步：

1. **写入新 tablet 完整元数据**：通过 `write_batch` 将 TabletMetaPB 和所有 rowset meta **打包为一个原子批次**写入目标磁盘的 RocksDB
2. **注册新 tablet 并回收旧 tablet**：通过 `TabletManager::_add_tablet_unlocked` 将新 tablet 注册为活跃 tablet，同时将旧 tablet 标记为待回收，加入 `_shutdown_tablets` 队列
3. **再次持久化 TabletMetaPB**：调用 `Tablet::save_meta` 将内存中的 TabletMetaPB 写回 RocksDB。注意：此步**只写 TabletMetaPB，不写 rowset meta**

第 2 步有一个需要关注的细节：`TabletManager::_add_shutdown_tablet_unlocked` 在将旧 tablet 加入 `_shutdown_tablets` 队列时，若队列中已有相同 `tablet_id` 的条目，会先调用 `TabletManager::_remove_tablet_meta` 清理残留元数据，再用新条目替换。

此外，迁移开始前 `EngineStorageMigrationTask::_storage_migrate` 还有一道额外防护：读取目标磁盘的 TabletMetaPB，若发现状态为 `TABLET_SHUTDOWN` 的旧条目，则提前调用 `TabletManager::delete_shutdown_tablet` 将其从 `_shutdown_tablets` 中移除，避免后续替换时误清新数据。

### 问题如何发生

本案中，主键表 tablet 8199056 在 6 分钟内经历了两次磁盘迁移（data1→data2→data1）。每次迁移会在目标磁盘上创建一个新实例（分配新的 `tablet_uid`），同时将旧实例标记为待回收，因此产生了三个实例：

| 实例 | tablet_uid | 所在磁盘 | 说明 |
| ------ | ----------- | --------- | ------ |
| V1 | uid-1 | data1 | 原始实例 |
| V2 | uid-2 | data2 | 第一次迁移（data1→data2）创建 |
| V3 | uid-3 | data1 | 第二次迁移（data2→data1）创建 |

关键点：V1 和 V3 位于**同一块磁盘**（data1），共享同一个 RocksDB 实例，且 `tablet_id` 相同（均为 8199056）。区分它们的唯一标识是 `tablet_uid`。

```
    时间          第一次迁移              GC (start_trash_sweep)           第二次迁移
                 (data1→data2)      ┌─────────────────────────────┐     (data2→data1)
                      │             │ 第一阶段: 逐个处理待回收tablet │
                      │             │ 第二阶段: 批量清理 _shutdown   │
  06:06:23 ───────────┤             │          _tablets 队列       │
                      │             └─────────────────────────────┘
                      │ V2 创建(data2)            │
                      │ V1 标记待回收               │
                      │ _shutdown_tablets           │
                      │  [8199056] → V1            │
                      ▼                            │
                                                   │
  06:12:04 ────────────────────────────────────────┤
                                     第一阶段处理 V1：│
                                       删除 V1 的    │
                                       RocksDB 元数据│
                                                   │
                              ┌─────────────────────────────────────┐
                              │ 不一致窗口：                          │
                              │  data1 RocksDB: V1 meta 已删除       │
                              │  _shutdown_tablets: V1 仍在队列中     │
                              └─────────────────────────────────────┘
                                                   │
  06:12:13 ────────────────────────────────────────┼──────────────────────┤
                                                   │    防护检查：读取 data1 上 V1 的 TabletMetaPB
                                                   │        → NotFound（GC 已删）→ 跳过清理
                                                   │                      │
                                                   │    ① write_batch：写入 V3 完整元数据到 data1
                                                   │        data1 RocksDB: [V3 meta + rowset meta] ✓
                                                   │                      │
                                                   │    ② 注册 V3，回收 V2：
                                                   │        _add_shutdown_tablet_unlocked(V2)
                                                   │        发现队列中已有 V1 → 替换逻辑触发
                                                   │        _remove_tablet_meta(V1)
                                                   │          → clear_meta(tablet_id=8199056)
                                                   │                      │
                                                   │        ╔═══════════════════════════════════╗
                                                   │        ║ ★ Bug: clear_meta 按 tablet_id    ║
                                                   │        ║   清除，不区分 tablet_uid           ║
                                                   │        ║   V3 的 rowset meta 被一并删除！    ║
                                                   │        ╚═══════════════════════════════════╝
                                                   │                      │
                                                   │    ③ save_meta：只写回 V3 的 TabletMetaPB
                                                   │        data1 RocksDB: [TabletMetaPB] ✓
                                                   │                      [rowset meta] ✗ 已丢失
                                                   │                      ▼
                                                   │
  06:12:xx ────────────────────────────────────────┤
                                     第二阶段：       │
                                       批量清理       │
                                       _shutdown     │
                                       _tablets 队列  │
                                       (为时已晚)     │
                                                   ▼

  最终结果：data1 上 V3 的 TabletMetaPB 还在（"目录"），rowset meta 已丢失（"内容"）
            BE 重启后加载失败：tablet init missing rowset
```

上图展示了完整的事件时序，以下补充图中无法体现的代码级原因：

**为什么 GC 会制造不一致窗口？** `TabletManager::start_trash_sweep` 分两阶段执行：第一阶段逐个处理待回收 tablet（移动文件、删除 RocksDB 元数据），第二阶段才**批量**从 `_shutdown_tablets` 队列中移除条目。这意味着在两阶段之间，磁盘上的 meta 已删除，但内存队列中的条目仍在。

**为什么防护检查未能阻止？** `EngineStorageMigrationTask::_storage_migrate` 的防护依赖读取目标磁盘上的 TabletMetaPB 来发现残留条目。但 GC 第一阶段已经删除了该 meta，防护读到 `NotFound` 后认为无需清理，V1 就留在了 `_shutdown_tablets` 中。

> **根因**：`TabletManager::_remove_tablet_meta` 对主键表会调用 `TabletUpdates::clear_meta()`，以 `tablet_id` 为 key 清除该磁盘 RocksDB 中的 rowset meta、del vector、delta column group 等，**不区分 `tablet_uid`**。V1 和 V3 在同一块磁盘且 `tablet_id` 相同，因此 V3 的 rowset meta 被一并清除。而第 3 步 `Tablet::save_meta()` 只写回 TabletMetaPB，无法恢复已删除的 rowset meta。

**代码调用链与 Bug 代码**：

```
// 防护检查失效
EngineStorageMigrationTask::_storage_migrate
  TabletMetaManager::get_tablet_meta(data1, 8199056) → NotFound（GC 已删）
  → 跳过 TabletManager::delete_shutdown_tablet，V1 留在 _shutdown_tablets

// 迁移核心流程
TabletManager::create_tablet_from_meta_snapshot
  ① write_batch
     写入 V3 完整元数据到 data1 RocksDB
  ② TabletManager::_add_tablet_unlocked(V3)
     → TabletManager::_drop_tablet_unlocked(V2, kMoveFilesToTrash)
       → TabletManager::_add_shutdown_tablet_unlocked(8199056, V2)
         → _shutdown_tablets 中已有 V1（GC 尚未从队列移除）
         → TabletManager::_remove_tablet_meta(V1)                    ← Bug
           → TabletUpdates::clear_meta()
             以 tablet_id=8199056 清除 data1 RocksDB 中所有元数据
             ★ V3 的 rowset meta、del vector 等被误删
  ③ Tablet::save_meta()
     只写回 TabletMetaPB，rowset meta 不恢复
```

Bug 代码（`TabletManager::_add_shutdown_tablet_unlocked`）：

```cpp
void TabletManager::_add_shutdown_tablet_unlocked(int64_t tablet_id, DroppedTabletInfo&& drop_info) {
    auto iter = _shutdown_tablets.find(tablet_id);
    if (iter != _shutdown_tablets.end()) {
        if ((iter->second).tablet != nullptr) {
            // Bug: 无条件调用 _remove_tablet_meta，未校验磁盘上的 meta 是否仍属于旧 tablet
            auto st = _remove_tablet_meta((iter->second).tablet);
        }
        // ...
    }
    _shutdown_tablets.emplace(tablet_id, drop_info);
}
```

## 修复建议

在 `TabletManager::_add_shutdown_tablet_unlocked` 中调用 `TabletManager::_remove_tablet_meta` 前，先通过 `TabletMetaManager::get_tablet_meta` 读取磁盘上的 TabletMetaPB，校验 `tablet_uid` 是否仍属于旧 tablet。如果 uid 不匹配（说明磁盘上已是新 tablet 的数据），跳过清除：

```cpp
void TabletManager::_add_shutdown_tablet_unlocked(int64_t tablet_id, DroppedTabletInfo&& drop_info) {
    auto iter = _shutdown_tablets.find(tablet_id);
    if (iter != _shutdown_tablets.end()) {
        if ((iter->second).tablet != nullptr) {
            TabletMeta tablet_meta;
            auto old_tablet = (iter->second).tablet;
            auto st = TabletMetaManager::get_tablet_meta(
                old_tablet->data_dir(), old_tablet->tablet_id(),
                old_tablet->schema_hash(), &tablet_meta);
            if (st.ok() && tablet_meta.tablet_uid() == old_tablet->tablet_uid()) {
                (void)_remove_tablet_meta(old_tablet);
            }
            // uid 不匹配或 meta 已不存在 → 跳过清除
        }
        // ... 其余逻辑不变
    }
}
```

## 附录

### 事件时间线

| 时间 | 事件 | 影响 |
|------|------|------|
| 06:06:23 | 第一次迁移 data1→data2：V2 创建，V1 标记为待回收 | V1 加入 `_shutdown_tablets` |
| 06:12:04 | GC 处理 V1：移动文件、删除 data1 RocksDB 中 V1 的元数据 | data1 RocksDB 中 V1 meta 已清除，但 `_shutdown_tablets` 中 V1 仍在 |
| 06:12:13 | 第二次迁移 data2→data1：V3 创建，防护检查因 V1 meta 已不存在而跳过 | V3 的 rowset meta 被 `clear_meta()` 误删 |
| 11:03:30 | BE 重启加载 data1 | tablet 8199056 加载失败：`tablet init missing rowset` |

### 原始日志

```
// 第一次迁移 (data1→data2)
I20260126 06:06:23.103506 engine_storage_migration_task.cpp:95] begin to process storage migrate. tablet_id=8199056, schema_hash=1806055599, tablet=8199056.1806055599.bc47711b679f5b1c-7c440e20cbd37781, dest_store=/data2/storage
I20260126 06:06:23.116407 tablet_manager.cpp:1739] create tablet from snapshot tablet:8199056 version:129 path:/data2/storage/data/322/8199056/1806055599
I20260126 06:06:23.441274 tablet_manager.cpp:1553] Start to drop tablet 8199056
I20260126 06:06:23.441440 tablet_manager.cpp:1604] Succeed to drop tablet 8199056
I20260126 06:06:23.441451 tablet_manager.cpp:159] Added duplicated tablet. tablet_id=8199056 old_tablet_path=/data1/storage/data/961/8199056/1806055599 new_tablet_path=/data2/storage/data/322/8199056/1806055599
I20260126 06:06:23.514267 agent_task.cpp:365] local tablet migration succeeded. status: OK, signature: 8199056

// GC sweep 处理 V1
I20260126 06:12:04.118543 tablet_manager.cpp:1103] Moved /data1/storage/data/961/8199056

// 第二次迁移 (data2→data1)
I20260126 06:12:13.430226 engine_storage_migration_task.cpp:95] begin to process storage migrate. tablet_id=8199056, schema_hash=1806055599, tablet=8199056.1806055599.d944499b95cbdc76-e056af7df5e87e9e, dest_store=/data1/storage
I20260126 06:12:13.441421 tablet_manager.cpp:1739] create tablet from snapshot tablet:8199056 version:129 path:/data1/storage/data/966/8199056/1806055599
I20260126 06:12:13.475021 tablet_manager.cpp:1553] Start to drop tablet 8199056
I20260126 06:12:13.475254 tablet_manager.cpp:1604] Succeed to drop tablet 8199056
I20260126 06:12:13.475288 tablet_manager.cpp:159] Added duplicated tablet. tablet_id=8199056 old_tablet_path=/data2/storage/data/322/8199056/1806055599 new_tablet_path=/data1/storage/data/966/8199056/1806055599
I20260126 06:12:13.571911 agent_task.cpp:365] local tablet migration succeeded. status: OK, signature: 8199056

// BE 重启后加载失败
E20260126 11:03:30.983487 tablet_updates.cpp:302] Corruption: tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129] #pending:0 all: active:0,1 missing:0,1: no version to purge when _purge_versions_to_fix_rowset_missing_inconsistency
W20260126 11:03:30.983560 tablet.cpp:119] Fail to init updates: Corruption: tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129] #pending:0 all: active:0,1 missing:0,1: no version to purge when _purge_versions_to_fix_rowset_missing_inconsistency
W20260126 11:03:30.983567 tablet_manager.cpp:936] Fail to init tablet 8199056.1806055599.9849bd692eed44b3-8586748e04e98ebf: tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129] #pending:0 all: active:0,1 missing:0,1: no version to purge when _purge_versions_to_fix_rowset_missing_inconsistency
W20260126 11:03:30.983582 data_dir.cpp:291] load tablet from header failed. status:Internal error: tablet init failed: Corruption: tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129] #pending:0 all: active:0,1 missing:0,1: no version to purge when _purge_versions_to_fix_rowset_missing_inconsistency, tablet=8199056.1806055599
I20260126 11:03:34.314133 tablet_manager.cpp:926] Loaded shutdown tablet 8199056
```
