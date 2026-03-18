# RCA: 主键表 Tablet 迁移导致 Rowset Meta 丢失，BE 重启后报 Missing Rowset

## 摘要

| 项目 | 说明 |
|------|------|
| 现象 | BE 重启后 tablet 加载失败，报错 `tablet init missing rowset` |
| 触发条件 | 同一 tablet 短时间内来回迁移（磁盘 A→B→A） |
| 根因 | 第二次迁移过程中，`TabletManager::_add_shutdown_tablet_unlocked` 清理旧 tablet 元数据时，`TabletUpdates::clear_meta()` 按 `tablet_id` 无差别清除了刚写入的新 tablet 的 rowset meta |
| 影响 | 受影响 tablet 在 BE 重启前正常（依赖内存），**重启后不可用且无法自动恢复** |

## 背景

存算一体模式下，主键表 tablet 8199056 在 6 分钟内经历了两次磁盘迁移。主键表迁移通过快照方式（`create_tablet_from_meta_snapshot`）实现，每次迁移会在目标磁盘上创建一个新实例（分配新的 `tablet_uid`），同时将旧实例标记为待回收。三个实例如下：

| 实例 | 所在磁盘 | 说明 |
|------|---------|------|
| V1 | data1 | 原始实例 |
| V2 | data2 | 第一次迁移（data1→data2）创建 |
| V3 | data1 | 第二次迁移（data2→data1）创建 |

关键点：V1 和 V3 位于**同一块磁盘**（data1），共享同一个 RocksDB 元数据存储，且 `tablet_id` 相同。

## 根因分析

### 迁移的正常流程

tablet 磁盘迁移通过 `TabletManager::create_tablet_from_meta_snapshot` 实现，核心流程分三步：

tablet 的元数据由多部分组成：tablet 基本信息（TabletMetaPB，包含 tablet_id、tablet_uid、版本号、引用的 rowset 列表等）、每个 rowset 的详细信息（rowset meta）、del vector、delta column group 等。

1. **写入新 tablet 完整元数据**：通过 `write_batch` 将上述**所有元数据打包为一个原子批次**写入目标磁盘的 RocksDB
2. **注册新 tablet**：通过 `TabletManager::_add_tablet_unlocked` 将新 tablet 注册为活跃 tablet，同时将旧 tablet 标记为待回收（加入 `TabletManager::_shutdown_tablets` 队列）
3. **写入TabletMetaPB**：`TabletManager::_add_tablet_unlocked` 注册完成后，调用 `Tablet::save_meta` 将内存中的 TabletMetaPB 持久化到 RocksDB。正常情况下这是对第 1 步写入的一次无害覆盖；**但该操作仅写 TabletMetaPB，不包含 rowset meta 等其它元数据**

### 问题如何发生

第一次迁移后：V2 为活跃 tablet，V1 被标记为待回收（`TabletManager::_shutdown_tablets[8199056]` → V1）。

第二次迁移在 data1 上创建 V3，执行上述三步。**问题出在第 2 步**：

注册 V3 时，需要先将旧的活跃 tablet V2 标记为待回收。在将 V2 加入待回收队列时（`TabletManager::_add_shutdown_tablet_unlocked`），发现 `TabletManager::_shutdown_tablets` 中已有 `tablet_id=8199056` 的旧条目（V1），于是调用 `TabletManager::_remove_tablet_meta(V1)` 试图清理 V1 的残留元数据。

**但 `TabletManager::_remove_tablet_meta` 内部调用的 `TabletUpdates::clear_meta()` 是按 `tablet_id` 清除整块磁盘上该 tablet 的所有元数据，不区分 `tablet_uid`。** 由于 V1 和 V3 在同一块磁盘（data1）且 `tablet_id` 相同，第 1 步刚写入的 V3 的 rowset meta、del vector 等被一并清除。

第 3 步 `save_meta()` 只写回 TabletMetaPB，不恢复 rowset meta。最终 data1 RocksDB 中只有 TabletMetaPB（记录着 version 129 引用 rowset {0,1}），但 rowset meta 已丢失。

### 代码调用链

```
TabletManager::create_tablet_from_meta_snapshot
  ① write_batch
     写入 V3 完整元数据到 data1 RocksDB
  ② TabletManager::_add_tablet_unlocked(V3)
     → TabletManager::_drop_tablet_unlocked(V2, kMoveFilesToTrash)
       → TabletManager::_add_shutdown_tablet_unlocked(8199056, V2)
         → TabletManager::_shutdown_tablets 中已有 V1 的条目
         → TabletManager::_remove_tablet_meta(V1)                  ← Bug
           → TabletUpdates::clear_meta()
             以 tablet_id=8199056 清除 data1 RocksDB 全部元数据
             ★ V3 的 rowset meta、del vector 等被误删
  ③ Tablet::save_meta()
     只写回 TabletMetaPB，rowset meta 不恢复
```

### Bug 代码

```cpp
// tablet_manager.cpp:1882-1897
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

`TabletManager::_remove_tablet_meta` 对主键表调用 `TabletUpdates::clear_meta()`（`tablet_updates.cpp:5132-5173`），该函数以 `_tablet.tablet_id()` 为 key 执行 `clear_rowset`、`clear_del_vector`、`clear_delta_column_group`、`clear_log`、`clear_persistent_index`、`remove_tablet_meta`，清除目标磁盘 RocksDB 中该 `tablet_id` 的所有元数据。

## BE 重启加载失败

BE 重启加载 data1 RocksDB 时，找到 V3 的 TabletMetaPB（version 129，引用 rowset {0, 1}），但遍历 RocksDB 未找到对应的 rowset meta，报错：

```
tablet init missing rowset, tablet:8199056 #version:1 [129 129@0 129]
  #pending:0 all: active:0,1 missing:0,1
```

自动修复（`_purge_versions_to_fix_rowset_missing_inconsistency`）因只有一个 version 无法 purge，修复失败。tablet 不可用。

## 修复建议

在 `TabletManager::_add_shutdown_tablet_unlocked` 中调用 `TabletManager::_remove_tablet_meta` 前，先读取磁盘上的 TabletMetaPB，校验 `tablet_uid` 是否仍属于旧 tablet。如果 uid 不匹配（说明磁盘上已是新 tablet 的数据），跳过清除：

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

## 附录：原始日志

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