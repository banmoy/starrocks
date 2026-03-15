# CN 在 Tablet 创建时做了什么

## 概述

FE 通过 `TabletTaskExecutor` 向 CN 发送 `CreateReplicaTask`（Thrift RPC），CN 收到后执行
`lake::TabletManager::create_tablet()`，做 4 件事。

## 1. 构建 TabletMetadataPB

**文件**：`be/src/storage/lake/tablet_manager.cpp:209-265`

将 FE 传来的 `TCreateTabletReq`（Thrift）转换为 `TabletMetadataPB`（Protobuf）。

转换用到 `convert_t_schema_to_pb_schema()`（`be/src/storage/metadata_util.cpp`），
这个函数**只存在于 BE C++ 代码中**，FE（Java）不直接使用。

设置的字段：

| 字段 | 值 | 来源 |
|------|------|------|
| `id` | tablet_id | `TCreateTabletReq.tablet_id` |
| `version` | 1 (`kInitialVersion`) | 硬编码 |
| `next_rowset_id` | 1 | 硬编码 |
| `cumulative_point` | 0 | 硬编码 |
| `gtid` | FE 生成 | `TCreateTabletReq.gtid` |
| `schema` | 完整的 tablet schema | `convert_t_schema_to_pb_schema(req.tablet_schema, compress_type)` |
| `range` | tablet 的值域范围（仅 Range 分布）| `TCreateTabletReq.range` → `TabletRangeHelper::convert_t_range_to_pb_range()` |
| `enable_persistent_index` | 主键表持久化索引 | `TCreateTabletReq.enable_persistent_index` |
| `persistent_index_type` | LOCAL / CLOUD_NATIVE | `TCreateTabletReq.persistent_index_type` |
| `flat_json_config` | JSON 列扁平化配置 | `TCreateTabletReq.flat_json_config` |
| `compaction_strategy` | DEFAULT / REAL_TIME | `TCreateTabletReq.compaction_strategy` |
| `compression_level` | 压缩级别 | `TCreateTabletReq.compression_level` |

## 2. 创建 Schema File

**文件**：`be/src/storage/lake/tablet_manager.cpp:266-268,1200-1214`

当 `TCreateTabletReq.create_schema_file == true` 时（每个 partition/index 的第一个 tablet），
调用 `create_schema_file()`：

```cpp
Status TabletManager::create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb) {
    auto schema_file_path = _location_provider->schema_file_location(tablet_id, schema_pb.id());
    ProtobufFile file(schema_file_path);
    RETURN_IF_ERROR(file.save(schema_pb));
    // 缓存到 GlobalTabletSchemaMap 和 _metacache
}
```

- **路径**：`{tablet_root}/SCHEMA_{schema_id:016X}`
- **内容**：`TabletSchemaPB` protobuf
- **用途**：独立的 schema 文件可被多个 tablet 共享，避免每次都从 metadata 中读取 schema

**FE 侧控制**（`TabletTaskExecutor.java:238,292,299`）：
- 同一 partition/index 下，只有第一个 tablet 设置 `createSchemaFile=true`
- 后续 tablet 设置 `createSchemaFile=false`

**Schema 读取的 fallback 链**：
1. 内存缓存（`GlobalTabletSchemaMap` / `_metacache`）
2. Schema file（`get_tablet_schema_by_id()` → `load_and_parse_schema_file()`）
3. FE RPC（`TableSchemaService::get_schema_for_load/scan()` → `_get_remote_schema()`）
4. Tablet metadata 中的 schema（`_fallback_load_to_schema_file()` → `get_tablet().get_schema()`）

## 3. 写入 Tablet Metadata 到对象存储

**文件**：`be/src/storage/lake/tablet_manager.cpp:270-274,307-326`

```cpp
// 不走 optimization 时
return put_tablet_metadata(std::move(tablet_metadata_pb));
// 走 optimization 时
return put_tablet_metadata(std::move(tablet_metadata_pb), tablet_initial_metadata_location(req.tablet_id));
```

`put_tablet_metadata()` 的实现：
```cpp
Status TabletManager::put_tablet_metadata(const TabletMetadataPtr& metadata, const std::string& metadata_location) {
    ProtobufFile file(metadata_location);
    RETURN_IF_ERROR(file.save(*metadata));           // 写入对象存储
    _metacache->cache_tablet_metadata(metadata_location, metadata);  // 缓存
    _metacache->cache_tablet_metadata(tablet_latest_metadata_cache_key(metadata->id()), metadata);
    return Status::OK();
}
```

**写入路径**：
- 不走 optimization：`{metadata_root}/{tablet_id:016X}_{version:016X}.meta`（例如 `0000000000012345_0000000000000001.meta`）
- 走 optimization：`{metadata_root}/0000000000000000_0000000000000001.meta`（共享 initial metadata）

**对象存储路径解析**：
- `StarletLocationProvider::root_location(tablet_id)` → `build_starlet_uri(tablet_id, "")`
- 通过 StarOS/Starlet 将 shard ID 映射到实际对象存储路径

## 4. 缓存 Metadata 和 Schema

**Metadata 缓存**（`put_tablet_metadata()` 中）：
- 按路径 key 缓存：`_metacache->cache_tablet_metadata(metadata_location, metadata)`
- 按 latest key 缓存：`_metacache->cache_tablet_metadata(tablet_latest_metadata_cache_key(id), metadata)`

**Schema 缓存**（`create_schema_file()` 中）：
- `GlobalTabletSchemaMap::Instance()->emplace(schema_pb)` — 全局 schema 去重
- `_metacache->cache_tablet_schema(cache_key, schema, cache_size)` — 本地 metacache

**缓存预热的作用**：
- 后续对该 tablet 的第一次操作（查询、写入、compaction）不需要从对象存储回读 metadata
- 如果 CN 重启，缓存丢失，需要从对象存储重新读取

---

## 为什么当前需要 CN 参与

| 原因 | 详细说明 |
|------|---------|
| Schema 序列化在 C++ 侧 | `convert_t_schema_to_pb_schema()` 只有 C++ 实现，FE（Java）没有等效逻辑 |
| 对象存储访问通过 Starlet | CN 通过 `StarletLocationProvider` + `StarOSWorker` 将 shard ID 映射到实际路径 |
| 缓存预热 | CN 写入时顺便缓存，避免后续第一次读的延迟 |
| 统一的任务框架 | 通过 Agent Task 框架管理超时、重试、失败报告 |

---

## Task 构建详情

### TabletTaskExecutor 构建 CreateReplicaTask

**文件**：`TabletTaskExecutor.java:229-307`

关键参数：
- `nodeId`：通过 `warehouseManager.getComputeNodeAssignedToTablet(computeResource, tablet.getId())` 选择 CN
- `tabletType`：`TTabletType.TABLET_TYPE_LAKE`
- `tabletSchema`：从 `MaterializedIndexMeta` 构建 `TTabletSchema`
- `createSchemaFile`：第一个 tablet 为 true，后续为 false
- `enableTabletCreationOptimization`：从 `CreateTabletOption` 传入
- `gtid`：从 `CreateTabletOption` 传入
- `range`：仅 Range 分布时设置

### LakeTableSchemaChangeJob 构建 CreateReplicaTask

**文件**：`LakeTableSchemaChangeJob.java:465-487`

与 `TabletTaskExecutor` 类似，但：
- 独立构建和发送（不走 `TabletTaskExecutor`）
- 使用 `sendAgentTaskAndWait()` 发送
- `createSchemaFile` 也是第一个 tablet 为 true

### LakeRollupJob 构建 CreateReplicaTask

**文件**：`LakeRollupJob.java:261-281`

同上，独立构建和发送。

---

## 任务完成回调

CN 执行完 `create_tablet` 后，通过 `unify_finish_agent_task()` 发送 `TFinishTaskRequest` 到 FE。

**FE 处理**（`LeaderImpl.finishCreateReplica()`）：
- 成功：`createReplicaTask.countDownLatch(backendId, signature)` — 释放等待的 latch
- 失败：`createReplicaTask.countDownToZero(errMsg)` — 标记错误，释放所有 latch

**对 lake tablet 的特殊处理**（`agent_task.cpp:101-106`）：
- Lake tablet 只回报 `tablet_id`
- **不** 回报 path hash、report version（与 shared-nothing 不同）
