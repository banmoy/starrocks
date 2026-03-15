# Impact Analysis: Removing CN Interaction During Tablet Creation

## Overview

This document exhaustively lists all features and code paths that would be affected if we remove
the CN (Compute Node) interaction during tablet creation in shared-data mode. The current flow is:

```
FE → CreateReplicaTask (Thrift RPC) → CN → build TabletMetadataPB → write to object storage
                                          → create schema file
                                          → cache metadata/schema locally
```

Removing this interaction affects 5 categories:
1. **FE code that sends CreateReplicaTask** — the senders
2. **BE code that executes the create task** — the executors
3. **BE code that reads the initial tablet metadata** — the downstream consumers
4. **FE code that depends on task completion** — the completion handlers
5. **Configuration, monitoring, and operational paths** — the supporting infrastructure

---

## Category 1: FE Code That Sends CreateReplicaTask

### 1.1 Via `TabletTaskExecutor` (shared path for most features)

**File:** `fe/fe-core/src/main/java/com/starrocks/task/TabletTaskExecutor.java`

`buildCreateReplicaTasks()` (line 229) constructs `CreateReplicaTask` objects. It is called by:

- `buildPartitionsSequentially()` (line 88) → `sendCreateReplicaTasksAndWaitForFinished()` (line 114)
- `buildPartitionsConcurrently()` (line 126) → `sendCreateReplicaTasks()` (line 161)

Both are called from `LocalMetastore.buildPartitions()` (line 1983).

**All callers of `buildPartitions()`:**

| Caller | File:Line | User Feature |
|--------|-----------|-------------|
| `OlapTableFactory.createTable()` (unpartitioned) | `OlapTableFactory.java:820` | CREATE TABLE (non-partitioned) |
| `OlapTableFactory.createTable()` (partitioned) | `OlapTableFactory.java:861` | CREATE TABLE (partitioned) |
| `LocalMetastore.addPartitions()` | `LocalMetastore.java:1377` | ADD PARTITION, auto-partition, dynamic partition, MV PCT refresh, INSERT OVERWRITE (new partitions), MERGE PARTITION |
| `LocalMetastore.addSubPartitions()` | `LocalMetastore.java:1749` | Add physical sub-partitions (random distribution) |
| `LocalMetastore.buildNonPartitionOlapTable()` | `LocalMetastore.java:3345` | Non-partitioned MV initialization |
| `LocalMetastore.truncateTable()` flow | `LocalMetastore.java:4856` | TRUNCATE TABLE |
| `LocalMetastore.createTempPartitionsFromPartitions()` | `LocalMetastore.java:5447` | INSERT OVERWRITE (temp partitions), ALTER TABLE OPTIMIZE, MERGE PARTITION |

### 1.2 `LakeTableSchemaChangeJob` (own task sending logic)

**File:** `fe/fe-core/src/main/java/com/starrocks/alter/LakeTableSchemaChangeJob.java`

- `CreateReplicaTask` constructed at line 465–487
- Sent via `sendAgentTaskAndWait()` at line 502
- **Feature:** ALTER TABLE schema change (add/drop/modify column) on lake tables

### 1.3 `LakeRollupJob` (own task sending logic)

**File:** `fe/fe-core/src/main/java/com/starrocks/alter/LakeRollupJob.java`

- `CreateReplicaTask` constructed at line 261–281
- Sent via `sendAgentTaskAndWait()` at line 294
- **Feature:** CREATE MATERIALIZED VIEW / ADD ROLLUP on lake tables

### 1.4 Paths that do NOT send CreateReplicaTask for lake tablets (no impact)

| Component | Reason |
|-----------|--------|
| `LakeRestoreJob.sendCreateReplicaTasks()` | Overridden as no-op; lake restore uses `restoreSnapshots` RPC |
| `SchemaChangeJobV2` / `RollupJobV2` | Uses `TABLET_TYPE_DISK`; shared-nothing only |
| `ReportHandler` | Uses `TABLET_TYPE_DISK`; replica recovery for LocalTablet only |
| `TabletScheduler` | Uses `TABLET_TYPE_DISK`; tablet repair for LocalTablet only |

---

## Category 2: BE Code That Executes the Create Task

### 2.1 Agent Task Dispatch

**File:** `be/src/agent/agent_server.cpp`
- Line 493–495: `HANDLE_TASK(TTaskType::CREATE, ..., run_create_tablet_task, ...)`

**File:** `be/src/agent/agent_task.cpp`
- Line 220: `run_create_tablet_task()` — entry point
- Line 248–251: Dispatches to `lake_tablet_manager()->create_tablet()` when `tablet_type == TABLET_TYPE_LAKE`

### 2.2 Lake Tablet Creation

**File:** `be/src/storage/lake/tablet_manager.cpp`
- Line 209–275: `TabletManager::create_tablet(const TCreateTabletReq& req)` — the core logic

This method does 4 things:
1. **Build `TabletMetadataPB`** (line 211–265): Sets id, version=1, schema, range, persistent index config, compaction strategy, compression, gtid, flat_json_config
2. **Create schema file** (line 266–268): Calls `create_schema_file()` if `req.create_schema_file` is true
3. **Write metadata to object storage** (line 270–274): Calls `put_tablet_metadata()` which serializes protobuf and writes to `{tablet_root}/meta/{tablet_id}_{version}.meta`
4. **Cache metadata locally** (line 315–321 in `put_tablet_metadata()`): Caches by location key AND latest-metadata key

### 2.3 Schema File Creation

**File:** `be/src/storage/lake/tablet_manager.cpp`
- Line 1200–1214: `create_schema_file()` — writes `TabletSchemaPB` to `{tablet_root}/SCHEMA_{schema_id}`
- Also caches the schema in `GlobalTabletSchemaMap` and `_metacache`

### 2.4 Task Completion Report

**File:** `be/src/agent/agent_task.cpp`
- Line 265–270: For lake tablets, reports only `tablet_id` back (no path hash, no report version increment)
- Line 94–96: `unify_finish_agent_task()` sends `TFinishTaskRequest` back to FE

---

## Category 3: BE Code That Reads Initial Tablet Metadata (Downstream Consumers)

This is the **most critical category** — these paths expect the initial metadata file (version 1) to exist on object storage.

### 3.1 First Data Write → Publish Version

**File:** `be/src/storage/lake/transactions.cpp`
- Line 253: `get_tablet_metadata(tablet_id, base_version)` — `base_version` is 1 for first publish
- **Impact:** First INSERT/LOAD into a newly created table will fail if version 1 metadata doesn't exist
- **Trigger:** Any data write to a new tablet

### 3.2 Schema Change Execution (ALTER on BE)

**File:** `be/src/storage/lake/schema_change.cpp`
- Line 373: `_tablet_manager->get_tablet(request.new_tablet_id, 1)` — explicitly reads version 1
- **Impact:** Schema change fails for new tablets if initial metadata missing
- **Trigger:** ALTER TABLE after shadow tablets are created

### 3.3 Query Execution (SELECT)

**File:** `be/src/connector/lake_connector.cpp`
- Line 192: `tablet_manager->get_tablet(tablet_id, version)` — version from scan range
- **Impact:** For empty tables, version is 1; query fails if metadata missing
- **Trigger:** SELECT on newly created empty table

### 3.4 Tablet Resharding (Split/Merge)

**File:** `be/src/storage/lake/tablet_reshard.cpp`
- Lines 879, 893 (split), 1024, 1036 (merge), 1191, 1204 (identical)
- `base_version` can be 1 for first reshard
- **Impact:** Resharding fails if initial metadata missing
- **Trigger:** Tablet split/merge publish

### 3.5 Compaction

**File:** `be/src/storage/lake/tablet_parallel_compaction_manager.cpp`
- Lines 331, 648, 1384, 2005: `get_tablet(tablet_id, version)`
- Compaction version is usually > 1 but reads metadata chain starting from version 1
- **Impact:** Unlikely for version 1 directly, but metadata chain integrity matters
- **Trigger:** Background compaction

### 3.6 Replication (Cross-Cluster)

**File:** `be/src/storage/lake/lake_replication_txn_manager.cpp`
- Line 335: `get_tablet_metadata(tablet_id, data_version)` — target tablet metadata
- **Impact:** Replication to newly created tablet fails if version 1 missing
- **Trigger:** Cross-cluster replication

### 3.7 Primary Key Index Loading

**File:** `be/src/storage/lake/lake_primary_index.cpp`
- `load_from_lake_tablet()` reads metadata via `get_tablet_metadata(tablet_id, base_version)`
- **Impact:** Primary key index load fails if initial metadata missing
- **Trigger:** First query on primary key table

### 3.8 Version 1 Metadata Fallback Logic

**File:** `be/src/storage/lake/tablet_manager.cpp`
- Lines 539–548: When `{tablet_id}_1.meta` is not found and version == `kInitialVersion`, falls back to `tablet_initial_metadata_filename()` (shared initial metadata from `tablet_creation_optimization`)
- Lines 641–642: `get_single_tablet_metadata()` returns `NotFound` for version 1 (no fallback to bundle)
- Lines 779–781: `list_tablet_metadata()` inserts `tablet_initial_metadata_filename()` when no files found
- **Impact:** This fallback chain is the mechanism that supports `tablet_creation_optimization`

### 3.9 Schema File Reading

**File:** `be/src/storage/lake/tablet_manager.cpp`
- Lines 1004, 1060: `get_tablet_schema_by_id()` reads schema file
- Lines 1007–1048: `get_tablet_schema()` falls back to tablet metadata if schema file missing
- **File:** `be/src/storage/lake/table_schema_service.cpp`
- Lines 378–403: `_fallback_load_to_schema_file()` — tries schema file, falls back to metadata
- **Impact:** Without schema file, schema resolution goes through metadata (more I/O); without metadata either, schema resolution fails entirely
- **Trigger:** Every query, load, compaction, schema change

### 3.10 Vacuum / GC

**File:** `be/src/storage/lake/vacuum.cpp`
- Line 529: Handles deletion of version 1 metadata file
- Lines 273, 359: `collect_alive_shared_files` and `collect_files_to_vacuum` iterate metadata files
- **Impact:** If no version 1 file exists, vacuum behavior changes (may skip or error)
- **Trigger:** Background GC

### 3.11 Lake Snapshot Loader (Restore)

**File:** `be/src/runtime/lake_snapshot_loader.cpp`
- Lines 141, 192: Reads snapshot metadata files
- Line 309: Writes restored metadata via `put_tablet_metadata()`
- **Impact:** Restore writes metadata directly, but destination tablet's initial metadata must be consistent
- **Trigger:** RESTORE operation

### 3.12 Metadata Listing / Admin Operations

**File:** `be/src/service/service_be/lake_service.cpp`
- Line 1145: `get_tablet_metadata` RPC
- Line 1695: Tablet metadata operations
- Line 1917: `repair_tablet_metadata` — writes metadata via `put_tablet_metadata()`
- **Impact:** Admin commands that inspect tablet state
- **Trigger:** ADMIN SHOW TABLET, ADMIN REPAIR

### 3.13 Meta Reader (Information Schema)

**File:** `be/src/storage/lake/lake_meta_reader.cpp`
- Line 44: `get_tablet_metadata(tablet_id, version)` for metadata scan
- **Impact:** `information_schema` queries on tablet metadata
- **Trigger:** SELECT from information_schema tables

---

## Category 4: FE Code That Depends on Task Completion

### 4.1 `MarkedCountDownLatch` — Synchronous Waiting

All tablet creation paths wait for CN completion synchronously:

| Location | Latch Creation | Wait Method |
|----------|---------------|-------------|
| `TabletTaskExecutor.buildPartitionsConcurrently()` | Line 140 | `waitForFinished()` line 182 |
| `TabletTaskExecutor.buildPartitionsSequentially()` | Line 312 (inside `sendCreateReplicaTasksAndWaitForFinished`) | Line 314 |
| `LakeTableSchemaChangeJob.sendAgentTaskAndWait()` | Line 328 | Line 353 |
| `LakeRollupJob.sendAgentTaskAndWait()` | Line 614 | Line 627 |

### 4.2 AgentTaskQueue Management

Tasks are tracked in `AgentTaskQueue` and cleaned up in multiple places:

| Location | Action |
|----------|--------|
| `TabletTaskExecutor.sendCreateReplicaTasks()` line 332 | `AgentTaskQueue.addTaskList()` |
| `TabletTaskExecutor.buildPartitionsSequentially()` finally block line 119 | `AgentTaskQueue.removeTask()` |
| `TabletTaskExecutor.buildPartitionsConcurrently()` finally block line 193 | `AgentTaskQueue.removeTask()` |
| `LakeTableSchemaChangeJob` line 338 | `AgentTaskQueue.removeBatchTask()` on failure |
| `LakeRollupJob` line 623 | `AgentTaskQueue.removeBatchTask()` on failure |
| `LeaderImpl.finishCreateReplica()` line 430 | `AgentTaskQueue.removeTask()` in finally |

### 4.3 Task Completion Callback (`LeaderImpl.finishCreateReplica`)

**File:** `fe/fe-core/src/main/java/com/starrocks/service/LeaderImpl.java`
- Lines 383–431: `finishCreateReplica()`
- Decrements the latch on success
- Updates backend report version (`updateBackendReportVersion()`)
- For recovery scenarios, delegates to `TabletScheduler.finishCreateReplicaTask()`
- **Impact:** This entire callback chain becomes unnecessary

### 4.4 Post-Completion State Transitions

After tablet creation succeeds, different features proceed differently:

| Feature | Post-Completion Action |
|---------|----------------------|
| CREATE TABLE | `LocalMetastore.onCreate()` → `logCreateTable()` → register table in catalog |
| ADD PARTITION | Lock table → add partition to table → `logAddPartition()` |
| TRUNCATE TABLE | `truncateTableInternal()` → replace old partitions → `logTruncateTable()` |
| Schema Change | `addShadowIndexToCatalog()` → set watershedTxnId → `persistStateChange(WAITING_TXN)` |
| Rollup | `addRollupIndexToCatalog()` → set watershedTxnId → `persistStateChange(WAITING_TXN)` |
| INSERT OVERWRITE | Continue to write data to temp partitions |
| MV refresh | Continue with data refresh |

### 4.5 Rollback on Failure

| Feature | Rollback Action |
|---------|----------------|
| CREATE TABLE | `OlapTableFactory` catches exception → `deleteUselessTablets()` (removes from inverted index) |
| ADD PARTITION | `deleteUselessTablets()` |
| TRUNCATE TABLE | `LocalMetastore.java:4859` — `deleteUselessTablets(tabletIdSet)` |
| Schema Change | `LakeTableSchemaChangeJob` → `AlterCancelException` → job cancelled → `cancel()` clears tasks |
| Rollup | Same as Schema Change |

### 4.6 ConsistencyChecker Interaction

**File:** `fe/fe-core/src/main/java/com/starrocks/server/LocalMetastore.java`
- Line 2022: `addCreatingTableId(table.getId())` — before `buildPartitions`
- Line 2035: `deleteCreatingTableId(table.getId())` — in `finally`

**File:** `fe/fe-core/src/main/java/com/starrocks/consistency/ConsistencyChecker.java`
- Lines 163, 213: Skips consistency check for tablets whose table is being created
- **Impact:** If tablet creation becomes instant (no CN wait), the window for consistency check interference shrinks to zero

---

## Category 5: Configuration, Monitoring, and Operational Paths

### 5.1 Configuration Parameters Affected

| Parameter | File | Current Use | Impact |
|-----------|------|-------------|--------|
| `tablet_create_timeout_second` | `Config.java:1041` | Timeout per replica creation | Becomes irrelevant for lake |
| `max_create_table_timeout_second` | `Config.java:1603` | Max total creation timeout | Becomes irrelevant for lake |
| `create_table_max_serial_replicas` | `Config.java:1607` | Threshold for sequential vs concurrent | Becomes irrelevant for lake |
| `lake_enable_tablet_creation_optimization` | `Config.java:1062` | Skip redundant metadata writes | The entire concept changes |

### 5.2 Metrics Affected

| Metric | File | Impact |
|--------|------|--------|
| `create_tablet_requests_total` | `starrocks_metrics.h:82` | No longer incremented for lake (only in `TabletManager::create_tablet` for shared-nothing) |
| `create_tablet_requests_failed` | `starrocks_metrics.h:83` | Same |
| `g_put_tablet_metadata_latency` | `tablet_manager.cpp:324` | Not triggered during creation |

### 5.3 Thrift Definitions

| File | Structure | Impact |
|------|-----------|--------|
| `gensrc/thrift/AgentService.thrift` | `TCreateTabletReq` | Still needed for shared-nothing; lake-specific fields may become unused |
| `gensrc/thrift/AgentService.thrift:132` | `create_schema_file` field | No longer sent for lake |
| `gensrc/thrift/AgentService.thrift:133` | `enable_tablet_creation_optimization` field | No longer sent |

### 5.4 `LakeTableAsyncFastSchemaChangeJob`

**File:** `fe/fe-core/src/main/java/com/starrocks/alter/LakeTableAsyncFastSchemaChangeJob.java`
- Line 124–126: Uses `TabletMetadataUpdateAgentTaskFactory.createTabletSchemaUpdateTask()` with `createSchemaFile` flag
- **This is NOT a CreateReplicaTask path** — it updates existing tablet metadata, not creating new tablets
- **Impact:** NOT directly affected, but the schema file creation pattern via CN would need review

### 5.5 Admin Commands

| Command | Impact |
|---------|--------|
| `ADMIN SHOW TABLET` | May need to handle missing initial metadata |
| `ADMIN REPAIR TABLE` | Repair logic may need adjustment for metadata that was never written |
| `SHOW TABLET` | FE-side only, no direct impact |
| `ADMIN CHECK TABLET` | May report inconsistency for tablets without metadata file |

---

## Summary: Complete Impact Matrix

### FE Senders (3 code paths)

| Path | File | Lines |
|------|------|-------|
| `TabletTaskExecutor` (shared) | `TabletTaskExecutor.java` | 88–307 |
| `LakeTableSchemaChangeJob` | `LakeTableSchemaChangeJob.java` | 399–502 |
| `LakeRollupJob` | `LakeRollupJob.java` | 196–294 |

### BE Executors (1 main path)

| Path | File | Lines |
|------|------|-------|
| `run_create_tablet_task` → `lake::TabletManager::create_tablet` | `agent_task.cpp:220`, `tablet_manager.cpp:209` | |

### BE Downstream Consumers of Initial Metadata (13 paths)

| # | Consumer | File | Version Read | Criticality |
|---|----------|------|-------------|-------------|
| 1 | Publish version (first write) | `transactions.cpp:253` | base=1 | **Critical** — blocks all data writes |
| 2 | Schema change execution | `schema_change.cpp:373` | 1 (explicit) | **Critical** — blocks ALTER |
| 3 | Query execution | `lake_connector.cpp:192` | visible version | **Critical** — blocks SELECT on empty table |
| 4 | Tablet resharding | `tablet_reshard.cpp:879,1024,1191` | base=1 possible | **High** — blocks split/merge |
| 5 | Replication | `lake_replication_txn_manager.cpp:335` | target version | **High** — blocks cross-cluster replication |
| 6 | Primary key index load | `lake_primary_index.cpp` | base version | **High** — blocks PK table operations |
| 7 | Schema resolution | `tablet_manager.cpp:1004,1007` | via metadata | **High** — all operations need schema |
| 8 | Compaction | `tablet_parallel_compaction_manager.cpp` | task version | **Medium** — version > 1 typically |
| 9 | Vacuum / GC | `vacuum.cpp:529` | version 1 | **Medium** — cleanup behavior changes |
| 10 | Meta reader | `lake_meta_reader.cpp:44` | query version | **Low** — admin use |
| 11 | Snapshot restore | `lake_snapshot_loader.cpp` | snapshot version | **Low** — restore has own write path |
| 12 | Metadata listing | `tablet_manager.cpp:779` | fallback to initial | **Low** — admin use |
| 13 | Admin repair | `lake_service.cpp:1917` | repair version | **Low** — admin use |

### FE Completion Dependencies (6 categories)

| # | Category | Key Files | Impact |
|---|----------|-----------|--------|
| 1 | MarkedCountDownLatch wait | `TabletTaskExecutor`, `LakeTableSchemaChangeJob`, `LakeRollupJob` | Must be replaced or removed |
| 2 | AgentTaskQueue management | `AgentTaskQueue`, `TabletTaskExecutor`, `LeaderImpl` | Queue management no longer needed for lake CREATE |
| 3 | Task completion callback | `LeaderImpl.finishCreateReplica()` | Lake path in callback becomes dead code |
| 4 | Post-completion state transitions | `LocalMetastore`, `LakeTableSchemaChangeJob`, `LakeRollupJob` | State transitions proceed immediately |
| 5 | Rollback paths | `LocalMetastore.deleteUselessTablets()` | Rollback logic simplifies |
| 6 | ConsistencyChecker | `LocalMetastore`, `ConsistencyChecker` | Creating-table window shrinks |

### Configuration/Monitoring (4 configs, 2 metrics)

| Type | Name | Impact |
|------|------|--------|
| Config | `tablet_create_timeout_second` | Irrelevant for lake |
| Config | `max_create_table_timeout_second` | Irrelevant for lake |
| Config | `create_table_max_serial_replicas` | Irrelevant for lake |
| Config | `lake_enable_tablet_creation_optimization` | Concept changes entirely |
| Metric | `create_tablet_requests_total` | Not incremented for lake |
| Metric | `create_tablet_requests_failed` | Not incremented for lake |
