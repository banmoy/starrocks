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

---

## Category 6: Upgrade/Downgrade Compatibility

Removing CN interaction during tablet creation introduces a **behavioral change in the contract between
FE, CN, and object storage**. The key invariant that changes is:

> **Old invariant:** After DDL completes, the initial tablet metadata file (version 1) exists on object storage.
>
> **New invariant:** After DDL completes, the initial tablet metadata file may NOT exist on object storage;
> it may be created lazily or through a different mechanism.

This affects every rolling upgrade, mixed-version, and downgrade scenario.

### 6.1 Rolling Upgrade Scenarios

**升级规范：先升 CN，再升 FE。**

#### Scenario A: Phase 1 — CN upgraded, FE not yet upgraded

| Phase | State | Risk |
|-------|-------|------|
| Old FE creates table | Old FE sends `CreateReplicaTask` to new CN | New CN must still support `run_create_tablet_task` → `lake::TabletManager::create_tablet()` |
| New CN handles CREATE task | CN writes metadata normally | **OK** — as long as new CN preserves backward compatibility in the handler |

**Conclusion:** Safe, as long as new CN does **not remove** the `create_tablet` handler.
New CN must keep handling `TTaskType::CREATE` for `TABLET_TYPE_LAKE` during this window.

#### Scenario B: Phase 2 — CN upgraded AND FE upgraded (steady state)

| Phase | State | Risk |
|-------|-------|------|
| New FE creates table | New FE skips CN interaction, does NOT write initial metadata via CN | — |
| New CN receives data write (INSERT) | New CN tries `publish_version` → reads `base_version=1` metadata | Must handle missing version 1 metadata (e.g., lazy init or FE writes metadata directly) |
| New CN receives query (SELECT on empty table) | New CN tries `get_tablet_metadata(tablet_id, 1)` | Must handle missing version 1 metadata |
| New CN receives schema change task | `schema_change.cpp:373` reads `get_tablet(new_tablet_id, 1)` | Must handle missing version 1 metadata |

**Conclusion:** This is the target steady state. New CN must have a mechanism to handle
tablets without initial metadata for ALL downstream operations (publish, query, schema change,
compaction, split/merge, replication, etc.). All 13 downstream consumers in Category 3 need
to be audited for this scenario.

#### Scenario C: Mixed FE cluster during rolling upgrade (Leader vs Follower)

| Phase | State | Risk |
|-------|-------|------|
| New FE Leader, old FE Followers | New Leader creates table without CN interaction, writes edit log | Old Followers replay edit log — `replayCreateTable()`, `replayAddPartition()` only apply FE metadata, do NOT send CN tasks |
| Old FE Leader, new FE Followers | Old Leader creates table via CN interaction normally | New Followers replay edit log normally |

**Conclusion:** Edit log replay is **NOT affected** because FE followers never send `CreateReplicaTask`
during replay — they only apply metadata changes. The edit log format (`CreateTableInfo`,
`AddPartitionsInfoV2`, `TruncateTableInfo`) does not need to change.

**However:** During FE rolling upgrade, leadership may transfer:
- New FE Leader creates a table (no metadata on object storage)
- Leadership transfers to old FE
- Old FE's subsequent operations on the SAME table (e.g., ADD PARTITION) will send
  `CreateReplicaTask` to CN for the NEW partitions — these succeed independently
- But old FE may also trigger operations (dynamic partition, MV refresh) that touch
  partitions created by new FE — these would send operations to CN that read version 1
  metadata of old partitions → **BUT all CNs are already new version at this point**
  (since CN upgraded first), so new CN should handle missing metadata gracefully

**Key insight:** Since CN is always upgraded before FE, by the time any FE (old or new) is
running, all CNs are already the new version. So CN handling of missing metadata is the
critical requirement, not FE-side compatibility.

### 6.2 Downgrade Scenarios

**降级规范：先降 FE，再降 CN。**

#### Scenario D: Phase 1 — FE downgraded, CN still new version

| Phase | State | Risk |
|-------|-------|------|
| Old FE is restored | Old FE sends `CreateReplicaTask` for NEW tables | New CN still handles it → metadata written → **OK** for new tables |
| Old FE operates on tablets created by new FE (no metadata) | Old FE sends normal operations (INSERT, SELECT, ALTER) | New CN receives these operations → new CN knows how to handle missing metadata → **OK** |
| Dynamic partition / MV refresh on tables created by new FE | Old FE's scheduler triggers ADD PARTITION → sends `CreateReplicaTask` to new CN | **OK** — new CN writes metadata for new partitions |

**Conclusion:** Phase 1 is **generally safe** because new CN (still running) has the logic to
handle tablets both with and without initial metadata. Old FE's behavior of sending
`CreateReplicaTask` is also handled normally by new CN.

**Risk:** If old FE triggers operations that depend on the version 1 metadata being readable
**by FE itself** (not CN) — currently FE does not read tablet metadata from object storage
directly, so this is not a concern.

#### Scenario E: Phase 2 — FE downgraded AND CN downgraded

| Phase | State | Risk |
|-------|-------|------|
| Old CN is restored | Old CN does NOT have logic to handle missing metadata | — |
| Operations on tablets created by new FE (no metadata) | Old CN tries to read version 1 metadata → **file not found** | **FAILURE** |
| INSERT to affected tablets | `publish_version` reads `base_version=1` → not found | **FAILURE** |
| SELECT on affected empty tablets | `get_tablet_metadata(tablet_id, 1)` → not found | **FAILURE** |
| Schema change on affected tablets | `get_tablet(new_tablet_id, 1)` → not found | **FAILURE** |
| Operations on tablets created by old FE (metadata exists) | Old CN reads metadata normally | **OK** |
| DROP TABLE on affected tablets | Shard deletion + vacuum | **OK** — vacuum handles missing files |

**Conclusion:** After full downgrade (FE + CN both old), tablets created during the new-version
window (when FE skipped CN interaction) are **broken on old CN**. Only tablets that were created
by old FE (or by new FE before the feature flag was enabled) remain functional.

**Mitigation options:**
1. During Phase 1 of downgrade (old FE, new CN still running), run a **repair tool** that
   retroactively writes version 1 metadata for all tablets missing it
2. Accept that downgrade is a **destructive operation** for tablets created with the new behavior,
   document that users must DROP and re-create affected tables
3. Maintain a **registry** (in FE metadata or StarManager) of which tablets were created without
   initial metadata, so a repair tool can target them specifically

### 6.3 Object Storage Artifact Compatibility

The implicit "contract" for object storage artifacts after tablet creation:

| Artifact | Old Version Expectation | New Version Behavior | Gap |
|----------|------------------------|---------------------|-----|
| `{tablet_id}_{version_1}.meta` (per-tablet) | Exists after creation when `tablet_creation_optimization=false` | May not exist | **Breaking** |
| `0000000000000000_0000000000000001.meta` (shared initial, per-partition) | Exists after creation when `tablet_creation_optimization=true` | May not exist | **Breaking** |
| `SCHEMA_{schema_id}` (schema file) | Exists for first tablet per partition | May not exist | **Degraded** — fallback to metadata works, but adds latency; may fail if metadata also missing |
| `TabletMetadataPB` protobuf format | Version N | Version N+1 (must be backward compatible) | **Must ensure** protobuf backward compatibility |

### 6.4 Cross-Feature Compatibility During Mixed Versions

#### Schema Change (ALTER TABLE) during rolling upgrade

1. New FE creates shadow tablets without CN interaction (no metadata written)
2. New FE sends ALTER task to old CN
3. Old CN tries `get_tablet(new_tablet_id, 1)` → **fails**

**Impact:** Schema change is blocked during mixed-version window for tables with shadow tablets
created by new FE.

#### Tablet Split/Merge during rolling upgrade

1. New FE creates new shard IDs via `createShardsForSplit/Merge`
2. New FE sends `PublishVersionRequest` with `resharding_tablet_infos` to CN
3. CN's `publish_resharding_tablet()` reads new tablet metadata
4. If new tablet metadata was never written, resharding **fails**

**Impact:** Tablet split/merge is blocked for new tablets during mixed-version window.

#### Backup/Restore across versions

**BACKUP 侧（读取 tablet metadata）会受影响。**

BACKUP 流程 (`LakeBackupJob` → `LakeSnapshotLoader::upload()`):
1. FE: `prepareSnapshotTask()` 中取 `visibleVersion` — 新建空表时为 **1**
2. FE: `lockTabletMetadata(tablet_id, version)` 锁定 metadata
3. BE: `LakeSnapshotLoader::upload()`:
   - `tablet->get_metadata(snapshot.version())` — **读取对象存储上的 metadata 文件**
   - 遍历 metadata 中的 rowset 获取 segment 文件列表
   - 将 metadata 文件 + segment 文件都上传到 backup 仓库

```191:199:be/src/runtime/lake_snapshot_loader.cpp
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        auto tablet_metadata = tablet->get_metadata(snapshot.version());  // version=1 for empty table
        for (const auto& rowset : (*tablet_metadata)->rowsets()) {
            for (const std::string& segment : rowset.segments()) {
                file_locations[segment] = tablet->segment_location(segment);
            }
        }
        file_locations[starrocks::lake::tablet_metadata_filename(tablet_id, snapshot.version())] =
                tablet->metadata_location(snapshot.version());
```

**如果 version=1 的 metadata 不存在（新版本跳过了 CN 交互），`get_metadata(1)` 会失败，
BACKUP 整个任务失败。**

**RESTORE 侧（写入 tablet metadata）不受影响。**

RESTORE 流程 (`LakeRestoreJob` → `LakeSnapshotLoader::restore()`):
1. FE: `resetTableForRestore()` → `allocateFilePath()` (不调用 `createShards`)
2. FE: `createTabletsForRestore()` → 创建 `LocalTablet` (FE 生成 ID)
3. FE: `createReplicas()` → no-op for lake (只更新 inverted index)
4. FE: `sendCreateReplicaTasks()` → no-op for lake
5. BE: `LakeSnapshotLoader::restore()` 从 backup 读取 metadata，设置新 tablet ID，
   直接通过 `put_tablet_metadata()` 写入对象存储，再拷贝 segment 文件

Restore 完全绕过 `CreateReplicaTask` 路径，由 `LakeSnapshotLoader` 自己写 metadata。

**场景分析：**

| 场景 | 影响 |
|------|------|
| **老版本 BACKUP → 新版本 RESTORE** | **OK** — backup 中包含 version 1 metadata 文件，restore 直接写入对象存储 |
| **新版本 BACKUP 有数据的表 (version>1) → 老版本 RESTORE** | **OK** — backup 读 version>1 的 metadata（存在），restore 写入正常 |
| **新版本 BACKUP 空表 (version=1，metadata 不存在) → 任何版本 RESTORE** | **FAILURE** — BACKUP 就失败了，无法读取 version 1 metadata |
| **新版本 BACKUP 有数据的表 → 老版本 RESTORE** | **OK** — backup 包含实际的 metadata + segment，老版本 restore 正常写入 |

**结论：BACKUP 空表（刚创建未写入数据）在新版本上会失败，这是一个需要处理的问题。
RESTORE 侧不受影响。**

#### Cross-Cluster Replication across versions

Lake replication (`LakeReplicationJob`) requires the target table to already exist with tablets.
The replication flow reads target tablet metadata during execution:

1. `LakeReplicationJob.run()` → `sendReplicateLakeRemoteStorageTasks()`
2. BE: `replicate_lake_remote_storage()` reads target tablet metadata:
   - `target_tablet.get_metadata(target_visible_version)` — version 1 for first replication
   - `get_tablet_metadata(target_tablet_id, data_version)` — version 1 for first replication
3. Copies data from source to target storage, writes txn log
4. `publish_version` reads `base_version=1` metadata to apply txn logs

**All three reads require version 1 metadata to exist on object storage.**

| Scenario | Impact |
|----------|--------|
| Target tablets created by old FE (with metadata) | **OK** — metadata exists, replication works |
| Target tablets created by new FE (without metadata) + new CN | **Depends** — new CN must handle missing version 1 metadata in `replicate_lake_remote_storage()` and `publish_version()` |
| Target tablets created by new FE (without metadata) + old CN after downgrade | **FAILURE** — old CN cannot handle missing metadata |

**This is an affected path.** Cross-cluster replication is one of the 13 downstream consumers
(Category 3, item #6) that reads initial tablet metadata. The new CN must be updated to handle
this case — either by lazily creating metadata on first replication, or by having FE supply the
necessary schema information in the replication request itself.

### 6.5 StarManager / StarOS Compatibility

| Aspect | Impact |
|--------|--------|
| Shard creation via `StarOSAgent.createShards()` | **Unchanged** — shard IDs are still allocated from StarManager regardless of whether CN interaction happens |
| Shard deletion / GC | **Unchanged** — shard deletion is driven by FE, not by whether metadata file exists |
| Shard info queries | **Unchanged** — StarManager tracks shards independently |
| Worker (CN) shard registration | **May be affected** — if CN's `StarOSWorker::add_shard()` depends on metadata existence for health checks |

### 6.6 Edit Log / Journal Compatibility

| Aspect | Impact |
|--------|--------|
| `logCreateTable(CreateTableInfo)` | **Format unchanged** — `CreateTableInfo` contains table schema, partition info, etc. No reference to whether CN was involved |
| `logAddPartition(PartitionPersistInfoV2)` | **Format unchanged** — contains partition metadata |
| `logTruncateTable(TruncateTableInfo)` | **Format unchanged** — contains partition replacement info |
| FE image (checkpoint) | **Format unchanged** — contains table/partition/tablet metadata |
| Follower replay | **Not affected** — followers never send `CreateReplicaTask`; they only apply metadata |
| Log replay after restart | **Not affected** — same as follower replay |

**Conclusion:** Edit log format does NOT need to change. The compatibility issue is purely at the
**object storage artifact level**, not at the FE metadata level.

### 6.7 FE Metadata Consistency

| Aspect | Old Behavior | New Behavior | Risk |
|--------|-------------|-------------|------|
| `PhysicalPartition.visibleVersion` | Set to `PARTITION_INIT_VERSION = 1` during creation | Same — FE metadata unchanged | **None** |
| `Tablet` in `MaterializedIndex` | `LakeTablet(shardId)` added to index | Same — FE metadata unchanged | **None** |
| `OlapTable.partitions` | Partition registered after `buildPartitions` succeeds | Partition registered after metadata/shard creation succeeds (no CN wait) | **Timing change** — partition may be visible to queries sooner, before CN has any metadata |

### 6.8 Summary: Compatibility Risk Matrix

Upgrade order: CN first → FE second. Downgrade order: FE first → CN second.

| Scenario | Risk Level | Failure Mode |
|----------|-----------|--------------|
| **Upgrade Phase 1:** CN new, FE old | **Low** | Safe if new CN keeps `create_tablet` handler |
| **Upgrade Phase 2:** CN new, FE new | **None** (target state) | New CN must handle missing metadata for all operations |
| **Mixed FE leader/follower** | **Low** | All CNs already new → new CN handles missing metadata |
| **Downgrade Phase 1:** FE old, CN new | **Low** | New CN handles both old and new tablets |
| **Downgrade Phase 2:** FE old, CN old | **High** | Tablets created by new FE (without metadata) are broken on old CN |
| Backup old → Restore new | **Low** | Restore has own write path |
| Backup new → Restore old | **Medium** | Old restore may not handle missing metadata |
| Replication old → new target (new FE) | **None** | New CN handles missing metadata |
| Replication old → new target then downgrade CN | **High** | Same as downgrade Phase 2 |
| Edit log replay | **None** | Format unchanged, replay doesn't send CN tasks |
| StarManager state | **None** | Shard allocation independent of metadata |

**Key takeaway:** The only truly risky scenario is **full downgrade (FE + CN both rolled back)**
for tablets that were created without initial metadata. All other scenarios are safe because
either (a) all CNs are already the new version and can handle missing metadata, or (b) old FE
still sends `CreateReplicaTask` and new CN handles it.

### 6.9 Required Compatibility Guarantees

Given upgrade order (CN first → FE second) and downgrade order (FE first → CN second):

1. **New CN MUST keep `create_tablet` handler for `TABLET_TYPE_LAKE`** — during upgrade Phase 1 and downgrade Phase 1, old FE still sends `CreateReplicaTask`
2. **New CN MUST handle all operations on tablets without initial metadata** — this is the core requirement; all 13 downstream consumers (Category 3) must gracefully handle missing version 1 metadata
3. **Protobuf `TabletMetadataPB` MUST remain backward compatible** — new fields must be optional with defaults
4. **Downgrade to old CN requires repair or acceptance of data loss** — a repair tool (run during downgrade Phase 1 when new CN is still running) can retroactively write metadata for tablets missing it; alternatively, document that full downgrade breaks tablets created with the new behavior
5. **Feature flag** is NOT strictly required for upgrade safety (since CN is always upgraded first), but is still recommended for operational control and gradual rollout
