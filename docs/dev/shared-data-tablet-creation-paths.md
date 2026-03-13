# Shared-Data (存算分离) Internal Table Tablet Creation Paths

## Overview

In shared-data mode, tablet creation follows this core flow:

1. **FE side**: Calls `StarOSAgent` → StarManager's `createShardGroup` + `createShards` to allocate shard IDs (shard ID = tablet ID)
2. **FE side**: Sends `CREATE` agent task (via `TabletTaskExecutor` → `CreateReplicaTask`) to BE/CN
3. **BE/CN side**: `lake::TabletManager::create_tablet()` writes tablet metadata (`TabletMetadataPB`) to object storage

### Core Classes

| Class | Location | Role |
|-------|----------|------|
| `StarOSAgent` | `fe/fe-core/.../lake/StarOSAgent.java` | Wraps StarManager shard creation APIs |
| `LocalMetastore` | `fe/fe-core/.../server/LocalMetastore.java` | Central partition/tablet creation logic |
| `LakeTablet` | `fe/fe-core/.../lake/LakeTablet.java` | Shared-data tablet (ID = shard ID) |
| `LakeTableHelper` | `fe/fe-core/.../lake/LakeTableHelper.java` | Routes alter/rollup for lake tables |
| `TabletTaskExecutor` | `fe/fe-core/.../task/TabletTaskExecutor.java` | Sends `CreateReplicaTask` agent tasks to BE/CN |
| `lake::TabletManager` | `be/src/storage/lake/tablet_manager.cpp` | Creates tablet metadata on object storage |

### StarOSAgent Shard Creation API Summary

| Method | Purpose | Callers (production code only) |
|--------|---------|------|
| `createShardGroup(dbId, tableId, partitionId, indexId)` | Create shard group for partition+index | `LocalMetastore.createPartition()`, `LakeTableRollupBuilder.build()` |
| `createShards(numShards, pathInfo, cacheInfo, groupId, matchShardIds, properties, computeResource)` | Create shards (tablets) | `LocalMetastore.createLakeTablets()`, `LakeTableRollupBuilder.build()`, `LakeTableAlterJobV2Builder.build()` |
| `createShardsForSplit(oldToNewShardIds, ...)` | Create shards for tablet split | `SplitTabletJobFactory.createNewShards()` |
| `createShardsForMerge(newToOldShardIds, ...)` | Create shards for tablet merge | `MergeTabletJobFactory.createNewShards()` |
| `createShardGroupForVirtualTablet()` | Shard group for storage volume | `SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` |
| `createShardWithVirtualTabletId(...)` | Single shard with explicit ID | `SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()` |

### `new LakeTablet(...)` Construction Sites (production code)

| File | Method | Context |
|------|--------|---------|
| `LocalMetastore.java:2248` | `createLakeTablets()` | Standard partition/tablet creation |
| `LakeTableAlterJobV2Builder.java:102` | `build()` | Schema change shadow tablets |
| `LakeTableRollupBuilder.java:111` | `build()` | Rollup/sync MV shadow tablets |
| `SplitTabletJobFactory.java:288` | `createMaterializedIndex()` | Tablet split new tablets |
| `MergeTabletJobFactory.java:372` | `createMaterializedIndex()` | Tablet merge new tablets |

---

## Complete Tablet Creation Paths (17 features)

### Category 1: Via `LocalMetastore.createPartition()` → `createLakeTablets()`

These all share the same bottom-level call chain:

```
LocalMetastore.createPartition()
  → StarOSAgent.createShardGroup()           // per index
  → createLakeTablets()
    → StarOSAgent.createShards(bucketNum)     // allocate shard IDs
    → new LakeTablet(shardId)                 // build FE metadata
  (then)
  buildPartitions()
    → TabletTaskExecutor.buildCreateReplicaTasks()
      → CreateReplicaTask (sent to BE/CN via Agent RPC)
        → lake::TabletManager::create_tablet()  // write metadata to object storage
```

#### 1. CREATE TABLE (non-partitioned)

```
OlapTableFactory.createTable()
  → LocalMetastore.createPartition() → createLakeTablets()
  → buildPartitions()
```

Entry: `OlapTableFactory.java:818`

#### 2. CREATE TABLE (partitioned)

```
OlapTableFactory.createTable()
  → for each partition: LocalMetastore.createPartition() → createLakeTablets()
  → buildPartitions()
```

Entry: `OlapTableFactory.java:856`

#### 3. ADD PARTITION (ALTER TABLE ... ADD PARTITION)

```
AlterJobExecutor → LocalMetastore.addPartitions()
  → addPartitions() → createPartitionMap() → createPartition() → createLakeTablets()
  → buildPartitions()
```

Entry: `AlterJobExecutor.java:736` → `LocalMetastore.java:987`

#### 4. Automatic Partition Creation (INSERT/LOAD triggered)

```
FrontendServiceImpl.createPartitionProcess()
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `FrontendServiceImpl.java:2322`

#### 5. Dynamic Partition

```
DynamicPartitionScheduler.runOneCycle()
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `DynamicPartitionScheduler.java:452`

#### 6. TRUNCATE TABLE

```
LocalMetastore.truncateTable()
  → for each partition: createPartition() → createShardGroup() + createLakeTablets()
  → buildPartitions()
  → truncateTableInternal() (replace old partitions with new ones)
```

Entry: `LocalMetastore.java:4776`

#### 7. INSERT OVERWRITE (create new partitions by value)

```
InsertOverwriteJobRunner.createPartitionByValue()
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `InsertOverwriteJobRunner.java:348`

#### 8. INSERT OVERWRITE (create temp partitions to replace)

```
InsertOverwriteJobRunner.prepareOverwrite()
  → PartitionUtils.createAndAddTempPartitionsForTable()
    → LocalMetastore.createTempPartitionsFromPartitions()
      → getNewPartitionsFromPartitions() → createPartition() → createLakeTablets()
      → buildPartitions()
```

Entry: `InsertOverwriteJobRunner.java:416`

#### 9. CREATE MATERIALIZED VIEW (non-partitioned)

```
LocalMetastore.createMaterializedView()
  → buildNonPartitionOlapTable()
    → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `LocalMetastore.java:3287`

#### 10. MV PCT Refresh (add new partitions to MV)

```
MVPCTRefreshListPartitioner / MVPCTRefreshRangePartitioner
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `MVPCTRefreshListPartitioner.java:523`, `MVPCTRefreshRangePartitioner.java:559`

#### 11. ADD Sub-Partition (physical partition for random distribution)

```
LocalMetastore.addSubPartitions()
  → createPhysicalPartition() → createLakeTablets() (reuses existing ShardGroup)
  → buildPartitions()
```

Entry: `LocalMetastore.java:1706`

#### 12. ALTER TABLE MERGE PARTITION

```
MergePartitionJob.runPendingJob()
  → LocalMetastore.addPartitions() (creates temp partitions)
    → createPartitionMap() → createPartition() → createLakeTablets()
    → buildPartitions()
```

Entry: `MergePartitionJob.java:348`

#### 13. ALTER TABLE OPTIMIZE (OptimizeJobV2 / OnlineOptimizeJobV2)

```
OptimizeJobV2.runPendingJob() / OnlineOptimizeJobV2.runPendingJob()
  → PartitionUtils.createAndAddTempPartitionsForTable()
    → LocalMetastore.createTempPartitionsFromPartitions()
      → getNewPartitionsFromPartitions() → createPartition() → createLakeTablets()
      → buildPartitions()
```

Entry: `OptimizeJobV2.java:198`, `OnlineOptimizeJobV2.java:210`

---

### Category 2: Schema Change / Rollup (via dedicated builders)

#### 14. Schema Change (ALTER TABLE MODIFY COLUMN / ADD COLUMN / etc.)

```
LakeTableHelper.alterTable()
  → LakeTableAlterJobV2Builder.build()
    → StarOSAgent.createShards(matchShardIds)  // shadow tablets, co-located with origin
    → new LakeTablet(shadowTabletId)
    → shadowIndex.addTablet(shadowTablet)
  (then schema change job sends ALTER agent tasks to BE)
```

Entry: `LakeTableAlterJobV2Builder.java:89`

#### 15. ADD ROLLUP / CREATE MATERIALIZED VIEW (sync)

```
LakeTableHelper.rollUp()
  → LakeTableRollupBuilder.build()
    → StarOSAgent.createShardGroup()            // per partition + rollup index
    → StarOSAgent.createShards(matchShardIds)    // co-located with origin tablets
    → new LakeTablet(shadowTabletId)
    → mvIndex.addTablet(shadowTablet)
  (then rollup job sends ALTER agent tasks to BE)
```

Entry: `LakeTableRollupBuilder.java:76-111`

---

### Category 3: Tablet Resharding (via StarOSAgent split/merge APIs)

#### 16. Tablet Split

```
SplitTabletJobFactory.createNewShards()
  → StarOSAgent.createShardsForSplit(oldToNewTabletIds)
SplitTabletJobFactory.createMaterializedIndex()
  → new LakeTablet(tabletId, oldTablet.getRange())
  (then publish_version on BE triggers publish_resharding_tablet → handle_splitting_tablet)
```

Entry: `SplitTabletJobFactory.java:313`, BE: `tablet_reshard.cpp`

#### 17. Tablet Merge

```
MergeTabletJobFactory.createNewShards()
  → StarOSAgent.createShardsForMerge(newToOldTabletIds)
MergeTabletJobFactory.createMaterializedIndex()
  → new LakeTablet(tabletId, oldTablet.getRange())
  (then publish_version on BE triggers publish_resharding_tablet → handle_merging_tablet)
```

Entry: `MergeTabletJobFactory.java:400`, BE: `tablet_reshard.cpp`

---

### Category 4: Special Paths

#### 18. Storage Volume Virtual Tablet

```
SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()
  → StarOSAgent.createShardGroupForVirtualTablet()
  → StarOSAgent.createShardWithVirtualTabletId(vTabletId)
```

Entry: `SharedDataStorageVolumeMgr.java:724-729`

Used for cross-cluster replication and storage volume mapping. Does NOT create a standard data tablet.

#### 19. Restore (Backup/Restore)

FE side:
```
LakeRestoreJob.resetTableForRestore()
  → RestoreJob.resetIdsForRestore()
    → RestoreJob.createTabletsForRestore()
      → new LocalTablet(newTabletId)  // NOTE: creates LocalTablet, not LakeTablet
```

BE side:
```
LakeSnapshotLoader::restore()
  → lake_tablet_manager()->put_tablet_metadata(meta)  // writes restored metadata directly
```

Entry FE: `LakeRestoreJob.java:302`, BE: `lake_snapshot_loader.cpp:309`

Note: Lake restore uses `LocalTablet` in FE metadata initialization (inherited from `RestoreJob`), but actual tablet metadata on object storage is handled by `LakeSnapshotLoader` on BE.

---

## BE Side Tablet Creation Summary

| Path | Handler | File | When Used |
|------|---------|------|-----------|
| CREATE agent task | `run_create_tablet_task()` → `lake_tablet_manager()->create_tablet()` | `agent_task.cpp:248-251`, `tablet_manager.cpp:209` | All Category 1 & 2 features above |
| Publish resharding (split) | `publish_resharding_tablet()` → `handle_splitting_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | Tablet split (#16) |
| Publish resharding (merge) | `publish_resharding_tablet()` → `handle_merging_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | Tablet merge (#17) |
| Publish resharding (identical) | `publish_resharding_tablet()` → `handle_identical_tablet()` → `put_tablet_metadata()` | `tablet_reshard.cpp` | Resharding with same bucket count |
| Lake restore | `LakeSnapshotLoader::restore()` → `put_tablet_metadata()` | `lake_snapshot_loader.cpp:309` | Restore (#19) |
| Repair metadata | `LakeServiceImpl::repair_tablet_metadata()` → `put_tablet_metadata()` | `lake_service.cpp:1917` | Admin repair (not new tablet creation) |

---

## Complete Summary Table

| # | Feature | FE Entry Point | StarOSAgent Method | BE Path |
|---|---------|---------------|-------------------|---------|
| 1 | CREATE TABLE (non-partitioned) | `OlapTableFactory:818` | `createShardGroup` + `createShards` | CREATE task |
| 2 | CREATE TABLE (partitioned) | `OlapTableFactory:856` | `createShardGroup` + `createShards` | CREATE task |
| 3 | ADD PARTITION | `AlterJobExecutor:736` → `LocalMetastore.addPartitions` | `createShardGroup` + `createShards` | CREATE task |
| 4 | Automatic partition (load) | `FrontendServiceImpl:2322` | `createShardGroup` + `createShards` | CREATE task |
| 5 | Dynamic partition | `DynamicPartitionScheduler:452` | `createShardGroup` + `createShards` | CREATE task |
| 6 | TRUNCATE TABLE | `LocalMetastore:4776` | `createShardGroup` + `createShards` | CREATE task |
| 7 | INSERT OVERWRITE (new partitions) | `InsertOverwriteJobRunner:348` | `createShardGroup` + `createShards` | CREATE task |
| 8 | INSERT OVERWRITE (temp partitions) | `InsertOverwriteJobRunner:416` | `createShardGroup` + `createShards` | CREATE task |
| 9 | CREATE MV (non-partitioned) | `LocalMetastore:3287` | `createShardGroup` + `createShards` | CREATE task |
| 10 | MV PCT refresh | `MVPCTRefresh*Partitioner` | `createShardGroup` + `createShards` | CREATE task |
| 11 | ADD sub-partition | `LocalMetastore:1706` | `createShards` (reuse group) | CREATE task |
| 12 | MERGE PARTITION | `MergePartitionJob:348` | `createShardGroup` + `createShards` | CREATE task |
| 13 | ALTER TABLE OPTIMIZE | `OptimizeJobV2:198` / `OnlineOptimizeJobV2:210` | `createShardGroup` + `createShards` | CREATE task |
| 14 | Schema change | `LakeTableAlterJobV2Builder:89` | `createShards` (with placement) | CREATE task |
| 15 | ADD ROLLUP / sync MV | `LakeTableRollupBuilder:76-111` | `createShardGroup` + `createShards` | CREATE task |
| 16 | Tablet split | `SplitTabletJobFactory:313` | `createShardsForSplit` | Publish resharding |
| 17 | Tablet merge | `MergeTabletJobFactory:400` | `createShardsForMerge` | Publish resharding |
| 18 | Storage volume virtual tablet | `SharedDataStorageVolumeMgr:724` | `createShardGroupForVirtualTablet` + `createShardWithVirtualTabletId` | N/A (metadata only) |
| 19 | Restore | `LakeRestoreJob:302` | N/A (uses resetIdsForRestore) | `LakeSnapshotLoader` → `put_tablet_metadata` |

---

## Operations That Do NOT Create Tablets in Shared-Data Mode

| Operation | Reason |
|-----------|--------|
| **Clone** | Shared-data uses replication (`remote_snapshot` + `replicate_snapshot`) instead of clone; target tablets already exist |
| **Storage medium migration** | Not applicable to lake tablets (data is on object storage) |
| **Compaction** | Only updates metadata and rowsets within existing tablets |
| **REPLACE PARTITION** (`replaceTempPartition`) | Only swaps partition metadata; temp partitions were already created earlier |
| **Publish version (normal)** | Updates version metadata on existing tablets, does not create new ones |
| **Repair tablet metadata** | Rewrites metadata for existing tablets, does not create new tablets |

---

## Verification Methodology

This document was verified by exhaustively searching:
1. All production callers of `StarOSAgent.createShards*()`, `createShardGroup*()`, `createShardWithVirtualTabletId()`
2. All production sites of `new LakeTablet(...)` construction
3. All callers of `LocalMetastore.createPartition()`, `createLakeTablets()`, `createPhysicalPartition()`
4. All callers of `LocalMetastore.addPartitions()`, `addSubPartitions()`, `truncateTable()`
5. All callers of `createTempPartitionsFromPartitions()`, `PartitionUtils.createAndAddTempPartitionsForTable()`
6. All callers of `buildNonPartitionOlapTable()`
7. BE-side: all callers of `lake::TabletManager::create_tablet()` and `put_tablet_metadata()`
8. BE-side: all `TABLET_TYPE_LAKE` references in `agent_task.cpp`
