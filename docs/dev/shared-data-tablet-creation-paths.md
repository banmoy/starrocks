# Shared-Data (存算分离) Internal Table Tablet Creation Paths

## Overview

In shared-data mode, tablet creation follows this core flow:

1. **FE side**: Calls `StarOSAgent` → StarManager's `createShardGroup` + `createShards` to allocate shard IDs (shard ID = tablet ID)
2. **BE/CN side**: FE sends `CREATE` agent task → `lake::TabletManager::create_tablet()` writes tablet metadata to object storage

### Core Classes

| Class | Location | Role |
|-------|----------|------|
| `StarOSAgent` | `fe/fe-core/.../lake/StarOSAgent.java` | Wraps StarManager shard creation APIs |
| `LocalMetastore` | `fe/fe-core/.../catalog/LocalMetastore.java` | Central partition/tablet creation logic |
| `LakeTablet` | `fe/fe-core/.../lake/LakeTablet.java` | Shared-data tablet (ID = shard ID) |
| `LakeTableHelper` | `fe/fe-core/.../lake/LakeTableHelper.java` | Routes alter/rollup for lake tables |
| `lake::TabletManager` | `be/src/storage/lake/tablet_manager.cpp` | Creates tablet metadata on object storage |

---

## All Tablet Creation Paths

### 1. CREATE TABLE

```
OlapTableFactory.createTable()
  → LocalMetastore.createPartition()
    → StarOSAgent.createShardGroup()
    → LocalMetastore.createLakeTablets()
      → StarOSAgent.createShards(bucketNum)
      → new LakeTablet(shardId)
```

### 2. ADD PARTITION (manual)

```
AddPartitionClause → LocalMetastore.addPartitions()
  → createPartitionMap() → createPartition()
    → createShardGroup() + createLakeTablets()
```

### 3. Automatic Partition (INSERT/LOAD triggered)

```
FrontendServiceImpl.createPartitionProcess()
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
```

### 4. Dynamic Partition

```
DynamicPartitionScheduler.runOneCycle()
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
```

### 5. TRUNCATE TABLE

```
LocalMetastore.truncateTable()
  → createPartition()
    → createShardGroup() + createLakeTablets()
  → truncateTableInternal()
```

### 6. INSERT OVERWRITE

```
InsertOverwriteJobRunner.run()
  → createPartitionByValue()
    → LocalMetastore.addPartitions()
      → createPartitionMap() → createPartition() → createLakeTablets()
```

### 7. CREATE MATERIALIZED VIEW

```
LocalMetastore.createMaterializedView()
  → OlapTableFactory → createPartition() → createLakeTablets()
```

### 8. ADD ROLLUP / Sync Materialized View

```
LakeTableRollupBuilder.build()
  → StarOSAgent.createShardGroup()
  → StarOSAgent.createShards(matchShardIds)
  → new LakeTablet(shardId)
```

### 9. Schema Change (ALTER TABLE)

```
LakeTableAlterJobV2Builder.build()
  → StarOSAgent.createShards(matchShardIds)
  → new LakeTablet(shadowTabletId)
```

### 10. Tablet Split (Resharding)

```
SplitTabletJobFactory.createNewShards()
  → StarOSAgent.createShardsForSplit(oldToNewTabletIds)
  → new LakeTablet(tabletId)
```

### 11. Tablet Merge (Resharding)

```
MergeTabletJobFactory.createNewShards()
  → StarOSAgent.createShardsForMerge(newToOldTabletIds)
  → new LakeTablet(tabletId)
```

### 12. ADD Sub-Partition

```
LocalMetastore.addSubPartitions()
  → createPhysicalPartition()
    → createShardGroup() + createLakeTablets()
```

### 13. MV PCT Refresh (new partitions)

```
MVPCTRefresh*Partitioner
  → LocalMetastore.addPartitions()
    → createPartitionMap() → createPartition() → createLakeTablets()
```

### 14. Storage Volume Virtual Tablet

```
SharedDataStorageVolumeMgr.getOrCreateVirtualTabletId()
  → StarOSAgent.createShardGroupForVirtualTablet()
  → StarOSAgent.createShardWithVirtualTabletId(vTabletId)
```

### 15. Restore

```
LakeRestoreJob.resetTableForRestore()
  → RestoreJob.resetIdsForRestore() → createTabletsForRestore()
```

---

## BE Side Tablet Creation

### Path 1: CREATE Agent Task (primary)

```
AgentServer::submit_tasks()
  → run_create_tablet_task()
    → lake_tablet_manager()->create_tablet()    // writes metadata to object storage
```

Files: `be/src/agent/agent_task.cpp`, `be/src/storage/lake/tablet_manager.cpp`

### Path 2: Publish Resharding

```
LakeServiceImpl::publish_version()
  → publish_resharding_tablet()
    → handle_splitting_tablet()   // 1 → N
    → handle_merging_tablet()     // N → 1
    → handle_identical_tablet()   // 1 → 1
  → put_tablet_metadata()
```

Files: `be/src/service/service_be/lake_service.cpp`, `be/src/storage/lake/tablet_reshard.cpp`

---

## Summary Table

| Feature | FE Key Class | StarOSAgent Method | BE Path |
|---------|-------------|-------------------|---------|
| CREATE TABLE | `OlapTableFactory`, `LocalMetastore` | `createShardGroup` + `createShards` | CREATE task |
| ADD PARTITION | `LocalMetastore.addPartitions` | `createShardGroup` + `createShards` | CREATE task |
| Auto Partition | `FrontendServiceImpl` | `createShardGroup` + `createShards` | CREATE task |
| Dynamic Partition | `DynamicPartitionScheduler` | `createShardGroup` + `createShards` | CREATE task |
| TRUNCATE TABLE | `LocalMetastore.truncateTable` | `createShardGroup` + `createShards` | CREATE task |
| INSERT OVERWRITE | `InsertOverwriteJobRunner` | `createShardGroup` + `createShards` | CREATE task |
| CREATE MV | `LocalMetastore.createMaterializedView` | `createShardGroup` + `createShards` | CREATE task |
| ADD ROLLUP | `LakeTableRollupBuilder` | `createShardGroup` + `createShards` | CREATE task |
| Schema Change | `LakeTableAlterJobV2Builder` | `createShards` | CREATE task |
| Tablet Split | `SplitTabletJobFactory` | `createShardsForSplit` | Publish resharding |
| Tablet Merge | `MergeTabletJobFactory` | `createShardsForMerge` | Publish resharding |
| Sub-Partition | `LocalMetastore.addSubPartitions` | `createShardGroup` + `createShards` | CREATE task |
| MV PCT Refresh | `MVPCTRefresh*Partitioner` | `createShardGroup` + `createShards` | CREATE task |
| Storage Volume vTablet | `SharedDataStorageVolumeMgr` | `createShardGroupForVirtualTablet` + `createShardWithVirtualTabletId` | CREATE task |
| Restore | `LakeRestoreJob` | via `createPartition` | CREATE task |

---

## Not Applicable in Shared-Data Mode

- **Clone task** — replaced by replication (remote_snapshot + replicate_snapshot)
- **Storage medium migration** — not applicable to lake tablets
- **Compaction** — only updates metadata/rowsets, does not create new tablets
