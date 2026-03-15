# 升降级兼容性分析

## 核心不变量的变化

> **老不变量**：DDL 完成后，对象存储上一定存在该 tablet 的 version 1 metadata 文件。
>
> **新不变量**：DDL 完成后，对象存储上可能不存在 version 1 metadata 文件；
> metadata 会在首次使用时 lazy 创建或通过其他机制提供。

## 升级规范

- **升级顺序：先升 CN，再升 FE**
- **降级顺序：先降 FE，再降 CN**

---

## 升级场景

### Phase 1：CN 已升级，FE 未升级

| 操作 | 行为 | 风险 |
|------|------|------|
| 老 FE 建表 | 发 `CreateReplicaTask` 到新 CN | **OK** — 新 CN 保留了 handler |
| 新 CN 处理 CREATE task | 写 metadata 到对象存储 | **OK** — 向后兼容 |

**要求**：新 CN **必须保留** `run_create_tablet_task()` 和 `lake::TabletManager::create_tablet()` 的处理逻辑。

### Phase 2：CN 已升级，FE 已升级（目标稳定态）

| 操作 | 行为 | 风险 |
|------|------|------|
| 新 FE 建表 | 跳过 CN 交互，不写 initial metadata | — |
| 新 CN 执行 INSERT | `publish_version` 读 `base_version=1` | 新 CN 必须处理缺失的 version 1 |
| 新 CN 执行 SELECT 空表 | `get_tablet_metadata(tablet_id, 1)` | 新 CN 必须处理缺失的 version 1 |
| 新 CN 执行 Schema Change | `get_tablet(new_tablet_id, 1)` | 新 CN 必须处理缺失的 version 1 |

**要求**：新 CN 必须能处理所有没有 initial metadata 的 tablet 操作。
需要适配的下游消费者有 13 条路径（详见 [04-affected-code-paths.md](04-affected-code-paths.md) Category 3）。

### 混合版本 FE（Leader/Follower）

- **Edit log 回放不受影响**：Follower 不发 `CreateReplicaTask`，只应用 FE 元数据变更
- **Edit log 格式不变**：`CreateTableInfo`、`AddPartitionsInfoV2`、`TruncateTableInfo` 不含 CN 交互信息
- **Leader 切换安全**：由于所有 CN 已是新版本（Phase 1 先升 CN），新 CN 能处理有/无 metadata 的 tablet

**关键洞察**：CN 总是先于 FE 升级，因此当任何 FE（新或旧）运行时，所有 CN 都已是新版本。
CN 处理缺失 metadata 的能力是核心要求，而非 FE 侧兼容性。

---

## 降级场景

### Phase 1：FE 已降级，CN 仍为新版本

| 操作 | 行为 | 风险 |
|------|------|------|
| 老 FE 新建表 | 发 `CreateReplicaTask` 到新 CN | **OK** — 新 CN 处理正常 |
| 老 FE 操作新 FE 创建的表（无 metadata）| 发正常操作到新 CN | **OK** — 新 CN 知道如何处理 |
| 动态分区/MV 刷新触及新 FE 创建的表 | 老 FE 发 ADD PARTITION → `CreateReplicaTask` 到新 CN | **OK** |

**结论**：Phase 1 基本安全，因为新 CN 同时支持有/无 metadata 的 tablet。

**重要窗口**：这是运行修复工具的最佳时机——新 CN 还在，可以为所有缺失 metadata 的 tablet 补写。

### Phase 2：FE 已降级，CN 已降级

| 操作 | 行为 | 风险 |
|------|------|------|
| 操作新 FE 创建的 tablet（无 metadata） | 老 CN 读 version 1 → 不存在 | **失败** |
| INSERT | `publish_version` 读 `base_version=1` | **失败** |
| SELECT 空表 | `get_tablet_metadata(tablet_id, 1)` | **失败** |
| Schema Change | `get_tablet(new_tablet_id, 1)` | **失败** |
| 操作老 FE 创建的 tablet（有 metadata） | 老 CN 正常读取 | **OK** |
| DROP TABLE | Shard 删除 + vacuum | **OK** — vacuum 处理缺失文件 |

**结论**：全量降级后，新版本期间创建的 tablet **在老 CN 上损坏**。

**缓解方案**：
1. 降级 Phase 1 期间运行修复工具，补写 metadata
2. 接受全量降级对新 tablet 是破坏性操作
3. 在 FE 元数据或 StarManager 中维护无 metadata tablet 的注册表

---

## 对象存储产物兼容性

| 产物 | 老版本预期 | 新版本行为 | 差距 |
|------|-----------|-----------|------|
| `{tablet_id}_{1}.meta`（per-tablet） | `optimization=false` 时存在 | 可能不存在 | **Breaking** |
| `0000000000000000_0000000000000001.meta`（共享 initial） | `optimization=true` 时存在 | 可能不存在 | **Breaking** |
| `SCHEMA_{schema_id}`（schema file） | 每个 partition 第一个 tablet 存在 | 可能不存在 | **降级** — fallback 有效但增加延迟 |
| `TabletMetadataPB` protobuf 格式 | Version N | Version N+1 | **必须向后兼容** |

---

## 混合版本期间的交叉功能兼容性

### Schema Change

1. 新 FE 创建 shadow tablets（无 metadata）
2. 发 ALTER task 到 CN
3. **所有 CN 已是新版本** → 新 CN 处理缺失 metadata → **OK**

### Tablet Split/Merge

1. 新 FE 创建 shard IDs
2. 发 `PublishVersionRequest` with `resharding_tablet_infos` 到 CN
3. **所有 CN 已是新版本** → 新 CN 处理缺失 metadata → **OK**

---

## Edit Log / Journal 兼容性

| 方面 | 影响 |
|------|------|
| `logCreateTable(CreateTableInfo)` | 格式不变 |
| `logAddPartition(PartitionPersistInfoV2)` | 格式不变 |
| `logTruncateTable(TruncateTableInfo)` | 格式不变 |
| FE image（checkpoint） | 格式不变 |
| Follower 回放 | 不受影响 |
| 重启后 log 回放 | 不受影响 |

**兼容性问题完全在对象存储产物层面，不在 FE 元数据层面。**

---

## FE 元数据一致性

| 方面 | 老行为 | 新行为 | 风险 |
|------|--------|--------|------|
| `PhysicalPartition.visibleVersion` | 创建时设为 `PARTITION_INIT_VERSION = 1` | 不变 | 无 |
| `MaterializedIndex` 中的 `Tablet` | `LakeTablet(shardId)` | 不变 | 无 |
| `OlapTable.partitions` | `buildPartitions` 成功后注册 | 不等 CN → 更快注册 | **时序变化** — partition 可能更早可见 |

---

## StarManager / StarOS 兼容性

| 方面 | 影响 |
|------|------|
| Shard 创建 | 不变 — shard ID 仍从 StarManager 分配 |
| Shard 删除 / GC | 不变 |
| Shard 信息查询 | 不变 |
| Worker shard 注册 | 可能受影响 — `StarOSWorker::add_shard()` 是否依赖 metadata 存在 |

---

## 风险矩阵

| 场景 | 风险级别 | 失败模式 |
|------|---------|---------|
| **升级 Phase 1**：CN 新 + FE 老 | **低** | 新 CN 保留 handler 即安全 |
| **升级 Phase 2**：CN 新 + FE 新 | **无**（目标态） | 新 CN 必须处理缺失 metadata |
| **混合 FE** | **低** | CN 已全部是新版本 |
| **降级 Phase 1**：FE 老 + CN 新 | **低** | 新 CN 同时支持两种 tablet |
| **降级 Phase 2**：FE 老 + CN 老 | **高** | 新 FE 创建的 tablet 损坏 |
| Edit log 回放 | **无** | 格式不变 |
| StarManager | **无** | Shard 分配独立 |

---

## 必须保证的兼容性

1. **新 CN 必须保留 `create_tablet` handler** — 升级 Phase 1 和降级 Phase 1 使用
2. **新 CN 必须处理所有无 initial metadata 的操作** — 13 条下游消费者路径
3. **Protobuf `TabletMetadataPB` 必须向后兼容** — 新字段必须 optional + 有默认值
4. **降级需修复或接受影响** — 降级 Phase 1 窗口可运行修复工具
5. **Feature flag** 非升级安全所必需（CN 先升级），但建议用于运维控制和灰度发布
