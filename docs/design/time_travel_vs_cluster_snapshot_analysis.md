# Cluster Snapshot vs Time Travel：数据恢复场景差异与产品定位分析

---

## 一、现有数据恢复/保护能力全景

StarRocks 当前在数据恢复领域有三个已有机制，加上本次设计的 Time Travel 共四个能力：

| 机制 | 引入版本 | 适用架构 | 核心定位 |
|------|---------|---------|---------|
| **RECOVER (RecycleBin)** | 早期 | 存算一体 | DROP 误操作的快速恢复 |
| **Backup/Restore** | 早期 | 存算一体 | 手动备份与跨集群恢复 |
| **Cluster Snapshot** | v3.4.2 | 存算分离 | 集群级灾难恢复 |
| **Time Travel** (新增) | 规划中 | 存算分离 | 表级精细数据恢复与历史查询 |

---

## 二、Cluster Snapshot 深度解析

### 2.1 设计目标

Cluster Snapshot 的核心目标是**集群级灾难恢复 (Disaster Recovery)**。它解决的是"整个集群不可用时如何恢复"的问题。

### 2.2 工作原理

```
┌─────────────────────────────────────────────────┐
│              Cluster Snapshot                     │
│                                                   │
│  FE Metadata Image ──┐                           │
│                      ├──→ Object Storage          │
│  StarMgr Image ──────┘    (统一存储)              │
│                                                   │
│  CN Data (已在对象存储) ──→ 无需额外操作             │
│                                                   │
│  保留策略：仅保留最新 1 个快照                      │
│  默认间隔：10 分钟                                  │
│  恢复方式：重启整个集群                              │
└─────────────────────────────────────────────────┘
```

### 2.3 关键特征

| 特征 | 描述 |
|------|------|
| **快照对象** | 集群的**所有对象**：catalog、database、table、用户与权限、loading tasks 等（不含外部 catalog 配置文件、本地 UDF JAR 等外部依赖） |
| **粒度** | 集群级别（全量元数据 + 全量数据），不可选择性恢复单个对象 |
| **触发方式** | 定时器驱动：每次 FE checkpoint 后自动创建（默认 ~10 分钟间隔） |
| **保留数量** | 仅 1 个（新快照替换旧快照） |
| **默认间隔** | 600 秒（10 分钟） |
| **恢复方式** | 需要停机、清理 meta 目录、以特殊参数重启 FE/CN |
| **恢复粒度** | 只能恢复整个集群，不能选择性恢复单表/单库 |
| **适用架构** | 仅存算分离（shared-data） |
| **存储开销** | 低（元数据 image 很小，数据文件本身已在对象存储） |

### 2.4 恢复流程

```
发现灾难 → 确认快照可用 → 编辑 cluster_snapshot.yaml
    → 停止所有节点 → 清理 meta/storage_root_path 目录
    → start_fe.sh --cluster_snapshot → 启动其他 FE → 启动 CN
    → 验证集群状态
```

**典型恢复时间**：分钟到小时级（取决于集群规模和元数据大小）
**需要停机**：是，必须完全停止再重启

---

## 三、Time Travel 深度解析（设计目标）

### 3.1 设计目标

Time Travel 的核心目标是**表级精细数据恢复与历史数据查询**。它解决的是"某张表/某个分区的数据出了问题如何快速恢复"以及"如何查看历史状态"的问题。

### 3.2 工作原理

```
┌─────────────────────────────────────────────────┐
│                Time Travel                        │
│                                                   │
│  保留每个 tablet 的历史版本元数据                    │
│  保留对应的历史数据文件（延迟 GC）                    │
│                                                   │
│  查询：直接读取历史版本数据                          │
│  恢复：基于历史版本创建新版本                        │
│                                                   │
│  保留策略：可配置（默认 7 天）                       │
│  恢复方式：在线 SQL 操作，无需停机                    │
│  恢复粒度：表级/分区级                              │
└─────────────────────────────────────────────────┘
```

### 3.3 关键特征

| 特征 | 描述 |
|------|------|
| **快照对象** | **表**（table）的数据状态：每个 tablet 的历史版本及对应数据文件 |
| **粒度** | 表级、分区级 |
| **触发方式** | 操作驱动：每次导致表状态改变的操作（DML/DDL）自然产生新版本，Time Travel 延长这些版本的保留期。每个已提交的事务都是一个可回溯的恢复点 |
| **保留时长** | 可配置，默认 7 天，最长 365 天 |
| **恢复方式** | 在线 SQL（`ALTER TABLE ... RESTORE TO ...`） |
| **恢复粒度** | 可精确到单表、单分区 |
| **适用架构** | 存算分离（利用对象存储低成本特性） |
| **存储开销** | 中等（需要保留历史版本的数据文件） |

---

## 四、核心差异对比

### 4.1 维度对比矩阵

| 对比维度 | Cluster Snapshot | Time Travel |
|---------|-----------------|-------------|
| **问题域** | **基础设施层面**的物理恢复（DR） | **数据逻辑层面**的逻辑恢复 |
| **典型场景** | 集群机房故障、FE 元数据损坏 | 业务误删数据、误更新、写入脏数据 |
| **快照粒度** | 集群（全有全无） | 表（精细） |
| **快照对象** | 集群所有对象（catalog、database、table、权限、tasks 等） | 表（table）的数据状态 |
| **触发方式** | 定时器驱动（每 ~10 分钟 FE checkpoint 后创建） | 操作驱动（每次 DML/DDL 提交自然产生新版本） |
| **保留策略** | 一般从最新恢复，只需要保留最新的 1 个 | 无法预测误操作发生时间，要保留一段时间的所有快照 |
| **快照成本** | 低（FE 元数据同步到对象存储） | 极低（维护 FE 元数据多版本） |
| **恢复粒度** | 集群或单个表 | 表 |
| **恢复点精度** | 稀疏（~10 分钟一个，且仅保留最新 1 个） | 稠密（每个已提交事务都是恢复点，保留期内均可回溯） |
| **恢复前预览** | 不支持（盲恢复） | 支持（`SELECT ... FOR TIMESTAMP AS OF` 先查后恢复） |
| **恢复复杂度** | 集群粒度复杂，表粒度简单（单条 SQL） | 简单（单条 SQL） |
| **操作者** | DBA / 运维团队 | 数据工程师 / 分析师 / DBA |

### 4.2 场景覆盖矩阵

| 恢复场景 | 层面 | Cluster Snapshot | Time Travel | 最佳选择 |
|---------|------|:---------------:|:-----------:|---------|
| 机房/存储整体故障 | 基础设施 | **能** | 不能 | Cluster Snapshot |
| 集群元数据损坏 | 基础设施 | **能** | 不能 | Cluster Snapshot |
| 集群迁移/克隆 | 基础设施 | **能** | 不能 | Cluster Snapshot |
| 误 DELETE 数据行 | 数据逻辑 | 能（表级恢复到新表，精度受限于快照间隔） | **能** | Time Travel |
| 误 DROP TABLE | 数据逻辑 | 能（表级恢复到新表，精度受限于快照间隔） | **能** | Time Travel |
| 误 DROP DATABASE | 数据逻辑 | 能（需逐表恢复） | **能** | Time Travel |
| 误 UPDATE（坏数据写入） | 数据逻辑 | 能（表级恢复到新表，精度受限于快照间隔） | **能** | Time Travel |
| ETL 写入脏数据 | 数据逻辑 | 能（表级恢复到新表，精度受限于快照间隔） | **能** | Time Travel |
| 查看历史数据状态 | 数据逻辑 | 不能 | **能** | Time Travel |
| 数据审计/合规 | 数据逻辑 | 不能 | **能** | Time Travel |
| 报表数据重现 | 数据逻辑 | 不能 | **能** | Time Travel |
| 跨时间点数据对比 | 数据逻辑 | 不能 | **能** | Time Travel |
| 增量变更追踪 | 数据逻辑 | 不能 | **能** | Time Travel |

### 4.3 关键洞察：物理恢复 vs 逻辑恢复

两者的核心定位差异可以概括为：

- **Cluster Snapshot** 面向**基础设施层面的物理恢复**——机房故障、存储损坏、元数据损坏等导致集群不可用的场景
- **Time Travel** 面向**数据逻辑层面的逻辑恢复**——误删数据、误更新、写入脏数据等用户操作导致的数据逻辑错误

**Cluster Snapshot 技术上也能覆盖数据逻辑恢复**——支持集群级全量恢复，也即将支持表级恢复（`RESTORE TABLE ... FROM SNAPSHOT ... TO TABLE ...`）。但与 Time Travel 相比，在数据逻辑恢复场景中仍有本质局限：

1. **恢复点精度不足**：Cluster Snapshot 由定时器触发（~10 分钟间隔），且仅保留最新 1 个快照。如果误操作发生在两次快照之间，快照中已包含错误数据，无法恢复到误操作前一刻。Time Travel 的恢复点是每个已提交事务，可精确恢复到任意历史版本
2. **无法预览**：Cluster Snapshot 的表级恢复是"盲恢复"——无法在恢复前查看快照中的数据是否正确。Time Travel 可先通过 `SELECT ... FOR TIMESTAMP AS OF` 查询验证，再决定是否恢复
3. **恢复到新表而非原地恢复**：Cluster Snapshot 的表级恢复将数据恢复到一张新表（`TO TABLE target_table`），用户需手动处理新旧表的切换。Time Travel 的 `ALTER TABLE ... RESTORE TO TIMESTAMP` 是原地恢复，直接生效

因此，Cluster Snapshot 的表级恢复是一个有用的补充能力（特别是在 Time Travel 未启用或保留期已过的场景下），但 Time Travel 在恢复精度、可预览性和操作便捷性上仍有不可替代的优势。两者形成互补的分层防护体系。

---

## 五、产品定位建议

### 5.1 定位框架

两个特性应该定位在**完全不同的产品层级**，形成互补而非竞争关系：

```
┌─────────────────────────────────────────────────────────────┐
│                    数据保护金字塔                              │
│                                                               │
│                        ┌───┐                                  │
│                       │ DR │  Cluster Snapshot                │
│                      │     │  集群级灾难恢复                   │
│                     │  RPO: │  面向：运维/DBA                  │
│                    │ ~10min │  频率：极低（灾难时）             │
│                   ├─────────┤                                  │
│                  │  Recovery │  Time Travel                    │
│                 │  数据恢复   │  表级精细恢复                    │
│                │  RPO: 秒级   │  面向：数据工程师/DBA           │
│               │  在线操作      │  频率：中等                    │
│              ├────────────────┤                                │
│             │   Query/Audit    │  Time Travel                  │
│            │   历史查询/审计    │  历史数据查询、变更追踪          │
│           │   无恢复需求        │  面向：分析师/合规              │
│          │   频率：高            │                              │
│         └─────────────────────-┘                              │
└─────────────────────────────────────────────────────────────┘

RPO = Recovery Point Objective（恢复点目标）
```

### 5.2 产品定位总结

| 维度 | Cluster Snapshot | Time Travel |
|------|-----------------|-------------|
| **一句话定位** | 集群级灾难恢复方案 | 表级数据时间旅行与精细恢复 |
| **产品类别** | 灾备 (Disaster Recovery) | 数据治理 (Data Governance) |
| **目标用户** | 运维团队、DBA | 数据工程师、分析师、DBA |
| **使用频率** | 极低（仅灾难时） | 高（日常查询和偶尔恢复） |
| **品牌定位** | "集群生命线" | "数据时光机" |
| **竞品对标** | AWS RDS Automated Backups, GCP Cloud SQL Backup | Snowflake Time Travel, Delta Lake Time Travel |
| **付费策略建议** | 基础能力（免费） | 可按保留期收费（超出默认 7 天） |

### 5.3 用户沟通策略

面向用户时，应清晰区分两个特性的使用场景：

**Cluster Snapshot — "当集群出问题时"**
> "Cluster Snapshot 是您集群的最后一道防线。当整个集群因硬件故障、元数据损坏等灾难性事件不可用时，可以从对象存储中的快照完整恢复集群。"

**Time Travel — "当数据出问题时"**
> "Time Travel 让您随时查看和恢复表的历史数据。无论是误删了数据、写入了脏数据，还是需要审计某个历史时间点的数据状态，都可以通过一条简单的 SQL 完成，无需停机。"

### 5.4 文档/产品中的位置

```
Administration & Management
├── Disaster Recovery（灾难恢复）
│   └── Cluster Snapshot          ← 运维视角
├── Data Management（数据管理）
│   ├── Time Travel               ← 数据视角
│   │   ├── Point-in-Time Query
│   │   ├── Data Recovery (RESTORE / UNDROP)
│   │   └── Change Tracking
│   └── Backup and Restore        ← 传统备份（存算一体）
```

---

## 六、两者协作关系

### 6.1 互补而非替代

Cluster Snapshot 和 Time Travel 不是竞争关系，而是互补关系，覆盖不同层级的风险：

```
风险等级          防护机制                          恢复方式
─────────────────────────────────────────────────────────────
低风险            Time Travel 历史查询              SELECT ... FOR TIMESTAMP AS OF
(数据查询)        无需恢复，仅查看

中风险            Time Travel 数据恢复              ALTER TABLE ... RESTORE TO ...
(表级误操作)      在线 SQL 原地恢复                 UNDROP TABLE ...
                  Cluster Snapshot 表级恢复(补充)   RESTORE TABLE ... FROM SNAPSHOT ...
                  恢复到新表，精度受限于快照间隔       TO TABLE ...

高风险            Cluster Snapshot                 stop → start --cluster_snapshot
(集群级灾难)      集群重建                          全量恢复
```

### 6.2 Time Travel 与 Cluster Snapshot 的技术联系

在存算分离架构下，两者共享底层基础设施：

1. **对象存储共享**：两者的数据都在对象存储中，Cluster Snapshot 利用已有的数据文件，Time Travel 延迟这些文件的 GC
2. **版本机制共用**：Cluster Snapshot 的 `retainVersions` 机制已经实现了阻止 Vacuum 删除特定版本的能力，Time Travel 可以复用这一机制
3. **元数据协同**：Cluster Snapshot 保存的元数据 image 天然包含版本信息，可以作为 Time Travel 的补充

### 6.3 技术实现上的差异

| 技术维度 | Cluster Snapshot | Time Travel |
|---------|-----------------|-------------|
| **快照对象** | 集群所有对象（catalog/database/table/权限/loading tasks） | 表的数据状态（tablet 历史版本） |
| **版本产生方式** | 定时器驱动：FE checkpoint 定期触发（~10 分钟间隔） | 操作驱动：DML（INSERT/DELETE/UPDATE/LOAD）和 DDL（ADD/DROP COLUMN、ADD/DROP PARTITION 等）每次提交自然产生新版本 |
| **恢复点密度** | 稀疏（~10 分钟一个） | 稠密（每个已提交事务一个） |
| **元数据保留** | FE/StarMgr image 全量快照 | 延迟 tablet 版本元数据的 GC |
| **数据文件保留** | 数据已在对象存储，无额外操作 | 阻止 Vacuum 删除历史版本文件 |
| **版本保护机制** | `ClusterSnapshotMgr.getVacuumRetainVersions()` | 需要新增基于时间的版本保留策略 |
| **恢复路径** | 从 image 重建集群状态 | 从历史版本创建新版本（纯元数据操作） |

### 6.4 Vacuum 参数关系

当前 Vacuum 相关参数与 Time Travel 的关系：

| 现有参数 | 当前默认值 | 与 Time Travel 的关系 |
|---------|-----------|---------------------|
| `lake_autovacuum_grace_period_minutes` | 30 分钟 | Time Travel 需要将此概念扩展为可配置的保留期 |
| `lake_autovacuum_max_previous_versions` | 0 (无限) | Time Travel 需要基于时间而非版本数的保留策略 |
| `catalog_trash_expire_second` | 86400 秒 (1天) | Time Travel 的 UNDROP 应扩展此机制 |

**关键设计决策**：Time Travel 的版本保留应该**独立于** Vacuum 的 grace period，因为：
- Vacuum grace period 的目的是保护正在执行中的查询（分钟级别）
- Time Travel 的保留期是为了支持历史查询和数据恢复（天/周/月级别）
- 两者可以共存：`actual_retain_period = max(vacuum_grace_period, time_travel_retention)`

---

## 七、与存算一体 RECOVER 的关系

### 7.1 现有 RECOVER 的局限

| 局限 | 描述 |
|------|------|
| 仅支持 DROP 操作 | 不能恢复 DELETE/UPDATE 导致的数据丢失 |
| 保留期短 | 默认仅 1 天 (`catalog_trash_expire_second = 86400`) |
| 无历史查询 | 不能查看历史数据状态，只能盲目恢复 |
| 不支持 TRUNCATE | TRUNCATE TABLE 后数据不可恢复 |
| 命名冲突 | 如果新建了同名对象，无法恢复旧对象 |

### 7.2 Time Travel 如何超越 RECOVER

Time Travel 的 UNDROP 功能是对 RECOVER 的全面升级：

| 能力 | RECOVER (现有) | UNDROP (Time Travel) |
|------|:-------------:|:-------------------:|
| 恢复 DROP TABLE | 支持 | 支持 |
| 恢复 DROP DATABASE | 支持 | 支持 |
| 恢复 DROP PARTITION | 支持 | 支持 |
| 恢复前预览数据 | 不支持 | **支持**（先 Time Travel 查询确认） |
| 恢复 DELETE/UPDATE | 不支持 | **支持**（RESTORE TO TIMESTAMP） |
| 恢复 TRUNCATE | 不支持 | **支持**（RESTORE TO TIMESTAMP） |
| 可配置保留期 | 仅全局 1 个参数 | **分级配置**（系统/库/表） |
| 安全网 | 无 | **7 天 Fail-safe** |

### 7.3 建议

- 存算分离模式下，Time Travel 的 UNDROP 应**完全替代** RECOVER 的功能
- 存算一体模式保留 RECOVER 作为基础能力
- 长期来看，如果存算一体也支持 Time Travel，RECOVER 可以逐步废弃

---

## 八、总结与建议

### 8.1 产品定位一句话

> **Cluster Snapshot 是集群的"保险"，Time Travel 是数据的"时光机"。保险用于灾难，时光机用于日常。**

### 8.2 实施建议

1. **不要将 Time Travel 定位为 Cluster Snapshot 的替代品**——两者解决不同层级的问题
2. **在文档中明确区分**——灾难恢复指向 Cluster Snapshot，数据恢复指向 Time Travel
3. **技术实现上复用基础设施**——利用已有的 Vacuum retainVersions 机制，避免重复造轮子
4. **用 Time Travel 的 UNDROP 替代存算分离模式下的 RECOVER**——更强大、更灵活
5. **存储成本透明化**——Time Travel 会增加存储成本，需要提供 `SHOW TIME TRAVEL STORAGE` 让用户掌握成本

### 8.3 用户决策指南（可用于文档）

```
我遇到了数据问题，应该用什么？

1. "我想看看昨天的数据长什么样"
   → Time Travel: SELECT ... FOR TIMESTAMP AS OF ...

2. "我不小心删了/改了一些数据"
   → Time Travel: ALTER TABLE ... RESTORE TO TIMESTAMP ...

3. "我不小心 DROP 了一张表"
   → Time Travel: UNDROP TABLE ...

4. "整个集群挂了 / 元数据损坏了"
   → Cluster Snapshot: 按灾难恢复流程重建集群

5. "我想把集群迁移到另一个区域"
   → Cluster Snapshot: 从快照在新区域重建集群
```
