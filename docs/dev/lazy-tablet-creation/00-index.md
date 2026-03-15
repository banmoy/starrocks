# Lazy Tablet Creation (去掉存算分离 tablet 创建时的 CN 交互) — 设计参考文档索引

本目录包含所有与"去掉存算分离 tablet 创建时的 CN 交互"相关的分析文档，供方案设计 agent 参考。

## 文档列表

| 文档 | 内容 | 核心信息 |
|------|------|---------|
| [01-current-tablet-creation-paths.md](01-current-tablet-creation-paths.md) | 当前所有 tablet 创建路径 | 19 条创建路径，涵盖 FE + BE 两侧 |
| [02-cn-interaction-details.md](02-cn-interaction-details.md) | CN 在 tablet 创建时做了什么 | TabletMetadataPB 构建、schema file 创建、对象存储写入、缓存 |
| [03-benefit-analysis.md](03-benefit-analysis.md) | 去掉 CN 交互的收益分析 | 性能、稳定性、可用性、易用性、资源效率、可扩展性 |
| [04-affected-code-paths.md](04-affected-code-paths.md) | 受影响的所有代码路径 | FE 发送方(3条)、BE 执行方(1条)、BE 下游消费者(13条)、FE 完成依赖(6类)、配置/监控、升降级兼容性、跨集群同步、Cluster Snapshot |

## 背景

存算分离模式下，tablet 创建的当前流程：

```
FE (Java)                        CN (C++)                        Object Storage
    │                                │                                │
    ├─ StarOSAgent.createShardGroup()│                                │
    ├─ StarOSAgent.createShards()   │                                │
    ├─ new LakeTablet(shardId)      │                                │
    ├─ buildPartitions()            │                                │
    │   └─ CreateReplicaTask ──────►│                                │
    │      (Thrift RPC)             ├─ build TabletMetadataPB        │
    │                               ├─ create_schema_file() ───────►│ SCHEMA_{id}
    │                               ├─ put_tablet_metadata() ──────►│ {tablet_id}_1.meta
    │                               ├─ cache metadata locally        │
    │◄──── TFinishTaskRequest ──────┤                                │
    ├─ countDown latch              │                                │
    ├─ logCreateTable / etc.        │                                │
    │                                │                                │
```

优化目标：去掉中间与 CN 的交互，使 DDL 操作不再依赖 CN。

## 核心挑战

去掉 CN 交互后，对象存储上不再有 version 1 的 `TabletMetadataPB` 文件。
所有依赖该文件存在的下游操作都需要适配。详见 [04-affected-code-paths.md](04-affected-code-paths.md) 的 Category 3。

## 当前优化现状

`lake_enable_tablet_creation_optimization` (默认 OFF) 已经将同一 partition/index 下的
tablet 创建合并为 1 次 CN RPC（而非每个 tablet 一次），但仍然需要 CN 交互。
