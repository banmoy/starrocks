# 去掉 CN 交互的收益分析

## 一、性能收益

### 1.1 DDL 延迟大幅降低

当前路径：`FE → (Thrift RPC) → CN → (protobuf 序列化 + 写入对象存储) → CN → (回包) → FE`

每一步开销：Thrift 序列化/反序列化、网络传输、CN 排队、对象存储写入。tablet 数量越多，总延迟越大。

| 场景 | 典型规模 | 当前延迟级别 |
|------|---------|------------|
| 建表（3 年日分区 × 128 bucket） | ~140,000 tablets | 分钟级 |
| ADD PARTITION（单分区 128 bucket） | 128 tablets | 秒级 |
| Schema Change 启动（1000 分区） | ~128,000 shadow tablets | 分钟级 PENDING |
| TRUNCATE TABLE（1000 分区） | ~128,000 tablets | 分钟级 |

即使开启 `tablet_creation_optimization`，1000 分区 × 3 index 仍需 3000 次 CN RPC。

### 1.2 实时数据摄入延迟更平滑

自动分区时 INSERT/Stream Load 在遇到新分区时阻塞等待分区创建完成。

对实时数仓场景（电商大促、风控、IoT）的影响：
- 分区边界时刻（如每天零点）大量任务同时触发新分区创建
- 写入延迟突然飙升（毛刺）
- Stream Load 超时失败
- Flink/Spark 写入任务背压

去掉 CN 交互后，自动分区创建延迟降低一个数量级。

### 1.3 DDL 并发吞吐量提升

多个 DDL 同时执行时 CN 成为共享瓶颈。多租户环境下不同用户的 DDL 相互竞争。

### 1.4 Schema Change / Rollup 启动更快

PENDING 阶段（创建 shadow tablets）可能耗时数分钟，用户感知 ALTER"卡住了"。

---

## 二、稳定性收益

### 2.1 消除 CN 故障对 DDL 的影响

| CN 故障类型 | 对 DDL 的影响（当前） | 去掉后 |
|------------|---------------------|--------|
| CN OOM / Crash | DDL 任务失败 | 无影响 |
| CN 假死 / GC 暂停 | CreateReplicaTask 超时 | 无影响 |
| CN → 对象存储网络抖动 | metadata 写入失败 | 无影响 |
| CN 选择错误（即将下线的 CN） | 任务刚发出 CN 就停了 | 无影响 |

### 2.2 消除超时问题

用户经常遇到的问题：
- 建大表超时：`max_create_table_timeout_second` 不够
- 加分区超时：CN 繁忙导致 task 排队
- 需要针对不同场景调不同的 timeout 参数

### 2.3 减少 FE-CN 状态不一致

`CreateReplicaTask` 通过 `AgentTaskQueue` 管理的边角情况：
- 任务发出但 CN 没收到
- CN 执行成功但回包丢失
- FE Leader 切换时 task queue 状态丢失

---

## 三、可用性收益

### 3.1 DDL 不再依赖 CN 可用性（最大的架构收益）

当前隐含约束：**要执行任何创建 tablet 的操作，必须有活跃的 CN。**

| 场景 | 当前行为 | 优化后 |
|------|---------|--------|
| 集群冷启动（CN 未就绪） | 建表失败 | 建表成功 |
| Serverless（CN 缩容到 0） | 必须先启动 CN 才能建表 | 直接建表 |
| Warehouse 隔离（某 warehouse 的 CN 全下线） | 该 warehouse 无法建表 | 可以建表 |

### 3.2 滚动升级期间 DDL 不中断

CN 滚动升级时，FE 选中的 CN 可能正在重启。当前 DDL 可能失败。

### 3.3 灾备恢复更快

恢复时 CN 可能还没就绪。当前恢复依赖 CN。

---

## 四、易用性收益

### 4.1 不再需要调 timeout 参数

可废弃的参数：
- `tablet_create_timeout_second`（默认 10s）
- `max_create_table_timeout_second`（默认 600s）
- `create_table_max_serial_replicas`（默认 128）

### 4.2 DDL 错误信息更清晰

不再出现 "no alive compute nodes"、"Create Replica Task timeout" 等让用户困惑的错误。

### 4.3 DDL 行为更可预测

延迟不再取决于 CN 负载、CN 数量、网络状况。

---

## 五、资源效率收益

### 5.1 CN 资源释放给查询

tablet 创建消耗的 CN 资源（CPU、内存、网络、线程池）全部释放。

### 5.2 减少不必要的对象存储写入

很多 version 1 metadata 是"空的"，后续第一次写入数据时又会写新版本。
可以做 lazy initialization，等到真正有数据写入时再创建。

---

## 六、可扩展性收益

### 6.1 支持更大规模的表

当前建表延迟 ∝ tablet 数量。10 万分区 × 128 bucket = 1280 万 tablets → 不可接受的等待时间。

### 6.2 更细粒度的分区策略

tablet 创建足够轻量后，用户可以大胆使用按小时分区（而非按天），不用担心分区创建开销。

---

## 七、架构清晰度收益

### 7.1 关注点分离

CN 不再承担控制面职责（tablet metadata 创建），变成纯粹的无状态计算节点。

### 7.2 简化代码路径

当前链条：
```
LocalMetastore → TabletTaskExecutor → CreateReplicaTask → AgentBatchTask
→ AgentTaskQueue → Thrift RPC → AgentServer → run_create_tablet_task
→ lake::TabletManager::create_tablet → put_tablet_metadata
```
可大幅缩短。

---

## 八、收益汇总

| 用户场景 | 影响维度 | 收益程度 |
|---------|---------|---------|
| 大表建表（1000+ 分区） | 性能 | **极高** |
| 实时摄入 + 自动分区 | 性能 + 稳定性 | **极高** |
| INSERT OVERWRITE / ETL | 性能 | **高** |
| Schema Change / ADD ROLLUP | 性能 | **高** |
| TRUNCATE TABLE | 性能 | **中高** |
| Dynamic Partition | 稳定性 | **高** |
| MV 创建和刷新 | 性能 | **中** |
| Tablet Split / Merge | 性能 | **中** |
| Serverless / 弹性 Warehouse | 可用性 | **极高** |
| 滚动升级 | 可用性 | **高** |
| 灾备恢复 | 可用性 + 性能 | **极高** |
| 多租户并发 DDL | 性能 + 稳定性 | **高** |
| 新集群初始化 | 可用性 | **中高** |
| 用户运维体验 | 易用性 | **中** |

---

## 一句话总结

核心收益是**将 DDL 的可用性和性能从"受限于最弱的 CN"提升到"仅受限于 FE 和 metadata 服务"**。
这不仅是性能优化，更是存算分离架构完整性的补全——既然存算分离的核心理念是计算节点无状态、
可弹性伸缩，那么 DDL 操作就不应该依赖计算节点的存在。
