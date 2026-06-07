# 存算分离 Runtime Filter 机制

> 本文以**存算分离(shared-data)集群 + 云原生内表**为唯一语境,端到端剖析 StarRocks 的 runtime filter 机制:FE 优化器如何决定生成与下推,BE 如何构建、合并、跨节点传输,以及云原生内表 scan 路径(`LakeDataSource`)如何在算子层与存储层消费这些过滤器。
>
> 覆盖三类 runtime filter:**Join RF**(bloom/in/min-max,local 与 global 两种作用域)、**TopN RF**、**Agg RF**。不讨论外表 scan,不做存算一体对比。
>
> 所有结论以当前代码为准;文中给出的默认值均核对自 `SessionVariable.java` 与 `be/src/common/config.h` 的字段初始值(而非注释或文档)。代码锚点以 `ClassName::method()` 为主,行号仅作辅助提示,可能随版本漂移。

---

## 0. 问题与方案总览

### 0.1 要解决的问题

考虑一个典型的分析查询:大事实表与小维表做等值连接,维表上还带过滤条件:

```sql
SELECT o.o_orderkey, o.o_totalprice
FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey
WHERE c.c_mktsegment = 'BUILDING' AND c.c_nationkey = 5;
```

静态优化(分区裁剪、谓词下推)只能利用查询里**写出来的**谓词。`orders` 上没有任何显式过滤条件,执行引擎只能把 1.5 亿行全部读出来、传给 join 算子,再由 join 用哈希表把绝大多数行丢掉。如果维表过滤后只剩 800 个客户,这意味着超过 99.9% 的扫描和传输是无用功。

问题的本质是:**join 的右侧(build 侧)数据集合本身就是一个针对左侧(probe 侧)的强谓词,但它只在运行期才知道。**

同样的模式也出现在:

- `ORDER BY k LIMIT n`:堆里第 n 名的值是一个动态下界/上界,排在它之外的行根本不必进入排序算子;
- `GROUP BY k LIMIT n`(无 HAVING):聚合一旦收集满 n 个分组,后续输入中分组键不在这 n 个值里的行都可以丢弃。

### 0.2 解法:运行期生成谓词,回灌给 scan

StarRocks 的做法是在运行期把这些"迟到的谓词"物化成紧凑的数据结构(哈希集合、bloom filter、min/max 区间),从产生它的算子(join build / sort / agg)**逆着数据流方向**送回 probe 侧的 scan,让 scan 在尽可能早的位置把无关数据过滤掉——最理想的情况是在存储层连页都不解压,直接用 zone map 跳过整段数据。

按"谓词需要走多远"分为两种作用域:

- **Local RF**:build 算子和 probe scan 在同一个 fragment instance 内(典型:broadcast join、colocate join)。过滤器在进程内通过指针传递,零序列化开销。
- **Global RF(GRF)**:build 与 probe 隔着 exchange,分布在不同节点(典型:shuffle join)。每个 build instance 只见到数据的一个分片,必须把各自的**部分过滤器(partial RF)**通过 RPC 汇聚到一个**合并节点(merge node)**拼成完整过滤器,再广播给所有 probe 节点。

Runtime filter 是纯粹的性能优化:它只多过滤、不改语义,**丢失或迟到只影响速度,不影响正确性**。这一性质贯穿整个设计——所有等待都有超时,所有传输都允许失败。

### 0.3 存算分离语境下的位置

存算分离集群中,云原生内表的 scan 在 BE/CN 上走 connector 框架:FE 仍按内表规划 scan 节点,BE 侧由 lake 专属的数据源实现负责读取,数据经对象存储/本地缓存进入。Runtime filter 的生成、合并、传输部分与表类型无关;**消费端**(scan 应用过滤器的方式)由 lake 路径实现,在算子层与存储层形成多级防线——具体组件在 §1.2 定义、§5 展开。

由于数据从对象存储读取的代价远高于本地盘,**在存储层尽早裁剪(少读数据、少占缓存)在存算分离下收益更大**,这正是本文重点着墨消费端的原因。

---

## 1. 概念模型

### 1.1 组件总览

一条 Global RF 的完整旅程涉及以下角色(local RF 只走左半边):

```
FE (规划期)
┌────────────────────────────────────────────────────────────────────┐
│ PlanFragmentBuilder                                                 │
│   └─ JoinNode/SortNode/AggregationNode::buildRuntimeFilters()       │
│        └─ PlanNode::pushDownRuntimeFilters()  (沿计划树下推到 scan)  │
│ DefaultCoordinator::setGlobalRuntimeFilterParams()                  │
│   (选 merge node、统计 builder 数、装配 TRuntimeFilterParams)        │
└────────────────────────────────────────────────────────────────────┘
        │ thrift: TRuntimeFilterDescription / TRuntimeFilterParams
        ▼
BE (执行期)
┌──────────────── build 侧 CN ───────────────┐   ┌── merge node CN ──┐
│ HashJoinBuildOperator::set_finishing()      │   │ RuntimeFilterWorker│
│   ├─ HashJoiner::create_runtime_filters()   │   │  └ RuntimeFilter-  │
│   ├─ PartialRuntimeFilterMerger (driver 间) │──▶│     Merger         │
│   └─ RuntimeFilterPort::publish_*()         │RPC│  (等齐→拼接→广播)  │
│        ├─ 本地: RuntimeFilterRegistry        │   └─────────┬─────────┘
│        └─ 远程: RuntimeFilterWorker 事件队列 │             │ RPC(中继树)
└─────────────────────────────────────────────┘             ▼
┌──────────────────────── probe 侧 CN(scan 所在) ─────────────────────┐
│ RuntimeFilterWorker::_receive_total_runtime_filter()                 │
│   └─ RuntimeFilterPort::receive_shared_runtime_filter()              │
│        └─ RuntimeFilterRegistry::install_shared() → 唤醒 observer    │
│ (早到兜底: RuntimeFilterCache → OperatorFactory::acquire_runtime_filter)│
│                                                                      │
│ ConnectorScanOperator / ConnectorChunkSource / LakeDataSource        │
│   ├─ 算子级: RuntimeFilterProbeCollector::evaluate() (chunk 级)      │
│   └─ 存储级: ScanConjunctsManager → SegmentIterator                  │
│        ├─ RuntimeFilterPredicates (行级,采样自适应)                  │
│        └─ RuntimeScanRangePruner (zone-map 动态剪枝)                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.2 术语表

正文里每个术语首次使用前先在这里定义。注意区分三对容易混淆的概念(build/probe 描述符、两种 in-filter、两个"合并器")。

| 术语 | 定义 | 代码锚点 |
|---|---|---|
| **RF** | runtime filter 的统称 | — |
| **build 侧 / probe 侧** | 产生 RF 的算子一侧(join 右子树、sort、agg)/ 消费 RF 的一侧(scan 等) | — |
| **`RuntimeFilterDescription`(FE)** | FE 侧一条 RF 的完整描述:build 表达式、各 probe 节点的 probe 表达式、join 分布模式、merge 节点地址等;`toThrift()` 产出 `TRuntimeFilterDescription` | `fe/.../planner/RuntimeFilterDescription.java` |
| **`RuntimeFilterBuildDescriptor`(BE)** | BE 侧 build 端描述符:持有 build 表达式、layout、最终 `RuntimeFilter*` 对象 | `be/src/exec/runtime_filter/runtime_filter_descriptor.h` |
| **`RuntimeFilterProbeDescriptor`(BE)** | BE 侧 probe 端描述符:持有 probe 表达式与一个原子指针 `_runtime_filter`,RF 到达即被写入 | `be/src/exec/runtime_filter/runtime_filter_probe.h` |
| **`RuntimeFilter`(BE 类)** | 过滤器对象的抽象基类;join RF 的实体是 `ComposedRuntimeFilter`(§2.1) | `be/src/runtime/runtime_filter.h` |
| **membership filter** | 做"成员资格测试"的部分:`TRuntimeBloomFilter` / `RuntimeBitsetFilter` / `RuntimeEmptyFilter` 之一 | 同上 |
| **min/max filter** | `MinMaxRuntimeFilter`:维护 build 数据的最小/最大值区间 | 同上 |
| **join local in-filter** | join build 侧行数很小时额外生成的 **IN 常量谓词**(`ExprContext`,非 `RuntimeFilter` 子类),仅本地使用,可被 scan 规范化下推到存储 | `HashJoiner::_create_runtime_in_filters()` |
| **`InRuntimeFilter`** | **Agg RF 专用**的 `RuntimeFilter` 子类:哈希集合,可序列化、可跨节点合并,序列化类型 `IN_FILTER`。与上一行的 join local in-filter 是两个东西 | `be/src/runtime/runtime_in_filter.h` |
| **local RF / global RF (GRF)** | 见 §0.2;FE 判定方式见 §3.6 | — |
| **partial RF** | 一个 build fragment instance 产出的部分过滤器,等待与其他 instance 的合并 | — |
| **merge node(GRF coordinator)** | 汇聚所有 partial RF 并广播 total RF 的 CN;由 FE 选定 | `DefaultCoordinator::prepareResultSink()` |
| **total RF** | merge node 拼接完成、广播给所有 probe 节点的完整 GRF | `RuntimeFilterMerger::_send_total_runtime_filter()` |
| **layout** | 描述一条 RF 的分区拓扑(单体还是按 hash/bucket 切分、几层)的元数据;决定 probe 时"一行该查哪个分区的 bloom filter" | `RuntimeFilterLayout`(FE/BE 同名) |
| **`PartialRuntimeFilterMerger`** | **同一 instance 内**多个 build driver 之间的合并器(进程内)。与跨节点的 `RuntimeFilterMerger` 区分 | `be/src/exec/pipeline/runtime_filter_types.h` |
| **`RuntimeFilterMerger`** | **merge node 上跨 instance** 的合并器(RPC 汇聚) | `be/src/runtime/runtime_filter_worker.h` |
| **`RuntimeFilterPort`** | 每个 fragment instance 一个的收发端口:发布本实例产出的 RF、接收安装到达的 RF | `be/src/runtime/runtime_filter_worker.cpp` |
| **`RuntimeFilterWorker`** | 每 BE 进程一个的后台线程,事件队列驱动,负责所有 RF 的 RPC 收发与 merge | `be/src/runtime/runtime_filter_worker.h` |
| **`RuntimeFilterRegistry`** | 每 fragment instance 一个:filter_id → probe 描述符列表;RF 到达时逐个安装 | `be/src/exec/runtime_filter/runtime_filter_registry.h` |
| **`RuntimeFilterHub`** | 每 fragment instance 一个:build 节点 id → `RuntimeFilterHolder`;承载 **join local in-filter** 的传递与 driver 唤醒 | `be/src/exec/pipeline/runtime_filter_hub.h` |
| **`RuntimeFilterCache`** | 每 BE 进程一个:暂存"比 fragment 还早到达"的 total GRF,供算子 prepare 时领取 | `be/src/runtime/runtime_filter_cache.h` |
| **`RuntimeFilterProbeCollector`** | probe 描述符的集合,带选择性自适应的 chunk 级求值入口;由同一 plan node 的所有算子共享 | `be/src/exec/runtime_filter/runtime_filter_probe.h` |
| **`ScanConjunctsManager`** | scan 谓词管理器:把 conjuncts(含 RF)规范化为存储层谓词,划分可下推/不可下推 | `be/src/exec/olap_scan_prepare.h` |
| **`RuntimeFilterPredicates`** | 存储层(`SegmentIterator`)内对 RF 的行级求值容器,带 INIT/SAMPLE/NORMAL 采样状态机 | `be/src/storage/runtime_filter_predicate.h` |
| **`RuntimeScanRangePruner`** | 监视"开扫时还没到的 RF",到达后将其 min/max(或 IN 集合)转为列谓词,用 zone map 重新裁剪行范围 | `be/src/storage/runtime_range_pruner.h` |
| **stream-build filter** | 在执行过程中**持续收紧**的 RF(TopN RF 与 Agg RF):probe 侧不等它(`skip_wait`),靠版本号驱动增量应用 | `RuntimeFilterProbeDescriptor::init()` |
| **`rf_version`** | `RuntimeFilter` 内的单调版本号,min/max 每次收紧时自增;动态剪枝据此判断"值变了,值得重剪" | `RuntimeFilter::_update_version()` |

### 1.3 一条 RF 的关键标志位

下列标志决定一条 RF 走哪条路,后文反复引用:

| 标志 | 取值点 | 含义 |
|---|---|---|
| `has_remote_targets` | FE `ExchangeNode::pushCrossExchange()` 中,RF 成功穿过 exchange 时置 true | true ⇒ 这是 GRF,build 侧必须向 merge node 发送 partial |
| `only_local` | FE 生成时设定(TopN RF、agg TopN RF 恒为 true) | true ⇒ 禁止穿 exchange,永远是 local RF |
| `filter_type` | `JOIN_FILTER` / `TOPN_FILTER` / `AGG_FILTER`(thrift `TRuntimeFilterBuildType`) | 决定 build 算子与 probe 侧待遇 |
| `_is_stream_build_filter`(BE probe 描述符) | `filter_type ∈ {TOPN_FILTER, AGG_FILTER}` | true ⇒ `skip_wait`(driver 不等它)、不进 `RuntimeFilterPredicates`、靠 `rf_version` 增量重剪 |
| `has_push_down_to_storage`(BE probe 描述符) | `ScanConjunctsManager::get_runtime_filter_predicates()` 收编该 RF 时置位 | true ⇒ 算子级 chunk 求值跳过它,避免双重过滤(§5.6) |
| `always_true`(BE `RuntimeFilter`) | 各种降级路径(超限、合并失败) | true ⇒ 求值时直接放行 |

### 1.4 运行示例(贯穿全文)

**集群**:3 个 CN(C1/C2/C3),存算分离模式;所有 session 变量、BE config 取默认值;pipeline 引擎,`pipeline_dop = 4`。

**表**(均为云原生内表):

- `orders`:1.5 亿行,分桶列 `o_orderkey`;
- `customer`:150 万行。

**示例查询 Q1(Join RF 主线)**:即 §0.1 的查询。FE 统计估出 `customer` 过滤后基数 **800 行**,计划为 shuffle join(`PARTITIONED`,两侧都按 join 键重分布;`orders` 分桶列是 `o_orderkey`,做不了 colocate/bucket-shuffle):

```
F0 (gather):      ResultSink ← Exchange
F1 (3 instances): HashJoin (PARTITIONED)
                   ├─ probe: Exchange ← F2 (shuffle by o_custkey)
                   └─ build: Exchange ← F3 (shuffle by c_custkey)
F2 (3 instances): OlapScanNode(orders)        ← RF 的目的地
F3 (3 instances): OlapScanNode(customer) + 谓词
```

> 现实中 800 行的 build 侧大概率会被优化器选成 broadcast join;这里假定计划为 shuffle join,是为了让示例覆盖最完整的 GRF 合并链路。broadcast 直发路径见 §4.6;§5.9 另设变体 **Q1-b**(同一查询的 broadcast 计划:probe 侧 scan 与 join 同 fragment)用来走查 join local in-filter 通道。

**示例查询 Q2(TopN RF)**:

```sql
SELECT * FROM orders ORDER BY o_orderdate DESC LIMIT 100;
```

**示例查询 Q3(Agg RF)**:

```sql
SELECT o_custkey, sum(o_totalprice) FROM orders GROUP BY o_custkey LIMIT 100;
```

三个查询在默认配置下分别触发 Join RF(local in-filter + global bloom)、TopN RF、AGG_IN_FILTER,各 §末尾用它们做数值走查。

---
## 2. 过滤器本体:数据结构与算法

本节是与执行路径无关的"共享算法层":无论 RF 由谁产生、走 local 还是 global,过滤器对象本身的结构、尺寸公式、分区数学和序列化格式都相同。

### 2.1 类层次:一条 join RF = min/max + membership

`be/src/runtime/runtime_filter.h` 中的类层次:

```
RuntimeFilter                       (抽象基类: evaluate/merge/concat/intersect/serialize, _rf_version)
├─ MinMaxRuntimeFilter<LT>          区间 [_min, _max] + 开闭标志 + _has_null
├─ RuntimeMembershipFilter          成员测试抽象 (_global, _join_mode, _size)
│  ├─ TRuntimeBloomFilter<LT>       SimdBlockFilter _bf  + vector<SimdBlockFilter> _hash_partition_bf
│  ├─ RuntimeBitsetFilter<LT>       位图 (值域有界的整型/日期, 仅 BROADCAST, §2.4)
│  └─ RuntimeEmptyFilter<LT>        no-op 占位 (§2.4)
├─ ComposedRuntimeFilter<LT, MembershipFilter>   ← join RF 的实体
│     = MinMaxRuntimeFilter + 上面三种 membership 之一
└─ InRuntimeFilter<LT>              哈希集合 (Agg RF 专用, 序列化类型 IN_FILTER)
```

要点:

- **join RF 永远是组合体**。`ComposedRuntimeFilter::evaluate()` 先跑 min/max 区间过滤,再跑 membership 过滤(`runtime_filter.h:1869`)。这意味着即使 bloom 部分被降级丢弃(§4.5),min/max 部分依然有效——这是"超大 build 侧仍能做 zone-map 剪枝"的关键。
- `MinMaxRuntimeFilter::update_min_max()` 在区间实际收紧时调用 `_update_version()` 自增 `rf_version`;TopN/Agg RF 的增量生效依赖它(§5.5)。
- `InRuntimeFilter` 是独立分支:`get_in_filter()` 返回自身、`get_min_max_filter()` 返回 nullptr;底层是 `DoublyBufferedData<HashSet>`(读多写少优化),`build(Column*)` 一次性灌入分组键(`runtime_in_filter.h:99`)。

### 2.2 SimdBlockFilter:块式 bloom filter

`SimdBlockFilter` 是 membership 的核心实现(split block Bloom filter):

**结构**:filter 由 2^`_log_num_buckets` 个 **bucket** 组成,每个 bucket 是 `uint32_t[8]`(`BITS_SET_PER_BLOCK = 8`),即 32 字节、256 bit,恰好一条 AVX2 cache line。

**尺寸公式**(`SimdBlockFilter::init()`,`runtime_filter.cpp:26`):

```
nums            = max(1, 去重前的插入元素数)          // MINIMUM_ELEMENT_NUM = 1
log_heap_space  = ceil(log2(nums))
_log_num_buckets = max(1, log_heap_space - 5)        // LOG_BUCKET_BYTE_SIZE = 5
alloc_size      = 2^(_log_num_buckets + 5) 字节       // = bucket 数 × 32B
_directory_mask = 2^_log_num_buckets - 1
```

直观理解:**每个元素约预算 1 字节**,向上取整到 2 的幂,最小 64B。内存以 64 字节对齐分配(`posix_memalign`)。

**插入/查询**(`make_mask()`,`runtime_filter.h:207` AVX2 版):

1. bucket 选择:`bucket_idx = hash & _directory_mask`(取 hash 低位);
2. 块内置位:用 8 个固定盐值(`0x47b6137b, 0x44974d91, ...`)分别乘以 hash 再右移 27 位,得到 8 个 [0,32) 的 bit 位置,在 8 个 uint32 里各置 1 bit——等效 8 个独立哈希函数;
3. 查询时 8 bit 全中才算命中(`_mm256_testc_si256`)。

**合并**(`SimdBlockFilter::merge()`):要求两侧 `_log_num_buckets` 相等,逐 bucket 按位 OR。这就是为什么**同一条 RF 的所有 partial 必须用同一个预估行数初始化**——尺寸由"合并后的总行数"决定,而不是各 partial 自己的行数(§4.3)。

**序列化**(`serialize()`):`[_log_num_buckets][_directory_mask][data_size][directory 原始字节]`。

### 2.3 哈希函数的选取

不同用途使用不同哈希,这是正确性约束而非风格问题:

| 用途 | 哈希 | 原因 |
|---|---|---|
| bloom 置位/查询 | 列的 `crc32` 系列(经 `compute_hash`) | build/probe 两侧一致即可 |
| **shuffle 分区选择**(GLOBAL_SHUFFLE 布局) | `exchange_hash_function_version` 决定:0 → FNV,1 → XXH3(`runtime_filter.h:498`) | **必须与 shuffle exchange 用同一函数**,这样"某行会被 shuffle 到分区 i"与"该行去查分区 i 的 bloom filter"才相互对应 |
| bucket 分区选择(GLOBAL_BUCKET 布局) | `CRC32_HASH` | 与建表分桶函数一致 |

### 2.4 RuntimeBitsetFilter 与 RuntimeEmptyFilter

- **`RuntimeBitsetFilter`**:当 build 值域有界(整型/日期类)且为 BROADCAST join、序列化版本 ≥ V3、`enable_join_runtime_bitset_filter = true`(默认)时,用精确位图替代 bloom filter(`PartialRuntimeFilterMerger::merge_singleton_local_bloom_filters()`,`runtime_filter_types.cpp:132`)。零误判、cache 友好。
- **`RuntimeEmptyFilter`**:membership 的 **no-op 占位**——`evaluate()` 什么都不做、全部放行(`runtime_filter.h:1651`)。它出现在"bloom 部分被放弃,但 min/max 仍保留"的降级场景(§4.3/§4.5)。注意:不要被名字误导,它不是"过滤一切",而是"不过滤"。

### 2.5 layout:分区拓扑与查哪个分区

GRF 在 shuffle/bucket join 下不是一个大 bloom filter,而是**分区 bloom filter 的数组**(`TRuntimeBloomFilter::_hash_partition_bf`):build instance i 只见到 shuffle 分区 i 的数据,它产出的 partial 就是分区 i 的精确写照;merge node 把各 partial **拼接(concat)而非 OR** 成数组(`TRuntimeBloomFilter::concat()` 将对方的 `_bf` `emplace_back` 进 `_hash_partition_bf`)。probe 时对每行算一次 shuffle hash,选中分区后只查那一个小 filter——等价于把"先 shuffle 后 join"的过滤提前到了 scan,且误判率远低于同尺寸的单体 filter。

layout 元数据由 FE 计算(§3.7)、thrift 传给 BE(`TRuntimeFilterLayout`),BE 侧 `dispatch_layout()` 按模式静态分发(`runtime_filter_layout.h:100`)。分区索引数学(`WithModuloArg::HashValueCompute::process_shuffle/process_bucket`,`runtime_filter.h:478`):

| layout 模式 | 分区索引公式 | 适用 |
|---|---|---|
| `SINGLETON` | 恒 0(单体 filter) | broadcast/replicated;以及所有 local RF |
| `GLOBAL_SHUFFLE_1L` | `hash % real_num_partitions` | shuffle join 的 GRF(**默认路径**) |
| `GLOBAL_BUCKET_1L` | `bucketseq_to_instance[hash % bucket数]` | colocate/bucket-shuffle join 的 GRF |
| `PIPELINE_SHUFFLE` / `GLOBAL_SHUFFLE_2L` / `PIPELINE_BUCKET*` / `GLOBAL_BUCKET_2L*` | instance×driver 两级(含 `xorshift32` 二次散列) | 仅当 `enable_pipeline_level_multi_partitioned_rf = true(默认 false)`,本文按默认不展开 |

> 分区编号与 build instance 的对应关系:merge node 按 `build_be_number`(FE 为每个 fragment instance 分配的序号)升序拼接 partial(`RuntimeFilterMergerStatus::filters` 是有序 map),而 shuffle exchange 的目的分区编号同样按 instance 序号排列,两侧再使用同一哈希函数(§2.3),三者共同保证"行 → 分区 → 子 filter"的一致性。

### 2.6 序列化格式

`RuntimeFilterSerde`(`be/src/runtime/runtime_filter_serde.h`)当前版本 `RF_VERSION_V3 (0x4)`:

```
[1B 版本][1B RuntimeFilterSerializeType: EMPTY/BLOOM/BITSET/IN][payload]
payload(ComposedRuntimeFilter): [logical type][membership filter][min/max 区间]
```

V3 引入类型字节后,"只有 min/max 的降级 filter"才能表达为 `EMPTY_FILTER`;V3 之前只能 `clear_bf()`(§4.5 两个分支并存的原因)。

### 2.7 运行示例:Q1 的 bloom filter 有多大

Q1 中 `customer` 过滤后 800 行,3 个 build instance 各分到约 267 行(shuffle 均匀假设):

- 每个 partial(instance 内合并后,SINGLETON local layout):`nums=267 → ceil(log2 267)=9 → _log_num_buckets=max(1,9-5)=4` → **16 bucket × 32B = 512B**;
- merge node 拼接后的 total GRF:3 个 512B 分区 filter 的数组,约 1.5KB + min/max + 头部——通过 RPC 广播绰绰有余(对比 §6.4 的 64MB HTTP 阈值)。

---
## 3. FE 决策层:生成、下推与装配

### 3.1 入口:物理计划翻译期

RF 不在 optimizer 的 memo 阶段决策,而是在 `PlanFragmentBuilder` 把物理算子翻译成 `PlanNode` 树时逐节点构建:

- `visitPhysicalHashAggregate` → `AggregationNode::buildRuntimeFilters()`(`PlanFragmentBuilder.java:2732`)
- `visitPhysicalTopN`(partial 段)→ `SortNode::buildRuntimeFilters()`(`:3052`)
- `visitPhysicalJoin` → `JoinNode::buildRuntimeFilters()`(`:3172`)

统一闸门 `shouldBuildGlobalRuntimeFilter()`(`:3072`):`enable_global_runtime_filter || enable_pipeline_engine`。pipeline 引擎下恒为 true——即使用户关掉 `enable_global_runtime_filter`,GRF 也会先规划出来(BE 的 `local_rf_waiting_set` 依赖它),只是在下发前被清理(代码注释 `:3067-3071`)。

每个 `PlanNode` 维护两个列表:`buildRuntimeFilters`(本节点产出的 RF)与 `probeRuntimeFilters`(下推到本节点、由本节点消费的 RF),最终随 `TPlanNode.build_runtime_filters / probe_runtime_filters` 下发。

### 3.2 Join RF 的生成条件

`JoinNode::buildRuntimeFilters()`(`JoinNode.java:156`)逐条等值连接条件(`eqJoinConjuncts`)生成一条 RF,前置条件全部满足才会进入:

1. **join 类型**:inner / left semi / right join / cross join(`:161`)。其余(left outer、anti 等)probe 侧不能被 build 集合过滤,直接返回;
2. **skew join 限制**:skew join 且倾斜侧是 build 侧时不生成(`:167`,GRF 可能不含倾斜值);
3. **build 基数上限**(仅 `PARTITIONED` / `SHUFFLE_HASH_BUCKET`):`0 < inner.cardinality ≤ global_runtime_filter_build_max_size(默认 64M)`,基数未知(≤0)也放弃(`:179-183`)——shuffle join 的 GRF 尺寸不可控时宁可不建。broadcast/colocate 无此项(它们可以只做 local);
4. 对每条等值条件:规范化出 build 表达式(绑定 build 子树)与 probe 表达式,然后**尝试向 probe 子树下推**——`getChild(0).pushDownRuntimeFilters(...)` 返回 true(有人接收)才把 RF 留在 `buildRuntimeFilters` 里(`:232`)。没有消费者的 RF 不会存在。

`RuntimeFilterDescription` 同时记录 `joinMode = distrMode`(BROADCAST/PARTITIONED/COLOCATE/LOCAL_HASH_BUCKET/SHUFFLE_HASH_BUCKET/REPLICATED)、`equalForNull`(`<=>` 条件)、`buildCardinality` 等。

另外,FE 在 `setJoinPushDown()`(`PlanFragmentBuilder.java:3059`)为 join 节点置 `is_push_down` 标志(`hash_join_push_down_right_table=true(默认)` 且 inner/left-semi/right join)——它不属于某条 RF,而是 BE 端 **join local in-filter** 的开关(§4.2)。

### 3.3 TopN RF 的生成条件

`SortNode::buildRuntimeFilters()`(`SortNode.java:177`):

```java
if ((perPipeline && topn_push_down_agg_mode >= 1) || limit < 0
        || !enable_topn_runtime_filter || orderingExprs.isEmpty()) return;
```

- `enable_topn_runtime_filter` 默认 true;`limit > 0` 必须;
- `perPipeline` 仅当 TopN 被 `PushDownTopNToPreAggRule` 改写为"预聚合式 TopN"时为 true——此时 RF 改由下方的 AggregationNode 出(§3.4),SortNode 让位。**普通 `ORDER BY ... LIMIT` 的 partial SortNode 默认会生成 TopN RF**;
- 只取**第一个排序列**做 build/probe 表达式;
- 固定属性:`type=TOPN_FILTER`、`joinMode=BROADCAST`、**`onlyLocal=true`**、`topn = offset<0 ? limit : offset+limit`、携带 `SortInfo`(升降序、nulls first);
- 同样要 `child.pushDownRuntimeFilters()` 成功才保留。

`onlyLocal=true` 意味着 TopN RF 永远只在 partial sort 所在 fragment instance 内生效——恰好就是数据所在的 scan instance,无需任何网络传输。

probe 侧接收时(`OlapScanNode::pushDownRuntimeFilters()`,`OlapScanNode.java:1798`)有两个附加动作:

- `setOrderHint(isAscFilter())`:提示存储层按排序方向读 tablet,让 RF 更快收紧;
- 背压配置:`topn_filter_back_pressure_mode`(默认 **0=关闭**;1=probe 基数 >5000 万自适应开;2=强制开)满足时,把 `back_pressure_max_rounds(默认 3)`、`back_pressure_throttle_time_upper_bound(默认 300ms)`、`backPressureNumRows = 10 × topN` 写进 `TLakeScanNode`(`:1176-1180`,§6.3)。

`canAcceptFilter()` 对 TopN/Agg RF 只允许 scan 节点接收,且要求 `ScanNode::supportTopNRuntimeFilter()`——`OlapScanNode` 返回 true(云原生内表同样走 `OlapScanNode`,`OlapScanNode.java:1731`),基类默认 false。

### 3.4 Agg RF 的生成条件

`AggregationNode::buildRuntimeFilters()`(`AggregationNode.java:584`)可产出两种:

**AGG_IN_FILTER**(`:588`):条件 `limit > 0 && limit < agg_in_filter_limit(默认 1024) && 有聚合函数 && 有 group by`。取**第一个分组表达式**,`joinMode=PARTITIONED`、`equalCount=1`,**不设 onlyLocal**——它可以跨 exchange 成为 GRF(BE 端实体是可序列化的 `InRuntimeFilter`)。语义:`GROUP BY k LIMIT n` 收满 n 个分组后,k 不在集合里的行对结果无贡献。

> 注意 FE 条件只看 plan 形态;真正"收满 n 个分组才建"由 BE 把关(§4.8)。HAVING 会让 BE 端条件不成立(见 `AggregateBlockingSinkOperator::prepare_local_state()` 的 `conjunct_ctxs().empty()`)。

**TOPN_FILTER(agg 变体)**(`:594`):条件 `enable_topn_runtime_filter && topNSortInfo != null` 且第一排序列能按 SlotId 匹配到某个分组列(`getGroupByExprOrder()`,`:611`)。`topNSortInfo` 由 `PushDownTopNToPreAggRule` 写入——即 `SELECT k, agg() ... GROUP BY k ORDER BY k LIMIT n` 形态。属性与 SortNode 版相同(BROADCAST + onlyLocal)。

### 3.5 下推算法:谁来消费这条 RF

`PlanNode::pushDownRuntimeFilters(context, probeExpr, partitionByExprs)`(`PlanNode.java:746`)是一个"深度优先、最深者优先"的递归:

1. `canPushDownRuntimeFilter()`:MultiCast fragment 内禁止(`:686`,无法保证对所有消费者生效);
2. 尝试**继续向子节点下推**(跨 project/agg/window 等会做 probe 表达式的候选改写,`candidatesOfSlotExpr`);任一子节点接收即成功返回——RF 尽量贴近叶子;
3. 子节点都不收时,若 probeExpr 被本节点物化(`couldBound()`)且 `canProbeUse()` 通过,则本节点自己消费:登记 `nodeIdToProbeExpr[本节点 id] = probeExpr` 并加入 `probeRuntimeFilters`。

**`canProbeUse()`(收益判定,`RuntimeFilterDescription.java:233`)**,local RF 一律收;GRF 依次检查:

```
buildMin = global_runtime_filter_build_min_size (默认 128K;colocate/bucket 类 join 再乘存活 BE 数)
probeMin = global_runtime_filter_probe_min_size (默认 100K)

probeMin == 0                          → 收(强制)
buildMin > 0 && buildCard ≤ buildMin   → 收(build 够小,稳赚)
probeCard < probeMin                   → 拒(probe 太小,不值得等)
buildCard / probeCard ≤ 1 - global_runtime_filter_probe_min_selectivity(默认 0.5)
                                       → 收(预估能滤掉超过一半)否则拒
```

**跨 exchange(`ExchangeNode::pushDownRuntimeFilters()`,`ExchangeNode.java:264`)**:

- 先问 `canPushAcrossExchangeNode()`(`RuntimeFilterDescription.java:449`):`onlyLocal` 或 skew-broadcast RF 直接 false;
- 再问 `canCrossExchangeNode()`(`ExchangeNode.java:322`):broadcast join 或单等值条件恒可;多列等值条件下需要 probe 列与 shuffle partition 列对得上(多列场景由 `enable_multicolumn_global_runtime_filter`(v2,默认 true)放行);
- 穿越期间 `enterExchangeNode()/exitExchangeNode()` 维护 `crossExchangeNodeTimes`,子树有人接收则 `setHasRemoteTargets(true)`(`:314`)——这就是 local/global 的判定本体;
- exchange 节点自身也可消费 RF:子树没人收时兜底,或 `runtime_filter_on_exchange_node = true(默认 false)` 时主动在 fragment 边界加一道(防 fragment 部署比 GRF 还慢的场景,注释 `:274-283`)。

### 3.6 local 还是 global:小结

| 判定 | 依据 |
|---|---|
| RF 是 local | 下推过程从未穿过 exchange(`crossExchangeNodeTimes == 0`,`inLocalFragmentInstance()`) |
| RF 是 global | `hasRemoteTargets == true`(至少一个 probe 在 exchange 之下) |
| 永远 local | `onlyLocal = true`(TopN RF、agg TopN RF) |
| local 也能用的 joinMode | BROADCAST/COLOCATE/LOCAL_HASH_BUCKET/SHUFFLE_HASH_BUCKET/REPLICATED(`isLocalApplicable()`);PARTITIONED 的 probe 必然隔着 exchange |

同一条 RF 可以同时有 local 与 remote 消费者(例:broadcast join 下方 scan 收 local,exchange 之外另一处也收)。

FE 还为每个 `PlanNode` 计算 `local_rf_waiting_set`(`PlanNode::fillLocalRfWaitingSet()`,`PlanNode.java:191`):本节点 probe 列表中 build 节点位于**同一 fragment** 的那些 build node id 集合,随 `TPlanNode.local_rf_waiting_set` 下发——BE 的 pipeline driver 用它决定"启动前要等哪些本地 RF"(§4.9)。

### 3.7 layout 选择

`RuntimeFilterDescription::computeLocalLayout()/computeGlobalLayout()`(`RuntimeFilterDescription.java:523/542`),默认 `enable_pipeline_level_multi_partitioned_rf = false` 时:

- local layout 一律 `SINGLETON`(instance 内所有 driver 合并成一个 filter);
- global layout:BROADCAST/REPLICATED → `SINGLETON`;PARTITIONED/SHUFFLE_HASH_BUCKET → `GLOBAL_SHUFFLE_1L`;COLOCATE/LOCAL_HASH_BUCKET → `GLOBAL_BUCKET_1L`。

调度器在 `ExecutionFragment::setLayoutInfosForRuntimeFilters()`(`qe/scheduler/dag/ExecutionFragment.java:147`)把实例数、每实例 driver 数、bucket 序列映射等填进描述,随 `toLayout()` 进 thrift。

### 3.8 Coordinator 装配:merge node 与收发名册

`DefaultCoordinator::prepareResultSink()`(`DefaultCoordinator.java:646`):**root fragment 第 0 个 instance 所在节点**被选为 GRF merge node(取其 brpc 地址),随后 `setGlobalRuntimeFilterParams()`(`:822`)遍历所有 fragment 装配:

- **probe 名册**:每条 probe RF → 所有消费 instance 的 `(instance_id, brpc 地址)` 列表,汇入 `TRuntimeFilterParams.id_to_prober_params`;
- **builder 计数**:`runtime_filter_builder_number[filter_id]`——非 broadcast 为 build fragment 的 instance 数(merge node 要等齐这么多份 partial);broadcast 为 1;
- **broadcast GRF 直发**(pipeline 下):broadcast join 的 build 数据每个 instance 都全量,无需 merge——`pickupFInstancesOnDifferentHosts(instances, 3)`(`:789`)挑至多 3 个不同节点的 instance 作 `broadcast_grf_senders`(先到者胜),probe 名册按地址聚合成 `broadcast_grf_destinations` 直接发;
- **merge 地址回填**:`PlanFragment::setRuntimeFilterMergeNodeAddresses()`(`PlanFragment.java:799`)把 merge node 地址写进每个 fragment 内 Join/Agg build RF 的 `mergeNodes`(Agg 仅 `hasRemoteTargets` 时);
- `runtime_filter_max_size = global_runtime_filter_build_max_size`(merge node 端的总尺寸闸,§4.5)。

整套 `TRuntimeFilterParams` 只交给 root fragment 的 instance(`isRuntimeFilterCoordinator()`,`Deployer.java:247`)——merge node 与 result sink 同机。

### 3.9 thrift 产物速览

| 载体 | 关键字段 | 去向 |
|---|---|---|
| `TRuntimeFilterDescription` | filter_id、build_expr、plan_node_id_to_target_expr(probe 表达式按节点)、has_remote_targets、runtime_filter_merge_nodes、broadcast_grf_senders/destinations、build_join_mode、layout、filter_type、is_asc/is_nulls_first/limit(TopN) | `TPlanNode.build/probe_runtime_filters` |
| `TPlanNode.local_rf_waiting_set` | 本节点须等待的本地 build node id 集合 | 每个 plan node |
| `TRuntimeFilterParams` | id_to_prober_params、runtime_filter_builder_number、runtime_filter_max_size、skew_join_runtime_filters | 仅 root instance(merge node) |
| `TLakeScanNode` | enable_topn_filter_back_pressure、back_pressure_*(§3.3) | lake scan |
| `TQueryOptions` | runtime_filter_wait_timeout_ms、runtime_filter_scan_wait_time_ms、runtime_join_filter_pushdown_limit、global_runtime_filter_build_max_size、runtime_filter_send_timeout_ms、runtime_filter_rpc_http_min_size、enable_join_runtime_filter_pushdown 等 | 全体 |

### 3.10 运行示例:Q1 的 FE 决策走查

1. `JoinNode::buildRuntimeFilters()`:PARTITIONED,`card=800 ∈ (0, 67108864]` ✓ → 创建 `filter_id=0`,build 表达式 `c_custkey`,probe 表达式 `o_custkey`,`buildCardinality=800`;
2. 下推:HashJoin 的 probe 子节点是 ExchangeNode(F2 的上端)。`canPushAcrossExchangeNode()`:非 onlyLocal、PARTITIONED ✓;`canCrossExchangeNode()`:单等值条件 ✓ → `enterExchangeNode()` 进入 F2 子树,`OlapScanNode(orders)` 绑定 `o_custkey` 成功;
3. `canProbeUse()`(此时 `crossExchangeNodeTimes=1`,走 GRF 分支):`buildMin=131072 > 0 && 800 ≤ 131072` → 直接通过(连选择率都不用估)。穿越成功 → `setHasRemoteTargets(true)`;
4. layout:local `SINGLETON` + global `GLOBAL_SHUFFLE_1L`,`num_instances=3, num_drivers_per_instance=4`;
5. coordinator:merge node = F0 instance0 所在 CN(设为 C1);`runtime_filter_builder_number[0]=3`(F1 的 instance 数);probe 名册 = F2 的 3 个 instance;merge 地址写进 F1 中该 RF 的 `mergeNodes`。

Q2:partial SortNode(F1,与 orders scan 同 fragment)生成 `TOPN_FILTER, onlyLocal`,probe 落在 `OlapScanNode(orders)`,无 coordinator 参与。

Q3:二阶段聚合的 final AggregationNode 上,`limit=100 < 1024` ✓ → `AGG_IN_FILTER, PARTITIONED`,probe 表达式 `o_custkey` 下推穿过聚合下方的 shuffle exchange,落到 orders scan → `hasRemoteTargets=true`,coordinator 为它登记 merge 流程。

---
## 4. BE 执行层:构建、合并、传输与等待

### 4.1 fragment 准备期的装配

`FragmentExecutor::prepare()` 阶段(`be/src/exec/pipeline/fragment_executor.cpp`):

- 为每个 fragment instance 创建 `RuntimeFilterPort`(`:247`);
- 若请求携带 `runtime_filter_params`(只有 root instance 有),则本 instance 是 GRF coordinator:`RuntimeFilterWorker::open_query()` 在 worker 线程内为每个 filter_id 建 `RuntimeFilterMerger`(`:287-292`;`RuntimeFilterMerger::init()` 记录 `expect_number` 与 `max_size`);
- ExecNode 树构建时,`ExecNode::init_join_runtime_filters()`(`exec_node.cpp:142`)把 `TPlanNode.probe_runtime_filters` 实例化为 `RuntimeFilterProbeDescriptor` 并注册进 `RuntimeFilterRegistry`;collector 的两个超时取自 query options:`wait_timeout_ms = runtime_filter_wait_timeout_ms(默认 20)`、`scan_wait_timeout_ms = runtime_filter_scan_wait_time_ms(默认 20)`;
- 翻译成 pipeline 时,`init_runtime_filter_for_operator()` 把 ExecNode 的 probe collector(引用计数包装 `RefCountedRuntimeFilterProbeCollector`)与 `local_rf_waiting_set` 安到各 OperatorFactory 上;`RuntimeFilterHub` 为每个 build 节点 `add_holder()`;
- **早到兜底**:GRF 可能比 fragment 部署还快(merge node 广播时本机 fragment 尚未注册)。`OperatorFactory::acquire_runtime_filter()`(`operator_factory.cpp:123`)在算子工厂 prepare 时去 `RuntimeFilterCache` 领取已到的 total RF,`set_shared_runtime_filter()` 直接安装。

### 4.2 build 侧:哈希表建完的那一刻

`HashJoinBuildOperator::set_finishing()`(`hash_join_build_operator.cpp:87`)在本 driver 哈希表 build 完成后执行:

```
build_ht() → create_runtime_filters() → (colocate ? 直接发布 : 交给 PartialRuntimeFilterMerger)
```

`HashJoiner::create_runtime_filters()`(`hash_joiner.cpp:345`)做两件事:

**a) join local in-filter**(`_create_runtime_in_filters()`,`:571`)——前提 `_is_push_down`(FE 的 `is_push_down`,§3.2):

- probe/build 两端都是 exchange → 放弃(probe 不是 scan,无处下推);
- `ht_row_count > runtime_join_filter_pushdown_limit(默认 1024000)` → 放弃;
- `ht_row_count > max_pushdown_conditions_per_column(BE config,默认 1024;可被同名 session 变量覆盖)` → 放弃;
- 通过后,用 `VectorizedInConstPredicateBuilder` 把哈希表 key 列灌成 **IN 常量谓词**(`ExprContext`)。它不是 `RuntimeFilter` 对象、不序列化、只在本 fragment instance 内使用,但能像普通谓词一样被 scan 规范化、下推到存储层 zone-map(§5.2 通道①)。

**b) bloom filter 的构建参数**(`_create_runtime_bloom_filters()`,`:619`):对每条 build 描述符:

- 无消费者(`!has_consumer()`)跳过;**纯 local RF** 且 `ht_row_count > limit` 跳过;
- 不立即建 filter,而是把 key 列引用、eq_null、类型打包成 `RuntimeMembershipFilterBuildParam` 暂存——**真正的构建推迟到 driver 间合并时**,因为 bloom 尺寸取决于所有 driver 的总行数(§2.2)。

### 4.3 instance 内合并:PartialRuntimeFilterMerger

同一 instance 的多个 build driver(dop 个)各自调用 `add_partial_filters(driver_seq, ...)`,**最后完成的 driver** 触发合并(`_try_do_merge()`,原子计数 `_num_active_builders` 归零,`runtime_filter_types.cpp:290`)。两个 limit 来自 `HashJoinNode::decompose_to_pipeline()`(`hash_join_node.cpp:245-259`):

- `_local_rf_limit = runtime_join_filter_pushdown_limit × dop`(默认 1024000×dop);
- `_global_rf_limit = global_runtime_filter_build_max_size`(默认 67108864)。

**in-filter 合并**(`merge_local_in_filters()`,`:40`):剔除空哈希表的份额后,任一非空 driver 缺 in-filter(说明它在 §4.2 已超限放弃),或各 driver 行数的**最大值**(注意不是总和,`:61` 取 `std::max`)> `max_pushdown_conditions_per_column`,则整组作废;否则把各 driver 的 IN 值集合并进第 0 份。

**bloom 合并**(默认 SINGLETON local layout,`merge_singleton_local_bloom_filters()`,`:110`):

1. `row_count = Σ 各 driver ht 行数`;
2. 选型:BROADCAST + V3 + `enable_join_runtime_bitset_filter(默认 true)` + 行数在限内 + 单 driver → 尝试 `RuntimeBitsetFilter`;否则 `TRuntimeBloomFilter`;
3. **降级**:`has_remote_targets && row_count > _global_rf_limit` → membership 换成 `RuntimeEmptyFilter`(V3)——partial 还是要发(merge node 在等齐数),但只携带 min/max;
4. 纯 local 且 `row_count > _local_rf_limit` → 干脆不建(本地没人强依赖);
5. `init(row_count)` 定尺寸后,把**所有 driver** 的 key 列经 `RuntimeFilterBuilder::fill()` 灌入同一个 filter。

合并完成后回到 `set_finishing()`(`hash_join_build_operator.cpp:180`):

- **in-filter 列表**装进 `RuntimeFilterCollector`,经 `RuntimeFilterHub::set_collector(plan_node_id, ...)` 发布并唤醒等待的 driver(§4.9);
- **bloom 描述符列表**交 `RuntimeFilterPort::publish_runtime_filters()`。

colocate join(group execution)走旁路:per-driver 的 holder(`hub->set_collector(id, driver_seq, ...)`)+ `publish_local_colocate_filters()`,filter 按 driver 序列拼接(`set_or_concat()`),本文不展开。

### 4.4 发布:local 安装与 remote 发送

`RuntimeFilterPort::publish_runtime_filters()`(`runtime_filter_worker.cpp:140`)对每条 bloom 描述符分两步:

**本地安装**(无条件):`receive_runtime_filter(filter_id, filter)` → `RuntimeFilterRegistry::install_local()` → 本 instance 所有同 id 的 probe 描述符 `set_runtime_filter()`(原子写)→ 触发 `_ready_observers` 唤醒被阻塞的 driver。local RF 的旅程到此结束——**零序列化**。

**远程发送**(仅 `has_remote_targets`):

- broadcast join 的空 filter 不发(可能被空 probe 短路,注释 `:162`);
- 序列化(`RuntimeFilterSerde::serialize`,版本 V3),发送动作进 `RuntimeFilterWorker` 的事件队列(`SEND_PART_RF`),由 worker 线程异步 RPC——**执行线程从不阻塞在 RF 网络上**;
- RPC 超时 `runtime_filter_send_timeout_ms(session,默认 400ms)`(BE 兜底 `send_rpc_runtime_filter_timeout_ms=1000ms`);超过 `runtime_filter_rpc_http_min_size(默认 64MB)` 的 filter 改走 HTTP(§6.4)。

### 4.5 merge node:等齐、拼接、广播

worker 线程收到 `RECEIVE_PART_RF` 后调 `RuntimeFilterMerger::merge_runtime_filter()`(`runtime_filter_worker.cpp:391`):

1. 查无消费者/未知 filter_id → 丢弃;按 `build_be_number` 去重;
2. 反序列化,`filters[be_number] = rf`,`arrives.insert(be_number)`;
3. `filters.size() < expect_number` → 继续等(**没有超时**:partial 不齐就永远不发 total,probe 侧靠自己的等待超时止损);
4. 齐了 → `finalize_membership_filters()`(`:351`):任一 partial 的 bf 不可用,或 Σ membership **元素数** > `runtime_filter_max_size`(即 FE 的 `global_runtime_filter_build_max_size`,默认 64M;注意这里比较的是元素个数而非字节数,`RuntimeMembershipFilter::_size` 语义见 `runtime_filter.h:99`)→ 所有 partial 降级为 `RuntimeEmptyFilter`(只剩 min/max);
5. `_send_total_runtime_filter()`(`:580`):`out = create_empty(); out->set_global(); for each partial: out->concat(partial)`——对 `GLOBAL_SHUFFLE_1L` 即把 3 个 instance 的 bf 依 be_number 序拼成 `_hash_partition_bf` 数组(§2.5);AGG_IN_FILTER(`IN_FILTER` 类型)则做集合合并,无 membership 处理。

**total RF 的分发是一棵二分中继树**(`:712-756`):目标节点按地址聚合,本机排第一(local→local 不转发);每次 RPC 把"剩余目标的一半"作为 `forward_targets` 捎带给接收方,接收方安装后继续按同样规则二分转发(`_receive_total_runtime_filter()`,`:1000-1041`)。N 个节点 O(log N) 轮发完,merge node 不必串行发 N 次。

**接收端安装**(`receive_total_runtime_filter_pipeline()`,`:915`):

- 查 `QueryContext`/`FragmentContext`;**不在**(还没部署或已结束)→ 存入 `RuntimeFilterCache`(`put_if_absent`),将来 `acquire_runtime_filter()` 领取(§4.1);
- 在 → `RuntimeFilterPort::receive_shared_runtime_filter()` → `RuntimeFilterRegistry::install_shared()` → probe 描述符原子安装 + observer 唤醒。同机所有 instance 共享同一个反序列化产物(`shared_ptr`)。

### 4.6 broadcast GRF 直发(无 merge)

broadcast join 每个 build instance 都持有全量数据,partial 即 total。pipeline 下 FE 选好 `broadcast_grf_senders`(≤3 个不同节点,§3.8):

- filter ≤ `deliver_broadcast_rf_passthrough_bytes_limit(默认 128KB)`:**所有 sender 并行直发**全部 destinations(先到者胜,probe 端重复安装无害),in-flight 上限 `deliver_broadcast_rf_passthrough_inflight_num(默认 10)`;
- 大 filter:只有 instance id 最小的 sender 发,且 destinations 随机洗牌后接力转发(`_process_send_broadcast_runtime_filter_event()`,`:1044`),避免单点重复传大对象。

### 4.7 TopN RF 的构建与持续收紧

TopN RF 没有"建完"的时刻,它随堆的演化**单调收紧**:

- `PartitionSortSinkOperator::push_chunk()`(`partition_sort_sink_operator.cpp:71`)每收一个 chunk,先喂给 `ChunksSorterTopn::update()`,然后调 `_chunks_sorter->runtime_filters(pool)`;
- `ChunksSorterTopn::runtime_filters()`(`chunks_sorter_topn.cpp:157`):内部归并段就绪且行数 ≥ `offset+limit` 时,取**第 offset+limit 名的排序键值**构造/更新 `MinMaxRuntimeFilter`(升序 → 设上界;降序 → 设下界;RANK 型取闭区间)。值没变就不更新(`update_min_max` 只在收紧时 bump `rf_version`);
- `RuntimeFilterBuildDescriptor::set_or_intersect_filter()`(`runtime_filter_descriptor.h:67`):同 instance 多个 sort driver 各自有堆,**交集语义**——只能取各 driver 第 k 名中最保守的那个界;
- `publish_runtime_filters()`:`onlyLocal` ⇒ `has_remote_targets=false`,只走本地 `install_local`,**每次 push_chunk 都可能重复发布**,probe 描述符里的指针不变、指向的 filter 的 `rf_version` 递增——消费端的增量响应见 §5.5/§5.7。

### 4.8 Agg RF 的构建

**AGG_IN_FILTER**(`AggregateBlockingSinkOperator`):

- 资格:`_agg_group_by_with_limit = 有 group by && limit ≠ -1 && 无 having(conjuncts 空) && AggrPhase2`(`aggregate_blocking_sink_operator.cpp:43`);
- 时机:`push_chunk` 中共享 limit 倒数(`_shared_limit_countdown`)归零——即全 instance 已凑满 limit 个分组——调 `_build_in_runtime_filters()`(`:141`):每 driver 用 `AggInRuntimeFilterBuilder::build()` 从聚合哈希表抽分组键灌 `InRuntimeFilter`(`agg_runtime_filter_builder.cpp:70`);
- `AggInRuntimeFilterMerger::merge()`(`:85`)等齐 dop 份:任何一份缺失,或合并后元素总数 > `max_pushdown_conditions_per_column(1024)` → `_always_true`,放弃;否则集合并进第 0 份,`publish_runtime_filters()` 发布——`PARTITIONED` + hasRemoteTargets 时同样走 §4.5 的 merge/广播(`IN_FILTER` 序列化类型);
- 一次性构建(`_in_runtime_filter_built` 置位后不再更新)。

**agg TopN RF**(`AggregateStreamingSinkOperator::_build_topn_runtime_filter()`,`aggregate_streaming_sink_operator.cpp:103`):流式聚合每个 chunk 后,若哈希表分组数 ≥ limit 且较上次有增长,经 `AggTopNRuntimeFilterBuilder` 维护"分组键的第 limit 名"堆,更新 min/max filter 并 `set_or_intersect_filter` + 发布;后续增量更新走 `Aggregator::build_hash_map_with_selection_and_allocation()` 内的 `update()`(`aggregator.cpp:1719`)。onlyLocal,行为与 §4.7 同型。

### 4.9 probe 侧等待:driver 的三段前置闸

pipeline driver 启动前在 `PRECONDITION_BLOCK` 状态依次过三道闸(`PipelineDriver::is_precondition_block()`,`pipeline_driver.h:251`):

1. **依赖闸**(`dependencies_block`):算子声明的硬依赖;
2. **local RF 闸**(`local_rf_block`,`:215`):`_local_rf_holders`(由 FE 的 `local_rf_waiting_set` 经 `RuntimeFilterHub::gather_holders()` 而来)全部 ready 才放行。**没有超时**——build 与本 driver 同 instance,迟早会完成,而 in-filter 一旦错过 scan 启动就再也无法注入 conjuncts,等是值得的;
3. **global RF 闸**(`global_rf_block`,`:224`):对所有 `!skip_wait`(即非 stream-build)且尚未到达的 GRF 描述符,**从前两闸放行那一刻起**再等至多 `_global_rf_wait_timeout_ns`;超时放行,RF 迟到也还有存储层动态剪枝兜底(§5.5)。

超时取该 pipeline 中各算子的最大值(`pipeline_driver.cpp:264`):scan 算子给 `runtime_filter_scan_wait_time(默认 20ms)`(`ScanOperator::global_rf_wait_timeout_ns()`,`scan_operator.cpp:330`),其余算子给 `runtime_filter_wait_timeout_ms(默认 20ms)`(`operator.cpp:153`)。

唤醒是事件驱动的:probe 描述符与 holder 都挂 observer(`pipeline_driver.cpp:260/278`),RF 安装时 `notify`;另有 timer observer 保证超时准点触发(`:268`)。

> stream-build filter(TopN/Agg)`skip_wait=true`(`runtime_filter_probe.cpp:41`):它们到达即收紧、永不"完整",等待没有意义。

### 4.10 运行示例:Q1 在 BE 的旅程

C1/C2/C3 各跑一个 F1 instance(4 个 build driver)、一个 F2 instance(orders scan):

1. F2 的 scan driver 启动:local RF 闸为空;global RF 闸有 `filter_id=0` → 最多等 20ms;
2. F3 扫 `customer`(800 行)→ shuffle → F1 各 instance 约 267 行;4 个 build driver 建哈希表,`set_finishing` 依次触发,最后一个 driver 执行合并:
   - in-filter:F1 的 probe 子节点是 exchange、build 子节点也是 exchange → **`_is_push_down` 被置 false,不生成 in-filter**(`hash_joiner.cpp:354`);
   - bloom:`row_count=267 ≤ 4096000(local) 且 ≤ 67108864(global)` → `init(267)` → 512B,灌入 4 个 driver 的 key 列;
3. 各 instance `publish_runtime_filters()`:本地 `install_local`(本机 F1 内无人消费,probe 在 F2——同机的 F2 实例不在此 registry,等 total)+ `SEND_PART_RF` → C1(merge node);
4. C1 的 worker:`expect_number=3`,三份 512B partial 到齐 → Σ 元素数 = 800 ≪ 64M,不降级 → concat 成 3 分区 GRF → 中继树发往 C1(本机 first)/C2/C3;
5. 各 CN 安装到 F2 instance 的 probe 描述符;20ms 内到达则 scan 起跑即带 filter,否则按 §5.5 动态剪枝补救。

Q2(TopN):F1 内 partial sort 与 orders scan 同 fragment;sort sink 每个 chunk 后发布收紧的 min/max;scan 的 driver 对它不等待(`skip_wait`),边扫边应用。

Q3(AGG_IN_FILTER):final agg 各 instance 凑满 100 个分组后建 `InRuntimeFilter`,经 merge node 集合合并、广播到 orders scan 所在各 instance;到达时 scan 多半已在跑,主要靠动态剪枝通道生效(§5.5)。

---
## 5. 消费层:云原生内表 scan 如何应用 RF

这是存算分离语境下最值得细看的部分。lake scan 不是"在某一处"应用 RF,而是按"过滤位置越深、收益越大,但可用时机越受限"的原则布了**多级防线**。

### 5.1 路径骨架

```
FE OlapScanNode (thrift: TLakeScanNode)
  → BE ConnectorScanNode
     → pipeline: ConnectorScanOperator (每 driver 一个)
        → ConnectorChunkSource (每 morsel 一个; IO 线程执行)
           → LakeDataSource (connector/lake_connector.cpp)
              → ScanConjunctsManager (谓词规范化)
              → lake::TabletReader → SegmentIterator (storage/rowset/)
```

RF 的注入点:

- `ConnectorChunkSource` 构造时(`connector_scan_operator.cpp:639`):
  - `op->runtime_in_filters()`(join local in-filter,经 `OperatorFactory::bind_runtime_in_filters()` 从 `RuntimeFilterHub` 收集,`operator_factory.cpp:63`)**追加进 conjuncts**(`:649`);
  - `op->get_factory()->get_runtime_bloom_filters()`(probe collector)通过 `set_runtime_filters()` 交给 data source;
- `ConnectorChunkSource::prepare()` 调 `DataSource::parse_runtime_filters()`(`:680`);
- `LakeDataSource::open()`(`lake_connector.cpp:88`)建 `ScanConjunctsManager`(`opts.runtime_filters = _runtime_filters`)并 `parse_conjuncts()`;
- `LakeDataSource::init_reader_params()`(`:354`)把三样东西塞进 `TabletReaderParams`:`runtime_range_pruner`(`:372`)、`enable_join_runtime_filter_pushdown`(`:405`)、`runtime_filter_preds`(`:407`);
- `lake::TabletReader` 原样转交 `RowsetReadOptions`(`storage/lake/tablet_reader.cpp:345/361`)→ `SegmentIterator`。

> `ScanConjunctsManagerOptions::is_olap_scan` 默认 true 且 lake 路径未改写(`olap_scan_prepare.h:72`)——下面所有"存储层"机制对云原生内表**全量生效**。

### 5.2 六条注入通道总览

| # | 通道 | 适用 RF | 生效时机 | 过滤粒度 |
|---|---|---|---|---|
| ① | join local in-filter → conjuncts → 谓词规范化 | local in-filter(ExprContext) | scan 启动前(driver 等过 local RF 闸) | zone-map 页级 + 行级,与普通谓词无异 |
| ② | `DataSource::parse_runtime_filters()`:已到达 membership RF 的 min/max → conjunct | 已到达的 join RF | chunk source prepare 时 | 同上(随 conjuncts 进规范化) |
| ③ | scan open 时已到达的 RF → 静态 min/max 范围 | 已到达的 join RF | `parse_conjuncts()` | zone-map 页级 |
| ④ | `RuntimeFilterPredicates`:RF 整体(含 bloom)作存储层行级谓词 | join RF(含晚到的) | segment 迭代全程,采样自适应 | **行级,晚物化第一阶段** |
| ⑤ | `RuntimeScanRangePruner`:晚到/更新的 RF → zone-map 重剪 | 所有类型(TopN/Agg 主要靠它) | RF 到达或 `rf_version` 增长时 | 页级(scan range 收缩) |
| ⑥ | 算子级 `eval_runtime_bloom_filters()` | 未被④收编的 RF(TopN/Agg、多列分区 RF 等) | 每个 chunk 出 buffer 时 | chunk 级兜底 |

①②③是"赶上了开扫"的快路径,④⑤是"晚到也不浪费"的动态路径,⑥是兜底。各通道间靠两个标志避免重复劳动:`has_push_down_to_storage`(④收编后⑥跳过)与谓词去重交给 chunk 内 selection 合并。

### 5.3 通道①②③:把 RF 变成普通谓词

**①** in-filter 本来就是 `ExprContext`(IN 常量谓词),`ScanConjunctsManager::parse_conjuncts()` 的规范化器把它转成存储层 `ColumnPredicate`,既能查 zone map/bloom 索引跳页,也能在行级用 SIMD 求值——与用户手写 `o_custkey IN (...)` 完全等价。driver 的 local RF 闸(§4.9)保证 scan 启动时它一定已就位。

**②**(`data_source.cpp:35`)对每个**已到达**的 membership RF 用 `RuntimeFilterHelper::create_min_max_value_predicate()` 造一个 `slot ≥ min AND slot ≤ max` 表达式插到 conjuncts 头部。注意它发生在 `LakeDataSource::open()` 之前,所以同样会被 ③ 的规范化器吃进去变成 zone-map 谓词。

**③**(`ChunkPredicateBuilder` 规范化各 slot 时,`olap_scan_prepare.cpp:1029-1061`)对 probe 表达式命中本 slot 的 RF:

- **已到达**且非 in-filter 型:立即将其 min/max 构建为静态 `ColumnValueRange`(有 null 则包一层 `OR IS NULL`,`normalized_rf_with_null`);
- **未到达**(`rf == nullptr`),或是 `InRuntimeFilter`(Agg):登记进 `UnarrivedRuntimeFilterList`(`rt_ranger_params.add_unarrived_rf()`,`:1041`)交给通道⑤。Agg in-filter 即便已到也走⑤,因为它的"IN 集合 → 范围谓词"转换逻辑在 pruner 里(`build_in_range`)。

### 5.4 通道④:存储层行级 RF 谓词(采样自适应)

`ScanConjunctsManager::get_runtime_filter_predicates()`(`olap_scan_prepare.cpp:1647`)决定哪些 RF 能当"存储层行级谓词",条件全部满足才收编:

- 总开关 `enable_join_runtime_filter_pushdown`(session,默认 true);
- probe 表达式是裸 `SlotRef`(列上有函数就没法直接对列存数据求值);
- 非 stream-build(TopN/Agg 不进——它们会反复变化,而这里的谓词集在 reader 打开时固定);
- `partition-by exprs ≤ 1`(多列分区 RF 算不出分区索引);
- `PredicateParser::can_pushdown(slot)`(类型支持);
- 非全局低基数字典列(存储层此时是局部字典编码,直接比对会错)。

收编的 RF 包成 `RuntimeFilterPredicate(desc, column_id)` 集合,并给 probe 描述符打 `has_push_down_to_storage` 标记(⑥据此跳过)。同时,`parse_conjuncts` 阶段为这些列插入**占位谓词**(`new_column_placeholder_predicate`,`olap_scan_prepare.cpp:1463`)——它不过滤任何行,但让晚物化(late materialization)把 RF 列划进第一阶段读取,RF 才有机会在读其余列之前过滤。

**求值点**:`SegmentIterator` 谓词阶段,`_opts.enable_join_runtime_filter_pushdown && !_runtime_filter_preds.empty()` 时调 `RuntimeFilterPredicates::evaluate()`(`segment_iterator.cpp:2931`)。

**采样状态机**(`runtime_filter_predicate.cpp:190`,设计注释见 `runtime_filter_predicate.h:61-67`):

```
INIT:   对每个 RuntimeFilterPredicate 调 init(driver_seq) ——
        本质是看 probe 描述符里 RF 到没到;到的进入采样组,一个都没有则本轮放弃
SAMPLE: 每个候选 RF 都对输入行求值并记录各自滤掉的行数;
        累计采样 ≥ rf_sample_rows(BE config,默认 1024)行后:
        _update_selectivity_map(): 按过滤率排序,
            ≥ 0.95 → 只留这一个(够强,别的都是浪费)
            ≥ 0.5  → 最多留 3 个
            < 0.5  → 不留
NORMAL: 只执行被选中的 RF;
        处理满 rf_sample_rows × rf_sample_ratio(默认 32,即 32768)行后回到 INIT 重新采样
```

回到 INIT 重新采样有两个目的:数据分布漂移时重选,以及**晚到的 RF 在下一轮 INIT 自动纳入**(`RuntimeFilterPredicate::init()` 每轮重读原子指针)。行稀疏时切换 branchless 执行(`num_rows ≥ rf_branchless_ratio(默认 8) × 选中行数`)。低基数字典列有专门的 `DictColumnRuntimeFilterPredicate`:对字典词表预求值出位图,行级查表即可——由 `RuntimeFilterPredicatesRewriter::rewrite()` 在 segment 打开、确认该列实际字典编码后改写而成(`segment_iterator.cpp:3383`)。

### 5.5 通道⑤:zone-map 动态重剪

`RuntimeScanRangePruner` 持有 ③ 登记的 unarrived 列表,在 `SegmentIterator` 每个谓词批次入口被询问(`_try_to_update_ranges_by_runtime_filter()`,`segment_iterator.cpp:2159/2187`):

触发条件(`runtime_range_pruner.hpp:258`):

- RF **首次到达**(此前 mask 为 false)→ 立即触发;
- RF 已应用过但 `rf_version` 增长(TopN/Agg 持续收紧)且自上次重剪已读 `rf_update_threshold = 40960` 行 → 再触发(限频,避免每个 chunk 都查 zone map)。

触发后(`RuntimeColumnPredicateBuilder`,`runtime_range_pruner.hpp:33`):

1. 取 RF 的 in-filter 集合(Agg)→ `IN` 固定值集;取 min/max(join/TopN)→ `≥min AND ≤max`(开闭由 filter 决定;低基数全局字典列经 `GlobalDictCodeDecoder` 反解码);**范围为空**且无 null → 直接 `Status::EndOfFile`,整个 segment 提前收工;有 null 则谓词包成 `(range AND ...) OR IS NULL`;
2. 回调把谓词交给列迭代器 `get_row_ranges_by_zone_map()`,新行范围与当前 `_scan_range` 求交(`segment_iterator.cpp:1249-1263`),被裁掉的行数计入 `stats->runtime_stats_filtered`。

这条通道是**晚到 GRF 与持续收紧的 TopN/Agg RF 在存储层的主要生效方式**:不必重开 reader,直接缩小后续要读的行范围。配合 §3.3 的 order hint(按排序方向扫 tablet),TopN 场景常出现"扫了几十万行后界值收紧、剩余 range 全部剪空"的效果。

### 5.6 通道⑥:算子级 chunk 求值

`ScanOperator::pull_chunk()` 对每个出 buffer 的 chunk 依次跑 `evaluate_topn_runtime_filters()`(仅背压开启时单独执行,见 §5.7)与 `eval_runtime_bloom_filters()`(`scan_operator.cpp:309-310`)。后者进 `RuntimeFilterProbeCollector::evaluate()`:

- `has_push_down_to_storage` 的 RF 跳过(④已处理,`runtime_filter_probe.cpp:227`);
- **选择性自适应**(`do_evaluate`/`update_selectivity`,`:199/:403`):每 32 个 chunk 全量重测一轮——所有 RF 各自求值算通过率,通过率 ≤0.5 才算"有用",最多留 3 个(有序 map 按选择性排序);若某 RF 通过率 < `runtime_filter_early_return_selectivity(session,默认 0.05)`,只留它一个。其余 31 个 chunk 只跑选中的 RF;
- 多分区 GRF 先 `compute_partition_index()`(§2.5)再按分区查小 filter;
- chunk 过滤后行数为 0 直接置空返回。

对 lake scan,这条通道实际承担的是④收不了的 RF:TopN/Agg(stream-build)、多列分区 GRF、全局字典列 RF、probe 表达式非裸列的 RF。

### 5.7 TopN RF 的消费与背压

默认(背压关):TopN RF 与其他 RF 一样在⑥的 M_ALL 模式里求值 + ⑤的版本化重剪,`skip_wait` 保证 driver 不会傻等它。

`topn_filter_back_pressure_mode` 开启(§3.3)时,scan 端行为变化(`ScanOperator::prepare`,`scan_operator.cpp:104-116`):

- TopN RF 改在独立的 `M_ONLY_TOPN` 上下文求值(`evaluate_topn_runtime_filters()`,`scan_operator.h:178`),普通 RF 上下文切到 `M_WITHOUT_TOPN`,互不干扰;
- `TopnRfBackPressure`(`topn_runtime_filter_back_pressure.h`)状态机:scan 先放行约 `10 × topN` 行(让 sort 把堆建起来、第一版 RF 发出来),然后进入 throttle/unthrottle 循环——TopN RF 的实测选择性仍差(> 0.1 通过率)时,scan 在 `has_output()` 处自我抑制(`should_throttle()`,`scan_operator.cpp:167`),给 sort 时间收紧界值,再放下一批。总抑制时长 ≤ `back_pressure_throttle_time_upper_bound(300ms)`、轮数 ≤ `back_pressure_max_rounds(3)`,任一耗尽即永久放行(`PH_PASS_THROUGH`)。

动机:不抑制的话,scan 可能在 RF 还很松的窗口期把大量无效行灌进 sort;代价是少量延迟,所以默认关闭,留给"probe 巨大且排序列与存储序相关"的场景手动/自适应开启。

### 5.8 端到端数据流图(Q1,GRF 及时到达的情形)

```
C2 上的 F2 instance (orders scan, 4 个 scan driver)
┌───────────────────────────────────────────────────────────────────┐
│ driver 启动: local闸(空) → global闸: filter#0 未到,等待…           │
│   t=8ms  total GRF 到达 → install_shared → observer 唤醒           │
│ ConnectorChunkSource.prepare:                                      │
│   ②min/max conjunct ──┐                                            │
│ LakeDataSource.open:   ├─→ ScanConjunctsManager.parse_conjuncts    │
│   ③RF已到 → 静态范围 ──┘      ├─ pushdown PredicateTree ───────┐    │
│ init_reader_params:           └─ unarrived RF 列表(空)        │    │
│   ④RuntimeFilterPredicates{filter#0} ────────────────────────┐│    │
│   ⑤RuntimeScanRangePruner(无未到项) ─────────────────────────┐││    │
│                                                              ▼▼▼   │
│ lake::TabletReader → SegmentIterator                                │
│   zone map: ③的 [min,max] 直接跳过 96% 的 page                      │
│   行级:    ④SAMPLE 1024 行 → 过滤率 0.997 ≥ 0.95 → 只留 filter#0   │
│            NORMAL: 每 32768 行回 INIT 重采样                         │
│ ConnectorChunkSource._read_chunk → chunk buffer                     │
│ ScanOperator.pull_chunk:                                            │
│   ⑥eval_runtime_bloom_filters: filter#0 已标记                      │
│     has_push_down_to_storage → 跳过, 零开销                          │
└───────────────────────────────────────────────────────────────────┘
```

GRF 超过 20ms 才到的情形:scan 直接起跑,③④的"已到"分支落空 → filter#0 进 unarrived 列表;到达后下一个谓词批次⑤触发,zone-map 重剪 + ④下一轮 INIT 纳入行级过滤;唯一损失是已经读出的那部分数据只能靠⑥在算子层补刀。

### 5.9 运行示例:Q1-b(broadcast 变体)走查通道①

设优化器选 broadcast join:F1'(orders scan + join,3 instance)、F2'(customer scan → broadcast exchange)。此时:

- FE:`joinMode=BROADCAST`,probe 子树就是同 fragment 的 orders scan,不穿 exchange → **local RF**;`local_rf_waiting_set = {join 节点 id}` 写进 scan 的 TPlanNode;
- BE:join 的 probe 子节点是 scan(非双 exchange)且 `hash_join_push_down_right_table=true` → `_is_push_down=true`;`ht_row_count=800 ≤ 1024` → **in-filter 生成**(800 个 `c_custkey` 值);bloom 同时生成(SINGLETON);
- scan driver 的 local RF 闸等到 join build 完成(`RuntimeFilterHub` holder ready)才放行 —— 因此 `ConnectorChunkSource` 构造时 in-filter **必然已就位**,追加进 conjuncts;
- 通道①:in-filter 规范化为 `o_custkey IN (...800 值)` 列谓词 → zone map 跳页 + 行级精确过滤;通道②为 bloom RF 的 min/max 又加一道范围 conjunct;④收编 bloom RF(此例其实已被①盖住,SAMPLE 后大概率因增益不足被淘汰——自适应机制自动避免重复劳动)。

---
## 6. 辅助机制

### 6.1 RuntimeFilterCache:解决"GRF 比 fragment 跑得快"

GRF 的产生(小 build 侧)可能快于 probe fragment 的部署(尤其多 fragment 大查询)。total RF 到达时若 `QueryContext`/`FragmentContext` 尚未注册,直接丢弃就永远丢了。`RuntimeFilterCache`(进程级,`be/src/runtime/runtime_filter_cache.{h,cpp}`)按 `(query_id, filter_id)` 暂存这类早到的 GRF(`put_if_absent`,`runtime_filter_worker.cpp:928/953`),fragment 部署后由 `OperatorFactory::acquire_runtime_filter()` 领取(§4.1)。缓存随 query 结束清理。它同时承担 RF 事件留痕(`add_rf_event`),本文按约定不展开可观测性。

### 6.2 事件驱动:谁在什么时刻被唤醒

RF 等待全部基于 observer,没有轮询(轮询版 `RuntimeFilterProbeCollector::wait()` 仅服务非 pipeline 旧引擎,5ms 间隔,`runtime_filter_probe_wait.cpp:25`——pipeline 下是死代码路径):

| 事件 | 通知者 | 被唤醒者 |
|---|---|---|
| local in-filter 就绪 | `RuntimeFilterHolder::notify()`(hub) | 在 local RF 闸阻塞的 driver |
| GRF/local bloom 安装 | `RuntimeFilterProbeDescriptor::set_runtime_filter()` 的 DeferOp(`runtime_filter_probe.cpp:543`) | 在 global RF 闸阻塞的 driver |
| global RF 等待超时 | fragment 级 timer observer(`pipeline_driver.cpp:268`) | 同上(到点放行) |

### 6.3 TopN RF 背压状态机

`TopnRfBackPressure`(`topn_runtime_filter_back_pressure.h:26`)三相:`PH_UNTHROTTLE ⇄ PH_THROTTLE → PH_PASS_THROUGH`。

- UNTHROTTLE:放行,直到本轮放行行数超过 `_num_rows_limiter`(首轮 `10×topN`,每轮 ×2);
- THROTTLE:`has_output()` 返回 false 抑制调度,本轮时长 = `back_pressure_throttle_time_upper_bound / max_rounds`(默认 100ms);
- 永久放行(PASS_THROUGH)的任一条件:轮数耗尽(默认 3)、累计抑制 ≥ 300ms、行数限幅溢出、或 **TopN RF 实测通过率 ≤ 0.1**(filter 已经足够锋利,无需再等)。

### 6.4 大 filter 传输与 worker 自保护

- filter 序列化体积 > `runtime_filter_rpc_http_min_size(session,默认 64MB)` 时改用 HTTP 传输(brpc 对超大附件不友好);判定在发送侧,阈值随请求捎带给转发者;
- broadcast 直发的 passthrough/relay 二态由 `deliver_broadcast_rf_passthrough_bytes_limit(128KB)` 切换(§4.6);
- `RuntimeFilterWorker` 队列限流 `runtime_filter_queue_limit`(默认 -1 不限;0 按内存预检;>0 按事件数),超限直接丢弃事件并告警——再次体现"RF 可丢"的设计前提。

### 6.5 skew join 协同(概述)

倾斜优化把一个 join 拆成 shuffle join + broadcast join(处理倾斜值)两支时,GRF 必须是两支的并集才安全。机制:broadcast 支不独立出 GRF,而是把 join key 列原样发给 merge node(`publish_runtime_filters_for_skew_broadcast_join()`,`runtime_filter_worker.cpp:112`);merge node 等 shuffle 支的 partial 齐 **且** 收到 broadcast 支的 key 列后,把 key 逐值插入各 hash 分区(`insert_skew_values`)再广播(`merge_runtime_filter()` 中 `is_skew_join` 的额外等待,`:447`)。FE 侧 `skew_join_runtime_filters` 名册(§3.8)告知 merge node 哪些 filter 需要这种等待。

### 6.6 容错语义

- **RF 丢失/迟到**:查询照常出正确结果,只是慢。所有发送都是 fire-and-forget,所有等待都有超时(唯一的无限等待是 local RF 闸,其安全性由"build 与 probe 同 instance、build 必然终止"保证;以及 merge node 等 partial,其后果只是该 GRF 不生效);
- **RF 错误性**的防护方向是"宁可漏过、不可错杀":bloom 误判只会放过该被过滤的行(交给 join 自己滤);min/max 与 in-filter 是精确语义;`equalForNull`(`<=>`)单独建模避免 null 误杀;
- **降级阶梯**:bloom 超限 → EmptyFilter(保 min/max)→ min/max 也无法构建 → `always_true` 直接放行。每一级都只损失过滤力。

---

## 7. 参考

### 7.1 FE session 变量(全部核对自 `SessionVariable.java` 字段初始值)

| 变量 | 默认值 | 作用点 |
|---|---|---|
| `enable_global_runtime_filter` | true | GRF 总开关(§3.1;pipeline 下关闭仅影响下发,不影响规划) |
| `enable_topn_runtime_filter` | true | TopN RF / agg TopN RF 生成(§3.3/§3.4) |
| `agg_in_filter_limit` | 1024 | AGG_IN_FILTER 的 limit 上限(§3.4) |
| `runtime_join_filter_push_down_limit` | 1024000 | build 行数超此值不建 local in-filter/local bloom(§4.2/§4.3) |
| `global_runtime_filter_build_max_size` | 67108864 (64M) | shuffle join 建 GRF 的基数上限(§3.2);merge node 总尺寸闸(§4.5);instance 内 global limit(§4.3) |
| `global_runtime_filter_build_min_size` | 131072 (128K) | `canProbeUse` 小 build 直采线(§3.5) |
| `global_runtime_filter_probe_min_size` | 102400 (100K) | probe 低于此基数不用 GRF(§3.5) |
| `global_runtime_filter_probe_min_selectivity` | 0.5 | 预估选择率闸(§3.5) |
| `global_runtime_filter_wait_timeout` | 20 (ms) | 非 scan 算子的 GRF 等待(§4.9) |
| `runtime_filter_scan_wait_time` | 20 (ms) | scan 算子的 GRF 等待(§4.9) |
| `global_runtime_filter_rpc_timeout` | 400 (ms) | partial/total RF 的 RPC 超时(§4.4) |
| `global_runtime_filter_rpc_http_min_size` | 67108864 | 改走 HTTP 的体积阈值(§6.4) |
| `runtime_filter_early_return_selectivity` | 0.05 | 算子级"独苗"阈值(§5.6) |
| `enable_join_runtime_filter_push_down` | true | 存储层行级 RF 谓词总开关(§5.4) |
| `runtime_filter_on_exchange_node` | false | RF 同时落在 exchange 上(§3.5) |
| `enable_multicolumn_global_runtime_filter`(alias→v2) | true | 多列等值条件下 GRF 跨 exchange(§3.5) |
| `enable_pipeline_level_multi_partitioned_rf` | false | 2 层 layout(§3.7,默认不启用) |
| `enable_join_runtime_bitset_filter` | true | broadcast 下 bitset 替代 bloom(§2.4) |
| `hash_join_push_down_right_table` | true | join `is_push_down`(in-filter 前提,§3.2) |
| `topn_push_down_agg_mode` | 1 | ≥1 时预聚合式 TopN 的 RF 让位给 agg(§3.3) |
| `topn_filter_back_pressure_mode` | 0(关) | 0/1(自适应,probe>5000 万)/2(强制)(§3.3/§5.7) |
| `back_pressure_max_rounds` | 3 | 背压轮数(§6.3) |
| `back_pressure_throttle_time_upper_bound` | 300 (ms) | 背压总时长(§6.3) |

### 7.2 BE config 与编译期常量

| 配置/常量 | 默认值 | 作用点 |
|---|---|---|
| `max_pushdown_conditions_per_column` | 1024 | in-filter/`InRuntimeFilter` 元素数上限(§4.2/§4.3/§4.8) |
| `send_rpc_runtime_filter_timeout_ms` | 1000 | RPC 超时兜底(session 未设时)(§4.4) |
| `send_runtime_filter_via_http_rpc_min_size` | 67108864 | HTTP 阈值兜底(§6.4) |
| `deliver_broadcast_rf_passthrough_bytes_limit` | 131072 (128K) | broadcast 直发 passthrough/relay 分界(§4.6) |
| `deliver_broadcast_rf_passthrough_inflight_num` | 10 | passthrough 并发 RPC 上限(§4.6) |
| `runtime_filter_queue_limit` | -1 | worker 队列限流(§6.4) |
| `rf_sample_rows` | 1024 | 存储层采样行数(§5.4) |
| `rf_sample_ratio` | 32 | NORMAL 期 = 采样行数×32(§5.4) |
| `rf_branchless_ratio` | 8 | branchless 求值切换比(§5.4) |
| `RuntimeScanRangePruner::rf_update_threshold` | 40960(编译期) | 版本化重剪的最小行间隔(§5.5) |
| `SimdBlockFilter`:`LOG_BUCKET_BYTE_SIZE=5`、`BITS_SET_PER_BLOCK=8`、`MINIMUM_ELEMENT_NUM=1` | 编译期 | 尺寸公式(§2.2) |

### 7.3 结构不变量

每条注明"约束什么"与"由哪段代码、在哪一层强制"。

1. **没有消费者的 RF 不存在。** FE 端 `pushDownRuntimeFilters()` 返回 false 时 build 列表不收录(`JoinNode.java:232` 等);BE 端 `!has_consumer()` 再次跳过(`runtime_filter_types.cpp:147`)。跨 FE/BE 双重强制。
2. **partial 与 total 的 bloom 尺寸一致性:同一 filter_id 的所有 driver/instance 使用"合并后总行数"初始化。** instance 内由 `PartialRuntimeFilterMerger` 以 Σht 行数统一 `init()`(`runtime_filter_types.cpp:128/171`);跨 instance 的 `GLOBAL_SHUFFLE_1L` 干脆不 OR 合并而是 concat 成分区数组(§2.5),绕开了尺寸约束;真正按位 OR 的 `SimdBlockFilter::merge()` 以 DCHECK 强制等尺寸(`runtime_filter.cpp:95`)。
3. **GRF 分区选择与 shuffle 路由一致。** "行 → 分区"两侧都用 `exchange_hash_function_version` 选定的同一哈希(`runtime_filter.h:498`);"分区 → 子 filter"由 merge node 按 `build_be_number` 升序 concat(`RuntimeFilterMergerStatus::filters` 为有序 map,`_send_total_runtime_filter` 顺序遍历)与 FE 按 instance 顺序分配 backend_num 共同保证。FE 调度层 + BE 两处协同的全局不变量。
4. **merge node 必须等齐 `expect_number` 份 partial,因此 build 侧"哪怕没东西也要发"。** 空哈希表、超限降级的 instance 仍发送(Empty)filter(`hash_joiner.cpp:366-370` 的注释即此;`runtime_filter_types.cpp:163-169` 的降级保发)。否则 GRF 永不下发——这是活性不变量,弱化它只损失性能不损坏正确性。
5. **同一 RF 不会在存储层和算子层被求值两次。** `ScanConjunctsManager::get_runtime_filter_predicates()` 收编时置 `has_push_down_to_storage`(`olap_scan_prepare.cpp:1676`),`RuntimeFilterProbeCollector::do_evaluate()/update_selectivity()` 检查该标志跳过(`runtime_filter_probe.cpp:227/429`)。这是 BE 单进程内的局部防重,**不是**全局语义约束(重复求值只浪费 CPU,不出错)。
6. **stream-build RF(TopN/Agg)只收紧、不放松。** `MinMaxRuntimeFilter::update_min_max()` 仅单向收窄并 bump 版本;多 driver 间取交集(`set_or_intersect_filter`)即"最保守的界"。若违反(界放松),已被⑤剪掉的行无法复活——单调性是正确性必需,由 filter 类自身的更新接口强制。
7. **`local_rf_waiting_set` 只含同 fragment 的 build 节点。** FE `fillLocalRfWaitingSet()` 按 fragment 内 build node 集合过滤(`PlanNode.java:191`)。这保证 local RF 闸的无限等待不会死锁(等的 build 与等待者同 instance 调度,必然完成)。FE 规划层强制、BE 信任。

### 7.4 主要代码文件索引

| 层 | 文件 |
|---|---|
| FE 描述与下推 | `fe/fe-core/src/main/java/com/starrocks/planner/RuntimeFilterDescription.java`、`PlanNode.java`、`ExchangeNode.java`、`JoinNode.java`、`SortNode.java`、`AggregationNode.java`、`OlapScanNode.java`、`RuntimeFilterLayout.java` |
| FE 调度装配 | `fe/.../qe/DefaultCoordinator.java`、`qe/scheduler/dag/ExecutionFragment.java`、`planner/PlanFragment.java` |
| 过滤器本体 | `be/src/runtime/runtime_filter.{h,cpp}`、`runtime_in_filter.h`、`runtime_filter_layout.{h,cpp}`、`runtime_filter_serde.{h,cpp}` |
| build/合并 | `be/src/exec/hash_joiner.cpp`、`exec/pipeline/hashjoin/hash_join_build_operator.cpp`、`exec/pipeline/runtime_filter_types.{h,cpp}`、`exec/chunks_sorter_topn.cpp`、`exec/pipeline/sort/partition_sort_sink_operator.cpp`、`exec/agg_runtime_filter_builder.{h,cpp}`、`exec/pipeline/aggregate/aggregate_{blocking,streaming}_sink_operator.cpp` |
| 传输 | `be/src/runtime/runtime_filter_worker.{h,cpp}`、`runtime_filter_cache.{h,cpp}` |
| probe 框架 | `be/src/exec/runtime_filter/runtime_filter_probe.{h,cpp}`、`runtime_filter_registry.{h,cpp}`、`runtime_filter_descriptor.{h,cpp}`、`exec/pipeline/runtime_filter_hub.h`、`exec/pipeline/pipeline_driver.{h,cpp}`、`exec/pipeline/operator_factory.cpp` |
| lake 消费 | `be/src/connector/lake_connector.cpp`、`connector/data_source.cpp`、`exec/pipeline/scan/connector_scan_operator.cpp`、`exec/pipeline/scan/scan_operator.{h,cpp}`、`exec/olap_scan_prepare.{h,cpp}`、`storage/runtime_filter_predicate.{h,cpp}`、`storage/runtime_range_pruner.{h,hpp}`、`storage/rowset/segment_iterator.cpp`、`storage/lake/tablet_reader.cpp`、`exec/pipeline/topn_runtime_filter_back_pressure.h` |

