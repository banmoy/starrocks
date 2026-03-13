# StarRocks 无 Broker 模式 Broker Load 与 AWS IMDS 交互分析

## 问题背景

用户在 AWS EC2 上运行 StarRocks 集群，使用 Broker Load（无 Broker 进程模式）每 4 小时从 S3 加载数据。
安全团队发现该 EC2 实例持续产生 IMDSv1 调用，且调用峰值与 Broker Load 定时任务同步。
用户使用 role-based 认证（IAM Role 绑定到 EC2 实例）。

---

## 根因定论

**已通过 tcpdump 抓包确认根因。**

### 现象

同一集群的两台 BE 节点行为不同：

| | BE 节点 1（有问题） | BE 节点 2（正常） |
|---|---|---|
| PUT `/latest/api/token` | **无**（完全跳过） | 有，返回 200 OK |
| GET 请求带 `x-aws-ec2-metadata-token` | **否** | 是 |
| IMDS 版本 | **IMDSv1** | IMDSv2 |
| User-Agent | `aws-sdk-cpp/1.11.267` | `aws-sdk-cpp/1.11.267` |
| 内核版本 | 6.8.0-1040-aws | 6.8.0-1036-aws |

同一台有问题的机器上，另一个 Go 程序（`aws-sdk-go/1.55.5`）正常使用 IMDSv2。

### 根因

AWS C++ SDK 1.11.267 的 `EC2MetadataClient` 中有一个 `m_tokenRequired` 状态标志：

- 初始值为 `true`（首次尝试 IMDSv2）
- 如果 IMDSv2 的 PUT `/latest/api/token` 请求失败（超时/非200），SDK 将 `m_tokenRequired` 设为 `false`
- **此后该 EC2MetadataClient 实例的所有请求永久走 IMDSv1，不再尝试 IMDSv2**

问题 BE 节点在启动时（或首次凭证获取时），IMDSv2 PUT 请求失败（可能因 IMDS 限流或 1 秒超时不够），导致整个 BE 生命周期内都回退到 IMDSv1。正常 BE 节点启动时 PUT 成功，所以一直走 IMDSv2。

这是非确定性问题，取决于 BE 启动那一瞬间 IMDS 是否能在 1 秒内响应 PUT 请求。

---

## SDK 版本信息

| 组件 | SDK | 版本 | IMDSv2 支持 |
|------|-----|------|------------|
| **BE** | AWS C++ SDK | 1.11.267 (2024-02-16) | 支持，但有回退问题 |
| **FE** | AWS Java SDK v2 | 2.29.52 | 完整支持 |
| **FE** | Hadoop (hadoop-aws) | 3.4.1 | 通过 SDK v2 支持 |

所有版本都满足 AWS IMDSv2 的最低版本要求，SDK 版本本身不是问题。

---

## 无 Broker 模式的整体架构

无 Broker 模式下（`WITH BROKER` 后不指定名称），一次 Broker Load 涉及 **FE 和 BE 两个组件分别与 AWS 交互**：

```
SQL: LOAD LABEL db.label (DATA INFILE ("s3://bucket/path") INTO TABLE t)
     WITH BROKER ("aws.s3.use_instance_profile" = "true", "aws.s3.region" = "us-east-1")
                  ↓
     ┌─────────── FE（Java）────────────┐     ┌──────────── BE（C++）───────────────┐
     │                                    │     │                                     │
     │  阶段1：列举文件                     │     │  阶段2：读取数据                       │
     │  Hadoop S3A + AWS Java SDK v2      │     │  AWS C++ SDK 1.11.267               │
     │  IAMInstanceCredentialsProvider    │     │  InstanceProfileCredentialsProvider  │
     │         ↓                           │     │          ↓                           │
     │  IMDS 获取临时凭证                    │     │  IMDS 获取临时凭证                     │
     │  访问 S3 列举文件列表                  │     │  访问 S3 读取文件内容                   │
     └────────────────────────────────────┘     └─────────────────────────────────────┘
```

---

## 第一部分：FE 与 AWS 的交互

### 触发时机

FE 在 **Pending 阶段**列举 S3 文件列表。

### 调用链

```
BrokerLoadPendingTask.executeTask()                          [BrokerLoadPendingTask.java]
  └─ getAllFileStatus()
       └─ HdfsUtil.parseFile(path, brokerDesc, fileStatuses)
            └─ HdfsService.listPath()
                 └─ HdfsFsManager.listPath()
                      └─ getFileSystem() → getS3FileSystem() / getS3AFileSystem()
                           └─ getFileSystemByCloudConfiguration()
                                ├── cloudConfiguration.applyToConfiguration(conf)
                                │   └── AwsCloudCredential.applyToConfiguration()
                                │       → conf.set("fs.s3a.aws.credentials.provider",
                                │                  "IAMInstanceCredentialsProvider")
                                ├── FileSystem.get(uri, conf)  ← Hadoop S3A 初始化
                                │   └── S3AFileSystem → IAMInstanceCredentialsProvider
                                │       └── AWS Java SDK v2 InstanceProfileCredentialsProvider
                                │           └── IMDS 调用
                                └── cloudConfiguration.toThrift(tCloudConfiguration)
                                    → tProperties.setCloud_configuration()  ← 传给 BE
```

### 关键代码

**属性解析** — `fe/fe-core/.../credential/aws/AwsCloudConfigurationProvider.java:74-104`：
- `aws.s3.use_instance_profile` → `boolean useInstanceProfile`
- `aws.s3.use_aws_sdk_default_behavior` → `boolean useAWSSDKDefaultBehavior`

**凭证提供者选择** — `fe/fe-core/.../credential/aws/AwsCloudCredential.java:233-268`：
- `useAWSSDKDefaultBehavior=true` → `OverwriteAwsDefaultCredentialsProvider`（包装 `DefaultCredentialsProvider`）
- `useInstanceProfile=true` → `IAMInstanceCredentialsProvider`（Hadoop S3A 对 SDK v2 `InstanceProfileCredentialsProvider` 的封装）
- AK/SK → `SimpleAWSCredentialsProvider`（无 IMDS）

**传递给 BE** — `fe/fe-core/.../planner/FileScanNode.java:295-333`：
- `!brokerDesc.hasBroker()` 时构建 `THdfsProperties`（含 `cloud_configuration`）
- `params.setHdfs_properties(hdfsProperties)` + `params.setUse_broker(false)`

---

## 第二部分：BE 与 AWS 的交互

### 触发时机

BE 在 **Loading 阶段**读取 S3 数据文件。

### 调用链

```
file_scanner.cpp: use_broker=false
  → FileSystem::CreateUniqueFromString(path, FSOptions(&params))
    → fs.cpp: is_s3_uri() → new_fs_s3(options)
      → S3FileSystem → new_s3client(uri, _options)
        ├── S3ClientFactory::getClientConfig()  [static, 首次触发 IMDS 查 region]
        ├── 检测到 cloud_configuration
        │   → S3ClientFactory::new_client(tCloudConfiguration)
        │     → CloudConfigurationFactory::create_aws()  [cloud_configuration_factory.cpp:23-55]
        │       → AWSCloudCredential{use_instance_profile=true, ...}
        │     → _get_aws_credentials_provider()  [fs_s3.cpp:80-112]
        │       → Aws::Auth::InstanceProfileCredentialsProvider
        │         → EC2MetadataClient → IMDS 调用
        └── S3Client 发起 S3 请求时获取凭证
```

### 关键代码

**凭证提供者选择** — `be/src/fs/fs_s3.cpp:80-112`：
```cpp
if (aws_cloud_credential.use_aws_sdk_default_behavior) {
    credential_provider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
} else if (aws_cloud_credential.use_instance_profile) {
    credential_provider = std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    // ← 使用默认构造函数，内部 EC2MetadataClient 超时仅 1 秒
} else if (!aws_cloud_credential.access_key.empty() && !aws_cloud_credential.secret_key.empty()) {
    credential_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(...);
}
```

**ClientConfiguration 静态初始化** — `be/src/fs/fs_s3.h:67-75`：
```cpp
static ClientConfiguration& getClientConfig() {
    // 默认构造函数会触发 EC2 metadata 查询 region（仅首次）
    // 参考 https://github.com/aws/aws-sdk-cpp/issues/1440
    static ClientConfiguration instance;
    return instance;
}
```

---

## 第三部分：C++ SDK IMDSv2→v1 回退机制源码分析

以下源码来自 `aws-sdk-cpp` 1.11.267 的 `AWSHttpResourceClient.cpp`。

### EC2MetadataClient 默认超时

```cpp
// MakeDefaultHttpResourceClientConfiguration()
res.connectTimeoutMs = 1000;     // 1 秒连接超时
res.requestTimeoutMs = 1000;     // 1 秒请求超时
res.retryStrategy = DefaultRetryStrategy(1, 1000);  // 仅 1 次重试，1 秒间隔
```

### GetDefaultCredentialsSecurely()（IMDSv2 入口）

```cpp
Aws::String EC2MetadataClient::GetDefaultCredentialsSecurely() const
{
    // 如果之前已经回退过，直接走 v1
    #if !defined(DISABLE_IMDSV1)
    if (!m_disableIMDSV1 && !m_tokenRequired) {
        return GetDefaultCredentials();   // ← 直接 IMDSv1，不再尝试 PUT
    }
    #endif

    // 尝试 IMDSv2：PUT /latest/api/token
    ss << m_endpoint << EC2_IMDS_TOKEN_RESOURCE;
    tokenRequest = CreateHttpRequest(ss.str(), HttpMethod::HTTP_PUT, ...);
    tokenRequest->SetHeaderValue(EC2_IMDS_TOKEN_TTL_HEADER, "21600");
    auto result = GetResourceWithAWSWebServiceResult(tokenRequest);

    // PUT 失败 → 回退到 IMDSv1
    #if !defined(DISABLE_IMDSV1)
    if (!m_disableIMDSV1 && (result.GetResponseCode() != HttpResponseCode::OK || trimmedTokenString.empty()))
    {
        m_tokenRequired = false;   // ← 永久缓存：不再尝试 IMDSv2
        AWS_LOGSTREAM_TRACE(..., "...falling back to less secure way.");
        return GetDefaultCredentials();   // ← 走 IMDSv1
    }
    #endif

    // PUT 成功 → 用 token 继续 GET（IMDSv2）
    m_token = trimmedTokenString;
    // ... GET with x-aws-ec2-metadata-token header
}
```

### GetDefaultCredentials()（IMDSv1 路径）

```cpp
Aws::String EC2MetadataClient::GetDefaultCredentials() const
{
    if (m_disableIMDSV1) {
        AWS_LOGSTREAM_INFO(..., "Attempting to call IMDSv1 Service while disabled");
        return {};   // 如果禁用了 v1，返回空
    }
    // 直接 GET，不带 token → IMDSv1
    auto result = GetResourceWithAWSWebServiceResult(
        m_endpoint.c_str(), EC2_SECURITY_CREDENTIALS_RESOURCE, nullptr);
    // ...
}
```

### 关键字段

- `m_tokenRequired`：初始 `true`，PUT 失败后设为 `false`，**永久生效直到进程重启**
- `m_disableIMDSV1`：来自 `ClientConfiguration.disableImdsV1`，StarRocks 当前未设置（默认 `false`）
- 也受环境变量 `AWS_EC2_METADATA_V1_DISABLED=true` 控制

---

## 第四部分：IMDS 触发点汇总

### FE 侧

| # | 触发点 | 条件 | SDK |
|---|--------|------|-----|
| 1 | Hadoop S3A 列举文件时获取凭证 | `use_instance_profile=true` 或 `use_aws_sdk_default_behavior=true` | AWS Java SDK v2 (2.29.52) |

### BE 侧

| # | 触发点 | 条件 | SDK |
|---|--------|------|-----|
| 1 | `S3ClientFactory::getClientConfig()` 静态初始化查询 region | 首次调用，无条件 | AWS C++ SDK 1.11.267 |
| 2 | `InstanceProfileCredentialsProvider` 获取凭证 | `use_instance_profile=true` 或 `use_aws_sdk_default_behavior=true` | AWS C++ SDK 1.11.267 |

---

## 第五部分：tcpdump 抓包证据

### 问题 BE 节点的抓包（IMDSv1）

```
# 完全没有 PUT /latest/api/token 请求
# 直接发送不带 token 的 GET — IMDSv1

GET /latest/meta-data/iam/security-credentials HTTP/1.1
host: 169.254.169.254
user-agent: aws-sdk-cpp/1.11.267 ...
（无 x-aws-ec2-metadata-token header）

→ 200 OK, 返回 role 名称

GET /latest/meta-data/iam/security-credentials/xxxxxx-role HTTP/1.1
host: 169.254.169.254
user-agent: aws-sdk-cpp/1.11.267 ...
（无 x-aws-ec2-metadata-token header）

→ 200 OK, 返回临时凭证 JSON
```

同一台机器上的 Go 程序正常使用 IMDSv2：
```
GET /latest/meta-data/iam/security-credentials/ HTTP/1.1
User-Agent: aws-sdk-go/1.55.5 ...
X-Aws-Ec2-Metadata-Token: xxxxxxx        ← 带 token，IMDSv2
```

### 正常 BE 节点的抓包（IMDSv2）

```
# 先 PUT 获取 token
PUT /latest/api/token HTTP/1.1
host: 169.254.169.254
user-agent: aws-sdk-cpp/1.11.267 ...
x-aws-ec2-metadata-token-ttl-seconds: 21600

→ 200 OK, 返回 token

# 再用 token 发 GET — IMDSv2
GET /latest/meta-data/iam/security-credentials HTTP/1.1
host: 169.254.169.254
user-agent: aws-sdk-cpp/1.11.267 ...
x-aws-ec2-metadata-token: xxxxxxxxx      ← 带 token，IMDSv2

→ 200 OK, 返回 role 名称

GET /latest/meta-data/iam/security-credentials/xxxx-role HTTP/1.1
host: 169.254.169.254
user-agent: aws-sdk-cpp/1.11.267 ...
x-aws-ec2-metadata-token: xxxxxxx        ← 带 token，IMDSv2

→ 200 OK, 返回临时凭证 JSON
```

---

## 第六部分：解决方案

### 方案 1：环境变量（推荐，立即可用，需重启）

在所有 BE 和 FE 的启动脚本中添加：

```bash
export AWS_EC2_METADATA_V1_DISABLED=true
```

效果：SDK 内部 `m_disableIMDSV1 = true`，即使 PUT 失败也不会回退到 v1，后续会重新尝试 IMDSv2。

无法不重启生效——`m_tokenRequired` 缓存在 BE 进程内存中的 `EC2MetadataClient` 实例里，没有外部接口可以重置。

### 方案 2：EC2 实例强制 IMDSv2（基础设施层面）

```bash
aws ec2 modify-instance-metadata-options \
    --instance-id <instance-id> \
    --http-tokens required \
    --http-put-response-hop-limit 2
```

### 方案 3：StarRocks 代码修复（长期）

在 `be/src/fs/fs_s3.cpp` 的 `_get_aws_credentials_provider()` 中，构建 `InstanceProfileCredentialsProvider` 时传入禁用 IMDSv1 回退的配置。当前代码使用默认构造函数：

```cpp
credential_provider = std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
```

需要改为通过 `ClientConfiguration` 设置 `disableImdsV1 = true`。

---

## 附录：完整调用链图

```
用户提交 SQL
  │
  ▼
FE: LoadStmt → BrokerDesc(properties: {"aws.s3.use_instance_profile":"true", ...})
  │
  ▼
FE: BulkLoadJob.fromLoadStmt() → BrokerLoadJob
  │
  ▼
FE: BrokerLoadPendingTask.executeTask()
  │
  ├──── FE 列举文件（FE 与 AWS 交互） ────────────────────────────────────────────
  │     │
  │     ▼
  │     HdfsUtil.parseFile() → HdfsFsManager.listPath()
  │     → HdfsFsManager.getFileSystem() → getS3FileSystem() / getS3AFileSystem()
  │     → CloudConfigurationFactory.buildCloudConfigurationForStorage(properties)
  │       → AwsCloudConfiguration(AwsCloudCredential{useInstanceProfile=true, ...})
  │     → getFileSystemByCloudConfiguration()
  │       ├── cloudConfiguration.applyToConfiguration(conf)
  │       │   → "fs.s3a.aws.credentials.provider" = "IAMInstanceCredentialsProvider"
  │       ├── FileSystem.get(uri, conf) → S3AFileSystem
  │       │   └── IAMInstanceCredentialsProvider → AWS Java SDK v2
  │       │       └── InstanceProfileCredentialsProvider → IMDS 调用
  │       └── cloudConfiguration.toThrift(tCloudConfiguration)
  │           → tProperties.setCloud_configuration(tCloudConfiguration)
  │
  ▼
FE: Pending 完成 → Loading 阶段 → FileScanNode.initParams()
  │   params.setHdfs_properties(hdfsProperties)  ← 含 cloud_configuration
  │   params.setUse_broker(false)
  │
  ▼
FE → BE: Thrift 发送 TBrokerScanRangeParams
  │
  ├──── BE 读取数据（BE 与 AWS 交互） ────────────────────────────────────────────
  │     │
  │     ▼
  │     file_scanner.cpp: use_broker=false
  │     → FileSystem::CreateUniqueFromString(path, FSOptions(&params))
  │     → new_fs_s3(options) → S3FileSystem
  │     → new_s3client(uri, _options)
  │       ├── S3ClientFactory::getClientConfig() [static, 首次触发 IMDS 查 region]
  │       ├── S3ClientFactory::new_client(tCloudConfiguration)
  │       │   → CloudConfigurationFactory::create_aws() → AWSCloudCredential
  │       │   → _get_aws_credentials_provider()
  │       │     → InstanceProfileCredentialsProvider（默认构造）
  │       │       → EC2MetadataClient（1 秒超时，1 次重试）
  │       │         ├── PUT /latest/api/token（IMDSv2）
  │       │         │   ├── 成功 → 用 token GET → IMDSv2 ✅
  │       │         │   └── 失败 → m_tokenRequired=false → GET 无 token → IMDSv1 ❌
  │       │         │              （此后永久 IMDSv1，直到进程重启）
  │       │         └── 后续调用：
  │       │             └── m_tokenRequired==false → 直接 IMDSv1，不再尝试 PUT
  │       └── S3Client 发起 GetObject 读取数据
  │
  ▼
Load 完成
```

---

## 附录：关键源码文件索引

| 文件 | 内容 |
|------|------|
| `be/src/fs/fs_s3.cpp:80-112` | BE 凭证提供者选择（`_get_aws_credentials_provider`） |
| `be/src/fs/fs_s3.h:67-75` | `getClientConfig()` 静态初始化（触发 IMDS region 查询） |
| `be/src/fs/credential/cloud_configuration_factory.cpp:23-55` | 解析 `TCloudConfiguration` → `AWSCloudCredential` |
| `be/src/fs/credential/cloud_configuration.h` | `AWSCloudCredential` 结构体定义 |
| `fe/fe-core/.../credential/aws/AwsCloudCredential.java:233-268` | FE `applyToConfiguration()`（设置 Hadoop 凭证提供者） |
| `fe/fe-core/.../credential/aws/AwsCloudConfigurationProvider.java:74-104` | FE 解析 `aws.s3.*` 属性 |
| `fe/fe-core/.../credential/aws/AwsCloudConfiguration.java:70-94` | FE `applyToConfiguration()`（设置 Hadoop S3A 配置） |
| `fe/fe-core/.../planner/FileScanNode.java:295-333` | FE `initParams()`（区分 broker/非 broker 路径） |
| `fe/fe-core/.../analysis/BrokerDesc.java:80-81` | `hasBroker()` 判断逻辑 |
| `fe/fe-core/.../fs/hdfs/HdfsFsManager.java:385-430` | FE `getFileSystem()` 路由 |
| `fe/fe-core/.../fs/hdfs/HdfsFsManager.java:710-782` | FE `getFileSystemByCloudConfiguration()` |
| `fe/fe-core/.../credential/provider/OverwriteAwsDefaultCredentialsProvider.java` | 自定义 `DefaultCredentialsProvider` 包装 |
| `thirdparty/vars.sh:325` | AWS C++ SDK 版本（1.11.267） |
| `fe/pom.xml:50,66` | Hadoop 版本（3.4.1）和 AWS Java SDK v2 版本（2.29.52） |
