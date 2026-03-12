# StarRocks 无 Broker 模式 Broker Load 与 AWS IMDS 交互分析

## 问题背景

用户在 AWS EC2 上运行 StarRocks 集群，使用 Broker Load（无 Broker 进程模式）每 4 小时从 S3 加载数据。
安全团队发现该 EC2 实例持续产生 IMDSv1 调用，且调用峰值与 Broker Load 定时任务同步。
用户使用 role-based 认证（IAM Role 绑定到 EC2 实例）。

---

## 无 Broker 模式的整体架构

无 Broker 模式下，一次 Broker Load 涉及 **FE 和 BE 两个组件分别与 AWS 交互**：

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

### 1.1 触发时机

FE 在 **Pending 阶段**需要列举 S3 上的文件列表，这是 Load 任务的第一步。

调用链：

```
BrokerLoadPendingTask.executeTask()
  └─ getAllFileStatus()
       └─ HdfsUtil.parseFile(path, brokerDesc, fileStatuses)
            └─ HdfsService.listPath(request, fileStatuses, ...)
                 └─ HdfsFsManager.listPath(path, fileNameOnly, properties)
                      └─ getFileSystem(path, properties, tProperties)
```

**代码位置：** `fe/fe-core/.../load/loadv2/BrokerLoadPendingTask.java`

### 1.2 S3 FileSystem 的创建

`HdfsFsManager.getFileSystem()` 根据 URI scheme 路由：

```392:399:fe/fe-core/src/main/java/com/starrocks/fs/hdfs/HdfsFsManager.java
        switch (scheme) {
            // ...
            case S3A_SCHEME:
                return getS3AFileSystem(path, loadProperties, tProperties);
            case S3_SCHEMA:
                return getS3FileSystem(path, loadProperties, tProperties);
```

- `s3://` → `getS3FileSystem()` → 直接走 `CloudConfiguration` 路径
- `s3a://` → `getS3AFileSystem()` → 先尝试 `CloudConfiguration`，如果参数不匹配则走 legacy `fs.s3a.*` 路径

### 1.3 CloudConfiguration 路径（核心路径）

当用户设置了 `aws.s3.*` 参数时，走 `getFileSystemByCloudConfiguration()`：

```710:782:fe/fe-core/src/main/java/com/starrocks/fs/hdfs/HdfsFsManager.java
    private HdfsFs getFileSystemByCloudConfiguration(CloudConfiguration cloudConfiguration, String path,
                                                     THdfsProperties tProperties) {
        // ...
        Configuration conf = new ConfigurationWrap();
        cloudConfiguration.applyToConfiguration(conf);    // 关键：设置凭证提供者
        // ...
        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        FileSystem innerFileSystem = FileSystem.get(pathUri.getUri(), conf);
        // ...
        // 同时构建 TCloudConfiguration 传给 BE
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tProperties.setCloud_configuration(tCloudConfiguration);
    }
```

这里做了两件事：
1. **创建 Hadoop FileSystem** 供 FE 自己列举文件
2. **构建 `TCloudConfiguration`** 通过 Thrift 传给 BE

### 1.4 FE 的凭证提供者选择

`AwsCloudCredential.applyToConfiguration()` 设置 Hadoop S3A 的凭证提供者：

```233:268:fe/fe-core/src/main/java/com/starrocks/credential/aws/AwsCloudCredential.java
    public void applyToConfiguration(Configuration configuration) {
        if (useAWSSDKDefaultBehavior) {
            // → OverwriteAwsDefaultCredentialsProvider (实际创建 DefaultCredentialsProvider)
            configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, DEFAULT_CREDENTIAL_PROVIDER);
        } else if (useInstanceProfile) {
            // → IAMInstanceCredentialsProvider
            configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, IAM_CREDENTIAL_PROVIDER);
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            configuration.set(Constants.ACCESS_KEY, accessKey);
            configuration.set(Constants.SECRET_KEY, secretKey);
            // → SimpleAWSCredentialsProvider (无 IMDS)
        }
        // ...
    }
```

三种凭证提供者和 IMDS 的关系：

| 用户配置 | Hadoop Credential Provider | 底层 AWS SDK | 是否触发 IMDS |
|----------|---------------------------|-------------|-------------|
| `use_aws_sdk_default_behavior=true` | `OverwriteAwsDefaultCredentialsProvider` | AWS Java SDK v2 `DefaultCredentialsProvider` | **是**（链中包含 IMDS） |
| `use_instance_profile=true` | `IAMInstanceCredentialsProvider` | AWS Java SDK v2 `InstanceProfileCredentialsProvider` | **是**（专门走 IMDS） |
| AK/SK | `SimpleAWSCredentialsProvider` | 无 | **否** |

### 1.5 FE 使用的 AWS SDK 版本

```
Hadoop: 3.4.1
AWS Java SDK: v2 (software.amazon.awssdk:bundle 2.29.52)
```

Hadoop 3.4.1 的 `hadoop-aws` 模块已经使用 AWS Java SDK v2。
SDK v2 的 `InstanceProfileCredentialsProvider` **默认先尝试 IMDSv2，如果失败会回退到 IMDSv1**。

### 1.6 FE 的 IMDS 调用时序

```
FE 收到 LOAD 语句
  ↓
BrokerLoadPendingTask 开始执行
  ↓
创建 Hadoop S3AFileSystem（首次或缓存未命中）
  ↓
S3AFileSystem 初始化时通过 credential provider 获取凭证
  ↓
IAMInstanceCredentialsProvider → 调用 IMDS 获取 IAM Role 临时凭证
  ↓  (PUT http://169.254.169.254/latest/api/token → IMDSv2 token)
  ↓  (GET http://169.254.169.254/latest/meta-data/iam/security-credentials/... → 临时凭证)
  ↓
使用临时凭证调用 S3 ListObjects 列举文件
  ↓
文件列表返回，Pending 阶段完成
  ↓
将 TCloudConfiguration 传给 BE，进入 Loading 阶段
```

---

## 第二部分：BE 与 AWS 的交互

### 2.1 触发时机

BE 在 **Loading 阶段**需要从 S3 读取实际数据文件。

调用链：

```
file_scanner.cpp → 检测 use_broker=false
  → FileSystem::CreateUniqueFromString(path, FSOptions(&params))
    → fs.cpp: is_s3_uri() → new_fs_s3(options)
      → S3FileSystem
        → new_s3client(uri, _options)
```

### 2.2 S3 Client 的创建

`new_s3client()` 检测到 `cloud_configuration` 后走 C++ SDK 路径：

```269:282:be/src/fs/fs_s3.cpp
static std::shared_ptr<Aws::S3::S3Client> new_s3client(const S3URI& uri, const FSOptions& opts, ...) {
    Aws::Client::ClientConfiguration config = S3ClientFactory::getClientConfig();
    const THdfsProperties* hdfs_properties = opts.hdfs_properties();
    if ((hdfs_properties != nullptr && hdfs_properties->__isset.cloud_configuration) ||
        (opts.cloud_configuration != nullptr && opts.cloud_configuration->cloud_type != TCloudType::DEFAULT)) {
        const TCloudConfiguration& tCloudConfiguration = ...;
        return S3ClientFactory::instance().new_client(tCloudConfiguration, operation_type);
    }
    // ...
}
```

### 2.3 从 TCloudConfiguration 构建凭证

`S3ClientFactory::new_client()` → `CloudConfigurationFactory::create_aws()`：

```23:55:be/src/fs/credential/cloud_configuration_factory.cpp
const AWSCloudConfiguration CloudConfigurationFactory::create_aws(const TCloudConfiguration& t_cloud_configuration) {
    std::map<std::string, std::string> properties = t_cloud_configuration.cloud_properties;

    AWSCloudCredential aws_cloud_credential{};
    aws_cloud_credential.use_aws_sdk_default_behavior =
            get_or_default(properties, AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, false);
    aws_cloud_credential.use_instance_profile =
            get_or_default(properties, AWS_S3_USE_INSTANCE_PROFILE, false);
    aws_cloud_credential.access_key = get_or_default(properties, AWS_S3_ACCESS_KEY, std::string());
    aws_cloud_credential.secret_key = get_or_default(properties, AWS_S3_SECRET_KEY, std::string());
    // ... 其余字段 ...
}
```

### 2.4 BE 的凭证提供者选择

`_get_aws_credentials_provider()` 根据配置选择 C++ SDK 的 provider：

```80:112:be/src/fs/fs_s3.cpp
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3ClientFactory::_get_aws_credentials_provider(
        const AWSCloudCredential& aws_cloud_credential) {
    if (aws_cloud_credential.use_aws_sdk_default_behavior) {
        credential_provider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    } else if (aws_cloud_credential.use_instance_profile) {
        credential_provider = std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    } else if (!aws_cloud_credential.access_key.empty() && !aws_cloud_credential.secret_key.empty()) {
        credential_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(...);
    }
    // 如果设置了 iam_role_arn，则包装为 STSAssumeRoleCredentialsProvider
    if (!aws_cloud_credential.iam_role_arn.empty()) {
        auto sts = std::make_shared<Aws::STS::STSClient>(credential_provider, clientConfiguration);
        credential_provider = std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(...);
    }
    return credential_provider;
}
```

| 用户配置 | C++ SDK Provider | 是否触发 IMDS |
|----------|-----------------|-------------|
| `use_aws_sdk_default_behavior=true` | `DefaultAWSCredentialsProviderChain` | **是** |
| `use_instance_profile=true` | `InstanceProfileCredentialsProvider` | **是** |
| AK/SK | `SimpleAWSCredentialsProvider` | **否** |

### 2.5 BE 使用的 AWS SDK 版本

```
AWS C++ SDK: 1.11.267
```

**IMDS 行为：**
- SDK 1.11.267 的 `InstanceProfileCredentialsProvider` 默认**先尝试 IMDSv2**（PUT 请求获取 token），如果 IMDSv2 不可用或超时，**回退到 IMDSv1**（直接 GET）
- 可通过环境变量 `AWS_EC2_METADATA_V1_DISABLED=true` 禁用 IMDSv1 回退

### 2.6 额外的 IMDS 触发点：ClientConfiguration 初始化

```67:75:be/src/fs/fs_s3.h
    static ClientConfiguration& getClientConfig() {
        // We cached config here and make a deep copy each time. Since aws sdk has changed the
        // Aws::Client::ClientConfiguration default constructor to search for the region
        // (where as before 1.8 it has been hard coded default of "us-east-1").
        // Part of that change is looking through the ec2 metadata, which can take a long time.
        // For more details, please refer https://github.com/aws/aws-sdk-cpp/issues/1440
        static ClientConfiguration instance;
        return instance;
    }
```

`ClientConfiguration` 的默认构造函数会通过 EC2 metadata 查找 region。这是一个 **static 变量**，只在第一次调用时触发一次 IMDS，后续复用缓存。

### 2.7 BE 的 IMDS 调用时序

```
BE 收到 FE 发来的 scan range（包含 TCloudConfiguration）
  ↓
创建 S3FileSystem → new_s3client()
  ↓
S3ClientFactory::getClientConfig()（首次调用时触发 IMDS 查询 region）
  ↓
CloudConfigurationFactory::create_aws() → 解析 TCloudConfiguration
  ↓
_get_aws_credentials_provider() → InstanceProfileCredentialsProvider
  ↓
首次 S3 请求时，provider 获取凭证
  ↓  (PUT http://169.254.169.254/latest/api/token → 尝试 IMDSv2)
  ↓  (如果失败，回退 GET http://169.254.169.254/... → IMDSv1)
  ↓
使用临时凭证读取 S3 数据
```

---

## 第三部分：完整的 IMDS 触发点汇总

### FE 侧

| # | 触发点 | 代码位置 | 条件 | SDK |
|---|--------|---------|------|-----|
| 1 | Hadoop S3A 文件列举时获取凭证 | `HdfsFsManager.getFileSystemByCloudConfiguration()` → `S3AFileSystem.initialize()` | `use_instance_profile=true` 或 `use_aws_sdk_default_behavior=true` | AWS Java SDK v2 (2.29.52) |

### BE 侧

| # | 触发点 | 代码位置 | 条件 | SDK |
|---|--------|---------|------|-----|
| 1 | `ClientConfiguration` 静态初始化时查询 region | `S3ClientFactory::getClientConfig()` (`fs_s3.h:72`) | 首次调用，无条件 | AWS C++ SDK 1.11.267 |
| 2 | S3 请求时获取凭证 | `_get_aws_credentials_provider()` (`fs_s3.cpp:85-88`) | `use_instance_profile=true` 或 `use_aws_sdk_default_behavior=true` | AWS C++ SDK 1.11.267 |

### 凭证刷新

临时凭证有效期通常为 6 小时（由 IAM Role 配置决定）。但每次新建 S3 Client 或凭证过期时都会重新通过 IMDS 获取。用户的 Broker Load 每 4 小时执行一次，每次都会触发 FE + BE 的 IMDS 调用。

---

## 第四部分：IMDSv1 vs IMDSv2 的行为分析

### 为什么会产生 IMDSv1 调用？

| SDK | IMDS 默认行为 | IMDSv1 何时触发 |
|-----|-------------|---------------|
| AWS Java SDK v2 (2.29.52) | 先 IMDSv2，失败回退 IMDSv1 | IMDSv2 token 请求失败时（如 hop limit 不足） |
| AWS C++ SDK (1.11.267) | 先 IMDSv2，失败回退 IMDSv1 | 同上 |

**关键洞察：** 两个 SDK 都**优先使用 IMDSv2**，但在 IMDSv2 不可用时会**回退到 IMDSv1**。

**可能导致 IMDSv2 失败的原因：**
1. EC2 实例的 `HttpPutResponseHopLimit` 设置为 1（默认值），如果 StarRocks 运行在容器中，IMDSv2 的 PUT 请求到达不了 IMDS endpoint
2. EC2 实例未启用 IMDSv2（`HttpTokens` 设置为 `optional` 而非 `required`），两种版本都可用时，部分日志/监控可能将任何 IMDS 调用都记录为 "v1"
3. 极少数情况：网络策略阻断了 IMDSv2 的 PUT 请求

---

## 第五部分：解决方案

### 方案 1：EC2 实例级别强制 IMDSv2（推荐，无需改代码）

```bash
aws ec2 modify-instance-metadata-options \
    --instance-id i-008e27e68cc111117 \
    --http-tokens required \
    --http-put-response-hop-limit 2
```

- `--http-tokens required`：完全禁用 IMDSv1，所有 IMDS 请求必须走 IMDSv2
- `--http-put-response-hop-limit 2`：如果 StarRocks 运行在容器中，需要增加 hop limit 使 IMDSv2 PUT 请求能到达

**前提**：两个 SDK 版本（Java v2 2.29.52 和 C++ 1.11.267）**都支持 IMDSv2**，所以强制后不会导致凭证获取失败。

### 方案 2：通过环境变量禁用 IMDSv1 回退

**BE 侧（C++ SDK）：**

在 BE 启动脚本中设置：
```bash
export AWS_EC2_METADATA_V1_DISABLED=true
```

**FE 侧（Java SDK v2）：**

在 FE 启动脚本中设置：
```bash
# Java SDK v2 环境变量
export AWS_EC2_METADATA_V1_DISABLED=true
```

或通过 Java 系统属性：
```bash
# 在 fe.conf 中添加
JAVA_OPTS="... -Daws.disableEc2MetadataV1"
```

这样即使 IMDSv2 请求失败，SDK 也不会回退到 IMDSv1，而是直接报错。

### 方案 3：代码层面增加 IMDS 版本控制（需改代码）

**BE 侧：** 在构建 `InstanceProfileCredentialsProvider` 时显式禁用 IMDSv1。

C++ SDK 1.11.267 暂不支持在 provider 构造时指定 IMDS 版本，但可以通过设置 `Aws::Auth::EC2MetadataClient` 的配置实现。更实际的做法是通过环境变量。

**FE 侧：** Hadoop S3A 的 `IAMInstanceCredentialsProvider` 底层使用 AWS Java SDK v2，可以通过 SDK v2 的 builder 配置。但由于 FE 不直接构造 SDK 的 provider（而是通过 Hadoop 间接使用），修改需要在 Hadoop 配置层面进行。

### 方案对比

| 方案 | 修改范围 | 风险 | 覆盖 FE | 覆盖 BE |
|------|---------|------|---------|---------|
| EC2 强制 IMDSv2 | 基础设施 | 低（SDK 支持 v2） | 是 | 是 |
| 环境变量 | FE/BE 启动脚本 | 低 | 是 | 是 |
| 代码修改 | StarRocks 代码 | 中 | 需改 Hadoop 配置 | 需改 C++ 代码 |

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
  │     │
  │     ▼
  │     HdfsFsManager.getFileSystem() → getS3FileSystem() / getS3AFileSystem()
  │     │
  │     ▼
  │     CloudConfigurationFactory.buildCloudConfigurationForStorage(properties)
  │     → AwsCloudConfiguration(AwsCloudCredential{useInstanceProfile=true, ...})
  │     │
  │     ▼
  │     getFileSystemByCloudConfiguration()
  │     │
  │     ├── cloudConfiguration.applyToConfiguration(conf)
  │     │   → conf.set("fs.s3a.aws.credentials.provider", "IAMInstanceCredentialsProvider")
  │     │
  │     ├── FileSystem.get(uri, conf)   ←── Hadoop S3A 初始化
  │     │   └── S3AFileSystem.initialize()
  │     │       └── IAMInstanceCredentialsProvider.resolveCredentials()
  │     │           └── AWS Java SDK v2: InstanceProfileCredentialsProvider
  │     │               └── ★ IMDS 调用 (先 v2, 失败回退 v1)
  │     │
  │     ├── S3AFileSystem.globStatus()  ←── S3 ListObjects
  │     │
  │     └── cloudConfiguration.toThrift(tCloudConfiguration)
  │         → AwsCloudCredential.toThrift(properties)
  │           → properties = {"aws.s3.use_instance_profile":"true", ...}
  │         → tProperties.setCloud_configuration(tCloudConfiguration)
  │
  │
  ▼
FE: Pending 阶段完成 → Loading 阶段 → 构建 scan plan
  │
  ▼
FE: FileScanNode.initParams()
  │
  ├── params.setHdfs_properties(hdfsProperties)  ←── 包含 cloud_configuration
  ├── params.setProperties(brokerDesc.getProperties())
  └── params.setUse_broker(false)
  │
  ▼
FE → BE: 通过 Thrift 发送 TBrokerScanRangeParams
  │
  │
  ├──── BE 读取数据（BE 与 AWS 交互） ────────────────────────────────────────────
  │     │
  │     ▼
  │     file_scanner.cpp: use_broker=false
  │     → FileSystem::CreateUniqueFromString(path, FSOptions(&params))
  │     │
  │     ▼
  │     fs.cpp: is_s3_uri() → new_fs_s3(options)
  │     │
  │     ▼
  │     S3FileSystem → new_s3client(uri, _options)
  │     │
  │     ├── S3ClientFactory::getClientConfig()
  │     │   └── static ClientConfiguration 初始化
  │     │       └── ★ IMDS 调用查询 region（仅首次）
  │     │
  │     ├── 检测到 hdfs_properties->cloud_configuration
  │     │   → S3ClientFactory::new_client(tCloudConfiguration)
  │     │
  │     ├── CloudConfigurationFactory::create_aws(tCloudConfiguration)
  │     │   → AWSCloudCredential{use_instance_profile=true, ...}
  │     │
  │     ├── _get_aws_credentials_provider(aws_cloud_credential)
  │     │   → Aws::Auth::InstanceProfileCredentialsProvider
  │     │
  │     └── S3Client 发起 GetObject 请求时获取凭证
  │         └── InstanceProfileCredentialsProvider.GetAWSCredentials()
  │             └── ★ IMDS 调用 (先 v2, 失败回退 v1)
  │
  ▼
Load 完成
```
