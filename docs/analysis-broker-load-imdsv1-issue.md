# StarRocks Broker Load 与 AWS IMDS 交互分析

## 问题背景

用户在 AWS EC2 上运行 StarRocks 集群，使用 Broker Load 每 4 小时从 S3 加载数据。
安全团队发现该 EC2 实例持续产生 IMDSv1 调用（不安全），且调用峰值与 Broker Load 定时任务同步。

用户使用的是 **role-based 认证**（IAM Role 绑定到 EC2 实例），而非 AK/SK 硬编码。

---

## 核心发现：两条完全不同的代码路径

StarRocks 的 "Broker Load" 有两种执行模式，它们的 S3 认证走的是**完全不同的代码路径**：

### 路径 A：使用 Broker 进程（`WITH BROKER "broker_name"`）

```
SQL: LOAD LABEL ... WITH BROKER "my_broker" ("fs.s3a.access.key"="xxx", ...)
```

**调用链：**
```
FE (FileScanNode)
  → params.setProperties(brokerDesc.getProperties())   // 原样传递用户属性
  → params.setUse_broker(true)
  ↓
BE (file_scanner.cpp)
  → 检测 use_broker=true → 走 BrokerFileSystem (Thrift RPC)
  ↓
Broker 进程 (Java, FileSystemManager.java)
  → getS3AFileSystem() → 只认 fs.s3a.* 属性
  → 使用 Hadoop S3A FileSystem (hadoop-aws 3.4.1)
  → 底层使用 AWS Java SDK v1 获取凭证
```

**关键代码** (`fs_brokers/.../FileSystemManager.java:494-545`)：
```java
public BrokerFileSystem getS3AFileSystem(String path, Map<String, String> properties) {
    String accessKey = properties.getOrDefault("fs.s3a.access.key", "");
    String secretKey = properties.getOrDefault("fs.s3a.secret.key", "");
    String endpoint = properties.getOrDefault("fs.s3a.endpoint", "");
    String awsCredProvider = properties.getOrDefault("fs.s3a.aws.credentials.provider", null);
    // ...
    conf.set("fs.s3a.access.key", accessKey);
    conf.set("fs.s3a.secret.key", secretKey);
    if (awsCredProvider != null) {
        conf.set("fs.s3a.aws.credentials.provider", awsCredProvider);
    }
    FileSystem s3AFileSystem = FileSystem.get(pathUri.getUri(), conf);
}
```

**问题：**
- Broker 进程**不认识** `aws.s3.use_instance_profile`、`aws.s3.use_aws_sdk_default_behavior` 等参数
- 它只认 Hadoop S3A 原生参数 `fs.s3a.*`
- 如果没有设置 `fs.s3a.aws.credentials.provider`，Hadoop S3A 会走默认凭证链
- Hadoop S3A 底层使用 **AWS Java SDK v1**，默认凭证链会通过 IMDS 获取凭证
- **AWS Java SDK v1 默认使用 IMDSv1**

### 路径 B：不使用 Broker 进程（`WITH BROKER` 不指定名称）

```
SQL: LOAD LABEL ... WITH BROKER ("aws.s3.use_instance_profile"="true", ...)
```

**调用链：**
```
FE (FileScanNode)
  → HdfsUtil.getTProperties() → CloudConfigurationFactory
  → 构建 TCloudConfiguration (包含 use_instance_profile 等)
  → params.setHdfs_properties(hdfsProperties)  // 带有 cloud_configuration
  ↓
BE (fs_s3.cpp, C++)
  → new_s3client() → 检测到 cloud_configuration
  → S3ClientFactory::new_client(tCloudConfiguration)
  → _get_aws_credentials_provider()
  → 根据配置选择 InstanceProfileCredentialsProvider / DefaultCredentialsProviderChain 等
  → 使用 AWS C++ SDK 1.11.267
```

**关键代码** (`be/src/fs/fs_s3.cpp:81-112`)：
```cpp
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3ClientFactory::_get_aws_credentials_provider(
        const AWSCloudCredential& aws_cloud_credential) {
    if (aws_cloud_credential.use_aws_sdk_default_behavior) {
        return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    } else if (aws_cloud_credential.use_instance_profile) {
        return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    } else if (!aws_cloud_credential.access_key.empty() && !aws_cloud_credential.secret_key.empty()) {
        return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(...);
    }
    // ...
}
```

---

## 两条路径的对比

| 维度 | 路径 A（有 Broker 进程） | 路径 B（无 Broker 进程） |
|------|-------------------------|-------------------------|
| **执行组件** | 独立 Java Broker 进程 | BE 进程 (C++) |
| **AWS SDK** | AWS Java SDK v1 (via Hadoop S3A) | AWS C++ SDK 1.11.267 |
| **属性前缀** | `fs.s3a.*` | `aws.s3.*` |
| **支持 `use_instance_profile`** | 不支持 | 支持 |
| **支持 `use_aws_sdk_default_behavior`** | 不支持 | 支持 |
| **IMDS 版本控制** | 受 Hadoop/Java SDK 控制，默认 IMDSv1 | 受 C++ SDK 控制 |
| **凭证提供者配置** | `fs.s3a.aws.credentials.provider` | 通过 `aws.s3.*` 参数自动选择 |

---

## 用户场景分析

### 判断用户使用的是哪条路径

用户的 SQL 语句决定了走哪条路径：

```sql
-- 路径 A：有 broker 名称 → 走 Broker 进程
LOAD LABEL db.label (...)
WITH BROKER "broker_name" ("aws.s3.xxx" = "yyy")

-- 路径 B：无 broker 名称 → 走 BE 直接访问
LOAD LABEL db.label (...)
WITH BROKER ("aws.s3.xxx" = "yyy")
```

判断依据是 `BrokerDesc.hasBroker()`（即 `name` 是否非空）。

### 如果用户使用路径 A（Broker 进程）

这是**最可能触发 IMDSv1 的场景**：
1. Broker 是 Java 进程，使用 Hadoop S3A + AWS Java SDK v1
2. 如果没有提供 AK/SK，Hadoop S3A 默认走 `DefaultAWSCredentialsProviderChain`
3. AWS Java SDK v1 的 `InstanceProfileCredentialsProvider` **默认使用 IMDSv1**
4. 用户设置 `aws.s3.use_instance_profile=true` 或 `aws.s3.use_aws_sdk_default_behavior=true` **对 Broker 进程无效**，因为 Broker 不解析这些参数

### 如果用户使用路径 B（无 Broker）

走 BE 的 C++ SDK 路径。AWS C++ SDK 1.11.267 的 `InstanceProfileCredentialsProvider` 和 `DefaultAWSCredentialsProviderChain` 的 IMDS 行为取决于 SDK 版本和环境变量：
- 环境变量 `AWS_EC2_METADATA_V1_DISABLED=true` 可禁用 IMDSv1
- C++ SDK 1.11.267（2024 年初版本）可能默认仍使用 IMDSv1

---

## 解决方案

### 方案 1：让用户改用无 Broker 模式（推荐，零代码改动）

如果用户当前使用的是 `WITH BROKER "broker_name"` 语法，建议切换为：

```sql
LOAD LABEL db.label (
    DATA INFILE ("s3://bucket/path/data.parquet")
    INTO TABLE my_table
) WITH BROKER (
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-east-1"
)
```

注意：`WITH BROKER` 后面**不要**写 broker 名称。这样会走 BE 的 C++ SDK 路径，完全绕开 Java Broker 进程。

**配合环境变量**：在 BE 进程启动前设置 `AWS_EC2_METADATA_V1_DISABLED=true`，强制 C++ SDK 只使用 IMDSv2。

### 方案 2：Broker 进程设置 credential provider（如果必须用 Broker 进程）

在 Broker Load 中显式指定 Hadoop S3A 的凭证提供者：

```sql
LOAD LABEL db.label (
    DATA INFILE ("s3a://bucket/path/data.parquet")
    INTO TABLE my_table
) WITH BROKER "my_broker" (
    "fs.s3a.endpoint" = "s3.us-east-1.amazonaws.com",
    "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
)
```

Hadoop 3.4.1 的 `IAMInstanceCredentialsProvider` 底层使用 AWS Java SDK v1 的 `InstanceProfileCredentialsProvider`。但 AWS Java SDK v1 **仍然默认使用 IMDSv1**。

要让 Java SDK v1 使用 IMDSv2，需要：
- 设置系统属性 `com.amazonaws.sdk.ec2MetadataServiceEndpointMode=IPv4` 配合 `com.amazonaws.sdk.disableEc2MetadataServiceV1=true`（SDK v1 未必支持此配置）
- 或者在 EC2 实例级别强制 IMDSv2（`--http-tokens required`）

### 方案 3：EC2 实例级别强制 IMDSv2（基础设施层面）

```bash
aws ec2 modify-instance-metadata-options \
    --instance-id i-008e27e68cc111117 \
    --http-tokens required \
    --http-put-response-hop-limit 2
```

- `--http-tokens required`：强制所有 IMDS 请求必须走 IMDSv2
- `--http-put-response-hop-limit 2`：如果 StarRocks 运行在容器中，需要增加 hop limit

**风险**：如果 StarRocks 使用的 AWS SDK 版本不支持 IMDSv2，这会导致凭证获取失败。

### 方案 4：代码修复 — 在 Broker 进程中支持 IMDSv2（代码改动）

在 `FileSystemManager.getS3AFileSystem()` 中，增加对 `aws.s3.use_instance_profile` 等参数的识别，并自动转换为对应的 Hadoop S3A 配置：

```java
// FileSystemManager.java - getS3AFileSystem()
// 新增：识别 aws.s3.* 参数并转换
String useInstanceProfile = properties.getOrDefault("aws.s3.use_instance_profile", "false");
if ("true".equalsIgnoreCase(useInstanceProfile) && awsCredProvider == null) {
    awsCredProvider = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider";
}

String useDefaultBehavior = properties.getOrDefault("aws.s3.use_aws_sdk_default_behavior", "false");
if ("true".equalsIgnoreCase(useDefaultBehavior) && awsCredProvider == null) {
    awsCredProvider = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
}
```

但这不能解决 Java SDK v1 默认走 IMDSv1 的问题。

### 方案 5：升级 Broker 中的 AWS Java SDK（长期方案）

将 Broker 的 Hadoop 依赖从 AWS Java SDK v1 切换到 v2（通过 hadoop-aws 4.x 或 AWS SDK Bundle），v2 默认支持 IMDSv2。

---

## 关键结论

1. **根本原因**：如果用户使用了带名称的 Broker（`WITH BROKER "name"`），则走 Java Broker 进程，该进程通过 Hadoop S3A + AWS Java SDK v1 获取凭证，**SDK v1 默认使用 IMDSv1**。

2. **最快解决方案**：让用户切换为无 Broker 模式（去掉 broker 名称），走 BE 的 C++ SDK 路径，配合环境变量 `AWS_EC2_METADATA_V1_DISABLED=true`。

3. **最彻底解决方案**：在 EC2 实例层面强制 IMDSv2（`--http-tokens required`），但需要确认所有 SDK 版本兼容。

4. **代码层面的问题**：`aws.s3.use_instance_profile` 等 StarRocks 自定义参数在 Broker 进程中没有任何效果，因为 Broker 只认 `fs.s3a.*` 参数。这是一个设计上的 gap。
