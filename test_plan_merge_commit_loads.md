# Merge Commit 导入在 information_schema.loads 中的测试计划

## 测试目标

验证 merge commit 导入任务可以通过 `information_schema.loads` 查询到，并且各字段符合预期。测试覆盖以下场景：
1. 导入成功场景 - 验证成功导入的记录可以在 loads 表中查到，各字段符合预期
2. 数据质量失败场景 - 验证因数据质量问题失败的导入可以在 loads 表中查到，各字段符合预期，并且可以通过 tracking sql 查询到错误数据
3. Profile 场景 - 验证开启 profile 后，成功导入可以通过 loads 表找到 profile id，并且可以通过 get_query_profile 获取 profile

## 测试文件

- **测试文件路径**: `test/sql/test_stream_load/T/test_merge_commit_loads`
- **预期结果文件**: `test/sql/test_stream_load/R/test_merge_commit_loads`

## 测试场景详细设计

### 场景 1: 导入成功场景

**目标**: 验证成功导入的记录可以在 `information_schema.loads` 中查到，各字段符合预期

**测试步骤**:
1. 创建测试数据库和表
2. 执行 merge commit stream load 导入（同步模式，确保导入完成）
3. 等待导入完成（使用 sync 或 sleep）
4. 查询 `information_schema.loads` 表，验证以下字段：
   - `TYPE` = 'MERGE_COMMIT'
   - `STATE` = 'FINISHED'
   - `DB_NAME` = 测试数据库名
   - `TABLE_NAME` = 测试表名
   - `USER` = 'root'（或实际用户）
   - `LABEL` 不为空
   - `ID` (JOB_ID) 不为空
   - `SCAN_ROWS` >= 0（根据实际导入数据量）
   - `SINK_ROWS` >= 0（根据实际导入数据量）
   - `FILTERED_ROWS` = 0（成功导入应该没有过滤行）
   - `UNSELECTED_ROWS` = 0
   - `SCAN_BYTES` >= 0
   - `CREATE_TIME`, `LOAD_START_TIME`, `LOAD_COMMIT_TIME`, `LOAD_FINISH_TIME` 不为空且时间顺序正确
   - `PROGRESS` 包含 "Merge Window" 字样
   - `ERROR_MSG` 为空或为成功消息
   - `TRACKING_SQL` 为空（成功导入没有错误追踪）
   - `PROFILE_ID` 可能为空（如果未开启 profile）
   - `PRIORITY` = 'NORMAL'
   - `PROPERTIES` 为 JSON 格式，包含 merge commit 相关参数

**验证数据**:
- 验证表中有正确的数据行数
- 验证 loads 表中的统计信息与实际导入数据一致

### 场景 2: 数据质量失败场景

**目标**: 验证因数据质量问题失败的导入可以在 `information_schema.loads` 中查到，各字段符合预期，并且可以通过 tracking sql 查询到错误数据

**测试步骤**:
1. 创建测试数据库和表（例如：表有 NOT NULL 约束的列）
2. 执行 merge commit stream load 导入包含错误数据的请求（例如：包含 NULL 值到 NOT NULL 列，或类型不匹配的数据）
3. 设置 `max_filter_ratio: 0` 确保数据质量问题会导致导入失败
4. 等待导入完成（使用 sync）
5. 查询 `information_schema.loads` 表，验证以下字段：
   - `TYPE` = 'MERGE_COMMIT'
   - `STATE` = 'CANCELLED'（失败状态）
   - `DB_NAME` = 测试数据库名
   - `TABLE_NAME` = 测试表名
   - `ERROR_MSG` 不为空，包含错误信息
   - `TRACKING_SQL` 不为空，格式为 `SELECT tracking_log FROM information_schema.load_tracking_logs WHERE JOB_ID=<taskId>`
   - `SCAN_ROWS` >= 0（扫描的行数）
   - `FILTERED_ROWS` > 0（有过滤的行）
   - `SINK_ROWS` = 0（失败导入没有成功写入的行）
   - `CREATE_TIME`, `LOAD_START_TIME`, `LOAD_FINISH_TIME` 不为空
   - `LOAD_COMMIT_TIME` 可能为空（如果事务未提交）
6. 使用 `TRACKING_SQL` 中的 SQL 查询 `information_schema.load_tracking_logs`，验证：
   - 可以查询到错误日志
   - 错误日志包含具体的错误信息（例如：NULL value in non-nullable column）
   - 错误日志包含出错的数据行信息

**验证数据**:
- 验证表中没有错误数据被写入
- 验证 tracking_logs 中的错误信息准确描述了数据质量问题

### 场景 3: Profile 场景

**目标**: 验证开启 profile 后，成功导入可以通过 loads 表找到 profile id，并且可以通过 get_query_profile 获取 profile

**测试步骤**:
1. 创建测试数据库和表
2. 开启表的 profile 功能：`alter table <table_name> set('enable_load_profile'='true');`
3. 设置 FE 配置以收集 profile：`ADMIN SET FRONTEND CONFIG ("stream_load_profile_collect_threshold_second" = "1");`
4. 执行 merge commit stream load 导入（确保执行时间超过阈值，以便收集 profile）
5. 等待导入完成（使用 sync）
6. 查询 `information_schema.loads` 表，验证：
   - `PROFILE_ID` 不为空
   - `PROFILE_ID` 格式正确（应该是 query_id 格式）
   - `STATE` = 'FINISHED'
7. 使用 `get_query_profile(PROFILE_ID)` 函数查询 profile，验证：
   - 可以成功获取 profile 内容
   - Profile 内容不为空
   - Profile 包含预期的执行信息（例如：Query, Summary, Execution Profile 等）
8. 恢复 FE 配置：`ADMIN SET FRONTEND CONFIG ("stream_load_profile_collect_threshold_second" = "0");`

**验证数据**:
- 验证 profile 内容包含 merge commit 相关的执行信息
- 验证可以通过 profile 分析导入性能

## 测试数据设计

### 成功场景测试数据
- 使用简单的 JSON 格式数据：`{"id":1,"name":"test1","score":100}`
- 确保数据符合表结构要求

### 失败场景测试数据
- 使用包含 NULL 值的数据（针对 NOT NULL 列）
- 或使用类型不匹配的数据（例如：字符串到 INT 列）
- 确保数据质量问题会导致导入失败

### Profile 场景测试数据
- 使用足够的数据量，确保执行时间超过 profile 收集阈值
- 可以使用多条记录或较大的数据

## 测试表结构设计

建议使用以下表结构（覆盖常见场景）：

```sql
CREATE TABLE `test_table`
(
    `id` int(11) NOT NULL COMMENT "ID",
    `name` varchar(65533) NULL COMMENT "名称",
    `score` int(11) NOT NULL COMMENT "分数"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
 "replication_num" = "1"
);
```

## 注意事项

1. **时间同步**: 使用 `sync` 确保导入操作完成后再查询 loads 表
2. **状态转换**: 注意 merge commit 的状态转换，成功导入最终状态为 `FINISHED`
3. **Profile 收集**: Profile 收集需要满足时间阈值，确保测试数据量足够
4. **错误追踪**: 数据质量失败时，需要验证 `TRACKING_SQL` 可以正确查询到错误日志
5. **字段验证**: 重点关注 `toThrift()` 方法中设置的字段，确保与 loads 表中的字段对应
6. **并发测试**: 如果需要测试并发场景，可以使用 `CONCURRENCY` 块

## 预期测试结果格式

测试结果文件应包含：
- 每个查询的预期结果
- 对于动态字段（如 ID、时间戳），使用 `[REGEX]` 进行模糊匹配
- 对于可能为空的字段，允许为空值
- 对于 JSON 字段，验证格式正确性

## 测试覆盖范围

- ✅ MergeCommitTask.toThrift() 方法返回的所有字段
- ✅ 成功导入场景的所有字段验证
- ✅ 失败导入场景的错误处理和追踪
- ✅ Profile 功能的集成验证
- ✅ information_schema.loads 表的查询功能
- ✅ information_schema.load_tracking_logs 表的查询功能
- ✅ get_query_profile 函数的使用

## 后续扩展

如果需要，可以添加以下测试场景：
- 异步模式（merge_commit_async=true）的测试
- 并发导入场景的测试
- 不同数据格式（CSV、JSON）的测试
- 不同状态转换的测试（PENDING -> LOADING -> FINISHED）
