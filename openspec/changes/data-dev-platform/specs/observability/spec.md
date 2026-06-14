## ADDED Requirements

### Requirement: appId 与实例双向关联

The system SHALL 在实例记录中保存 YARN application_id,并允许通过 application_id 反查实例。

#### Scenario: 从实例查 application_id

- **WHEN** 用户打开实例详情
- **THEN** 详情页明确展示 application_id(或"提交失败,无 application_id"),提供跳转 YARN UI 的链接

#### Scenario: 从 application_id 查实例

- **WHEN** 运维通过 API 用 application_id 查实例
- **THEN** 平台返回唯一对应的实例记录

### Requirement: 前端"看日志"直达 driver 输出

The system SHALL 让用户在前端实例详情中查看 driver 容器的 stdout/stderr 日志,而不仅是 spark-submit 客户端日志。

#### Scenario: 实例运行中查看日志

- **WHEN** 实例处于 `running`,用户点击"看日志"
- **THEN** 前端展示 driver 容器实时(或近实时)日志,包含 driver 启动信息、SQL 渲染结果、`spark.sql` 调用进度

#### Scenario: 实例已完成查看日志

- **WHEN** 实例终态后用户查看日志
- **THEN** 平台从 YARN 日志聚合获取完整 driver 日志并展示,与运行中查看的内容连续可读

### Requirement: trace_id 跨层贯穿

The system SHALL 为每次任务实例生成 trace_id,并在平台 API、命令字符串、driver 日志、DS task 日志中保留同一 trace_id。

#### Scenario: 故障追踪

- **WHEN** 一次实例失败,运维拿到任意一层(平台 API/DS/driver)的日志片段
- **THEN** 可通过 trace_id 关联同实例的其他层日志,形成完整链路

### Requirement: driver 结构化日志

The system SHALL 让 pyspark_driver 在关键节点输出结构化字段(application_id、trace_id、biz_date、version_id、当前 SQL 序号),便于平台日志聚合检索。

#### Scenario: SQL 执行节点日志

- **WHEN** driver 执行第 i 条 SQL
- **THEN** 日志中至少打印 trace_id、application_id、biz_date、version_id、SQL 序号 i 与总数 N、SQL 内容片段或哈希

### Requirement: 命令产物可重放

The system SHALL 把每次实例触发使用的命令字符串存档,便于线下重放。

#### Scenario: 重放历史实例

- **WHEN** 运维选择某历史实例并点击"复制命令"
- **THEN** 平台返回该实例当时的完整命令字符串(脱敏 keytab 等敏感字段),可在测试环境直接执行
