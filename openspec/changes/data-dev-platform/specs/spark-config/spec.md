## ADDED Requirements

### Requirement: 结构化 Spark 配置字段

The system SHALL 提供结构化的 Spark 资源与执行配置表单字段，包括 YARN 队列、driver 内存、driver 核数、executor 内存、executor 核数、executor 数量上下限、shuffle 分区数、动态分配开关。

#### Scenario: 用户填写资源配置

- **WHEN** 用户在表单中设定各资源字段并保存
- **THEN** 系统校验字段类型与取值范围,通过后将其作为任务配置的一部分持久化

#### Scenario: 用户填写非法资源值

- **WHEN** 用户输入超出系统允许范围的内存或并发数
- **THEN** 表单立即在该字段下提示具体边界与原因,阻止保存

### Requirement: 队列提交权限校验

The system SHALL 在保存与发布两个时机校验当前用户/租户对所选 YARN 队列的提交权限。

#### Scenario: 用户选择有权限的队列

- **WHEN** 用户保存或发布任务时所选队列在该用户/租户的授权列表中
- **THEN** 操作通过

#### Scenario: 用户选择无权限的队列

- **WHEN** 用户保存或发布任务时所选队列不在授权列表中
- **THEN** 系统拒绝该操作并返回权限错误,任务不进入"已发布"状态

### Requirement: 高级 conf 受控输入

The system SHALL 在 Spark 配置区提供"高级 conf"文本框,允许用户提交额外的 `spark.*` 配置,但服务端按白名单/黑名单二次校验后才接受。

#### Scenario: 用户提交白名单内的高级 conf

- **WHEN** 用户在高级 conf 输入仅包含白名单 key 的若干键值对并保存
- **THEN** 系统接受并合并到任务配置中,在最终命令中按结构化方式渲染

#### Scenario: 用户提交黑名单 key 的高级 conf

- **WHEN** 用户提交包含 `spark.driver.extraJavaOptions`、`spark.executor.extraJavaOptions`、`spark.driver.extraClassPath`、`spark.yarn.dist.*`、`spark.kerberos.*` 等黑名单 key
- **THEN** 系统拒绝保存,提示具体被拒的 key 与原因

#### Scenario: 高级 conf 含 shell 元字符

- **WHEN** 用户提交的 conf 值中含 shell 特殊字符(`;`、`` ` ``、`$()`、换行等)
- **THEN** 这些字符在最终命令生成时被严格转义,不会改变 shell 解析结构

### Requirement: 配置默认值与模板

The system SHALL 为每个项目/租户提供 Spark 配置的默认值模板,新建任务时表单预填默认值。

#### Scenario: 新建任务时加载默认值

- **WHEN** 用户在某项目下新建任务并打开 Spark 配置表单
- **THEN** 表单各字段以该项目/租户的默认值预填,用户可覆盖
