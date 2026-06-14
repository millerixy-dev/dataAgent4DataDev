## ADDED Requirements

### Requirement: 定时调度配置

The system SHALL 允许用户为任务配置 cron 风格的定时表达式,支持按日、按小时、按月等常见周期。

#### Scenario: 用户配置每日定时任务

- **WHEN** 用户填写合法的 cron 表达式并保存
- **THEN** 系统校验表达式合法性后持久化,发布后该任务按该 cron 由调度器触发

#### Scenario: 用户填写非法 cron

- **WHEN** 用户填写无法解析的 cron 表达式
- **THEN** 表单立即提示语法错误,阻止保存

### Requirement: 上下游依赖配置

The system SHALL 允许用户为任务声明上游依赖,依赖以"任务 + 业务日期偏移"的方式表达。

#### Scenario: 用户声明跨项目上游依赖

- **WHEN** 用户为任务 B 声明依赖任务 A 的同 `biz_date` 实例
- **THEN** B 的实例在触发前必须等待 A 同 `biz_date` 实例为成功态

#### Scenario: 上游缺失或失败时的下游行为

- **WHEN** 上游 A 在指定 `biz_date` 实例为失败、未生成或被取消
- **THEN** 下游 B 的对应实例不被触发,状态停留在"等待依赖"

### Requirement: 补数任务

The system SHALL 提供按 `biz_date` 区间触发补数的能力,补数允许选择"使用当前已发布版本"或"使用指定历史版本"。

#### Scenario: 用户对指定区间补数

- **WHEN** 用户选择 `biz_date` 区间 [D1, D2] 与版本来源,提交补数
- **THEN** 系统为区间内每个 `biz_date` 创建一个任务实例,并按用户选择锁定 version_id

#### Scenario: 补数串行/并行选项

- **WHEN** 用户在补数提交界面选择串行或并行执行模式
- **THEN** 系统按所选模式调度区间内的实例,不混用模式

### Requirement: SLA 与告警

The system SHALL 允许用户为任务配置 SLA 截止时间与告警接收人。

#### Scenario: 任务超时

- **WHEN** 任务实例运行时长超过配置的 SLA 截止时间
- **THEN** 系统向配置的接收人发送告警,实例继续运行直到结束或被 kill

### Requirement: 周期与时区

The system SHALL 在调度配置中明确周期类型与时区,所有 `biz_date` 与定时表达式按该时区解释。

#### Scenario: 周期类型与运行时变量映射

- **WHEN** 用户选择"日级"周期
- **THEN** 实例的 `biz_date` 派生出 `${dt}/${date}` 等日级运行时变量;月级周期同理派生出 `${month}` 等
