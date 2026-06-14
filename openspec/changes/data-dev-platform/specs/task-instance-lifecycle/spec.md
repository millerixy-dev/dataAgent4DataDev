## ADDED Requirements

### Requirement: 实例状态机

The system SHALL 维护任务实例的明确状态集合,涵盖 `pending`、`submitting`、`submit_failed`、`running`、`succeeded`、`failed`、`killed`、`waiting_dependency` 等状态,并定义合法的状态迁移路径。

#### Scenario: 正常生命周期

- **WHEN** 一个实例从创建到完成
- **THEN** 状态依次经过 `pending` -> `waiting_dependency`(若有) -> `submitting` -> `running` -> `succeeded`,任何阶段都不能跳过 `submitting`/`running` 直接进入终态

#### Scenario: 非法迁移被拒

- **WHEN** 任何组件尝试将处于 `succeeded`/`failed`/`killed` 终态的实例重新置回 `running`
- **THEN** 平台拒绝该状态变更并记录告警

### Requirement: 实例创建时锁定版本

The system SHALL 在实例创建时刻确定 `(task_id, version_id, biz_date)` 三元组,并在实例后续整个生命周期内保持不变。

#### Scenario: 调度触发创建实例

- **WHEN** 调度器在 T 时刻为任务创建实例
- **THEN** 实例 `version_id` 锁定为 T 时刻该任务的"已发布最新版本",`biz_date` 由 T 与周期推导

#### Scenario: 用户补数创建实例

- **WHEN** 用户提交补数,选择 `biz_date = D` 与 `version_id = V`
- **THEN** 实例三元组锁定为 `(task_id, V, D)`,即便此后已发布版本变化也不影响

### Requirement: 重试不重新选版本

The system SHALL 在重试某实例时,沿用该实例创建时锁定的 `version_id` 与 `biz_date`。

#### Scenario: 自动或手动重试

- **WHEN** 实例从 `failed`/`submit_failed` 进入重试
- **THEN** 重试次数累加,但 `version_id` 与 `biz_date` 保持不变;重试结果决定下一终态

#### Scenario: 自动重试上限

- **WHEN** 自动重试次数达到任务配置上限
- **THEN** 实例进入 `failed` 终态,不再自动重试,等待人工干预

### Requirement: 补数实例与定时实例的区分

The system SHALL 在实例记录中显式区分"定时触发"与"补数触发"来源,前端可按来源筛选。

#### Scenario: 来源标识

- **WHEN** 用户在实例列表按"来源 = 补数"筛选
- **THEN** 列表只展示由补数操作创建的实例

### Requirement: kill 等价语义

The system SHALL 提供"kill 实例"操作,使该实例最终进入 `killed` 终态;若该实例已处于终态则操作无效但不报错。

#### Scenario: kill 运行中实例

- **WHEN** 用户对 `running` 实例发起 kill
- **THEN** 实例最终进入 `killed`,且对应 YARN application 被终止

#### Scenario: kill 终态实例

- **WHEN** 用户对 `succeeded`/`failed`/`killed` 实例发起 kill
- **THEN** 操作被忽略,实例状态不变,前端给出"已为终态"提示
