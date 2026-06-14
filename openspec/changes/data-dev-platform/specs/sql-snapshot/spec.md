## ADDED Requirements

### Requirement: 发布即生成不可变快照

The system SHALL 在用户每次"发布"成功时,生成一份按 `(env, task_id, version_id)` 寻址的 SQL 物料,写入 HDFS 后不可修改、不可删除。

#### Scenario: 用户发布任务

- **WHEN** 用户点击"发布"且校验通过
- **THEN** 系统将渲染过项目变量的 SQL 文本写入 HDFS,得到唯一的 `version_id`,并将 `(task_id, version_id)` 与作者、发布时间一同记录到平台元数据

#### Scenario: 发布同一份草稿两次

- **WHEN** 用户连续两次点击"发布",且 SQL 与项目变量值都未变化
- **THEN** 系统仍然生成新的 `version_id`,旧版本保留;`version_id` 单调不重用

#### Scenario: 已写入的 snapshot 不可改

- **WHEN** 任意系统组件或用户操作尝试覆盖、修改或删除已写入的 snapshot 路径
- **THEN** 操作被拒绝,平台保证历史版本可重复读取

### Requirement: 项目变量发布时锁定

The system SHALL 在生成 snapshot 前,使用发布时刻的项目变量值渲染 SQL,渲染结果写入 snapshot;运行时变量保持原样占位,等待 driver 渲染。

#### Scenario: 项目变量替换

- **WHEN** 草稿 SQL 含 `${prj.warehouse}` 而该项目变量值为 `s3://prod/dwh`
- **THEN** snapshot 中该处被替换为 `s3://prod/dwh`,不再含 `${prj.warehouse}`

#### Scenario: 运行时变量保留

- **WHEN** 草稿 SQL 含 `${dt}`、`${date}` 等运行时变量
- **THEN** snapshot 中这些占位符原样保留,由 driver 在执行时按 `--biz-date` 渲染

### Requirement: 任务实例锁定 version_id

The system SHALL 在创建任务实例(定时触发或补数)时,把当前已发布的或用户显式选择的 `version_id` 写入实例记录;实例之后的所有运行/重试都使用该 `version_id`。

#### Scenario: 定时触发实例

- **WHEN** 调度器在 T 时刻为任务创建实例
- **THEN** 实例记录写入"创建时刻已发布版本"的 `version_id`;同一实例的所有重试都引用该 `version_id`

#### Scenario: 用户在补数时选择历史版本

- **WHEN** 用户为某 `biz_date` 创建补数实例并选择 `version_id = V`
- **THEN** 实例记录中固化 `version_id = V`,即便此时已发布的最新版本是 V'

### Requirement: snapshot 可读性

The system SHALL 提供按 `version_id` 检索 snapshot 内容的能力,支持前端"查看实例所跑 SQL"。

#### Scenario: 用户从实例打开"所跑 SQL"

- **WHEN** 用户在实例详情页点击"所跑 SQL"
- **THEN** 系统读取该实例锁定 `version_id` 对应的 snapshot 内容并展示
