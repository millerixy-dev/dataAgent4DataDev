## ADDED Requirements

### Requirement: 通过 Open API 同步 workflow 与 task

The system SHALL 在用户发布任务时,通过 DolphinScheduler Open API upsert 对应的 workflow 与 task,不依赖 DS Web UI 或人工配置。

#### Scenario: 首次发布

- **WHEN** 用户首次发布某任务
- **THEN** 平台调用 DS Open API 创建对应的 workflow 与 shell task,task body 模板为 `eval "$SPARK_CMD"` 加 kinit、appId 抓取与退出码透传

#### Scenario: 再次发布

- **WHEN** 用户再次发布同一任务,且配置或 SQL 已变更
- **THEN** 平台 upsert 同一 DS task,DS metadata 与平台 metadata 保持单一映射关系,不在 DS 中产生孤儿 task

### Requirement: shell task 模板最小化

The system SHALL 让 DS shell task 仅承担执行职责,不在 task body 中拼接 spark-submit 参数、不在 body 中嵌入业务逻辑。

#### Scenario: task body 内容

- **WHEN** 任意发布后的 DS task 被打开查看
- **THEN** 其 body 仅包含 set 选项、kinit、`eval "$SPARK_CMD"`、appId 抓取、退出码透传等执行性步骤,所有业务字段都从平台注入的环境变量来源

### Requirement: appId 回写

The system SHALL 在 spark-submit 客户端日志中抓取 YARN application_id,并写回到平台对应的实例记录。

#### Scenario: 提交成功

- **WHEN** spark-submit 成功向 YARN 提交并打印 `application_xxx`
- **THEN** shell task 解析出该 application_id,通过平台回调或日志结构化字段写回实例;前端实例详情可直达 application_id

#### Scenario: 提交失败

- **WHEN** spark-submit 在提交阶段就失败,无 application_id 产出
- **THEN** 实例 application_id 字段保持空,实例状态为"提交失败",前端展示客户端日志而非 application 日志

### Requirement: kill 联动

The system SHALL 在用户从前端 kill 实例时,先 kill DS task,再 kill 对应的 YARN application,确保两侧都终止。

#### Scenario: 实例已拿到 application_id

- **WHEN** 用户对运行中的实例点击"停止"
- **THEN** 平台先调 DS Open API kill 对应 DS instance,再调 `yarn application -kill` 终止 application;两步任何一步失败都明确告警,不留半运行状态

#### Scenario: 实例尚未拿到 application_id

- **WHEN** 实例处于"提交中",尚未抓到 application_id
- **THEN** 平台仅 kill DS task;后续若 spark-submit 仍上去,平台监测到 application_id 后立即追加 kill

### Requirement: DS metadata 与平台 metadata 一致性

The system SHALL 保证 DS 中存在的 workflow/task 与平台中"已发布"任务一一对应,定期校验并修复不一致。

#### Scenario: DS 侧出现孤儿 task

- **WHEN** 巡检发现 DS 中存在未在平台 metadata 注册的 task
- **THEN** 系统告警并允许运维以"删除/挂靠"两种方式修复,不静默删除

#### Scenario: 平台侧已发布但 DS 未同步成功

- **WHEN** 巡检发现平台某发布版本未在 DS 落地
- **THEN** 系统自动重试一次同步;仍失败则告警,该任务实例不被触发
