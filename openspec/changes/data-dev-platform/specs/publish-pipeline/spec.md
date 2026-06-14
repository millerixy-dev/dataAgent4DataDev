## ADDED Requirements

### Requirement: 发布是原子动作

The system SHALL 让"发布"在用户视角是一个原子动作,要么全部生效(snapshot 写入 + DS upsert + 平台 metadata 落库),要么全部回滚至发布前状态。

#### Scenario: 全链路成功

- **WHEN** snapshot 写入成功、DS upsert 成功、metadata 落库成功
- **THEN** 任务的"已发布版本"前进到新 version_id,前端立即可见

#### Scenario: 任一环节失败

- **WHEN** 三个步骤中任意一步失败
- **THEN** 平台不把新 version_id 标记为"已发布",失败原因明确返回给用户;snapshot 即便已写入也不会被任何调度引用

### Requirement: 发布前置校验

The system SHALL 在发布前执行一组校验:SQL 语法可解析、变量集合在变量字典中、Spark 配置在白名单内、队列与 keytab 权限有效。

#### Scenario: 校验全部通过

- **WHEN** 所有前置校验通过
- **THEN** 进入实际发布流程

#### Scenario: 校验失败

- **WHEN** 任一前置校验失败
- **THEN** 发布被拒,前端展示具体失败项,不产生 snapshot 也不调用 DS

### Requirement: 发布幂等

The system SHALL 让"发布"操作具备幂等性,客户端在网络抖动时重试不会产生多余的 DS task 或重复触发实例。

#### Scenario: 客户端重试

- **WHEN** 同一发布请求因超时被客户端重试
- **THEN** 服务端用幂等键识别为同一发布,不重复创建 snapshot 引用与 DS task

### Requirement: 回滚到历史版本

The system SHALL 提供"回滚到指定历史版本"操作,让任务"已发布版本"指向某个历史 version_id。

#### Scenario: 回滚

- **WHEN** 用户从历史版本列表选择 V_old 并确认回滚
- **THEN** 任务的"已发布版本"指针更新为 V_old,后续新建的实例锁定 V_old;运行中的实例不受影响

#### Scenario: 回滚不删除

- **WHEN** 任意时机执行回滚
- **THEN** 所有历史 version_id 与对应 snapshot 仍然保留,可被再次回滚或被补数引用

### Requirement: 灰度发布

The system SHALL 支持按项目/租户灰度开启新发布链路,允许新旧两套链路在过渡期共存。

#### Scenario: 灰度开关

- **WHEN** 项目 P 未开启新链路
- **THEN** 该项目的发布走旧 DS 资源中心模式;开启后,后续发布走新 snapshot 模式;已经发布的旧任务不被强制迁移
