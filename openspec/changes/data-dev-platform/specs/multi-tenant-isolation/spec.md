## ADDED Requirements

### Requirement: 按租户路由 Kerberos 身份

The system SHALL 维护"租户 -> Kerberos principal 与 keytab"的映射,任务实例提交时按所属租户选择对应 principal/keytab,不同租户不共用一个 principal。

#### Scenario: 不同租户独立提交身份

- **WHEN** 租户 A 与租户 B 各自提交任务
- **THEN** A 的实例命令使用 A 的 principal/keytab,B 的实例命令使用 B 的;Hive/HDFS 审计日志中可分别看到 A、B 的操作主体

#### Scenario: 租户缺失映射

- **WHEN** 任务关联的租户没有有效的 principal/keytab 映射
- **THEN** 命令生成阶段直接失败,实例不下发

### Requirement: 队列提交权限

The system SHALL 在两个时机校验当前用户/租户对所选 YARN queue 的提交权限:任务保存与发布、实例触发。

#### Scenario: 发布时校验

- **WHEN** 用户保存或发布任务时所选 queue 不在租户授权列表
- **THEN** 操作被拒绝,提示具体队列与原因

#### Scenario: 触发时复核

- **WHEN** 实例触发瞬间,租户的 queue 授权已被收回
- **THEN** 实例提交被拒绝,状态进入 `submit_failed`,不退化为默认队列

### Requirement: 审计归属

The system SHALL 在实例记录中保存"提交人"(平台用户)、"提交租户"、"使用 principal" 三个字段,并写入结构化日志,便于审计回溯。

#### Scenario: 审计反查

- **WHEN** 审计需查询"租户 X 在区间 [T1,T2] 内的所有实例"
- **THEN** 平台可基于上述字段在元数据中精确返回结果,字段不被前端 UI 修改

### Requirement: keytab 文件保护

The system SHALL 保证 keytab 文件仅被授权进程读取,不在 DS 资源中心明文存放、不在日志中泄露绝对路径以外的内容。

#### Scenario: 日志泄露防护

- **WHEN** shell task 在执行期间打印 set -x 等调试信息
- **THEN** keytab 文件内容、Kerberos token 不出现在日志;仅文件路径与 principal 名可见

### Requirement: 跨租户资源隔离

The system SHALL 保证 SQL snapshot 路径、平台 metadata 与日志按租户划分访问域,租户不能读取其他租户的 snapshot 或实例日志。

#### Scenario: 跨租户读取被拒

- **WHEN** 租户 A 的用户尝试通过 API 或 UI 查看租户 B 的 snapshot 或实例
- **THEN** 系统返回权限错误,不暴露目标存在与否的信息
