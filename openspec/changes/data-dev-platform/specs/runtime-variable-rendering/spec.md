## ADDED Requirements

### Requirement: 运行时变量集合契约

The system SHALL 由 driver 承诺一份固定的运行时时间变量集合,定义在共享契约文件 `contracts/runtime_variables.yaml`,driver / Backend Variable Resolver / Frontend 变量面板三方读同一份文件作为单一事实源。

#### Scenario: 共享契约文件存在

- **WHEN** driver 启动
- **THEN** driver 从 `contracts/runtime_variables.yaml` 加载变量定义;文件缺失 / schema 非法时启动失败并打印明确错误

#### Scenario: 基础时间变量

- **WHEN** driver 收到 `--biz-date 20260613`
- **THEN** 渲染时 `${dt}` -> `20260613`、`${date}` -> `2026-06-13`、`${month}` -> `202606` 全部按 `biz_date` 派生,不依赖物理时间

#### Scenario: 带偏移的时间变量

- **WHEN** SQL 含 `${dt-N}` 或 `${date-N}` 形式(`N` 为正整数,无上限),`--biz-date 20260613`
- **THEN** `${dt-N}` 渲染为 `(biz_date - N 日)` 的 `yyyyMMdd`;`${date-N}` 渲染为 `yyyy-MM-dd`;`N` 必须是十进制正整数(无前导零除单字符 `0` 外)否则视为未定义变量

#### Scenario: 小时维度变量

- **WHEN** SQL 含 `${hr}`,driver 收到 `--biz-hour 03`
- **THEN** `${hr}` 渲染为两位字符串 `03`(取值范围 `00`~`23`)

#### Scenario: 缺失小时参数

- **WHEN** SQL 含 `${hr}`,但 driver 未收到 `--biz-hour`
- **THEN** driver 启动 SQL 渲染阶段直接失败,提示缺少 `--biz-hour`,不下发任何 `spark.sql`

#### Scenario: 未声明变量严格失败

- **WHEN** SQL 含运行时与项目变量集合都未覆盖的 `${...}` 占位符
- **THEN** 默认 driver 直接报错并终止执行,错误信息列出所有未定义变量;仅在显式开启 `--allow-unresolved-vars` 时保留原样

### Requirement: 渲染语义与预览一致

The system SHALL 保证 driver 实际渲染出的 SQL 与平台前端"预览"展示的 SQL,在相同 `(biz_date, biz_hour)` 与相同 snapshot 下,字符级别一致。

#### Scenario: 预览与实跑对比

- **WHEN** 用户对实例 X 触发"对比",平台分别取 X 锁定的 snapshot 与 driver 在该实例日志中输出的最终 SQL
- **THEN** 两者字符级别一致

### Requirement: 时区一致性

The system SHALL 在 driver 内部按平台声明的时区解释 `(biz_date, biz_hour)`,运行时变量派生不受 driver 容器本机时区影响。

#### Scenario: 跨时区容器运行

- **WHEN** YARN 容器本机时区与平台声明时区不同
- **THEN** `${dt}/${date}/${month}/${dt-N}/${date-N}/${hr}` 等变量值仅依赖 `--biz-date` / `--biz-hour` 与平台时区,与容器本机时区无关
