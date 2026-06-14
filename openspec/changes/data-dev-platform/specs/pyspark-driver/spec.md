## ADDED Requirements

### Requirement: 扩展运行时时间变量集合

The driver SHALL 在原有 `${dt}/${date}/${month}` 基础上,新增 `${dt-N}`、`${date-N}`(`N` 为正整数,无上限)与 `${hr}` 运行时变量。

#### Scenario: 新增变量解析

- **WHEN** SQL 含 `${dt-1}/${date-7}/${dt-30}/${hr}` 等占位符,且 driver 收到 `--biz-date 20260613 --biz-hour 03`
- **THEN** 渲染结果为 `20260612/2026-06-06/20260514/03`,与 [[runtime-variable-rendering]] 中的契约一致

### Requirement: 时区由 driver 参数注入

The driver SHALL 接受时区参数,运行时变量按该时区派生,不再依赖容器本机时区。

#### Scenario: 显式时区参数

- **WHEN** 命令传入 `--timezone Asia/Shanghai`
- **THEN** driver 在该时区下解析 `biz_date` / `biz_hour` 并派生所有时间变量;未传时按平台默认时区

### Requirement: 结构化日志关键字段

The driver SHALL 在启动、读取 SQL、渲染、执行每条 SQL、结束等关键节点,输出含 `application_id`、`trace_id`、`biz_date`、`biz_hour`、`version_id`、当前 SQL 序号等字段的结构化日志行。

#### Scenario: 启动日志

- **WHEN** driver 启动并完成 SparkSession 初始化
- **THEN** 日志至少包含 `application_id`、`trace_id`、`biz_date`、`biz_hour`(若有)、`version_id`、Python 版本与执行入口路径

#### Scenario: 每条 SQL 执行日志

- **WHEN** driver 执行第 i 条 SQL
- **THEN** 日志记录 `i/N`、SQL 哈希(用于与 snapshot 内容核对),执行完成后追加耗时与状态

### Requirement: 向后兼容老调用

The driver SHALL 保持现有命令行参数 `--sql-file` 与 `--biz-date` 的语义不变,新增参数(如 `--biz-hour`、`--timezone`、`--trace-id`、`--version-id`)以可选形式加入,缺省时不破坏老调用。

#### Scenario: 老命令仍可工作

- **WHEN** 仅传入 `--sql-file`、`--biz-date`(同当前生产行为),且 SQL 不含 `${hr}` 等需要新参数的变量
- **THEN** driver 正常执行,新增字段在日志中打印 `unknown` 或为空,不报错

#### Scenario: SQL 用了 ${hr} 但未传 --biz-hour

- **WHEN** 命令未带 `--biz-hour`,SQL 含 `${hr}`
- **THEN** driver 在渲染阶段直接失败,提示需提供 `--biz-hour`

### Requirement: 默认严格模式

The driver SHALL 在未传 `--allow-unresolved-vars` 时,对任意未在运行时变量集合中的 `${...}` 占位符直接失败,且失败信息列出所有未定义变量名。

#### Scenario: SQL 含项目变量未渲染

- **WHEN** 命令未带 `--allow-unresolved-vars`,SQL 中残留 `${prj.foo}` 等本应在发布时被烤入的项目变量
- **THEN** driver 启动 SQL 渲染阶段直接失败,提示该变量未定义,避免错把未渲染语句下发到 spark.sql
