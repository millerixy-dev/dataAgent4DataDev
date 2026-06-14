## ADDED Requirements

### Requirement: 命令完全由后端生成

The system SHALL 在调度链路上,把 spark-submit 命令字符串完全由平台后端生成;DolphinScheduler shell task 仅承担执行,不参与命令拼接。

#### Scenario: 后端生成命令并下发

- **WHEN** 任务实例需触发
- **THEN** 平台后端生成完整的 spark-submit 命令字符串,作为变量 `SPARK_CMD` 注入 DS task,DS shell 模板仅以 `eval "$SPARK_CMD"` 执行

#### Scenario: 命令字符串可审计

- **WHEN** 实例已创建
- **THEN** 平台元数据记录该实例使用的命令字符串原文,便于审计与重放

### Requirement: conf 白名单

The system SHALL 维护 spark-submit `--conf` 的白名单,只有白名单内的 key 才会被合入最终命令。

#### Scenario: 白名单 key 正常合入

- **WHEN** 用户配置 `spark.sql.shuffle.partitions=400`,且 `spark.sql.*` 在白名单内
- **THEN** 该 conf 被合入最终命令的 `--conf spark.sql.shuffle.partitions=400`

#### Scenario: 黑名单 key 拒绝合入

- **WHEN** 用户配置含 `spark.driver.extraJavaOptions`、`spark.executor.extraJavaOptions`、`spark.driver.extraClassPath`、`spark.yarn.dist.*`、`spark.kerberos.*` 中任一黑名单 key
- **THEN** 命令生成阶段拒绝该 conf,返回错误并不下发实例

### Requirement: shell 严格转义

The system SHALL 在生成命令字符串时,对所有可能含 shell 元字符的字段(队列名、SQL 路径、principal、conf 值、应用名、`biz_date`)做严格转义。

#### Scenario: conf 值含特殊字符

- **WHEN** 用户提交 `spark.app.name="my app; ls /"`
- **THEN** 最终命令中该值被单引号包裹并转义内嵌单引号,shell 解析时不会拆分出额外命令

#### Scenario: 命令的执行边界稳定

- **WHEN** 任意字段含换行、反引号、`$()` 等
- **THEN** 命令执行结果仅包含 spark-submit 调用本身,不会触发任何额外子进程或文件读写

### Requirement: 命令产物含必要参数

The system SHALL 保证生成的命令至少包含:`--master yarn --deploy-mode cluster`、租户对应的 `--principal` 与 `--keytab`、`--queue`、`--name`、白名单 conf、`--files <hdfs snapshot 路径>`、driver 入口、`--sql-file <basename>`、`--biz-date <yyyyMMdd>`。

#### Scenario: 命令产物结构

- **WHEN** 平台后端为某实例生成命令
- **THEN** 命令字符串可被无歧义解析为上述参数集合,缺一项即视为生成失败,不下发实例
