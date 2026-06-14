## Why

当前生产链路依赖一段手工维护的 `spark_submit.sh` 与单点 `pyspark_driver.py`，配合 DolphinScheduler 3.1.7 的资源中心分发 SQL 文件。这套模式在单任务/小团队场景能跑通，但要支撑"项目目录树 + SQL editor + Spark config + Scheduler"这样面向多租户、多项目的数据开发中台时，会在四个方向同时承压：

1. **版本一致性**：DS 资源中心只持有"当前版本"，用户改完 SQL 立即影响在跑/补数实例，历史回算无法重现当时的代码。
2. **参数注入与多租户安全**：spark-submit 命令在 shell 模板里手工拼接，前端"高级 conf"自由文本是注入面；所有任务共用一个 Kerberos principal，权限审计无法回溯到真实用户。
3. **变量语义漂移**：editor 预览的 SQL、平台渲染后的 SQL、driver 实际执行的 SQL 三处都有可能渲染变量，导致"所见不是所跑"。
4. **状态可见性**：YARN cluster 模式下 driver 日志在 AM 容器，DS Worker 只能看到 spark-submit 客户端日志；用户在前端"看日志"和"停止任务"经常拿不到真状态。

这些问题不是 driver 一层能解决的——必须把"前端编排 → 平台后端物料化 → DS 触发 → driver 执行"整条链路重新切分，让每一层职责单一、边界清晰。

## What Changes

### 新增（前端 / 平台后端）

- 项目目录树、SQL editor（含变量面板与所见即所得预览）、Spark config 表单（结构化字段 + 受控的高级 conf）、Scheduler 配置（定时、依赖、周期、补数）、发布流。
- 平台后端：
  - **项目变量解析**：在发布时把项目级、环境级变量烤进 SQL；运行时变量保持原样。
  - **SQL snapshot 层**：发布即生成不可变 HDFS 物料，按 `(env, task_id, version_id)` 寻址；任务实例在创建时锁定 version_id。
  - **命令生成器**：以白名单方式拼接 spark-submit，严格转义；DS shell 模板退化为 `eval "$SPARK_CMD"`。
  - **DS Open API 集成**：发布同步 workflow/task 到 DS；维护与平台元数据的最终一致性。
  - **任务实例生命周期**：创建、触发、重试、补数、kill 由平台主导，DS 只承担定时/依赖图。
  - **多租户隔离**：按租户路由 Kerberos principal/keytab；queue 提交权限校验。
  - **可观测性**:appId 回写、driver 日志聚合接入、跨层 trace_id。

### 修改

- **pyspark_driver.py**：扩展运行时时间变量集合（`${dt}/${date}/${month}/${dt-N}/${date-N}/${hr}`），承诺一份与前端变量面板对齐的契约；driver 不再承担项目变量渲染。driver 改动保持对老 SQL 的向后兼容。
- **spark_submit.sh**：从"业务逻辑模板"退化为"命令执行器"——`eval "$SPARK_CMD"`，附 kinit、appId 抓取、退出码透传。
- **DS 资源中心**：不再承载用户 SQL 与 driver 物料，仅保留 keytab 等基础设施级资源。

### 移除

- DS 资源中心对用户 SQL 的版本管理职责。
- shell 模板里的 `realpath` 本地落地、`--files` 传 DS Worker 本地路径的链路（改为 `--files hdfs://...snapshot 路径`）。

### 不在本 change 范围内（Non-goals）

- 替换 DolphinScheduler 本身（继续用 3.1.7）。
- Spark 版本升级（继续 Spark 2.4.7 + CDP 7.1.x）。
- 重写 driver 核心执行能力（拆分 SQL、注释处理、Hive 兼容等已就位，本 change 只扩变量集合）。
- 实时/流式作业（仅覆盖批 SQL）。
- BI/数据可视化层。

## Capabilities

### New Capabilities

- `sql-editor`：项目目录树 + 多 tab SQL 编辑、变量面板（标注项目/运行时变量）、所见即所得预览、保存/发布动作。
- `spark-config`：结构化字段（队列、driver/executor 规格、shuffle、动态分配上下限）+ 受控的"高级 conf"白名单文本框。
- `schedule-management`：定时表达式、上下游依赖、周期、补数（含版本选择）、SLA 配置。
- `sql-snapshot`：发布即生成 HDFS 不可变物料；按 `(env, task_id, version_id)` 寻址；任务实例创建时锁定 version_id；补数允许显式选版本。
- `command-generation`：把前端配置编译成 spark-submit 命令字符串；conf 白/黑名单；shell 转义；命令产物可审计。
- `runtime-variable-rendering`：driver 承诺的运行时变量集合（基于 `--biz-date` 派生），与前端变量面板的契约。
- `dolphinscheduler-integration`：通过 DS Open API upsert workflow/task；shell task body 模板化；appId 回写；kill 链路（同时 kill DS task 与 YARN application）。
- `task-instance-lifecycle`：创建/触发/重试/补数/kill 的状态机；与 DS instance 的双向同步；版本锁定语义。
- `multi-tenant-isolation`：按租户路由 principal/keytab；queue 提交权限校验；审计日志归属真实用户。
- `observability`：appId 抓取、YARN 日志聚合接入、driver 结构化日志、跨层 trace_id；前端"看日志"按钮直达 driver 输出。
- `publish-pipeline`：从 editor 草稿到 DS 上线的发布事务；snapshot/DS task/平台元数据三方一致性；失败回滚。

### Modified Capabilities

- `pyspark-driver`：扩展运行时变量集合（兼容老的 `${dt}/${date}/${month}`）；输出结构化日志含 `application_id`；保持对项目变量未渲染情况下的 strict-fail 行为不变。

## Impact

- **Affected users/teams**：数据开发同学（编排层体验全变）；平台运维（DS 资源中心使用方式收敛）；安全/合规（多租户审计链路落地）。
- **Affected modules/services**：前端 web 应用（新建）、平台后端服务（新建）、`pyspark_driver.py`（小幅扩展）、`spark_submit.sh`（瘦身）、DolphinScheduler（接入方式由资源中心改为 Open API）。
- **Affected APIs/events**：新增平台后端 REST API（项目/任务/发布/实例/补数/日志）；DS Open API 调用面（workflow/task/instance）；HDFS snapshot 路径协议。
- **Affected data/storage**：新增平台元数据库（项目、任务、版本、实例、变量定义）；新增 HDFS snapshot 目录布局；DS metadata 与平台 metadata 的一致性边界。
- **Dependencies/infrastructure**：HDFS（snapshot 存储）、YARN（执行）、Hive Metastore（不变）、Ranger/ACL（多租户隔离落地点）、Kerberos（按租户 keytab 路由）、DolphinScheduler 3.1.7 Open API。
- **Security**：spark-submit 命令的 conf 白名单是核心防线；snapshot 路径写入权限按租户隔离；keytab 文件权限收敛；前端高级 conf 文本框需服务端二次校验。
- **Compatibility/migration**：老的"DS 资源中心 + 手工 shell 模板"任务需提供迁移工具，能从现有 SQL 文件 + DS task 导入为平台任务；过渡期允许两种模式并存，但新发布只走新链路。
- **Rollback considerations**：平台后端可灰度发布（按项目开关）；snapshot 物料一旦写入 HDFS 不删除，回滚只需让 DS task 指回旧 snapshot 路径；driver 改动遵循向后兼容原则，可独立回退。
