## Context

把"前端编辑 + DS 调度 + spark-submit + 文件分发"四件套整合成一个数据开发中台。当前 `pyspark_driver.py` + `spark_submit.sh` + DS 资源中心模式在单团队跑得通,但要支撑多租户、多项目、补数可重放、可审计的规模化场景,必须重新切分各层职责:让 driver 极薄、让命令生成与物料管理在平台后端完成、让 DS 退化为定时器与依赖图。

### Related Design Artifacts

- [Proposal](./proposal.md)
- [Architecture](./architecture.md)
- [Data Flow](./data-flow.md)
- [Runtime Flow](./runtime-flow.md)
- Delta specifications: `./specs/`

## Goals / Non-Goals

**Goals**:

- 编辑器预览、平台烤入、driver 渲染三处的变量语义严格一致("所见即所跑")。
- 发布即生成不可变 HDFS snapshot,补数与回滚靠 version\_id 寻址。
- spark-submit 命令字符串完全由后端生成、严格转义、conf 走白名单。
- DS 仅承担定时器/依赖图,不持有用户物料。
- 多租户身份按 principal/keytab 路由,审计可追溯到提交人与租户。
- 跨层 trace\_id 贯穿,前端"看日志"可直达 driver 输出。
- 现有 `pyspark_driver.py` 兼容老调用,平滑升级。
- 老任务可平滑迁移(灰度),新发布走新链路。

**Non-Goals**:

- 替换 DolphinScheduler、Spark、CDP 版本。
- 实时/流任务、BI 可视化。
- 自动化数据质量检查。
- 表级 lineage 自动抓取(数据治理平台承担)。
- 重写 driver 已稳定的拆分/注释处理/UTF-8 兼容能力。

## Constraints and Invariants

源自上游 artifact,在此固化为不可违反约束。设计与实现必须服从。

1. **DS 不持有用户物料**:用户 SQL 与 driver py 都来自 HDFS snapshot;DS 资源中心只留 keytab 等基础设施级文件。
2. **snapshot 写入即不可变**:回滚靠"重新指针",不靠"覆盖"或"删除"。
3. **实例三元组** **`(task_id, version_id, biz_date)`** **创建时锁定**:重试不重选版本,kill 不变更三元组。
4. **命令字符串只能由后端生成**:DS shell body 唯一动作是 `eval "$SPARK_CMD"` 加 kinit/appId 抓取/退出码透传。
5. **driver 严格向后兼容**:driver 升级不能让历史 snapshot 跑挂。
6. **principal 不跨租户**:命令生成阶段保证 1:1 路由。
7. **跨层 trace\_id 贯穿**:Frontend → Backend → DS task body → driver 日志 → Log Aggregator,100% 覆盖。
8. **状态机迁移由 DB CAS 保证**:任意非法迁移视为 bug。
9. **Frontend 不直接访问 HDFS/YARN/Hive**:Backend 是唯一信任域入口。

## Decisions

### Decision 1: 变量渲染分两层(发布层 + Driver 层)

- **Status**: Accepted
- **Context**: SQL 中既有项目级变量(`${prj.warehouse}`)也有运行时变量(`${dt}`)。两类变量可分别在前端预览、平台后端发布、driver 运行时三处任意组合渲染,会导致"所见不是所跑"。
- **Decision**:
  - 项目变量在 Publish 时由 Variable Resolver 烤入 snapshot。
  - 运行时变量(基于 `--biz-date` 派生的时间变量)在 driver 运行期渲染。
  - Frontend"预览"调用 Backend Variable Resolver,使用同一组规则生成预览。
- **Rationale**: 项目变量与 `biz_date` 解耦,同一 snapshot 可被任意 `biz_date` 重用 → 补数天然支持。运行时变量由 driver 渲染避免在 publish 时刻就锁死时间维度。
- **Alternatives considered**:
  - 全前置(driver 不感知任何变量)— 拒绝:补数时 snapshot 与 biz\_date 强绑定,引发 N×M 倍 snapshot 量。
  - 全下沉(driver 接收变量字典 JSON)— 拒绝:driver 复杂化,前端无法离线复现 publish 渲染结果。
- **Consequences**:
  - 正向:driver 心智极简、snapshot 数量少、所见即所跑可达成。
  - 负向:Frontend 与 Backend 必须共享一份"运行时变量目录",变更需双向回归。
- **Affected**: Variable Resolver、pyspark\_driver、SQL Editor 变量面板、specs/runtime-variable-rendering、specs/sql-editor。

### Decision 2: 发布即快照(immutable snapshot in HDFS)

- **Status**: Accepted
- **Context**: 老链路依赖 DS 资源中心"当前版本",补数无法重放历史 SQL。
- **Decision**: 发布事件原子地写一份 `(env, task_id, version_id)` 寻址的 HDFS snapshot,回写后用 sha256 校验,version\_id 单调不重用。任务实例创建时锁定 version\_id。
- **Rationale**: 不可变物料是"补数可重放、变更可审计"的最小可信单元。回滚 = 让"已发布版本"指针指回旧 version,而不删除任何东西。
- **Alternatives considered**:
  - DS 资源中心带版本号 — 拒绝:DS 3.1.7 资源中心版本管理弱,且把"用户物料"耦合进 DS 增加运维风险。
  - Git 仓库存 SQL — 拒绝:Git 不是面向高频"按 version 拉取"的对象存储,且引入新的协议层。
- **Consequences**:
  - 正向:补数可选版本、回滚便宜、审计强。
  - 负向:HDFS 上多了"小文件"压力(每发布一个文件)— 缓解:按 task 聚合目录,定期归档冷存(不删除)。
- **Affected**: Snapshot Service、Publish Orchestrator、HDFS layout、specs/sql-snapshot、specs/publish-pipeline。

### Decision 3: 命令字符串后端生成 + DS shell 退化为 eval

- **Status**: Accepted
- **Context**: 现 shell 模板手工拼接 spark-submit + 用户输入"高级 conf"自由文本是注入面;DS 资源同步带来"模板版本漂移"。
- **Decision**: Command Generator 在后端把任务配置编译成 spark-submit 命令字符串,通过 DS task 自定义参数 `SPARK_CMD` 注入;DS shell body 模板退化为 `kinit ... ; eval "$SPARK_CMD" ; <appId 抓取>`。conf 走 key 级白/黑名单,所有字段单引号包裹并转义。
- **Rationale**: 命令生成集中在一处 → 单点防御 + 可审计 + 可重放。DS 模板不再承担业务字段拼装。
- **Alternatives considered**:
  - 命令字符串落到文件,DS 拉文件执行 — 拒绝:多一层 IO,且文件存放是新攻击面。
  - 用 spark-submit 的"参数文件"形式 — 不取消,作为长链字段降级方案,默认不用。
  - `argv` 数组而非命令字符串(spawn) — DS shell task 仅支持 shell,无法直接 spawn argv;故仍走 string + 严格转义。
- **Consequences**:
  - 正向:注入面收敛、命令可审计、可在测试环境重放。
  - 负向:DS 自定义参数有长度上限(实测 \~32KB,够用但需监控);命令字符串需要严格转义自检(用 shlex 二次解析比对)。
- **Affected**: Command Generator、DS Adapter、spark\_submit.sh、specs/command-generation。

### Decision 4: 实例三元组创建时锁定,补数可显式选版本

- **Status**: Accepted
- **Context**: 用户在补数时常希望"用现在最新代码"或"用当时跑的代码",二者语义不同。
- **Decision**: 实例创建时把 `(task_id, version_id, biz_date)` 写死;定时实例锁定"创建时刻已发布的最新版本";补数允许在 UI 显式选择 version\_id;重试沿用三元组,retry\_count++。
- **Rationale**: 与 immutable snapshot 自洽,语义清晰。
- **Alternatives considered**: 触发时刻锁定 — 拒绝:在 schedule create→trigger 之间用户改了发布,会导致同一实例的不同重试用不同版本。
- **Consequences**: 正向:可重放、可审计;负向:补数 UI 多一个"版本来源"选择项。
- **Affected**: Instance Service、Frontend Backfill UI、specs/task-instance-lifecycle、specs/schedule-management、specs/sql-snapshot。

### Decision 5: 后端运行栈 — 模块化单体 Python 3.10+ / FastAPI + MySQL 8

- **Status**: Accepted
- **Context**: Backend 需要 HDFS client、DS Open API client、Kerberos 认证、稳定 RDBMS 驱动;团队主语言为 Python;CDP 集群默认开启 WebHDFS。
- **Decision**:
  - 后端用 Python 3.10+ + FastAPI,模块化单体(modules: project, variable, snapshot, command, publish, ds, instance, log, audit);
  - 元数据库 MySQL 8(单实例起,后续按需主从);ORM 用 SQLAlchemy 2.x,迁移用 Alembic;
  - HDFS 访问走 WebHDFS REST + SPNEGO Kerberos(`hdfs` / `requests-kerberos`),不引入 JNI/JVM;
  - DS Open API 用 `httpx` 同步 client(MVP 不上 MQ);
  - 部署:K8s 多副本,反代统一入口;后台 Job(巡检、自动重试、Kerberos 续票)以独立进程或 worker 形式拉起;
  - 依赖与环境管理:`uv`(`pyproject.toml` + `uv.lock`,本地 `uv sync`/`uv run`,镜像构建 `uv pip install --system --no-deps -r requirements.txt` 或 `uv pip sync`);代码风格 ruff + mypy。
- **Rationale**:
  - 与团队语言栈一致,迭代成本最低;
  - WebHDFS 让 Python 后端不依赖 JVM,容器镜像轻;
  - MySQL 起步避免 SQLite 在 FastAPI 多 worker 下的写串行瓶颈与后期数据迁移成本;ORM 抽象后未来切 PG 代价可控。
- **Alternatives considered**:
  - Java/Spring Boot — 拒绝:与团队语言栈不一致,Python 已能覆盖所需集成。
  - SQLite(MVP)→ MySQL — 拒绝:状态机 CAS 与多 worker 并发写在 SQLite 受限,迁移阶段还要重做 DDL/索引。
  - PyArrow `HadoopFileSystem`(libhdfs JNI) — 拒绝:依赖 JVM,违背"轻镜像"目标。
  - subprocess 调 `hdfs dfs` CLI — 备用降级方案,默认不用。
  - 微服务 — 拒绝:首版规模不必要。
- **Consequences**:
  - 正向:工程效率高;镜像体积小;Python 与 driver、运维脚本同栈,共享代码与 fixture(尤其"运行时变量目录"工具类);
  - 负向:Python 在 CPU 密集场景吞吐低于 JVM(本系统是 IO 密集,可接受);Kerberos/SPNEGO 在 Python 生态成熟度低于 JVM,需要 CI 中加 Kerberos 集成回归。
- **Affected**: Backend modules、DB schema、deployment、CI/CD、Snapshot Service(WebHDFS 写入)、DS Adapter(httpx)、architecture.md 的 Open Decisions 表。

### Decision 6: 时区显式注入 driver

- **Status**: Accepted
- **Context**: YARN 容器本机时区不一定与平台一致,会导致 `${dt}` 语义跳变。
- **Decision**: spark\_submit.sh 把平台时区作为 `--timezone` 参数传给 driver;driver 用该时区解释 `--biz-date` 并派生所有运行时变量;不依赖容器本机时区。
- **Rationale**: 显式优于隐式;避免容器迁移引发的语义漂移。
- **Alternatives considered**: 全集群强制 TZ=Asia/Shanghai — 不在本 change 控制范围内。
- **Consequences**: 正向:语义稳定;负向:driver 多一个参数。
- **Affected**: pyspark\_driver、Command Generator、specs/pyspark-driver、specs/runtime-variable-rendering。

### Decision 7: trace\_id 标准走 OpenTelemetry traceparent 字段

- **Status**: Accepted
- **Context**: 跨 4 层(Frontend、Backend、DS、Driver)需统一追踪 ID。
- **Decision**: Frontend 入口生成 W3C `traceparent`;Backend 透传到命令字符串(`--trace-id`);DS task body 通过环境变量传给 spark\_submit.sh;driver 在每条结构化日志带 trace\_id;后端日志聚合按 trace\_id 索引。Backend 接入 `opentelemetry-instrumentation-fastapi`、`-httpx`、`-sqlalchemy`,通过 OTLP exporter 上报至公司 OTel collector。
- **Rationale**: W3C 标准,易接 APM。
- **Alternatives considered**: 自定义 UUID — 可工作但失去 APM 接入。
- **Consequences**: 正向:跨层追踪、可接 APM;负向:需要跑通 OTel collector(基础设施)。
- **Affected**: API Gateway、spark\_submit.sh、pyspark\_driver、Log Aggregator。

### Decision 8: 灰度按项目维度

- **Status**: Accepted
- **Context**: 老任务存在,不能强制迁移。
- **Decision**: Backend 维护"项目级"灰度开关,开启后该项目新发布走新链路;旧任务在用户主动迁移前仍走旧 DS 资源中心模式。
- **Rationale**: 项目维度比租户更细,比任务更便于推进。
- **Consequences**: 正向:风险可控;负向:Backend 需同时维护两套 publish 路径(过渡期)。
- **Affected**: Publish Orchestrator、Frontend、specs/publish-pipeline。

### Decision 9: 高级 conf 用 key 级白名单 + 显式黑名单

- **Status**: Accepted
- **Context**: 用户可能要调 `spark.sql.shuffle.partitions`、`spark.sql.adaptive.*` 等,但不能开 JVM 选项类高危项。
- **Decision**:
  - 白名单(允许):`spark.sql.*`(除黑名单)、`spark.shuffle.*`、`spark.dynamicAllocation.*`、`spark.executor.memory*`、`spark.executor.cores`、`spark.driver.memory*`、`spark.driver.cores`、`spark.yarn.appMasterEnv.*`、`spark.executorEnv.*`、`spark.hadoop.hive.exec.dynamic.partition*`。
  - 黑名单(优先级高于白名单):`spark.driver.extraJavaOptions`、`spark.executor.extraJavaOptions`、`spark.driver.extraClassPath`、`spark.driver.extraLibraryPath`、`spark.executor.extraClassPath`、`spark.executor.extraLibraryPath`、`spark.yarn.dist.*`、`spark.kerberos.*`、`spark.yarn.principal`、`spark.yarn.keytab`、`spark.security.credentials.*`。
- **Rationale**: key 级粒度能抓住所有"远程代码执行/越权 token"类高危项;value 级正则成本高、误伤多。
- **Consequences**: 正向:简单、清晰;负向:遇到边缘合法需求需要平台审批后扩白名单(走变更流程,合理)。
- **Affected**: Spark Config Form、Command Generator、specs/spark-config、specs/command-generation。

### Decision 10: kill 路径 — 双重 kill 串行 + 巡检兜底

- **Status**: Accepted
- **Context**: DS kill 不一定能停住 YARN application;反过来 yarn -kill 也不能让 DS task 状态收敛。
- **Decision**: kill 同步串行先 DS 再 YARN;两步都重试到成功或返回明确终态;另外有巡检 Job 周期对比(DS state, YARN state, 平台 instance state),不一致即继续 kill 并告警。
- **Rationale**: 任何单点失败都可能让任务半运行,只能靠并发兜底。
- **Consequences**: 正向:最终一致;负向:kill 路径逻辑复杂,需要测试覆盖。
- **Affected**: Instance Service、DS Adapter、巡检 Job、specs/dolphinscheduler-integration、specs/task-instance-lifecycle。

### Decision 11: 前端栈 — Vue 3 + vue-element-plus-admin / vben-admin 底座

- **Status**: Accepted
- **Context**: 团队倾向 vue-admin-template 风格的后台底座,但原仓库基于 Vue 2 + Element UI,已停止主线维护。需要选用 Vue 3 同源生态。
- **Decision**:
  - 采用 Vue 3 + TypeScript;
  - 底座二选一:`vue-element-plus-admin`(Element Plus,更接近原 vue-admin-template 体验)或 `vben-admin`(Ant Design Vue,组件更丰富);MVP 先选 `vue-element-plus-admin`,如组件不够再切换;
  - SQL 编辑器集成 Monaco(`@guolao/vue-monaco-editor` 或 `monaco-editor-vue3`);
  - 调度依赖图编辑器用 `@vue-flow/core`;
  - 状态管理 Pinia,网络请求 axios + 类型从 OpenAPI 自动生成。
- **Rationale**:
  - 与团队现有审美/习惯接近(后台模板风格);
  - Vue 3 生态活跃,Element Plus 与 vue-element-plus-admin 仍在持续维护;
  - 避免锁死 Vue 2,降低后续技术债。
- **Alternatives considered**:
  - vue-admin-template(Vue 2) — 拒绝:已停止主线维护,生态滞后。
  - React + Monaco — 拒绝:与团队偏好不符。
- **Consequences**:
  - 正向:底座成熟、组件库齐全;
  - 负向:`vue-element-plus-admin` 与 `vben-admin` API 风格差异大,若中途切换会有改造成本(MVP 先单选,文档化决策)。
- **Affected**: Frontend SPA、API 客户端生成流水线、specs/sql-editor、specs/spark-config、specs/schedule-management。

## MVP Scope

本 change 的实施分阶段。MVP 是"端到端能跑通"的最小可用单元;phase 2+ 是"规模化与丰满度"。
切分原则:**架构不可变量**(snapshot、命令生成、三元组锁定、状态机 CAS、driver 兼容)从第一天就在;**功能丰满度**(多租户、依赖图、补数选版本、巡检、实时日志、SLA)放后续。

### MVP 必做(对应 specs 的核心 requirements)

- **Driver 改造**:`${dt}/${date}/${month}/${dt-N}/${date-N}/${hr}`(`N` 正整数,`${hr}` 需 `--biz-hour`,缺则 fail-fast);`--timezone`/`--trace-id`/`--version-id`/`--instance-id`/`--biz-hour` 可选参数;结构化日志;默认严格模式;向后兼容老命令。
- **HDFS Snapshot 物料层**:WebHDFS REST + SPNEGO,`(env, task_id, version_id)` 寻址,`O_CREAT` 不覆盖,sha256 回读校验,不可变。
- **Variable Resolver**:发布期烤入项目变量,运行时变量保留;预览端点用同一 Resolver(所见即所跑)。
- **Command Generator**:白名单 + 黑名单(Decision 9 全集);单引号包裹 + shlex 二次解析自检。
- **Publish Orchestrator**:事务化(校验 → snapshot → 命令 → DS upsert → DB commit);任一失败回滚;幂等键;**项目级灰度开关从第一天就在**(老任务保留旧链路)。
- **DS Adapter**:Open API upsert workflow/task;DS shell body 模板退化为 `eval $SPARK_CMD`;appId 抓取与回写;kill 双重串行(DS → YARN)。
- **Instance Service 状态机**:三元组创建时锁定;`pending/submitting/submit_failed/running/succeeded/failed/killed` 全状态;DB CAS 迁移;手动重试沿用三元组;手动 kill 幂等。
- **审计字段**:`task_instance` 落 `submitter/tenant_id/principal/queue/version_id/biz_date/biz_hour/application_id/trace_id/command_text`(脱敏)— 字段在,UI 后做。
- **Frontend 最小集**:项目目录树 + SQL Editor(Monaco)+ 变量面板(展示 MVP 变量集合,未声明变量警告)+ 预览 + 结构化 Spark Config 表单(无"高级 conf 文本框")+ 简单 cron 调度 + 发布按钮 + 实例列表与详情 + kill/重试按钮。
- **trace_id**:W3C traceparent 字段贯穿 4 层并打入日志;**OTel collector 接入留 phase 2**(本地结构化日志先用)。
- **MVP 验收 e2e 9 步**:
  1. SQL Editor 写含 `${dt}/${dt-1}/${hr}` 的 SQL
  2. 预览看到渲染结果
  3. 发布 → snapshot v1 + DS upsert task
  4. DS 到点触发 → 实例锁 v1 + SPARK_CMD 注入
  5. DS Worker `kinit` + `eval $SPARK_CMD` → spark-submit
  6. driver 跑 INSERT OVERWRITE
  7. 实例详情页有 application_id、状态从 submitting → running → succeeded;日志按钮跳 YARN UI(不强求实时流)
  8. 用户 kill running 实例 → DS task + YARN application 双终止
  9. 用户改 SQL 再发布 → version v2,新实例锁 v2,老实例不受影响

### MVP 砍掉(明确放 phase 2 及之后)

| 砍项 | 临时方案 | 后续节点 |
|---|---|---|
| 多租户 principal/keytab 路由 | 单租户单 principal,沿用 `a_xy_mn`;字段保留值固定 | phase 2 |
| Queue 提交权限校验 | 表单可填,无校验;按 `root.a_dc_qysjrh` 默认 | phase 2 |
| Spark Config "高级 conf 文本框" | 仅结构化字段(队列、driver/executor、shuffle、动态分配上下限) | phase 2(白名单逻辑骨架先在) |
| 跨任务依赖(dependent task) | DS 自带 dependent 任务暂可用,平台 UI 不暴露 | phase 2 |
| 补数选历史版本 | MVP 补数=用当前已发布版本;UI 仅暴露 biz_date 区间 | phase 3 |
| 回滚到历史版本 | 用户可重新 publish 旧 SQL 实现等价回滚 | phase 3 |
| SLA / 告警接收人 | 不实现 | phase 2 |
| 巡检 Job(DS↔Backend、kill 半途轮询) | runbook 文档化,运维手工兜底 | phase 2 |
| 自动重试预算 | 仅手动重试;UI 提供"重试"按钮 | phase 2 |
| Frontend 实时日志流(SSE) | 终态后展示 YARN logs 链接;运行中只展示状态 | phase 2 |
| 命令字符串前端可见(脱敏视图) | 字段已存,UI 不暴露 | phase 3 |
| OTel collector 接入 | trace_id 字段进结构化日志,collector 后接 | phase 2 |
| Migration 工具(扫 DS 现有 task 导入) | 用户手工新建任务 | phase 2 |
| 性能压测 | 上线后基于真实流量观测 | phase 2 |
| 渗透测试 | 安全 fuzz 测试 + shlex 自检覆盖核心注入面;渗透测试 phase 2 | phase 2 |
| 周期与时区前端表单 | 时区固定 `Asia/Shanghai`(driver `--timezone` 已就绪) | phase 2 |

### 砍项的"地基已埋"清单(防止后补改 schema)

下列字段/接口在 MVP 即落 schema 与 API,即便 UI/逻辑暂不暴露,也不能省:

- `task_instance.principal` / `tenant_id` / `queue`(多租户用)
- `task_version.command_template_text` 与 `task_instance.command_text`(命令字符串归档,phase 3 UI 用)
- `trace_id` 字段全表都有
- `task_instance.biz_hour`(小时维度任务用,即便 MVP 用户没填也保留 NULL)
- 状态机的 `waiting_dependency` 状态值(MVP 不用,但留枚举)
- `task_version.status`(`published/superseded/rolled_back`,MVP 只用前两个)
- `audit_log` 表(MVP 写所有发布 + 实例状态迁移,UI 不暴露查询)



## Interface Design

### REST API(Backend → Frontend)

| Interface                          | Caller → Provider  | Contract             | Auth                        | Error model                          | Compatibility       |
| ---------------------------------- | ------------------ | -------------------- | --------------------------- | ------------------------------------ | ------------------- |
| `POST /api/v1/projects/:id/tasks`  | Frontend → Backend | OpenAPI v1           | Session/JWT + tenant header | `{code, message, details, trace_id}` | semver,major 不破坏 v1 |
| `PUT /api/v1/tasks/:id/draft`      | 同上                 | 草稿写入                 | 同上                          | 同上                                   | 同上                  |
| `POST /api/v1/tasks/:id/preview`   | 同上                 | 渲染预览                 | 同上                          | 同上                                   | 同上                  |
| `POST /api/v1/tasks/:id/publish`   | 同上                 | 发布(idempotency\_key) | 同上                          | 同上                                   | 同上                  |
| `POST /api/v1/tasks/:id/backfill`  | 同上                 | 补数(区间 + 版本)          | 同上                          | 同上                                   | 同上                  |
| `GET /api/v1/instances/:id`        | 同上                 | 实例详情                 | 同上                          | 同上                                   | 同上                  |
| `GET /api/v1/instances/:id/logs`   | 同上                 | 日志流(SSE/分段)          | 同上                          | 同上                                   | 同上                  |
| `POST /api/v1/instances/:id/kill`  | 同上                 | kill                 | 同上                          | 同上                                   | 同上                  |
| `POST /api/v1/instances/:id/retry` | 同上                 | 手动重试                 | 同上                          | 同上                                   | 同上                  |

### Backend ↔ DS Open API

| Action          | DS endpoint                         | Sync/async | Retry     | Idempotent? |
| --------------- | ----------------------------------- | ---------- | --------- | ----------- |
| upsert workflow | `/projects/{p}/process-definition`  | sync       | 1         | 用幂等键        |
| upsert task     | 同上                                  | sync       | 1         | 同上          |
| trigger run     | `/instances/start-process-instance` | sync       | 0(避免重复触发) | 平台前置去重      |
| kill instance   | `/instances/{id}/stop`              | sync       | 重试到收敛     | 是           |
| query instance  | `/instances/{id}`                   | sync       | 读不限       | 是           |

### DS task body 模板 → spark\_submit.sh 接口

注入到 DS task 的环境变量:

- `SPARK_CMD`(命令模板,含占位 `${biz_date}`)
- `BIZ_DATE`(运行时填充)
- `KEYTAB_PATH`、`PRINCIPAL`、`PLATFORM_TZ`、`TRACE_ID`、`VERSION_ID`、`TASK_ID`、`INSTANCE_ID`
- `PLATFORM_CALLBACK_URL`(用于 appId 回写)

DS shell body(发布期 upsert 后不再变):

```
set -euo pipefail
[ -n "$KEYTAB_PATH" ] && [ -n "$PRINCIPAL" ] || { echo "missing kerberos env"; exit 2; }
kinit -kt "$KEYTAB_PATH" "$PRINCIPAL"
CMD="${SPARK_CMD/\$\{biz_date\}/$BIZ_DATE}"
echo "[trace] trace_id=$TRACE_ID instance_id=$INSTANCE_ID"
{ eval "$CMD"; } 2>&1 | tee /tmp/spark-submit.${INSTANCE_ID}.log
EXIT=${PIPESTATUS[0]}
APP_ID=$(grep -oE 'application_[0-9]+_[0-9]+' /tmp/spark-submit.${INSTANCE_ID}.log | head -n1 || true)
curl -fsS -X POST "$PLATFORM_CALLBACK_URL" -d "{\"instance_id\":\"$INSTANCE_ID\",\"application_id\":\"$APP_ID\",\"exit\":$EXIT}" || true
exit $EXIT
```

### pyspark\_driver CLI 契约

| Arg                       | Type             | Required | Default   | Compatibility |
| ------------------------- | ---------------- | -------- | --------- | ------------- |
| `--sql-file`              | string(basename) | yes      | —         | 既有            |
| `--biz-date`              | yyyyMMdd         | yes      | —         | 既有            |
| `--biz-hour`              | HH(00-23)       | no       | —         | 新增;含 `${hr}` 时必填,缺则 fail-fast |
| `--timezone`              | string(IANA)     | no       | 容器本机      | 新增            |
| `--trace-id`              | string           | no       | `unknown` | 新增            |
| `--version-id`            | string           | no       | `unknown` | 新增            |
| `--instance-id`           | string           | no       | `unknown` | 新增            |
| `--allow-unresolved-vars` | bool             | no       | false     | 既有            |
| `--dry-run`               | bool             | no       | false     | 既有            |

新增参数都是可选,缺省时不影响老调用语义。

## Data Model and Consistency

### Core entities

```
project(id, tenant_id, name, default_spark_conf_json)
project_variable(project_id, name, value_or_vault_ref, version, effective_at)
task(id, project_id, name, schedule_conf_json)
task_draft(task_id, draft_revision_id, sql_text, spark_conf_json, schedule_conf_json, updated_at)
task_version(task_id, version_id, snapshot_path, sha256, draft_revision_id,
             baked_at, baked_by, project_var_versions_json, command_template_text,
             status: published/superseded/rolled_back)
task_instance(instance_id, task_id, version_id, biz_date, trigger_source, state,
              submitter, tenant_id, principal, queue, application_id, trace_id,
              command_text, retry_count, kill_request_id, ds_workflow_instance_id,
              created_at, started_at, ended_at)
ds_sync(task_id, ds_workflow_id, ds_task_id, last_sync_at, last_sync_state)
audit_log(id, actor, tenant_id, target, action, before_json, after_json, trace_id, at)
```

### Schema 变更

- 新建 schema(本 change 一次性)。后续小改:`task_instance` 加字段时优先 nullable,避免大表 ALTER。
- 唯一约束:
  - `task_version(task_id, version_id)` PK,`version_id` 全局单调。
  - `task_instance(task_id, biz_date, trigger_source, retry_count)` 唯一。
  - `task_draft(task_id, draft_revision_id)` 唯一。
  - `task(project_id, name)` 唯一。
- 索引:实例查询常用 `(state, started_at)`、`(task_id, biz_date)`、`(application_id)`、`(trace_id)`。
- 大表预案:`task_instance`、`audit_log` 按月分表或归档冷存(运维基线)。

### Transaction boundary

- **Publish 事务**:对 task 行加排它锁;step 顺序:render → write snapshot → 回读 sha256 → command generate(含 shlex 自检)→ DS upsert(可重试 1 次)→ insert `task_version`、提交;任一失败回滚,snapshot path 留 `.failed` 后缀。
- **Instance 创建**:单一 INSERT,带唯一约束;DS 触发回调路径若超时重试,依赖唯一约束去重。
- **Instance 状态迁移**:`UPDATE ... WHERE state = <expected>` 形式 CAS;非法迁移返回受影响 0 行,告警。
- **Audit**:与主事务同库同事务一并写;失败视为主操作失败(合规优先)。

### Cache

可选 Redis 缓存"项目变量字典快照"和"queue 权限",TTL ≤ 60s,任何写操作主动失效。缓存仅做加速,以 DB 为唯一事实源。

## Security

- **AuthN**: Frontend 走 SSO/OIDC;Backend 之间走 mTLS 或内网 token;Backend 自身 keytab 在 K8s secret/Vault。
- **AuthZ**: 用户-租户-项目-资源四级矩阵;每次写操作和实例触发都校验。
- **Secrets**:
  - 租户 keytab 仅 DS Worker 可读(0400),磁盘静态加密。
  - DB 密码、DS Open API token、KDC 主体 keytab 在 Vault。
  - 命令字符串归档脱敏(隐藏 keytab 路径外字段,不落 stdout 中的 token)。
- **Network**: Browser → Backend HTTPS only;Backend 不暴露 HDFS/YARN/Hive 直连给浏览器。
- **审计**: append-only,记录 actor、tenant、target、before/after、trace\_id;独立 audit 角色才能查询。
- **注入面**: command-generation 的 shell 转义 + 黑名单是单点防线;每次发布做 shlex 二次解析自检。

## Performance / Capacity

参考 [data-flow.md](./data-flow.md) 和 [runtime-flow.md](./runtime-flow.md) 中的目标值。

| 项                 | 目标      | 评估                                |
| ----------------- | ------- | --------------------------------- |
| Backend QPS(常态)   | 100 RPS | 模块化单体 + MySQL 主从足够                |
| 同时 running 实例     | 数百      | 由 YARN queue 决定,Backend 不主动限      |
| Publish 延迟 P95    | < 5s    | snapshot < 100KB,HDFS 写 + DS 调用主导 |
| 触发到 spark-submit  | < 30s   | DS 调度延迟 + Worker pull 耗时          |
| 日志流端到端            | < 10s   | YARN 容器 stdout + 平台流式拉取           |
| `task_instance` 表 | 千万级/年   | 按月分表归档                            |

容量瓶颈预案:

- DS Worker 池吃紧:加 Worker 节点,且 Backend 限流并发提交。
- HDFS 小文件:snapshot 按 task 聚合目录,定期归档冷存(打包,不删除原 path)。
- DB 大表:分表 + 冷存。

## Reliability

- 状态机 CAS,无并发非法迁移。
- Publish 事务化 + snapshot 不可变 → 任何失败可回滚到一致状态。
- DS Adapter 限重试 + 失败回滚发布。
- 巡检 Job 兜底 DS↔Backend 漂移、YARN↔Instance 漂移。
- Backend 多副本 + 无状态;DB 主从切换是停机点。
- HDFS/Hive/YARN 不可用 → 平台进只读模式(可发布暂停,旧实例继续跑)。

## Observability

- Logs(结构化 JSON):`level, ts, service, trace_id, instance_id, task_id, version_id, biz_date, application_id, msg`。
- Metrics:发布成功率、发布耗时直方图、实例触发延迟、DS Adapter 错误率、状态机非法迁移计数(应为 0)。
- Traces:OpenTelemetry,跨 Frontend/Backend/DS/driver 4 层。
- Alerts:DS metadata 漂移、HDFS 写失败率、kill 半途失败率、状态机非法迁移、自身 keytab 即将过期。

## Test Strategy

- **Unit**: Variable Resolver(项目变量替换 + 运行时变量保留)、Command Generator(白/黑名单 + 转义 + shlex 自检)、状态机迁移表、shell 转义边界 case。
- **Contract**: pyspark\_driver 与 Frontend 共用一份"运行时变量目录" YAML,fixture 驱动两端测试。
- **Integration**: Backend ↔ DS Open API mock + 真实 DS(staging)双跑;HDFS 小集群 + Hive 小集群 e2e。
- **End-to-End**: 发布 → 调度 → driver 跑通 → 日志可见 → kill 收敛,覆盖 spec scenario。
- **Security**: command-generation 的 fuzz 测试(各种 shell 元字符、超长 conf、非法 conf key)。
- **Migration**: 老 DS 资源中心模式 + 新链路并行运行,对照同任务的两种 publish 结果。
- **Acceptance evidence**: 每个 spec scenario 对应一条自动化测试或人工验证;runtime-flow 的状态机分支映射表逐条覆盖。

## Rollout / Migration / Rollback

### Rollout

1. 完成 Backend、Frontend、driver 改造;CI 全绿。
2. 在 staging 跑通 e2e。
3. 选 1-2 个低风险项目灰度,观察一周指标。
4. 按项目逐步推开,允许新旧两套链路共存。
5. 全量后,设废弃日期,停止老链路新发布。

### Migration

- 提供导入工具:扫描 DS 现有 task,根据约定从 SQL 文件 + DS 配置生成"草稿 + 配置",由用户在新链路 publish。
- 历史 DS 资源中心 SQL 不删除,仅停止被新链路引用。

### Rollback

- 单任务级:回滚到指定历史 version\_id(指针更新,snapshot 保留)。
- 平台级:Backend 回退版本(无状态多副本支持);DB schema 改动均非破坏,允许老 Backend 读新表。
- DS 侧:DS task body 模板未变,只是不再 upsert 新 task,旧 DS task 仍可手工触发。

## Risks / Trade-offs / Open Questions

| Risk                     | Likelihood | Impact   | Mitigation                              |
| ------------------------ | ---------- | -------- | --------------------------------------- |
| DS Open API 在 3.1.7 的稳定性 | Medium     | High     | DS Adapter 加重试 + 巡检 + 失败回滚发布            |
| HDFS snapshot 小文件膨胀      | Medium     | Medium   | 按 task 聚合目录 + 定期归档冷存                    |
| 命令字符串注入漏网                | Low        | Critical | 白/黑名单 + shlex 自检 + 安全 fuzz 测试           |
| Driver 升级破坏老 snapshot    | Low        | High     | driver 严格向后兼容 + CI 用历史 snapshot 回归      |
| trace\_id 标准未定           | Medium     | Low      | 已决定走 OpenTelemetry W3C traceparent;依赖公司 OTel collector 基础设施 |
| 多租户 keytab 路由错配          | Low        | Critical | 命令生成阶段强制校验,跨租户即拒                        |
| DS Worker 上 keytab 文件外泄  | Medium     | High     | 0400 权限 + DS shell `set +x` 包裹 + 文件系统加密 |
| 灰度期"两套链路并存"复杂度           | Medium     | Medium   | 项目级开关,清晰边界,设废弃日期                        |

### Resolved Decisions(原 Open questions,已确认)

| 议题 | 决议 | 锚定 |
|---|---|---|
| 后端语言/框架 | Python 3.10+ + FastAPI(模块化单体) | Decision 5 |
| 元数据库 | MySQL 8(MVP 起步即用,放弃 SQLite 过渡方案) | Decision 5 |
| 前端框架 | Vue 3 + TypeScript,底座选 `vue-element-plus-admin`(`vben-admin` 备选) | Decision 11 |
| trace\_id 协议 | OpenTelemetry W3C traceparent,Backend 接 OTLP collector | Decision 7 |
| Backend ↔ DS 是否上 MQ | MVP 不上,同步 httpx 调用即可;若后期 DS 调用阻塞主流程再评估 | — |
| Snapshot 物料层 | HDFS(WebHDFS REST + SPNEGO Kerberos),不引入对象存储 | Decision 5 |


## Acceptance Evidence(对齐 specs)

| Capability                   | Evidence                                       |
| ---------------------------- | ---------------------------------------------- |
| sql-editor                   | e2e 录屏:编辑、未声明变量警告、预览                           |
| spark-config                 | 单测 + 集成:白/黑名单、queue 权限拒绝                       |
| schedule-management          | 集成:cron 触发、依赖等待、补数选版本                          |
| sql-snapshot                 | 集成:发布两次 → 两个 version\_id;回滚 → 指针更新,snapshot 保留 |
| command-generation           | 安全 fuzz 报告 + shlex 自检通过                        |
| runtime-variable-rendering   | 共用 fixture 在 Frontend/driver 各自跑过              |
| dolphinscheduler-integration | 集成:upsert、appId 回写、kill 联动                     |
| task-instance-lifecycle      | 状态机分支覆盖率 100%                                  |
| multi-tenant-isolation       | 跨租户访问被拒、审计反查 demo                              |
| observability                | trace\_id 跨层贯穿 demo + log 聚合可达                 |
| publish-pipeline             | 故障注入:每个 step 失败 → 回滚正确                         |
| pyspark-driver               | 老命令兼容性回归 + 新参数 fixture                         |

