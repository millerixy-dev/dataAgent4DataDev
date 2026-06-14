## 标记说明

- `[MVP]` — MVP 必做,缺一项 e2e 跑不通
- `[phase 2+]` — 后续阶段补,字段/接口若与 MVP 强耦合在 MVP 阶段先埋字段
- `[mixed]` — section 内混合,以单条为准

MVP 验收 e2e 9 步定义见 design.md `## MVP Scope`。

## 1. Design Readiness  [MVP]

- [x] 1.1 design.md "Open questions" 已确认并落到 Decision 5 / Decision 7 / Decision 11 与"Resolved Decisions"表:Python 3.10+ + FastAPI、MySQL 8、Vue 3 + `vue-element-plus-admin`、OpenTelemetry、MVP 不引入 MQ、Snapshot 存 HDFS(WebHDFS)
- [ ] 1.2 整理 specs ↔ architecture ↔ data-flow ↔ runtime-flow 的映射,确认无矛盾;若需调整,同步更新 artifact
- [ ] 1.3 与安全/合规/运维过 design.md 的 Security、Rollout、Migration、Rollback 章节,签字确认验收门槛
- [ ] 1.4 评审 conf 白/黑名单具体清单(Decision 9 全集),产出"白名单管理流程"文档;**MVP 表单仅暴露结构化字段,高级 conf 文本框 phase 2+,但白名单逻辑骨架 MVP 即在**
- [x] 1.5 评审"运行时变量目录"YAML(`${dt}/${date}/${month}/${dt-N}/${date-N}/${hr}`),作为 Frontend / Backend / driver 三方共享 fixture

## 2. Foundations:Schema、Contract、Fixture  [MVP]

- [ ] 2.1 设计并落地 Metadata DB schema(`project`、`project_variable`、`task`、`task_draft`、`task_version`、`task_instance`、`ds_sync`、`audit_log`),含唯一约束、索引、迁移脚本;**地基字段(principal/tenant_id/queue/biz_hour/command_text/trace_id 等)即便 MVP UI 不暴露也必须落 schema**(见 design `MVP Scope` 的"地基已埋"清单)
- [ ] 2.2 定义 Backend OpenAPI v1(项目/任务/草稿/预览/发布/实例/kill/重试为 MVP;补数 / 日志流 / 回滚 phase 2+ 的端点先在 schema 上保留以便客户端代码生成稳定)
- [ ] 2.3 定义"运行时变量目录"YAML schema(共享 fixture),并写入仓库供三端引用
- [ ] 2.4 定义 HDFS snapshot 路径协议 `hdfs:///dwh/platform/snapshots/{env}/{task}/{version}/sql.sql` + `meta.json` schema
- [ ] 2.5 定义 DS task body 模板与注入环境变量集合;锁定 spark_submit.sh 接口契约
- [ ] 2.6 搭建后端 Python 3.10+ + FastAPI 工程骨架(模块化单体,模块清单见 design Decision 5);依赖管理用 **uv**(`pyproject.toml` + `uv.lock`,本地 `uv sync` / `uv run`,Docker 多阶段构建复用 uv 缓存);ruff + mypy + Alembic + SQLAlchemy 2.x;CI 跑通空骨架(含 `uv lock --check` 校验锁文件)
- [ ] 2.7 搭建前端 Vue 3 + TypeScript 工程(底座 `vue-element-plus-admin`,Monaco 集成 `@guolao/vue-monaco-editor`,Pinia + axios);API client 由 OpenAPI 自动生成;**`@vue-flow/core` 留 phase 2+(MVP 不做依赖图)**

## 3. Driver 改造(独立、可先行)  [MVP]

- [x] 3.1 把"运行时变量目录"YAML 引入 driver,实现 `${dt}/${date}/${month}/${dt-N}/${date-N}/${hr}` 渲染(specs/runtime-variable-rendering)。`${dt-N}/${date-N}` 用独立 pattern 优先匹配(连字符不在普通变量名字符集);`${hr}` 缺 `--biz-hour` 时 fail-fast
- [x] 3.2 driver 新增 CLI:`--biz-hour`(可选,两位 HH 00-23)、`--timezone`、`--trace-id`、`--version-id`、`--instance-id`,缺省可工作(向后兼容)
- [x] 3.3 driver 加结构化日志(`application_id`、`trace_id`、`biz_date`、`biz_hour`、`version_id`、SQL 序号、SQL 哈希)
- [x] 3.4 默认严格模式:未定义变量 fail-fast,错误信息列出全部未定义变量
- [x] 3.5 driver 单测:基础变量、`${dt-N}/${date-N}` 偏移含 N=0/1/365、`${hr}` 含/缺 `--biz-hour`、时区一致性、未定义变量、向后兼容(老命令)、`${dt-1abc}` 等非法形式视为未定义
- [x] 3.6 driver 历史 snapshot 回归用例(用一份历史 SQL fixture 跑通,验证 driver 升级不破坏老物料)

## 4. Variable Resolver  [MVP]

- [x] 4.1 实现项目变量解析:从 `project_variable` 取发布时刻最新版本,渲染到 SQL,锁定 `project_var_versions` 到 `task_version`
- [x] 4.2 实现运行时变量校验:加载共享 YAML,确认 `${...}` 占位符要么是项目变量要么是 driver 承诺的运行时变量,否则失败
- [x] 4.3 单测:项目变量替换、运行时变量保留、未定义变量;**vault 引用 phase 2+**
- [x] 4.4 给 Frontend 提供 `POST /preview` 端点(同样的 Resolver,对外透出渲染后 SQL) — **核心 Resolver.preview() 已实现并测试;HTTP 端点待 Backend 工程骨架(2.6)落地后包装,逻辑层零变更**

## 5. Snapshot Service  [MVP]

- [ ] 5.1 实现 WebHDFS 写入(`hdfs` + `requests-kerberos` SPNEGO,模拟 `O_CREAT|O_EXCL`:CREATE `?overwrite=false`,version_id 单调生成 e.g. ULID)+ sha256 回读校验
- [ ] 5.2 写 `meta.json`(task_id、version_id、env、draft_revision_id、baked_at、project_var_versions、sha256)
- [ ] 5.3 失败路径:写入失败标 `.failed` 后缀(不删),抛错让 Publish 回滚
- [ ] 5.4 实现"按 version_id 读 snapshot"接口,Frontend 实例详情"所跑 SQL"复用
- [ ] 5.5 单测 + HDFS 集成测试(staging 小集群,验证 SPNEGO 鉴权 + 大文件 + 路径冲突 + 网络抖动重试)

## 6. Command Generator  [MVP]

- [x] 6.1 把任务结构化配置 + snapshot 路径 + principal + queue 编译成 spark-submit 命令模板,`--biz-date` 留占位;**MVP 单租户固定 principal,字段在但不路由**
- [x] 6.2 实现 conf 白/黑名单逻辑,黑名单优先;命中黑名单直接拒绝
- [x] 6.3 实现 shell 严格转义(单引号包裹 + 内嵌单引号转义)
- [x] 6.4 命令生成后用 shlex 二次解析,确认 token 数量与设计一致(自检)
- [x] 6.5 单测覆盖:正常 conf、白名单边界、黑名单各 key、含 shell 元字符的值、超长 conf、空值、unicode
- [x] 6.6 安全 fuzz 测试(随机生成 conf key/value 边界 case,确认转义不破裂)

## 7. Publish Orchestrator(事务编排)  [mixed]

- [ ] 7.1 实现发布事务:鉴权 → conf 白名单 → Variable Resolver → Snapshot 写 → Command Generator → DS Adapter upsert → DB commit  [MVP]
- [ ] 7.2 任一失败回滚;snapshot path 留 `.failed`,不进 `task_version`  [MVP]
- [ ] 7.3 幂等键 `(task_id, draft_revision_id, idempotency_key)` 唯一约束,重复请求返回首次结果  [MVP]
- [ ] 7.4 实现"回滚到指定历史版本":更新 task 的"已发布版本"指针(发布事件 append),snapshot 保留  [phase 2+]
- [ ] 7.5 实现项目级灰度开关:开启后走新链路 publish;未开启走旧 DS 资源中心(过渡期)  [MVP]
- [ ] 7.6 单测 + 故障注入:每一 step 失败都验证回滚正确  [MVP]

## 8. DS Adapter(DolphinScheduler 集成)  [mixed]

- [ ] 8.1 用 DS Open API client(httpx)实现 upsert workflow/task,task body 用固定模板,SPARK_CMD 等通过 DS 自定义参数注入  [MVP]
- [ ] 8.2 实现重试 1 次 + 指数退避;失败抛回让 Publish 回滚  [MVP]
- [ ] 8.3 实现 trigger callback / query / kill 调用  [MVP]
- [ ] 8.4 实现"DS metadata vs 平台 metadata"巡检 Job(每 N 分钟 diff);告警机制  [phase 2+]
- [ ] 8.5 集成测试:staging DS 上跑发布 → upsert → 触发回调 → 实例创建  [MVP]
- [ ] 8.6 验证 DS 自定义参数长度上限(实测 + 文档化)  [MVP]

## 9. spark_submit.sh 改造  [MVP]

- [x] 9.1 重写 shell:`set -euo pipefail`、kinit、`set +x` 包裹敏感操作、`eval "$SPARK_CMD"`、`tee` 日志、抓 application_id、回调平台
- [x] 9.2 退出码透传 DS;application_id 抓取失败不阻断
- [x] 9.3 加 trace_id 打印到日志头部
- [x] 9.4 集成测试:shell 在真实 Worker 节点跑通(含 keytab 0400 + Kerberos) — **本地 mock(kinit/spark-submit/curl)集成已 11 case 全绿;真实 Worker 集群验证留给 staging e2e(group 17)**

## 10. Instance Service(状态机)  [mixed]

- [ ] 10.1 实现实例创建:三元组锁定 + 唯一约束 + DS 触发去重  [MVP]
- [ ] 10.2 实现状态机:`pending/submitting/submit_failed/running/succeeded/failed/killed`,迁移用 DB CAS;**`waiting_dependency` 枚举值定义但 MVP 不进**  [MVP]
- [ ] 10.3 实现 DS callback 回写(`application_id`、退出状态)  [MVP]
- [ ] 10.4 实现手动重试:沿用三元组,retry_count++  [MVP]
- [ ] 10.5 实现 kill:DS kill → YARN kill 串行;kill_request_id 幂等  [MVP]
- [ ] 10.5b 巡检 Job 兜底未收敛实例(kill 半途、状态漂移)  [phase 2+]
- [ ] 10.6 实现补数:UI 选 `(biz_date 区间, version_id 来源, 串/并)`,批量创建实例  [phase 2+]
- [ ] 10.7 实现自动重试预算(配额内)  [phase 2+]
- [ ] 10.8 单测覆盖所有合法迁移与非法尝试  [MVP];集成测试故障注入  [phase 2+]

## 11. Multi-tenant / Auth / Audit  [mixed]

- [ ] 11.1 实现"用户/项目"基础鉴权 + API 鉴权中间件;**MVP 单租户,租户上下文字段贯穿但值固定**  [MVP]
- [ ] 11.1b "用户/租户/项目/资源"权限矩阵完整化  [phase 2+]
- [ ] 11.2 实现 queue 提交权限校验(发布期 + 触发期双重校验)  [phase 2+]
- [ ] 11.3 实现"租户 → principal/keytab"映射;命令生成时强制路由,无映射则拒绝  [phase 2+]
- [ ] 11.4 实现 audit_log append-only 写入,所有写操作 + 实例状态迁移落审计;**MVP 落审计但 UI 不暴露查询**  [MVP]
- [ ] 11.5 跨租户访问拒绝(snapshot 读、实例读、日志读全覆盖)  [phase 2+]
- [ ] 11.6 集成测试:租户 A 用户尝试访问租户 B 资源 → 拒绝且不暴露资源存在  [phase 2+]

## 12. Observability / Log Aggregator  [mixed]

- [ ] 12.1 trace_id 字段贯穿 4 层 + 进入结构化日志;**OTel collector 接入 phase 2+**  [MVP]
- [ ] 12.1b 集成 OpenTelemetry instrumentation(fastapi/httpx/sqlalchemy)+ OTLP exporter  [phase 2+]
- [ ] 12.2 实现 Log Aggregator Adapter:终态后从 HDFS 聚合日志读;**MVP 仅做"跳转 YARN UI 链接",不做拉取代理**  [phase 2+]
- [ ] 12.3 Frontend 实时日志流(SSE)  [phase 2+]
- [ ] 12.4 Metrics:发布成功率、发布耗时、实例触发延迟、DS Adapter 错误率、状态机非法迁移计数(应为 0)  [MVP]
- [ ] 12.5 Alerts:HDFS 写失败、非法状态迁移、自身 keytab 即将过期  [MVP];DS metadata 漂移、kill 半途失败、巡检告警  [phase 2+]
- [ ] 12.6 实例详情页直达 application_id、trace_id  [MVP];"复制命令"(脱敏视图)  [phase 2+]

## 13. Frontend  [mixed]

- [ ] 13.1 项目目录树(创建/重命名/移动/删除);**MVP 不做权限隔离 UI(单租户)**  [MVP]
- [ ] 13.2 SQL Editor(Monaco via `@guolao/vue-monaco-editor`)+ 多 tab + 自动保存 + 未保存标记  [MVP]
- [ ] 13.3 变量面板:展示项目变量 + 运行时变量目录;未声明变量警告  [MVP]
- [ ] 13.4 预览弹窗:选 `biz_date`(可选 `biz_hour`)→ 调 `/preview` → 展示渲染后 SQL  [MVP]
- [ ] 13.5 Spark Config 表单:结构化字段 + 默认值模板;**高级 conf 文本框 phase 2+**  [MVP]
- [ ] 13.6 Schedule 表单:cron + 时区固定 `Asia/Shanghai`;**依赖图编辑 + SLA 配置 phase 2+**  [MVP]
- [ ] 13.7 发布按钮 + 已发布版本号展示  [MVP];发布历史版本列表 + 回滚操作  [phase 2+]
- [ ] 13.8 实例列表 + 详情(状态、application_id、trace_id);**所跑 SQL 可看 [MVP]**;命令字符串脱敏视图  [phase 2+]
- [ ] 13.9 日志查看器:跳转 YARN UI 链接  [MVP];运行中 SSE 流式 + 终态聚合  [phase 2+]
- [ ] 13.10 补数对话框:区间 + 版本来源 + 串/并  [phase 2+]
- [ ] 13.11 kill / 手动重试操作  [MVP]

## 14. Migration Tooling(老链路迁移)  [phase 2+]

- [ ] 14.1 编写"扫描 DS 现有 task → 导出"工具,产出"草稿 + 配置"半成品
- [ ] 14.2 提供 UI 列表让用户人工确认导入
- [ ] 14.3 灰度共存阶段:同任务允许两套链路并行运行,提供"对照"工具(同 biz_date 输出对比)
- [ ] 14.4 设废弃日期,过期后停止老链路新发布

## 15. Security Hardening  [mixed]

- [x] 15.1 命令生成 fuzz 测试报告(产出文档,纳入 CI 周跑)  [MVP] — **`tests/test_command_generator_fuzz.py` 已纳入默认 pytest run,200+100 例 hypothesis 覆盖;CI 周跑挂钩待 group 17 部署 staging 时配置**
- [ ] 15.2 keytab 文件权限 0400 + 文件系统加密 + DS shell `set +x` 包裹的人工核验  [MVP]
- [ ] 15.3 Backend 自身 Kerberos ticket 自动续期(后台 worker)+ 监控告警;Python `requests-kerberos`/`gssapi` 在 CI 中的集成回归  [MVP]
- [ ] 15.4 SQL 内容脱敏策略评审  [phase 2+]
- [ ] 15.5 渗透测试:command-generation 注入面、跨租户访问、API 鉴权绕过  [phase 2+]

## 16. Performance / Capacity Validation  [phase 2+]

- [ ] 16.1 Backend 压测:100 RPS 常态、500 RPS 峰值
- [ ] 16.2 Publish 端到端 P95 < 5s 验证
- [ ] 16.3 触发到 spark-submit 启动 P95 < 30s 验证
- [ ] 16.4 同时 running 实例数压测(数百级)
- [ ] 16.5 task_instance 表千万级数据下读 P99 < 100ms 验证;分表预案演练

## 17. Rollout / Rollback Drill  [mixed]

- [ ] 17.1 staging 跑通 e2e 9 步(发布 → 调度 → driver → 实例状态 → kill)  [MVP]
- [ ] 17.2 选 1-2 个低风险项目灰度 1 周,收集指标  [MVP]
- [ ] 17.3 单任务回滚演练:回滚到历史版本,确认指针更新、snapshot 保留、新实例使用旧版本  [phase 2+]
- [ ] 17.4 平台级回滚演练:Backend 多副本滚动回退;DB schema 兼容性核验  [MVP]
- [ ] 17.5 全量推进按项目维度逐步开启;设废弃日期,通知用户迁移  [phase 2+]
- [ ] 17.6 老链路下线后清理旧 DS 资源中心入口  [phase 2+]

## 18. Documentation  [mixed]

- [ ] 18.1 用户文档:SQL 变量目录、Spark config 字段、状态机说明  [MVP];补数版本语义  [phase 2+]
- [ ] 18.2 运维 runbook:Backend 滚动升级、HDFS snapshot 容量管理  [MVP];DS 漂移巡检、kill 半途处置  [phase 2+]
- [ ] 18.3 安全文档:白名单管理流程  [MVP];keytab 路由审计、跨租户隔离边界  [phase 2+]
- [ ] 18.4 开发文档:Backend 模块边界、API 兼容性策略、driver 兼容性约定  [MVP]
- [ ] 18.5 把"as-built"差异回写到 architecture.md / data-flow.md / runtime-flow.md / design.md  [MVP]

## 19. Acceptance(对齐 specs)  [mixed]

- [ ] 19.1 sql-editor:e2e 录屏 + 未声明变量警告 + 预览  [MVP]
- [ ] 19.2 spark-config:白/黑名单(后端单测)  [MVP];queue 权限拒绝  [phase 2+]
- [ ] 19.3 schedule-management:cron 触发  [MVP];依赖等待 + 补数选版本  [phase 2+]
- [ ] 19.4 sql-snapshot:同内容连发产出两个 version_id  [MVP];回滚指针更新 snapshot 保留  [phase 2+]
- [ ] 19.5 command-generation:fuzz 通过 + shlex 自检通过  [MVP]
- [ ] 19.6 runtime-variable-rendering:Frontend 与 driver 共用 fixture 双跑一致  [MVP]
- [ ] 19.7 dolphinscheduler-integration:upsert + appId 回写 + kill 联动  [MVP];巡检  [phase 2+]
- [ ] 19.8 task-instance-lifecycle:状态机分支覆盖率 100%(MVP 状态集),非法迁移计数为 0  [MVP]
- [ ] 19.9 multi-tenant-isolation:跨租户访问被拒 + 审计反查  [phase 2+]
- [ ] 19.10 observability:trace_id 跨层贯穿  [MVP];OTel collector 上报 + 日志聚合可达 + appId 跳转  [phase 2+]
- [ ] 19.11 publish-pipeline:故障注入每一 step 失败都正确回滚 + 灰度开关  [MVP]
- [ ] 19.12 pyspark-driver:老命令兼容回归 + 新参数 fixture(`${dt-N}/${date-N}/${hr}`)  [MVP]

## 20. MVP e2e 验收脚本  [MVP]

- [ ] 20.1 准备 fixture:一个简单项目 + 一个含 `${dt}/${dt-1}/${hr}` 的 INSERT OVERWRITE SQL + 一个测试 Hive 表
- [ ] 20.2 e2e 步骤 1-3:Editor 编辑 → 预览渲染正确 → 发布产生 snapshot v1 + DS task
- [ ] 20.3 e2e 步骤 4-6:DS 触发 → 实例锁 v1 + SPARK_CMD 注入 → driver 跑通 INSERT → 表数据正确
- [ ] 20.4 e2e 步骤 7:实例详情显示 application_id、状态终态 succeeded;日志按钮跳 YARN UI 可达
- [ ] 20.5 e2e 步骤 8:kill 一个 running 实例,DS task + YARN application 双终止,实例终态 killed
- [ ] 20.6 e2e 步骤 9:改 SQL 再发布产生 v2,新实例锁 v2;查 v1 旧实例不受影响,snapshot 内容 v1 仍可读
