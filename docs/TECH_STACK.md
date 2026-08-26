# 技术栈与代码地图

最后更新：2026-08-26 15:02 +08:00

## 运行与桌面

| 层 | 技术 | 当前事实 |
|---|---|---|
| 操作系统目标 | Windows | 当前为便携 Windows 桌面应用 |
| Python | 3.12.13 | 便携 `runtime/python/python.exe` 或源码克隆的 `.venv`; `DBAGENT_PYTHON` 可显式覆盖 |
| 桌面容器 | pywebview 6.2.1 | 启动器创建原生窗口，页面由本地 bridge 提供 |
| 本地服务 | aiohttp 3.14.1 | HTTP、WebSocket、静态资源与 API |
| 启动入口 | `dbagent_launcher.pyw` | 复用同项目已鉴权 bridge，否则启动 bridge、选择空闲端口、加载图标与窗口；不再调用退役 service exit，不主动终止端口占用进程 |

## 前端

| 层 | 技术 | 当前事实 |
|---|---|---|
| UI | 原生 HTML/CSS/JavaScript | 无 React/Vue 构建链，便携、直接加载；结构化单行录入弹层以横向 schema 表格显示真实示例行、字段类型和值/默认/NULL 模式；自定义建表弹层只提交表名、类型白名单和受控约束，两者提交后均只进入变更预览 |
| 主页面 | `runtime/app/frontends/desktop/static/db.html` | 单机数据库工作台；支持会话、数据源、模型配置、问答、结果表、图表、语义层、只读调度、审计和受控写入。普通及分支表格默认 10 行，可展开当前已返回数据；历史使用独立有界富快照恢复表格、SQL 和真实总行数。API Key 只写入 Git 忽略的本机 `model_profiles.json`，界面仅显示是否已配置和尾部提示。写入与建表只打开结构化表单，最终执行仍以后端预览和显式确认为准。 |
| 主题 | `calm-theme.css` | 克制风格和响应式覆盖 |
| 图表 | 本地 ECharts | `desktop/static/vendor/echarts.min.js`，不依赖 CDN；后端在单个范围化只读连接上为基础业务表生成时间趋势/分类贡献/记录规模，排除 FTS 虚拟索引及影子存储；响应包含可见行数、分组数、覆盖率、截断状态和确定性摘要。前端使用 12 色稳定调色板、响应式卡片和视口附近惰性绘制；命中同一缓存时保留现有实例，每 5 秒轻量指纹探针只在数据库变化或手动刷新时替换数据 |
| 字体 | 系统字体栈 | 不捆绑字体二进制，使用 Windows/macOS/Linux 可用的系统 UI 与等宽字体回退 |
| 图标 | PNG/ICO | `dbagent-icon-v2.ico`、favicon 和源 PNG |

## 后端与 Agent

| 模块 | 技术/职责 |
|---|---|
| `dbagent_core.py` | Python dataclass 协议、独立 `BasicConversationRouter` 基础沟通分域、自研操作规划、SemanticCatalog 2.8、MultiMetricAggregatePlan 1.0、TrendAggregatePlan 1.3、DimensionAggregatePlan 1.2、CalendarFilterPlan 1.2、IANA/DST UTC 换日、SQL 词法安全、SchemaRelationAnalyzer、OperationGraph 3.1、schema 发现、RAG、写入安全与确认，以及 NL2SQL 双合同主链。`IntentRouter` 对不完整录入要求模型输出 `interaction=guided_insert` 和可选 `target_table`；目标表按授权 schema 精确复检，非法输出退回选表，明确高风险写操作与模型故障保留本地安全守卫/兜底。基础沟通仅整句高置信匹配问候/感谢/身份/能力/用法/告别/简单情绪，零模型、SQL 和数据读取；礼貌前缀后仍含数据库指令时继续进入 DB 主链。Schema 上下文对超大结构执行全库 token 频率降噪、保守英文单复数归一和边界匹配，按“可信匹配表间唯一最短声明 FK 路径 → 最多 64 个问句相关列 → 命中表主键 → 全局评分”分配 384 列预算；等长多路径不自动选边，无明细表以分块索引保留全部名称和省略数。token/词频/稀疏业务字典分析按表顺序、列名、PK/FK 和字典元数据签名预编译；相同标识符共享不可变结果，FK 图按多表命中惰性构建，结构原地变化自动失效，并发冷启动只发布完整索引。`RelationalAlgebraContract 1.10` 在首次模型调用前由问句/schema/显式字典编译有序物理/聚合输出、统一结果行粒度、输出所有者与实体键、聚合主体、业务/物理类型域、类型化过滤/排序、唯一 FK 路径、聚合阶段、比率分母、相关键、集合交/差/跨多跳关系的同输出实体多值覆盖、实体/输出组合去重、spending 金额 SUM、问句约束的谓词字面量来源、布尔修饰范围歧义与证据化 tie 策略；最外层投影解析跳过 CTE/subquery，显式 `also` 输出可跨句合并。唯一物理样本列上的显式引号精确过滤、带过滤的限定负关系、单数/保守复数 superlative 基数、精确值通配扩大门禁、`ALL_VALUES` 可见元组集合去重和已证明实体键的稳定单行 tie breaker 继续保留。唯一标量阈值证据优先于入向 FK 关系计数推断，唯一 schema 输出绑定优先于旧投影启发式。作用域有两种合理解释时在模型调用前澄清，澄清结果编译为强类型布尔过滤并贯穿候选搜索与语义门禁。`RelationalScalarRankingPlan` 和 `RelationalGroupedAggregatePlan` 分别编译闭合的标量 arg-min/arg-max 与保留零事实的实体标签+关系计数；关系排名计划按稳定实体键分组，显示名称不能替代身份键。统一 Local Contract Compiler 支持完整投影、稳定排序、可见元组 `DISTINCT` 和唯一抽样枚举的大小写/精确操作符编译，并可在最多 4 步内按“冲突必须变化、SQL 不得循环”单调组合；最终重跑全部范围、关系、只读和语义门禁，零额外模型调用。比率总体只接受有界问句范围和经唯一 FK 连接的 evidence 列，函数名/分子条件不能污染分母。未覆盖形状整句回退模型；`BoundedCandidateSearch 1.1` 只在高置信冲突后生成最多 3 个候选，每个重新经过访问范围、关系、单语句只读和完整语义门禁，只有唯一合格替代可执行，多解/无解 fail-closed。模型 `QueryIntentContract 1.0`、隐私最小化 grounding、空结果复核和脱敏错误传播继续保留；不按 benchmark gold 自动改写已接受 SQL。 |
| `RelationalGroupedMetricsPlan 1.0` | 物理 schema 绑定的通用分组多指标 IR；当问句含一个精确维度、2–6 个 COUNT/SUM/AVG/MIN/MAX、有界字面量过滤，且所有指标属于同一事实表时编译。同表直接执行；两表仅接受唯一声明 FK 或用户精确等值边，拒绝多 FK 歧义和多事实扇出。方言渲染支持 SQLite/MySQL/PostgreSQL，渲染前复核列、数值类型、聚合、关系、过滤和排序；未完整表达的问句不部分执行。 |
| `StructuredInsertWorkflow` | 本地 SQLite 单行录入专用执行器；消费已结构化识别的 `guided_insert`，只暴露授权 schema 和经 `SQLSecurity` 读取的一行示例。模型只决定交互意图和建议表，不生成表单 SQL；提交使用字段白名单、值/默认/NULL 显式模式和本地类型化构造单条 INSERT，然后复用 `WriteSecurity → WritePreviewer 回滚 → WriteProposal → confirm_write`；不支持 BLOB、远程只读连接或行级凭据 |
| `StructuredCreateTableWorkflow` | 本地 SQLite 自定义建表执行器；后端重新校验 1–64 个字段、标识符、8 类 SQLite 类型白名单、单主键/整数自增/必填/唯一与受控默认值，确定性编译单条 CREATE TABLE。不让模型生成 DDL，必须复用写校验、事务回滚预览、一次性确认单、admin 批准、审计和执行前复检；远程或任何数据范围受限凭据拒绝 DDL |
| `timezone_release_contract.py` | 项目自带 IANA 发布清单、版本解析、ZIP/TZif 加载、SHA-256 与区域/探针校验、确定性归档构建、活动发布原子切换和清单恢复 |
| `timezone_releases/` | 可并存的版本化 TZif ZIP 与 `manifest.json`；当前完整归档 `tzdata 2026.3` / IANA `2026c`，598 区域、8 个跨区域/历史探针 |
| `scripts/manage_timezone_release.py` | 纯离线 `status/prepare/activate/rollback` 管理入口；候选先暂存验证，同版本不同内容和半更新被拒绝 |
| `scripts/manage_audit_ledger.py` | 审计账本离线管理；除既有备份/恢复/目标命令外，提供 `target-history` 与 `check-target-health`。目标配置/替换/探测/同步均需显式确认且不删除历史，健康检查只读并用退出码暴露最近失败、过期或包校验异常 |
| `scripts/manage_local_roles.py` | 本地角色 token `status/issue` 工具；默认只显示角色能力与指纹，显式 issue 才输出 viewer/operator 凭据 |
| `scripts/manage_local_identities.py` | 本地个人凭据 `list/issue/revoke` 离线入口；发行必须显式选择 `--all-databases` 或一个以上 `--database-ref`，可重复 `--table DATABASE_REF:TABLE` 和 `--column DATABASE_REF:TABLE:COLUMN` 表达本地 SQLite 表/字段范围；CLI 只做结构校验，原始 token 只在发行时显示一次，发行/吊销受审计完整性门禁和双事件约束 |
| `scripts/create_demo_database.py` | 生成版本化 `2026.08.18-v1` 电商 SQLite 人工测试夹具；固定 8 表、外键/日历/文本/零事实样本，生成时执行 quick check、外键校验和 Windows 安全的显式关闭+原子替换，`--force` 只重置精确输出文件 |
| `desktop_bridge.py` | aiohttp API、token/CORS、共享角色/个人凭据、数据库/表/字段/行范围中间件、上传、数据源、会话、图表、调度和静态页面。模型配置委托 `ModelProfileStore`，上传落盘委托 `UploadStorage`；bridge 不包含通用 Agent 会话或工具运行时。会话将模型用紧凑上下文与 UI-only 只读富快照分离；写表单、确认单和 DDL 均保持角色、范围、预览、审计和一次性确认边界。 |
| `model_profiles.py` | 线程安全的本机 JSON 模型配置 CRUD、原子替换、字段校验和脱敏公开视图；在线连通性测试仅返回状态，不回显 API Key。运行文件 `runtime/app/model_profiles.json` 被 Git 忽略，仓库只提供无真实凭据的示例。 |
| `model_gateway.py` | 项目专用 OpenAI-compatible `chat/completions` / `responses` 文本传输；支持流式/非流式解析、可重试状态、连接/读取/总截止时间、协作取消、代理/TLS 配置和线程级 token 统计。不提供工具注册、自治循环、通用会话或提供商故障转移编排。 |
| `upload_storage.py` | 上传文件名清洗、会话桶哈希、随机落盘名和有界保留清理；bridge 只通过该边界写入上传目录。 |
| `db_sessions_store.py` | SQLite 会话、待澄清状态和数据库+表/字段范围指纹持久化；`messages.display_payload` 保存最大 768 KiB 的 UI-only 只读结果快照，`get_history` 永远只返回紧凑 role/content，`get_session` 才恢复富结果。旧消息表在线新增该列，旧会话迁移为 `all`，表级 v1 指纹保持兼容，显式事务关闭连接 |
| `db_semantic_store.py` | 独立 SQLite 语义目录；按稳定数据源标识保存八类定义，普通指标过滤、比率公式、业务日历、维度层级/固定过滤和时间默认粒度以分类型结构化 JSON 持久化；兼容旧维度层级载荷，并提供内容版本快照和单事务批量合并 |
| `db_audit_store.py` | 独立 SQLite 追加式操作审计账本；受控详情字段白名单（建表只新增字段数量，不记字段名/默认值原文）、不可逆指纹、连续序号与 SHA-256 前向哈希链、查询/校验/脱敏导出、批准/终态未决对账、追加式管理员处置、30–3650 天非破坏性保留评估、原子外部前缀归档、绑定文件 SHA-256/head hash 的本地/外部备份与离线恢复、配置式文件系统目标/能力探测/同步状态/最新包复验、本地/外部隔离演练、DB/WAL/SHM 现场评估、严格损坏证据包和带原现场自动回滚的异常恢复，以及可移出的历史前缀锚点；不提供历史更新/删除 API |
| `db_access_control.py` | `local-rbac-v1` 角色 token HMAC 派生、固定时间匹配、viewer/operator/admin 能力矩阵与 HTTP 最小角色策略 |
| `db_identity_store.py` | schema v5 独立 SQLite 本地个人凭据目录；仅持久化 SHA-256 token 哈希、名称/角色/创建/到期/吊销时间、`all/restricted` 数据库范围、每库可选表、每表可选字段集合和结构化行条件，范围引用为稳定 SHA-256；提供 v1/v2/v3/v4 保守迁移、强制有效期、认证、脱敏列表和不可逆单条吊销 |
| `db_chart_cache.py` | schema v2 本地图表快照缓存；以稳定数据库引用＋访问范围为快照键，每张基础业务表独立保存最多 120 个聚合点及业务摘要，事务替换并校验数量、唯一表名、单图大小和 128 表上限；运行态 SQLite 位于 `frontends/data/`，不进入源码仓库 |
| `db_scheduler.py` | 线程式本地只读 SQL/NL 调度；SQL 保存/执行双重只读门禁、NL 写确认边界阻断、统一调度事件和脱敏引用日志；启动时停用旧 SQL 写任务并脱敏旧原文日志 |
| `nl2db_evaluation.py` | 固定临时 SQLite、64 表合成 Schema、规划/确定性日历/确定性维度/确定性趋势/操作图/关系事实/参考 SQL/安全策略评测；12 项可选真实模型通道、脱敏身份、提示词/数据集指纹、逐题延迟和进度报告 |
| `scripts/run_spider_benchmark.py` | Spider 1.0 官方 dev 真实模型评测；固定难度/数据库轮转抽样、schema-faithful 空 SQLite、生产 NL2SQL/只读安全链、官方忽略值 Exact Set Match、旧解析器兼容适配、模型查询合同/关系代数合同/候选搜索/本地关系计划诊断逐题记录与聚合；summary 单列关系路径、聚合、过滤、排序、集合、去重计数、比率和相关性八类目标 IR 覆盖及本地计划执行/Exact。评分副本可为旧解析器补 `AS`、移除投影别名，并只对 schema 已知的限定简单列名及 FROM/JOIN 表名去安全双引号；原始生产 SQL不变。Prompt 契约哈希完整 executor/schema 上下文源码，动态提示或重试变化后拒绝复用旧检查点；Spider 用于暴露架构缺口和外部观察，不以 gold SQL 结构驱动生产渲染，不冒充执行准确率或官方排行榜提交 |
| `scripts/spider_execution_scoring.py` | 独立 Spider 单库/多库 denotation 比较器；物理只读 SQLite、执行超时/结果上限、bag 重复、gold `ORDER BY` 顺序、全局列排列及上游安全 `DISTINCT` 后处理。导出固定 comparator 契约，支持严格产品口径和上游兼容 TSA |
| `scripts/rescore_spider_execution.py/.cmd` | 对存量 Spider SQL 做零模型单库重评分和当前语义门禁反事实重放；先校验历史数据哈希、当前官方 dev 全量身份、166 个规范 schema、固定样本身份及数据库 quick-check/清单。无预测查询时重放保存的被拒候选；结果保存脱敏关系合同与类型化冲突约束。本地编译结果必须重新真实执行，非稳定并列策略的旧正确→修复错误会使脚本非零退出 |
| `scripts/rescore_spider_test_suite.py/.cmd` | 对同一固定候选在官方 20-schema/695-database 扰动集执行只读 TSA；验证 evaluator 提交、数据库归档/源结果/单库结果哈希、SQLite header、主库 quick-check 和完整 DB 清单，同时输出保留 `DISTINCT` 的严格产品口径与上游兼容口径，并对全部本地编译 SQL 做多库复核；非稳定并列策略回归时非零退出 |
| `scripts/verify_spider_test_suite_upstream.py/.cmd` | 使用隔离在 D 盘 Git 忽略目录的 `sqlparse` 和固定上游 `eval_exec_match`，逐题交叉校验本地上游兼容 TSA，防止自研比较器语义漂移 |
| `scripts/replay_bird_architecture.py/.cmd` | 哈希绑定固定 BIRD Mini-Dev 60 条历史结果、官方提交、数据/字典/11 个 SQLite 库；重建与生产一致的 FK/列字典 schema，零模型重放当前合同和本地编译器，保存脱敏合同/冲突快照，并对本地修复真实执行复核。历史正确项被新架构拒绝或本地修复执行回归时非零退出 |
| `scripts/analyze_spider_runs.py` | 2 次以上 Spider 完成运行的可比性和稳定性分析；强制核对 dev/tables SHA-256、样本指纹/大小/seed、模型、Prompt 与评分契约，生成 Exact 均值/样本标准差、稳定通过/失败/波动题、事后 oracle 上限、稳定失败观测、目标 IR 并集与全调用延迟 JSON/Markdown。Oracle 明确不是生产选择策略 |
| `scripts/run_bird_benchmark.py` | BIRD Mini-Dev 500 道 SELECT 的真实 SQLite 执行评测；固定难度/数据库轮转抽样、专家 evidence、官方列字典和已声明 FK 恢复、生产 NL2SQL/SQLSecurity、独立物理只读预测/金标执行、官方集合式 Execution Accuracy、严格顺序诊断、模型查询合同/本地关系代数合同/候选搜索/本地关系计划逐题记录及触发/选择/歧义聚合；summary 单列关系路径、聚合、过滤、排序、集合、去重计数、比率和相关性八类目标 IR 覆盖及本地计划执行/Execution Exact。支持可重复 `--case-id` 定向集、数据库/字典/数据/Prompt/评分契约、逐题原子检查点和脱敏模型身份。基础设施失败与语义分母分离，终态模型错误停止继续提交新题并可在 `--resume` 时重试；CRUD 不执行、不自动确认 |
| `model_baseline_contract.py` | 真实模型运行的脱敏追加式历史、严格 schema、原子写入、同题集版本比较、改进/回归和延迟差异 |
| `evaluation/nl2db_cases.json` | 版本化固定问题、人工意图、预期计划、确定性日历/维度/趋势结果、64 表关系事实、参考 SQL/结果、对抗安全样本和 12 项独立真实模型题 |

## 数据库与文件

| 能力 | 实现 | 状态 |
|---|---|---|
| SQLite | Python `sqlite3` | 核心路径已验证；查询只读、写入受控 |
| CSV | 标准库 `csv` → SQLite | 已实现并验证基础上传链路 |
| Excel `.xlsx` | openpyxl 3.1.5 → SQLite | 已实现并实测，多 sheet 转多表；旧 `.xls` 不受支持，选择器已移除，前端与 bridge 明确提示转换，服务端在解码/落盘前返回 415 |
| MySQL | `PyMySQL 1.2.0` 只读适配器 | MySQL 8.4.9 本机非默认端口实测；表/列/PK/FK/行数/抽样、500 行上限、分组多指标、JOIN、拒写、多语句、超时、错密码/关端口通过；不开放远程写入与细粒度凭据 |
| PostgreSQL | `psycopg2-binary 2.9.12` 只读适配器 | PostgreSQL 17.11 本机非默认端口实测；PK/FK 通过最小权限可见的 `pg_catalog` 发现，与 MySQL 执行同一只读合同全部通过；当前只覆盖 `public` schema |

## 当前关键依赖版本

- 直接运行依赖：aiohttp 3.14.1、pywebview 6.2.1、openpyxl 3.1.5、requests 2.34.2、tzdata 2026.3、
  PyMySQL 1.2.0、psycopg2-binary 2.9.12。
- `requirements.txt` 固定 7 个直接依赖；`requirements.lock` 固定 Windows x64 / CPython 3.12
  的 26 包完整依赖闭包及每个下载文件 SHA-256。`bootstrap_dev.cmd` 使用 `--require-hashes` 安装并
  运行 `pip check`；`.python-version` 固定 3.12.13。
- 项目归档 tzdata 2026.3（IANA 2026c；运行时从哈希绑定 ZIP 读取，不依赖主机 `TZPATH` 或已安装包内容）
- Python 包 tzdata 2026.3（只作为当前便携发行和人工准备新候选时的本地来源；业务查询不直接读取该包）

## 源码发布与本机运行态边界

- 私有 GitHub 源码仓库：`jamesdffgy-source/DB-Agent`。
- GitHub 首页以英文 `README.md` 为默认入口、`README.zh-CN.md` 为等价中文入口，使用实际桌面
  UI 与固定演示 SQLite 生成的 `docs/assets/dbagent-overview.png`；不使用概念性生成图冒充产品
  界面。`SECURITY.md` 定义漏洞信息最小化和私有报告边界。上述首页文档、截图与安全策略均由
  仓库卫生/源码指纹覆盖；README 不把本地固定子集成绩描述成官方排行榜提交。
- 版本库包含应用源码、静态前端资源、测试、固定评测、项目归档时区包、门禁脚本和长期文档。
- `.gitignore` 排除便携 Python、本机 `model_profiles.json`、上传文件、临时目录、会话/语义/审计/身份数据库、bridge token、日志、公开 benchmark 数据/原始运行结果和根目录个人文档，防止本机秘密或运行态影响发布候选与源码指纹。
- `demo_data/dbagent_demo.sqlite` 由生成脚本在本机创建并受现有 `*.sqlite` 排除；可复现生成器和 `docs/DEMO_TEST_GUIDE.md` 属于源码与测试资料。
- 源码克隆不包含便携 Python，但可在 Windows/Python 3.12 通过哈希锁创建 `.venv` 后验证和启动；
  本轮第二个独立 clean worktree 已从零安装并通过 350 项回归和完整非登记门禁。GitHub Actions
  工作流已纳入源码，但首轮因账户付款/额度在 runner 分配前被阻断；远程实际首跑、
  签名安装包、SBOM/依赖许可证汇总和发行签名仍是后续事项。
- 根 MIT LICENSE 归属于 DB-Agent contributors；ECharts 与项目 tzdata 归档的来源、署名和
  完整许可证位于 `THIRD_PARTY_NOTICES.md`/`third_party/licenses/`。发布树不包含字体二进制。

## 安全机制

- bridge 随机本地 token，支持 Header/Cookie 引导，不开放任意跨域。
- SQLite 查询连接使用 `mode=ro` 和 `query_only`。
- 查询仅允许单条 SELECT/WITH，拦截写关键词并限制结果行数和执行时间。
- 安全词法层先屏蔽字符串、引号标识符、行注释和块注释，再检查真实代码分号、关键字和 WHERE；只读路径拒绝列明的文件/扩展/系统函数。
- 写 SQL 限定操作种类，UPDATE/DELETE 强制 WHERE，事务预览后回滚。
- 写确认单一次性消费并绑定数据库路径；执行前再次校验，失败回滚。
- 审计账本不保存原始问题、SQL、结果行、路径、连接串或凭据，只接收白名单元数据和不可逆指纹；事件按序号/前序哈希/事件哈希链接，查询与导出可检测普通修改或断链。
- 用户写确认与语义目录变更在实际执行前必须成功写入批准/拒绝事件并验证已有链；账本不可用或完整性异常时 fail-closed。自然语言只读结果审计采用 fail-open，避免日志故障改变只读查询结果。
- 用户数据库与本地审计库是两个独立事务域，执行后结果事件不能与用户数据库提交形成原子事务；批准/待处理与终态自动对账可发现缺口。管理员可追加绑定原序号/数据库/correlation 的处置事件，必须提供只保存 SHA-256 的外部证据引用；这仍是人工判断，不能推断用户数据库原子终态。当前已有手工外部备份/归档/锚点，但仍缺自动外部目标、复制、签名和生命周期执行；SHA-256 链不是数字签名或 WORM，不能抵御拥有本地文件完全写权限并同步重算所有同权限域文件的攻击者。
- 调度只允许自动读：SQL 仅接受单条 `SELECT/WITH` 并使用物理只读连接；自然语言产生写确认单时停止，不自动批准。调度配置变更在审计异常时 fail-closed，运行结果审计 fail-open 并返回警告。
- 审计备份使用 SQLite backup API、事件链/count/head hash 和文件 SHA-256 交叉校验；恢复只在离线脚本开放，要求预期当前 head hash、显式确认、恢复前安全备份和临时库校验。备份仍在本地同一权限域，不是异地备份或不可变存储。
- 隔离恢复演练使用 SQLite backup API 把已验证备份物化到管理员显式指定的非受管目录；校验事件数/head hash 和当前账本前后不变后清除临时数据库，只原子保留带载荷 SHA-256 的版本化报告。报告复验重新绑定仍可用的源备份；不是签名、RTO/RPO 或灾备切换证明。
- 外部备份采用单个未压缩 ZIP，只允许根目录 `manifest.json` 与 `audit.db` 两项；清单绑定源备份、数据库 SHA-256、事件数/head hash 和自身规范载荷 SHA-256。验证流式物化 SQLite 并检查完整链，不依赖当前账本或原本地备份；压缩、加密、额外/穿越条目均拒绝。外部来源可进入隔离演练和普通真实离线恢复，后者继续要求 current head、固定确认和恢复前安全备份；链损坏时普通路径仍 fail-closed。
- 损坏链专用离线恢复先对当前 DB/WAL/SHM 做前后稳定 SHA-256/大小快照，并在独立临时副本上验证链，避免评估本身创建附属文件。assessment token 同时绑定文件清单和错误结论；证据 ZIP 只包含清单及实际存在的固定名原文件条目，逐项验证未压缩/未加密、大小、SHA-256、评估令牌和载荷 SHA-256。恢复必须固定确认并先产生可独立复验的非受管证据包；切换前建立受管临时回滚副本，原子替换后复验失败会恢复原 DB/WAL/SHM 并重新核对令牌。该路径仍要求桌面进程完全退出，不是签名、WORM 或在线自愈。
- 配置式外部目标当前只支持一个已存在的本地/挂载文件系统绝对目录；目标不得位于审计受管目录或其祖先/后代。配置以随机 target ID 和载荷 SHA-256 防普通漂移，替换必须绑定旧 ID。显式 probe 实际执行随机临时文件独占写、fsync、同目录 `os.replace`、读回和清理；sync 创建新本地备份与严格外部包、复验后才原子记录最近成功状态，配置在同步期间变化则不登记，失败不覆盖旧成功，任何路径都不删除既有包。latest verification 同时匹配包内容、整个 ZIP SHA-256 和状态；不提供后台周期执行、远程对象存储凭据或保留删除。
- 同步尝试历史为最多 16 MiB 的追加 JSONL；逐行严格字段和规范载荷 SHA-256，成功保存包摘要，失败只保存异常类型与错误文本 SHA-256，不含路径/错误原文/凭据。读取验证重复 ID、空/破损行并按 target ID 筛选。健康检查要求最近尝试成功、成功时间在 1–8760 小时阈值内，并重新验证最新包；任何失败通过 CLI 非零退出码暴露，不自动调度或发送告警。
- 外部锚点 JSON 只保存事件数、当时 head hash 和规范载荷 SHA-256，并拒绝输出到审计受管目录；验证当前完整链及对应历史前缀，账本增长后仍有效。它没有密钥签名，只有被另存到独立受保护介质/系统时才比同机哈希链增加攻击者难以同步改写的证据，不是自动异地备份、WORM 或可信时间戳。
- 外部归档 JSON 保存从 genesis 开始的完整脱敏历史前缀、前缀 head hash 和规范载荷 SHA-256；同目录临时文件验证后原子替换，并同时验证归档内部链与当前账本对应前缀。保留期评估只建议连续旧前缀，归档不会自动移动或删除事件。归档同样没有密钥签名，留在同机时不是异地备份、WORM 或可信时间戳。
- 本地主 token 为 admin；viewer/operator token 通过 `HMAC-SHA256(admin_token, policy_version + role)` 派生，不落额外秘密文件。中间件先固定时间验 token、再校验同源、最后按方法/路径执行最小角色；DB 主链外的模型配置与凭据管理默认 admin。查看者数据库列表移除本地路径和远程主机/用户。
- 写确认策略在执行前读取未消费提案：非危险 INSERT/UPDATE 且 dry-run 影响 0–100 行允许 operator；DELETE、DDL、未知或超过 100 行要求 admin。越权不消费确认单并登记脱敏 `access_control` 事件。
- 可选本地个人凭据使用 256 位级高熵随机 token，SQLite 只保存 SHA-256 哈希；有效期为 1 小时到 1 年，过期/吊销服务端立即拒绝。schema v5 以规范化关联表保存最多 64 个稳定数据库 SHA-256 引用、合计 256 张表、1024 个字段和 256 条结构化行过滤；行过滤每表最多 4 条，仅支持受控比较/IN/null 操作符并以 AND 组合，不接收 SQL。新 HTTP 发行必须明确 `all/restricted` 并按真实 Schema 验证表/字段/策略字段名。SQLite 表/字段范围继续由 `sqlite3.Connection.set_authorizer` 约束 SQL、RAG、dry-run 和最终写事务；行范围用连接内 TEMP 同名过滤视图承载，再由 authorizer 只允许该视图读取 `main` 基表，拒绝 `main.table`、CTE 和表值 PRAGMA 元数据旁路。Schema、采样、SQL、RAG、图表和类型推断均读取过滤视图；行级凭据完全只读，鉴权能力和写确认边界同步拒绝写入。字段受限 INSERT/UPDATE 规则保持不变，DELETE/DDL 拒绝。远程数据库因没有已验证的等价执行门禁而拒绝表/字段/行级发行；调度、按库审计和整库摘除对数据受限凭据保守拒绝。受限 admin 不能管理全局凭据或审计备份；v1/v2/v3/v4 凭据保守保留旧范围的全部行语义。凭据变更前审计 fail-closed，审计只保存 `credential_ref`、范围模式/数量，不保存名称/token/数据库路径/表/字段/行值。共享角色 token 继续兼容并保持全库全表全字段全行；本地名称不是外部核验身份，尚无 MFA、设备绑定、远程等价强制、可写行策略或双人审批。
- 多步骤 OperationGraph 只允许 `inspect_relations/query/retrieve/synthesize`，并在任何工具调用前校验节点上限、策略/条件白名单、查询表范围、输入输出契约、依赖完整性与无环性；写入执行器不注册到操作图。
- 明确独立查询只允许 2–6 个分支；每支仅绑定一张唯一目标表，必须按序完整覆盖图目标且直接进入综合依赖，拒绝关系推断依赖、JOIN、CTE、多表 FROM 和越界表名。操作图中间数据限制为查询 100 行、证据 20 条、每查询综合预览 20 行、提示总长 12000 字符。
- 涉及多张目标表的查数先通过共享关系分析器检查 schema 已声明 FK 图或用户明确给出的合法等值关系；仍不连通时先澄清或跳过 query，不调用 NL2SQL。
- 查询在进入模型前检查会改变语义的缺口：跨表关系、未定义派生指标、未指定聚合字段、维度下钻目标、多个时间字段、含糊时间范围、趋势时间粒度和缺少适用业务日历；补充内容以结构化标签累积到原请求。
- 语义目录写入前校验当前 schema；维度/时间/业务日历必须绑定真实字段，同名维度层级必须同表且级次唯一，时间默认粒度必须来自固定枚举且同一物理字段不可冲突。日历例外表的日期、名称和工作日覆盖字段按存在性及类型校验；聚合、指标过滤和比率仍受既有白名单、数量和同表边界约束，不能携带 SQL 片段。
- 业务日历包含财年起点、显式起始年/结束年标注、UTC/IANA 风格时区标识、时间值存储基准、`fixed_offset/iana_tzdata` 换日方式、可选固定业务 UTC 偏移、固定时区数据版本、ISO 周起始/周末及节假日表绑定。旧时间戳配置标为 `legacy_default/unspecified`，旧 UTC 配置继续归一为固定偏移。
- 确定性日历对 SQLite 声明型 DATE 编译日期边界，对显式 `local_datetime` 取已存业务墙上日期，对显式 `utc_datetime` 使用 -840..840 分钟固定偏移或已归档版本的 IANA 动态换日；支持财年、明确财季、工作日、COUNT、普通受控指标、指标过滤与同表枚举条件。动态换日由只读连接注册的确定性三参数 UDF 执行，SQL 携带版本令牌；旧日历继续读取其原归档。TEXT、远程方言、复杂形状和额外条件保守回退 NL2SQL。
- 确定性并列指标仅对 SQLite 同表 2–6 个受控普通指标生成一条聚合 SELECT；指标自身过滤编译为独立 CASE 条件聚合，最多一个同表枚举编译为全局 WHERE。比率、自由算术、维度/趋势/日历混合、多个枚举、7 个以上指标、跨表与远程方言整体回退，不部分执行。
- 确定性维度仅对 SQLite 单表、一个业务维度或同表层级路径、COUNT/1–6 个普通受控指标生成 GROUP BY；复用维度固定过滤、指标局部过滤和最多一个同表枚举全局过滤。自由条件、比率、7 个以上指标、跨表与远程方言保守回退 NL2SQL。
- 确定性趋势仅对 SQLite 单表、一个受控时间字段、日/周/月/季度/年粒度、COUNT/1–6 个普通受控指标生成时间分桶 GROUP BY；支持完整 ISO 起止范围、最多 3660 天的相对日/周窗口、业务日历财年/财季/工作日窗口、指标局部过滤和最多一个同表枚举全局过滤。DATE 直接分桶，声明型时间戳必须通过同字段唯一业务日历提供显式本地、UTC 固定偏移或固定版本 IANA 口径；无效/非 UTC 源值排除。滚动月/季/年、冲突窗口、自由条件、比率、7 个以上指标、跨表与远程方言保守回退 NL2SQL。
- 语义配置导出不携带本地 ID、时间戳、路径或连接凭据；导入先校验格式/大小/重复术语/schema/合并目录，再通过绑定数据库、目录版本和当前表/字段范围指纹的一次性令牌执行原子合并。字段外既有定义不会进入列表或错误原文，同名导入不能覆盖。当前只有 merge 模式；导出 `schema_version=8`，导入兼容 v1/v2/v3/v4/v5/v6/v7/v8。

## 测试与门禁

- 测试框架：标准库 `unittest` + `aiohttp.test_utils`。
- 基线文件：`runtime/app/frontends/test_security_regressions.py`。
- 当前基线：413 项测试；最新新增分组多指标本地计划、原始 SQL 多语句、协作取消、
  远程 DECIMAL/日期类型与 MySQL/PostgreSQL PK/FK/行数发现回归；此前会话富快照、SQLite/CSV 正常上传 API 回归覆盖只读结果切换
  会话后恢复、模型上下文与 UI 快照分离、旧 messages 表迁移、Base64 解码、会话目录落盘、
  SQLite 校验/接入、CSV 转库和数据读回，`.xls` 解码前拒绝继续保留；此前 5 项基础沟通边界回归覆盖无模型本地回答、数据库操作不被
  社交前缀抢占、澄清候选优先、HTTP 多轮中断恢复、审计分类和前端呈现；此前预编译 Schema
  索引复用、列名/FK 原地变化自动失效和并发冷启动一致性，以及 3000 列 Schema 上下文召回/
  长度压力、唯一 FK 路径保留、等长多路径无权威和保守
  复数归一；此前 Spider 单库/多库只读执行比较、严格/上游兼容 `DISTINCT` 口径、
  上游 comparator 契约、精确引号字面量/通配扩大门禁、限定负关系、superlative 基数和稳定 tie breaker；此前谓词字面量来源、跨多跳
  `ALL_VALUES` 输出粒度、spending SUM、描述组合去重和布尔范围澄清/澄清后强类型验收；此前源码发布卫生、精确/哈希依赖锁、当前模型模板、运行时选择和
  LF/CRLF 指纹稳定性回归；此前按问句位置的物理输出顺序、实体标签+关联计数混合输出、
  关系数量阈值与本地投影修复、Spider 基础设施分母/断路/续跑及旧解析器 JOIN 适配；此前标量 arg-min、
  类别值使用粒度、类型化过滤、集合、
  Native Relational Planner、候选搜索、查询意图合同、业务字典、隐私最小化 grounding、语义
  fail-closed、reviewer、BIRD 字典/官方 FK、OpenAI 兼容 `/v1`、日期/枚举、schema 与关系门禁均保留。
- 固定评测入口：`scripts/run_evaluation.cmd`；默认离线生成 `docs/EVALUATION_REPORT.md` 与 `docs/EVALUATION_REPORT.json`，`--with-model` 才运行真实模型 NL2SQL。`scripts/run_model_baseline.cmd` 显式运行模型并把脱敏结果追加到 `docs/MODEL_BASELINES.json`。
- 当前固定评测版本：`2026.08.17-v23`，离线仍为 134 项；规划 46、确定性并列指标 4、确定性日历 6、确定性维度 8、确定性趋势 17、动态操作图 12、大 Schema 关系事实 5、参考 SQL 6、安全策略 30，直接结果 134/134。真实模型通道独立为 12 项；已配置 `gpt-5.6-sol` 的最新单次结果 12/12，不并入离线合计，也不代表真实业务准确率。
- 公开 benchmark 入口：`scripts/run_spider_benchmark.cmd` 与 `scripts/run_bird_benchmark.cmd`。Spider runner
  检查点 schema v2 / scoring adapter v5 单独统计 scoreable、attempted、target 和基础设施失败；
  HTTP 402/鉴权/限流/服务中断不入 SQL 正确率分母，首个终止型错误后停止提交并保留续跑点。
  Spider 固定 100 题最新 1.8 已用同一 DeepSeek V4 Flash Prompt/样本/评分契约完成三次从零生成：
  Exact 43/44/43、生产 SQL 97/97/97、官方 AST 73/75/71；单发布库执行一致 76/77/76，官方
  20-schema/695-database 严格产品 TSA 61/62/60、上游兼容 TSA 64/61/60。严格 TSA 稳定通过
  58、稳定失败 36、波动 6，每轮单库假阳性 15/15/16；三轮上游交叉校验均为 100/100。
  当前 1.10 对 run1 的零模型生产路径重放为 91 接受/8 拒绝/1 澄清；单库本地改进 3、普通回归 0。
  官方多库对 8 条本地计划复核得到严格改进 2、普通回归 0；BIRD 固定 60 条重放 55/5，旧正确
  误伤 0、本地修复改进 1、回归 0。重放器 v2 会先执行当前原生关系计划，再尝试旧 SQL 局部编译。
  旧 1.5 固定候选 TSA 74/75 只保留为不同 Prompt/时间点的历史回放基线。BIRD 当前
  1.1 固定 60 题真实库为集合 EX 41/60、严格行序 38/60、生产查询 56/60；难度 18/20、14/20、
  9/20；查询意图覆盖 60/60、一次修复后接受 10、持续冲突拒绝 2。完整方法、成对归因、
  反过拟合边界和限制见
  `docs/BENCHMARK_REPORT.md`。公开数据与原始逐题结果只保存在 Git 忽略目录，真实模型公开 benchmark 不进入
  离线门禁。
- 门禁：`scripts/project_gate.py`，标准 Windows 入口为 `scripts/check_project.cmd`；先执行
  `check_repository_hygiene.py` 的候选文件秘密扫描、依赖/许可证/第三方资产/CI 契约，再覆盖根发布文件、
  文档、项目源码、静态字体/ECharts、JSON 评测集、时区 ZIP 和门禁脚本的指纹、脏工作区文档新鲜度、
  Python 编译、动态回归、固定离线评测、全部时区发布/探针及 UTF-8 JavaScript 语法。文本指纹先把
  LF/CRLF/CR 规范为 LF，二进制保留原字节；`.gitattributes` 同时固定源码/脚本 checkout 策略。
- 门禁状态：`docs/PROJECT_STATE.json` schema v5；状态动态保存受控文件数、源码指纹、回归数、固定评测、
  时区发布、最新脱敏模型基线和仓库卫生摘要，模型服务不会被离线门禁自动调用。精确最新值以该 JSON 为准。

## 端口与运行模型

- bridge 默认监听 `127.0.0.1:14169`。
- 若端口属于其他服务，启动器在 `14170–14179` 中选择可用端口。
- bridge 和 pywebview 都是常驻进程，没有开发态热加载。

## 技术边界

- 不接入外部数据库助手框架；不得把 `engine` 字段解释为外部框架接入口。当前值 `native` 表示自研执行链。
- 前端目前无打包器，不应无必要引入大型 SPA 框架。
- 远程数据库仅开放已实测的整库只读能力；远程写请求在产品边界显式拒绝，不复用 SQLite
  的 `connect_rw`/回滚预览。可复现合同入口为 `scripts/run_remote_database_e2e.py`，凭据只从
  `DBAGENT_REMOTE_TEST_PASSWORD` 环境变量读取。

## 发布与可复现安装

- 发布目标：Windows 10/11 x64，CPython 3.12；`.python-version` 和 CI 固定 3.12.13，锁文件包含
  Windows/Python 3.12 所需 26 个精确版本及哈希。
- `scripts/install_and_start.cmd`：顺序执行 `.venv` bootstrap、设备 doctor 和桌面启动。
- `scripts/doctor.py` / `doctor.cmd`：检查 Python 主次版本、Windows、关键文件、六个运行依赖、
  `runtime/app/temp` 可写性和 WebView2 检测；WebView2 未检测到时给出警告，导入/平台错误则失败。
- `scripts/smoke_startup.py`：随机选择 loopback 端口和 token，真实启动 `desktop_bridge.py`，验证鉴权
  `/status`、app root 和 `/db` 静态页面，再有界终止进程；不连接用户数据库或模型接口。
- `.github/workflows/ci.yml`：全新 Windows runner 创建隔离环境、运行 doctor、正式门禁和启动探针。
- `.github/workflows/release.yml`：只响应语义版本标签；重新 bootstrap、门禁、启动探针，然后通过
  `git archive` 生成版本化源码 ZIP、SHA-256 文件并创建 GitHub release。
- 当前不使用 PyInstaller，不分发本机 `runtime/python` 或 `.venv`，也不声称存在签名安装器。
- README 原创主视觉为 `docs/assets/dbagent-handdrawn-workflow.png`，真实界面证据为
  `docs/assets/dbagent-overview.png`；两者都按二进制进入来源审计和源码指纹。
