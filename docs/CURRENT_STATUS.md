# 当前项目状态

最后更新：2026-08-27 13:00 +08:00

## 发布判断

DBQuill 已达到 Windows x64 源码优先公开发布水准：全新 Windows runner 可以从不含本机运行时、缓存、配置和数据的源码创建 `.venv`，按哈希锁文件安装 26 个依赖，通过完整门禁与带鉴权启动探针，并发布带 SHA-256 的版本化源码包。当前还没有签名原生安装器，因此不能描述成“下载即用”或跨平台产品。

公开展示名为 **DBQuill — Open-Source AI Database Agent**，中文为“DBQuill — 开源 AI 数据库智能体”；仓库短名保持 `DBQuill`。`open-source` 指 MIT 许可下的项目源码，不代表所连接的模型、数据库或用户数据开源。

发布历史清洗已通过删除并重建仓库完成；现有公开仓库随后原位更名为
`https://github.com/jamesdffgy-source/DBQuill`。公开 API 确认默认分支只有一个无父根提交，旧完整 SHA
下的已删文件返回 404，README 和原创图片可匿名访问，GitHub 社区健康文件识别率为 100%。私有漏洞
报告、依赖漏洞提醒和自动安全修复已启用。

## 当前架构

```text
pywebview + WebView2
  └─ Vanilla HTML/CSS/JavaScript + ECharts
       └─ loopback aiohttp desktop_bridge
            ├─ token、CORS、角色与数据库/表/字段/行范围
            ├─ 会话、流式上传、数据源、图表、调度与审计 API
            ├─ ModelProfileStore → model_profiles.json（本机、Git 忽略）
            └─ DBQuillAgent
                 ├─ 基础沟通、意图路由和结构化操作计划
                 ├─ Schema / Semantic Catalog / 关系与歧义门禁
                 ├─ 类型化关系计划与 NL-to-SQL
                 ├─ 检索与只读 OperationGraph
                 └─ 写校验 → 回滚预览 → 一次性确认 → 事务执行

模型请求：DBQuillAgent → model_gateway → OpenAI-compatible text API
```

`model_gateway.py` 只负责文本请求、响应解析、重试、总截止时间、协作取消和用量计数；它不提供工具注册、自治循环、通用会话或多角色编排。模型配置和上传持久化分别由 `model_profiles.py` 与 `upload_storage.py` 管理。

## 已实现能力

- SQLite 完整本地主链：Schema、检索、组合分析、图表、语义定义、调度和受控写入。
- SQLite/CSV/`.xlsx` 使用 multipart 分块上传；前端显示传输与数据库检查进度，后端以 1 MiB 分块、
  200 MiB 原始文件上限和临时文件原子发布约束资源；响应按 JSON 类型读取，失败不会悬挂上传状态；
  UTF-8 文件名安全解码后再清洗，旧版 `.xls` 在文件内容落盘前拒绝。
- launcher 只复用 app root、bridge 协议和上传协议同时匹配的本项目服务；前端上传前再次检查
  `multipart-v1`，旧 JSON bridge 不会再收到数据库二进制并误报 `invalid UTF-8`。
- MySQL 8.4.9 与 PostgreSQL 17.11 整库只读链路：Schema、PK/FK、行数/抽样、受限查询、分组多指标、物理拒写、超时和连接失败。
- 远程连接默认只读；显式选择受控读写后可提出 `INSERT`、带条件的 `UPDATE` 和带条件的 `DELETE`。查询仍走独立物理只读会话；写入使用事务回滚预览、一次性确认、角色审批、执行前复检和失败回滚。真实 MySQL/PostgreSQL 写服务矩阵尚待补证，因此 README 明确区分“已实现回归合同”和“真实服务端已验证”。
- 模型优先识别查询、检索、组合与写入意图；所有输出仍经本地 schema、范围、单语句、关系、语义与执行门禁。
- 实体资料问答使用“明确实体→长词→泛词”召回顺序，每个明确实体先扫完受权表，再允许泛词消耗
  证据预算；文本列按姓名、标题、研究、摘要、描述和证据等语义优先级选择，不再只查物理顺序前 3 列。
- 实体精确命中后排除泛词召回的其他人物；NL-to-SQL 空集只在用户原话含明确实体时回退到受限跨表证据检索。
- 执行层的多轮上下文会跳过“是什么/那他呢/证据呢”等省略轮，从最近有界会话中找到完整用户主题作为执行锚点；
  这与单纯把历史文本送给模型分开，主题会真正进入检索/查询计划。
- “我的意思是他……/我指的是她……”等纠正性指代同样使用执行锚点，并跳过历史中已失去实体的依赖轮。裸问“某老师的工作”会请用户选择
  “任职/职务”或“研究工作/成果”，而不是默认将工作解释为研究方向。含明确姓名和档案字段的规范人物表优先于审计摘要/自由文本提及表召回。
- 表感知单行录入：选择目标表后展示真实字段、类型和一行只读示例；提交只生成绑定当前数据库和权限的预览确认单。
- 受控新建表表单：字段类型和约束白名单、确定性 DDL、管理员预览和显式确认。
- viewer/operator/admin 最小权限；本地个人凭据可限制数据库，并对 SQLite 限制表、字段和结构化行条件。
- 会话模型文本与 UI 富快照分离；表格默认 10 行，可展开当前回答已返回或已保存的全部行。
- 左上角 DBQuill 品牌旁提供“中 / EN”语言选择，首次打开默认英文，主动偏好保存在本机；主要静态控件和动态生成的菜单、
  表单与状态提示使用本地翻译目录，不联网翻译，也不改写数据库对象名、用户输入和结果数据。
- 项目前端 JavaScript 语法门禁同时检查 `db.html` 内联逻辑与静态目录中的项目自有 `.js` 文件；
  新增本地翻译运行时后，不再只验证旧内联入口。
- 图表在线程工作器构建并按访问范围缓存；查询默认物理只读，写入确认只能使用一次。

## 发布体验

- 中英文 README 已改成价值说明、原创主视觉、真实截图、一条命令 Quick Start、支持矩阵、架构、安全和贡献入口。
- `scripts/install_and_start.cmd` 完成创建环境、诊断和启动；`doctor.cmd` 检查 Python、依赖、项目文件、运行目录与 WebView2。
- `scripts/smoke_startup.py` 在随机 loopback 端口启动 bridge，验证带 token 的 `/status` 与桌面页面后关闭进程。
- GitHub Actions 在全新 Windows runner 中执行 bootstrap、诊断、完整门禁和启动探针；标签工作流重新验证后生成带 SHA-256 的版本化源码包并创建 release。Python 3.12 系列选择、`aiohttp` 3.14.3 和全新 runner 门禁均已通过公开验证；无实际命中的 pip 缓存配置已删除，避免保留无意义的 Actions 警告。
- 已补充安装文档、支持入口、原创行为规范、Issue/PR 模板、依赖更新配置和变更记录。
- 原创手绘流程图不含第三方品牌、人物、机器人或可读文字；真实截图继续作为产品证据。

## 当前验证证据

- 51,863,552 字节真实 SQLite 经 multipart 本地上传后成功接入 9 张表，耗时 0.322 秒；bridge 工作集
  观测增量约 0.9 MiB。英文默认、前端零 Base64、SQLite/CSV multipart、`.xls` 拒绝和超限残片清理
  6 项定向回归通过；速度和内存数值只作为当前设备的回归证据。
- 旧 bridge 实际复现 multipart 被当成 JSON 后返回 `invalid request: ... invalid start byte`；协议握手修复后，
  同一 51,863,552 字节 SQLite 以 `中文研究数据库.db` 上传成功，名称保持中文、接入 9 张表、耗时 0.253 秒。
- 前一个 `advisor_research_demo.db` 由用户确认为误上传，只保留为空值/空结果回归样本，不再用它判断目标数据。正确的 `fudan_cs_research.db` 含 33 张表；
  `scholars.id=188` 明确记录肖仰华的 `title=教授、博导`、`advisor_status=博导`，`short_bio` 记录其为复旦大学计算与智能创新学院教授、博士生导师和上海市数据科学重点实验室主任。
  DeepSeek V4 Flash 实测“肖仰华老师的工作”首先返回两个含义选项；回复“我的意思是他担任的工作”后，首条证据为 `scholars` 规范人物行并正确返回上述三项职务。本轮完整回归为 433/433。
- 重命名与受控远程 DML 后完整回归为 419/419；公开 `v0.2.0` 的项目门禁和源码发布工作流均已通过。
- 本轮发布工程定向验证：关键新脚本编译通过；当前环境 doctor 通过；带鉴权启动探针通过。
- D 盘 `D:\DBQuill-Clean-20260827-0130` 全新导出验证：源码不含 `runtime/python`、`.venv`、缓存、凭据或数据库，使用 CPython 3.12.10 在 D 盘从零安装哈希锁定依赖，确认 `aiohttp` 3.14.3；`pip check`、doctor、带鉴权启动探针、419/419 回归和完整门禁全部通过。0.2.0 源码包为 120 个条目、禁止运行态条目 0。
- 当前仓库卫生：103 个候选、96 个文本、7 个直接依赖、26 个锁定依赖、凭据发现 0、合法再分发资产 2 类。
- 当前来源比较：103 个候选对指定参考树 917 个有效文件，SHA-256 精确命中 0，20-token 高重合命中 0；禁止标记命中 0。
- 当前主工作树正式门禁通过：102 个受控文件、419/419 回归、134/134 离线评测、8/8 时区探针、12/12 历史脱敏模型基线，JavaScript 与仓库卫生通过；最终登记时间和源码指纹以 `PROJECT_STATE.json` 为准。
- 首次公开 `main` 和 `v0.1.0` 工作流都在 `setup-python` 阶段失败：Windows 2025 runner 无精确 CPython 3.12.13，任何项目测试都尚未启动。这是可复现环境合同缺陷，不是业务测试失败；`.python-version` 已改为 `3.12`。
- 新根提交第一次公开推送后 Dependabot 发现 `aiohttp` 3.14.1 对应 1 个高危与 2 个中危公告，其中高危问题的首个修复版本为 3.14.3。直接依赖和 Windows/CPython 3.12 wheel 哈希升级到 3.14.3 后，公开依赖图已刷新，Dependabot 未修复警报为 0。
- 公开 `v0.2.0` 的 `project-gate` 与 `source-release` 均成功；GitHub Release 源码 ZIP 的公开 SHA-256 digest 为 `66b44d8e44a29bd601fa9fbe5b1ae065acf1ea532654a0232c23e0d321296cfd`。包内包含 README、原创图片和 3.14.3 锁定，不含 Git/虚拟环境/便携运行时/凭据/上传/缓存。

## 安全与来源边界

- bridge 使用随机 token、固定时间比较和同源/CORS 校验。
- SQLite 查询使用 `mode=ro`、`query_only`、单语句校验、authorizer 和 500 行物化上限。
- 远程数据库默认整库只读；可显式启用受控 DML，但远程表/字段/行范围、图表、DDL 和真实服务端写入矩阵继续关闭/待验证。
- API Key 只保存在 Git 忽略的 `runtime/app/model_profiles.json`；公开接口不回显。
- 审计保存受控元数据和哈希，不保存原始问题、SQL、凭据或结果行。
- 发布树不包含本机便携 Python、模型配置、运行数据库、上传、日志、benchmark 下载或原始模型输出。
- 只再分发固定 ECharts 文件和项目时区归档；许可证见 `THIRD_PARTY_NOTICES.md` 与 `third_party/licenses/`。
- `scripts/check_source_provenance.py --reference <path>` 只输出文件路径与指标，不复制参考内容。

## 已知限制与下一步

1. 保持 `main` 门禁、标签发布门禁、Dependabot 和私有漏洞报告开启；任何依赖或发布合同变更都重跑干净 Windows 安装。
2. 下一个发行工程优先级是签名 Windows 安装器、SBOM、完整依赖许可证报告和外部安全审计；完成前不对外宣称。
3. macOS、Linux、Windows on ARM、云代理/TLS、更多数据库版本和远程受控写入的真实服务端矩阵均未验证。
4. Spider/BIRD 结果是架构诊断证据，不是官方榜单成绩，也不能替代真实业务数据验证。
5. 当实体姓名只存在一张表、其论文/项目只通过数字主外键存在另一张表时，仍需要显式的关系实体扩展计划；当前检索器不会为提高召回率而猜测 JOIN。
6. 当前通用读路径仍以“意图→单个主执行器”为主，只有组合问题和若干精确失败形状会重规划。要达到“模型观察结果后反复探索”，还需将通用读路径升级为有步数/时间/行数/权限预算的只读观察—重规划循环；写路径不进入该循环。

## 运行注意事项

- 推荐新用户运行 `scripts\install_and_start.cmd`；开发和门禁可使用 `runtime/python/python.exe` 或 `.venv`。
- Python 后端和前端资源不热加载；代码变更后必须完整退出并重启桌面应用。
- 标准检查：`scripts\check_project.cmd`；验证并登记：
  `scripts\check_project.cmd --record --summary "说明"`。
