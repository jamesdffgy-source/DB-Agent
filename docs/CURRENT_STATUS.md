# 当前项目状态

最后更新：2026-08-26 16:11 +08:00

## 发布判断

DB-Agent 已达到源码优先的公开预发布候选水准：Windows x64 上可以从不含本机运行时、缓存、配置和数据的全新导出目录创建 `.venv`，按哈希锁文件安装 26 个依赖，并通过环境诊断与带鉴权的 bridge 启停探针。当前还没有签名原生安装器，因此不能描述成“下载即用”或跨平台产品。

远程仓库暂时保持私有。旧默认分支已经被无父提交替换，但 GitHub 仍能按旧完整 SHA 读取不可达对象；为满足来源清洁的绝对要求，必须在最终门禁后删除旧仓库、同名新建，再只推送最终根提交。仓库没有 star、fork、issue 或 watcher 迁移负担。

## 当前架构

```text
pywebview + WebView2
  └─ Vanilla HTML/CSS/JavaScript + ECharts
       └─ loopback aiohttp desktop_bridge
            ├─ token、CORS、角色与数据库/表/字段/行范围
            ├─ 会话、上传、数据源、图表、调度与审计 API
            ├─ ModelProfileStore → model_profiles.json（本机、Git 忽略）
            └─ DBAgent
                 ├─ 基础沟通、意图路由和结构化操作计划
                 ├─ Schema / Semantic Catalog / 关系与歧义门禁
                 ├─ 类型化关系计划与 NL-to-SQL
                 ├─ 检索与只读 OperationGraph
                 └─ 写校验 → 回滚预览 → 一次性确认 → 事务执行

模型请求：DBAgent → model_gateway → OpenAI-compatible text API
```

`model_gateway.py` 只负责文本请求、响应解析、重试、总截止时间、协作取消和用量计数；它不提供工具注册、自治循环、通用会话或多角色编排。模型配置和上传持久化分别由 `model_profiles.py` 与 `upload_storage.py` 管理。

## 已实现能力

- SQLite 完整本地主链：Schema、检索、组合分析、图表、语义定义、调度和受控写入。
- CSV/`.xlsx` 导入成本地 SQLite；旧版 `.xls` 在解码前拒绝。
- MySQL 8.4.9 与 PostgreSQL 17.11 整库只读链路：Schema、PK/FK、行数/抽样、受限查询、分组多指标、物理拒写、超时和连接失败。
- 模型优先识别查询、检索、组合与写入意图；所有输出仍经本地 schema、范围、单语句、关系、语义与执行门禁。
- 表感知单行录入：选择目标表后展示真实字段、类型和一行只读示例；提交只生成绑定当前数据库和权限的预览确认单。
- 受控新建表表单：字段类型和约束白名单、确定性 DDL、管理员预览和显式确认。
- viewer/operator/admin 最小权限；本地个人凭据可限制数据库，并对 SQLite 限制表、字段和结构化行条件。
- 会话模型文本与 UI 富快照分离；表格默认 10 行，可展开当前回答已返回或已保存的全部行。
- 图表在线程工作器构建并按访问范围缓存；查询默认物理只读，写入确认只能使用一次。

## 发布体验

- 中英文 README 已改成价值说明、原创主视觉、真实截图、一条命令 Quick Start、支持矩阵、架构、安全和贡献入口。
- `scripts/install_and_start.cmd` 完成创建环境、诊断和启动；`doctor.cmd` 检查 Python、依赖、项目文件、运行目录与 WebView2。
- `scripts/smoke_startup.py` 在随机 loopback 端口启动 bridge，验证带 token 的 `/status` 与桌面页面后关闭进程。
- GitHub Actions 在全新 Windows runner 中执行 bootstrap、诊断、完整门禁和启动探针；标签工作流重新验证后生成带 SHA-256 的源码包并创建 release。
- 已补充安装文档、支持入口、原创行为规范、Issue/PR 模板、依赖更新配置和变更记录。
- 原创手绘流程图不含第三方品牌、人物、机器人或可读文字；真实截图继续作为产品证据。

## 当前验证证据

- 上一轮正式产品门禁：安全与功能回归 413/413，固定离线评测 134/134，时区探针 8/8，历史脱敏模型基线 12/12。
- 本轮发布工程定向验证：关键新脚本编译通过；当前环境 doctor 通过；带鉴权启动探针通过。
- D 盘全新导出验证：无 `runtime/python`、`.venv`、缓存、凭据或数据库，使用 CPython 3.12.10 从零安装锁定依赖，`pip check`、doctor 和启动探针全部通过。
- 当前仓库卫生：103 个候选、96 个文本、7 个直接依赖、26 个锁定依赖、凭据发现 0、合法再分发资产 2 类。
- 当前来源比较：103 个候选对指定参考树 917 个有效文件，SHA-256 精确命中 0，20-token 高重合命中 0；禁止标记命中 0。
- 本轮发布工程正式门禁已通过：102 个受控文件、413/413 回归、134/134 离线评测、8/8 时区探针、
  12/12 历史模型基线，JavaScript 与仓库卫生通过。首次登记时间为 2026-08-26T16:15:22+08:00；
  本条结果回填后还需执行一次最终登记，最终指纹以 `PROJECT_STATE.json` 为准。

## 安全与来源边界

- bridge 使用随机 token、固定时间比较和同源/CORS 校验。
- SQLite 查询使用 `mode=ro`、`query_only`、单语句校验、authorizer 和 500 行物化上限。
- 远程数据库仅开放整库只读；远程表/字段/行范围、图表和写入继续关闭。
- API Key 只保存在 Git 忽略的 `runtime/app/model_profiles.json`；公开接口不回显。
- 审计保存受控元数据和哈希，不保存原始问题、SQL、凭据或结果行。
- 发布树不包含本机便携 Python、模型配置、运行数据库、上传、日志、benchmark 下载或原始模型输出。
- 只再分发固定 ECharts 文件和项目时区归档；许可证见 `THIRD_PARTY_NOTICES.md` 与 `third_party/licenses/`。
- `scripts/check_source_provenance.py --reference <path>` 只输出文件路径与指标，不复制参考内容。

## 已知限制与下一步

1. 回填正式门禁结果后执行最终登记和来源复扫，生成最终无父提交。
2. 删除旧私有远程仓库、同名新建公开仓库，只推送最终无父提交；匿名验证旧 SHA 404、README/图片可访问、社区健康文件被识别。
3. 创建 `v0.1.0` 标签，让 release 工作流验证并发布源码包及 SHA-256；CI 通过前不对外宣传。
4. 目前没有签名安装器、SBOM、完整依赖许可证报告或外部安全审计；发行说明必须继续明确。
5. macOS、Linux、Windows on ARM、云代理/TLS、更多数据库版本和远程受控写入均未验证。
6. Spider/BIRD 结果是架构诊断证据，不是官方榜单成绩，也不能替代真实业务数据验证。

## 运行注意事项

- 推荐新用户运行 `scripts\install_and_start.cmd`；开发和门禁可使用 `runtime/python/python.exe` 或 `.venv`。
- Python 后端和前端资源不热加载；代码变更后必须完整退出并重启桌面应用。
- 标准检查：`scripts\check_project.cmd`；验证并登记：
  `scripts\check_project.cmd --record --summary "说明"`。
