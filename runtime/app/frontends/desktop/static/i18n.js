(function () {
  'use strict';

  const STORAGE_KEY = 'dbquill_locale';
  const ENGLISH = Object.freeze({
    '会话': 'Conversations',
    '图表': 'Charts',
    '定时操作': 'Schedules',
    '语义层': 'Semantics',
    '审计记录': 'Audit',
    '设置': 'Settings',
    '新对话': 'New chat',
    '打开设置': 'Open settings',
    '尚未配置模型 API Key——基础沟通和结构查看仍可使用；复杂数据问答需先在设置中填写模型与密钥。': 'No model API key is configured. Basic conversation and schema inspection still work; configure a model and key in Settings for complex data questions.',
    '可以先打个招呼，也可以直接用自然语言查询、分析或操作数据库。': 'Start with a greeting, or query, analyze, and operate your database in natural language.',
    '未选择数据库': 'No database selected',
    '默认模型': 'Default model',
    '＋ 新建表': '+ New table',
    '输入问题，或先打个招呼…': 'Ask a question, or start with a greeting…',
    '发送': 'Send',
    'Enter 发送 · Shift+Enter 换行 · 可上传任意 SQLite / CSV 文件对话（CSV 自动转库）': 'Enter to send · Shift+Enter for a new line · Upload SQLite, CSV, or XLSX data',
    '数据库': 'Database',
    '手动刷新': 'Refresh',
    '等待选择数据库': 'Waiting for a database',
    '每张表保留一张图表；数据库变化或手动刷新时才重新生成': 'One chart per table; charts regenerate only after data changes or a manual refresh.',
    '选择数据库，自动生成图表': 'Choose a database to generate charts',
    '系统自动分析各表，生成柱状 / 折线统计图表': 'DBQuill analyzes each table and builds useful bar or line charts.',
    '定时任务': 'Scheduled tasks',
    '刷新': 'Refresh',
    '暂无定时任务': 'No scheduled tasks',
    '新建定时任务': 'New scheduled task',
    '任务名称': 'Task name',
    '如：每周一统计待处理订单': 'Example: Count pending orders every Monday',
    '操作类型': 'Operation type',
    '只读 SQL': 'Read-only SQL',
    '自然语言查询': 'Natural-language query',
    '内容': 'Content',
    'SQL：SELECT / WITH 查询；自然语言：如「统计最近 30 天的订单数」': 'SQL: a SELECT or WITH query; natural language: e.g. “Count orders from the last 30 days”',
    '调度模式': 'Schedule mode',
    '一次性': 'Once',
    '间隔（分钟）': 'Interval (minutes)',
    '每天': 'Daily',
    '每周': 'Weekly',
    '执行时间（YYYY-MM-DD HH:MM）': 'Run at (YYYY-MM-DD HH:MM)',
    '定时任务仅自动执行物理只读查询。若自然语言规划出写操作，系统只记录“需要确认”，必须回到对话查看变更预览并显式批准。': 'Scheduled tasks only execute physically read-only queries. If a natural-language request plans a write, it stops for an explicit preview and approval in Conversations.',
    '取消编辑': 'Cancel editing',
    '保存任务': 'Save task',
    '执行日志': 'Run log',
    '刷新日志': 'Refresh log',
    '暂无执行日志': 'No run history',
    '语义定义': 'Semantic definitions',
    '导出配置': 'Export',
    '导入配置': 'Import',
    '选择数据库后添加业务术语': 'Choose a database to add business terms',
    '新增语义定义': 'New semantic definition',
    '定义类型': 'Definition type',
    '表别名': 'Table alias',
    '字段别名': 'Column alias',
    '业务维度': 'Business dimension',
    '时间字段': 'Time field',
    '业务日历（受控）': 'Business calendar (controlled)',
    '枚举值': 'Enum value',
    '指标': 'Metric',
    '比率指标（受控）': 'Ratio metric (controlled)',
    '业务术语': 'Business term',
    '如：商品、成交额、已付款': 'Example: product, revenue, paid',
    '目标表': 'Target table',
    '目标字段': 'Target column',
    '维度层级名称（可选）': 'Hierarchy name (optional)',
    '如：地域层级': 'Example: Regional hierarchy',
    '层级顺序': 'Hierarchy level',
    '维度固定过滤': 'Fixed dimension filters',
    '＋ 添加条件': '+ Add condition',
    '随该维度的分组或下钻生效；最多 4 条，仅按 AND 组合。': 'Applied when grouping or drilling into this dimension; up to four AND conditions.',
    '趋势默认粒度': 'Default trend grain',
    '未设置（趋势请求需要明确）': 'Not set (trend requests must specify)',
    '按日': 'Daily',
    '按周': 'Weekly',
    '按月': 'Monthly',
    '按季度': 'Quarterly',
    '按年': 'Yearly',
    '对应值': 'Mapped value',
    '如：paid、1、true': 'Example: paid, 1, true',
    '聚合方式': 'Aggregation',
    '记录数 COUNT(*)': 'Row count COUNT(*)',
    '去重计数 COUNT DISTINCT': 'Distinct count COUNT DISTINCT',
    '求和 SUM': 'Sum SUM',
    '平均值 AVG': 'Average AVG',
    '最小值 MIN': 'Minimum MIN',
    '最大值 MAX': 'Maximum MAX',
    '结构化日历口径': 'Structured calendar rules',
    '绑定上方时间字段；ISO 星期一为 1，星期日为 7': 'Uses the time field above; ISO Monday is 1 and Sunday is 7.',
    '财年起始月': 'Fiscal year start month',
    '财年起始日': 'Fiscal year start day',
    '财年年份标注': 'Fiscal year label',
    '按起始年': 'Start year',
    '按结束年': 'End year',
    '每周起始': 'Week starts on',
    '星期一': 'Monday',
    '星期六': 'Saturday',
    '星期日': 'Sunday',
    '时间值存储基准': 'Timestamp storage basis',
    '未声明（仅模型链路）': 'Unspecified (model path only)',
    '日期 DATE（无需换日）': 'DATE (no timezone conversion)',
    '本地时间戳（不换时区）': 'Local timestamp (no timezone conversion)',
    'UTC 时间戳（需选择换日）': 'UTC timestamp (conversion required)',
    'UTC 换日方式': 'UTC conversion method',
    '固定分钟偏移': 'Fixed minute offset',
    'IANA 动态规则（含 DST）': 'IANA rules (including DST)',
    '业务 UTC 偏移（分钟）': 'Business UTC offset (minutes)',
    '时区': 'Time zone',
    '周末 / 固定非工作日': 'Weekend / fixed non-working days',
    '一': 'Mon',
    '二': 'Tue',
    '三': 'Wed',
    '四': 'Thu',
    '五': 'Fri',
    '六': 'Sat',
    '日': 'Sun',
    '节假日例外表（可选）': 'Holiday exception table (optional)',
    '日期字段': 'Date column',
    '名称字段（可选）': 'Name column (optional)',
    '工作日覆盖字段（可选）': 'Working-day override column (optional)',
    '本地时间戳必须已经是业务墙上时间且不带时区后缀；UTC 时间戳可选择固定偏移，或使用项目归档的 IANA 规则动态处理夏令时。不绑定例外表时只使用周末规则。': 'Local timestamps must already represent business wall time without a timezone suffix. UTC timestamps may use a fixed offset or the bundled IANA rules. Without an exception table, only weekend rules apply.',
    '时区发布状态加载中。': 'Loading timezone release status.',
    '指标过滤条件': 'Metric filters',
    '最多 4 条，仅按 AND 组合；字段、操作符和值分别校验。': 'Up to four AND conditions; columns, operators, and values are validated independently.',
    '受控比率公式': 'Controlled ratio formula',
    '分子 ÷ 分母；分母为 0 时返回 NULL': 'Numerator ÷ denominator; returns NULL when the denominator is zero.',
    '分子聚合': 'Numerator aggregation',
    '分子字段': 'Numerator column',
    '分母聚合': 'Denominator aggregation',
    '分母字段': 'Denominator column',
    '分子过滤': 'Numerator filters',
    '分母过滤': 'Denominator filters',
    '比值（× 1）': 'Ratio (× 1)',
    '百分比（× 100）': 'Percentage (× 100)',
    '说明（可选）': 'Description (optional)',
    '给团队看的口径说明': 'Definition notes for your team',
    '保存定义': 'Save definition',
    '维度、时间和业务日历必须绑定真实字段；维度/指标过滤与日历例外表均经 schema 校验，比率指标只支持单表分子除以分母，不接受 SQL 表达式。': 'Dimensions, time fields, and calendars must bind to real columns. Filters and calendar exceptions are schema-validated; ratio metrics only support a single-table numerator divided by a denominator, never raw SQL expressions.',
    '操作事件': 'Event',
    '全部': 'All',
    '自然语言操作': 'Natural-language operation',
    '写确认': 'Write confirmation',
    '写执行': 'Write execution',
    '调度变更': 'Schedule change',
    '调度执行': 'Schedule run',
    '语义变更': 'Semantic change',
    '审计处置': 'Audit resolution',
    '结果': 'Result',
    '成功': 'Success',
    '失败': 'Failed',
    '已拒绝': 'Rejected',
    '已取消': 'Cancelled',
    '已批准': 'Approved',
    '待处理': 'Pending',
    '刷新记录': 'Refresh records',
    '导出记录': 'Export records',
    '创建备份': 'Create backup',
    '等待校验': 'Waiting for verification',
    '本地个人凭据': 'Local personal credentials',
    '权限控制': 'Access control',
    '角色': 'Role',
    '查看者': 'Viewer',
    '操作员': 'Operator',
    '管理员': 'Administrator',
    '有效期': 'Lifetime',
    '24 小时': '24 hours',
    '7 天': '7 days',
    '30 天': '30 days',
    '90 天': '90 days',
    '1 年': '1 year',
    '数据库范围': 'Database scope',
    '仅勾选数据库': 'Selected databases only',
    '全部数据库': 'All databases',
    '表范围': 'Table scope',
    '所选库全部表': 'All tables in selected databases',
    '限定表': 'Selected tables',
    '字段范围': 'Column scope',
    '所选表全部字段': 'All columns in selected tables',
    '限定字段': 'Selected columns',
    '行范围': 'Row scope',
    '所选表全部行': 'All rows in selected tables',
    '限定行': 'Filtered rows',
    '先限定一张本地表': 'Select one local table first',
    '条件字段': 'Filter column',
    '条件': 'Condition',
    '等于': 'Equals',
    '不等于': 'Does not equal',
    '大于': 'Greater than',
    '大于等于': 'Greater than or equal',
    '小于': 'Less than',
    '小于等于': 'Less than or equal',
    '为空': 'Is null',
    '不为空': 'Is not null',
    '比较值': 'Value',
    '桌面端每张凭据配置一个结构化条件；行级凭据固定为只读。': 'Each desktop credential supports one structured condition per table. Row-scoped credentials are always read-only.',
    '发行凭据': 'Issue credential',
    '按数据库授权；token 只显示一次': 'Database-scoped access; the token is shown once.',
    '默认授权所选数据库内全部表': 'All tables in selected databases are allowed by default.',
    '默认授权所选表内全部字段': 'All columns in selected tables are allowed by default.',
    '请立即保存': 'Save this now',
    '关闭或刷新后无法再次查看此 token': 'This token cannot be shown again after closing or refreshing.',
    '复制 token': 'Copy token',
    '展开后加载凭据状态': 'Expand to load credential status',
    '未决处置': 'Pending resolutions',
    '追加管理员判断，不改写历史事件': 'Append an administrator decision without rewriting history.',
    '暂无未决事件': 'No pending events',
    '选择数据库后查看操作记录': 'Choose a database to view audit records',
    '连接远程数据库': 'Connect a remote database',
    '类型': 'Type',
    '名称（可选，默认用库名）': 'Name (optional; database name by default)',
    '如 销售库': 'Example: Sales database',
    '主机': 'Host',
    '端口': 'Port',
    '用户名': 'Username',
    '数据库账号': 'Database user',
    '密码': 'Password',
    '数据库 / Schema': 'Database / schema',
    '要查询的库名': 'Database name',
    '访问模式': 'Access mode',
    '只读（推荐）': 'Read-only (recommended)',
    '受控读写': 'Controlled writes',
    '受控读写仅开放 INSERT、UPDATE、DELETE；所有变更先回滚预览，再由操作员或管理员确认。数据库账号还必须具备相应权限。': 'Controlled writes allow INSERT, UPDATE, and DELETE only. Every change is executed in a rollback preview before operator or administrator approval; the database account must also have the required permissions.',
    '取消': 'Cancel',
    '连接并接入': 'Connect',
    '新增一条记录': 'Insert one row',
    '当前仅编辑草稿，不会直接写入数据库': 'You are editing a draft; nothing is written yet.',
    '请先选择一张表': 'Choose a table first',
    '＋ 新建表': '+ New table',
    '安全流程': 'Safety flow',
    '字段校验 → 回滚预览 → 显式确认 → 执行前复检': 'Validate fields → rollback preview → explicit confirmation → final verification',
    '选择目标表后，这里会显示字段、类型和一行原表示例。': 'Choose a target table to see its columns, types, and one example row.',
    '生成变更预览': 'Generate change preview',
    '自定义新建表': 'Create a custom table',
    '类型白名单与字段约束由本地程序编译，模型不生成 SQL': 'The local program compiles allowed types and constraints; the model does not generate this SQL.',
    '高风险结构变更': 'High-risk schema change',
    '字段校验 → DDL 回滚预览 → 管理员显式确认 → 执行前复检': 'Validate fields → DDL rollback preview → administrator confirmation → final verification',
    '表名': 'Table name',
    '如：customer_orders': 'Example: customer_orders',
    '请定义表名和字段': 'Define a table name and its columns',
    '字段名': 'Column name',
    '约束': 'Constraints',
    '默认值': 'Default value',
    '＋ 添加字段': '+ Add column',
    '最多 64 个字段；可选主键、自增、必填、唯一和受控默认值': 'Up to 64 columns with optional primary key, auto-increment, required, unique, and controlled defaults.',
    '生成 DDL 预览': 'Generate DDL preview',
    '导入语义配置': 'Import semantic configuration',
    '预检不会修改当前目录': 'Preflight does not modify the current catalog.',
    '应用采用合并模式：同名术语覆盖，不同名新增，完全一致的定义跳过。预检后目录若发生变化，需要重新预检。': 'Import merges the catalog: matching terms are replaced, new terms are added, and identical definitions are skipped. If the catalog changes after preflight, run preflight again.',
    '确认应用': 'Apply import',
    '新对话 · 选择数据库': 'New chat · Choose a database',
    '上传': 'Upload',
    '上传数据': 'Upload data',
    '文件过大（>200MB）': 'File is too large (>200 MB)',
    '本地上传连接中断': 'The local upload connection was interrupted',
    '上传已取消': 'Upload canceled',
    '文件已上传（未接入数据库）': 'File uploaded, but no database was attached',
    '连接库': 'Connect database',
    '模型设置': 'Model settings',
    'API Key 只保存在本机 model_profiles.json，不写入源码、文档或审计记录': 'API keys are stored only in the local model_profiles.json file and never written to source, documentation, or audit records.',
    '新增配置': 'New profile',
    '名称': 'Name',
    '如 DeepSeek V4 Flash': 'Example: DeepSeek V4 Flash',
    '模型名': 'Model name',
    '如 deepseek-v4-flash': 'Example: deepseek-v4-flash',
    'API Base（OpenAI 兼容）': 'API base (OpenAI-compatible)',
    '高级（可选）': 'Advanced (optional)',
    '重试次数': 'Retries',
    '默认 5': 'Default: 5',
    '连接超时(秒)': 'Connection timeout (seconds)',
    '默认 15': 'Default: 15',
    '读取超时(秒)': 'Read timeout (seconds)',
    '默认 300': 'Default: 300',
    '流式': 'Streaming',
    '默认（流式）': 'Default (streaming)',
    '非流式': 'Non-streaming',
    '测试连接': 'Test connection',
    '删除': 'Delete',
    '设为默认': 'Set as default',
    '保存': 'Save',
    '关闭': 'Close',
    '当前数据库；新对话状态可点击更换': 'Current database; click to change it before the conversation starts.',
    '当前模型配置（在设置中修改）': 'Current model profile; change it in Settings.',
    '为当前本地 SQLite 数据库自定义创建表': 'Create a custom table in the current local SQLite database.',
    '新建对话并选择数据库': 'Start a new conversation and choose a database.',
    '选择要写入的表': 'Choose the table to write to.',
    '配置模型与 API Key（保存在本机本地配置，不进入源码或审计）': 'Configure a model and API key. They remain in local configuration and never enter source or audit records.',
    'bridge 连接状态': 'Local bridge connection status',
    '凭据授权数据库': 'Credential database access',
    '凭据授权表': 'Credential table access',
    '凭据授权字段': 'Credential column access',
    '凭据授权行': 'Credential row access',
    '一次性本地凭据 token': 'One-time local credential token',
    '给团队看的口径说明': 'Definition notes for your team',
    '如 数据分析员甲': 'Example: Data analyst A',
    '如 tenant-a': 'Example: tenant-a',
    '如：Asia/Shanghai': 'Example: Asia/Shanghai',
    '暂无会话': 'No conversations yet',
    '点击左下角“新对话”开始': 'Use “New chat” below to begin.',
    '最近对话': 'Recent',
    '历史对话': 'History',
    '重命名': 'Rename',
    '重命名会话': 'Rename conversation',
    '删除该会话记录？': 'Delete this conversation?',
    '删除该模型配置？（已保存的 API Key 一并删除）': 'Delete this model profile and its saved API key?',
    '这会追加管理员处置记录，不会修改原事件，也不能证明数据库原子终态。确认继续？': 'This appends an administrator resolution without changing the original event or claiming an atomic database outcome. Continue?',
    '该凭据将可访问当前及后续接入的全部数据库，确认继续？': 'This credential will access every current and future database. Continue?',
    '管理员凭据可执行高风险写入与安全管理，确认发行？': 'Administrator credentials can approve high-risk writes and manage security. Issue this credential?',
    '吊销后该 token 会立即失效且不能恢复，确认继续？': 'Revoking this token takes effect immediately and cannot be undone. Continue?',
    '删除这条语义定义？': 'Delete this semantic definition?',
    '确认删除该定时任务？': 'Delete this scheduled task?',
    '右键可重命名 / 删除': 'Right-click to rename or delete',
    '（无标题）': '(Untitled)',
    '你能做什么？': 'What can you do?',
    '一共多少条记录？': 'How many rows are there?',
    '有哪些表？每张表多少行？': 'What tables are available, and how many rows are in each?',
    '请先选择数据库': 'Choose a database first',
    '暂无模型配置': 'No model profiles',
    '当前数据库没有未决操作': 'No pending operations for this database',
    '尚未选择数据库': 'No database selected',
    '选择数据库后查看未决操作': 'Choose a database to view pending operations',
    '当前没有可授权数据库': 'No databases are available for authorization',
    '默认授权所选数据库内全部表': 'All tables in selected databases are allowed by default.',
    '当前数据库没有可授权业务表': 'No business tables are available for authorization',
    '请先选择数据库': 'Choose a database first',
    '未命名任务': 'Untitled task',
    '编辑任务': 'Edit task',
    '启用': 'Enabled',
    '停用': 'Disabled',
    '立即执行': 'Run now',
    '编辑': 'Edit',
    '未执行过': 'Never run',
    '日志加载失败': 'Failed to load logs',
    '脱敏运行元数据；完整事件请查看审计记录': 'Redacted execution metadata; see Audit for the complete event.'
  });

  const DYNAMIC_ENGLISH = Object.freeze([
    [/^介绍一下表\s+(.+)$/u, (_, table) => `Describe table ${table}`],
    [/^当前数据库：(.+?)（(.+?)）$/u, (_, name, kind) => `Current database: ${name} (${kind})`],
    [/^(\d+)\s*条$/u, (_, count) => `${count} items`],
    [/^(\d+)\s*张表$/u, (_, count) => `${count} tables`],
    [/^已开始新对话：(.+)$/u, (_, name) => `Started a new conversation: ${name}`],
    [/^已接入：(.+?)（(\d+) 张表）$/u, (_, name, count) => `Connected: ${name} (${count} tables)`],
    [/^正在上传：(.+?)（(\d+)%）$/u, (_, name, percent) => `Uploading ${name} (${percent}%)`],
    [/^正在上传：(.+)$/u, (_, name) => `Uploading ${name}`],
    [/^正在检查数据库：(.+)$/u, (_, name) => `Inspecting database ${name}`],
    [/^已接入数据库：(.+?)（(\d+) 张表）$/u, (_, name, count) => `Database attached: ${name} (${count} tables)`],
    [/^连接成功（但模型列表中未找到该模型名）$/u, () => 'Connected, but the model name was not found in the model list.'],
    [/^连接成功，模型存在$/u, () => 'Connected; the model is available.'],
    [/^连接成功$/u, () => 'Connected successfully.'],
    [/^操作失败：(.+)$/u, (_, detail) => `Operation failed: ${detail}`],
    [/^初始化失败：(.+)$/u, (_, detail) => `Initialization failed: ${detail}`],
    [/^(.+?)失败：(.+)$/u, (_, action, detail) => `${ENGLISH[action] || action} failed: ${detail}`],
    [/^执行完成：(.+)$/u, (_, detail) => `Run completed: ${detail}`],
    [/^库：(.+)$/u, (_, detail) => `Database: ${detail}`],
    [/^上次：(.+)$/u, (_, detail) => `Last run: ${detail}`]
  ]);

  const textSources = new WeakMap();
  const attributeSources = new WeakMap();
  const TRANSLATED_ATTRIBUTES = ['title', 'placeholder', 'aria-label'];
  let locale = readInitialLocale();
  let observer = null;
  let applying = false;

  function readInitialLocale() {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === 'en' || saved === 'zh-CN') return saved;
    return 'en';
  }

  function translateCore(source) {
    if (Object.prototype.hasOwnProperty.call(ENGLISH, source)) return ENGLISH[source];
    for (const [pattern, render] of DYNAMIC_ENGLISH) {
      const match = source.match(pattern);
      if (match) return render(...match);
    }
    return source;
  }

  function translateValue(value, requestedLocale = locale) {
    const source = String(value == null ? '' : value);
    if (requestedLocale !== 'en' || !source) return source;
    const match = source.match(/^(\s*)([\s\S]*?)(\s*)$/u);
    if (!match) return source;
    return match[1] + translateCore(match[2]) + match[3];
  }

  function shouldSkip(element) {
    return !element || Boolean(element.closest('script, style, code, pre, [data-i18n-control]'));
  }

  function rememberAttributes(element) {
    if (shouldSkip(element)) return;
    let sources = attributeSources.get(element);
    if (!sources) {
      sources = {};
      attributeSources.set(element, sources);
    }
    for (const name of TRANSLATED_ATTRIBUTES) {
      if (element.hasAttribute(name)) sources[name] = element.getAttribute(name);
    }
  }

  function applyAttributes(element) {
    if (shouldSkip(element)) return;
    let sources = attributeSources.get(element);
    if (!sources) {
      rememberAttributes(element);
      sources = attributeSources.get(element) || {};
    }
    for (const name of TRANSLATED_ATTRIBUTES) {
      if (!Object.prototype.hasOwnProperty.call(sources, name)) continue;
      const expected = translateValue(sources[name]);
      const current = element.getAttribute(name);
      if (locale === 'en' && current !== expected && current != null) {
        sources[name] = current;
      }
      const target = locale === 'en' ? translateValue(sources[name]) : sources[name];
      if (element.getAttribute(name) !== target) element.setAttribute(name, target);
    }
  }

  function applyTextNode(node) {
    if (!node || shouldSkip(node.parentElement)) return;
    if (!textSources.has(node)) textSources.set(node, node.nodeValue || '');
    let source = textSources.get(node);
    const expected = translateValue(source);
    const current = node.nodeValue || '';
    if (locale === 'en' && current !== expected) {
      source = current;
      textSources.set(node, source);
    }
    const target = locale === 'en' ? translateValue(source) : source;
    if (node.nodeValue !== target) node.nodeValue = target;
  }

  function walk(root, visitor) {
    if (!root) return;
    if (root.nodeType === Node.TEXT_NODE) {
      visitor(root);
      return;
    }
    if (root.nodeType !== Node.ELEMENT_NODE && root.nodeType !== Node.DOCUMENT_FRAGMENT_NODE) return;
    if (root.nodeType === Node.ELEMENT_NODE) applyAttributes(root);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();
    while (node) {
      if (node.nodeType === Node.TEXT_NODE) visitor(node);
      else applyAttributes(node);
      node = walker.nextNode();
    }
  }

  function applyTree(root) {
    walk(root, applyTextNode);
  }

  function captureTree(root) {
    walk(root, node => {
      if (!shouldSkip(node.parentElement)) textSources.set(node, node.nodeValue || '');
    });
    if (root && root.querySelectorAll) {
      [root, ...root.querySelectorAll('*')].forEach(element => {
        if (element.nodeType === Node.ELEMENT_NODE) rememberAttributes(element);
      });
    }
  }

  function updateControls() {
    const control = document.querySelector('.locale-switch');
    if (!control) return;
    control.setAttribute('aria-label', locale === 'en' ? 'Language' : '语言');
    control.querySelectorAll('[data-locale]').forEach(button => {
      const active = button.dataset.locale === locale;
      button.classList.toggle('active', active);
      button.setAttribute('aria-pressed', String(active));
      button.title = button.dataset.locale === 'en'
        ? (locale === 'en' ? 'English selected' : 'Switch to English')
        : (locale === 'en' ? '切换到中文' : '已选择中文');
    });
  }

  function setLocale(nextLocale, persist = true) {
    const next = nextLocale === 'en' ? 'en' : 'zh-CN';
    if (locale === 'zh-CN' && document.body) captureTree(document.body);
    locale = next;
    if (persist) localStorage.setItem(STORAGE_KEY, locale);
    document.documentElement.lang = locale;
    document.title = locale === 'en'
      ? 'DBQuill · Open-Source AI Database Agent'
      : 'DBQuill · 开源 AI 数据库智能体';
    applying = true;
    try {
      applyTree(document.body);
      updateControls();
    } finally {
      applying = false;
    }
    window.dispatchEvent(new CustomEvent('dbquill:localechange', { detail: { locale } }));
  }

  function observe() {
    if (observer || !document.body) return;
    observer = new MutationObserver(mutations => {
      if (applying) return;
      applying = true;
      try {
        for (const mutation of mutations) {
          if (locale === 'zh-CN') {
            if (mutation.type === 'characterData') textSources.set(mutation.target, mutation.target.nodeValue || '');
            else if (mutation.type === 'attributes') rememberAttributes(mutation.target);
            else mutation.addedNodes.forEach(node => captureTree(node));
            continue;
          }
          if (mutation.type === 'characterData') applyTextNode(mutation.target);
          else if (mutation.type === 'attributes') applyAttributes(mutation.target);
          else mutation.addedNodes.forEach(node => applyTree(node));
        }
      } finally {
        applying = false;
      }
    });
    observer.observe(document.body, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: TRANSLATED_ATTRIBUTES
    });
  }

  function start() {
    document.querySelectorAll('[data-locale]').forEach(button => {
      button.addEventListener('click', () => setLocale(button.dataset.locale));
    });
    setLocale(locale, false);
    observe();
  }

  window.DBQuillI18n = Object.freeze({
    start,
    setLocale,
    t: translateValue,
    get locale() { return locale; }
  });
}());
