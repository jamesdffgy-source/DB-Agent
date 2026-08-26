# NL-to-Database 固定评测报告

生成时间：2026-08-17T10:49:21+08:00

评测集版本：`2026.08.17-v23`

> 口径说明：离线总通过率不是模型 NL2SQL 准确率。规划用例使用人工标注意图；参考 SQL 用例使用人工 SQL。只有显式运行的模型通道才反映当前模型在这组固定题上的执行准确率。

## 结果摘要

| 指标 | 结果 | 含义 |
|---|---:|---|
| 确定性操作规划精确匹配 | 46/46（100.0%） | 动作、目标、风险、确认、语义和澄清字段全匹配 |
| 确定性并列指标聚合 | 4/4（100.0%） | 同表受控指标、指标独立过滤、全局过滤和回退边界匹配 |
| 确定性业务日历编译 | 6/6（100.0%） | DATE/声明型时间戳的财年、财季、工作日边界、结果和保守回退匹配 |
| 确定性业务维度聚合 | 8/8（100.0%） | 单表分组、同表下钻、受控指标和回退边界匹配 |
| 确定性时间趋势聚合 | 17/17（100.0%） | 单表日/周/月/季度/年分桶、时间戳口径、受控指标和回退边界匹配 |
| 动态操作图与契约匹配 | 12/12（100.0%） | 节点选择、依赖、跨表预检和输出契约全匹配 |
| 大 Schema 关系事实匹配 | 5/5（100.0%） | 64 表合成 schema 的多跳关系、隔离和索引事实匹配 |
| 参考 SQL 执行结果匹配 | 6/6（100.0%） | 参考 SQL 通过只读安全层后，列与行结果匹配 |
| 只读危险语句拦截召回 | 9/9（100.0%） | 固定危险查询被拒绝 |
| 只读有效语句接受率 | 6/6（100.0%） | 固定有效只读语句被接受 |
| 写路径无效语句拦截召回 | 10/10（100.0%） | 无界写、多语句和越权类型被拒绝 |
| 写路径策略有效语句接受率 | 5/5（100.0%） | 仍需预览和确认，不代表已落库 |
| 离线固定用例合计 | 134/134（100.0%） | 仅用于版本回归，不与模型指标合并 |
| 真实模型 NL2SQL 执行准确率 | 100.0% | 状态：completed；固定模型题不并入离线合计 |

## 评测范围

- `planner`：人工标注 oracle_intent 下的确定性操作规划精确匹配；不评估模型意图分类。
- `multi_metric_compiler`：SQLite 同表 2–6 个受控普通指标的一次性聚合、指标过滤隔离、全局枚举过滤和保守回退。
- `calendar_compiler`：SQLite 声明型 DATE/时间戳字段上的财年、财季、工作日编译、执行结果与保守回退。
- `dimension_compiler`：SQLite 单表业务维度分组、同表层级下钻、受控指标和复杂形状回退。
- `trend_compiler`：SQLite 单表 DATE/显式时间戳上的日、周、月、季度、年聚合、受控指标和保守回退。
- `operation_graph`：确定性动态节点、跨表关系预检和节点输出契约精确匹配。
- `large_schema`：64 表合成 schema 的多跳关系、隔离表、非法表和紧凑索引事实匹配。
- `reference_sql`：人工参考 SQL 的安全执行与结果匹配；不评估自然语言生成 SQL。
- `security`：读写校验器对固定策略有效/无效语句的接受与拦截。
- `model_nl2sql`：可选真实模型 SQL 生成执行准确率；与离线指标隔离。

## 离线用例

### 确定性规划

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `plan-schema-overview` | 通过 | 有哪些表 |
| `plan-table-schema` | 通过 | orders 表有哪些字段 |
| `plan-query-target` | 通过 | orders 一共多少条记录 |
| `plan-query-ambiguous-table` | 通过 | 一共多少条记录 |
| `plan-semantic-metric` | 通过 | 成交额是多少 |
| `plan-controlled-filtered-metric` | 通过 | 已付款成交额是多少 |
| `plan-semantic-enum-query` | 通过 | 查询已付款订单 |
| `plan-retrieve-target` | 通过 | 介绍客户 Alice 的记录 |
| `plan-compose-multiple-targets` | 通过 | 结合订单统计和客户记录分析区域表现 |
| `plan-update-missing-table` | 通过 | 把 status 改为 paid |
| `plan-update-missing-filter` | 通过 | 把 orders 的 status 改为 paid |
| `plan-update-complete` | 通过 | 把 orders 中 id=2 的 status 改为 paid |
| `plan-semantic-update-missing-filter` | 通过 | 把订单的订单金额改为 100 |
| `plan-delete-missing-filter` | 通过 | 删除 orders 中的记录 |
| `plan-insert-missing-values` | 通过 | 新增一条 orders 记录 |
| `plan-insert-complete` | 通过 | 新增一条 orders 记录，customer_id=2，amount=88，status=paid |
| `plan-create-missing-definition` | 通过 | 创建 orders_archive 表 |
| `plan-create-complete` | 通过 | 创建 orders_archive 表 (id INTEGER 主键, note TEXT) |
| `plan-alter-missing-definition` | 通过 | 修改 orders 表的字段 |
| `plan-alter-complete` | 通过 | 修改 orders 表，增加字段 note TEXT |
| `plan-drop-table` | 通过 | 删除 customers 表 |
| `plan-disconnected-tables-need-relationship` | 通过 | 统计 orders 和 isolated_events 的数量 |
| `plan-connected-tables-use-foreign-key` | 通过 | 统计 customers 和 orders 的数量 |
| `plan-explicit-relationship-connects-tables` | 通过 | 统计 orders 和 isolated_events 的数量，关联条件：orders.id = isolated_events.id |
| `plan-derived-metric-needs-definition` | 通过 | 统计 orders 的转化率 |
| `plan-controlled-ratio-metric-is-defined` | 通过 | 统计客单价 |
| `plan-controlled-filtered-ratio-metric` | 通过 | 统计付款订单占比 |
| `plan-aggregate-needs-field` | 通过 | 计算 orders 的平均值 |
| `plan-aggregate-explicit-field` | 通过 | 计算 orders.amount 的平均值 |
| `plan-time-needs-field` | 通过 | 统计 orders 最近的趋势 |
| `plan-time-needs-range` | 通过 | 统计 orders.created_at 最近的趋势 |
| `plan-time-explicit-range` | 通过 | 统计 orders.created_at 从 2026-08-01 到 2026-08-10 的趋势 |
| `plan-semantic-dimension-targets-group-field` | 通过 | 按客户区域分组统计数量 |
| `plan-semantic-time-field-with-explicit-range` | 通过 | 统计下单时间最近 30 天的趋势 |
| `plan-semantic-time-field-still-needs-range` | 通过 | 统计下单时间最近的趋势 |
| `plan-time-needs-grain-without-configured-default` | 通过 | 统计 orders.updated_at 从 2026-08-01 到 2026-08-10 的趋势 |
| `plan-explicit-time-grain-needs-no-default` | 通过 | 按周统计 orders.updated_at 从 2026-08-01 到 2026-08-10 的趋势 |
| `plan-semantic-hierarchy-city-targets-dimension` | 通过 | 按客户城市分组统计数量 |
| `plan-dimension-drilldown-needs-target-level` | 通过 | 客户区域下钻统计数量 |
| `plan-independent-tables-do-not-need-relationship` | 通过 | 分别统计 orders 和 isolated_events 的数量 |
| `plan-multihop-foreign-key-chain` | 通过 | 统计 customers、orders、order_items 和 products 的数量 |
| `plan-fiscal-year-needs-calendar` | 通过 | 统计 orders.updated_at 2026 财年的成交额 |
| `plan-working-days-need-calendar` | 通过 | 统计 orders.updated_at 从 2026-08-01 到 2026-08-31 的工作日订单数量 |
| `plan-fiscal-year-structured-calendar` | 通过 | 统计 orders.created_at 2026 财年的成交额 |
| `plan-working-days-structured-calendar` | 通过 | 统计 orders.created_at 从 2026-08-01 到 2026-08-31 的工作日订单数量 |
| `plan-fiscal-year-explicit-calendar` | 通过 | 统计 orders.updated_at 2026 财年的成交额；业务日历：财年从 2026-04-01 开始 |

### 确定性并列指标

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `multi-metric-two-measures` | 通过 | 统计成交额和订单笔数 |
| `multi-metric-scoped-filter` | 通过 | 统计成交额和已付款成交额 |
| `multi-metric-global-enum-filter` | 通过 | 统计已付款的成交额和订单笔数 |
| `multi-metric-arithmetic-falls-back` | 通过 | 计算成交额除以订单笔数 |

### 确定性业务日历

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `calendar-fiscal-year-controlled-metric` | 通过 | 统计 orders.created_at 2026 财年的成交额 |
| `calendar-workdays-with-overrides` | 通过 | 统计 orders.created_at 从 2026-08-01 到 2026-08-05 的工作日订单数量 |
| `calendar-fiscal-quarter-controlled-metric` | 通过 | 统计 orders.created_at 2026 财年第2季度的成交额 |
| `calendar-utc-timestamp-fixed-offset` | 通过 | 统计 timestamp_events.occurred_at 从 2026-08-02 到 2026-08-02 的工作日记录数 |
| `calendar-iana-dst-dynamic-date` | 通过 | 统计 dst_events.occurred_at 从 2024-07-01 到 2024-07-01 的工作日记录数 |
| `calendar-complex-grouping-falls-back` | 通过 | 统计 orders.created_at 2026 财年的成交额，并按 orders.status 分组 |

### 确定性业务维度

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `dimension-group-count` | 通过 | 按客户区域统计数量 |
| `dimension-controlled-metric-filter` | 通过 | 按客户区域统计活跃客户数 |
| `dimension-multi-metric-filter-isolation` | 通过 | 按客户区域统计客户总数和活跃客户数 |
| `dimension-multi-metric-drilldown` | 通过 | 从客户区域下钻到客户城市统计活跃客户数和客户总数 |
| `dimension-fixed-filter-multi-metric` | 通过 | 按活跃客户区域统计客户总数和非活跃客户数 |
| `dimension-fixed-filter-drilldown-deduplicated` | 通过 | 从活跃客户区域下钻到活跃客户城市统计客户总数 |
| `dimension-explicit-same-table-drilldown` | 通过 | 从客户区域下钻到客户城市统计数量 |
| `dimension-complex-condition-falls-back` | 通过 | 按客户区域统计数量，并且 status = active |

### 确定性时间趋势

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `trend-day-count` | 通过 | 按日统计下单时间的订单数量 |
| `trend-week-count` | 通过 | 按周统计下单时间的订单数量 |
| `trend-month-controlled-metric` | 通过 | 按月统计下单时间的已付款成交额 |
| `trend-quarter-count` | 通过 | 按季度统计下单时间的订单数量 |
| `trend-year-count` | 通过 | 按年统计下单时间的订单数量 |
| `trend-utc-timestamp-day` | 通过 | 按日统计事件时间的数量 |
| `trend-iana-dst-winter-summer-boundaries` | 通过 | 按日统计美东事件时间的数量 |
| `trend-month-multi-metric-filter-isolation` | 通过 | 按月统计下单时间的订单笔数和已付款订单笔数 |
| `trend-range-multi-metric-filter-isolation` | 通过 | 统计下单时间从 2026-08-01 到 2026-08-03 的成交额和已付款成交额趋势 |
| `trend-relative-days-fixed-reference` | 通过 | 统计下单时间最近 5 天的订单数量趋势 |
| `trend-relative-days-explicit-anchor` | 通过 | 截至 2026-08-03，按日统计下单时间最近 2 天的订单数量趋势 |
| `trend-relative-weeks-inclusive-window` | 通过 | 统计下单时间最近 1 周的订单数量趋势 |
| `trend-relative-months-ambiguous-falls-back` | 通过 | 统计下单时间最近 2 个月的订单数量趋势 |
| `trend-fiscal-workdays-calendar-window` | 通过 | 按月统计下单时间 2026 财年工作日的订单数量趋势 |
| `trend-fiscal-quarter-calendar-window` | 通过 | 按月统计下单时间 2026 财年第 2 季度的成交额趋势 |
| `trend-conflicting-fiscal-and-exact-range-falls-back` | 通过 | 统计下单时间 2026 财年从 2026-08-01 到 2026-08-05 的订单数量趋势 |
| `trend-complex-condition-falls-back` | 通过 | 按月统计下单时间的订单数量，并且 status = paid |

### 动态操作图

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `graph-quantitative-query-only` | 通过 | 统计 orders 的数量和平均金额 |
| `graph-context-retrieval-only` | 通过 | 根据 orders 的备注内容说明主要原因 |
| `graph-mixed-parallel-branches` | 通过 | 结合 orders 的数量和记录内容给出结论 |
| `graph-connected-cross-table-preflight` | 通过 | 统计 customers 和 orders 的金额对比 |
| `graph-disconnected-cross-table-preflight` | 通过 | 统计 orders 和 isolated_events 的数量对比 |
| `graph-explicit-cross-table-relationship` | 通过 | 统计 orders 和 isolated_events 的数量对比，关联条件：orders.id = isolated_events.id |
| `graph-independent-multi-query` | 通过 | 分别统计 orders 和 isolated_events 的数量 |
| `graph-independent-conditional-retrieval` | 通过 | 分别统计 orders 和 isolated_events 的数量，如果有数据再查看记录内容 |
| `graph-three-independent-queries` | 通过 | 分别统计 customers、products 和 isolated_events 的数量 |
| `graph-six-independent-queries` | 通过 | 分别统计 customers、orders、products、order_items、isolated_events 和 holidays 的数量 |
| `graph-independent-word-does-not-bypass-relation` | 通过 | 分别统计 orders 和 isolated_events 的关联数量 |
| `graph-multihop-four-table-preflight` | 通过 | 统计 customers、orders、order_items 和 products 的数量对比 |

### 大 Schema 关系事实

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `large-schema-four-hop-relationship` | 通过 | 验证 customers 到 products 的多跳关系 |
| `large-schema-shared-hub-relationship` | 通过 | 验证 categories 和 suppliers 的共享产品关系 |
| `large-schema-isolated-table` | 通过 | 验证 customers 和 audit_events 保持隔离 |
| `large-schema-invalid-table` | 通过 | 验证不存在表被明确标记 |
| `large-schema-explicit-relation` | 通过 | 关联条件：audit_events.id = products.id |

### 参考 SQL

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `sql-count-orders` | 通过 | 订单一共有多少条？ |
| `sql-paid-revenue` | 通过 | 已付款订单的成交额是多少？ |
| `sql-paid-revenue-by-region` | 通过 | 按区域统计已付款订单成交额。 |
| `sql-customers-without-orders` | 通过 | 哪些客户还没有订单？ |
| `sql-top-paid-customer` | 通过 | 已付款订单成交额最高的客户是谁？ |
| `sql-paid-line-revenue-by-region` | 通过 | 按客户区域统计已付款订单明细金额 |

### 安全策略

| 用例 | 结果 | 问题/路径 |
|---|---|---|
| `read-allow-select` | 通过 | read |
| `read-allow-cte` | 通过 | read |
| `read-block-update` | 通过 | read |
| `read-block-delete` | 通过 | read |
| `read-block-pragma` | 通过 | read |
| `read-block-multiple-statements` | 通过 | read |
| `read-block-second-select` | 通过 | read |
| `read-allow-dangerous-words-in-literal` | 通过 | read |
| `read-allow-dangerous-words-in-comment` | 通过 | read |
| `read-allow-postgres-dollar-literal` | 通过 | read |
| `read-block-load-extension` | 通过 | read |
| `read-block-file-function` | 通过 | read |
| `read-block-quoted-dangerous-function` | 通过 | read |
| `read-block-mysql-executable-comment` | 通过 | read |
| `read-allow-executable-comment-text-in-literal` | 通过 | read |
| `write-allow-update-with-filter` | 通过 | write |
| `write-allow-insert` | 通过 | write |
| `write-allow-confirmed-ddl-policy` | 通过 | write |
| `write-block-unbounded-update` | 通过 | write |
| `write-block-unbounded-delete` | 通过 | write |
| `write-block-select` | 通过 | write |
| `write-block-system-operation` | 通过 | write |
| `write-block-multiple-statements` | 通过 | write |
| `write-block-comment-only-where` | 通过 | write |
| `write-block-literal-only-where` | 通过 | write |
| `write-block-mysql-comment-only-where` | 通过 | write |
| `write-block-postgres-dollar-only-where` | 通过 | write |
| `write-block-postgres-nested-comment-only-where` | 通过 | write |
| `write-allow-semicolon-in-literal` | 通过 | write |
| `write-allow-dangerous-words-in-comment` | 通过 | write |

## 真实模型通道

- 模型：`gpt-5.6-sol`（配置 `native_oai_config`，接口仅记录脱敏指纹 `7f0fcf454d967b65`）
- 提示词契约：`nl2sql-93aa256507b05f12`；中位延迟 19561 ms；总耗时 263014 ms。

| 用例 | 结果 | 延迟 | SQL/错误 |
|---|---|---:|---|
| `model-count-orders` | 通过 | 49520 ms | SELECT COUNT(*) AS order_count FROM orders LIMIT 100 |
| `model-paid-revenue` | 通过 | 16980 ms | SELECT SUM(amount) AS total_amount FROM orders WHERE status = 'paid' LIMIT 100 |
| `model-paid-revenue-by-region` | 通过 | 25082 ms | SELECT c.region, SUM(o.amount) AS total_amount FROM orders AS o JOIN customers AS c ON o.customer_id = c.id WHERE o.status = 'paid' GROUP BY c.region LIMIT 100 |
| `model-customers-without-orders` | 通过 | 11714 ms | SELECT c.name FROM customers AS c WHERE NOT EXISTS (SELECT 1 FROM orders AS o WHERE o.customer_id = c.id) LIMIT 100 |
| `model-top-paid-customer` | 通过 | 13098 ms | SELECT c.name FROM customers AS c JOIN orders AS o ON o.customer_id = c.id WHERE o.status = 'paid' GROUP BY c.id, c.name ORDER BY SUM(o.amount) DESC LIMIT 1 |
| `model-count-active-customers` | 通过 | 32126 ms | SELECT COUNT(*) AS active_customer_count FROM customers WHERE status = 'active' LIMIT 100 |
| `model-average-paid-order` | 通过 | 24373 ms | SELECT AVG(amount) AS average_order_amount FROM orders WHERE status = 'paid' LIMIT 100 |
| `model-paid-order-count-by-region` | 通过 | 12478 ms | SELECT c.region, COUNT(o.id) AS order_count FROM customers AS c JOIN orders AS o ON o.customer_id = c.id WHERE o.status = 'paid' GROUP BY c.region LIMIT 100 |
| `model-product-quantity-by-category` | 通过 | 22141 ms | SELECT p.category, SUM(oi.quantity) AS sales_quantity FROM order_items AS oi JOIN products AS p ON oi.product_id = p.id GROUP BY p.category LIMIT 100 |
| `model-customer-total-including-zero` | 通过 | 24631 ms | SELECT c.name, COALESCE(SUM(o.amount), 0) AS total_amount FROM customers AS c LEFT JOIN orders AS o ON o.customer_id = c.id GROUP BY c.id, c.name LIMIT 100 |
| `model-august-order-revenue` | 通过 | 15209 ms | SELECT SUM(amount) AS total_amount FROM orders WHERE created_at >= '2026-08-01' AND created_at < '2026-09-01' LIMIT 100 |
| `model-cancelled-order-customers` | 通过 | 14869 ms | SELECT DISTINCT c.name FROM customers AS c JOIN orders AS o ON o.customer_id = c.id WHERE o.status = 'cancelled' LIMIT 100 |

### 基线历史

- 本次运行：`model-d16e171596b241519d5002b65b7cd4a5`；历史共 3 次。

## 限制

- 64 表通道验证本地关系分析和紧凑索引事实，不运行模型，不能代表大 schema 下的模型选表、长上下文或性能表现。
- SQLite 结果不能替代 MySQL/PostgreSQL 真实兼容性验证。
- 并列指标确定性通道只覆盖 SQLite 同表 2–6 个受控普通指标和最多一个全局枚举过滤；比率、算术表达、维度/趋势混合、自由条件及远程方言仍回退既有链路。
- 业务日历确定性通道只覆盖 SQLite 声明型 DATE/DATETIME/TIMESTAMP 与显式存储口径；已归档 IANA/DST 可用于受控 UTC 换日，但跨表、分组和复杂条件仍不在该通道内。
- 业务维度确定性通道只覆盖 SQLite 单表、一个显式维度或同表层级路径以及 COUNT/1–6 个受控普通指标；自由条件、比率、跨表和远程方言仍回退模型链路。
- 安全用例验证已列出的策略边界，不等同于形式化安全证明或完整 SQL 语法覆盖。
- 真实模型通道当前只有 12 个合成 SQLite 问题；单次 12/12 不能外推为真实业务准确率。模型输出具有波动，跨版本比较必须同时记录数据集哈希、提示词契约、模型身份和重复次数。
