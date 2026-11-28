# 发现后查询 SEC / FRED 原始记录

`skills/research_sources.py` 供 agent 在发现问题后按公司或宏观指标查询，不维护固定股票组合。默认只返回研究 JSON；显式 `--publish` 才将原始记录交给 `source_raw → Flink → Codex filter → embedding → Fast/Mid/Strong`。Flink 放行这两类已被主动请求的来源，后续过滤照常执行。

```sh
python skills/research_sources.py '{"source":"sec_filings","symbol":"NVDA","limit":3}'
python skills/research_sources.py '{"source":"sec_facts","symbol":"NVDA","concept":"Revenues","limit":3}' --publish
python skills/research_sources.py '{"source":"fred","series_id":"DGS10","as_of":"2026-10-16","limit":5}'
```

环境配置：`SEC_EDGAR_USER_AGENT` 必须含应用名称和获准使用的真实联系邮箱；FRED 需要 `FRED_API_KEY`；发布使用 `KAFKA_BOOTSTRAP`。凭据只由环境传入，不能写进请求 JSON、源码或输出。未配置时返回 `status: blocked`、空 `records` 并退出 2，不会声称取得数据。

输入为一个 JSON 对象，`source` 必须是 `sec_filings`、`sec_facts`、`fred`。SEC 接受 `symbol` 或 `cik`，facts 还需精确的 `concept`，可选 `taxonomy`（默认 `us-gaap`）；filings 可选 `forms`。FRED 需 `series_id`，可选 `as_of`、`start`、`end`（日期格式 `YYYY-MM-DD`，满足 `start ≤ end ≤ as_of`）。三者均可设 `limit`（1–20，默认 5）。结果状态为 `ok/empty/blocked/invalid`，包含 `records`；成功查询还返回 `snapshots`。

每条记录保留 `source/source_id/raw/raw_sha256`，以及 `data.url/observed_at/published_at/filing_date/vintage`。`retrieval` 保存公开请求 URL、抓取时间、响应哈希和本地快照路径。完整 API JSON 按内容哈希存入忽略目录 `environments/attention_judgment/runs/research-sources/objects/`。原始日期、数值和单位不由模型改写。

- SEC：实时读取当前 submissions/companyfacts 快照，**不支持历史 as_of 重建**。submissions 只覆盖返回的 recent 集合；filing 记录是元数据和正文链接，未下载申报全文。companyfacts 保留每个事实的 `start/end/filed/accn/unit`，不合并季度与年初至今数值，不擅自替换 XBRL 标签。查询返回 `latest_record_filing_date`；旧标签可能多年未更新。只有真实 `acceptanceDateTime` 才作为发布时间，日期精度的 `filed` 不伪造成午夜发布时间。
- FRED：同时固定 `realtime_start`、`realtime_end` 为 `as_of`，保留每条 observation 的实时区间和原始值（含缺失值 `.`）。`observation_date` 是经济数据所属日期，不等于发布日期；每日 vintage 不提供盘中发布时间。默认 as_of 使用提供方 America/Chicago 当日，禁止未来日期。
- 发布：调用现有 `produce_confirmed`，只在 broker ACK 后记录本地去重键（来源、原始 ID、原文哈希）。失败返回 `uncertain` 并保留重试能力；ACK 后进程崩溃仍可能重放，因此下游继续依赖原文身份去重。原始数据进入 Kafka 不等于投资判断已经正确。

## 来源与实现边界

设计参照 TradingAgents `main` 的已核对版本 [`e8f5503`](https://github.com/TauricResearch/TradingAgents/tree/e8f5503e9b86d5c2710b0c76b1f686703968c9df)（2026-10-16）：[`vendors/sec_edgar.py`](https://github.com/TauricResearch/TradingAgents/blob/e8f5503e9b86d5c2710b0c76b1f686703968c9df/tradingagents/dataflows/vendors/sec_edgar.py)、[`vendors/fred.py`](https://github.com/TauricResearch/TradingAgents/blob/e8f5503e9b86d5c2710b0c76b1f686703968c9df/tradingagents/dataflows/vendors/fred.py)、[`default_config.py`](https://github.com/TauricResearch/TradingAgents/blob/e8f5503e9b86d5c2710b0c76b1f686703968c9df/tradingagents/default_config.py)。本实现独立编写，仅借鉴提供方分离、申报日期与统计期间区分、FRED vintage 固定等约束，没有整份复制 vendor 实现。

官方契约：[SEC API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)、[SEC 自动访问规范](https://www.sec.gov/about/developer-resources)、[FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)、[FRED 实时区间](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)。本机共享请求锁限制为至多每 0.25 秒一次，低于 SEC 的每秒 10 次上限；多机部署仍需共享限流。

实际端点：

- `https://www.sec.gov/files/company_tickers.json`
- `https://data.sec.gov/submissions/CIK0001045810.json`
- `https://data.sec.gov/api/xbrl/companyfacts/CIK0001045810.json`
- `https://api.stlouisfed.org/fred/series`
- `https://api.stlouisfed.org/fred/series/observations`
