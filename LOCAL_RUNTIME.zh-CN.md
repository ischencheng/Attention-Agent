# 本地完整流水线

> The current deployment uses one Fast model filter. See the updated
> [English runtime guide](LOCAL_RUNTIME.md) for the relay, role models and upgrade.

本地部署沿用项目的消息链路：

`X / YouTube → source_raw → Flink → source_clean → 转发 → Fast → attention_tickets → Mid → annotations → Strong → decision_packets`

通过过滤的原文同时进入本地语义向量库。PostgreSQL 保存原始内容、各级结果和 Mid/Strong 的长期状态。前端在 <http://localhost:5173>，API 在 <http://localhost:8000>，Flink 状态在 <http://localhost:8081>。

Docker 中运行消息、数据库和工作进程；宿主机运行 Codex gateway、真实的 `all-mpnet-base-v2` 768 维 CPU embedding，以及 Mid/Strong 的独立循环。这允许使用宿主机现有的 Codex 登录，不读取或复制认证文件。`container-agents` profile 是另一个需要独立认证的部署方式，默认本地流程不启动那些空等容器。

## 安装与启动

需要 Docker Desktop、Python 3.12、Node.js 和已登录的 Codex。初次安装：

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r scripts/requirements-local.txt
npm install --prefix environments/attention_judgment/runs/runtime @openai/codex@0.163.0
docker compose build
```

安装 [Agent Reach CLI](https://github.com/Panniantong/Agent-Reach)。在 `.env` 的 `ATTENTION_X_ACCOUNTS` 填 X 账号，在 `ATTENTION_YOUTUBE_CHANNELS` 填 YouTube `@handle` 或 `UC…` 频道 ID，多个目标用逗号分隔。留空会禁用该来源。X 需要执行 `agent-reach add twitter` 和 `agent-reach login twitter`，然后在打开的浏览器中手动登录。YouTube 读取频道 Videos 标签内的普通视频简介及可用字幕。先预览，再按需启动流水线。旧部署升级时，先停旧服务，然后执行 `docker compose build flink-job` 构建包含新路由的 Flink 镜像。

```bash
.venv/bin/python scripts/ingest_social.py --preview --limit 1
.venv/bin/python scripts/local_pipeline.py start
```

预览直接读来源，不写 Kafka 或投递账本。持续采集每 20 分钟轮询，默认每个账号或频道最多读三条。两路状态单独写入 `environments/attention_judgment/runs/local-pipeline/social.log`。缺登录或需要平台验证时，该来源会暂停，处理后先用预览确认，再重启采集器。没有字幕时保留视频简介并标 `metadata_only`。Kafka 确认收到后才登记去重。SEC、FRED 继续作为按需查证工具。

`start` 启动 Docker 服务和一个有文件锁的宿主机 supervisor。重复调用不会重复开启 supervisor。模型网关使用随机本地 token；首次下载 embedding 模型时可能暂时未就绪，消费者保留消息并重试。分类调用按内容、角色、模型和提示版本缓存；模型失败不会伪装成过滤否决。

## 持续运行与验证

Mid 每 30 分钟检查新消息，Strong 每 40 分钟检查新注释，并每天复核已有研究。没有新信息的普通周期跳过模型调用。每个角色不重叠执行；尚未处理的输入保留到下一个周期。进程和模型请求失败可重试，Kafka 输入只有在下游消息确认后才提交 offset。交付语义是 at-least-once，Mid 必须按原文身份、版本和独立事件去重。

```bash
.venv/bin/python scripts/local_pipeline.py status
.venv/bin/python scripts/local_pipeline.py cycle mid
.venv/bin/python scripts/local_pipeline.py cycle strong
.venv/bin/python scripts/local_pipeline.py evaluate
curl http://localhost:8000/api/overview
```

运行日志、模型事件、token usage 和调度状态保存在 `environments/attention_judgment/runs/local-pipeline/`，不提交 Git。验收要检查实际 stage count、Mid/Strong 的 `agent_state`、写入的决策以及 `/api/decisions/{id}/evidence` 的 `verified` 状态。容器为 running、模型返回 JSON、或页面能打开都不单独算端到端成功。

本地进程依赖这台电脑和 Docker 保持运行；电脑睡眠期间不会继续采集。已有消息、去重记录、数据库和评估账本会在重启后保留。不要删除 Kafka/PostgreSQL 数据目录来解决启动问题。

macOS 可执行 `.venv/bin/python scripts/install_local_service.py` 安装当前用户的
`local.attention-proxy` LaunchAgent：登录后自动恢复 supervisor，退出异常后重启。
配置保存在 `~/Library/LaunchAgents/local.attention-proxy.plist`，不包含 API key。
Docker Desktop 也需保持运行。停止服务可执行
`launchctl bootout gui/$(id -u)/local.attention-proxy`；这不会删除数据库或账本。

## 生产结果与评估

独立的 `pipeline.py` 每五分钟读取实际 Strong `decision_packets`，验证证据后冻结到 `environments/attention_judgment/runs/production/ledger.sqlite3`。修改过的判断追加新版本，保留旧版本。它不调用模型另造判断，也不从固定股票池生成组合。

日、周、月价格评价从观察者冻结之后的下一次正常开盘起算，不能倒填到新闻发布时间。只有明确注册了方向概率的输出才计算 Brier 等预测评分；普通观点仅做价格观察。价格上涨不等于长期因果论点正确，证据、反证和失效条件要独立复核。尚未到期为 pending，缺失价格为 unresolved。

早期 `runs/paper/` 是独立 RSS 判断试验，保留供追溯，不计为这个完整系统的生产表现。合成 replay 只检验状态与证据规则，不是收益证明。
