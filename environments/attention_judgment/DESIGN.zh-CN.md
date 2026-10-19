# Attention Proxy：持续发现与判断

本环境检验代理能否从长期信息流中发现值得研究的问题，建立有依据的判断，处理反证，并在真实结果到来后诚实复盘。主题来自原项目的信息入口；公司和证券可以在研究中出现，任务不要求固定股票池、目标仓位或模拟基金经理。证据链是支撑设施，最终产品是用户值得花时间阅读的发现与判断。

## 从现有项目出发

[`docker-compose.yaml`](../../docker-compose.yaml) 的实际主题配置包含 AI、财报、指引、资本开支、数据中心、存储、电力、能源、核能、半导体、GPU、推理、LLM、利润率、估值、CPI、Fed 与收益率。Flink 按这些关键词筛选部分来源；关注账号的 Twitter 内容另行通过，原始价格订阅也有自己的证券配置。因此，配置里的 `WATCHLIST=NVDA,SNDK,SPY,QQQ` 是旧 Strong 阶段的限制，不应被解释为 Attention Proxy 的完整研究世界。

保留原设计的三层职责：Fast 分配注意力，避免过早摘要；Mid 归并事件、交叉核对和保存反证；Strong 将证据与长期问题联系起来。新的产物是发现记录和版本化判断：**目前相信什么、为什么、有哪些反对证据、什么条件会改变看法、下次应检查什么**。`watch / abstain / research / positive / negative` 均可以是合理输出；后两者表示观点方向，不代表下单。

## MrBrain 提供判断原则，不提供标准股价答案

已检查技能参考、仓库框架及以下五份原始文字稿。它们表达作者的分析方法；涉及具体历史数字或金融断言时，实际研究仍需独立核实。

| 设计要求 | 仓库中的原始依据 |
|---|---|
| 持续检查原始逻辑、估值与机会成本；没有新变化时，保留判断或继续观察有效。 | [卖出纪律，视频 nMEOcS8hzgg](../../MrBrain-invest-principle/transcripts/nMEOcS8hzgg_巴菲特们藏私的一招_股票卖点的秘密.txt)，650–780 行 |
| 区分资本开支、供需、库存与滞后利润；同一指标在不同周期阶段可能有不同含义。 | [周期，视频 VP09PAKYUq4](../../MrBrain-invest-principle/transcripts/VP09PAKYUq4_一口气讲透周期_强周期股投资要义.txt)，738–1004 行 |
| 承认概率估计的不确定性，先说明错了会怎样；现金与等待可以有价值。 | [风险与生存，视频 6WjdqxcEjR8](../../MrBrain-invest-principle/transcripts/6WjdqxcEjR8_概率的傲慢与赔率的智慧__随机漫步的傻瓜_与_黑天鹅_述评.txt)，82–148、448–590 行 |
| 保留选择权、限制不可承受的下行，不能把某一种交易工具当作框架本身。 | [反脆弱，视频 vBu6zJAWcGs](../../MrBrain-invest-principle/transcripts/vBu6zJAWcGs_右派投资者的自我修养__反脆弱_与_非对称风险_述评.txt)，1722–1791 行 |
| 护城河应落实为持续经济性，如转换成本、定价权与难复制的优势，不能只复述热门叙事。 | [竞争与护城河，视频 cOxXsKLXbAw](../../MrBrain-invest-principle/transcripts/cOxXsKLXbAw_为什么竞争一文不值_颠覆你对投资和人生的全部认知.txt)，400–540、570–710 行 |

不将 PEG、90/10 配置或固定周期年数设成普适答案。`skills/mrbrain_invest.py` 当前返回分析问题与检查清单；调用该脚本不等于完成判断，更不等于学到了投资能力。环境应检查观点怎样随证据更新，而非是否机械填满所有框架字段。

## 长期记忆与实际运行

原始观察只追加，记录事件时间和获知时间。判断可以修订，但旧版本、反证、失效原因和替代关系保留。恢复执行依赖持久化消费游标、判断版本、研究待办和已提交记录，不能依赖某个模型会话还记得什么。

这采用 [ai-agent-book 第 3 章](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter3.md) 对轨迹、记忆与业务状态的区分，以及[第 6 章](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter6.md) 的事件处理机制；[第 9 章](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter9.md) 的记忆整理用于压缩活动上下文，不能删除不利证据。版本固定到 `dbc046eb896ac4e39aa19c7774c8bf49583b89a6`。这些是本项目的设计映射，书中没有规定本环境的金融判断结构。

至少区分三种运行：合成事件用于验证软件行为；历史材料按当时可见时间回放；前瞻观察在结果未知时冻结判断，再等待事实。历史回放即使隔离了数据，模型仍可能通过预训练知道后续结果，必须明确标为回顾性实验，不能冒充盲测预测能力。

## 评分为什么不能只看涨跌

发现发生时就冻结主题、判断版本、证据、预测问题、时间范围、检验来源、价格参照与基准。可交易对象可以随后报告日、周、月收盘变化和相对基准结果，但必须在看到结果前绑定对象与基准。没有合适证券映射的产业判断，按预先定义的经营或事件指标验证；不得事后挑一只涨得最好的股票代表原观点。

日、周、月价格观察与长期论点的到期检验分开。十二个月的供需判断不会因一天上涨自动成功；预测尚未到期就是 `pending`，缺数据就是 `unresolved`。未表达方向的观察、保留判断与研究请求不计作方向命中；它们的覆盖率和后续研究价值另报。

结果报告分别呈现：证据与事实质量、反证处理、跨期记忆、信息时点、关注成本、可结算预测、市场表现。比较“无记忆”“只追加摘要”“版本化判断”时固定材料、模型、工具和预算。少量事件或一轮行情只能展示轨迹，不能证明校准、稳定优势或训练收益。

## Mechanize 与岗位理念的具体应用

创始人的 [How to fully automate software engineering](https://www.mechanize.work/blog/how-to-fully-automate-software-engineering/)（2025-05-30）强调完整职责与可靠性；这里把发现、记忆、研究、冻结和复盘连成真实产品流程。[The upcoming GPT-3 moment for RL](https://www.mechanize.work/blog/the-upcoming-gpt-3-moment-for-rl/)（2025-06-20）及 [Cheap RL tasks will waste compute](https://www.mechanize.work/blog/cheap-rl-tasks-will-waste-compute/)（2025-08-18）支持构建上下文充分、可检验的任务，而非拼接大量相似问答。

这并不意味着投资判断存在与程序测试相同的单一标准答案。参考[第 7 章的评估与验证器边界](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter7.md)，代码验证确定性约束，来源支持与推理质量接受可审计评审，真实结果按冻结规则延迟结算。模型不得改写自己的验收标准。完整行为契约见 [TASK.md](TASK.md)。
