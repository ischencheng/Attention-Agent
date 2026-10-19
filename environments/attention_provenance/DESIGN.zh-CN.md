# Attention Proxy：修订后仍可核验的决策证据

本环境把项目的原始设计变成一项可评分的软件维护任务：**批注可以更新，已发布决策所依据的历史证据必须保持可追溯。** 完整行为契约见 [TASK.md](TASK.md)。

## 从项目本身出发

原项目的认知流水线有四个原则：Fast 只分配注意力，避免过早摘要；Mid 整理关联和批注；Strong 穿透至原始材料再决策；代理框架与业务逻辑解耦。它的价值取决于原文和加工内容始终能被区分，而不只是多层代理能否串起来。

基线 `ed4d938` 存在具体缺口：

- `skills/db_write.py` 只检查引用 ID 是否存在，数据库不可用时还会放行，没有冻结发布时的证据。
- `skills/db_update.py` 可覆盖批注与决策引用；`set` 引用未验证，追加 JSON 对象时基于哈希的去重也不适用。
- API 提供当前批注和票据，缺少历史证据核验接口。
- `001_schema.sql` 用 JSONB 保存原文，却未禁止普通应用写入修改或删除原文。

因此选择一个真实故障：用户回看旧决策时，批注已经修订；同一来源 ID 被不同 feed 使用；旧消息再次送达。直接拼接当前数据会展示错误的历史。

## 任务及评分

被测 coding agent 在现有代码库中修复写入、更新、SQL 保护和查询接口。发布前，在同一数据库快照内解析决策→批注→票据→原文，保存批注内容及其哈希。原文以来源、来源 ID 和规范 JSON 摘要共同识别；票据中的摘要或富化文本不能冒充原件。

评分器通过真实技能入口和 FastAPI 调用检验结果，分开报告原文保留、身份与重放、历史一致性、无效证据、API 兼容性及持久化。场景包含重复投递、冲突修订、嵌套 Unicode JSON、引用损坏与并发批注修改。缺少证据应明确失败，历史记录不能用当前批注补造。关键证据组全部通过才算成功，不能靠拒绝所有发布获得通过。

这把“保留原始信息”的产品原则落实成可观察的不变量，同时要求代理处理跨模块修改、事务一致性、迁移和兼容性。它没有改写项目的注意力评分策略，也没有加入主观的信息价值或投资收益奖励。

## 博客理念与岗位对应

以下是设计依据及本项目的工程解释，不代表作者为本环境背书。

- **完整工程职责。** Ege Erdil、Matthew Barnett、当时 CEO Tamay Besiroglu 的 [How to fully automate software engineering](https://www.mechanize.work/blog/how-to-fully-automate-software-engineering/)（2025-05-30）强调可靠性、规划、维护与真实需求。这里要求改好整条证据路径，而不只写一个哈希函数。
- **可比较的行为。** 三位创始人的 [The upcoming GPT-3 moment for RL](https://www.mechanize.work/blog/the-upcoming-gpt-3-moment-for-rl/)（2025-06-20）提出参考行为驱动的复制任务，也承认全面测试的难度。本环境公开行为契约，允许不同实现；隐藏实例不能夹带未说明规则。
- **质量与验证。** Ege 等的 [Cheap RL tasks will waste compute](https://www.mechanize.work/blog/cheap-rl-tasks-will-waste-compute/)（2025-08-18）主张高质量、上下文丰富的任务。其经济预测是观点；本项目采取的做法是先验证一项真实维护任务，不用随机改名充当任务多样性。
- **覆盖真实使用。** CTO Ege 参与的 [How bad data teaches models to write terrible code](https://mechanizework.substack.com/p/how-bad-data-teaches-models-to-write)（2026-06-16）讨论简单测试漏掉真实交互的问题。这里用“发布后再修订、再回看”的状态序列检验用户结果。
- **诊断模型失败。** [Mechanize 工作说明](https://www.mechanize.work/what-working-here-is-like/)（未标发布日期，2026-10-19 核对）强调 grader 公平性、确定性及轨迹分析。后续实验应区分模型失误、环境故障和评分缺陷。
- **Polaris 对应。** [Google DeepMind 官方 JD](https://www.google.com/about/careers/applications/jobs/results/114077679900074694-research-engineer-polaris-deepmind)（未标发布日期，2026-10-19 核对）要求手工构造贴近真实工程的 prompt 与 grader，评估长程能力和模型弱点。本项目对应这些交付物；90 分钟只是初始尝试预算，不能据此宣称已达到长程前沿难度。

## 范围与证据边界

这是**软件工程评测原型**，尚不能证明前沿模型难度或 RL 训练收益。运行使用真实 PostgreSQL；Kafka 发布边界在进程内捕获并持久化回放，未覆盖真实 broker、Flink、重平衡或网络故障。原始信息保留指 JSON 值等价，不指字节、空白或键顺序保真。快照证明材料可追溯，不能证明 LLM 阅读或理解了原文；数据库保护也不针对恶意超级用户。

下一步应冻结任务与 grader，收集真实 coding agent 的多次尝试，记录版本、时间和可得的成本，分析失败轨迹并审计评分漏洞。完成这些实验后，才能讨论任务的区分度及作为训练信号的适用性。
