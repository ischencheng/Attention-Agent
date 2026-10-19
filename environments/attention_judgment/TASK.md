# Attention Judgment: discover, remember, revise, resolve

Build and run the longitudinal judgment loop of AI Attention Proxy. The agent
consumes a topic-led information stream, discovers questions worth investigating,
maintains evidence-backed theses, and revisits them as new observations and actual
outcomes arrive. A fixed portfolio, fixed asset universe, allocation optimizer,
or compulsory trade is not the task.

## Existing product and topic scope

Use the project's Fast attention gate, Mid curation, Strong judgment, source
records, annotations, and persistent agent state as the product foundation.
Preserve access to original information rather than replacing it with summaries.

The deployed topic configuration includes AI, earnings, guidance, capex,
datacenters, storage, power, energy, nuclear, semiconductors, GPU, inference, LLM,
margin, valuation, CPI, Fed, and yields. Followed-account content supplies another
discovery channel. Topic relevance concerns the content and its connection to
the user's interests, not literal keyword matches alone. The legacy WATCHLIST
and price-feed subscriptions must not become a mandatory research universe.
Entities, tickers, and new related themes may be discovered from evidence.

## Required behavior

1. **Observe without future information.** Preserve original observations with
   source identity, event time, availability/ingestion time, and content identity.
   A replay step may use only observations available by its cutoff. Distinguish
   delayed reports, corrections, and independent observations; syndication or
   repeated delivery is not independent corroboration.
2. **Record discoveries.** A discovery has a stable identity, topic, concise
   question or hypothesis, first-seen time, materiality explanation, and source
   references. It may concern an industry, mechanism, policy, or company without
   having a ticker. Explain why it deserves attention instead of emitting a
   recommendation for every item. A single authoritative filing can be material;
   three reposts do not automatically pass an evidence threshold.
3. **Maintain a judgment ledger.** Persist thesis identity and version, subject,
   statement, intended horizon, relevant MrBrain dimensions, supporting and
   contrary evidence, uncertainty, invalidation conditions, and next research
   questions. Record whether new evidence leaves a thesis unchanged, strengthens
   it, weakens it, contradicts it, or replaces it. Old versions remain inspectable.
   Summarizing working memory must not erase contrary evidence or invalidation.
4. **Allow meaningful non-action.** Supported judgments include `watch`,
   `abstain`, `research`, `positive`, and `negative`. These are research outcomes,
   not brokerage instructions. Waiting, retaining an existing judgment, or saying
   evidence is insufficient is valid. Do not force target weights, trades, or
   confidence inflation. A changed view needs new evidence or an explicit
   correction to prior reasoning; a duplicate headline is insufficient.
5. **Apply the framework to facts.** Select relevant MrBrain dimensions and
   explain the mechanism: durable economics, cycle drivers, alternative outcomes,
   thesis failure, or survivability. `not applicable` and `unknown` are valid with
   reasons. Calling a framework helper or filling fields does not establish that
   reasoning is sound. Distinguish a price move from a change in business facts.
6. **Freeze testable predictions.** Before observing an outcome, save the exact
   question, forecast time, horizon/due date, probability if supplied, target
   metric, comparison operator or directional rule, resolution source, benchmark
   if applicable, and missing-data policy. Keep thesis confidence separate from
   probability for a precisely defined event. Revisions create a new forecast;
   they do not rewrite a prior forecast or its resolution rule.
7. **Settle actual observations.** For a discovered traded entity with a frozen
   mapping, report daily, weekly, and monthly closing-price observations when
   available. Freeze the exchange calendar, start convention, price adjustment
   basis, horizon definitions, and benchmark before comparison. Distinguish
   calendar-week/month closes from a fixed number of trading sessions. Report
   absolute and benchmark-relative results, with data timestamps and provenance.
   Non-traded subjects use their predeclared event or fundamental metric instead.
   Do not select winning asset proxies or convenient horizons retrospectively.
8. **Preserve unresolved outcomes.** Future maturity is `pending`; unavailable,
   incompatible, or unverifiable observations are `unresolved` with a reason.
   Holidays, missing prices, splits, dividends, restatements, and delistings need
   an explicit handling rule. Never replace missing values with zero returns or
   success. A short-term price observation cannot resolve a longer-term business
   thesis unless that was its frozen criterion.
9. **Recover and replay faithfully.** Persist the consumption cursor, judgment
   versions, outstanding research, frozen forecasts, and completed resolutions.
   Restarting must not lose a thesis, submit a duplicate outcome, or present an
   old prediction as newly made. Corrections append provenance and supersession
   information; original observations and historical decisions remain available.

## Longitudinal evaluation sequence

Exercise a topic discovered from the stream, then repeated reports, a price move
without new operating evidence, a credible contradictory observation, a change
in cycle indicators, and a later correction. Carry unresolved questions across
steps and a restart. Include no-ticker subjects and legitimate abstention. Vary
identities and content rather than making one named stock the answer.

The evaluator must distinguish deterministic failures (future data, lost state,
fabricated references, overwritten forecasts, invalid settlement) from disputed
judgment. There is no universal correct bullish/bearish answer. Assess whether
the stated conclusion is supported, addresses material counterevidence, and has
honest limits. If natural-language review is used, retain its rubric, model or
reviewer, evidence, and rationale; it is not an infallible reward oracle.

## Run protocol and acceptance evidence

- Label every run **synthetic**, **historical replay**, or **forward observation**.
  Synthetic fixtures test behavior; they are not market-performance evidence.
- Record actual model/version, timestamps, data manifest, tool access, budget,
  and cost when available. Identify human-written, heuristic, and model-generated
  judgments separately. Never describe a static helper as a learned analyst.
- Historical replay must enforce point-in-time data, but the model may already
  know subsequent events from training. Disclose that contamination risk;
  retrospective returns do not establish blind predictive ability.
- Forward observations must freeze judgments before outcomes. Report the
  completed observations and the pending maturity schedule honestly. Writing a
  scheduler is not evidence that a week or month of observations has occurred.
- Report discovery coverage/attention cost, evidence correctness, contradictory
  updates, memory/recovery, forecast coverage and calibration where sample size
  supports it, and price/benchmark outcomes separately. Abstentions are not
  directional wins; do not hide them from coverage denominators.
- Freeze baselines before comparison. Useful controls are no long-term memory,
  append-only summaries, and versioned judgments, using comparable inputs,
  model/tool budgets, and observation cutoffs. A market benchmark is a separate
  return comparison, not a substitute for an agent baseline.
- Keep the evaluator and final resolution rules outside the candidate's control.
  Inspect failure trajectories and grader errors. Report software checks, actual
  judgment runs, and matured outcomes as separate evidence levels. Do not claim
  durable investment skill, model training gains, or general long-horizon ability
  from a passing schema test or a small profitable sample.

The source-grounded rationale, relevant local transcripts, and external design
references are in [DESIGN.zh-CN.md](DESIGN.zh-CN.md).
