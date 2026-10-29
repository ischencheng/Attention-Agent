# Research-to-portfolio evaluation card

This environment evaluates a continuous research policy: receive information,
identify a question, obtain evidence, optionally discover an instrument, form a
judgment, adjust paper exposure, and continue investigating until a fixed
horizon. The final scalar reward is the account's net return.

The environment and its execution checks are implemented. A training algorithm,
held-out policy improvement and investment skill are separate claims. The
bundled synthetic transcript validates engineering behavior.

The [experiment record](EXPERIMENTS.md) reports actual model attempts separately.
The initial archived attempts failed at CLI launch and action-interface use;
their zero account returns do not demonstrate a successful research policy.

## Episode contract

| Component | Contract |
| --- | --- |
| Initial state | Cash account, topic scope, time-indexed source arrivals, a declared instrument-coverage catalog and frozen market data. |
| Observation | Originals available by the current cutoff, recorded research state, discovered instruments, tool results, account state and remaining budgets. |
| Policy | Fast routes information; Mid accumulates source-attributed facts and questions; Strong investigates, revises judgments and proposes allocations. |
| Research | Read originals, inspect the available catalog, acquire price history and the bundled analytical framework, and document counterevidence. The tool set is recorded for the selected replay mode. |
| Action | A typed role action with recorded arguments and execution feedback. Rejected actions consume budget. |
| Transition | A research-state change, a source arrival, a scheduled review, or a market/account event. |
| Execution | Cash-only, long-only paper account; complete target weights; next eligible open strictly after submission; frozen fees and slippage. |
| Termination | Fixed horizon. Early stop and budget exhaustion settle existing holdings at that same horizon. |

The standard interface supports archived lexical search. Strict replay exposes
a paginated source catalog and requires a counterevidence assessment grounded
in originals the role has actually read. An empty catalog result cannot certify
that contrary evidence does not exist outside the archive.

Scheduled replay uses an independent virtual clock: source arrivals activate
Fast, Mid and Strong retain separate 30/40-minute schedules, and a recorded
thesis review deadline can activate Strong. Idle cutoffs advance without an
ordinary model call. These are isolated replay mechanics; the production
supervisor is operated separately.

A policy receives public observations through the inference adapter. The full
scenario, future price tape and evaluator artifacts belong to the environment.
The local Python object itself is not a sandbox for arbitrary in-process code.

## Reward and accounting

```text
NAV(t) = cash(t) + sum(quantity(asset, t) * raw_mark(asset, t)) + receivables(t)
reward(T) = NAV(T) / initial_cash - 1
reward(t < T) = 0
```

Fees debit cash once. Slippage changes execution prices. Splits change share
quantities; dividends enter NAV as receivables at entitlement and become cash
at payment. Terminal valuation marks remaining holdings at the declared close,
so unrealized losses remain in the outcome. The account does not assume a
liquidation trade.

There are no bonuses for reading, writing a rationale, producing a forecast or
making more tool calls. A positive return does not establish that the thesis
was true. An eligible completed cash-only policy can have zero return; a model
transport failure is an ineligible run with its accounting outcome retained.

The current broker models fractional shares, fixed slippage and fees. It does
not model order-book liquidity, market impact, borrowing, short positions or
bankruptcy/delisting. All assets need complete required session coverage and a
terminal close before the episode can run.

## Time and data admission

Sources carry separate publication and actual receipt times. Both must be at or
before the observation cutoff. The captured-source importer retains the production
`raw_messages` identity and receipt timestamp; it does not backdate receipt to
the publisher's date. Corrections remain distinct source versions.

An archive contains the following inputs and their hashes:

| Artifact | Evidence and scope |
| --- | --- |
| `archive-contract.json` | Episode selection, artifact hashes, policy identities and the recorded admission verdict. |
| `sources.json` | Captured original records, source-version identities, publication/receipt times, canonical content hashes and receipt metadata. |
| `market.json` | Provider responses, projected raw bars, corporate-action inputs and their hashes, with collection time and market-data vintage. |
| `policy-manifest.json` | Immutable model revision, release evidence and artifact hashes for Fast, Mid and Strong; knowledge cutoff remains unknown when unavailable. |
| `episode.json` | Executable projection of the admitted source and market inputs. |

Hash validation establishes artifact consistency. It does not certify the
publisher's claims, prove that all relevant sources were captured, or provide
independent attestation of the collector.

The current market importer declares **historical reconstruction**: prices are
downloaded after the interval and may include provider revisions. Such an archive
can support a disclosed retrospective replay; it does not certify the original
price vintage available at each historical instant.

A checkpoint published before the episode provides a model release boundary.
It is not a substitute for a documented training-data cutoff. The serving
adapter verifies the declared weight, tokenizer and template files before
inference, then checks the serving identity against the archive manifest. The
current GPU adapter does this on the remote worker, with a local transport
bridge. This is a trusted adapter contract, not independent remote attestation.
Unknown cutoffs and unverified files remain visible in `replay_integrity` and
the viewer.

The instrument catalog is the dataset's declared coverage universe. A policy
must explicitly discover an instrument before using it. Catalog membership
neither recommends an asset nor establishes coverage of the whole market.

## Observable execution

| Run artifact | What it establishes | What needs separate evidence |
| --- | --- | --- |
| `contract.json` | Scenario and implementation identity, action limits and fixed reward contract. | Generalization beyond the selected scenario. |
| `reset.json` | The public observation supplied at episode start. | Earlier information outside the archive. |
| `trajectory.jsonl` | Executed actions, tool returns, state transitions, rejections, role and invocation links. | Truth of generated analysis or hidden model reasoning. |
| `model-calls.json`, `model-calls/` | Invocation status, model identity, raw returned outputs and usage when reported. | Unreported token cost, latency or training statistics. |
| `actions.json` | The executed action sequence for deterministic replay. | A new independent policy attempt. |
| `reconciliation.json` | Independent account arithmetic from fills, costs, corporate actions and the frozen price tape. | Optimality of the policy or independent verification of target-weight sizing. |
| `summary.json` | Episode status, eligibility, research counts, account outcome and model-call counts. | Investment skill from a single realized return. |
| `trace.html` | Human inspection of the above public trajectory; the cutoff view and frozen full-run result are labelled separately. | An isolation boundary for policy execution. |
| `credit-contract.json` | Which role outputs are candidate policy actions and which segments are observations or harness markers. | Alignment with bridge-audited token IDs, log probabilities and a trainer-specific loss mask. |

A batch cannot use a tool result it has not yet observed. After research tools
return, the model is invoked again before dependent judgments or allocations.
Actions sampled for an older cutoff are discarded when time advances. Invalid
JSON is recorded as a decoder-origin rejection and returned as feedback;
transport failures remain failed runs. The durable trajectory retains completed
actions even when a later invocation fails.

The policy observation retains durable research state and receipt metadata.
Newly read original bodies are delivered intact after the previous action batch;
older bodies can be retrieved again. The projection removes repeated historical
bodies from context without creating a hidden model-written summary. Context
limits remain explicit; oversized inputs fail rather than silently losing new
evidence.

## Checks against false success

The [tests](tests/) exercise the declared contract with positive and negative
cases. Relevant examples include:

- **Future information:** changing an unreleased source or future close must not
  change the current public observation. Premature reads are rejected.
- **Research prerequisites:** unread citations, missing research tool results,
  duplicate evidence and unsupported exposure increases must not pass gates.
- **Time and scheduling:** equal arrival times use a stable order; independent
  role timers, due reviews and idle periods remain distinct.
- **Execution boundaries:** orders use the eligible next open, risk reductions
  cannot become accidental share increases after gaps, and fees/corporate actions
  preserve accounting identities.
- **Independent reconciliation:** [reconcile.py](reconcile.py) recomputes cash,
  positions, receivables, execution prices, fees, terminal NAV and reward without
  calling the broker implementation or trusting its cash deltas. Tampered
  account results fail. Recorded fill quantities are inputs to this check.
- **Failure visibility:** failed or incomplete episodes do not become successful
  zero-return policies. Untrusted source content remains inert in the viewer.

Run the suite from the repository root:

```bash
python -m unittest discover -s environments/research_portfolio/tests -v
```

These are environment checks with explicit coverage. Passing them does not
establish resistance to every information leak or deliberate grader attack.

## Protocol for a policy comparison

Before claiming a policy improvement, freeze the archive, time/event split,
model revisions, harness settings, tool interface, horizon, costs and budgets.
Keep development cases separate from evaluation intervals. Preserve every run's
eligibility and failure reason alongside its outcome.

Compare policies under identical inputs, including a cash baseline and any
additional preregistered allocation baseline. Repeat stochastic model attempts
and report each result, completion/eligibility rates, inference cost when
available, and return distributions. A repeated action replay is a
reproducibility check, not another model attempt. Related observations from the
same market interval do not become independent financial samples by changing
random seeds.

Report the role coverage: training only Strong would be a partial-policy
experiment. A learning claim additionally needs an implemented trainer,
verified observation/action masks, checkpoint recovery and held-out results from
the final policy. More completed tool calls alone do not demonstrate learning.

## Related contracts in this repository

The [production judgment evaluator](../attention_judgment/README.md) independently
freezes actual Strong decision packets and settles eligible forecast windows.
Research snapshots with no formal decision remain unscored. Its operational
health, forecast scores and this environment's portfolio returns are distinct.

The [Attention Provenance coding task](../attention_provenance/README.md) evaluates
software repairs to historical decision evidence. It exports a frozen unsolved
checkout and runs an external PostgreSQL grader against real skill/API entries.
Its [verification record](../attention_provenance/VERIFICATION.md) covers the
reference repair, untouched baseline and selected defects. Kafka publication is
captured at a boundary in that grader; the task does not exercise the full
streaming deployment. The current checkout contains the reference repair, so
blind attempts must use the exported baseline. Coding-task scores do not measure
investment performance or policy training improvement.
