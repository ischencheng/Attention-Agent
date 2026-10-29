# Recorded replay experiments

The first remote GPU episode **failed after repeated reads and an output-token
limit**. It reached one accumulated theme but produced no research question,
judgment or position. Its account remained at $100,000; the recorded zero return
is an ineligible accounting outcome, not demonstrated investment performance.

These runs were executed on October 29, 2026. This is a sanitized aggregate
record derived from the saved summaries, invocation records, trajectories,
archive contract and account reconciliation. Original source bodies, production
record identifiers and raw model traces remain local.

## Remote GPU result

The [machine-readable result](results/2026-10-29-qwen14b-l40s-v1.json) records
aggregate counts, validity flags, model identity and hashes of the archive,
contract and implementation. The episode ran code commit
`683e03a52af9fc68f9573f0a588d2ded77c1eeb8`; all 12 implementation hashes recorded
in its contract match that commit.

| Measure | Observed result |
| --- | --- |
| Model calls | 47 attempts: 44 completed, 2 invalid action-JSON outputs, 1 failed generation. Fast: 45; Mid: 2; Strong: 0. |
| Executed model actions | 56 successful reads of **one distinct original**, one triage and one accumulation. Two decoder-origin rejection markers were recorded separately. |
| Research state | 1 ticket and 1 theme; 0 research questions, judgments or revisions. |
| Ending | The last generation reached its fixed 2,048-token output limit. Codex reported an incomplete response and failed the run. The 96-call budget was not exhausted. |
| Validity | `run_status=failed`, `policy_outcome=failed`, `evaluation_eligible=false`. The harness still settled the account to the original horizon. |
| Financial state | Initial cash, final cash and terminal NAV: $100,000. Reward: 0. No positions or fills; no explicit hold decision was established. |
| Accounting check | Independent reconciliation passed, with 0 fills checked. |
| Replay check | Replaying all 74 recorded transitions reproduced the account, reward, research counts and terminal flags. This is an action-replay check, not another model attempt. |

The final failed request contained 21,186 input tokens and produced exactly
2,048 output tokens. The bridge recorded **674,354 input and 5,558 output
tokens** across the 47 episode requests, including that failed generation and
excluding the transport smoke. The 46 usage-bearing model-call records report
653,168 input and 3,510 output tokens; the difference is the failed request.
Billing was not retrieved.

| Runtime | Recorded configuration |
| --- | --- |
| Hardware and checkpoint | One NVIDIA L40S; `Qwen/Qwen3-14B`, revision `40c069824f4251a91eefaf281ebe4c544efd3e18`, BF16. |
| Software | Codex CLI 0.163.0; vLLM 0.32.0; PyTorch 2.13.0+cu130; Transformers 5.19.0; huggingface-hub 2.2.0. |
| Decoding | Thinking disabled, temperature 0, seed 0, concurrency 1; 24,000 input and 2,048 output tokens per request. |
| Validation | Model files and serving identity checked. Structured decoding constrained the response envelope; action JSON and environment prerequisites remained separately validated. |
| Lifecycle | The GPU app was confirmed stopped with 0 tasks at 07:06:32 UTC. No training occurred. |

The first GPU transport smoke failed before generating tokens because the
installed tokenizer returned a `BatchEncoding` where the adapter expected a
token list. After an explicit token-list correction, the Codex smoke returned
`ok=true` with 162 input and 6 output tokens. That transport check is separate
from the unsuccessful research episode.

The source episode and financial contract were retained from the local
iterations, with a new policy manifest for the larger checkpoint. The action
instructions and observation-memory projection also changed during development.
This is therefore not a controlled model comparison. The run predates the later
`framework.json` snapshot fix and never called the framework tool; the later
fix does not retroactively change its recorded implementation.

## Local development history

| Attempt | Execution outcome | Research and trading | Account outcome |
| --- | --- | --- | --- |
| v1 | One invocation attempted. The relative Codex executable path could not be found from the invocation's working directory; inference did not start. `run_status=failed`, `evaluation_eligible=false`. | 0 source reads, questions, theses or fills. | Cash and terminal NAV: $100,000. Recorded reward: 0. Reconciliation passed with 0 fills. |
| v2 | 96 model calls completed, all in Fast. All 96 actions were rejected as `unknown action type`; the model-call budget was exhausted. | 0 accepted triage actions, source reads, questions, theses, research tools or fills. Mid and Strong were never reached. | Cash and terminal NAV: $100,000. Recorded reward: 0. Reconciliation passed with 0 fills. |
| v3 | 46 model calls completed; a 47th failed after the local server was stopped at the user's request to move inference to a remote GPU. `run_status=failed`, `evaluation_eligible=false`. | 1 source read, 1 ticket, 1 theme and 2 research-tool calls; 42 rejected actions. No questions, theses or fills. | Cash and terminal NAV: $100,000. Recorded reward: 0. Reconciliation passed with 0 fills. |

v2 completed the accounting episode and its artifact records
`run_status=completed` and `evaluation_eligible=true`. These flags mean the
harness could finish and score the attempt under its declared contract. The
research policy failed to make progress. In particular, cash-only accounting
does not establish that the model deliberately chose to hold cash.

## What failed and what changes next

**v1 — process launch.** The executable path was relative to the repository,
while model invocations use their own working directories. Resolving the
executable before starting the child process allowed v2 to reach actual local
inference. v1 remains an infrastructure failure, excluded from policy comparison.

**v2 — action interface.** The model returned parseable JSON with a nested
`triage` object. The environment requires a flat action with a literal `type`
field and arguments at the same level. The decoder therefore recorded zero
invalid-JSON outputs, while the environment rejected every action. Rejection
feedback did not recover this attempt before its budget ran out.

**v3 — explicit action syntax and interruption.** The observation contract now
included a concrete flat action example. The policy progressed through one
original and an accumulated theme. It also repeatedly routed an already-routed
source: 41 rejections were for immutable routing, and one rejected Mid
accumulation before reading the original. The local server was stopped when the
user requested remote GPU inference. The resulting transport failure is an
interrupted development run, not a completed policy comparison.

These iterations retained the account, data, research gates and reward while
changing execution instructions. Later attempts must preserve this history and
disclose changes to the harness, model and inference backend. A development
rerun is not a held-out policy comparison.

## Local model and runtime

| Setting | Recorded value |
| --- | --- |
| Hardware | Apple M5 Pro, 48 GB unified memory. |
| Configured policy | The same local `mlx-community/Qwen3-4B-4bit` checkpoint for Fast, Mid and Strong; v2 invoked only Fast and v3 reached Fast and Mid. |
| Immutable revision | `4dcb3d101c2a062e5c1d4bb173588c54ea6c4d25`. |
| Inference path | Codex CLI 0.163.0 through a local Responses bridge to MLX. The inference model is the pinned Qwen checkpoint. |
| Packages | MLX 0.32.4; mlx-lm 0.32.0; Transformers 5.17.0. |
| Decoding | Thinking disabled; temperature 0; seed 0; one concurrent request. |
| Limits | 24,000 input tokens and 2,048 output tokens per request; 96 model calls in the v2 attempt. |
| Action formatting | Prompted JSON, validated by the caller; no grammar-constrained decoder. |
| v2 reported usage | 259,603 input tokens; 12,725 output tokens; 0 reported cached-input or reasoning-output tokens. |
| Model verification | Local weight, tokenizer and template bytes checked against the manifest; serving identity checked for each invocation. |
| Training | None. These are inference attempts with a fixed checkpoint. |

The [checkpoint revision](https://huggingface.co/mlx-community/Qwen3-4B-4bit/commit/4dcb3d101c2a062e5c1d4bb173588c54ea6c4d25)
and [Qwen3 release record](https://qwenlm.github.io/blog/qwen3/) predate the replay
interval. The model's training-data cutoff is unknown. File verification and the
local serving identity establish reproducibility within this trusted local
setup; they do not provide remote attestation.

## Archive and financial scope

The frozen episode spans **October 23, 2026 at 00:00 UTC through October 26 at
20:00 UTC**, with **27 captured arrival versions across 10 source-native event
IDs** from NVIDIA and OpenAI. There are 26 distinct body values, and 18 arrival
versions share one Kazakhstan-related title. This is a small, duplicate-heavy
capture; distinct event IDs do not certify independent causal evidence.

The archive uses actual production receipt times and verifies content hashes.
Its contract records that the
interval and instrument-coverage selection were frozen before requesting price
values or examining returns.

The declared price coverage is NVDA, NET and UBER, selected from companies
explicitly named in captured titles. Instruments become discoverable when the
corresponding captured information arrives. This is limited dataset coverage;
there is no required position in any of these assets. Other companies can
remain research-only.

The account starts with $100,000, permits long-only fractional positions, caps
each asset at 50% target weight, and charges 10 basis points in fees plus
5 basis points of execution slippage. Orders fill at the next eligible open.
The scalar reward is terminal NAV divided by initial cash, minus one; no
research-activity bonus is added.

Prices were downloaded after the interval and are labelled
**historical reconstruction**. Provider revisions may be present; original
historical price vintages are not certified. Together with the unknown model
knowledge cutoff, this limits the result to a disclosed retrospective replay.
Strict point-in-time eligibility is false even though source-receipt and local
model-file checks passed.

Independent reconciliation confirms the cash-only arithmetic in the three local
attempts and the GPU episode. With zero fills, they do not exercise trading behavior.
The synthetic example and tests cover additional mechanics under their own
explicit contracts; their results are separate from model performance.

See the [evaluation card](EVALUATION.md) for the full contract, evidence matrix,
adversarial checks and requirements for a future policy comparison.
