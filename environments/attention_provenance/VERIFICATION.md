# Verification — 2026-10-19

Machine-readable evidence: [validation.json](validation.json).

| Run | Result |
| --- | --- |
| Reference repair, seed 1729 | 100/100, all 10 groups passed |
| Reference repair, seed 271828 | 100/100, all 10 groups passed |
| Unmodified baseline | 0/100, all 10 groups failed |
| No generated snapshot | Rejected by original-preservation group |
| Fail open on validation/DB failure | Rejected by publication group |
| Always report verified | Rejected by API validation group |
| READ COMMITTED mixed-version snapshot | Rejected by consistency group |
| Drop original decision fields | Rejected by original-preservation group |
| Non-idempotent migration | Rejected by existing-data migration group |

Mutants run their named group, not the full suite. These are reference and
seeded-defect checks; they are not independent model attempts. Full synthetic
request/response traces are retained locally under `runs/` and can be reproduced
with the README commands. The compact record includes candidate-source and
frozen-grader digests; Git HEAD during testing was the baseline with the repair
applied to the working tree.

The final grader also checks ordinary decision-field updates remain usable,
annotation-object appends, missing and malformed references, source collisions,
revisions and duplicate delivery, legacy responses, SQL mutation rejection,
persisted evidence across fresh application processes, and migration reapplication.

Execution used Python 3.12.13 and PostgreSQL 16.2 on Apple Silicon. The original
pgvector schema was used. Kafka publication was captured, then persisted by the
evaluator; no live broker, sink-consumer reliability, Flink, external LLM or
market API was exercised. No Docker daemon was installed, so Dockerfile source
paths were checked but the image build was not run. Python compilation and
`git diff --check` passed.

The task and scorer were reviewed separately, revealing and closing three
false-pass gaps: upgrade compatibility, preservation of decision fields, and
reject-all update behavior. This was agent-assisted review, not human approval.
The local subprocess harness is not a hostile-code sandbox. Model difficulty,
held-out generalization, user utility, and RL training improvement remain
unmeasured.
