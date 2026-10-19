# Attention Provenance Environment

A repository-level software engineering task built from AI Attention Proxy's
actual evidence pipeline. The coding agent repairs a concrete product defect:
an old decision must still resolve to its publication-time annotations and the
correct original messages after corrections, duplicate deliveries, and restarts.

- [TASK.md](TASK.md): complete agent-facing behavioral contract.
- [DESIGN.zh-CN.md](DESIGN.zh-CN.md): project rationale, founder sources, Polaris alignment.
- `prepare.py`: export the frozen **unsolved** repository, without Git history,
  reference implementation, or evaluator.
- `grade.py`, `cases.py`, `bridge.py`: externally run, seeded integration grader.
- `selfcheck.py`: reference, untouched baseline, and deliberately broken solutions.
- [VERIFICATION.md](VERIFICATION.md): measured results and their scope.

The current checkout contains the reference repair. Do not give it to a model
and call the result a blind attempt. The original task baseline is
`ed4d938ff89645fbeb1e58a10d66509425ede379`.

## Run locally

Python 3.12 is required by the bundled PostgreSQL wheel. On supported macOS and
Linux wheels, no system PostgreSQL, Docker, Kafka, API keys, or model API calls
are needed. Install dependencies in an isolated environment:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r environments/attention_provenance/requirements.txt

# Grade the reference repair in this checkout.
.venv/bin/python environments/attention_provenance/grade.py \
  --candidate "$PWD" --seed 1729 \
  --report environments/attention_provenance/runs/reference.json

# Check that meaningful defects are rejected.
.venv/bin/python environments/attention_provenance/selfcheck.py \
  --report-dir environments/attention_provenance/runs/selfcheck
```

Each run starts an isolated temporary PostgreSQL database, applies the frozen
deployed schema and candidate migration, then cleans up the server and database.
The migration is also reapplied twice with persisted evidence to verify upgrades.
The PostgreSQL helper also uses an OS runtime lock directory. Restricted execution
environments must allow local child processes/sockets and that lock directory.

## Run a coding-agent attempt

```bash
# Destination must not already exist; choose a fresh path for each attempt.
.venv/bin/python environments/attention_provenance/prepare.py /tmp/attention-attempt-01

# Give the agent only this exported directory and its TASK.md.
# After it finishes, run the evaluator from this original checkout:
.venv/bin/python environments/attention_provenance/grade.py \
  --candidate /tmp/attention-attempt-01 --seed 271828 \
  --report environments/attention_provenance/runs/attempt-01.json
```

Record model/version, attempt budget, elapsed time, tool calls, and token or
inference cost when available. Do not infer these from the grader runtime.
Use development feedback, then fix the evaluation seed/protocol before a
comparison. Seed variation changes fixtures; it does **not** create independent
tasks or a secret held-out test set. For actual held-out evaluation, keep the
evaluator and final cases in an operator-controlled environment.

## Scoring and trace

The grader checks actual `db_write.main`, `prepare_payload`, `db_update.run_update`
and the FastAPI route in fresh subprocesses. The database is real PostgreSQL.
Kafka's producer boundary is replaced with a capture function; the operator
persists the captured payload to replay the sink boundary. This does not test
the broker, sink consumer delivery guarantees, Flink, or model classification.

Scores are weighted fractions of passed outcome groups (0–100); **success
requires every group to pass**. Missing or crashing implementations fail their
group; no-op/refuse-all implementations fail positive cases. Reports include
group errors, commands and observed outputs, source hash, seed, runtime, and
grader hash/scope. `--case NAME` is development feedback only; `full_suite: false` must not
be presented as full acceptance.

The concurrency case deliberately changes an annotation and ticket after an
annotation read, testing that one snapshot never mixes versions. Transaction
row locks are also accepted. The hook recognizes the ordinary annotation SELECT
used by this repository; it is a diagnostic check, not exhaustive concurrency
model checking or a guarantee for every SQL implementation.

The task measures evidence integrity, not whether a model understood the cited
material, investment returns, or usefulness of a natural-language analysis.
The initial 90-minute attempt budget is uncalibrated. There is no claim yet of
frontier-model difficulty, training uplift, or broad long-horizon coverage.

## Trust boundary

The candidate does not control the scorer or expected fixtures. Subprocess
separation prevents ordinary module-global interference, and the bridge strips
unneeded environment variables. **This local harness is not a security sandbox:**
candidate code still runs under the operator's OS account and uses a test DB
owner. Only run trusted/local solutions here. Evaluating hostile submissions
requires a separate container or VM with restricted mounts/network/resources,
an operator-owned scorer, and an independent DB role. We have not validated
resistance to deliberate grader tampering or RL reward hacking.

## Use the repair in the application

Fresh Compose volumes run `postgres/init/002_evidence.sql` automatically. For an
existing database, apply the additive migration explicitly:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U bluesky -d bluesky \
  < postgres/init/002_evidence.sql
docker compose up -d --build backend-fastapi
```

The API image includes the shared `skills/evidence.py`; build it from the repo
root with `docker build -f backend-fastapi/Dockerfile .`. The existing agent
containers already mount `skills/` read-only. New decisions
published through the write skill carry snapshots; older decisions remain
`legacy_unverified`. Retrieve evidence at `/api/decisions/<id>/evidence`.
Raw JSON values become immutable to ordinary UPDATE/DELETE; PostgreSQL
superusers can still disable triggers. No existing data are rewritten.
