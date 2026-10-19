# Codex environment-policy acceptance

Issue: https://github.com/ischencheng/Attention-Agent/issues/15

The runner now filters the initial environment for both its version probe and
model invocation. Fast explicitly selects `model`; Mid/Strong select `research`.
Metadata records admitted names rather than values. This change does not install
Docker Sandboxes or enforce a separate host/storage/network security boundary;
that work remains tracked in issue #16.

## Verification

- **24 adapter/trigger tests** and **32 gateway/scheduler tests** passed.
- Tests execute a fake CLI and child process with sentinel credentials and
  startup hooks. Unknown and disallowed variables are absent; intended settings
  survive; the version and execution environments match; metadata contains no
  sentinel secret values. This verifies the Python launch boundary, not every
  behavior of the native CLI's tool environment.
- A new production Fast Luna artifact completed under `model` and is linked to
  routing event **1372**. Its admitted names contain no DB, Kafka, source,
  embedding-gateway or unrelated application credentials. Existing saved Codex
  login continued to work.
- A real Mid Luna cycle completed under `research`, reviewed **20** selected
  tickets and saved durable state. Its start audit and actual run agree on the
  profile and model. The cycle recovered from shell/JSON construction errors;
  those attempts remain in its runtime trace.
- Existing Fast/embedding consumers stopped cleanly for the idle host-supervisor
  reload, then resumed with the same containers, topics and groups. No offsets,
  data volumes, source records, decision snapshots or evaluator state were reset.
- Independent agent code review found no blocking issue. This is not human review
  or a claim of external CI results.

The accompanying JSON contains the source/trace identifiers, actual model
metadata, admitted variable names and Mid completion evidence. It includes no
environment values, raw prompts, tool arguments or transcripts.

## Remaining boundary

A read-only role query still confirms the application's database account is a
superuser. The research profile intentionally preserves current DB/Kafka/tool
access so existing work continues. Agents can also encounter credentials through
readable files; helper validation and ledger hash links do not establish resource
isolation. See [execution boundaries](../SECURITY_BOUNDARIES.md) and issue #16.

No adversarial production write or sandbox migration was attempted. No new
Strong model cycle, improvement in judgment quality or aggregate cost reduction
is claimed by this change.
