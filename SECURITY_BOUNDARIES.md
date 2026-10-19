# Agent execution boundaries

The local pipeline uses ordinary Docker Compose for services and a host Codex
process for Mid/Strong. It does not currently deploy Docker Sandboxes. The
classifier runs with tools disabled and a read-only Codex sandbox; research
cycles use a workspace-write Codex sandbox with network access for their tools.

This document distinguishes implemented controls from resource isolation still
tracked in [issue #16](https://github.com/ischencheng/Attention-Agent/issues/16).

## Explicit child environments

`codex_runner.run` constructs the Codex process environment from named profiles,
instead of copying every parent variable. The same policy covers both the CLI
version probe and the model invocation.

- `model` is the default. It admits the explicitly enumerated platform, login,
  proxy and TLS settings required to execute the CLI. Fast selects this profile
  explicitly and cannot combine tool-free classification with the research
  profile. Database, Kafka, source-ingestion and embedding gateway credentials
  are not included.
- `research` adds the current research tools' database, Kafka, semantic-recall
  and SEC/FRED settings. Mid/Strong select it explicitly, including the role-based
  CLI entry point. This preserves their existing research operations.

The source allowlists in `agent/codex_runner.py` are authoritative. Unknown
variables, SSH-agent forwarding and executable-loader settings are not passed
implicitly. Empty optional model API keys are omitted so saved CLI login can
still work. Run metadata records the profile and admitted variable names, never
their values. Cycle start audits record the selected research profile.

This limits implicit environment exposure. It is not a file-access boundary:
an agent with readable host files may find configuration elsewhere. It also does
not remove the privileges of credentials deliberately admitted for research.

## Existing resource permissions

Read-only inspection on October 19, 2026 found that the local application's
PostgreSQL role owns the application tables and has superuser privileges.
`db_query.py` uses read-only transactions and normal write helpers validate
payloads, but the credentials do not prevent bypassing those helpers. The
`agent_state.py` interface accepts a caller-supplied state ID without a separate
server-side role binding.

The local Kafka listeners use PLAINTEXT without an agent-specific authorization
boundary. Direct publication can bypass `db_write.py` validation. Administrative
connection defaults are also present in project files. Merely passing a different
database username does not close these alternate paths while the files and
services remain accessible.

The evaluator independently verifies and freezes evidence, and its ledger detects
changes through hash links. These are provenance controls, not proof of separate
filesystem permissions. No unauthorized production mutation or exploit was
attempted during the audit.

## Planned enforced boundary

The follow-up design needs an authenticated research-tools service that derives
the role and permitted state ID on the trusted side, validates publications, and
provides bounded evidence reads. Agent environments must have no direct DB/Kafka
route, administrative credentials, host Docker socket or evaluator-file access.
Use distinct non-owner service database roles as an additional control. Keep
source evidence, durable memory, audit history and evaluator state outside
replaceable agent workspaces.

Docker Sandboxes is one candidate execution environment. Its official
[security model](https://docs.docker.com/ai/sandboxes/security/) describes microVM
isolation, with explicitly shared workspaces, integrations and allowed network
channels remaining consequential. Choosing it would not replace application
authorization at the research-tools boundary. No sandbox installation or
migration is part of the environment-profile change.

Acceptance must show both permitted research flows and denied operations:
cross-role state writes, invalid publications, direct storage access and evaluator
modification. Test denial in isolated fixtures; preserve production offsets and
history during migration. Audit role/run identity and operation outcomes without
secret values. Durable state must survive replacement of the agent environment.
Independent long-term evaluation must still establish judgment quality.
