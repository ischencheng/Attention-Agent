# Trace explorer acceptance: October 19, 2026

Issue #8 makes the actual research path inspectable in the local dashboard. The
accompanying JSON captures verified identifiers, links, hash checks and real
agent-cycle metadata. Original records and credentials remain in local storage.

## Acceptance

| Requirement | Evidence |
| --- | --- |
| Follow a continuing judgment back to its sources | The live Fed thesis links to raw record 3, ticket 5 and annotation 1 using recorded references. |
| Explain a discarded signal | Raw record 318 passed the relevance filter and was rejected by Fast. Routing events 1 and 2 share the original hash and exact Kafka handoff coordinates. Their original reasons are visible. |
| Preserve judgment history | The current revision and two independently frozen state versions are visible; both frozen entries pass integrity verification. |
| Observe actual execution | Two Mid attempts and one Strong attempt completed through Codex with verified state saves. The Strong attempt found no new information and published no duplicate decision. |
| Read archived state safely | A standalone SQLite backup works through the API's read-only Docker mount with the writer closed and no WAL sidecars. Capture time and staleness are exposed. |
| Keep absent telemetry explicit | Missing historical routing and per-tool timestamps remain unavailable. Imported latest summaries are labeled separately from recorded attempts. |
| Make outcomes honest | Continuing research is unscored. No production investment decision or resolved forecast was present at capture. |

The runtime suite passed 88 tests, the frontend semantic suite passed seven, and
both API and frontend container builds passed. The runtime suite covers interrupted
attempts, retries, routing linkage, private-field redaction and projection failure
preservation. The frontend suite covers stage semantics and source-time distinctions.
A browser check exercised the deployed UI, including the actual rejection reasons,
publication gate, research gaps, revisions, archive hashes and cycle tool metadata.
An absent original returns HTTP 404 rather than a fabricated graph.

This is a feature and integrity change, not a model or GPU performance claim.
Framework kernel, numerical-model and hardware-specific skills do not apply.
The comparison is against the prior dashboard, which showed the pipeline status
but did not expose these auditable paths. No throughput or forecast-accuracy
improvement has been measured.

See [the design mapping](../TRACE_DESIGN.md) for the selected AI Agent Book sections
and the project's resulting contracts. The pipeline continues on the local Mac;
the host and Docker must remain available. Snapshot polling cannot recover states
overwritten between observations.
