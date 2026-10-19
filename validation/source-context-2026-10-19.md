# Source-context acceptance: October 19, 2026

Issue #11 closes the gap between an inspectable original message and its recorded
surrounding conversation. The existing raw provider payloads supply native reply
references; the lookup does not infer relationships from similar text, authors
or nearby timestamps.

## Verified behavior

- The deployed API and agent CLI resolve original **42** to collected parent
  **420**, with the same native reply edge and authoritative original hashes.
- Original **25** explicitly reports its referenced parent as not collected.
- A one-record limit returns the anchor and marks the remaining context as
  truncated. An unknown original returns HTTP 404.
- The browser's original-record lookup and “Read recorded conversation” action
  display both the resolved-parent and missing-parent cases. Native publication
  time and collection time are separate. Parent/child record links and original
  source URLs are available.
- Current context is labeled separately from frozen decision evidence. A native
  message reference does not identify which historical content version was
  visible when the reply was written.
- All 16 Docker services and the host supervisor remained running after deployment.

The integrated runtime suite passed **102 tests**, including **14 source-context
tests** with a temporary PostgreSQL server. Coverage includes source collisions,
native/envelope identity mismatches, multiple stored versions, cycles, bounded
traversal, native timestamps, malformed provider fields, CLI parity and HTTP
display redaction. The frontend's **eight semantic tests** and production build
passed. Both deployed container images built successfully. The new tool name is
also recognized by the safe cycle-event projection without exposing arguments or
command output.

Acceptance used read-only queries against already collected production originals.
No synthetic source records or model judgments were inserted for the demo.
The accompanying JSON records the observed IDs, edges, hashes and limits.

## Scope and limits

The current adapter follows locally collected Twitter reply ancestors only. It
does not fetch every reply, reconstruct a complete thread, or retrieve chat time
windows. Sources without supported native identities remain explicitly
unsupported. Display redaction and size limits do not alter stored originals or
their hashes.

This supports the product's delegated information-consumption contract. Investment
research remains a downstream application. Kernel, model-numerical and GPU
performance skills do not apply to this read-only source-context feature. No
token-efficiency, throughput or decision-quality improvement is claimed.
