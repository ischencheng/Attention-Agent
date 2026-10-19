# Delegated information consumption

Attention Proxy lets AI consume incoming information on a user's behalf. Its
purpose is to filter noise, accumulate useful context and release human attention.
It provides evidence that downstream agents can inspect and use for their own
goals. Investment research is the current application of this infrastructure.

## Responsibilities

Fast identifies material worth further attention. It retains original references
and the reason for advancing or rejecting an item. Mid accumulates events across
time and sources, resolves context where available, merges repeated coverage and
retains open questions. Strong applies an explicit decision framework and tools
to the accumulated evidence. Insufficient evidence can warrant waiting or further
research without producing an insight, prediction or action.

Original records remain the source of truth. Summaries and vector matches help
an agent locate evidence; their references must lead back to the original record
and its source context. Repeated coverage does not become independent evidence.
Accumulation should reduce repeated reading and token expenditure without losing
the ability to inspect a claim. These efficiency gains need measurement.

The running contracts retain their current names: attention tickets hold routed
clues, annotations hold accumulated factual evidence, and decision packets hold
published judgments. Their function matters more than renaming the objects.

## Inspectability

A reader should be able to walk backward from a judgment through its cited facts
and clues to the original messages. Recorded native reply references can recover
locally collected surrounding messages. Missing parents, unsupported source
formats and bounded retrieval must be visible; an incomplete local collection
cannot establish the full platform conversation.

Source context and decision evidence have different lifetimes. Context retrieved
today may contain records collected after a decision. Displaying it must not add
it to that decision's frozen evidence or imply that an agent read it earlier.
Native event time, collection time and decision time remain separate.

Runtime tracing answers which process or tool executed, when, and with what
outcome. The business evidence path answers which observations support a claim
and how the judgment changed. Infrastructure health, evidence integrity and
decision quality are separate measurements.

## Scope of the current implementation

The pipeline runs topic-led filtering, Mid accumulation and Strong research on
the configured sources. The dashboard exposes source records, recorded routing
reasons, factual annotations, continuing judgments, frozen versions and agent
cycles. Financial evaluation applies only to eligible, explicitly declared
forecasts when their observation windows close.

Source-context support is bounded by the native references actually preserved
in each provider's originals. It is not a general platform crawler, a guarantee
of complete chat history, or proof that all retrieved context influenced a
judgment. See [the trace contract](TRACE_DESIGN.md) and the acceptance records in
`validation/` for implemented coverage and measured limitations.
