"""Research rules shared by the streaming pipeline and isolated replay adapters."""

FAST_RULES = (
    "This is the single Fast model filter, before Mid curation and Strong judgment. "
    "No upstream model has screened this observation. Check both relevance to the research topic scope "
    "and whether it contains concrete, potentially useful information. Reject clearly unrelated content "
    "and content with no concrete potentially relevant information. Keep concrete observations whose "
    "relevance is uncertain but plausible, with a brief reason that states the uncertainty. "
    "For the investment research use case, flag potentially material "
    "facts about competitive advantages, business/credit/technology cycles, investment spending, demand, "
    "margins, valuation or risk. A single source need not establish a thesis: Mid checks independence and "
    "Strong checks causality later. "
    "do not claim novelty or corroboration unavailable in this single observation. "
)

MID_RULES = (
    "Organize unannotated attention tickets into pending themes. Deduplicate by independent causal event. "
    "Publish fact-only annotations only for 3+ independent signals from 2+ sources, or an explicit "
    "authoritative-hard-data exception. Keep unresolved themes in state. Do not perform investment analysis."
)

STRONG_RULES = (
    "Review both qualifying multi-signal annotations and authoritative-hard-data exceptions. "
    "Continue existing theses across cycles, record supporting and contradicting evidence, state invalidation "
    "conditions and what changed. Carry out the mandatory MrBrain framework/checklist, price-history and "
    "semantic contradiction checks when assessing an investment action. Risk gates may block new exposure, "
    "but must never block recording a thesis invalidation, correction or risk reduction. "
    "If no new supported decision is warranted, preserve state without duplicate publication."
)


def role_rules(role):
    return {"fast": FAST_RULES, "mid": MID_RULES, "strong": STRONG_RULES}[role]
