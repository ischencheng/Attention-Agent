"""Shared pure judgment validation for live and offline research callers."""
import re

if __package__:
    from .ledger import timestamp
    from .schema import SCHEMA
else:  # Existing direct-script entry points.
    from ledger import timestamp
    from schema import SCHEMA


def validate_final(final, observations, previous, as_of):
    from jsonschema import validate
    validate(final, SCHEMA)
    visible = {o["id"]: o for o in observations if timestamp(o["available_at"]) <= as_of and timestamp(o["published_at"]) <= as_of}
    if not final["discoveries"] and not final["no_action_reason"].strip():
        raise ValueError("empty discovery batch requires a reason")
    ids = set()
    for thesis in final["discoveries"]:
        key = thesis["thesis_id"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,99}", key) or key in ids:
            raise ValueError("thesis IDs must be unique stable lowercase identifiers")
        ids.add(key)
        for field in ("title", "statement", "thesis_horizon", "uncertainty", "change_reason", "invalidation"):
            if not thesis[field].strip():
                raise ValueError(f"missing {field}")
        refs = thesis["evidence_ids"] + thesis["counterevidence_ids"]
        if not refs or not set(refs) <= visible.keys():
            raise ValueError("every thesis needs visible, existing original evidence")
        # References identify entire documents, which may contain both a
        # supporting fact and a limiting/contradictory fact. Count each source
        # event once below; overlap is not independent corroboration.
        events = {visible[r]["event_id"] for r in refs}
        if set(thesis["observed_event_ids"]) != events or len(thesis["observed_event_ids"]) != len(events):
            raise ValueError("source event IDs must exactly deduplicate referenced observations")
        if timestamp(thesis["next_review_at"]) <= as_of:
            raise ValueError("next review must be in the future")
        if key in previous:
            prior = previous[key]["discovery"]
            old_refs = set(prior["evidence_ids"] + prior["counterevidence_ids"])
            if not set(prior["counterevidence_ids"]) <= set(refs):
                raise ValueError("retain prior counterevidence in each revision; explain any resolution")
            if not set(refs) - old_refs:
                raise ValueError("do not publish duplicate thesis revisions without new evidence")
        symbols = {e["symbol"] for e in thesis["entities"] if e["symbol"] is not None}
        if any(not re.fullmatch(r"[A-Z][A-Z0-9.\-^]{0,14}", s) for s in symbols):
            raise ValueError("unsupported symbol format")
        seen_forecasts = set()
        for f in thesis["forecasts"]:
            fk = (f["symbol"], f["horizon"])
            if f["symbol"] not in symbols or f["symbol"] == "SPY" or fk in seen_forecasts:
                raise ValueError("forecasts must identify a unique discovered symbol/horizon other than benchmark")
            seen_forecasts.add(fk)
    return final

