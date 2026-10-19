"""Structured judgment contract; semantic constraints are checked separately."""

def obj(properties):
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}

def arr(items):
    return {"type": "array", "items": items}

STRING = {"type": "string"}
STRINGS = arr(STRING)
FORECAST = obj({
    "symbol": STRING,
    "horizon": {"type": "string", "enum": ["day", "week", "month"]},
    "p_outperform_spy": {"type": "number", "minimum": 0, "maximum": 1},
    "reason": STRING,
})
DISCOVERY = obj({
    "thesis_id": STRING,
    "title": STRING,
    "topics": STRINGS,
    "entities": arr(obj({"name": STRING, "symbol": {"type": ["string", "null"]}})),
    "statement": STRING,
    "thesis_horizon": STRING,
    "uncertainty": STRING,
    "next_questions": STRINGS,
    "status": {"type": "string", "enum": ["active", "weakened", "invalidated", "unknown"]},
    "action": {"type": "string", "enum": ["research", "watch", "abstain", "positive", "negative"]},
    "evidence_ids": STRINGS,
    "counterevidence_ids": STRINGS,
    "observed_event_ids": STRINGS,
    "framework_dimensions": STRINGS,
    "change_reason": STRING,
    "invalidation": STRING,
    "next_review_at": STRING,
    "forecasts": arr(FORECAST),
})
SCHEMA = obj({"discoveries": arr(DISCOVERY), "no_action_reason": STRING})
