"""Disclosed hand-written demonstration policy; no training or private labels.

The rules below illustrate evidence accumulation on the fictional demo. They
consume only the observation returned by reset/step, never an environment,
manifest, resolution file, or future observation. This is not an investment
analyst or a general financial benchmark baseline.
"""
from __future__ import annotations

from datetime import datetime, timedelta


def demo_action(observation: dict) -> dict:
    """Read originals, revise one thesis on new events, forecast once, then wait."""
    read = observation["read_observations"]
    read_ids = {item["id"] for item in read}
    for source in observation["available_sources"]:
        if source["id"] not in read_ids:
            return {"type": "read", "observation_id": source["id"]}

    # Syndicated copies have the same causal event ID. The demonstration keeps
    # the original rather than treating another URL as corroboration.
    events = {}
    for item in read:
        events.setdefault(item["event_id"], item)
    originals = list(events.values())
    thesis_id = "lanyu-capacity-contract"
    previous = observation["theses"].get(thesis_id)
    prior = previous["discovery"] if previous else None
    prior_events = set(prior["observed_event_ids"]) if prior else set()
    if originals and set(events) != prior_events:
        kinds = {item["raw"].get("event_type") for item in originals}
        cancellation = "cancellation" in kinds
        correction = "correction" in kinds
        acceptance = "acceptance" in kinds
        if acceptance:
            statement = "Signed customer acceptance supports the delivery milestone; margins and cash recovery remain unverified."
            status, action = "active", "watch"
        elif correction:
            statement = "The minimum purchase contract remains valid, but acceptance is delayed and committed capex remains at risk."
            status, action = "unknown", "research"
        elif cancellation:
            statement = "The reported customer cancellation weakens the expansion case while committed capex remains exposed."
            status, action = "weakened", "research"
        else:
            statement = "The proposed capacity expansion has a customer commitment, but acceptance, economics and financing require verification."
            status, action = "unknown", "research"
        contrary = [item["id"] for item in originals
                    if item["raw"].get("event_type") in {"cancellation", "financing", "missing-disclosure"}]
        supporting = [item["id"] for item in originals if item["id"] not in contrary]
        next_review = (datetime.fromisoformat(observation["current_time"].replace("Z", "+00:00"))
                       + timedelta(days=1)).isoformat()
        return {"type": "judge", "judgment": {"discoveries": [{
            "thesis_id": thesis_id,
            "title": "Lanyu expansion: customer commitment and cash recovery",
            "topics": ["semiconductor", "datacenter", "capex", "margin"],
            "entities": [{"name": "Lanyu Packaging Lab", "symbol": None}],
            "statement": statement,
            "thesis_horizon": "Twelve months of delivery and cash recovery",
            "uncertainty": "Company disclosures do not establish durable economics or independent verification.",
            "next_questions": ["What gross margin and cash collections does the new line realize?"],
            "status": status, "action": action,
            "evidence_ids": supporting, "counterevidence_ids": contrary,
            "observed_event_ids": list(events),
            "framework_dimensions": ["Operating execution", "Cash-flow survivability"],
            "change_reason": "New original evidence updates the same thesis. " + (
                "The correction resolves the permanent-cancellation claim but retains the original announcement and delay risk."
                if correction else "Unverified conditions and conflicting disclosures stay explicit."),
            "invalidation": "A valid cancellation, failed acceptance, or inability to finance committed capex would undermine the expansion case.",
            "next_review_at": next_review,
            "forecasts": [],
        }], "no_action_reason": ""}}

    forecasted = {item["question_id"] for item in observation["forecasts"]}
    now = datetime.fromisoformat(observation["current_time"].replace("Z", "+00:00"))
    for question in observation["questions"]:
        deadline = datetime.fromisoformat(question["deadline"].replace("Z", "+00:00"))
        if prior and question["question_id"] not in forecasted and now < deadline:
            metric = question["criterion"]["metric"]
            # These fixed, disclosed probabilities are heuristic choices made
            # from the public question, not optimized against demo outcomes.
            probability = 0.65 if metric == "customer_acceptance" else 0.5
            return {"type": "forecast", "thesis_id": thesis_id,
                    "question_id": question["question_id"], "probability": probability,
                    "evidence_ids": prior["evidence_ids"] + prior["counterevidence_ids"],
                    "reason": "Hand-written demo prior: a signed commitment gives limited support to acceptance; undisclosed economics remain uncertain."}
    return {"type": "wait", "reason": "No unread original or new causal event remains; wait for the next evidence cutoff."}
