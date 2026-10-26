"""Fixed synthetic workflow for regression testing, never a performance baseline."""


def demo_actions():
    actions = []
    retained = {}
    facts = {
        "capex": "The issuer reports 30% backlog growth and net cash; conversion takes two years.",
        "permits": "The regulator approved permits; grid connection dates remain separate.",
        "supplier": "Interviewed suppliers report long lead times without confirmed Lanyu orders.",
        "delay": "The issuer moves ten percent of deliveries into next year after a grid delay.",
        "storage": "Harbor reports positive free cash flow and rising warranty claims.",
        "warranty": "One customer reports early battery replacement without a fleet-wide rate.",
    }

    def add(role, kind, **fields):
        actions.append({"role": role, "type": kind, **fields})

    def tool(role, name, args, question=None):
        fields = {"tool": name, "args": args}
        if question:
            fields["question_id"] = question
        add(role, "tool", **fields)

    def intake(ids, theme, question):
        for sid in ids:
            tool("fast", "read", {"source_id": sid})
            add("fast", "triage", source_id=sid, disposition="advance",
                reason="An original source changes the infrastructure research question.")
            tool("mid", "read", {"source_id": sid})
        retained.setdefault(theme, []).extend(ids)
        add("mid", "accumulate", theme_id=theme,
            ticket_ids=[f"ticket-{sid}" for sid in ids],
            summary=question,
            facts=[{"source_id": sid, "claim": facts[sid]} for sid in retained[theme]],
            open_questions=[question], authoritative_exception=True,
            exception_reason="The bundle includes an official issuer hard-data update.")

    intake(["capex", "permits", "supplier"], "power", "Will equipment backlog become cash flow?")
    add("fast", "triage", source_id="sports", disposition="ignore",
        reason="Football scheduling is outside the configured research topics.")
    add("strong", "open_research", question_id="power-cash", theme_id="power",
        question="Can Lanyu convert the growing backlog without excessive delivery risk?")
    for sid in ("capex", "permits", "supplier"):
        tool("strong", "read", {"source_id": sid}, "power-cash")
    tool("strong", "search", {"query": "Lanyu", "intent": "contradiction"}, "power-cash")
    tool("strong", "discover", {"query": "Lanyu"}, "power-cash")
    tool("strong", "price_history", {"asset_id": "lyps"}, "power-cash")
    tool("strong", "framework", {"asset_id": "lyps"}, "power-cash")
    analysis = {
        "traversability": "A cash-only 40% position leaves substantial cash even after total loss.",
        "barbell_position": "Bounded speculative exposure supported by a cash reserve.",
        "moat": "Lead times suggest constraints; durable pricing power remains uncertain.",
        "cycle_position": "Equipment expansion may precede realized cash receipts.",
        "prohibitions_check": "No borrowing or momentum-only rationale; reconsider on delays.",
        "technical_analysis": "Only one completed synthetic bar exists; trend evidence is weak.",
    }
    checks = {name: {"status": "PASS", "evidence_ids": ["capex"],
                     "reason": reason} for name, reason in {
        "leverage": "The simulated cash account cannot borrow.",
        "understanding": "Equipment backlog, cash conversion and delivery uncertainty are identified.",
        "bankruptcy": "Reported net cash plus a 60% account cash reserve limits exposure.",
        "momentum_chasing": "The hypothesis concerns backlog conversion rather than recent price rises.",
        "never_sell": "Delivery delays invalidate the near-term conversion assumption.",
    }.items()}
    add("strong", "judge", thesis_id="power-thesis", question_id="power-cash",
        claim="A limited paper position can express backlog conversion, subject to delivery risk.",
        evidence_ids=["capex", "supplier"], counterevidence_ids=["permits"],
        uncertainty="Backlog is not revenue and grid timing remains uncertain.",
        invalidation_conditions=["A material delivery delay or loss of deposits."],
        status="investable", asset_ids=["lyps"], framework_analysis=analysis,
        risk_checks=checks)
    add("strong", "allocate", target_weights={"lyps": 0.4},
        thesis_ids=["power-thesis"], reason="Bound exposure to the researched delivery thesis.")
    add("strong", "wait", reason="Continue source intake while the order and research develop.")
    intake(["delay"], "delivery", "Does the delivery delay invalidate near-term conversion?")
    tool("strong", "read", {"source_id": "delay"}, "power-cash")
    add("strong", "judge", thesis_id="power-thesis", question_id="power-cash",
        claim="The official delay weakens the near-term thesis; reduce exposure.",
        evidence_ids=["delay", "capex"], counterevidence_ids=["permits"],
        uncertainty="The longer-term backlog may still convert.",
        invalidation_conditions=["A verified recovery in delivery timing."],
        status="invalidated", asset_ids=["lyps"], what_changed="A six-month connection delay.")
    add("strong", "allocate", target_weights={"lyps": 0.1}, thesis_ids=["power-thesis"],
        reason="Reduce exposure after the evidence changes; retain some optionality.")
    add("strong", "wait", reason="Keep monitoring both existing and unrelated topics.")
    intake(["storage"], "storage", "Are storage margins durable as warranty claims rise?")
    add("strong", "open_research", question_id="storage-margin", theme_id="storage",
        question="Does Harbor storage growth compensate for warranty risk?")
    tool("strong", "read", {"source_id": "storage"}, "storage-margin")
    tool("strong", "discover", {"query": "Harbor"}, "storage-margin")
    add("strong", "judge", thesis_id="storage-thesis", question_id="storage-margin",
        claim="Storage is worth researching; current warranty evidence does not justify exposure.",
        evidence_ids=["storage"], counterevidence_ids=[],
        uncertainty="Fleet-wide warranty costs and durable margins are unknown.",
        invalidation_conditions=["Verified warranty costs overwhelm incremental margins."],
        status="research", asset_ids=[])
    add("strong", "wait", reason="Retain the research question while awaiting customer evidence.")
    intake(["warranty"], "storage", "How representative is the customer warranty report?")
    tool("strong", "read", {"source_id": "warranty"}, "storage-margin")
    add("strong", "judge", thesis_id="storage-thesis", question_id="storage-margin",
        claim="The customer report adds uncertainty; keep the question open without allocating.",
        evidence_ids=["storage"], counterevidence_ids=["warranty"],
        uncertainty="One customer cannot establish a fleet-wide defect rate.",
        invalidation_conditions=["A verified fleet-wide failure assessment."],
        status="research", asset_ids=[], what_changed="New customer counterevidence arrived.")
    add("strong", "wait", reason="Reach the fixed evaluation horizon with existing holdings.")
    return actions
