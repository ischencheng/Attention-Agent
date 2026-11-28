"""Read optional user-defined investment principles as research guidance."""
from __future__ import annotations

import json
import os
from pathlib import Path

TEMPLATE_PATH = Path(__file__).with_name("template.json")
LOCAL_PATH = TEMPLATE_PATH.parents[1] / "investment_principles.json"
GROUPS = (
    "thesis_and_horizon",
    "evidence_and_counterevidence",
    "valuation_and_alternatives",
    "risk_and_position_size",
    "review_and_exit",
)


def _read(path: Path) -> dict[str, list[str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"{path.name} must contain valid UTF-8 JSON") from exc
    if not isinstance(data, dict) or set(data) != set(GROUPS):
        raise ValueError(f"{path.name} must contain these fields: {', '.join(GROUPS)}")
    for group in GROUPS:
        principles = data[group]
        if not isinstance(principles, list) or any(
            not isinstance(item, str) or not item.strip() for item in principles
        ):
            raise ValueError(f"{path.name}: {group} must be an array of nonempty strings")
    return data


def load_principles() -> dict:
    """Return a fresh snapshot of the local principles or the blank template."""
    override = os.environ.get("ATTENTION_INVESTMENT_PRINCIPLES")
    try:
        framework = _read(Path(override).expanduser() if override else LOCAL_PATH)
    except FileNotFoundError:
        if override:
            raise
        return {"kind": "investment_principles_v1", "source": "template",
                "framework": _read(TEMPLATE_PATH)}
    return {"kind": "investment_principles_v1", "source": "local", "framework": framework}


if __name__ == "__main__":
    try:
        print(json.dumps(load_principles(), ensure_ascii=False))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Investment principles: {exc}") from exc
