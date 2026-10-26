"""Run and inspect isolated research-to-portfolio episodes."""
import argparse
import json
from pathlib import Path

from .demo import demo_actions
from .runner import run_episode


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["demo", "replay", "codex"])
    parser.add_argument("--scenario", type=Path)
    parser.add_argument("--actions", type=Path, help="JSON array of action objects for replay")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", help="Override all roles; default uses the production role models")
    parser.add_argument("--codex-bin", default="codex", help="Path to a Codex 0.163-compatible executable")
    parser.add_argument("--max-calls", type=int, default=24)
    parser.add_argument("--timeout", type=float, default=120, help="Seconds per model invocation")
    parser.add_argument("--max-steps", type=int, default=512)
    parser.add_argument("--actions-per-cutoff", type=int, default=64)
    args = parser.parse_args(argv)
    if args.command == "demo" and (args.scenario or args.actions):
        parser.error("demo uses its frozen synthetic scenario and action transcript")
    if args.command == "replay" and (args.actions is None or args.scenario is None):
        parser.error("replay requires --scenario and --actions")
    if args.command == "codex" and args.actions:
        parser.error("codex selects its own actions")
    scenario = args.scenario or Path(__file__).parent / "fixtures" / "demo.json"
    actions = demo_actions() if args.command == "demo" else (
        json.loads(args.actions.read_text()) if args.actions else None)
    try:
        summary = run_episode(scenario, args.output, actions=actions, codex=args.command == "codex",
                              model=args.model, max_calls=args.max_calls, timeout=args.timeout,
                              codex_bin=args.codex_bin,
                              max_steps=args.max_steps, actions_per_cutoff=args.actions_per_cutoff)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"{type(exc).__name__}: {exc}\n")
    print(json.dumps(summary, ensure_ascii=False, allow_nan=False))
    return 1 if summary["run_status"] == "failed" else 0
