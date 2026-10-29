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
    parser.add_argument("--archive", type=Path, help="Validated archive directory for pinned-model historical replay")
    parser.add_argument("--model-endpoint", help="Local pinned Responses bridge: http://127.0.0.1:<port>/v1")
    parser.add_argument("--strict-replay", action="store_true", help="Use original reads and counterevidence review, without search")
    parser.add_argument("--scheduled", action="store_true", help="Replay independent 30/40-minute role schedules")
    parser.add_argument("--actions", type=Path, help="JSON array of action objects for replay")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", help="Override all roles; default uses the production role models")
    parser.add_argument("--codex-bin", default="codex", help="Path to a Codex 0.163-compatible executable")
    parser.add_argument("--max-calls", type=int, default=24)
    parser.add_argument("--timeout", type=float, default=120, help="Seconds per model invocation")
    parser.add_argument("--max-steps", type=int, default=512)
    parser.add_argument("--actions-per-cutoff", type=int, default=64)
    args = parser.parse_args(argv)
    if args.command == "demo" and (args.scenario or args.actions or args.archive or args.strict_replay or args.scheduled):
        parser.error("demo uses its frozen synthetic scenario and action transcript")
    if args.command == "replay" and (args.actions is None or args.scenario is None):
        parser.error("replay requires --scenario and --actions")
    if args.command == "codex" and args.actions:
        parser.error("codex selects its own actions")
    if args.archive and (args.command != "codex" or args.scenario or args.model or not args.model_endpoint):
        parser.error("--archive requires codex and --model-endpoint; its scenario and model are frozen")
    if args.command != "codex" and args.model_endpoint:
        parser.error("--model-endpoint requires codex")
    scenario = args.scenario or Path(__file__).parent / "fixtures" / "demo.json"
    actions = demo_actions() if args.command == "demo" else (
        json.loads(args.actions.read_text()) if args.actions else None)
    try:
        if args.archive:
            from .experiment import run_archived
            summary = run_archived(args.archive, args.output, model_endpoint=args.model_endpoint,
                                  codex_bin=args.codex_bin, max_calls=args.max_calls, timeout=args.timeout,
                                  max_steps=args.max_steps, actions_per_cutoff=args.actions_per_cutoff)
        else:
            summary = run_episode(scenario, args.output, actions=actions, codex=args.command == "codex",
                              model=args.model, max_calls=args.max_calls, timeout=args.timeout,
                              codex_bin=args.codex_bin,
                              max_steps=args.max_steps, actions_per_cutoff=args.actions_per_cutoff,
                              strict_replay=args.strict_replay, scheduled=args.scheduled,
                              model_endpoint=args.model_endpoint)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"{type(exc).__name__}: {exc}\n")
    print(json.dumps(summary, ensure_ascii=False, allow_nan=False))
    return 1 if summary["run_status"] == "failed" else 0
