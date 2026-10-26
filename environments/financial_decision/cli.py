"""Offline demo and external-action replay; no model, network, or production DB."""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys

from .baseline import demo_action
from .env import FinancialDecisionEnv
from .trace_view import render_trace


def _json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"


def _read_actions(path):
    def reject_constant(value):
        raise ValueError("Non-finite JSON number: " + value)
    actions = []
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip():
            continue
        action = json.loads(line, parse_constant=reject_constant)
        if not isinstance(action, dict):
            raise ValueError(f"Action at line {number} must be a JSON object")
        actions.append(action)
    return actions


def run_episode(scenario, output, *, actions=None, seed=0, max_steps=128):
    """Persist actual returned transitions; an unfinished transcript stays so."""
    env = FinancialDecisionEnv(scenario, max_steps=max_steps)
    observation, info = env.reset(seed=seed)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    records = [{"type": "reset", "step": 0, "observation": deepcopy(observation), "info": deepcopy(info)}]
    terminated = truncated = False
    failure = None
    step = 0
    try:
        iterator = iter(actions) if actions is not None else None
        while True:
            if iterator is not None:
                try:
                    action = next(iterator)
                except StopIteration:
                    break
                if terminated or truncated:
                    raise ValueError("Action transcript contains extra actions after episode termination or truncation")
            else:
                if terminated or truncated:
                    break
                action = demo_action(deepcopy(observation))
            before = deepcopy(observation)
            observation, reward, terminated, truncated, info = env.step(action)
            step += 1
            records.append({"type": "step", "step": step, "observation": before,
                            "action": deepcopy(action), "next_observation": deepcopy(observation),
                            "reward": reward, "terminated": terminated, "truncated": truncated,
                            "info": deepcopy(info)})
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    summary = {
        **env.summary(),
        "episode_id": observation["episode_id"], "mode": observation["mode"],
        "policy": "hand-written demonstration rules" if actions is None else "external action transcript",
        "seed": seed, "steps": step, "terminated": terminated, "truncated": truncated,
        "status": "error" if failure else "terminated" if terminated else "truncated" if truncated else "unfinished",
        "error": failure,
        "rejected_actions": sum(bool(record.get("info", {}).get("rejection")) for record in records),
        "thesis_count": len(observation["theses"]),
        "current_time": observation["current_time"],
        "evidence_label": ("SYNTHETIC: fictional events and curated outcomes; no market-performance or learning claim."
                           if observation["mode"] == "synthetic" else
                           "HISTORICAL REPLAY: archived observations; hindsight and model contamination remain possible."),
        "artifacts": {"episode": "episode.json", "checkpoint": "checkpoint.json",
                      "trajectory": "trajectory.jsonl", "summary": "summary.json", "trace": "trace.html"},
    }
    env.export_trajectory(output / "episode.json")
    env.save_checkpoint(output / "checkpoint.json")
    with (output / "trajectory.jsonl").open("x") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
    (output / "summary.json").write_text(_json(summary))
    (output / "trace.html").write_text(render_trace(records, summary))
    if failure:
        raise ValueError(f"{failure}; partial episode artifacts saved to {output}")
    return summary


def serve_stdio(scenario, *, max_steps=128, incoming=None, outgoing=None):
    """One JSON request/response per line for an external policy process.

    Requests are reset(seed), step(action), or summary. Errors are explicit and
    do not terminate the server. The caller owns recording this public stream.
    """
    env = FinancialDecisionEnv(scenario, max_steps=max_steps)
    incoming, outgoing = incoming or sys.stdin, outgoing or sys.stdout
    ready = False
    for line in incoming:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("Request must be a JSON object")
            operation = request.get("op")
            if operation == "reset" and set(request) <= {"op", "seed"}:
                observation, info = env.reset(seed=request.get("seed"))
                ready = True
                response = {"ok": True, "observation": observation, "info": info}
            elif operation == "step" and set(request) == {"op", "action"}:
                if not ready:
                    raise ValueError("Send reset before step")
                observation, reward, terminated, truncated, info = env.step(request["action"])
                response = {"ok": True, "observation": observation, "reward": reward,
                            "terminated": terminated, "truncated": truncated, "info": info}
            elif operation == "summary" and set(request) == {"op"}:
                if not ready:
                    raise ValueError("Send reset before summary")
                response = {"ok": True, "summary": env.summary()}
            else:
                raise ValueError("Use reset with optional seed, step with action, or summary")
        except (ValueError, TypeError, KeyError, RuntimeError) as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        outgoing.write(json.dumps(response, ensure_ascii=False, allow_nan=False) + "\n")
        outgoing.flush()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("demo", "replay"):
        command = commands.add_parser(name)
        command.add_argument("--output", type=Path, required=True, help="New isolated output directory; never overwritten")
        command.add_argument("--seed", type=int, default=0)
        command.add_argument("--max-steps", type=int, default=128)
        if name == "replay":
            command.add_argument("--scenario", type=Path, required=True)
            command.add_argument("--actions", type=Path, required=True, help="JSONL action objects in execution order")
    stdio = commands.add_parser("stdio", help="JSONL reset/step/summary protocol for external policy processes")
    stdio.add_argument("--scenario", type=Path, required=True)
    stdio.add_argument("--max-steps", type=int, default=128)
    args = parser.parse_args(argv)
    scenario = Path(__file__).with_name("fixtures") / "demo.json" if args.command == "demo" else args.scenario
    try:
        if args.command == "stdio":
            serve_stdio(args.scenario, max_steps=args.max_steps)
            return 0
        summary = run_episode(scenario, args.output, seed=args.seed, max_steps=args.max_steps,
                              actions=None if args.command == "demo" else _read_actions(args.actions))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, f"Error: {exc}\n")
    print(_json(summary), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
