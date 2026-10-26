"""Deterministic long-horizon research environment with delayed forecast rewards."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import MappingProxyType

from jsonschema import ValidationError

from ..attention_judgment.judgment_validation import validate_final
from ..attention_judgment.ledger import canonical, digest
from .scenario import load_manifest, public_question, topic_scope, utc_time

REWARD_CONTRACT = MappingProxyType({
    "version": 1,
    "forecast_score": "0.25 - (probability - outcome)^2; once, at resolution availability",
    "first_read_cost": 0.001,
    "invalid_action_penalty": 0.01,
    "judge_reward": 0.0,
    "wait_reward": 0.0,
    "unresolved": "no score; no zero-filled outcome",
})
_REWARD_HASH = digest(dict(REWARD_CONTRACT))
_ACTION_FIELDS = {
    "read": {"type", "observation_id"},
    "judge": {"type", "judgment"},
    "forecast": {"type", "thesis_id", "question_id", "probability", "evidence_ids", "reason"},
    "wait": {"type", "reason"},
}


def _write_new(path, value):
    path = Path(path)
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    return path


def _action_copy(action):
    try:
        return json.loads(canonical(action))
    except (TypeError, ValueError, OverflowError):
        # Preserve a replayable rejected attempt without serializing NaN or objects.
        return {"type": "__invalid_json_action__"}


class FinancialDecisionEnv:
    """Offline dict API; manifests/checkpoints belong to the trusted operator.

    Policies receive copies of public observations. Python attribute access is not
    a security boundary. No model, broker, network, ledger, or production service
    is called. The seed is recorded; all transitions are deterministic.
    """

    def __init__(self, manifest, max_steps=128):
        if type(max_steps) is not int or max_steps < 1:
            raise ValueError("max_steps must be a positive integer")
        self._manifest = load_manifest(manifest)
        self._manifest_hash = digest(self._manifest)
        self._originals = {o["id"]: o for o in self._manifest["observations"]}
        self._questions = {q["question_id"]: q for q in self._manifest["questions"]}
        self._max_steps = max_steps
        self._started = False

    @property
    def manifest_hash(self):
        return self._manifest_hash

    @property
    def reward_contract_hash(self):
        return _REWARD_HASH

    @property
    def reward_contract(self):
        return deepcopy(dict(REWARD_CONTRACT))

    def reset(self, *, seed=None, options=None):
        if options is not None and (not isinstance(options, dict) or options):
            raise ValueError("reset options cannot inject or change the episode")
        if seed is not None and type(seed) is not int:
            raise ValueError("seed must be an integer or null")
        self._seed = seed
        self._index = 0
        self._steps = 0
        self._read = []
        self._theses = {}
        self._history = []
        self._forecasts = []
        self._settlements = []
        self._records = []
        self._terminated = False
        self._truncated = False
        self._totals = {"forecast_score": 0.0, "read_cost": 0.0, "invalid_action_penalty": 0.0}
        self._started = True
        observation = self._observation()
        self._reset_observation = deepcopy(observation)
        return observation, self._info()

    def _require_started(self):
        if not self._started:
            raise RuntimeError("call reset before using the episode")

    @property
    def _now(self):
        return utc_time(self._manifest["timeline"][self._index])

    @property
    def _time(self):
        return self._manifest["timeline"][self._index]

    def _visible(self, item):
        return all(utc_time(item[k]) <= self._now for k in ("published_at", "available_at"))

    def _resolution_visible(self, question):
        available = question["resolution"]["available_at"]
        return available is not None and utc_time(available) <= self._now

    def _observation(self):
        sources = []
        for item in self._originals.values():
            if self._visible(item):
                sources.append({**{k: v for k, v in item.items() if k != "raw"},
                                "title": item["raw"].get("title", item["source_id"])})
        questions = []
        for question in self._questions.values():
            if utc_time(question["available_at"]) <= self._now:
                public = public_question(question)
                if self._resolution_visible(question):
                    public["resolution"] = deepcopy(question["resolution"])
                questions.append(public)
        return deepcopy({
            "episode_id": self._manifest["episode_id"], "mode": self._manifest["mode"],
            "current_time": self._time, "topic_scope": topic_scope(),
            "available_sources": sources,
            "read_observations": [self._originals[key] for key in self._read],
            "questions": questions, "theses": self._theses,
            "thesis_history": self._history, "forecasts": self._forecasts,
            "settlements": self._settlements,
            "budget": {"steps_used": self._steps, "max_steps": self._max_steps,
                       "steps_remaining": self._max_steps - self._steps},
        })

    def _info(self, *, components=None, explanations=None, rejection=None):
        return deepcopy({"manifest_hash": self._manifest_hash,
                         "reward_contract_hash": _REWARD_HASH,
                         "reward_contract_version": 1,
                         "reward_components": components or {key: 0.0 for key in self._totals},
                         "reward_explanations": explanations or [],
                         "rejection": rejection,
                         "current_time": self._time,
                         "steps_used": self._steps,
                         "terminated": self._terminated, "truncated": self._truncated})

    def _validate_action(self, action):
        if not isinstance(action, dict) or not isinstance(action.get("type"), str):
            raise ValueError("action requires a supported type")
        kind = action["type"]
        if kind == "__invalid_json_action__":
            raise ValueError("action must contain finite JSON data")
        if kind not in _ACTION_FIELDS or set(action) != _ACTION_FIELDS[kind]:
            raise ValueError("action has unsupported type or fields")
        if kind == "read":
            key = action["observation_id"]
            if not isinstance(key, str) or key not in self._originals or not self._visible(self._originals[key]):
                raise ValueError("observation is not currently available")
        elif kind == "judge":
            self._validate_judgment(action["judgment"])
        elif kind == "forecast":
            self._validate_forecast(action)
        if kind in ("wait", "forecast") and (not isinstance(action["reason"], str) or not action["reason"].strip()):
            raise ValueError("action requires a nonempty reason")
        return kind

    def _validate_judgment(self, judgment):
        try:
            validate_final(judgment, [self._originals[key] for key in self._read], self._theses, self._now)
        except ValidationError:
            raise ValueError("judgment does not match the discovery schema") from None
        for discovery in judgment["discoveries"]:
            if discovery["forecasts"]:
                raise ValueError("use the forecast action for registered questions")
            key = discovery["thesis_id"]
            if key not in self._theses:
                continue
            prior_versions = [version["discovery"] for version in self._history
                              if version["discovery"]["thesis_id"] == key]
            old_counterevidence = {r for prior in prior_versions for r in prior["counterevidence_ids"]}
            refs = discovery["evidence_ids"] + discovery["counterevidence_ids"]
            if not old_counterevidence <= set(refs):
                raise ValueError("retain historical counterevidence in every revision")
            old_ids = {r for prior in prior_versions for r in prior["evidence_ids"] + prior["counterevidence_ids"]}
            old = [self._originals[r] for r in old_ids]
            hashes, events = {o["raw_sha256"] for o in old}, {o["event_id"] for o in old}
            novel = False
            for ref in set(refs) - old_ids:
                item = self._originals[ref]
                same_original = any((o["source"], o["source_id"], o["event_id"]) ==
                                    (item["source"], item["source_id"], item["event_id"]) for o in old)
                if item["raw_sha256"] not in hashes and (item["event_id"] not in events or same_original):
                    novel = True
            if not novel:
                raise ValueError("revision needs new original content or a correction, not only duplicate evidence")

    def _validate_forecast(self, action):
        thesis_id, question_id = action["thesis_id"], action["question_id"]
        if not isinstance(thesis_id, str) or thesis_id not in self._theses:
            raise ValueError("forecast requires an existing thesis")
        question = self._questions.get(question_id) if isinstance(question_id, str) else None
        if (question is None or utc_time(question["available_at"]) > self._now
                or self._now >= utc_time(question["deadline"]) or self._resolution_visible(question)):
            raise ValueError("question is not currently open for forecasts")
        if any(f["question_id"] == question_id for f in self._forecasts):
            raise ValueError("question already has a frozen forecast")
        p = action["probability"]
        if type(p) not in (float, int) or not 0 <= p <= 1:
            raise ValueError("probability must be finite and between 0 and 1")
        refs = action["evidence_ids"]
        if (not isinstance(refs, list) or not refs or any(not isinstance(r, str) for r in refs)
                or len(refs) != len(set(refs)) or not set(refs) <= set(self._read)):
            raise ValueError("forecast requires unique, previously read original evidence")

    def _freeze_judgment(self, judgment):
        for discovery in judgment["discoveries"]:
            old = self._theses.get(discovery["thesis_id"])
            refs = discovery["evidence_ids"] + discovery["counterevidence_ids"]
            record = {"discovery": deepcopy(discovery), "version": old["version"] + 1 if old else 1,
                      "created_at": self._time, "step": self._steps,
                      "previous_judgment_hash": old["judgment_hash"] if old else None,
                      "evidence_hashes": {r: self._originals[r]["raw_sha256"] for r in refs}}
            record["judgment_hash"] = digest(record)
            self._theses[discovery["thesis_id"]] = record
            self._history.append(record)

    def _freeze_forecast(self, action):
        question = self._questions[action["question_id"]]
        record = {k: deepcopy(action[k]) for k in ("thesis_id", "question_id", "probability", "evidence_ids", "reason")}
        record.update({"question": public_question(question), "created_at": self._time, "step": self._steps,
                       "judgment_hash": self._theses[action["thesis_id"]]["judgment_hash"],
                       "evidence_hashes": {r: self._originals[r]["raw_sha256"] for r in action["evidence_ids"]}})
        record["forecast_id"] = digest(record)
        self._forecasts.append(record)

    def _settle_due(self, components, explanations):
        completed = {s["forecast_id"] for s in self._settlements}
        for forecast in self._forecasts:
            question = self._questions[forecast["question_id"]]
            if forecast["forecast_id"] in completed or not self._resolution_visible(question):
                continue
            resolution = question["resolution"]
            record = {"forecast_id": forecast["forecast_id"], "question_id": forecast["question_id"],
                      "thesis_id": forecast["thesis_id"], "settled_at": self._time,
                      **deepcopy(resolution),
                      "evidence_hashes": {r: self._originals[r]["raw_sha256"] for r in resolution["evidence_ids"]}}
            if resolution["status"] == "resolved":
                brier = (forecast["probability"] - resolution["value"]) ** 2
                score = 0.25 - brier
                record.update({"probability": forecast["probability"], "brier": brier, "score": score})
                components["forecast_score"] += score
                explanations.append({"component": "forecast_score", "amount": score,
                                     "forecast_id": forecast["forecast_id"], "question_id": forecast["question_id"],
                                     "reason": "resolved original evidence is now available; centered Brier paid once"})
            self._settlements.append(record)

    def step(self, action):
        self._require_started()
        if self._terminated or self._truncated:
            raise RuntimeError("episode is over; reset before another action")
        before = self._observation()
        action = _action_copy(action)
        components = {key: 0.0 for key in self._totals}
        explanations = []
        rejection = None
        self._steps += 1
        try:
            kind = self._validate_action(action)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            rejection = str(exc) if isinstance(exc, ValueError) else "invalid action value"
            components["invalid_action_penalty"] = -REWARD_CONTRACT["invalid_action_penalty"]
            explanations.append({"component": "invalid_action_penalty", "amount": components["invalid_action_penalty"],
                                 "reason": rejection})
        else:
            if kind == "read" and action["observation_id"] not in self._read:
                self._read.append(action["observation_id"])
                components["read_cost"] = -REWARD_CONTRACT["first_read_cost"]
                explanations.append({"component": "read_cost", "amount": components["read_cost"],
                                     "observation_id": action["observation_id"], "reason": "first read of this original"})
            elif kind == "judge":
                self._freeze_judgment(action["judgment"])
            elif kind == "forecast":
                self._freeze_forecast(action)
            elif kind == "wait":
                if self._index + 1 < len(self._manifest["timeline"]):
                    self._index += 1
                    self._settle_due(components, explanations)
                else:
                    self._terminated = True
        if self._steps >= self._max_steps and not self._terminated:
            self._truncated = True
        for key, value in components.items():
            self._totals[key] += value
        reward = sum(components.values())
        after = self._observation()
        info = self._info(components=components, explanations=explanations, rejection=rejection)
        self._records.append(deepcopy({"step": self._steps, "observation": before, "action": action,
                                       "next_observation": after, "reward": reward,
                                       "reward_components": components, "reward_explanations": explanations,
                                       "current_time": self._time, "episode_id": self._manifest["episode_id"],
                                       "terminated": self._terminated, "truncated": self._truncated, "info": info}))
        return after, reward, self._terminated, self._truncated, info

    def summary(self):
        self._require_started()
        settled = {s["forecast_id"]: s for s in self._settlements}
        results = []
        for forecast in self._forecasts:
            if forecast["forecast_id"] in settled:
                results.append(deepcopy(settled[forecast["forecast_id"]]))
            else:
                due = self._now >= utc_time(self._questions[forecast["question_id"]]["resolve_at"])
                results.append({"forecast_id": forecast["forecast_id"], "question_id": forecast["question_id"],
                                "thesis_id": forecast["thesis_id"], "status": "unresolved" if due else "pending", "value": None,
                                "reason": "resolution evidence is not available at the current cutoff" if due
                                else "the public resolution time has not arrived"})
        return deepcopy({"episode_id": self._manifest["episode_id"], "current_time": self._time,
                         "steps": self._steps, "forecasts": len(self._forecasts),
                         "resolved_forecasts": sum(r["status"] == "resolved" for r in results),
                         "unresolved_forecasts": sum(r["status"] == "unresolved" for r in results),
                         "pending_forecasts": sum(r["status"] == "pending" for r in results),
                         "total_reward": sum(self._totals.values()), "reward_components": self._totals,
                         "terminated": self._terminated, "truncated": self._truncated, "results": results})

    def export_trajectory(self, path):
        self._require_started()
        trajectory = {"schema_version": 1, "episode_id": self._manifest["episode_id"],
                      "manifest_hash": self._manifest_hash, "reward_contract_hash": _REWARD_HASH,
                      "reward_contract": dict(REWARD_CONTRACT), "seed": self._seed,
                      "max_steps": self._max_steps, "reset_observation": self._reset_observation,
                      "steps": self._records, "summary": self.summary()}
        trajectory["trajectory_hash"] = digest(trajectory)
        return _write_new(path, trajectory)

    def get_state(self):
        """Export replay instructions and commitments, never mutable reward balances."""
        self._require_started()
        state = {"schema_version": 1, "manifest_hash": self._manifest_hash,
                 "reward_contract_hash": _REWARD_HASH, "seed": self._seed, "max_steps": self._max_steps,
                 "actions": [deepcopy(record["action"]) for record in self._records],
                 "trace_hash": digest(self._records), "observation_hash": digest(self._observation()),
                 "summary_hash": digest(self.summary())}
        state["checkpoint_hash"] = digest(state)
        return state

    def load_state(self, state):
        """Validate, replay in a fresh instance, then atomically replace this episode."""
        state = deepcopy(state)
        expected = {"schema_version", "manifest_hash", "reward_contract_hash", "seed", "max_steps",
                    "actions", "trace_hash", "observation_hash", "summary_hash", "checkpoint_hash"}
        try:
            if not isinstance(state, dict) or set(state) != expected:
                raise ValueError("invalid checkpoint fields")
            checksum = state.pop("checkpoint_hash")
            if checksum != digest(state):
                raise ValueError("checkpoint integrity mismatch")
            if (type(state["schema_version"]) is not int or state["schema_version"] != 1
                    or state["manifest_hash"] != self._manifest_hash or state["reward_contract_hash"] != _REWARD_HASH):
                raise ValueError("checkpoint manifest or reward contract mismatch")
            if state["max_steps"] != self._max_steps or not isinstance(state["actions"], list):
                raise ValueError("checkpoint episode configuration mismatch")
            restored = type(self)(self._manifest, max_steps=self._max_steps)
            restored.reset(seed=state["seed"])
            for action in state["actions"]:
                restored.step(action)
            if (digest(restored._records) != state["trace_hash"] or digest(restored._observation()) != state["observation_hash"]
                    or digest(restored.summary()) != state["summary_hash"]):
                raise ValueError("checkpoint replay does not match committed state")
        except (TypeError, KeyError, RuntimeError, OverflowError):
            raise ValueError("invalid checkpoint replay") from None
        self.__dict__.update(restored.__dict__)
        return self._observation(), self._info()

    def save_checkpoint(self, path):
        return _write_new(path, self.get_state())

    def load_checkpoint(self, path):
        return self.load_state(json.loads(Path(path).read_text()))
