"""The research reference is an immutable input to a recorded rollout."""
import copy
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from environments.research_portfolio.archive import digest
from environments.research_portfolio.cli import main
from environments.research_portfolio.demo import demo_actions
from environments.research_portfolio.env import ResearchPortfolioEnv
from environments.research_portfolio.runner import run_episode


FIXTURES = Path(__file__).parents[1] / "fixtures"
FRAMEWORK = FIXTURES / "mrbrain_framework.json"


class FrameworkSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.original = json.loads(FRAMEWORK.read_text())

    def prepare_framework(self, env):
        for action in demo_actions():
            if action.get("tool") == "framework":
                return action
            _, _, _, _, info = env.step(action)
            self.assertNotIn("rejection", info)
        self.fail("demo must exercise framework retrieval")

    def read_framework(self, env, action):
        _, _, _, _, info = env.step(action)
        self.assertNotIn("rejection", info)
        return info["result"]["result"]["original_framework"]

    def test_source_file_changes_cannot_change_an_existing_environment_or_reset(self):
        env = ResearchPortfolioEnv(FIXTURES / "demo.json")
        original_read = Path.read_text
        changed = copy.deepcopy(self.original)
        changed["framework"] = {"changed_after_environment_creation": True}

        def changed_read(path, *args, **kwargs):
            if path == FRAMEWORK:
                return json.dumps(changed)
            return original_read(path, *args, **kwargs)

        with patch.object(Path, "read_text", new=changed_read):
            action = self.prepare_framework(env)
            result = self.read_framework(env, action)
            self.assertEqual(result, self.original)
            result["framework"].clear()
            self.assertEqual(self.read_framework(env, action), self.original)
            env.reset()
            self.assertEqual(self.read_framework(env, self.prepare_framework(env)), self.original)

    def test_full_snapshot_and_digest_exist_before_policy_invocation(self):
        output = self.root / "before-policy"
        observed = []

        def probe(*args, **kwargs):
            retained = json.loads((output / "framework.json").read_text())
            contract = json.loads((output / "contract.json").read_text())
            observed.append(retained)
            self.assertEqual(retained, self.original)
            self.assertEqual(contract["framework_path"], "framework.json")
            self.assertEqual(contract["framework_sha256"], digest(retained))
            raise RuntimeError("intentional policy-boundary probe")

        summary = run_episode(FIXTURES / "demo.json", output, codex=True, policy_runner=probe)
        self.assertEqual(observed, [self.original])
        self.assertIn("intentional policy-boundary probe", summary["error"])

    def test_retained_reference_replays_after_the_bundled_reference_is_unavailable(self):
        first_dir = self.root / "first"
        first = run_episode(FIXTURES / "demo.json", first_dir, actions=demo_actions())
        retained = json.loads((first_dir / "framework.json").read_text())
        transcript = json.loads((first_dir / "actions.json").read_text())
        original_read = Path.read_text

        def unavailable_reference(path, *args, **kwargs):
            if path == FRAMEWORK:
                raise FileNotFoundError("bundled reference changed after recording")
            return original_read(path, *args, **kwargs)

        with patch.object(Path, "read_text", new=unavailable_reference):
            second = run_episode(first_dir / "episode.json", self.root / "second",
                                 actions=transcript, framework_snapshot=retained)
        self.assertEqual(first["account"], second["account"])
        self.assertEqual(first["reward"], second["reward"])
        rows = [json.loads(line) for line in (self.root / "second" / "trajectory.jsonl").read_text().splitlines()]
        receipts = [row["result"]["result"]["result"]["original_framework"] for row in rows
                    if row["action"].get("tool") == "framework"]
        self.assertEqual(receipts, [retained])

    def test_supplied_reference_and_public_snapshot_cannot_mutate_environment(self):
        supplied = copy.deepcopy(self.original)
        env = ResearchPortfolioEnv(FIXTURES / "demo.json", framework_snapshot=supplied)
        supplied["framework"].clear()
        exposed = env.framework_snapshot
        exposed["framework"].clear()
        self.assertEqual(self.read_framework(env, self.prepare_framework(env)), self.original)

    def test_cli_replay_uses_the_explicit_retained_reference(self):
        retained = copy.deepcopy(self.original)
        retained["framework"]["test_retained_reference"] = "Saved before a bundled reference update"
        reference = self.root / "retained.json"
        reference.write_text(json.dumps(retained))
        transcript = self.root / "actions.json"
        transcript.write_text(json.dumps(demo_actions()))
        output = self.root / "cli-replay"
        with redirect_stdout(io.StringIO()):
            status = main(["replay", "--scenario", str(FIXTURES / "demo.json"),
                           "--actions", str(transcript), "--framework-snapshot", str(reference),
                           "--output", str(output)])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads((output / "framework.json").read_text()), retained)
        contract = json.loads((output / "contract.json").read_text())
        self.assertEqual(contract["framework_sha256"], digest(retained))
        rows = [json.loads(line) for line in (output / "trajectory.jsonl").read_text().splitlines()]
        receipt = next(row["result"]["result"]["result"] for row in rows
                       if row["action"].get("tool") == "framework")
        self.assertEqual(receipt["original_framework"], retained)

    def test_cli_snapshot_override_cannot_change_codex_or_fixed_demo_defaults(self):
        for command in ("codex", "demo"):
            with self.subTest(command=command), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main([command, "--framework-snapshot", str(self.root / "absent.json"),
                      "--output", str(self.root / command)])
            self.assertEqual(error.exception.code, 2)
            self.assertFalse((self.root / command).exists())


if __name__ == "__main__":
    unittest.main()
