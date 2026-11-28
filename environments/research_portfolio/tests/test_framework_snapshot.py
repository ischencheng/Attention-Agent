"""The research reference is an immutable input to a recorded rollout."""
import copy
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from skills.investment_principles import tool as principles
from environments.research_portfolio.archive import digest
from environments.research_portfolio.cli import main
from environments.research_portfolio.demo import demo_actions
from environments.research_portfolio.env import ResearchPortfolioEnv
from environments.research_portfolio.runner import run_episode


FIXTURES = Path(__file__).parents[1] / "fixtures"


class FrameworkSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.local = self.root / "investment_principles.json"
        self.enterContext(patch.dict("os.environ", {"ATTENTION_INVESTMENT_PRINCIPLES": ""}))
        local_path = patch.object(principles, "LOCAL_PATH", self.local)
        local_path.start()
        self.addCleanup(local_path.stop)
        self.original = principles.load_principles()

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
        changed = copy.deepcopy(self.original["framework"])
        changed["risk_and_position_size"] = ["Use evidence-based sizing limits."]
        self.local.write_text(json.dumps(changed))
        new_env = ResearchPortfolioEnv(FIXTURES / "demo.json")
        self.assertEqual(new_env.framework_snapshot["framework"], changed)
        self.assertEqual(new_env.framework_snapshot["source"], "local")
        action = self.prepare_framework(env)
        result = self.read_framework(env, action)
        self.assertEqual(result, self.original)
        result["framework"].clear()
        self.assertEqual(self.read_framework(env, action), self.original)
        env.reset()
        self.assertEqual(self.read_framework(env, self.prepare_framework(env)), self.original)

    def test_standalone_skill_reads_the_local_file_in_the_mounted_skills_tree(self):
        skill = self.root / "skills" / "investment_principles"
        skill.mkdir(parents=True)
        command = skill / "tool.py"
        shutil.copyfile(principles.__file__, command)
        shutil.copyfile(principles.TEMPLATE_PATH, skill / "template.json")
        profile = copy.deepcopy(self.original["framework"])
        profile["review_and_exit"] = ["重新检查失效条件。"]
        (skill.parent / "investment_principles.json").write_text(
            json.dumps(profile, ensure_ascii=False), encoding="utf-8")

        result = subprocess.run([sys.executable, str(command)], cwd=self.root,
                                check=True, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(json.loads(result.stdout), {"kind": "investment_principles_v1",
                                                     "source": "local", "framework": profile})

    def test_malformed_local_file_fails_without_overriding_retained_snapshot(self):
        self.local.write_text('{"risk_and_position_size": "not an array"}')
        with self.assertRaisesRegex(ValueError, "must contain these fields"):
            ResearchPortfolioEnv(FIXTURES / "demo.json")
        env = ResearchPortfolioEnv(FIXTURES / "demo.json", framework_snapshot=self.original)
        self.assertEqual(env.framework_snapshot, self.original)

    def test_explicit_path_overrides_local_file_and_accepts_utf8_bom(self):
        self.local.write_text("invalid local settings")
        config = self.root / "explicit.json"
        profile = copy.deepcopy(self.original["framework"])
        profile["review_and_exit"] = ["Review every thesis after material news."]
        with patch.dict("os.environ", {"ATTENTION_INVESTMENT_PRINCIPLES": str(config)}):
            with self.assertRaises(FileNotFoundError):
                principles.load_principles()
            config.write_text(json.dumps(profile), encoding="utf-8-sig")
            self.assertEqual(ResearchPortfolioEnv(FIXTURES / "demo.json").framework_snapshot["framework"], profile)

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
        retained = {"kind": "frozen_reference_not_an_asset_assessment", "source": "archived_fixture:FRAMEWORK",
                    "framework": {"review": "Recheck the thesis against new evidence."}}
        first = run_episode(FIXTURES / "demo.json", first_dir, actions=demo_actions(), framework_snapshot=retained)
        transcript = json.loads((first_dir / "actions.json").read_text())
        original_read = Path.read_text

        def unavailable_reference(path, *args, **kwargs):
            if path == principles.TEMPLATE_PATH:
                raise FileNotFoundError("bundled reference changed after recording")
            return original_read(path, *args, **kwargs)

        with patch.object(Path, "read_text", new=unavailable_reference):
            second = run_episode(first_dir / "episode.json", self.root / "second",
                                 actions=transcript, framework_snapshot=retained)
        self.assertEqual(first["account"], second["account"])
        self.assertEqual(first["reward"], second["reward"])
        rows = [json.loads(line) for line in (self.root / "second" / "trajectory.jsonl").read_text().splitlines()]
        receipts = [row["result"]["result"]["result"] for row in rows
                    if row["action"].get("tool") == "framework"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["kind"], "frozen_original_framework")
        self.assertEqual(receipts[0]["original_framework"], retained)
        first_rows = [json.loads(line) for line in (first_dir / "trajectory.jsonl").read_text().splitlines()]
        self.assertEqual(rows, first_rows)

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
