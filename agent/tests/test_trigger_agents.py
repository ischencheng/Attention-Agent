"""Exercise scheduler dispatch with a fake Docker CLI; no containers are run."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / "trigger_agents.sh"
FAKE_DOCKER = r'''
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if args[0] == "inspect":
    print("true")
    sys.exit(0)
if args[0] != "exec":
    sys.exit(99)
with Path(os.environ["TEST_DISPATCH_LOG"]).open("a") as stream:
    stream.write(json.dumps(args) + "\n")
print("fake cycle")
sys.exit(7 if args[1] == os.environ.get("TEST_FAIL_CONTAINER") else 0)
'''


class TriggerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        fake = self.root / "docker"
        fake.write_text(f"#!{sys.executable}\n" + FAKE_DOCKER)
        fake.chmod(0o755)
        self.log = self.root / "dispatch.jsonl"
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ["PATH"],
                        TEST_DISPATCH_LOG=str(self.log), MID_CONTAINER="mid-agent", STRONG_CONTAINER="strong-agent")

    def invoke(self, *args):
        return subprocess.run(["bash", str(SCRIPT), *args], env=self.env,
                              capture_output=True, text=True, timeout=5)

    def test_both_roles_use_codex_adapter(self):
        result = self.invoke("both")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual([call[1] for call in calls], ["mid-agent", "strong-agent"])
        for call, role in zip(calls, ("mid", "strong")):
            self.assertEqual(call[2:4], ["python", "/app/codex_runner.py"])
            self.assertIn(role, call)
            self.assertIn("--network-access", call)
            self.assertNotIn("--dangerously-skip-permissions", call)

    def test_both_runs_second_role_but_preserves_first_failure(self):
        self.env["TEST_FAIL_CONTAINER"] = "mid-agent"
        result = self.invoke("both")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(self.log.read_text().splitlines()), 2)

    def test_one_shot_propagates_failure(self):
        self.env["TEST_FAIL_CONTAINER"] = "strong-agent"
        self.assertEqual(self.invoke("strong").returncode, 7)

    def test_invalid_intervals_fail_before_dispatch(self):
        result = self.invoke("--mid-interval", "0")
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
