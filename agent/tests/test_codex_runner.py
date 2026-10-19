"""Offline adapter tests. The fake CLI never contacts a model or reads auth."""
from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import time
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("codex_runner", ROOT / "codex_runner.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

SCHEMA = {
    "type": "object", "properties": {"value": {"type": "integer"}},
    "required": ["value"], "additionalProperties": False,
}

FAKE = r'''
import json
from pathlib import Path
import subprocess
import sys
import time

args = sys.argv[1:]
if args == ["--version"]:
    print("codex-cli 0.163.0 (offline fake)")
    sys.exit(0)
assert args[0] == "exec"
assert "--json" in args and "--output-schema" in args and "--ephemeral" in args
assert "--ignore-user-config" in args and "--skip-git-repo-check" in args
assert "--dangerously-bypass-approvals-and-sandbox" not in args
assert args[-1] == "-"
prompt = sys.stdin.read()
final = Path(args[args.index("--output-last-message") + 1])
(final.parent / "observed.json").write_text(json.dumps({"args": args, "prompt": prompt}))
print(json.dumps({"type": "thread.started", "thread_id": "offline-fake"}), flush=True)
if prompt == "signal":
    import os
    (final.parent / "child.pid").write_text(str(os.getpid()))
    time.sleep(30)
if prompt == "timeout":
    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    time.sleep(30)
if prompt == "bad-events":
    print("not-json")
if prompt != "missing-final":
    final.write_text(json.dumps({"value": "not-an-integer" if prompt == "bad-schema" else 7}))
if prompt == "failed-turn":
    print(json.dumps({"type": "turn.failed", "error": {"message": "fake failure"}}))
elif prompt != "incomplete":
    print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 4}}))
if prompt == "nonzero":
    print("offline failure diagnostic", file=sys.stderr)
    sys.exit(7)
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.cli = self.root / "fake-codex"
        self.cli.write_text(f"#!{sys.executable}\n" + FAKE)
        self.cli.chmod(0o755)

    def invoke(self, prompt="good", **kwargs):
        return runner.run(prompt, SCHEMA, self.root, self.root / "run", 2,
                          codex_bin=str(self.cli), **kwargs)

    def test_native_schema_events_and_usage(self):
        prompt = "Literal prompt: `echo no` $(echo no) 中文"
        result = self.invoke(prompt, model="test-model")
        self.assertEqual(result["final"], {"value": 7})
        self.assertEqual(result["cli_version"], "codex-cli 0.163.0 (offline fake)")
        self.assertEqual(result["usage"], {"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 4})
        observed = json.loads((self.root / "run/observed.json").read_text())
        self.assertEqual(observed["prompt"], prompt)
        self.assertIn("test-model", observed["args"])
        self.assertNotIn("sandbox_workspace_write.network_access=true", observed["args"])
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertTrue(report["success"])
        self.assertEqual(report["cli_version"], result["cli_version"])

    def test_network_is_explicit_and_uses_sandbox_config(self):
        self.invoke(network_access=True)
        observed = json.loads((self.root / "run/observed.json").read_text())
        self.assertIn("sandbox_workspace_write.network_access=true", observed["args"])
        self.assertIn("workspace-write", observed["args"])

    def test_classification_disables_tools_and_project_instructions(self):
        self.invoke(disable_tools=True, sandbox="read-only")
        observed = json.loads((self.root / "run/observed.json").read_text())
        for flag in ("features.shell_tool=false", "features.apps=false", "features.plugins=false",
                     'web_search="disabled"', "project_doc_max_bytes=0", "agents.enabled=false", "skills.include_instructions=false"):
            self.assertIn(flag, observed["args"])
        self.assertTrue(json.loads((self.root / "run/run.json").read_text())["disable_tools"])

    def test_classification_custom_instructions_are_saved_and_scoped(self):
        self.invoke(disable_tools=True, sandbox="read-only", model_instructions="Only classify the observation.")
        self.assertEqual((self.root / "run/model-instructions.txt").read_text(), "Only classify the observation.")
        observed = json.loads((self.root / "run/observed.json").read_text())
        self.assertTrue(any(argument.startswith("model_instructions_file=") for argument in observed["args"]))

    def test_custom_instructions_cannot_override_agent_cycles(self):
        with self.assertRaisesRegex(ValueError, "classification"):
            self.invoke(model_instructions="Only classify.")

    def test_disabled_tools_cannot_enable_network_or_writes(self):
        with self.assertRaisesRegex(ValueError, "disable_tools"):
            self.invoke(disable_tools=True)

    def test_nonzero_exit_cannot_be_saved_as_success(self):
        with self.assertRaises(runner.CodexRunError) as caught:
            self.invoke("nonzero")
        self.assertEqual(caught.exception.returncode, 7)
        self.assertIn("offline failure diagnostic", (self.root / "run/stderr.log").read_text())
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertFalse(report["success"])
        self.assertEqual(report["returncode"], 7)

    def test_failed_turn_rejected(self):
        with self.assertRaisesRegex(runner.CodexRunError, "failed turn"):
            self.invoke("failed-turn")

    def test_incomplete_turn_rejected(self):
        with self.assertRaisesRegex(runner.CodexRunError, "completed turn"):
            self.invoke("incomplete")

    def test_malformed_events_rejected(self):
        with self.assertRaisesRegex(runner.CodexRunError, "Invalid JSONL"):
            self.invoke("bad-events")

    def test_missing_final_rejected(self):
        with self.assertRaisesRegex(runner.CodexRunError, "no final"):
            self.invoke("missing-final")

    def test_schema_invalid_final_rejected(self):
        with self.assertRaisesRegex(runner.CodexRunError, "structured output"):
            self.invoke("bad-schema")

    def test_timeout_records_failure(self):
        with self.assertRaisesRegex(runner.CodexRunError, "run budget"):
            runner.run("timeout", SCHEMA, self.root, self.root / "run", 0.2, codex_bin=str(self.cli))
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertFalse(report["success"])
        self.assertLess(report["wall_seconds"], 4)

    @unittest.skipUnless(os.name == "posix", "SIGTERM cleanup uses POSIX process groups")
    def test_sigterm_terminates_codex_child_and_records_failure(self):
        output = self.root / "signal-run"
        driver = (
            "import sys; from pathlib import Path; "
            f"sys.path.insert(0, {str(ROOT)!r}); import codex_runner; "
            f"codex_runner.run('signal', {SCHEMA!r}, {str(self.root)!r}, {str(output)!r}, 30, codex_bin={str(self.cli)!r})"
        )
        process = subprocess.Popen([sys.executable, "-c", driver], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        deadline = time.monotonic() + 5
        while not (output / "child.pid").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue((output / "child.pid").exists())
        child_pid = int((output / "child.pid").read_text())
        process.terminate()
        process.communicate(timeout=5)
        self.assertNotEqual(process.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)
        metadata = json.loads((output / "run.json").read_text())
        self.assertFalse(metadata["success"])
        self.assertLess(metadata["wall_seconds"], 5)

    def test_stale_final_cannot_be_reused(self):
        self.invoke()
        with self.assertRaisesRegex(ValueError, "new or empty"):
            self.invoke("missing-final")

    def test_bypass_and_invalid_timeout_rejected_before_launch(self):
        with self.assertRaisesRegex(ValueError, "bypass"):
            self.invoke(sandbox="danger-full-access")
        with self.assertRaisesRegex(ValueError, "finite positive"):
            runner.run("good", SCHEMA, self.root, self.root / "run", float("inf"), codex_bin=str(self.cli))


if __name__ == "__main__":
    unittest.main()
