"""Offline adapter tests. The fake CLI never contacts a model or reads auth."""
from __future__ import annotations

import importlib.util
from contextlib import redirect_stdout
import io
import json
import os
import signal
import subprocess
import shutil
import time
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


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
import os
from pathlib import Path
import subprocess
import sys
import time

args = sys.argv[1:]
if (Path.cwd() / "capture-environment").exists():
    phase = "version" if args == ["--version"] else "exec"
    (Path.cwd() / f"{phase}-environment.json").write_text(json.dumps(dict(os.environ)))
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
if prompt == "environment":
    inherited = subprocess.check_output([sys.executable, "-c",
        "import json, os; print(json.dumps(dict(os.environ)))"], text=True)
    (final.parent / "tool-environment.json").write_text(inherited)
print(json.dumps({"type": "thread.started", "thread_id": "offline-fake"}), flush=True)
if prompt == "signal":
    import os
    (final.parent / "child.pid").write_text(str(os.getpid()))
    time.sleep(30)
if prompt == "timeout":
    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    time.sleep(30)
if prompt in {"unicode-events", "unicode-then-bad-events"}:
    print(json.dumps({"type": "item.completed", "item": {"type": "command_execution",
        "aggregated_output": "before\u2028middle\u2029next\u0085after"}}, ensure_ascii=False))
if prompt in {"bad-events", "unicode-then-bad-events"}:
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

    def test_container_copy_layout_imports_without_the_repository(self):
        # Build exactly the Python COPY layout; no checkout paths can rescue a
        # missing shared dependency in the image or bind-mounted application.
        dockerfile = (ROOT / "Dockerfile").read_text()
        for line in dockerfile.splitlines():
            fields = line.split()
            if len(fields) == 3 and fields[0] == "COPY" and fields[1].endswith(".py"):
                shutil.copyfile(ROOT / fields[1], self.root / Path(fields[2]).name)
        result = subprocess.run([sys.executable, "-I", "-c",
            "import sys; sys.path.insert(0,sys.argv[1]); import codex_runner; "
            "assert 'Deduplicate' in codex_runner.MID_RULES; "
            "assert 'invalidation' in codex_runner.STRONG_RULES", str(self.root)],
            cwd=self.root, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

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

    def test_research_receipt_directory_is_an_explicit_narrow_write_grant(self):
        store = self.root / "receipts"
        store.mkdir()
        self.invoke(environment_profile="research", additional_writable_dirs=[store])
        observed = json.loads((self.root / "run/observed.json").read_text())
        args = observed["args"]
        self.assertEqual(args.count("--add-dir"), 1)
        self.assertEqual(args[args.index("--add-dir") + 1], str(store.resolve()))
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertEqual(report["additional_writable_dirs"], [str(store.resolve())])
        self.assertFalse(report["network_access"])
        self.assertNotIn("sandbox_workspace_write.network_access=true", args)

    def test_research_profile_alone_does_not_grant_extra_writes(self):
        self.invoke(environment_profile="research")
        observed = json.loads((self.root / "run/observed.json").read_text())
        self.assertNotIn("--add-dir", observed["args"])
        self.assertEqual(json.loads((self.root / "run/run.json").read_text())["additional_writable_dirs"], [])

    def test_extra_write_grants_cannot_widen_model_or_read_only_calls(self):
        for options in ({}, {"sandbox": "read-only"},
                        {"sandbox": "read-only", "disable_tools": True},
                        {"environment_profile": "research", "sandbox": "read-only"}):
            with self.subTest(options=options), patch.object(runner.subprocess, "run") as version, \
                    patch.object(runner.subprocess, "Popen") as execute:
                with self.assertRaisesRegex(ValueError, "additional_writable_dirs"):
                    self.invoke(additional_writable_dirs=[self.root], **options)
                version.assert_not_called()
                execute.assert_not_called()
                self.assertFalse((self.root / "run").exists())

    def test_extra_write_grants_require_existing_directories(self):
        file_path = self.root / "regular-file"
        file_path.write_text("fixture")
        for paths in ([self.root / "missing"], [file_path], str(self.root), [None]):
            with self.subTest(paths=paths), patch.object(runner.subprocess, "run") as version, \
                    patch.object(runner.subprocess, "Popen") as execute:
                with self.assertRaisesRegex(ValueError, "additional_writable_dirs"):
                    self.invoke(environment_profile="research", additional_writable_dirs=paths)
                version.assert_not_called()
                execute.assert_not_called()
                self.assertFalse((self.root / "run").exists())

    def test_environment_profiles_restrict_real_child_and_version_processes(self):
        common = {
            "PATH": os.defpath, "HOME": "/offline-test-home", "LANG": "C.UTF-8",
            "CODEX_HOME": "/offline-test-codex-home", "CODEX_API_KEY": "fixture-secret-codex",
            "OPENAI_API_KEY": "fixture-secret-openai", "HTTPS_PROXY": "http://127.0.0.1:8123",
            "NO_PROXY": "localhost,127.0.0.1", "SSL_CERT_FILE": "/offline-test-ca.pem",
        }
        research = {
            "DB_HOST": "db.invalid", "DB_PORT": "6543", "DB_NAME": "fixture-db", "DB_USER": "fixture-user",
            "DB_PASSWORD": "fixture-secret-db", "KAFKA_BOOTSTRAP": "broker.invalid:9092",
            "EMBEDDING_PROVIDER": "http", "EMBEDDING_MODEL": "fixture-embedding", "EMBEDDING_DIM": "768",
            "EMBEDDING_GATEWAY_URL": "http://127.0.0.1:8765/embed", "ATTENTION_GATEWAY_TOKEN": "fixture-secret-gateway",
            "SEC_EDGAR_USER_AGENT": "fixture-app contact@example.test", "FRED_API_KEY": "fixture-secret-fred",
            "GEMINI_API_KEY": "fixture-secret-gemini",
        }
        excluded = {
            "UNRELATED_ENV_SENTINEL": "fixture-secret-unrelated", "GH_TOKEN": "fixture-secret-gh",
            "GITHUB_TOKEN": "fixture-secret-github", "AWS_SECRET_ACCESS_KEY": "fixture-secret-aws",
            "FINNHUB_API_KEY": "fixture-secret-finnhub", "CARAVO_API_KEY": "fixture-secret-caravo",
            "TWITTER_AUTH_TOKEN": "fixture-secret-twitter", "SSH_AUTH_SOCK": "/offline-test-ssh-agent",
            "CODEX_UNLISTED_SECRET": "fixture-secret-unlisted", "NODE_OPTIONS": "--require /offline-test-hook",
            "PYTHONPATH": "/offline-test-import-hook", "BASH_ENV": "/offline-test-shell-hook",
            "LD_PRELOAD": "/offline-test-loader-hook", "DYLD_INSERT_LIBRARIES": "/offline-test-loader-hook",
        }
        supplied = {**common, **research, **excluded}
        (self.root / "capture-environment").touch()
        for profile in ("model", "research"):
            with self.subTest(profile=profile), patch.dict(os.environ, supplied, clear=True):
                output = self.root / profile
                runner.run("environment", SCHEMA, self.root, output, 2,
                           codex_bin=str(self.cli), environment_profile=profile)
                expected = common if profile == "model" else {**common, **research}
                observations = [json.loads((self.root / f"{phase}-environment.json").read_text())
                                for phase in ("version", "exec")]
                observations.append(json.loads((output / "tool-environment.json").read_text()))
                for actual in observations:
                    for name, value in expected.items():
                        self.assertEqual(actual.get(name), value, name)
                    for name in excluded.keys() | (research.keys() if profile == "model" else set()):
                        self.assertNotIn(name, actual)
                self.assertEqual(observations[0], observations[1])
                metadata_text = (output / "run.json").read_text()
                metadata = json.loads(metadata_text)
                self.assertEqual(metadata["environment_profile"], profile)
                self.assertEqual(metadata["environment_names"], sorted(expected))
                for value in supplied.values():
                    if value.startswith("fixture-secret-"):
                        self.assertNotIn(value, metadata_text)
                self.assertEqual(dict(os.environ), supplied)

    def test_default_environment_removes_empty_auth_without_changing_login_paths(self):
        (self.root / "capture-environment").touch()
        with patch.dict(os.environ, {"HOME": "/offline-home", "CODEX_HOME": "/offline-codex-home",
                                    "CODEX_API_KEY": "", "OPENAI_API_KEY": "",
                                    "DB_PASSWORD": "fixture-secret-db"}, clear=True):
            self.invoke()
        for phase in ("version", "exec"):
            observed = json.loads((self.root / f"{phase}-environment.json").read_text())
            self.assertEqual(observed["HOME"], "/offline-home")
            self.assertEqual(observed["CODEX_HOME"], "/offline-codex-home")
            for name in ("CODEX_API_KEY", "OPENAI_API_KEY", "DB_PASSWORD"):
                self.assertNotIn(name, observed)
        metadata = json.loads((self.root / "run/run.json").read_text())
        self.assertEqual(metadata["environment_profile"], "model")

    def test_invalid_environment_profile_cannot_launch_or_create_artifacts(self):
        for options in ({"environment_profile": "inherit-all"},
                        {"environment_profile": "research", "disable_tools": True, "sandbox": "read-only"}):
            with self.subTest(options=options), patch.object(runner.subprocess, "run") as version, \
                    patch.object(runner.subprocess, "Popen") as execute:
                with self.assertRaisesRegex(ValueError, "environment_profile"):
                    self.invoke(**options)
                version.assert_not_called()
                execute.assert_not_called()
                self.assertFalse((self.root / "run").exists())

    def test_cli_selects_research_for_roles_and_model_for_custom_prompts(self):
        prompt_path, schema_path = self.root / "prompt.txt", self.root / "schema.json"
        prompt_path.write_text("offline custom prompt")
        schema_path.write_text(json.dumps(SCHEMA))
        cases = [(["--role", "mid"], "research"), (["--role", "strong"], "research"),
                 (["--prompt-file", str(prompt_path), "--schema", str(schema_path)], "model"),
                 (["--prompt-file", str(prompt_path), "--schema", str(schema_path),
                   "--environment-profile", "research"], "research")]
        result = {"final": {"status": "completed"}, "usage": None, "output_dir": "offline-run"}
        for arguments, expected in cases:
            with self.subTest(arguments=arguments), \
                    patch.object(sys, "argv", ["codex_runner.py", *arguments]), \
                    patch.object(runner, "run", return_value=result) as execute, redirect_stdout(io.StringIO()):
                self.assertEqual(runner.main(), 0)
                self.assertEqual(execute.call_args.kwargs["environment_profile"], expected)

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

    def test_unicode_separators_roundtrip_inside_physical_jsonl_records(self):
        result = self.invoke("unicode-events")
        self.assertEqual([event["type"] for event in result["events"]],
                         ["thread.started", "item.completed", "turn.completed"])
        self.assertEqual(result["events"][1]["item"]["aggregated_output"],
                         "before\u2028middle\u2029next\u0085after")
        encoded = (self.root / "run/events.jsonl").read_bytes()
        for separator in ("\u2028", "\u2029", "\u0085"):
            self.assertIn(separator.encode("utf-8"), encoded)
        self.assertEqual(encoded.count(b"\n"), 3)
        self.assertEqual(result["usage"], {"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 4})
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertTrue(report["success"])
        self.assertEqual(report["event_count"], 3)

    def test_malformed_record_after_unicode_reports_physical_line(self):
        with self.assertRaisesRegex(runner.CodexRunError, "Invalid JSONL event at line 3:"):
            self.invoke("unicode-then-bad-events")
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertFalse(report["success"])
        self.assertEqual(report["event_count"], 2)
        self.assertEqual(report["returncode"], 0)

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
