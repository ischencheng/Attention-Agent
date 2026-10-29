"""A rollout keeps its admitted evidence and binds the selected served profile."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from environments.research_portfolio.archive import build_archive, digest, load_archive
from environments.research_portfolio.experiment import run_archived
from environments.research_portfolio.runner import run_episode
from environments.research_portfolio.tests.test_archive import inputs


class ArchivedExperimentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.bundle = build_archive(*inputs(), output=self.source)

    def test_experiment_passes_full_bundle_and_verified_served_model(self):
        with patch("environments.research_portfolio.experiment.verify_server_identity",
                   return_value={"served_model": "pinned-remote-14b"}) as verify, \
             patch("environments.research_portfolio.experiment.run_episode", return_value={"done": True}) as run:
            result = run_archived(self.source, self.root / "output", model_endpoint="http://127.0.0.1:123/v1")
        self.assertEqual(result, {"done": True})
        verify.assert_called_once_with("http://127.0.0.1:123/v1", self.bundle["policy_manifest"])
        self.assertEqual(run.call_args.kwargs["model"], "pinned-remote-14b")
        self.assertEqual(run.call_args.kwargs["archive_bundle"], self.bundle)
        self.assertNotIn("archive_contract", run.call_args.kwargs)

    def test_full_archive_is_saved_before_first_policy_call_and_survives_source_removal(self):
        output = self.root / "run"
        calls = []
        def fake(prompt, *args, **kwargs):
            # All five artifacts must be loadable before any policy response.
            self.assertEqual(load_archive(output / "archive"), self.bundle)
            self.assertNotIn('"raw_response"', prompt)
            self.assertNotIn('"artifact_hashes"', prompt)
            calls.append(prompt)
            action = {"type": "triage", "role": "fast", "source_id": self.bundle["manifest"]["sources"][0]["id"],
                      "disposition": "ignore", "reason": "No research action selected in this fixture."}
            return {"events": [], "final": {"actions_json": json.dumps([action])}}
        with patch("environments.research_portfolio.local_model.verify_server_identity",
                   return_value={"served_model": "fixture", "model_files_verified": True}):
            summary = run_episode(self.bundle["manifest"], output, codex=True, model="fixture",
                                  policy_runner=fake, max_calls=1, archive_bundle=self.bundle,
                                  model_endpoint="http://127.0.0.1:123/v1", scheduled=True)
        self.assertEqual(len(calls), 1)
        shutil.rmtree(self.source)
        copied = load_archive(output / "archive")
        self.assertEqual(copied, self.bundle)
        self.assertEqual(len(list((output / "archive").iterdir())), 5)
        contract = json.loads((output / "contract.json").read_text())
        self.assertEqual(contract["archive_path"], "archive")
        self.assertEqual(contract["archive_contract_sha256"], digest(copied["contract"]))
        self.assertEqual(summary["archive_contract_sha256"], contract["archive_contract_sha256"])
        self.assertIn("collector.py", contract["implementation_sha256"])
        self.assertIn("experiment.py", contract["implementation_sha256"])
        self.assertTrue(summary["replay_integrity"]["model_files_verified"])
        self.assertFalse(copied["contract"]["replay_integrity"]["model_files_verified"])
        self.assertNotIn("artifact_hashes", summary["replay_integrity"])
        self.assertNotEqual(json.loads((output / "episode.json").read_text())["timeline"],
                            copied["manifest"]["timeline"])
        # A corrupted retained original fails admission even when the source
        # archive is no longer available for a fallback read.
        path = output / "archive" / "sources.json"
        value = json.loads(path.read_text())
        value["records"][0]["raw"]["title"] = "Edited after rollout"
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "artifact hashes"):
            load_archive(output / "archive")

    def test_bundle_or_episode_substitution_fails_before_creating_output(self):
        for mutation in ("contract", "episode", "policy"):
            with self.subTest(mutation=mutation):
                bundle = deepcopy(self.bundle)
                manifest = deepcopy(bundle["manifest"])
                policies = deepcopy(bundle["policy_manifest"])
                if mutation == "contract":
                    bundle["contract"]["replay_integrity"]["strict_point_in_time_eligible"] = True
                elif mutation == "episode":
                    manifest["sources"][0]["body"] = "Substituted observation"
                else:
                    policies["roles"]["fast"]["revision"] = "f" * 40
                output = self.root / mutation
                with self.assertRaises(ValueError):
                    run_episode(manifest, output, actions=[{"type": "stop", "role": "strong", "reason": "done"}],
                                archive_bundle=bundle, policy_manifest=policies)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
