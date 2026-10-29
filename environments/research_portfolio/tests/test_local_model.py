import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from environments.research_portfolio import local_model


class LocalModelTests(unittest.TestCase):
    def test_role_identity_is_complete_and_independent(self):
        manifest = local_model.policy_manifest()
        self.assertEqual(set(manifest["roles"]), {"fast", "mid", "strong"})
        first = manifest["roles"]["fast"]
        self.assertEqual(first["revision"], local_model.REVISION)
        self.assertIsNone(first["knowledge_cutoff"])
        first["artifact_sha256"].clear()
        self.assertTrue(manifest["roles"]["strong"]["artifact_sha256"])

    def test_snapshot_bytes_are_checked_including_template_and_weights(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            data = {"model.safetensors": b"weights", "tokenizer_config.json": b"template"}
            hashes = {name: hashlib.sha256(value).hexdigest() for name, value in data.items()}
            for name, value in data.items():
                (path / name).write_bytes(value)
            with patch.object(local_model, "ARTIFACT_SHA256", hashes):
                local_model.verify_snapshot(path)
                (path / "tokenizer_config.json").write_text("changed")
                with self.assertRaisesRegex(ValueError, "tokenizer_config.json"):
                    local_model.verify_snapshot(path)
                (path / "tokenizer_config.json").write_bytes(data["tokenizer_config.json"])
                (path / "adapter_config.json").write_text("{}")
                with self.assertRaisesRegex(ValueError, "unregistered"):
                    local_model.verify_snapshot(path)

    def test_provider_is_loopback_only_and_does_not_weaken_sandbox(self):
        config = local_model.codex_provider_config("http://127.0.0.1:12345/v1")
        provider = config["model_providers.attention_local"]
        self.assertFalse(provider["requires_openai_auth"])
        self.assertFalse(provider["supports_websockets"])
        self.assertNotIn("sandbox", repr(config))
        for url in ["https://example.com/v1", "http://localhost:123/v1", "http://127.0.0.1/v1",
                    "http://127.0.0.1:123/v1?key=x", "http://user:pass@127.0.0.1:123/v1"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                local_model.codex_provider_config(url)

    def test_live_identity_binding_rejects_a_different_role_or_runtime(self):
        manifest = local_model.policy_manifest()
        health = {"served_model": local_model.SERVED_MODEL, "policy_manifest": manifest,
                  "policy_manifest_sha256": local_model.manifest_sha256(manifest),
                  "runtime": {"backend": "mlx", "enable_thinking": False, "concurrency": 1}}
        opener = Mock()
        with patch.object(local_model, "build_opener", return_value=opener):
            opener.open.return_value = io.BytesIO(json.dumps(health).encode())
            result = local_model.verify_server_identity("http://127.0.0.1:123/v1", manifest)
            self.assertEqual(result["served_model"], local_model.SERVED_MODEL)
            for change in [{"served_model": "other"}, {"policy_manifest_sha256": "0" * 64},
                           {"runtime": {"backend": "hosted", "enable_thinking": False, "concurrency": 1}}]:
                opener.open.return_value = io.BytesIO(json.dumps({**health, **change}).encode())
                with self.subTest(change=change), self.assertRaises(ValueError):
                    local_model.verify_server_identity("http://127.0.0.1:123/v1", manifest)

    def test_remote_identity_requires_actual_gpu_and_verified_model_files(self):
        manifest = {**local_model.policy_manifest(), "served_model": "pinned-remote-model"}
        runtime = {"backend": "vllm", "gpu": "NVIDIA L40S", "model_files_verified": True,
                   "enable_thinking": False, "concurrency": 1}
        health = {"served_model": manifest["served_model"], "policy_manifest": manifest,
                  "policy_manifest_sha256": local_model.manifest_sha256(manifest), "runtime": runtime}
        opener = Mock()
        with patch.object(local_model, "build_opener", return_value=opener):
            opener.open.return_value = io.BytesIO(json.dumps(health).encode())
            result = local_model.verify_server_identity("http://127.0.0.1:123/v1", manifest)
            self.assertEqual(result["served_model"], "pinned-remote-model")
            self.assertIn("remote GPU", result["verification_scope"])
            for change in ({"gpu": None}, {"model_files_verified": False}):
                opener.open.return_value = io.BytesIO(json.dumps({**health, "runtime": {**runtime, **change}}).encode())
                with self.assertRaisesRegex(ValueError, "verified artifacts and GPU identity"):
                    local_model.verify_server_identity("http://127.0.0.1:123/v1", manifest)


if __name__ == "__main__":
    unittest.main()
