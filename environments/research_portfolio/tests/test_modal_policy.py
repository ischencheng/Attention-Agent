"""No-cloud checks of the pinned remote policy contract and token transport."""
from copy import deepcopy
import hashlib
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from environments.research_portfolio import modal_policy as policy


def identity():
    manifest = policy.policy_manifest()
    return {"served_model": policy.SERVED_MODEL, "policy_manifest": manifest,
            "policy_manifest_sha256": policy.manifest_digest(manifest),
            "runtime": {"backend": "vllm", "gpu": "NVIDIA L40S", "gpu_count": 1,
                        "enable_thinking": False, "concurrency": 1, "model_files_verified": True,
                        "max_input_tokens": 24000, "max_output_tokens": 2048, "max_requests": 1,
                        "expires_at": int(time.time()) + 120}}


class ModalPolicyTests(unittest.TestCase):
    def test_manifest_pins_all_roles_without_inventing_a_training_cutoff(self):
        manifest = policy.policy_manifest()
        self.assertEqual(manifest["served_model"], policy.SERVED_MODEL)
        for role in ("fast", "mid", "strong"):
            item = manifest["roles"][role]
            self.assertEqual(item["revision"], "40c069824f4251a91eefaf281ebe4c544efd3e18")
            self.assertIsNone(item["knowledge_cutoff"])
            self.assertEqual(len(item["artifact_sha256"]), 15)
        manifest["roles"]["fast"]["artifact_sha256"].clear()
        self.assertTrue(manifest["roles"]["mid"]["artifact_sha256"])
        self.assertTrue(policy.policy_manifest()["roles"]["fast"]["artifact_sha256"])

    def test_artifact_verification_rejects_mutation_and_extra_adapter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "model.safetensors").write_bytes(b"test checkpoint")
            hashes = {"model.safetensors": hashlib.sha256(b"test checkpoint").hexdigest()}
            with patch.object(policy, "ARTIFACT_SHA256", hashes):
                policy.verify_artifacts(root)
                (root / "adapter_config.json").write_text("{}")
                with self.assertRaisesRegex(ValueError, "unexpected file"):
                    policy.verify_artifacts(root)
                (root / "adapter_config.json").unlink()
                (root / "model.safetensors").write_bytes(b"modified checkpoint")
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    policy.verify_artifacts(root)

    def test_limits_reject_unbounded_requests_and_context_overflow(self):
        policy.validate_limits(24000, 2048, 110)
        for limits in ((24000, 2048, 121), (30000, 4096, 110), (24000, 2048, True), (0, 2048, 1)):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                policy.validate_limits(*limits)

    def test_tokenizer_requests_flat_tokens_and_rejects_encodings_or_truncation(self):
        calls = []
        def apply(messages, **options):
            calls.append(options)
            return [12, 34, 56]
        tokenizer = SimpleNamespace(apply_chat_template=apply)
        self.assertEqual(policy.render_tokens(tokenizer, [], 3), [12, 34, 56])
        self.assertEqual(calls, [{"tokenize": True, "return_dict": False,
                                  "add_generation_prompt": True, "enable_thinking": False}])
        with self.assertRaisesRegex(ValueError, "truncation is forbidden"):
            policy.render_tokens(tokenizer, [], 2)
        for result in ({"input_ids": [12, 34]}, [[12, 34]], ["input_ids"], []):
            tokenizer = SimpleNamespace(apply_chat_template=lambda *args, **kwargs: result)
            with self.subTest(result=result), self.assertRaisesRegex(ValueError, "flat integer"):
                policy.render_tokens(tokenizer, [], 4)

    def test_wrong_checkpoint_hardware_or_budget_is_rejected_before_calls(self):
        original = identity()
        changes = [("served_model", "other"), ("policy_manifest_sha256", "0" * 64),
                   ("runtime", {**original["runtime"], "gpu": "NVIDIA A100"}),
                   ("runtime", {**original["runtime"], "max_requests": 120}),
                   ("runtime", {**original["runtime"], "expires_at": 0})]
        for key, value in changes:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                policy.ModalBackend(None, {**original, key: value}, max_input_tokens=24000,
                                    max_output_tokens=2048, max_requests=1)

    def test_transport_keeps_sampled_text_and_token_ids_and_enforces_request_cap(self):
        called = []
        record = identity()
        raw_text = '{"actions_json":"[invalid inner JSON"}'
        def remote(messages, max_output_tokens, schema):
            called.append((messages, max_output_tokens, schema))
            return {"text": raw_text, "token_ids": [12, 45, 98], "prompt_tokens": 9,
                    "output_tokens": 3, "finish_reason": "stop",
                    "policy_manifest_sha256": record["policy_manifest_sha256"]}
        backend = policy.ModalBackend(SimpleNamespace(generate=SimpleNamespace(remote=remote)), record,
                                      max_input_tokens=24000, max_output_tokens=2048, max_requests=1)
        schema = {"type": "object", "properties": {"actions_json": {"type": "string"}}}
        messages = [{"role": "user", "content": "Choose an action"}]
        chunks = list(backend.generate(messages, 100, schema=schema))
        self.assertEqual(called, [(messages, 100, schema)])
        self.assertEqual("".join(chunk.text for chunk in chunks), raw_text)
        self.assertEqual([chunk.token_id for chunk in chunks], [12, 45, 98])
        self.assertEqual(chunks[-1].finish_reason, "stop")
        self.assertEqual(chunks[-1].output_tokens, 3)
        with self.assertRaisesRegex(ValueError, "budget exhausted"):
            list(backend.generate(messages, 100, schema=schema))

    def test_remote_mismatch_or_incomplete_generation_is_never_repaired(self):
        record = identity()
        valid = {"text": '{"ok":', "token_ids": [4, 7], "prompt_tokens": 5,
                 "output_tokens": 2, "finish_reason": "length",
                 "policy_manifest_sha256": record["policy_manifest_sha256"]}
        for modified in (valid, {**valid, "policy_manifest_sha256": "0" * 64},
                         {**valid, "output_tokens": 3}):
            worker = SimpleNamespace(generate=SimpleNamespace(remote=lambda *args: deepcopy(modified)))
            backend = policy.ModalBackend(worker, record, max_input_tokens=24000,
                                          max_output_tokens=2048, max_requests=1)
            if modified == valid:
                chunks = list(backend.generate([{"role": "user", "content": "JSON"}], 2, schema={}))
                self.assertEqual(chunks[-1].finish_reason, "length")
                self.assertEqual("".join(chunk.text for chunk in chunks), '{"ok":')
            else:
                with self.assertRaises(ValueError):
                    list(backend.generate([{"role": "user", "content": "JSON"}], 2, schema={}))


if __name__ == "__main__":
    unittest.main()
