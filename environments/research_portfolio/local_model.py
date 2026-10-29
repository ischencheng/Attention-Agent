"""Identity and Codex configuration for a local, pre-episode checkpoint.

No model is loaded on import. Artifact hashes pin the complete inference input,
including the tokenizer and template, rather than trusting a model alias.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener


MODEL_ID = "mlx-community/Qwen3-4B-4bit"
REVISION = "4dcb3d101c2a062e5c1d4bb173588c54ea6c4d25"
SERVED_MODEL = "attention-qwen3-4b-4bit-4dcb3d1"
ARTIFACT_SHA256 = {
    "added_tokens.json": "c0284b582e14987fbd3d5a2cb2bd139084371ed9acbae488829a1c900833c680",
    "config.json": "b5efdcf3b0035a3638e7228dad4d85f5c4a23f156eb7cdb0b44c8366a5d34d9b",
    "merges.txt": "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5",
    "model.safetensors": "e240c0bdc0ebb0681bf0da0f98d9719fd6ebe269a3633f81542c13e81345651d",
    "model.safetensors.index.json": "f7825defe5865d179c3b593173d37056be5f202dcb7153985cf74e75ecf1628b",
    "special_tokens_map.json": "76862e765266b85aa9459767e33cbaf13970f327a0e88d1c65846c2ddd3a1ecd",
    "tokenizer.json": "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
    "tokenizer_config.json": "253153d0738ceb4c668d2eff957714dd2bea0b56de772a9fdccd96cbf517e6a0",
    "vocab.json": "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910",
}


def default_snapshot():
    return Path.home() / ".cache/huggingface/hub" / ("models--" + MODEL_ID.replace("/", "--")) / "snapshots" / REVISION


def policy_manifest():
    """Declared identity; ``verify_snapshot`` must pass before serving it."""
    identity = {
        "model_id": MODEL_ID, "revision": REVISION,
        "released_at": "2025-04-29T00:00:00Z",
        "release_evidence_url": "https://qwenlm.github.io/blog/qwen3/",
        "revision_evidence_url": f"https://huggingface.co/{MODEL_ID}/commit/{REVISION}",
        "knowledge_cutoff": None,
        "temporal_basis": "checkpoint_published_before_episode",
        "artifact_sha256": deepcopy(ARTIFACT_SHA256),
    }
    return {"schema_version": 1, "roles": {role: deepcopy(identity) for role in ("fast", "mid", "strong")}}


def verify_snapshot(path):
    """Fail closed on modified or additional inference files; never download."""
    path = Path(path).resolve(strict=True)
    if not path.is_dir():
        raise ValueError("model snapshot must be a local directory")
    allowed = set(ARTIFACT_SHA256) | {".gitattributes", "README.md", "LICENSE"}
    if any(item.name not in allowed for item in path.iterdir()):
        raise ValueError("unregistered files in pinned model snapshot")
    for name, expected in ARTIFACT_SHA256.items():
        digest = hashlib.sha256()
        with (path / name).open("rb") as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected:
            raise ValueError(f"pinned model artifact differs: {name}")
    return policy_manifest()


def manifest_sha256(manifest):
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def codex_provider_config(base_url):
    """Narrow, unauthenticated loopback provider; caller retains Codex sandbox."""
    parsed = urlsplit(base_url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or
            not parsed.port or parsed.path != "/v1" or parsed.query or parsed.fragment or
            parsed.username or parsed.password):
        raise ValueError("local model URL must be http://127.0.0.1:<port>/v1")
    return {
        "model_provider": "attention_local",
        "model_providers.attention_local": {
            "name": "Attention local pinned policy", "base_url": base_url,
            "wire_api": "responses", "requires_openai_auth": False,
            "supports_websockets": False, "request_max_retries": 0,
            "stream_max_retries": 0, "stream_idle_timeout_ms": 120000,
        },
        "model_context_window": 32768,
        "model_auto_compact_token_limit": 30000,
    }


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("local model identity endpoint must not redirect")


def verify_server_identity(base_url, expected_manifest, *, timeout=5):
    """Bind declared role identities to the actual local serving process.

    This validates the serving contract, not remote attestation. The maintained
    bridge itself checks artifact bytes before loading the model.
    """
    codex_provider_config(base_url)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("identity timeout must be finite and positive")
    opener = build_opener(ProxyHandler({}), _NoRedirects())
    with opener.open(base_url.removesuffix("/v1") + "/health", timeout=timeout) as response:
        payload = response.read(256 * 1024 + 1)
    if len(payload) > 256 * 1024:
        raise ValueError("oversized model identity response")
    health = json.loads(payload)
    expected_hash = manifest_sha256(expected_manifest)
    expected_model = expected_manifest.get("served_model", SERVED_MODEL)
    if (health.get("served_model") != expected_model or
            health.get("policy_manifest") != expected_manifest or
            health.get("policy_manifest_sha256") != expected_hash):
        raise ValueError("served model identity differs from the archived policy manifest")
    runtime = health.get("runtime", {})
    if runtime.get("backend") not in {"mlx", "vllm"} or runtime.get("enable_thinking") is not False or runtime.get("concurrency") != 1:
        raise ValueError("model runtime does not satisfy the declared policy contract")
    if runtime["backend"] == "vllm" and (runtime.get("model_files_verified") is not True
                                            or not runtime.get("gpu")):
        raise ValueError("remote model runtime requires verified artifacts and GPU identity")
    verification = ("Trusted remote GPU worker verifies artifact bytes before loading; the authenticated bridge checks the full identity for every invocation. This is not hardware attestation."
                    if runtime["backend"] == "vllm" else
                    "Trusted local bridge verifies artifact bytes at startup; identity is checked for every invocation, not remote attestation.")
    return {"base_url": base_url, "served_model": expected_model,
            "policy_manifest_sha256": expected_hash, "runtime": runtime,
            "model_files_verified": True,
            "verification_scope": verification,
            "request_count": health.get("request_count"), "max_requests": health.get("max_requests")}
