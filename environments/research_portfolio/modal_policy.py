"""Bounded, authenticated GPU inference for the Codex replay bridge.

The checkpoint is public and baked into a CPU-built image. The GPU worker has
no network access, no public web endpoint, no tools and no archive files. Modal
is imported only when explicitly starting the remote backend.
"""
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import threading
import time


APP_NAME = "attention-replay-qwen3-14b-l40s"
MODEL_ID = "Qwen/Qwen3-14B"
REVISION = "40c069824f4251a91eefaf281ebe4c544efd3e18"
SERVED_MODEL = "attention-qwen3-14b-40c0698"
VLLM_VERSION = "0.32.0"
MODEL_DIR = "/opt/attention-model"
MAX_LIFETIME_SECONDS = 45 * 60
ARTIFACT_SHA256 = {
    "config.json": "e73c3664ca09b10a673fef0c22e8a6b456201d49bd4713c9691f775720e8857a",
    "generation_config.json": "2325da0f15bb848e018c5ae071b7943332e9f871d6b60e2ed22ca97d4cb993d2",
    "merges.txt": "8831e4f1a044471340f7c0a83d7bd71306a5b867e95fd870f74d0c5308a904d5",
    "model-00001-of-00008.safetensors": "e942bdbdf08857d16a8fef7d1dae9fceabeb4e84def6043485fe2f6f085dab0e",
    "model-00002-of-00008.safetensors": "f7c9c6eee628f5ad831d2d1d292e120505e5fcadeb38f88b4d3c4cb86306ccf9",
    "model-00003-of-00008.safetensors": "dfb8c5df9404b41ad6ae74e8b6b367135f017b4467b884cf71b17c71954f18a9",
    "model-00004-of-00008.safetensors": "eab286fec759e3e59ab228621aefa0fef14ed56039e06f959e67257d5af7604d",
    "model-00005-of-00008.safetensors": "97f0dc2992e59da95c466eff6f4fd0c8335843bbc36ed5c913a6f5150748c0e6",
    "model-00006-of-00008.safetensors": "9e8e76a013cd5e253865b792991e0b410f869b136b3c500079b531b09198e99e",
    "model-00007-of-00008.safetensors": "0aee70ee6e91dc00d818804fb47f124d13ee4ad5b4a64553e09dbf9391cd5750",
    "model-00008-of-00008.safetensors": "0d6b92296e326d39bbbaeb32c3ec454ac606da843d4c8ffa8edf010b62b8c9e0",
    "model.safetensors.index.json": "62d7ad35757bae5e7baa452cb1483178b7daa50e869e923226b8da10871f7ebc",
    "tokenizer.json": "aeb13307a71acd8fe81861d94ad54ab689df773318809eed3cbe794b4492dae4",
    "tokenizer_config.json": "d5d09f07b48c3086c508b30d1c9114bd1189145b74e982a265350c923acd8101",
    "vocab.json": "ca10d7e9fb3ed18575dd1e277a2579c16d108e32f27439684afa0e10b1440910",
}


def policy_manifest():
    identity = {
        "model_id": MODEL_ID, "revision": REVISION,
        "released_at": "2025-07-26T03:49:23Z",
        "release_evidence_url": "https://qwenlm.github.io/blog/qwen3/",
        "revision_evidence_url": f"https://huggingface.co/{MODEL_ID}/commit/{REVISION}",
        "knowledge_cutoff": None, "temporal_basis": "checkpoint_published_before_episode",
        "artifact_sha256": deepcopy(ARTIFACT_SHA256),
    }
    return {"schema_version": 1, "served_model": SERVED_MODEL,
            "roles": {role: deepcopy(identity) for role in ("fast", "mid", "strong")}}


def manifest_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def verify_artifacts(path):
    root = Path(path)
    allowed = set(ARTIFACT_SHA256) | {"LICENSE", ".cache"}
    if any(item.name not in allowed for item in root.iterdir()):
        raise ValueError("unexpected file in pinned GPU checkpoint")
    for name, expected in ARTIFACT_SHA256.items():
        digest = hashlib.sha256()
        with (root / name).open("rb") as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected:
            raise ValueError(f"GPU checkpoint hash mismatch: {name}")
    return policy_manifest()


def download_model():
    """Run during CPU image construction, before any billable GPU allocation."""
    from huggingface_hub import snapshot_download
    snapshot_download(MODEL_ID, revision=REVISION, local_dir=MODEL_DIR,
                      allow_patterns=[*ARTIFACT_SHA256, "LICENSE"], token=False,
                      max_workers=8)
    verify_artifacts(MODEL_DIR)


def validate_limits(max_input_tokens, max_output_tokens, max_requests):
    for name, value, upper in (("max_input_tokens", max_input_tokens, 30000),
                               ("max_output_tokens", max_output_tokens, 4096),
                               ("max_requests", max_requests, 120)):
        if type(value) is not int or not 1 <= value <= upper:
            raise ValueError(f"{name} must be between 1 and {upper}")
    if max_input_tokens + max_output_tokens > 32768:
        raise ValueError("input and output budgets exceed the native context window")


def render_tokens(tokenizer, messages, max_input_tokens):
    # Transformers 5 can default to BatchEncoding. vLLM's TokensPrompt requires
    # the flat sequence, not iteration over the encoding's dictionary keys.
    prompt = tokenizer.apply_chat_template(messages, tokenize=True, return_dict=False,
                                           add_generation_prompt=True, enable_thinking=False)
    if not isinstance(prompt, list) or not prompt or any(type(item) is not int for item in prompt):
        raise ValueError("tokenizer did not return a flat integer token sequence")
    if len(prompt) > max_input_tokens:
        raise ValueError("model input exceeds the declared budget; truncation is forbidden")
    return prompt


def build_app():
    """Construct SDK objects without creating a deployment or public endpoint."""
    import modal
    cached_image = os.environ.get("ATTENTION_MODAL_MODEL_IMAGE_ID")
    if cached_image:
        # Reuse a previously built private image without downloading weights
        # again. Startup still checks every checkpoint byte against the pin.
        image = modal.Image.from_id(cached_image)
    else:
        image = (modal.Image.from_registry(f"vllm/vllm-openai:v{VLLM_VERSION}", add_python="3.12")
                 .entrypoint([])
                 .env({"PYTHONPATH": "/usr/local/lib/python3.12/dist-packages:/usr/local/lib/python3.12/site-packages:/root"})
                 .add_local_file(__file__, "/root/environments/research_portfolio/modal_policy.py", copy=True)
                 .run_function(download_model, cpu=4, memory=8192, timeout=1800, include_source=False)
                 .env({"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                       "VLLM_NO_USAGE_STATS": "1", "DO_NOT_TRACK": "1"}))
    image = image.add_local_file(__file__, "/root/environments/research_portfolio/modal_policy.py", copy=True)
    app = modal.App(APP_NAME)

    @app.cls(image=image, gpu="L40S", cpu=4, memory=65536, max_containers=1,
             min_containers=0, buffer_containers=0, scaledown_window=120,
             timeout=300, startup_timeout=600, retries=0, block_network=True,
             serialized=True, include_source=False)
    class RemotePolicy:
        expires_at: int = modal.parameter()
        input_limit: int = modal.parameter(default=24000)
        output_limit: int = modal.parameter(default=2048)
        request_limit: int = modal.parameter(default=120)

        @modal.enter()
        def load(self):
            import torch
            from vllm import LLM
            validate_limits(self.input_limit, self.output_limit, self.request_limit)
            remaining = self.expires_at - time.time()
            if not 0 < remaining <= MAX_LIFETIME_SECONDS:
                raise ValueError("GPU worker deadline is expired or exceeds 45 minutes")
            # This kills a stuck generation as well as rejecting late requests.
            self.watchdog = threading.Timer(remaining, lambda: os._exit(124))
            self.watchdog.daemon = True
            self.watchdog.start()
            self.manifest = verify_artifacts(MODEL_DIR)
            gpu = torch.cuda.get_device_name(0)
            if torch.cuda.device_count() != 1 or "L40S" not in gpu:
                raise ValueError("replay requires exactly one NVIDIA L40S")
            self.llm = LLM(model=MODEL_DIR, tokenizer=MODEL_DIR, dtype="bfloat16",
                           trust_remote_code=False, max_model_len=32768,
                           max_num_seqs=1, max_num_batched_tokens=4096,
                           gpu_memory_utilization=0.92, enforce_eager=True, seed=0)
            self.tokenizer = self.llm.get_tokenizer()
            self.requests = 0
            self.runtime = {
                "backend": "vllm", "gpu": gpu, "gpu_count": 1,
                "enable_thinking": False, "concurrency": 1, "temperature": 0.0,
                "seed": 0, "max_input_tokens": self.input_limit,
                "max_output_tokens": self.output_limit, "max_requests": self.request_limit,
                "expires_at": self.expires_at, "model_files_verified": True,
                "schema_enforcement": "vLLM structured JSON schema decoding; no output repair",
                "transport": "authenticated Modal SDK method; no public endpoint",
                "packages": {name: importlib.metadata.version(name)
                             for name in ("vllm", "torch", "transformers", "huggingface-hub")},
            }

        @modal.method()
        def identity(self):
            if time.time() >= self.expires_at:
                raise ValueError("GPU worker deadline exhausted")
            return {"served_model": SERVED_MODEL, "policy_manifest": self.manifest,
                    "policy_manifest_sha256": manifest_digest(self.manifest), "runtime": self.runtime}

        @modal.method()
        def generate(self, messages, max_output_tokens, schema):
            from vllm import SamplingParams
            from vllm.sampling_params import StructuredOutputsParams
            if time.time() >= self.expires_at or self.requests >= self.request_limit:
                raise ValueError("GPU policy request or time budget exhausted")
            self.requests += 1
            if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= self.output_limit:
                raise ValueError("invalid GPU output token budget")
            if not isinstance(schema, dict):
                raise ValueError("GPU policy requires an explicit JSON schema")
            if (not isinstance(messages, list) or not messages or any(
                    not isinstance(item, dict) or set(item) != {"role", "content"}
                    or item["role"] not in {"system", "user", "assistant"}
                    or not isinstance(item["content"], str) for item in messages)):
                raise ValueError("GPU policy accepts text messages only")
            prompt = render_tokens(self.tokenizer, messages, self.input_limit)
            sampled = SamplingParams(temperature=0, seed=0, max_tokens=max_output_tokens,
                                     structured_outputs=StructuredOutputsParams(json=schema))
            result = self.llm.generate({"prompt_token_ids": prompt}, sampled, use_tqdm=False)[0]
            output = result.outputs[0]
            return {"text": output.text, "token_ids": list(output.token_ids),
                    "prompt_tokens": len(result.prompt_token_ids), "output_tokens": len(output.token_ids),
                    "finish_reason": output.finish_reason,
                    "policy_manifest_sha256": manifest_digest(self.manifest),
                    "request_number": self.requests, "runtime": self.runtime}

    return app, RemotePolicy


class ModalBackend:
    """Translate authenticated remote results into the existing SSE adapter."""

    def __init__(self, worker, identity, *, max_input_tokens, max_output_tokens, max_requests):
        validate_limits(max_input_tokens, max_output_tokens, max_requests)
        expected = policy_manifest()
        if (identity.get("served_model") != SERVED_MODEL or identity.get("policy_manifest") != expected
                or identity.get("policy_manifest_sha256") != manifest_digest(expected)):
            raise ValueError("GPU worker identity differs from the pinned policy")
        runtime = identity.get("runtime", {})
        if (runtime.get("backend") != "vllm" or runtime.get("model_files_verified") is not True
                or runtime.get("enable_thinking") is not False or runtime.get("concurrency") != 1
                or runtime.get("gpu_count") != 1 or "L40S" not in runtime.get("gpu", "")):
            raise ValueError("GPU worker runtime does not satisfy the inference contract")
        if (runtime.get("max_input_tokens") != max_input_tokens
                or runtime.get("max_output_tokens") != max_output_tokens
                or runtime.get("max_requests") != max_requests
                or type(runtime.get("expires_at")) is not int
                or not 0 < runtime["expires_at"] - time.time() <= MAX_LIFETIME_SECONDS):
            raise ValueError("GPU worker limits differ from the requested budget")
        self.worker, self.manifest, self.metadata = worker, expected, deepcopy(runtime)
        self.served_model = SERVED_MODEL
        self.max_input_tokens, self.max_output_tokens = max_input_tokens, max_output_tokens
        self.max_requests, self.requests = max_requests, 0

    def generate(self, messages, max_tokens, *, on_progress=None, schema=None):
        from .model_server import Chunk
        if self.requests >= self.max_requests or time.time() >= self.metadata["expires_at"]:
            raise ValueError("GPU policy request or time budget exhausted")
        if not isinstance(schema, dict):
            raise ValueError("GPU policy requires an explicit JSON schema")
        self.requests += 1
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.worker.generate.remote, messages,
                                     min(max_tokens, self.max_output_tokens), schema)
            while True:
                try:
                    result = future.result(timeout=1)
                    break
                except FutureTimeout:
                    if on_progress:
                        on_progress()
        if result.get("policy_manifest_sha256") != manifest_digest(self.manifest):
            raise ValueError("GPU response came from a different checkpoint")
        tokens = result.get("token_ids")
        if (not isinstance(tokens, list) or not tokens or any(type(token) is not int for token in tokens)
                or result.get("output_tokens") != len(tokens) or not isinstance(result.get("text"), str)
                or result.get("finish_reason") not in {"stop", "length"}):
            raise ValueError("GPU worker returned malformed generation data")
        for index, token in enumerate(tokens):
            yield Chunk(result["text"] if index == 0 else "", token,
                        result["prompt_tokens"], index + 1,
                        result["finish_reason"] if index == len(tokens) - 1 else None)


@contextmanager
def remote_backend(*, max_input_tokens=24000, max_output_tokens=2048, max_requests=120):
    """Own one ephemeral app; exiting this context stops its GPU worker."""
    import modal
    validate_limits(max_input_tokens, max_output_tokens, max_requests)
    app, worker_class = build_app()
    with modal.enable_output(), app.run():
        expires_at = int(os.environ.get("ATTENTION_MODAL_DEADLINE", int(time.time()) + MAX_LIFETIME_SECONDS))
        if not 0 < expires_at - time.time() <= MAX_LIFETIME_SECONDS:
            raise ValueError("GPU deadline is expired or exceeds 45 minutes")
        worker = worker_class(expires_at=expires_at,
                              input_limit=max_input_tokens, output_limit=max_output_tokens,
                              request_limit=max_requests)
        identity = worker.identity.remote()
        backend = ModalBackend(worker, identity, max_input_tokens=max_input_tokens,
                               max_output_tokens=max_output_tokens, max_requests=max_requests)
        backend.metadata["app_id"] = app.app_id
        yield backend


if __name__ == "__main__":
    with remote_backend(max_output_tokens=128, max_requests=2) as backend:
        schema = {"type": "object", "properties": {"ok": {"type": "boolean"}},
                  "required": ["ok"], "additionalProperties": False}
        parts = list(backend.generate([{"role": "user", "content": "Return {\"ok\":true}."}], 128, schema=schema))
        result = "".join(part.text for part in parts)
        if json.loads(result) != {"ok": True}:
            raise ValueError("GPU smoke returned unexpected output")
        print(json.dumps({"smoke": "passed", "result": result,
                          "prompt_tokens": parts[-1].prompt_tokens,
                          "output_tokens": parts[-1].output_tokens,
                          "model": backend.served_model, "runtime": backend.metadata}))
