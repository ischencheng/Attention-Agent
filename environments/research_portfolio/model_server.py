"""Loopback Responses/SSE bridge for pinned policy backends used by Codex.

Only text, tool-disabled, stateless requests are supported. The bridge does not
execute environment actions or repair model output. Codex validates the final
JSON schema. This is inference instrumentation, not an RL trainer.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.metadata
import json
import os
from pathlib import Path
import threading
import time
import uuid

from .local_model import SERVED_MODEL, default_snapshot, manifest_sha256, verify_snapshot


MAX_BODY_BYTES = 2 * 1024 * 1024


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def request_messages(body, served_model=SERVED_MODEL):
    """Translate only supported text messages; never silently drop tool state."""
    if not isinstance(body, dict) or body.get("model") != served_model:
        raise ValueError("request must select the exact pinned served model")
    if body.get("tools") or body.get("previous_response_id") or body.get("conversation"):
        raise ValueError("only tool-disabled stateless requests are supported")
    if body.get("stream") is not True:
        raise ValueError("stream=true is required")
    instructions = body.get("instructions") or ""
    if not isinstance(instructions, str):
        raise ValueError("instructions must be text")
    fmt = body.get("text", {}).get("format", {})
    if fmt.get("type") == "json_schema":
        schema = fmt.get("schema")
        if not isinstance(schema, dict):
            raise ValueError("json_schema format requires a schema object")
        instructions += ("\nReturn only a JSON object satisfying this JSON schema. "
                         "Do not use Markdown fences, commentary, or reasoning tags.\n" +
                         json.dumps(schema, ensure_ascii=False, allow_nan=False))
    elif fmt.get("type") not in (None, "text"):
        raise ValueError("unsupported output format")
    messages = [{"role": "system", "content": instructions}] if instructions else []
    inputs = body.get("input")
    if isinstance(inputs, str):
        inputs = [{"role": "user", "content": inputs}]
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("nonempty input is required")
    for item in inputs:
        if not isinstance(item, dict) or item.get("type", "message") != "message":
            raise ValueError("only text message input items are supported")
        role = item.get("role")
        if role not in ("system", "developer", "user", "assistant"):
            raise ValueError("unsupported message role")
        content = item.get("content")
        if isinstance(content, list):
            if any(not isinstance(part, dict) or part.get("type") not in ("input_text", "output_text")
                   or not isinstance(part.get("text"), str) for part in content):
                raise ValueError("only text content parts are supported")
            content = "\n".join(part["text"] for part in content)
        if not isinstance(content, str):
            raise ValueError("message content must be text")
        messages.append({"role": "system" if role == "developer" else role, "content": content})
    return messages


@dataclass
class Chunk:
    text: str
    token_id: int
    prompt_tokens: int
    output_tokens: int
    finish_reason: str | None = None


class MLXBackend:
    """Load only the verified local snapshot; one request owns the model at once."""

    def __init__(self, snapshot, *, max_input_tokens=24000, max_output_tokens=1536, seed=0):
        self.served_model = SERVED_MODEL
        self.manifest = verify_snapshot(snapshot)
        if type(max_input_tokens) is not int or not 1 <= max_input_tokens <= 30000:
            raise ValueError("max_input_tokens must be between 1 and 30000")
        if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 4096:
            raise ValueError("max_output_tokens must be between 1 and 4096")
        if max_input_tokens + max_output_tokens > 32768:
            raise ValueError("input and output budgets exceed the native context window")
        if type(seed) is not int or seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        # No repository resolution, remote custom code, adapters, or credentials.
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        from mlx_lm import load
        self.model, self.tokenizer = load(str(Path(snapshot).resolve()),
                                         tokenizer_config={"local_files_only": True},
                                         trust_remote_code=False)
        self.max_input_tokens, self.max_output_tokens, self.seed = max_input_tokens, max_output_tokens, seed
        self.metadata = {"backend": "mlx", "enable_thinking": False, "temperature": 0.0,
                         "seed": seed, "max_input_tokens": max_input_tokens,
                         "max_output_tokens": max_output_tokens, "concurrency": 1,
                         "schema_enforcement": "prompted; caller validates output, no grammar decoder",
                         "packages": {name: importlib.metadata.version(name)
                                      for name in ("mlx", "mlx-lm", "transformers")}}

    def generate(self, messages, max_tokens, *, on_progress=None, schema=None):
        import mlx.core as mx
        from mlx_lm import stream_generate
        from mlx_lm.sample_utils import make_sampler
        prompt = self.tokenizer.apply_chat_template(messages, tokenize=True,
                                                   add_generation_prompt=True, enable_thinking=False)
        if len(prompt) > self.max_input_tokens:
            raise ValueError("model input exceeds the declared budget; truncation is forbidden")
        mx.random.seed(self.seed)
        for part in stream_generate(self.model, self.tokenizer, prompt=prompt,
                                    max_tokens=min(max_tokens, self.max_output_tokens),
                                    sampler=make_sampler(temp=0.0),
                                    prompt_progress_callback=on_progress):
            yield Chunk(part.text, int(part.token), int(part.prompt_tokens),
                        int(part.generation_tokens), part.finish_reason)


class ResponseServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, backend, audit_dir, *, max_requests=64):
        if address[0] != "127.0.0.1":
            raise ValueError("model server binds only 127.0.0.1")
        if type(max_requests) is not int or not 1 <= max_requests <= 1000:
            raise ValueError("max_requests must be between 1 and 1000")
        self.backend = backend
        self.audit_dir = Path(audit_dir)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        if any(self.audit_dir.iterdir()):
            raise ValueError("model server audit directory must be empty")
        self.generation_lock = threading.Lock()
        self.request_count, self.max_requests = 0, max_requests
        self.served_model = getattr(backend, "served_model", SERVED_MODEL)
        self.server_manifest = {"served_model": self.served_model, "policy_manifest": backend.manifest,
                                "policy_manifest_sha256": manifest_sha256(backend.manifest),
                                "runtime": backend.metadata}
        write_json(self.audit_dir / "model-manifest.json", self.server_manifest)
        super().__init__(address, ResponseHandler)


class ResponseHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass  # Requests are audited without headers, tokens, or credentials.

    def _json(self, status, payload):
        data = json.dumps(payload, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def _valid_origin(self):
        return (self.headers.get("Host") == f"127.0.0.1:{self.server.server_port}" and
                not self.headers.get("Origin"))

    def do_GET(self):
        if not self._valid_origin():
            return self._json(403, {"error": {"message": "loopback requests without browser Origin only"}})
        if self.path == "/health":
            return self._json(200, {**self.server.server_manifest, "request_count": self.server.request_count,
                                    "max_requests": self.server.max_requests})
        if self.path == "/v1/models":
            return self._json(200, {"object": "list", "data": [{"id": self.server.served_model, "object": "model"}]})
        return self._json(404, {"error": {"message": "unknown route"}})

    def do_POST(self):
        if not self._valid_origin():
            return self._json(403, {"error": {"message": "loopback requests without browser Origin only"}})
        if self.path != "/v1/responses":
            return self._json(404, {"error": {"message": "only /v1/responses is supported"}})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY_BYTES or self.headers.get("Content-Encoding", "identity") != "identity":
                raise ValueError("invalid size or unsupported request encoding")
            if self.headers.get_content_type() != "application/json":
                raise ValueError("application/json is required")
            body = json.loads(self.rfile.read(length))
            json.dumps(body, allow_nan=False)
            messages = request_messages(body, self.server.served_model)
            max_tokens = body.get("max_output_tokens", self.server.backend.max_output_tokens)
            if type(max_tokens) is not int or max_tokens < 1:
                raise ValueError("max_output_tokens must be a positive integer")
            max_tokens = min(max_tokens, self.server.backend.max_output_tokens)
        except (ValueError, TypeError, AttributeError) as exc:
            return self._json(400, {"error": {"message": str(exc), "type": "invalid_request_error"}})
        if not self.server.generation_lock.acquire(blocking=False):
            return self._json(429, {"error": {"message": "local model is busy"}})
        try:
            if self.server.request_count >= self.server.max_requests:
                return self._json(429, {"error": {"message": "model request budget exhausted"}})
            self.server.request_count += 1
            self._stream(body, messages, max_tokens)
        finally:
            self.server.generation_lock.release()

    def _stream(self, body, messages, max_tokens):
        response_id, item_id = "resp_" + uuid.uuid4().hex, "msg_" + uuid.uuid4().hex
        call_dir = self.server.audit_dir / f"call-{self.server.request_count:04d}"
        call_dir.mkdir()
        write_json(call_dir / "request.json", body)
        write_json(call_dir / "messages.json", messages)
        response = {"id": response_id, "object": "response", "created_at": int(time.time()),
                    "model": self.server.served_model, "status": "in_progress", "output": [],
                    "error": None, "incomplete_details": None, "usage": None}
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        sequence = 0
        last_keepalive = time.monotonic()

        def emit(kind, **fields):
            nonlocal sequence
            event = {"type": kind, "sequence_number": sequence, **fields}
            sequence += 1
            data = json.dumps(event, ensure_ascii=False, allow_nan=False)
            self.wfile.write(f"event: {kind}\ndata: {data}\n\n".encode())
            self.wfile.flush()

        def keepalive(*_):
            nonlocal last_keepalive
            if time.monotonic() - last_keepalive >= 5:
                self.wfile.write(b": prefill-in-progress\n\n")
                self.wfile.flush()
                last_keepalive = time.monotonic()

        text, token_ids, last = "", [], None
        started = time.monotonic()
        failure = None
        try:
            emit("response.created", response=response)
            emit("response.in_progress", response=response)
            item = {"id": item_id, "type": "message", "role": "assistant", "status": "in_progress", "content": []}
            emit("response.output_item.added", output_index=0, item=item)
            emit("response.content_part.added", item_id=item_id, output_index=0, content_index=0,
                 part={"type": "output_text", "text": "", "annotations": [], "logprobs": []})
            schema = body.get("text", {}).get("format", {}).get("schema")
            for last in self.server.backend.generate(messages, max_tokens, on_progress=keepalive, schema=schema):
                text += last.text
                token_ids.append(last.token_id)
                if last.text:
                    emit("response.output_text.delta", item_id=item_id, output_index=0, content_index=0,
                         delta=last.text, logprobs=[])
            if last is None or last.finish_reason not in ("stop", "length"):
                raise RuntimeError("generation did not provide a terminal status")
            part = {"type": "output_text", "text": text, "annotations": [], "logprobs": []}
            item.update(status="completed", content=[part])
            emit("response.output_text.done", item_id=item_id, output_index=0, content_index=0, text=text, logprobs=[])
            emit("response.content_part.done", item_id=item_id, output_index=0, content_index=0, part=part)
            emit("response.output_item.done", output_index=0, item=item)
            response.update(output=[item], status="completed" if last.finish_reason == "stop" else "incomplete",
                            usage={"input_tokens": last.prompt_tokens, "output_tokens": last.output_tokens,
                                   "total_tokens": last.prompt_tokens + last.output_tokens,
                                   "input_tokens_details": {"cached_tokens": 0},
                                   "output_tokens_details": {"reasoning_tokens": 0}})
            if last.finish_reason == "length":
                response["incomplete_details"] = {"reason": "max_output_tokens"}
            emit("response.completed" if last.finish_reason == "stop" else "response.incomplete", response=response)
        except (BrokenPipeError, ConnectionResetError):
            failure = "client_disconnected"
        except Exception as exc:
            failure = f"{type(exc).__name__}: {exc}"
            response.update(status="failed", error={"code": "generation_failed", "message": failure})
            try:
                emit("response.failed", response=response)
            except (BrokenPipeError, ConnectionResetError):
                pass
        finally:
            write_json(call_dir / "generation.json", {
                "response_id": response_id, "served_model": self.server.served_model,
                "policy_manifest_sha256": self.server.server_manifest["policy_manifest_sha256"],
                "text": text, "generated_token_ids": token_ids,
                "logprobs": None, "training_performed": False,
                "max_output_tokens": max_tokens,
                "finish_reason": last.finish_reason if last else None,
                "failure": failure, "usage": response["usage"],
                "wall_seconds": round(time.monotonic() - started, 3),
            })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("modal", "mlx"), default="modal")
    parser.add_argument("--snapshot", type=Path, default=default_snapshot())
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--max-input-tokens", type=int, default=24000)
    parser.add_argument("--max-output-tokens", type=int, default=1536)
    parser.add_argument("--max-requests", type=int, default=64)
    args = parser.parse_args()
    if args.backend == "modal":
        from .modal_policy import remote_backend
        context = remote_backend(max_input_tokens=args.max_input_tokens,
                                 max_output_tokens=args.max_output_tokens, max_requests=args.max_requests)
    else:
        context = nullcontext(MLXBackend(args.snapshot, max_input_tokens=args.max_input_tokens,
                                        max_output_tokens=args.max_output_tokens))
    with context as backend:
        server = ResponseServer(("127.0.0.1", args.port), backend, args.audit_dir,
                                max_requests=args.max_requests)
        endpoint = f"http://127.0.0.1:{server.server_port}"
        write_json(args.audit_dir / "server.json", {"base_url": endpoint + "/v1", "pid": os.getpid(),
                                                    **server.server_manifest})
        print(json.dumps({"base_url": endpoint + "/v1", "served_model": server.served_model}), flush=True)
        try:
            server.serve_forever()
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
