import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from environments.research_portfolio.local_model import SERVED_MODEL, policy_manifest
from environments.research_portfolio.model_server import Chunk, ResponseServer, request_messages


class StubBackend:
    manifest = policy_manifest()
    metadata = {"backend": "protocol_test_stub", "concurrency": 1}
    max_output_tokens = 32

    def __init__(self):
        self.calls = []
        self.finish = "stop"
        self.fail = False

    def generate(self, messages, max_tokens, *, on_progress=None):
        self.calls.append((messages, max_tokens))
        if self.fail:
            raise RuntimeError("test generation failure")
        yield Chunk('{"actions_json":', 11, 20, 1)
        yield Chunk('"[]"}', 12, 20, 2, self.finish)


class MessageContractTests(unittest.TestCase):
    def test_schema_and_text_preserved_without_executing_tools(self):
        schema = {"type": "object", "properties": {"actions_json": {"type": "string"}}}
        messages = request_messages({"model": SERVED_MODEL, "stream": True, "instructions": "system",
                                     "text": {"format": {"type": "json_schema", "schema": schema}},
                                     "input": [{"role": "developer", "content": [{"type": "input_text", "text": "policy"}]},
                                               {"role": "user", "content": "observation"}]})
        self.assertIn(json.dumps(schema), messages[0]["content"])
        self.assertEqual(messages[-1], {"role": "user", "content": "observation"})
        self.assertEqual(messages[1]["role"], "system")

    def test_unsupported_input_cannot_be_silently_dropped(self):
        valid = {"model": SERVED_MODEL, "stream": True, "input": "visible observation"}
        changes = [{"model": "other-model"}, {"tools": [{"type": "web_search"}]},
                   {"previous_response_id": "prior"}, {"stream": False},
                   {"input": [{"type": "function_call_output", "call_id": "a", "output": "future"}]},
                   {"input": [{"role": "user", "content": [{"type": "input_image", "image_url": "future"}]}]}]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                request_messages({**valid, **change})


class ServerProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = StubBackend()
        self.server = ResponseServer(("127.0.0.1", 0), self.backend, Path(self.temp.name) / "audit", max_requests=2)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def post(self, body=None, headers=None):
        body = body or {"model": SERVED_MODEL, "input": "visible", "stream": True, "max_output_tokens": 1000}
        request = Request(self.url + "/v1/responses", data=json.dumps(body).encode(),
                          headers={"Content-Type": "application/json", **(headers or {})})
        with urlopen(request, timeout=3) as response:
            payload = response.read().decode()
        return [json.loads(line[6:]) for line in payload.splitlines() if line.startswith("data: ")]

    def test_sse_completion_and_audit_match_actual_output(self):
        events = self.post()
        self.assertEqual(events[0]["type"], "response.created")
        self.assertEqual(events[-1]["type"], "response.completed")
        self.assertEqual([event["sequence_number"] for event in events], list(range(len(events))))
        final = events[-1]["response"]
        self.assertEqual(final["output"][0]["content"][0]["text"], '{"actions_json":"[]"}')
        self.assertEqual(final["usage"]["total_tokens"], 22)
        self.assertEqual(self.backend.calls[0][1], 32)
        audit = json.loads((self.server.audit_dir / "call-0001/generation.json").read_text())
        self.assertEqual(audit["generated_token_ids"], [11, 12])
        self.assertIsNone(audit["logprobs"])
        self.assertFalse(audit["training_performed"])
        self.assertEqual(audit["text"], final["output"][0]["content"][0]["text"])

    def test_token_limit_is_incomplete_not_success(self):
        self.backend.finish = "length"
        final = self.post()[-1]
        self.assertEqual(final["type"], "response.incomplete")
        self.assertEqual(final["response"]["incomplete_details"]["reason"], "max_output_tokens")

    def test_backend_failure_is_a_failed_response(self):
        self.backend.fail = True
        final = self.post()[-1]
        self.assertEqual(final["type"], "response.failed")
        audit = json.loads((self.server.audit_dir / "call-0001/generation.json").read_text())
        self.assertIn("test generation failure", audit["failure"])

    def test_second_active_generation_and_exhausted_budget_are_rejected(self):
        self.server.generation_lock.acquire()
        try:
            with self.assertRaises(HTTPError) as caught:
                self.post()
            self.assertEqual(caught.exception.code, 429)
        finally:
            self.server.generation_lock.release()
        self.post()
        self.post()
        with self.assertRaises(HTTPError) as caught:
            self.post()
        self.assertEqual(caught.exception.code, 429)
        self.assertEqual(len(self.backend.calls), 2)

    def test_external_tools_and_browser_origin_are_rejected_before_inference(self):
        for body, headers in [({"model": SERVED_MODEL, "stream": True, "input": "x", "tools": [{"type": "web_search"}]}, {}),
                              (None, {"Origin": "https://example.com"})]:
            with self.assertRaises(HTTPError):
                self.post(body, headers)
        self.assertEqual(self.backend.calls, [])


if __name__ == "__main__":
    unittest.main()
