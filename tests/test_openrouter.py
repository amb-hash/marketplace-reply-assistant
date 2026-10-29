from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from mra import generate


class OpenRouter(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        patch("mra.generate.PROVIDER", "openrouter").start()
        patch("mra.generate.load_rules", return_value="Synthetic rules").start()
        self.material = [("fact", "Synthetic material")]

    def response(self, *, finish="stop", text=None):
        return {"id": "generation-fixture", "model": "openai/gpt-6.1-sol",
                "choices": [{"finish_reason": finish, "message": {"content": text or json.dumps({
                    "decision": "answer", "answer": "Fixture answer", "used": [1], "note": ""})}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20, "cost": 0.0004}}

    def test_feedback_payload_keeps_question_canon_and_previous_draft_separate(self):
        payload=generate.build_payload("Original question","A",self.material,operator_feedback="Make it shorter",previous_answer="Previous buyer answer")
        system,user=[m["content"] for m in payload["messages"]]
        self.assertIn("MATERIAL:",user)
        self.assertIn("Original question",user)
        self.assertIn("PREVIOUS_DRAFT:\nPrevious buyer answer",user)
        self.assertIn("OPERATOR_FEEDBACK:\nMake it shorter",user)
        self.assertIn("not verified product facts",system)
        self.assertTrue(payload["response_format"]["json_schema"]["strict"])

    def test_payload_keeps_exact_prompts_schema_and_bounded_single_route(self):
        with patch("mra.generate.PROVIDER", "openai"):
            original = generate.build_payload("Question", "A", self.material)
        routed = generate.build_payload("Question", "A", self.material, model="openai/gpt-6.1-sol")
        self.assertEqual([m["content"] for m in routed["messages"]], [original["instructions"], original["input"]])
        self.assertEqual(routed["max_tokens"], original["max_output_tokens"])
        self.assertEqual(routed["response_format"]["json_schema"]["schema"], original["text"]["format"]["schema"])
        self.assertTrue(routed["response_format"]["json_schema"]["strict"])
        self.assertEqual(routed["provider"], {"allow_fallbacks": False, "require_parameters": True, "data_collection": "deny"})
        self.assertNotIn("tools", routed)
        self.assertNotIn("models", routed)

    def test_real_transport_shape_uses_only_openrouter_key_and_records_usage(self):
        body = self.response()
        response = io.BytesIO(json.dumps(body).encode())
        response.status, response.headers = 200, {"x-request-id": "req-fixture"}
        events = []
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openrouter_key", return_value="fixture-key"), \
                patch("mra.generate.load_openai_key") as wrong_key, patch("urllib.request.urlopen", return_value=response) as post:
            reply = generate.generate_reply("Question", "A", self.material, model="openai/gpt-6.1-sol", allow_llm=True,
                audit=lambda kind, payload: events.append((kind, payload)))
        wrong_key.assert_not_called()
        self.assertEqual(post.call_count, 1)
        self.assertEqual(post.call_args.args[0].full_url, generate.OPENROUTER_CHAT_URL)
        self.assertEqual((reply.decision, reply.source, reply.input_tokens, reply.output_tokens), ("answer", "openrouter", 100, 20))
        received = next(payload for kind, payload in events if kind == "model.response.received")
        self.assertEqual(received["response"]["usage"]["cost"], 0.0004)
        self.assertNotIn("fixture-key", json.dumps(events))

    def test_incomplete_chat_is_operator_but_keeps_measured_usage(self):
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openrouter_key", return_value="fixture-key"), \
                patch("mra.generate._post_responses", return_value=self.response(finish="length")):
            reply = generate.generate_reply("Question", "A", self.material, allow_llm=True)
        self.assertEqual(reply.decision, "operator")
        self.assertEqual(reply.text, "")
        self.assertTrue(reply.usage_reported)
        self.assertEqual(reply.output_tokens, 20)

    def test_invalid_or_refused_chat_never_becomes_answer(self):
        for body in [self.response(text="not JSON"), {"choices": []}, {"error": {"code": 402}},
                     {"choices": [{"finish_reason": "stop", "message": {"refusal": "fixture"}}]}]:
            with self.subTest(body=body), patch("mra.generate.DISABLE_LLM", False), \
                    patch("mra.generate.load_openrouter_key", return_value="fixture-key"), \
                    patch("mra.generate._post_responses", return_value=body):
                self.assertEqual(generate.generate_reply("Question", "A", self.material, allow_llm=True).decision, "operator")

    def test_explicit_auth_file_reads_only_named_openrouter_entry_without_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "auth.json"
            path.write_text(json.dumps({"openrouter": {"type": "api", "key": "fixture-or-key"}, "other": {"key": "unused"}}))
            with patch.dict("os.environ", {"OPENROUTER_API_KEY": "", "MRA_OPENROUTER_KEY_FILE": "", "MRA_OPENROUTER_AUTH_FILE": str(path)}), \
                    patch("mra.generate.DISABLE_LLM", False):
                self.assertEqual(generate.load_openrouter_key(), "fixture-or-key")
            self.assertEqual(sorted(p.name for p in Path(temporary).iterdir()), ["auth.json"])

    def test_auth_file_is_never_scanned_implicitly(self):
        with patch.dict("os.environ", {"OPENROUTER_API_KEY": "", "MRA_OPENROUTER_KEY_FILE": "", "MRA_OPENROUTER_AUTH_FILE": ""}), \
                patch("mra.generate.DISABLE_LLM", False), patch.object(Path, "read_text") as read:
            self.assertIsNone(generate.load_openrouter_key())
        read.assert_not_called()

    def test_credit_error_is_single_request_without_provider_switch(self):
        error = urllib.error.HTTPError("https://example.invalid", 429, "fixture", {},
            io.BytesIO(b'{"error":{"code":"credit_balance_exhausted","message":"fixture-key"}}'))
        with patch("mra.generate.DISABLE_LLM", False), patch("mra.generate.load_openrouter_key", return_value="fixture-key"), \
                patch("urllib.request.urlopen", side_effect=error) as post:
            result = generate.generate_reply("Question", "A", self.material, allow_llm=True)
        self.assertEqual(post.call_count, 1)
        self.assertIn("credit_balance_exhausted", result.note)
        self.assertNotIn("fixture-key", result.note)

    def test_unknown_endpoint_never_receives_key(self):
        with patch("urllib.request.urlopen") as post, self.assertRaises(generate.ProviderError):
            generate._post_responses("fixture-key", {}, endpoint="https://example.invalid")
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
