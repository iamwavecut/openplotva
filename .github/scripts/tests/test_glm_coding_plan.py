import asyncio
import contextlib
import importlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    glm = importlib.import_module("glm_coding_plan")
except ModuleNotFoundError:
    glm = None


def events(text="review:\n  key_issues_to_review: []", reason="end_turn"):
    return [
        {"type": "message_start", "message": {"usage": {"input_tokens": 8869}}},
        {"type": "content_block_start", "index": 0,
         "content_block": {"type": "thinking", "thinking": ""}},
        {"type": "content_block_delta", "index": 0,
         "delta": {"type": "thinking_delta", "thinking": "PRIVATE_REASONING"}},
        {"type": "content_block_stop", "index": 0},
        {"type": "content_block_start", "index": 1,
         "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 1,
         "delta": {"type": "text_delta", "text": text}},
        {"type": "content_block_stop", "index": 1},
        {"type": "message_delta", "delta": {"stop_reason": reason},
         "usage": {"output_tokens": 400, "private": "PRIVATE_USAGE"}},
        {"type": "message_stop"},
    ]


def encode(rows):
    return "".join(f"event: {row['type']}\ndata: {json.dumps(row)}\n\n" for row in rows).encode()


class GlmCodingPlanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(glm, "The GLM Coding Plan adapter must exist")
        self.requests = []
        self.output = io.StringIO()

    def client(self, rows=None, status=200, stream=None):
        async def receive(request):
            self.requests.append(request)
            return httpx.Response(status, headers={"content-type": "text/event-stream"},
                                  content=None if stream else encode(rows if rows is not None else events()),
                                  stream=stream)
        real_client = httpx.AsyncClient
        return patch.object(glm.httpx, "AsyncClient", lambda **kwargs:
            real_client(**kwargs, transport=httpx.MockTransport(receive)))

    async def invoke(self, handler=None):
        handler = handler or glm.GlmCodingPlanHandler("PRIVATE_KEY", timeout=540)
        with contextlib.redirect_stdout(self.output), contextlib.redirect_stderr(self.output):
            return await handler.chat_completion("openai/glm-5.3", "PRIVATE_SYSTEM", "FULL_USER" * 2000, 0.2)

    def assert_safe(self, error=None):
        diagnostic = self.output.getvalue() + (str(error) if error else "")
        for private in ("PRIVATE_KEY", "PRIVATE_SYSTEM", "FULL_USER", "PRIVATE_REASONING", "PRIVATE_USAGE"):
            self.assertNotIn(private, diagnostic)

    async def test_complete_response_and_exact_native_protocol(self):
        with self.client():
            answer, finish = await self.invoke()
        self.assertEqual(answer, "review:\n  key_issues_to_review: []")
        self.assertEqual(finish, "stop")
        self.assertEqual(len(self.requests), 1)
        request = self.requests[0]
        self.assertEqual(str(request.url), "https://api.z.ai/api/anthropic/v1/messages")
        self.assertEqual(request.headers["x-api-key"], "PRIVATE_KEY")
        self.assertEqual(request.headers["anthropic-version"], "2023-06-01")
        self.assertEqual(json.loads(request.content), {
            "model": "glm-5.3", "system": "PRIVATE_SYSTEM",
            "messages": [{"role": "user", "content": "FULL_USER" * 2000}],
            "max_tokens": 131072, "stream": True,
            "thinking": {"type": "enabled", "budget_tokens": 4096, "display": "summarized"},
            "output_config": {"effort": "low"},
        })
        self.assert_safe()

    async def test_rejects_error_eof_truncation_and_empty_without_retry(self):
        cases = [
            ([{"type": "error", "error": {"message": "PRIVATE_KEY"}}], "stream_error"),
            (events()[:-1], "incomplete_stream"),
            (events(reason="max_tokens"), "incomplete_stop"),
            (events(reason="refusal"), "incomplete_stop"),
            (events(text="  "), "empty_output"),
        ]
        for rows, expected in cases:
            with self.subTest(expected=expected):
                self.requests.clear()
                with self.client(rows), self.assertRaises(glm.GlmCompletionError) as raised:
                    await self.invoke()
                self.assertEqual(str(raised.exception), expected)
                self.assertEqual(len(self.requests), 1)
                self.assert_safe(raised.exception)

    async def test_http_error_is_safe_and_not_retried(self):
        with self.client(status=429), self.assertRaises(glm.GlmCompletionError) as raised:
            await self.invoke()
        self.assertEqual(str(raised.exception), "http_error")
        self.assertEqual(len(self.requests), 1)
        self.assert_safe(raised.exception)

    async def test_whole_call_deadline_and_latched_failure(self):
        class Stalled(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b": heartbeat\n\n"
                await asyncio.sleep(1)
        handler = glm.GlmCodingPlanHandler("PRIVATE_KEY", timeout=0.03)
        started = asyncio.get_running_loop().time()
        with self.client(stream=Stalled()), self.assertRaises(glm.GlmCompletionError) as raised:
            await self.invoke(handler)
        self.assertEqual(str(raised.exception), "deadline")
        self.assertLess(asyncio.get_running_loop().time() - started, 0.5)
        with self.assertRaises(glm.GlmCompletionError):
            handler.ensure_complete()
        with self.client(), self.assertRaises(glm.GlmCompletionError):
            await self.invoke(handler)
        self.assertEqual(len(self.requests), 1)
        self.assert_safe(raised.exception)

    async def test_timeout_is_capped_and_no_fallback_model_is_allowed(self):
        handler = glm.GlmCodingPlanHandler("PRIVATE_KEY", timeout=900)
        self.assertEqual(handler.timeout, 540)
        with self.client(), self.assertRaises(glm.GlmCompletionError):
            await handler.chat_completion("openai/other-model", "system", "user")
        self.assertEqual(self.requests, [])

    async def test_broken_event_order_cannot_produce_success(self):
        with self.client([{"type": "message_stop"}]), self.assertRaises(glm.GlmCompletionError):
            await self.invoke()

    async def test_cancellation_closes_stream_and_preserves_cancellation(self):
        ready = asyncio.Event()
        class Stalled(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                ready.set()
                await asyncio.sleep(1)
                yield b""
            async def aclose(self):
                self.closed = True
        stream = Stalled()
        handler = glm.GlmCodingPlanHandler("PRIVATE_KEY")
        with self.client(stream=stream):
            task = asyncio.create_task(self.invoke(handler))
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(stream.closed)
        self.assertEqual(len(self.requests), 1)
        self.assertIn('"phase": "cancelled"', self.output.getvalue())
        with self.assertRaises(glm.GlmCompletionError):
            handler.ensure_complete()
        self.assert_safe()

    async def test_transport_exception_does_not_expose_request_or_provider_text(self):
        real_client = httpx.AsyncClient
        async def fail(request):
            self.requests.append(request)
            raise httpx.ConnectError("PRIVATE_KEY PRIVATE_SYSTEM", request=request)
        with patch.object(glm.httpx, "AsyncClient", lambda **kwargs:
                real_client(**kwargs, transport=httpx.MockTransport(fail))):
            with self.assertRaises(glm.GlmCompletionError) as raised:
                await self.invoke()
        self.assertEqual(str(raised.exception), "transport_error")
        self.assertTrue(raised.exception.__suppress_context__)
        self.assertEqual(len(self.requests), 1)
        self.assert_safe(raised.exception)


class PairIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = types.SimpleNamespace(
            config=types.SimpleNamespace(model="openai/glm-5.3", ai_timeout=600),
            get=lambda key, default=None: "PRIVATE_KEY" if key == "OPENAI.KEY" else default,
        )
        self.reviewer = Mock()
        self.suggester = Mock()
        reviewer_factory, suggester_factory = self.reviewer, self.suggester
        class Reviewer:
            def __new__(cls, *args, **kwargs):
                return reviewer_factory(*args, **kwargs)
        class Suggester:
            def __new__(cls, *args, **kwargs):
                return suggester_factory(*args, **kwargs)
        async def fallback(method, **_):
            return await method(self.settings.config.model)
        self.fallback = AsyncMock(side_effect=fallback)
        exports = {
            "litellm.litellm_core_utils.logging_worker": {"GLOBAL_LOGGING_WORKER": Mock()},
            "pr_agent.algo.pr_processing": {"retry_with_fallback_models": self.fallback},
            "pr_agent.algo.utils": {"ModelType": types.SimpleNamespace(REGULAR="regular")},
            "pr_agent.config_loader": {"get_settings": lambda: self.settings},
            "pr_agent.git_providers.utils": {"apply_repo_settings": Mock()},
            "pr_agent.log": {"get_logger": Mock(return_value=Mock()), "setup_logger": Mock()},
            "pr_agent.tools.pr_code_suggestions": {"PRCodeSuggestions": Suggester},
            "pr_agent.tools.pr_reviewer": {"PRReviewer": Reviewer},
        }
        modules = {}
        for name, attrs in exports.items():
            pieces = name.split(".")
            for index in range(1, len(pieces) + 1):
                prefix = ".".join(pieces[:index])
                modules.setdefault(prefix, types.ModuleType(prefix))
            modules[name].__dict__.update(attrs)
        path = Path(__file__).resolve().parents[1] / "run_pr_agent_pair.py"
        spec = importlib.util.spec_from_file_location("pair_under_test", path)
        self.pair = importlib.util.module_from_spec(spec)
        modules[spec.name] = self.pair
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.pair)

    def tool(self, **kwargs):
        handler = kwargs["ai_handler"]() if "ai_handler" in kwargs else Mock()
        return types.SimpleNamespace(ai_handler=handler,
            git_provider=types.SimpleNamespace(get_files=lambda: ["file.py"]),
            prediction="review", _prepare_prediction=AsyncMock(),
            _prepare_pr_review=lambda: "No major issues detected",
            prepare_prediction_main=AsyncMock(return_value={"code_suggestions": []}))

    async def test_glm_injected_into_both_tools_without_fallback_dispatch(self):
        self.reviewer.side_effect = lambda *_, **kwargs: self.tool(**kwargs)
        self.suggester.side_effect = lambda *_, **kwargs: self.tool(**kwargs)
        await self.pair.generate_review("public-pr")
        await self.pair.generate_suggestions("public-pr")
        self.assertIn("ai_handler", self.reviewer.call_args.kwargs)
        self.assertIn("ai_handler", self.suggester.call_args.kwargs)
        self.fallback.assert_not_awaited()

    async def test_swallowed_reflection_failure_still_fails_pair(self):
        handler = glm.GlmCodingPlanHandler("PRIVATE_KEY")
        tool = self.tool()
        tool.ai_handler = handler
        async def swallowed(_):
            handler._failure = glm.GlmCompletionError("incomplete_stream")
            return {"code_suggestions": []}
        tool.prepare_prediction_main = swallowed
        self.suggester.return_value = tool
        with self.assertRaises(glm.GlmCompletionError):
            await self.pair.generate_suggestions("public-pr")

    async def test_other_model_keeps_existing_handler_and_dispatch(self):
        self.settings.config.model = "openai/other-model"
        self.reviewer.side_effect = lambda *_, **kwargs: self.tool(**kwargs)
        self.suggester.side_effect = lambda *_, **kwargs: self.tool(**kwargs)
        await self.pair.generate_review("public-pr")
        await self.pair.generate_suggestions("public-pr")
        self.assertEqual(self.reviewer.call_args.kwargs, {})
        self.assertEqual(self.suggester.call_args.kwargs, {})
        self.assertEqual(self.fallback.await_count, 2)


if __name__ == "__main__":
    unittest.main()
