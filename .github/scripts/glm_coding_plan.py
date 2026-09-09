"""Bounded GLM Coding Plan inference using OMP's native Anthropic request shape."""
import asyncio
import json
import math
import time

import httpx
from maintenance.quota import limited, retry_after


MODEL = "openai/glm-5.3-flash"
MODELS = {MODEL, "openai/glm-5.3"}
ENDPOINT = "https://api.z.ai/api/anthropic/v1/messages"


class GlmCompletionError(RuntimeError):
    """A static failure classification safe for PR-Agent's exception logging."""


class GlmQuotaUnavailable(GlmCompletionError):
    def __init__(self, delay=None):
        super().__init__('quota_unavailable')
        self.retry_after_seconds = delay


class GlmCodingPlanHandler:
    def __init__(self, api_key: str, timeout: float = 540):
        if not isinstance(api_key, str) or not api_key.strip():
            raise GlmCompletionError("missing_credential")
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise GlmCompletionError("invalid_deadline")
        self._api_key = api_key
        self.timeout = min(timeout, 540)
        self._failure = None

    @property
    def deployment_id(self):
        return None

    def ensure_complete(self):
        # PR-Agent catches reflection errors internally. Keep them fatal to the
        # surrounding review/suggestions pair, including subsequent invocations.
        if self._failure is not None:
            raise self._failure

    async def chat_completion(self, model, system, user, temperature=0.2, img_path=None):
        self.ensure_complete()
        started = time.monotonic()
        receipt = {"phase": "starting", "attempts": 1, "usage": {}}
        try:
            if model not in MODELS or img_path is not None:
                raise GlmCompletionError("unsupported_request")
            if not isinstance(system, str) or not isinstance(user, str):
                raise GlmCompletionError("invalid_prompt")
            payload = {
                "model": model.removeprefix('openai/'), "system": system,
                "messages": [{"role": "user", "content": user}],
                "max_tokens": 131072, "stream": True,
                "thinking": {"type": "enabled", "budget_tokens": 4096, "display": "summarized"},
                "output_config": {"effort": "low"},
            }
            # No SDK retry layer, redirects, proxy environment, or fallback.
            # OMP omits sampling controls when Anthropic thinking is enabled.
            async with asyncio.timeout(self.timeout):
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=10, read=None, write=30, pool=10),
                    follow_redirects=False, trust_env=False,
                ) as client:
                    async with client.stream("POST", ENDPOINT, json=payload, headers={
                        "x-api-key": self._api_key, "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    }) as response:
                        receipt["http_status"] = response.status_code
                        receipt["headers_seconds"] = round(time.monotonic() - started, 3)
                        if response.status_code != 200:
                            raw = bytearray()
                            async for chunk in response.aiter_bytes():
                                raw.extend(chunk[:8192-len(raw)])
                                if len(raw) >= 8192:
                                    break
                            try:
                                document = json.loads(raw)
                            except ValueError:
                                document = None
                            if limited(response.status_code, document):
                                raise GlmQuotaUnavailable(retry_after(response.headers.get('retry-after')))
                            raise GlmCompletionError("http_error")
                        if response.headers.get("content-type", "").split(";")[0].strip() != "text/event-stream":
                            raise GlmCompletionError("invalid_content_type")
                        answer = await self._read_stream(response, receipt)
            receipt["phase"] = "complete"
            return answer, "stop"
        except GlmCompletionError as error:
            self._failure = error
            receipt["phase"] = str(error)
            raise
        except TimeoutError:
            self._failure = GlmCompletionError("deadline")
            receipt["phase"] = "deadline"
            raise self._failure from None
        except asyncio.CancelledError:
            self._failure = GlmCompletionError("cancelled")
            receipt["phase"] = "cancelled"
            raise
        except httpx.HTTPError:
            self._failure = GlmCompletionError("transport_error")
            receipt["phase"] = "transport_error"
            raise self._failure from None
        except (ValueError, TypeError, KeyError, AttributeError):
            self._failure = GlmCompletionError("invalid_stream")
            receipt["phase"] = "invalid_stream"
            raise self._failure from None
        finally:
            receipt["elapsed_seconds"] = round(time.monotonic() - started, 3)
            print("GLM Coding Plan " + json.dumps(receipt, sort_keys=True), flush=True)

    async def _read_stream(self, response, receipt):
        started = False
        stop_reason = None
        blocks = {}
        seen_blocks = set()
        parts = []
        data_lines = []
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))
                continue
            if line or not data_lines:
                continue
            event = json.loads("\n".join(data_lines))
            data_lines.clear()
            kind = event["type"]
            if kind == "error":
                if limited(200, event):
                    raise GlmQuotaUnavailable()
                raise GlmCompletionError("stream_error")
            if kind == "ping":
                continue
            if kind == "message_start":
                if started:
                    raise GlmCompletionError("invalid_stream")
                started = True
                usage = event["message"].get("usage", {})
            else:
                if not started:
                    raise GlmCompletionError("invalid_stream")
                usage = event.get("usage", {})
            for name in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
                value = usage.get(name)
                if type(value) is int and value >= 0:
                    receipt["usage"][name] = value
            if kind == "content_block_start":
                index = event["index"]
                block = event["content_block"]
                if type(index) is not int or index < 0 or index in seen_blocks or stop_reason is not None:
                    raise GlmCompletionError("invalid_stream")
                if block["type"] not in {"text", "thinking", "redacted_thinking"}:
                    raise GlmCompletionError("unsupported_output")
                blocks[index] = block["type"]
                seen_blocks.add(index)
                if block["type"] == "text":
                    if not isinstance(block["text"], str):
                        raise GlmCompletionError("invalid_stream")
                    parts.append(block["text"])
            elif kind == "content_block_delta":
                delta = event["delta"]
                block_type = blocks.get(event["index"])
                if block_type == "text" and delta["type"] == "text_delta":
                    if not isinstance(delta["text"], str):
                        raise GlmCompletionError("invalid_stream")
                    parts.append(delta["text"])
                elif block_type not in {"thinking", "redacted_thinking"} or delta["type"] not in {
                    "thinking_delta", "signature_delta",
                }:
                    raise GlmCompletionError("invalid_stream")
            elif kind == "content_block_stop":
                if event["index"] not in blocks:
                    raise GlmCompletionError("invalid_stream")
                del blocks[event["index"]]
            elif kind == "message_delta":
                reason = event["delta"].get("stop_reason")
                if reason is not None:
                    if reason != "end_turn":
                        raise GlmCompletionError("incomplete_stop")
                    stop_reason = reason
            elif kind == "message_stop":
                if blocks or stop_reason != "end_turn":
                    raise GlmCompletionError("incomplete_stream")
                answer = "".join(parts)
                if not answer.strip():
                    raise GlmCompletionError("empty_output")
                return answer
            elif kind not in {"message_start", "content_block_start"}:
                raise GlmCompletionError("invalid_stream")
        raise GlmCompletionError("incomplete_stream")
