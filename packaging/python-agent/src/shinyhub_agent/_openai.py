from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from ._core import ToolError, ToolRegistry
from ._errors import ProviderError
from ._formatting import panel_instructions
from ._runtime import (
    FINAL_INSTRUCTIONS,
    Approve,
    ToolCall,
    Turn,
    bounded_history,
    bounded_int,
    check_calls,
    validate_limits,
)
from ._usage import ModelCall, UsageCallback, usage_context


def _base_url(value: str) -> str:
    message = (
        "base_url must be an HTTPS URL (HTTP only for localhost or 127.0.0.1) "
        "without credentials, a query, or a fragment"
    )
    if not isinstance(value, str) or any(
        char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value
    ):
        raise ValueError(message)
    try:
        parsed = urlsplit(value)
        # Accessing port also rejects malformed and out-of-range ports.
        port = parsed.port
        secure = parsed.scheme == "https" or (
            parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1")
        )
        if (
            not secure
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "\\" in value
            or "?" in value
            or "#" in value
            or (port is None and parsed.netloc.endswith(":"))
            or not httpx.URL(value).host
        ):
            raise ValueError(message)
    except (ValueError, httpx.InvalidURL) as error:
        raise ValueError(message) from error
    return value.rstrip("/")


def _budget_exceeded(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    markers = {"budget_exceeded", "budget_exceeded_error", "BudgetExceededError"}
    if any(
        isinstance(error.get(key), str) and error[key] in markers
        for key in ("type", "code")
    ):
        return True
    # Older LiteLLM errors only carry a message and an HTTP status as the code.
    message = error.get("message")
    return isinstance(message, str) and message.startswith(
        (
            "Budget has been exceeded!",
            "BudgetExceededError:",
            "litellm.BudgetExceededError:",
        )
    )


async def _lines(response: httpx.Response) -> AsyncIterator[str]:
    """Bound pending bytes before decoding, even without a line terminator."""
    pending = bytearray()
    skip_lf = False
    first = True
    total_bytes = 0
    async for chunk in response.aiter_bytes():
        total_bytes += len(chunk)
        if total_bytes > 1_048_576:
            raise ToolError("model_too_large", "The assistant sent too much data.")
        for byte in chunk:
            if skip_lf:
                skip_lf = False
                if byte == 10:
                    continue
            if byte in (10, 13):
                yield pending.decode("utf-8-sig" if first else "utf-8")
                first = False
                pending.clear()
                skip_lf = byte == 13
            else:
                pending.append(byte)
                if len(pending) > 65_536:
                    raise ToolError(
                        "model_too_large", "The assistant sent too much data."
                    )
    if pending:
        yield pending.decode("utf-8-sig" if first else "utf-8")


async def _events(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    """Decode bounded SSE events, including multiline data and keepalives."""
    data: list[str] = []
    event_bytes = 0

    def decode() -> dict[str, Any] | None:
        raw = "\n".join(data).strip()
        if not raw or raw == "[DONE]":
            return None
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ToolError("model_failed", "The assistant sent an invalid event.")
        return value

    async for line in _lines(response):
        if not line:
            if data:
                event = decode()
                if event is not None:
                    yield event
            data = []
            event_bytes = 0
        elif line.startswith("data:"):
            value = line[5:]
            value = value.removeprefix(" ")
            event_bytes += len(value.encode()) + 1
            if event_bytes > 65_536:
                raise ToolError("model_too_large", "The assistant sent too much data.")
            data.append(value)
    # Retain compatibility with endpoints that omit the final empty line.
    if data:
        event = decode()
        if event is not None:
            yield event


@dataclass(frozen=True)
class OpenAIChat:
    """Responses API adapter, also exported as ResponsesChat, using app tools."""

    api_key: str = field(repr=False)
    instructions: str
    model: str = "gpt-4.1-mini"
    max_output_tokens: int = 500
    max_tool_calls_per_step: int = 2
    max_tool_rounds: int = 4
    max_tool_calls_per_turn: int = 8
    on_usage: UsageCallback | None = field(default=None, repr=False, compare=False)
    base_url: str = field(default="https://api.openai.com/v1", kw_only=True)
    send_end_user: bool = field(default=False, kw_only=True)
    provider: str = field(default="openai", kw_only=True)

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("An API key is required")
        if not self.instructions.strip():
            raise ValueError("App-specific agent instructions are required")
        object.__setattr__(self, "base_url", _base_url(self.base_url))
        if not isinstance(self.send_end_user, bool):
            raise TypeError("send_end_user must be a boolean")
        if not isinstance(self.provider, str) or not re.fullmatch(
            r"[a-z][a-z0-9._-]{0,63}", self.provider
        ):
            raise ValueError(
                "provider must be a lower-case label of 1 to 64 characters"
            )
        bounded_int("max_output_tokens", self.max_output_tokens, 100, 8_192)
        validate_limits(
            self.max_tool_calls_per_step,
            self.max_tool_rounds,
            self.max_tool_calls_per_turn,
        )
        if self.on_usage is not None and not callable(self.on_usage):
            raise TypeError("on_usage must be callable")

    async def _response(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        request: dict[str, Any],
        usage: ModelCall,
    ) -> AsyncIterator[dict[str, Any]]:
        output = None
        text_seen = False
        async with client.stream(
            "POST", self.base_url + "/responses", headers=headers, json=request
        ) as response:
            if response.status_code in (400, 429):
                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65_536):
                    body.extend(chunk)
                    if len(body) > 65_536:
                        break
                if len(body) <= 65_536:
                    try:
                        error_body = json.loads(body)
                    except (ValueError, UnicodeError):
                        error_body = None
                    if isinstance(error_body, dict) and _budget_exceeded(
                        error_body.get("error")
                    ):
                        raise ProviderError("budget_exceeded")
            response.raise_for_status()
            async for event in _events(response):
                kind = event.get("type")
                if kind == "response.output_text.delta":
                    delta = event.get("delta")
                    if isinstance(delta, str) and delta:
                        text_seen = True
                        yield {"type": "delta", "text": delta}
                elif (
                    kind == "response.output_item.added"
                    and isinstance(event.get("item"), dict)
                    and event["item"].get("type") == "function_call"
                ):
                    name = event["item"].get("name")
                    if isinstance(name, str):
                        usage.tool_names += (name,)
                    yield {"type": "status", "text": "Using app tools"}
                elif kind in (
                    "response.completed",
                    "response.incomplete",
                    "response.failed",
                ):
                    body = event.get("response", {})
                    if not isinstance(body, dict):
                        raise ToolError(
                            "model_failed", "The assistant sent an invalid response."
                        )
                    tokens = body.get("usage")
                    if not isinstance(tokens, dict):
                        tokens = {}
                    details = tokens.get("input_tokens_details")
                    usage.tokens = {
                        "input_tokens": tokens.get("input_tokens"),
                        "output_tokens": tokens.get("output_tokens"),
                        "cache_read_tokens": details.get("cached_tokens")
                        if isinstance(details, dict)
                        else None,
                    }
                    if _budget_exceeded(body.get("error")):
                        raise ProviderError("budget_exceeded")
                    if kind != "response.completed":
                        raise ToolError(
                            "model_failed", "The assistant could not finish its reply."
                        )
                    output = body.get("output", [])
                elif kind == "error":
                    if _budget_exceeded(event.get("error", event)):
                        raise ProviderError("budget_exceeded")
                    raise ToolError(
                        "model_failed", "The assistant could not finish its reply."
                    )
                elif _budget_exceeded(event.get("error")):
                    raise ProviderError("budget_exceeded")
        if not isinstance(output, list) or any(
            not isinstance(item, dict) for item in output
        ):
            raise ToolError("model_failed", "The assistant connection ended early.")
        calls = []
        for item in output:
            if item.get("type") != "function_call":
                continue
            raw = item.get("arguments", "")
            if not isinstance(raw, str) or len(raw.encode()) > 8_192:
                raise ToolError(
                    "model_too_large", "The assistant sent too much tool data."
                )
            try:
                arguments = json.loads(raw)
            except ValueError as error:
                raise ToolError(
                    "model_failed", "The assistant sent invalid tool arguments."
                ) from error
            calls.append(ToolCall(item.get("call_id"), item.get("name"), arguments))
        check_calls(calls)
        usage.tool_names = tuple(call.name for call in calls)
        if not calls and not text_seen:
            answer = "\n".join(
                part["text"]
                for item in output
                if item.get("type") == "message"
                for part in item.get("content", [])
                if part.get("type") == "output_text"
                and isinstance(part.get("text"), str)
            ).strip()
            if not answer:
                raise ToolError("model_failed", "The assistant returned no answer.")
            yield {"type": "delta", "text": answer}
        yield {"type": "model_response", "output": output, "calls": calls}

    async def run(
        self,
        message: str,
        history: Sequence[dict[str, str]],
        tools: ToolRegistry,
        approve: Approve,
        *,
        thread_id: str,
    ) -> AsyncIterator[dict[str, Any]]:
        if not isinstance(message, str) or not 1 <= len(message.strip()) <= 2_000:
            raise ToolError(
                "invalid_message", "Enter a question of at most 2,000 characters."
            )
        end_user = None
        if self.send_end_user:
            end_user = usage_context.get().get("username")
            if (
                not isinstance(end_user, str)
                or not end_user.strip()
                or len(end_user) > 256
            ):
                raise ProviderError("end_user_required")
        inputs: list[dict[str, Any]] = bounded_history(history)
        inputs.append({"role": "user", "content": message.strip()})
        model_tools = [
            {
                "type": "function",
                "name": item.name,
                "description": item.description,
                "parameters": dict(item.input_schema),
                "strict": False,
            }
            for item in (
                tools.get(spec["name"]) for spec in tools.public_spec()["tools"]
            )
            if item is not None
        ]
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        turn = Turn(self.max_tool_calls_per_step, self.max_tool_calls_per_turn)
        async with httpx.AsyncClient(timeout=httpx.Timeout(45, connect=5)) as client:
            for round_index in range(self.max_tool_rounds + 1):
                final = round_index == self.max_tool_rounds or turn.exhausted
                instructions = panel_instructions(self.instructions)
                if final:
                    instructions += "\n\n" + FINAL_INSTRUCTIONS
                request = {
                    "model": self.model,
                    "instructions": instructions,
                    "input": inputs,
                    "tools": model_tools,
                    "parallel_tool_calls": False,
                    "max_output_tokens": self.max_output_tokens,
                    "store": False,
                    "stream": True,
                }
                if final:
                    request["tool_choice"] = "none"
                if end_user is not None:
                    request["user"] = end_user
                buffered = []
                try:
                    async with ModelCall(
                        self.provider,
                        self.model,
                        turn.id,
                        round_index + 1,
                        final,
                        self.on_usage,
                    ) as usage:
                        async for event in self._response(
                            client, headers, request, usage
                        ):
                            if event["type"] == "model_response":
                                response = event
                            elif final:
                                buffered.append(event)
                            else:
                                yield event
                        if final and response["calls"]:
                            raise ToolError(
                                "model_failed",
                                "The assistant requested tools after its budget ended.",
                            )
                except ProviderError:
                    raise
                except Exception:
                    if not final:
                        raise
                    yield {"type": "delta", "text": turn.fallback()}
                    return
                if final:
                    for event in buffered:
                        yield event
                    return
                if not response["calls"]:
                    return
                inputs.extend(response["output"])
                async for event in turn.execute(response["calls"], tools, approve):
                    if event["type"] == "tool_result":
                        inputs.append(
                            {
                                "type": "function_call_output",
                                "call_id": event["id"],
                                "output": json.dumps(event["result"]),
                            }
                        )
                    else:
                        yield event
                yield {"type": "status", "text": "Writing answer"}
