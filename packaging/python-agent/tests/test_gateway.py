import asyncio
import json
from collections import defaultdict
from contextlib import contextmanager

import httpx
import pytest
import shinyhub_agent
from shinyhub_agent import AgentTool, OpenAIChat, ResponsesChat, ToolError, ToolRegistry
from shinyhub_agent._chat import ChatSession
from shinyhub_agent._errors import ProviderError
from shinyhub_agent._formatting import ANSWER_FORMAT
from shinyhub_agent._usage import usage_context

SCHEMA = {"type": "object", "properties": {}, "additionalProperties": False}


def tools():
    async def read(_):
        return {"period": "week"}

    async def write(_):
        return {"period": "year", "applied": True}

    return ToolRegistry(
        [
            AgentTool("get_view", "Read view", SCHEMA, read),
            AgentTool(
                "set_view",
                "Change view",
                SCHEMA,
                write,
                read_only=False,
                confirmation="Change period?",
            ),
        ]
    )


def reply(*, call=None, text="Answer", usage=True):
    output = []
    events = []
    if call:
        output = [
            {
                "type": "function_call",
                "call_id": call + "-id",
                "name": call,
                "arguments": "{}",
            }
        ]
        events.append({"type": "response.output_item.added", "item": output[0]})
    else:
        events.append({"type": "response.output_text.delta", "delta": text})
    body = {"output": output}
    if usage:
        body["usage"] = {
            "input_tokens": 11,
            "output_tokens": 5,
            "input_tokens_details": {"cached_tokens": 3},
        }
    events.append({"type": "response.completed", "response": body})
    return sse(events)


def sse(events):
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        text="".join("data: " + json.dumps(e) + "\n\n" for e in events),
    )


def mock_gateway(monkeypatch, handler):
    client = httpx.AsyncClient
    monkeypatch.setattr(
        "shinyhub_agent._openai.httpx.AsyncClient",
        lambda *a, **kw: client(*a, transport=httpx.MockTransport(handler), **kw),
    )


async def turn(agent, *, username=None, approve=True, raw=None):
    events = []

    async def send(event):
        events.append(event)
        if event["type"] == "approval_required":
            chat.decide(
                {
                    "version": 1,
                    "session": chat.nonce,
                    "approvalId": event["approvalId"],
                    "approved": approve,
                }
            )

    chat = ChatSession(
        agent, tools(), send, usage_metadata={"username": username, "app": "local-app"}
    )
    await chat.handle(
        {
            "version": 1,
            "session": chat.nonce,
            "requestId": "request_12345678",
            "message": "Change view",
            **(raw or {}),
        }
    )
    return chat, events


@pytest.mark.parametrize(
    "base, expected",
    [
        ("https://gateway.example/v1/", "https://gateway.example/v1/responses"),
        (
            "https://gateway.example/prefix/v1///",
            "https://gateway.example/prefix/v1/responses",
        ),
        ("https://gateway.example", "https://gateway.example/responses"),
        ("http://localhost:4000/v1", "http://localhost:4000/v1/responses"),
        ("http://127.0.0.1:4000/v1/", "http://127.0.0.1:4000/v1/responses"),
    ],
)
def test_gateway_url_preserves_prefix_and_uses_bearer_key(monkeypatch, base, expected):
    requests = []

    def respond(request):
        requests.append(request)
        return reply()

    mock_gateway(monkeypatch, respond)
    chat, events = asyncio.run(
        turn(OpenAIChat("gateway-key", "Use app facts", base_url=base))
    )
    assert events[-1]["type"] == "done"
    assert len(requests) == 1
    assert str(requests[0].url) == expected
    assert requests[0].headers["authorization"] == "Bearer gateway-key"
    assert "gateway-key" not in json.dumps(chat.capabilities())


@pytest.mark.parametrize(
    "base",
    [
        None,
        42,
        "",
        "gateway.example/v1",
        "/v1",
        "//gateway.example/v1",
        "file:///v1",
        "http://gateway.example/v1",
        "http://localhost.evil/v1",
        "http://127.0.0.2/v1",
        "https:///v1",
        "https://user:pass@gateway.example/v1",
        "https://@gateway.example/v1",
        "https://gateway.example/v1?token=secret",
        "https://gateway.example/v1?",
        "https://gateway.example/v1#fragment",
        "https://gateway.example/v1#",
        "https://gateway.example:bad/v1",
        "https://gateway.example:65536/v1",
        "https://gateway.example:/v1",
        "https://[bad/v1",
        "https://gateway.example/v1\n",
        "https://gate way.example/v1",
        "https://gateway.example/\x00v1",
        "https://gateway.example/\\v1",
    ],
)
def test_invalid_gateway_url_is_rejected_at_construction(base):
    with pytest.raises(ValueError, match="base_url"):
        OpenAIChat("key", "Instructions", base_url=base)


@pytest.mark.parametrize("value", [None, "true", 1, 0])
def test_forwarding_requires_a_real_boolean(value):
    with pytest.raises(TypeError, match="send_end_user"):
        OpenAIChat("key", "Instructions", send_end_user=value)


@pytest.mark.parametrize(
    "provider", [None, "", "OpenAI", "space label", "x\n", "x" * 65]
)
def test_provider_labels_are_bounded(provider):
    with pytest.raises(ValueError, match="provider"):
        OpenAIChat("key", "Instructions", provider=provider)


def test_default_request_and_privacy_contract_are_unchanged(monkeypatch):
    assert ResponsesChat is OpenAIChat
    assert "ResponsesChat" in shinyhub_agent.__all__
    requests, records = [], []

    def respond(request):
        requests.append(request)
        return reply()

    mock_gateway(monkeypatch, respond)
    # Preserve the existing positional constructor, including on_usage.
    agent = OpenAIChat(
        "key", "Use app facts", "model-alias", 500, 2, 4, 8, records.append
    )
    asyncio.run(turn(agent, username="private-viewer"))
    assert str(requests[0].url) == "https://api.openai.com/v1/responses"
    body = json.loads(requests[0].content)
    assert body == {
        "model": "model-alias",
        "instructions": "Use app facts\n\n" + ANSWER_FORMAT,
        "input": [{"role": "user", "content": "Change view"}],
        "tools": [
            {
                "type": "function",
                "name": spec["name"],
                "description": spec["description"],
                "parameters": spec["inputSchema"],
                "strict": False,
            }
            for spec in tools().public_spec()["tools"]
        ],
        "parallel_tool_calls": False,
        "max_output_tokens": 500,
        "store": False,
        "stream": True,
    }
    assert "private-viewer" not in requests[0].content.decode()
    assert "private-viewer" not in str(requests[0].headers)
    assert records[0].provider == "openai"
    assert records[0].username == "private-viewer"


@pytest.mark.parametrize("username", [None, "", " \n ", 42, "x" * 257])
def test_opt_in_without_identity_fails_before_network(monkeypatch, username):
    def forbidden(*a, **kw):
        raise AssertionError("must not create a client without attribution")

    monkeypatch.setattr("shinyhub_agent._openai.httpx.AsyncClient", forbidden)

    async def run():
        token = usage_context.set({"username": username})
        try:
            agent = OpenAIChat("key", "Instructions", send_end_user=True)
            with pytest.raises(ProviderError, match="verified user identity"):
                async for _ in agent.run(
                    "Question", [], tools(), None, thread_id="thread"
                ):
                    pass
        finally:
            usage_context.reset(token)

    asyncio.run(run())


@pytest.mark.parametrize("approved", [True, False])
def test_shared_gateway_isolates_viewers_through_tools_approval_and_final_round(
    monkeypatch, approved
):
    requests, records = [], []
    steps = defaultdict(int)

    async def respond(request):
        body = json.loads(request.content)
        viewer = body["user"]
        step = steps[viewer]
        steps[viewer] += 1
        requests.append(request)
        await asyncio.sleep(0)  # Interleave viewers using the same frozen adapter.
        if step == 0:
            return reply(call="get_view")
        if step == 1:
            return reply(call="set_view")
        return reply(text="Showing the resulting view.")

    mock_gateway(monkeypatch, respond)
    agent = ResponsesChat(
        "key",
        "Instructions",
        model="bedrock-alias",
        base_url="https://gateway.example/v1",
        provider="litellm",
        send_end_user=True,
        max_tool_rounds=2,
        on_usage=records.append,
    )

    async def run():
        async def viewer_turn(viewer):
            result = await turn(
                agent,
                username=viewer,
                approve=approved,
                raw={"user": "forged", "usage_metadata": {"username": "forged"}},
            )
            # Check cleanup inside the task that set the ContextVar.
            assert usage_context.get() == {}
            return result

        result = await asyncio.gather(
            *[viewer_turn(viewer) for viewer in ("viewer-one", "viewer-two")]
        )
        assert usage_context.get() == {}
        return result

    chats = asyncio.run(run())
    assert steps == {"viewer-one": 3, "viewer-two": 3}
    for viewer in steps:
        bodies = [
            json.loads(r.content)
            for r in requests
            if json.loads(r.content)["user"] == viewer
        ]
        assert (
            bodies[1]["input"][-2]["call_id"]
            == bodies[1]["input"][-1]["call_id"]
            == "get_view-id"
        )
        result = json.loads(bodies[2]["input"][-1]["output"])
        assert result == (
            {"period": "year", "applied": True}
            if approved
            else {
                "code": "action_declined",
                "error": "The visitor declined the action.",
            }
        )
        assert bodies[2]["tool_choice"] == "none"
        assert all(
            b["store"] is False and b["parallel_tool_calls"] is False for b in bodies
        )
        assert all(
            viewer not in json.dumps([b["input"], b["instructions"], b["tools"]])
            for b in bodies
        )
        usage = [r for r in records if r.username == viewer]
        assert [r.round for r in usage] == [1, 2, 3]
        assert [r.final for r in usage] == [False, False, True]
        assert all(
            r.provider == "litellm"
            and r.input_tokens == 11
            and r.output_tokens == 5
            and r.cache_read_tokens == 3
            for r in usage
        )
    assert all("viewer-" not in str(r.headers) for r in requests)
    assert all(events[-1]["type"] == "done" for _, events in chats)
    assert all(
        sum(e["type"] == "action_applied" for e in events) == int(approved)
        for _, events in chats
    )
    assert len({r.turn_id for r in records}) == 2


BUDGET_ERRORS = [
    {"type": "budget_exceeded", "message": "private details"},
    {"code": "budget_exceeded", "message": "private details"},
    {"type": "BudgetExceededError", "message": "private details"},
    {"code": "429", "message": "Budget has been exceeded! EndUser=private details"},
    {"message": "litellm.BudgetExceededError: private details"},
]


@pytest.mark.parametrize("status", [400, 429])
@pytest.mark.parametrize("error", BUDGET_ERRORS)
def test_only_budget_exhausted_viewer_is_blocked_with_safe_message(
    monkeypatch, status, error
):
    records = []

    def respond(request):
        if json.loads(request.content)["user"] == "blocked":
            return httpx.Response(status, json={"error": error})
        return reply()

    mock_gateway(monkeypatch, respond)
    agent = OpenAIChat(
        "key",
        "Instructions",
        base_url="https://gateway.example/v1",
        send_end_user=True,
        provider="litellm",
        on_usage=records.append,
    )

    async def run():
        return await asyncio.gather(
            turn(agent, username="blocked"), turn(agent, username="allowed")
        )

    (blocked, failed), (_, succeeded) = asyncio.run(run())
    assert blocked.history == []
    assert failed[-1]["code"] == "budget_exceeded"
    assert failed[-1]["message"] == ProviderError.MESSAGES["budget_exceeded"]
    assert "private details" not in json.dumps(failed)
    assert succeeded[-1]["type"] == "done"
    assert {r.username: r.outcome for r in records} == {
        "blocked": "error",
        "allowed": "completed",
    }
    assert next(r for r in records if r.username == "blocked").input_tokens is None


@pytest.mark.parametrize(
    "failure",
    [
        {"type": "error", "code": "budget_exceeded", "message": "private"},
        {"type": "error", "error": {"type": "budget_exceeded", "message": "private"}},
        {"error": {"type": "budget_exceeded", "message": "private"}},
        {
            "type": "response.failed",
            "response": {
                "error": {"code": "budget_exceeded"},
                "usage": {"input_tokens": 7, "output_tokens": 2},
            },
        },
    ],
)
def test_streaming_budget_errors_reach_panel(monkeypatch, failure):
    records = []
    mock_gateway(monkeypatch, lambda _: sse([failure]))
    _, events = asyncio.run(
        turn(OpenAIChat("key", "Instructions", on_usage=records.append))
    )
    assert events[-1]["code"] == "budget_exceeded"
    assert records[0].outcome == "error"
    if failure.get("type") == "response.failed":
        assert records[0].input_tokens == 7


@pytest.mark.parametrize("final", [False, True])
def test_budget_failure_after_approved_write_retains_receipt_and_is_not_swallowed(
    monkeypatch, final
):
    records, bodies = [], []

    def respond(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return reply(call="set_view")
        return httpx.Response(
            429, json={"error": {"type": "budget_exceeded", "message": "private"}}
        )

    mock_gateway(monkeypatch, respond)
    chat, events = asyncio.run(
        turn(
            OpenAIChat(
                "key",
                "Instructions",
                max_tool_rounds=1 if final else 2,
                on_usage=records.append,
            )
        )
    )
    assert any(e["type"] == "action_applied" for e in events)
    assert events[-1]["code"] == "budget_exceeded"
    assert not any(e["type"] == "delta" for e in events)
    assert records[-1].final == final
    assert records[-1].outcome == "error"
    assert (
        len(chat.history) == 2
        and "Applied app action: set_view" in chat.history[-1]["content"]
    )
    assert (bodies[-1].get("tool_choice") == "none") == final


@pytest.mark.parametrize(
    "status, body",
    [
        (429, {"error": {"type": "rate_limit_exceeded", "message": "private"}}),
        (400, {"error": {"message": "Invalid parameter budget_exceeded"}}),
        (401, {"error": {"type": "budget_exceeded"}}),
        (500, {"error": {"type": "budget_exceeded"}}),
        (429, ["budget_exceeded"]),
        (429, {"error": "budget_exceeded"}),
    ],
)
def test_other_http_failures_keep_generic_panel_message(monkeypatch, status, body):
    mock_gateway(monkeypatch, lambda _: httpx.Response(status, json=body))
    _, events = asyncio.run(turn(OpenAIChat("key", "Instructions")))
    assert events[-1]["message"] == "The assistant could not finish. Try again."
    assert "code" not in events[-1]


def test_custom_gateway_does_not_follow_redirect_with_key_or_identity(monkeypatch):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(
            307, headers={"location": "https://other.example/responses"}
        )

    mock_gateway(monkeypatch, respond)
    _, events = asyncio.run(
        turn(
            OpenAIChat(
                "key",
                "Instructions",
                send_end_user=True,
                base_url="https://gateway.example/v1",
            ),
            username="viewer",
        )
    )
    assert len(requests) == 1
    assert requests[0].url.host == "gateway.example"
    assert events[-1]["type"] == "error"


def test_provider_label_reaches_model_span(monkeypatch):
    trace = pytest.importorskip("opentelemetry.trace")
    attributes = {}

    class Span:
        def set_attribute(self, key, value):
            attributes[key] = value

    class Tracer:
        @contextmanager
        def start_as_current_span(self, name):
            yield Span()

    monkeypatch.setattr(trace, "get_tracer", lambda _: Tracer())
    mock_gateway(monkeypatch, lambda _: reply())
    asyncio.run(turn(OpenAIChat("key", "Instructions", provider="litellm")))
    assert attributes["shinyhub.agent.provider"] == "litellm"
    assert attributes["shinyhub.agent.input_tokens"] == 11


def test_arbitrary_tool_error_cannot_inject_provider_message():
    class Agent:
        async def run(self, *a, **kw):
            raise ToolError("budget_exceeded", "private untrusted text")
            yield

    _, events = asyncio.run(turn(Agent()))
    assert events[-1]["message"] == "The assistant could not finish. Try again."


def test_browser_cannot_supply_missing_verified_identity(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Missing identity must fail before creating an HTTP client")

    monkeypatch.setattr("shinyhub_agent._openai.httpx.AsyncClient", forbidden)
    agent = OpenAIChat("key", "Instructions", send_end_user=True)
    _, events = asyncio.run(
        turn(agent, raw={"user": "forged", "usage_metadata": {"username": "forged"}})
    )
    assert events[-1]["type"] == "error"
    assert events[-1]["code"] == "end_user_required"
    assert "forged" not in json.dumps(events)


def test_custom_gateway_keeps_identity_local_by_default(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        return reply()

    mock_gateway(monkeypatch, handler)
    agent = OpenAIChat("key", "Instructions", base_url="https://gateway.example/v1")
    asyncio.run(turn(agent, username="local-viewer"))
    assert len(requests) == 1
    assert "user" not in json.loads(requests[0].content)
    assert "local-viewer" not in requests[0].content.decode()
    assert "local-viewer" not in str(requests[0].headers)


def test_oversized_error_body_is_not_fully_read_or_shown(monkeypatch):
    closed = []

    class Oversized(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b" " * 65_536
            yield b" " * 65_536
            pytest.fail("Error body read exceeded the bound")

        async def aclose(self):
            closed.append(True)

    mock_gateway(monkeypatch, lambda request: httpx.Response(429, stream=Oversized()))
    _, events = asyncio.run(turn(OpenAIChat("key", "Instructions")))
    assert closed == [True]
    assert events[-1]["type"] == "error"
    assert events[-1].get("code") != "budget_exceeded"


@pytest.mark.parametrize("usage", ["invalid", [], {"input_tokens_details": "invalid"}])
def test_malformed_optional_usage_does_not_discard_valid_answer(monkeypatch, usage):
    records = []
    mock_gateway(
        monkeypatch,
        lambda _: sse(
            [
                {
                    "type": "response.completed",
                    "response": {
                        "output": [
                            {
                                "type": "message",
                                "content": [{"type": "output_text", "text": "Answer"}],
                            }
                        ],
                        "usage": usage,
                    },
                },
            ]
        ),
    )
    _, events = asyncio.run(
        turn(ResponsesChat("key", "Instructions", on_usage=records.append))
    )
    assert events[-1]["type"] == "done"
    assert records[0].input_tokens is None
    assert records[0].cache_read_tokens is None


def test_multiline_stream_event_has_a_combined_size_bound(monkeypatch):
    # Individually small data lines must not bypass the per-event bound.
    text = "data: {\n" + "data: " + '"padding":"' + "x" * 40_000 + '"\n'
    text += 'data: ,"more":"' + "x" * 40_000 + '"}\n\n'
    mock_gateway(monkeypatch, lambda _: httpx.Response(200, text=text))
    records = []
    _, events = asyncio.run(
        turn(ResponsesChat("key", "Instructions", on_usage=records.append))
    )
    assert events[-1]["type"] == "error"
    assert not any(event["type"] == "delta" for event in events)
    assert records[0].outcome == "error"


def test_empty_sse_data_events_are_ignored_between_valid_events(monkeypatch):
    data = reply(text="Still connected").text
    first, rest = data.split("\n\n", 1)
    data = first + "\n\ndata:\n\ndata: \n\n: keepalive\n\n" + rest
    mock_gateway(monkeypatch, lambda _: httpx.Response(200, text=data))
    _, events = asyncio.run(turn(ResponsesChat("key", "Instructions")))
    assert events[-1]["type"] == "done"
    assert (
        "".join(event["text"] for event in events if event["type"] == "delta")
        == "Still connected"
    )


@pytest.mark.parametrize("ending", ["", "\n"])
def test_response_completes_without_final_empty_sse_line(monkeypatch, ending):
    records = []
    data = reply().text.rstrip("\n") + ending
    mock_gateway(monkeypatch, lambda _: httpx.Response(200, text=data))
    _, events = asyncio.run(
        turn(ResponsesChat("key", "Instructions", on_usage=records.append))
    )
    assert events[-1]["type"] == "done"
    assert records[0].input_tokens == 11
    assert records[0].output_tokens == 5


def test_blank_sse_lines_count_toward_stream_limit():
    from shinyhub_agent._openai import _events

    async def run():
        response = httpx.Response(200, text="\n" * 1_048_577)
        with pytest.raises(ToolError) as error:
            async for _ in _events(response):
                pytest.fail("Blank lines must not produce model events")
        assert error.value.code == "model_too_large"

    asyncio.run(run())


def test_unterminated_sse_line_is_rejected_before_stream_exhaustion(monkeypatch):
    consumed, closed = [], []

    class NoNewline(httpx.AsyncByteStream):
        async def __aiter__(self):
            for i in range(9):
                consumed.append(i)
                yield b"x" * 8_192
            pytest.fail("Unterminated line was buffered beyond the limit")

        async def aclose(self):
            closed.append(True)

    mock_gateway(monkeypatch, lambda _: httpx.Response(200, stream=NoNewline()))
    _, events = asyncio.run(turn(ResponsesChat("key", "Instructions")))
    assert events[-1]["type"] == "error"
    assert consumed == list(range(9))
    assert closed == [True]


@pytest.mark.parametrize("separator", ["\n", "\r", "\r\n"])
def test_sse_byte_boundaries_preserve_utf8_bom_and_line_endings(monkeypatch, separator):
    class ByteStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            data = "\ufeff" + reply(text="View ✓").text.replace("\\u2713", "✓").replace(
                "\n", separator
            )
            for byte in data.encode():
                yield bytes([byte])

    mock_gateway(monkeypatch, lambda _: httpx.Response(200, stream=ByteStream()))
    _, events = asyncio.run(turn(ResponsesChat("key", "Instructions")))
    assert events[-1]["type"] == "done"
    assert (
        "".join(event["text"] for event in events if event["type"] == "delta")
        == "View ✓"
    )


@pytest.mark.parametrize("data", [b"data: \xff\n\n", b"data: \xff"])
def test_invalid_utf8_ends_turn_safely_and_closes_stream(monkeypatch, data):
    closed, records = [], []

    class InvalidUTF8(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield data

        async def aclose(self):
            closed.append(True)

    mock_gateway(monkeypatch, lambda _: httpx.Response(200, stream=InvalidUTF8()))
    _, events = asyncio.run(
        turn(ResponsesChat("key", "Instructions", on_usage=records.append))
    )
    assert events[-1]["type"] == "error"
    assert events[-1]["message"] == "The assistant could not finish. Try again."
    assert closed == [True]
    assert records[0].outcome == "error"
