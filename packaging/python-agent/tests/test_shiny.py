import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from shiny import Inputs, reactive, ui
from shiny._namespaces import Root
from shinyhub_agent import AgentTool, OpenAIChat, register
from shinyhub_agent._shiny import (
    CHAT_CAPABILITIES_MESSAGE,
    CHAT_EVENT_MESSAGE,
    CHAT_INPUT,
    CHAT_DECISION_INPUT,
    DISCOVER_INPUT,
    REQUEST_INPUT,
)

SCHEMA = {"type": "object", "properties": {}, "additionalProperties": False}


class Session:
    ns = Root

    def __init__(self):
        self.flushed = []
        self.ended = []
        self.messages = []
        self.updates = []
        self.bookmark = SimpleNamespace(exclude=[])

    def on_flushed(self, fn, once):
        self.flushed.append(fn)

    def on_ended(self, fn):
        self.ended.append(fn)

    def is_stub_session(self):
        return False

    def send_input_message(self, id, message):
        self.updates.append((id, message))

    async def send_custom_message(self, name, message):
        self.messages.append((name, message))


@pytest.mark.parametrize("chat", [None, object()])
def test_agent_payloads_are_excluded_from_native_bookmarks(chat):
    async def run():
        session = Session()
        session.bookmark.exclude = ["app_secret", CHAT_INPUT]
        inputs = Inputs({
            "period": reactive.Value("week"),
            REQUEST_INPUT: reactive.Value({"tool": "read_view"}),
            DISCOVER_INPUT: reactive.Value({"version": 1}),
            CHAT_INPUT: reactive.Value({"message": "sensitive prompt"}),
            CHAT_DECISION_INPUT: reactive.Value({"approved": True}),
            "app_secret": reactive.Value("secret"),
        })
        register(
            session=session,
            input=inputs,
            tools=[AgentTool("read_view", "Read view", SCHEMA, read_view)],
            chat=chat,
        )
        try:
            assert session.bookmark.exclude.count(CHAT_INPUT) == 1
            assert await inputs._serialize(
                exclude=session.bookmark.exclude, state_dir=None
            ) == {"period": "week"}
        finally:
            for fn in session.ended:
                fn()

    asyncio.run(run())


def test_registered_callbacks_read_update_flush_and_undo_without_app_plumbing(
    monkeypatch,
):
    # Shiny's graph lock is process-global; each asyncio.run below uses a fresh loop.
    from shiny.reactive._core import _reactive_environment

    monkeypatch.setattr(_reactive_environment, "_lock", None)

    async def run():
        session = Session()
        inputs = Inputs({"period": reactive.Value("week")})
        state = reactive.Value("week")
        observed = []

        @reactive.effect(session=None)
        def output():
            observed.append(state.get())

        def validate(args):
            assert reactive.lock().locked()
            assert inputs.period() == "week"

        async def write(args):
            assert reactive.lock().locked()
            previous = state.get()
            ui.update_select("period", selected="year")
            state.set("year")
            return {"period": state.get(), "previous": previous}

        async def undo(args, result):
            assert reactive.lock().locked()
            ui.update_select("period", selected=result["previous"])
            state.set(result["previous"])
            return {"period": state.get()}

        tools = register(
            session=session,
            input=inputs,
            tools=[
                AgentTool(
                    "write_view",
                    "Change view",
                    SCHEMA,
                    write,
                    read_only=False,
                    confirmation="Change view?",
                    validate=validate,
                    undo=undo,
                )
            ],
        )
        result = await tools.execute("write_view", {})
        assert result == {"period": "year", "previous": "week"}
        assert observed[-1] == "year"
        await tools.undo("write_view", {}, result)
        assert observed[-1] == "week"
        assert [update[1]["value"] for update in session.updates] == [
            ["year"],
            ["week"],
        ]
        assert not reactive.lock().locked()
        output.destroy()
        for fn in session.ended:
            fn()

    asyncio.run(run())


@pytest.mark.parametrize("undo_timeout", [1.5, 3600, None, "default"])
def test_session_end_cancels_model_work_without_holding_reactive_lock(monkeypatch, undo_timeout):
    from shinyhub_agent import _shiny
    from shiny.reactive._core import _reactive_environment

    monkeypatch.setattr(_reactive_environment, "_lock", None)
    chat_sessions = []
    original = _shiny.ChatSession

    def capture_chat(*args, **kwargs):
        chat = original(*args, **kwargs)
        chat_sessions.append(chat)
        return chat

    monkeypatch.setattr(_shiny, "ChatSession", capture_chat)

    async def run():
        session = Session()
        chat_input = reactive.Value(None)
        inputs = Inputs({CHAT_INPUT: chat_input})
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def read(args):
            return {}

        class Agent:
            async def run(self, *args, **kwargs):
                assert not reactive.lock().locked()
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
                yield {"type": "delta", "text": "unreachable"}

        register(
            session=session,
            input=inputs,
            tools=[AgentTool("read_view", "Read view", SCHEMA, read)],
            chat=Agent(),
            **({} if undo_timeout == "default" else {"undo_timeout": undo_timeout}),
        )
        assert chat_sessions[0].undo_timeout == (
            None if undo_timeout == "default" else undo_timeout
        )
        await session.flushed[0]()
        nonce = next(
            message["session"]
            for name, message in session.messages
            if name == CHAT_CAPABILITIES_MESSAGE
        )
        chat_input.set(
            {
                "version": 1,
                "session": nonce,
                "requestId": "request_12345678",
                "message": "Question",
            }
        )
        await reactive.flush()
        await asyncio.wait_for(started.wait(), 1)
        assert not reactive.lock().locked()
        for fn in session.ended:
            fn()
        await asyncio.wait_for(cancelled.wait(), 1)
        await asyncio.sleep(0)
        assert any(
            name == CHAT_EVENT_MESSAGE and message["type"] == "error"
            for name, message in session.messages
        )

    asyncio.run(run())


async def read_view(_):
    return {}


@pytest.mark.parametrize(
    "notice", [None, "", "  \n", "<b>AI can make mistakes</b>", "x" * 1_000]
)
def test_notice_is_only_published_as_panel_configuration(notice):
    async def run():
        session = Session()
        register(
            session=session,
            input=Inputs({}),
            tools=[AgentTool("read_view", "Read view", SCHEMA, read_view)],
            chat=object(),
            notice=notice,
        )
        await session.flushed[0]()
        capabilities = next(
            value
            for name, value in session.messages
            if name == CHAT_CAPABILITIES_MESSAGE
        )
        if notice and notice.strip():
            assert capabilities["notice"] == notice
        else:
            assert set(capabilities) == {"version", "session", "enabled"}
        for fn in session.ended:
            fn()

    asyncio.run(run())


@pytest.mark.parametrize(
    "notice, error",
    [(False, TypeError), (42, TypeError), ([], TypeError), ("x" * 1_001, ValueError)],
)
def test_notice_validation_precedes_registration(notice, error):
    session = Session()
    with pytest.raises(error, match="notice"):
        register(session=session, input=Inputs({}), tools=[], notice=notice)
    assert session.flushed == []
    assert not hasattr(session, "_shinyhub_agent_registration")


def test_registered_notice_never_reaches_model_payload_or_usage(monkeypatch):
    from shiny.reactive._core import _reactive_environment

    monkeypatch.setattr(_reactive_environment, "_lock", None)
    requests, records = [], []
    notice = "NOTICE_ONLY: Check important figures against the charts."

    def endpoint(request):
        requests.append(json.loads(request.content))
        events = [
            {"type": "response.output_text.delta", "delta": "Costs increased."},
            {
                "type": "response.completed",
                "response": {
                    "output": [],
                    "usage": {"input_tokens": 11, "output_tokens": 5},
                },
            },
        ]
        return httpx.Response(
            200, text="".join("data: " + json.dumps(e) + "\n\n" for e in events)
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        "shinyhub_agent._openai.httpx.AsyncClient",
        lambda *a, **kw: real_client(*a, transport=httpx.MockTransport(endpoint), **kw),
    )

    async def run():
        session = Session()
        value = reactive.Value(None)
        register(
            session=session,
            input=Inputs({CHAT_INPUT: value}),
            tools=[AgentTool("read_view", "Read view", SCHEMA, read_view)],
            notice=notice,
            chat=OpenAIChat("fake-key", "Use app facts", on_usage=records.append),
        )
        await session.flushed[0]()
        capabilities = next(
            v for name, v in session.messages if name == CHAT_CAPABILITIES_MESSAGE
        )
        assert capabilities["notice"] == notice
        assert requests == records == []
        for index, action in enumerate(["message", "message", "reset", "message"]):
            value.set(
                {
                    "version": 1,
                    "session": capabilities["session"],
                    "requestId": f"request_{index:08}",
                    **(
                        {"action": action}
                        if action == "reset"
                        else {"message": "What changed?"}
                    ),
                }
            )
            await reactive.flush()

            async def finished(request_id=f"request_{index:08}"):
                while not any(
                    name == CHAT_EVENT_MESSAGE
                    and event["requestId"] == request_id
                    and event["type"] in ("done", "reset", "error")
                    for name, event in session.messages
                ):
                    await asyncio.sleep(0)

            await asyncio.wait_for(finished(), 2)
        for fn in session.ended:
            fn()

    asyncio.run(run())
    assert len(requests) == len(records) == 3
    assert notice not in json.dumps(requests)
    assert "Costs increased." in json.dumps(requests[1])
    assert "Costs increased." not in json.dumps(requests[2])
    assert all(
        record.input_tokens == 11 and record.output_tokens == 5 for record in records
    )
