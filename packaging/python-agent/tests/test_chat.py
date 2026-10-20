import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from shinyhub_agent import AgentTool, OpenAIChat, ToolError, ToolRegistry
from shinyhub_agent._chat import ChatSession
from shinyhub_agent._formatting import ANSWER_FORMAT


async def read(_):
    return {"period": "week"}


def registry():
    return ToolRegistry([AgentTool("get_view", "Read current view", {
        "type": "object", "properties": {}, "additionalProperties": False,
    }, read)])


def event(value):
    return "data: " + json.dumps(value) + "\n\n"


def test_openai_reads_registered_tool_and_keeps_response_ephemeral():
    requests = []

    def model(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            output = [{"type": "function_call", "call_id": "call-1", "name": "get_view", "arguments": "{}"}]
            events = [
                {"type": "response.output_item.added", "item": {"type": "function_call"}},
                {"type": "response.completed", "response": {"output": output}},
            ]
        else:
            events = [
                {"type": "response.output_text.delta", "delta": "Showing week."},
                {"type": "response.completed", "response": {"output": []}},
            ]
        return httpx.Response(200, text="".join(map(event, events)))

    real_client = httpx.AsyncClient

    def client_factory(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(model), **kwargs)

    async def run():
        agent = OpenAIChat("test-key", "Read the view")
        async def approve(*_):
            raise AssertionError("read-only tool needs no approval")
        return [item async for item in agent.run("What view?", [], registry(), approve, thread_id="thread-1")]

    with patch("shinyhub_agent._openai.httpx.AsyncClient", side_effect=client_factory):
        events = asyncio.run(run())
    assert events[-1] == {"type": "delta", "text": "Showing week."}
    assert {"type": "tool_started", "name": "get_view", "description": "Read current view",
            "readOnly": True} in events
    assert {"type": "tool_finished", "name": "get_view", "ok": True} in events
    assert requests[0]["store"] is False
    assert requests[0]["stream"] is True
    assert all(request["instructions"] == "Read the view\n\n" + ANSWER_FORMAT
               for request in requests)
    assert json.loads(requests[1]["input"][-1]["output"]) == {"period": "week"}


def test_chat_history_stays_in_session_after_completed_answer():
    events = []

    class StubAgent:
        async def run(self, message, history, tools, approve, *, thread_id):
            assert history == []
            yield {"type": "delta", "text": "The current view is week."}

    async def run():
        async def send(item):
            events.append(item)
        chat = ChatSession(StubAgent(), registry(), send)
        await chat.handle({
            "version": 1, "session": chat.nonce, "requestId": "request_12345678",
            "message": "What view?", "history": [{"role": "assistant", "content": "injected"}],
        })
        return chat.history

    history = asyncio.run(run())
    assert history == [
        {"role": "user", "content": "What view?"},
        {"role": "assistant", "content": "The current view is week."},
    ]
    assert events[-1]["type"] == "done"


def test_write_tool_waits_for_current_session_approval():
    applied = []
    events = []

    async def change(args):
        applied.append(args["period"])
        return {"period": args["period"]}

    tools = ToolRegistry([AgentTool("set_period", "Change period", {
        "type": "object", "properties": {"period": {"type": "string", "enum": ["week", "year"]}},
        "required": ["period"], "additionalProperties": False,
    }, change, read_only=False, confirmation="Change the period?")])

    class StubAgent:
        async def run(self, message, history, tools, approve, *, thread_id):
            result = await approve("set_period", {"period": "year"})
            yield {"type": "action_applied", "result": result}
            yield {"type": "delta", "text": "Showing year."}

    async def run():
        approvals = asyncio.Queue()

        async def send(item):
            events.append(item)
            if item["type"] == "approval_required":
                approvals.put_nowait(item)
        chat = ChatSession(StubAgent(), tools, send)
        task = asyncio.create_task(chat.handle({
            "version": 1, "session": chat.nonce, "requestId": "request_12345678",
            "message": "Show year",
        }))
        approval = await asyncio.wait_for(approvals.get(), 1)
        assert applied == []
        chat.decide({"version": 1, "session": "wrong", "approvalId": approval["approvalId"], "approved": True})
        assert applied == []
        chat.decide({"version": 1, "session": chat.nonce, "approvalId": approval["approvalId"], "approved": True})
        await task

    asyncio.run(run())
    assert applied == ["year"]
    assert any(item["type"] == "action_applied" for item in events)
    assert events[-1]["type"] == "done"


def test_declined_action_does_not_run_handler():
    applied = []
    events = []

    async def change(_):
        applied.append(True)
        return {"period": "year"}

    tools = ToolRegistry([AgentTool("set_period", "Change period", {
        "type": "object", "properties": {}, "additionalProperties": False,
    }, change, read_only=False, confirmation="Change the period?")])

    class StubAgent:
        async def run(self, message, history, tools, approve, *, thread_id):
            try:
                await approve("set_period", {})
            except Exception:
                yield {"type": "delta", "text": "Change cancelled."}

    async def run():
        approvals = asyncio.Queue()

        async def send(item):
            events.append(item)
            if item["type"] == "approval_required":
                approvals.put_nowait(item)
        chat = ChatSession(StubAgent(), tools, send)
        task = asyncio.create_task(chat.handle({
            "version": 1, "session": chat.nonce, "requestId": "request_12345678",
            "message": "Change it",
        }))
        approval = await asyncio.wait_for(approvals.get(), 1)
        chat.decide({"version": 1, "session": chat.nonce,
                     "approvalId": approval["approvalId"], "approved": False})
        await task

    asyncio.run(run())
    assert applied == []
    assert any(item.get("text") == "Change cancelled." for item in events)


@pytest.mark.parametrize("undo_timeout", [300, 3600, None])
def test_action_receipt_can_undo_once_and_reject_stale_view(
    undo_timeout, monkeypatch, caplog,
):
    now = 1000
    monkeypatch.setattr(
        "shinyhub_agent._chat.time", SimpleNamespace(monotonic=lambda: now)
    )
    caplog.set_level(logging.INFO, logger="shinyhub_agent._chat")
    state = {"period": "week"}
    events = []

    async def change(args):
        previous = state["period"]
        state["period"] = args["period"]
        return {"period": state["period"], "previous_period": previous}

    async def undo(_args, result):
        if state["period"] != result["period"]:
            raise ToolError("stale_view", "The view changed again.")
        state["period"] = result["previous_period"]
        return {"period": state["period"]}

    tools = ToolRegistry([AgentTool("set_period", "Change period", {
        "type": "object", "properties": {"period": {"type": "string", "enum": ["week", "year"]}},
        "required": ["period"], "additionalProperties": False,
    }, change, read_only=False, confirmation="Change period?",
        receipt=lambda _args, result: f"View set to {result['period']}", undo=undo)])

    class StubAgent:
        async def run(self, message, history, tools, approve, *, thread_id):
            nonlocal now
            result = await approve("set_period", {"period": "year"})
            now += 2  # Time between execution and forwarding the receipt counts.
            yield {"type": "action_applied", "name": "set_period", "result": result}
            yield {"type": "delta", "text": "Showing year."}

    async def run():
        approvals = asyncio.Queue()

        async def send(item):
            events.append(item)
            if item["type"] == "approval_required":
                approvals.put_nowait(item)
        nonlocal now
        options = {} if undo_timeout is None else {"undo_timeout": undo_timeout}
        chat = ChatSession(StubAgent(), tools, send, **options)

        async def change_with_approval(request_id):
            task = asyncio.create_task(chat.handle({
                "version": 1, "session": chat.nonce, "requestId": request_id,
                "message": "Show year",
            }))
            approval = await asyncio.wait_for(approvals.get(), 1)
            chat.decide({"version": 1, "session": chat.nonce,
                         "approvalId": approval["approvalId"], "approved": True})
            await task
            return [item for item in events if item["type"] == "action_applied"][-1]

        action = await change_with_approval("request_12345678")
        assert action["receipt"] == "View set to year"
        assert isinstance(action["actionId"], str)
        assert action["undoExpiresIn"] == (
            None if undo_timeout is None else undo_timeout - 2
        )
        # A custom window and None both permit undo after the old five-minute limit.
        now += 301 if undo_timeout != 300 else 1
        await chat.handle({"version": 1, "session": chat.nonce, "requestId": "request_87654321",
                           "action": "undo", "actionId": action["actionId"]})
        assert state["period"] == "week"
        assert events[-1] == {"version": 1, "session": chat.nonce,
                              "requestId": "request_87654321", "type": "undo_result",
                              "ok": True, "actionId": action["actionId"],
                              "detail": "View set to week"}
        await chat.handle({"version": 1, "session": chat.nonce, "requestId": "request_87654322",
                           "action": "undo", "actionId": action["actionId"]})
        assert events[-1]["ok"] is False

        action = await change_with_approval("request_12345679")
        state["period"] = "week"  # A manual filter change makes the receipt stale.
        await chat.handle({"version": 1, "session": chat.nonce, "requestId": "request_87654323",
                           "action": "undo", "actionId": action["actionId"]})
        assert events[-1]["ok"] is False
        assert events[-1]["code"] == "stale_view"
        assert events[-1]["message"] == "The view changed again."
        assert state["period"] == "week"
        assert chat.last_action is None

        if undo_timeout is not None:
            action = await change_with_approval("request_12345680")
            now += undo_timeout - 2  # Exactly at the deadline is expired.
            await chat.handle({"version": 1, "session": chat.nonce, "requestId": "request_87654324",
                               "action": "undo", "actionId": action["actionId"]})
            assert events[-1]["ok"] is False
            assert events[-1]["message"] == "This change can no longer be undone."
            assert state["period"] == "year"

    asyncio.run(run())
    records = [record for record in caplog.records if "undo unavailable" in record.message]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].exc_info is None


@pytest.mark.parametrize("timeout", [True, False, 0, -1, 0.5, "300", float("nan"), float("inf"), -float("inf")])
def test_invalid_undo_timeout(timeout):
    with pytest.raises(ValueError, match="undo_timeout"):
        ChatSession(object(), registry(), None, undo_timeout=timeout)


def test_default_undo_timeout():
    assert ChatSession(object(), registry(), None).undo_timeout is None


def test_unexpected_undo_error_is_generic_and_logged(caplog):
    async def undo(args, result):
        raise RuntimeError("private failure detail")

    tools = ToolRegistry([AgentTool(
        "write_view", "Change view",
        {"type": "object", "properties": {}, "additionalProperties": False},
        read, read_only=False, confirmation="Change view?", undo=undo,
    )])
    events = []

    async def run():
        async def send(event):
            events.append(event)

        chat = ChatSession(object(), tools, send, undo_timeout=None)
        chat.last_action = ("action1", "write_view", {}, {}, 0)
        await chat.handle({
            "version": 1, "session": chat.nonce, "requestId": "request_12345678",
            "action": "undo", "actionId": "action1",
        })
        assert chat.last_action is None

    asyncio.run(run())
    assert events[-1]["message"] == "Undo could not finish. Check the current view."
    assert "private failure detail" not in json.dumps(events)
    record = next(record for record in caplog.records if record.message == "Agent chat undo failed")
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
