import asyncio
import logging
from types import SimpleNamespace

import pytest
from shiny import Inputs, reactive
from shiny.session import get_current_session
from shinyhub_agent import AgentTool, ToolError, register
from shinyhub_agent import _shiny

from test_shiny import SCHEMA, Session


@pytest.fixture
def undo_app(monkeypatch):
    from shiny.reactive._core import _reactive_environment

    monkeypatch.setattr(_reactive_environment, "_lock", None)
    captured = []
    original = _shiny.ChatSession

    def capture(*args, **kwargs):
        chat = original(*args, **kwargs)
        captured.append(chat)
        return chat

    monkeypatch.setattr(_shiny, "ChatSession", capture)

    def build(validity=None, *, charts=False, pause=False, undo_timeout=None):
        states = {"period": reactive.Value("week"), "other": reactive.Value("week")}
        calls, undone = [], []
        paused, resume = asyncio.Event(), asyncio.Event()

        class AutoApprovalSession(Session):
            async def send_custom_message(self, name, event):
                await super().send_custom_message(name, event)
                if event.get("type") == "approval_required":
                    chat.decide({
                        "version": 1, "session": chat.nonce,
                        "approvalId": event["approvalId"], "approved": True,
                    })

        session = AutoApprovalSession()

        def write(field):
            async def handler(args):
                previous = states[field].get()
                target = "month" if previous == "year" else "year"
                states[field].set(target)
                return {"field": field, "period": target, "previous": previous}
            return handler

        async def undo(args, result):
            assert states[result["field"]].get() == result["period"]
            undone.append((args, result))
            states[result["field"]].set(result["previous"])
            return {"period": result["previous"]}

        def valid(args, result):
            assert reactive.lock().locked()
            assert get_current_session() is session
            current = states[result["field"]].get()
            calls.append((result["field"], current))
            if validity is not None:
                return validity(args, result)
            return current == result["period"]

        class Agent:
            async def run(self, message, history, tools, approve, **kwargs):
                result = await approve(message, {})
                yield {"type": "action_applied", "name": message, "result": result}
                if pause:
                    paused.set()
                    await resume.wait()
                yield {"type": "delta", "text": "View changed."}

        register(
            session=session, input=Inputs({}), chat=Agent(), charts=charts,
            undo_timeout=undo_timeout,
            tools=[
                AgentTool(
                    name, "Change view", SCHEMA, write(field),
                    read_only=False, confirmation="Change view?", undo=undo,
                    undo_valid=None if name == "unwatched" else valid,
                )
                for name, field in [
                    ("set_period", "period"), ("set_other", "other"), ("unwatched", "other")
                ]
            ],
        )
        chat = captured[-1]
        request = 0

        async def send(**payload):
            nonlocal request
            request += 1
            await asyncio.wait_for(chat.handle({
                "version": 1, "session": chat.nonce, "requestId": f"request_{request:08}",
                **payload,
            }), 2)

        async def change(field, value):
            async with reactive.lock():
                states[field].set(value)
                await reactive.flush()

        def events(kind):
            return [event for _, event in session.messages if event.get("type") == kind]

        def end():
            for callback in session.ended:
                callback()

        return SimpleNamespace(
            chat=chat, session=session, send=send, change=change, events=events,
            calls=calls, undone=undone, states=states, end=end,
            paused=paused, resume=resume,
        )

    return build


@pytest.mark.parametrize("charts", [False, True])
def test_reactive_changes_withdraw_only_the_current_undo_permanently(undo_app, charts):
    async def run():
        app = undo_app(charts=charts)
        await app.send(message="set_period")
        receipt = app.events("action_applied")[-1]
        assert app.calls == [("period", "year")]
        assert app.events("undo_withdrawn") == []
        await app.change("other", "month")
        assert len(app.calls) == 1  # Unrelated state is not a dependency.
        await app.change("period", "month")
        assert app.events("undo_withdrawn") == [{
            "version": 1, "session": app.chat.nonce, "type": "undo_withdrawn",
            "actionId": receipt["actionId"], "message": "Undo no longer available",
        }]
        assert app.chat.last_action is None
        await app.change("period", "year")
        assert len(app.calls) == 2  # The withdrawn action never revives.
        await app.send(action="undo", actionId=receipt["actionId"])
        assert app.events("undo_result")[-1]["ok"] is False
        assert app.undone == []
        app.end()

    asyncio.run(run())


@pytest.mark.parametrize("ending", ["reset", "undo", "replacement", "unwatched", "session"])
def test_monitor_lifecycle_releases_previous_reactive_dependencies(undo_app, ending):
    async def run():
        app = undo_app()
        await app.send(message="set_period")
        old_id = app.events("action_applied")[-1]["actionId"]
        if ending == "reset":
            await app.send(action="reset")
        elif ending == "undo":
            await app.send(action="undo", actionId=old_id)
            assert app.events("undo_result")[-1]["ok"] is True
        elif ending in ("replacement", "unwatched"):
            await app.send(message="set_other" if ending == "replacement" else "unwatched")
        else:
            app.end()
        call_count = len(app.calls)
        await app.change("period", "month")
        assert len(app.calls) == call_count
        assert app.events("undo_withdrawn") == []
        if ending == "replacement":
            await app.change("other", "month")
            assert app.events("undo_withdrawn")[-1]["actionId"] != old_id
        app.end()

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["false", "tool_error", "unexpected", "non_bool", "coroutine"])
def test_invalid_or_failed_check_withdraws_after_receipt_without_leaking_errors(
    undo_app, outcome, caplog,
):
    caplog.set_level(logging.INFO)

    async def mistaken_async():
        return True

    def validity(args, result):
        if outcome == "tool_error":
            raise ToolError("stale_view", "The date range changed again.")
        if outcome == "unexpected":
            raise RuntimeError("private detail")
        if outcome == "non_bool":
            return 1
        if outcome == "coroutine":
            return mistaken_async()
        return False

    async def run():
        app = undo_app(validity)
        await app.send(message="set_period")
        types = [event.get("type") for _, event in app.session.messages]
        assert types.index("action_applied") < types.index("undo_withdrawn")
        assert types[-1] == "done"
        assert app.chat.last_action is None
        message = app.events("undo_withdrawn")[-1]["message"]
        assert message == (
            "The date range changed again." if outcome == "tool_error"
            else "Undo no longer available"
        )
        app.end()

    asyncio.run(run())
    records = [record for record in caplog.records if "validity check" in record.message]
    if outcome == "tool_error":
        assert records[-1].levelno == logging.INFO
        assert records[-1].exc_info is None
    elif outcome != "false":
        assert records[-1].levelno == logging.ERROR
        assert records[-1].exc_info is not None


def test_validity_callback_cannot_mutate_the_stored_undo_snapshot(undo_app):
    def validity(args, result):
        args["injected"] = True
        result["previous"] = "corrupted"
        return True

    async def run():
        app = undo_app(validity)
        await app.send(message="set_period")
        await app.send(action="undo", actionId=app.events("action_applied")[-1]["actionId"])
        assert app.undone == [({}, {"field": "period", "period": "year", "previous": "week"})]
        app.end()

    asyncio.run(run())


def test_withdrawal_during_streaming_does_not_wait_for_the_chat_lock(undo_app):
    async def run():
        app = undo_app(pause=True)
        task = asyncio.create_task(app.send(message="set_period"))
        await asyncio.wait_for(app.paused.wait(), 1)
        assert app.chat.lock.locked()
        await asyncio.wait_for(app.change("period", "month"), 1)
        assert len(app.events("undo_withdrawn")) == 1
        assert not task.done()
        app.resume.set()
        await task
        assert len(app.events("done")) == 1
        app.end()

    asyncio.run(run())


def test_monitors_are_scoped_to_the_viewer_session(undo_app):
    async def run():
        first, second = undo_app(), undo_app()
        await first.send(message="set_period")
        await second.send(message="set_period")
        await first.change("period", "month")
        assert len(first.events("undo_withdrawn")) == 1
        assert second.events("undo_withdrawn") == []
        await second.send(action="undo", actionId=second.events("action_applied")[-1]["actionId"])
        assert second.events("undo_result")[-1]["ok"] is True
        first.end()
        second.end()

    asyncio.run(run())


@pytest.mark.parametrize("ending", ["stop", "session"])
@pytest.mark.parametrize("phase", ["after_send", "during_send"])
def test_monitor_installation_handles_cancellation_while_waiting_for_graph(undo_app, ending, phase):
    async def run():
        app = undo_app()
        receipt_sent = asyncio.Event()
        original_send = app.session.send_custom_message

        async def send_with_busy_graph(name, event):
            await original_send(name, event)
            if event.get("type") == "action_applied":
                # Another session can hold the process-wide graph lock just
                # after the receipt is sent, delaying monitor installation.
                await reactive.lock().acquire()
                receipt_sent.set()
                if phase == "during_send":
                    await asyncio.Event().wait()  # Delivered, then transport backpressure.

        app.session.send_custom_message = send_with_busy_graph
        question = asyncio.create_task(app.send(message="set_period"))
        await asyncio.wait_for(receipt_sent.wait(), 1)
        try:
            if ending == "session":
                app.end()
                # Production launches chat requests in its session task set;
                # this fixture calls ChatSession directly, so cancel it too.
            await app.send(action="cancel")
            await question
        finally:
            reactive.lock().release()
        await app.change("period", "month")
        assert len(app.events("undo_withdrawn")) == (0 if ending == "session" else 1)
        if ending == "stop":
            assert app.chat.last_action is None
        else:
            assert app.calls == []
        app.end()

    asyncio.run(run())


def test_session_end_does_not_cancel_another_sessions_effect_during_monitor_flush(undo_app):
    async def run():
        app = undo_app()
        started, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_send = app.session.send_custom_message
        effects = []

        async def send_with_other_effect(name, event):
            await original_send(name, event)
            if event.get("type") == "action_applied":
                @reactive.effect(session=None)
                async def other_session_effect():
                    started.set()
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        raise
                effects.append(other_session_effect)

        app.session.send_custom_message = send_with_other_effect
        question = asyncio.create_task(app.send(message="set_period"))
        await asyncio.wait_for(started.wait(), 1)
        app.end()
        await app.send(action="cancel")
        await question
        release.set()
        # Wait until the shared flush finishes before inspecting its result.
        async with reactive.lock():
            assert not cancelled.is_set()
            assert app.calls == []
            assert app.events("undo_withdrawn") == []
            for effect in effects:
                effect.destroy()

    asyncio.run(run())


def test_replacement_of_same_state_withdraws_old_action_before_new_receipt(undo_app):
    async def run():
        app = undo_app()
        await app.send(message="set_period")
        first = app.events("action_applied")[-1]
        await app.send(message="set_period")
        second = app.events("action_applied")[-1]
        assert app.events("undo_withdrawn")[-1]["actionId"] == first["actionId"]
        events = [event for _, event in app.session.messages]
        assert events.index(app.events("undo_withdrawn")[-1]) < events.index(second)
        assert app.chat.last_action[0] == second["actionId"]
        await app.send(action="undo", actionId=second["actionId"])
        assert app.events("undo_result")[-1]["ok"] is True
        app.end()

    asyncio.run(run())


def test_monitor_expires_while_idle_without_running_predicate_or_sending_withdrawal(undo_app):
    async def run():
        app = undo_app(undo_timeout=1)
        await app.send(message="set_period")

        async def expired():
            while app.chat.last_action is not None:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(expired(), 2)
        await app.change("period", "month")
        assert app.calls == [("period", "year")]
        assert app.events("undo_withdrawn") == []
        app.end()

    asyncio.run(run())


def test_predicate_tracks_inputs_through_reactive_calculations(undo_app):
    async def run():
        selected = reactive.Value("year")
        inputs = Inputs({"period": selected})

        @reactive.calc(session=None)
        def period():
            return inputs.period()

        app = undo_app(lambda args, result: period() == result["period"])
        await app.send(message="set_period")
        async with reactive.lock():
            selected.set("month")
            await reactive.flush()
        assert len(app.events("undo_withdrawn")) == 1
        app.end()

    asyncio.run(run())


@pytest.mark.parametrize("source", ["unset_input", "req", "req_cancel_output"])
def test_silent_reactive_failures_withdraw_without_exception_logging(undo_app, source, caplog):
    from shiny import req
    inputs = Inputs({"missing": reactive.Value()})

    def validity(args, result):
        if source == "unset_input":
            return inputs.missing()
        req(False, cancel_output=source == "req_cancel_output")

    async def run():
        app = undo_app(validity)
        await app.send(message="set_period")
        assert app.events("undo_withdrawn")[-1]["message"] == "Undo no longer available"
        assert not any(record.levelno >= logging.WARNING for record in caplog.records)
        app.end()

    asyncio.run(run())


def test_reset_does_not_wait_for_another_sessions_graph_lock(undo_app):
    async def run():
        app = undo_app()
        await app.send(message="set_period")
        async with reactive.lock():
            await asyncio.wait_for(app.send(action="reset"), 0.5)
        assert len(app.events("reset")) == 1
        await app.change("period", "month")
        assert app.calls == [("period", "year")]
        assert app.events("undo_withdrawn") == []
        app.end()

    asyncio.run(run())


def test_monitor_setup_failure_withdraws_undo_without_failing_answer(undo_app, monkeypatch, caplog):
    original_flush = reactive.flush
    app = None

    async def failing_monitor_flush():
        if app is not None and app.events("action_applied"):
            raise RuntimeError("private observer detail")
        await original_flush()

    monkeypatch.setattr(reactive, "flush", failing_monitor_flush)

    async def run():
        nonlocal app
        app = undo_app()
        await app.send(message="set_period")
        assert len(app.events("done")) == 1
        assert app.events("error") == []
        assert app.chat.last_action is None
        assert app.events("undo_withdrawn")[-1]["message"] == "Undo no longer available"
        failures = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(failures) == 1
        assert failures[0].message == "Agent undo monitor update failed"
        app.end()

    asyncio.run(run())
