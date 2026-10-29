from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import os
import time
from collections.abc import Sequence
from typing import Any

from ._chat import ChatAgent, ChatSession
from ._core import AgentTool, ToolError, ToolRegistry
from ._runtime import bounded_int
from ._session import VERSION, SessionTools
from ._suggestions import validate_starters

REQUEST_INPUT = ".shinyhub_agent_request"
DISCOVER_INPUT = ".shinyhub_agent_discover"
CAPABILITIES_MESSAGE = "shinyhub-agent-capabilities"
RESULT_MESSAGE = "shinyhub-agent-result"
CHAT_CAPABILITIES_MESSAGE = "shinyhub-agent-chat-capabilities"
CHAT_EVENT_MESSAGE = "shinyhub-agent-chat-event"
CHAT_INPUT = ".shinyhub_agent_chat_request"
CHAT_DECISION_INPUT = ".shinyhub_agent_chat_decision"
REGISTRATION_MARKER = "_shinyhub_agent_registration"
logger = logging.getLogger(__name__)


def agent_dependency():
    """Add to the UI to expose tools to supported browser agents."""
    from htmltools import HTMLDependency

    return HTMLDependency(
        "shinyhub-agent",
        "0.4.1",
        source={"package": "shinyhub_agent", "subdir": "www"},
        script={"src": "bridge.js", "defer": "defer"},
        stylesheet={"href": "bridge.css"},
    )


def chat_dependency():
    """Add the built-in chat panel to the UI when registering a chat agent."""
    from htmltools import HTMLDependency

    return HTMLDependency(
        "shinyhub-agent-chat",
        "0.4.1",
        source={"package": "shinyhub_agent", "subdir": "www"},
        script=[
            {"src": src, "defer": "defer"}
            for src in (
                "charts.js",
                "chat.js",
            )
        ],
        stylesheet={"href": "chat.css"},
    )


def register(
    *,
    session: Any,
    input: Any,
    tools: Sequence[AgentTool],
    chat: ChatAgent | None = None,
    notice: str | None = None,
    starters: Sequence[str] | None = None,
    followups: int = 0,
    charts: bool = False,
    approval_timeout: float = 30,
    undo_timeout: float | None = None,
    history_exchanges: int = 6,
    max_answer_chars: int = 8_000,
    usage_metadata: dict[str, str | None] | None = None,
    allow_browser_writes: bool = False,
) -> ToolRegistry:
    """Register session tools; browser writes require an explicit opt-in.

    Chat writes retain server-side approval regardless of this setting.
    An optional plain-text notice stays visible in the chat panel and is never
    sent to the model. Blank notices are treated as absent; the limit is 1,000
    characters. Optional starters replace the built-in empty-state questions
    (None preserves defaults; an empty sequence hides them). followups enables
    up to three experimental, editable, never auto-submitted suggestions on supporting
    chat agents. charts enables experimental chat-only rendering of ChartData results.
    These settings leave existing apps unchanged by default.

    undo_timeout defaults to None, keeping undo until the next write or
    chat/session reset. Set finite seconds (at least 1) to limit the window.
    Tools may supply a synchronous undo_valid callback: reactive reads are
    tracked and a False result permanently withdraws that action's chat Undo.
    Tool and undo callbacks hold the reactive lock. Inputs changed with
    ui.update_*() cannot reflect the browser update until the callback returns;
    report the normalized values sent instead of waiting for an input echo.
    """
    from shiny import reactive
    from shiny.session import session_context
    from shiny.types import SilentCancelOutputException, SilentException

    starters = validate_starters(starters)

    if notice is not None:
        if not isinstance(notice, str):
            raise TypeError("notice must be a string or None")
        if len(notice) > 1_000:
            raise ValueError("notice must be at most 1,000 characters")
        if not notice.strip():
            notice = None

    if getattr(session, REGISTRATION_MARKER, None) is not None:
        raise RuntimeError("Register agent tools only once per session")
    namespace_probe = "__shinyhub_namespace_probe"
    if str(session.ns(namespace_probe)) != namespace_probe:
        raise ValueError("Register agent tools from the top-level server session")

    async def execute(operation):
        # Only app callbacks hold the graph lock; model I/O and approval do not.
        async with reactive.lock():
            with session_context(session), reactive.isolate():
                try:
                    return await operation()
                finally:
                    await reactive.flush()

    bounded_int("followups", followups, 0, 3)
    if followups and chat is None:
        raise ValueError("followups requires a chat agent")
    if not isinstance(charts, bool):
        raise TypeError("charts must be a bool")
    if charts and chat is None:
        raise ValueError("charts requires a chat agent")
    registry = ToolRegistry(tools, executor=execute)
    dispatcher = SessionTools(registry, allow_browser_writes=allow_browser_writes)
    chat_session = (
        ChatSession(
            chat,
            registry,
            lambda event: session.send_custom_message(CHAT_EVENT_MESSAGE, event),
            approval_timeout=approval_timeout,
            undo_timeout=undo_timeout,
            history_exchanges=history_exchanges,
            max_answer_chars=max_answer_chars,
            followups=followups,
            charts=charts,
            usage_metadata={
                "app": os.environ.get("SHINYHUB_APP_SLUG"),
                **(usage_metadata or {}),
            },
        )
        if chat is not None
        else None
    )
    # These transport inputs can contain prompts and tool arguments. Shiny
    # bookmarks serialize dot-prefixed inputs unless explicitly excluded.
    for input_id in (REQUEST_INPUT, DISCOVER_INPUT, CHAT_INPUT, CHAT_DECISION_INPUT):
        if input_id not in session.bookmark.exclude:
            session.bookmark.exclude.append(input_id)
    setattr(session, REGISTRATION_MARKER, registry)
    tasks: set[asyncio.Task[Any]] = set()
    undo_updates: set[asyncio.Task[Any]] = set()
    undo_monitor = None
    ended = False

    def stop_undo_monitor():
        nonlocal undo_monitor
        if undo_monitor is not None:
            undo_monitor.destroy()
            undo_monitor = None

    async def watch_undo():
        nonlocal undo_monitor
        # Chat callbacks run outside the graph lock. Install and first-run the
        # monitor under that lock, without taking the chat lock from an effect.
        async with reactive.lock():
            stop_undo_monitor()
            if ended or chat_session is None:
                return
            action = chat_session.last_action
            tool = registry.get(action[1]) if action is not None else None
            if action is None or tool is None or tool.undo_valid is None:
                return

            # Manage this effect ourselves: registering a fresh session.on_ended
            # callback for every receipt would retain all previous monitors.
            @reactive.effect(session=None)
            async def monitor():
                if chat_session.last_action is not action:
                    stop_undo_monitor()
                    return
                if chat_session.undo_timeout is not None:
                    remaining = chat_session.undo_timeout - (time.monotonic() - action[4])
                    if remaining <= 0:
                        chat_session.last_action = None
                        stop_undo_monitor()
                        return
                    # Dispose even when the app is idle. Destroying the effect
                    # also cancels this context's timer.
                    # Some event loops wake timers slightly early. A small
                    # margin avoids rechecking app state just before expiry.
                    reactive.invalidate_later(remaining + 0.05, session=None)
                message = "Undo no longer available"
                try:
                    # Unlike tool execution, this must NOT be isolated: reads
                    # subscribe the monitor to the app's reactive dependencies.
                    with session_context(session):
                        valid = tool.undo_valid(
                            copy.deepcopy(action[2]), copy.deepcopy(action[3])
                        )
                    if inspect.iscoroutine(valid):
                        valid.close()
                    if not isinstance(valid, bool):
                        raise TypeError("undo_valid must return a bool")
                    if valid:
                        return
                except (SilentException, SilentCancelOutputException):
                    pass  # Unset inputs and req() withdraw quietly.
                except ToolError as error:
                    logger.info("Agent undo validity check rejected: %s", error.code)
                    message = str(error)
                except Exception:
                    logger.exception("Agent undo validity check failed")
                # A withdrawal is permanent, even if the view later changes back.
                # There is no await between checking the action and consuming it.
                chat_session.last_action = None
                stop_undo_monitor()
                await session.send_custom_message(
                    CHAT_EVENT_MESSAGE,
                    {
                        "version": VERSION,
                        "session": chat_session.nonce,
                        "type": "undo_withdrawn",
                        "actionId": action[0],
                        "message": message,
                    },
                )

            undo_monitor = monitor
            await reactive.flush()

    def launch(operation):
        async def run():
            with session_context(session), reactive.isolate():
                await operation()

        task = asyncio.create_task(run())
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    def undo_update_finished(task):
        undo_updates.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error(
                "Agent undo monitor update failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def refresh_undo_monitor():
        try:
            await watch_undo()
        except Exception:
            # An applied write remains a successful answer even if observer
            # setup or the shared flush fails. Fail closed for its undo.
            logger.exception("Agent undo monitor update failed")
            action = chat_session.last_action
            if action is None:
                return
            chat_session.last_action = None
            stop_undo_monitor()
            try:
                await session.send_custom_message(
                    CHAT_EVENT_MESSAGE,
                    {
                        "version": VERSION,
                        "session": chat_session.nonce,
                        "type": "undo_withdrawn",
                        "actionId": action[0],
                        "message": "Undo no longer available",
                    },
                )
            except Exception:
                logger.exception("Agent undo withdrawal notification failed")

    def update_undo_monitor():
        if ended or chat_session.last_action is None:
            stop_undo_monitor()
            return
        # A flush can run other sessions' effects. Never cancel it with this
        # answer or session; teardown destroys our monitor and the ended guard
        # prevents pending updates from installing another one.
        task = asyncio.create_task(refresh_undo_monitor())
        undo_updates.add(task)
        task.add_done_callback(undo_update_finished)
        return task

    if chat_session is not None and any(tool.undo_valid is not None for tool in tools):
        chat_session._on_undo_change = update_undo_monitor

    def cancel_tasks():
        nonlocal ended
        ended = True
        stop_undo_monitor()
        for task in tuple(tasks):
            task.cancel()

    session.on_ended(cancel_tasks)

    async def publish() -> None:
        await session.send_custom_message(
            CAPABILITIES_MESSAGE,
            dispatcher.capabilities(),
        )
        if chat_session is not None:
            capabilities = chat_session.capabilities()
            if starters is not None:
                capabilities["starters"] = starters
            if followups:
                capabilities["followups"] = followups
            if notice is not None:
                capabilities["notice"] = notice
            await session.send_custom_message(CHAT_CAPABILITIES_MESSAGE, capabilities)

    session.on_flushed(publish, once=True)

    @reactive.effect
    @reactive.event(input[DISCOVER_INPUT], ignore_none=True)
    async def _discover() -> None:
        await publish()

    @reactive.effect
    @reactive.event(input[REQUEST_INPUT], ignore_none=True)
    def _request() -> None:
        raw = input[REQUEST_INPUT]()

        async def respond():
            response = await dispatcher.handle(raw)
            if response is not None:
                await session.send_custom_message(RESULT_MESSAGE, response)

        launch(respond)

    if chat_session is not None:

        @reactive.effect
        @reactive.event(input[CHAT_INPUT], ignore_none=True)
        def _chat_request() -> None:
            raw = input[CHAT_INPUT]()
            launch(lambda: chat_session.handle(raw))

        @reactive.effect
        @reactive.event(input[CHAT_DECISION_INPUT], ignore_none=True)
        def _chat_decision() -> None:
            chat_session.decide(input[CHAT_DECISION_INPUT]())

    return registry
