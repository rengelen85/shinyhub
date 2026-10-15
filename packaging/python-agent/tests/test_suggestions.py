"""Suggestions are metadata: never actions, extra rounds, or chat history."""

import asyncio
import json

import pytest
from shiny import Inputs
from shinyhub_agent import (
    AgentTool,
    AGUIChat,
    OpenAIChat,
    ToolError,
    ToolRegistry,
    register,
)
from shinyhub_agent._chat import ChatSession
from shinyhub_agent._formatting import FOLLOWUP_INSTRUCTIONS
from shinyhub_agent._suggestions import (
    clean_followups,
    validate_starters,
)
from test_reliability import registry, run_backend
from test_shiny import SCHEMA, Session, read_view


@pytest.mark.parametrize("value", ["Question?", 3, {}, iter(["Question?"])])
def test_starters_reject_non_sequences(value):
    with pytest.raises(TypeError):
        validate_starters(value)


@pytest.mark.parametrize(
    "value",
    [
        ["x"] * 5,
        [""],
        ["  "],
        ["x" * 121],
        ["a\nb"],
        ["a\rb"],
        ["a\u2028b"],
        ["a\x00b"],
        ["<b>Question?</b>"],
        [None],
        ["Cost?", " cost? "],
        ["Straße?", "STRASSE?"],
    ],
)
def test_invalid_starters_fail_at_registration(value):
    with pytest.raises(ValueError):
        register(session=Session(), input=Inputs({}), tools=[], starters=value)


@pytest.mark.parametrize(
    "starters,expected", [(None, None), ([], []), ([" Cost? "], ["Cost?"])]
)
def test_starters_capabilities(starters, expected):
    async def run():
        session = Session()
        register(
            session=session,
            input=Inputs({}),
            tools=[AgentTool("read_view", "Read", SCHEMA, read_view)],
            chat=object(),
            starters=starters,
        )
        await session.flushed[0]()
        return session.messages[-1][1]

    capabilities = asyncio.run(run())
    if expected is None:
        assert "starters" not in capabilities
    else:
        assert capabilities["starters"] == expected


@pytest.mark.parametrize("value", [True, False, -1, 4, 1.5, "2", None])
def test_followup_configuration_is_bounded(value):
    with pytest.raises(ValueError):
        register(session=Session(), input=Inputs({}), tools=[], followups=value)


def test_followups_require_a_supporting_agent_and_reserve_the_tool_name():
    for agent in [None, object(), AGUIChat("https://example.test/run")]:
        with pytest.raises(ValueError, match="(requires a chat|does not support)"):
            register(
                session=Session(),
                input=Inputs({}),
                tools=[AgentTool("read_view", "Read", SCHEMA, read_view)],
                chat=agent,
                followups=2,
            )
    with pytest.raises(ValueError, match="reserved"):
        ChatSession(
            OpenAIChat("key", "Help"),
            ToolRegistry(
                [AgentTool("suggest_followups", "App tool", SCHEMA, read_view)]
            ),
            read_view,
            followups=2,
        )


def test_configuring_one_session_does_not_mutate_shared_adapter():
    agent = OpenAIChat("key", "Help")
    one = ChatSession(agent, registry([]), read_view, followups=1)
    three = ChatSession(agent, registry([]), read_view, followups=3)
    assert agent.followups == 0
    assert one.agent.followups == 1
    assert three.agent.followups == 3


def test_followups_drop_invalid_items_trim_deduplicate_and_cap():
    values = [
        None,
        "",
        " \n",
        "x" * 101,
        "a\nb",
        "<img src=x>",
        " Cost? ",
        "COST?",
        "Straße?",
        "STRASSE?",
        "Other?",
    ]
    assert clean_followups(values, 2) == ["Cost?", "Straße?"]
    assert clean_followups([], 3) == []
    assert clean_followups("Cost?", 3) == []
    assert clean_followups(values, 0) == []


async def no_approval(*_):
    raise AssertionError("Suggestions never request approval")


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_terminal_suggestions_complete_in_one_call_without_executing_tools(backend):
    seen, usage = [], []
    events, requests = asyncio.run(
        run_backend(
            backend,
            [
                {
                    "text": "The total is 123.",
                    "calls": [
                        (
                            "suggest_followups",
                            {"questions": [" Cost? ", "COST?", "Trend?", "Other?"]},
                        )
                    ],
                }
            ],
            registry(seen),
            no_approval,
            followups=2,
            on_usage=usage.append,
        )
    )
    assert len(requests) == 1
    assert seen == []
    assert events[-1] == {"type": "suggestions", "questions": ["Cost?", "Trend?"]}
    assert not any(e["type"] in ("tool_started", "tool_finished") for e in events)
    assert len(usage) == 1
    assert "suggest_followups" in usage[0].tool_names
    encoded = json.dumps(requests[0])
    assert "suggest_followups" in encoded
    instructions = (
        requests[0]["system"][0]["text"]
        if backend == "bedrock"
        else requests[0]["instructions"]
    )
    assert FOLLOWUP_INSTRUCTIONS in instructions


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
@pytest.mark.parametrize(
    "arguments",
    [
        {"questions": []},
        {"questions": ["\n", "x" * 101]},
        {"questions": "bad"},
        {"questions": ["Cost?"], "extra": True},
        [],
        None,
    ],
)
def test_bad_suggestions_do_not_fail_a_valid_answer(backend, arguments):
    events, requests = asyncio.run(
        run_backend(
            backend,
            [{"text": "Answer.", "calls": [("suggest_followups", arguments)]}],
            registry([]),
            no_approval,
            followups=2,
        )
    )
    assert len(requests) == 1
    assert not any(e["type"] == "suggestions" for e in events)
    assert any(e.get("text") == "Answer." for e in events)


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
@pytest.mark.parametrize("suggestion_first", [True, False])
def test_mixed_suggestions_leave_no_orphan_call_and_do_not_use_execution_budget(
    backend, suggestion_first
):
    calls = [("suggest_followups", {"questions": ["Cost?"]}), ("read_view", {})]
    if not suggestion_first:
        calls.reverse()
    seen = []
    events, requests = asyncio.run(
        run_backend(
            backend,
            [
                {"text": "Reading.", "calls": calls},
                {"text": "Answer."},
            ],
            registry(seen),
            no_approval,
            followups=2,
            max_tool_calls_per_step=1,
        )
    )
    assert seen == ["read"]
    assert len(requests) == 2
    assert not any(e["type"] == "suggestions" for e in events)
    transcript = (
        requests[1]["messages"] if backend == "bedrock" else requests[1]["input"]
    )
    assert "suggest_followups" not in json.dumps(transcript)
    assert "tool_deferred" not in json.dumps(transcript)
    assert "123" in json.dumps(transcript)
    assert "Reading." in json.dumps(transcript)


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_repeated_metadata_calls_are_dropped_without_another_round(backend):
    events, requests = asyncio.run(
        run_backend(
            backend,
            [
                {
                    "text": "Answer.",
                    "calls": [
                        ("suggest_followups", {"questions": ["Cost?"]}),
                        ("suggest_followups", {"questions": ["Trend?"]}),
                    ],
                }
            ],
            registry([]),
            no_approval,
            followups=2,
        )
    )
    assert len(requests) == 1
    assert not any(e["type"] == "suggestions" for e in events)


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_suggestion_only_response_is_not_a_completed_answer(backend):
    with pytest.raises(ToolError):
        asyncio.run(
            run_backend(
                backend,
                [
                    {
                        "calls": [
                            ("suggest_followups", {"questions": ["Cost?"]}),
                        ]
                    }
                ],
                registry([]),
                no_approval,
                followups=2,
            )
        )


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_incomplete_provider_response_never_emits_suggestions(backend):
    with pytest.raises(ToolError):
        asyncio.run(
            run_backend(
                backend,
                [
                    {
                        "text": "Partial.",
                        "fail": True,
                        "calls": [
                            ("suggest_followups", {"questions": ["Cost?"]}),
                        ],
                    }
                ],
                registry([]),
                no_approval,
                followups=2,
            )
        )


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_final_budget_response_cannot_offer_suggestions(backend):
    events, requests = asyncio.run(
        run_backend(
            backend,
            [
                {"calls": [("read_view", {})]},
                {
                    "text": "Answer.",
                    "calls": [("suggest_followups", {"questions": ["Cost?"]})],
                },
            ],
            registry([]),
            no_approval,
            followups=2,
            max_tool_rounds=1,
        )
    )
    assert len(requests) == 2
    assert not any(e["type"] == "suggestions" for e in events)
    assert "action limit" in events[-1]["text"]
    if backend == "bedrock":
        assert "toolConfig" not in requests[1]
    else:
        assert requests[1]["tool_choice"] == "none"


@pytest.mark.parametrize(
    "outcome", ["complete", "error", "cancel", "expired", "empty", "event_error"]
)
def test_session_holds_metadata_until_success_and_keeps_it_out_of_history(outcome):
    async def run():
        events = []
        waiting = asyncio.Event()

        class Agent:
            def with_followups(self, maximum):
                assert maximum == 2
                return self

            async def run(self, message, history, tools, approve, **kwargs):
                yield {
                    "type": "suggestions",
                    "questions": [" Cost? ", "COST?", "Trend?", "Other?"],
                }
                if outcome == "expired":
                    try:
                        await approve("write_view", {})
                    except ToolError:
                        pass  # The normal runtime continues to explain the expired action.
                if outcome != "empty":
                    yield {"type": "delta", "text": "Answer."}
                if outcome == "error":
                    raise ToolError("failed", "Failure")
                if outcome == "event_error":
                    yield {"type": "error", "message": "Failed"}
                if outcome == "cancel":
                    waiting.set()
                    await asyncio.Event().wait()

        async def send(event):
            events.append(event)

        chat = ChatSession(Agent(), registry([]), send, followups=2, approval_timeout=1)
        task = asyncio.create_task(
            chat.handle(
                {
                    "version": 1,
                    "session": chat.nonce,
                    "requestId": "request_12345678",
                    "message": "Question",
                }
            )
        )
        if outcome == "cancel":
            await waiting.wait()
            await chat.handle(
                {
                    "version": 1,
                    "session": chat.nonce,
                    "requestId": "cancel_12345678",
                    "action": "cancel",
                }
            )
        await task
        suggestions = [e for e in events if e["type"] == "suggestions"]
        if outcome == "complete":
            assert suggestions[0]["questions"] == ["Cost?", "Trend?"]
            assert [e["type"] for e in events][-2:] == ["suggestions", "done"]
        else:
            assert not suggestions
        assert "Cost?" not in json.dumps(chat.history)
        assert events[-1]["type"] == (
            "done" if outcome in ("complete", "expired") else "error"
        )

    asyncio.run(run())


def test_four_starters_at_the_length_boundary_are_valid():
    values = [str(i) + "x" * 119 for i in range(4)]
    assert validate_starters(tuple(values)) == values


def test_enabled_followups_are_published_as_capabilities():
    async def run():
        session = Session()
        register(
            session=session,
            input=Inputs({}),
            tools=[AgentTool("read_view", "Read", SCHEMA, read_view)],
            chat=OpenAIChat("key", "Help"),
            followups=2,
        )
        await session.flushed[0]()
        assert session.messages[-1][1]["followups"] == 2

    asyncio.run(run())


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_malformed_suggestion_json_does_not_fail_the_answer(backend, monkeypatch):
    import test_reliability

    original = test_reliability.model_reply

    def malformed(kind, reply):
        result = original(kind, reply)
        if kind == "bedrock":
            for item in result:
                delta = item.get("contentBlockDelta", {}).get("delta", {})
                if "toolUse" in delta:
                    delta["toolUse"]["input"] = "{"
            return result
        return result.replace('"arguments": "{}"', '"arguments": "{"')

    monkeypatch.setattr(test_reliability, "model_reply", malformed)
    events, requests = asyncio.run(
        run_backend(
            backend,
            [{"text": "Answer.", "calls": [("suggest_followups", {})]}],
            registry([]),
            no_approval,
            followups=2,
        )
    )
    assert len(requests) == 1
    assert any(e.get("text") == "Answer." for e in events)
    assert not any(e["type"] == "suggestions" for e in events)


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_text_in_an_earlier_tool_round_does_not_make_metadata_only_response_terminal(
    backend,
):
    with pytest.raises(ToolError):
        asyncio.run(
            run_backend(
                backend,
                [
                    {"text": "Reading your view.", "calls": [("read_view", {})]},
                    {"calls": [("suggest_followups", {"questions": ["Cost?"]})]},
                ],
                registry([]),
                no_approval,
                followups=2,
            )
        )


@pytest.mark.parametrize("followups", [0, 2])
@pytest.mark.parametrize(
    "content", [None, [None], [{"type": "output_text", "text": None}]]
)
def test_gateway_nullable_message_fields_do_not_break_tool_rounds(
    followups, content, monkeypatch
):
    import test_reliability

    original = test_reliability.model_reply

    def nullable(kind, reply):
        result = original(kind, reply)
        frames = [
            json.loads(line[5:])
            for line in result.splitlines()
            if line.startswith("data:")
        ]
        for frame in frames:
            output = frame.get("response", {}).get("output", [])
            if any(item.get("type") == "function_call" for item in output):
                output.append({"type": "message", "content": content})
        return "".join("data: " + json.dumps(frame) + "\n\n" for frame in frames)

    monkeypatch.setattr(test_reliability, "model_reply", nullable)
    seen = []
    events, requests = asyncio.run(
        run_backend(
            "openai",
            [
                {"calls": [("read_view", {})]},
                {"text": "Answer."},
            ],
            registry(seen),
            no_approval,
            followups=followups,
        )
    )
    assert seen == ["read"]
    assert len(requests) == 2
    assert events[-1] == {"type": "delta", "text": "Answer."}


def test_gateway_final_output_text_without_deltas_can_carry_suggestions(monkeypatch):
    import test_reliability

    original = test_reliability.model_reply

    def no_deltas(kind, reply):
        frames = [
            json.loads(line[5:])
            for line in original(kind, reply).splitlines()
            if line.startswith("data:")
        ]
        return "".join(
            "data: " + json.dumps(frame) + "\n\n"
            for frame in frames
            if frame["type"] != "response.output_text.delta"
        )

    monkeypatch.setattr(test_reliability, "model_reply", no_deltas)
    events, requests = asyncio.run(
        run_backend(
            "openai",
            [
                {
                    "text": "Answer.",
                    "calls": [("suggest_followups", {"questions": ["Cost?"]})],
                }
            ],
            registry([]),
            no_approval,
            followups=2,
        )
    )
    assert len(requests) == 1
    assert events == [
        {"type": "delta", "text": "Answer."},
        {"type": "suggestions", "questions": ["Cost?"]},
    ]


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
def test_token_truncation_during_suggestions_never_commits_an_answer(
    backend, monkeypatch
):
    import test_reliability

    original = test_reliability.model_reply

    def truncated(kind, reply):
        result = original(kind, reply)
        if kind == "bedrock":
            for event in result:
                if "messageStop" in event:
                    event["messageStop"]["stopReason"] = "max_tokens"
            return result
        frames = [
            json.loads(line[5:])
            for line in result.splitlines()
            if line.startswith("data:")
        ]
        for frame in frames:
            if frame["type"] == "response.completed":
                frame["type"] = "response.incomplete"
                frame["response"]["incomplete_details"] = {
                    "reason": "max_output_tokens"
                }
        return "".join("data: " + json.dumps(frame) + "\n\n" for frame in frames)

    monkeypatch.setattr(test_reliability, "model_reply", truncated)
    with pytest.raises(ToolError):
        asyncio.run(
            run_backend(
                backend,
                [
                    {
                        "text": "Answer.",
                        "calls": [("suggest_followups", {"questions": ["Cost?"]})],
                    }
                ],
                registry([]),
                no_approval,
                followups=2,
            )
        )


@pytest.mark.parametrize("outcome", ["success", "error"])
def test_custom_agents_keep_default_error_messages_and_cannot_enable_metadata(outcome):
    async def run():
        class Agent:
            async def run(self, *args, **kwargs):
                yield {"type": "delta", "text": "Answer."}
                yield {"type": "suggestions", "questions": ["Cost?"]}
                if outcome == "error":
                    yield {
                        "type": "error",
                        "message": "Please choose a shorter period.",
                        "code": "shorten_period",
                    }
                    yield {"type": "delta", "text": "Unreachable."}

        events = []

        async def send(event):
            events.append(event)

        chat = ChatSession(Agent(), registry([]), send)
        await chat.handle(
            {
                "version": 1,
                "session": chat.nonce,
                "requestId": "request_12345678",
                "message": "Question",
            }
        )
        assert not any(event["type"] == "suggestions" for event in events)
        if outcome == "error":
            assert events[-1]["message"] == "Please choose a shorter period."
            assert events[-1]["code"] == "shorten_period"
            assert [event["type"] for event in events].count("error") == 1
            assert not any(event["type"] == "done" for event in events)
            assert chat.history == []
        else:
            assert events[-1]["type"] == "done"
            assert chat.history[-1]["content"] == "Answer."

    asyncio.run(run())
