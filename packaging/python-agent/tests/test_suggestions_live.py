"""Opt-in, billable probes for actual model/gateway follow-up compatibility.

Only synthetic facts are sent. No app writes or budget changes are performed.
"""

import asyncio
import os

import pytest
from shinyhub_agent import AgentTool, BedrockChat, OpenAIChat, ToolRegistry
from shinyhub_agent._chat import ChatSession

pytestmark = pytest.mark.skipif(
    os.environ.get("SHINYHUB_AGENT_LIVE_FOLLOWUPS") != "1",
    reason="Real-model follow-up probes require explicit opt-in and model configuration",
)


@pytest.mark.parametrize("backend", ["bedrock", "openai"])
@pytest.mark.parametrize("scenario", ["ordinary", "terminal", "after_read"])
def test_real_model_followup_compatibility(backend, scenario):
    records, reads = [], []
    instructions = "You explain a synthetic spend dashboard using the supplied facts."
    if scenario != "after_read":
        instructions += (
            " The current period costs 120 units; the previous period cost 100 units. "
            "These are all the facts you need; do not call app tools."
        )
    else:
        instructions += " First call read_view exactly once to obtain the facts."
    if scenario != "ordinary":
        instructions += (
            " For this compatibility check, provide your final answer as ordinary "
            "response text and call suggest_followups in that same response with one "
            "standalone question about comparing business lines. Do not repeat the "
            "question in your answer or call suggest_followups before the final answer."
        )
    required = (
        ("SHINYHUB_AGENT_BEDROCK_MODEL", "AWS_REGION")
        if backend == "bedrock"
        else ("SHINYHUB_AGENT_OPENAI_MODEL", "OPENAI_API_KEY")
    )
    if any(not os.environ.get(name) for name in required):
        pytest.skip("Configure " + " and ".join(required) + " for this backend")
    agent = (
        BedrockChat(
            os.environ[required[0]],
            instructions,
            os.environ[required[1]],
            on_usage=records.append,
        )
        if backend == "bedrock"
        else OpenAIChat(
            os.environ[required[1]],
            instructions,
            model=os.environ[required[0]],
            base_url=os.environ.get(
                "SHINYHUB_AGENT_OPENAI_BASE_URL", "https://api.openai.com/v1"
            ),
            provider=os.environ.get("SHINYHUB_AGENT_OPENAI_PROVIDER", "openai"),
            on_usage=records.append,
        )
    )

    # Deliberately retain production defaults, including the 500-token output budget.
    async def read(_):
        assert scenario == "after_read", "Facts are already supplied for direct answers"
        reads.append(True)
        return {"current_spend": 120, "previous_spend": 100}

    async def run():
        events = []

        async def send(event):
            events.append(event)

        tools = ToolRegistry(
            [
                AgentTool(
                    "read_view",
                    "Read synthetic spend for current and previous periods",
                    {"type": "object", "properties": {}, "additionalProperties": False},
                    read,
                )
            ]
        )
        chat = ChatSession(agent, tools, send, followups=2)
        await chat.handle(
            {
                "version": 1,
                "session": chat.nonce,
                "requestId": "probe_12345678",
                "message": "How much did spend increase compared with the previous period?",
            }
        )
        return chat, events

    chat, events = asyncio.run(run())
    assert not any(event["type"] == "error" for event in events)
    assert events[-1]["type"] == "done"
    assert len(records) == (2 if scenario == "after_read" else 1), (
        "Suggestions must not cause an extra model request"
    )
    assert all(record.outcome == "success" for record in records)
    assert len(reads) == (1 if scenario == "after_read" else 0)
    if scenario == "after_read":
        assert records[0].tool_names == ("read_view",)
    assert len(chat.history) == 2
    answer = chat.history[-1]["content"]
    assert answer.strip()
    suggestions = [event for event in events if event["type"] == "suggestions"]
    if scenario != "ordinary":
        assert len(suggestions) == 1
    if suggestions:
        assert [event["type"] for event in events][-2:] == ["suggestions", "done"]
        questions = suggestions[0]["questions"]
        assert 1 <= len(questions) <= 2
        assert all(
            question.casefold() not in answer.casefold() for question in questions
        )
        assert all(question not in str(chat.history) for question in questions)
