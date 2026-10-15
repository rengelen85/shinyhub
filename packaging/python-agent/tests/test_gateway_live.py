"""Opt-in, billable integration checks; never configure gateway budgets here."""

import asyncio
import os

import pytest
from shinyhub_agent import ResponsesChat
from test_gateway import turn

pytestmark = pytest.mark.skipif(
    os.environ.get("SHINYHUB_AGENT_LIVE_GATEWAY") != "1",
    reason="Live gateway requires explicit opt-in and deployment configuration",
)


def gateway(records):
    names = (
        "LITELLM_BASE_URL",
        "LITELLM_API_KEY",
        "LITELLM_MODEL",
        "LITELLM_TEST_USER",
        "LITELLM_BLOCKED_TEST_USER",
        "LITELLM_ZERO_BUDGET_TEST_USER",
    )
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.fail("Missing live gateway configuration: " + ", ".join(missing))
    if len({os.environ[name] for name in names[3:]}) != 3:
        pytest.fail("Allowed, exhausted and fresh zero-budget test users must differ")
    return ResponsesChat(
        api_key=os.environ["LITELLM_API_KEY"],
        base_url=os.environ["LITELLM_BASE_URL"],
        model=os.environ["LITELLM_MODEL"],
        instructions=(
            "For every request, first call get_view, then in a separate round "
            "call set_view. After the tool results, answer briefly using them. "
            "Do not request any other tools or repeat a tool call. "
            "If the visitor declines, report that no change was made."
        ),
        provider="litellm",
        send_end_user=True,
        on_usage=records.append,
        max_tool_rounds=2,
    )


@pytest.mark.parametrize("approve", [True, False])
def test_live_gateway_tool_approval_and_final_usage(approve):
    records = []
    agent = gateway(records)
    _, events = asyncio.run(
        turn(agent, username=os.environ["LITELLM_TEST_USER"], approve=approve)
    )
    assert not any(event["type"] == "error" for event in events)
    assert any(event["type"] == "approval_required" for event in events)
    assert any(event["type"] == "action_applied" for event in events) == approve
    assert any(event["type"] == "delta" for event in events)
    assert events[-1]["type"] == "done"
    assert len(records) == 3
    assert records[-1].final
    assert [record.tool_names for record in records[:2]] == [
        ("get_view",),
        ("set_view",),
    ]
    for record in records:
        assert record.provider == "litellm"
        assert record.username == os.environ["LITELLM_TEST_USER"]
        assert record.input_tokens is not None and record.input_tokens > 0
        assert record.output_tokens is not None and record.output_tokens > 0


def test_live_gateway_budget_isolation():
    records = []
    agent = gateway(records)

    async def run():
        _, blocked = await turn(agent, username=os.environ["LITELLM_BLOCKED_TEST_USER"])
        _, allowed = await turn(agent, username=os.environ["LITELLM_TEST_USER"])
        return blocked, allowed

    blocked, allowed = asyncio.run(run())
    assert any(
        event["type"] == "error" and event.get("code") == "budget_exceeded"
        for event in blocked
    )
    assert not any(event["type"] == "done" for event in blocked)
    assert not any(event["type"] == "error" for event in allowed)
    assert allowed[-1]["type"] == "done"


def test_live_gateway_fresh_zero_budget_user_is_blocked():
    records = []
    agent = gateway(records)
    _, events = asyncio.run(
        turn(agent, username=os.environ["LITELLM_ZERO_BUDGET_TEST_USER"])
    )
    assert events[-1]["type"] == "error"
    assert events[-1].get("code") == "budget_exceeded"
    assert not any(
        event["type"] in ("delta", "approval_required", "action_applied", "done")
        for event in events
    )
    assert len(records) == 1
    assert records[0].outcome == "error"
