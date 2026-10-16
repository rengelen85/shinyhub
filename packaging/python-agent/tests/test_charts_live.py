"""Opt-in, billable chart acceptance tests using only synthetic app-owned data."""

import asyncio
import os

import pytest
from shinyhub_agent import (
    AgentTool,
    AGUIChat,
    BedrockChat,
    ChartData,
    ResponsesChat,
    ToolRegistry,
)
from shinyhub_agent._chat import ChatSession


@pytest.mark.parametrize("backend", ["responses", "openrouter", "bedrock", "agui"])
def test_real_model_chart_and_followup(backend):
    if os.environ.get("SHINYHUB_AGENT_LIVE_CHARTS") != backend:
        pytest.skip(
            "Set SHINYHUB_AGENT_LIVE_CHARTS to the backend to opt into billable tests"
        )
    instructions = (
        "Use get_sales to read authorized synthetic data, then render_chart to draw requested charts. "
        "Reuse the previous dataset on follow-up questions. Do not invent values. Answer briefly."
    )
    required = {
        "responses": ["OPENAI_API_KEY", "SHINYHUB_AGENT_OPENAI_MODEL"],
        "openrouter": ["OPENROUTER_API_KEY", "OPENROUTER_MODEL"],
        "bedrock": ["AWS_REGION", "SHINYHUB_AGENT_BEDROCK_MODEL"],
        "agui": ["SHINYHUB_AGENT_AGUI_URL"],
    }[backend]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        pytest.fail(
            "Configure " + ", ".join(missing) + " before running live acceptance"
        )
    if backend == "openrouter":
        agent = ResponsesChat(
            api_key=os.environ["OPENROUTER_API_KEY"],
            model=os.environ["OPENROUTER_MODEL"],
            base_url="https://openrouter.ai/api/v1",
            provider="openrouter",
            instructions=instructions,
            max_output_tokens=1200,
        )
    elif backend == "responses":
        agent = ResponsesChat(
            api_key=os.environ["OPENAI_API_KEY"],
            model=os.environ["SHINYHUB_AGENT_OPENAI_MODEL"],
            base_url=os.environ.get(
                "SHINYHUB_AGENT_OPENAI_BASE_URL", "https://api.openai.com/v1"
            ),
            instructions=instructions,
            max_output_tokens=1200,
        )
    elif backend == "bedrock":
        agent = BedrockChat(
            model_id=os.environ["SHINYHUB_AGENT_BEDROCK_MODEL"],
            region=os.environ["AWS_REGION"],
            instructions=instructions,
            max_output_tokens=1200,
        )
    else:
        agent = AGUIChat(
            endpoint=os.environ["SHINYHUB_AGENT_AGUI_URL"],
            bearer_token=os.environ.get("SHINYHUB_AGENT_AGUI_TOKEN", ""),
        )
    rows = [
        {"month": f"2026-{i:02d}-01", "revenue": revenue, "cost": cost}
        for i, revenue, cost in [(1, 120, 70), (2, 190, 110), (3, 160, 95)]
    ]

    async def read(_):
        return ChartData(
            rows,
            {"month": "temporal", "revenue": "quantitative", "cost": "quantitative"},
            "Synthetic sales",
        )

    async def run():
        events = []

        async def send(event):
            events.append(event)

        chat = ChatSession(
            agent,
            ToolRegistry(
                [
                    AgentTool(
                        "get_sales",
                        "Read synthetic monthly sales for charts",
                        {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                        read,
                    )
                ]
            ),
            send,
            charts=True,
        )
        previous = None
        for index, (prompt, kind, x) in enumerate(
            [
                (
                    "Read get_sales and draw a line chart of revenue by month using render_chart.",
                    "line",
                    "month",
                ),
                (
                    "Using the same dataset from the previous chart, draw a scatter chart with cost on x and revenue on y using render_chart.",
                    "scatter",
                    "cost",
                ),
            ]
        ):
            events.clear()
            await chat.handle(
                {
                    "version": 1,
                    "session": chat.nonce,
                    "requestId": f"chart_probe_{index:08d}",
                    "message": prompt,
                }
            )
            assert events[-1]["type"] == "done"
            assert not any(e["type"] in ("error", "approval_required") for e in events)
            charts = [e["chart"] for e in events if e["type"] == "chart"]
            assert len(charts) == 1
            chart = charts[0]
            assert (chart["kind"], chart["x"], chart["y"]) == (kind, x, "revenue")
            assert chart["rows"] == rows
            if previous:
                assert chart["dataset_id"] == previous
            previous = chart["dataset_id"]
        await chat.handle(
            {
                "version": 1,
                "session": chat.nonce,
                "requestId": "chart_reset_12345678",
                "action": "reset",
            }
        )
        assert not chat.charts.datasets and not chat.history

    asyncio.run(run())
