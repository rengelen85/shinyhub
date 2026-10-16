"""Inline charts from session-owned data; runnable without model credentials."""

import os

from shiny import App, ui
from shinyhub_agent import (
    AgentTool,
    BedrockChat,
    ChartData,
    ResponsesChat,
    chat_dependency,
    register,
)

# Synthetic, pre-aggregated values. App-owned code controls every plotted number.
ROWS = [
    {
        "month": f"2026-{month:02d}-01",
        "region": region,
        "revenue_eur": revenue,
        "cost_eur": round(revenue * cost),
    }
    for region, values, cost in [
        ("North", [12000, 19000, 16000, 21000, 23500, 25000], 0.61),
        ("South", [9000, 13500, 18000, 17200, 22000, 24500], 0.68),
        ("West", [8000, 11000, 12500, 14800, 16700, 19500], 0.55),
    ]
    for month, revenue in enumerate(values, 1)
]


class ScriptedCharts:
    """Exercise the real tools and renderer without an external model."""

    async def run(self, message, history, tools, approve, *, thread_id):
        lower = message.lower()
        kind = next(
            (kind for kind in ("scatter", "area", "bar") if kind in lower), "line"
        )
        yield {"type": "status", "text": "Reading illustrative sales"}
        dataset = await tools.execute("get_sales", {})
        await tools.execute(
            "render_chart",
            {
                "dataset_id": dataset["dataset_id"],
                "kind": kind,
                "x": "cost_eur" if kind == "scatter" else "month",
                "y": "revenue_eur",
                "color": "region",
                "title": "Illustrative revenue by region (EUR)",
            },
        )
        yield {
            "type": "delta",
            "text": (
                "Here is the requested chart of synthetic sales. Hover for exact values, "
                "expand the chart, or open **View data**. Each answer preserves its own snapshot."
            ),
        }


instructions = (
    "Help explore this illustrative sales dataset. All values are synthetic. "
    "Use get_sales for authorized chart data and render_chart for inline charts. "
    "Use existing dataset IDs to adjust charts on follow-up questions. "
    "Rows are monthly values per region. For revenue comparisons, plot month on x, "
    "revenue_eur on y, and region as color; use cost_eur on x for scatter plots. "
    "Values are EUR. Samples are not the full dataset: avoid inferring trends "
    "or aggregates you have not read. Explain briefly, grounded in returned statistics."
)
key = os.environ.get("OPENAI_API_KEY")
bedrock_model = os.environ.get("SHINYHUB_AGENT_BEDROCK_MODEL")
openrouter_key = os.environ.get("OPENROUTER_API_KEY")
if openrouter_key:
    chat = ResponsesChat(
        api_key=openrouter_key,
        model=os.environ["OPENROUTER_MODEL"],
        base_url="https://openrouter.ai/api/v1",
        provider="openrouter",
        instructions=instructions,
        max_output_tokens=1200,
    )
    mode = "Live model mode · OpenRouter · " + os.environ["OPENROUTER_MODEL"]
elif bedrock_model:
    chat = BedrockChat(
        model_id=bedrock_model,
        region=os.environ["AWS_REGION"],
        instructions=instructions,
        max_output_tokens=1200,
    )
    mode = "Live model mode · Amazon Bedrock · " + bedrock_model
elif key:
    chat = ResponsesChat(
        api_key=key,
        model=os.environ.get("SHINYHUB_AGENT_OPENAI_MODEL", "gpt-4.1-mini"),
        base_url=os.environ.get(
            "SHINYHUB_AGENT_OPENAI_BASE_URL", "https://api.openai.com/v1"
        ),
        max_output_tokens=1200,
        instructions=instructions,
    )
    mode = "Live model mode · Responses API"
else:
    chat = ScriptedCharts()
    mode = "Scripted demo mode · No external model calls. Ask for a chart type to try the real tool and rendering pipeline."

app_ui = ui.page_fluid(
    chat_dependency(),
    ui.tags.style("""
      body { background: #f4f7fc; color: #16203a; font-family: system-ui, sans-serif; }
      main { max-width: 720px; margin: 12vh auto; padding: 24px; }
      h1 { font-size: clamp(2rem, 5vw, 3rem); letter-spacing: -.03em; }
      p { color: #45526e; line-height: 1.6; }
      button.demo-open { background: #0369a1; color: white; border: 0; border-radius: 8px; padding: 12px 20px; font: inherit; cursor: pointer; }
    """),
    ui.tags.main(
        ui.h1("Explore sales in conversation"),
        ui.p(
            "Ask for a line, bar, area, or scatter chart. Explore three regions across six months of illustrative revenue and costs."
        ),
        ui.p(mode),
        ui.tags.button(
            "Open assistant",
            class_="demo-open",
            onclick="window.shinyhubAgentChat?.open()",
        ),
    ),
    title="Inline charts · ShinyHub",
)


def server(input, output, session):
    async def get_sales(_):
        # Real apps must apply the viewer's authorization and filters here.
        return ChartData(
            ROWS,
            {
                "month": "temporal",
                "region": "nominal",
                "revenue_eur": "quantitative",
                "cost_eur": "quantitative",
            },
            "Illustrative monthly sales (EUR)",
        )

    register(
        session=session,
        input=input,
        chat=chat,
        charts=True,
        starters=[
            "Show monthly revenue by region",
            "Make a grouped bar chart of monthly revenue",
            "Plot costs versus revenue as a scatter chart",
        ],
        tools=[
            AgentTool(
                "get_sales",
                "Read synthetic monthly revenue and costs by region for charts",
                {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                get_sales,
            )
        ],
    )


app = App(app_ui, server)
