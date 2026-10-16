# Inline chart demo

Run against the local helper implementation from this directory:

```sh
uv venv
uv pip install -e ../../packaging/python-agent
uv run --no-project shiny run --port 8000 app.py
```

Open http://127.0.0.1:8000 and choose **Open assistant**. The default mode uses
scripted responses and makes no model calls. It exercises the real chart tool,
server validation, session data storage, and locally bundled browser renderer.
Try “line chart”, “bar chart”, “area chart”, and “scatter chart”.

Set `OPENAI_API_KEY` before starting to use ResponsesChat instead. Requests then
use the configured provider's paid API, and dataset samples/statistics go to it.
For Bedrock, install the optional SDK (`uv pip install boto3`) and set
`SHINYHUB_AGENT_BEDROCK_MODEL`, `AWS_REGION`, and the usual AWS credentials
or `AWS_PROFILE` before starting. Bedrock takes precedence when configured.
For Responses, optional `SHINYHUB_AGENT_OPENAI_MODEL` and
`SHINYHUB_AGENT_OPENAI_BASE_URL` select the model and endpoint. The page labels
scripted versus live mode. AGUIChat can also use these same tools.

Every number is illustrative. Real app tools must apply the current viewer's
permissions and filters before creating `ChartData`. See the helper README's
**Inline charts** section for dataset limits, follow-up behavior, and privacy.

For OpenRouter, set `OPENROUTER_API_KEY` and `OPENROUTER_MODEL` (a model ID
supporting tool calls), then start the app. This uses the Responses-compatible
endpoint `https://openrouter.ai/api/v1` and takes precedence over other demo
providers. Subscription login tokens are never used.
