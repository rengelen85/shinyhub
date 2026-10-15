# ShinyHub agent tools for Python Shiny

`0.3.0` is a regular release of the session-scoped Python helper. The API is
still below `1.0`; pin the package and review migration notes before upgrading.
Browser WebMCP availability depends on the browser. The app remains responsible
for tool permissions and for checking consequential results.

This helper lets an app declare a small, typed set of tools for the **current
viewer session**. The app owns all data access and state changes. ShinyHub does
not infer tools from visible inputs or let a model run arbitrary R/Python code.
The same registry powers built-in chat and browser WebMCP.

```python
from shiny import App, reactive, ui
from shinyhub_agent import AgentTool, agent_dependency, register

app_ui = ui.page_fluid(agent_dependency(), ui.input_select("period", "Period", ["week", "year"]))

def server(input, output, session):
    selected_period = reactive.value("week")

    @reactive.effect
    @reactive.event(input.period)
    def from_control():
        selected_period.set(input.period())

    async def current_view(args):
        return {"period": selected_period.get()}

    async def set_period(args):
        selected_period.set(args["period"])
        ui.update_select("period", selected=args["period"])
        return {"period": selected_period.get()}

    # Explicitly allow browser agents to change this session's display filter.
    register(session=session, input=input, allow_browser_writes=True, tools=[
        AgentTool("get_view", "Read the selected period", {
            "type": "object", "properties": {}, "additionalProperties": False,
        }, current_view),
        AgentTool("set_period", "Change the selected period", {
            "type": "object", "properties": {"period": {"type": "string", "enum": ["week", "year"]}},
            "required": ["period"], "additionalProperties": False,
        }, set_period, read_only=False, confirmation="Change the dashboard period?"),
    ])

app = App(app_ui, server)
```

The helper sends version 1 capability messages over the existing Shiny session.
Arguments are validated again on the server with JSON Schema. Each session has
its own registry, nonce, concurrency lock, request budget, timeouts, and bounded
messages. Errors do not reveal handler exceptions or tool arguments. A handler
must perform its own permission checks for sensitive data or actions.

## Built-in chat or a hoster-owned agent

Add `chat_dependency()` to the UI and pass one chat backend to `register()`:

On ShinyHub versions with toolbar chat support, the helper announces chat
availability and the toolbar shows **Ask**. On hosts with native chat support,
ShinyHub supplies the overlay frame and controls while the helper supplies the
conversation body and agent backend. Its own launcher and frame remain available
outside ShinyHub or when the toolbar is hidden. Apps without chat do not show
**Ask**.

Assistant answers render basic Markdown in both panel layouts: paragraphs,
line breaks, `**bold**`, `*italic*`, inline code, bullet and numbered lists
with one nested level, fenced code blocks, and simple pipe tables. ATX headings
(`# Heading`) appear as bold paragraphs. **Copy answer** copies the original
Markdown, including its tables. Questions remain plain text.

Tables require a header and separator with matching column counts; each
separator cell contains at least three hyphens. Outer pipes are optional.
Escape a pipe within a cell as `\|`, including inside inline code. Use `---:`
to right-align a column or `:---:` to center it. Body rows must match the header
width; malformed rows remain visible as text rather than losing values. Wide
tables and code blocks scroll horizontally inside the answer.

On desktop, drag the panel's left edge or choose **Expand assistant** for more
room; **Restore assistant width** returns to your previous width. The resize
edge also supports Left/Right arrows (Shift for larger steps) and Home/End.
Width and expansion are remembered for the tab, when session storage is
available, and constrained to the viewport. Small screens keep the full-screen
layout. Descriptive table cells wrap; numeric values stay together. Native
panel resizing requires a ShinyHub toolbar that supports these controls.

Links, autolinks, images and HTML never create active browser content. Their
syntax stays visible, as do unsupported markers such as blockquotes, task lists,
footnotes, underscore emphasis and strikethrough. Inline emphasis uses asterisks;
backslash escapes preserve literal punctuation. Every source line break is
displayed. This is a limited dialect, not full CommonMark or GFM support.

`ResponsesChat` (also available as `OpenAIChat`) and `BedrockChat` append a
shared description of these formatting
capabilities to the app's `instructions` on every model request, including tool
follow-ups. Explicit app formatting preferences still take precedence. With
`AGUIChat`, configure the same formatting guidance on the hoster-owned endpoint;
the helper does not modify that agent's system prompt. Formatting guidance is
advisory; safe rendering does not depend on the model following it.

```python
import os
from shinyhub_agent import AGUIChat, BedrockChat, ResponsesChat, chat_dependency

# Include chat_dependency() alongside agent_dependency() in the app UI.
if os.environ.get("SHINYHUB_AGENT_AGUI_URL"):
    chat = AGUIChat(
        endpoint=os.environ["SHINYHUB_AGENT_AGUI_URL"],
        bearer_token=os.environ.get("SHINYHUB_AGENT_AGUI_TOKEN", ""),
    )
elif os.environ.get("SHINYHUB_AGENT_BEDROCK_MODEL_ID"):
    chat = BedrockChat(
        model_id=os.environ["SHINYHUB_AGENT_BEDROCK_MODEL_ID"],
        region=os.environ["AWS_REGION"],
        instructions="You help with this dashboard. Use registered tools for app facts.",
    )
else:
    chat = ResponsesChat(
        api_key=os.environ["OPENAI_API_KEY"],
        instructions="You help with this dashboard. Use registered tools for app facts.",
    )
register(session=session, input=input, tools=tools, chat=chat)
```

Pass `notice=` to `register()` to display an app-owned notice below the chat
header, separate from assistant messages:

```python
register(
    session=session, input=input, tools=tools, chat=chat,
    notice="The assistant uses generative AI and can make mistakes. "
           "Check important figures against the charts before you act on them.",
)
```

The notice stays visible above the conversation in native and fallback panels,
including after **New chat** and closing/reopening the panel. It is plain text:
HTML and Markdown appear literally. It is never added to model requests,
conversation history, or `UsageRecord` token counts. Long notices scroll within
their own keyboard-accessible area to leave room for the conversation and
composer. The question field references the notice for screen readers. Short
screens use a compact layout when a notice is configured.

`notice` accepts a string of at most 1,000 characters or `None` (the default).
Empty or whitespace-only strings behave like `None`; nonblank strings retain
their whitespace. Without a notice the existing panel is unchanged. Configure
it per viewer alongside the app's existing chat access gate. It has no effect
when `chat` is absent.

Install `shinyhub-agent[bedrock]` for `BedrockChat`. It uses Bedrock's
`ConverseStream` API and the standard AWS credential chain. Give the app only
`bedrock:InvokeModelWithResponseStream` for its chosen model or inference
profile. The model must support streaming tool use. On an on-premises ShinyHub
host, store AWS
credentials as private per-app secrets; on AWS, prefer a scoped workload role.
Choose a model ID available in the selected region. The adapter does not infer
one because model and tool support vary by region. Bedrock requests inherit the
AWS account's invocation logging and data policies.

The browser never receives model or endpoint credentials. OpenAI requests use
the Responses API with streaming, bounded output, and `store: false`. AG-UI
requests carry the current session's thread ID, recent messages, and registered
tool schemas; they do not carry ShinyHub cookies, identity headers, or other
apps' data. The hoster must
authorize and secure their endpoint. Tool calls from all three backends are
validated again by the app. A write pauses for visitor approval in the chat
panel, then runs the handler and returns its applied result to the agent.
For a clear action result, a write tool may provide `receipt=lambda args,
result: f"View set to {result['period']}"`. A tool may also provide an async
`undo(args, result)` handler. The chat then offers Undo for five minutes. Undo
runs in the same viewer's Shiny session without asking the model; the handler
must verify the app is still in the state created by that action before
restoring the previous state. Only the most recent write remains undoable.

An AG-UI agent hosted in Amazon Bedrock AgentCore needs an authenticated
`InvokeAgentRuntime` client or a hoster-managed HTTPS relay. `AGUIChat` does
not sign AgentCore requests itself.

When WebMCP is available, `bridge.js` registers the exposed tools. In other
browsers it makes them available through `window.shinyhubAgentTools.invoke()`
for an app-supplied assistant. By default, browser capabilities contain only
read-only tools, and the server rejects direct calls to write tools even with
a valid session nonce. Chat retains the full registry and server-side approval
for writes. To expose writes to browser agents, explicitly pass
`allow_browser_writes=True` to `register()` (as in the display-filter example).
Opted-in browser writes show visitor confirmation before dispatch, but that
confirmation can be bypassed by a client. It is **not a security authorization
boundary**; the app handler must still decide what the viewer may do. Keep
consequential actions on the chat path when server-side approval is required.

Upgrading from a release that exposed browser writes automatically requires
this explicit opt-in to retain that behavior. Chat-only integrations need no
change.

## Starter questions and follow-ups

Both settings are optional and belong to `register()`:

```python
register(
    session=session,
    input=input,
    tools=tools,
    chat=chat,
    starters=[
        "How did spend change versus the previous period?",
        "Which business lines cost the most?",
        "What will this month cost at the current run rate?",
    ],
    followups=2,
)
```

`starters=None` preserves the two built-in questions; `starters=[]` hides them.
Supply a sequence of at most four strings, each 1–120 characters after trimming.
Blank entries, line breaks, control characters, HTML tags, and duplicates
(case-insensitive after trimming) raise at registration. Starters are rendered
as literal text and keep the existing submit-on-click behaviour. They are panel
configuration, not model instructions.

Follow-ups are **experimental** until verified with your intended model and gateway.
`followups=0` (the default) disables them. Integers 1–3 enable optional suggestions in `BedrockChat` and `OpenAIChat` / `ResponsesChat`. The setting uses
a session-specific adapter copy, so sharing a chat adapter between viewers is
safe. `AGUIChat` does not currently support this setting; enabling it with an
unsupported agent raises at registration rather than silently doing nothing.

The adapters expose `suggest_followups` as a metadata tool and instruct the model
to offer follow-ups only through that channel. A successfully completed model
response with nonblank answer text and exactly one suggestion call ends the turn
without a tool result or another model request. Suggestion calls do not consume
app-tool budgets; their model tokens and request still appear in usage records.
Suggestions are best-effort: a model may return none. No additional model request
is made to obtain them, and the budget-ending response keeps all tools disabled.
There is no guaranteed latency or token saving. Answer text and suggestion arguments
share `max_output_tokens` (500 by default). Leave headroom for both: reaching the
output-token limit fails the turn even if answer text has already streamed, and
no suggestions are shown. Incomplete provider responses are never promoted to
successful answers.

If suggestions appear beside app tools, they are discarded from both execution
and the replayed tool transcript. Repeated suggestion calls are also discarded.
Malformed suggestion JSON or invalid questions are dropped without losing a valid
answer, within the existing stream and payload safety limits. The server trims
questions, rejects blank, multiline, control-character, HTML-tag, and overlong
entries (100 characters), removes case-insensitive duplicates, and keeps at most
the configured count. None of this validates factual accuracy or relevance.

Buttons appear only when an answer completes. A click fills and focuses the
composer **without sending**. Edit the question before sending; existing write
approvals still apply. While the composer contains a draft, the buttons are
disabled with an explanation to prevent overwriting it. Suggestions clear on the
next submission, reset, reconnect, error, cancellation, or successful Undo. An
expired approval suppresses suggestions for the rest of that turn, even when the
assistant finishes an explanation. Suggestions are never stored in conversation
history; sending one creates an ordinary user message.

Custom agents can opt in by implementing `with_followups(maximum: int)`, returning
a session-specific agent configured for that limit. Its existing `run()` signature
does not change. Yield `{"type": "suggestions", "questions": [...]}` as metadata;
`ChatSession` holds and validates it, then emits it immediately before `done` only
after a successful, nonempty answer. An agent's `error` event is forwarded with its
original message and terminates the turn without suggestions or a completed reply
in history. Applied actions are still recorded when a reply is interrupted. Agents must not execute suggestions as app
tools or make extra model requests solely to generate them. The name
`suggest_followups` is reserved among registered app tools while enabled.

## Backend protocols and routing

Choose an adapter by the protocol exposed by your endpoint:

| Adapter | Protocol | Where routing happens |
| --- | --- | --- |
| `ResponsesChat` | OpenAI Responses API with streaming and function tools | The configured endpoint, such as OpenAI or a compatible gateway |
| `BedrockChat` | AWS Bedrock ConverseStream | Bedrock, using the configured model or inference profile |
| `AGUIChat` | AG-UI streaming agent endpoint | The externally hosted agent |

`ResponsesChat` and `OpenAIChat` are names for the **same class**, with identical
arguments and defaults. Existing `OpenAIChat` integrations continue to work;
there is no deprecation or migration requirement. `ResponsesChat` is the clearer
name when the endpoint is a gateway rather than OpenAI itself. It still defaults
to OpenAI's endpoint and an OpenAI model, so configure both for other services.

The package does not choose between providers, balance requests, or configure
fallbacks. Put those policies in your gateway or hosted agent. A separate
adapter per vendor is unnecessary when the endpoint implements a supported
protocol. Compatibility still requires the streaming events, tool calls and
usage shapes that the adapter consumes; a gateway's “OpenAI-compatible” label
alone is not sufficient. Chat Completions-only endpoints are not supported by
`ResponsesChat`.

Applications can also supply a custom backend implementing the exported
`ChatAgent` protocol to `register(chat=...)`. That is an extension interface,
not a built-in router. All built-in adapters validate app tool calls and retain
server-side write approval.

## Responses-compatible gateways (LiteLLM example)

`ResponsesChat` can use a gateway's Responses API with a server-side virtual key
and model alias:

```python
chat = ResponsesChat(
    api_key=os.environ["LITELLM_API_KEY"],
    base_url=os.environ["LITELLM_BASE_URL"],  # e.g. https://gateway.example/v1
    model=os.environ["LITELLM_MODEL"],
    instructions="Use app tools for facts.",
    provider="litellm",
    send_end_user=True,
    on_usage=record_usage,  # Callback example in Usage attribution below.
)
# Inside server(), using identity verified by the app's authentication layer:
register(session=session, input=input, tools=tools, chat=chat,
         usage_metadata={"username": viewer.username})
```

`base_url` defaults to `https://api.openai.com/v1`. Requests append `/responses`
after removing trailing slashes, preserving any path prefix. HTTPS is required,
except HTTP on exactly `localhost` or `127.0.0.1` for local development. URLs
with credentials, queries, or fragments are rejected. Redirects are not followed.
`provider` defaults to `openai`; the explicit label appears in usage records and
model-call spans. It identifies the gateway, while `model` holds its alias.

`send_end_user` defaults to `False`. Enabling it sends the server-provided
`usage_metadata["username"]` in the JSON `user` field on **every** model request,
including tool follow-ups and the final round. The app must verify that identity;
the library does not authenticate it. Missing or blank identity fails before
network access. Browser-supplied identity is ignored. No end-user header or
other metadata forwarding is added. Shared adapters isolate identity per turn.
This flag is intended for gateways that use `user` for end-user budgets. If
enabled with the default OpenAI endpoint, it sends that username to OpenAI too.

Recognized LiteLLM budget errors produce a fixed visitor message: “The assistant
budget is used up. Contact your administrator.” Upstream error text is never
displayed. Ordinary rate limits remain generic failures. The gateway enforces
budgets; usage records only observe calls, with unknown token counts left as
`None`. Configure end-user budgets separately from app-key budgets.

HTTPX uses environment configuration by default: `SSL_CERT_FILE` or
`SSL_CERT_DIR` for corporate CAs, `HTTPS_PROXY` for an egress proxy, and `NO_PROXY`
for gateway hosts that bypass it. See [HTTPX environment variables](https://www.python-httpx.org/environment_variables/).
No custom HTTP client or header-forwarding option is provided.
For local HTTP endpoints, set `NO_PROXY=localhost,127.0.0.1` so proxy environment
settings do not send the virtual key and end-user identity through a proxy over
plain HTTP.

LiteLLM documents a [Responses API bridge](https://docs.litellm.ai/docs/response_api)
for other providers. Verify the deployed LiteLLM version and Bedrock model alias
before rollout: function-call IDs and results, streaming usage, approval and
denial, `store: false`, `parallel_tool_calls: false`, and the tools-disabled
final round must survive that bridge. Gateway spend logs must also show the
expected end user. Zero-budget rejection semantics can vary by gateway version;
explicitly verify that a zero-budget user is blocked before their first call.

An opt-in live smoke test exercises local read/write tools against your gateway.
Set `LITELLM_BASE_URL`, `LITELLM_API_KEY`, `LITELLM_MODEL`,
`LITELLM_TEST_USER` (allowed), and `LITELLM_BLOCKED_TEST_USER` (already exhausted).
Also set `LITELLM_ZERO_BUDGET_TEST_USER` to a distinct, freshly configured user
with zero spend and a zero budget. All three users must differ. A gateway that
allows the zero-budget user's first request fails this rollout check.
It makes billable requests but does not configure gateway users or budgets.
From this package's directory, in its test environment, run:

```sh
SHINYHUB_AGENT_LIVE_GATEWAY=1 PYTHONPATH=src python -m pytest tests/test_gateway_live.py -q
```

These tests are skipped by default. Passing mocked tests does not establish
compatibility with a particular live gateway deployment.

The default suite also uses a local HTTP contract server to exercise the real
HTTPX transport, multiline SSE, split UTF-8, tools, approvals, denial, usage and
budget isolation. This verifies client behavior without contacting a provider.
For a rollout, additionally record the deployed LiteLLM version and model alias,
run the live tests, and inspect gateway spend logs for those test users. The
live tests check the app-side records; they do not query gateway administration
APIs or establish spend-log attribution by themselves.

## Data sent to agents

Tool results are model input. Return the same authorized and anonymised data
as the page, reduced to the fields needed for the question. Resolve identities
such as "me" from verified session identity in the handler, and apply access
filters before aggregation. Do not accept a caller-supplied user or project as
the authority for scope. Avoid returning identity tokens, credentials, or
unnecessary names and identifiers, even if the UI can display them.

Build tools and schemas inside the top-level server function for each viewer.
Enums can be computed from that viewer's authorized values; schemas and
descriptions are also visible to agents. An enum is a schema constraint, not
an authorization check: the handler must re-check access on every call,
including after permissions change. Test ordinary, manager, admin, and
anonymised views against the page's access rules, including requests for
another viewer's data. A result-size limit does not redact sensitive fields.

The chat history exists only in the viewer's Shiny session and defaults to
the last six exchanges (`history_exchanges` in `register()`). A new chat clears it. The app currently has no durable
conversation store. The helper does not provide a remote MCP server or a
platform-wide agent registry, administration UI, or billing controls.

## Approval and validation

JSON Schema validation runs before a chat approval card or an opted-in browser
confirmation. For constraints that depend on current app state, add a
side-effect-free `validate(args)` callback. It may be synchronous or async,
must return `None` on success, and can raise `ToolError(code, message)` with a
visitor-safe explanation. Validation runs again immediately before execution:
available data may change while a visitor considers an action. Read tools can
also have validators. Unexpected callback errors are sanitised.

An optional synchronous `describe(args)` gives a write an app-specific summary:

```python
# Inside server(), alongside selected_period and set_period above:
from shinyhub_agent import ToolError

available_periods = reactive.value(["week", "year"])

def validate_period(args):
    if args["period"] not in available_periods.get():
        raise ToolError("no_data", "That reporting period has no data yet.")

set_period_tool = AgentTool(
    "set_period", "Change the dashboard period",
    {"type": "object", "properties": {"period": {"type": "string"}},
     "required": ["period"], "additionalProperties": False},
    set_period, read_only=False, confirmation="Change the dashboard period?",
    validate=validate_period,
    describe=lambda args: f"Reporting period = {args['period']}",
)
```

Descriptions contain 1–300 characters; invalid descriptions fall back to the
argument display. Callbacks receive snapshots and cannot silently rewrite the
approved arguments. The chat shows nested keys and array values as plain text;
long summaries and custom descriptions retain expandable complete JSON details.
HTML is always literal. Approval has a visible deadline; expired cards remove
their buttons. Ask again to propose a fresh action.

## Shiny state and write results

Registered handlers, validators, descriptions and undo callbacks run in the
viewer session, with reactive reads isolated and the reactive graph locked.
Pending reactive work is flushed before callbacks complete. They can read
`input.x()` or a reactive calculation, call `ui.update_*()` without `session=`,
and set reactive values without a manual `reactive.flush()`. Model requests and
approval waits do not hold the lock. Session end cancels outstanding tasks.
Keep callbacks short: the graph lock is shared across sessions in the process.
Do not wait inside a callback for browser input updates or unrelated slow I/O.

`ui.update_*()` sends a client update; it does not synchronously change
`input.x()`. Return normalized, server-owned view state, as `selected_period`
does above. Route manual input changes through the same normalization function,
including clamping dates and clearing incompatible filters. Use that state for
subsequent reads and outputs. A browser acknowledgment cannot guarantee that
later app effects or asynchronous work have completed. This package provides
no app-wide `settle()` guarantee.

These guarantees apply to tools returned by `register()`. A standalone
`ToolRegistry` has validation and payload bounds but no Shiny execution context.
Writes are not transactional: a handler that raises after a mutation must
handle its own recovery. Undo must still check that the applied state is current.

## Budgets and limits

All backends execute calls in order. When a per-step budget or the
one-write-per-step rule defers a call, all later calls in that step are deferred
too. Each receives a correlated result with the `tool_deferred` error code;
the model can request that work in another step. Malformed or incomplete
streams never trigger app tools.

When the tool-round or per-turn call budget is exhausted, one final call with
app tools disabled answers from completed results. If it fails or still
requests tools, a deterministic response lists completed reads, applied actions
and recent safe tool errors. The final call is additional to `max_tool_rounds`
and is included in usage records.

| Setting | Where | Default | Allowed range |
| --- | --- | ---: | --- |
| `max_tool_calls_per_step` | Any backend | 2 | 1–8 |
| `max_tool_calls_per_turn` | Any backend | 8 | 1–64 |
| `max_tool_rounds` | Any backend | 4 | 1–8, plus one final call |
| `max_output_tokens` | OpenAI/Bedrock | 500 | 100–8192; provider limits also apply |
| `approval_timeout` | `register()` | 30 s | 1–300 s |
| `history_exchanges` | `register()` | 6 | 1–32 complete exchanges |
| `max_answer_chars` | `register()` | 8000 | 1000–32768 characters |

History drops the oldest complete exchanges when full. Backends retain the
registered history without a second six-exchange or 2000-character truncation.
Increase `max_answer_chars` when raising model output limits. Larger histories
and budgets increase token use.

Fixed bounds: 32 tools; 8192 UTF-8 bytes per schema or argument object; 32768
UTF-8 bytes per tool or undo result; 500 characters per tool description;
300 per confirmation; 140 per receipt; 1,000 per panel notice. Tool execution, preflight and undo have
an 8-second timeout including waiting for the reactive lock. Streams allow
64 KiB per event and 1 MiB per response. Chat questions are capped at 2000
characters, 10 per minute and 40 per session; reset does not reset those budgets.
Browser dispatch allows 20 requests per minute, with write preflight and
execution each consuming one request. Browser confirmation expires after 30
seconds; each dispatch waits up to 10 seconds. Only the last chat write is
undoable, for five minutes.

## Usage attribution

All backends accept `on_usage(record)`, a synchronous or async callback receiving
a frozen `UsageRecord` per provider request, including final calls, failures
and cancellations. Records include UTC timestamp, provider, app, optional
username, model, turn/call IDs, one-based round, final-call flag, tokens, cache
tokens when available, duration in milliseconds, requested tool names and
outcome. Unknown counts are `None`, not zero. AG-UI records describe endpoint
requests (`model="external"`); its protocol does not supply model token usage.
Account for its internal model calls on the endpoint.

```python
from dataclasses import asdict
import logging

usage_log = logging.getLogger("app.assistant_usage")

def record_usage(record):
    usage_log.info("assistant_usage", extra={"usage": asdict(record)})

chat = BedrockChat(
    model_id=os.environ["SHINYHUB_AGENT_BEDROCK_MODEL_ID"],
    region=os.environ["AWS_REGION"], instructions="Use app tools for facts.",
    on_usage=record_usage,
)

# Inside server(), after resolving viewer from verified session identity:
register(session=session, input=input, tools=tools, chat=chat,
         usage_metadata={"username": viewer.username if viewer else None})
```

App slug defaults from `SHINYHUB_APP_SLUG`. `usage_metadata` may set `app` and
`username` (up to 256 characters each). By default metadata stays in local logs,
callbacks and tracing; it is not added to prompts or HTTP headers. With
`ResponsesChat(send_end_user=True)` (or `OpenAIChat` with the same flag), the
username also leaves the app in the
Responses API JSON `user` field, as described above. Other usage metadata is
not forwarded. Resolve "me" in session-scoped tools.
Shared adapters keep usage context separate for each viewer and turn.

`shinyhub_agent._usage` also emits single-line JSON at INFO level with structured
`agent_usage` logging metadata. Where OpenTelemetry is installed, model-call
spans receive `shinyhub.agent.*` attributes. Traces may be sampled; use logs or
callbacks for complete accounting. Async callbacks have a two-second timeout;
keep callbacks short and enqueue durable accounting separately. Callback and
tracing failures do not fail answers. Records are observations, not exactly-once
billing events; use call IDs for deduplication.

## Opening chat in tests

The public `window.shinyhubAgentChat.open()` and `.close()` methods are
idempotent. They return `true` when a chat session is available, or `false`
before connection. They work with the native closed shadow root and fallback:

```javascript
await page.waitForFunction(() => window.shinyhubAgentChat?.open());
await page.locator(".sh-agent-input").fill("What is the current reporting period?");
await page.locator(".sh-agent-send").click();
```

The existing `shinyhub:chat:toggle` event with `{version: 1}` also works, but
toggles instead of ensuring the panel is open. Neither hook submits questions
or approves changes. The toolbar's own controls require separate toolbar tests.

From the repository root, `make test-py-agent` runs the Python and browser unit
tests. `make test-browser-agent-chat-e2e` checks native and fallback panels at
desktop and mobile sizes, including keyboard access, accessibility and plain
text argument rendering. `make test-browser-agent-shiny-e2e` exercises a real
Shiny session with a local stub agent: approval, updates, output recomputation,
undo and cancellation. These browser checks require Chrome and make no model
provider requests.

## Operational requirements

- Keep app access behind ShinyHub's authentication and per-app access policy.
- Give tools the least authority required, and check the viewer's permissions
  inside handlers for sensitive reads and actions.
- Return only data the viewer may see. Tool results are sent to the model
  provider or hoster-owned AG-UI endpoint to compose an answer.
- Set model and endpoint secrets per app, never in the page or manifest.
- Review tool names, schemas, descriptions, and app instructions when the app
  changes. Add a regression test for each consequential action.
- Run at most one chat request and one tool request at a time per viewer;
  the adapter enforces per-session request budgets and bounded payloads.

The package is a reusable app integration. A platform-owned chat service,
central cost policy, admin configuration, and R Shiny helper remain separate
work before this becomes a platform-wide production feature.

### Real-model follow-up compatibility checks

The deterministic adapter tests verify protocol handling and request counts.
Before enabling follow-ups with a new model or gateway, run the opt-in, billable
probes against that deployment. They use synthetic dashboard facts and the default
500-token output limit. They check ordinary instructions (suggestions optional),
a forced text-plus-suggestion response, and a read-tool round followed by an answer
and suggestion. Suggestions must not add a request or repeat in the answer/history:

```sh
# Configure only the backend being tested; normal AWS credentials apply to Bedrock.
export SHINYHUB_AGENT_BEDROCK_MODEL="your-model-id"
export AWS_REGION="your-region"
# Or configure OPENAI_API_KEY and SHINYHUB_AGENT_OPENAI_MODEL.
# Gateways can also set SHINYHUB_AGENT_OPENAI_BASE_URL and
# SHINYHUB_AGENT_OPENAI_PROVIDER (for example, "litellm").
SHINYHUB_AGENT_LIVE_FOLLOWUPS=1 uv run --extra test --extra bedrock \
  pytest tests/test_suggestions_live.py -q
```

Run this for each intended model. A passing probe establishes basic compatibility,
not suggestion quality or a latency improvement. Application evaluations should
also check relevance, self-contained wording, injected-data handling, and latency
against the same prompts with follow-ups disabled.
