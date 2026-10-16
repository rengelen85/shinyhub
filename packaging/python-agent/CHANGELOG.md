# Changelog

## 0.4.0 — 2026-10-16

- Add optional app-defined starter questions through `register(..., starters=...)`.
  Keep the built-in questions by default, or hide them with an empty list.
- Add experimental, opt-in follow-up suggestions through
  `register(..., followups=1..3)` for Bedrock and Responses-compatible adapters.
  Suggestions fill the composer for editing and require an explicit send.
- Validate and isolate suggestions per session, show them only after successful
  answers, and preserve app-tool budgets, write approvals, and usage attribution.
  Add deterministic protocol, browser, and opt-in real-model compatibility tests.
- Raise minimum dependencies to `jsonschema>=4.26`, `httpx>=0.28`, and optional
  `boto3>=1.43`. Existing apps can leave follow-ups disabled (the default).

## 0.3.0 — 2026-10-15

- Export `ResponsesChat` as a protocol-oriented name for `OpenAIChat`, preserving
  the original name, class identity, arguments and defaults. Document adapter
  protocols separately from gateway routing policies.
- Support OpenAI-compatible Responses gateways with a validated `base_url`,
  opt-in server-supplied end-user attribution, and a configurable usage provider.
- Show fixed visitor messages for recognized gateway budget errors and missing
  required end-user identity, preserving applied-action receipts.
- Handle bounded multiline Responses SSE and tolerate malformed optional usage
  metadata without discarding valid answers. Exercise the real HTTP transport
  locally, with explicit live checks for fresh zero-budget users.
- Add optional `register(..., notice=...)` plain-text panel notices that persist
  across new chats and never enter model requests or usage token counts.

## 0.2.0 — 2026-10-01

First regular release of the 0.2 series, following 0.2.0b4.

- Show nested approval arguments as plain text, with complete details available
  for shortened lists and custom descriptions. Display approval deadlines and
  remove expired decisions.
- Add `AgentTool.validate` before approval and again before execution, plus
  `AgentTool.describe` for action-specific confirmation details.
- Recover from excess tool calls using correlated deferred results. Configure
  step, turn and round budgets; reserve a final tools-disabled answer.
- Run registered callbacks in the Shiny session with isolated reads and
  automatic reactive flushing. Cancel outstanding tasks when the session ends.
- Configure approval timeouts, conversation history and answer limits.
- Attribute model calls through structured usage records, callbacks and tracing.
- Expose public chat-opening hooks for browser tests, including native toolbar
  chat with a closed shadow root.

### Migration

Browser tools now expose only reads by default. Apps that intentionally permit
browser writes must use `register(..., allow_browser_writes=True)`. Browser
confirmation is not a security boundary; consequential writes should use chat's
server-side approval and app permission checks.

Return normalized server-owned view state from write tools. Client input updates
remain asynchronous; there is no general app-wide settlement guarantee.

### Verification

Verified clean wheel and source installations, provider protocol and recovery
tests, per-viewer usage isolation, desktop/mobile browser accessibility, and a
real Shiny session covering approval, updates, undo and cancellation. Live model
behavior and the reporting client's dashboard were not part of this verification.
