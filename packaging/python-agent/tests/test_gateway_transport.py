"""Exercise the production HTTP client against a local Responses contract server."""

import asyncio
import json
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from shinyhub_agent import ResponsesChat
from test_gateway import reply, turn


@pytest.mark.parametrize("approved", [True, False])
def test_real_http_streaming_tools_and_budget_isolation(monkeypatch, approved):
    requests = []
    steps = defaultdict(int)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass  # Do not log request identifiers or test identities.

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.path, dict(self.headers), body))
            viewer = body["user"]
            step = steps[viewer]
            steps[viewer] += 1
            if viewer == "blocked":
                payload = json.dumps(
                    {"error": {"type": "budget_exceeded", "message": "private details"}}
                ).encode()
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
            else:
                response = (
                    reply(call="get_view")
                    if step == 0
                    else reply(call="set_view")
                    if step == 1
                    else reply(text="Updated view ✓")
                )
                # Valid SSE with CRLF, comments, fields and multiline JSON;
                # writes split UTF-8 code points and event boundaries arbitrarily.
                frames = []
                for line in response.text.splitlines():
                    if line.startswith("data:"):
                        value = json.loads(line[5:])
                        formatted = json.dumps(value, indent=2, ensure_ascii=False)
                        frames.append(
                            ": keepalive\r\nevent: response\r\nid: event-id\r\n"
                            + "\r\n".join(
                                "data: " + part for part in formatted.splitlines()
                            )
                            + "\r\n\r\n"
                        )
                payload = ("".join(frames) + "data: [DONE]\r\n\r\n").encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            for offset in range(0, len(payload), 7):
                self.wfile.write(payload[offset : offset + 7])
                self.wfile.flush()

    # Ensure workstation proxy settings cannot route this local test off-host.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    records = []
    agent = ResponsesChat(
        "test-key",
        "Use tools",
        base_url=f"http://127.0.0.1:{server.server_port}/prefix/v1/",
        model="test-alias",
        provider="contract",
        send_end_user=True,
        max_tool_rounds=2,
        on_usage=records.append,
    )

    async def run():
        blocked = await turn(agent, username="blocked")
        allowed = await turn(agent, username="allowed", approve=approved)
        return blocked, allowed

    try:
        (_, failed), (chat, succeeded) = asyncio.run(run())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert steps == {"blocked": 1, "allowed": 3}
    assert failed[-1]["code"] == "budget_exceeded"
    assert "private details" not in json.dumps(failed)
    assert succeeded[-1]["type"] == "done"
    assert chat.history[-1]["content"] == "Updated view ✓"
    assert sum(e["type"] == "action_applied" for e in succeeded) == int(approved)
    for path, headers, body in requests:
        assert path == "/prefix/v1/responses"
        assert headers["Authorization"] == "Bearer test-key"
        assert "Cookie" not in headers
        assert "allowed" not in str(headers) and "blocked" not in str(headers)
        assert body["store"] is False and body["parallel_tool_calls"] is False
    assert requests[-1][2]["tool_choice"] == "none"
    allowed_records = [record for record in records if record.username == "allowed"]
    assert [record.round for record in allowed_records] == [1, 2, 3]
    assert all(record.provider == "contract" for record in records)
    assert all(
        record.input_tokens == 11 and record.output_tokens == 5
        for record in allowed_records
    )
    assert all(record.cache_read_tokens == 3 for record in allowed_records)
    assert allowed_records[-1].final
