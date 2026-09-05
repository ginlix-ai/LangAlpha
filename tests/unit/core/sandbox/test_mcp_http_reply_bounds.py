"""The HTTP reply reader must bound an untrusted (non-relay) MCP server.

httpx's read timeout resets on every byte, so a server that floods or drips
would otherwise OOM or hang the sandbox interpreter. ``_parse_http_reply`` /
``_read_body_capped`` cap the accumulated bytes and enforce a total-exchange
deadline that the per-read timeout can't provide.

Both bounds are inert unless the call sites hand the reader a *streamed*
response, so the second half of this file pins every reply-bearing request to
``client.stream`` — a buffering ``client.post`` fails there before the reader
ever runs.
"""

import json as _json
import time

import httpx
import pytest

from ptc_agent.core.sandbox import mcp_client_runtime as m


class _FakeResp:
    """The subset of a streamed httpx response the reader touches.

    Deliberately exposes ``iter_text`` (chunked) and not ``iter_lines``: line
    iteration buffers an unterminated line without limit inside httpx, so a
    reader that regresses to it fails here instead of passing by accident.
    ``lines`` is convenience — each entry becomes one newline-terminated chunk.
    """

    def __init__(
        self,
        *,
        ctype="application/json",
        lines=None,
        chunks=None,
        body=b"",
        status=200,
    ):
        self.headers = {"content-type": ctype}
        self.status_code = status
        if chunks is None:
            chunks = [f"{line}\n" for line in (lines or [])]
        self._chunks = chunks
        self._body = body
        self.served = 0

    def iter_text(self):
        for chunk in self._chunks:
            self.served += len(chunk)
            yield chunk

    def iter_bytes(self):
        yield self._body

    def raise_for_status(self):
        pass


def _match_id() -> int:
    return 1


class TestByteCap:
    def test_an_oversized_sse_stream_without_a_reply_is_refused(self, monkeypatch):
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", 64)
        resp = _FakeResp(
            ctype="text/event-stream",
            lines=[f"data: {i}" for i in range(200)],  # no blank line, no id match
        )
        with pytest.raises(RuntimeError, match="reply_too_large"):
            m._parse_http_reply(resp, _match_id(), "srv")

    def test_an_oversized_json_body_is_refused(self, monkeypatch):
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", 64)
        resp = _FakeResp(ctype="application/json", body=b"x" * 4096)
        with pytest.raises(RuntimeError, match="reply_too_large"):
            m._parse_http_reply(resp, _match_id(), "srv")

    def test_a_data_line_that_never_terminates_still_trips_the_cap(
        self, monkeypatch
    ):
        # The cap must bind per chunk: waiting for a newline that never comes
        # would accumulate the whole flood first. The flood is finite so a
        # capless regression fails on the error name instead of hanging.
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", 64)

        flood = ["data: spew spew " for _ in range(1000)]  # no newline, ever
        resp = _FakeResp(ctype="text/event-stream", chunks=flood)
        with pytest.raises(RuntimeError, match="reply_too_large"):
            m._parse_http_reply(resp, _match_id(), "srv")
        # ...and it tripped mid-stream, not after draining the flood.
        assert resp.served <= 64 + len("data: spew spew ")

    def test_an_unterminated_final_data_line_still_frames(self):
        # A server may end the stream without the trailing newline; the
        # buffered tail is still the reply.
        resp = _FakeResp(
            ctype="text/event-stream",
            chunks=['data: {"jsonrpc":"2.0",', '"id":1,"result":{"ok":true}}'],
        )
        assert m._parse_http_reply(resp, 1, "srv")["result"] == {"ok": True}


class TestDeadline:
    def test_a_past_deadline_cuts_the_sse_read(self):
        resp = _FakeResp(
            ctype="text/event-stream",
            lines=['data: {"jsonrpc":"2.0","id":1,"result":{}}', ""],
        )
        with pytest.raises(RuntimeError, match="stream_deadline"):
            m._parse_http_reply(resp, 1, "srv", deadline=time.monotonic() - 1)

    def test_a_past_deadline_cuts_the_json_read(self):
        resp = _FakeResp(ctype="application/json", body=b"{}")
        with pytest.raises(RuntimeError, match="stream_deadline"):
            m._parse_http_reply(resp, 1, "srv", deadline=time.monotonic() - 1)


class TestHappyPath:
    def test_a_matching_sse_reply_returns_within_bounds(self):
        resp = _FakeResp(
            ctype="text/event-stream",
            lines=['data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}', ""],
        )
        assert m._parse_http_reply(resp, 1, "srv")["result"] == {"ok": True}

    def test_a_plain_json_reply_returns_within_bounds(self):
        resp = _FakeResp(ctype="application/json", body=b'{"jsonrpc":"2.0","id":1,"result":{}}')
        assert m._parse_http_reply(resp, 1, "srv")["id"] == 1


class TestJsonBodyValidation:
    """A JSON body carries exactly one message — anything that is not the
    awaited reply must be a named refusal, never flow onward as the result."""

    def test_unparseable_json_is_refused(self):
        resp = _FakeResp(ctype="application/json", body=b"<html>oops</html>")
        with pytest.raises(RuntimeError, match="invalid_reply"):
            m._parse_http_reply(resp, 1, "srv")

    def test_a_non_object_body_is_refused(self):
        resp = _FakeResp(ctype="application/json", body=b'[{"jsonrpc":"2.0","id":1}]')
        with pytest.raises(RuntimeError, match="invalid_reply"):
            m._parse_http_reply(resp, 1, "srv")

    def test_a_server_initiated_request_is_refused_by_name(self):
        resp = _FakeResp(
            ctype="application/json",
            body=b'{"jsonrpc":"2.0","id":9,"method":"sampling/createMessage","params":{}}',
        )
        with pytest.raises(RuntimeError, match="unsupported_server_request"):
            m._parse_http_reply(resp, 1, "srv")

    def test_a_mismatched_reply_id_is_refused(self):
        resp = _FakeResp(
            ctype="application/json", body=b'{"jsonrpc":"2.0","id":42,"result":{}}'
        )
        with pytest.raises(RuntimeError, match="mismatched_reply"):
            m._parse_http_reply(resp, 1, "srv")

    def test_a_notification_body_is_refused(self):
        resp = _FakeResp(
            ctype="application/json",
            body=b'{"jsonrpc":"2.0","method":"notifications/progress"}',
        )
        with pytest.raises(RuntimeError, match="mismatched_reply"):
            m._parse_http_reply(resp, 1, "srv")


# ---------------------------------------------------------------------------
# Call sites: every reply-bearing request must stream
# ---------------------------------------------------------------------------


class _FakeStream:
    """The subset of a streamed httpx response a call site touches."""

    def __init__(self, *, status=200, headers=None, body=b""):
        self.status_code = status
        self.headers = {"content-type": "application/json", **(headers or {})}
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_bytes(self):
        yield self._body

    def iter_lines(self):
        return iter(())

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPError(f"status {self.status_code}")


class _StreamOnlyClient:
    """httpx.Client stand-in that refuses the buffering ``.post`` outright.

    Replies are dispatched off the JSON-RPC method so one client can serve a
    whole handshake; ``relay_auth_times`` makes the first N initializes come
    back as a relay credential rejection.
    """

    def __init__(self, *, relay_auth_times=0):
        self.streamed = []
        self.relay_auth_times = relay_auth_times
        self._inits = 0
        self._pending_id = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        raise AssertionError("reply-bearing request used a buffering client.post")

    def stream(self, method, url, *, json=None, headers=None):
        payload = json or {}
        self._pending_id = payload.get("id")
        name = payload.get("method", "")
        self.streamed.append(name)
        return self._reply_for(name)

    def _reply_for(self, name):
        if name == "server/discover":
            return _FakeStream(status=404)  # no modern era → legacy handshake
        if name == "initialize":
            self._inits += 1
            if self._inits <= self.relay_auth_times:
                return _FakeStream(status=401, headers={"x-relay-error": "relay_auth"})
            return self._json({"protocolVersion": "2025-11-25"})
        if name == "notifications/initialized":
            return _FakeStream()  # body is never read
        if name == "tools/list":
            return self._json({"tools": [{"name": "probe_tool", "inputSchema": {}}]})
        return self._json({"content": [{"type": "text", "text": "ok"}]})

    def _json(self, result):
        body = _json_dumps({"jsonrpc": "2.0", "id": self._pending_id, "result": result})
        return _FakeStream(body=body)


def _json_dumps(payload) -> bytes:
    return _json.dumps(payload).encode("utf-8")


class _FakeHttpx:
    """Stands in for the ``httpx`` module so one client serves the call site."""

    HTTPError = httpx.HTTPError

    def __init__(self, client):
        self._client = client

    def Client(self, *args, **kwargs):  # noqa: N802 - mirrors httpx.Client
        return self._client


@pytest.fixture
def configured(monkeypatch):
    """One plain http server, no vault and no relay, with a cold proto cache."""
    monkeypatch.setattr(
        m,
        "_SERVER_CONFIGS",
        {
            "srv": m._normalize(
                "srv",
                {"transport": "http", "untrusted": False, "url": "https://mcp.invalid/rpc"},
            )
        },
    )
    monkeypatch.setattr(m, "_PROTO", {})


def _install(monkeypatch, **kwargs) -> _StreamOnlyClient:
    client = _StreamOnlyClient(**kwargs)
    monkeypatch.setattr(m, "httpx", _FakeHttpx(client))
    return client


def _negotiated(monkeypatch):
    monkeypatch.setattr(
        m, "_PROTO", {"srv": {"mode": "legacy", "version": "2025-11-25", "session_id": None}}
    )


class TestCallSitesStream:
    def test_the_handshake_streams_its_reply_and_its_notification(
        self, configured, monkeypatch
    ):
        client = _install(monkeypatch)
        assert m._ensure_http_server("srv")["mode"] == "legacy"
        assert client.streamed == [
            "server/discover",
            "initialize",
            "notifications/initialized",
        ]

    def test_discovery_streams_the_tools_list_reply(self, configured, monkeypatch):
        _negotiated(monkeypatch)
        client = _install(monkeypatch)
        assert m._discover_http("srv") == [{"name": "probe_tool", "inputSchema": {}}]
        assert client.streamed == ["tools/list"]

    def test_a_tool_call_streams_its_reply(self, configured, monkeypatch):
        _negotiated(monkeypatch)
        client = _install(monkeypatch)
        assert m._call_mcp_tool_http("srv", "probe_tool", {}) == "ok"
        assert client.streamed == ["tools/call"]


class _FakeClock:
    """The runtime's ``time`` module with a hand-advanced monotonic clock."""

    def __init__(self, start=1000.0):
        self.now = start

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class _StallingStream(_FakeStream):
    """A 200 whose body arrives only after ``cost`` seconds of wall clock."""

    def __init__(self, clock, cost):
        super().__init__(body=_json_dumps({"jsonrpc": "2.0", "id": 1, "result": {}}))
        self._clock = clock
        self._cost = cost

    def iter_bytes(self):
        self._clock.now += self._cost
        yield self._body


class _StallingProbeClient(_StreamOnlyClient):
    """server/discover drips past its budget; initialize answers normally."""

    def __init__(self, clock, *, init_status=200):
        super().__init__()
        self._clock = clock
        self._init_status = init_status

    def _reply_for(self, name):
        if name == "server/discover":
            return _StallingStream(self._clock, m._HTTP_EXCHANGE_BUDGET + 1)
        if name == "initialize" and self._init_status >= 400:
            return _FakeStream(status=self._init_status)
        return super()._reply_for(name)


class _SessionExpiryClient(_StreamOnlyClient):
    """404s the first tools/call, and makes re-negotiating cost wall clock.

    The cost lands on the notification that closes the handshake — the one step
    no deadline covers — so it models a slow re-negotiation without tripping the
    handshake's own budgets.
    """

    def __init__(self, clock, cost):
        super().__init__()
        self._clock = clock
        self._cost = cost
        self._calls = 0

    def _reply_for(self, name):
        if name == "notifications/initialized":
            self._clock.now += self._cost
        if name == "tools/call":
            self._calls += 1
            if self._calls == 1:
                return _FakeStream(status=404)  # legacy session expired
        return super()._reply_for(name)


def _install_client(monkeypatch, client):
    monkeypatch.setattr(m, "httpx", _FakeHttpx(client))
    return client


class TestPhaseDeadlines:
    """Each phase of an exchange gets its own clock.

    One deadline for the whole thing let a hung server/discover probe spend it
    all, so the legacy initialize that exists to rescue exactly that server was
    refused before its first chunk — and the same inheritance made the retry
    after a session re-negotiation start on an already-spent budget.
    """

    def test_a_stalled_probe_still_leaves_the_fallback_a_live_deadline(
        self, configured, monkeypatch
    ):
        clock = _FakeClock()
        monkeypatch.setattr(m, "time", clock)
        client = _install_client(monkeypatch, _StallingProbeClient(clock))

        assert m._ensure_http_server("srv")["mode"] == "legacy"
        assert client.streamed == [
            "server/discover",
            "initialize",
            "notifications/initialized",
        ]

    def test_a_stalled_probe_is_named_when_the_fallback_also_fails(
        self, configured, monkeypatch
    ):
        clock = _FakeClock()
        monkeypatch.setattr(m, "time", clock)
        _install_client(monkeypatch, _StallingProbeClient(clock, init_status=500))

        with pytest.raises(RuntimeError, match="discover_probe_timeout"):
            m._ensure_http_server("srv")

    def test_the_retry_after_a_session_reinit_gets_a_fresh_budget(
        self, configured, monkeypatch
    ):
        clock = _FakeClock()
        monkeypatch.setattr(m, "time", clock)
        monkeypatch.setattr(
            m,
            "_PROTO",
            {"srv": {"mode": "legacy", "version": "2025-11-25", "session_id": "s1"}},
        )
        client = _install_client(
            monkeypatch, _SessionExpiryClient(clock, m._HTTP_CALL_BUDGET + 5)
        )

        assert m._call_mcp_tool_http("srv", "probe_tool", {}) == "ok"
        assert client.streamed.count("tools/call") == 2


class TestHandshakeRelayAuthRetry:
    """The handshake self-heals a raced credential re-mint exactly once.

    Every execute_code starts with an empty ``_PROTO``, so the first MCP call of
    a code block negotiates — making this, not the tool-call path, where a
    concurrent host re-mint is actually met.
    """

    def test_a_relay_auth_rejection_re_reads_credentials_and_retries_once(
        self, configured, monkeypatch
    ):
        reads = []

        def _resolve(cfg, *, discovery=False):
            reads.append(discovery)
            return "https://relay.invalid/v1/egress/g", {
                "Authorization": f"Bearer t{len(reads)}"
            }

        monkeypatch.setattr(m, "_resolve_http", _resolve)
        client = _install(monkeypatch, relay_auth_times=1)

        assert m._ensure_http_server("srv")["mode"] == "legacy"
        assert len(reads) == 2  # the credential file was read again, not reused
        assert client.streamed.count("initialize") == 2

    def test_a_persistent_relay_auth_rejection_still_raises(
        self, configured, monkeypatch
    ):
        client = _install(monkeypatch, relay_auth_times=9)
        with pytest.raises(RuntimeError, match="relay_auth"):
            m._ensure_http_server("srv")
        assert client.streamed.count("initialize") == 2  # bounded to one retry
