"""A built-in http server on the computer's data token.

The client presents the token from the token file the host keeps current, and
a refusal gets one forced re-read, because the file can rotate under a request
already in flight. A failure before any reply is resent on a short backoff
inside the call's budget, so a restart of the data service costs latency, not
the call. Every other http server's tools/call goes out exactly once: it may
have side effects, so no status code makes a resend safe.
"""

import json
import os
import threading

import httpcore
import httpx
import pytest

from ptc_agent.core.sandbox import mcp_client_runtime as m

URL = "https://data.invalid/mcp"
MODERN = {"mode": "modern", "version": m._MODERN_VERSIONS[0], "session_id": None}


def _write_token(path, token: str) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"access_token": token, "expires_at": 0}))
    os.replace(tmp, path)


class _Resp:
    def __init__(
        self,
        status: int,
        body: dict | None = None,
        *,
        raw: bytes | None = None,
        ctype: str = "application/json",
        headers: dict | None = None,
    ):
        self.status_code = status
        self.headers = {"content-type": ctype, **(headers or {})}
        self._body = json.dumps(body or {}).encode() if raw is None else raw
        self.request = httpx.Request("POST", URL)
        self.is_stream_consumed = False
        self.is_closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_bytes(self):
        yield self._body

    def iter_text(self):
        yield self._body.decode()

    def iter_raw(self):
        self.is_stream_consumed = True
        yield self._body
        self.is_closed = True

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=httpx.Request("POST", URL),
                response=httpx.Response(self.status_code),
            )


class _Client:
    """Answers each send with the next scripted status or reply, or raises it
    when it is an exception; logs what it was sent."""

    def __init__(self, statuses, on_send=None):
        self.statuses = list(statuses)
        self.sent: list[dict] = []
        self.on_send = on_send

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass

    def stream(self, method, url, *, json=None, headers=None, timeout=None):
        self.sent.append(dict(headers or {}))
        status = self.statuses.pop(0)
        if self.on_send:
            self.on_send(status)
        if isinstance(status, Exception):
            raise status
        if isinstance(status, _Resp):
            return status
        body = {
            "jsonrpc": "2.0",
            "id": json.get("id"),
            "result": {"structuredContent": {"price": 1.0}, "content": []},
        }
        return _Resp(status, body)


@pytest.fixture
def token_file(tmp_path, monkeypatch):
    path = tmp_path / ".mcp_tokens.json"
    _write_token(path, "tok-1")
    monkeypatch.setattr(m, "_MCP_TOKENS_FILE", str(path))
    monkeypatch.setattr(m, "_data_token_cache", {})
    return path


def _install(monkeypatch, name, entry, client):
    monkeypatch.setattr(m, "_SERVER_CONFIGS", {name: m._normalize(name, entry)})
    monkeypatch.setattr(m, "_PROTO", {name: dict(MODERN)})
    monkeypatch.setattr(m.httpx, "Client", lambda **kw: client)
    # The pooled client outlives a call, so each test starts without one.
    monkeypatch.setattr(m, "_data_client_held", {})


class _Clock:
    """The runtime's ``time`` with a hand-advanced clock; a send can cost time."""

    def __init__(self):
        self.now = 1000.0
        self.slept: list[float] = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(m, "time", clock)
    monkeypatch.setattr(m.random, "uniform", lambda low, high: (low + high) / 2)
    return clock


def _refused():
    return httpx.ConnectError("connection refused")


DATA_ENTRY = {"transport": "http", "untrusted": False, "url": URL, "auth": "data_token"}


class TestWhoGetsTheToken:
    def test_only_a_trusted_direct_http_entry_presents_it(self):
        assert m._normalize("d", DATA_ENTRY).data_token
        # Each flag is decided host-side; an entry that contradicts one fails
        # closed rather than handing the token to whatever it names.
        assert not m._normalize("d", {**DATA_ENTRY, "untrusted": True}).data_token
        assert not m._normalize("d", {**DATA_ENTRY, "relay_bound": True}).data_token
        assert not m._normalize("d", {**DATA_ENTRY, "transport": "sse"}).data_token
        assert not m._normalize(
            "d", {k: v for k, v in DATA_ENTRY.items() if k != "untrusted"}
        ).data_token

    def test_a_rotation_never_drops_the_held_server(self, monkeypatch):
        monkeypatch.setattr(m, "_SERVER_CONFIGS", {"d": m._normalize("d", DATA_ENTRY)})
        assert m.server_secret_dependencies("d") == []


class TestBearer:
    def test_the_token_rides_every_send(self, monkeypatch, token_file):
        client = _Client([200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {"symbol": "AAPL"})

        assert reply["result"]["structuredContent"] == {"price": 1.0}
        assert client.sent[0]["Authorization"] == "Bearer tok-1"

    def test_a_401_rereads_the_file_once_and_resends(self, monkeypatch, token_file):
        # The file rotates while the first send is in flight: the resend must
        # carry the new token even though this process cached the old one.
        def rotate(status):
            if status == 401:
                _write_token(token_file, "tok-2")

        client = _Client([401, 200], on_send=rotate)
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert [h["Authorization"] for h in client.sent] == [
            "Bearer tok-1",
            "Bearer tok-2",
        ]
        assert "isError" not in reply["result"]

    def test_a_second_401_is_the_auth_failed_envelope(self, monkeypatch, token_file):
        client = _Client([401, 401])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert len(client.sent) == 2
        # A reply, not an exception: through the daemon an exception after the
        # ack would read as "the call may have run".
        assert reply["result"]["isError"] is True
        assert reply["result"]["structuredContent"]["error"] == "auth_failed"

    def test_no_401_resend_once_the_budget_is_too_short_for_it(
        self, monkeypatch, token_file, clock
    ):
        # The refusal came back late: a resend now would run past the deadline.
        def slow(status):
            clock.now += m._HTTP_CALL_BUDGET - m._HTTP_PHASE_FLOOR + 1

        client = _Client([401, 200], on_send=slow)
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert len(client.sent) == 1
        assert reply["result"]["structuredContent"]["error"] == "auth_failed"

    def test_a_missing_token_file_is_auth_failed_before_any_send(
        self, monkeypatch, token_file
    ):
        token_file.unlink()
        client = _Client([200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert client.sent == []
        assert reply["result"]["structuredContent"]["error"] == "auth_failed"


class TestTransientFailures:
    def test_a_restart_is_waited_out_on_the_backoff(
        self, monkeypatch, token_file, clock
    ):
        client = _Client([_refused(), _refused(), 503, 200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {"price": 1.0}
        assert clock.slept == list(m._DATA_TOKEN_TRANSIENT_BACKOFF_S[:3])

    def test_a_lasting_outage_is_upstream_error_once_the_backoff_is_spent(
        self, monkeypatch, token_file, clock
    ):
        backoff = m._DATA_TOKEN_TRANSIENT_BACKOFF_S
        client = _Client([_refused()] * (len(backoff) + 1))
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"]["error"] == "upstream_error"
        assert clock.slept == list(backoff)

    def test_slow_failures_stop_inside_the_call_budget(
        self, monkeypatch, token_file, clock
    ):
        # Each connect times out late, so the backoff would outrun the budget.
        def slow(status):
            clock.now += 15.0

        start = clock.now
        client = _Client([httpx.ConnectTimeout("timed out")] * 5, on_send=slow)
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"]["error"] == "upstream_error"
        assert clock.now - start <= m._HTTP_CALL_BUDGET
        assert len(client.sent) < 5

    def test_a_resend_after_a_503_reuses_the_connection(
        self, monkeypatch, token_file, clock
    ):
        # Left unread, the 503 would close its connection, and every resend
        # would pay a new handshake at the edge just while it is failing.
        reply_200 = b'{"jsonrpc":"2.0","id":7,"result":{"structuredContent":{"price":1.0},"content":[]}}'
        page_503 = (
            b"<html><body><h1>503 Service Temporarily Unavailable</h1></body></html>"
        )

        class Backend(httpcore.MockBackend):
            connections = 0

            def connect_tcp(self, *args, **kwargs):
                Backend.connections += 1
                return super().connect_tcp(*args, **kwargs)

        backend = Backend(
            [
                b"HTTP/1.1 503 Service Temporarily Unavailable\r\n"
                b"Content-Type: text/html\r\n"
                b"Content-Length: %d\r\n\r\n%s" % (len(page_503), page_503),
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: %d\r\n\r\n%s" % (len(reply_200), reply_200),
            ]
        )
        transport = httpx.HTTPTransport()
        transport._pool = httpcore.ConnectionPool(network_backend=backend)
        monkeypatch.setattr(m, "_get_next_message_id", lambda: 7)
        with httpx.Client(transport=transport) as client:
            _install(monkeypatch, "d", DATA_ENTRY, client)

            reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {"price": 1.0}
        assert Backend.connections == 1

    def test_a_read_timeout_is_never_resent(self, monkeypatch, token_file, clock):
        # The request reached the server and may still be spending vendor quota.
        client = _Client([httpx.ReadTimeout("timed out"), 200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"]["error"] == "upstream_error"
        assert len(client.sent) == 1


class TestRateLimit:
    @pytest.mark.parametrize("edge", [min, max])
    def test_a_pause_is_stretched_never_shortened(
        self, monkeypatch, token_file, clock, edge
    ):
        # Calls the limiter refused together would otherwise resend together.
        monkeypatch.setattr(m.random, "uniform", lambda low, high: edge(low, high))
        client = _Client([429, _Resp(429, headers={"retry-after": "3"}), 200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {"price": 1.0}
        stretch = 1.0 if edge is min else 1 + m._DATA_TOKEN_JITTER
        assert clock.slept == pytest.approx(
            [m._DATA_TOKEN_RATE_PAUSE_S * stretch, 3.0 * stretch]
        )


class TestUnreadableReplies:
    """A reply the client refuses to read is an envelope as well, and is never
    resent: the cap or deadline that stopped one read would stop the next."""

    @pytest.mark.parametrize(
        "unreadable, reason",
        [
            (_Resp(200, raw=b"<html>"), "invalid_reply"),
            (
                _Resp(200, {"jsonrpc": "2.0", "id": -1, "result": {}}),
                "mismatched_reply",
            ),
            (_Resp(200, raw=b"data: {}\n\n", ctype="text/event-stream"), "no_reply"),
        ],
    )
    def test_a_malformed_reply_is_upstream_error(
        self, monkeypatch, token_file, unreadable, reason
    ):
        client = _Client([unreadable, 200])
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {
            "error": "upstream_error",
            "detail": f"the data service call failed ({reason})",
        }
        assert len(client.sent) == 1

    def test_a_reply_past_the_deadline_is_upstream_error(
        self, monkeypatch, token_file, clock
    ):
        def late(status):
            clock.now += m._HTTP_CALL_BUDGET + 1

        client = _Client([200, 200], on_send=late)
        _install(monkeypatch, "d", DATA_ENTRY, client)

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"]["detail"] == (
            "the data service call failed (stream_deadline)"
        )
        assert len(client.sent) == 1

    def test_an_unreadable_handshake_reads_as_a_failed_handshake(
        self, monkeypatch, token_file
    ):
        # The discover probe, then the legacy initialize it falls back to.
        client = _Client([_Resp(200, raw=b"<html>"), _Resp(200, raw=b"<html>")])
        _install(monkeypatch, "d", DATA_ENTRY, client)
        monkeypatch.setattr(m, "_PROTO", {})

        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {
            "error": "upstream_error",
            "detail": "the data service failed the handshake (invalid_reply)",
        }


def _hold_first_send(monkeypatch, callers: int):
    """An ``on_send`` holding the first send until every caller waits on the
    server's lock, so each has started before the handshake ahead of it ends."""
    arrived = threading.Semaphore(0)
    lock = threading.RLock()

    class _CountedLock:
        def __enter__(self):
            arrived.release()
            return lock.__enter__()

        def __exit__(self, *exc):
            return lock.__exit__(*exc)

    monkeypatch.setattr(m, "_get_server_lock", lambda name: _CountedLock())
    held = []

    def on_send(status):
        if not held:
            held.append(status)
            for _ in range(callers):
                assert arrived.acquire(timeout=5)

    return on_send


def _in_parallel(callers: int, call) -> list:
    results, errors = [], []

    def run():
        try:
            results.append(call())
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(callers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not errors, errors
    assert len(results) == callers
    return results


class TestParallelFirstCalls:
    def test_calls_queued_behind_a_failed_handshake_share_its_failure(
        self, monkeypatch, token_file, clock
    ):
        # Each waiter running its own retry cycle would finish one cycle later
        # than the one ahead of it, holding a daemon slot all the while.
        callers = 4
        cycle = len(m._DATA_TOKEN_TRANSIENT_BACKOFF_S) + 1
        client = _Client(
            [_refused()] * cycle * callers,
            on_send=_hold_first_send(monkeypatch, callers),
        )
        _install(monkeypatch, "d", DATA_ENTRY, client)
        monkeypatch.setattr(m, "_PROTO", {})
        monkeypatch.setattr(m, "_handshake_failures", {})

        replies = _in_parallel(callers, lambda: m._http_exchange("d", "quote", {}))

        assert [r["result"]["structuredContent"]["error"] for r in replies] == [
            "upstream_error"
        ] * callers
        assert len(client.sent) == cycle
        assert clock.slept == list(m._DATA_TOKEN_TRANSIENT_BACKOFF_S)

        # A call that starts after the failure negotiates afresh.
        client.statuses[:] = [200] * 4
        reply = m._http_exchange("d", "quote", {})

        assert reply["result"]["structuredContent"] == {"price": 1.0}


class TestNoResendElsewhere:
    @pytest.mark.parametrize("status", [401, 429, 502, 503])
    def test_another_http_servers_call_goes_out_once(self, monkeypatch, status):
        client = _Client([status, 200])
        _install(
            monkeypatch,
            "u",
            {"transport": "http", "untrusted": True, "url": URL, "headers": {}},
            client,
        )

        with pytest.raises(httpx.HTTPStatusError):
            m._http_exchange("u", "place_order", {})

        assert len(client.sent) == 1
        assert "Authorization" not in client.sent[0]
