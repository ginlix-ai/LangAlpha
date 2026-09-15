"""What a remote endpoint is allowed to write onto a catalog row.

A probe's ``error`` is durable: it is stored on the row, logged, and served by
``GET /api/v1/mcp/servers``. The endpoint on the other end chooses that text, so
these tests pin the two things the host decides instead -- how long it may be,
and that a credential we sent never comes back in it.
"""

from __future__ import annotations

import logging
import httpx2
import pytest

from src.server.services import mcp_probe
from src.server.services.mcp_probe import (
    PROBE_ERROR_MAX_CHARS,
    ProbeOutcome,
    _bounded,
    _describe,
    _scrub_sent_values,
)


def test_bounded_caps_and_marks() -> None:
    out = _bounded("x" * 5000)
    assert len(out) <= PROBE_ERROR_MAX_CHARS
    assert out.endswith("[truncated]")
    assert out.startswith("xxx")


def test_bounded_collapses_control_runs() -> None:
    assert _bounded("  first\r\n\tsecond\x00third  ") == "first second third"


def test_bounded_passes_short_text_through() -> None:
    assert _bounded("connection refused") == "connection refused"


def test_describe_bounds_a_huge_remote_message() -> None:
    # An SDK error carrying whatever JSON-RPC message the server sent; the
    # discovery client allows a response far larger than this.
    out = _describe(RuntimeError("boom " * 20_000))
    assert len(out) <= PROBE_ERROR_MAX_CHARS
    assert out.endswith("[truncated]")


def test_describe_bounds_a_connect_error() -> None:
    out = _describe(httpx2.ConnectError("nope " * 20_000))
    assert out.startswith("could not connect: ")
    assert len(out) <= PROBE_ERROR_MAX_CHARS + len("could not connect: ")
    assert out.endswith("[truncated]")


def test_scrub_redacts_a_raw_sent_value() -> None:
    out = _scrub_sent_values(
        "server answered HTTP 401: key k-123-secret is not valid",
        {"X-Api-Key": "k-123-secret"},
    )
    assert "k-123-secret" not in out
    assert "[redacted]" in out


def test_scrub_redacts_the_token_half_of_a_bearer_value() -> None:
    out = _scrub_sent_values(
        "rejected token tok-abc",
        {"Authorization": "Bearer tok-abc"},
    )
    assert "tok-abc" not in out
    assert out == "rejected token [redacted]"


def test_scrub_leaves_an_unrelated_error_alone() -> None:
    error = "could not connect: connection refused"
    assert _scrub_sent_values(error, {"X-Api-Key": "k-123-secret"}) == error


def test_scrub_ignores_empty_values() -> None:
    error = "server answered HTTP 500 to the MCP handshake"
    assert _scrub_sent_values(error, {"X-Api-Key": "", "X-Blank": "   "}) == error


@pytest.mark.asyncio
async def test_probe_remote_server_scrubs_an_echoed_credential(monkeypatch) -> None:
    async def _probe(url, headers, *, timeout_s):
        # The shape of a server that quotes the header it refused.
        return ProbeOutcome(
            ok=False,
            auth="credential",
            http_status=401,
            error=f"discovery failed: bad key {headers['X-Api-Key']}",
        )

    monkeypatch.setattr(mcp_probe, "_probe", _probe)

    outcome = await mcp_probe.probe_remote_server(
        "https://mcp.invalid/rpc", {"X-Api-Key": "k-123-secret"}
    )

    assert "k-123-secret" not in outcome.error
    assert outcome.error == "discovery failed: bad key [redacted]"
    assert outcome.sent_credential is True


@pytest.mark.asyncio
async def test_probe_scrubs_an_echoed_credential_before_logging_it(
    monkeypatch, caplog
) -> None:
    """The log line runs inside ``_probe``, before the outcome is scrubbed on
    the way out, so it has to scrub for itself."""
    secret = "EXAMPLE-OPAQUE-TOKEN-AAA"

    async def _pin(url):
        return url

    async def _preflight(url, headers):
        raise RuntimeError(f"401 from upstream: rejected header {secret}")

    monkeypatch.setattr(mcp_probe, "pin_public_url", _pin)
    monkeypatch.setattr(mcp_probe, "_preflight", _preflight)
    with caplog.at_level(logging.INFO, logger="src.server.services.mcp_probe"):
        outcome = await mcp_probe._probe(
            "https://example.com/mcp?sig=EXAMPLE-QUERY-SIG",
            {"X-Api-Key": secret},
            timeout_s=5,
        )

    assert secret not in caplog.text
    # The URL itself is logged as its host: a row may carry a key in its query.
    assert "EXAMPLE-QUERY-SIG" not in caplog.text
    assert "example.com" in caplog.text
    assert "[redacted]" in caplog.text
    assert secret not in outcome.error
    assert "[redacted]" in outcome.error
