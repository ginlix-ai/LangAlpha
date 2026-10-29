"""The sse-upgrade check a plugin install runs before offering the rewrite.

A package's legacy ``sse`` entries are held back and probed over streamable
HTTP: an endpoint that answers is offered as ``upgradable``, anything else stays
a skipped sse entry. The question here is narrower than a probe verdict, so this
path reads the raw outcome: an auth challenge still proves the transport, and
the credential is the connector flow's problem rather than the installer's.

The fan-out phase is bounded by ``MAX_PROBED_ENTRIES`` together with the shared
probe gate, which is the only reason a package with 10,000 sse entries cannot
hold the install request open for hours.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.server.services.plugins import server_fanout
from src.server.services.plugins.mcp import McpEntryPlan
from src.server.services.mcp_probe import ProbeOutcome


def _plan(key: str, url: str = "https://x.example.com/sse") -> McpEntryPlan:
    return McpEntryPlan(
        key=key, name=key, renamed=False, transport="sse", config={"url": url}
    )


@pytest.fixture
def probe(monkeypatch):
    mock = AsyncMock()
    monkeypatch.setattr(
        "src.server.services.mcp_probe.bounded_probe", mock, raising=True
    )
    return mock


@pytest.mark.asyncio
@pytest.mark.parametrize("auth", ["credential", "oauth"])
async def test_an_auth_challenge_still_confirms_streamable_http(probe, auth):
    """The regression this replaces: the old probe module sniffed the string
    "401" out of an exception message. The shared probe reads the real status,
    and a challenge is a completed handshake as far as transport goes."""
    probe.return_value = ProbeOutcome(ok=False, auth=auth, http_status=401)

    probed = await server_fanout._probe_sse_entries([_plan("a")])

    assert probed["a"].ok is True
    assert "streamable HTTP confirmed" in probed["a"].detail


@pytest.mark.asyncio
async def test_an_open_endpoint_that_answers_is_confirmed_with_no_caveat(probe):
    probe.return_value = ProbeOutcome(ok=True, auth="none")

    probed = await server_fanout._probe_sse_entries([_plan("a")])

    assert (probed["a"].ok, probed["a"].detail) == (True, "")


@pytest.mark.asyncio
async def test_an_unreachable_endpoint_keeps_its_reason(probe):
    """The reason reaches the install report, so a failed upgrade tells the user
    what the endpoint did rather than only that it was not offered."""
    probe.return_value = ProbeOutcome(ok=False, auth="none", error="connect timeout")

    probed = await server_fanout._probe_sse_entries([_plan("a")])

    assert probed["a"].ok is False
    assert probed["a"].detail == "connect timeout"


@pytest.mark.asyncio
async def test_the_probe_sends_no_headers(probe):
    """Nothing is stored yet for a package being installed, and a probe that
    guessed at credentials would report an upgrade the install cannot deliver."""
    probe.return_value = ProbeOutcome(ok=True, auth="none")

    await server_fanout._probe_sse_entries([_plan("a", "https://y.example.com/sse")])

    assert probe.await_args.args == ("https://y.example.com/sse", {})


@pytest.mark.asyncio
async def test_entries_past_the_cap_are_reported_not_dropped(probe, monkeypatch):
    """A dropped key would read as "no result" downstream, which is the same
    branch as a failed probe but with an empty reason."""
    monkeypatch.setattr(server_fanout, "MAX_PROBED_ENTRIES", 2)
    probe.return_value = ProbeOutcome(ok=True, auth="none")

    probed = await server_fanout._probe_sse_entries([_plan(k) for k in "abcd"])

    assert probe.await_count == 2
    assert set(probed) == set("abcd")
    assert [probed[k].ok for k in "abcd"] == [True, True, False, False]
    assert "not probed" in probed["c"].detail
