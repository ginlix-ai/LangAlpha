"""The one place a probe becomes a word, and the one caller that still reads
the raw outcome instead.

``probe_verdict`` is computed on the host because only the host knows whether a
credential was sent: the wire for "connect this server" and "your key is wrong"
is the same 401. Every surface (the catalog row's stored ``last_probe``, the add
form's ad-hoc probe) renders that word, so the mapping is pinned here rather
than re-derived per caller.

The plugin installer's sse upgrade check is the deliberate exception: it is
asking a narrower question than the verdict answers, and the last test says so.

The middle section covers the other boundary a probe crosses: ``resolve_header_refs``,
where a stored vault value becomes an HTTP header. Storage keeps a multi-line
secret intact, so this is the sink that has to trim what a paste left behind and
refuse what would split the request.

The closing section covers where the verdict lands: every guarded snapshot
write carries one, including the failures, which is the whole reason it needed
storage of its own.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.server.database.mcp_tool_schemas import SchemaWrite
from src.server.models.mcp_server import ProbeResult
from src.server.services.mcp_oauth import discovery
from src.server.services.mcp_probe import (
    INVALID_HEADER_VALUE,
    ProbeOutcome,
    probe_result,
    probe_verdict,
    rejected_header_result,
)


def _outcome(**overrides) -> ProbeOutcome:
    base = {"ok": False, "auth": "none"}
    return ProbeOutcome(**(base | overrides))


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (_outcome(ok=True, auth="none"), "ok"),
        (_outcome(ok=True, auth="credential", sent_credential=True), "ok_authed"),
        (_outcome(auth="credential", http_status=401), "needs_credential"),
        (_outcome(auth="credential", http_status=403), "needs_credential"),
        (
            _outcome(auth="credential", http_status=401, sent_credential=True),
            "credential_rejected",
        ),
        (_outcome(auth="oauth", http_status=401), "oauth"),
        # A supplied credential does not change an OAuth challenge: the server
        # is naming a flow, not rejecting a key.
        (_outcome(auth="oauth", http_status=401, sent_credential=True), "oauth"),
        (_outcome(auth="none", error="timeout"), "unreachable"),
        (_outcome(auth="none", http_status=503), "unreachable"),
    ],
)
def test_the_verdict_mapping(outcome, expected):
    assert probe_verdict(outcome) == expected


def test_missing_secrets_outrank_everything_else():
    """Nothing was dialled. Sending the literal ``${vault:...}`` string would
    come back a rejected key, which is the wrong thing to tell the user."""
    assert (
        probe_verdict(_outcome(auth="credential"), missing_secrets=["API_KEY"])
        == "missing_secrets"
    )


def test_an_open_server_that_answers_is_ok_not_ok_authed():
    """``ok_authed`` is what makes a working key visible in the UI, so it must
    not fire for a server that was never asked for one."""
    assert probe_verdict(_outcome(ok=True, auth="none", sent_credential=False)) == "ok"


def test_a_stored_verdict_carries_no_tools():
    """The row's tools are its cached snapshot's and outlive a probe that starts
    failing, so the verdict must never be the thing that supplies them."""
    result = probe_result(
        _outcome(ok=True, auth="none", tools=[{"name": "quote", "description": "d"}])
    )

    assert result.tools == []


def test_the_ad_hoc_probe_previews_name_and_description_only():
    """The add form is deciding whether to save an address; no snapshot exists
    yet for an input schema to belong to."""
    result = probe_result(
        _outcome(
            ok=True,
            auth="none",
            tools=[{"name": "quote", "description": "d", "input_schema": {"x": 1}}],
        ),
        include_tools=True,
    )

    assert [(t.name, t.description) for t in result.tools] == [("quote", "d")]
    assert not hasattr(result.tools[0], "input_schema")


def test_the_result_round_trips_through_json():
    """``last_probe`` is stored as jsonb and read back into this model, so the
    dumped shape has to validate as itself."""
    stored = probe_result(
        _outcome(auth="credential", http_status=401, error="401 Unauthorized")
    ).model_dump(mode="json")

    restored = ProbeResult.model_validate(stored)

    assert restored.verdict == "needs_credential"
    assert restored.http_status == 401
    assert restored.probed_at is not None


# ---------------------------------------------------------------------------
# The header sink: where a stored vault value becomes an HTTP header
# ---------------------------------------------------------------------------


def test_a_ref_that_resolves_is_expanded_in_place():
    resolved, missing = discovery.resolve_header_refs(
        {"Authorization": "Bearer ${vault:K}"}, {"K": "sk-live"}
    )

    assert resolved == {"Authorization": "Bearer sk-live"}
    assert missing == []


def test_a_trailing_newline_is_trimmed_off_the_resolved_value():
    """The vault stores what was pasted so a PEM survives it; this is the sink
    that has to turn that into something HTTP can frame, and a trailing newline
    is the one case where the user's intent is not in doubt."""
    resolved, _ = discovery.resolve_header_refs(
        {"Authorization": "Bearer ${vault:K}"}, {"K": "sk-live\n"}
    )

    assert resolved == {"Authorization": "Bearer sk-live"}


@pytest.mark.parametrize(
    "value",
    ["one\ntwo", "one\r\ntwo", "-----BEGIN KEY-----\nabc\n-----END KEY-----"],
    ids=["lf", "crlf", "pem"],
)
def test_a_line_break_inside_the_value_is_refused(value):
    """An embedded break is a second header, not a credential, so nothing here
    guesses at a repair."""
    with pytest.raises(discovery.RejectedHeaderValue) as caught:
        discovery.resolve_header_refs({"X-Api-Key": "${vault:K}"}, {"K": value})

    assert caught.value.header == "X-Api-Key"
    assert str(caught.value) == INVALID_HEADER_VALUE


def test_the_refusal_reads_the_same_as_the_one_httpx_would_have_raised():
    """One wording, whichever side catches it: the row's stored verdict and the
    add form's answer must not describe the same value two ways."""
    import httpx2

    from src.server.services.mcp_probe import _describe

    assert _describe(httpx2.LocalProtocolError("quotes the value")) == (
        INVALID_HEADER_VALUE
    )
    assert rejected_header_result().error == INVALID_HEADER_VALUE
    assert rejected_header_result().verdict == "unreachable"


# ---------------------------------------------------------------------------
# Where the verdict lands: every guarded write carries one
# ---------------------------------------------------------------------------


class _Writes(list):
    """The captured writes, plus the digest the stubbed write reports having
    overwritten, which is the one input to the fan-out decision."""

    replaced: str | None = None


@pytest.fixture
def writes(monkeypatch):
    """Captures the fenced snapshot write's kwargs without a database."""
    captured = _Writes()

    async def _write(self, *, probe, **kwargs):
        captured.append({"probe": probe} | kwargs)
        return SchemaWrite(
            {"status": kwargs.get("status", "ok")},
            replaced_digest=captured.replaced,
        )

    monkeypatch.setattr(discovery._SnapshotWriter, "write", _write)
    return captured


def _writer() -> "discovery._SnapshotWriter":
    return discovery._SnapshotWriter("u1", "authy", "fingerprint-1", None)


@pytest.mark.asyncio
async def test_a_failing_probe_writes_its_verdict_with_the_error_row(writes):
    await _writer().settle(
        _outcome(auth="credential", http_status=401, error="401 Unauthorized")
    )

    (write,) = writes
    assert write["status"] == "error"
    assert write["probe"].verdict == "needs_credential"
    assert write["probe"].http_status == 401


@pytest.mark.asyncio
async def test_a_successful_probe_writes_the_verdict_beside_the_tools(
    writes, monkeypatch
):
    monkeypatch.setattr(
        discovery, "bump_user_workspaces_mcp_version", AsyncMock(return_value=None)
    )

    await _writer().settle(
        _outcome(ok=True, auth="none", tools=[{"name": "quote"}])
    )

    (write,) = writes
    assert write["status"] == "ok"
    assert write["probe"].verdict == "ok"
    assert write["probe"].error == ""


@pytest.mark.asyncio
async def test_the_stored_verdict_is_plain_json(writes, monkeypatch):
    """It rides into a jsonb column, so a datetime or a model instance would
    only fail at the adapter."""
    import json

    await _writer().settle(_outcome(auth="none", error="timeout"))

    json.dumps(writes[0]["probe"].model_dump(mode="json"))


@pytest.mark.asyncio
async def test_the_fan_out_follows_what_the_write_replaced(writes, monkeypatch):
    """Two workers can probe one row at once (an explicit kick is not
    throttled) and both read the stored digest before either write lands.
    Deciding the bump from that pre-read let the second writer publish a
    surface no workspace was ever told to re-resolve."""
    bump = AsyncMock(return_value=None)
    monkeypatch.setattr(discovery, "bump_user_workspaces_mcp_version", bump)
    outcome = _outcome(ok=True, auth="none", tools=[{"name": "quote"}])

    writes.replaced = discovery._schema_digest(outcome.tools)
    await _writer().settle(outcome)

    bump.assert_not_awaited()

    writes.replaced = "what-the-other-worker-left"
    await _writer().settle(outcome)

    bump.assert_awaited_once()
