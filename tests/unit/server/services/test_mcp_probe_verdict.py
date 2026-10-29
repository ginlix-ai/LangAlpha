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
storage of its own, and what a crossing of the servable set owes the user's
workspaces -- in the same transaction the verdict lands in, because a verdict
that commits without its bump is one nothing re-kicks.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from src.server.database.mcp_tool_schemas import SchemaWrite
from src.server.models.mcp_server import ProbeResult
from src.server.services.mcp_config import user_row_to_server_config
from src.server.services.mcp_discovery import mcp_discovery_fingerprint
from src.server.services.mcp_oauth import discovery
from src.server.services.mcp_oauth.lifecycle import TokenUnavailable
from src.server.services.mcp_probe import (
    INVALID_HEADER_VALUE,
    ProbeOutcome,
    RejectedHeaderValue,
    probe_result,
    probe_verdict,
    resolve_header_refs,
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
        # A sign-in challenge in answer to a key is that key turned down; the
        # save refuses it as such, so the preview must not offer a sign-in.
        (
            _outcome(auth="oauth", http_status=401, sent_credential=True),
            "credential_rejected",
        ),
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
        probe_verdict(_outcome(auth="credential", missing_secrets=("API_KEY",)))
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
    resolved, missing = resolve_header_refs(
        {"Authorization": "Bearer ${vault:K}"}, {"K": "sk-live"}
    )

    assert resolved == {"Authorization": "Bearer sk-live"}
    assert missing == []


def test_a_trailing_newline_is_trimmed_off_the_resolved_value():
    """The vault stores what was pasted so a PEM survives it; this is the sink
    that has to turn that into something HTTP can frame, and a trailing newline
    is the one case where the user's intent is not in doubt."""
    resolved, _ = resolve_header_refs(
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
    with pytest.raises(RejectedHeaderValue) as caught:
        resolve_header_refs({"X-Api-Key": "${vault:K}"}, {"K": value})

    assert caught.value.header == "X-Api-Key"
    assert str(caught.value) == INVALID_HEADER_VALUE


@pytest.mark.asyncio
async def test_the_refusal_reads_the_same_as_the_one_httpx_would_have_raised(
    monkeypatch,
):
    """One wording, whichever side catches it: the row's stored verdict and the
    add form's answer must not describe the same value two ways."""
    import httpx2

    from src.server.services import mcp_probe

    monkeypatch.setattr(
        mcp_probe,
        "get_user_secrets_decrypted",
        AsyncMock(return_value={"K": "one\ntwo"}),
    )
    refused = await mcp_probe.probe_configured(
        "u1", "https://mcp.invalid/mcp", {"X-Api-Key": "${vault:K}"}
    )

    assert mcp_probe._describe(httpx2.LocalProtocolError("quotes the value")) == (
        INVALID_HEADER_VALUE
    )
    assert refused.error == INVALID_HEADER_VALUE
    assert probe_verdict(refused) == "unreachable"


# ---------------------------------------------------------------------------
# Where the verdict lands: every guarded write carries one
# ---------------------------------------------------------------------------


_ROW = {
    "name": "authy",
    # Every catalog SELECT projects these, and the fingerprint the writer
    # fences on is computed from them, so the row has to be the real shape.
    "enabled": True,
    "transport": "http",
    "command": None,
    "args": [],
    "url": "https://api.example.com/mcp",
    "env": {},
    "headers": {},
    "description": "d",
    "instruction": "i",
    "tool_exposure_mode": "summary",
}


class _Writes(list):
    """The captured upsert kwargs, plus what the stubbed write reports having
    overwritten: the digest and the verdict, which are the two inputs to the
    fan-out decision. ``row`` is what the write lands, None for one the
    connection guard refused under the lock."""

    row: dict | None = {"status": "ok"}
    connection_status: str | None = None
    replaced: str | None = None
    replaced_verdict: str | None = None


class _FakeCursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    """Records when the fence transaction exits, so a test can pin that the
    bump was issued while it was still open."""

    def __init__(self) -> None:
        self.exited = False
        self.cur = _FakeCursor()

    @asynccontextmanager
    async def transaction(self):
        try:
            yield self
        finally:
            self.exited = True

    def cursor(self) -> _FakeCursor:
        return self.cur


@pytest.fixture
def writes(monkeypatch):
    """Captures the fenced write's upsert without a database. The fence itself
    is left real, because the fan-out now rides inside it."""
    captured = _Writes()
    captured.conn = _FakeConn()

    @asynccontextmanager
    async def _connection(conn=None):
        yield captured.conn

    async def _upsert(user_id, server_name, config_hash, **kwargs):
        captured.append(kwargs)
        return SchemaWrite(
            captured.row,
            captured.connection_status,
            replaced_digest=captured.replaced,
            replaced_verdict=captured.replaced_verdict,
        )

    monkeypatch.setattr(discovery, "get_db_connection", _connection)
    monkeypatch.setattr(discovery, "get_catalog_server", AsyncMock(return_value=_ROW))
    monkeypatch.setattr(discovery, "upsert_user_tool_schemas", _upsert)
    monkeypatch.setattr(discovery, "enqueue_if_new", AsyncMock(return_value=False))
    return captured


@pytest.fixture
def bump(monkeypatch):
    """The fan-out itself, stubbed: these tests are about when it fires."""
    stub = AsyncMock(return_value=None)
    monkeypatch.setattr(discovery, "bump_user_versions", stub)
    return stub


def _writer() -> "discovery._SnapshotWriter":
    return discovery._SnapshotWriter(
        "u1", "authy", mcp_discovery_fingerprint(user_row_to_server_config(_ROW)), None
    )


@pytest.mark.asyncio
async def test_a_failing_probe_writes_its_verdict_with_the_error_row(writes, bump):
    await _writer().settle(
        _outcome(auth="credential", http_status=401, error="401 Unauthorized")
    )

    (write,) = writes
    assert write["status"] == "error"
    assert write["last_probe"]["verdict"] == "needs_credential"
    assert write["last_probe"]["http_status"] == 401


@pytest.mark.asyncio
async def test_a_successful_probe_writes_the_verdict_beside_the_tools(writes, bump):
    await _writer().settle(
        _outcome(ok=True, auth="none", tools=[{"name": "quote"}])
    )

    (write,) = writes
    assert write["status"] == "ok"
    assert write["last_probe"]["verdict"] == "ok"
    assert write["last_probe"]["error"] == ""


@pytest.mark.asyncio
async def test_the_stored_verdict_is_plain_json(writes, bump):
    """It rides into a jsonb column, so a datetime or a model instance would
    only fail at the adapter."""
    import json

    await _writer().settle(_outcome(auth="none", error="timeout"))

    json.dumps(writes[0]["last_probe"])


@pytest.mark.asyncio
async def test_the_fan_out_follows_what_the_write_replaced(writes, bump):
    """Two workers can probe one row at once (an explicit kick is not
    throttled) and both read the stored digest before either write lands.
    Deciding the bump from that pre-read let the second writer publish a
    surface no workspace was ever told to re-resolve."""
    outcome = _outcome(ok=True, auth="none", tools=[{"name": "quote"}])

    writes.replaced = discovery._schema_digest(outcome.tools)
    writes.replaced_verdict = "ok"
    await _writer().settle(outcome)

    bump.assert_not_awaited()

    writes.replaced = "what-the-other-worker-left"
    await _writer().settle(outcome)

    bump.assert_awaited_once()


@pytest.mark.asyncio
async def test_the_fan_out_is_issued_before_the_fence_commits(writes, monkeypatch):
    """The regression this shape fixes: a verdict that commits without its bump
    leaves every warm session holding grants the row no longer earns, and the
    next probe carries the same verdict, so it crosses nothing and the bump is
    never made up."""
    seen = []

    async def _bump(cur, user_id):
        seen.append((writes.conn.exited, cur, user_id))

    monkeypatch.setattr(discovery, "bump_user_versions", _bump)

    await _writer().settle(_outcome(ok=True, auth="none", tools=[{"name": "quote"}]))

    assert seen == [(False, writes.conn.cur, "u1")]
    assert writes.conn.exited


@pytest.mark.asyncio
async def test_the_fence_takes_the_catalog_row_exclusively(writes, bump):
    """A shared lock stops an edit but not a second probe, and on a config's
    FIRST probe there is no snapshot row downstream for the pair to serialize
    on either: both read no replaced verdict, so an ok-then-rejected pair
    crosses nothing and the warm session keeps a grant the row has lost."""
    await _writer().settle(_outcome(ok=True, auth="none", tools=[{"name": "quote"}]))

    kwargs = discovery.get_catalog_server.await_args.kwargs
    assert kwargs["for_update"] is True
    assert "for_share" not in kwargs


@pytest.mark.asyncio
async def test_a_write_refused_under_the_lock_fans_nothing_out(writes, bump):
    """A disconnect that commits during the network phase already purged both
    tiers; the refused write has nothing to publish, so the bump that would
    push it to every workspace must not fire either."""
    writes.row = None
    writes.connection_status = "revoked"

    with pytest.raises(TokenUnavailable) as caught:
        await _writer().settle(
            _outcome(ok=True, auth="none", tools=[{"name": "quote"}])
        )

    assert caught.value.reason == "revoked"
    bump.assert_not_awaited()


# ---------------------------------------------------------------------------
# Crossing the servable set: the fan-out the digest alone cannot see
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_row_that_stops_answering_fans_the_bump_out(writes, bump):
    """The regression. The no-downgrade upsert keeps the tools, the status and
    the digest of a row that listed once and now 401s, so ``last_probe`` is the
    only thing that moved, and a warm PTC session re-resolves its grants only
    when the config version does. The direct tool and its ``header_mcp`` grant
    otherwise outlive the verdict that retired them."""
    writes.replaced_verdict = "ok_authed"

    await _writer().settle(
        _outcome(auth="credential", http_status=401, sent_credential=True)
    )

    bump.assert_awaited_once_with(writes.conn.cur, "u1")


@pytest.mark.asyncio
async def test_a_row_that_was_already_failing_fans_nothing_out(writes, bump):
    """A self-heal pass re-probes an unreachable row every two minutes, and a
    bump per pass would rebuild every workspace's wrappers for a row that has
    not moved."""
    writes.replaced_verdict = "unreachable"

    await _writer().settle(_outcome(auth="none", error="timeout"))

    bump.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_recovery_with_the_same_tools_fans_the_bump_out(writes, bump):
    """The other direction, and the reason the digest is not the whole test: a
    vendor outage that ends comes back with the tool surface it left with, so
    the digest matches and the retired grant would never be re-issued."""
    outcome = _outcome(ok=True, auth="none", tools=[{"name": "quote"}])
    writes.replaced = discovery._schema_digest(outcome.tools)
    writes.replaced_verdict = "unreachable"

    await _writer().settle(outcome)

    bump.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_token_that_cannot_be_minted_fans_out_like_any_other_failure(
    writes, bump
):
    """``fail`` also lands the OAuth arm's verdict, and a connection whose token
    will not mint costs the row its grant exactly as a 401 does."""
    writes.replaced_verdict = "ok_authed"

    await _writer().fail(
        probe_result(
            ProbeOutcome(
                ok=False, auth="oauth", error="token unavailable: needs_reauth"
            )
        )
    )

    bump.assert_awaited_once_with(writes.conn.cur, "u1")


# ---------------------------------------------------------------------------
# The wire: which of a row's headers survive into the handshake
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_row_cannot_overwrite_the_preflights_own_protocol_version(monkeypatch):
    """The preflight merges the row's map over its own headers, so before the
    strip widened, a row spelling ``MCP-Protocol-Version`` announced one version
    in the header and another in the initialize body it never wrote."""
    from types import SimpleNamespace

    from src.server.services import mcp_probe
    from src.server.services.mcp_oauth.tokens import PROTOCOL_VERSION
    from src.server.utils.egress_guard import PinnedTarget

    sent: list[dict] = []

    async def _pin(url, **kwargs):
        return PinnedTarget(
            url="https://93.184.216.34/rpc",
            host="mcp.invalid",
            ip="93.184.216.34",
            authority="mcp.invalid",
        )

    async def _pinned_request(client, method, url, *, headers=None, **kwargs):
        sent.append(dict(headers or {}))
        # 500 ends the preflight before the SDK session, which is as far as
        # this test needs the handshake to get.
        return SimpleNamespace(status_code=500, headers={})

    monkeypatch.setattr(mcp_probe, "pin_public_url", _pin)
    monkeypatch.setattr(mcp_probe, "pinned_request", _pinned_request)

    outcome = await mcp_probe.probe_remote_server(
        "https://mcp.invalid/rpc",
        {
            "MCP-Protocol-Version": "1999-01-01",
            "Mcp-Session-Id": "forged",
            "host": "elsewhere.invalid",
            "X-Api-Key": "k-123",
        },
    )

    assert outcome.http_status == 500
    probe = sent[0]
    assert probe["MCP-Protocol-Version"] == PROTOCOL_VERSION
    # No alternate-casing duplicate riding alongside the one the probe framed.
    assert [k for k in probe if k.lower() == "mcp-protocol-version"] == [
        "MCP-Protocol-Version"
    ]
    assert not any(k.lower() == "mcp-session-id" for k in probe)
    assert not any(k.lower() == "host" for k in probe)
    # The credential itself still travels, and still counts as one sent: the
    # verdict for a 401 turns on it.
    assert probe["X-Api-Key"] == "k-123"
    assert outcome.sent_credential is True


def _answering(monkeypatch, status: int, body: bytes, content_type: str) -> None:
    """Answer the preflight with one canned reply and dial nothing else."""
    import httpx2

    from src.server.services import mcp_probe
    from src.server.utils.egress_guard import PinnedTarget

    async def _pin(url, **kwargs):
        return PinnedTarget(
            url="https://93.184.216.34/mcp",
            host="mcp.invalid",
            ip="93.184.216.34",
            authority="mcp.invalid",
        )

    async def _pinned_request(client, method, url, **kwargs):
        return httpx2.Response(
            status, headers={"content-type": content_type}, content=body
        )

    monkeypatch.setattr(mcp_probe, "pin_public_url", _pin)
    monkeypatch.setattr(mcp_probe, "pinned_request", _pinned_request)


# CoinMarketCap's answer to a wrong key: a 200 whose result is a tool-style
# error, which the SDK can only report as a schema mismatch.
_ISERROR_REPLY = (
    b'{"jsonrpc":"2.0","id":0,"result":{"content":[{"type":"text","text":'
    b'"{\\"error\\":{\\"code\\":1001,\\"message\\":\\"This API Key is invalid. \\"}}"}],'
    b'"isError":true}}'
)


@pytest.mark.asyncio
async def test_a_key_refused_inside_the_handshake_reads_as_the_servers_words(
    monkeypatch,
):
    from src.server.services import mcp_probe

    _answering(monkeypatch, 200, _ISERROR_REPLY, "application/json")

    outcome = await mcp_probe.probe_remote_server(
        "https://mcp.invalid/mcp", {"X-Api-Key": "k-123"}
    )

    assert probe_result(outcome).verdict == "credential_rejected"
    assert outcome.error == "the server refused the handshake: This API Key is invalid."


@pytest.mark.asyncio
async def test_a_json_rpc_error_on_an_event_stream_is_read_too(monkeypatch):
    from src.server.services import mcp_probe

    body = (
        b'event: message\ndata: {"jsonrpc":"2.0","id":0,"error":'
        b'{"code":-32001,"message":"Unauthorized: bad token"}}\n\n'
    )
    _answering(monkeypatch, 200, body, "text/event-stream")

    outcome = await mcp_probe.probe_remote_server("https://mcp.invalid/mcp", {})

    # No credential went out, so the refusal cannot be a rejected key.
    assert probe_result(outcome).verdict == "unreachable"
    assert outcome.error == "the server refused the handshake: Unauthorized: bad token"


@pytest.mark.asyncio
async def test_a_refusal_quoting_the_key_is_scrubbed_before_it_is_cut(monkeypatch):
    """The server's words are kept whole until the key is out of them: a cap
    applied first would cut the quoted key in half, past any scrub."""
    import json

    from src.server.services import mcp_probe

    key = "k-STRADDLE-0123456789abcdef"
    # Placed across where a 160-character cap on the refusal would fall.
    message = "x" * 125 + f" key {key} is not valid"
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 0, "error": {"code": -32001, "message": message}}
    ).encode()
    _answering(monkeypatch, 200, body, "application/json")

    outcome = await mcp_probe.probe_remote_server(
        "https://mcp.invalid/mcp", {"X-Api-Key": key}
    )

    assert outcome.error.endswith(" key [redacted] is not valid")
    assert not any(key[:n] in outcome.error for n in range(8, len(key) + 1))


_ECHOED_KEY = "sk_live_9f8e7d6c5b4a3210"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "headers", "vault"),
    [
        (f"https://mcp.invalid/mcp?apiKey={_ECHOED_KEY}", {}, {}),
        (
            "https://mcp.invalid/mcp",
            {"Authorization": "Token ${vault:VENDOR_KEY}"},
            {"VENDOR_KEY": _ECHOED_KEY},
        ),
        (
            "https://mcp.invalid/mcp",
            {"X-Auth": "acct=42;key=${vault:VENDOR_KEY}"},
            {"VENDOR_KEY": _ECHOED_KEY},
        ),
        # An import vaults a literal header whole, scheme word and all.
        (
            "https://mcp.invalid/mcp",
            {"Authorization": "${vault:VENDOR_KEY}"},
            {"VENDOR_KEY": f"Token {_ECHOED_KEY}"},
        ),
    ],
    ids=["url-query", "other-scheme-word", "composite-header", "vaulted-with-scheme"],
)
async def test_a_refusal_quoting_the_key_alone_is_scrubbed_wherever_it_was_sent(
    monkeypatch, url, headers, vault
):
    """A server quotes the part it rejected, not the field it sat in, and the
    refusal is stored on the row, logged, and served in a failed check."""
    import json

    from src.server.services import mcp_probe

    message = f"key {_ECHOED_KEY} is not valid for {url}"
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 0, "error": {"code": -32001, "message": message}}
    ).encode()
    _answering(monkeypatch, 200, body, "application/json")
    monkeypatch.setattr(
        mcp_probe, "get_user_secrets_decrypted", AsyncMock(return_value=vault)
    )

    outcome = await mcp_probe.probe_configured("u1", url, headers)

    assert outcome.error == (
        "the server refused the handshake: key [redacted] is not valid for mcp.invalid"
    )


def test_a_real_handshake_reply_is_not_a_refusal():
    from src.server.services.mcp_probe import _handshake_refusal

    reply = (
        b'event: message\ndata: {"jsonrpc":"2.0","id":0,"result":{"protocolVersion":'
        b'"2025-06-18","capabilities":{},"serverInfo":{"name":"x","version":"1"}}}\n\n'
    )
    assert _handshake_refusal(reply) is None
    assert _handshake_refusal(b"") is None
    assert _handshake_refusal(b"\xff\xfe not json") is None


@pytest.mark.asyncio
async def test_a_rate_limit_says_to_try_again(monkeypatch):
    from src.server.services import mcp_probe

    _answering(monkeypatch, 429, b"slow down", "text/plain")

    outcome = await mcp_probe.probe_remote_server("https://mcp.invalid/mcp", {})

    assert outcome.http_status == 429
    assert "try again in a minute" in outcome.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "oauth", "verdict"),
    [
        # The form offered a sign-in, and the save then refused the key.
        ({"Authorization": "Bearer x"}, True, "credential_rejected"),
        # Plain config is not a key: both said one was rejected when none went.
        ({"X-Region": "us"}, False, "needs_credential"),
    ],
    ids=["key-to-an-oauth-server", "plain-config-to-a-401"],
)
async def test_the_preview_says_what_the_save_will(monkeypatch, headers, oauth, verdict):
    """The add form's probe is a preview of the save's check, so one answer
    from the server has to read the same on both."""
    from src.server.models.mcp_server import McpServerInput
    from src.server.services import mcp_probe
    from src.server.services.mcp_validation import check_server

    _answering(monkeypatch, 401, b"", "text/plain")
    monkeypatch.setattr(
        mcp_probe, "_oauth_metadata_behind", AsyncMock(return_value=oauth)
    )
    url = "https://mcp.invalid/mcp"

    preview = probe_result(await mcp_probe.probe_configured("u1", url, headers))
    saved = await check_server(
        "u1", McpServerInput(name="vendor", transport="http", url=url, headers=headers)
    )

    assert (preview.verdict, saved.verdict) == (verdict, verdict)
