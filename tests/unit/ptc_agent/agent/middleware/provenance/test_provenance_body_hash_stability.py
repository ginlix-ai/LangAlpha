"""Hash-stability of the content-addressed body store on the SAFE FREQUENT PATH.

The body handed to ``store_result_body`` is the ``canonical`` string from
``fingerprint_result_with_body(value)`` — the exact string hashed into
``result_sha256`` — after passing through ``_redact_body``. So for content carrying NO secret value and NO
``gxsa_/gxsr_`` sandbox token (SEC filings, market-data snapshots, static docs —
the very content the GLOBAL dedup store exists to share across users/threads),
redaction is the identity and ``sha256(stored_body) == result_sha256`` exactly.

That equality is what both (a) a post-hoc verifier and (b) the content-addressed
dedup key rest on, so it is pinned here against a LIVE, ARMED
``LeakDetectionMiddleware`` (real secret values configured, just absent from the
content) to prove the redactor does not false-positive-corrupt clean content.

The intentional exclusion — a genuine leak (sandbox token / configured secret)
present in the body IS redacted, so the stored body no longer hashes to the
raw-content sha — is documented by the negative-control tests at the bottom.
"""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.middleware.provenance import ProvenanceMiddleware
from ptc_agent.agent.middleware.tool.leak_detection import LeakDetectionMiddleware
from ptc_agent.agent.provenance.types import RESULT_BODY_MAX_BYTES

_WRITER_PATH = (
    "ptc_agent.agent.middleware.provenance.middleware.get_stream_writer"
)
# store_result_bodies is imported lazily inside _flush_bodies; the middleware
# batches a turn's bodies into ONE call whose sole arg is a list of
# (sha, body, byte_len, content_type) tuples.
_STORE_PATH = "src.server.database.provenance_bodies.store_result_bodies"

# An armed redactor, exactly like production: secret VALUES configured. These are
# deliberately obvious non-secrets (just long enough to clear the redactor's >=8
# char gate) so they don't trip secret scanners; none appears in the clean SEC /
# market payloads below, so a correct redactor leaves those bodies byte-identical.
_VAULT_SECRETS = {
    "FMP_API_KEY": "fake-fmp-key-for-tests-only",
    "OPENAI_API_KEY": "fake-openai-key-for-tests-only",
    "GITHUB_TOKEN": "fake-github-token-for-tests-only",
}


def _armed_middleware() -> ProvenanceMiddleware:
    redactor = LeakDetectionMiddleware(vault_secrets=dict(_VAULT_SECRETS)).redact
    return ProvenanceMiddleware(redactor=redactor)


def _canonical(value) -> str:
    """Mirror of types._canonicalize for assertions."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
    return str(value)


def _make_request(name, args, tool_call_id="call-1"):
    return SimpleNamespace(
        tool_call={"name": name, "args": args, "id": tool_call_id}
    )


def _result(content="", artifact=None):
    return SimpleNamespace(content=content, artifact=artifact)


async def _run(middleware, request, result):
    """Run awrap_tool_call with patched writer + store; return (emitted, store)."""
    emitted: list = []
    store = AsyncMock()

    async def handler(_req):
        return result

    with patch(_WRITER_PATH, return_value=emitted.append), patch(
        _STORE_PATH, store
    ):
        await middleware.awrap_tool_call(request, handler)
        # Drain the background flush inside the patch so the task's lazy
        # store_result_bodies import resolves to the mock.
        await middleware._drain_body_flushes()
    return emitted, store


def _stored(store):
    """[(sha, body, size, content_type), ...] handed to the batched store call."""
    return [
        item
        for c in store.await_args_list
        for item in (c.args[0] if c.args else [])
    ]


# --- realistic, secret-free payloads (the frequent dedup path) -------------

_SEC_TEXT = (
    "Apple Inc. CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS (Unaudited). "
    "Net sales: Products $61,564; Services $24,213; Total net sales $85,777. "
    "Cost of sales $46,099. Gross margin $39,678. Operating income $25,352. "
    "Net income $21,448. Diluted EPS $1.40."
)


def _sec_artifact(text: str) -> dict:
    return {
        "symbol": "AAPL",
        "filings": [
            {
                "source_url": (
                    "https://www.sec.gov/Archives/edgar/data/320193/"
                    "000032019324000081/aapl-20240629.htm"
                ),
                "form_type": "10-Q",
                "filed_at": "2024-08-02",
                "accession_no": "0000320193-24-000081",
                "text": text,
            }
        ],
    }


_MARKET_SNAPSHOT = {
    "symbol": "AAPL",
    "as_of": "2024-08-02T20:00:00Z",
    "open": 219.15,
    "high": 225.60,
    "low": 217.71,
    "close": 219.86,
    "volume": 105568400,
    "change_pct": -4.82,
}


# ---------------------------------------------------------------------------
# Safe frequent path: stored body hashes EXACTLY to result_sha256.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sec_filing_body_hashes_to_sha():
    """A small 10-Q filing (no secrets): the body persisted under `sha` hashes to `sha`."""
    req = _make_request("get_sec_filing", {"symbol": "AAPL"})
    _, store = await _run(_armed_middleware(), req, _result(artifact=_sec_artifact(_SEC_TEXT)))

    calls = _stored(store)
    assert len(calls) == 1
    sha, body, size, content_type = calls[0]
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() == sha
    assert size == len(body.encode("utf-8"))
    assert content_type == "text/plain; charset=utf-8"
    assert "[REDACTED" not in body  # armed redactor left clean content untouched


@pytest.mark.asyncio
async def test_market_snapshot_body_hashes_to_sha():
    """A market-data snapshot (no secrets): stored body is the canonical artifact and hashes to `sha`."""
    req = _make_request("get_daily_prices", {"symbol": "AAPL"})
    _, store = await _run(_armed_middleware(), req, _result(artifact=_MARKET_SNAPSHOT))

    sha, body, size, _ = _stored(store)[0]
    assert body == _canonical(_MARKET_SNAPSHOT)
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() == sha
    assert "[REDACTED" not in body


@pytest.mark.asyncio
async def test_large_sec_filing_full_body_hashes_to_sha():
    """A >64 KiB filing: the middleware hands the FULL canonical body to the store
    (the head/spill split happens INSIDE store_result_body), so the persisted body
    still hashes to `sha`. The spilled full body is the verifier's ground truth."""
    big_text = "MD&A. " + ("Quarterly revenue grew across all segments. " * 4000)
    art = _sec_artifact(big_text)
    assert len(_canonical(art["filings"][0]).encode("utf-8")) > RESULT_BODY_MAX_BYTES

    req = _make_request("get_sec_filing", {"symbol": "AAPL"})
    _, store = await _run(_armed_middleware(), req, _result(artifact=art))

    sha, body, size, _ = _stored(store)[0]
    # Full body handed off (not a 64 KiB head) and still hash-consistent.
    assert len(body.encode("utf-8")) > RESULT_BODY_MAX_BYTES
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() == sha
    assert size == len(body.encode("utf-8"))


# ---------------------------------------------------------------------------
# Dedup-key stability: identical content -> identical sha -> one body row.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_static_doc_dedups_to_one_sha_across_turns():
    """The same static filing fetched in two separate turns yields the same sha, so
    ON CONFLICT collapses it to a single stored body (the whole point of the store)."""
    req1 = _make_request("get_sec_filing", {"symbol": "AAPL"}, tool_call_id="turn-1")
    req2 = _make_request("get_sec_filing", {"symbol": "AAPL"}, tool_call_id="turn-2")

    _, s1 = await _run(_armed_middleware(), req1, _result(artifact=_sec_artifact(_SEC_TEXT)))
    # Independently-constructed identical content (fresh dict, different turn).
    _, s2 = await _run(_armed_middleware(), req2, _result(artifact=_sec_artifact(_SEC_TEXT)))

    sha1, body1, _, _ = _stored(s1)[0]
    sha2, body2, _, _ = _stored(s2)[0]
    assert sha1 == sha2
    assert body1 == body2


@pytest.mark.asyncio
async def test_different_content_gets_different_sha():
    """Content that differs by one sentence must NOT collide on the dedup key."""
    art_a = _sec_artifact(_SEC_TEXT)
    art_b = _sec_artifact(_SEC_TEXT + " Subsequent event: a cash dividend was declared.")
    req = _make_request("get_sec_filing", {"symbol": "AAPL"})

    _, sa = await _run(_armed_middleware(), req, _result(artifact=art_a))
    _, sb = await _run(_armed_middleware(), req, _result(artifact=art_b))
    assert _stored(sa)[0][0] != _stored(sb)[0][0]


@pytest.mark.asyncio
async def test_dict_key_order_does_not_change_sha():
    """Canonicalization sorts keys, so the same snapshot with shuffled key order
    dedups to the same body — a real risk since upstream JSON ordering is unstable."""
    shuffled = dict(reversed(list(_MARKET_SNAPSHOT.items())))
    req = _make_request("get_daily_prices", {"symbol": "AAPL"})

    _, s1 = await _run(_armed_middleware(), req, _result(artifact=_MARKET_SNAPSHOT))
    _, s2 = await _run(_armed_middleware(), req, _result(artifact=shuffled))
    assert _stored(s1)[0][0] == _stored(s2)[0][0]


# ---------------------------------------------------------------------------
# Negative controls: a genuine leak IS redacted, so the body diverges from the
# raw-content sha. This is the ONLY case the safe-path invariant excludes — and
# clean content under the SAME armed redactor stays untouched (proven above).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sandbox_token_in_body_is_redacted_and_diverges():
    art = _sec_artifact(_SEC_TEXT + " internal token=gxsa_abc123DEF456ghi789")
    req = _make_request("get_sec_filing", {"symbol": "AAPL"})
    _, store = await _run(_armed_middleware(), req, _result(artifact=art))

    sha, body, _, _ = _stored(store)[0]
    assert "gxsa_abc123DEF456ghi789" not in body
    assert "[REDACTED:SANDBOX_TOKEN]" in body
    # By design: sha is over the RAW content (with the token); body is redacted.
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() != sha


@pytest.mark.asyncio
async def test_configured_secret_value_in_body_is_redacted_and_diverges():
    secret = _VAULT_SECRETS["FMP_API_KEY"]
    art = _sec_artifact(_SEC_TEXT + f" debug apikey={secret} end")
    req = _make_request("get_sec_filing", {"symbol": "AAPL"})
    _, store = await _run(_armed_middleware(), req, _result(artifact=art))

    sha, body, _, _ = _stored(store)[0]
    assert secret not in body
    assert "[REDACTED:FMP_API_KEY]" in body
    assert hashlib.sha256(body.encode("utf-8")).hexdigest() != sha
