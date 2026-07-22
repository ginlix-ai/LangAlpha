"""Route-level tests for GET /api/v1/threads/{thread_id}/provenance.

Covers auth (404 unknown thread, 403 non-owner) and the per-turn grouping +
by_source_type summary shape. Mirrors the dependency-override + AsyncClient
pattern used by the other threads route tests. Neutral placeholder data only.
"""

import hashlib
from contextlib import ExitStack, asynccontextmanager, contextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

THREADS_MOD = "src.server.app.threads.provenance"

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RESPONSE_0 = "22222222-2222-2222-2222-222222222222"
RESPONSE_1 = "33333333-3333-3333-3333-333333333333"
OWNER_ID = "test-user-123"  # matches create_test_app's auth override


@pytest_asyncio.fixture
async def threads_client():
    from src.server.app.threads import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _row(turn_index, response_id, source_type, identifier, sha):
    return {
        "provenance_record_id": f"rec-{identifier}",
        "conversation_response_id": response_id,
        "conversation_thread_id": THREAD_ID,
        "turn_index": turn_index,
        "tool_call_id": "call-1",
        "source_type": source_type,
        "identifier": identifier,
        "title": "A title",
        "detail": "company_overview",
        "args_fingerprint": {"q": "test"},
        "args": {"symbol": "AAPL", "api_key": "[redacted]"},
        "result_sha256": sha,
        "result_size": 100,
        "result_snippet": "snippet",
        "agent": "main",
        "provider": "tavily",
        "created_at": datetime.now(timezone.utc),
    }


def _body_meta(body_inline, *, byte_len=None, object_key=None):
    return {
        "body_inline": body_inline,
        "object_key": object_key,
        "byte_len": byte_len if byte_len is not None else len(body_inline.encode()),
        "content_type": "text/plain; charset=utf-8",
    }


@asynccontextmanager
async def _fake_db_conn():
    yield AsyncMock()


@contextmanager
def _patch_body_store(owner, rows, bodies, *, full_body=None):
    """Patch owner + record lookups + the body-store reads the verifier endpoints use.

    Patches both the list read (``get_provenance_body_refs``, used by ``/bodies``,
    which the endpoint filters + caps in SQL) and the targeted single-record read
    (``get_provenance_record``, used by ``/{id}/body``); the latter matches ``rows``
    by record id so unknown ids 404.
    """

    async def _get_record(thread_id, record_id):
        return next(
            (r for r in rows if str(r["provenance_record_id"]) == str(record_id)),
            None,
        )

    async def _get_body_refs(conn, thread_id, limit):
        # Mirror the SQL: eligible rows in order, capped at limit + 1.
        eligible = [r for r in rows if r.get("result_sha256")]
        return eligible[: limit + 1]

    patches = [
        patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=owner),
        ),
        patch(
            f"{THREADS_MOD}.get_provenance_body_refs",
            new=AsyncMock(side_effect=_get_body_refs),
        ),
        patch(
            f"{THREADS_MOD}.get_provenance_record",
            new=AsyncMock(side_effect=_get_record),
        ),
        patch(
            "src.server.database.pool.get_db_connection",
            new=_fake_db_conn,
        ),
        patch(
            "src.server.database.provenance_bodies.fetch_result_bodies",
            new=AsyncMock(return_value=bodies),
        ),
        patch(
            "src.server.database.provenance_bodies.fetch_full_body",
            new=AsyncMock(return_value=full_body),
        ),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


class TestGetProvenanceBodies:
    @pytest.mark.asyncio
    async def test_verified_true_when_body_hashes_to_sha(self, threads_client):
        body = "the exact bytes the agent reasoned over"
        sha = hashlib.sha256(body.encode()).hexdigest()
        rows = [_row(0, RESPONSE_0, "web_fetch", "https://x.test/a", sha)]
        bodies = {sha: _body_meta(body)}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        assert resp.status_code == 200
        rec = resp.json()["records"][0]
        assert rec["result_sha256"] == sha
        assert rec["truncated"] is False
        assert rec["verified"] is True

    @pytest.mark.asyncio
    async def test_verified_false_when_body_redacted(self, threads_client):
        # Stored body was redacted, so it no longer hashes to the raw-content sha:
        # present + not truncated + verified=False is the redaction signal.
        raw_sha = hashlib.sha256(b"raw body with a secret").hexdigest()
        redacted = "raw body with [REDACTED]"
        rows = [_row(0, RESPONSE_0, "web_fetch", "https://x.test/a", raw_sha)]
        bodies = {raw_sha: _body_meta(redacted)}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        rec = resp.json()["records"][0]
        assert rec["truncated"] is False
        assert rec["verified"] is False

    @pytest.mark.asyncio
    async def test_redacted_inline_body_is_not_truncated(self, threads_client):
        # A body that redaction shrank stays COMPLETE. `byte_len` tracks the stored
        # (post-redaction) length, `body_inline` holds the whole redacted body, and
        # there's no spilled object — so `truncated` is False even when the original
        # was larger (this is the case that mislabeled when byte_len was the raw
        # pre-redaction size: a body redacted from over the inline cap down to under
        # it is whole, not a head). The "not the raw bytes" signal is `verified=false`.
        raw_body = b"api_key=sk-supersecretvalue-0123456789abcdef"
        raw_sha = hashlib.sha256(raw_body).hexdigest()
        redacted = "api_key=[REDACTED:API_KEY]"
        assert len(redacted.encode()) < len(raw_body)  # redaction shortened it
        rows = [_row(0, RESPONSE_0, "web_fetch", "https://x.test/a", raw_sha)]
        # byte_len = stored (redacted) length, as _body_item records it post-redaction.
        bodies = {raw_sha: _body_meta(redacted)}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        rec = resp.json()["records"][0]
        assert rec["body_inline"] == redacted  # complete redacted body, not a head
        assert rec["truncated"] is False
        assert rec["verified"] is False

    @pytest.mark.asyncio
    async def test_no_bucket_head_is_truncated(self, threads_client):
        # Over-cap body with no object store configured → only the inline head was
        # kept (object_key None). The stored byte_len exceeds the inline slice, so
        # truncated flips True without a spilled object — the head is incomplete.
        head = "h" * 200
        sha = "b" * 64
        rows = [_row(0, RESPONSE_0, "web_fetch", "https://x.test/a", sha)]
        bodies = {sha: _body_meta(head, byte_len=500_000, object_key=None)}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        rec = resp.json()["records"][0]
        assert rec["truncated"] is True
        assert rec["verified"] is False

    @pytest.mark.asyncio
    async def test_truncated_head_is_not_verified(self, threads_client):
        head = "h" * 100
        sha = "a" * 64
        rows = [_row(0, RESPONSE_0, "web_fetch", "https://x.test/a", sha)]
        # byte_len far exceeds the inline head → truncated, can't verify the head.
        bodies = {sha: _body_meta(head, byte_len=500_000, object_key="provenance/x")}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        rec = resp.json()["records"][0]
        assert rec["truncated"] is True
        assert rec["verified"] is False

    @pytest.mark.asyncio
    async def test_capped_when_more_than_limit(self, threads_client):
        rows = [
            _row(0, RESPONSE_0, "web_fetch", f"https://x.test/{i}", f"{i:064d}")
            for i in range(3)
        ]
        bodies = {f"{i:064d}": _body_meta(f"body {i}") for i in range(3)}
        with _patch_body_store(OWNER_ID, rows, bodies):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies?limit=1"
            )
        body = resp.json()
        assert body["capped"] is True
        assert len(body["records"]) == 1

    @pytest.mark.asyncio
    async def test_non_owner_returns_403(self, threads_client):
        with _patch_body_store("someone-else", [], {}):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/bodies"
            )
        assert resp.status_code == 403


class TestGetProvenanceRecordBody:
    @pytest.mark.asyncio
    async def test_full_body_verified(self, threads_client):
        body = "full body content the agent saw"
        sha = hashlib.sha256(body.encode()).hexdigest()
        rows = [_row(0, RESPONSE_0, "web_fetch", "doc", sha)]
        bodies = {sha: _body_meta(body)}
        with _patch_body_store(OWNER_ID, rows, bodies, full_body=body):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/rec-doc/body?full=true"
            )
        assert resp.status_code == 200
        out = resp.json()
        assert out["body"] == body
        assert out["verified"] is True

    @pytest.mark.asyncio
    async def test_unknown_record_returns_404(self, threads_client):
        rows = [_row(0, RESPONSE_0, "web_fetch", "doc", "a" * 64)]
        with _patch_body_store(OWNER_ID, rows, {}):
            resp = await threads_client.get(
                f"/api/v1/threads/{THREAD_ID}/provenance/rec-MISSING/body"
            )
        assert resp.status_code == 404


class TestGetProvenanceAuth:
    @pytest.mark.asyncio
    async def test_unknown_thread_returns_404(self, threads_client):
        with patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value=None),
        ):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/provenance")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_non_owner_returns_403(self, threads_client):
        with patch(
            "src.server.database.conversation.get_thread_owner_id",
            new=AsyncMock(return_value="someone-else"),
        ):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/provenance")
        assert resp.status_code == 403


class TestGetProvenanceShape:
    @pytest.mark.asyncio
    async def test_groups_by_turn_with_source_type_counts(self, threads_client):
        rows = [
            _row(0, RESPONSE_0, "web_search", "https://example.test/a", "s1"),
            _row(0, RESPONSE_0, "web_search", "https://example.test/b", "s2"),
            _row(1, RESPONSE_1, "mcp_tool", "server:get_prices", "s3"),
            _row(1, RESPONSE_1, "web_search", "https://example.test/c", "s4"),
        ]
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                f"{THREADS_MOD}.get_provenance_for_thread",
                new=AsyncMock(return_value=rows),
            ),
        ):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/provenance")

        assert resp.status_code == 200
        body = resp.json()
        assert body["thread_id"] == THREAD_ID

        turns = body["turns"]
        assert [t["turn_index"] for t in turns] == [0, 1]
        assert turns[0]["conversation_response_id"] == RESPONSE_0
        assert len(turns[0]["sources"]) == 2
        assert turns[1]["conversation_response_id"] == RESPONSE_1
        assert len(turns[1]["sources"]) == 2

        assert body["by_source_type"] == {"web_search": 3, "mcp_tool": 1}

    @pytest.mark.asyncio
    async def test_empty_provenance(self, threads_client):
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                f"{THREADS_MOD}.get_provenance_for_thread",
                new=AsyncMock(return_value=[]),
            ),
        ):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/provenance")
        assert resp.status_code == 200
        body = resp.json()
        assert body["turns"] == []
        assert body["by_source_type"] == {}

    @pytest.mark.asyncio
    async def test_source_uses_record_id_key_and_iso_timestamp(self, threads_client):
        # The response renames the DB's provenance_record_id to record_id (to match
        # the SSE/replay record field) and exposes source_timestamp as ISO-8601.
        ts = datetime(2026, 6, 14, 12, 0, tzinfo=timezone.utc)
        row = _row(0, RESPONSE_0, "web_search", "https://example.test/a", "s1")
        row["source_timestamp"] = ts
        with (
            patch(
                "src.server.database.conversation.get_thread_owner_id",
                new=AsyncMock(return_value=OWNER_ID),
            ),
            patch(
                f"{THREADS_MOD}.get_provenance_for_thread",
                new=AsyncMock(return_value=[row]),
            ),
        ):
            resp = await threads_client.get(f"/api/v1/threads/{THREAD_ID}/provenance")
        assert resp.status_code == 200
        source = resp.json()["turns"][0]["sources"][0]
        assert source["record_id"] == "rec-https://example.test/a"
        assert "provenance_record_id" not in source  # renamed, not duplicated
        assert source["timestamp"] == ts.isoformat()
        # detail (the data-kind slug) is passed through for the verification agent.
        assert source["detail"] == "company_overview"
        # Readable redacted args are passed through to the REST shape.
        assert source["args"] == {"symbol": "AAPL", "api_key": "[redacted]"}
