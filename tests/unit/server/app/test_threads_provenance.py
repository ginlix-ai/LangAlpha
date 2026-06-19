"""Route-level tests for GET /api/v1/threads/{thread_id}/provenance.

Covers auth (404 unknown thread, 403 non-owner) and the per-turn grouping +
by_source_type summary shape. Mirrors the dependency-override + AsyncClient
pattern used by the other threads route tests. Neutral placeholder data only.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

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
                "src.server.app.threads.get_provenance_for_thread",
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
                "src.server.app.threads.get_provenance_for_thread",
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
                "src.server.app.threads.get_provenance_for_thread",
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
