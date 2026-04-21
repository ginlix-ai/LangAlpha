"""Tests for x_mcp_server: search_posts, get_user_by_username, get_tweet_by_id, get_conversation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from mcp_servers import x_mcp_server as xm

_MOD = "mcp_servers.x_mcp_server"


def _mock_response(
    status_code: int = 200,
    json_body: dict | list | None = None,
    headers: dict | None = None,
    malformed: bool = False,
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    if malformed:
        resp.json = MagicMock(side_effect=ValueError("not json"))
    else:
        resp.json = MagicMock(return_value=json_body if json_body is not None else {})
    resp.text = "" if json_body else "body"
    return resp


@pytest.fixture
def patched_client(monkeypatch):
    """Patch the module-level _client with a fresh AsyncMock each test.

    Returns the mock client so tests can configure `.get.return_value` etc.
    """
    mock_client = AsyncMock()
    monkeypatch.setattr(xm, "_client", mock_client)
    return mock_client


_SEARCH_PAYLOAD = {
    "data": [
        {
            "id": "1700000000000000001",
            "text": "TSLA ripping after earnings",
            "created_at": "2026-04-20T12:00:00.000Z",
            "lang": "en",
            "author_id": "U1",
            "conversation_id": "1700000000000000001",
            "public_metrics": {
                "retweet_count": 10,
                "reply_count": 2,
                "like_count": 50,
                "quote_count": 1,
                "impression_count": 9000,
            },
        },
        {
            "id": "1700000000000000002",
            "text": "skeptical",
            "created_at": "2026-04-20T12:05:00.000Z",
            "lang": "en",
            "author_id": "U_DELETED",
            "conversation_id": "1700000000000000002",
            "public_metrics": {"like_count": 3},
        },
    ],
    "includes": {
        "users": [
            {"id": "U1", "username": "alice", "name": "Alice", "verified": True},
        ]
    },
    "meta": {"result_count": 2, "next_token": "abc123"},
}


class TestSearchPosts:
    @pytest.mark.asyncio
    async def test_success_normalizes_and_inlines_author(self, patched_client):
        patched_client.get.return_value = _mock_response(200, _SEARCH_PAYLOAD)
        out = await xm.search_posts(query="$TSLA", bearer_token="tok", max_results=10)

        assert out["result_count"] == 2
        assert out["next_token"] == "abc123"
        assert len(out["posts"]) == 2
        assert out["posts"][0]["author"] == {
            "id": "U1", "username": "alice", "name": "Alice", "verified": True
        }
        # Unresolved author (deleted/suspended user): still shaped
        assert out["posts"][1]["author"] == {"id": "U_DELETED", "unresolved": True}

    @pytest.mark.asyncio
    async def test_missing_token(self, monkeypatch):
        monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
        out = await xm.search_posts(query="$TSLA", bearer_token=None)
        assert out["error"] == "missing_token"
        assert "vault" in out["detail"].lower()

    @pytest.mark.asyncio
    async def test_env_fallback(self, monkeypatch, patched_client):
        monkeypatch.setenv("X_BEARER_TOKEN", "envtok")
        patched_client.get.return_value = _mock_response(200, _SEARCH_PAYLOAD)
        out = await xm.search_posts(query="$TSLA", bearer_token=None)
        assert "posts" in out
        called_headers = patched_client.get.call_args.kwargs["headers"]
        assert called_headers["Authorization"] == "Bearer envtok"

    @pytest.mark.asyncio
    async def test_invalid_max_results(self):
        out = await xm.search_posts(query="$TSLA", bearer_token="tok", max_results=5)
        assert out == {"error": "invalid_argument", "detail": "max_results must be 10-100"}

    @pytest.mark.asyncio
    async def test_query_too_long(self):
        q = "x" * 600
        out = await xm.search_posts(query=q, bearer_token="tok")
        assert out["error"] == "invalid_argument"
        assert "512" in out["detail"]

    @pytest.mark.asyncio
    async def test_rate_limited(self, patched_client, monkeypatch):
        # Freeze time so retry_after_seconds is deterministic.
        monkeypatch.setattr(xm.time, "time", lambda: 1777777700)
        patched_client.get.return_value = _mock_response(
            429, headers={"x-rate-limit-reset": "1777777760"}
        )
        out = await xm.search_posts(query="$TSLA", bearer_token="tok")
        assert out["error"] == "rate_limited"
        assert out["reset_at_epoch"] == 1777777760
        assert out["retry_after_seconds"] == 60

    @pytest.mark.asyncio
    async def test_rate_limited_missing_header(self, patched_client):
        patched_client.get.return_value = _mock_response(429)
        out = await xm.search_posts(query="$TSLA", bearer_token="tok")
        assert out == {
            "error": "rate_limited",
            "reset_at_epoch": None,
            "retry_after_seconds": None,
        }

    @pytest.mark.asyncio
    async def test_auth_failed_whitelists_body(self, patched_client):
        patched_client.get.return_value = _mock_response(
            401,
            json_body={
                "title": "Unauthorized",
                "type": "about:blank",
                "secret": "should not appear",
            },
        )
        out = await xm.search_posts(query="$TSLA", bearer_token="bad")
        assert out["error"] == "auth_failed"
        assert out["status"] == 401
        assert out["detail"] == {"title": "Unauthorized", "type": "about:blank"}
        assert "secret" not in str(out["detail"])

    @pytest.mark.asyncio
    async def test_http_error_sanitizes_body(self, patched_client):
        patched_client.get.return_value = _mock_response(
            400,
            json_body={
                "errors": [
                    {"title": "Invalid query", "detail": "bad operator", "other": "x"}
                ],
                "echo": {"Authorization": "Bearer MUST-NOT-LEAK"},
            },
        )
        out = await xm.search_posts(query="$TSLA", bearer_token="tok")
        assert out["error"] == "http_error"
        assert out["status"] == 400
        assert "MUST-NOT-LEAK" not in str(out["detail"])
        assert out["detail"]["errors"][0]["title"] == "Invalid query"
        assert "other" not in out["detail"]["errors"][0]

    @pytest.mark.asyncio
    async def test_network_error_no_leak(self, patched_client):
        patched_client.get.side_effect = httpx.ConnectError(
            "boom at https://api.x.com/2/... with Bearer TOKEN"
        )
        out = await xm.search_posts(query="$TSLA", bearer_token="tok")
        assert out == {"error": "network_error", "detail": "ConnectError"}

    @pytest.mark.asyncio
    async def test_malformed_response(self, patched_client):
        patched_client.get.return_value = _mock_response(200, malformed=True)
        out = await xm.search_posts(query="$TSLA", bearer_token="tok")
        assert out["error"] == "malformed_response"

    @pytest.mark.asyncio
    async def test_passes_query_params(self, patched_client):
        patched_client.get.return_value = _mock_response(
            200, {"data": [], "meta": {"result_count": 0}}
        )
        await xm.search_posts(
            query="$TSLA",
            bearer_token="tok",
            max_results=50,
            start_time="2026-04-20T00:00:00Z",
            end_time="2026-04-20T23:59:59Z",
            next_token="cursor",
        )
        params = patched_client.get.call_args.kwargs["params"]
        assert params["query"] == "$TSLA"
        assert params["max_results"] == 50
        assert params["start_time"] == "2026-04-20T00:00:00Z"
        assert params["end_time"] == "2026-04-20T23:59:59Z"
        assert params["next_token"] == "cursor"
        assert "tweet.fields" in params


class TestSearchAllPosts:
    @pytest.mark.asyncio
    async def test_hits_full_archive_endpoint(self, patched_client):
        patched_client.get.return_value = _mock_response(200, _SEARCH_PAYLOAD)
        out = await xm.search_all_posts(query="$TSLA", bearer_token="tok")
        # Must hit /tweets/search/all, NOT /tweets/search/recent
        url = patched_client.get.call_args.args[0]
        assert "/tweets/search/all" in url
        assert "/tweets/search/recent" not in url
        assert out["result_count"] == 2

    @pytest.mark.asyncio
    async def test_allows_max_results_up_to_500(self, patched_client):
        patched_client.get.return_value = _mock_response(
            200, {"data": [], "meta": {"result_count": 0}}
        )
        out = await xm.search_all_posts(query="$TSLA", bearer_token="tok", max_results=500)
        assert "error" not in out
        assert patched_client.get.call_args.kwargs["params"]["max_results"] == 500

    @pytest.mark.asyncio
    async def test_rejects_max_results_over_500(self):
        out = await xm.search_all_posts(query="$TSLA", bearer_token="tok", max_results=501)
        assert out == {"error": "invalid_argument", "detail": "max_results must be 10-500"}

    @pytest.mark.asyncio
    async def test_rejects_max_results_below_10(self):
        out = await xm.search_all_posts(query="$TSLA", bearer_token="tok", max_results=5)
        assert out == {"error": "invalid_argument", "detail": "max_results must be 10-500"}

    @pytest.mark.asyncio
    async def test_allows_query_up_to_1024_chars(self, patched_client):
        patched_client.get.return_value = _mock_response(
            200, {"data": [], "meta": {"result_count": 0}}
        )
        q = "x" * 1024
        out = await xm.search_all_posts(query=q, bearer_token="tok")
        assert "error" not in out

    @pytest.mark.asyncio
    async def test_rejects_query_over_1024_chars(self):
        q = "x" * 1025
        out = await xm.search_all_posts(query=q, bearer_token="tok")
        assert out["error"] == "invalid_argument"
        assert "1024" in out["detail"]

    @pytest.mark.asyncio
    async def test_missing_token(self, monkeypatch):
        monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
        out = await xm.search_all_posts(query="$TSLA", bearer_token=None)
        assert out["error"] == "missing_token"

    @pytest.mark.asyncio
    async def test_paid_tier_required_returns_auth_failed(self, patched_client):
        # Free-tier tokens get 403 from /tweets/search/all.
        patched_client.get.return_value = _mock_response(
            403, json_body={"title": "Forbidden", "type": "about:blank"}
        )
        out = await xm.search_all_posts(query="$TSLA", bearer_token="free-tok")
        assert out["error"] == "auth_failed"
        assert out["status"] == 403


class TestGetUserByUsername:
    @pytest.mark.asyncio
    async def test_success(self, patched_client):
        payload = {
            "data": {
                "id": "U1",
                "username": "alice",
                "name": "Alice",
                "verified": True,
                "description": "hi",
                "created_at": "2010-01-01T00:00:00Z",
                "public_metrics": {"followers_count": 100},
            }
        }
        patched_client.get.return_value = _mock_response(200, payload)
        out = await xm.get_user_by_username(username="alice", bearer_token="tok")
        assert out == {"user": payload["data"]}
        assert "users/by/username/alice" in patched_client.get.call_args.args[0]

    @pytest.mark.asyncio
    async def test_not_found(self, patched_client):
        patched_client.get.return_value = _mock_response(
            200, {"errors": [{"title": "Not Found"}]}
        )
        out = await xm.get_user_by_username(username="nouser", bearer_token="tok")
        assert out == {"error": "not_found", "detail": "User 'nouser' not found"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad",
        ["", "has space", "toolongusername_123", "bad/slash", "../traversal"],
    )
    async def test_invalid_username(self, bad):
        out = await xm.get_user_by_username(username=bad, bearer_token="tok")
        assert out["error"] == "invalid_argument"


class TestGetTweetById:
    @pytest.mark.asyncio
    async def test_success_inlines_author(self, patched_client):
        payload = {
            "data": {
                "id": "1700000000000000001",
                "text": "hi",
                "author_id": "U1",
                "conversation_id": "1700000000000000001",
                "created_at": "2026-04-20T00:00:00Z",
                "lang": "en",
                "public_metrics": {"like_count": 10},
            },
            "includes": {
                "users": [
                    {"id": "U1", "username": "alice", "name": "Alice", "verified": False}
                ]
            },
        }
        patched_client.get.return_value = _mock_response(200, payload)
        out = await xm.get_tweet_by_id(
            tweet_id="1700000000000000001", bearer_token="tok"
        )
        assert out["post"]["id"] == "1700000000000000001"
        assert out["post"]["author"]["username"] == "alice"

    @pytest.mark.asyncio
    async def test_not_found(self, patched_client):
        patched_client.get.return_value = _mock_response(200, {})
        out = await xm.get_tweet_by_id(tweet_id="1700000000000000404", bearer_token="tok")
        assert out["error"] == "not_found"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad", ["", "abc", "12/34", "../x", "1" * 30])
    async def test_invalid_tweet_id(self, bad):
        out = await xm.get_tweet_by_id(tweet_id=bad, bearer_token="tok")
        assert out["error"] == "invalid_argument"


class TestGetConversation:
    @pytest.mark.asyncio
    async def test_delegates_with_conversation_query(self, patched_client):
        patched_client.get.return_value = _mock_response(
            200, {"data": [], "meta": {"result_count": 0}}
        )
        await xm.get_conversation(
            conversation_id="1700000000000000001", bearer_token="tok", max_results=25
        )
        params = patched_client.get.call_args.kwargs["params"]
        assert params["query"] == "conversation_id:1700000000000000001"
        assert params["max_results"] == 25

    @pytest.mark.asyncio
    async def test_invalid_max_results(self):
        out = await xm.get_conversation(
            conversation_id="1700000000000000001",
            bearer_token="tok",
            max_results=500,
        )
        assert out == {"error": "invalid_argument", "detail": "max_results must be 10-100"}

    @pytest.mark.asyncio
    async def test_invalid_conversation_id(self):
        out = await xm.get_conversation(
            conversation_id="not-numeric", bearer_token="tok"
        )
        assert out["error"] == "invalid_argument"


class TestHelpers:
    def test_index_users_handles_malformed(self):
        out = xm._index_users({"users": [{"id": "U1"}, "junk", None, {"no_id": True}]})
        assert out == {"U1": {"id": "U1"}}

    def test_index_users_none(self):
        assert xm._index_users(None) == {}
        assert xm._index_users({}) == {}

    def test_enrich_post_no_author_id(self):
        out = xm._enrich_post({"id": "T", "text": "x"}, {})
        assert out["author"] is None

    def test_resolve_token_empty_string(self, monkeypatch):
        monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
        assert xm._resolve_token("") is None
        assert xm._resolve_token(None) is None

    def test_resolve_token_env_only(self, monkeypatch):
        monkeypatch.setenv("X_BEARER_TOKEN", "env-tok")
        assert xm._resolve_token(None) == "env-tok"
        assert xm._resolve_token("explicit") == "explicit"
