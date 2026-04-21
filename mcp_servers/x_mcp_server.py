#!/usr/bin/env python3
"""X (Twitter) API MCP Server — read-only v2 endpoints for PTC-mode agents.

Auth: per-call Bearer Token. Primary source is the workspace vault; falls back
to the X_BEARER_TOKEN host env when explicitly passed through in agent_config.

Tools: search_posts, search_all_posts, get_user_by_username, get_tweet_by_id,
get_conversation. See skills/x-api/ for usage and error handling.
"""

from __future__ import annotations

import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

X_API_BASE = "https://api.x.com/2"
_HTTP_TIMEOUT = 30.0

# Per-endpoint constraints (X API v2).
# Recent Search:       query <=512 chars, page size 10-100.
# Full-archive Search: query <=1024 chars, page size 10-500. Paid-tier only.
_MIN_PAGE_SIZE = 10
_MAX_QUERY_LEN_RECENT = 512
_MAX_PAGE_SIZE_RECENT = 100
_MAX_QUERY_LEN_ALL = 1024
_MAX_PAGE_SIZE_ALL = 500

_TWEET_FIELDS = "created_at,public_metrics,lang,author_id,conversation_id"
# Only the four fields _enrich_post surfaces onto each post's `author` object —
# anything else would just pad the response. get_user_by_username asks for the
# fuller profile via _USER_LOOKUP_FIELDS.
_USER_FIELDS = "username,name,verified"
_EXPANSIONS = "author_id"
_USER_LOOKUP_FIELDS = "verified,public_metrics,description,created_at"

_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
_TWEET_ID_RE = re.compile(r"^\d{1,25}$")

_ERROR_DETAIL_KEYS = ("title", "type", "detail", "status", "reason")

_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def _lifespan(app):
    global _client
    _client = httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        headers={"User-Agent": "langalpha-x-mcp/1.0"},
    )
    try:
        yield
    finally:
        await _client.aclose()
        _client = None


mcp = FastMCP("XApiMCP", lifespan=_lifespan)


def _resolve_token(bearer_token: Optional[str]) -> Optional[str]:
    if bearer_token:
        return bearer_token
    env = os.getenv("X_BEARER_TOKEN")
    return env or None


def _missing_token_error() -> dict:
    return {
        "error": "missing_token",
        "detail": (
            "X bearer token required. Pass bearer_token from the workspace vault "
            '(from vault import get; token = get("X_BEARER_TOKEN")), or set '
            "X_BEARER_TOKEN in the host env for single-tenant deployments."
        ),
    }


def _index_users(includes: dict | None) -> dict[str, dict]:
    if not includes:
        return {}
    users = includes.get("users") or []
    return {u["id"]: u for u in users if isinstance(u, dict) and "id" in u}


def _enrich_post(post: dict, users_by_id: dict[str, dict]) -> dict:
    out = dict(post)
    author_id = post.get("author_id")
    if not author_id:
        out["author"] = None
        return out
    u = users_by_id.get(author_id)
    if u is None:
        out["author"] = {"id": author_id, "unresolved": True}
    else:
        out["author"] = {
            "id": u.get("id"),
            "username": u.get("username"),
            "name": u.get("name"),
            "verified": u.get("verified"),
        }
    return out


def _safe_body(resp: httpx.Response) -> Any:
    """Extract a safe error payload — whitelisted keys only, never raw text."""
    try:
        body = resp.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    safe = {k: body[k] for k in _ERROR_DETAIL_KEYS if k in body}
    errs = body.get("errors")
    if isinstance(errs, list):
        safe["errors"] = [
            {k: e.get(k) for k in _ERROR_DETAIL_KEYS if k in e}
            for e in errs
            if isinstance(e, dict)
        ]
    return safe or None


def _error_from_response(resp: httpx.Response) -> dict:
    if resp.status_code in (401, 403):
        return {
            "error": "auth_failed",
            "status": resp.status_code,
            "detail": _safe_body(resp),
        }
    if resp.status_code == 429:
        reset_raw = resp.headers.get("x-rate-limit-reset")
        reset_at_epoch: Optional[int] = None
        if reset_raw is not None:
            try:
                reset_at_epoch = int(reset_raw)
            except ValueError:
                reset_at_epoch = None
        retry_after: Optional[int] = None
        if reset_at_epoch is not None:
            retry_after = max(0, reset_at_epoch - int(time.time()))
        return {
            "error": "rate_limited",
            "reset_at_epoch": reset_at_epoch,
            "retry_after_seconds": retry_after,
        }
    return {
        "error": "http_error",
        "status": resp.status_code,
        "detail": _safe_body(resp),
    }


async def _get(path: str, params: dict[str, Any], token: str) -> httpx.Response | dict:
    """Authenticated GET against X_API_BASE + path. Callers are responsible for
    validating any interpolated path segments (see _USERNAME_RE, _TWEET_ID_RE);
    this helper does no URL escaping."""
    if _client is None:
        return {"error": "client_unavailable", "detail": "HTTP client not initialized"}
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{X_API_BASE}{path}"
    clean = {k: v for k, v in params.items() if v is not None}
    try:
        resp = await _client.get(url, headers=headers, params=clean)
    except httpx.HTTPError as exc:
        return {"error": "network_error", "detail": type(exc).__name__}
    return resp


def _parse_json_body(resp: httpx.Response) -> tuple[Any, dict | None]:
    """Parse the response JSON. Returns (payload, None) on success or
    (None, error_dict) on decode failure. Using a tuple instead of a sentinel
    dict avoids any collision with X response bodies that happen to carry an
    ``error`` key."""
    try:
        return resp.json(), None
    except ValueError:
        return None, {
            "error": "malformed_response",
            "detail": "X API returned a non-JSON 2xx body",
        }


async def _search(
    path: str,
    query: str,
    token: str,
    max_results: int,
    max_query_len: int,
    max_page_size: int,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    next_token: Optional[str] = None,
) -> dict:
    if len(query) > max_query_len:
        return {
            "error": "invalid_argument",
            "detail": f"query must be <= {max_query_len} chars (got {len(query)})",
        }
    if not _MIN_PAGE_SIZE <= max_results <= max_page_size:
        return {
            "error": "invalid_argument",
            "detail": f"max_results must be {_MIN_PAGE_SIZE}-{max_page_size}",
        }

    params: dict[str, Any] = {
        "query": query,
        "max_results": max_results,
        "tweet.fields": _TWEET_FIELDS,
        "expansions": _EXPANSIONS,
        "user.fields": _USER_FIELDS,
        "start_time": start_time,
        "end_time": end_time,
        "next_token": next_token,
    }
    resp_or_err = await _get(path, params, token)
    if isinstance(resp_or_err, dict):
        return resp_or_err
    resp = resp_or_err
    if resp.status_code != 200:
        return _error_from_response(resp)

    payload, err = _parse_json_body(resp)
    if err is not None:
        return err

    users_by_id = _index_users(payload.get("includes"))
    posts = [_enrich_post(p, users_by_id) for p in payload.get("data") or []]
    meta = payload.get("meta") or {}
    return {
        "posts": posts,
        "next_token": meta.get("next_token"),
        "result_count": meta.get("result_count", len(posts)),
    }


@mcp.tool()
async def search_posts(
    query: str,
    bearer_token: Optional[str] = None,
    max_results: int = 10,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    next_token: Optional[str] = None,
) -> dict:
    """Search recent X posts (last ~7 days) using X search syntax.

    Args:
        query: X search syntax. Example: '$TSLA -is:retweet lang:en'.
        bearer_token: Loaded from the sandbox vault:
            `from vault import get; get("X_BEARER_TOKEN")`.
        max_results: Posts per page. Default 10, max 100.
        start_time: ISO8601 inclusive lower bound (e.g. "2026-04-20T00:00:00Z").
        end_time: ISO8601 exclusive upper bound.
        next_token: Pagination cursor from a previous response.

    Returns:
        {"posts": [...], "next_token": str | None, "result_count": int},
        or {"error": ..., ...} on failure. See skills/x-api/ for post shape.
    """
    token = _resolve_token(bearer_token)
    if not token:
        return _missing_token_error()
    return await _search(
        path="/tweets/search/recent",
        query=query,
        token=token,
        max_results=max_results,
        max_query_len=_MAX_QUERY_LEN_RECENT,
        max_page_size=_MAX_PAGE_SIZE_RECENT,
        start_time=start_time,
        end_time=end_time,
        next_token=next_token,
    )


@mcp.tool()
async def search_all_posts(
    query: str,
    bearer_token: Optional[str] = None,
    max_results: int = 10,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    next_token: Optional[str] = None,
) -> dict:
    """Full-archive X post search, back to March 2006. Use when `search_posts`
    (recent 7-day window) doesn't reach far enough back.

    Args:
        query: X search syntax. Example: '$TSLA (earnings OR guidance)'.
        bearer_token: Loaded from the sandbox vault (`get("X_BEARER_TOKEN")`).
        max_results: Posts per page. Default 10, max 500.
        start_time: ISO8601 inclusive lower bound (back to 2006-03-26).
        end_time: ISO8601 exclusive upper bound.
        next_token: Pagination cursor from a previous response.

    Returns:
        Same shape as search_posts, or {"error": ..., ...} on failure.
    """
    token = _resolve_token(bearer_token)
    if not token:
        return _missing_token_error()
    return await _search(
        path="/tweets/search/all",
        query=query,
        token=token,
        max_results=max_results,
        max_query_len=_MAX_QUERY_LEN_ALL,
        max_page_size=_MAX_PAGE_SIZE_ALL,
        start_time=start_time,
        end_time=end_time,
        next_token=next_token,
    )


@mcp.tool()
async def get_user_by_username(
    username: str,
    bearer_token: Optional[str] = None,
) -> dict:
    """Look up an X user profile by handle (without @).

    Args:
        username: Handle without @ (e.g. "XDevelopers"). 1-15 chars, [A-Za-z0-9_].
        bearer_token: Loaded from the sandbox vault (`get("X_BEARER_TOKEN")`).

    Returns:
        {"user": {id, username, name, verified, description, created_at,
        public_metrics}}, or {"error": ...} on failure.
    """
    token = _resolve_token(bearer_token)
    if not token:
        return _missing_token_error()
    if not _USERNAME_RE.match(username or ""):
        return {
            "error": "invalid_argument",
            "detail": "username must match ^[A-Za-z0-9_]{1,15}$",
        }

    resp_or_err = await _get(
        f"/users/by/username/{username}",
        {"user.fields": _USER_LOOKUP_FIELDS},
        token,
    )
    if isinstance(resp_or_err, dict):
        return resp_or_err
    resp = resp_or_err
    if resp.status_code != 200:
        return _error_from_response(resp)

    payload, err = _parse_json_body(resp)
    if err is not None:
        return err
    data = payload.get("data")
    if not data:
        return {"error": "not_found", "detail": f"User '{username}' not found"}
    return {"user": data}


@mcp.tool()
async def get_tweet_by_id(
    tweet_id: str,
    bearer_token: Optional[str] = None,
) -> dict:
    """Fetch a single X post by its numeric id.

    Args:
        tweet_id: Numeric tweet id as a string (the trailing digits of an X URL).
        bearer_token: Loaded from the sandbox vault (`get("X_BEARER_TOKEN")`).

    Returns:
        {"post": {...}}, same shape as a search_posts item. Deleted or private
        tweets return {"error": "not_found"}.
    """
    token = _resolve_token(bearer_token)
    if not token:
        return _missing_token_error()
    if not _TWEET_ID_RE.match(tweet_id or ""):
        return {"error": "invalid_argument", "detail": "tweet_id must be numeric"}

    params = {
        "tweet.fields": _TWEET_FIELDS,
        "expansions": _EXPANSIONS,
        "user.fields": _USER_FIELDS,
    }
    resp_or_err = await _get(f"/tweets/{tweet_id}", params, token)
    if isinstance(resp_or_err, dict):
        return resp_or_err
    resp = resp_or_err
    if resp.status_code != 200:
        return _error_from_response(resp)

    payload, err = _parse_json_body(resp)
    if err is not None:
        return err
    data = payload.get("data")
    if not data:
        return {"error": "not_found", "detail": f"Tweet '{tweet_id}' not found"}
    users_by_id = _index_users(payload.get("includes"))
    return {"post": _enrich_post(data, users_by_id)}


@mcp.tool()
async def get_conversation(
    conversation_id: str,
    bearer_token: Optional[str] = None,
    max_results: int = 50,
    next_token: Optional[str] = None,
) -> dict:
    """Fetch replies to an X conversation thread (last ~7 days).

    Args:
        conversation_id: Often equals the root tweet id — find it via
            get_tweet_by_id on the root.
        bearer_token: Loaded from the sandbox vault (`get("X_BEARER_TOKEN")`).
        max_results: Posts per page. Default 50, max 100.
        next_token: Pagination cursor from a previous response.

    Returns:
        Same shape as search_posts. Root tweet is NOT included — fetch it
        separately with get_tweet_by_id if needed.
    """
    token = _resolve_token(bearer_token)
    if not token:
        return _missing_token_error()
    if not _TWEET_ID_RE.match(conversation_id or ""):
        return {
            "error": "invalid_argument",
            "detail": "conversation_id must be numeric",
        }
    return await _search(
        path="/tweets/search/recent",
        query=f"conversation_id:{conversation_id}",
        token=token,
        max_results=max_results,
        max_query_len=_MAX_QUERY_LEN_RECENT,
        max_page_size=_MAX_PAGE_SIZE_RECENT,
        next_token=next_token,
    )


if __name__ == "__main__":
    mcp.run()
