"""Unit tests for the service-token auth resolvers in server/utils/api.py.

Covers ``_service_token_user_id`` (the shared header parser) and
``get_stamp_auth`` (the stamp route's ``Optional[str]`` resolver) at the
function level — the header-matching, fall-through, and delegation branches
that the route-level tests in ``test_threads_stamp.py`` don't exercise directly.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.utils import api


def _req(headers=None):
    """Duck-typed request: ``_service_token_user_id`` only calls headers.get()."""
    return SimpleNamespace(headers=headers or {})


# ---------------------------------------------------------------------------
# _service_token_user_id — the shared (matched, user_id) parser
# ---------------------------------------------------------------------------


def test_no_service_token_configured_falls_through():
    """No INTERNAL_SERVICE_TOKEN set → service auth off → (False, None),
    regardless of any headers the caller sends."""
    with patch.object(api, "_SERVICE_TOKEN", ""):
        matched, user_id = api._service_token_user_id(
            _req({"X-Service-Token": "anything", "X-User-Id": "alice"})
        )
    assert matched is False
    assert user_id is None


def test_service_token_configured_but_header_absent_falls_through():
    """Token configured but request omits X-Service-Token → (False, None),
    so the caller falls through to the normal auth path (not a 401)."""
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        matched, user_id = api._service_token_user_id(_req({"X-User-Id": "alice"}))
    assert matched is False
    assert user_id is None


def test_valid_token_with_user_id_matches():
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        matched, user_id = api._service_token_user_id(
            _req({"X-Service-Token": "svc-secret", "X-User-Id": "alice"})
        )
    assert matched is True
    assert user_id == "alice"


def test_valid_token_without_user_id_matches_none():
    """Token-only privileged caller: matched True, user_id None."""
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        matched, user_id = api._service_token_user_id(
            _req({"X-Service-Token": "svc-secret"})
        )
    assert matched is True
    assert user_id is None


def test_wrong_token_raises_401():
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        with pytest.raises(HTTPException) as exc_info:
            api._service_token_user_id(_req({"X-Service-Token": "wrong"}))
    assert exc_info.value.status_code == 401


def test_non_ascii_token_raises_401_not_500():
    """A non-ASCII X-Service-Token returns 401, not a TypeError→500.

    hmac.compare_digest rejects non-ASCII str, and header values arrive
    latin-1-decoded, so the comparison is done on bytes.
    """
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        with pytest.raises(HTTPException) as exc_info:
            api._service_token_user_id(_req({"X-Service-Token": "å-not-ascii"}))
    assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# get_stamp_auth — Optional[str] resolver for the stamp route
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_stamp_auth_service_caller_returns_user_id_verbatim():
    """A matched service call returns the (possibly None) X-User-Id without
    delegating to get_current_user_id."""
    with patch.object(api, "_SERVICE_TOKEN", "svc-secret"):
        with patch.object(
            api, "get_current_user_id", new=AsyncMock()
        ) as delegate:
            result = await api.get_stamp_auth(
                _req({"X-Service-Token": "svc-secret"}), None
            )
    assert result is None
    delegate.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_stamp_auth_non_service_caller_delegates():
    """Not a service call → delegate to get_current_user_id and pass its result
    straight through."""
    with patch.object(api, "_SERVICE_TOKEN", ""):
        with patch.object(
            api, "get_current_user_id", new=AsyncMock(return_value="delegated-user")
        ) as delegate:
            result = await api.get_stamp_auth(_req(), None)
    assert result == "delegated-user"
    delegate.assert_awaited_once()


# ---------------------------------------------------------------------------
# service_token_matches — the pure constant-time compare primitive
# (shared with threads.py's dispatch preflight)
# ---------------------------------------------------------------------------


def test_service_token_matches_true_on_exact_match():
    assert api.service_token_matches("svc-secret", "svc-secret") is True


def test_service_token_matches_false_on_mismatch():
    assert api.service_token_matches("wrong", "svc-secret") is False


def test_service_token_matches_false_when_secret_unset():
    """No configured secret → never a match, even for an empty candidate."""
    assert api.service_token_matches("svc-secret", "") is False
    assert api.service_token_matches("", "") is False


def test_service_token_matches_false_when_candidate_empty():
    assert api.service_token_matches("", "svc-secret") is False


def test_service_token_matches_non_ascii_returns_false_not_typeerror():
    """A non-ASCII candidate must compare False, never raise TypeError — the bug
    that made both auth paths 500 on a latin-1 header byte."""
    assert api.service_token_matches("å-not-ascii", "svc-secret") is False
