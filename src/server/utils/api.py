"""
API utilities for FastAPI routers.

Provides common patterns for exception handling and authentication.
"""

import functools
import hmac
import inspect
import logging
import os
import re
from typing import Annotated, Callable, Optional, TypeVar
from urllib.parse import parse_qs, unquote

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config.settings import HOST_MODE, LOCAL_DEV_USER_ID
from src.server.auth.jwt_bearer import _decode_token

# Type variable for generic return type preservation
T = TypeVar("T")

_optional_bearer = HTTPBearer(auto_error=False)
_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")


async def get_current_user_id(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_optional_bearer),
) -> str:
    """
    FastAPI dependency to extract user ID.

    When Supabase auth is disabled (``SUPABASE_URL`` unset), returns the
    configured local user ID (``AUTH_USER_ID`` env var, default ``local-dev-user``).

    When auth is enabled, requires a valid Bearer JWT (Supabase).
    """
    # Service-to-service auth (only active if INTERNAL_SERVICE_TOKEN is set)
    if _SERVICE_TOKEN:
        token = request.headers.get("X-Service-Token")
        if token:
            if not hmac.compare_digest(token, _SERVICE_TOKEN):
                raise HTTPException(status_code=401, detail="Invalid service token")
            user_id = request.headers.get("X-User-Id")
            if not user_id:
                raise HTTPException(status_code=401, detail="Missing X-User-Id")
            return user_id

    if HOST_MODE == "oss":
        return LOCAL_DEV_USER_ID

    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing authentication")

    return _decode_token(credentials.credentials).user_id


# Annotated type for cleaner endpoint signatures
CurrentUserId = Annotated[str, Depends(get_current_user_id)]


def handle_api_exceptions(
    action: str,
    logger: logging.Logger,
    *,
    conflict_on_value_error: bool = False,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to handle common API exception patterns.

    Catches exceptions and converts them to appropriate HTTP responses:
    - HTTPException: Re-raised as-is
    - ValueError: 409 Conflict (if conflict_on_value_error=True) or re-raised
    - Exception: Logged and converted to 500 Internal Server Error

    Args:
        action: Description of the action for error messages (e.g., "create user")
        logger: Logger instance for exception logging
        conflict_on_value_error: If True, ValueError becomes 409 Conflict

    Usage:
        @router.post("/users")
        @handle_api_exceptions("create user", logger, conflict_on_value_error=True)
        async def create_user(...):
            ...
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            try:
                return await func(*args, **kwargs)
            except HTTPException:
                raise
            except ValueError as e:
                if conflict_on_value_error:
                    raise HTTPException(status_code=409, detail=str(e))
                raise
            except Exception as e:
                logger.exception(f"Error {action}: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to {action}",
                )

        # Preserve function signature for FastAPI dependency injection
        wrapper.__signature__ = inspect.signature(func)
        return wrapper

    return decorator


async def require_thread_owner(thread_id: str, user_id: str) -> None:
    """Verify the user owns the thread (via workspace). Raises 404 or 403."""
    from src.server.database.conversation import get_thread_owner_id

    owner_id = await get_thread_owner_id(thread_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    if owner_id != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")


def require_workspace_owner(workspace: dict | None, *, user_id: str) -> None:
    """Verify workspace exists and belongs to user. Raises 404 or 403."""
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if workspace.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")


def raise_not_found(resource: str, resource_id: Optional[str] = None) -> None:
    """
    Raise a 404 Not Found HTTPException.

    Args:
        resource: Name of the resource (e.g., "User", "Portfolio holding")
        resource_id: Optional ID to include in the message

    Raises:
        HTTPException: 404 Not Found
    """
    detail = f"{resource} not found"
    raise HTTPException(status_code=404, detail=detail)


# TEMP diagnostic (malformed-id-diag): a file/dir name from the SPA file tree sometimes
# lands in a workspace_id or thread_id slot (e.g. /workspaces/<file>.md,
# /threads/results), which the backend now short-circuits to a clean 404. This
# pure helper lets MalformedIdDiagnosticMiddleware log such ids + Referer so the
# next real prod occurrence names the SPA route. Remove with the middleware once
# the frontend writer is identified.
_ROUTE_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
# Literal (non-UUID) path segments that are valid endpoints, not ids.
_ROUTE_ID_ALLOWLIST = frozenset({"messages", "flash", "reorder"})
_WS_PATH_RE = re.compile(r"^/api/v1/workspaces/([^/]+)")
_THREAD_PATH_RE = re.compile(r"^/api/v1/threads/([^/]+)")


def find_malformed_route_ids(
    path: str, query_string: bytes = b""
) -> list[tuple[str, str]]:
    """Return (slot, value) pairs where a workspace/thread id isn't a UUID.

    Inspects the workspaces/threads path segment and the ``workspace_id`` query
    param; allowlisted literal segments (messages/flash/reorder) are ignored.
    """
    findings: list[tuple[str, str]] = []

    def _flag(slot: str, value: str) -> None:
        if (
            value
            and value not in _ROUTE_ID_ALLOWLIST
            and not _ROUTE_UUID_RE.match(value)
        ):
            findings.append((slot, value))

    ws = _WS_PATH_RE.match(path)
    if ws:
        _flag("workspace_path_id", unquote(ws.group(1)))
    th = _THREAD_PATH_RE.match(path)
    if th:
        _flag("thread_path_id", unquote(th.group(1)))
    if query_string:
        try:
            params = parse_qs(query_string.decode("latin-1"))
        except (UnicodeDecodeError, ValueError):
            params = {}
        for value in params.get("workspace_id", []):
            _flag("workspace_id_param", value)

    return findings
