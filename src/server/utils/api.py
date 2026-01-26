"""
API utilities for FastAPI routers.

Provides common patterns for exception handling and authentication.
"""

import functools
import inspect
import logging
from typing import Annotated, Callable, Optional, TypeVar

from fastapi import Depends, Header, HTTPException

# Type variable for generic return type preservation
T = TypeVar("T")


def get_current_user_id(
    x_user_id: str = Header(..., alias="X-User-Id", description="User ID from auth"),
) -> str:
    """
    FastAPI dependency to extract user ID from X-User-Id header.

    Usage:
        @router.get("/endpoint")
        async def endpoint(user_id: CurrentUserId):
            ...
    """
    return x_user_id


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
