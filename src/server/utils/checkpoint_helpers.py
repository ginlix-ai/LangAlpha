"""Checkpoint configuration and validation helpers.

Consolidates repeated checkpoint config building and checkpointer validation
patterns used across workflow endpoints.
"""

from functools import wraps
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

# Import setup module to access initialized globals
from src.server.app import setup


# Type variable for decorated functions
F = TypeVar("F", bound=Callable[..., Any])


def build_checkpoint_config(
    thread_id: str,
    checkpoint_id: str | None = None,
) -> dict[str, Any]:
    """Build a checkpoint configuration dict.

    Args:
        thread_id: Thread identifier
        checkpoint_id: Optional specific checkpoint ID

    Returns:
        Configuration dict with "configurable" key containing thread_id
        and optionally checkpoint_id
    """
    config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
        }
    }
    if checkpoint_id:
        config["configurable"]["checkpoint_id"] = checkpoint_id
    return config


def require_checkpointer(func: F) -> F:
    """Decorator that ensures checkpointer is initialized before endpoint execution.

    Raises HTTPException with status 500 if checkpointer is not available.

    Usage:
        @router.get("/endpoint")
        @require_checkpointer
        async def my_endpoint(...):
            # checkpointer is guaranteed to exist here
            ...
    """
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not setup.checkpointer:
            raise HTTPException(
                status_code=500,
                detail="Checkpointer not initialized"
            )
        return await func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def get_checkpointer():
    """Get the checkpointer instance, raising HTTPException if not available.

    Returns:
        The initialized checkpointer

    Raises:
        HTTPException: If checkpointer is not initialized
    """
    if not setup.checkpointer:
        raise HTTPException(
            status_code=500,
            detail="Checkpointer not initialized"
        )
    return setup.checkpointer
