"""Computer lifecycle routes shared with workspace aliases.

Aliases resolve workspace to computer and call these actions so transitions
cannot drift while both API paths remain live.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.app.background_starts import schedule_start
from src.server.app.status_stream import (
    SSE_HEADERS,
    sse_status_event,
    status_event_stream,
)
from src.server.database.computer import (
    get_computer,
    get_computers_for_user,
    observed_layout_version,
    update_computer_status,
)
from src.server.database.workspace import count_live_workspaces_by_computer
from src.server.dependencies.usage_limits import (
    ComputerLimitCheck,
    assert_always_on_allowed,
    assert_spec_allowed,
)
from src.server.models.computer import (
    CLAIMABLE_FOR_START,
    ComputerActionResponse,
    ComputerAlwaysOnRequest,
    ComputerCreate,
    ComputerListResponse,
    ComputerResponse,
    ComputerSessionResponse,
    ComputerSpecRequest,
)
from src.server.services.workspace_status_pubsub import (
    publish_computer_status_change,
    subscribe_to_computer_status,
)
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/computers", tags=["Computers"])


class ComputerRuntimeUnavailable(RuntimeError):
    """Workspace aliases fall back when the runtime is absent; computer routes return 503."""


def _computer_manager() -> Any:
    """Defer provider-stack imports so read routes remain loadable without the runtime."""
    try:
        from src.server.services.computer_manager import ComputerManager
    except ImportError as e:
        raise ComputerRuntimeUnavailable(str(e)) from None
    return ComputerManager.get_instance()


@contextlib.asynccontextmanager
async def _computer_action_errors(action: str, computer_id: str):
    """Let sandbox failures reach the app-level handler that owns their wording."""
    try:
        yield
    except HTTPException:
        raise
    except ComputerRuntimeUnavailable as e:
        logger.warning("Computer runtime unavailable for %s: %s", action, e)
        raise HTTPException(
            status_code=503, detail="Computer runtime is not available"
        ) from None
    except (SandboxGoneError, SandboxTransientError):
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error {action} computer {computer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to {action} computer")


def require_computer_owner(computer: Optional[dict], *, user_id: str) -> None:
    if not computer:
        raise HTTPException(status_code=404, detail="Computer not found")
    if computer.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")


def computer_to_response(
    computer: Dict[str, Any], *, owner: bool = True, workspace_count: int = 0
) -> ComputerResponse:
    """Withhold provider_ref from non-owners because it addresses a real billed machine."""
    return ComputerResponse(
        computer_id=str(computer["computer_id"]),
        user_id=computer["user_id"],
        kind=computer.get("kind") or "daytona",
        name=computer.get("name") or "",
        status=computer["status"],
        resource_tier=computer.get("resource_tier") or "standard",
        is_always_on=bool(computer.get("is_always_on")),
        is_primary=bool(computer.get("is_primary")),
        root_dir=computer.get("root_dir") or "",
        workspace_count=workspace_count,
        layout_version=observed_layout_version(computer),
        provider_ref=computer.get("provider_ref") if owner else None,
        created_at=computer["created_at"],
        updated_at=computer["updated_at"],
        last_activity_at=computer.get("last_activity_at"),
        stopped_at=computer.get("stopped_at"),
        config=computer.get("config"),
    )


async def _owned_computer(computer_id: str, user_id: str) -> Dict[str, Any]:
    computer = await get_computer(computer_id)
    require_computer_owner(computer, user_id=user_id)
    return computer  # type: ignore[return-value]


async def _report_start_failure(computer_id: str, exc: Exception) -> None:
    """A background start behind 202 must publish failure or subscribers wait until timeout.

    Settle any surviving claim before publishing so clients re-reading on error
    see the settled row; the start's own revert normally owns this cleanup.
    """
    try:
        row = await get_computer(computer_id)
        if row is not None and row.get("status") == "starting":
            await update_computer_status(computer_id, "stopped", expected="starting")
    except Exception:
        logger.exception(
            "Could not settle computer %s after a failed start", computer_id
        )
    await publish_computer_status_change(
        computer_id, "error", extra={"error": str(exc) or type(exc).__name__}
    )


async def _run_computer_start(manager: Any, computer_id: str) -> None:
    """Do not report shutdown cancellation as failure; the start already reverts its claim."""
    try:
        await manager.start_computer(computer_id)
    except Exception as exc:
        await _report_start_failure(computer_id, exc)
        raise


def schedule_computer_start(computer_id: str) -> None:
    manager = _computer_manager()
    schedule_start(
        f"computer:{computer_id}",
        lambda: _run_computer_start(manager, computer_id),
    )


@router.post("", response_model=ComputerResponse, status_code=201)
async def create_computer(
    request: ComputerCreate,
    x_user_id: ComputerLimitCheck,
):
    """Enforce the machine cap here: workspaces join existing computers.

    Creating the row provisions nothing and incurs no machine cost until a project runs.
    """
    async with _computer_action_errors("create", "new"):
        await assert_spec_allowed(x_user_id, request.resource_tier)
        computer = await _computer_manager().create_computer_for_user(
            x_user_id,
            name=request.name,
            resource_tier=request.resource_tier,
            # The insert squashes this to FALSE when a live primary already
            # exists, so it reads as "primary if they have none". Without it a
            # user's first, named machine is not primary and their first project
            # mints a second one.
            is_primary=True,
        )
        logger.info(
            "Created computer %s for user %s", computer["computer_id"], x_user_id
        )
        return computer_to_response(computer)


@router.get("", response_model=ComputerListResponse)
async def list_computers(x_user_id: CurrentUserId):
    rows = await get_computers_for_user(x_user_id)
    # One query for the whole list: stopping a machine takes every project on it
    # down, so the count is what the card needs to say so before asking.
    counts = await count_live_workspaces_by_computer(
        [str(row["computer_id"]) for row in rows]
    )
    return ComputerListResponse(
        computers=[
            computer_to_response(
                row, workspace_count=counts.get(str(row["computer_id"]), 0)
            )
            for row in rows
        ],
        total=len(rows),
    )


async def _counted_response(computer: Dict[str, Any]) -> ComputerResponse:
    """A single machine's response with its live project count.

    Every route that answers with a whole machine carries the count, including
    the ones that just changed it: a response is what the client replaces its
    row with, so one answering zero erases the number the card warns with.
    """
    computer_id = str(computer["computer_id"])
    counts = await count_live_workspaces_by_computer([computer_id])
    return computer_to_response(computer, workspace_count=counts.get(computer_id, 0))


@router.get("/{computer_id}", response_model=ComputerResponse)
async def get_computer_details(computer_id: str, x_user_id: CurrentUserId):
    return await _counted_response(await _owned_computer(computer_id, x_user_id))


@router.get("/{computer_id}/session", response_model=ComputerSessionResponse)
async def get_computer_session(computer_id: str, x_user_id: CurrentUserId):
    """Resolve sessions only when Postgres says running so this read cannot provision."""
    computer = await _owned_computer(computer_id, x_user_id)
    response = ComputerSessionResponse(
        computer_id=str(computer["computer_id"]),
        status=computer["status"],
        provider_ref=computer.get("provider_ref"),
    )
    if computer["status"] != "running":
        return response

    try:
        session = await _computer_manager().get_session_for_computer(computer_id)
    except ComputerRuntimeUnavailable:
        return response
    except Exception as e:
        logger.debug("No session for computer %s: %s", computer_id, e)
        return response

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        return response
    ref = getattr(sandbox, "sandbox_id", None)
    return response.model_copy(
        update={
            "ready": getattr(sandbox, "runtime", None) is not None,
            "session_provider_ref": ref if isinstance(ref, str) and ref else None,
        }
    )


@router.get("/{computer_id}/events")
async def computer_status_events(computer_id: str, x_user_id: CurrentUserId):
    computer = await _owned_computer(computer_id, x_user_id)

    def _frame(
        status: str, sandbox_state: str | None = None, error: str | None = None
    ) -> str:
        return sse_status_event(
            {"computer_id": computer_id}, status, sandbox_state, error
        )

    async def _read_status() -> str | None:
        row = await get_computer(computer_id)
        return row["status"] if row else None

    return StreamingResponse(
        status_event_stream(
            initial_status=computer["status"],
            read_status=_read_status,
            subscribe=lambda: subscribe_to_computer_status(computer_id),
            frame=_frame,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.post("/{computer_id}/start", response_model=ComputerActionResponse)
async def start_computer(
    computer_id: str,
    x_user_id: CurrentUserId,
    lazy: bool = Query(
        False,
        description=(
            "If true, schedule the start in the background and return 202 "
            "immediately with status='starting'. If false (default), block "
            "until the computer is up and return status='running'."
        ),
    ),
):
    async with _computer_action_errors("start", computer_id):
        computer = await _owned_computer(computer_id, x_user_id)
        status = computer["status"]

        if status in ("running", "starting"):
            return ComputerActionResponse(
                computer_id=computer_id,
                status=status,
                message=f"Computer is already {status}",
            )
        if status not in CLAIMABLE_FOR_START:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot start computer in '{status}' state",
            )

        if lazy:
            schedule_computer_start(computer_id)
            payload = ComputerActionResponse(
                computer_id=computer_id,
                status="starting",
                message="Computer start initiated",
            )
            return Response(
                status_code=202,
                content=payload.model_dump_json(),
                media_type="application/json",
            )

        updated = await _computer_manager().start_computer(computer_id)
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "running",
            message="Computer started successfully",
        )


@router.post("/{computer_id}/stop", response_model=ComputerActionResponse)
async def stop_computer(computer_id: str, x_user_id: CurrentUserId):
    async with _computer_action_errors("stop", computer_id):
        await _owned_computer(computer_id, x_user_id)
        updated = await _computer_manager().stop_computer(computer_id)
        logger.info(f"Stopped computer {computer_id}")
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "stopped",
            message="Computer stopped successfully",
        )


@router.post("/{computer_id}/archive", response_model=ComputerActionResponse)
async def archive_computer(computer_id: str, x_user_id: CurrentUserId):
    async with _computer_action_errors("archive", computer_id):
        await _owned_computer(computer_id, x_user_id)
        updated = await _computer_manager().archive_computer(computer_id)
        logger.info(f"Archived computer {computer_id}")
        return ComputerActionResponse(
            computer_id=computer_id,
            status=(updated or {}).get("status") or "stopped",
            message="Computer archived successfully",
        )


@router.post("/{computer_id}/spec", response_model=ComputerResponse)
async def set_computer_spec(
    computer_id: str,
    request: ComputerSpecRequest,
    x_user_id: CurrentUserId,
):
    """Skip the count check for the current tier so retries at quota remain valid.

    Platform gates return 403 off-plan or 429 over quota; OSS gates are no-ops.
    """
    async with _computer_action_errors("set spec for", computer_id):
        computer = await _owned_computer(computer_id, x_user_id)
        await assert_spec_allowed(
            x_user_id,
            request.tier,
            current_tier=computer.get("resource_tier") or "standard",
        )

        updated = await _computer_manager().set_computer_spec(
            computer_id, request.tier, user_id=x_user_id
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Computer not found")
        logger.info(
            f"Set computer {computer_id} spec to {request.tier!r} for user {x_user_id}"
        )
        return await _counted_response(updated)


@router.post("/{computer_id}/always-on", response_model=ComputerResponse)
async def set_computer_always_on(
    computer_id: str,
    request: ComputerAlwaysOnRequest,
    x_user_id: CurrentUserId,
):
    """Disabling and idempotent enabling consume no slot and must not fail at quota.

    Start stopped machines in the background because always-on takes effect immediately.
    """
    async with _computer_action_errors("set always-on for", computer_id):
        computer = await _owned_computer(computer_id, x_user_id)
        if request.enabled and not computer.get("is_always_on"):
            await assert_always_on_allowed(x_user_id)

        updated = await _computer_manager().set_computer_always_on(
            computer_id, request.enabled
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Computer not found")

        if request.enabled and updated.get("status") == "stopped":
            schedule_computer_start(computer_id)
            logger.info(
                f"Always-on enabled: scheduled start for computer {computer_id}"
            )
            updated = {**updated, "status": "starting"}

        logger.info(
            f"Set computer {computer_id} always-on to {request.enabled} "
            f"for user {x_user_id}"
        )
        return await _counted_response(updated)
