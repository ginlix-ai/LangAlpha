"""
Automations API Router.

Provides REST endpoints for creating, managing, and monitoring
scheduled automations (cron and one-time triggers).

Endpoints (/api/v1/automations):
- POST   /automations                              - Create automation
- GET    /automations                              - List automations
- GET    /automations/executions                   - Run feed across automations
- GET    /automations/delivery-options             - Chats a workspace's runs can reach
- PUT    /automations/delivery-default             - Set a workspace's default chat
- GET    /automations/{automation_id}              - Get automation
- PATCH  /automations/{automation_id}              - Update automation
- DELETE /automations/{automation_id}              - Delete automation
- POST   /automations/{automation_id}/trigger      - Manual trigger
- POST   /automations/{automation_id}/pause        - Pause
- POST   /automations/{automation_id}/resume       - Resume
- GET    /automations/{automation_id}/executions    - Execution history
- POST   /automations/{automation_id}/executions/{execution_id}/skip
                                                   - Skip a waiting run
- POST   /automations/{automation_id}/executions/{execution_id}/dismiss
                                                   - Dismiss a failed run
"""

import logging
from typing import Any, Optional
from uuid import UUID

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from src.server.database import automation as auto_db
from src.server.database import automation_executions as exec_db
from src.server.database.workspace import get_workspace
from src.server.services.automations import lifecycle as handler
from src.server.models.automation import (
    AutomationCreate,
    AutomationResponse,
    AutomationRunResponse,
    AutomationRunsListResponse,
    AutomationsListResponse,
    AutomationUpdate,
    DeliveryDefaultUpdate,
    ExecutionStatus,
)
from src.server.utils.api import (
    CurrentUserId,
    handle_api_exceptions,
    raise_not_found,
    require_workspace_owner,
)
from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Automations"])


# =============================================================================
# CRUD Endpoints
# =============================================================================


@router.post("/automations", response_model=AutomationResponse, status_code=201)
@handle_api_exceptions("create automation", logger, conflict_on_value_error=True)
async def create_automation(
    request: AutomationCreate,
    user_id: CurrentUserId,
):
    """Create a new scheduled automation."""
    try:
        automation = await handler.create_automation(user_id=user_id, data=request)
    except handler.DeliveryRefused as e:
        return _delivery_refused(e)
    return AutomationResponse.model_validate(automation)


@router.get("/automations", response_model=AutomationsListResponse)
@handle_api_exceptions("list automations", logger)
async def list_automations(
    user_id: CurrentUserId,
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List automations for the current user, each with its newest execution."""
    automations, total = await auto_db.list_automations(
        user_id, status=status, limit=limit, offset=offset,
    )
    return AutomationsListResponse(
        automations=[AutomationResponse.model_validate(a) for a in automations],
        total=total,
    )


# Declared before /automations/{automation_id}, which would otherwise capture
# "executions" as an id.
@router.get("/automations/executions", response_model=AutomationRunsListResponse)
@handle_api_exceptions("list automation runs", logger)
async def list_automation_runs(
    user_id: CurrentUserId,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    thread_id: Optional[UUID] = Query(None),
    status: Optional[ExecutionStatus] = Query(None),
):
    """List executions across all of the current user's automations."""
    return await _runs_page(
        user_id,
        thread_id=str(thread_id) if thread_id else None,
        status=status,
        limit=limit,
        offset=offset,
    )


# =============================================================================
# Delivery through the messaging service
# =============================================================================

# Listing the chats asks each linked app; saving a default checks the chat.
_DELIVERY_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


def _delivery_refused(e: handler.DeliveryRefused) -> JSONResponse:
    """A write refused for its delivery: the 409 it has always been, with the
    sentence in ``detail`` and each entry's reason in ``problems``."""
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(e),
            "problems": [{"entry": entry, "message": why} for entry, why in e.refusals],
        },
    )


def _service_message(answer: messaging.GatewayAnswer, fallback: str) -> str:
    message = (answer.data or {}).get("message")
    return message if isinstance(message, str) and message else fallback


def _unavailable(detail: str) -> HTTPException:
    return HTTPException(status_code=503, detail=detail)


# Declared before /automations/{automation_id}, which would otherwise capture
# the path as an id.
@router.get("/automations/delivery-options")
@handle_api_exceptions("list delivery options", logger)
async def get_delivery_options(
    user_id: CurrentUserId,
    workspace_id: UUID = Query(...),
) -> dict[str, Any]:
    """The chats each linked app offers a workspace's automations, and the
    one an entry naming only the app reaches. ``enabled`` is false, with no
    apps, on a server with no messaging service."""
    require_workspace_owner(await get_workspace(str(workspace_id)), user_id=user_id)
    if not messaging.messaging_enabled():
        return {"enabled": False, "apps": {}}
    try:
        answer = await messaging.gateway_request(
            "GET",
            "/agent/automation-targets",
            user_id=user_id,
            timeout=_DELIVERY_TIMEOUT,
            params={"workspace_id": str(workspace_id)},
        )
    except messaging.GatewayError as e:
        raise _unavailable(e.message)
    if answer.status != 200:
        logger.warning(f"[AUTOMATIONS] Delivery options answered {answer.status}")
        raise _unavailable(
            _service_message(answer, f"The messaging service failed ({answer.status}).")
        )
    apps = (answer.data or {}).get("apps")
    if not isinstance(apps, dict):
        raise _unavailable("The messaging service sent an answer that could not be read.")
    return {"enabled": True, "apps": apps}


@router.put("/automations/delivery-default")
@handle_api_exceptions("set delivery default", logger)
async def set_delivery_default(
    request: DeliveryDefaultUpdate,
    user_id: CurrentUserId,
):
    """Set the chat on one app that a workspace's automations deliver to when
    an entry names only the app, or clear it with a null address. Answers
    the chat as saved."""
    require_workspace_owner(await get_workspace(str(request.workspace_id)), user_id=user_id)
    if not messaging.messaging_enabled():
        raise HTTPException(status_code=404, detail="No messaging service is configured.")
    try:
        answer = await messaging.gateway_request(
            "PUT",
            "/agent/automation-output",
            user_id=user_id,
            timeout=_DELIVERY_TIMEOUT,
            body={
                "workspace_id": str(request.workspace_id),
                "platform": request.platform,
                "address": request.address,
            },
        )
    except messaging.GatewayError as e:
        raise _unavailable(e.message)
    data = answer.data or {}
    if answer.status == 200 and answer.data is not None:
        return {"address": data.get("address"), "name": data.get("name")}
    if answer.status == 400:
        problems = data.get("problems")
        return JSONResponse(
            status_code=400,
            content={
                "detail": _service_message(answer, "The default was not saved."),
                "problems": problems if isinstance(problems, list) else [],
            },
        )
    if answer.status == 409:
        raise HTTPException(
            status_code=409,
            detail=_service_message(
                answer, "The default is being changed elsewhere. Try again."
            ),
        )
    logger.warning(f"[AUTOMATIONS] Delivery default answered {answer.status}")
    if answer.status == 200:
        raise _unavailable("The messaging service sent an answer that could not be read.")
    raise _unavailable(
        _service_message(answer, f"The messaging service failed ({answer.status}).")
    )


@router.get("/automations/{automation_id}", response_model=AutomationResponse)
@handle_api_exceptions("get automation", logger)
async def get_automation(
    automation_id: UUID,
    user_id: CurrentUserId,
):
    """Get a specific automation."""
    automation = await auto_db.get_automation(str(automation_id), user_id)
    if not automation:
        raise_not_found("Automation")
    return AutomationResponse.model_validate(automation)


@router.patch("/automations/{automation_id}", response_model=AutomationResponse)
@handle_api_exceptions("update automation", logger, conflict_on_value_error=True)
async def update_automation(
    automation_id: UUID,
    request: AutomationUpdate,
    user_id: CurrentUserId,
):
    """Partial update of an automation."""
    try:
        automation = await handler.update_automation(
            automation_id=str(automation_id),
            user_id=user_id,
            fields=request.model_dump(exclude_none=True),
        )
    except handler.DeliveryRefused as e:
        return _delivery_refused(e)
    except ValidationError as e:
        # The body parsed, then failed against the stored kind of trigger:
        # a 422 shaped like the one FastAPI answers a bad body with.
        raise HTTPException(
            status_code=422,
            detail=e.errors(include_url=False, include_context=False, include_input=False),
        )
    if not automation:
        raise_not_found("Automation")
    return AutomationResponse.model_validate(automation)


@router.delete("/automations/{automation_id}", status_code=204)
@handle_api_exceptions("delete automation", logger)
async def delete_automation(
    automation_id: UUID,
    user_id: CurrentUserId,
):
    """Delete an automation (cascade deletes executions)."""
    deleted = await auto_db.delete_automation(str(automation_id), user_id)
    if not deleted:
        raise_not_found("Automation")
    return Response(status_code=204)


# =============================================================================
# Control Endpoints
# =============================================================================


@router.post("/automations/{automation_id}/trigger")
@handle_api_exceptions("trigger automation", logger, conflict_on_value_error=True)
async def trigger_automation(
    automation_id: UUID,
    user_id: CurrentUserId,
):
    """Manually trigger an automation immediately (doesn't affect next_run_at)."""
    result = await handler.trigger_automation(str(automation_id), user_id)
    return result


@router.post("/automations/{automation_id}/pause", response_model=AutomationResponse)
@handle_api_exceptions("pause automation", logger, conflict_on_value_error=True)
async def pause_automation(
    automation_id: UUID,
    user_id: CurrentUserId,
):
    """Pause an active automation."""
    automation = await handler.pause_automation(str(automation_id), user_id)
    if not automation:
        raise_not_found("Automation")
    return AutomationResponse.model_validate(automation)


@router.post("/automations/{automation_id}/resume", response_model=AutomationResponse)
@handle_api_exceptions("resume automation", logger, conflict_on_value_error=True)
async def resume_automation(
    automation_id: UUID,
    user_id: CurrentUserId,
):
    """Resume a paused or disabled automation."""
    automation = await handler.resume_automation(str(automation_id), user_id)
    if not automation:
        raise_not_found("Automation")
    return AutomationResponse.model_validate(automation)


@router.post("/automations/{automation_id}/executions/{execution_id}/skip")
@handle_api_exceptions("skip execution", logger, conflict_on_value_error=True)
async def skip_execution(
    automation_id: UUID,
    execution_id: UUID,
    user_id: CurrentUserId,
):
    """Skip a run that is waiting for its thread's current turn to end."""
    result = await handler.skip_execution(
        str(automation_id), str(execution_id), user_id
    )
    if result is None:
        raise_not_found("Automation")
    return result


@router.post(
    "/automations/{automation_id}/executions/{execution_id}/dismiss",
    response_model=AutomationResponse,
)
@handle_api_exceptions("dismiss execution", logger, conflict_on_value_error=True)
async def dismiss_execution(
    automation_id: UUID,
    execution_id: UUID,
    user_id: CurrentUserId,
):
    """Take a failed run out of Needs attention until another run fails."""
    automation = await handler.dismiss_execution(
        str(automation_id), str(execution_id), user_id
    )
    if not automation:
        raise_not_found("Automation")
    return AutomationResponse.model_validate(automation)


# =============================================================================
# Execution History
# =============================================================================


@router.get(
    "/automations/{automation_id}/executions",
    response_model=AutomationRunsListResponse,
)
@handle_api_exceptions("list executions", logger)
async def list_executions(
    automation_id: UUID,
    user_id: CurrentUserId,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List execution history for an automation."""
    return await _runs_page(
        user_id, automation_id=str(automation_id), limit=limit, offset=offset,
    )


async def _runs_page(user_id: str, **filters) -> AutomationRunsListResponse:
    """The feed and one automation's history: one query, one page shape."""
    executions, has_more = await exec_db.list_executions(user_id, **filters)
    return AutomationRunsListResponse(
        executions=[AutomationRunResponse.model_validate(e) for e in executions],
        has_more=has_more,
    )
