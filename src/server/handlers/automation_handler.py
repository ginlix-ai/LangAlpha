"""
Automation Handler — Business logic for automation CRUD and control.

Validates inputs (through the request models, plus timezones and target
ownership), calculates next_run_at, and delegates to the database layer.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from src.server.database import automation as auto_db
from src.server.database.workspace import get_workspace
from src.server.models.automation import AutomationCreate, AutomationUpdate
from src.server.services.automation_scheduler import AutomationScheduler
from src.server.services.automation_settlement import Outcome, settle
from src.server.utils.api import (
    raise_not_found,
    require_thread_owner,
    require_workspace_owner,
)
from src.utils.timezone_utils import zone_or_none

logger = logging.getLogger(__name__)


async def require_target_ownership(
    user_id: str,
    workspace_id: UUID | str | None,
    conversation_thread_id: UUID | str | None,
) -> None:
    """Verify caller-supplied automation targets belong to ``user_id``.

    Gated here rather than in the router because the REST endpoints and the
    agent's automation tool both reach the database through this module. The
    check mostly holds for the row's life: nothing reassigns
    ``workspaces.user_id`` and nothing moves a thread between workspaces, so a
    workspace owned at write time stays owned. A pinned thread is the gap —
    deleting one hard-deletes the row and frees its id for whoever posts to it
    next, and the column carries no FK to catch that. Raises 404/403, never
    ValueError, which these routes map to 409.
    """
    if workspace_id:
        require_workspace_owner(
            await get_workspace(str(workspace_id)), user_id=user_id
        )
    if conversation_thread_id:
        await require_thread_owner(str(conversation_thread_id), user_id)


def validate_timezone(tz_name: str) -> None:
    """Validate an IANA timezone name. Raises ValueError if invalid."""
    if zone_or_none(tz_name) is None:
        raise ValueError(f"Invalid timezone: '{tz_name}'")


async def create_automation(
    user_id: str,
    data: AutomationCreate,
) -> Dict[str, Any]:
    """Create a new automation.

    Raises:
        ValueError: On an invalid timezone, or agent_mode='ptc' without a
            workspace
    """
    validate_timezone(data.timezone)

    # A price trigger has no next_run_at: the price monitor fires it.
    next_run_at = (
        AutomationScheduler.calculate_first_run(data.cron_expression, data.timezone)
        if data.trigger_type == "cron"
        else data.next_run_at
    )

    if data.agent_mode == "ptc" and not data.workspace_id:
        raise ValueError("workspace_id is required for agent_mode='ptc'")

    # Targets are checked whatever the mode: flash ignores workspace_id at run
    # time, but a later PATCH to agent_mode='ptc' activates the stored one.
    await require_target_ownership(
        user_id, data.workspace_id, data.conversation_thread_id
    )

    automation = await auto_db.create_automation(
        user_id=user_id,
        name=data.name,
        trigger_type=data.trigger_type,
        instruction=data.instruction,
        description=data.description,
        cron_expression=data.cron_expression,
        timezone=data.timezone,
        trigger_config=data.trigger_config,
        next_run_at=next_run_at,
        agent_mode=data.agent_mode,
        workspace_id=str(data.workspace_id) if data.workspace_id else None,
        llm_model=data.llm_model,
        additional_context=data.additional_context,
        thread_strategy=data.thread_strategy,
        conversation_thread_id=(
            str(data.conversation_thread_id) if data.conversation_thread_id else None
        ),
        max_failures=data.max_failures,
        delivery_config=(
            data.delivery_config.model_dump() if data.delivery_config else None
        ),
        metadata=data.metadata,
    )

    logger.info(
        f"[AUTOMATION] Created automation {automation['automation_id']} "
        f"for user {user_id} (trigger={data.trigger_type}, next_run={next_run_at})"
    )
    return automation


async def update_automation(
    automation_id: str,
    user_id: str,
    fields: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Update an automation with validation and next_run_at recalculation.

    Args:
        automation_id: Automation UUID
        user_id: Owner user ID
        fields: The fields to change, read through AutomationUpdate against
            the stored kind of trigger. REST drops None fields before they get
            here, so a None ``conversation_thread_id`` was named on purpose:
            the agent tool's thread='new' clearing the pin.

    Returns:
        Updated automation dict, or None if not found

    Raises:
        ValidationError: A field AutomationUpdate refuses, such as a schedule
            field of another kind than the stored one
        ValueError: On an invalid timezone, or a merged state that leaves
            agent_mode='ptc' without a workspace
    """
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None

    update = AutomationUpdate.model_validate(
        fields, context={"trigger_type": current["trigger_type"]}
    )
    update_kwargs = update.model_dump(exclude_unset=True, exclude={"trigger_type"})
    for field in ("workspace_id", "conversation_thread_id"):
        if update_kwargs.get(field) is not None:
            update_kwargs[field] = str(update_kwargs[field])

    await require_target_ownership(
        user_id, update.workspace_id, update.conversation_thread_id
    )

    new_tz = update.timezone
    if new_tz:
        validate_timezone(new_tz)

    # create enforces this, but a PATCH can flip agent_mode without naming a
    # workspace — check the merged state, or 'ptc' activates on a row that has
    # none and the executor only finds out at run time, burning a failure.
    merged_mode = update.agent_mode or current.get("agent_mode")
    merged_workspace = update.workspace_id or current.get("workspace_id")
    if merged_mode == "ptc" and not merged_workspace:
        raise ValueError("workspace_id is required for agent_mode='ptc'")

    # Recalculate next_run_at if cron expression or timezone changed. Only an
    # active row gets one: a paused or disabled cron keeps the none that pause
    # left it, and resume computes it from the edited expression and zone.
    new_cron = update.cron_expression
    cron_expr = new_cron or current["cron_expression"]
    tz_name = new_tz or current["timezone"]

    if (
        (new_cron or new_tz)
        and current["trigger_type"] == "cron"
        and current["status"] == "active"
        and cron_expr
    ):
        update_kwargs["next_run_at"] = AutomationScheduler.calculate_first_run(
            cron_expr, tz_name
        )

    if not update_kwargs:
        return current

    result = await auto_db.update_automation(automation_id, user_id, **update_kwargs)
    if result:
        logger.info(
            f"[AUTOMATION] Updated automation {automation_id} "
            f"fields={list(update_kwargs.keys())}"
        )
    return result


async def pause_automation(
    automation_id: str,
    user_id: str,
) -> Optional[Dict[str, Any]]:
    """Pause an active automation.

    A one-time automation keeps its next_run_at: that time is its whole
    schedule, and resume has nowhere else to read it from. Keeping it is safe
    because the scheduler claims only 'active' rows. A cron's is recomputed
    on resume, so it is cleared.
    """
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None

    if current["status"] != "active":
        raise ValueError(
            f"Cannot pause automation in '{current['status']}' status "
            f"(must be 'active')"
        )

    update_kwargs: Dict[str, Any] = {"status": "paused"}
    if current["trigger_type"] != "once":
        update_kwargs["next_run_at"] = None
    return await auto_db.update_automation(automation_id, user_id, **update_kwargs)


async def resume_automation(
    automation_id: str,
    user_id: str,
) -> Optional[Dict[str, Any]]:
    """Resume a paused/disabled automation (recalculates next_run_at, resets failures)."""
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None

    if current["status"] not in ("paused", "disabled"):
        raise ValueError(
            f"Cannot resume automation in '{current['status']}' status "
            f"(must be 'paused' or 'disabled')"
        )

    # The reason goes with the status it explained, in the same write.
    update_kwargs: Dict[str, Any] = {
        "status": "active",
        "failure_count": 0,
        "disable_reason": None,
    }

    # Recalculate next_run_at
    if current["trigger_type"] == "cron" and current.get("cron_expression"):
        update_kwargs["next_run_at"] = AutomationScheduler.calculate_first_run(
            current["cron_expression"],
            current.get("timezone", "UTC"),
        )
    elif current["trigger_type"] == "once":
        # For one-time, check if original time has passed
        if current.get("next_run_at") and current["next_run_at"] > datetime.now(timezone.utc):
            update_kwargs["next_run_at"] = current["next_run_at"]
        elif current["status"] == "disabled":
            # Switched off by the run it already had: back on, it waits for
            # Run now, as a one-time automation whose run failed does.
            update_kwargs["next_run_at"] = None
        else:
            raise ValueError(
                "Cannot resume a one-time automation whose scheduled time has passed"
            )
    elif current["trigger_type"] == "price":
        # Price triggers don't use next_run_at — just re-activate
        pass

    return await auto_db.update_automation(automation_id, user_id, **update_kwargs)


async def trigger_automation(
    automation_id: str,
    user_id: str,
) -> Dict[str, Any]:
    """Manually trigger an automation immediately (doesn't affect next_run_at).

    Returns:
        Dict with execution_id and status.
    """
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        raise ValueError("Automation not found")

    if current["status"] not in ("active", "paused", "completed"):
        raise ValueError(
            f"Cannot trigger automation in '{current['status']}' status"
        )

    # Create execution record
    scheduler = AutomationScheduler.get_instance()
    execution_id = await auto_db.create_execution(
        automation_id=automation_id,
        scheduled_at=datetime.now(timezone.utc),
        server_id=scheduler.server_id,
    )

    scheduler.dispatch(current, execution_id, name=f"manual_exec_{automation_id[:8]}")

    logger.info(
        f"[AUTOMATION] Manual trigger: automation_id={automation_id} "
        f"execution_id={execution_id}"
    )

    return {
        "execution_id": execution_id,
        "automation_id": automation_id,
        "status": "triggered",
    }


async def skip_execution(
    automation_id: str,
    execution_id: str,
    user_id: str,
) -> Optional[Dict[str, Any]]:
    """Skip a firing that is waiting for its thread's turn to end.

    Returns None when the automation is not the user's and raises 404 when
    the firing does not exist; raises ValueError when it already started or
    settled.
    """
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None
    # settle raises nothing once the row is written, so True is the skip
    # having happened; False needs a read to tell missing from moved on.
    if not await settle(current, execution_id, Outcome.SKIPPED, skip_reason="user"):
        if await auto_db.get_execution_status(
            execution_id, automation_id=automation_id
        ) is None:
            raise_not_found("Execution")
        raise ValueError("This run is no longer waiting")
    return {
        "execution_id": execution_id,
        "automation_id": automation_id,
        "status": "skipped",
    }


async def dismiss_execution(
    automation_id: str,
    execution_id: str,
    user_id: str,
) -> Optional[Dict[str, Any]]:
    """Take a failed run out of Needs attention, answering with the automation.

    Returns None when the automation is not the user's and raises 404 when
    the run does not exist; raises ValueError when the run did not fail.
    """
    if not await auto_db.get_automation(automation_id, user_id):
        return None
    if not await auto_db.dismiss_execution(execution_id, automation_id=automation_id):
        if await auto_db.get_execution_status(
            execution_id, automation_id=automation_id
        ) is None:
            raise_not_found("Execution")
        raise ValueError("Only a failed run can be dismissed")
    return await auto_db.get_automation(automation_id, user_id)
