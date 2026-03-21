"""
Automation Handler — Business logic for automation CRUD and control.

Validates inputs (cron expressions, timezones), calculates next_run_at,
and delegates to the database layer.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from src.server.database import automation as auto_db
from src.server.services.automation_scheduler import AutomationScheduler
from src.server.services.automation_executor import AutomationExecutor

logger = logging.getLogger(__name__)


def validate_cron_expression(expr: str) -> None:
    """Validate a cron expression. Raises ValueError if invalid."""
    if not croniter.is_valid(expr):
        raise ValueError(f"Invalid cron expression: '{expr}'")


def validate_timezone(tz_name: str) -> None:
    """Validate an IANA timezone name. Raises ValueError if invalid."""
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError):
        raise ValueError(f"Invalid timezone: '{tz_name}'")


async def create_automation(
    user_id: str,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    """Create a new automation with validation.

    Args:
        user_id: Owner user ID
        data: Validated request data (from AutomationCreate)

    Returns:
        Created automation dict

    Raises:
        ValueError: On invalid cron/timezone/configuration
    """
    trigger_type = data["trigger_type"]
    cron_expression = data.get("cron_expression")
    tz_name = data.get("timezone", "UTC")

    # Validate timezone
    validate_timezone(tz_name)

    # Validate trigger-specific requirements
    if trigger_type == "cron":
        if not cron_expression:
            raise ValueError("cron_expression is required for trigger_type='cron'")
        validate_cron_expression(cron_expression)

        # Calculate first next_run_at
        next_run_at = AutomationScheduler.calculate_first_run(
            cron_expression, tz_name
        )
    elif trigger_type == "once":
        next_run_at = data.get("next_run_at")
        if not next_run_at:
            raise ValueError("next_run_at is required for trigger_type='once'")
        # Ensure timezone-aware
        if next_run_at.tzinfo is None:
            next_run_at = next_run_at.replace(tzinfo=timezone.utc)
    elif trigger_type == "price":
        # Validate trigger_config as PriceTriggerConfig
        trigger_config = data.get("trigger_config")
        if not trigger_config:
            raise ValueError("trigger_config is required for trigger_type='price'")
        from src.server.models.automation import PriceTriggerConfig
        try:
            PriceTriggerConfig(**trigger_config)
        except Exception as e:
            raise ValueError(f"Invalid price trigger config: {e}")
        # Price triggers have no scheduled next_run_at
        next_run_at = None
    else:
        raise ValueError(f"Unsupported trigger_type: '{trigger_type}'")

    # Validate agent mode requirements
    agent_mode = data.get("agent_mode", "flash")
    if agent_mode == "ptc" and not data.get("workspace_id"):
        raise ValueError("workspace_id is required for agent_mode='ptc'")

    # Create in database
    automation = await auto_db.create_automation(
        user_id=user_id,
        name=data["name"],
        trigger_type=trigger_type,
        instruction=data["instruction"],
        description=data.get("description"),
        cron_expression=cron_expression,
        timezone=tz_name,
        trigger_config=data.get("trigger_config"),
        next_run_at=next_run_at,
        agent_mode=agent_mode,
        workspace_id=str(data["workspace_id"]) if data.get("workspace_id") else None,
        llm_model=data.get("llm_model"),
        additional_context=data.get("additional_context"),
        thread_strategy=data.get("thread_strategy", "new"),
        conversation_thread_id=str(data["conversation_thread_id"]) if data.get("conversation_thread_id") else None,
        max_failures=data.get("max_failures", 3),
        delivery_config=data.get("delivery_config"),
        metadata=data.get("metadata"),
    )

    logger.info(
        f"[AUTOMATION] Created automation {automation['automation_id']} "
        f"for user {user_id} (trigger={trigger_type}, next_run={next_run_at})"
    )
    return automation


async def update_automation(
    automation_id: str,
    user_id: str,
    data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Update an automation with validation and next_run_at recalculation.

    Args:
        automation_id: Automation UUID
        user_id: Owner user ID
        data: Validated update data (from AutomationUpdate, only non-None fields)

    Returns:
        Updated automation dict, or None if not found

    Raises:
        ValueError: On invalid cron/timezone
    """
    # Get current automation for reference
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None

    # Build update kwargs (only non-None values)
    update_kwargs: Dict[str, Any] = {}

    for field in [
        "name", "description", "cron_expression", "timezone",
        "trigger_config", "next_run_at",
        "agent_mode", "instruction", "workspace_id", "llm_model",
        "additional_context", "thread_strategy", "conversation_thread_id",
        "max_failures", "delivery_config", "metadata",
    ]:
        value = data.get(field)
        if value is not None:
            # Convert UUIDs to strings
            if field in ("workspace_id", "conversation_thread_id"):
                value = str(value)
            update_kwargs[field] = value

    # Validate timezone if changed
    new_tz = update_kwargs.get("timezone")
    if new_tz:
        validate_timezone(new_tz)

    # Validate and recalculate next_run_at if cron or timezone changed
    new_cron = update_kwargs.get("cron_expression")
    if new_cron:
        validate_cron_expression(new_cron)

    # Recalculate next_run_at if cron expression or timezone changed
    cron_expr = new_cron or current["cron_expression"]
    tz_name = new_tz or current["timezone"]

    if (new_cron or new_tz) and current["trigger_type"] == "cron" and cron_expr:
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
    """Pause an active automation (clears next_run_at)."""
    current = await auto_db.get_automation(automation_id, user_id)
    if not current:
        return None

    if current["status"] != "active":
        raise ValueError(
            f"Cannot pause automation in '{current['status']}' status "
            f"(must be 'active')"
        )

    return await auto_db.update_automation(
        automation_id, user_id,
        status="paused",
        next_run_at=None,
    )


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

    update_kwargs: Dict[str, Any] = {
        "status": "active",
        "failure_count": 0,
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

    # Dispatch execution
    executor = AutomationExecutor.get_instance()
    import asyncio
    asyncio.create_task(
        executor.execute(current, execution_id),
        name=f"manual_exec_{automation_id[:8]}",
    )

    logger.info(
        f"[AUTOMATION] Manual trigger: automation_id={automation_id} "
        f"execution_id={execution_id}"
    )

    return {
        "execution_id": execution_id,
        "automation_id": automation_id,
        "status": "triggered",
    }
