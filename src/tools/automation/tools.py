"""
Automation tools for creating and managing scheduled automations.

These tools run on the HOST (not in sandbox) and have direct database access.
They allow the agent to create, inspect, and manage automations during conversations.

- check_automations: List all or inspect a specific automation + executions
- create_automation: Create a new scheduled automation (cron or one-time)
- manage_automation: Update, pause, resume, trigger, or delete automations
"""

import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from croniter import croniter
from fastapi import HTTPException
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from pydantic import ValidationError

from src.server.database import automation as auto_db
from src.server.handlers import automation_handler as auto_handler
from src.server.models.automation import AutomationCreate
from src.server.utils.error_sanitization import validation_error_text
from src.utils.timezone_utils import zone_or_none

logger = logging.getLogger(__name__)


# ==================== Helpers ====================


def _get_user_id(config: RunnableConfig) -> str:
    """Extract user_id from the runnable config."""
    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        raise ValueError("user_id not found in config.")
    return user_id


def _get_workspace_id(config: RunnableConfig) -> str | None:
    """Extract workspace_id from the runnable config."""
    configurable = config.get("configurable", {})
    return configurable.get("workspace_id")


def _get_timezone(config: RunnableConfig) -> str:
    """Extract user timezone from the runnable config, defaulting to UTC."""
    configurable = config.get("configurable", {})
    return configurable.get("timezone") or "UTC"


def _get_agent_mode(config: RunnableConfig) -> str:
    """Extract agent_mode from the runnable config, defaulting to 'flash'."""
    configurable = config.get("configurable", {})
    return configurable.get("agent_mode") or "flash"


def _parse_schedule(schedule: str, tz: str) -> dict[str, Any]:
    """Auto-detect cron vs one-time from schedule string.

    A one-time datetime with no offset is read on the user's clock (`tz`),
    the clock the agent is shown the current time on.

    Returns:
        Dict with trigger_type and either cron_expression or next_run_at.

    Raises:
        ValueError: If schedule is neither valid cron nor valid ISO datetime.
    """
    try:
        croniter(schedule)
        return {"trigger_type": "cron", "cron_expression": schedule}
    except (ValueError, KeyError):
        try:
            dt = datetime.fromisoformat(schedule)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=zone_or_none(tz) or timezone.utc)
            return {"trigger_type": "once", "next_run_at": dt}
        except ValueError:
            raise ValueError(
                f"Invalid schedule: '{schedule}'. "
                f"Use a cron expression (e.g. '0 9 * * 1-5') or "
                f"ISO datetime (e.g. '2026-03-01T10:00:00')."
            )


def _disable_reason(automation: dict[str, Any]) -> dict[str, Any]:
    """Why a disabled automation was switched off, when a disable said so."""
    if automation.get("status") != "disabled":
        return {}
    reason = automation.get("disable_reason")
    return {"disable_reason": reason} if reason else {}


def _error_text(e: ValueError) -> str:
    return validation_error_text(e) if isinstance(e, ValidationError) else str(e)


def _serialize(obj: Any) -> Any:
    """Convert non-serializable types (datetime, UUID) for clean LLM output."""
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    return obj


# ==================== Tools ====================


@tool(response_format="content_and_artifact")
async def check_automations(
    config: RunnableConfig,
    automation_id: Annotated[
        str | None,
        "Automation ID to inspect. Omit to list all automations.",
    ] = None,
) -> tuple[str, dict]:
    """Check automations. Without an ID, lists all automations. With an ID, returns full details and recent execution history.

    Returns:
        Each automation's status, and each recent run's with the reason it was skipped or failed.

    A waiting run starts once the turn on its thread ends. A disabled automation's
    disable_reason is provider_auth (the provider rejected the user's own key: it
    runs again once the key is fixed and the automation resumed) or max_failures.
    A run's failure_reason usage_limit is a usage limit, which never disables it.
    """
    try:
        user_id = _get_user_id(config)

        if automation_id is None:
            automations, total = await auto_db.list_automations(user_id)
            result = _serialize(
                {
                    "automations": [
                        {
                            "automation_id": a["automation_id"],
                            "name": a["name"],
                            "status": a["status"],
                            "agent_mode": a["agent_mode"],
                            "schedule": a.get("cron_expression")
                            or (
                                a["next_run_at"].isoformat()
                                if a.get("next_run_at")
                                else None
                            ),
                            "next_run_at": a.get("next_run_at"),
                            **({"trigger_config": a.get("trigger_config")} if a.get("trigger_type") == "price" else {}),
                            **_disable_reason(a),
                        }
                        for a in automations
                    ],
                    "total": total,
                }
            )
            artifact = _serialize(
                {
                    "type": "automations",
                    "mode": "list",
                    "automations": [
                        {
                            "automation_id": a["automation_id"],
                            "name": a["name"],
                            "status": a["status"],
                            "agent_mode": a["agent_mode"],
                            "schedule": a.get("cron_expression")
                            or (
                                a["next_run_at"].isoformat()
                                if a.get("next_run_at")
                                else None
                            ),
                            "next_run_at": a.get("next_run_at"),
                            "trigger_type": a.get("trigger_type", "cron"),
                            **({"trigger_config": a.get("trigger_config")} if a.get("trigger_type") == "price" else {}),
                            **_disable_reason(a),
                        }
                        for a in automations
                    ],
                    "total": total,
                }
            )
            return json.dumps(result), artifact

        # Get details + last 5 executions
        automation = await auto_db.get_automation(automation_id, user_id)
        if not automation:
            return json.dumps({"error": f"Automation '{automation_id}' not found."}), {}

        executions, _ = await auto_db.list_executions(
            user_id, automation_id=automation_id, limit=5
        )
        exec_total = await auto_db.count_executions(automation_id)

        result = _serialize(
            {
                "automation": {
                    "automation_id": automation["automation_id"],
                    "name": automation["name"],
                    "description": automation.get("description"),
                    "instruction": automation["instruction"],
                    "trigger_type": automation["trigger_type"],
                    "schedule": automation.get("cron_expression")
                    or (
                        automation["next_run_at"].isoformat()
                        if automation.get("next_run_at")
                        else None
                    ),
                    **({"trigger_config": automation.get("trigger_config")} if automation.get("trigger_type") == "price" else {}),
                    "agent_mode": automation["agent_mode"],
                    "thread_strategy": automation.get("thread_strategy", "new"),
                    "conversation_thread_id": automation.get("conversation_thread_id"),
                    "status": automation["status"],
                    **_disable_reason(automation),
                    "next_run_at": automation.get("next_run_at"),
                    "last_run_at": automation.get("last_run_at"),
                    "created_at": automation.get("created_at"),
                },
                "executions": [
                    {
                        "execution_id": e["automation_execution_id"],
                        "status": e["status"],
                        "scheduled_at": e.get("scheduled_at"),
                        "started_at": e.get("started_at"),
                        "completed_at": e.get("completed_at"),
                        "error_message": e.get("error_message"),
                        **({"skip_reason": e["skip_reason"]} if e.get("skip_reason") else {}),
                        **({"failure_reason": e["failure_reason"]} if e.get("failure_reason") else {}),
                    }
                    for e in executions
                ],
                "total_executions": exec_total,
            }
        )
        artifact = {
            "type": "automations",
            "mode": "detail",
            "automation": result["automation"],
            "executions": result["executions"],
            "total_executions": exec_total,
        }
        return json.dumps(result), artifact

    except Exception as e:
        logger.exception("[automation_tools] check_automations error")
        return json.dumps({"error": str(e)}), {}


@tool(response_format="content_and_artifact")
async def create_automation(
    name: Annotated[str, "Short name for the automation"],
    instruction: Annotated[str, "The prompt the agent will execute on each run"],
    config: RunnableConfig,
    schedule: Annotated[
        str | None,
        "Cron expression (e.g. '0 9 * * 1-5') for recurring, "
        "or ISO datetime (e.g. '2026-03-01T10:00:00') for one-time, in the user's local time. "
        "Required for cron/once triggers. Omit for price triggers.",
    ] = None,
    trigger_type: Annotated[
        str | None,
        "Trigger type: 'cron', 'once', or 'price'. "
        "Auto-detected from schedule if omitted.",
    ] = None,
    trigger_config: Annotated[
        dict | None,
        "Price trigger config (required when trigger_type='price'). "
        "Schema: {symbol: 'AAPL', market: 'stock', conditions: [{type: 'price_below', value: 150}], "
        "retrigger: {mode: 'one_shot'}}. "
        "market: 'stock' (default) or 'index'. For index, use bare symbol (e.g. 'SPX', 'DJI', 'VIX'). "
        "Condition types: price_above, price_below, pct_change_above, pct_change_below. "
        "Retrigger modes: one_shot (fire once, default), "
        "recurring (re-arm after cooldown; omit cooldown_seconds for once-per-trading-day default, "
        "or set cooldown_seconds >= 14400 for custom interval).",
    ] = None,
    description: Annotated[str | None, "Optional description"] = None,
    thread: Annotated[
        str | None,
        "Thread mode: 'new' (fresh thread each run, default), "
        "'persistent' (single thread for all runs), "
        "'current' (continue in this conversation thread)",
    ] = None,
    delivery: Annotated[str | None, "Comma-separated delivery methods (e.g. 'slack'). Omit to skip delivery."] = None,
) -> tuple[str, dict]:
    """Create a new automation — scheduled (cron/once) or price-triggered."""
    try:
        user_id = _get_user_id(config)
        tz = _get_timezone(config)

        # Which argument names the trigger; the model checks the trigger itself.
        if trigger_config and schedule:
            return json.dumps({"error": "Provide schedule (for cron/once) or trigger_config (for price), not both"}), {}
        if trigger_type == "price" or trigger_config:
            trigger = {"trigger_type": "price", "trigger_config": trigger_config}
        elif schedule:
            trigger = _parse_schedule(schedule, tz)
        else:
            return json.dumps({"error": "Either schedule or trigger_config is required"}), {}

        # Resolve thread strategy
        thread_strategy = "new"
        conversation_thread_id = None
        if thread == "persistent":
            thread_strategy = "continue"
        elif thread == "current":
            thread_strategy = "continue"
            conversation_thread_id = config.get("configurable", {}).get("thread_id")

        delivery_config = None
        delivery_warning = None
        if delivery:
            delivery_config = {"methods": [m.strip() for m in delivery.split(",")]}
            from src.config import settings
            if not settings.AUTOMATION_WEBHOOK_URL:
                delivery_warning = (
                    "Delivery methods were saved, but AUTOMATION_WEBHOOK_URL is not configured. "
                    "Delivery will not work until the webhook URL is set."
                )

        automation = await auto_handler.create_automation(
            user_id,
            AutomationCreate(
                name=name,
                instruction=instruction,
                description=description,
                agent_mode=_get_agent_mode(config),
                timezone=tz,
                workspace_id=_get_workspace_id(config),
                thread_strategy=thread_strategy,
                conversation_thread_id=conversation_thread_id,
                delivery_config=delivery_config,
                **trigger,
            ),
        )

        schedule_display = automation.get("cron_expression") or (
            automation["next_run_at"].isoformat()
            if automation.get("next_run_at")
            else None
        )
        result_data: dict[str, Any] = {
            "success": True,
            "automation_id": automation["automation_id"],
            "name": automation["name"],
            "status": automation["status"],
            "trigger_type": automation["trigger_type"],
            "schedule": schedule_display,
            "next_run_at": automation.get("next_run_at"),
        }
        if automation["trigger_type"] == "price":
            result_data["trigger_config"] = automation.get("trigger_config")
        if delivery_warning:
            result_data["warning"] = delivery_warning
        result = _serialize(result_data)
        artifact = {
            "type": "automations",
            "mode": "created",
            "automation_id": result["automation_id"],
            "name": result["name"],
            "status": result["status"],
            "trigger_type": result["trigger_type"],
            "schedule": result.get("schedule"),
            "next_run_at": result.get("next_run_at"),
        }
        if result.get("trigger_type") == "price":
            artifact["trigger_config"] = result.get("trigger_config")
        return json.dumps(result), artifact

    except ValueError as e:
        return json.dumps({"error": _error_text(e)}), {}
    except HTTPException as e:
        return json.dumps({"error": e.detail}), {}
    except Exception as e:
        logger.exception("[automation_tools] create_automation error")
        return json.dumps({"error": str(e)}), {}


@tool
async def manage_automation(
    automation_id: Annotated[str, "Automation ID to manage"],
    action: Annotated[
        str, "Action: 'update', 'pause', 'resume', 'trigger', or 'delete'"
    ],
    config: RunnableConfig,
    name: Annotated[str | None, "New name (action='update' only)"] = None,
    description: Annotated[str | None, "New description (action='update' only)"] = None,
    instruction: Annotated[
        str | None, "New instruction/prompt (action='update' only)"
    ] = None,
    schedule: Annotated[
        str | None,
        "New cron expression, or ISO datetime for a one-time automation, in the user's local time "
        "(action='update' only). Must keep the automation's kind; to switch between recurring and "
        "one-time, create a new automation.",
    ] = None,
    thread: Annotated[
        str | None,
        "Thread mode: 'new', 'persistent', or 'current' (update only)",
    ] = None,
    delivery: Annotated[str | None, "Comma-separated delivery methods (update only, e.g. 'slack')"] = None,
    remove_delivery: Annotated[bool, "Set to true to remove delivery configuration (update only)"] = False,
) -> dict[str, Any]:
    """Manage an existing automation: update settings, pause, resume, trigger immediately, or delete."""
    try:
        user_id = _get_user_id(config)

        if action == "pause":
            result = await auto_handler.pause_automation(automation_id, user_id)
            if not result:
                return {"error": f"Automation '{automation_id}' not found."}
            return _serialize(
                {
                    "success": True,
                    "status": result["status"],
                }
            )

        elif action == "resume":
            result = await auto_handler.resume_automation(automation_id, user_id)
            if not result:
                return {"error": f"Automation '{automation_id}' not found."}
            return _serialize(
                {
                    "success": True,
                    "status": result["status"],
                    "next_run_at": result.get("next_run_at"),
                }
            )

        elif action == "trigger":
            result = await auto_handler.trigger_automation(automation_id, user_id)
            return _serialize(result)

        elif action == "delete":
            deleted = await auto_db.delete_automation(automation_id, user_id)
            if not deleted:
                return {"error": f"Automation '{automation_id}' not found."}
            return {"success": True, "deleted": automation_id}

        elif action == "update":
            update_data: dict[str, Any] = {}
            if name is not None:
                update_data["name"] = name
            if description is not None:
                update_data["description"] = description
            if instruction is not None:
                update_data["instruction"] = instruction
            delivery_warning = None
            if remove_delivery:
                update_data["delivery_config"] = {}
            elif delivery:
                update_data["delivery_config"] = {
                    "methods": [m.strip() for m in delivery.split(",")],
                }
                from src.config import settings
                if not settings.AUTOMATION_WEBHOOK_URL:
                    delivery_warning = (
                        "Delivery methods were saved, but AUTOMATION_WEBHOOK_URL is not configured. "
                        "Delivery will not work until the webhook URL is set."
                    )
            if thread is not None:
                if thread == "new":
                    update_data["thread_strategy"] = "new"
                    update_data["conversation_thread_id"] = None
                elif thread == "persistent":
                    update_data["thread_strategy"] = "continue"
                elif thread == "current":
                    update_data["thread_strategy"] = "continue"
                    update_data["conversation_thread_id"] = config.get("configurable", {}).get("thread_id")
            if schedule is not None:
                tz = _get_timezone(config)
                schedule_info = _parse_schedule(schedule, tz)
                # A cron's fields are read on the zone stored beside it, so the
                # user's clock the expression was written on travels with it.
                if schedule_info["trigger_type"] == "cron":
                    schedule_info["timezone"] = tz
                update_data.update(schedule_info)

            if not update_data:
                return {
                    "error": "No fields to update. Provide at least one of: "
                    "name, description, instruction, schedule, thread, delivery."
                }

            result = await auto_handler.update_automation(
                automation_id, user_id, update_data
            )
            if not result:
                return {"error": f"Automation '{automation_id}' not found."}
            return _serialize(
                {
                    "success": True,
                    "automation_id": result["automation_id"],
                    "name": result["name"],
                    "status": result["status"],
                    "next_run_at": result.get("next_run_at"),
                    **({"warning": delivery_warning} if delivery_warning else {}),
                }
            )

        else:
            return {
                "error": f"Unknown action: '{action}'. "
                f"Use 'update', 'pause', 'resume', 'trigger', or 'delete'."
            }

    except ValueError as e:
        return {"error": _error_text(e)}
    except HTTPException as e:
        return {"error": e.detail}
    except Exception as e:
        logger.exception("[automation_tools] manage_automation error")
        return {"error": str(e)}
