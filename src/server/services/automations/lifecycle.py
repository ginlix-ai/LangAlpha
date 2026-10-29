"""Automation create, update and control: the rules every surface shares.

The REST API and the automation files both write through here, so a
rule lives once. Checks the request models can make alone
(schedule, a known zone) are theirs; the ones that need the stored row, the
user's targets or their models are made here. The guards against an agent's
slips (a past or date-only time, a fixed-offset zone) are the models
module's helpers, which only the agent's surfaces call. A refusal carries the
field it is about, so an automation's file can point at that field.
"""

import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import httpx
from fastapi import HTTPException

from src.config import settings
from src.llms.llm import get_configured_llm_models
from src.llms.preferences import custom_model_names
from src.server.database import automation as auto_db
from src.server.database import automation_executions as exec_db
from src.server.database import conversation as conversation_db
from src.server.database.home_workspace import get_home_workspace_id, is_home_id
from src.server.database.workspace import (
    HomeWorkspaceTaken,
    get_or_create_home_workspace,
    get_workspace,
)
from src.server.models.automation import (
    AutomationCreate,
    AutomationUpdate,
    DeliveryConfig,
    delivery_overflow,
    on_clock,
)
from src.server.services.automation_scheduler import AutomationScheduler
from src.server.services.automation_settlement import Outcome, settle
from src.server.services.llm import user_models
from src.server.utils.api import (
    raise_not_found,
    require_thread_owner,
    require_workspace_owner,
)
from src.server.utils.pg_sanitize import normalize_uuid
from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)


class AutomationRefusal(ValueError):
    """A rule refused the write; REST answers it as a conflict."""

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class DeliveryRefused(AutomationRefusal):
    """Delivery entries that can't be used, each with why.

    ``refusals`` pairs each entry with its reason, which REST answers entry by
    entry; ``problems`` are the same as one line each, which the message
    joins.
    """

    def __init__(self, refusals: list[tuple[str, str]]) -> None:
        self.refusals = refusals
        self.problems = [f"{entry!r}: {why}" for entry, why in refusals]
        super().__init__("; ".join(self.problems), field="delivery_config")


class TargetRefused(HTTPException):
    """A named workspace or thread the user can't use, answered 404 or 403 as
    REST always has, with the field that named it."""

    def __init__(self, refused: HTTPException, *, field: str) -> None:
        super().__init__(status_code=refused.status_code, detail=refused.detail)
        self.field = field


async def require_target_ownership(
    user_id: str,
    workspace_id: UUID | str | None,
    conversation_thread_id: UUID | str | None,
    *,
    conn=None,
) -> None:
    """Verify caller-supplied automation targets belong to ``user_id``.

    The check mostly holds for the row's life: nothing reassigns
    ``workspaces.user_id`` and nothing moves a thread between workspaces, so a
    workspace owned at write time stays owned. A pinned thread is the gap:
    deleting one hard-deletes the row and frees its id for whoever posts to it
    next, and the column carries no FK to catch that. Raises 404/403, never
    ValueError, which these routes map to 409.
    """
    if workspace_id:
        try:
            require_workspace_owner(
                await get_workspace(str(workspace_id), conn=conn), user_id=user_id
            )
        except HTTPException as exc:
            raise TargetRefused(exc, field="workspace_id") from None
    if conversation_thread_id:
        try:
            await require_thread_owner(str(conversation_thread_id), user_id, conn=conn)
        except HTTPException as exc:
            raise TargetRefused(exc, field="conversation_thread_id") from None


_NO_WORKSPACE = (
    "workspace_id is required: the workspace each run happens in, or Home's "
    "for the user's Home"
)


async def _home(user_id: str, conn) -> str:
    """Home's id, once its row is the user's, made first if they have none.

    Home may be named before the user has ever opened it, as a turn may name
    it; a run puts it on the user's computer as any turn there does. Read
    before the upsert, which would hold the row's lock for the rest of a
    caller's transaction.
    """
    home_id = get_home_workspace_id(user_id)
    row = await get_workspace(home_id, conn=conn)
    try:
        if row is None:
            await get_or_create_home_workspace(user_id, conn=conn)
        else:
            require_workspace_owner(row, user_id=user_id)
    except HomeWorkspaceTaken:
        refused = HTTPException(status_code=403, detail="Forbidden")
        raise TargetRefused(refused, field="workspace_id") from None
    except HTTPException as exc:
        raise TargetRefused(exc, field="workspace_id") from None
    return home_id


async def _placement(user_id: str, workspace_id: UUID | str | None, conn) -> str:
    """The workspace a write stores, so every automation runs in one the
    user owns."""
    if not workspace_id:
        raise AutomationRefusal(_NO_WORKSPACE, field="workspace_id")
    if is_home_id(user_id, workspace_id):
        return await _home(user_id, conn)
    workspace_id = str(workspace_id).lower()
    await require_target_ownership(user_id, workspace_id, None, conn=conn)
    return workspace_id


def _place(user_id: str, workspace_id: str) -> str:
    if is_home_id(user_id, workspace_id):
        return "the user's Home"
    return f"workspace {workspace_id}"


async def _require_pin_in(
    user_id: str, thread_id: str, workspace_id: str, conn
) -> None:
    """Refuse a pinned thread outside the workspace the automation names.

    A run continues its pinned thread only in the automation's workspace, so
    a pin elsewhere would never be continued. Read after the ownership check,
    which refuses a thread that isn't the user's.
    """
    thread = await conversation_db.get_thread_auth_meta(thread_id, conn=conn)
    there = str(thread["workspace_id"]).lower() if thread else None
    if there is None or there == workspace_id:
        return
    raise AutomationRefusal(
        f"thread {thread_id} is in {_place(user_id, there)}, but the automation "
        f"runs in {_place(user_id, workspace_id)}: pin a thread from "
        f"{_place(user_id, workspace_id)}, or run the automation in "
        f"{_place(user_id, there)}",
        field="conversation_thread_id",
    )


async def _check_model(user_id: str, name: str, pref: dict[str, Any] | None) -> None:
    """Refuse a model the run would reject: an unknown name fails every
    firing until the automation is disabled."""
    if pref is None:
        pref = await user_models.get_model_preference(user_id)
    source, _ = await user_models.classify_model(user_id, name, _pref_cache=pref)
    if source != user_models.ModelSource.UNKNOWN:
        return
    if await user_models.get_custom_provider_config(user_id, name, _pref_cache=pref):
        return
    names = [
        *sorted(custom_model_names(pref)),
        *sorted(n for group in get_configured_llm_models().values() for n in group),
    ]
    raise AutomationRefusal(
        f"unknown model {name!r}; use null for the user's default, or one of: {', '.join(names)}",
        field="llm_model",
    )


def delivery_warning(methods: Sequence[str] | None) -> str | None:
    """What to tell whoever gave an automation a channel the server can't
    post to yet. With a messaging service, a run's agent sends the results
    itself."""
    if not methods or settings.AUTOMATION_WEBHOOK_URL or messaging.messaging_enabled():
        return None
    return (
        "Delivery was saved, but AUTOMATION_WEBHOOK_URL is not configured, so runs "
        "post only in the app until it is set."
    )


_CHECK_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


def names_chat(method: str) -> bool:
    """Whether a delivery entry names one chat ("slack:T1/C0123") rather
    than an app ("slack")."""
    return ":" in method


def checks_entry(method: str) -> bool:
    """Whether saving ``method`` anew asks the messaging service about it: a
    chat always, and with a service, an app too, which it refuses while the
    user hasn't linked that app."""
    return names_chat(method) or messaging.messaging_enabled()


async def _check_chat(user_id: str, address: str) -> tuple[str | None, str | None]:
    """The entry as the messaging service files it, or why it can't take an
    automation's results."""
    try:
        answer = await messaging.gateway_request(
            "POST",
            "/agent/check-target",
            user_id=user_id,
            timeout=_CHECK_TIMEOUT,
            body={"address": address, "purpose": "automation"},
        )
    except messaging.GatewayError as exc:
        return None, f"couldn't be checked: {exc.message} Try again shortly."
    data = answer.data or {}
    if answer.status != 200 or not isinstance(data.get("ok"), bool):
        logger.warning("[AUTOMATION] check-target answered %s", answer.status)
        failed = f"the messaging service failed ({answer.status})"
        return None, f"couldn't be checked: {failed}. Try again shortly."
    if not data["ok"]:
        return None, str(data.get("message") or "the messaging service refused it")
    canonical = data.get("address")
    return canonical if isinstance(canonical, str) and canonical else address, None


async def check_delivery(
    user_id: str, methods: Sequence[str], *, stored: Sequence[str] = ()
) -> list[str]:
    """``methods`` with each chat address as the messaging service files it.
    An app name is kept as written, and so is an entry already in
    ``stored``, which was checked when it was saved. With a messaging
    service, a new app name is checked too, so one the user hasn't linked is
    refused; it still follows the app's chain, so it is never replaced by the
    chat the service answers. Entries past the limits (``delivery_overflow``)
    are refused before anything is asked, stored ones included, since a run
    hands the service the whole list.

    Raises:
        DeliveryRefused: naming every entry that can't be used, and why
    """
    overflow = delivery_overflow(list(methods))
    if overflow:
        raise DeliveryRefused(overflow)
    fresh = [m for m in dict.fromkeys(methods) if checks_entry(m) and m not in stored]
    if not fresh:
        return list(dict.fromkeys(methods))
    if not messaging.messaging_enabled():
        raise DeliveryRefused(
            [
                (
                    m,
                    "naming a chat needs a connected messaging service, and this "
                    'server has none; name an app such as "slack" instead',
                )
                for m in fresh
            ]
        )
    answers = await asyncio.gather(*(_check_chat(user_id, m) for m in fresh))
    refusals = [(m, why) for m, (_, why) in zip(fresh, answers) if why]
    if refusals:
        raise DeliveryRefused(refusals)
    canonical = {
        m: address for m, (address, _) in zip(fresh, answers) if names_chat(m)
    }
    return list(dict.fromkeys(canonical.get(m, m) for m in methods))


async def create_automation(
    user_id: str,
    data: AutomationCreate,
    *,
    conn=None,
    model_pref: dict[str, Any] | None = None,
    file_name: str | None = None,
    delivery_checked: bool = False,
) -> Dict[str, Any]:
    """Create a new automation, filed as ``file_name`` or else as one derived
    from its name. ``conn`` joins the write to a caller's transaction, as a
    save of an automation's file does. Such a caller passes the user's
    ``model_pref``, read before it took its locks: read here, it would hold a
    second pool connection while they wait. For the same reason it checks the
    delivery's chats itself (``check_delivery``) and says so with
    ``delivery_checked``.

    Raises:
        AutomationRefusal: no workspace, a pinned thread in another workspace,
            or a model the user can't run
        DeliveryRefused: a delivery chat that can't be used
        TargetRefused: a workspace or thread that isn't the user's
    """
    # A price trigger has no next_run_at: the price monitor fires it.
    next_run_at = (
        AutomationScheduler.calculate_first_run(data.cron_expression, data.timezone)
        if data.trigger_type == "cron"
        else data.next_run_at
    )

    workspace_id = await _placement(user_id, data.workspace_id, conn)
    await require_target_ownership(
        user_id, None, data.conversation_thread_id, conn=conn
    )
    if data.conversation_thread_id:
        await _require_pin_in(
            user_id, str(data.conversation_thread_id), workspace_id, conn
        )
    if data.llm_model:
        await _check_model(user_id, data.llm_model, model_pref)
    if data.delivery_config and not delivery_checked:
        methods = await check_delivery(user_id, data.delivery_config.methods)
        delivery = DeliveryConfig(methods=methods)
        data = data.model_copy(update={"delivery_config": delivery})

    automation = await auto_db.create_automation(
        user_id=user_id,
        name=data.name,
        file_name=file_name,
        trigger_type=data.trigger_type,
        instruction=data.instruction,
        description=data.description,
        cron_expression=data.cron_expression,
        timezone=data.timezone,
        trigger_config=data.trigger_config,
        next_run_at=next_run_at,
        workspace_id=workspace_id,
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
        conn=conn,
    )

    logger.info(
        f"[AUTOMATION] Created automation {automation['automation_id']} "
        f"for user {user_id} (trigger={data.trigger_type}, next_run={next_run_at})"
    )
    return automation


async def _current(
    automation_id: str, user_id: str, conn, current: Dict[str, Any] | None
) -> Optional[Dict[str, Any]]:
    if current is not None:
        return current
    return await auto_db.get_automation(automation_id, user_id, conn=conn)


async def update_automation(
    automation_id: str,
    user_id: str,
    fields: Dict[str, Any],
    *,
    conn=None,
    model_pref: dict[str, Any] | None = None,
    current: Dict[str, Any] | None = None,
    delivery_checked: bool = False,
) -> Optional[Dict[str, Any]]:
    """Update an automation with validation and next_run_at recalculation.

    Args:
        automation_id: Automation UUID
        user_id: Owner user ID
        fields: The fields to change, read through AutomationUpdate against
            the stored kind of trigger. REST drops None fields before they
            get here, so a None ``conversation_thread_id`` was named on
            purpose: the agent's thread "new" clearing the pin.
        conn, model_pref, delivery_checked: as ``create_automation`` takes them
        current: the row as a caller holding the user's automation locks
            already read it, which spares reading it again.

    Returns:
        Updated automation dict, or None if not found

    Raises:
        ValidationError: A field AutomationUpdate refuses, such as a schedule
            field of another kind than the stored one
        AutomationRefusal: A write naming no workspace, a new pin or a move
            that leaves the pinned thread in another workspace, or a model
            the user can't run
        DeliveryRefused: a delivery chat not already saved that can't be used
        TargetRefused: a workspace or thread that isn't the user's
    """
    current = await _current(automation_id, user_id, conn, current)
    if not current:
        return None

    update = AutomationUpdate.model_validate(
        fields, context={"trigger_type": current["trigger_type"]}
    )
    update_kwargs = update.model_dump(exclude_unset=True, exclude={"trigger_type"})
    for field in ("workspace_id", "conversation_thread_id"):
        if update_kwargs.get(field) is not None:
            update_kwargs[field] = str(update_kwargs[field])

    stored_in = normalize_uuid(current.get("workspace_id"))
    # A named workspace is a move to it.
    if "workspace_id" in update.model_fields_set:
        update_kwargs["workspace_id"] = await _placement(
            user_id, update.workspace_id, conn
        )
        # The pinned thread stays where it is, so a move that names no thread
        # unpins it: the next run starts one in the new place.
        if (
            current.get("conversation_thread_id")
            and "conversation_thread_id" not in update.model_fields_set
            and update_kwargs["workspace_id"] != stored_in
        ):
            update_kwargs["conversation_thread_id"] = None

    await require_target_ownership(
        user_id, None, update.conversation_thread_id, conn=conn
    )
    # Only a new pin, a move, or a switch to continuing is checked: the stored
    # pin was checked where it was written, and the fold that moves a thread
    # moves its automations. A row that starts a new thread each run can hold
    # an older pin nothing checked.
    stored_pin = normalize_uuid(current.get("conversation_thread_id"))
    if pin := update_kwargs.get("conversation_thread_id", stored_pin):
        runs_in = update_kwargs.get("workspace_id") or stored_in
        continues = (
            update.thread_strategy == "continue"
            and current.get("thread_strategy") != "continue"
        )
        if pin != stored_pin or runs_in != stored_in or continues:
            # A row whose workspace was deleted runs nowhere until moved.
            if runs_in is None:
                raise AutomationRefusal(_NO_WORKSPACE, field="workspace_id")
            await _require_pin_in(user_id, pin, runs_in, conn)
    # Only a new name is checked: a stored one the user can no longer run
    # must not block an edit to anything else.
    if update.llm_model and update.llm_model != current.get("llm_model"):
        await _check_model(user_id, update.llm_model, model_pref)
    # Likewise only a chat not already saved.
    delivery = update_kwargs.get("delivery_config")
    if isinstance(delivery, dict) and delivery.get("methods") and not delivery_checked:
        stored = (current.get("delivery_config") or {}).get("methods") or []
        methods = await check_delivery(user_id, delivery["methods"], stored=stored)
        update_kwargs["delivery_config"] = {**delivery, "methods": methods}

    # Recalculate next_run_at if cron expression or timezone changed. Only an
    # active row gets one: a paused or disabled cron keeps the none that pause
    # left it, and resume computes it from the edited expression and zone.
    new_cron = update.cron_expression
    new_tz = update.timezone
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

    result = await auto_db.update_automation(
        automation_id, user_id, conn=conn, **update_kwargs
    )
    if result:
        logger.info(
            f"[AUTOMATION] Updated automation {automation_id} "
            f"fields={list(update_kwargs.keys())}"
        )
    return result


async def pause_automation(
    automation_id: str,
    user_id: str,
    *,
    conn=None,
    current: Dict[str, Any] | None = None,
) -> Optional[Dict[str, Any]]:
    """Pause an active automation; ``current`` as ``update_automation``
    takes it.

    A one-time automation keeps its next_run_at: that time is its whole
    schedule, and resume has nowhere else to read it from. Keeping it is safe
    because the scheduler claims only 'active' rows. A cron's is recomputed
    on resume, so it is cleared.
    """
    current = await _current(automation_id, user_id, conn, current)
    if not current:
        return None

    if current["status"] != "active":
        raise AutomationRefusal(
            f"Cannot pause automation in '{current['status']}' status "
            f"(must be 'active')",
            field="status",
        )

    update_kwargs: Dict[str, Any] = {"status": "paused"}
    if current["trigger_type"] != "once":
        update_kwargs["next_run_at"] = None
    return await auto_db.update_automation(
        automation_id, user_id, conn=conn, **update_kwargs
    )


async def resume_automation(
    automation_id: str,
    user_id: str,
    *,
    conn=None,
    current: Dict[str, Any] | None = None,
) -> Optional[Dict[str, Any]]:
    """Resume a paused/disabled automation (recalculates next_run_at, resets
    failures); ``current`` as ``update_automation`` takes it."""
    current = await _current(automation_id, user_id, conn, current)
    if not current:
        return None

    if current["status"] not in ("paused", "disabled"):
        raise AutomationRefusal(
            f"Cannot resume automation in '{current['status']}' status "
            f"(must be 'paused' or 'disabled')",
            field="status",
        )

    # The reason goes with the status it explained, in the same write.
    update_kwargs: Dict[str, Any] = {
        "status": "active",
        "failure_count": 0,
        "disable_reason": None,
    }

    if current["trigger_type"] == "cron" and current.get("cron_expression"):
        update_kwargs["next_run_at"] = AutomationScheduler.calculate_first_run(
            current["cron_expression"],
            current.get("timezone", "UTC"),
        )
    elif current["trigger_type"] == "once":
        when = current.get("next_run_at")
        if when and when > datetime.now(timezone.utc):
            update_kwargs["next_run_at"] = when
        elif current["status"] == "disabled":
            # Switched off by the run it already had: back on, it waits for
            # Run now, as a one-time automation whose run failed does.
            update_kwargs["next_run_at"] = None
        else:
            passed = f" ({on_clock(when, current.get('timezone'))})" if when else ""
            raise AutomationRefusal(
                f"Cannot resume a one-time automation whose scheduled time has passed{passed}; "
                "set next_run_at to a future time first",
                field="status",
            )
    # A price trigger has no next_run_at: it is only re-activated.

    return await auto_db.update_automation(
        automation_id, user_id, conn=conn, **update_kwargs
    )


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

    scheduler = AutomationScheduler.get_instance()
    execution_id = await exec_db.create_execution(
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
        if await exec_db.get_execution_status(
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
    if not await exec_db.dismiss_execution(execution_id, automation_id=automation_id):
        if await exec_db.get_execution_status(
            execution_id, automation_id=automation_id
        ) is None:
            raise_not_found("Execution")
        raise ValueError("Only a failed run can be dismissed")
    return await auto_db.get_automation(automation_id, user_id)
