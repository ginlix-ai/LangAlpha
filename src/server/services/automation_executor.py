"""
AutomationExecutor — Executes a single automation run.

Shared by all trigger types (time-based now, event-based later).
Builds a ChatRequest, invokes the appropriate agent workflow,
and drains the async generator (no HTTP client to consume SSE).
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from src.server.database import automation as auto_db
from src.server.models.automation import PriceTriggerConfig, RetriggerMode
from src.server.database.workspace import get_or_create_flash_workspace
from src.server.models.chat import ChatMessage, ChatRequest
from src.server.services.webhook_client import WebhookClient

logger = logging.getLogger(__name__)


class AutomationExecutor:
    """Singleton that executes automation runs."""

    _instance: Optional["AutomationExecutor"] = None

    @classmethod
    def get_instance(cls) -> "AutomationExecutor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _fire_webhook(
        self,
        event: str,
        automation: Dict[str, Any],
        execution_id: str,
        thread_id: str | None,
        workspace_id: str | None,
        error: str | None = None,
    ) -> list[dict] | None:
        """Fire webhook event. Never raises. Returns per-method results."""
        try:
            return await WebhookClient().fire_event(
                event, automation, execution_id, thread_id, workspace_id, error=error
            )
        except Exception as e:
            logger.error(f"[AUTOMATION_EXEC] Webhook fire failed: {e}")
            return None

    async def execute(
        self,
        automation: Dict[str, Any],
        execution_id: str,
    ) -> None:
        """Execute a single automation.

        Steps:
        1. Mark execution as running
        2. Resolve workspace (flash auto-creates, ptc validates)
        3. Determine thread_id (new or continue)
        4. Build ChatRequest and invoke agent workflow
        5. Drain the async generator
        6. Update execution record (completed/failed)
        7. Handle failure counting and one-time completion

        Args:
            automation: Full automation row dict from DB
            execution_id: The automation_execution_id to track this run
        """
        automation_id = str(automation["automation_id"])
        user_id = automation["user_id"]
        agent_mode = automation["agent_mode"]
        instruction = automation["instruction"]

        logger.info(
            f"[AUTOMATION_EXEC] Starting execution: "
            f"automation_id={automation_id} execution_id={execution_id} "
            f"mode={agent_mode}"
        )

        # Mark as running
        await auto_db.update_execution_status(
            execution_id,
            "running",
            started_at=datetime.now(timezone.utc),
        )

        thread_id = None
        try:
            # ─── Resolve workspace ─────────────────────────────────
            workspace_id = None
            if agent_mode == "flash":
                flash_ws = await get_or_create_flash_workspace(user_id)
                workspace_id = str(flash_ws["workspace_id"])
            elif agent_mode == "ptc":
                ws_id = automation.get("workspace_id")
                if not ws_id:
                    raise ValueError(
                        "PTC mode requires a workspace_id, but automation has none "
                        "(workspace may have been deleted)"
                    )
                workspace_id = str(ws_id)

            # ─── Determine thread_id ───────────────────────────────
            thread_strategy = automation.get("thread_strategy", "new")

            if thread_strategy == "continue" and automation.get("conversation_thread_id"):
                thread_id = str(automation["conversation_thread_id"])
            else:
                thread_id = str(uuid4())
                # If strategy is 'continue' but no pinned thread yet, pin this one
                if thread_strategy == "continue":
                    await auto_db.update_automation(
                        automation_id, user_id,
                        conversation_thread_id=thread_id,
                    )

            # ─── Build ChatRequest ─────────────────────────────────
            additional_context = automation.get("additional_context")

            request = ChatRequest(
                agent_mode=agent_mode,
                workspace_id=workspace_id,
                messages=[
                    ChatMessage(role="user", content=instruction),
                ],
                llm_model=automation.get("llm_model"),
                additional_context=additional_context,
            )

            # ─── Invoke agent workflow ─────────────────────────────
            from src.server.handlers.chat import (
                astream_flash_workflow,
                astream_ptc_workflow,
            )

            if agent_mode == "flash":
                generator = astream_flash_workflow(
                    request=request,
                    thread_id=thread_id,
                    user_input=instruction,
                    user_id=user_id,
                )
            else:
                generator = astream_ptc_workflow(
                    request=request,
                    thread_id=thread_id,
                    user_input=instruction,
                    user_id=user_id,
                    workspace_id=workspace_id,
                )

            # Notify webhooks: started
            await self._fire_webhook("automation.started", automation, execution_id, thread_id, workspace_id)

            # Drain the async generator — no HTTP client to consume SSE
            event_count = 0
            async for _event in generator:
                event_count += 1

            logger.info(
                f"[AUTOMATION_EXEC] Workflow complete: "
                f"execution_id={execution_id} events={event_count}"
            )

            # ─── Success ───────────────────────────────────────────
            await auto_db.update_execution_status(
                execution_id,
                "completed",
                conversation_thread_id=thread_id,
                completed_at=datetime.now(timezone.utc),
            )

            # Reset failure count on success
            await auto_db.reset_failure_count(automation_id)

            # Notify webhooks: completed
            delivery_result = await self._fire_webhook("automation.completed", automation, execution_id, thread_id, workspace_id)
            if delivery_result is not None:
                await auto_db.update_execution_status(
                    execution_id, "completed", delivery_result=delivery_result,
                )

            # Mark one-time / one-shot automations as completed
            if automation["trigger_type"] == "once":
                await auto_db.update_automation_next_run(
                    automation_id, next_run_at=None, status="completed"
                )
            elif automation["trigger_type"] == "price":
                tc = automation.get("trigger_config") or {}
                config = PriceTriggerConfig(**tc)
                if config.retrigger.mode == RetriggerMode.ONE_SHOT:
                    await auto_db.update_automation_next_run(
                        automation_id, next_run_at=None, status="completed"
                    )
                else:
                    # Recurring — restore to active only if still 'executing'
                    # (user may have paused/disabled during execution)
                    await auto_db.restore_executing_to_active(automation_id)

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)[:500]}"
            logger.error(
                f"[AUTOMATION_EXEC] Execution failed: "
                f"execution_id={execution_id} error={error_msg}"
            )

            # Mark execution as failed
            await auto_db.update_execution_status(
                execution_id,
                "failed",
                conversation_thread_id=thread_id,
                error_message=error_msg,
                completed_at=datetime.now(timezone.utc),
            )

            # Increment failure count (may auto-disable)
            await auto_db.increment_failure_count(automation_id)

            # Restore price automations from 'executing' to 'active' on failure
            # (increment_failure_count may have set 'disabled' — only restore if still 'executing')
            if automation.get("trigger_type") == "price":
                await auto_db.restore_executing_to_active(automation_id)

            # Notify webhooks: failed
            delivery_result = await self._fire_webhook(
                "automation.failed", automation, execution_id, thread_id, workspace_id, error=error_msg
            )
            if delivery_result is not None:
                await auto_db.update_execution_status(
                    execution_id, "failed", delivery_result=delivery_result,
                )
