"""Generic webhook client for firing automation lifecycle events."""

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

import httpx

logger = logging.getLogger(__name__)


class WebhookClient:
    """Fires webhook events to configured URLs with optional HMAC signing."""

    async def fire(self, url: str, payload: dict, secret: str | None = None) -> bool:
        """POST JSON payload to url with optional HMAC-SHA256 signature.

        Returns True on 2xx, False otherwise. Never raises: this is the one
        guard on a delivery, so a method that fails leaves the others sent.
        """
        try:
            body = json.dumps(payload, default=str)
            headers = {"Content-Type": "application/json"}
            if secret:
                sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
                headers["X-Webhook-Signature"] = f"sha256={sig}"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, content=body, headers=headers)
                if not resp.is_success:
                    logger.warning(
                        f"[WEBHOOK] Non-2xx response: url={url} status={resp.status_code}"
                    )
                return resp.is_success
        except Exception as e:
            logger.error(f"[WEBHOOK] Request failed: url={url} error={e}")
            return False

    async def fire_event(
        self,
        event: str,
        automation: Dict[str, Any],
        execution_id: str,
        thread_id: str | None,
        workspace_id: str | None,
        error: str | None = None,
        run_id: str | None = None,
        failure_reason: str | None = None,
    ) -> list[dict] | None:
        """Fire an event to all configured delivery methods.

        Reads delivery_config.methods from the automation and resolves
        the webhook URL and secret from environment variables. A failed
        delivery never raises: ``fire`` reports it as that method's result.

        ``run_id`` names the turn this execution ran. A thread can hold a
        newer turn by the time the event lands (the next firing of a pinned
        thread can start within seconds), so a receiver reading the report
        should read that turn, not the thread's latest.

        ``failure_reason`` sets apart a failed run that was not the
        automation's own failure: ``usage_limit`` (``error`` is then the quota
        service's own message), ``provider_auth`` (their key was rejected and
        the automation is now disabled), ``server_error`` (the service failed
        it) or ``interrupted`` (the server cut its run off). None for any other
        event or failure.

        Returns a list of per-method results, or None if no delivery configured.
        Each result: {"method": str, "success": bool}
        """
        delivery_config = automation.get("delivery_config") or {}
        methods = delivery_config.get("methods", [])
        if not methods:
            return None

        from src.config import settings

        webhook_url = settings.AUTOMATION_WEBHOOK_URL
        webhook_secret = settings.AUTOMATION_WEBHOOK_SECRET
        if not webhook_url:
            logger.warning("[WEBHOOK] AUTOMATION_WEBHOOK_URL not configured, skipping delivery")
            return None

        base_payload = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "automation_id": str(automation["automation_id"]),
            "automation_name": automation.get("name"),
            "execution_id": execution_id,
            "thread_id": thread_id,
            "run_id": run_id,
            "failure_reason": failure_reason,
            "user_id": automation["user_id"],
            "agent_mode": automation.get("agent_mode"),
            "workspace_id": str(workspace_id) if workspace_id else None,
            "title": automation.get("name"),
        }
        if error:
            base_payload["error"] = error

        return [
            {
                "method": method,
                "success": await self.fire(
                    webhook_url,
                    {**base_payload, "config": {"channel": method}},
                    webhook_secret or None,
                ),
            }
            for method in methods
        ]
