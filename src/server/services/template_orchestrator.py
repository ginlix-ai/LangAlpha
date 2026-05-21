"""TemplateOrchestrator — manages template entry lifecycle.

Responsibilities:
  - Instantiate: create workspace + create entry + kick off the agent run
  - Track: progress / status updates from sandbox-side scripts
  - Finalize: persist agent's structured output to template_entries
  - Rerun: reset and re-trigger analysis
  - List / read entries

Reuses existing infrastructure (no rewrite):
  - WorkspaceManager.create_workspace() for sandbox creation
  - astream_ptc_workflow for agent execution

Agent Knowledge (agent.md injection):
  When a template workspace is created, the orchestrator seeds a rich agent.md
  that tells the agent:
    1. Which template this workspace belongs to (template_id, entry_id)
    2. The entry_key / company being analyzed
    3. HOW to persist results back to the dashboard DB (persist_entry.py)
    4. How to re-run analysis after modifying rules (user-configurable section)

  This means the agent ALWAYS knows how to update the database — no matter
  whether it's doing an initial analysis or a follow-up after the user changed
  the D7 logic.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from uuid import uuid4

from src.server.database import templates as tdb
from src.server.database.workspace import (
    delete_workspace,
    get_workspace,
)
from src.server.models.chat import ChatMessage, ChatRequest
from src.server.services.workspace_manager import WorkspaceManager
from src.server.templates.registry import TemplateDefinition, get_template

logger = logging.getLogger(__name__)


class TemplateError(Exception):
    """Base error for template-orchestrator-level issues."""


class TemplateOrchestrator:
    """Singleton service for template entry lifecycle."""

    _instance: "TemplateOrchestrator | None" = None

    @classmethod
    def get_instance(cls) -> "TemplateOrchestrator":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # =====================================================================
    # Public API
    # =====================================================================

    async def instantiate(
        self,
        template_id: str,
        user_id: str,
        entry_key: str,
        display_name: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a new template entry.

        Steps:
          1. Look up template definition.
          2. Create a dedicated workspace (1:1 with entry).
          3. INSERT template_entries row in 'pending' state.
          4. Fire-and-forget kick off the agent run in the background.
          5. Return the entry record immediately so the UI can show it.

        Raises:
          TemplateError if the template id is unknown or there's a duplicate
          (entry_key already exists for this user/template).
        """
        template = get_template(template_id)
        if template is None:
            raise TemplateError(f"Unknown template: {template_id!r}")

        # Resolve entry_key: if empty, derive a stable placeholder from
        # display_name. The agent will replace it with the real ticker
        # during analysis (and this placeholder still satisfies the
        # UNIQUE(user_id, template_id, entry_key) constraint).
        effective_entry_key = (entry_key or "").strip()
        if not effective_entry_key:
            if not display_name or not display_name.strip():
                raise TemplateError("display_name or entry_key is required")
            effective_entry_key = _auto_key_from_name(display_name)

        # 1. Build workspace name + create workspace (independent sandbox).
        ws_name = template.build_workspace_name(
            effective_entry_key, display_name, params
        )
        manager = WorkspaceManager.get_instance()
        workspace = await manager.create_workspace(
            user_id=user_id,
            name=ws_name,
            description=f"[{template.manifest.name}] {effective_entry_key}",
            config={"template_id": template_id},
        )
        workspace_id = str(workspace["workspace_id"])

        # 2. INSERT entry row.
        try:
            entry = await tdb.create_entry(
                user_id=user_id,
                template_id=template_id,
                workspace_id=workspace_id,
                entry_key=effective_entry_key,
                display_name=display_name,
                params=params,
            )
        except Exception as e:
            logger.warning(
                "Entry creation failed (template=%s, key=%s): %s. "
                "Cleaning up workspace %s.",
                template_id, effective_entry_key, e, workspace_id,
            )
            try:
                await delete_workspace(workspace_id, hard_delete=True)
            except Exception as cleanup_err:
                logger.error("Workspace cleanup also failed: %s", cleanup_err)
            raise TemplateError(
                f"Could not create entry for {effective_entry_key!r}: {e}"
            ) from e

        # 3. Seed template-aware agent.md into the sandbox.
        # This runs BEFORE the agent, so the first (and every subsequent)
        # conversation sees the template context injected via WorkspaceContextMiddleware.
        await self._seed_template_agent_md(
            template=template,
            entry_id=str(entry["entry_id"]),
            entry_key=effective_entry_key,
            display_name=display_name,
            params=params,
            workspace_id=workspace_id,
        )

        # 4. Kick off the agent in the background.
        asyncio.create_task(
            self._run_agent_safe(template, user_id, entry, workspace_id)
        )

        return entry

    async def list_entries(
        self,
        template_id: str,
        user_id: str,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """List entries for (user, template)."""
        return await tdb.list_entries(
            user_id=user_id,
            template_id=template_id,
            status=status,
            limit=limit,
            offset=offset,
        )

    async def get_entry(
        self, entry_id: str, user_id: str
    ) -> dict[str, Any] | None:
        """Get a single entry. Returns None if not found OR not owned."""
        entry = await tdb.get_entry(entry_id)
        if entry is None or entry["user_id"] != user_id:
            return None
        return entry

    async def rerun(
        self, entry_id: str, user_id: str
    ) -> dict[str, Any]:
        """Reset and re-trigger analysis for an existing entry.

        Reuses the same workspace; just sends a fresh agent message.
        """
        entry = await self.get_entry(entry_id, user_id)
        if entry is None:
            raise TemplateError(f"Entry not found: {entry_id}")

        # Allow rerun even if status is 'analyzing' — the previous agent task
        # may have been killed by a server reload. We just reset and start fresh.

        template = get_template(entry["template_id"])
        if template is None:
            raise TemplateError(
                f"Template {entry['template_id']!r} no longer registered"
            )

        # Verify workspace is still alive.
        ws = await get_workspace(entry["workspace_id"])
        if ws is None or ws.get("status") == "deleted":
            raise TemplateError(
                "Workspace was deleted; cannot rerun (delete this entry instead)"
            )

        # Reset to 'pending' (keeps last payload as fallback display).
        entry = await tdb.reset_for_rerun(entry_id)
        assert entry is not None

        asyncio.create_task(
            self._run_agent_safe(template, user_id, entry, str(entry["workspace_id"]))
        )
        return entry

    async def finalize(
        self,
        entry_id: str,
        status: str,
        summary: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        error_message: str | None,
    ) -> dict[str, Any] | None:
        """Called by the internal finalize endpoint (sandbox-side script)."""
        return await tdb.finalize_entry(
            entry_id=entry_id,
            status=status,
            summary=summary,
            payload=payload,
            error_message=error_message,
        )

    async def update_progress(
        self,
        entry_id: str,
        progress: dict[str, Any],
        status: str | None = None,
    ) -> dict[str, Any] | None:
        """Called by the internal progress endpoint."""
        return await tdb.update_progress(
            entry_id=entry_id, progress=progress, status=status
        )

    # =====================================================================
    # Internals
    # =====================================================================

    async def _run_agent_safe(
        self,
        template: TemplateDefinition,
        user_id: str,
        entry: dict[str, Any],
        workspace_id: str,
    ) -> None:
        """Background task: run the agent, never raises."""
        entry_id = str(entry["entry_id"])
        try:
            # Mark analyzing (best-effort; ignore failures).
            try:
                await tdb.update_progress(
                    entry_id, progress={}, status="analyzing"
                )
            except Exception as e:
                logger.warning("update_progress(analyzing) failed: %s", e)

            await self._run_agent(template, user_id, entry, workspace_id)

            # Fallback: if agent finished without calling persist_entry.py
            # (i.e. entry is still 'analyzing'), mark it as failed so the user
            # sees a clear status instead of "analyzing forever".
            try:
                current = await tdb.get_entry(entry_id)
                if current and current["status"] == "analyzing":
                    logger.warning(
                        "[TEMPLATE] Agent finished but entry %s still 'analyzing' "
                        "— marking as failed (model did not call persist_entry.py)",
                        entry_id,
                    )
                    await tdb.finalize_entry(
                        entry_id=entry_id,
                        status="failed",
                        summary=None,
                        payload=None,
                        error_message=(
                            "Agent completed without finalizing results. "
                            "This usually means the model did not follow "
                            "the full execution flow (D6→D7→persist). "
                            "Please rerun."
                        ),
                    )
            except Exception as e:
                logger.warning("Fallback status check failed: %s", e)

        except Exception as e:
            logger.exception(
                "Agent run failed for entry %s (template=%s): %s",
                entry_id, template.id, e,
            )
            # Mark failed in DB.
            try:
                await tdb.finalize_entry(
                    entry_id=entry_id,
                    status="failed",
                    summary=None,
                    payload=None,
                    error_message=str(e)[:1000],
                )
            except Exception as e2:
                logger.error("Failed to mark entry failed: %s", e2)

    async def _run_agent(
        self,
        template: TemplateDefinition,
        user_id: str,
        entry: dict[str, Any],
        workspace_id: str,
    ) -> None:
        """Build prompt + run astream_ptc_workflow + drain generator."""
        entry_id = str(entry["entry_id"])
        entry_key = entry["entry_key"]
        display_name = entry.get("display_name")
        params = dict(entry.get("params") or {})

        # Inject system-derived params for the prompt template.
        # ``symbol_dir`` mirrors what fetch_data.py creates: 1357.HK -> 1357_HK
        params.setdefault("symbol_dir", _safe_symbol_dir(entry_key))
        params["entry_id"] = entry_id

        prompt = template.build_initial_prompt(
            entry_key=entry_key, display_name=display_name, params=params
        )

        thread_id = str(uuid4())
        request = ChatRequest(
            agent_mode="ptc",
            workspace_id=workspace_id,
            messages=[ChatMessage(role="user", content=prompt)],
        )

        # Late import to avoid circular dependency at module load.
        from src.server.handlers.chat import astream_ptc_workflow

        logger.info(
            "[TEMPLATE] entry=%s template=%s starting agent thread=%s",
            entry_id, template.id, thread_id,
        )

        generator = astream_ptc_workflow(
            request=request,
            thread_id=thread_id,
            user_input=prompt,
            user_id=user_id,
            workspace_id=workspace_id,
        )

        event_count = 0
        async for _ in generator:
            event_count += 1

        logger.info(
            "[TEMPLATE] entry=%s drained %d events", entry_id, event_count
        )

    async def _seed_template_agent_md(
        self,
        template: TemplateDefinition,
        entry_id: str,
        entry_key: str,
        display_name: str | None,
        params: dict[str, Any],
        workspace_id: str,
    ) -> None:
        """Write the template's agent.md into the sandbox.

        The content comes entirely from ``template.build_agent_md()`` —
        the orchestrator does NOT know what the file contains. This is
        the key decoupling: template authors own the agent.md template,
        the orchestrator just fills placeholders and writes the file.

        agent.md is then injected into every LLM system prompt by
        WorkspaceContextMiddleware (the "slot" the framework provides).
        """
        try:
            manager = WorkspaceManager.get_instance()
            session = manager._sessions.get(workspace_id)
            sandbox = getattr(session, "sandbox", None) if session else None
            if sandbox is None:
                logger.warning(
                    "[TEMPLATE] Cannot seed agent.md: no sandbox session for %s",
                    workspace_id,
                )
                return
        except Exception as e:
            logger.warning("[TEMPLATE] Failed to get sandbox for agent.md: %s", e)
            return

        # Inject entry_id so the template's agent.md can reference it.
        enriched_params = {**params, "entry_id": entry_id, "symbol_dir": _safe_symbol_dir(entry_key)}

        try:
            content = template.build_agent_md(
                entry_key=entry_key,
                display_name=display_name,
                params=enriched_params,
            )
        except Exception as e:
            logger.warning("[TEMPLATE] build_agent_md failed for %s: %s", template.id, e)
            return

        try:
            written = await sandbox.awrite_file_text("agent.md", content)
            if written:
                logger.info(
                    "[TEMPLATE] Seeded agent.md for entry=%s workspace=%s",
                    entry_id, workspace_id,
                )
            else:
                logger.warning(
                    "[TEMPLATE] Failed to write agent.md for workspace=%s",
                    workspace_id,
                )
        except Exception as e:
            logger.warning("[TEMPLATE] Error writing agent.md: %s", e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_symbol_dir(symbol: str) -> str:
    """Mirror fetch_data.py's directory naming: '1357.HK' -> '1357_HK'."""
    out = re.sub(r"[^A-Za-z0-9_]+", "_", symbol)
    return out.strip("_")


def _auto_key_from_name(display_name: str) -> str:
    """Build a stable placeholder entry_key when the user did NOT provide one.

    Used as the unique business key (so the UNIQUE constraint still holds)
    while the agent will go figure out the real ticker. The prefix ``auto_``
    lets the prompt-enricher detect this case and adjust instructions.
    """
    import hashlib

    digest = hashlib.sha1(display_name.strip().encode("utf-8")).hexdigest()[:10]
    return f"auto_{digest}"
