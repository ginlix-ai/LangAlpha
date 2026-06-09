"""PTC Graph Factory — builds per-conversation agents with dependency-injected session management."""

import asyncio
import logging
from typing import Any, Protocol, runtime_checkable

from ptc_agent.agent.agent import PTCAgent
from ptc_agent.config import AgentConfig
from ptc_agent.core.session import Session

logger = logging.getLogger(__name__)


_USER_PROFILE_TTL = 86400  # 24h — freshness via explicit invalidation


async def fetch_user_data_counts(user_id: str | None) -> dict[str, Any] | None:
    """Lightweight counts for the static `<user_profile>` block.

    Three indexed queries in parallel. Failure is non-fatal — returns None and
    the awareness block omits the counts line.
    """
    if not user_id:
        return None
    try:
        from src.server.services import user_data_io as io
        portfolio_count, watchlist_counts, prefs_set = await asyncio.gather(
            io.count_portfolio_for_user(user_id),
            io.count_watchlist_for_user(user_id),
            io.exists_preferences_for_user(user_id),
        )
        wl_count, item_count = watchlist_counts
        return {
            "portfolio_count": int(portfolio_count),
            "watchlist_summary": f"{wl_count}:{item_count}",
            "prefs_set": bool(prefs_set),
        }
    except Exception:
        logger.warning("user-data counts fetch failed; awareness block will omit counts", exc_info=True)
        return None


async def get_user_profile_for_prompt(user_id: str) -> dict[str, Any] | None:
    """Fetch user profile for system prompt injection, cached in Redis for up to ``_USER_PROFILE_TTL`` seconds.

    Explicitly invalidated by ``invalidate_user_profile_cache`` on profile/preferences updates.
    Returns None on DB error; callers silently omit the profile block.
    """
    import json as _json

    cache_key = f"user_profile_prompt:{user_id}"
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            try:
                cached = await cache.client.get(cache_key)
                if cached is not None:
                    return _json.loads(cached) if cached != b"null" else None
            except Exception:
                pass
    except Exception:
        cache = None

    profile = None
    try:
        from src.server.database import user as user_db

        result = await user_db.get_user_with_preferences(user_id)
        if result:
            user = result.get("user", {})
            preferences = result.get("preferences", {}) or {}
            profile = {
                "name": user.get("name"),
                "timezone": user.get("timezone"),
                "locale": user.get("locale"),
                "agent_preference": preferences.get("agent_preference"),
            }
    except Exception as e:
        logger.warning(f"Failed to fetch user profile for {user_id}: {e}")
        return None

    if cache and cache.enabled and cache.client:
        try:
            await cache.client.set(
                cache_key,
                _json.dumps(profile) if profile else b"null",
                ex=_USER_PROFILE_TTL,
            )
        except Exception:
            pass

    return profile


async def invalidate_user_profile_cache(user_id: str) -> None:
    """Delete the cached ``get_user_profile_for_prompt`` result."""
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            await cache.client.delete(f"user_profile_prompt:{user_id}")
    except Exception:
        pass


@runtime_checkable
class SessionProvider(Protocol):
    """Dependency-injection boundary for session management (server, CLI, tests)."""

    async def get_or_create_session(
        self, conversation_id: str, sandbox_id: str | None = None
    ) -> Session:
        ...


async def build_ptc_graph(
    conversation_id: str,
    config: AgentConfig,
    session_provider: SessionProvider,
    subagent_names: list[str] | None = None,
    sandbox_id: str | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
    store: Any | None = None,
    on_signed_url: Any | None = None,
    user_id: str | None = None,
) -> Any:
    """Build a BackgroundSubagentOrchestrator for ``conversation_id``, acquiring a session via ``session_provider``."""
    logger.debug(f"Building PTC graph for conversation: {conversation_id}")

    # Get session from provider
    session = await session_provider.get_or_create_session(
        conversation_id=conversation_id,
        sandbox_id=sandbox_id,
    )

    if not session.sandbox or not session.mcp_registry:
        raise RuntimeError(
            f"Failed to initialize session for conversation {conversation_id}"
        )

    ptc_agent, user_data_counts = await asyncio.gather(
        asyncio.to_thread(PTCAgent, config),
        fetch_user_data_counts(user_id),
    )

    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        subagent_names=subagent_names or config.subagents.enabled,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        # session gives workspace-tier memory a real namespace.
        session=session,
        store=store,
        on_signed_url=on_signed_url,
        user_id=user_id,
        user_data_counts=user_data_counts,
        tool_summary=getattr(session, "mcp_tool_summary", None),
    )

    logger.debug(
        f"Created PTC agent for {conversation_id} with "
        f"subagents: {subagent_names or config.subagents.enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent


async def build_ptc_graph_with_session(
    session: Session,
    config: AgentConfig,
    subagent_names: list[str] | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
    user_id: str | None = None,
    plan_mode: bool = False,
    thread_id: str | None = None,
    store: Any | None = None,
    on_signed_url: Any | None = None,
) -> Any:
    """Build a BackgroundSubagentOrchestrator from a pre-acquired session (WorkspaceManager path)."""
    workspace_id = session.conversation_id
    logger.debug(f"Building PTC graph with session for workspace: {workspace_id}")

    if not session.sandbox or not session.mcp_registry:
        raise RuntimeError(
            f"Session for workspace {workspace_id} is not properly initialized"
        )

    if user_id:
        user_profile, user_data_counts, ptc_agent = await asyncio.gather(
            get_user_profile_for_prompt(user_id),
            fetch_user_data_counts(user_id),
            asyncio.to_thread(PTCAgent, config),
        )
        if user_profile:
            logger.debug(f"Loaded user profile for {user_id}: {user_profile}")
    else:
        user_profile = None
        user_data_counts = None
        ptc_agent = await asyncio.to_thread(PTCAgent, config)

    vault_secrets = getattr(session.sandbox, "vault_secrets", None)

    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        subagent_names=subagent_names or config.subagents.enabled,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        user_profile=user_profile,
        plan_mode=plan_mode,
        session=session,
        thread_id=thread_id,
        on_agent_md_write=session.invalidate_agent_md,
        store=store,
        on_signed_url=on_signed_url,
        vault_secrets=vault_secrets,
        user_id=user_id,
        user_data_counts=user_data_counts,
        # Session-cached tool summary (precomputed once per session) so the per
        # turn create_agent never recomputes it — keeps the prompt-cache prefix
        # byte-stable. None → create_agent computes from the registry.
        tool_summary=getattr(session, "mcp_tool_summary", None),
    )

    logger.debug(
        f"Created PTC agent for workspace {workspace_id} with "
        f"subagents: {subagent_names or config.subagents.enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent
