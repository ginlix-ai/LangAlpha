"""
Chat Handler — Business logic for chat/message streaming.

Extracted from src/server/app/chat.py to separate business logic from route definitions.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional
from uuid import uuid4
from fastapi import HTTPException
from langgraph.types import Command

from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
    summarize_hitl_response_map,
)
from src.server.handlers.streaming_handler import WorkflowStreamHandler
from ptc_agent.agent.graph import build_ptc_graph_with_session
from ptc_agent.agent.flash import build_flash_graph
from ptc_agent.agent.graph import get_user_profile_for_prompt
from src.server.services.workspace_manager import WorkspaceManager
from src.server.database.workspace import (
    update_workspace_activity,
    get_or_create_flash_workspace,
    get_workspace as db_get_workspace,
)
from src.server.services.background_task_manager import (
    BackgroundTaskManager,
    TaskStatus,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.workflow_tracker import WorkflowTracker

# Database persistence imports
from src.server.database import conversation as qr_db
from src.server.services.persistence.conversation import (
    ConversationPersistenceService,
)

# Token and tool tracking imports
from src.utils.tracking import (
    TokenTrackingManager,
    ExecutionTracker,
)
from src.tools.decorators import ToolUsageTracker

from src.server.utils.skill_context import (
    detect_slash_commands,
    parse_skill_contexts,
    build_skill_content,
)
from src.server.utils.multimodal_context import (
    parse_multimodal_contexts,
    inject_multimodal_context,
)
from src.server.utils.directive_context import (
    parse_directive_contexts,
    build_directive_reminder,
)
from src.server.dependencies.usage_limits import release_burst_slot

# Locale/timezone configuration
from src.config.settings import (
    get_locale_config,
    get_langsmith_tags,
    get_langsmith_metadata,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)
_sse_logger = logging.getLogger("sse_events")

from src.config.settings import is_sse_event_log_enabled

_SSE_LOG_ENABLED = is_sse_event_log_enabled()

# Maps agent mode → (config field on llm, preference key in other_preference)
_MODE_MODEL_MAP = {
    "ptc": ("name", "preferred_model"),
    "flash": ("flash", "preferred_flash_model"),
}


def _append_to_last_user_message(messages: list[dict], text: str) -> None:
    """Append text to the last user message in a message list (mutates in-place)."""
    if not messages:
        return
    last_msg = messages[-1]
    if not isinstance(last_msg, dict) or last_msg.get("role") != "user":
        return
    content = last_msg.get("content")
    if isinstance(content, str):
        last_msg["content"] = content + text
    elif isinstance(content, list):
        last_msg["content"].append({"type": "text", "text": text})


async def _setup_fork_and_persistence(
    *,
    request: ChatRequest,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    log_prefix: str = "FORK",
) -> tuple[str, bool, ConversationPersistenceService]:
    """Compute query_type, apply fork cleanup, and init persistence service.

    Shared by both flash and PTC handlers. Returns (query_type, is_fork, persistence_service).
    """
    # Determine query type
    is_resume = bool(request.hitl_response)
    is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)
    if is_resume:
        query_type = "resume_feedback"
    elif is_checkpoint_replay:
        query_type = "regenerate"
    else:
        query_type = "initial"

    # Fork cleanup: truncate app DB when branching from a checkpoint
    is_fork = request.fork_from_turn is not None and request.checkpoint_id
    if is_fork:
        deleted = await qr_db.truncate_thread_from_turn(
            thread_id,
            request.fork_from_turn,
            preserve_query_at_fork=is_checkpoint_replay,
        )
        logger.info(
            f"[{log_prefix}] Truncated {deleted} rows from turn_index>={request.fork_from_turn} "
            f"thread_id={thread_id} checkpoint_id={request.checkpoint_id}"
        )
        # Clear Redis event buffer (stale events from old branch)
        try:
            manager = BackgroundTaskManager.get_instance()
            await manager.clear_event_buffer(thread_id)
        except Exception as e:
            logger.warning(f"[{log_prefix}] Failed to clear event buffer: {e}")
        # Update branch tip to fork checkpoint
        await qr_db.update_thread_checkpoint_id(thread_id, request.checkpoint_id)

    # Initialize persistence service
    persistence_service = ConversationPersistenceService.get_instance(
        thread_id=thread_id, workspace_id=workspace_id, user_id=user_id
    )

    # Reset persistence cache if forking, otherwise calculate from DB
    if is_fork:
        persistence_service.reset_for_fork(request.fork_from_turn)
    else:
        await persistence_service.get_or_calculate_turn_index()

    return query_type, is_fork, persistence_service


async def backfill_queued_queries(
    thread_id: str, queued_messages: list[dict]
) -> None:
    """Backfill query records for queued messages that produced orphan responses.

    After a workflow completes, responses may exist at turn indices that have no
    matching query (because the user message was injected mid-workflow via
    MessageQueueMiddleware rather than arriving as a normal HTTP request).
    This function finds those orphan response turns and creates query records.
    """
    if not queued_messages:
        return

    from src.server.database.conversation import (
        create_query,
        get_queries_for_thread,
        get_responses_for_thread,
    )

    try:
        queries = await get_queries_for_thread(thread_id)
        responses = await get_responses_for_thread(thread_id)

        query_turns = {q["turn_index"] for q in queries}
        response_turns = {r["turn_index"] for r in responses}
        orphan_turns = sorted(response_turns - query_turns)

        if not orphan_turns:
            return

        # Match orphan turns with queued messages (FIFO order)
        for turn_index, msg in zip(orphan_turns, queued_messages):
            content = msg.get("content", "")
            if not content:
                continue
            await create_query(
                conversation_query_id=str(uuid4()),
                conversation_thread_id=thread_id,
                turn_index=turn_index,
                content=content,
                query_type="queued",
            )
            logger.info(
                f"[CHAT] Backfilled queued query: thread_id={thread_id} "
                f"turn_index={turn_index}"
            )
    except Exception as e:
        logger.error(f"[CHAT] Failed to backfill queued queries: {e}")


async def drain_queued_messages(thread_id: str) -> list[dict] | None:
    """Drain any unconsumed queued messages from Redis after workflow completion.

    Returns the messages so they can be sent back to the client for input restoration.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    try:
        key = f"workflow:queued_messages:{thread_id}"
        pipe = cache.client.pipeline()
        pipe.lrange(key, 0, -1)
        pipe.delete(key)
        results = await pipe.execute()

        raw_messages = results[0]
        if not raw_messages:
            return None

        messages = []
        for raw in raw_messages:
            try:
                data = json.loads(
                    raw.decode("utf-8") if isinstance(raw, bytes) else raw
                )
                messages.append(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        return messages or None
    except Exception as e:
        logger.error(f"[CHAT] Failed to drain queued messages: {e}")
        return None


async def queue_message_for_thread(
    thread_id: str, content: str, user_id: str
) -> dict | None:
    """Queue a user message for injection into a running workflow via Redis.

    The MessageQueueMiddleware will pick these up before the next LLM call.

    Args:
        thread_id: The thread with an active workflow
        content: The user's message text
        user_id: User identifier

    Returns:
        Dict with queue position if successful, None if queuing failed
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        return None

    try:
        key = f"workflow:queued_messages:{thread_id}"
        message = json.dumps(
            {"content": content, "user_id": user_id, "timestamp": time.time()}
        )
        pipe = cache.client.pipeline()
        pipe.rpush(key, message)
        pipe.llen(key)
        pipe.expire(key, 3600)  # 1h TTL
        results = await pipe.execute()
        position = results[1]
        logger.info(
            f"[CHAT] Queued message for running workflow: "
            f"thread_id={thread_id} position={position}"
        )
        return {"position": position}
    except Exception as e:
        logger.error(f"[CHAT] Failed to queue message: {e}")
        return None


async def queue_message_for_subagent(
    thread_id: str,
    task_id: str,
    content: str,
    user_id: str,
) -> dict:
    """Queue a user message for injection into a running subagent via Redis.

    The SubagentMessageQueueMiddleware will pick these up before the subagent's next LLM call.

    Args:
        thread_id: The thread with an active workflow
        task_id: The subagent task ID (e.g., 'k7Xm2p')
        content: The message text to send
        user_id: User identifier

    Returns:
        Dict with success status and queue position
    """
    from src.utils.cache.redis_cache import get_cache_client

    # 1. Look up the registry for this thread
    registry_store = BackgroundRegistryStore.get_instance()
    registry = await registry_store.get_registry(thread_id)
    if registry is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active workflow for thread {thread_id}",
        )

    # 2. Look up the task by ID
    task = await registry.get_by_task_id(task_id)
    if task is None:
        raise HTTPException(
            status_code=404,
            detail=f"Task-{task_id} not found in thread {thread_id}",
        )

    # 3. Reject if already completed or cancelled
    if task.completed or task.cancelled:
        status = "cancelled" if task.cancelled else "completed"
        raise HTTPException(
            status_code=409,
            detail=f"Task-{task_id} has already {status}",
        )

    # 4. Queue to Redis (same pattern as _queue_followup_to_redis)
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(
            status_code=503,
            detail="Message queuing unavailable (Redis not connected)",
        )

    try:
        key = f"subagent:queued_messages:{task.tool_call_id}"
        payload = json.dumps(content)
        pipe = cache.client.pipeline()
        pipe.rpush(key, payload)
        pipe.llen(key)
        pipe.expire(key, 3600)  # 1h TTL
        results = await pipe.execute()
        position = results[1]

        logger.info(
            f"[SUBAGENT_MSG] Queued message for subagent: "
            f"thread_id={thread_id} task={task.display_id} position={position}"
        )
        return {
            "success": True,
            "tool_call_id": task.tool_call_id,
            "display_id": task.display_id,
            "queue_position": position,
        }
    except Exception as e:
        logger.error(f"[SUBAGENT_MSG] Failed to queue message: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to queue message: {e}",
        )


async def _resolve_custom_model_byok(
    user_id: str,
    model_name: str,
    custom_config: dict,
    mc,
    _pref_cache: dict | None = None,
):
    """
    Resolve BYOK key + base_url for a user-defined custom model.

    Key lookup order:
    1. model name as a custom sub-provider (model and provider share a name)
    2. custom model's provider field as a custom sub-provider
    3. parent of the custom model's provider (system provider)
    """
    from src.server.database.api_keys import get_byok_config_for_provider

    provider = custom_config["provider"]

    # 1. Model name is itself a custom sub-provider with a key
    cp_by_name = await get_custom_provider_config(user_id, model_name, _pref_cache=_pref_cache)
    if cp_by_name:
        byok_config = await get_byok_config_for_provider(user_id, model_name)
        if byok_config:
            base_url = byok_config.get("base_url") or mc.get_provider_info(cp_by_name["parent_provider"]).get("base_url")
            if cp_by_name.get("use_response_api"):
                custom_config = {**custom_config, "_use_response_api": True}
            return byok_config, base_url, custom_config

    # 2. Provider field is a custom sub-provider
    cp_by_provider = await get_custom_provider_config(user_id, provider, _pref_cache=_pref_cache)
    if cp_by_provider:
        byok_config = await get_byok_config_for_provider(user_id, provider)
        if byok_config:
            base_url = byok_config.get("base_url") or mc.get_provider_info(cp_by_provider["parent_provider"]).get("base_url")
            if cp_by_provider.get("use_response_api"):
                custom_config = {**custom_config, "_use_response_api": True}
            return byok_config, base_url, custom_config

    # 3. System/parent provider
    parent = mc.get_parent_provider(provider)
    byok_config = await get_byok_config_for_provider(user_id, parent)
    if byok_config:
        base_url = byok_config.get("base_url") or mc.get_provider_info(parent).get("base_url")
        return byok_config, base_url, custom_config

    return None, None, custom_config


async def resolve_byok_llm_client(
    user_id: str,
    model_name: str,
    is_byok: bool,
    reasoning_effort: str | None = None,
    _pref_cache: dict | None = None,
):
    """
    If BYOK is active, look up the user's key for the model's **parent** provider
    and return a fresh LLM client.  Returns None if BYOK isn't applicable.

    Auto-reroutes from sub-providers (e.g., anthropic-aws) to the parent provider's
    official endpoint (or user's custom base_url if set).
    """
    if not is_byok:
        return None

    from src.server.database.api_keys import get_byok_config_for_provider
    from src.llms.llm import LLM as LLMFactory, create_llm, create_llm_from_custom

    mc = LLMFactory.get_model_config()
    model_info = mc.get_model_config(model_name)

    if not model_info:
        # Fall back to user's custom models
        custom_config = await get_custom_model_config(user_id, model_name, _pref_cache=_pref_cache)

        if not custom_config:
            # Check if model_name is a BYOK custom provider name
            # (user selected a custom provider directly as their model)
            cp_config = await get_custom_provider_config(user_id, model_name, _pref_cache=_pref_cache)
            if cp_config:
                custom_config = {
                    "name": model_name,
                    "model_id": model_name,
                    "provider": cp_config["parent_provider"],
                }
            else:
                return None

        byok_config, base_url, custom_config = await _resolve_custom_model_byok(
            user_id, model_name, custom_config, mc, _pref_cache=_pref_cache,
        )
        if not byok_config:
            logger.warning(
                f"[CHAT] No BYOK key found for custom model={model_name} "
                f"provider={custom_config['provider']}. Falling back to system default."
            )
            return None

        logger.info(
            f"[CHAT] Using BYOK key for custom model={model_name} "
            f"provider={custom_config['provider']} base_url={base_url or 'SDK default'}"
        )
        return create_llm_from_custom(
            custom_config,
            api_key=byok_config["api_key"],
            base_url=base_url,
        )

    provider = model_info["provider"]
    parent = mc.get_parent_provider(provider)

    # Look up BYOK key for parent provider (e.g., "anthropic" not "anthropic-aws")
    byok_config = await get_byok_config_for_provider(user_id, parent)
    if not byok_config:
        return None

    # Resolve base_url: user custom > parent provider's official > None (SDK default)
    base_url = byok_config.get("base_url")
    if not base_url:
        parent_info = mc.get_provider_info(parent)
        base_url = parent_info.get("base_url")  # None for anthropic = SDK default

    logger.info(
        f"[CHAT] Using BYOK key for parent_provider={parent} "
        f"(model_provider={provider}) base_url={base_url or 'SDK default'}"
    )
    # Always pass base_url (even None) to override the sub-provider's URL via sentinel
    return create_llm(
        model_name,
        api_key=byok_config["api_key"],
        base_url=base_url,
        reasoning_effort=reasoning_effort,
    )


async def resolve_oauth_llm_client(
    user_id: str,
    model_name: str,
    reasoning_effort: str | None = None,
):
    """Resolve OAuth-connected LLM client. Independent of BYOK toggle."""
    from src.llms.llm import LLM as LLMFactory, create_llm

    mc = LLMFactory.get_model_config()
    model_info = mc.get_model_config(model_name)
    if not model_info:
        return None

    provider = model_info["provider"]
    provider_info = mc.get_provider_info(provider)
    if provider_info.get("auth_type") != "oauth":
        return None

    # Dispatch to the correct OAuth service by provider
    if provider == "claude-oauth":
        from src.server.services.claude_oauth import get_valid_token
    else:
        from src.server.services.codex_oauth import get_valid_token

    token_data = await get_valid_token(user_id)
    if not token_data:
        return None

    access_token = token_data["access_token"]
    if not access_token or not isinstance(access_token, str):
        logger.error(
            f"[CHAT] OAuth token is empty or not a string for provider={provider}: type={type(access_token)}"
        )
        return None

    # Provider-specific headers
    headers = {}
    if provider == "claude-oauth":
        logger.info(f"[CHAT] Using Claude OAuth for provider={provider}")
    else:
        # Codex: set ChatGPT-Account-Id header
        account_id = token_data.get("account_id", "")
        token_type = "sk-key" if access_token.startswith("sk-") else "oauth-jwt"
        logger.info(
            f"[CHAT] Using Codex OAuth for provider={provider} token_type={token_type} account_id={account_id[:8]}..."
        )
        if account_id:
            headers["ChatGPT-Account-Id"] = account_id

    return create_llm(
        model_name,
        api_key=access_token,
        default_headers=headers if headers else None,
        reasoning_effort=reasoning_effort,
    )


async def get_model_preference(user_id: str) -> dict:
    """Return model preferences from other_preference (not agent_preference, which is dumped to agent context)."""
    from src.server.database.user import get_user_preferences

    prefs = await get_user_preferences(user_id)
    if not prefs:
        return {}
    return prefs.get("other_preference") or {}


async def get_custom_model_config(user_id: str, model_name: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined custom model by name from other_preference.custom_models."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    for cm in model_pref.get("custom_models") or []:
        if cm.get("name") == model_name:
            return cm
    return None


async def get_custom_provider_config(user_id: str, provider: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined sub-provider config (name, parent_provider, use_response_api, etc.)."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    for cp in model_pref.get("custom_providers") or []:
        if cp.get("name") == provider:
            return cp
    return None


async def resolve_llm_config(
    base_config,
    user_id: str,
    request_model: str | None,
    is_byok: bool,
    mode: str = "ptc",
    reasoning_effort: str | None = None,
):
    """
    Resolve final LLM config with priority:
    per-request model > user preferred model > default.
    Then inject BYOK/OAuth client if active, and apply reasoning effort.

    Mode determines which config field and preference key to use
    (see _MODE_MODEL_MAP). Easy to extend for new modes.
    """
    model_field, pref_key = _MODE_MODEL_MAP[mode]
    config = base_config
    model_pref = await get_model_preference(user_id)

    if request_model:
        config = config.model_copy(deep=True)
        setattr(config.llm, model_field, request_model)
        config.llm_client = None
        logger.info(f"[CHAT] Using per-request LLM model: {request_model}")
    else:
        preferred = model_pref.get(pref_key)
        if preferred:
            config = config.model_copy(deep=True)
            setattr(config.llm, model_field, preferred)
            config.llm_client = None
            logger.info(f"[CHAT] Using {pref_key}: {preferred}")
        else:
            logger.info(
                f"[CHAT] No {pref_key} set, using system default: {getattr(config.llm, model_field, None) or config.llm.name}"
            )

    # Apply other model overrides from user preferences
    _other_model_keys = [
        ("summarization_model", "summarization"),
        ("fetch_model", "fetch"),
    ]
    for pref_key_other, config_field in _other_model_keys:
        user_val = model_pref.get(pref_key_other)
        if user_val:
            if config is base_config:
                config = config.model_copy(deep=True)
            setattr(config.llm, config_field, user_val)

    user_fallback = model_pref.get("fallback_models")
    if user_fallback:
        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm.fallback = user_fallback

    # Resolve the effective model from whichever field we just set
    effective_model = getattr(config.llm, model_field, None) or config.llm.name

    # If effective model is a custom model but BYOK is off, fall back to system default
    from src.llms.llm import LLM as LLMFactory

    mc = LLMFactory.get_model_config()
    is_system_model = mc.get_model_config(effective_model) is not None
    if not is_system_model:
        is_custom = await get_custom_model_config(user_id, effective_model, _pref_cache=model_pref) is not None
        is_custom_provider = not is_custom and await get_custom_provider_config(user_id, effective_model, _pref_cache=model_pref) is not None
        if (is_custom or is_custom_provider) and not is_byok:
            # Custom model/provider requires BYOK — revert to system default
            default_model = getattr(base_config.llm, model_field, None) or base_config.llm.name
            logger.warning(
                f"[CHAT] Custom model {effective_model} selected but BYOK disabled, "
                f"falling back to system default: {default_model}"
            )
            effective_model = default_model
            config = base_config

    # Resolve reasoning effort: per-request > user pref > None (use model default)
    effective_reasoning = reasoning_effort
    if not effective_reasoning:
        effective_reasoning = model_pref.get("reasoning_effort")

    # Try OAuth-connected providers first (independent of BYOK toggle)
    oauth_client = await resolve_oauth_llm_client(
        user_id, effective_model, effective_reasoning
    )
    if oauth_client:
        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm_client = oauth_client
    # Then try BYOK
    elif is_byok:
        byok_client = await resolve_byok_llm_client(
            user_id, effective_model, is_byok, effective_reasoning,
            _pref_cache=model_pref,
        )
        if byok_client:
            if config is base_config:
                config = config.model_copy(deep=True)
            config.llm_client = byok_client
    # Default path (system key) — apply reasoning_effort if set
    elif effective_reasoning:
        from src.llms.llm import create_llm

        if config is base_config:
            config = config.model_copy(deep=True)
        config.llm_client = create_llm(
            effective_model, reasoning_effort=effective_reasoning
        )
        logger.info(
            f"[CHAT] Applied reasoning_effort={effective_reasoning} to {effective_model}"
        )

    return config


async def astream_flash_workflow(
    request: ChatRequest,
    thread_id: str,
    user_input: str,
    user_id: str,
    is_byok: bool = False,
    config=None,
):
    """
    Async generator that streams Flash agent workflow events.

    Flash mode is optimized for speed - no sandbox, no MCP, no workspace required.
    Uses only external tools (web search, market data, SEC filings).

    Args:
        request: The chat request
        thread_id: Thread identifier
        user_input: Extracted user input text
        user_id: User identifier

    Yields:
        SSE-formatted event strings
    """
    start_time = time.time()
    handler = None
    token_callback = None
    tool_tracker = None
    flash_graph = None
    persistence_service = None

    logger.info(f"[FLASH_CHAT] Starting flash workflow: thread_id={thread_id}")

    try:
        # Validate agent_config is available
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="Flash Agent not initialized. Check server startup logs.",
            )

        # =====================================================================
        # Database Persistence Setup
        # =====================================================================

        # Get or create the shared flash workspace for this user
        flash_ws = await get_or_create_flash_workspace(user_id)
        workspace_id = str(flash_ws["workspace_id"])

        # Ensure thread exists in database
        ensure_kwargs = dict(
            workspace_id=workspace_id,
            conversation_thread_id=thread_id,
            user_id=user_id,
            initial_query=user_input,
            initial_status="in_progress",
            msg_type="flash",
        )
        if request.external_thread_id and request.platform:
            ensure_kwargs["external_id"] = request.external_thread_id
            ensure_kwargs["platform"] = request.platform
        await qr_db.ensure_thread_exists(**ensure_kwargs)

        query_type, is_fork, persistence_service = await _setup_fork_and_persistence(
            request=request,
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            log_prefix="FLASH_FORK",
        )
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start (with attachment and context metadata for display in history)
        effective_model = config.llm.flash if config else None
        query_metadata = {"msg_type": "flash"}
        if effective_model:
            query_metadata["llm_model"] = effective_model
        if request.additional_context:
            multimodal_ctxs = parse_multimodal_contexts(request.additional_context)
            if multimodal_ctxs:
                att_meta = [
                    {
                        "name": ctx.description or "file",
                        "type": "image"
                        if not ctx.data.startswith("data:application/pdf")
                        else "pdf",
                        "size": len(ctx.data.split(",", 1)[1]) * 3 // 4
                        if "," in ctx.data
                        else 0,
                    }
                    for ctx in multimodal_ctxs
                ]
                query_metadata["attachments"] = att_meta

            # Persist lightweight additional_context (skip heavy multimodal data)
            serialized_ctx = []
            for ctx in request.additional_context:
                ctx_type = getattr(ctx, "type", None)
                if ctx_type == "skills":
                    serialized_ctx.append({"type": "skills", "name": ctx.name})
                elif ctx_type == "directive":
                    serialized_ctx.append({"type": "directive", "content": ctx.content})
            if serialized_ctx:
                query_metadata["additional_context"] = serialized_ctx

        # Also detect slash commands from message text for persistence
        if "additional_context" not in query_metadata:
            _, early_detected = detect_slash_commands(user_input, mode="flash")
            if early_detected:
                query_metadata["additional_context"] = [
                    {"type": "skills", "name": s.name} for s in early_detected
                ]

        # Skip query persistence for checkpoint replay (regenerate/retry) —
        # the original user message is preserved (or no new message exists)
        if is_checkpoint_replay:
            turn_to_mark = (
                request.fork_from_turn
                if request.fork_from_turn is not None
                else await persistence_service.get_or_calculate_turn_index()
            )
            persistence_service.mark_query_persisted(turn_to_mark)
            logger.info(
                f"[FLASH_CHAT] Skipped query persist (checkpoint replay): "
                f"thread_id={thread_id} turn_index={turn_to_mark}"
            )
        else:
            await persistence_service.persist_query_start(
                content=user_input,
                query_type=query_type,
                metadata=query_metadata,
            )

        logger.info(
            f"[FLASH_CHAT] Database records created: workspace_id={workspace_id}"
        )

        # =====================================================================
        # Token and Tool Tracking
        # =====================================================================

        # Initialize token tracking
        token_callback = TokenTrackingManager.initialize_tracking(
            thread_id=thread_id, track_tokens=True
        )

        # Create tool tracker for infrastructure cost tracking
        tool_tracker = ToolUsageTracker(thread_id=thread_id)

        # =====================================================================
        # Build Flash Agent Graph
        # =====================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config, user_id, request.llm_model, is_byok, mode="flash",
                reasoning_effort=getattr(request, "reasoning_effort", None),
            )

        # Propagate fetch model override to tool context
        if config.llm.fetch:
            from src.tools.fetch import fetch_model_override
            fetch_model_override.set(config.llm.fetch)

        # Fetch user profile for prompt injection
        flash_user_profile = None
        if user_id:
            flash_user_profile = await get_user_profile_for_prompt(user_id)

        # Build flash graph (no sandbox, no session)
        flash_graph = build_flash_graph(
            config=config,
            checkpointer=setup.checkpointer,
            user_profile=flash_user_profile,
            store=setup.store,
        )

        # Build input state from messages
        messages = []
        for msg in request.messages:
            if isinstance(msg.content, str):
                messages.append({"role": msg.role, "content": msg.content})
            elif isinstance(msg.content, list):
                content_items = []
                for item in msg.content:
                    if hasattr(item, "type"):
                        if item.type == "text" and item.text:
                            content_items.append({"type": "text", "text": item.text})
                        elif item.type == "image" and item.image_url:
                            content_items.append(
                                {
                                    "type": "image_url",
                                    "image_url": {"url": item.image_url},
                                }
                            )
                messages.append(
                    {"role": msg.role, "content": content_items or str(msg.content)}
                )

        # Multimodal Context Injection (images and PDFs)
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts:
            messages = inject_multimodal_context(messages, multimodal_contexts)
            logger.info(
                f"[FLASH_CHAT] Multimodal context injected: {len(multimodal_contexts)} attachment(s)"
            )

        # Skill Context Injection (Flash mode) — inline with last user message
        loaded_skill_names: list[str] = []
        skill_contexts = parse_skill_contexts(request.additional_context)

        # Detect slash commands from message text (fallback for missing additional_context)
        if not skill_contexts and not request.hitl_response and messages:
            last_msg = messages[-1]
            msg_text = last_msg.get("content", "") if isinstance(last_msg.get("content"), str) else ""
            if msg_text:
                cleaned_text, detected = detect_slash_commands(msg_text, mode="flash")
                if detected:
                    skill_contexts = detected
                    if cleaned_text != msg_text:
                        last_msg["content"] = cleaned_text

        if skill_contexts:
            skill_dirs = [
                local_dir
                for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()
            ]
            skill_result = build_skill_content(
                skill_contexts, skill_dirs=skill_dirs, mode="flash"
            )
            if skill_result:
                _append_to_last_user_message(messages, "\n\n" + skill_result.content)
                loaded_skill_names = skill_result.loaded_skill_names
                logger.info(f"[FLASH_CHAT] Skills injected: {loaded_skill_names}")

        # Directive Context Injection (inline with user message)
        directives = parse_directive_contexts(request.additional_context)
        directive_reminder = build_directive_reminder(directives)
        if directive_reminder:
            _append_to_last_user_message(messages, directive_reminder)
            logger.info(
                f"[FLASH_CHAT] Directive context injected inline ({len(directives)} directives)"
            )

        # Build input state or resume command
        if request.hitl_response:
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[FLASH_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            input_state = None
            logger.info(
                f"[FLASH_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {"messages": messages}
            if loaded_skill_names:
                input_state["loaded_skills"] = loaded_skill_names

        # Build LangGraph config
        graph_config = {
            "configurable": {
                "thread_id": thread_id,
                "user_id": user_id,
            },
            "recursion_limit": 100,
            "tags": ["flash_agent"],
            "metadata": {
                "user_id": user_id,
                "thread_id": thread_id,
                "workflow_type": "flash_agent",
            },
        }

        if request.checkpoint_id:
            graph_config["configurable"]["checkpoint_id"] = request.checkpoint_id

        # Add token tracking callbacks
        if token_callback:
            graph_config["callbacks"] = [token_callback]

        # Create stream handler
        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
        )

        # Track queued messages injected mid-workflow for post-completion backfill
        async def _track_queued_messages(messages):
            handler.injected_queued_messages.extend(
                msg for msg in messages if msg.get("content")
            )
        handler.on_queued_message_injected = _track_queued_messages

        # =====================================================================
        # Background Execution (same pattern as PTC for reconnection support)
        # =====================================================================

        tracker = WorkflowTracker.get_instance()
        manager = BackgroundTaskManager.get_instance()

        # Wait for any running/soft-interrupted workflow to complete before starting new one
        ready_for_new_request = await manager.wait_for_soft_interrupted(
            thread_id, timeout=30.0
        )
        if not ready_for_new_request:
            # Try to queue the message for injection into the running workflow
            queued = await queue_message_for_thread(thread_id, user_input, user_id)
            if queued:
                event_data = json.dumps(
                    {
                        "thread_id": thread_id,
                        "content": user_input,
                        "position": queued["position"],
                    }
                )
                yield f"event: message_queued\ndata: {event_data}\n\n"
                return

            # Fallback: raise 409 if queuing failed
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Workflow {thread_id} is still running. "
                    "Wait a moment, or use /reconnect to continue streaming, or /cancel to stop it."
                ),
            )

        # Mark workflow as active in Redis tracker
        await tracker.mark_active(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            metadata={
                "started_at": datetime.now().isoformat(),
                "msg_type": "flash",
            },
        )

        # Completion callback for background persistence
        async def on_flash_workflow_complete():
            try:
                execution_time = time.time() - start_time
                _per_call_records = (
                    token_callback.per_call_records if token_callback else None
                )
                _tool_usage = handler.get_tool_usage() if handler else None
                _sse_events = handler.get_sse_events() if handler else None

                if persistence_service:
                    await persistence_service.persist_completion(
                        metadata={"msg_type": "flash", "is_byok": is_byok},
                        execution_time=execution_time,
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                        sse_events=_sse_events,
                    )

                await tracker.mark_completed(
                    thread_id=thread_id,
                    metadata={
                        "completed_at": datetime.now().isoformat(),
                        "execution_time": execution_time,
                    },
                )

                # Backfill query records for queued messages that produced orphan responses
                if handler and handler.injected_queued_messages:
                    await backfill_queued_queries(
                        thread_id, handler.injected_queued_messages
                    )

                logger.info(
                    f"[FLASH_COMPLETE] Background completion persisted: "
                    f"thread_id={thread_id} duration={execution_time:.2f}s"
                )
            except Exception as e:
                logger.error(
                    f"[FLASH_CHAT] Background completion persistence failed: {e}",
                    exc_info=True,
                )
            finally:
                await release_burst_slot(user_id)

        # Start workflow in background
        try:
            await manager.start_workflow(
                thread_id=thread_id,
                workflow_generator=handler.stream_workflow(
                    graph=flash_graph,
                    input_state=input_state,
                    config=graph_config,
                ),
                metadata={
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "started_at": datetime.now().isoformat(),
                    "start_time": start_time,
                    "msg_type": "flash",
                    "is_byok": is_byok,
                    "handler": handler,
                    "token_callback": token_callback,
                },
                completion_callback=on_flash_workflow_complete,
                graph=flash_graph,
            )
        except RuntimeError:
            # Race condition: another request registered first — queue the message
            await release_burst_slot(user_id)
            queued = await queue_message_for_thread(thread_id, user_input, user_id)
            if queued:
                event_data = json.dumps(
                    {
                        "thread_id": thread_id,
                        "content": user_input,
                        "position": queued["position"],
                    }
                )
                yield f"event: message_queued\ndata: {event_data}\n\n"
                return

            raise HTTPException(
                status_code=409,
                detail=(
                    f"Workflow {thread_id} is still running. "
                    "Wait a moment, or use /reconnect to continue streaming, or /cancel to stop it."
                ),
            )

        # Stream live events from background task to client
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        await manager.subscribe_to_live_events(thread_id, live_queue)
        await manager.increment_connection(thread_id)

        _disconnected = False
        try:
            while True:
                try:
                    sse_event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                    if sse_event is None:
                        break
                    yield sse_event
                except asyncio.TimeoutError:
                    status = await manager.get_task_status(thread_id)
                    if status in [
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    ]:
                        break
                    continue

            # After workflow ends, return any unconsumed queued messages to the client
            unconsumed = await drain_queued_messages(thread_id)
            if unconsumed:
                logger.info(
                    f"[FLASH_CHAT] Returning {len(unconsumed)} unconsumed queued "
                    f"message(s) to client: thread_id={thread_id}"
                )
                event_data = json.dumps({
                    "thread_id": thread_id,
                    "messages": [
                        {"content": m["content"], "user_id": m.get("user_id")}
                        for m in unconsumed
                    ],
                })
                yield f"event: queued_message_returned\ndata: {event_data}\n\n"

        except (asyncio.CancelledError, GeneratorExit):
            _disconnected = True
            asyncio.create_task(
                _handle_sse_disconnect(
                    tracker=tracker,
                    manager=manager,
                    thread_id=thread_id,
                    workspace_id=workspace_id,
                    user_id=user_id,
                    live_queue=live_queue,
                    handler=handler,
                    token_callback=token_callback,
                    persistence_service=persistence_service,
                    start_time=start_time,
                    request=request,
                    is_byok=is_byok,
                ),
                name=f"sse-disconnect-cleanup-{thread_id}",
            )
            raise
        finally:
            if not _disconnected:
                try:
                    await manager.unsubscribe_from_live_events(thread_id, live_queue)
                except Exception:
                    pass
                try:
                    await manager.decrement_connection(thread_id)
                except Exception:
                    pass

    except Exception as e:
        logger.exception(f"[FLASH_ERROR] thread_id={thread_id}: {e}")

        # Release burst slot on error (setup errors before background task starts)
        await release_burst_slot(user_id)

        # Persist error
        if persistence_service:
            try:
                execution_time = time.time() - start_time
                per_call_records = (
                    token_callback.per_call_records if token_callback else None
                )
                tool_usage = handler.get_tool_usage() if handler else None
                sse_events = handler.get_sse_events() if handler else None

                await persistence_service.persist_error(
                    error_message=str(e),
                    execution_time=execution_time,
                    metadata={"msg_type": "flash", "is_byok": is_byok},
                    per_call_records=per_call_records,
                    tool_usage=tool_usage,
                    sse_events=sse_events,
                )
            except Exception as persist_error:
                logger.error(f"[FLASH_CHAT] Failed to persist error: {persist_error}")

        # Yield error event
        if handler:
            error_event = handler._format_sse_event(
                "error",
                {
                    "thread_id": thread_id,
                    "error": str(e),
                    "type": "workflow_error",
                },
            )
            yield error_event
        else:
            error_event = json.dumps(
                {
                    "thread_id": thread_id,
                    "error": str(e),
                    "type": "workflow_error",
                }
            )
            yield f"event: error\ndata: {error_event}\n\n"

        raise


async def _handle_sse_disconnect(
    tracker,
    manager,
    thread_id: str,
    workspace_id: str,
    user_id: str,
    live_queue,
    handler,
    token_callback,
    persistence_service,
    start_time: float,
    request,
    is_byok: bool = False,
):
    """Fire-and-forget cleanup when the SSE client disconnects.

    Runs as an independent asyncio.Task outside Starlette's anyio cancel scope,
    so awaits work normally. Handles both explicit cancel (user clicked cancel)
    and accidental disconnect (tab close, refresh, network drop).
    """
    try:
        is_explicit_cancel = await tracker.is_cancelled(thread_id)

        if is_explicit_cancel:
            logger.info(
                f"[CHAT] Workflow explicitly cancelled by user: thread_id={thread_id}"
            )
            await tracker.mark_cancelled(thread_id)

            _per_call_records = (
                token_callback.per_call_records if token_callback else None
            )
            _tool_usage = handler.get_tool_usage() if handler else None

            try:
                _sse_events = handler.get_sse_events() if handler else None
                await persistence_service.persist_cancelled(
                    execution_time=time.time() - start_time,
                    metadata={
                        "workspace_id": request.workspace_id,
                        "is_byok": is_byok,
                    },
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                    sse_events=_sse_events,
                )
            except Exception as persist_error:
                logger.error(f"[CHAT] Failed to persist cancellation: {persist_error}")

            await manager.cancel_workflow(thread_id)
            await release_burst_slot(user_id)

            registry_store = BackgroundRegistryStore.get_instance()
            await registry_store.cancel_and_clear(thread_id, force=True)
        else:
            logger.info(
                f"[CHAT] SSE client disconnected, workflow continues in "
                f"background: thread_id={thread_id}"
            )
            await tracker.mark_disconnected(
                thread_id=thread_id,
                metadata={
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "disconnected_at": datetime.now().isoformat(),
                },
            )
    except Exception as e:
        logger.error(
            f"[CHAT] Error during SSE disconnect cleanup for {thread_id}: {e}",
            exc_info=True,
        )
    finally:
        try:
            await manager.unsubscribe_from_live_events(thread_id, live_queue)
        except Exception:
            pass
        try:
            await manager.decrement_connection(thread_id)
        except Exception:
            pass


async def _is_plan_interrupt_pending(thread_id: str) -> bool:
    """Check if the pending interrupt is a SubmitPlan (plan mode) interrupt.

    Plan interrupts from HumanInTheLoopMiddleware have action_requests with
    name="SubmitPlan". Other interrupts (AskUserQuestion, onboarding) use
    a "type" field instead. Returns False on any error.
    """
    try:
        checkpointer = setup.checkpointer
        if not checkpointer:
            return False
        config = {"configurable": {"thread_id": thread_id}}
        checkpoint_tuple = await checkpointer.aget_tuple(config)
        if not checkpoint_tuple or not checkpoint_tuple.pending_writes:
            return False
        for _task_id, channel, value in checkpoint_tuple.pending_writes:
            if channel != "__interrupt__":
                continue
            interrupts = value if isinstance(value, list) else [value]
            for intr in interrupts:
                intr_value = (
                    getattr(intr, "value", intr)
                    if not isinstance(intr, dict)
                    else intr.get("value", intr)
                )
                if not isinstance(intr_value, dict):
                    continue
                action_requests = intr_value.get("action_requests", [])
                if action_requests and isinstance(action_requests[0], dict):
                    if action_requests[0].get("name") == "SubmitPlan":
                        return True
        return False
    except Exception:
        logger.warning(
            f"[PTC_CHAT] Failed to check pending interrupt type for "
            f"thread_id={thread_id}, defaulting to non-plan mode",
            exc_info=True,
        )
        return False


async def astream_ptc_workflow(
    request: ChatRequest,
    thread_id: str,
    user_input: str,
    user_id: str,
    workspace_id: str,
    is_byok: bool = False,
    config=None,
):
    """
    Async generator that streams PTC agent workflow events.

    Uses build_ptc_graph to create a per-workspace LangGraph graph,
    then reuses the standard WorkflowStreamHandler for SSE streaming.

    Args:
        request: The chat request
        thread_id: Thread identifier
        user_input: Extracted user input text
        user_id: User identifier
        workspace_id: Workspace identifier

    Yields:
        SSE-formatted event strings
    """
    start_time = time.time()
    handler = None
    persistence_service = None
    token_callback = None
    tool_tracker = None
    ptc_graph = None

    # Start execution tracking to capture agent messages
    ExecutionTracker.start_tracking()
    logger.debug("PTC execution tracking started")

    try:
        # Validate agent_config is available
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # =====================================================================
        # Phase 1: Database Persistence Setup
        # =====================================================================

        # Ensure thread exists in database (linked to workspace)
        ensure_kwargs = dict(
            workspace_id=workspace_id,
            conversation_thread_id=thread_id,
            user_id=user_id,
            initial_query=user_input,
            initial_status="in_progress",
            msg_type="ptc",
        )
        if request.external_thread_id and request.platform:
            ensure_kwargs["external_id"] = request.external_thread_id
            ensure_kwargs["platform"] = request.platform
        await qr_db.ensure_thread_exists(**ensure_kwargs)

        query_type, is_fork, persistence_service = await _setup_fork_and_persistence(
            request=request,
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            log_prefix="PTC_FORK",
        )
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start
        feedback_action = None
        query_content = user_input
        effective_model = config.llm.name if config else None
        query_metadata = {
            "workspace_id": request.workspace_id,
            "msg_type": "ptc",
        }
        if effective_model:
            query_metadata["llm_model"] = effective_model

        # Extract attachment and context metadata for display in history
        if request.additional_context and not request.hitl_response:
            multimodal_ctxs = parse_multimodal_contexts(request.additional_context)
            if multimodal_ctxs:
                att_meta = [
                    {
                        "name": ctx.description or "file",
                        "type": "image"
                        if not ctx.data.startswith("data:application/pdf")
                        else "pdf",
                        "size": len(ctx.data.split(",", 1)[1]) * 3 // 4
                        if "," in ctx.data
                        else 0,
                    }
                    for ctx in multimodal_ctxs
                ]
                query_metadata["attachments"] = att_meta

            # Persist lightweight additional_context (skip heavy multimodal data)
            serialized_ctx = []
            for ctx in request.additional_context:
                ctx_type = getattr(ctx, "type", None)
                if ctx_type == "skills":
                    serialized_ctx.append({"type": "skills", "name": ctx.name})
                elif ctx_type == "directive":
                    serialized_ctx.append({"type": "directive", "content": ctx.content})
            if serialized_ctx:
                query_metadata["additional_context"] = serialized_ctx

        # Also detect slash commands from message text for persistence
        # (covers the case where frontend didn't send additional_context)
        if not request.hitl_response and "additional_context" not in query_metadata:
            _, early_detected = detect_slash_commands(user_input, mode="ptc")
            if early_detected:
                query_metadata["additional_context"] = [
                    {"type": "skills", "name": s.name} for s in early_detected
                ]

        if request.hitl_response:
            # HITL resume payloads typically have empty user_input (CLI sends message="").
            summary = summarize_hitl_response_map(request.hitl_response)
            feedback_action = summary["feedback_action"]
            query_content = summary["content"]
            query_metadata["hitl_interrupt_ids"] = summary["interrupt_ids"]

            # Store per-interrupt answers for replay reconstruction.
            # Format: { interrupt_id: "answer" | null (skipped) }
            hitl_answers = {}
            for interrupt_id, response in request.hitl_response.items():
                decisions = (
                    response.decisions
                    if hasattr(response, "decisions")
                    else response.get("decisions", [])
                )
                for d in decisions:
                    d_type = d.type if hasattr(d, "type") else d.get("type")
                    d_msg = (
                        d.message if hasattr(d, "message") else d.get("message")
                    ) or ""
                    if d_type == "approve" and d_msg:
                        hitl_answers[interrupt_id] = d_msg
                    elif d_type == "reject" and not d_msg:
                        hitl_answers[interrupt_id] = None
            if hitl_answers:
                query_metadata["hitl_answers"] = hitl_answers
                has_answers = any(v is not None for v in hitl_answers.values())
                feedback_action = (
                    "QUESTION_ANSWERED" if has_answers else "QUESTION_SKIPPED"
                )

        # Skip query persistence for checkpoint replay (regenerate/retry) —
        # the original user message is preserved (or no new message exists)
        if is_checkpoint_replay:
            # Mark the preserved query's turn as already persisted so
            # persist_completion doesn't skip due to missing query tracking
            turn_to_mark = (
                request.fork_from_turn
                if request.fork_from_turn is not None
                else await persistence_service.get_or_calculate_turn_index()
            )
            persistence_service.mark_query_persisted(turn_to_mark)
            logger.info(
                f"[PTC_CHAT] Skipped query persist (checkpoint replay): "
                f"thread_id={thread_id} turn_index={turn_to_mark}"
            )
        else:
            await persistence_service.persist_query_start(
                content=query_content,
                query_type=query_type,
                feedback_action=feedback_action,
                metadata=query_metadata,
            )
            logger.info(
                f"[PTC_CHAT] Database records created: workspace_id={workspace_id} "
                f"thread_id={thread_id} query_type={query_type}"
            )

        # =====================================================================
        # Timezone and Locale Validation
        # =====================================================================

        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        timezone_str = "UTC"  # Default

        if request.timezone:
            # Validate user-provided timezone
            try:
                ZoneInfo(request.timezone)
                timezone_str = request.timezone
                logger.debug(f"[PTC_CHAT] Using user-provided timezone: {timezone_str}")
            except ZoneInfoNotFoundError as e:
                logger.warning(
                    f"[PTC_CHAT] Invalid timezone '{request.timezone}': {e}. "
                    f"Falling back to locale-based timezone."
                )
                timezone_str = None  # Will use locale fallback

        if not timezone_str or timezone_str == "UTC":
            # Fallback to locale-based timezone
            locale_config = get_locale_config(
                request.locale or "en-US",
                "en",  # Default prompt language
            )
            timezone_str = locale_config.get("timezone", "UTC")
            logger.debug(
                f"[PTC_CHAT] Using locale-based timezone: {timezone_str} "
                f"(locale: {request.locale})"
            )

        # =====================================================================
        # Phase 3: Token and Tool Tracking
        # =====================================================================

        # Initialize token tracking (always enabled)
        token_callback = TokenTrackingManager.initialize_tracking(
            thread_id=thread_id, track_tokens=True
        )

        # Create tool tracker for infrastructure cost tracking
        tool_tracker = ToolUsageTracker(thread_id=thread_id)

        # =====================================================================
        # Session and Graph Setup
        # =====================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config, user_id, request.llm_model, is_byok, mode="ptc",
                reasoning_effort=getattr(request, "reasoning_effort", None),
            )

        # Propagate fetch model override to tool context
        if config.llm.fetch:
            from src.tools.fetch import fetch_model_override
            fetch_model_override.set(config.llm.fetch)

        subagents = request.subagents_enabled or config.subagents.enabled
        sandbox_id = None

        # Use WorkspaceManager for workspace-based sessions
        logger.info(f"[PTC_CHAT] Using workspace: {workspace_id}")
        workspace_manager = WorkspaceManager.get_instance()

        # Check if workspace needs startup — emit early notification so frontend
        # can show "Starting workspace..." instead of a silent wait.
        workspace_record = await db_get_workspace(workspace_id)
        ws_status = workspace_record.get("status") if workspace_record else None
        if ws_status == "stopped":
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id})}\n\n"

        session = await workspace_manager.get_session_for_workspace(
            workspace_id, user_id=user_id
        )

        if ws_status == "stopped":
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'ready', 'workspace_id': workspace_id})}\n\n"

        # Update workspace activity
        await update_workspace_activity(workspace_id)

        registry_store = BackgroundRegistryStore.get_instance()
        background_registry = await registry_store.get_or_create_registry(thread_id)

        # Effective plan_mode: only enable if explicitly requested or resuming
        # from a SubmitPlan interrupt. Other interrupt types (AskUserQuestion,
        # onboarding) must NOT activate plan mode.
        if request.plan_mode:
            effective_plan_mode = True
        elif request.hitl_response:
            effective_plan_mode = await _is_plan_interrupt_pending(thread_id)
        else:
            effective_plan_mode = False

        # Build graph with the workspace's session
        # Note: agent.md is injected dynamically by WorkspaceContextMiddleware
        # on every model call, ensuring it's always the latest content.
        ptc_graph = await build_ptc_graph_with_session(
            session=session,
            config=config,
            subagent_names=subagents,
            operation_callback=None,
            checkpointer=setup.checkpointer,
            background_registry=background_registry,
            user_id=user_id,
            plan_mode=effective_plan_mode,
            thread_id=thread_id,
            store=setup.store,
        )

        if session.sandbox:
            sandbox_id = getattr(session.sandbox, "sandbox_id", None)

        # Store graph for persistence snapshots
        setup.graph = ptc_graph

        # Build input state from messages
        messages = []
        for msg in request.messages:
            if isinstance(msg.content, str):
                messages.append({"role": msg.role, "content": msg.content})
            elif isinstance(msg.content, list):
                # Handle multi-part content
                content_items = []
                for item in msg.content:
                    if hasattr(item, "type"):
                        if item.type == "text" and item.text:
                            content_items.append({"type": "text", "text": item.text})
                        elif item.type == "image" and item.image_url:
                            content_items.append(
                                {
                                    "type": "image_url",
                                    "image_url": {"url": item.image_url},
                                }
                            )
                messages.append(
                    {"role": msg.role, "content": content_items or str(msg.content)}
                )

        # =====================================================================
        # Skill Context Injection (inline with last user message)
        # =====================================================================
        # When skills are requested via additional_context, load SKILL.md content
        # and append inline to the last user message using <loaded-skill> tags.
        # The original user_input is preserved for database persistence.
        #
        # Server-side slash command detection: also scan the last user message
        # for /<command> prefixes as a fallback when additional_context is missing.
        loaded_skill_names: list[str] = []
        skill_contexts = parse_skill_contexts(request.additional_context)

        # Detect slash commands from message text (fallback for missing additional_context)
        if not skill_contexts and not request.hitl_response and messages:
            last_msg = messages[-1]
            msg_text = last_msg.get("content", "") if isinstance(last_msg.get("content"), str) else ""
            if msg_text:
                cleaned_text, detected = detect_slash_commands(msg_text, mode="ptc")
                if detected:
                    skill_contexts = detected
                    if cleaned_text != msg_text:
                        last_msg["content"] = cleaned_text

        if skill_contexts and not request.hitl_response:
            skill_dirs = [
                local_dir
                for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()
            ]
            skill_result = build_skill_content(
                skill_contexts, skill_dirs=skill_dirs, mode="ptc"
            )
            if skill_result:
                _append_to_last_user_message(messages, "\n\n" + skill_result.content)
                loaded_skill_names = skill_result.loaded_skill_names
                logger.info(f"[PTC_CHAT] Skills injected: {loaded_skill_names}")

        # Multimodal Context Injection (images and PDFs)
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts and not request.hitl_response:
            messages = inject_multimodal_context(messages, multimodal_contexts)
            logger.info(
                f"[PTC_CHAT] Multimodal context injected: {len(multimodal_contexts)} attachment(s)"
            )

        # Build input state or resume command
        if request.hitl_response:
            # Structured HITL resume payload.
            # Pydantic validates this into HITLResponse models, but LangChain's
            # HumanInTheLoopMiddleware expects plain dicts (subscriptable).
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[PTC_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            # Checkpoint replay/regenerate: no new messages, resume from checkpoint_id.
            # LangGraph will re-execute from the specified checkpoint state.
            input_state = None
            logger.info(
                f"[PTC_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {
                "messages": messages,
                "current_agent": "ptc",  # For FileOperationMiddleware SSE events
            }
            # Auto-load skill tools when skills were injected via additional_context
            if loaded_skill_names:
                input_state["loaded_skills"] = loaded_skill_names

        # =====================================================================
        # Plan Mode Injection
        # =====================================================================
        # When plan_mode is enabled, inject a reminder for the agent to create
        # a plan and submit it for approval before executing any changes.
        if effective_plan_mode and not request.hitl_response:
            plan_mode_reminder = (
                "\n\n<system-reminder>\n"
                "[PLAN MODE ENABLED]\n"
                "Before making any changes, you MUST:\n"
                "1. Explore the codebase to understand the current state\n"
                "2. Create a detailed plan describing what you intend to do\n"
                "3. Call the `SubmitPlan` tool with your plan description\n"
                "4. Wait for user approval before proceeding with execution\n"
                "Do NOT execute any write operations until the plan is approved.\n"
                "</system-reminder>"
            )
            # Append reminder to the last user message
            if isinstance(input_state, dict) and input_state.get("messages"):
                _append_to_last_user_message(
                    input_state["messages"], plan_mode_reminder
                )
            logger.info(f"[PTC_CHAT] Plan mode enabled for thread_id={thread_id}")

        # =====================================================================
        # Directive Context Injection (inline with user message)
        # =====================================================================
        directives = parse_directive_contexts(request.additional_context)
        directive_reminder = build_directive_reminder(directives)
        if directive_reminder and not request.hitl_response:
            if isinstance(input_state, dict) and input_state.get("messages"):
                _append_to_last_user_message(
                    input_state["messages"], directive_reminder
                )
                logger.info(
                    f"[PTC_CHAT] Directive context injected inline ({len(directives)} directives)"
                )

        # =====================================================================
        # Save user request to system thread directory (non-critical)
        # =====================================================================
        if not request.hitl_response and session.sandbox:
            short_id = thread_id[:8]
            try:
                request_path = session.sandbox.normalize_path(
                    f".agent/threads/{short_id}/request.md"
                )
                await session.sandbox.awrite_file_text(request_path, user_input)
            except Exception:
                pass  # Non-critical, don't fail the request

        # =====================================================================
        # LangSmith Tracing Configuration
        # =====================================================================

        # Build LangSmith tags for filtering/grouping traces
        langsmith_tags = get_langsmith_tags(
            msg_type="ptc",
            deepthinking=False,  # PTC agent doesn't use deep thinking mode
            auto_accepted_plan=False,
            locale=request.locale,
        )

        # Build LangSmith metadata for detailed trace context
        langsmith_metadata = get_langsmith_metadata(
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
            workflow_type="ptc_agent",
            locale=request.locale,
            timezone=timezone_str,
            deepthinking=False,
            auto_accepted_plan=False,
            track_tokens=True,
        )

        # Build LangGraph config
        config = {
            "configurable": {
                "thread_id": thread_id,
                "user_id": user_id,  # For user-scoped tools
                "workspace_id": workspace_id,  # For workspace-scoped tools
            },
            "recursion_limit": 1000,
            "tags": langsmith_tags,
            "metadata": langsmith_metadata,
        }

        if request.checkpoint_id:
            config["configurable"]["checkpoint_id"] = request.checkpoint_id

        # Add token tracking callbacks
        if token_callback:
            config["callbacks"] = [token_callback]

        # Extract background task registry from orchestrator (single source of truth for SSE events)
        # The orchestrator wraps the middleware which owns the registry
        background_registry = None
        if hasattr(ptc_graph, "middleware") and hasattr(
            ptc_graph.middleware, "registry"
        ):
            background_registry = ptc_graph.middleware.registry
            logger.debug(
                f"[PTC_CHAT] Background registry attached for thread_id={thread_id}"
            )

        # Reuse WorkflowStreamHandler for SSE streaming
        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
            background_registry=background_registry,
        )

        # Track queued messages injected mid-workflow for post-completion backfill
        async def _track_queued_messages(messages):
            handler.injected_queued_messages.extend(
                msg for msg in messages if msg.get("content")
            )
        handler.on_queued_message_injected = _track_queued_messages

        # Initialize workflow tracker
        tracker = WorkflowTracker.get_instance()
        await tracker.mark_active(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            metadata={
                "type": "ptc_agent",
                "sandbox_id": sandbox_id,
                "locale": request.locale,
                "timezone": timezone_str,
            },
        )

        # =====================================================================
        # Phase 2: Background Execution with Completion Callback
        # =====================================================================

        manager = BackgroundTaskManager.get_instance()

        # Wait for any soft-interrupted workflow to complete before starting new one
        # This ensures seamless continuation after ESC interrupt
        ready_for_new_request = await manager.wait_for_soft_interrupted(
            thread_id, timeout=30.0
        )
        if not ready_for_new_request:
            # Try to queue the message for injection into the running workflow
            queued = await queue_message_for_thread(thread_id, user_input, user_id)
            if queued:
                # Return a short SSE response confirming the queue, then exit
                event_data = json.dumps(
                    {
                        "thread_id": thread_id,
                        "content": user_input,
                        "position": queued["position"],
                    }
                )
                yield f"event: message_queued\ndata: {event_data}\n\n"
                return

            # Fallback: raise 409 if queuing failed
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Workflow {thread_id} is still running. "
                    "Wait a moment, or use /reconnect to continue streaming, or /cancel to stop it."
                ),
            )

        # Define completion callback for background persistence
        async def on_background_workflow_complete():
            """Persists workflow data after background execution completes.

            Reads fresh handler/token_callback from task_info metadata because
            reinvocation may have replaced them with new instances.
            """
            try:
                # Read fresh refs from task_info (may have been updated by reinvoke)
                task_info = manager.tasks.get(thread_id)
                _handler = task_info.metadata.get("handler") if task_info else handler
                _token_cb = (
                    task_info.metadata.get("token_callback")
                    if task_info
                    else token_callback
                )
                _start_time = (
                    task_info.metadata.get("start_time", start_time)
                    if task_info
                    else start_time
                )

                execution_time = time.time() - _start_time

                _persistence_service = ConversationPersistenceService.get_instance(
                    thread_id
                )
                _persistence_service._on_pair_persisted = (
                    lambda: manager.clear_event_buffer(thread_id)
                )

                # Get per-call records for usage tracking
                _per_call_records = _token_cb.per_call_records if _token_cb else None

                # Get tool usage summary from handler
                _tool_usage = None
                if _handler:
                    _tool_usage = _handler.get_tool_usage()

                # Persist completion to database
                _sse_events = _handler.get_sse_events() if _handler else None
                await _persistence_service.persist_completion(
                    metadata={
                        "workspace_id": request.workspace_id,
                        "sandbox_id": sandbox_id,
                        "locale": request.locale,
                        "timezone": timezone_str,
                        "msg_type": "ptc",
                        "is_byok": is_byok,
                    },
                    execution_time=execution_time,
                    per_call_records=_per_call_records,
                    tool_usage=_tool_usage,
                    sse_events=_sse_events,
                )

                # Mark completed in Redis tracker
                await tracker.mark_completed(
                    thread_id=thread_id,
                    metadata={
                        "completed_at": datetime.now().isoformat(),
                        "execution_time": execution_time,
                    },
                )

                # Backfill query records for queued messages that produced orphan responses
                if _handler and _handler.injected_queued_messages:
                    await backfill_queued_queries(
                        thread_id, _handler.injected_queued_messages
                    )

                logger.info(
                    f"[PTC_COMPLETE] Background completion persisted: thread_id={thread_id} "
                    f"duration={execution_time:.2f}s"
                )

                # Backup sandbox files to DB after each message
                try:
                    ws_manager = WorkspaceManager.get_instance()
                    await ws_manager._backup_files_to_db(request.workspace_id)
                except Exception as backup_err:
                    logger.warning(
                        f"[PTC_COMPLETE] File backup failed for {thread_id}: {backup_err}"
                    )

            except Exception as e:
                logger.error(
                    f"[PTC_CHAT] Background completion persistence failed for {thread_id}: {e}",
                    exc_info=True,
                )
            finally:
                # Release burst slot so it doesn't block future requests
                await release_burst_slot(user_id)

        # Start workflow in background with event buffering
        await manager.start_workflow(
            thread_id=thread_id,
            workflow_generator=handler.stream_workflow(
                graph=ptc_graph,
                input_state=input_state,
                config=config,
            ),
            metadata={
                "workspace_id": workspace_id,
                "user_id": user_id,
                "sandbox_id": sandbox_id,
                "started_at": datetime.now().isoformat(),
                "start_time": start_time,
                "msg_type": "ptc",
                "is_byok": is_byok,
                "locale": request.locale,
                "timezone": timezone_str,
                "handler": handler,
                "token_callback": token_callback,
            },
            completion_callback=on_background_workflow_complete,
            graph=ptc_graph,  # Pass graph for state queries in completion/error handlers
        )

        # Create local queue for this connection to receive live events
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

        # Subscribe to live events from the background workflow
        await manager.subscribe_to_live_events(thread_id, live_queue)
        await manager.increment_connection(thread_id)

        _disconnected = False
        try:
            while True:
                try:
                    sse_event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                    if sse_event is None:
                        break
                    yield sse_event
                except asyncio.TimeoutError:
                    status = await manager.get_task_status(thread_id)
                    if status in [
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    ]:
                        break
                    continue

            # After workflow ends, return any unconsumed queued messages to the client
            unconsumed = await drain_queued_messages(thread_id)
            if unconsumed:
                logger.info(
                    f"[PTC_CHAT] Returning {len(unconsumed)} unconsumed queued "
                    f"message(s) to client: thread_id={thread_id}"
                )
                event_data = json.dumps({
                    "thread_id": thread_id,
                    "messages": [
                        {"content": m["content"], "user_id": m.get("user_id")}
                        for m in unconsumed
                    ],
                })
                yield f"event: queued_message_returned\ndata: {event_data}\n\n"

        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected (tab close, refresh, network drop).
            # Cannot await here — Starlette's anyio cancel scope is active.
            # Spawn cleanup in an independent task outside the cancel scope.
            _disconnected = True
            asyncio.create_task(
                _handle_sse_disconnect(
                    tracker=tracker,
                    manager=manager,
                    thread_id=thread_id,
                    workspace_id=workspace_id,
                    user_id=user_id,
                    live_queue=live_queue,
                    handler=handler,
                    token_callback=token_callback,
                    persistence_service=persistence_service,
                    start_time=start_time,
                    request=request,
                    is_byok=is_byok,
                ),
                name=f"sse-disconnect-cleanup-{thread_id}",
            )
            raise
        finally:
            if not _disconnected:
                try:
                    await manager.unsubscribe_from_live_events(thread_id, live_queue)
                except Exception:
                    pass
                try:
                    await manager.decrement_connection(thread_id)
                except Exception:
                    pass

    except Exception as e:
        # =====================================================================
        # Phase 4: Error Recovery with Retry Logic
        # =====================================================================

        # Release burst slot on error so it doesn't block future requests
        await release_burst_slot(user_id)

        # Get token/tool usage for billing even on errors
        _per_call_records = token_callback.per_call_records if token_callback else None
        _tool_usage = handler.get_tool_usage() if handler else None

        # Non-recoverable error types (code bugs, config issues)
        non_recoverable_types = (
            AttributeError,  # Code bug - missing attribute
            NameError,  # Code bug - undefined variable
            SyntaxError,  # Code bug - syntax error
            ImportError,  # Missing dependency
            TypeError,  # Wrong type passed
            KeyError,  # Missing key (usually code issue)
        )

        is_non_recoverable = isinstance(e, non_recoverable_types)

        # Recoverable error patterns (transient issues)
        import psycopg

        is_postgres_connection = isinstance(
            e, psycopg.OperationalError
        ) and "server closed the connection" in str(e)

        is_timeout = (
            isinstance(e, TimeoutError)
            or "timeout" in str(e).lower()
            or "timed out" in str(e).lower()
        )

        is_network_issue = (
            isinstance(e, ConnectionError)
            or "connection" in str(e).lower()
            or "network" in str(e).lower()
            or "unreachable" in str(e).lower()
            or "connection refused" in str(e).lower()
        )

        # API errors (transient server errors, rate limits, etc.)
        is_api_error = False
        error_str = str(e).lower()
        error_type_name = type(e).__name__.lower()

        # Check for API error types (InternalServerError, APIError, etc.)
        api_error_indicators = [
            "internal server error",
            "api_error",
            "system error",
            "error code: 500",
            "error code: 502",
            "error code: 503",
            "error code: 429",  # Rate limit
            "rate limit",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        ]

        is_api_error = (
            any(indicator in error_str for indicator in api_error_indicators)
            or "internal" in error_type_name
            or "api" in error_type_name
            or "server" in error_type_name
        )

        # Determine if error is recoverable
        is_recoverable = (
            is_postgres_connection or is_timeout or is_network_issue or is_api_error
        ) and not is_non_recoverable

        MAX_RETRIES = 3  # Maximum automatic retries

        if is_recoverable:
            # Recoverable error - check retry count
            tracker = WorkflowTracker.get_instance()
            retry_count = await tracker.increment_retry_count(thread_id)

            error_type = (
                "connection_error"
                if is_postgres_connection or is_network_issue
                else "timeout_error"
                if is_timeout
                else "api_error"
                if is_api_error
                else "transient_error"
            )

            if retry_count > MAX_RETRIES:
                # Exceeded max retries - treat as non-recoverable
                logger.error(
                    f"[PTC_CHAT] Max retries exceeded ({retry_count}/{MAX_RETRIES}) for "
                    f"thread_id={thread_id}: {type(e).__name__}: {str(e)[:100]}"
                )

                # Persist error with retry info
                if persistence_service:
                    try:
                        error_msg = f"Max retries exceeded ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {str(e)}"
                        _sse_events = handler.get_sse_events() if handler else None
                        await persistence_service.persist_error(
                            error_message=error_msg,
                            errors=[error_msg],
                            execution_time=time.time() - start_time,
                            metadata={
                                "workspace_id": request.workspace_id,
                                "msg_type": "ptc",
                                "is_byok": is_byok,
                            },
                            per_call_records=_per_call_records,
                            tool_usage=_tool_usage,
                            sse_events=_sse_events,
                        )
                    except Exception as persist_error:
                        logger.error(
                            f"[PTC_CHAT] Failed to persist error: {persist_error}"
                        )

                # Yield error with retry info
                error_data = {
                    "message": f"Workflow failed after {MAX_RETRIES} retry attempts",
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retry_count": retry_count,
                    "max_retries": MAX_RETRIES,
                    "thread_id": thread_id,
                }
                yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
            else:
                # Within retry limit - allow retry
                logger.warning(
                    f"[PTC_CHAT] Recoverable error ({error_type}) for thread_id={thread_id} "
                    f"(retry {retry_count}/{MAX_RETRIES}): "
                    f"{type(e).__name__}: {str(e)[:100]}"
                )

                # Yield retry info event (not error)
                retry_data = {
                    "message": "Temporary error occurred, you can retry or resume the workflow",
                    "thread_id": thread_id,
                    "auto_retry": True,
                    "error_type": error_type,
                    "error_class": type(e).__name__,
                    "retry_count": retry_count,
                    "max_retries": MAX_RETRIES,
                }
                yield f"event: retry\ndata: {json.dumps(retry_data)}\n\n"

                # Mark as interrupted (not error) so it can be resumed
                await qr_db.update_thread_status(thread_id, "interrupted")

        else:
            # Non-recoverable error - persist and fail
            logger.exception(f"[PTC_ERROR] thread_id={thread_id}: {e}")

            # Persist error to database
            if persistence_service:
                try:
                    _sse_events = handler.get_sse_events() if handler else None
                    await persistence_service.persist_error(
                        error_message=str(e),
                        execution_time=time.time() - start_time,
                        metadata={
                            "workspace_id": request.workspace_id,
                            "msg_type": "ptc",
                            "is_byok": is_byok,
                        },
                        per_call_records=_per_call_records,
                        tool_usage=_tool_usage,
                        sse_events=_sse_events,
                    )
                except Exception as persist_error:
                    logger.error(f"[PTC_CHAT] Failed to persist error: {persist_error}")

            # Yield error event using handler's format method if available
            if handler:
                error_event = handler._format_sse_event(
                    "error",
                    {
                        "thread_id": thread_id,
                        "error": str(e),
                        "type": "workflow_error",
                    },
                )
                yield error_event
            else:
                # Fallback error formatting
                error_event = json.dumps(
                    {
                        "thread_id": thread_id,
                        "error": str(e),
                        "type": "workflow_error",
                    }
                )
                yield f"event: error\ndata: {error_event}\n\n"

        raise

    finally:
        # Always stop execution tracking to prevent memory leaks and context pollution
        ExecutionTracker.stop_tracking()
        logger.debug("PTC execution tracking stopped")


async def reconnect_to_workflow_stream(
    thread_id: str,
    last_event_id: Optional[int] = None,
):
    """
    Reconnect to a running or completed PTC workflow.

    Args:
        thread_id: Workflow thread identifier
        last_event_id: Optional last event ID for filtering duplicates

    Yields:
        SSE-formatted event strings
    """
    manager = BackgroundTaskManager.get_instance()
    tracker = WorkflowTracker.get_instance()

    # Get workflow info
    task_info = await manager.get_task_info(thread_id)
    workflow_status = await tracker.get_status(thread_id)

    if not task_info:
        if workflow_status and workflow_status.get("status") == "completed":
            raise HTTPException(
                status_code=410, detail="Workflow completed and results expired"
            )
        raise HTTPException(status_code=404, detail=f"Workflow {thread_id} not found")

    # Replay buffered events (during tailing, Redis only holds tail-phase
    # events because the buffer is cleared after pre-tail persist)
    buffered_events = await manager.get_buffered_events_redis(
        thread_id,
        from_beginning=True,
        after_event_id=last_event_id,
    )

    logger.info(
        f"[PTC_RECONNECT] Replaying {len(buffered_events)} events for {thread_id}"
    )

    for event in buffered_events:
        yield event

    # Attach to live stream if still running or tailing
    status = await manager.get_task_status(thread_id)
    if status == TaskStatus.RUNNING:
        live_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        await manager.subscribe_to_live_events(thread_id, live_queue)
        await manager.increment_connection(thread_id)

        try:
            while True:
                try:
                    event = await asyncio.wait_for(live_queue.get(), timeout=1.0)
                    if event is None:
                        break
                    yield event
                except asyncio.TimeoutError:
                    current_status = await manager.get_task_status(thread_id)
                    if current_status in [
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    ]:
                        break
                    continue

            # After workflow ends, return any unconsumed queued messages to the client
            unconsumed = await drain_queued_messages(thread_id)
            if unconsumed:
                event_data = json.dumps({
                    "thread_id": thread_id,
                    "messages": [
                        {"content": m["content"], "user_id": m.get("user_id")}
                        for m in unconsumed
                    ],
                })
                yield f"event: queued_message_returned\ndata: {event_data}\n\n"

        finally:
            await manager.unsubscribe_from_live_events(thread_id, live_queue)
            await manager.decrement_connection(thread_id)


async def stream_subagent_task_events(
    thread_id: str, task_id: str, last_event_id: int | None = None
):
    """SSE stream of a single subagent's content events.

    Per-task SSE stream with its own Redis buffer. Events are
    message_chunk, tool_calls, tool_call_result, and message_queued.

    Redis key: subagent:events:{thread_id}:{task_id}
    Cleared after task completion + persistence (mirrors main stream per-turn clearing).

    Args:
        thread_id: Workflow thread identifier
        task_id: The 6-char alphanumeric task identifier
        last_event_id: Last received event ID for reconnect replay

    Yields:
        SSE-formatted event strings
    """
    from src.utils.cache.redis_cache import get_cache_client
    from src.server.services.background_task_manager import drain_task_captured_events

    registry_store = BackgroundRegistryStore.get_instance()
    cache = get_cache_client()
    redis_key = f"subagent:events:{thread_id}:{task_id}"
    seq = 0
    cursor = 0
    max_wait, waited = 30, 0

    def _format_sse(seq_id: int, event_type: str, data: dict) -> str:
        result = f"id: {seq_id}\nevent: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        if _SSE_LOG_ENABLED:
            _sse_logger.info(result)
        return result

    def _parse_sse_id(raw_sse: str) -> int | None:
        """Extract event ID from raw SSE string."""
        try:
            first_line = raw_sse.split("\n", 1)[0]
            if first_line.startswith("id: "):
                return int(first_line[4:].strip())
        except (ValueError, IndexError):
            pass
        return None

    # Phase 1: Replay from Redis buffer on reconnect
    if last_event_id is not None:
        try:
            stored = await cache.list_range(redis_key, 0, -1) or []
            for raw_sse in stored:
                eid = _parse_sse_id(raw_sse)
                if eid is not None and eid > last_event_id:
                    yield raw_sse
                seq = max(seq, eid or 0)
        except Exception as e:
            logger.warning(f"[SubagentStream:{task_id}] Redis replay failed: {e}")

        # Seed cursor past already-buffered events
        registry = await registry_store.get_registry(thread_id)
        if registry:
            task = await registry.get_task_by_task_id(task_id)
            if task:
                cursor = len(task.captured_events)

    # Phase 2: Live polling
    while True:
        registry = await registry_store.get_registry(thread_id)
        if not registry:
            if waited >= max_wait:
                break
            waited += 0.5
            await asyncio.sleep(0.5)
            continue

        task = await registry.get_task_by_task_id(task_id)
        if not task:
            if waited >= max_wait:
                break
            waited += 0.5
            await asyncio.sleep(0.5)
            continue

        # Reset wait counter once we find the task
        waited = 0

        # Drain new captured_events (shared helper)
        for ev, agent_id in drain_task_captured_events(task, cursor):
            seq += 1
            data = {"thread_id": thread_id, "agent": agent_id, **ev["data"]}
            sse = _format_sse(seq, ev["event"], data)
            # Buffer to per-task Redis key
            try:
                await cache.list_append(redis_key, sse, max_size=100, ttl=3600)
            except Exception:
                pass  # Non-fatal: live delivery still works
            yield sse
        cursor = len(task.captured_events)

        # Task done → final drain complete → close
        if task.completed or (task.asyncio_task and task.asyncio_task.done()):
            # Signal collector that all events have been emitted to the client
            task.sse_drain_complete.set()
            break

        await asyncio.sleep(0.5)
