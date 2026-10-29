"""What every agent graph shares about "which model is this turn running".

The PTC agent and the market-insight research graph diverge honestly on tools,
sandbox, MCP and middleware shape. They do not diverge on this question, and
answering it twice is how the call metadata came to reach one client kind and
not the others.
"""

from dataclasses import dataclass
from typing import Any

from ptc_agent.agent.middleware.model_resilience import (
    ModelResilienceMiddleware,
    build_fallback_pairs,
)
from ptc_agent.config import AgentConfig


@dataclass(frozen=True)
class TurnModel:
    """The client a turn calls, the name to report it under, and the prompt
    scaffolding level that name resolves to."""

    client: Any
    name: str
    guidance: str


def turn_model(
    config: AgentConfig,
    llm_override: Any | None,
    default_client: Any,
    *,
    background: bool,
) -> TurnModel:
    """Settle the model for this turn and stamp what the factory cannot see.

    ``background`` is for a graph that runs on the Background model rather than the
    main one; ``default_client`` is what the caller resolved at construction.
    ``resolve_llm_config`` stamps every client it builds, so the stamp
    here is for the two it never sees: one the caller passed in, and one the
    lazy factory path built.
    """
    from src.llms.llm import stamp_call_metadata

    client = llm_override if llm_override is not None else default_client
    name = ""
    if config.llm is not None:
        name = (config.llm.background_name if background else config.llm.name) or ""

    guidance = turn_guidance(config, name)
    stamp_call_metadata(
        client,
        prompt_guidance=guidance,
        compaction_profile=config.compaction.profile,
    )
    return TurnModel(client=client, name=name, guidance=guidance)


def turn_guidance(config: AgentConfig, model_name: str) -> str:
    """Scaffolding level for this turn.

    ``resolve_llm_config`` settles it per model and stamps it on the config, so
    the manifest is probed only for a build that never went through the
    resolver: the library path, and tests.
    """
    if config.prompt_guidance:
        return config.prompt_guidance
    from ptc_agent.agent.prompts import resolve_prompt_guidance

    return resolve_prompt_guidance(model_name)


def build_model_resilience_middleware(
    config: AgentConfig, turn: TurnModel
) -> ModelResilienceMiddleware:
    """Retry + fallback + client-visible progress in a single middleware."""
    return ModelResilienceMiddleware(
        primary_name=turn.name,
        primary_client=turn.client,
        fallbacks=build_fallback_pairs(config),
        users_fallbacks=config.fallback_users_choice,
        max_retries=3,
        backoff_factor=2.0,
        initial_delay=1.0,
        max_delay=60.0,
        jitter=True,
        # Every stack built from this config mounts compaction outside
        # resilience exactly when it is on.
        compacts_overflow=config.compaction.enabled,
    )
