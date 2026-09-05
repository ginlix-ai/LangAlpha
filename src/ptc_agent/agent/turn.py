"""What the two agents share about "which model is this turn running".

PTC and Flash diverge honestly on tools, sandbox, MCP and middleware shape.
They do not diverge on this question, and answering it twice is how the call
metadata came to reach one client kind and not the others.
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
    flash: bool,
) -> TurnModel:
    """Settle the model for this turn and stamp what the factory cannot see.

    ``flash`` is the single point the two agents differ on; ``default_client``
    is what each resolved at construction, which they do differently on
    purpose. ``resolve_llm_config`` stamps every client it builds, so the stamp
    here is for the two it never sees: one the caller passed in, and one the
    lazy factory path built.
    """
    from src.llms.llm import stamp_call_metadata

    client = llm_override if llm_override is not None else default_client
    name = ""
    if config.llm is not None:
        name = (config.llm.flash_name if flash else config.llm.name) or ""

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
        max_retries=3,
        backoff_factor=2.0,
        initial_delay=1.0,
        max_delay=60.0,
        jitter=True,
    )
