"""Resolution of the prompt guidance level (``lean`` vs ``detailed``).

Templates ship one body with the expanded scaffolding wrapped in
``{% if guidance | default("detailed") == "detailed" %}`` — lean is a strict
subset of detailed rather than a second copy, so the two cannot drift apart
unnoticed.
"""

from typing import Any, Literal

from src.llms.preferences import DEFAULT_GUIDANCE, GUIDANCE_LEVELS

PromptGuidance = Literal["lean", "detailed"]

VALID_GUIDANCE: frozenset[str] = frozenset(GUIDANCE_LEVELS)


def resolve_prompt_guidance(
    model_name: str | None = None,
    entry: dict[str, Any] | None = None,
) -> PromptGuidance:
    """Deployment pin, then what the model declares, then :data:`DEFAULT_GUIDANCE`.

    The user's own choice is not read here: it is resolved once per turn in
    ``resolve_llm_config``, which stamps the answer on the config and on every
    client. This is the floor under that, for a build that never went through
    the resolver. ``entry`` is the model's own row when the caller holds one; a
    custom model has no manifest row, so its declaration is the only thing
    standing between it and the fail-safe.
    """
    from src.config.settings import get_prompt_guidance_default

    pinned = get_prompt_guidance_default()
    if pinned in VALID_GUIDANCE:
        return pinned  # type: ignore[return-value]

    if entry is not None:
        declared = entry.get("prompt_guidance")
    elif model_name:
        # Lazy import matches the house pattern for src.llms access from ptc_agent.
        from src.llms import LLM

        declared = LLM.get_model_config().get_prompt_guidance(model_name)
    else:
        declared = None
    return declared if declared in VALID_GUIDANCE else DEFAULT_GUIDANCE  # type: ignore[return-value]


def guidance_template_vars(guidance: str) -> dict[str, Any]:
    """Template context for a resolved level.

    One key, not a level plus a derived boolean — templates compare the level
    by name so a fence says which axis it is gating on. ``prompts/formatter.py``
    independently calls its MCP tool-exposure mode "detailed", and a bare
    ``{% if detailed %}`` in the same package does not say which of the two it
    means.
    """
    return {"guidance": guidance}
