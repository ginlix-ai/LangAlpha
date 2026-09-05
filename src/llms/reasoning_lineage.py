"""Reasoning-payload compatibility classes ("lineage") for provider routes.

Reasoning blocks are opaque and provider-bound: an Anthropic ``thinking`` block
carries a cryptographic ``signature`` only api.anthropic.com can verify, and an
OpenAI reasoning item carries ``encrypted_content`` only OpenAI can decrypt.
Replaying one provider's block to another is a hard 400, so the request path
needs to know which routes can stand in for each other. That equivalence class
is a lineage, and it is keyed on the resolved *route* rather than the provider
key — see ``LLM._provider_route`` for why the two differ.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.llms.llm import ModelConfig

# Sentinel for "provenance could not be established". Treated as incompatible
# with every real lineage, so unattributable reasoning is always stripped.
UNKNOWN_LINEAGE = "unknown"

# The lineage that reaches api.anthropic.com. Named because the signature
# requirement on ``thinking`` blocks is that API's rule specifically — the
# Anthropic-compatible shims accept their own unsigned blocks happily.
ANTHROPIC_LINEAGE = "anthropic"

# Joins a provider key to an off-manifest endpoint in a route string. Absent
# from every manifest key, so its presence alone marks a redirected route.
ROUTE_ENDPOINT_SEP = "@"


def _config() -> ModelConfig:
    # Imported lazily: ``src.llms.llm`` is heavy and imports this module's
    # neighbours, so a module-level import would cycle.
    from src.llms.llm import LLM

    return LLM.get_model_config()


def lineage_for_route(route: str | None, config: ModelConfig | None = None) -> str:
    """Compatibility class for a resolved provider route.

    A route's lineage is its own key unless ``providers.json`` merges it into a
    ``reasoning_compat_group``. Absent that field every key stands alone, which
    is the safe default: an unnecessary split only drops a reasoning block,
    while an unwarranted merge breaks the turn.
    """
    if not route:
        return UNKNOWN_LINEAGE
    if ROUTE_ENDPOINT_SEP in route:
        # Redirected off its manifest endpoint, so the key's group says nothing
        # about who can verify these signatures. The route stands alone.
        return route
    info = (config or _config()).get_provider_info(route)
    # Declared on a provider *group* and inherited by its variants
    # (``_flatten_providers`` merges group fields into each), so declaring it on
    # a group whose variants are different upstreams — ``moonshot`` vs
    # ``moonshot-coding``, ``volcengine`` vs ``doubao-coding`` — silently
    # merges them. ``claude-oauth`` inherits ``anthropic`` correctly because
    # both reach api.anthropic.com; ``codex-oauth`` opts out with its own key.
    # ``test_reasoning_lineage`` locks the partition so a bad merge fails there.
    group = info.get("reasoning_compat_group")
    return group if isinstance(group, str) and group else route


@lru_cache(maxsize=2)
def _model_id_index(config: ModelConfig) -> dict[str, str | None]:
    """Map API model id → lineage, or ``None`` where the id is ambiguous.

    Keyed on the config object so a rebuilt manifest singleton gets a fresh
    index instead of silently reusing a stale one. Used only to attribute
    historical messages that predate origin stamping; a model id served by two
    incompatible routes (e.g. a BYOK provider and a platform proxy in a
    different group) resolves to ``None`` → strip.
    """
    index: dict[str, set[str]] = {}
    for model_info in config.llm_config.values():
        if not isinstance(model_info, dict):
            continue
        model_id = model_info.get("model_id")
        if not isinstance(model_id, str) or not model_id:
            continue
        for key in ("provider", "system_provider"):
            provider = model_info.get(key)
            if isinstance(provider, str) and provider:
                index.setdefault(model_id, set()).add(
                    lineage_for_route(provider, config)
                )
    return {
        model_id: next(iter(lineages)) if len(lineages) == 1 else None
        for model_id, lineages in index.items()
    }


def lineage_for_model_id(model_id: str | None) -> str:
    """Best-effort lineage for an API model id; ``UNKNOWN_LINEAGE`` if ambiguous."""
    if not model_id:
        return UNKNOWN_LINEAGE
    return _model_id_index(_config()).get(model_id) or UNKNOWN_LINEAGE
