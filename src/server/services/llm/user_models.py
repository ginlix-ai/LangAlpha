"""The models and providers a user has defined, and what a model name means.

Read by everything downstream: which client to build, which endpoint to build
it against, and which declarations a model makes about itself.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from src.llms.preferences import custom_model, resolve_tuning_field

#: Model-resolution inputs that were never part of the column move and stay in
#: ``other_preference`` permanently. Named so the legacy shim below can be
#: deleted on its own without silently taking these with it.
_NEVER_MOVED = ("feature_overrides", "search_provider", "search_depth")


async def get_model_preference(user_id: str) -> dict:
    """Settings that drive model resolution — never ``agent_preference``, which is dumped to agent context.

    The model keys have their own column. ``other_preference`` is read
    underneath it for two unrelated reasons: a rollback window for rows written
    before the column existed, and the three keys in ``_NEVER_MOVED`` that live
    there by design. Only the first is temporary — dropping it means deleting
    the ``MOVED_MODEL_KEYS`` half of the filter, nothing else.

    Absence in the column is the only thing that reaches the copy underneath.
    Clearing a key clears both columns at the write, so nothing here has to tell
    a cleared preference apart from one that predates the move. A preference
    that came back from underneath would be raised on, cleared, and raised on
    again every turn.
    """
    from src.llms.preferences import MOVED_MODEL_KEYS
    from src.server.database.user import get_user_preferences

    prefs = await get_user_preferences(user_id)
    if not prefs:
        return {}
    legacy = prefs.get("other_preference") or {}
    current = prefs.get("model_preference") or {}
    # Merging the whole legacy bag conflated the two reasons above, and pulled
    # in keys other services write to that column.
    merged = {
        k: v
        for k, v in legacy.items()
        if (k in MOVED_MODEL_KEYS or k in _NEVER_MOVED) and k not in current
    }
    merged.update({k: v for k, v in current.items() if v is not None})
    return merged


def model_entry(model_pref: dict, name: str | None) -> dict[str, Any] | None:
    """The model's own row: the user's custom entry over the manifest's.

    A custom model shadows a built-in of the same name, so every rule that
    reads what a model declares about itself has to consult both, in this
    order. Compaction and prompt guidance both do, and used to each carry their
    own copy of the order. A shadow reaches the same model through the user's
    own key, so what it does not declare it inherits rather than losing.
    """
    if not name:
        return None
    from src.llms.llm import LLM as LLMFactory
    from src.llms.model_spec import with_inherited_declarations

    builtin = LLMFactory.get_model_config().get_model_config(name)
    entry = custom_model(model_pref, name)
    if entry is not None:
        return with_inherited_declarations(entry, builtin)
    return builtin


async def get_custom_model_config(user_id: str, model_name: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined custom model by name from ``model_preference.custom_models``."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    return custom_model(model_pref, model_name)


async def get_custom_provider_config(user_id: str, provider: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined sub-provider config (name, parent_provider, use_response_api, etc.)."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    for cp in model_pref.get("custom_providers") or []:
        if cp.get("name") == provider:
            return cp
    return None


# ---------------------------------------------------------------------------
# Central model classification — single entry point used by every call site
# that needs to answer "what is this model?". System vs custom is a flat
# namespace guaranteed by ``_validate_custom_models`` (users.py), so this
# function does at most one in-memory dict hit and one pref-cache scan.
# ---------------------------------------------------------------------------


class ModelSource(StrEnum):
    SYSTEM = "system"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


async def classify_model(
    user_id: str,
    model_name: str,
    _pref_cache: dict | None = None,
) -> tuple[str, dict]:
    """Classify ``model_name`` as system / custom / unknown.

    Returns a ``(source, config)`` pair where ``config`` is:
      - the user's ``custom_models`` entry for custom models
      - the entry from ``models.json`` for system models
      - ``{}`` for unknown

    Custom is checked first. When a user's ``custom_models`` entry shadows a
    built-in of the same name, the custom entry wins — lets users route a
    built-in model name (e.g. ``glm-5.2``) through a variant's own key.
    ``_pref_cache`` keeps the chat hot path free of extra DB reads.
    """
    from src.llms.llm import LLM as LLMFactory

    custom_cm = await get_custom_model_config(user_id, model_name, _pref_cache=_pref_cache)
    if custom_cm:
        return ModelSource.CUSTOM, custom_cm

    mc = LLMFactory.get_model_config()
    system_info = mc.get_model_config(model_name)
    if system_info:
        return ModelSource.SYSTEM, system_info

    return ModelSource.UNKNOWN, {}


def guidance_for(model_pref: dict, model_name: str | None) -> str:
    """Prompt scaffolding level for the model named here.

    The user's choice for that model comes first; failing that the deployment
    pin and the model's own declaration answer, which is where a custom model
    (no manifest row) gets its say.
    """
    from ptc_agent.agent.prompts.guidance import (
        VALID_GUIDANCE,
        resolve_prompt_guidance,
    )

    chosen = resolve_tuning_field(model_pref, model_name, "prompt_guidance")
    if chosen in VALID_GUIDANCE:
        return chosen
    return resolve_prompt_guidance(model_name, model_entry(model_pref, model_name))
