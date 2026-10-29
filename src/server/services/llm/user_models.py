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


def model_preference_of(prefs: dict | None) -> dict:
    """Settings that drive model resolution, read from a preferences row; never
    ``agent_preference``, which is dumped to agent context.

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


async def get_model_preference(user_id: str) -> dict:
    """This user's model settings, merged as ``model_preference_of`` reads a row."""
    from src.server.database.user import get_user_preferences

    return without_removed_providers(model_preference_of(await get_user_preferences(user_id)))


def without_removed_providers(model_pref: dict) -> dict:
    """``model_pref`` without the custom providers and models on a provider the
    manifest no longer has.

    No client can be built for one, so every reader treats its name as gone:
    a pick of it gets the ``model_removed`` banner and the scrub, as a removed
    manifest model does, rather than an internal error. The row is not
    rewritten, so restoring the provider restores them. An empty manifest is a
    load failure, not a catalog with nothing in it, and removes nothing.
    """
    from src.llms.llm import LLM as LLMFactory

    providers = LLMFactory.get_model_config().flat_providers
    if not providers:
        return model_pref
    live = dict(model_pref)
    custom_providers = model_pref.get("custom_providers")
    if isinstance(custom_providers, list):
        live["custom_providers"] = [
            cp for cp in custom_providers
            if not isinstance(cp, dict) or _names(cp.get("parent_provider"), providers)
        ]
    live_names = set(providers) | {
        cp["name"] for cp in live.get("custom_providers") or []
        if isinstance(cp, dict) and isinstance(cp.get("name"), str)
    }
    custom_models = model_pref.get("custom_models")
    if isinstance(custom_models, list):
        live["custom_models"] = [
            cm for cm in custom_models
            if not isinstance(cm, dict) or _names(cm.get("provider"), live_names)
        ]
    return live


def _names(value: object, names) -> bool:
    # Stored values were never type-checked on read: a list here would raise.
    return isinstance(value, str) and value in names


def model_entry(model_pref: dict, name: str | None) -> dict[str, Any] | None:
    """The model's own row: the user's custom entry over the manifest's.

    A custom model shadows a built-in of the same name, so every rule that
    reads what a model declares about itself has to consult both, in this
    order. Compaction and prompt guidance both do, and used to each carry their
    own copy of the order. A shadow reaches the same model through the user's
    own key, so what it does not declare it inherits rather than losing.

    Routed as the resolver routes an entry whose key sits under its own
    provider. Two of the resolver's routes depend on where the key is stored,
    which a read with no keys in hand cannot see: a key under a custom provider
    named after the model, and one held by a sibling variant, whose endpoint
    the client is then built against. There the ladder read here can differ
    from the client's; a level is clamped against the client's own ladder when
    sent, so what differs is a level shown, not one sent. Refusing such a
    holder instead would strand every model whose only key is a sibling's.
    """
    if not name:
        return None
    from src.llms.llm import LLM as LLMFactory
    from src.llms.model_spec import inherited_declarations, shadowed_model

    mc = LLMFactory.get_model_config()
    entry = custom_model(model_pref, name)
    if entry is None:
        return mc.get_model_config(name)
    routed = entry
    provider_def = custom_provider(model_pref, entry.get("provider"))
    if provider_def is not None and isinstance(provider_def.get("parent_provider"), str):
        routed = route_custom_entry(entry, provider_def, mc)
    # Judged on the route, laid over the row as stored: routing is the
    # resolver's to apply, not a declaration of the model's.
    inherited = inherited_declarations(routed, shadowed_model(routed, mc), mc)
    return {**entry, **inherited} if inherited else entry


def with_borrowed_ladders(model_pref: dict) -> dict:
    """``model_pref`` with an empty ladder spelled out on each custom model
    whose route borrows none of the built-in ladder its name would suggest.

    The web fills a custom entry that declares no ladder from the built-in of
    the same name. A route on another API borrows nothing (``model_entry``), and
    an entry saved since says so itself (``reasoning: {efforts: []}``). Older
    entries do not, so the menu would offer levels a save then refuses.
    """
    from src.llms.llm import LLM as LLMFactory
    from src.llms.model_spec import reasoning_block

    entries = model_pref.get("custom_models")
    if not isinstance(entries, list):
        return model_pref
    mc = LLMFactory.get_model_config()
    spelled = []
    for entry in entries:
        name = entry.get("name") if isinstance(entry, dict) else None
        builtin = mc.get_model_config(name) if isinstance(name, str) else None
        if (
            builtin
            and not reasoning_block(entry)
            and reasoning_block(builtin).get("efforts")
            and not reasoning_block(model_entry(model_pref, name)).get("efforts")
        ):
            entry = {**entry, "reasoning": {"efforts": []}}
        spelled.append(entry)
    return {**model_pref, "custom_models": spelled}


def custom_provider(model_pref: dict, name: object) -> dict | None:
    """The user's custom provider ``name``, if they defined one."""
    for cp in model_pref.get("custom_providers") or []:
        if isinstance(cp, dict) and cp.get("name") == name:
            return cp
    return None


# Parents with no dialect of their own. ``openai`` is the base shape a great
# many services implement rather than a vendor whose behaviour you would want
# copied, and a provider declaring ``sdk: "openai"`` is saying exactly that
# about itself. A parent on this list is skipped by ``route_custom_entry``, so
# its OpenAI-specific request shaping (``use_response_api``,
# ``prompt_cache_key``) stays opt-in.
_OPENAI_SHAPED_SDKS = (None, "openai")


def route_custom_entry(entry: dict, provider_def: dict, mc) -> dict:
    """``entry`` on the user's custom provider ``provider_def``, rewritten onto
    the manifest parent whose SDK and headers it speaks (#221).

    A user-defined custom provider slug isn't in the manifest, so the factory
    derives ``sdk`` from an empty ``provider_info`` and defaults to
    ``"openai"``, which 404s an Anthropic-shaped endpoint (``/chat/completions``
    vs ``/v1/messages``). Rewriting ``provider`` to the manifest parent fixes
    the SDK and ``default_headers``.

    Naming a parent is how a custom provider says what it is, so it gets that
    parent's dialect: SDK, ``default_headers``, and the request shaping the
    manifest entry declares. ``openai`` is the exception, because it is the base
    shape a great many unrelated services implement rather than a vendor whose
    behaviour a gateway means to copy. Declaring it says "my API is OpenAI
    shaped", not "treat me as OpenAI", so its ``use_response_api`` and
    ``prompt_cache_key`` stay opt-in and a gateway that only speaks
    ``/chat/completions`` is left alone. Providers that declare ``sdk:
    "openai"`` themselves (vLLM, Groq, DeepInfra, LM Studio) are saying the same
    thing about themselves and are covered by the same entry.

    A parent with its own SDK is a real dialect, so declaring it matches
    everything. DashScope is the case that makes this concrete: it has its own
    client because its raw reasoning is rescued by a bridge scoped to
    ``ChatDashScope``, and a gateway left on a plain ``ChatOpenAI`` would stream
    every Qwen turn with the thinking silently dropped.
    """
    parent = provider_def["parent_provider"]
    updates: dict = {}
    if mc.get_provider_info(parent).get("sdk") not in _OPENAI_SHAPED_SDKS:
        updates["provider"] = parent
    elif entry.get("provider") == provider_def.get("name") and mc.get_provider_info(entry["provider"]):
        # The manifest has since given this name a provider of its own (``xai``
        # was added after users had set it up by hand). Kept on its name, the
        # entry would take that provider's dialect instead of the opt-in one
        # every other custom provider with this parent gets.
        updates["_own_provider"] = True
    # Tri-state, not a truthiness check: the save validates this field as a
    # boolean and so accepts an explicit ``false``, which is a gateway saying it
    # speaks only ``/chat/completions``. Collapsing that into "absent" hands it
    # the parent's manifest default instead, and for a parent that defaults the
    # Responses API on, every call would go to ``/responses`` and fail.
    declared_response_api = provider_def.get("use_response_api")
    if declared_response_api is not None:
        updates["_use_response_api"] = bool(declared_response_api)
    return {**entry, **updates} if updates else entry


async def get_custom_model_config(user_id: str, model_name: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined custom model by name from ``model_preference.custom_models``."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    return custom_model(model_pref, model_name)


async def get_custom_provider_config(user_id: str, provider: str, _pref_cache: dict | None = None) -> dict | None:
    """Look up a user-defined sub-provider config (name, parent_provider, use_response_api, etc.)."""
    model_pref = _pref_cache if _pref_cache is not None else await get_model_preference(user_id)
    return custom_provider(model_pref, provider)


# ---------------------------------------------------------------------------
# Central model classification — single entry point used by every call site
# that needs to answer "what is this model?". System vs custom is a flat
# namespace guaranteed by ``validate_custom_models`` (custom_catalog.py), so this
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
