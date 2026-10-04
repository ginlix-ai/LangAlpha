"""Which model a turn runs, and everything that follows from the answer.

``resolve_llm_config`` sequences the stages: pick the model, resolve its
tuning, load the user's context, gate their capabilities, and build the clients
the turn will reach. Building a client is ``clients``; what a model name means
is ``user_models``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.llms.preferences import (
    Tuning,
    compaction_profile_for,
    resolve_tuning,
)
from src.server.services.features import effective_flags_for_user

from .availability import (
    _MODEL_PREF_KEYS,
    cleanup_stale_model_preferences,
    drop_dead_models,
    raise_byok_key_required,
    raise_model_removed,
)

from . import logger

from .capabilities import gate_capabilities
from .clients import resolve_clients
from . import user_models

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODE_MODEL_MAP = {
    "ptc": ("name", "preferred_model"),
    "flash": ("flash", "preferred_flash_model"),
}

# Saved models a turn reaches besides its own. The flash key is one only on a
# PTC turn, whose compaction and fetch inherit it. With no flash default saved,
# the flash slot is the PTC key on either turn (``saved_default_model``), and
# that is the turn's own model either way, so never a role.
_ROLE_PREF_KEYS = {
    "ptc": tuple(k for k in _MODEL_PREF_KEYS if k != "preferred_model"),
    "flash": tuple(
        k for k in _MODEL_PREF_KEYS if k not in ("preferred_model", "preferred_flash_model")
    ),
}


#: Values of ``model_preference.flash_follows``: what an unset flash default
#: runs. Absent is ``primary``.
FLASH_FOLLOWS = frozenset({"primary", "deployment"})


def saved_default_model(model_pref: dict, mode: str) -> str | None:
    """The default the user saved for ``mode``; None runs the deployment's.

    An unset flash default is the primary one, as Settings says ("Same as
    Primary model"), unless the user chose Auto for flash, which keeps the
    deployment's flash model even beside a saved primary.
    """
    saved = model_pref.get(_MODE_MODEL_MAP[mode][1])
    if saved or mode != "flash" or model_pref.get("flash_follows") == "deployment":
        return saved or None
    return model_pref.get("preferred_model") or None


def system_default_models() -> dict[str, str]:
    """The deployment's default models, as the models API publishes them."""
    from src.server.app import setup

    llm = setup.agent_config.llm if setup.agent_config and setup.agent_config.llm else None
    return {
        "default_model": llm.name if llm else "",
        "flash_model": (llm.flash or "") if llm else "",
    }


# ---------------------------------------------------------------------------
# Resolution stages. ``resolve_llm_config`` sequences these; each owns one
# question and mutates the request-scoped config copy in place.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModelSelection:
    """Which model this turn runs, and the ``LLMConfig`` field that named it."""

    effective_model: str
    model_field: str


def select_model(
    config, model_pref: dict, mode: str, request_model: str | None
) -> ModelSelection:
    """Settle this turn's model and the other slots that follow from it.

    Priority is per-request > user preference > deployment default; mode picks
    which config field and preference key that means (see ``_MODE_MODEL_MAP``).
    """
    from ptc_agent.config import LLMConfig

    model_field, pref_key = _MODE_MODEL_MAP[mode]
    saved = saved_default_model(model_pref, mode)

    if config.llm is None:
        # Bootstrap when agent_config.yaml has llm: null. The user must have
        # configured a model via the UI or a per-request param.
        resolved_name = request_model or saved
        if not resolved_name:
            raise ValueError(
                "No model configured. Set llm in agent_config.yaml or select a model in Settings."
            )
        config.llm = LLMConfig(
            name=resolved_name if mode == "ptc" else "placeholder",
            flash=resolved_name if mode == "flash" else model_pref.get("preferred_flash_model"),
            compaction=(
                model_pref.get("compaction_model")
                or model_pref.get("summarization_model")
                or model_pref.get("preferred_flash_model")
            ),
            fetch=model_pref.get("fetch_model"),
            fallback=model_pref.get("fallback_models"),
        )
        config.llm_client = None
        logger.debug(f"[CHAT] No system default LLM; bootstrapped from user preferences: {resolved_name}")
    elif request_model:
        setattr(config.llm, model_field, request_model)
        config.llm_client = None
        logger.debug(f"[CHAT] Using per-request LLM model: {request_model}")
    elif saved:
        setattr(config.llm, model_field, saved)
        config.llm_client = None
        logger.debug(f"[CHAT] Using the saved {mode} default: {saved}")
    else:
        logger.debug(
            f"[CHAT] No {pref_key} set, using system default: {getattr(config.llm, model_field, None) or config.llm.name}"
        )

    # A PTC turn never runs the flash model, but compaction and fetch default to
    # it, so the user's flash default reaches ``llm.flash`` here too, by the
    # rule a flash turn runs on: an unset one is the saved primary.
    if mode != "flash" and (flash_default := saved_default_model(model_pref, "flash")):
        config.llm.flash = flash_default

    # Both "compaction_model" (new) and "summarization_model" (legacy) map to
    # the renamed ``compaction`` field; legacy is read so existing rows keep
    # working. Order matters: legacy first, so the new key wins when both are set.
    for other_key, config_field in (
        ("summarization_model", "compaction"),
        ("compaction_model", "compaction"),
        ("fetch_model", "fetch"),
    ):
        user_val = model_pref.get(other_key)
        if user_val:
            setattr(config.llm, config_field, user_val)

    user_fallback = model_pref.get("fallback_models")
    if user_fallback is not None:
        config.llm.fallback = user_fallback

    return ModelSelection(
        effective_model=getattr(config.llm, model_field, None) or config.llm.name,
        model_field=model_field,
    )


async def load_user_context(
    config, user_id: str, workspace_id: str | None, model_pref: dict
) -> None:
    """Per-user feature flags and skill tier, onto ``config`` in place.

    Resolved once here so every build surface reads ``config.feature_enabled()``
    instead of global state. ``workspace_id`` scopes the skill tier: workspace
    rows shadow user rows and workspace disables apply; ``None`` (maintenance
    paths) resolves the user tier alone.
    """
    from src.server.services.user_skills import load_user_skill_bundle

    # Reuses this turn's prefs read; plan gates fetch the platform tier lazily
    # inside the flag resolver, only when a plan feature exists.
    overrides = model_pref.get("feature_overrides")
    config.features = await effective_flags_for_user(
        user_id, overrides if isinstance(overrides, dict) else {}
    )

    # One query + the prefs read above; a skill-less user costs a single
    # indexed SELECT and sets nothing.
    bundle = await load_user_skill_bundle(user_id, workspace_id)
    config.user_skills = list(bundle.skills)
    config.disabled_skills = bundle.disabled_builtins
    config.user_skill_dir = bundle.dir
    config.workspace_skill_dir = bundle.workspace_dir
    config.skill_command_overrides = dict(bundle.command_overrides)


def apply_compaction(config, tuning: Tuning, entry: dict[str, Any] | None) -> None:
    """Apply the named compaction preset (aggressive/moderate/extended/relaxed).

    The user's choice wins; failing that the model answers for itself, by
    declaration or by the band its context window falls in. Only a model that
    states no window keeps the YAML default, which is one number for every
    model and so is right for almost none of them.
    """
    from ptc_agent.config.agent import COMPACTION_PROFILES

    profile = tuning.compaction_profile
    if not isinstance(profile, str):
        profile = compaction_profile_for(entry)
    preset = COMPACTION_PROFILES.get(profile) if isinstance(profile, str) else None
    if not preset:
        return
    for field, value in preset.items():
        setattr(config.compaction, field, value)
    config.compaction.profile = profile


async def check_model_available(
    config, user_id: str, model_pref: dict, effective_model: str,
    request_model: str | None, *, is_byok: bool, scrub: bool = True,
) -> bool:
    """Fail loudly when this turn's model cannot be served; report whether the
    name is user-defined.

    Returns True for a custom model or a custom-provider slug, which the client
    resolver needs to tell "no usable key" from "no client wanted yet".
    """
    # Classify via the single entry point. System and custom share a flat
    # namespace (enforced by ``_validate_custom_models``), so one call answers
    # the question for the entire downstream flow.
    source, resolved_config = await user_models.classify_model(
        user_id, effective_model, _pref_cache=model_pref
    )
    is_custom = source == user_models.ModelSource.CUSTOM
    # ``is_custom_provider`` only matters when the model name didn't classify
    # as a known custom model — catches the case where the user typed a
    # custom *provider* slug as the model preference.
    is_custom_provider = source == user_models.ModelSource.UNKNOWN and (
        await user_models.get_custom_provider_config(
            user_id, effective_model, _pref_cache=model_pref
        )
        is not None
    )

    # Custom model/provider requires BYOK. No silent fallback — raise a clear
    # error so the frontend can show a CTA linking to Settings.
    if (is_custom or is_custom_provider) and not is_byok:
        raise_byok_key_required(effective_model)

    # Stale-model recovery. Scrub prefs if the user's saved name is the
    # culprit; raise a user-facing CTA either way. YAML-default UNKNOWN
    # falls through so the downstream error surfaces the config bug.
    if source == user_models.ModelSource.UNKNOWN and not is_custom_provider:
        # Only the five scalar keys feed ``effective_model`` — fallback_models
        # is resolved separately in ``_resolve_fallback_clients`` and never
        # flows through here, so it's intentionally excluded from this
        # attribution check (the scrub in ``cleanup_stale_model_preferences``
        # still filters fallback_models once it fires).
        if any(model_pref.get(k) == effective_model for k in _MODEL_PREF_KEYS):
            removed = await cleanup_stale_model_preferences(user_id) if scrub else []
            raise_model_removed(effective_model, removed)
        elif request_model == effective_model:
            raise_model_removed(effective_model, [])

    if is_custom and resolved_config.get("input_modalities"):
        config.input_modalities = resolved_config["input_modalities"]

    return is_custom or is_custom_provider


async def resolve_llm_config(
    base_config,
    user_id: str,
    request_model: str | None,
    is_byok: bool | None = None,
    mode: str = "ptc",
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
    thread_id: str | None = None,
    *,
    enabled_subagents: list[str] | None = None,
    workspace_id: str | None = None,
    user_facing: bool = True,
):
    """Resolve the final LLM config for one turn: which model runs, how it is
    tuned, what it is allowed to reach, and every client it can call.

    ``is_byok=None`` self-resolves via ``is_byok_active`` (guards future entry
    points; all current callers pass it explicitly). ``enabled_subagents``
    threads the request's active subagent list so per-subagent model roles get
    their own credential resolution; ``None`` falls back to the config default.
    ``user_facing=False`` is a call no user is waiting on (a title, memo
    metadata): nobody would see the ``model_removed`` CTA, so its own saved
    model is read like a role and nothing is scrubbed.
    """
    if is_byok is None:
        from src.server.database.api_keys import is_byok_active

        is_byok = await is_byok_active(user_id)

    # One copy up front. Everything below mutates the request's own config, so
    # nothing has to reason about whether it is still aliased to the caller's.
    config = base_config.model_copy(deep=True)
    saved_pref = await user_models.get_model_preference(user_id)

    # A role or fallback saved as a model this manifest lacks resolves as if
    # never set: without this a platform user's role reaches the name-based
    # factory and every turn fails at graph build. The view is read-only.
    # "Absent here" is not "retired" (a rollback or an older worker knows fewer
    # models), so nothing is deleted on the strength of it. The turn's own key
    # is not a role; a dead one there is refused below with a CTA instead.
    role_keys = _ROLE_PREF_KEYS[mode] if user_facing else _MODEL_PREF_KEYS
    model_pref, dropped = drop_dead_models(saved_pref, role_keys)
    if dropped:
        logger.info(f"[CHAT] Ignoring saved models not in the manifest for user={user_id}: {dropped}")

    await load_user_context(config, user_id, workspace_id, model_pref)
    selection = select_model(config, model_pref, mode, request_model)

    # The model running this turn is settled here — everything below tunes it
    # rather than choosing it. Tuning reads go through the per-model resolver so
    # a profile beats the account-wide value; reading the account-wide value
    # directly is what applied one compaction threshold to both a 200k-context
    # model and a 1M one.
    tuning = resolve_tuning(model_pref, selection.effective_model)
    config.prompt_guidance = user_models.guidance_for(model_pref, selection.effective_model)
    apply_compaction(config, tuning, user_models.model_entry(model_pref, selection.effective_model))
    await gate_capabilities(config, user_id, model_pref)

    # The saved prefs, not the view: attribution asks whether the refused name
    # is one the user saved, and a dropped role may be that name.
    user_defined = await check_model_available(
        config, user_id, saved_pref, selection.effective_model,
        request_model, is_byok=is_byok, scrub=user_facing,
    )

    # The request's overrides go down whole. Per-request > per-model profile >
    # account-wide is applied per client by ``resolve_clients``, because each
    # one runs a different model and a level only means something against the
    # ladder of the model that will honor it. An unsupported level degrades in
    # ``LLM.__init__``, the one place that sees both the request and the ladder.
    await resolve_clients(
        config, user_id, selection.effective_model, model_pref, enabled_subagents,
        is_byok=is_byok, thread_id=thread_id,
        reasoning_effort=reasoning_effort,
        fast_mode=fast_mode,
        user_defined=user_defined,
    )

    return config
