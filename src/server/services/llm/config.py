"""Which model a turn runs, and everything that follows from the answer.

``resolve_llm_config`` sequences the stages: pick the model, resolve its
tuning, load the user's context, gate their capabilities, and build the clients
the turn will reach. Building a client is ``clients``; what a model name means
is ``user_models``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from ptc_agent.agent.roles import AgentRole
from ptc_agent.agent.subagents.merge import turn_roster

from src.llms.preferences import (
    Tuning,
    compaction_profile_for,
    custom_model,
    resolve_tuning,
)
from src.server.services.features import effective_flags_for_user
from src.tools.web.fetch import fetch_llm_client_override, fetch_model_override
from src.utils.concurrency import cancel_and_join

from .availability import (
    _MODEL_PREF_KEYS,
    cleanup_stale_model_preferences,
    drop_dead_models,
    drop_off_plan_models,
    model_on_plan,
    on_subscription,
    raise_byok_key_required,
    raise_model_not_on_plan,
    raise_model_removed,
)

from . import logger

from .capabilities import PlanTier, gate_capabilities
from .clients import resolve_clients
from .subagents import (
    demote_clientless,
    read_user_subagents,
    resolve_user_subagents,
    subagent_models,
    subagent_roles,
)
from . import user_models

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SLOT_MODEL_MAP = {
    "primary": ("name", "preferred_model"),
    "background": ("background", "preferred_background_model"),
}


def slot_model(llm: Any, slot: str) -> str:
    """The model ``slot`` runs on ``llm``; an unset slot falls back to the main model."""
    return getattr(llm, _SLOT_MODEL_MAP[slot][0], None) or llm.name


# Saved models a turn reaches besides its own. The Background key is one only
# on a primary turn, whose fetch inherits it. With no Background default saved,
# the Background slot is the primary key on either turn (``saved_default_model``),
# and that is the turn's own model either way, so never a role.
_ROLE_PREF_KEYS = {
    "primary": tuple(k for k in _MODEL_PREF_KEYS if k != "preferred_model"),
    "background": tuple(
        k
        for k in _MODEL_PREF_KEYS
        if k not in ("preferred_model", "preferred_background_model")
    ),
}


#: Values of ``model_preference.background_follows``: what an unset Background
#: default runs. Absent is ``primary``.
BACKGROUND_FOLLOWS = frozenset({"primary", "deployment"})


def saved_default_model(model_pref: dict, slot: str) -> str | None:
    """The default the user saved for ``slot``; None runs the deployment's.

    An unset Background default is the primary one, as Settings says ("Same as
    Primary model"), unless the user chose Auto for it, which keeps the
    deployment's Background model even beside a saved primary.
    """
    saved = model_pref.get(_SLOT_MODEL_MAP[slot][1])
    if saved or slot != "background" or model_pref.get("background_follows") == "deployment":
        return saved or None
    return model_pref.get("preferred_model") or None


def system_default_models() -> dict[str, str]:
    """The deployment's default models, as the models API publishes them."""
    from src.server.app import setup

    llm = setup.agent_config.llm if setup.agent_config and setup.agent_config.llm else None
    return {
        "default_model": llm.name if llm else "",
        "background_model": (llm.background or "") if llm else "",
    }


def deployment_key(provider: str) -> str | None:
    """The deployment's own key for a provider, when a user's turn may run on it.

    Standalone, the server's keys are its user's, which is how the turns use
    them. In platform mode they are the platform's, which only a plan reaches.
    Either way a turn takes one only for a manifest model naming the
    provider itself, never for a model the user picked
    (``resolve_model_client``) or one of another provider's rows, so a
    provider with no rows of its own (a gateway, a plan offering its brand's)
    gets none: listed as connected, its every pick would fail asking for a key.
    """
    import os

    from src.config.settings import HOST_MODE
    from src.llms.llm import LLM as LLMFactory

    mc = LLMFactory.get_model_config()
    env_key = mc.get_provider_info(provider).get("env_key")
    if HOST_MODE != "oss" or not env_key or not mc.runs_manifest_models(provider):
        return None
    return os.getenv(env_key) or None


def _default_route() -> tuple[str | None, dict]:
    """The provider the deployment's default model runs on with no user key,
    and its manifest entry; ``(None, {})`` without a default model."""
    from src.llms.llm import LLM as LLMFactory

    model = system_default_models()["default_model"]
    mc = LLMFactory.get_model_config()
    entry = mc.get_model_config(model) if model else None
    if not entry:
        return None, {}
    # Built without a user key, the client takes the system route (``LLM``).
    provider = entry.get("system_provider") or entry.get("provider")
    return provider, mc.get_provider_info(provider)


def deployment_local_provider() -> str | None:
    """The local server the deployment's default model runs on, standalone,
    where the server is its user's too. It needs no key, so nothing the user
    stored says it is there, and the key list names it on this."""
    from src.config.settings import HOST_MODE

    provider, info = _default_route()
    return provider if HOST_MODE == "oss" and info.get("access_type") == "local" else None


def deployment_serves_default() -> bool:
    """Whether a user with no credential of their own can run a turn: the
    deployment's default model on the deployment's own key or local server.

    Outside platform mode a turn needs no plan (the guard in ``messaging``), so
    this is the whole question; the web's setup gate asks it so it stops
    sending such a user to set up a provider the deployment already has. In
    platform mode the plan answers instead.
    """
    provider, info = _default_route()
    if provider is None:
        return False
    if info.get("access_type") == "local":
        return deployment_local_provider() is not None
    return deployment_key(provider) is not None


# ---------------------------------------------------------------------------
# Resolution stages. ``resolve_llm_config`` sequences these; each owns one
# question and mutates the request-scoped config copy in place.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModelSelection:
    """Which model this turn runs, and the ``LLMConfig`` field that named it.

    ``users_fallbacks`` is whether ``llm.fallback`` is a list the user saved,
    even an empty one, rather than the deployment's: only theirs may move an
    own-key turn onto the platform's key.
    """

    effective_model: str
    model_field: str
    users_fallbacks: bool


def select_model(
    config, model_pref: dict, slot: str, request_model: str | None
) -> ModelSelection:
    """Settle this turn's model and the other slots that follow from it.

    Priority is per-request > user preference > deployment default; slot picks
    which config field and preference key that means (see ``_SLOT_MODEL_MAP``).
    """
    from ptc_agent.config import LLMConfig

    model_field, pref_key = _SLOT_MODEL_MAP[slot]
    saved = saved_default_model(model_pref, slot)

    if config.llm is None:
        # Bootstrap when agent_config.yaml has llm: null. The user must have
        # configured a model via the UI or a per-request param.
        resolved_name = request_model or saved
        if not resolved_name:
            raise ValueError(
                "No model configured. Set llm in agent_config.yaml or select a model in Settings."
            )
        config.llm = LLMConfig(
            name=resolved_name if slot == "primary" else "placeholder",
            background=(
                resolved_name
                if slot == "background"
                else model_pref.get("preferred_background_model")
            ),
            compaction=(
                model_pref.get("compaction_model") or model_pref.get("summarization_model")
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
        logger.debug(f"[CHAT] Using the saved {slot} default: {saved}")
    else:
        logger.debug(
            f"[CHAT] No {pref_key} set, using system default: {slot_model(config.llm, slot)}"
        )

    # A primary turn never runs the Background model, but fetch defaults to it,
    # so the user's Background default reaches ``llm.background`` here too, by
    # the rule a Background turn runs on: an unset one is the saved primary.
    if slot != "background" and (
        background_default := saved_default_model(model_pref, "background")
    ):
        config.llm.background = background_default

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

    # With no model picked for it, a summary runs on the model the turn runs.
    # Written in, because ``compaction_name`` alone cannot tell a Background
    # turn's model from the primary one.
    if not config.llm.compaction:
        config.llm.compaction = slot_model(config.llm, slot)

    user_fallback = model_pref.get("fallback_models")
    if user_fallback is not None:
        config.llm.fallback = user_fallback
    # Repeats only: the turn's own model stays, since a subagent on another
    # model may still fall back to it. The resilience middleware drops
    # whichever model a request targets.
    if config.llm.fallback:
        config.llm.fallback = config.llm.fallbacks_for(None)

    return ModelSelection(
        effective_model=slot_model(config.llm, slot),
        model_field=model_field,
        users_fallbacks=user_fallback is not None,
    )


async def gate_models_by_plan(
    base_config, user_id: str, model_pref: dict, request_model: str | None,
    *, slot: str, permits: PlanTier, is_byok: bool,
) -> dict:
    """``model_pref`` once the plan has had its say on what runs on the
    platform's key.

    A model the turn names is refused when the plan does not cover it:
    someone chose it, and running another would answer a question they did
    not ask. Every saved model reads as unset instead
    (``drop_off_plan_models``), the Default included, so a turn that names
    none runs the deployment's, as the composer starts on it. The
    deployment's defaults are the operator's call, as a subagent's own pin
    is, and pass where they would run unasked: its main model anywhere, its
    Background only in the Background slot and the roles that inherit it,
    not as a primary turn's own model.
    """
    llm = base_config.llm
    main = frozenset({llm.name}) if llm else frozenset()
    background = main | {slot_model(llm, "background")} if llm else frozenset()
    exempt = {key: background for key in _MODEL_PREF_KEYS}
    exempt["preferred_model"] = exempt["fallback_models"] = main
    named_exempt = background if slot == "background" else main
    byok_cache: dict[str, dict | None] = {}

    async def on_plan(name: str) -> bool:
        return await model_on_plan(
            user_id, name, model_pref=model_pref, permits=permits,
            is_byok=is_byok, byok_cache=byok_cache,
        )

    if request_model and request_model not in named_exempt and not await on_plan(request_model):
        await permits.refresh()
        if not await on_plan(request_model):
            if await permits.tier() is None:
                from src.server.dependencies.usage_limits import quota_service_unavailable

                raise quota_service_unavailable()
            raise_model_not_on_plan(request_model)

    view, dropped = await drop_off_plan_models(
        user_id, model_pref, permits=permits, is_byok=is_byok,
        byok_cache=byok_cache, exempt=exempt,
    )
    if dropped:
        logger.info(f"[CHAT] Ignoring saved models off plan for user={user_id}: {dropped}")
    return view


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


#: The most of a model's window a compaction threshold may claim. The rest is
#: the reply, the tool results that land before the next check, and the
#: summary call itself, which reads past the threshold.
_THRESHOLD_SHARE = (3, 4)


def apply_compaction(
    config, tuning: Tuning, entry: dict[str, Any] | None, *, custom: bool = False
) -> None:
    """Apply the named compaction preset, capped to what the model's window
    can hold.

    The user's choice wins; failing that the model answers for itself, by
    declaration or by the band its context window falls in. Only a model that
    states no window keeps the YAML default, which is one number for every
    model and so is right for almost none of them.

    An account-wide preset reaches every model, so a 750k preset chosen for a
    1M model also lands on a 400k one, where it would never trigger and every
    long turn would compact only after the provider refused it.

    ``native`` is the user's per-model choice. A ``custom`` model never
    compacts natively: the settings offer the choice only for manifest models,
    so one of the user's own would compact with no switch to turn it off. The
    ``provider_compaction`` feature, and whether the provider can compact for
    this turn, are weighed when the middleware is built and called.
    """
    from ptc_agent.config.agent import COMPACTION_PROFILES

    config.compaction.native = not custom and tuning.native_compaction is not False
    profile = tuning.compaction_profile
    if not isinstance(profile, str):
        profile = compaction_profile_for(entry)
    preset = COMPACTION_PROFILES.get(profile) if isinstance(profile, str) else None
    if not preset:
        return
    for field, value in preset.items():
        setattr(config.compaction, field, value)
    config.compaction.profile = profile
    context = (entry or {}).get("context")
    if isinstance(context, int) and context > 0:
        share, of = _THRESHOLD_SHARE
        config.compaction.token_threshold = min(
            config.compaction.token_threshold, context * share // of
        )


def apply_fetch_override(config) -> None:
    """Propagate fetch model / client overrides from *config* into context vars.

    A blank fetch means the turn's Background model, the same default
    ``role_registry`` resolves a client for. Without the name override,
    web_fetch would fall back to re-reading agent_config.yaml and miss the
    user's own Background model.
    """
    fetch_client = config.subsidiary_llm_clients.get("fetch")
    if fetch_client:
        fetch_llm_client_override.set(fetch_client)
    fetch_model = config.llm and config.llm.fetch_name
    if fetch_model:
        fetch_model_override.set(fetch_model)


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
    # namespace (enforced by ``validate_custom_models``), so one call answers
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

    # Custom model/provider requires BYOK, unless a subscription serves it.
    # No silent fallback — raise a clear error so the frontend can show a CTA
    # linking to Settings.
    if (is_custom or is_custom_provider) and not is_byok:
        if not (is_custom and on_subscription(resolved_config)):
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
    slot: str = "primary",
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
    thread_id: str | None = None,
    *,
    enabled_subagents: list[str] | None = None,
    workspace_id: str | None = None,
    user_facing: bool = True,
    role: AgentRole = "analyst",
):
    """Resolve the final LLM config for one turn: which model runs, how it is
    tuned, what it is allowed to reach, and every client it can call.

    ``is_byok=None`` self-resolves via ``is_byok_active`` (guards future entry
    points; all current callers pass it explicitly). ``enabled_subagents``
    narrows the user's subagents to the request's list; ``None`` is all of
    them, and ``role`` is the agent the turn builds, whose subagents alone get
    clients. ``user_facing=False`` is a call no user is waiting on (a title, memo
    metadata): nobody would see the ``model_removed`` CTA, so its own saved
    model is read like a role and nothing is scrubbed.
    """
    reads: asyncio.Task | None = None
    try:
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
        role_keys = _ROLE_PREF_KEYS[slot] if user_facing else _MODEL_PREF_KEYS
        model_pref, dropped = drop_dead_models(saved_pref, role_keys)
        if dropped:
            logger.info(f"[CHAT] Ignoring saved models not in the manifest for user={user_id}: {dropped}")

        # One tier read for every plan gate the turn runs.
        permits = PlanTier(user_id)
        model_pref = await gate_models_by_plan(
            base_config, user_id, model_pref, request_model,
            slot=slot, permits=permits, is_byok=is_byok,
        )

        # The user's own subagents, for a build that compiles any. Their reads
        # depend on nothing below, so they start now and the merge waits for
        # them; not before the plan check, whose refusal is routine and would
        # cancel them inside their transaction.
        if slot == "primary" and user_facing:
            reads = asyncio.create_task(read_user_subagents(user_id, base_config.subagents))

        await load_user_context(config, user_id, workspace_id, model_pref)
        selection = select_model(config, model_pref, slot, request_model)

        # The model running this turn is settled here: everything below tunes it
        # rather than choosing it. Tuning reads go through the per-model resolver so
        # a profile beats the account-wide value; reading the account-wide value
        # directly is what applied one compaction threshold to both a 200k-context
        # model and a 1M one.
        tuning = resolve_tuning(model_pref, selection.effective_model)
        config.prompt_guidance = user_models.guidance_for(model_pref, selection.effective_model)
        apply_compaction(
            config,
            tuning,
            user_models.model_entry(model_pref, selection.effective_model),
            custom=custom_model(model_pref, selection.effective_model) is not None,
        )
        await gate_capabilities(config, user_id, model_pref, permits)

        # After the gate, whose tier check the merge reuses, and before the
        # clients, which read the merged definitions for their roles.
        if reads is not None:
            config.subagents.resolved = await resolve_user_subagents(
                user_id, await reads, model_pref=model_pref, permits=permits,
                is_byok=is_byok, search_api=config.search_api,
                search_depth=config.search_depth, requested=enabled_subagents,
            )
    finally:
        # A stage above that raises must not leave the reads running, or let
        # their own failure go unretrieved.
        if reads is not None:
            await cancel_and_join(reads)

    # The saved prefs, not the view: attribution asks whether the refused name
    # is one the user saved, and a dropped role may be that name.
    user_defined = await check_model_available(
        config, user_id, saved_pref, selection.effective_model,
        request_model, is_byok=is_byok, scrub=user_facing,
    )

    # Only a PTC build compiles subagents, so only it builds their clients.
    roles = (
        subagent_roles(turn_roster(config.subagents), model_pref, selection.effective_model, role=role)
        if slot == "primary"
        else []
    )

    # The request's overrides go down whole. Per-request > per-model profile >
    # account-wide is applied per client by ``resolve_clients``, because each
    # one runs a different model and a level only means something against the
    # ladder of the model that will honor it. An unsupported level degrades in
    # ``LLM.__init__``, the one place that sees both the request and the ladder.
    await resolve_clients(
        config, user_id, selection.effective_model, model_pref, roles,
        is_byok=is_byok, thread_id=thread_id,
        reasoning_effort=reasoning_effort,
        fast_mode=fast_mode,
        user_defined=user_defined,
        users_fallbacks=selection.users_fallbacks,
    )
    demote_clientless(config)
    if slot == "primary":
        config.subagent_models = subagent_models(
            config, model_pref, selection.effective_model, role=role,
        )

    return config
