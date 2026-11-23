"""The model one Task names, resolved and gated as a turn's own model is.

``resolve_llm_config`` builds the gate once a primary turn's model is
settled, over what it read for that turn: the plan-gated preferences, the
tier, whose credential pays and where Claude Code runs. The Task tool reaches
it from inside the turn's graph, on the turn's own worker, so it is execution
context for that turn and holds nothing another worker needs: a resume on a
later turn reads the name back from the run's ledger row and resolves it on
that turn's gate.

A model that cannot run is refused, never replaced: the agent named it
because the user or a skill asked for it, and running another would answer a
question nobody asked.
"""

from __future__ import annotations

from fastapi import HTTPException

from ptc_agent.agent.subagents.merge import turn_roster
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.config.agent import AgentConfig, Admitted, TaskAsk, TaskModel, TaskModelRefused
from src.llms.claude_code import is_claude_code_model
from src.llms.llm import LLM as LLMFactory, stamp_call_metadata
from src.llms.model_identity import catalog_model
from src.llms.preferences import resolve_tuning

from . import logger, user_models
from .availability import model_resolves, plan_covers
from .capabilities import PlanTier
from .claude_code import PLACEMENT_UNREAD, cli_admission
from .clients import is_own_key_turn, resolve_model_client, subscription_route
from .subagents import card_model

_LIST = "Pick another from `la models list`, or leave model out."


def _unknown(name: str) -> TaskModelRefused:
    return TaskModelRefused(
        f'Error: there is no model named "{name}". Run `la models list` for the names '
        "you can pass, or leave model out to use the subagent's own.",
        "there is no model of that name",
    )


def _off_plan(name: str) -> TaskModelRefused:
    return TaskModelRefused(
        f"Error: {name} is not on the user's plan. {_LIST}", "it is not on the user's plan",
    )


def _unchecked(name: str) -> TaskModelRefused:
    return TaskModelRefused(
        f"Error: {name} could not be checked just now. Try again, or leave model out.",
        "it could not be checked just now",
    )


def _needs_key(name: str, provider: str) -> TaskModelRefused:
    return TaskModelRefused(
        f"Error: {name} needs the user's own API key for {provider}, which they have not "
        f"added. {_LIST}",
        f"it needs the user's own API key for {provider}",
    )


def _sign_in(name: str, account: str) -> TaskModelRefused:
    return TaskModelRefused(
        f"Error: {name} runs on the user's {account} sign-in, which needs signing in "
        "again. Ask the user to sign in again in Settings, or leave model out.",
        f"the user's {account} sign-in needs signing in again",
    )


def _no_claude_code(name: str, why: str) -> TaskModelRefused:
    return TaskModelRefused(
        f"Error: {name} cannot run here: {why} Leave model out, or pick a model that is "
        "not Claude Code.",
        why.rstrip("."),
    )


def _cannot_run(name: str, why: str) -> TaskModelRefused:
    return TaskModelRefused(f"Error: {name} cannot run now: {why} {_LIST}", why.rstrip("."))


#: OAuth refusals that signing in again answers; any other is quoted.
_SIGN_IN_AGAIN = frozenset({"oauth_reauth_required", "oauth_required"})


class TaskModelGate:
    """The models this turn's Tasks name, each resolved once per subagent
    type and named effort, since a subagent's own set effort applies whatever
    model serves it unless the Task names another.

    ``model_pref`` is the plan-gated view the turn resolved its own model
    from, and ``plan_exempt`` the deployment's own models a primary turn runs
    whatever the plan. Whose credential pays, the compaction profile and
    where the turn placed Claude Code are read off ``config``, which
    ``resolve_llm_config`` has finished with by the time a Task names one.
    """

    def __init__(
        self,
        config: AgentConfig,
        *,
        user_id: str,
        workspace_id: str | None,
        thread_id: str | None,
        model_pref: dict,
        permits: PlanTier,
        is_byok: bool,
        turn_model: str,
        plan_exempt: frozenset[str],
    ) -> None:
        self._config = config
        self._user_id = user_id
        self._workspace_id = workspace_id
        self._thread_id = thread_id
        self._model_pref = model_pref
        self._permits = permits
        self._is_byok = is_byok
        self._turn_model = turn_model
        self._plan_exempt = plan_exempt
        self._byok_cache: dict[str, dict | None] = {}
        # Where the turn's own admission placed a Claude Code CLI; None where
        # it placed none, read on the first Claude Code name.
        self._claude_code: Admitted | str | None = (
            config.claude_code_admitted if config.claude_code else config.claude_code_pins
        )
        self._resolved: dict[tuple[str, str, str | None], TaskModel] = {}

    async def resolve(
        self, ask: TaskAsk, subagent_type: str
    ) -> TaskModel | TaskModelRefused | None:
        own = self._own_model(subagent_type)
        name = own if ask.model is None else ask.model.strip()
        if name == own and ask.effort is None:
            return None
        key = (subagent_type, name, ask.effort)
        if key in self._resolved:
            return self._resolved[key]
        picked = await self._resolve(name, subagent_type, ask.effort, own=name == own)
        if isinstance(picked, TaskModel):
            self._resolved[key] = picked
        return picked

    def _own_model(self, subagent_type: str) -> str:
        """The catalog model a task of ``subagent_type`` runs naming none,
        read off the client its graph was compiled on, as its card is."""
        client = self._config.subsidiary_llm_clients.get(f"subagent:{subagent_type}")
        if client is not None and (runs := catalog_model(client)):
            return runs
        definition = self._definition(subagent_type)
        return (definition.model if definition is not None else None) or self._turn_model

    def _definition(self, subagent_type: str) -> SubagentDefinition | None:
        return turn_roster(self._config.subagents).definitions.get(subagent_type)

    async def _resolve(
        self, name: str, subagent_type: str, effort: str | None, *, own: bool
    ) -> TaskModel | TaskModelRefused:
        """``own``: the subagent's own model, here only for a named effort.
        The turn already admitted it, a deployment's pin past the user's
        plan included (``plan_gated``), so the plan is not read again."""
        user_id, model_pref = self._user_id, self._model_pref
        source, entry = await user_models.classify_model(user_id, name, _pref_cache=model_pref)
        if (
            not name
            or not model_resolves(model_pref, name)
            or (
                source == user_models.ModelSource.UNKNOWN
                and await user_models.get_custom_provider_config(
                    user_id, name, _pref_cache=model_pref
                ) is None
            )
        ):
            return _unknown(name)

        claude_code: Admitted | None = None
        # A custom model of the same name shadows the row, and its client is
        # built from the custom entry (``resolve_model_client``).
        if source == user_models.ModelSource.SYSTEM and is_claude_code_model(name):
            where = await self._claude_code_where()
            if isinstance(where, str):
                return _no_claude_code(name, where)
            claude_code = where

        if not own and name not in self._plan_exempt:
            covered = await plan_covers(
                user_id, name, model_pref=model_pref, permits=self._permits,
                is_byok=self._is_byok, byok_cache=self._byok_cache,
            )
            if covered is None:
                return _unchecked(name)
            if not covered:
                return _off_plan(name)

        definition = self._definition(subagent_type)
        platform_ok = not is_own_key_turn(self._config)
        mc = LLMFactory.get_model_config()
        try:
            resolved = await resolve_model_client(
                user_id, name, is_byok=self._is_byok, cache_key=self._thread_id,
                allow_platform_fallback=platform_ok,
                reasoning_effort=(
                    effort
                    or (definition.reasoning_effort if definition is not None else None)
                    or resolve_tuning(model_pref, name).reasoning_effort
                ),
                _pref_cache=model_pref, _byok_cache=self._byok_cache,
            )
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            route = subscription_route(
                name, mc, custom_entry=entry if source == user_models.ModelSource.CUSTOM else None,
            )
            if detail.get("type") in _SIGN_IN_AGAIN and route is not None:
                return _sign_in(name, route.subscription.account_name)
            return _cannot_run(name, str(detail.get("message") or "its sign-in refused it."))
        except Exception:
            logger.warning(f"[CHAT] No client for a Task on {name} for user={user_id}", exc_info=True)
            return _cannot_run(name, "its client could not be built.")
        if resolved.client is None:
            provider = _provider_name(model_pref, entry, mc, fallback=name)
            # A catalog model the platform's key may pay for got no client
            # only because the server holds no key for its provider.
            if platform_ok and source == user_models.ModelSource.SYSTEM:
                return _cannot_run(name, f"this server has no API key for {provider}.")
            return _needs_key(name, provider)

        guidance = user_models.guidance_for(model_pref, name)
        stamp_call_metadata(
            resolved.client, prompt_guidance=guidance,
            compaction_profile=self._config.compaction.profile,
        )
        return TaskModel(
            name=name,
            display_name=(user_models.model_entry(model_pref, name) or {}).get("display_name") or name,
            client=resolved.client,
            guidance=guidance,
            model=card_model(model_pref, catalog_model(resolved.client) or name, self._turn_model),
            claude_code=claude_code,
            effort=effort,
        )

    async def _claude_code_where(self) -> Admitted | str:
        if self._claude_code is None or self._claude_code == PLACEMENT_UNREAD:
            self._claude_code = await cli_admission(
                self._config, user_id=self._user_id, workspace_id=self._workspace_id,
            )
        return self._claude_code


def _provider_name(model_pref: dict, entry: dict, mc, *, fallback: str) -> str:
    """The provider a key is asked for, as the user's settings name it."""
    provider = entry.get("provider") or fallback
    custom = user_models.custom_provider(model_pref, provider)
    if custom is not None:
        return str(custom.get("display_name") or provider)
    return mc.get_display_name(provider)
