"""What one user's subagent pane and file reads need to know about them.

Loaded once per request, never cached: the user's model settings and keys,
their plan check, the search pair their own turns run on, and the names a
preferred list may hold. Rows are not here, because a writer reads them under
its lock.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.subagents.merge import BaseSubagent, add_plugin_bases, base_subagents

if TYPE_CHECKING:
    from ptc_agent.config.agent import AgentConfig, SubagentsConfig
    from src.server.services.llm.capabilities import TierPermits

class SubagentsUnavailable(RuntimeError):
    """The server has no agent configuration yet (still starting)."""


@dataclass(frozen=True)
class Connector:
    """A server name a subagent's preferred list may hold."""

    name: str
    kind: str  # builtin | plugin | user | brokerage
    label: str
    enabled: bool


@dataclass
class SubagentContext:
    """One user's facts for one request; created per request, never shared."""

    user_id: str
    bases: dict[str, BaseSubagent]
    #: The deployment's offered names, then the subagents of plugins that are on.
    offered: tuple[str, ...]
    model_pref: dict
    is_byok: bool
    permits: TierPermits
    #: The search pair the user's own turns run on, gated as a turn gates it.
    search_provider: str
    search_depth: str
    skills: frozenset[str]
    connectors: tuple[Connector, ...]
    brokerages_connected: bool
    #: The OAuth providers the user has an account connected for, each with
    #: its plan, lowercase ("" when the provider didn't say).
    oauth_accounts: Mapping[str, str]
    _byok_cache: dict[str, Any] = field(default_factory=dict)

    @property
    def connector_names(self) -> frozenset[str]:
        return frozenset(c.name for c in self.connectors)

    def model_known(self, name: str) -> bool:
        from src.server.services.llm.availability import model_resolves

        return model_resolves(self.model_pref, name)

    async def model_on_plan(self, name: str) -> bool:
        """The turn's own plan check for a model the user chose.

        Shared with the per-turn merge so a model the pane accepts is one the
        turn runs, and a key or subscription for its provider lifts the plan.
        """
        from src.server.services.llm.availability import model_on_plan

        return await model_on_plan(
            self.user_id, name, model_pref=self.model_pref, permits=self.permits,
            is_byok=self.is_byok, byok_cache=self._byok_cache,
        )

    async def model_has_key(self, name: str) -> bool:
        """False for a model with no credential to run it on: a catalog model
        whose provider has neither the deployment's key nor one of the user's,
        a custom model or custom provider with no key, or an OAuth provider's
        with no account or on a plan the model isn't offered on.

        The turn finds this only once it builds the role's client, and then
        runs the subagent on the thread's model; the pane says so up front.
        """
        from src.llms.llm import LLM, platform_key_configured
        from src.server.services.llm import user_models
        from src.server.services.llm.clients import (
            _custom_provider_model,
            _resolve_custom_model_byok,
            walk_byok_candidates,
        )
        from src.server.services.oauth_providers import SUBSCRIPTIONS

        source, entry = await user_models.classify_model(
            self.user_id, name, _pref_cache=self.model_pref
        )
        if source == user_models.ModelSource.SYSTEM:
            provider = entry.get("provider")
            if LLM.get_model_config().get_provider_info(provider).get("access_type") != "oauth":
                if platform_key_configured(name):
                    return True
                if not self.is_byok:
                    return False
                found, _ = await walk_byok_candidates(
                    self.user_id, provider, LLM.get_model_config(), _byok_cache=self._byok_cache
                )
                return found is not None
            if provider not in self.oauth_accounts:
                return False
            # The turn's client gate: a known plan off the model's list can't
            # run it, an unknown one passes and the upstream decides.
            plan = self.oauth_accounts[provider]
            allowed = {p.lower() for p in entry.get("oauth_plans") or ()}
            return not (allowed and plan and plan not in allowed)
        if source == user_models.ModelSource.UNKNOWN:
            entry = await _custom_provider_model(self.user_id, name, _pref_cache=self.model_pref)
            if entry is None:
                return True
        if entry.get("provider") in SUBSCRIPTIONS:
            # A model the subscription lists and the manifest doesn't runs on
            # the sign-in, as the turn's OAuth client takes it, not on a key.
            return entry["provider"] in self.oauth_accounts
        if not self.is_byok:
            return False
        found, _, _ = await _resolve_custom_model_byok(
            self.user_id, name, entry, LLM.get_model_config(),
            _pref_cache=self.model_pref, _byok_cache=self._byok_cache,
        )
        return bool(found)

    def effort_ladder(self, name: str) -> tuple[str, ...]:
        """The levels ``name`` offers, custom entry over manifest; () for none."""
        from src.llms.model_spec import canonical_reasoning_efforts, reasoning_block
        from src.server.services.llm.user_models import model_entry

        entry = model_entry(self.model_pref, name) or {}
        return canonical_reasoning_efforts(reasoning_block(entry).get("efforts"))

    async def model_options(self) -> list[str]:
        """The models this user can pick, for a refusal to list."""
        from src.config.settings import HOST_MODE
        from src.llms.llm import get_configured_llm_models
        from src.llms.preferences import custom_model_names

        names = [n for group in get_configured_llm_models().values() for n in group]
        names += sorted(custom_model_names(self.model_pref))
        names = list(dict.fromkeys(names))
        if HOST_MODE != "platform":
            return names
        return [n for n in names if await self.model_on_plan(n)]

    def main_values(self) -> dict[str, Any]:
        """What the agent itself runs on: the default model for new threads, the
        effort a turn on it gets with nothing chosen in the composer, and the
        user's search pair. Read-only to both writers (``main.md``)."""
        return _main_values(self.model_pref, self.search_provider, self.search_depth)

    async def gate_search(self, provider: str | None, depth: str | None):
        """A subagent's saved search pair as its turn would run it."""
        from src.server.services.llm.capabilities import gate_search

        return await gate_search(
            provider, depth,
            effective_provider=self.search_provider,
            effective_depth=self.search_depth,
            permits=self.permits,
        )


def _main_values(model_pref: dict, search_provider: str, search_depth: str) -> dict[str, Any]:
    from src.llms.model_spec import (
        canonical_reasoning_efforts,
        clamp_reasoning_effort,
        default_reasoning_effort,
        reasoning_block,
    )
    from src.llms.preferences import resolve_tuning
    from src.server.services.llm.thread_model import effective_default_model
    from src.server.services.llm.user_models import model_entry

    model = effective_default_model(model_pref)
    effort = None
    if model:
        block = reasoning_block(model_entry(model_pref, model) or {})
        ladder = canonical_reasoning_efforts(block.get("efforts"))
        default = default_reasoning_effort(ladder, block.get("default"))
        chosen = resolve_tuning(model_pref, model).reasoning_effort
        effort = clamp_reasoning_effort(ladder, default, chosen) if chosen else default
    return {
        "model": model,
        "effort": effort,
        "search_provider": search_provider,
        "search_depth": search_depth,
    }


async def load_main_values(user_id: str) -> dict[str, Any]:
    """``SubagentContext.main_values`` without the facts only the subagents' checks need."""
    model_pref, _, search_provider, search_depth = await _gated(_agent_config(), user_id)
    return _main_values(model_pref, search_provider, search_depth)


async def _gated(agent_config: AgentConfig, user_id: str) -> tuple[dict, TierPermits, str, str]:
    """The user's model settings, then the turn's own gate over them, so the
    plan check and the search pair are the ones a turn gets.

    The gate runs on a copy that keeps the shared config untouched. Crawl is
    pinned off: its gate needs a tier fetch these reads have no use for.
    """
    from src.server.services.llm import user_models
    from src.server.services.llm.capabilities import gate_capabilities

    model_pref = await user_models.get_model_preference(user_id)
    probe = agent_config.model_copy(
        update={"features": {**(agent_config.features or {}), "site_crawl": False}}
    )
    permits = await gate_capabilities(probe, user_id, model_pref)
    return model_pref, permits, probe.search_api, probe.search_depth


async def roster(
    user_id: str, subagents: SubagentsConfig | None = None, *, conn=None
) -> tuple[dict[str, BaseSubagent], tuple[str, ...]]:
    """The user's bases and the names offered to them: all a view of values needs.

    The deployment's, then the ones the user's plugins ship. ``conn`` reads
    the plugins' under a writer's lock, which uninstall takes too, or in a
    turn's snapshot.
    """
    from .plugin_bases import plugin_bases

    subagents = subagents or _agent_config().subagents
    return add_plugin_bases(
        base_subagents(subagents), subagents.enabled, await plugin_bases(user_id, conn=conn)
    )


async def list_connectors(user_id: str) -> tuple[Connector, ...]:
    """The names a preferred list may hold, without the rest of the context."""
    connectors, _ = await _connectors(user_id, _agent_config())
    return connectors


def _agent_config() -> AgentConfig:
    from src.server.app import setup

    if setup.agent_config is None:
        raise SubagentsUnavailable("agent configuration not loaded")
    return setup.agent_config


def _label(name: str) -> str:
    words = name.replace("_", " ").replace("-", " ").strip()
    return words[:1].upper() + words[1:] if words else name


async def _skill_names(user_id: str) -> frozenset[str]:
    """Platform skills a subagent of the user's loads and the user's own
    user-wide ones, on or off.

    A platform skill behind a feature the user hasn't turned on is left out,
    as is one no subagent gets, as the turn's subagent registry leaves both
    out. Workspace skills are left out too: a subagent's settings are
    user-wide, so a name only one workspace has would be missing everywhere
    else.
    """
    from ptc_agent.agent.middleware.skills import SUBAGENT_WITHHELD_SKILLS, get_skill_registry
    from src.config.features import default_feature_enabled
    from src.server.database.user_skills import list_user_skills
    from src.server.services.features import effective_flags_for_user

    flags, rows = await asyncio.gather(effective_flags_for_user(user_id), list_user_skills(user_id))

    def feature_enabled(key: str) -> bool:
        return flags[key] if key in flags else default_feature_enabled(key)

    platform = {
        name
        for name, skill in get_skill_registry(feature_resolver=feature_enabled).items()
        if not skill.hidden
    }
    own = {row["name"] for row in rows}
    return frozenset(platform | own) - SUBAGENT_WITHHELD_SKILLS


async def _connectors(
    user_id: str, agent_config: AgentConfig
) -> tuple[tuple[Connector, ...], bool]:
    """Every server the user has, and whether a brokerage is connected.

    A brokerage is told apart by its address, never its name, which is the
    user's to edit; the same join the relay's policy makes.
    """
    from src.server.database.mcp_oauth import ConnectionStatus, list_connections
    from src.server.database.mcp_servers import list_catalog_servers
    from src.server.services.brokerage_capabilities import vendor_for_url
    from src.server.services.brokerages import brokerage_for_url
    from src.server.services.mcp_config import account_disabled_builtins

    disabled, rows, connections = await asyncio.gather(
        account_disabled_builtins(user_id),
        list_catalog_servers(user_id),
        list_connections(user_id),
    )
    live = {
        c["server_name"]
        for c in connections
        if c.get("status") != ConnectionStatus.REVOKED
    }
    out: list[Connector] = [
        Connector(s.name, "builtin", _label(s.name), s.name not in disabled)
        for s in agent_config.mcp.servers
        if getattr(s, "enabled", True)
    ]
    connected = False
    for row in rows:
        enabled = bool(row.get("enabled")) and row.get("plugin_enabled") is not False
        if vendor_for_url(row.get("url")) is not None:
            brokerage = brokerage_for_url(row.get("url"))
            out.append(Connector(
                row["name"], "brokerage",
                brokerage.label if brokerage else row["name"], enabled,
            ))
            if enabled and row["name"] in live:
                connected = True
        elif row.get("plugin_name"):
            out.append(Connector(row["name"], "plugin", row["name"], enabled))
        else:
            out.append(Connector(row["name"], "user", row["name"], enabled))
    return tuple(out), connected


async def _oauth_accounts(user_id: str) -> dict[str, str]:
    """The OAuth providers with a working account connected, as the model
    list reads them, and each one's plan. One that needs signing in again
    cannot serve a turn, so its models read as unavailable."""
    from src.llms.llm import LLM
    from src.server.database.oauth_tokens import get_oauth_connection

    providers = [
        name for name, info in LLM.get_model_config().flat_providers.items()
        if info.get("access_type") == "oauth"
    ]
    rows = await asyncio.gather(*(get_oauth_connection(user_id, p) for p in providers))
    return {
        p: (row.get("plan_type") or "").lower()
        for p, row in zip(providers, rows)
        if row and not row.get("needs_reauth_at")
    }


async def load_context(user_id: str) -> SubagentContext:
    """Everything but the rows, for one request."""
    from src.server.database.api_keys import is_byok_active

    agent_config = _agent_config()
    # Each read opens its own connection; only the gate waits, for the model settings.
    (
        gated, is_byok, oauth_accounts, skills, (connectors, connected), (bases, offered)
    ) = await asyncio.gather(
        _gated(agent_config, user_id),
        is_byok_active(user_id),
        _oauth_accounts(user_id),
        _skill_names(user_id),
        _connectors(user_id, agent_config),
        roster(user_id, agent_config.subagents),
    )
    model_pref, permits, search_provider, search_depth = gated
    return SubagentContext(
        user_id=user_id,
        bases=bases,
        offered=offered,
        model_pref=model_pref,
        is_byok=is_byok,
        permits=permits,
        search_provider=search_provider,
        search_depth=search_depth,
        skills=skills,
        connectors=connectors,
        brokerages_connected=connected,
        oauth_accounts=oauth_accounts,
    )
