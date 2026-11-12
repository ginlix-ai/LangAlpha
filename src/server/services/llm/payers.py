"""Who pays for each model a user can pick, for a client that lists them.

Each model's credential is the turn's own answer (``credential_route``, the
ladder ``resolve_model_client`` builds on), so nothing here decides which
credential serves a model; this module only names who that bills. A
subscription bills the user's plan with its maker, and serves a turn only
while signed in on a plan that includes the model; a key of the user's own
bills them, or their coding plan; the deployment bills credits on a hosted
account when the plan reaches the model (``model_on_plan``) or the model is
the deployment's own default, and spends its own key on a self-hosted one.
The web reads the same ladder off its own copy (``web/src/lib/modelPayer.ts``).
"""

from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import BaseModel

from ptc_agent.config.agent import CredentialPayer, Payer
from src.config import settings as app_settings
from src.llms.claude_code import is_claude_code_model
from src.llms.llm import LLM as LLMFactory
from src.server.database.api_keys import get_byok_configs_for_providers, is_byok_active
from src.server.database.oauth_tokens import get_oauth_connection
from src.server.dependencies.usage_limits import platform_gating_active
from src.server.services.claude_code.flow import rows_on
from src.server.services.claude_code.hosted import saved_token_runs
from src.server.services.claude_code.saved import usable_token
from src.server.services.claude_oauth import CLAUDE_PROVIDER
from src.server.services.oauth_providers import SUBSCRIPTIONS

from . import user_models
from .availability import model_on_plan
from .byok import byok_candidates
from .capabilities import PlanTier
from .clients import (
    ClaudeCodeRoute,
    NoRoute,
    OwnKeyRoute,
    Route,
    SubscriptionRoute,
    credential_route,
    plan_includes,
)
from .config import deployment_key, deployment_llm, plan_exempt_models


class ModelPayer(BaseModel):
    """One model the user can pick and who a turn on it bills.

    ``payer`` is ``subscription`` (``plan`` names it, ``signed_in`` says
    whether it can serve a turn now), ``own_key``, ``credits``,
    ``server_key``, ``local`` (a server on the user's machine, which nobody
    bills), or None when nothing serves it, ``needs`` then saying what would:
    ``plan`` or ``key``. Fields a row has no answer for are left unset, and
    the route leaves them out.
    """

    name: str
    provider: str
    custom: bool
    payer: Payer | None
    plan: str | None = None
    signed_in: bool | None = None
    needs: Literal["plan", "key"] | None = None


def _plan_name(provider: str, mc) -> str:
    """A subscription named as its maker sells it, a coding plan by its catalog name."""
    subscription = SUBSCRIPTIONS.get(provider)
    return subscription.account_name if subscription else mc.get_display_name(provider)


def source_payer(credential_source: object) -> CredentialPayer:
    """Who pays when only the credential's source is known.

    ``platform`` means opposite things in the two host modes: self-hosted, the
    key is the operator's own and the operator is the reader; hosted, the user
    holds no key and the call bills credits. An unknown source on a hosted
    deployment reads as credits, since telling a user to fix an account they
    do not hold is worse than a generic failure.
    """
    source = str(credential_source)
    if source == "oauth":
        return CredentialPayer("subscription")
    if source == "byok":
        return CredentialPayer("own_key")
    return CredentialPayer("server_key" if app_settings.HOST_MODE == "oss" else "credits")


def route_payer(route: Route, mc) -> CredentialPayer | None:
    """Who ``route`` bills when it is a credential of the user's, and the
    account it bills, for the picker's badge and a failed turn's card alike:
    a coding plan is a subscription bought with a key, and a local server is
    nobody's account. None for the deployment's route, or no route, which
    the caller words."""
    if isinstance(route, ClaudeCodeRoute):
        return CredentialPayer("subscription", _plan_name(CLAUDE_PROVIDER, mc))
    if isinstance(route, SubscriptionRoute):
        return CredentialPayer("subscription", route.subscription.account_name)
    if isinstance(route, NoRoute):
        return None
    if route.provider and mc.get_provider_info(route.provider).get("access_type") == "local":
        return CredentialPayer("local")
    if isinstance(route, OwnKeyRoute):
        # The key bills the account it was saved for, which is not the
        # model's own provider where a base URL let a sibling's key serve.
        holder = route.holding or route.provider
        info = mc.get_provider_info(holder)
        if info.get("access_type") == "coding_plan":
            return CredentialPayer("subscription", _plan_name(holder, mc))
        # A provider the user added is named as they named it.
        return CredentialPayer("own_key", mc.get_display_name(holder) if info else holder)
    return None


async def _sign_ins(user_id: str) -> dict[str, dict | None]:
    """Each subscription's stored connection (no tokens), read once."""
    providers = list(SUBSCRIPTIONS)
    rows = await asyncio.gather(*(get_oauth_connection(user_id, p) for p in providers))
    return dict(zip(providers, rows))


async def _claude_code(user_id: str) -> tuple[bool, bool | None]:
    """Whether the user's Claude Code rows are on, which the list leaves out
    otherwise as the app does, and whether the saved setup token a turn on the
    backend runs on is usable, as admission asks it. None where the CLI runs
    only on the user's own computers, each signed in with a token of its own."""
    if not await rows_on(user_id):
        return False, None
    if not await saved_token_runs(user_id):
        return True, None
    return True, await usable_token(user_id) is not None


async def model_payers(user_id: str) -> list[ModelPayer]:
    """One row per model the user can pick: the manifest's visible models,
    then the ones they added, a custom model taking a built-in's place."""
    mc = LLMFactory.get_model_config()
    model_pref, is_byok, sign_ins, (claude_code_on, claude_code_signed_in) = await asyncio.gather(
        user_models.get_model_preference(user_id),
        is_byok_active(user_id),
        _sign_ins(user_id),
        _claude_code(user_id),
    )
    gated = platform_gating_active()
    exempt = plan_exempt_models(deployment_llm(), "primary")
    permits = PlanTier(user_id)
    customs = [
        cm for cm in model_pref.get("custom_models") or []
        if isinstance(cm, dict) and isinstance(cm.get("name"), str)
    ]
    custom_names = {cm["name"] for cm in customs}
    builtins = {
        name: meta for name, meta in mc.get_model_metadata().items()
        if name not in custom_names
        and (claude_code_on or not is_claude_code_model(name))
    }

    # One read of the user's keys for every route below, as a turn prefetches:
    # each provider's family, and the user's own providers, whose keys a
    # custom model may sit on.
    byok_cache: dict[str, dict | None] = {}
    if is_byok:
        own_providers = model_pref.get("custom_providers")
        slugs = {
            slug
            for provider in {m["provider"] for m in builtins.values()}
            | {cm.get("provider") for cm in customs if cm.get("provider")}
            for slug in byok_candidates(provider, mc)
        } | {
            cp["name"] for cp in (own_providers if isinstance(own_providers, list) else [])
            if isinstance(cp, dict) and isinstance(cp.get("name"), str)
        }
        found = await get_byok_configs_for_providers(user_id, sorted(slugs))
        byok_cache = {slug: found.get(slug) for slug in slugs}

    async def bills(name: str, route: Route) -> dict:
        paid = route_payer(route, mc)
        if isinstance(route, SubscriptionRoute):
            connection = sign_ins.get(route.model_info["provider"])
            signed_in = connection is not None and not connection.get("needs_reauth_at")
            named = {"plan": paid.account, "signed_in": signed_in}
            # The turn refuses a model the connected plan does not include
            # (``oauth_plan_unsupported``) rather than moving to a key.
            if signed_in and not plan_includes(route.model_info, connection.get("plan_type")):
                return {"payer": None, "needs": "plan", **named}
            return {"payer": "subscription", **named}

        if isinstance(route, ClaudeCodeRoute):
            return {"payer": "subscription", "plan": paid.account, "signed_in": claude_code_signed_in}
        if paid is not None:
            # A key of the user's, a coding plan bought with one, or a server
            # on their machine.
            if paid.payer == "subscription":
                return {"payer": "subscription", "plan": paid.account, "signed_in": True}
            return {"payer": paid.payer}
        if isinstance(route, NoRoute):
            # Only the user's own credential runs a model they added.
            return {"payer": None, "needs": "key"}
        provider = route.provider
        if gated:
            if name in exempt or await model_on_plan(
                user_id, name, model_pref=model_pref, permits=permits,
                is_byok=is_byok, byok_cache=byok_cache,
            ):
                return {"payer": "credits"}
            # A model with no tier is one no plan includes: a key of the
            # user's own is the only way to it.
            tiered = isinstance(route.model_info.get("tier"), int)
            return {"payer": None, "needs": "plan" if tiered else "key"}
        if provider and deployment_key(provider):
            return {"payer": "server_key"}
        return {"payer": None, "needs": "key"}

    async def row(name: str, provider: str, custom: bool) -> ModelPayer:
        route = await credential_route(
            user_id, name, is_byok=is_byok, _pref_cache=model_pref, _byok_cache=byok_cache,
        )
        return ModelPayer(name=name, provider=provider, custom=custom, **await bills(name, route))

    return [
        *[await row(name, meta["provider"], False) for name, meta in builtins.items()],
        *[await row(cm["name"], cm.get("provider") or "", True) for cm in customs],
    ]
