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

from src.llms.claude_code import backend_available, is_claude_code_model
from src.llms.llm import LLM as LLMFactory
from src.server.database.api_keys import get_byok_configs_for_providers, is_byok_active
from src.server.database.oauth_tokens import get_oauth_connection
from src.server.dependencies.usage_limits import platform_gating_active
from src.server.services.claude_code.flow import rows_on
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
    payer: Literal["subscription", "own_key", "credits", "server_key", "local"] | None
    plan: str | None = None
    signed_in: bool | None = None
    needs: Literal["plan", "key"] | None = None


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
    if not backend_available():
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

    def plan_name(provider: str) -> str:
        """A subscription named as its maker sells it, a coding plan by its catalog name."""
        subscription = SUBSCRIPTIONS.get(provider)
        return subscription.account_name if subscription else mc.get_display_name(provider)

    async def bills(name: str, route: Route) -> dict:
        if isinstance(route, SubscriptionRoute):
            provider = route.model_info["provider"]
            connection = sign_ins.get(provider)
            signed_in = connection is not None and not connection.get("needs_reauth_at")
            named = {"plan": plan_name(provider), "signed_in": signed_in}
            # The turn refuses a model the connected plan does not include
            # (``oauth_plan_unsupported``) rather than moving to a key.
            if signed_in and not plan_includes(route.model_info, connection.get("plan_type")):
                return {"payer": None, "needs": "plan", **named}
            return {"payer": "subscription", **named}

        if isinstance(route, ClaudeCodeRoute):
            return {"payer": "subscription", "plan": plan_name(CLAUDE_PROVIDER),
                    "signed_in": claude_code_signed_in}

        if isinstance(route, NoRoute):
            # Only the user's own credential runs a model they added.
            return {"payer": None, "needs": "key"}
        provider = route.provider
        access = mc.get_provider_info(provider).get("access_type") if provider else None
        if access == "local":
            return {"payer": "local"}
        if isinstance(route, OwnKeyRoute):
            # The key bills the account it was saved for, which is not the
            # model's own provider where a base URL let a sibling's key serve.
            holder = route.holding or provider
            # A coding plan is a subscription bought with a key.
            if mc.get_provider_info(holder).get("access_type") == "coding_plan":
                return {"payer": "subscription", "plan": plan_name(holder), "signed_in": True}
            return {"payer": "own_key"}
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
