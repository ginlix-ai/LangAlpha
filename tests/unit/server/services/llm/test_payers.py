"""Who pays for each model, by the credential a turn would run it on
(``credential_route``): the sign-in, the user's own key, then the deployment
(credits on a hosted account, when the plan reaches the model's tier or the
model is the deployment's default). Only the reads are stubbed; the route is
the turn's own."""

from types import SimpleNamespace
from typing import get_args
from unittest.mock import AsyncMock, patch

import pytest

PAYERS = "src.server.services.llm.payers"
LIMITS = "src.server.dependencies.usage_limits"

_PROVIDERS = {
    "deepseek": {"access_type": "api_key"},
    "openai": {"access_type": "api_key"},
    "anthropic": {"access_type": "api_key"},
    "claude-oauth": {"access_type": "oauth"},
    "z-ai-coding": {"access_type": "coding_plan"},
    "glm": {"access_type": "api_key"},
    "glm-coding": {"access_type": "coding_plan"},
    "ollama": {"access_type": "local"},
    "claude-code": {"access_type": "oauth", "sdk": "claude-code"},
}
_MODELS = {
    "house": {"provider": "deepseek"},
    "tiered": {"provider": "openai", "tier": 1},
    "premium": {"provider": "openai", "tier": 3},
    "untiered": {"provider": "openai"},
    "keyed": {"provider": "anthropic", "tier": 1},
    "signed": {"provider": "claude-oauth"},
    "max-only": {"provider": "claude-oauth", "oauth_plans": ["max"]},
    "coded": {"provider": "z-ai-coding"},
    "borrowed": {"provider": "glm-coding"},
    "nearby": {"provider": "ollama"},
    "coder": {"provider": "claude-code"},
}
#: The user's stored keys.
_KEYS = {"anthropic", "z-ai-coding", "glm"}


class _Manifest:
    def get_model_metadata(self):
        return _MODELS

    def get_model_config(self, name):
        return _MODELS.get(name)

    def get_provider_info(self, provider):
        return _PROVIDERS.get(provider, {})

    def get_display_name(self, provider):
        return {"z-ai-coding": "GLM Coding Plan"}.get(provider, provider)

    def get_parent_provider(self, provider):
        return "glm" if provider == "glm-coding" else None

    def get_child_variants(self, provider):
        return []

    def shares_api_key(self, provider, holder):
        return True

    def connection_of(self, provider):
        return provider

    def models_from(self, provider):
        return None


async def _keys(user_id, slugs):
    # The glm key carries a base URL, which lets it serve the coding plan's models.
    return {
        slug: {"api_key": "k", "base_url": "https://glm.example" if slug == "glm" else None}
        for slug in slugs
        if slug in _KEYS
    }


async def _payers(
    pref=None, *, byok=True, gated=True, connection=None, server_key=None, claude_code=(False, None)
):
    """``connection`` is the stored Claude sign-in; ``gated`` a hosted
    deployment on a plan reaching tier 1; ``claude_code`` whether the user's
    Claude Code rows are on, and whether the backend's saved token is usable
    (None where only their own computers run the CLI)."""
    from src.server.services.llm.payers import model_payers

    async def sign_in(user_id, provider):
        return connection if provider == "claude-oauth" else None

    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=_Manifest()),
        patch(f"{PAYERS}.user_models.get_model_preference", AsyncMock(return_value=pref or {})),
        patch(f"{PAYERS}.is_byok_active", AsyncMock(return_value=byok)),
        patch(f"{PAYERS}.get_byok_configs_for_providers", _keys),
        patch(f"{PAYERS}.get_oauth_connection", sign_in),
        patch(f"{LIMITS}.HOST_MODE", "platform" if gated else "oss"),
        patch(f"{LIMITS}.AUTH_SERVICE_URL", "https://auth.example"),
        patch(f"{LIMITS}._fetch_platform_tier", AsyncMock(return_value=1)),
        patch(f"{PAYERS}.deployment_llm", return_value=SimpleNamespace(name="house", background=None)),
        patch(f"{PAYERS}.deployment_key", side_effect=lambda p: server_key if p == "openai" else None),
        patch(f"{PAYERS}._claude_code", AsyncMock(return_value=claude_code)),
    ):
        return {row.name: row.model_dump(exclude_unset=True) for row in await model_payers("u1")}


@pytest.mark.asyncio
async def test_each_model_names_who_pays():
    rows = await _payers(connection={"needs_reauth_at": None, "plan_type": "max"})
    assert rows["house"]["payer"] == "credits"
    assert rows["tiered"]["payer"] == "credits"
    assert (rows["premium"]["payer"], rows["premium"]["needs"]) == (None, "plan")
    assert (rows["untiered"]["payer"], rows["untiered"]["needs"]) == (None, "key")
    assert rows["keyed"]["payer"] == "own_key"
    assert rows["signed"] == {
        "name": "signed", "provider": "claude-oauth", "custom": False,
        "payer": "subscription", "plan": "Claude", "signed_in": True,
    }
    assert rows["max-only"]["payer"] == "subscription"
    assert {"payer": "subscription", "plan": "GLM Coding Plan"}.items() <= rows["coded"].items()
    assert rows["nearby"]["payer"] == "local"


@pytest.mark.asyncio
async def test_a_key_borrowed_from_a_sibling_bills_as_the_key_it_is():
    # A pay-as-you-go key serving a coding plan's model is not that plan.
    rows = await _payers()
    assert rows["borrowed"]["payer"] == "own_key"


@pytest.mark.asyncio
async def test_a_sign_in_on_a_plan_without_the_model_does_not_pay_for_it():
    # The turn refuses it (``oauth_plan_unsupported``) rather than run it.
    rows = await _payers(connection={"needs_reauth_at": None, "plan_type": "Pro"})
    assert {"payer": None, "needs": "plan", "plan": "Claude"}.items() <= rows["max-only"].items()
    assert rows["signed"]["payer"] == "subscription"


@pytest.mark.asyncio
async def test_a_subscription_not_connected_still_names_it():
    rows = await _payers(connection=None)
    assert rows["signed"]["payer"] == "subscription"
    assert rows["signed"]["signed_in"] is False
    # No plan to judge it by until the user signs in.
    assert rows["max-only"]["payer"] == "subscription"


@pytest.mark.asyncio
async def test_a_claude_code_row_runs_on_its_setup_token_and_is_listed_only_when_on():
    assert "coder" not in await _payers()
    rows = await _payers(claude_code=(True, False))
    assert {"payer": "subscription", "plan": "Claude", "signed_in": False}.items() <= rows["coder"].items()
    assert (await _payers(claude_code=(True, True)))["coder"]["signed_in"] is True
    # Each own computer holds its own token, so the list names none.
    assert (await _payers(claude_code=(True, None)))["coder"]["signed_in"] is None


@pytest.mark.asyncio
async def test_keys_switched_off_leave_the_plan_to_pay():
    rows = await _payers(byok=False)
    assert rows["keyed"]["payer"] == "credits"


@pytest.mark.asyncio
async def test_a_self_hosted_deployment_spends_its_own_key():
    rows = await _payers(gated=False, server_key="sk-example")
    assert rows["premium"]["payer"] == "server_key"
    assert (rows["house"]["payer"], rows["house"]["needs"]) == (None, "key")


@pytest.mark.asyncio
async def test_a_model_the_user_added_takes_the_built_ins_place_and_needs_their_key():
    rows = await _payers({"custom_models": [{"name": "tiered", "provider": "openai"}]})
    assert {"custom": True, "payer": None, "needs": "key"}.items() <= rows["tiered"].items()
    keyed = await _payers({"custom_models": [{"name": "tiered", "provider": "anthropic"}]})
    assert keyed["tiered"]["payer"] == "own_key"
    signed = await _payers(
        {"custom_models": [{"name": "untuned", "provider": "claude-oauth"}]},
        connection={"needs_reauth_at": None, "plan_type": "pro"},
    )
    assert {"custom": True, "payer": "subscription", "plan": "Claude"}.items() <= signed["untuned"].items()


@pytest.mark.asyncio
async def test_the_route_leaves_out_what_a_row_has_no_answer_for():
    """The ``la`` CLI reads ``payer`` on every row, null included, and the
    rest only where they apply."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.server.app import users
    from src.server.dependencies.principal import ReadUserId
    from src.server.services.llm.payers import ModelPayer

    rows = [
        ModelPayer(name="a", provider="openai", custom=False, payer=None, needs="plan"),
        ModelPayer(name="b", provider="claude-oauth", custom=False, payer="subscription",
                   plan="Claude", signed_in=False),
        ModelPayer(name="c", provider="openai", custom=True, payer="own_key"),
    ]
    app = FastAPI()
    app.include_router(users.router)
    app.dependency_overrides[get_args(ReadUserId)[1].dependency] = lambda: "u1"
    with patch.object(users, "model_payers", AsyncMock(return_value=rows)):
        body = TestClient(app).get("/api/v1/users/me/models").json()
    assert body == {"models": [
        {"name": "a", "provider": "openai", "custom": False, "payer": None, "needs": "plan"},
        {"name": "b", "provider": "claude-oauth", "custom": False, "payer": "subscription",
         "plan": "Claude", "signed_in": False},
        {"name": "c", "provider": "openai", "custom": True, "payer": "own_key"},
    ]}
