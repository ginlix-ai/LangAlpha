"""The effective policy: strict layers, the narrowing merge, unknown vs none."""

from contextlib import ExitStack
from types import MappingProxyType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.config.models import InfrastructureConfig, PolicyLayer
from src.server.services.account_contract import NO_ORG, Account, Org
from src.server.services.account import Standing
from src.server.services.policy import (
    ALLOW_ALL,
    MODELS_ALLOWED,
    OWN_API_KEYS,
    OWN_SUBSCRIPTIONS,
    merge,
    raise_model_denied,
)
from src.server.services.account import policy_for, require_policy

POLICY = "src.server.services.policy"
ACCOUNT = "src.server.services.account"


def _layer(**raw) -> PolicyLayer:
    return PolicyLayer.model_validate(raw)


def _org(policy: dict) -> Account:
    return Account(
        org=Org(id="o", name="Acme"),
        features=MappingProxyType({"teams": True}),
        policy=PolicyLayer.model_validate(policy),
    )


def _with(*, deployment=None, account=NO_ORG):
    stack = ExitStack()
    stack.enter_context(patch("src.server.services.account.deployment_policy", return_value=deployment))
    stack.enter_context(
        patch(
            f"{ACCOUNT}.read_standing",
            new_callable=AsyncMock,
            return_value=Standing(account, account),
        )
    )
    return stack


@pytest.mark.parametrize(
    "raw",
    [
        {"models": {"alowed": ["a"]}},
        {"models": {"allowed": "a"}},
        {"models": {"allowed": [1]}},
        {"models": None},
        {"own_model_access": {"api_keys": "false"}},
        {"own_model_access": {"api_keys": None}},
        {"own_model_access": {"api_keys": 0}},
        {"public_links": {"enabled": False}},
    ],
)
def test_a_deployment_policy_this_version_cannot_enforce_fails_config_load(raw):
    with pytest.raises(ValidationError):
        InfrastructureConfig(policy=raw)


def test_no_policy_section_is_no_deployment_layer():
    assert InfrastructureConfig().policy is None
    assert InfrastructureConfig(policy={"models": {"allowed": None}}).policy.models.allowed is None


def test_the_merge_takes_the_most_restrictive_of_each_rule():
    deployment = _layer(models={"allowed": ["a", "b", "c"]}, own_model_access={"api_keys": False})
    organization = _layer(models={"allowed": ["b", "c", "d"]}, own_model_access={"subscriptions": False})
    policy = merge(deployment, organization)
    assert policy.models_allowed == {"b", "c"}
    assert not policy.allows(OWN_API_KEYS) and not policy.allows(OWN_SUBSCRIPTIONS)
    assert policy.set_by(OWN_API_KEYS) == "deployment"
    assert policy.set_by(OWN_SUBSCRIPTIONS) == "organization"
    assert policy.set_by(MODELS_ALLOWED, "a") == "organization"
    assert policy.set_by(MODELS_ALLOWED, "d") == "deployment"


def test_one_layer_setting_a_list_is_the_list():
    assert merge(None, _layer(models={"allowed": ["x"]})).models_allowed == {"x"}
    assert merge(_layer(), None).models_allowed is None
    assert merge(None, None) is ALLOW_ALL
    assert ALLOW_ALL.unrestricted


@pytest.mark.asyncio
async def test_nothing_configured_reads_and_asks_nothing():
    """No deployment policy and the teams flag off: no cache read, no request."""
    cache, client = MagicMock(), AsyncMock()
    with (
        patch(f"{ACCOUNT}.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.server.dependencies.usage_limits.HOST_MODE", "platform"),
        patch("src.server.services.platform_client.HOST_MODE", "platform"),
        patch("src.server.dependencies.usage_limits.AUTH_SERVICE_URL", "http://auth"),
        patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://auth"),
        patch("src.server.services.platform_client.get_http_client", return_value=client),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("src.server.services.account.deployment_policy", return_value=None),
    ):
        assert await policy_for("u") is ALLOW_ALL
    assert not cache.method_calls
    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_a_standalone_deployment_applies_its_own_layer():
    deployment = _layer(own_model_access={"api_keys": False})
    with _with(deployment=deployment):
        policy = await policy_for("u")
    assert not policy.allows(OWN_API_KEYS)


@pytest.mark.asyncio
async def test_an_organization_can_only_narrow_the_deployment():
    deployment = _layer(models={"allowed": ["a"]})
    with _with(deployment=deployment, account=_org({"models": {"allowed": ["a", "b"]}})):
        policy = await policy_for("u")
    assert policy.models_allowed == {"a"}


@pytest.mark.asyncio
async def test_an_unknown_account_refuses_as_unknown():
    with _with(account=None):
        assert await policy_for("u") is None
        with pytest.raises(HTTPException) as refused:
            await require_policy("u")
    assert refused.value.status_code == 503
    assert refused.value.detail["type"] == "service_unavailable"


@pytest.mark.asyncio
async def test_an_unknown_account_is_an_unknown_policy():
    with _with(account=None):
        assert await policy_for("u") is None


def test_a_refusal_names_the_rule_and_who_set_it():
    policy = merge(None, _layer(models={"allowed": ["a"]}))
    with pytest.raises(HTTPException) as refused:
        raise_model_denied(policy, MODELS_ALLOWED, "b")
    detail = refused.value.detail
    assert refused.value.status_code == 400
    assert (detail["type"], detail["rule"], detail["set_by"]) == (
        "policy_denied", "models.allowed", "organization",
    )
    assert "your organization" in detail["message"] and "'b'" in detail["message"]
    assert detail["allowed"] == ["a"]


def test_the_client_shape():
    policy = merge(_layer(own_model_access={"subscriptions": False}), _layer(models={"allowed": ["b", "a"]}))
    assert policy.for_client() == {
        # As written, not sorted: the first the user can be served is the default.
        "models": {"allowed": ["b", "a"]},
        "own_model_access": {"api_keys": True, "subscriptions": False},
        "public_links": True,
        "trading": True,
        "own_machines": True,
        "user_connectors": True,
        "set_by": {"models.allowed": "organization", "own_model_access.subscriptions": "deployment"},
    }
    assert ALLOW_ALL.for_client()["set_by"] == {}


def test_the_client_and_a_refusal_list_models_in_the_order_the_default_is_picked():
    """Both lists set: the organization's order, narrowed by the deployment's."""
    policy = merge(
        _layer(models={"allowed": ["a", "b", "c", "d"]}),
        _layer(models={"allowed": ["d", "x", "b", "a"]}),
    )
    assert policy.for_client()["models"]["allowed"] == ["d", "b", "a"]
    assert list(policy.allowed_in_order) == ["d", "b", "a"]
    with pytest.raises(HTTPException) as refused:
        raise_model_denied(policy, MODELS_ALLOWED, "c")
    assert refused.value.detail["allowed"] == ["d", "b", "a"]
