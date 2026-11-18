"""Every refusal a policy rule makes, as a client or the agent reads it.

One case per rule, setter and call-site wording: the HTTP status and ``detail``
of each refusal, the tool-result text the agent reads, and what ``/users/me``
reports. The expected values are locked in ``policy_messages_lock.json``; a
change to any of them is a change to what users and the agent read, so it is
made there on purpose, never as a side effect. Regenerate with
``LOCK_POLICY_MESSAGES=1``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi import HTTPException

from src.config.models import PolicyLayer
from src.server.services import policy as P
from src.server.services import account as A
from tests.unit.server.policy_standing import standing_under

LOCK = Path(__file__).with_name("policy_messages_lock.json")

#: Each rule's denying layer.
DENY = {
    "models": {"models": {"allowed": ["a", "b"]}},
    "api_keys": {"own_model_access": {"api_keys": False}},
    "subscriptions": {"own_model_access": {"subscriptions": False}},
    "public_links": {"public_links": False},
    "trading": {"trading": False},
    "own_machines": {"own_machines": False},
    "user_connectors": {"user_connectors": False},
    "org_library": {"org_library": {"publish": False}},
    "shared_workspaces": {"shared_workspaces": {"create": False}},
}
SETTERS = ("deployment", "organization", "both")
CONNECT = {
    P.OWN_API_KEYS: ("api_keys", ("your own API keys", "a custom model or provider")),
    P.OWN_SUBSCRIPTIONS: ("subscriptions", ("a Claude Code subscription", "a model sign-in")),
}
TRADING_GRANTS = ("Raising the trading permission", "Granting live orders")
USER_CONNECTOR_ACTS = (
    "Turning on a connector", "Adding a connector", "Importing connectors",
    "Connecting a connector",
)
OWN_SERVER_URL = "https://mcp.example.com/mcp"


def _layer(raw: dict) -> PolicyLayer:
    return PolicyLayer.model_validate(raw)


def _policy(key: str, setter: str) -> P.Policy:
    deny = _layer(DENY[key])
    return {
        "deployment": lambda: P.merge(deny, None),
        "organization": lambda: P.merge(None, deny),
        "both": lambda: P.merge(deny, deny),
    }[setter]()


def _refused(exc: HTTPException) -> list:
    assert P.is_policy_refusal(exc)
    return [exc.status_code, exc.detail, exc.headers]


def _caught(fn) -> list | None:
    try:
        fn()
    except HTTPException as e:
        return _refused(e)
    return None


async def _acaught(fn, policy: P.Policy | None) -> list | None:
    with standing_under(policy):
        try:
            await fn()
        except HTTPException as e:
            return _refused(e)
    return None


# --- the calls, in the shape the code makes them ---------------------------------


def _model_denied(policy, rule, model):
    return _caught(lambda: P.raise_model_denied(policy, rule, model))


def _no_model_allowed(policy, model):
    return _caught(lambda: P.raise_no_model_allowed(policy, model))


async def _connect(policy, rule, what):
    return await _acaught(
        lambda: _require(rule, f"Connecting {what}"), policy
    )


async def _require(rule, act=None):
    (await A.require_policy("u")).require(rule, act)


async def _public_link(policy):
    return await _acaught(lambda: _require(P.PUBLIC_LINKS), policy)


def _shared_workspace_create(policy):
    return _caught(lambda: policy.require(P.SHARED_WORKSPACES_CREATE))


def _library_publish(policy):
    return _caught(lambda: policy.require(P.ORG_LIBRARY_PUBLISH))


async def _trading_grant(policy, what):
    return await _acaught(lambda: _require(P.TRADING, what), policy)


async def _machine_use(policy, owner="u"):
    return await _acaught(lambda: A.refuse_machine_use("ssh", owner), policy)


async def _user_connector(policy, what):
    return await _acaught(
        lambda: A.refuse_user_connector("u", what, url=OWN_SERVER_URL), policy
    )


async def _unknown_work():
    return await _acaught(lambda: A.require_policy("u"), None)


def _live_orders_text(policy):
    return P.withheld_text(policy, P.TRADING)


def _user_connectors_text(policy):
    return P.withheld_text(policy, P.USER_CONNECTORS)


def _own_connectors_withheld(policy):
    from src.server.services.plugins.mcp import withheld_reason

    return withheld_reason(policy)


# --- the table ---------------------------------------------------------------------


async def _observed() -> dict[str, object]:
    out: dict[str, object] = {}

    def put(value, *key):
        out["|".join(key)] = value

    for s in SETTERS:
        models = _policy("models", s)
        put(_model_denied(models, P.MODELS_ALLOWED, "x"), "model_denied", P.MODELS_ALLOWED, s)
        put(_no_model_allowed(models, "d"), "no_model_allowed", s)
        put(
            [models.set_by(P.MODELS_ALLOWED, "x"), models.set_by(P.MODELS_ALLOWED)],
            "set_by_model", s,
        )
    for rule, (key, whats) in CONNECT.items():
        for s in SETTERS:
            put(_model_denied(_policy(key, s), rule, "m"), "model_denied", rule, s)
            for what in whats:
                put(await _connect(_policy(key, s), rule, what), "connect", rule, what, s)
        put(await _connect(P.ALLOW_ALL, rule, whats[0]), "connect", rule, "allowed")
    for s in SETTERS:
        put(await _public_link(_policy("public_links", s)), "public_link", s)
        put(_shared_workspace_create(_policy("shared_workspaces", s)), "shared_workspace_create", s)
        put(_library_publish(_policy("org_library", s)), "library_publish", s)
        put(await _machine_use(_policy("own_machines", s)), "machine_use", s)
        put(_live_orders_text(_policy("trading", s)), "live_orders_text", s)
        put(_user_connectors_text(_policy("user_connectors", s)), "user_connectors_text", s)
        put(_own_connectors_withheld(_policy("user_connectors", s)), "own_connectors_withheld", s)
    put(await _public_link(P.ALLOW_ALL), "public_link", "allowed")
    put(_shared_workspace_create(P.ALLOW_ALL), "shared_workspace_create", "allowed")
    put(_library_publish(P.ALLOW_ALL), "library_publish", "allowed")
    for what in TRADING_GRANTS:
        for s in SETTERS:
            put(await _trading_grant(_policy("trading", s), what), "trading_grant", what, s)
        put(await _trading_grant(P.ALLOW_ALL, what), "trading_grant", what, "allowed")
        put(await _trading_grant(None, what), "trading_grant", what, "unknown")
    put(await _machine_use(P.ALLOW_ALL), "machine_use", "allowed")
    put(await _machine_use(None), "machine_use", "unknown")
    put(await _machine_use(P.ALLOW_ALL, owner=None), "machine_use", "no_owner")
    for what in USER_CONNECTOR_ACTS:
        for s in SETTERS:
            put(await _user_connector(_policy("user_connectors", s), what), "user_connector", what, s)
        put(await _user_connector(P.ALLOW_ALL, what), "user_connector", what, "allowed")
        put(await _user_connector(None, what), "user_connector", what, "unknown")
    put(await _unknown_work(), "unknown_work")
    for name, fn in (
        ("live_orders_text", _live_orders_text),
        ("user_connectors_text", _user_connectors_text),
        ("own_connectors_withheld", _own_connectors_withheld),
    ):
        put(fn(None), name, "unknown")
        put(fn(P.ALLOW_ALL), name, "allowed")
    for key in DENY:
        for s in SETTERS:
            put(_policy(key, s).for_client(), "for_client", key, s)
    put(P.ALLOW_ALL.for_client(), "for_client", "allow_all")
    mixed = P.merge(
        _layer({"trading": False, "models": {"allowed": ["a", "b", "c"]}}),
        _layer({
            "user_connectors": False,
            "models": {"allowed": ["c", "b"]},
            "org_library": {"publish": False},
        }),
    )
    put(mixed.for_client(), "for_client", "mixed")
    return json.loads(json.dumps(out))


@pytest.mark.asyncio
async def test_every_refusal_reads_as_locked():
    observed = await _observed()
    if os.getenv("LOCK_POLICY_MESSAGES"):
        LOCK.write_text(json.dumps(observed, indent=1, sort_keys=True) + "\n")
    locked = json.loads(LOCK.read_text())
    assert observed.keys() == locked.keys()
    for key, value in locked.items():
        assert observed[key] == value, key
