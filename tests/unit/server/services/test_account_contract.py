"""The account contract: the version this build declares on the membership
fetch, and the cache keys that keep each version's entries apart.

With the ``teams`` door shut, the membership read sends and stores exactly
what it did before organizations existed."""

import ast
import dataclasses
import inspect
import types
from contextlib import ExitStack
from typing import Annotated, Literal, Union, get_args, get_origin
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from pydantic import BaseModel

from src.config.features import FEATURES, FeatureGate
from src.config.models import PolicyLayer
from src.server.dependencies import usage_limits
from src.server.services import membership
from src.server.services.membership import MEMBERSHIP_ACCOUNT_KEYS
from src.server.services import account
from src.server.services.account_contract import (
    _CONNECTOR_AUTHS, _CONNECTOR_KEYS, _LEVELS, _TEAM_KEYS, _TEAM_ROLES,
)
from src.server.services.account_contract import (
    ACCOUNT_CONTRACT,
    Group,
    Org,
    OrgConnector,
)

LIMITS = "src.server.dependencies.usage_limits"
ACCOUNT = "src.server.services.account"
EPOCH_TTL = 31 * 24 * 3600

# What each version of the contract reads. A version, once shipped, never
# changes: the auth service sends a build exactly this for the version it
# declares.
READABLE = {
    1: {
        "blocks": ["connectors", "features", "groups", "org", "policy"],
        # ``suspended`` is optional: absent reads as not suspended.
        "org": ["id", "name", "suspended"],
        "org_required": ["id", "name"],
        "group": ["id", "level"],
        "group_levels": ["manage", "read", "write"],
        # Each rule's type as it is validated, at every level: a new value or
        # a looser or stricter type is a change in what is read.
        "policy": {
            "models": "object",
            "models.allowed": "list[Annotated[str, Strict(strict=True)]] | None",
            "own_machines": "bool Strict(strict=True)",
            "own_model_access": "object",
            "own_model_access.api_keys": "bool Strict(strict=True)",
            "own_model_access.subscriptions": "bool Strict(strict=True)",
            "public_links": "bool Strict(strict=True)",
            "trading": "bool Strict(strict=True)",
            "user_connectors": "bool Strict(strict=True)",
        },
        "connector_fields": [
            "auth", "description", "id", "instruction", "name", "revision", "url",
        ],
        "connector_required": ["auth", "id", "name", "revision", "url"],
        "connector_auths": ["member", "shared"],
        # The features an organization can grant: ``teams`` and each one
        # that requires it, whose grant gate the organization's value meets.
        "features": ["org_connectors", "teams"],
    },
    # The organization library: who may publish to it, and the feature an
    # organization is granted it by.
    2: {
        "blocks": ["connectors", "features", "groups", "org", "policy"],
        "org": ["id", "name", "suspended"],
        "org_required": ["id", "name"],
        "group": ["id", "level"],
        "group_levels": ["manage", "read", "write"],
        "policy": {
            "models": "object",
            "models.allowed": "list[Annotated[str, Strict(strict=True)]] | None",
            "org_library": "object",
            "org_library.publish": "bool Strict(strict=True)",
            "own_machines": "bool Strict(strict=True)",
            "own_model_access": "object",
            "own_model_access.api_keys": "bool Strict(strict=True)",
            "own_model_access.subscriptions": "bool Strict(strict=True)",
            "public_links": "bool Strict(strict=True)",
            "trading": "bool Strict(strict=True)",
            "user_connectors": "bool Strict(strict=True)",
        },
        "connector_fields": [
            "auth", "description", "id", "instruction", "name", "revision", "url",
        ],
        "connector_required": ["auth", "id", "name", "revision", "url"],
        "connector_auths": ["member", "shared"],
        "features": ["org_connectors", "org_library", "teams"],
    },
    # Shared workspaces: who may create one, and the feature an organization
    # is granted them by.
    3: {
        "blocks": ["connectors", "features", "groups", "org", "policy"],
        "org": ["id", "name", "suspended"],
        "org_required": ["id", "name"],
        "group": ["id", "level"],
        "group_levels": ["manage", "read", "write"],
        "policy": {
            "models": "object",
            "models.allowed": "list[Annotated[str, Strict(strict=True)]] | None",
            "org_library": "object",
            "org_library.publish": "bool Strict(strict=True)",
            "own_machines": "bool Strict(strict=True)",
            "own_model_access": "object",
            "own_model_access.api_keys": "bool Strict(strict=True)",
            "own_model_access.subscriptions": "bool Strict(strict=True)",
            "public_links": "bool Strict(strict=True)",
            "shared_workspaces": "object",
            "shared_workspaces.create": "bool Strict(strict=True)",
            "trading": "bool Strict(strict=True)",
            "user_connectors": "bool Strict(strict=True)",
        },
        "connector_fields": [
            "auth", "description", "id", "instruction", "name", "revision", "url",
        ],
        "connector_required": ["auth", "id", "name", "revision", "url"],
        "connector_auths": ["member", "shared"],
        "features": ["org_connectors", "org_library", "shared_workspaces", "teams"],
    },
    # The account's teams in its organization, with its role in each.
    4: {
        "blocks": ["connectors", "features", "groups", "org", "policy", "teams"],
        "org": ["id", "name", "suspended"],
        "org_required": ["id", "name"],
        "group": ["id", "level"],
        "group_levels": ["manage", "read", "write"],
        "policy": {
            "models": "object",
            "models.allowed": "list[Annotated[str, Strict(strict=True)]] | None",
            "org_library": "object",
            "org_library.publish": "bool Strict(strict=True)",
            "own_machines": "bool Strict(strict=True)",
            "own_model_access": "object",
            "own_model_access.api_keys": "bool Strict(strict=True)",
            "own_model_access.subscriptions": "bool Strict(strict=True)",
            "public_links": "bool Strict(strict=True)",
            "shared_workspaces": "object",
            "shared_workspaces.create": "bool Strict(strict=True)",
            "trading": "bool Strict(strict=True)",
            "user_connectors": "bool Strict(strict=True)",
        },
        "connector_fields": [
            "auth", "description", "id", "instruction", "name", "revision", "url",
        ],
        "connector_required": ["auth", "id", "name", "revision", "url"],
        "connector_auths": ["member", "shared"],
        "features": ["org_connectors", "org_library", "shared_workspaces", "teams"],
        "team": ["id", "name", "role"],
        "team_roles": ["admin", "member"],
    },
}


def _type_text(kind) -> str:
    if kind is type(None):
        return "None"
    origin = get_origin(kind)
    if origin is Annotated:
        base, *meta = get_args(kind)
        return f"Annotated[{_type_text(base)}, {', '.join(map(repr, meta))}]"
    if origin is Literal:
        return f"Literal[{', '.join(map(repr, get_args(kind)))}]"
    if origin in (Union, types.UnionType):
        return " | ".join(_type_text(a) for a in get_args(kind))
    if origin is not None:
        return f"{origin.__name__}[{', '.join(_type_text(a) for a in get_args(kind))}]"
    return kind.__name__


def _policy_types(model: type[BaseModel], prefix: str = "") -> dict[str, str]:
    types_ = {}
    for name, field in sorted(model.model_fields.items()):
        kind = field.annotation
        if isinstance(kind, type) and issubclass(kind, BaseModel):
            types_[f"{prefix}{name}"] = "object"
            types_.update(_policy_types(kind, f"{prefix}{name}."))
        else:
            types_[f"{prefix}{name}"] = " ".join([_type_text(kind), *map(repr, field.metadata)])
    return types_


def _required(cls) -> list[str]:
    return sorted(
        f.name
        for f in dataclasses.fields(cls)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    )


def _readable_now() -> dict:
    return {
        "blocks": sorted(MEMBERSHIP_ACCOUNT_KEYS),
        "org": sorted(f.name for f in dataclasses.fields(Org)),
        "org_required": _required(Org),
        "group": sorted(f.name for f in dataclasses.fields(Group)),
        "group_levels": sorted(_LEVELS),
        "policy": _policy_types(PolicyLayer),
        "connector_fields": sorted(_CONNECTOR_KEYS),
        "connector_required": _required(OrgConnector),
        "connector_auths": sorted(_CONNECTOR_AUTHS),
        "team": sorted(_TEAM_KEYS),
        "team_roles": sorted(_TEAM_ROLES),
        "features": sorted(
            key
            for key, spec in FEATURES.items()
            if key == "teams" or "teams" in spec.requires
        ),
    }


def test_every_organization_feature_is_granted_by_the_organization():
    """A feature an organization can be granted sits on the grant gate, so
    the organization's ``true`` turns it on and its absence leaves it off."""
    for key in READABLE[ACCOUNT_CONTRACT]["features"]:
        assert FEATURES[key].gate is FeatureGate.GRANT, key


def test_what_is_read_moves_the_contract_version():
    assert ACCOUNT_CONTRACT in READABLE, (
        f"ACCOUNT_CONTRACT is {ACCOUNT_CONTRACT} but no shape is pinned for it: "
        "add READABLE[{ACCOUNT_CONTRACT}] with what this version reads"
    )
    assert _readable_now() == READABLE[ACCOUNT_CONTRACT], (
        "What langalpha reads from the account changed (a block, a policy key "
        "or its type, a group level, a connector field or auth mode) but "
        "ACCOUNT_CONTRACT did not: the version must "
        "move, in src/server/services/account.py, with the new shape pinned "
        "here under the new version, or an auth service would send a build "
        "keys it cannot read"
    )


# --- the fetch ----------------------------------------------------------------


class _Cache:
    """Redis as the membership read uses it, recording every call."""

    def __init__(self, store=None):
        self.store: dict = dict(store or {})
        self.calls: list = []

    async def mget(self, keys, strict=False):
        self.calls.append(("mget", list(keys)))
        return [self.store.get(k) for k in keys]

    async def set(self, key, value, ttl=None):
        self.calls.append(("set", key, ttl))
        self.store[key] = value
        return True

    async def set_many(self, items):
        self.calls.append(("set_many", [(k, t) for k, _, t in items]))
        for key, value, _ttl in items:
            self.store[key] = value
        return True

    async def set_many_fenced(self, items, *, counter, delete=()):
        call = ("set_many_fenced", [(k, t) for k, _, t in items], counter)
        self.calls.append((*call, tuple(delete)) if delete else call)
        key, expected, _ttl = counter
        if int(self.store.get(key) or 0) != expected:
            return False
        for key, value, _ttl in items:
            self.store[key] = value
        for key in delete:
            self.store.pop(key, None)
        return True

    async def delete(self, key, *more):
        self.calls.append(("delete", key, *more))
        return any([self.store.pop(k, None) is not None for k in (key, *more)])

    async def bump(self, key, ttl, *, also_set=None, also_delete=()):
        call = ("bump", key, ttl)
        if also_set is not None or also_delete:
            call = (*call, also_set, tuple(also_delete))
        self.calls.append(call)
        self.store[key] = int(self.store.get(key) or 0) + 1
        if also_set is not None:
            self.store[also_set[0]] = also_set[1]
        for k in also_delete:
            self.store.pop(k, None)
        return self.store[key]


class _Auth:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.requests: list[dict] = []

    async def post(self, url, json=None, headers=None):
        self.requests.append({"url": url, "json": json, "headers": dict(headers)})
        answer = self.answers.pop(0) if self.answers else {"valid": True, "access_tier": 2}
        if isinstance(answer, Exception):
            raise answer
        return httpx.Response(200, json=answer)


def _deployment(cache, auth, *, door, contract=ACCOUNT_CONTRACT):
    stack = ExitStack()
    stack.enter_context(patch(f"{ACCOUNT}.is_feature_enabled_system", lambda key: door))
    stack.enter_context(patch("src.server.services.membership.is_feature_enabled_system", lambda key: door))
    stack.enter_context(patch("src.server.services.membership.ACCOUNT_CONTRACT", contract))
    stack.enter_context(patch(f"{LIMITS}.HOST_MODE", "platform"))
    stack.enter_context(patch("src.server.services.platform_client.HOST_MODE", "platform"))
    stack.enter_context(patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://auth.test"))
    stack.enter_context(patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://auth.test"))
    stack.enter_context(patch("src.server.services.platform_client.get_http_client", AsyncMock(return_value=auth)))
    stack.enter_context(
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache)
    )
    stack.enter_context(patch.dict("os.environ", {"INTERNAL_SERVICE_TOKEN": "svc"}))
    return stack


ORG = {"org": {"id": "o-1", "name": "Acme"}, "features": {"teams": True}}
BASE_HEADERS = {"X-User-Id": "u", "X-Service-Token": "svc"}


@pytest.mark.asyncio
async def test_a_shut_door_reads_and_writes_what_it_did_before():
    """Same request, same keys, same commands, and no organization blocks
    stored even if an answer carried them."""
    cache, auth = _Cache(), _Auth({"valid": True, "access_tier": 2, **ORG})
    with _deployment(cache, auth, door=False):
        assert await membership.read_membership("u") == {
            "access_tier": 2, "plan_display_name": None,
        }
    assert auth.requests == [
        {"url": "http://auth.test/api/auth/validate", "json": None,
         "headers": BASE_HEADERS},
    ]
    assert cache.calls == [
        ("mget", [
            "platform_membership:u",
            "platform_membership:last_known:u",
            "platform_membership:unreachable:u",
        ]),
        ("set_many", [
            ("platform_membership:u", 300),
            ("platform_membership:last_known:u", 30 * 24 * 3600),
        ]),
    ]
    assert cache.store["platform_membership:u"] == {
        "access_tier": 2, "plan_display_name": None,
    }


@pytest.mark.asyncio
async def test_a_shut_door_pauses_and_refreshes_as_before():
    cache, auth = _Cache(), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=False):
        assert await membership.read_membership("u", refresh=True) is None
    assert cache.calls[1:] == [
        ("delete", "platform_membership:u"),
        ("set", "platform_membership:unreachable:u", 15),
    ]


@pytest.mark.asyncio
async def test_an_open_door_declares_the_contract_and_keys_by_it():
    cache, auth = _Cache(), _Auth({"valid": True, "access_tier": 2, **ORG})
    with _deployment(cache, auth, door=True):
        answer = await membership.read_membership("u")
    assert answer["org"] == ORG["org"]
    [request] = auth.requests
    assert request["headers"] == {**BASE_HEADERS, "X-Account-Contract": "4"}
    assert cache.calls == [
        ("mget", [
            "platform_membership:v4:u",
            "platform_membership:last_known:v4:u",
            "platform_membership:unreachable:u",
            "platform_membership:epoch:u",
            "platform_membership:suspended:u",
        ]),
        ("set_many_fenced", [
            ("platform_membership:v4:u", 300),
            ("platform_membership:last_known:v4:u", 30 * 24 * 3600),
        ], ("platform_membership:epoch:u", 0, EPOCH_TTL),
            ("platform_membership:suspended:u",)),
    ]


@pytest.mark.asyncio
async def test_an_open_door_pauses_on_the_shared_key_fenced():
    cache, auth = _Cache(), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=True):
        assert await membership.read_membership("u") is None
    assert cache.calls[1] == (
        "set_many_fenced",
        [("platform_membership:unreachable:u", 15)],
        ("platform_membership:epoch:u", 0, EPOCH_TTL),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        # Fetched with the door shut: no blocks, which would read as no org.
        "platform_membership:{kind}u",
        # Fetched for another version, with a rule this build may not know.
        "platform_membership:{kind}v5:u",
    ],
)
async def test_an_entry_fetched_under_another_contract_is_never_read(key):
    other = {"access_tier": 2, "plan_display_name": "Pro",
             "policy": {"a_rule_from_a_later_version": True}}
    store = {key.format(kind=""): other, key.format(kind="last_known:"): other}
    cache, auth = _Cache(store), _Auth({"valid": True, "access_tier": 3, **ORG})
    with _deployment(cache, auth, door=True):
        acct = await account.account_for("u")
    assert acct.org == Org(id="o-1", name="Acme")
    assert len(auth.requests) == 1

    # Nor does one stand in during an outage: the account is unknown.
    cache, auth = _Cache(store), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=True):
        assert await account.account_for("u") is None


@pytest.mark.asyncio
async def test_one_notice_fences_every_versions_entries():
    """Two builds share the cache; a notice reaching either one drops what
    both had cached, and an ask either had out cannot land after it."""
    cache = _Cache()
    v1, v2 = _Auth({"valid": True, "access_tier": 2, **ORG}), _Auth(
        {"valid": True, "access_tier": 2, **ORG}
    )
    with _deployment(cache, v1, door=True, contract=1):
        await membership.read_membership("u")
    with _deployment(cache, v2, door=True, contract=2):
        await membership.read_membership("u")
        await membership.forget_membership("u")
    assert cache.store["platform_membership:epoch:u"] == 1
    assert "platform_membership:v2:u" not in cache.store
    # Version 1's entry is still in Redis but no longer read.
    assert "platform_membership:v1:u" in cache.store
    removed = _Auth({"valid": True, "access_tier": 2})
    with _deployment(cache, removed, door=True, contract=1):
        assert await account.account_for("u") is account.NO_ORG
    assert len(removed.requests) == 1

    # In an outage version 1's record keeps its plan, not its organization.
    cache.store["platform_membership:epoch:u"] = 2
    down = _Auth(httpx.ConnectError("down"))
    with _deployment(cache, down, door=True, contract=1):
        assert await account.account_for("u") is None
        assert await membership.access_tier("u") == 2


@pytest.mark.asyncio
async def test_a_notice_to_a_build_with_the_door_shut_still_fences():
    """A cutover that turns ``teams`` on: the notice may reach the build
    without it, and the build with it must still see the change."""
    cache = _Cache()
    with _deployment(cache, _Auth({"valid": True, "access_tier": 2, **ORG}), door=True):
        await membership.read_membership("u")
    with _deployment(cache, _Auth(), door=False):
        await membership.forget_membership("u")
    assert cache.calls[-3:] == [
        ("bump", "platform_membership:epoch:u", EPOCH_TTL),
        ("delete", "platform_membership:u"),
        ("delete", "platform_membership:unreachable:u"),
    ]
    removed = _Auth({"valid": True, "access_tier": 2})
    with _deployment(cache, removed, door=True):
        assert await account.account_for("u") is account.NO_ORG
    assert len(removed.requests) == 1


@pytest.mark.asyncio
async def test_a_notice_drops_this_builds_entry():
    cache = _Cache()
    with _deployment(cache, _Auth(), door=True):
        await membership.forget_membership("u")
    assert cache.calls == [
        ("bump", "platform_membership:epoch:u", EPOCH_TTL),
        # A door-shut worker sharing the cache reads no epoch, so its entry
        # goes in the same command.
        ("delete", "platform_membership:v4:u", "platform_membership:u"),
        ("delete", "platform_membership:unreachable:u"),
    ]


@pytest.mark.asyncio
async def test_a_notice_to_an_open_door_clears_a_shut_door_workers_entry():
    cache = _Cache()
    with _deployment(cache, _Auth({"valid": True, "access_tier": 2}), door=False):
        await membership.read_membership("u")
    with _deployment(cache, _Auth(), door=True):
        await membership.forget_membership("u")
    removed = _Auth({"valid": True, "access_tier": 0})
    with _deployment(cache, removed, door=False):
        assert await membership.access_tier("u") == 0
    assert len(removed.requests) == 1


@pytest.mark.asyncio
async def test_an_open_door_refresh_drops_a_shut_door_workers_entry_too():
    cache, auth = _Cache(), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=True):
        await membership.read_membership("u", refresh=True)
    assert ("delete", "platform_membership:v4:u", "platform_membership:u") in cache.calls


# --- the plan standing in across a switch -------------------------------------

PLAN = {"access_tier": 2, "plan_display_name": "Pro"}


@pytest.mark.asyncio
@pytest.mark.parametrize("paused", [False, True])
async def test_turning_teams_on_keeps_the_plan_through_an_outage(paused):
    """The record a door-shut build kept stands in for the plan; the
    organization reads as unknown."""
    store = {"platform_membership:last_known:u": PLAN}
    if paused:
        store["platform_membership:unreachable:u"] = True
    cache, auth = _Cache(store), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=True):
        assert await membership.read_membership("u") == {
            **PLAN, "account_unknown": True,
        }
        assert await account.account_for("u") is None
    assert ("mget", [
        "platform_membership:last_known:v3:u",
        "platform_membership:last_known:v2:u",
        "platform_membership:last_known:v1:u",
        "platform_membership:last_known:u",
    ]) in cache.calls


@pytest.mark.asyncio
async def test_a_contract_bump_reads_the_nearest_earlier_record_for_its_plan():
    store = {
        "platform_membership:last_known:v2:u": {
            "access_tier": 3, "plan_display_name": "Team", "membership_epoch": 4, **ORG,
        },
        "platform_membership:last_known:v1:u": {"access_tier": 1, "plan_display_name": "Old"},
        "platform_membership:last_known:u": PLAN,
    }
    cache, auth = _Cache(store), _Auth(httpx.ConnectError("down"))
    with _deployment(cache, auth, door=True, contract=3):
        assert await membership.read_membership("u") == {
            "access_tier": 3, "plan_display_name": "Team", "account_unknown": True,
        }
    assert cache.calls[-1] == ("mget", [
        "platform_membership:last_known:v2:u",
        "platform_membership:last_known:v1:u",
        "platform_membership:last_known:u",
    ])


@pytest.mark.asyncio
async def test_no_earlier_record_is_read_unless_needed():
    """Not when the auth service answers, nor when this version's record
    exists, nor with the door shut."""
    earlier = {"platform_membership:last_known:u": PLAN}
    answered = _Cache(earlier)
    with _deployment(answered, _Auth({"valid": True, "access_tier": 2, **ORG}), door=True):
        await membership.read_membership("u")
    kept = _Cache({**earlier, "platform_membership:last_known:v4:u": {**PLAN, **ORG}})
    with _deployment(kept, _Auth(httpx.ConnectError("down")), door=True):
        assert (await membership.read_membership("u"))["org"] == ORG["org"]
    for cache in (answered, kept):
        assert [c for c in cache.calls if c[0] == "mget"] == [cache.calls[0]]
    cache = _Cache()
    with _deployment(cache, _Auth(httpx.ConnectError("down")), door=False):
        assert await membership.read_membership("u") is None
    assert [c[0] for c in cache.calls] == ["mget", "set"]


# --- only the membership fetch declares it -------------------------------------


@pytest.mark.asyncio
async def test_no_other_call_to_the_auth_service_declares_the_contract():
    auth = _Auth(*[{"valid": True, "access_tier": 2, "allowed": True, "scopes": [],
                    "quota": {"used": 0, "limit": 5}}] * 3)
    with _deployment(_Cache(), auth, door=True):
        usage_limits._scope_cache.pop("u", None)
        await usage_limits.enforce_credit_limit("u")
        await usage_limits._get_user_scopes("u")
        await usage_limits.get_capacity_status("u", "computers")
        usage_limits._scope_cache.pop("u", None)
    assert len(auth.requests) == 3
    assert all(r["headers"] == BASE_HEADERS for r in auth.requests)


def test_only_the_membership_fetch_passes_a_contract():
    passing = [
        fn.name
        for module in (usage_limits, membership)
        for fn in ast.walk(ast.parse(inspect.getsource(module)))
        if isinstance(fn, ast.AsyncFunctionDef | ast.FunctionDef)
        for call in ast.walk(fn)
        if isinstance(call, ast.Call)
        and getattr(call.func, "attr", None) == "call_validate"
        and any(k.arg == "account_contract" for k in call.keywords)
    ]
    assert passing == ["_ask_platform_membership"]
