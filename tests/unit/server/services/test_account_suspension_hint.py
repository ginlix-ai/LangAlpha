"""The suspension a change notice carries: it holds while the auth service
cannot be asked again, every refusal of a suspended account counts it, any
answer from the auth service replaces it, and nothing about it runs with
``teams`` off."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException

from src.server.services import membership
from src.server.services import account
from src.server.services.account import ACCOUNT_SUSPENDED, AccountSuspended
from tests.unit.server.services.test_account_contract import _Auth, _Cache, _deployment

pytestmark = pytest.mark.asyncio

HINT = "platform_membership:suspended:u"
EPOCH = "platform_membership:epoch:u"
MEMBER = {
    "valid": True,
    "access_tier": 2,
    "plan_display_name": "Pro",
    "org": {"id": "o-1", "name": "Acme"},
    "features": {"teams": True},
}
SUSPENDED = {**MEMBER, "org": {**MEMBER["org"], "suspended": True}}
HINT_MESSAGE = (
    "This account's access through its organization has ended. Ask an admin "
    "of your organization if this is unexpected."
)


async def _refuse_if_suspended(user_id):
    (await account.read_standing(user_id)).refuse_if_suspended()


class _Hints(_Cache):
    async def get(self, key):
        self.calls.append(("get", key))
        return self.store.get(key)


class _Down(_Auth):
    """An auth service that cannot be reached."""

    async def post(self, url, json=None, headers=None):
        self.requests.append({"url": url})
        raise httpx.ConnectError("down")


class _Held(_Auth):
    """Answers once ``release`` is set, to race a notice against the ask."""

    def __init__(self, answer):
        super().__init__(answer)
        self.asked, self.release = asyncio.Event(), asyncio.Event()

    async def post(self, url, json=None, headers=None):
        self.asked.set()
        await self.release.wait()
        return await super().post(url, json=json, headers=headers)


async def _removed_then_down(cache, *, plan_on_record=True):
    """A member the auth service answered for, then a notice saying it
    suspended them, then an auth service that cannot be asked."""
    if plan_on_record:
        with _deployment(cache, _Auth(MEMBER), door=True):
            assert (await account.account_for("u")).org.name == "Acme"
    with _deployment(cache, _Auth(), door=True):
        await membership.forget_membership("u", suspended=True)


async def _refusals(cache) -> dict:
    """Every place a suspended account is refused, asked as it asks."""
    from src.server.services import account_suspension
    from src.server.services.credit_gate_port import PlatformCreditGatePort
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from src.server.services.egress.policy_gate import PolicyGate
    from src.server.services.account import refuse_machine_use, require_policy

    async def raised(call) -> object:
        try:
            await call
        except AccountSuspended as e:
            return e
        except HTTPException as e:
            return e.status_code
        return None

    stop = AsyncMock()
    with (
        _deployment(cache, _Down(), door=True),
        patch(
            "src.server.database.runs.lifecycle.user_has_running_work",
            AsyncMock(return_value=True),
        ),
        patch.object(account_suspension, "stop_account_work", stop),
        patch("src.server.services.egress.direct_tools.get_connection", AsyncMock()),
    ):
        epoch = cache.store.get(EPOCH)
        out = {
            "gate": await raised(account.admit("u")),
            "work": await raised(require_policy("u")),
            "computer": await raised(refuse_machine_use("daytona", "u")),
            "automation": await raised(_refuse_if_suspended("u")),
            "owner": (await account.read_standing("u", unknown_on_cache_error=True)).suspended,
            "in_run": await PlatformCreditGatePort().account_refusal("u"),
            "relay": await PolicyGate("u", via="relay", caller=None).suspended(),
            "direct": await DirectMCPBinding(user_id="u").check("crm", "search"),
        }
        await account_suspension._stop_if_suspended("u", epoch)
        out["notice_stop"] = stop.await_count == 1
    return out


async def test_an_unknown_account_the_notice_suspended_is_refused_everywhere():
    cache = _Hints()
    await _removed_then_down(cache)
    out = await _refusals(cache)
    for point in ("gate", "work", "computer", "automation", "in_run"):
        refusal = out[point]
        assert isinstance(refusal, AccountSuspended), point
        assert refusal.status_code == 403
        assert refusal.detail == {
            "message": HINT_MESSAGE, "type": ACCOUNT_SUSPENDED, "organization": None,
        }
    assert out["owner"] is True
    assert out["relay"] is True
    assert out["direct"].startswith("No call was made")
    assert out["notice_stop"] is True


async def test_with_no_plan_on_record_the_hint_alone_still_refuses():
    cache = _Hints()
    await _removed_then_down(cache, plan_on_record=False)
    out = await _refusals(cache)
    assert isinstance(out["gate"], AccountSuspended)
    assert out["owner"] is True
    with _deployment(cache, _Down(), door=True):
        # No plan stands in: a plan reader still reads it as unknown.
        assert await membership.access_tier("u") is None


async def test_an_unknown_account_with_no_hint_behaves_as_before():
    cache = _Hints()
    with _deployment(cache, _Auth(MEMBER), door=True):
        await account.account_for("u")
        await membership.forget_membership("u")  # a notice that says nothing
    out = await _refusals(cache)
    assert out["gate"] is None  # reads stay open
    assert out["work"] == 503  # work is refused as unknown
    assert out["automation"] is None
    assert out["owner"] is False
    assert out["in_run"] is None
    assert out["relay"] is False
    assert out["notice_stop"] is False
    assert HINT not in cache.store


async def test_a_known_answer_is_never_overridden_by_the_hint():
    cache = _Hints({HINT: True})
    with _deployment(cache, _Auth(MEMBER), door=True):
        # The fresh answer replaces the hint, and the known account wins.
        assert (await account.read_standing("u")).suspended is False
    assert HINT not in cache.store
    # Even a hint left beside a known entry (no writer leaves one) loses.
    cache.store[HINT] = True
    with _deployment(cache, _Down(), door=True):
        assert (await account.read_standing("u")).suspended is False
        await account.admit("u")


async def test_a_fresh_answer_sets_and_clears_the_hint():
    cache = _Hints()
    with _deployment(cache, _Auth(SUSPENDED), door=True):
        await account.account_for("u")
    assert cache.store[HINT] is True
    with _deployment(cache, _Auth(MEMBER), door=True):
        await membership.forget_membership("u")
        await account.account_for("u")
    assert HINT not in cache.store


@pytest.mark.parametrize(
    "org",
    [{"id": "o-1"}, {"id": "o-1", "name": "Acme", "suspended": "no"}, ["o-1"]],
)
async def test_a_fresh_answer_it_cannot_read_leaves_the_hint(org):
    cache = _Hints()
    await _removed_then_down(cache)
    with _deployment(cache, _Auth({**MEMBER, "org": org}), door=True):
        await membership.forget_membership("u")
        standing = await account.read_standing("u")
        assert standing.account is None
        assert standing.suspended
    assert cache.store[HINT] is True


async def test_a_restoring_notice_clears_the_hint():
    cache = _Hints()
    await _removed_then_down(cache)
    with _deployment(cache, _Auth(), door=True):
        await membership.forget_membership("u", suspended=False)
    assert HINT not in cache.store
    out = await _refusals(cache)
    assert out["gate"] is None and out["owner"] is False


async def test_a_restore_notice_racing_a_slow_fetch_keeps_the_hint_cleared():
    cache = _Hints()
    await _removed_then_down(cache)
    held = _Held(SUSPENDED)  # asked before the restore, answered after it
    with _deployment(cache, held, door=True):
        cache.store.pop("platform_membership:unreachable:u", None)
        fetch = asyncio.create_task(account.account_for("u"))
        await held.asked.wait()
        await membership.forget_membership("u", suspended=False)
        held.release.set()
        assert (await fetch).suspended  # the caller still gets its answer
    # Its write, the hint with it, was refused: the restore is newer.
    assert HINT not in cache.store
    assert "platform_membership:v5:u" not in cache.store


async def test_a_suspending_notice_racing_a_slow_fetch_keeps_the_hint():
    cache = _Hints()
    held = _Held(MEMBER)  # asked before the removal, answered after it
    with _deployment(cache, held, door=True):
        fetch = asyncio.create_task(account.account_for("u"))
        await held.asked.wait()
        await membership.forget_membership("u", suspended=True)
        held.release.set()
        await fetch
    # The answer from before the removal could not clear what it said.
    assert cache.store[HINT] is True
    out = await _refusals(cache)
    assert isinstance(out["gate"], AccountSuspended)


async def test_of_two_notices_the_later_one_stands():
    cache = _Hints()
    with _deployment(cache, _Auth(), door=True):
        await membership.forget_membership("u", suspended=True)
        await membership.forget_membership("u", suspended=False)
        assert HINT not in cache.store
        await membership.forget_membership("u", suspended=True)
        await membership.forget_membership("u")  # says nothing: kept
        assert cache.store[HINT] is True
    # Each moved the hint in the bump's own step, never after it.
    assert [c for c in cache.calls if c[0] == "bump"][0] == (
        "bump", EPOCH, membership._PLATFORM_MEMBERSHIP_EPOCH_TTL,
        (HINT, True, 30 * 24 * 3600), (),
    )


# --- the notice route -------------------------------------------------------------


class _Request:
    """A request whose body arrives in ``chunks``, counting its reads."""

    def __init__(self, *chunks: bytes):
        self.chunks, self.reads = chunks, 0

    async def stream(self):
        self.reads += 1
        for chunk in self.chunks:
            yield chunk


def _request(body: bytes | tuple):
    return _Request(*(body if isinstance(body, tuple) else (body,)))


async def _notice(body: bytes, *, door: bool):
    from src.server.app import users

    forget = AsyncMock(return_value=3)
    request = _request(body)
    with (
        patch(f"{membership.__name__}.forget_membership", forget),
        patch("src.server.services.membership.teams_door_open", return_value=door),
        patch("src.server.services.account.is_feature_enabled_system", return_value=door),
        patch("src.server.services.membership.is_feature_enabled_system", return_value=door),
        patch(
            "src.server.database.mcp_config_version.bump_user_workspaces_mcp_version",
            AsyncMock(),
        ),
        patch("src.server.services.account_suspension.stop_if_suspended_soon"),
    ):
        await users.membership_changed("u", request)
    return forget.await_args.kwargs["suspended"], request.reads


@pytest.mark.parametrize(
    ("body", "hint"),
    [
        (b'{"suspended": true}', True),
        (b'{"suspended": false}', False),
        (b"", None),
        (b"{}", None),
        (b'{"suspended": "yes"}', None),
        (b'{"suspended": 1}', None),
        (b'{"suspended": null}', None),
        (b"[true]", None),
        (b"not json", None),
        (b"\xff\xfe", None),
        # Too deep for the parser, and too long to be a hint.
        (b"[" * 900 + b"]" * 900, None),
        (b'{"suspended": true' + b" " * 2000 + b"}", None),
        ((b'{"suspended": ', b"true}"), True),
    ],
)
async def test_the_notice_reads_only_a_boolean_and_never_fails_on_its_body(body, hint):
    assert await _notice(body, door=True) == (hint, 1)


async def test_a_body_that_cannot_be_read_never_fails_the_notice():
    class _Cut(_Request):
        async def stream(self):
            self.reads += 1
            yield b'{"suspended": '
            raise RuntimeError("client went away")

    from src.server.app import users

    with patch.object(users, "_NOTICE_BODY_LIMIT", 10**6):
        assert await users._notice_suspended(_Cut()) is None
        deep = b"[" * 100_000 + b"]" * 100_000
        assert await users._notice_suspended(_Request(deep)) is None


async def test_with_teams_off_the_notice_reads_no_body():
    assert await _notice(b'{"suspended": true}', door=False) == (None, 0)


async def test_with_teams_off_the_notice_touches_no_hint():
    """The same commands as before the hint existed."""
    cache = _Hints()
    with _deployment(cache, _Auth(), door=False):
        await membership.forget_membership("u", suspended=None)
    assert cache.calls == [
        ("bump", EPOCH, membership._PLATFORM_MEMBERSHIP_EPOCH_TTL),
        ("delete", "platform_membership:u"),
        ("delete", "platform_membership:unreachable:u"),
    ]


# --- /users/me --------------------------------------------------------------------


async def test_users_me_says_suspended_when_only_the_hint_is_known():
    from src.server.app import users

    cache = _Hints()
    await _removed_then_down(cache)
    record = {"user": {"user_id": "u", "email": "u@example.com",
                       "created_at": "2026-11-01T00:00:00Z",
                       "updated_at": "2026-11-01T00:00:00Z"},
              "preferences": None}
    with (
        _deployment(cache, _Down(), door=True),
        patch.object(users, "get_user_with_preferences", AsyncMock(return_value=record)),
    ):
        answer = await users.get_current_user.__wrapped__("u", refresh_tier=False)
    assert answer.user.access_tier == 2  # the plan on record stands in
    assert answer.user.organization.model_dump() == {
        "id": None, "name": None, "suspended": True, "teams": [],
    }
    assert answer.user.policy is None


async def test_users_me_says_suspended_with_no_plan_on_record():
    """Newly suspended during an outage, with nothing on record: answered
    with no access rather than 503, so the client learns of the suspension."""
    from src.server.app import users

    cache = _Hints()
    await _removed_then_down(cache, plan_on_record=False)
    record = {"user": {"user_id": "u", "email": "u@example.com",
                       "created_at": "2026-11-01T00:00:00Z",
                       "updated_at": "2026-11-01T00:00:00Z"},
              "preferences": None}
    with (
        _deployment(cache, _Down(), door=True),
        patch.object(users, "get_user_with_preferences", AsyncMock(return_value=record)),
    ):
        answer = await users.get_current_user.__wrapped__("u", refresh_tier=False)
    assert answer.user.access_tier == -1
    assert answer.user.plan_display_name is None
    assert answer.user.organization.model_dump() == {
        "id": None, "name": None, "suspended": True, "teams": [],
    }
    assert answer.user.policy is None


async def test_users_me_with_nothing_on_record_and_no_hint_is_still_503():
    from src.server.app import users

    cache = _Hints()
    record = {"user": {"user_id": "u", "email": "u@example.com",
                       "created_at": "2026-11-01T00:00:00Z",
                       "updated_at": "2026-11-01T00:00:00Z"},
              "preferences": None}
    with (
        _deployment(cache, _Down(), door=True),
        patch.object(users, "get_user_with_preferences", AsyncMock(return_value=record)),
        pytest.raises(HTTPException) as refused,
    ):
        await users.get_current_user.__wrapped__("u", refresh_tier=False)
    assert refused.value.status_code == 503
