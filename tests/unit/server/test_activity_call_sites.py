"""Each group's acts reach the organization's activity log where they are
decided, and a member of no organization costs nothing at any of them."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database import activity_events
from src.server.services import activity
from src.server.services.account_contract import Account, Org
from src.server.services.policy import ALLOW_ALL

pytestmark = pytest.mark.asyncio

ACME = Account(org=Org(id="org-1", name="Acme"))
THREAD = "70000000-0000-4000-8000-000000000001"
WS = "90000000-0000-4000-8000-000000000001"


def _as_owner(thread_id, caller, **_):
    """The access check as it answers a thread's owner."""
    from src.server.services.shared_workspaces.access import Access

    return Access(caller)



@pytest.fixture
def insert(monkeypatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr(activity_events, "insert", mock)
    return mock


@pytest.fixture
def in_org(monkeypatch):
    """For the whole test, so the writes still pending at its end land as a
    member of an organization's."""
    from src.server.services import account

    monkeypatch.setattr("src.server.services.membership.teams_door_open", lambda: True)
    monkeypatch.setattr(account, "account_for", AsyncMock(return_value=ACME))


async def _rows(insert: AsyncMock) -> list[dict]:
    """The rows written, once every pending write has landed."""
    await activity.drain()
    return [c.kwargs for c in insert.await_args_list]


@contextmanager
def _thread_share():
    """The thread's row, kept across requests as the share route writes it."""
    row = {
        "is_shared": False, "share_token": None, "workspace_id": WS,
        "share_permissions": {"allow_files": True, "root_path": "r"},
    }

    async def update(thread_id, *, is_shared, share_permissions=None, shared_at=None):
        row["is_shared"] = is_shared
        row["share_token"] = row["share_token"] or "t"
        if share_permissions is not None:
            row["share_permissions"] = share_permissions
        return dict(row)

    async def read(thread_id):
        return dict(row)

    with (
        patch("src.server.services.account.policy_for", new_callable=AsyncMock,
              return_value=ALLOW_ALL),
        patch("src.server.services.shared_workspaces.access.thread_access", new_callable=AsyncMock, side_effect=_as_owner),
        patch("src.server.app.threads.sharing.get_thread_by_id", read),
        patch("src.server.app.threads.sharing.update_thread_sharing", update),
    ):
        yield


async def _share(on: bool, **permissions):
    from src.server.app.threads.sharing import update_thread_share
    from src.server.models.conversation import ThreadShareRequest

    await update_thread_share(
        THREAD,
        ThreadShareRequest(is_shared=on, permissions=permissions or None),
        "u1",
    )


# --- sharing ------------------------------------------------------------------


async def test_a_thread_link_turned_on_and_off_is_recorded(insert, in_org):
    with _thread_share():
        await _share(True)
        await _share(False)
    on, off = await _rows(insert)
    assert (on["action"], off["action"]) == ("sharing.thread_link_on", "sharing.thread_link_off")
    assert (on["resource_kind"], on["resource_id"]) == ("thread", THREAD)
    assert (on["thread_id"], on["workspace_id"]) == (THREAD, WS)
    assert on["detail"] == {"allow_files": True}  # flags only, never the path


async def test_a_link_already_on_records_only_what_its_permissions_changed(insert, in_org):
    with _thread_share():
        await _share(True)
        await _share(True)  # asked again: nothing changed
        await _share(True, allow_download=True)
        await _share(True, root_path="elsewhere")
        await _share(False)
        await _share(False)  # already off
    rows = await _rows(insert)
    assert [r["action"] for r in rows] == [
        "sharing.thread_link_on",
        "sharing.thread_link_permissions_changed",
        "sharing.thread_link_permissions_changed",
        "sharing.thread_link_off",
    ]
    assert rows[1]["detail"] == {"allow_download": True}
    assert rows[2]["detail"] == {"root_path_changed": True}
    assert "elsewhere" not in repr(rows)


async def test_with_the_teams_flag_off_a_call_site_reads_and_writes_nothing(insert):
    cache = MagicMock()
    with (
        patch("src.server.services.account.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.account.deployment_policy", return_value=None),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("src.server.services.shared_workspaces.access.thread_access", new_callable=AsyncMock, side_effect=_as_owner),
        patch("src.server.app.threads.sharing.get_thread_by_id", new_callable=AsyncMock,
              return_value={"share_permissions": {}}),
        patch("src.server.app.threads.sharing.update_thread_sharing", new_callable=AsyncMock,
              return_value={"is_shared": True, "share_token": "t"}),
    ):
        await _share(True)
        await activity.drain()
    insert.assert_not_awaited()
    assert not cache.method_calls


# --- connectors ---------------------------------------------------------------


async def test_switching_a_connector_off_is_recorded(insert, in_org):
    from src.server.services import mcp_catalog

    with (
        patch.object(mcp_catalog, "set_catalog_server_enabled",
                     AsyncMock(return_value={"name": "acme"})),
        patch("src.server.services.mcp_oauth.lifecycle.revoke_live_grants", AsyncMock()),
    ):
        await mcp_catalog.apply_catalog_enabled("u1", "acme", False)
    (row,) = await _rows(insert)
    assert row["action"] == "connector.turned_off"
    assert row["detail"] == {"name": "acme", "scope": "account"}


async def test_a_brokerage_sign_in_records_its_consented_groups_never_its_token(insert, in_org):
    from src.server.services.mcp_oauth import connect

    record = SimpleNamespace(
        user_id="u1",
        server_url="https://broker.example.com/mcp",
        granted_capabilities=["trading", "account"],
    )
    with (
        patch.object(connect, "brokerage_for_url", return_value=SimpleNamespace(name="moomoo")),
    ):
        connect._record_sign_in(record, "moomoo", org=False)
    signed_in, consent = await _rows(insert)
    assert signed_in["action"] == "connector.signed_in"
    assert signed_in["detail"] == {
        "name": "moomoo", "host": "broker.example.com", "brokerage": "moomoo",
    }
    assert consent["action"] == "trading.brokerage_consent_granted"
    assert consent["detail"] == {"brokerage": "moomoo", "groups": ["account", "trading"]}


# --- credentials --------------------------------------------------------------


async def test_a_vault_secret_is_recorded_by_name_only(insert, monkeypatch, in_org):
    from src.server.app import user_vault
    from src.server.models.vault import CreateSecretRequest

    monkeypatch.setattr(user_vault, "create_user_secret", AsyncMock())
    monkeypatch.setattr(user_vault, "after_secret_change", AsyncMock())
    await user_vault.create_secret(
        CreateSecretRequest(name="OPENAI_KEY", value="sk-very-secret"), "u1"
    )
    (row,) = await _rows(insert)
    assert row["action"] == "credential.secret_created"
    assert row["detail"] == {"name": "OPENAI_KEY"}
    assert "sk-very-secret" not in repr(row)


async def test_a_model_key_removed_is_recorded(insert, monkeypatch, in_org):
    from src.server.app import api_keys

    monkeypatch.setattr(api_keys, "_get_custom_providers", AsyncMock(return_value=[]))
    monkeypatch.setattr(api_keys, "_get_allowed_providers", lambda _: {"openai"})
    monkeypatch.setattr(api_keys, "delete_api_key", AsyncMock())
    monkeypatch.setattr(api_keys, "invalidate_byok_cache", AsyncMock())
    monkeypatch.setattr(api_keys, "invalidate_available_models", AsyncMock())
    monkeypatch.setattr(api_keys, "_keys_view", AsyncMock(return_value={}))
    await api_keys.remove_api_key("openai", "u1")
    (row,) = await _rows(insert)
    assert (row["action"], row["detail"]) == (
        "credential.model_key_removed", {"provider": "openai"},
    )


@pytest.mark.parametrize("updated", [True, False], ids=["key-row", "no-key-row"])
async def test_a_key_redirected_to_another_host_is_recorded_by_host_only(
    insert, monkeypatch, in_org, updated
):
    from contextlib import asynccontextmanager

    from src.server.app import api_keys

    @asynccontextmanager
    async def writes(_user_id):
        yield None

    monkeypatch.setattr(api_keys, "require_policy", AsyncMock(return_value=ALLOW_ALL))
    monkeypatch.setattr(api_keys, "_get_custom_providers", AsyncMock(return_value=[]))
    monkeypatch.setattr(api_keys, "_get_allowed_providers", lambda _: {"openai"})
    monkeypatch.setattr(api_keys, "_refuse_base_urls", AsyncMock())
    monkeypatch.setattr(api_keys, "key_writes", writes)
    monkeypatch.setattr(api_keys, "update_base_url", AsyncMock(return_value=updated))
    monkeypatch.setattr(api_keys, "invalidate_byok_cache", AsyncMock())
    monkeypatch.setattr(api_keys, "invalidate_available_models", AsyncMock())
    monkeypatch.setattr(api_keys, "_keys_view", AsyncMock(return_value={}))
    monkeypatch.setattr(
        "src.server.services.onboarding.maybe_complete_personalization", AsyncMock()
    )
    await api_keys.update_api_keys(
        api_keys.UpdateApiKeysRequest(
            base_urls={"openai": "https://user:pw@llm.example.com/v1?key=abc"}
        ),
        "u1",
    )
    rows = await _rows(insert)
    if not updated:
        assert rows == []  # no key there, so nothing was redirected
        return
    (row,) = rows
    assert row["action"] == "credential.model_key_endpoint_changed"
    assert row["detail"] == {"provider": "openai", "host": "llm.example.com"}
    assert "pw" not in repr(row) and "abc" not in repr(row)


# --- computers and trading ------------------------------------------------------


async def test_the_trading_permission_change_is_recorded_from_and_to(insert, monkeypatch, in_org):
    from src.server.app import trading_permission
    from src.server.services.trading_permission import TradingPermission

    monkeypatch.setattr(
        trading_permission, "set_trading_permission",
        AsyncMock(return_value={
            "level": "no_trading", "agreement_version": None, "agreed_at": None,
            "updated_at": None, "previous_level": "approve_each",
        }),
    )
    with patch(
        "src.server.services.account.policy_for", new_callable=AsyncMock, return_value=ALLOW_ALL
    ):
        await trading_permission.update_trading_permission(
            trading_permission.TradingPermissionUpdate(level=TradingPermission.NO_TRADING), "u1"
        )
    (row,) = await _rows(insert)
    assert row["action"] == "trading.permission_changed"
    assert row["detail"] == {"from": "approve_each", "to": "no_trading"}


@pytest.mark.parametrize("previous", ["no_trading", None], ids=["same", "never-chosen-default"])
async def test_a_trading_permission_set_to_what_it_was_records_nothing(
    insert, monkeypatch, in_org, previous
):
    from src.server.app import trading_permission
    from src.server.services.trading_permission import (
        DEFAULT_TRADING_PERMISSION,
        TradingPermission,
    )

    level = TradingPermission(previous) if previous else DEFAULT_TRADING_PERMISSION
    monkeypatch.setattr(
        trading_permission, "set_trading_permission",
        AsyncMock(return_value={
            "level": level.value, "agreement_version": None, "agreed_at": None,
            "updated_at": None, "previous_level": previous,
        }),
    )
    with patch(
        "src.server.services.account.policy_for", new_callable=AsyncMock, return_value=ALLOW_ALL
    ):
        await trading_permission.update_trading_permission(
            trading_permission.TradingPermissionUpdate(level=level), "u1"
        )
    assert await _rows(insert) == []


async def test_a_computer_added_is_recorded_with_its_kind(insert, in_org):
    from tests.unit.server.services.test_computer_manager import _make_computer, _make_manager

    computer = {**_make_computer(status="stopped"), "name": "Research", "is_primary": False}
    with (
        patch("src.server.services.computer_manager._machines.create_computer",
              AsyncMock(return_value=computer)),
    ):
        await _make_manager().create_computer_for_user("u1", name="Research")
    (row,) = await _rows(insert)
    assert row["action"] == "computer.added"
    assert row["resource_id"] == str(computer["computer_id"])
    assert row["detail"] == {"kind": "daytona", "name": "Research", "primary": False}


async def test_a_failed_record_does_not_fail_the_act(insert, in_org):
    insert.side_effect = RuntimeError("database down")
    with _thread_share():
        await _share(True)
        await activity.drain()
    insert.assert_awaited_once()


async def _named(_user_id, base, insert):
    return await insert(base)


@pytest.mark.parametrize("won", [True, False], ids=["stands", "lost-the-race"])
async def test_a_primary_computer_made_for_the_member_is_recorded_as_automatic(
    insert, in_org, won
):
    from tests.unit.server.services.test_computer_manager import _make_computer, _make_manager

    mine = _make_computer("comp-1", status="stopped", is_primary=won)
    theirs = _make_computer("comp-2", status="stopped")
    with (
        patch("src.server.services.computer_manager._machines.get_primary_computer",
              AsyncMock(side_effect=[None, theirs])),
        patch("src.server.services.computer_manager._machines.create_computer",
              AsyncMock(return_value=mine)),
        patch("src.server.services.computer_manager._machines.update_computer_status",
              AsyncMock()),
        patch("src.server.services.computer_manager._machines.insert_under_free_name",
              _named),
        # No other computer to promote and no choice made, so one is built.
        patch("src.server.services.computer_manager._machines.promote_primary_computer",
              AsyncMock(return_value=None)),
        patch("src.server.services.computer_manager._machines.get_user",
              AsyncMock(return_value=None)),
    ):
        got = await _make_manager().ensure_primary_computer("u1")
    rows = await _rows(insert)
    if not won:
        # The losing insert is tombstoned at once: nothing was added.
        assert got is theirs and rows == []
        return
    (row,) = rows
    assert row["action"] == "computer.added"
    assert row["detail"]["automatic"] is True
