"""An install parked for a sign-in survives its round trip and, once committed,
takes back what it wrote if a step after its row landed fails.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.mcp_server import McpServerInput
from src.server.models.plugin import ComponentResult, InstallReport
from src.server.services.mcp_validation import SignInNeeded
from src.server.services.plugins import pending as pending_mod
from src.server.services.plugins.checks import CheckedServers
from src.server.services.plugins.pending import PendingPluginInstall
from tests.unit.server.plugin_builders import PLUGIN_ID, USER


def test_a_pending_install_survives_its_json_round_trip():
    pending = PendingPluginInstall(
        plugin_name="demo",
        archive=b"PK\x03\x04 bytes",
        subdir="plugins/demo",
        name_hint="demo",
        source_type="git",
        source_ref="https://example.com/demo",
        content_hash="abc",
        secrets={"VENDOR_KEY": "sk-test-value"},
        checked=CheckedServers.of(
            [McpServerInput(name="vendor", transport="http", url="https://vendor.example.com/mcp")]
        ),
        sign_in=SignInNeeded(server="vendor", url="https://vendor.example.com/mcp"),
        sign_in_key="vendor",
    )
    payload = json.loads(json.dumps(pending.model_dump(mode="json")))
    assert payload["archive"] == "UEsDBCBieXRlcw=="
    assert PendingPluginInstall.model_validate(payload) == pending


def _pending() -> PendingPluginInstall:
    return PendingPluginInstall(
        plugin_name="demo",
        archive=b"zip",
        subdir=None,
        name_hint=None,
        source_type="zip",
        source_ref=None,
        content_hash="abc",
        secrets={"VENDOR_KEY": "sk-test-value"},
        checked=CheckedServers(),
        sign_in=SignInNeeded(server="vendor", url="https://vendor.example.com/mcp"),
        sign_in_key="vendor",
    )


async def _commit(install):
    package = type("P", (), {"name": "demo", "content_hash": "abc"})()
    uninstall = AsyncMock()
    drop = AsyncMock()
    invalidate = AsyncMock()
    with (
        patch.object(pending_mod, "validate_package", lambda *_a, **_k: package),
        patch.object(pending_mod, "get_plugin", AsyncMock(return_value=None)),
        patch.object(pending_mod, "get_catalog_server", AsyncMock(return_value=None)),
        patch.object(pending_mod, "install_plugin_package", install),
        patch.object(pending_mod, "list_plugin_owned_secrets", AsyncMock(
            return_value={"VENDOR_KEY"}
        )),
        patch.object(pending_mod, "uninstall_plugin", uninstall),
        patch.object(pending_mod, "drop_secrets", drop),
        patch.object(pending_mod, "after_secrets_changed", invalidate),
    ):
        try:
            await pending_mod.commit_pending_plugin_install(USER, _pending())
        except Exception as e:
            return e, uninstall, drop, invalidate
    return None, uninstall, drop, invalidate


@pytest.mark.asyncio
async def test_a_pending_install_that_fails_after_its_row_landed_is_taken_back():
    plugin = {"user_plugin_id": PLUGIN_ID, "name": "demo"}

    async def install(*_a, progress, **_k):
        progress.plugin = plugin
        progress.report = InstallReport(secrets_created=["VENDOR_AUTHORIZATION"])
        progress.introduced = ["UNCLAIMED_KEY"]
        raise RuntimeError("skill archive store is down")

    error, uninstall, drop, invalidate = await _commit(AsyncMock(side_effect=install))
    assert isinstance(error, RuntimeError)
    uninstall.assert_awaited_once_with(USER, plugin)
    introduced = ["UNCLAIMED_KEY", "VENDOR_AUTHORIZATION", "VENDOR_KEY"]
    assert drop.await_args.args == (USER, introduced)
    # Running sandboxes were handed them at install, so they are told too.
    assert invalidate.await_args.args == (USER, introduced)


@pytest.mark.asyncio
async def test_a_pending_install_refused_before_its_row_takes_nothing_back():
    error, uninstall, drop, invalidate = await _commit(
        AsyncMock(side_effect=ValueError("Plugin 'demo' is already installed"))
    )
    assert isinstance(error, ValueError)
    uninstall.assert_not_awaited()
    drop.assert_not_awaited()
    invalidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_pending_install_missing_any_component_is_taken_back():
    """The callback reports only connected or not, so an install that landed
    its sign-in server around a component in error is not one that worked."""
    plugin = {"user_plugin_id": PLUGIN_ID, "name": "demo"}

    async def install(*_a, progress, **_k):
        progress.plugin = plugin
        progress.report = InstallReport(components=[
            ComponentResult(kind="mcp", key="vendor", name="vendor", status="created"),
            ComponentResult(
                kind="mcp", key="other", name="other", status="error",
                reason="Maximum of 100 secrets per user reached",
            ),
        ])
        return plugin, progress.report

    error, uninstall, _drop, _invalidate = await _commit(AsyncMock(side_effect=install))
    assert isinstance(error, ValueError)
    assert str(error).startswith("other could not be added (Maximum of 100")
    uninstall.assert_awaited_once_with(USER, plugin)
