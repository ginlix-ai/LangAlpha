"""A rotated plugin key reaches the vault only after every row reading it passed.

The check runs before the fan-out lock with the new value standing in for the
vault; under the lock the write re-asks whether the name is still the
plugin's to write and whether a new reader appeared meanwhile.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.mcp_validation import CheckResult, McpCheckFailed
from src.server.services.plugins import gate, post_install
from src.server.services.plugins.errors import PluginRejected
from src.server.services.plugins.grants import BindGrants
from tests.unit.server.mcp_builders import check_results, checked_servers
from tests.unit.server.plugin_builders import (
    KEYED,
    MCP_SCHEMA,
    PLUGIN_ID,
    PLUGIN_SCHEMA,
    REFERENCING,
    USER,
    FanOutLock,
    catalog_row,
)

pytestmark = pytest.mark.asyncio


def _keyed_plugin() -> dict:
    return {
        "user_plugin_id": PLUGIN_ID, "name": "demo",
        "manifest": {
            "$schema": PLUGIN_SCHEMA, "name": "demo",
            "extensions": {"ai.langalpha": {"secrets": KEYED}},
        },
    }


async def _rotate(
    outcome: list[CheckResult],
    rows: list[dict],
    *,
    rows_under_lock: list[dict] | None = None,
    grants: list[BindGrants] | None = None,
    lock: FanOutLock | None = None,
):
    check = AsyncMock(return_value=outcome)
    lock = lock or FanOutLock()
    write = AsyncMock(return_value=(["VENDOR_KEY"], []))

    async def written(*args, **kwargs):
        # The vault write happens only while the fan-out lock is held.
        assert lock.held
        return ["VENDOR_KEY"], []

    write.side_effect = written
    granted = BindGrants(
        granted=frozenset({"VENDOR_KEY"}),
        held=frozenset({"VENDOR_KEY"}),
        already=frozenset({"VENDOR_KEY"}),
    )
    with (
        patch.object(gate, "resolve_bind_grants", AsyncMock(
            side_effect=grants or [granted, granted]
        )),
        patch.object(post_install, "list_catalog_servers", AsyncMock(
            side_effect=[rows, rows if rows_under_lock is None else rows_under_lock]
        )),
        patch.object(gate, "connected_bearers", AsyncMock(return_value={})),
        patch.object(gate, "check_servers", check),
        patch.object(post_install, "plugin_fan_out_lock", lock),
        patch.object(gate, "get_plugin", AsyncMock(return_value=_keyed_plugin())),
        patch.object(post_install, "write_supplied", write),
        patch.object(post_install, "claim_plugin_secrets", AsyncMock()),
        patch.object(post_install, "after_secrets_changed", AsyncMock()),
    ):
        try:
            return await post_install.apply_bindings(
                USER, _keyed_plugin(), {"VENDOR_KEY": "sk-new-value"}
            ), check, write
        except (McpCheckFailed, PluginRejected, ValueError) as e:
            return e, check, write


async def test_a_rotated_key_is_checked_before_the_vault_changes():
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
    )
    unrelated = catalog_row("other", "https://other.example.com/mcp")
    result, check, write = await _rotate(check_results(failed=["vendor"]), [row, unrelated])
    assert isinstance(result, McpCheckFailed)
    assert checked_servers(check.await_args) == {
        "vendor": {"VENDOR_KEY": "sk-new-value"}
    }
    write.assert_not_awaited()


async def test_a_rotated_key_that_passes_is_written():
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
    )
    result, _check, write = await _rotate(check_results(passed=["vendor"]), [row])
    assert result == ["VENDOR_KEY"]
    write.assert_awaited_once()


async def test_a_rotation_answered_with_a_sign_in_is_a_failure():
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
    )
    result, _check, write = await _rotate(check_results(sign_in=["vendor"]), [row])
    assert isinstance(result, McpCheckFailed)
    assert result.failures[0].verdict == "needs_credential"
    write.assert_not_awaited()


async def test_a_rotation_checks_every_row_that_reads_the_key_whoever_owns_it():
    # The plugin's own row is gone (forked off it); the fork keeps the
    # reference, and a hand-made row names the key too.
    fork = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
        plugin_id=None, plugin_server_key=None,
    )
    hand = catalog_row(
        "mine", "https://mine.example.com/mcp",
        headers={"Authorization": "Bearer ${vault:VENDOR_KEY}"},
        plugin_id=None, plugin_server_key=None,
    )
    unrelated = catalog_row("other", "https://other.example.com/mcp", plugin_id=None)
    result, check, write = await _rotate(
        check_results(failed=["vendor"]), [fork, hand, unrelated]
    )
    assert isinstance(result, McpCheckFailed)
    assert sorted(checked_servers(check.await_args)) == ["mine", "vendor"]
    write.assert_not_awaited()


async def test_a_rotation_takes_a_key_the_stored_entries_reference_and_nothing_else():
    plugin = {
        **_keyed_plugin(),
        "mcp_document": {"$schema": MCP_SCHEMA, "mcpServers": REFERENCING},
    }
    row = catalog_row(
        "foo", "https://foo.example.com/mcp",
        headers={"Authorization": "Bearer ${vault:FOO_TOKEN}"},
    )
    grants = BindGrants(
        granted=frozenset({"VENDOR_KEY"}),
        held=frozenset({"FOO_TOKEN"}),
        already=frozenset({"FOO_TOKEN"}),
    )
    check = AsyncMock(return_value=check_results(passed=["foo"]))
    write = AsyncMock(return_value=(["FOO_TOKEN"], []))
    with (
        patch.object(gate, "resolve_bind_grants", AsyncMock(return_value=grants)),
        patch.object(post_install, "list_catalog_servers", AsyncMock(return_value=[row])),
        patch.object(gate, "connected_bearers", AsyncMock(return_value={})),
        patch.object(gate, "check_servers", check),
        patch.object(post_install, "plugin_fan_out_lock", FanOutLock()),
        patch.object(gate, "get_plugin", AsyncMock(return_value=plugin)),
        patch.object(post_install, "write_supplied", write),
        patch.object(post_install, "claim_plugin_secrets", AsyncMock()),
        patch.object(post_install, "after_secrets_changed", AsyncMock()),
    ):
        with pytest.raises(PluginRejected, match="not asked for by this plugin: OTHER"):
            await post_install.apply_bindings(USER, plugin, {"OTHER": "x"})
        check.assert_not_awaited()
        written = await post_install.apply_bindings(
            USER, plugin, {"FOO_TOKEN": "foo-new-value"}
        )
    assert written == ["FOO_TOKEN"]
    assert checked_servers(check.await_args) == {"foo": {"FOO_TOKEN": "foo-new-value"}}
    write.assert_awaited_once()


async def test_a_value_the_user_created_during_the_check_is_not_overwritten():
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:OTHER_KEY}"},
    )
    lock = FanOutLock()
    before = BindGrants(granted=frozenset({"VENDOR_KEY"}))
    # VENDOR_KEY appeared in the vault, for something else, while the check ran.
    after = BindGrants(
        granted=frozenset({"VENDOR_KEY"}), held=frozenset({"VENDOR_KEY"})
    )
    result, _check, write = await _rotate(
        check_results(), [row], grants=[before, after], lock=lock
    )
    assert isinstance(result, PluginRejected)
    assert "already holds VENDOR_KEY" in str(result)
    assert lock.entered == 1
    write.assert_not_awaited()


async def test_a_rotation_refuses_a_reader_that_appeared_during_the_check():
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
    )
    added = catalog_row(
        "added", "https://added.example.com/mcp",
        headers={"X-Api-Key": "${vault:VENDOR_KEY}"}, plugin_id=None,
    )
    result, check, write = await _rotate(
        check_results(passed=["vendor"]), [row], rows_under_lock=[row, added]
    )
    assert isinstance(result, ValueError)
    assert "added changed while" in str(result)
    assert list(checked_servers(check.await_args)) == ["vendor"]
    write.assert_not_awaited()


async def test_a_rotation_refuses_a_reader_whose_other_key_changed_during_the_check():
    """The check proved the row with its second key B; an edit to C since is
    a different server, though both read the rotated key the same way."""
    row = catalog_row(
        "vendor", "https://vendor.example.com/mcp",
        headers={
            "X-Api-Key": "${vault:VENDOR_KEY}", "X-Account": "${vault:ACCOUNT_B}",
        },
    )
    edited = {
        **row,
        "headers": {
            "X-Api-Key": "${vault:VENDOR_KEY}", "X-Account": "${vault:ACCOUNT_C}",
        },
    }
    result, _check, write = await _rotate(
        check_results(passed=["vendor"]), [row], rows_under_lock=[edited]
    )
    assert isinstance(result, ValueError)
    assert "vendor changed while" in str(result)
    write.assert_not_awaited()
