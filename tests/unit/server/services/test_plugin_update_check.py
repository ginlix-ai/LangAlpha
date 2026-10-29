"""An update or sse upgrade dials only what it changes, and lands only what passed.

A server whose dial moved, a new one, and any server that reads a value the
request supplied are checked before the lock; the write re-asks its refusals
under it, and each arm lands only a server the check approved.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, nullcontext
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.mcp_server import McpServerInput
from src.server.models.plugin import InstallReport
from src.server.services.mcp_validation import McpCheckFailed
from src.server.services.plugins import gate, post_install, update
from src.server.services.plugins.checks import CheckedServers
from src.server.services.plugins.errors import PluginRefused, PluginRejected
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.grants import BindGrants
from tests.unit.server.mcp_builders import check_results, checked_servers
from tests.unit.server.plugin_builders import (
    KEYED,
    KEYED_ROW,
    MCP_SCHEMA,
    OTHER,
    PLUGIN_ID,
    PLUGIN_SCHEMA,
    USER,
    VENDOR,
    FanOutLock,
    catalog_row,
    plugin_package,
)

pytestmark = pytest.mark.asyncio

INSTALLED_AT = "2026-01-01T00:00:00+00:00"


async def _check_update(package, rows, *, outcome=None, held=(), supplied=None):
    # The update materializes binds before it checks.
    materialize_binds(
        package.extension, package.entry_plans,
        {s.name for s in package.extension.secrets},
    )
    check = AsyncMock(return_value=outcome or [])
    with (
        patch.object(update, "list_catalog_servers", AsyncMock(return_value=rows)),
        patch.object(gate, "check_servers", check),
        patch.object(gate, "connected_bearers", AsyncMock(return_value={})),
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        ),
    ):
        checked = await update._check_update(
            USER, PLUGIN_ID, package, supplied or {},
            grants=BindGrants(held=frozenset(held), already=frozenset(held)),
        )
    return checked, check


async def test_an_update_that_moves_nothing_dials_nothing():
    package = plugin_package(VENDOR)
    checked, check = await _check_update(
        package, [catalog_row("vendor", "https://vendor.example.com/mcp")]
    )
    check.assert_not_awaited()
    assert checked == CheckedServers()


async def test_an_update_checks_a_moved_server_and_a_new_one():
    package = plugin_package(
        {"vendor": {"type": "streamable-http", "url": "https://vendor.example.com/v2/mcp"}, **OTHER},
        version="2.0.0",
    )
    checked, check = await _check_update(
        package,
        [catalog_row("vendor", "https://vendor.example.com/mcp")],
        outcome=check_results(passed=["vendor", "other"]),
    )
    assert sorted(checked_servers(check.await_args)) == ["other", "vendor"]
    assert checked.approves(
        McpServerInput(name="vendor", transport="http", url="https://vendor.example.com/v2/mcp")
    )


async def test_an_update_that_fails_a_check_raises_with_every_failure():
    package = plugin_package(
        {"vendor": {"type": "streamable-http", "url": "https://vendor.example.com/v2/mcp"}, **OTHER},
        version="2.0.0",
    )
    with pytest.raises(McpCheckFailed) as caught:
        await _check_update(
            package,
            [catalog_row("vendor", "https://vendor.example.com/mcp")],
            outcome=check_results(failed=["other", "vendor"]),
        )
    assert [f.server for f in caught.value.failures] == ["other", "vendor"]


async def test_an_update_that_needs_a_new_sign_in_is_refused():
    package = plugin_package({**VENDOR, **OTHER}, version="2.0.0")
    with pytest.raises(PluginRefused) as caught:
        await _check_update(
            package,
            [catalog_row("vendor", "https://vendor.example.com/mcp")],
            outcome=check_results(sign_in=["other"]),
        )
    assert caught.value.code == "plugin_sign_in_unsupported"
    assert "Remove the plugin and install it again" in str(caught.value)


async def test_an_update_asks_only_for_keys_its_changed_servers_use():
    secrets = [*KEYED, {
        "name": "OTHER_KEY", "label": "Other key",
        "bind": [{"server": "other", "header": "X-Api-Key"}],
    }]
    package = plugin_package({**VENDOR, **OTHER}, secrets=secrets, version="2.0.0")
    with pytest.raises(PluginRefused) as caught:
        await _check_update(
            package,
            # vendor is already installed and its row already carries its key.
            [catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW)],
        )
    assert [s["name"] for s in caught.value.fields["secrets"]] == ["OTHER_KEY"]


async def test_the_in_place_arm_refuses_a_moved_server_its_check_did_not_pass():
    package = plugin_package(
        {"vendor": {"type": "streamable-http", "url": "https://vendor.example.com/v2/mcp"}},
        version="2.0.0",
    )
    edit = AsyncMock()
    report = InstallReport()
    with (
        patch.object(update, "list_plugin_server_names", AsyncMock(
            return_value=[{"name": "vendor", "plugin_server_key": "vendor"}]
        )),
        patch.object(update, "get_catalog_server", AsyncMock(
            return_value=catalog_row("vendor", "https://vendor.example.com/mcp")
        )),
        patch.object(update, "get_user_secret_names", AsyncMock(return_value=[])),
        patch.object(update, "apply_catalog_edit", edit),
    ):
        await update._update_servers(
            USER, PLUGIN_ID, package, report,
            checked=CheckedServers(), installed_at=INSTALLED_AT,
        )
    assert [(c.key, c.status) for c in report.components] == [("vendor", "error")]
    edit.assert_not_awaited()


# --- sse upgrade -----------------------------------------------------------


async def test_an_upgrade_reports_the_entries_that_failed_and_lands_the_rest():
    plugin = {
        "user_plugin_id": PLUGIN_ID, "name": "demo",
        "manifest": {"$schema": PLUGIN_SCHEMA, "name": "demo"},
        "mcp_document": {
            "$schema": MCP_SCHEMA,
            "mcpServers": {
                "good": {"type": "sse", "url": "https://good.example.com/sse"},
                "bad": {"type": "sse", "url": "https://bad.example.com/sse"},
            },
        },
    }
    fan_out = AsyncMock()
    relink = AsyncMock()
    with (
        patch.object(post_install, "list_catalog_servers", AsyncMock(return_value=[])),
        patch.object(post_install, "get_user_secret_names", AsyncMock(return_value=[])),
        patch.object(post_install, "resolve_bind_grants", AsyncMock(return_value=BindGrants())),
        patch.object(gate, "check_servers", AsyncMock(
            return_value=check_results(passed=["good"], failed=["bad"])
        )),
        patch.object(post_install, "plugin_fan_out_lock", lambda _: nullcontext()),
        patch.object(gate, "get_plugin", AsyncMock(return_value=plugin)),
        patch.object(post_install, "fan_out_servers", fan_out),
        patch.object(post_install, "relink_subagents", relink),
        patch.object(post_install, "after_secrets_changed", AsyncMock()),
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        ),
    ):
        report = await post_install.apply_sse_upgrades(USER, plugin, ["good", "bad"])
    assert [(c.key, c.status) for c in report.components] == [("bad", "error")]
    assert "did not pass its check: bad key" in report.components[0].reason
    landed = fan_out.await_args.args[2]
    assert [p.key for p in landed] == ["good"]
    assert fan_out.await_args.kwargs["checked"].approves(
        McpServerInput(name="good", transport="http", url="https://good.example.com/sse")
    )
    # The plugin's subagents that prefer a server that just landed link to it.
    relink.assert_awaited_once_with(USER, PLUGIN_ID, "demo")


# --- every server that reads a supplied value is checked -------------------


async def test_an_update_checks_an_unmoved_server_that_reads_a_supplied_key():
    package = plugin_package(VENDOR, secrets=KEYED)
    row = catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW)
    with pytest.raises(McpCheckFailed):
        await _check_update(
            package, [row], outcome=check_results(failed=["vendor"]),
            held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "bogus"},
        )
    checked, check = await _check_update(
        package, [row], outcome=check_results(passed=["vendor"]),
        held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "sk-new-value"},
    )
    assert checked_servers(check.await_args) == {
        "vendor": {"VENDOR_KEY": "sk-new-value"}
    }
    assert checked.approves(
        McpServerInput(
            name="vendor", transport="http", url="https://vendor.example.com/mcp",
            headers=KEYED_ROW,
        )
    )


async def test_an_update_that_moves_one_server_still_checks_the_one_reading_the_key():
    package = plugin_package(
        {**VENDOR, "other": {"type": "streamable-http", "url": "https://other.example.com/v2/mcp"}},
        secrets=KEYED, version="2.0.0",
    )
    rows = [
        catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW),
        catalog_row("other", "https://other.example.com/mcp"),
    ]
    _checked, check = await _check_update(
        package, rows, outcome=check_results(passed=["other", "vendor"]),
        held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "sk-new-value"},
    )
    assert sorted(checked_servers(check.await_args)) == ["other", "vendor"]


async def test_an_update_checks_a_fork_of_its_server_that_reads_a_supplied_key():
    package = plugin_package(OTHER, secrets=[{**KEYED[0], "bind": []}])
    fork = catalog_row(
        "vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW,
        plugin_id=None, plugin_server_key=None,
    )
    owned = catalog_row("other", "https://other.example.com/mcp")
    _checked, check = await _check_update(
        package, [fork, owned], outcome=check_results(passed=["vendor"]),
        held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "sk-new-value"},
    )
    assert list(checked_servers(check.await_args)) == ["vendor"]


async def test_an_unmoved_server_answering_a_supplied_key_with_a_sign_in_fails():
    package = plugin_package(VENDOR, secrets=KEYED)
    row = catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW)
    with pytest.raises(McpCheckFailed) as caught:
        await _check_update(
            package, [row], outcome=check_results(sign_in=["vendor"]),
            held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "sk-new-value"},
        )
    assert caught.value.failures[0].verdict == "needs_credential"


async def test_a_moved_server_keeps_the_key_it_is_given_and_asks_for_it_otherwise():
    package = plugin_package(
        {"vendor": {"type": "streamable-http", "url": "https://vendor.example.com/v2/mcp"}},
        secrets=KEYED, version="2.0.0",
    )
    row = catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW)
    # The move strips the key it was given for the old address.
    with pytest.raises(PluginRefused) as caught:
        await _check_update(package, [row], held=["VENDOR_KEY"])
    assert caught.value.code == "plugin_secrets_required"
    assert [s["name"] for s in caught.value.fields["secrets"]] == ["VENDOR_KEY"]
    # A value in the request is the user handing it to the new one.
    checked, check = await _check_update(
        package, [row], outcome=check_results(passed=["vendor"]),
        held=["VENDOR_KEY"], supplied={"VENDOR_KEY": "sk-new-value"},
    )
    assert checked_servers(check.await_args) == {
        "vendor": {"VENDOR_KEY": "sk-new-value"}
    }
    assert checked.approves(
        McpServerInput(
            name="vendor", transport="http", url="https://vendor.example.com/v2/mcp",
            headers=KEYED_ROW,
        )
    )


# --- what is checked is what the reconciler creates ------------------------


BRAVE_URL = "https://brave.example.com/mcp"


async def test_a_key_renamed_onto_its_old_row_name_is_checked_for_its_recreate():
    # v1 key brave-search landed as row brave_search; v2 ships key brave_search.
    package = plugin_package(
        {"brave_search": {"type": "streamable-http", "url": BRAVE_URL}},
        version="2.0.0",
    )
    row = catalog_row("brave_search", BRAVE_URL, plugin_server_key="brave-search")
    checked, check = await _check_update(
        package, [row], outcome=check_results(passed=["brave_search"])
    )
    assert list(checked_servers(check.await_args)) == ["brave_search"]
    # The fan-out's persist asks exactly this of the row it creates.
    assert checked.approves(
        McpServerInput(name="brave_search", transport="http", url=BRAVE_URL)
    )


async def test_a_renamed_key_that_needs_a_sign_in_is_refused_before_the_delete():
    package = plugin_package(
        {"brave_search": {"type": "streamable-http", "url": BRAVE_URL}},
        version="2.0.0",
    )
    row = catalog_row("brave_search", BRAVE_URL, plugin_server_key="brave-search")
    bearers = AsyncMock(return_value={})
    with (
        patch.object(update, "list_catalog_servers", AsyncMock(return_value=[row])),
        patch.object(gate, "check_servers", AsyncMock(
            return_value=check_results(sign_in=["brave_search"])
        )),
        patch.object(gate, "connected_bearers", bearers),
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        ),
        pytest.raises(PluginRefused) as caught,
    ):
        await update._check_update(
            USER, PLUGIN_ID, package, {}, grants=BindGrants()
        )
    assert caught.value.code == "plugin_sign_in_unsupported"
    # The row it replaces is about to be deleted, so its sign-in is not lent.
    assert bearers.await_args.args[1] == []


async def test_a_new_server_that_fits_only_once_a_dropped_one_is_gone_is_checked():
    package = plugin_package(OTHER, version="2.0.0")
    owned = catalog_row("vendor", "https://vendor.example.com/mcp")
    others = [
        catalog_row(f"hand_{i}", f"https://h{i}.example.com/mcp", plugin_id=None)
        for i in range(update.MAX_CATALOG_SERVERS_PER_USER - 1)
    ]
    checked, check = await _check_update(
        package, [owned, *others], outcome=check_results(passed=["other"])
    )
    assert list(checked_servers(check.await_args)) == ["other"]
    assert checked.approves(
        McpServerInput(name="other", transport="http", url="https://other.example.com/mcp")
    )


async def test_the_create_arm_lands_a_renamed_key_the_check_passed():
    package = plugin_package(
        {"brave_search": {"type": "streamable-http", "url": BRAVE_URL}},
        version="2.0.0",
    )
    row = catalog_row("brave_search", BRAVE_URL, plugin_server_key="brave-search")
    checked, _check = await _check_update(
        package, [row], outcome=check_results(passed=["brave_search"])
    )
    report = InstallReport()
    catalog = {"brave_search": row}

    async def delete(_user, name, owned_by_plugin=None, **_kw):
        return catalog.pop(name, None) is not None

    async def get(_user, name, **_kw):
        return catalog.get(name)

    created = AsyncMock()

    @asynccontextmanager
    async def connection():
        class _Conn:
            @asynccontextmanager
            async def transaction(self):
                yield

        yield _Conn()

    with (
        patch("src.server.services.mcp_import.get_db_connection", connection),
        patch(
            "src.server.services.mcp_import.get_user_secret_names",
            AsyncMock(return_value=[]),
        ),
        patch.object(update, "list_plugin_server_names", AsyncMock(
            return_value=[{"name": "brave_search", "plugin_server_key": "brave-search"}]
        )),
        patch.object(update, "get_catalog_server", AsyncMock(side_effect=get)),
        patch.object(update, "delete_catalog_server", AsyncMock(side_effect=delete)),
        patch.object(update, "oauth_fence", lambda *_a: nullcontext()),
        patch.object(update, "get_user_secret_names", AsyncMock(return_value=[])),
        patch(
            "src.server.services.plugins.server_fanout.list_catalog_servers",
            AsyncMock(side_effect=lambda _u: list(catalog.values())),
        ),
        patch(
            "src.server.services.plugins.server_fanout.create_catalog_server", created
        ),
        patch(
            "src.server.services.plugins.server_fanout.schedule_catalog_discovery"
        ),
        patch(
            "src.server.services.mcp_import.reserved_catalog_names",
            return_value=set(),
        ),
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        ),
    ):
        await update._update_servers(
            USER, PLUGIN_ID, package, report,
            checked=checked, installed_at=INSTALLED_AT,
        )
    assert [(c.key, c.status) for c in report.components] == [
        ("brave-search", "deleted"),
        ("brave_search", "created"),
    ]
    created.assert_awaited_once()


# --- the write re-asks its refusals under the lock -------------------------


async def _update_with_secrets(
    package, *, grants: list[BindGrants], rows: list[list[dict]], outcome=None
):
    materialize_binds(
        package.extension, package.entry_plans,
        {s.name for s in package.extension.secrets},
    )
    lock = FanOutLock()
    write = AsyncMock(return_value=(["VENDOR_KEY"], []))
    plugin = {
        "user_plugin_id": PLUGIN_ID, "name": "demo", "installed_at": INSTALLED_AT,
    }
    with (
        patch.object(update, "get_user_secret_names", AsyncMock(return_value=[])),
        patch.object(gate, "resolve_bind_grants", AsyncMock(side_effect=grants)),
        patch.object(update, "materialize_binds"),
        patch.object(update, "list_catalog_servers", AsyncMock(side_effect=rows)),
        patch.object(gate, "check_servers", AsyncMock(
            return_value=outcome or []
        )),
        patch.object(gate, "connected_bearers", AsyncMock(return_value={})),
        patch.object(update, "plugin_fan_out_lock", lock),
        patch.object(gate, "get_plugin", AsyncMock(return_value=plugin)),
        patch.object(update, "write_supplied", write),
        patch.object(update, "claim_plugin_secrets", AsyncMock()),
        patch.object(update, "_update_servers", AsyncMock()),
        patch.object(update, "_update_skills", AsyncMock()),
        patch.object(update, "reconcile_subagents", AsyncMock()),
        patch.object(update, "update_plugin_row", AsyncMock(return_value=plugin)),
        patch.object(update, "after_secrets_changed", AsyncMock()),
        patch.object(update, "pending_secret_declarations", AsyncMock(return_value=[])),
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        ),
    ):
        try:
            await update.update_plugin_package(
                USER, plugin, package, source_ref=None,
                secrets={"VENDOR_KEY": "sk-new-value"},
            )
        except (PluginRejected, ValueError) as e:
            return e, write
    return None, write


async def test_an_update_does_not_overwrite_a_value_created_during_its_check():
    package = plugin_package(VENDOR, secrets=KEYED, version="2.0.0")
    before = BindGrants(granted=frozenset({"VENDOR_KEY"}))
    after = BindGrants(granted=frozenset({"VENDOR_KEY"}), held=frozenset({"VENDOR_KEY"}))
    result, write = await _update_with_secrets(
        package, grants=[before, after], rows=[[], []],
        outcome=check_results(passed=["vendor"]),
    )
    assert isinstance(result, PluginRejected)
    assert "already holds VENDOR_KEY" in str(result)
    write.assert_not_awaited()


async def test_an_update_refuses_a_reader_of_its_values_that_appeared_under_it():
    package = plugin_package(OTHER, secrets=[{**KEYED[0], "bind": []}], version="2.0.0")
    grants = BindGrants(
        granted=frozenset({"VENDOR_KEY"}),
        held=frozenset({"VENDOR_KEY"}),
        already=frozenset({"VENDOR_KEY"}),
    )
    owned = catalog_row("other", "https://other.example.com/mcp")
    added = catalog_row(
        "added", "https://added.example.com/mcp", headers=KEYED_ROW, plugin_id=None
    )
    result, write = await _update_with_secrets(
        package, grants=[grants, grants], rows=[[owned], [owned, added]],
    )
    assert isinstance(result, ValueError)
    assert "added changed while" in str(result)
    write.assert_not_awaited()


async def test_an_update_writes_its_values_once_every_reader_passed():
    package = plugin_package(VENDOR, secrets=KEYED, version="2.0.0")
    grants = BindGrants(
        granted=frozenset({"VENDOR_KEY"}),
        held=frozenset({"VENDOR_KEY"}),
        already=frozenset({"VENDOR_KEY"}),
    )
    row = catalog_row("vendor", "https://vendor.example.com/mcp", headers=KEYED_ROW)
    result, write = await _update_with_secrets(
        package, grants=[grants, grants], rows=[[row], [row]],
        outcome=check_results(passed=["vendor"]),
    )
    assert result is None
    write.assert_awaited_once()
