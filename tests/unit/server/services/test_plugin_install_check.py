"""An install writes nothing until every server it would write passed.

The check dials each server as it would be saved, with the values the request
supplied standing in for the vault; no secret, plugin row or server lands
when any of them does not pass.
"""

from __future__ import annotations

from contextlib import ExitStack, nullcontext
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_validation import (
    CheckResult,
    McpCheckFailed,
    McpSignInRequired,
)
from src.server.services.plugins import lifecycle, supplied_secrets
from src.server.services.plugins.checks import CheckedServers, dial_signature
from src.server.services.plugins.errors import PluginRefused, PluginRejected
from src.server.services.plugins.grants import BindGrants
from tests.unit.server.mcp_builders import check_results, checked_servers
from tests.unit.server.plugin_builders import (
    KEYED,
    KEYED_ROW,
    OTHER,
    PLUGIN_ID,
    REFERENCING,
    USER,
    VENDOR,
    catalog_row,
    plugin_package,
)

pytestmark = pytest.mark.asyncio

L = "src.server.services.plugins.lifecycle."
G = "src.server.services.plugins.gate."


def _install_env(
    stack: ExitStack, *, held=(), outcome: list[CheckResult] | None = None
):
    """Patch the install's reads and writes; return the mocks a test asserts on."""
    check = AsyncMock(return_value=outcome or [])
    mocks = {
        "create_plugin": AsyncMock(return_value={"user_plugin_id": PLUGIN_ID, "name": "demo"}),
        "fan_out_servers": AsyncMock(),
        "fan_out_skills": AsyncMock(),
        "install_subagents": AsyncMock(),
        "claim_plugin_secrets": AsyncMock(),
        "after_secrets_changed": AsyncMock(),
        "write_supplied": AsyncMock(return_value=([], [])),
        "stamp_plugin_content_hash": AsyncMock(return_value=None),
    }
    stack.enter_context(patch(L + "bundled_names", return_value=frozenset()))
    stack.enter_context(patch(L + "plugin_fan_out_lock", lambda _: nullcontext()))
    stack.enter_context(patch(L + "get_plugin", new=AsyncMock(return_value=None)))
    stack.enter_context(patch(L + "list_plugins", new=AsyncMock(return_value=[])))
    stack.enter_context(patch(L + "list_catalog_servers", new=AsyncMock(return_value=[])))
    stack.enter_context(patch(L + "list_user_skills", new=AsyncMock(return_value=[])))
    stack.enter_context(
        patch(L + "get_user_secret_names", new=AsyncMock(return_value=list(held)))
    )
    stack.enter_context(patch(G + "check_servers", new=check))
    stack.enter_context(patch(G + "connected_bearers", new=AsyncMock(return_value={})))
    stack.enter_context(
        patch(
            G + "resolve_bind_grants",
            new=AsyncMock(
                side_effect=lambda _u, ext, plugin_id: BindGrants(
                    granted=frozenset(s.name for s in ext.secrets),
                    held=frozenset(held),
                )
            ),
        )
    )
    stack.enter_context(
        patch(L + "pending_secret_declarations", new=AsyncMock(return_value=[]))
    )
    stack.enter_context(
        patch(
            "src.server.services.plugins.checks.reserved_catalog_names",
            return_value=set(),
        )
    )
    for name, mock in mocks.items():
        stack.enter_context(patch(L + name, new=mock))
    return {"check_servers": check, **mocks}


async def _install(package, **kw):
    return await lifecycle.install_plugin_package(
        USER, package, source_type="zip", source_ref=None, **kw
    )


async def test_a_declared_key_with_no_value_is_asked_for_before_anything_is_dialled():
    package = plugin_package(VENDOR, secrets=KEYED)
    with ExitStack() as stack:
        mocks = _install_env(stack)
        with pytest.raises(PluginRefused) as caught:
            await _install(package)
    assert caught.value.detail() == {
        "code": "plugin_secrets_required",
        "message": caught.value.detail()["message"],
        "secrets": [{
            "name": "VENDOR_KEY",
            "label": "Vendor key",
            "description": "From the vendor console.",
        }],
    }
    mocks["check_servers"].assert_not_awaited()
    mocks["create_plugin"].assert_not_awaited()
    mocks["write_supplied"].assert_not_awaited()


async def test_supplied_values_reach_the_check_and_not_the_vault_when_it_fails():
    package = plugin_package({**VENDOR, **OTHER}, secrets=KEYED)
    with ExitStack() as stack:
        mocks = _install_env(
            stack, outcome=check_results(passed=["other"], failed=["vendor"])
        )
        with pytest.raises(McpCheckFailed) as caught:
            await _install(package, secrets={"VENDOR_KEY": "sk-test-value"})
    assert [f.server for f in caught.value.failures] == ["vendor"]
    checked = mocks["check_servers"].await_args
    assert checked_servers(checked) == {
        "other": {"VENDOR_KEY": "sk-test-value"},
        "vendor": {"VENDOR_KEY": "sk-test-value"},
    }
    vendor = next(s for s, _extras in checked.args[1] if s.name == "vendor")
    # Checked as it would be saved: the bind materialized into a vault ref.
    assert vendor.headers == {"X-Api-Key": "${vault:VENDOR_KEY}"}
    mocks["write_supplied"].assert_not_awaited()
    mocks["create_plugin"].assert_not_awaited()
    mocks["fan_out_servers"].assert_not_awaited()


async def test_a_passing_install_writes_the_values_then_lands_what_was_checked():
    package = plugin_package(VENDOR, secrets=KEYED)
    with ExitStack() as stack:
        mocks = _install_env(stack, outcome=check_results(passed=["vendor"]))
        mocks["write_supplied"].return_value = (["VENDOR_KEY"], ["VENDOR_KEY"])
        await _install(package, secrets={"VENDOR_KEY": "sk-test-value"})
    mocks["write_supplied"].assert_awaited_once()
    mocks["claim_plugin_secrets"].assert_awaited_once_with(USER, PLUGIN_ID, ["VENDOR_KEY"])
    checked = mocks["fan_out_servers"].await_args.kwargs["checked"]
    assert checked.approves(
        McpServerInput(
            name="vendor", transport="http", url="https://vendor.example.com/mcp",
            headers={"X-Api-Key": "${vault:VENDOR_KEY}"},
        )
    )
    mocks["after_secrets_changed"].assert_awaited_once_with(USER, ["VENDOR_KEY"])


async def test_one_sign_in_is_a_409_for_the_client_to_start():
    package = plugin_package({**VENDOR, **OTHER})
    with ExitStack() as stack:
        mocks = _install_env(stack, outcome=check_results(passed=["other"], sign_in=["vendor"]))
        with pytest.raises(McpSignInRequired):
            await _install(package)
    mocks["create_plugin"].assert_not_awaited()


async def test_a_failure_outranks_a_sign_in():
    package = plugin_package({**VENDOR, **OTHER})
    with ExitStack() as stack:
        _install_env(stack, outcome=check_results(failed=["other"], sign_in=["vendor"]))
        with pytest.raises(McpCheckFailed):
            await _install(package)


async def test_more_than_one_sign_in_is_refused_even_for_a_pending_sign_in():
    package = plugin_package({**VENDOR, **OTHER})
    with ExitStack() as stack:
        _install_env(stack, outcome=check_results(sign_in=["other", "vendor"]))
        with pytest.raises(PluginRefused) as caught:
            await lifecycle.check_plugin_install(USER, package, allow_sign_in=True)
    assert caught.value.code == "plugin_sign_in_unsupported"
    assert [s["server"] for s in caught.value.fields["servers"]] == ["other", "vendor"]


async def test_a_pending_sign_in_gets_its_server_back_approved():
    package = plugin_package({**VENDOR, **OTHER})
    with ExitStack() as stack:
        _install_env(stack, outcome=check_results(passed=["other"], sign_in=["vendor"]))
        result = await lifecycle.check_plugin_install(USER, package, allow_sign_in=True)
    assert result.sign_in.server == "vendor"
    assert result.checked.approves(
        McpServerInput(name="vendor", transport="http", url="https://vendor.example.com/mcp")
    )


async def test_a_name_the_plugin_neither_declares_nor_references_is_not_a_vault_write():
    package = plugin_package({**VENDOR, **REFERENCING}, secrets=KEYED)
    with ExitStack() as stack:
        mocks = _install_env(stack)
        with pytest.raises(PluginRejected, match="not asked for by this plugin: OTHER"):
            await _install(
                package, secrets={"VENDOR_KEY": "v", "FOO_TOKEN": "f", "OTHER": "x"}
            )
    mocks["check_servers"].assert_not_awaited()
    mocks["write_supplied"].assert_not_awaited()


@pytest.mark.parametrize("passes", [True, False])
async def test_a_referenced_key_the_plugin_does_not_declare_is_written_only_after_its_check(
    passes: bool,
):
    package = plugin_package({**VENDOR, **REFERENCING}, secrets=KEYED)
    supplied = {"VENDOR_KEY": "sk-test-value", "FOO_TOKEN": "foo-test-value"}
    outcome = (
        check_results(passed=["vendor", "foo"])
        if passes
        else check_results(passed=["vendor"], failed=["foo"])
    )
    order: list[str] = []
    with ExitStack() as stack:
        mocks = _install_env(stack)
        mocks["check_servers"].side_effect = lambda *_a, **_k: (
            order.append("check") or outcome
        )
        mocks["write_supplied"].side_effect = lambda *_a, **_k: (
            order.append("write") or (list(supplied), list(supplied))
        )
        if passes:
            await _install(package, secrets=supplied)
        else:
            with pytest.raises(McpCheckFailed):
                await _install(package, secrets=supplied)
    # The check resolves it from the request, as it does a declared key.
    assert checked_servers(mocks["check_servers"].await_args) == {
        "vendor": supplied,
        "foo": supplied,
    }
    if passes:
        assert order == ["check", "write"]
        assert mocks["write_supplied"].await_args.args[3] == supplied
        mocks["claim_plugin_secrets"].assert_awaited_once_with(
            USER, PLUGIN_ID, list(supplied)
        )
    else:
        assert order == ["check"]
        mocks["write_supplied"].assert_not_awaited()
        mocks["create_plugin"].assert_not_awaited()


async def test_a_referenced_key_lands_with_the_plugin_s_fallback_description():
    package = plugin_package(REFERENCING)
    create = AsyncMock()
    with patch.object(supplied_secrets, "create_user_secret", create):
        written, introduced = await supplied_secrets.write_supplied(
            USER, "demo", package.extension, {"FOO_TOKEN": "foo-test-value"}, held=()
        )
    assert written == introduced == ["FOO_TOKEN"]
    create.assert_awaited_once_with(
        USER, "FOO_TOKEN", "foo-test-value", "Required by plugin demo"
    )


async def test_a_value_for_a_name_held_for_something_else_is_refused():
    package = plugin_package(VENDOR, secrets=KEYED)
    with ExitStack() as stack:
        mocks = _install_env(stack, held=["VENDOR_KEY"])
        with pytest.raises(PluginRejected, match="already holds VENDOR_KEY"):
            await _install(package, secrets={"VENDOR_KEY": "sk-test-value"})
    mocks["check_servers"].assert_not_awaited()
    mocks["write_supplied"].assert_not_awaited()


async def test_an_install_past_the_server_cap_is_refused_before_any_check():
    package = plugin_package({**VENDOR, **OTHER})
    with ExitStack() as stack:
        mocks = _install_env(stack)
        stack.enter_context(patch(L + "MAX_CATALOG_SERVERS_PER_USER", 1))
        with pytest.raises(ValueError, match="adds 2 servers"):
            await _install(package)
    mocks["check_servers"].assert_not_awaited()


async def test_an_install_checks_a_row_already_waiting_on_a_key_it_supplies():
    package = plugin_package(VENDOR, secrets=KEYED)
    hand_made = catalog_row(
        "hand", "https://hand.example.com/mcp", headers=KEYED_ROW,
        plugin_id=None, plugin_server_key=None,
    )
    with ExitStack() as stack:
        mocks = _install_env(stack, outcome=check_results(passed=["hand", "vendor"]))
        stack.enter_context(
            patch(L + "list_catalog_servers", new=AsyncMock(return_value=[hand_made]))
        )
        await _install(package, secrets={"VENDOR_KEY": "sk-test-value"})
    # The value the install writes reaches that row too, so it is checked with it.
    assert checked_servers(mocks["check_servers"].await_args)["hand"] == {
        "VENDOR_KEY": "sk-test-value"
    }
    mocks["write_supplied"].assert_awaited_once()


async def test_an_install_counts_the_inline_credentials_it_will_vault():
    package = plugin_package({
        "vendor": {
            "type": "streamable-http",
            "url": "https://vendor.example.com/mcp",
            "headers": {"Authorization": "Bearer sk-live-abcdef0123456789"},
        }
    })
    with ExitStack() as stack:
        mocks = _install_env(stack, held=["ALREADY_THERE"])
        stack.enter_context(patch.object(supplied_secrets, "MAX_SECRETS_PER_USER", 1))
        with pytest.raises(ValueError, match="Maximum of 1 secrets"):
            await _install(package)
    mocks["check_servers"].assert_not_awaited()


async def test_a_stand_in_for_a_key_is_asked_for_and_lands_under_one_name():
    package = plugin_package({
        "vendor": {
            "type": "streamable-http",
            "url": "https://vendor.example.com/mcp",
            "headers": {"Authorization": "Bearer <your-api-key>"},
        }
    })
    # The key alone: the scheme the docs wrote stays in the header.
    supplied = {"VENDOR_AUTHORIZATION": "sk-test-value"}
    with ExitStack() as stack:
        mocks = _install_env(stack, outcome=check_results(passed=["vendor"]))
        await _install(package, secrets=supplied)
    assert checked_servers(mocks["check_servers"].await_args) == {"vendor": supplied}
    assert mocks["write_supplied"].await_args.args[3] == supplied
    assert mocks["fan_out_servers"].await_args.kwargs["checked"].approves(
        McpServerInput(
            name="vendor", transport="http", url="https://vendor.example.com/mcp",
            headers={"Authorization": "Bearer ${vault:VENDOR_AUTHORIZATION}"},
        )
    )


async def test_an_install_reports_its_row_before_the_steps_that_can_still_fail():
    package = plugin_package(VENDOR)
    progress = lifecycle.InstallProgress()
    with ExitStack() as stack:
        mocks = _install_env(stack, outcome=check_results(passed=["vendor"]))
        mocks["write_supplied"].return_value = ([], ["VENDOR_AUTHORIZATION"])
        mocks["fan_out_skills"].side_effect = RuntimeError("archive store down")
        with pytest.raises(RuntimeError):
            await _install(package, progress=progress)
    assert progress.plugin == {"user_plugin_id": PLUGIN_ID, "name": "demo"}
    assert progress.introduced == ["VENDOR_AUTHORIZATION"]
    assert progress.report is not None


# --- the write proves itself against the check -----------------------------


def test_a_signature_ignores_which_name_an_inline_credential_was_vaulted_under():
    def server(ref: str) -> McpServerInput:
        return McpServerInput(
            name="vendor", transport="http", url="https://vendor.example.com/mcp",
            headers={"Authorization": f"Bearer ${{vault:{ref}}}"},
        )

    assert dial_signature(server("VENDOR_AUTHORIZATION")) == dial_signature(
        server("VENDOR_AUTHORIZATION_2")
    )
    moved = McpServerInput(
        name="vendor", transport="http", url="https://elsewhere.example.com/mcp",
        headers={"Authorization": "Bearer ${vault:VENDOR_AUTHORIZATION}"},
    )
    assert not CheckedServers.of([server("VENDOR_AUTHORIZATION")]).approves(moved)
