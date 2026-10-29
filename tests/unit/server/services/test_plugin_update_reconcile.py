"""Update-path reconciliation invariants (services/plugins/update.py).

Install and update reach the same rows by different roads: install goes through
run_mcp_import, update writes through apply_catalog_edit. Anything the import
loop does on the way in, the update arm owes too — these pin the two places
where the roads had diverged, both of which cost the user something real (a
plaintext credential, a deleted skill).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.plugin import InstallReport
from src.server.services.plugins import validate_package
from src.server.services.plugins.extension import materialize_binds
from src.server.services.plugins.update import (
    _preserve_vault_refs,
    _update_servers,
    _update_skills,
)
from tests.unit.server.mcp_builders import zip_package

USER = "test-user-123"
PLUGIN_ID = "22222222-2222-2222-2222-222222222222"
CANONICAL_PLUGIN_SCHEMA = (
    "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
)
CANONICAL_MCP_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
SECRET = "sk-live-abcdef0123456789"
INSTALLED_AT = "2026-01-01T00:00:00+00:00"


class _Passed:
    """Every server passed its check; these tests are about the reconcile."""

    def approves(self, server) -> bool:
        return True


_PASSED = _Passed()


def _package(*, mcp: dict | None = None, skills: dict[str, str] | None = None):
    members = {
        "plugin.json": json.dumps({
            "$schema": CANONICAL_PLUGIN_SCHEMA,
            "name": "demo",
            "version": "2.0.0",
        }).encode(),
    }
    if mcp is not None:
        members["mcp.json"] = json.dumps(
            {"$schema": CANONICAL_MCP_SCHEMA, "mcpServers": mcp}
        ).encode()
    for directory, skill_md in (skills or {}).items():
        members[f"skills/{directory}/SKILL.md"] = skill_md.encode()
    return validate_package(zip_package(members))


@pytest.mark.asyncio
async def test_update_vaults_a_newly_introduced_credential():
    """A credential the package adds in THIS version must not be persisted as a
    literal. _preserve_vault_refs only defends refs the row already holds, so
    without extraction on this arm the plaintext lands in user_mcp_servers."""
    package = _package(mcp={
        "svc": {
            "type": "streamable-http",
            "url": "https://example.com/mcp",
            "headers": {"Authorization": f"Bearer {SECRET}"},
        }
    })
    existing = {
        "name": "svc",
        "transport": "http",
        "url": "https://example.com/mcp",
        "env": {},
        "headers": {},  # the previous version shipped no credential at all
        "plugin_id": PLUGIN_ID,
    }
    edit = AsyncMock(return_value={"name": "svc"})
    create_secret = AsyncMock()
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "svc", "plugin_server_key": "svc"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.create_user_secret",
            new=create_secret,
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )

    # The secret was created with the real value...
    create_secret.assert_awaited_once()
    assert SECRET in create_secret.await_args.args[2]
    # ...and what the row is rewritten to holds a reference, not the literal.
    written = edit.await_args.args[2]
    assert SECRET not in json.dumps(written)
    assert "${vault:" in written["headers"]["Authorization"]
    # The vault name reaches the caller, which is what fires the invalidation.
    assert report.secrets_created


@pytest.mark.asyncio
async def test_a_skill_renamed_without_moving_its_directory_keeps_the_row():
    """Install enforces name == directory, so this package is malformed rather
    than renamed. Deleting first and recreating after could only ever delete:
    the fan-out rejects the replacement on the same mismatch."""
    package = _package(skills={
        "foo": "---\nname: bar\ndescription: renamed in place\n---\n\nBody.\n"
    })
    delete = AsyncMock()
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_skill_names",
            new=AsyncMock(
                return_value=[{
                    "name": "foo",
                    "plugin_skill_dir": "foo",
                    "workspace_id": None,
                }]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_user_skill",
            new=AsyncMock(
                return_value={
                    "name": "foo",
                    "content_hash": "old",
                    "plugin_id": PLUGIN_ID,
                }
            ),
        ),
        patch(
            "src.server.services.plugins.update.delete_user_skill", new=delete
        ),
    ):
        await _update_skills(
            USER, PLUGIN_ID, package, report, installed_at=INSTALLED_AT
        )

    delete.assert_not_awaited()
    result = next(c for c in report.components if c.kind == "skill")
    assert result.status == "invalid"
    assert result.name == "foo"


@pytest.mark.asyncio
async def test_a_repointed_entry_does_not_inherit_the_old_endpoints_secret():
    """The v1-honest, v2-repointed attack. A ref is consent to send that
    credential to the host it was set up for, so moving the url drops it."""
    package = _package(mcp={
        "api": {
            "type": "streamable-http",
            "url": "https://attacker.example.com/mcp",
            "headers": {},
        }
    })
    edit = AsyncMock(return_value=None)
    report = InstallReport()
    existing = {
        "name": "api",
        "transport": "http",
        "url": "https://vendor.example.com/mcp",
        "headers": {"Authorization": "${vault:VENDOR_KEY}"},
        "env": {},
        "plugin_id": PLUGIN_ID,
    }
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "api", "plugin_server_key": "api"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )

    written = edit.await_args.args[2]
    assert "${vault:VENDOR_KEY}" not in json.dumps(written)
    assert any(d.code == "secret_not_carried" for d in report.diagnostics)


@pytest.mark.asyncio
async def test_a_customize_mid_update_is_never_overwritten():
    """The row stopped being the plugin's between the enumeration and the
    write; the package's version must not land on the user's fork."""
    package = _package(mcp={
        "api": {"type": "streamable-http", "url": "https://x.example/mcp"}
    })
    edit = AsyncMock(return_value=None)
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "api", "plugin_server_key": "api"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value={
                "name": "api", "transport": "http",
                "url": "https://x.example/mcp",
                "headers": {}, "env": {}, "plugin_id": None,
            }),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )

    edit.assert_not_awaited()
    assert any(c.status == "detached" for c in report.components)


_STDIO_ENTRY = {
    "svc": {
        "type": "stdio",
        "command": "uvx",
        "args": ["thing@1.0.0"],
        "env": {"REGION": "us-east-1"},
    }
}
# What that entry looks like once it is a row: the shape apply_catalog_edit
# would write, so an update reading it back has nothing left to do.
_SETTLED_ROW = {
    "name": "svc",
    "plugin_id": PLUGIN_ID,
    "transport": "stdio",
    "command": "uvx",
    "args": ["thing@1.0.0"],
    "url": None,
    "env": {"REGION": "us-east-1"},
    "headers": {},
    "description": "",
    "instruction": "",
    "tool_exposure_mode": "summary",
    "discovery_uses_secrets": False,
}


async def _reconcile_stdio(existing: dict, edit: AsyncMock) -> InstallReport:
    package = _package(mcp=_STDIO_ENTRY)
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "svc", "plugin_server_key": "svc"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )
    return report


@pytest.mark.asyncio
async def test_an_entry_that_did_not_move_is_not_rewritten():
    """Update rebuilds every config from scratch, so an entry nobody touched
    still arrives at the write looking brand new. Writing it anyway reported
    "updated" for every component of every update forever, and spent two
    workspace-wide MCP version bumps delivering a row identical to the one
    already there."""
    edit = AsyncMock(return_value={"name": "svc"})
    report = await _reconcile_stdio(dict(_SETTLED_ROW), edit)

    edit.assert_not_awaited()
    result = next(c for c in report.components if c.kind == "mcp")
    assert result.status == "unchanged"
    assert result.name == "svc"


@pytest.mark.asyncio
async def test_a_row_that_drifted_is_still_rewritten():
    """The other half of the same claim: the comparison has to detect a
    difference, not merely always answer "unchanged". A hand edit to a field
    the package owns is exactly what an update exists to put back."""
    edit = AsyncMock(return_value={"name": "svc"})
    report = await _reconcile_stdio(
        {**_SETTLED_ROW, "args": ["thing@0.9.0"]}, edit
    )

    assert edit.await_args.args[2]["args"] == ["thing@1.0.0"]
    result = next(c for c in report.components if c.kind == "mcp")
    assert result.status == "updated"


@pytest.mark.asyncio
async def test_a_row_under_a_since_reserved_name_still_updates():
    """The update arm writes under the row's own name, so a row installed
    before that name was reserved has to pass. Refusing it left delete and
    reinstall, which takes the OAuth connection, tool schemas, per-workspace
    switches and plugin ownership with the row."""
    package = _package(mcp={"class": _STDIO_ENTRY["svc"]})
    existing = {**_SETTLED_ROW, "name": "class", "args": ["thing@0.9.0"]}
    edit = AsyncMock(return_value={"name": "class"})
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "class", "plugin_server_key": "class"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )

    edit.assert_awaited_once()
    assert edit.await_args.args[1] == "class"
    result = next(c for c in report.components if c.kind == "mcp")
    assert (result.status, result.name) == ("updated", "class")


def _bound_package(header: str | dict | None, *, prefix: str | None = None):
    """A v2 package whose remote entry gets its Authorization from a bind
    (``header=None``) or ships one in the portable document."""
    entry: dict = {"type": "streamable-http", "url": "https://example.com/mcp"}
    if header is not None:
        entry["headers"] = {"Authorization": header}
    bind = {"server": "svc", "header": "Authorization"}
    if prefix is not None:
        bind["prefix"] = prefix
    return validate_package(zip_package({
        "plugin.json": json.dumps({
            "$schema": CANONICAL_PLUGIN_SCHEMA,
            "name": "demo",
            "version": "2.0.0",
            "extensions": {"ai.langalpha": {"secrets": [
                {"name": "SVC_KEY", "label": "Key", "bind": [bind]}
            ]}} if header is None else {},
        }).encode(),
        "mcp.json": json.dumps({
            "$schema": CANONICAL_MCP_SCHEMA, "mcpServers": {"svc": entry},
        }).encode(),
    }))


async def _written_authorization(package, stored: str, granted: set[str]) -> str:
    materialize_binds(package.extension, package.entry_plans, granted)
    existing = {
        "name": "svc",
        "transport": "http",
        "url": "https://example.com/mcp",
        "env": {},
        "headers": {"Authorization": stored},
        "plugin_id": PLUGIN_ID,
    }
    edit = AsyncMock(return_value={"name": "svc"})
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(
                return_value=[{"name": "svc", "plugin_server_key": "svc"}]
            ),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value=existing),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=["SVC_KEY", "OLD_KEY"]),
        ),
        patch(
            "src.server.services.plugins.update.create_user_secret",
            new=AsyncMock(),
        ),
        patch(
            "src.server.services.plugins.update.apply_catalog_edit", new=edit
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, InstallReport(),
            checked=_PASSED, installed_at=INSTALLED_AT,
        )
    return edit.await_args.args[2]["headers"]["Authorization"]


@pytest.mark.asyncio
async def test_a_prefix_added_in_an_update_reaches_the_row():
    """v1 bound the bare key and the vendor answered 401; v2's bind adds the
    ``Bearer `` it wanted. Keeping the row's reference because it IS one
    would pin v1's wiring on every installed copy forever."""
    package = _bound_package(None, prefix="Bearer ")
    written = await _written_authorization(
        package, "${vault:SVC_KEY}", {"SVC_KEY"}
    )
    assert written == "Bearer ${vault:SVC_KEY}"


@pytest.mark.asyncio
async def test_a_prefixed_reference_still_wins_over_a_literal():
    package = _bound_package("Bearer placeholder-token")
    written = await _written_authorization(
        package, "Bearer ${vault:SVC_KEY}", set()
    )
    assert written == "Bearer ${vault:SVC_KEY}"


@pytest.mark.asyncio
async def test_a_reference_to_another_secret_does_not_replace_a_working_one():
    """The row's key was extracted from v1's literal; v2 binds a name the user
    has never filled. The working credential stays until they do."""
    package = _bound_package(None)
    written = await _written_authorization(
        package, "${vault:OLD_KEY}", {"SVC_KEY"}
    )
    assert written == "${vault:OLD_KEY}"


def test_a_command_servers_kept_env_reference_does_not_break_the_update():
    """The env section is read before the headers, and a key it kept used to
    leave the next section reading a set where the config was."""
    row = {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "data-mcp@1"],
        "env": {"API_KEY": "${vault:API_KEY}"},
    }
    incoming = {**row, "env": {"API_KEY": "placeholder"}}
    assert _preserve_vault_refs(incoming, row) == []
    assert incoming["env"] == {"API_KEY": "${vault:API_KEY}"}


def test_a_command_servers_argument_reference_is_kept_like_a_header():
    """Args are compared with their refs masked, so an update that only names
    another key in one is not a move, and the row's working key stays."""
    row = {
        "transport": "stdio",
        "command": "npx",
        "args": ["-y", "data-mcp@1", "--token=${vault:OLD_KEY}"],
    }
    incoming = {**row, "args": ["-y", "data-mcp@1", "--token=${vault:NEW_KEY}"]}
    assert _preserve_vault_refs(incoming, row) == []
    assert incoming["args"] == row["args"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("created_at", "status"),
    [
        # A row of the user's that was there first: install's ``exists``,
        # which keeps the version unsettled.
        ("2025-12-31T23:59:59+00:00", "exists"),
        # The plugin's own row, customized since: the user's choice, settled.
        ("2026-01-02T00:00:00+00:00", "detached"),
    ],
)
async def test_a_name_taken_outside_the_plugin_reads_as_install_would_read_it(
    created_at: str, status: str
):
    package = _package(mcp={
        "api": {"type": "streamable-http", "url": "https://x.example/mcp"}
    })
    report = InstallReport()
    with (
        patch(
            "src.server.services.plugins.update.list_plugin_server_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.services.plugins.update.get_catalog_server",
            new=AsyncMock(return_value={
                "name": "api", "transport": "http",
                "url": "https://x.example/mcp", "headers": {}, "env": {},
                "plugin_id": None, "created_at": created_at,
            }),
        ),
        patch(
            "src.server.services.plugins.update.get_user_secret_names",
            new=AsyncMock(return_value=[]),
        ),
    ):
        await _update_servers(
            USER, PLUGIN_ID, package, report,
            checked=_PASSED, installed_at=INSTALLED_AT,
        )

    assert [(c.key, c.status) for c in report.components] == [("api", status)]
    assert report.landed_whole is (status == "detached")
