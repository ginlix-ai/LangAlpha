"""Update-path reconciliation invariants (services/plugins/update.py).

Install and update reach the same rows by different roads: install goes through
run_mcp_import, update writes through apply_catalog_edit. Anything the import
loop does on the way in, the update arm owes too — these pin the two places
where the roads had diverged, both of which cost the user something real (a
plaintext credential, a deleted skill).
"""

from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.plugin import InstallReport
from src.server.services.plugins import validate_package
from src.server.services.plugins.update import _update_servers, _update_skills

USER = "test-user-123"
PLUGIN_ID = "22222222-2222-2222-2222-222222222222"
CANONICAL_PLUGIN_SCHEMA = (
    "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
)
CANONICAL_MCP_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
SECRET = "sk-live-abcdef0123456789"


def _zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


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
    return validate_package(_zip(members))


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
        await _update_servers(USER, PLUGIN_ID, package, report)

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
        await _update_skills(USER, PLUGIN_ID, package, report)

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
        await _update_servers(USER, PLUGIN_ID, package, report)

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
        await _update_servers(USER, PLUGIN_ID, package, report)

    edit.assert_not_awaited()
    assert any(c.status == "detached" for c in report.components)
