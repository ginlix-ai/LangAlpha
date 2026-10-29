"""Shared builders for the plugin install, update, rotation and pending-commit
tests: a validated package from its servers and secrets, a catalog row as the
database returns it, and a fan-out lock that records whether it is held.
"""

from __future__ import annotations

import json

from src.server.services.plugins import ValidatedPackage, validate_package
from tests.unit.server.mcp_builders import zip_package

USER = "test-user-123"
PLUGIN_ID = "33333333-3333-3333-3333-333333333333"
PLUGIN_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
MCP_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"

KEYED = [{
    "name": "VENDOR_KEY",
    "label": "Vendor key",
    "description": "From the vendor console.",
    "bind": [{"server": "vendor", "header": "X-Api-Key"}],
}]
VENDOR = {"vendor": {"type": "streamable-http", "url": "https://vendor.example.com/mcp"}}
OTHER = {"other": {"type": "streamable-http", "url": "https://other.example.com/mcp"}}
#: An entry that sends a key the plugin never declares.
REFERENCING = {
    "foo": {
        "type": "streamable-http",
        "url": "https://foo.example.com/mcp",
        "headers": {"Authorization": "Bearer ${vault:FOO_TOKEN}"},
    }
}
#: The headers of a row that already carries the vendor's key.
KEYED_ROW = {"X-Api-Key": "${vault:VENDOR_KEY}"}


def plugin_package(
    servers: dict, *, secrets: list | None = None, version: str = "1.0.0"
) -> ValidatedPackage:
    """A plugin named demo declaring ``servers`` and, if given, ``secrets``."""
    manifest: dict = {"$schema": PLUGIN_SCHEMA, "name": "demo", "version": version}
    if secrets:
        manifest["extensions"] = {"ai.langalpha": {"secrets": secrets}}
    return validate_package(zip_package({
        "plugin.json": json.dumps(manifest).encode(),
        "mcp.json": json.dumps(
            {"$schema": MCP_SCHEMA, "mcpServers": servers}
        ).encode(),
    }))


def catalog_row(name: str, url: str, **extra) -> dict:
    """A user_mcp_servers row the demo plugin owns, unless ``extra`` says otherwise."""
    return {
        "name": name, "transport": "http", "url": url, "command": None,
        "args": [], "env": {}, "headers": {}, "plugin_id": PLUGIN_ID,
        "plugin_server_key": name, **extra,
    }


class FanOutLock:
    """Stands in for ``plugin_fan_out_lock`` and records whether it is held."""

    def __init__(self) -> None:
        self.held = False
        self.entered = 0

    def __call__(self, _user_id):
        lock = self

        class _Ctx:
            async def __aenter__(self):
                lock.held = True
                lock.entered += 1

            async def __aexit__(self, *exc):
                lock.held = False

        return _Ctx()
