"""The plugin routes answer a refused check in the shapes the client reads."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.mcp_validation import (
    CheckFailure,
    McpCheckFailed,
    McpSignInRequired,
    SignInNeeded,
)
from src.server.services.plugins.errors import PluginRefused
from tests.conftest import create_test_app
from tests.unit.server.mcp_builders import zip_package

pytestmark = pytest.mark.asyncio

A = "src.server.app.plugins."
SOURCE = "src.server.services.plugins.source."


def _zip() -> bytes:
    return zip_package({
        "plugin.json": json.dumps({
            "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
            "name": "demo",
            "version": "1.0.0",
        }),
        "mcp.json": json.dumps({
            "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
            "mcpServers": {
                "vendor": {"type": "streamable-http", "url": "https://vendor.example.com/mcp"}
            },
        }),
    })


@pytest_asyncio.fixture
async def client():
    from src.server.app.plugins import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def _install(client, error: Exception, body: dict | None = None):
    with (
        patch(SOURCE + "fetch_plugin_source", new=AsyncMock(return_value=(_zip(), None))),
        patch(A + "install_plugin_package", new=AsyncMock(side_effect=error)) as install,
    ):
        resp = await client.post(
            "/api/v1/plugins",
            json={"source_url": "https://example.com/demo.zip", **(body or {})},
        )
    return resp, install


async def test_secrets_required_is_a_422_with_the_form_fields(client):
    refused = PluginRefused(
        "plugin_secrets_required", "needs a key",
        secrets=[{"name": "VENDOR_KEY", "label": "Vendor key"}],
    )
    resp, _ = await _install(client, refused)
    assert resp.status_code == 422
    assert resp.json()["detail"] == {
        "code": "plugin_secrets_required",
        "message": "needs a key",
        "secrets": [{"name": "VENDOR_KEY", "label": "Vendor key"}],
    }


async def test_a_failed_check_is_a_422_not_a_conflict(client):
    failed = McpCheckFailed([
        CheckFailure(server="vendor", verdict="credential_rejected", reason="bad key")
    ])
    resp, _ = await _install(client, failed, {"secrets": {"VENDOR_KEY": "v"}})
    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "mcp_check_failed"
    assert resp.json()["detail"]["failures"][0]["server"] == "vendor"


async def test_a_sign_in_is_a_409(client):
    resp, _ = await _install(
        client, McpSignInRequired([SignInNeeded(server="vendor", url="https://vendor.example.com/mcp")])
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "mcp_sign_in_required"


async def test_the_body_secrets_reach_the_install(client):
    resp, install = await _install(
        client, ValueError("Plugin 'demo' is already installed; use update"),
        {"secrets": {"VENDOR_KEY": "v"}},
    )
    assert resp.status_code == 409
    assert install.await_args.kwargs["secrets"] == {"VENDOR_KEY": "v"}


async def test_an_upload_takes_its_secrets_as_a_json_form_field(client):
    install = AsyncMock(side_effect=ValueError("stop here"))
    with patch(A + "install_plugin_package", new=install):
        resp = await client.post(
            "/api/v1/plugins/upload",
            files={"file": ("demo.zip", _zip(), "application/zip")},
            data={"secrets": json.dumps({"VENDOR_KEY": "v"})},
        )
        assert resp.status_code == 409
        assert install.await_args.kwargs["secrets"] == {"VENDOR_KEY": "v"}

        resp = await client.post(
            "/api/v1/plugins/upload",
            files={"file": ("demo.zip", _zip(), "application/zip")},
            data={"secrets": "not json"},
        )
    assert resp.status_code == 422
    assert "JSON object" in resp.json()["detail"]


@pytest.mark.parametrize(
    ("body", "secrets"),
    [(None, None), ({"secrets": {"VENDOR_KEY": "v"}}, {"VENDOR_KEY": "v"})],
)
async def test_an_update_takes_an_optional_body_of_secrets(client, body, secrets):
    plugin = {
        "user_plugin_id": "p1", "name": "demo", "source_type": "git",
        "source_ref": "https://example.com/demo.zip",
    }
    update = AsyncMock(side_effect=ValueError("stop here"))
    with (
        patch(A + "get_plugin", new=AsyncMock(return_value=plugin)),
        patch(SOURCE + "fetch_plugin_source", new=AsyncMock(return_value=(_zip(), None))),
        patch(A + "update_plugin_package", new=update),
    ):
        resp = await client.post("/api/v1/plugins/demo/update", json=body)
    assert resp.status_code == 409
    assert update.await_args.kwargs["secrets"] == secrets


# --- updating to another revision ------------------------------------------

_PINNED = "https://github.com/acme/agents/tree/{}/plugins/demo"
_OLD = "4236bb91f8395b0435f1d8b8baf9e8e4c69a8620"
_NEW = "574ed3624aebd0418c7e96cd101262f30210ab26"


async def _update_to(client, source_url: str):
    plugin = {
        "user_plugin_id": "p1", "name": "demo", "source_type": "git",
        "source_ref": _PINNED.format(_OLD),
    }
    fetch = AsyncMock(return_value=(_zip(), None))
    update = AsyncMock(side_effect=ValueError("stop here"))
    with (
        patch(A + "get_plugin", new=AsyncMock(return_value=plugin)),
        patch(SOURCE + "fetch_plugin_source", new=fetch),
        patch(A + "update_plugin_package", new=update),
    ):
        resp = await client.post(
            "/api/v1/plugins/demo/update", json={"source_url": source_url}
        )
    return resp, fetch, update


@pytest.mark.parametrize(
    "source_url",
    [
        _PINNED.format(_NEW),
        # The chooser's spelling of the same folder.
        f"https://github.com/acme/agents/tree/{_NEW}#subdir=plugins/demo",
    ],
)
async def test_an_update_moves_to_another_revision_of_its_own_source(client, source_url):
    resp, fetch, update = await _update_to(client, source_url)

    assert resp.status_code == 409  # the stubbed write's refusal
    assert fetch.await_args.args == (source_url,)
    # The row then names the revision it holds.
    assert update.await_args.kwargs["source_ref"] == source_url


@pytest.mark.parametrize(
    "source_url",
    [
        f"https://github.com/someone/agents/tree/{_NEW}/plugins/demo",
        f"https://github.com/acme/agents/tree/{_NEW}/plugins/other",
        f"https://github.com/acme/agents/tree/{_NEW}",
        "https://example.com/demo.zip",
        "http://github.com/acme/agents/tree/HEAD/plugins/demo",
    ],
)
async def test_an_update_refuses_any_other_package_before_fetching(client, source_url):
    resp, fetch, update = await _update_to(client, source_url)

    assert resp.status_code == 422
    assert resp.json()["detail"]["code"] == "plugin_source_mismatch"
    fetch.assert_not_awaited()
    update.assert_not_awaited()


# --- switching a plugin on ---------------------------------------------------

_PLUGIN = {"user_plugin_id": "p-1", "name": "demo", "enabled": False}


def _server(name: str, *, enabled: bool = True, plugin_id: str = "p-1") -> dict:
    return {
        "name": name,
        "transport": "http",
        "url": f"https://{name}.example.com/mcp",
        "enabled": enabled,
        "plugin_id": plugin_id,
    }


async def _switch_on(client, rows, missing: set[str], plugin=_PLUGIN):
    async def _missing(user_id, row):
        if row["name"] in missing:
            return SignInNeeded(server=row["name"], url=row["url"])
        return None

    asked = AsyncMock(side_effect=_missing)
    with (
        patch(A + "get_plugin", new=AsyncMock(return_value=plugin)),
        patch(A + "list_catalog_servers", new=AsyncMock(return_value=rows)),
        patch(A + "sign_in_missing", new=asked),
        patch(
            A + "set_plugin_enabled",
            new=AsyncMock(return_value={**(plugin or {}), "enabled": True}),
        ) as toggle,
    ):
        resp = await client.patch("/api/v1/plugins/demo/enabled", json={"enabled": True})
    return resp, toggle, asked


async def test_a_plugin_with_an_unconnected_server_is_not_switched_on(client):
    rows = [_server("a"), _server("b"), _server("c")]
    resp, toggle, _ = await _switch_on(client, rows, missing={"a", "c"})

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "mcp_sign_in_required"
    assert [s["server"] for s in detail["servers"]] == ["a", "c"]
    toggle.assert_not_awaited()


async def test_only_this_plugins_servers_that_are_on_are_asked_about(client):
    rows = [
        _server("off", enabled=False),
        _server("other", plugin_id="p-2"),
        _server("mine"),
    ]
    resp, toggle, asked = await _switch_on(client, rows, missing={"off", "other"})

    assert resp.status_code == 200
    assert [c.args[1]["name"] for c in asked.await_args_list] == ["mine"]
    toggle.assert_awaited_once()


async def test_a_plugin_already_on_is_not_asked_again(client):
    resp, toggle, asked = await _switch_on(
        client, [_server("a")], missing={"a"}, plugin={**_PLUGIN, "enabled": True}
    )

    assert resp.status_code == 200
    asked.assert_not_awaited()
    toggle.assert_awaited_once()


async def test_switching_a_plugin_off_never_asks(client):
    asked = AsyncMock()
    with (
        patch(A + "sign_in_missing", new=asked),
        patch(A + "set_plugin_enabled", new=AsyncMock(return_value={**_PLUGIN})),
        patch(A + "list_plugin_server_names", new=AsyncMock(return_value=[])),
        patch(
            "src.server.services.mcp_oauth.lifecycle.revoke_live_grants",
            new=AsyncMock(),
        ),
    ):
        resp = await client.patch("/api/v1/plugins/demo/enabled", json={"enabled": False})

    assert resp.status_code == 200
    asked.assert_not_awaited()
