"""Fork-on-edit's interaction with plugin-level suppression.

Plugin disable is enforced by the delivery join predicate, `(plugin_id IS NULL
OR p.enabled)`. That makes clearing plugin_id the one write that can turn a
suppressed row into an unconditionally delivered one, which is how editing a
description came to start a server the user had switched off.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.mcp_catalog import apply_catalog_edit

USER = "test-user-123"


def _prior(*, plugin_enabled):
    return {
        "name": "weather",
        "transport": "http",
        "url": "https://example.com/mcp",
        "enabled": True,
        "plugin_id": "22222222-2222-2222-2222-222222222222",
        "plugin_name": "acme",
        "plugin_enabled": plugin_enabled,
    }


async def _edit(prior):
    toggle = AsyncMock(return_value=True)
    with (
        patch(
            "src.server.services.mcp_catalog.get_catalog_server",
            new=AsyncMock(return_value=prior),
        ),
        patch(
            "src.server.services.mcp_catalog.update_catalog_server",
            new=AsyncMock(return_value=dict(prior)),
        ),
        patch(
            "src.server.services.mcp_catalog.set_catalog_server_enabled",
            new=toggle,
        ),
        patch(
            "src.server.services.mcp_oauth.lifecycle.revoke_if_consent_moved",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "src.server.services.mcp_oauth.discovery."
            "schedule_post_edit_rediscovery",
            new=lambda *a, **k: None,
        ),
    ):
        await apply_catalog_edit(
            USER, "weather", {"description": "typo fix"}, detach_plugin=True
        )
    return toggle


@pytest.mark.asyncio
async def test_detaching_from_a_disabled_plugin_keeps_the_row_off():
    # The user switched the plugin off. Clearing plugin_id would make the
    # predicate pass unconditionally, so the OFF state has to land on the row.
    toggle = await _edit(_prior(plugin_enabled=False))
    toggle.assert_awaited_once_with(USER, "weather", False)


@pytest.mark.asyncio
async def test_detaching_from_an_enabled_plugin_leaves_the_row_alone():
    # Nothing was being suppressed, so there is nothing to carry over and the
    # edit must not silently disable a running server.
    toggle = await _edit(_prior(plugin_enabled=True))
    toggle.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_row_with_no_plugin_is_untouched():
    prior = _prior(plugin_enabled=None)
    prior["plugin_id"] = None
    prior["plugin_name"] = None
    toggle = await _edit(prior)
    toggle.assert_not_awaited()
