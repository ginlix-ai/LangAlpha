"""A plugin's page names the connectors a rule on the user's own connectors
keeps from landing, as its install report did."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.config.models import PolicyLayer
from src.server.models.plugin import PluginComponentRef
from src.server.services.policy import merge

ROW = {
    "user_plugin_id": "p1",
    "mcp_document": {
        "mcpServers": {
            "edgar": {"type": "http", "url": "https://edgar.example.test/mcp"},
            "tikr": {"type": "http", "url": "https://tikr.example.test/mcp"},
            "landed": {"type": "http", "url": "https://landed.example.test/mcp"},
        }
    },
}
LANDED = {"p1": [PluginComponentRef(kind="mcp", name="landed", key="landed")]}


def _Policy(user_connectors: bool):
    return merge(None, PolicyLayer(user_connectors=user_connectors))


@pytest.mark.asyncio
async def test_connectors_the_rule_keeps_out_are_named_on_the_plugin():
    from src.server.app.plugins import _withheld_by_plugin

    with patch("src.server.services.account.policy_for", AsyncMock(return_value=_Policy(False))):
        assert await _withheld_by_plugin("u", [ROW], LANDED) == {"p1": ["edgar", "tikr"]}


@pytest.mark.asyncio
async def test_nothing_is_held_back_while_the_rule_allows_them():
    from src.server.app.plugins import _withheld_by_plugin

    with patch("src.server.services.account.policy_for", AsyncMock(return_value=_Policy(True))):
        assert await _withheld_by_plugin("u", [ROW], LANDED) == {}


@pytest.mark.asyncio
async def test_no_policy_is_read_when_every_connector_landed():
    from src.server.app.plugins import _withheld_by_plugin

    whole = {"p1": [PluginComponentRef(kind="mcp", name=k, key=k) for k in ("edgar", "tikr", "landed")]}
    read = AsyncMock()
    with patch("src.server.services.account.policy_for", read):
        assert await _withheld_by_plugin("u", [ROW], whole) == {}
    read.assert_not_called()
