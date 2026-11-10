"""Unit tests for ``manage_chart_layers``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.server.models.chart_layer import ChartRef, LayerRef
from src.tools.chart_annotation.schemas import ManageChartLayersArgs
from src.tools.chart_annotation.tools import manage_chart_layers


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _tool_call(name: str, args: dict, call_id: str = "call_test") -> dict:
    return {"name": name, "args": args, "id": call_id, "type": "tool_call"}


def _config(workspace_id: str | None = "ws_abc") -> dict:
    configurable: dict = {"user_id": "user_xyz"}
    if workspace_id is not None:
        configurable["workspace_id"] = workspace_id
    return {"configurable": configurable}


class FakeChartDB:
    """In-memory stand-in for ``src.server.database.chart_annotation``: rows
    keyed by ``(workspace_id, chart_id, layer id)``, insertion order kept per
    chart, upsert on a repeated id."""

    def __init__(self):
        # (workspace_id, chart_id) -> {layer_id: stored layer}
        self.instances: dict[tuple[str, str], dict[str, dict]] = {}

    def seed(self, ref: LayerRef, layer: dict) -> None:
        self.instances.setdefault((ref.workspace_id, ref.chart_id), {})[ref.layer_id] = ref.stored(layer)

    async def list_layers(self, workspace_id, chart_id):
        return list(self.instances.get((workspace_id, chart_id), {}).values())

    async def remove_layers(self, workspace_id, chart_id, ids):
        bucket = self.instances.get((workspace_id, chart_id), {})
        removed = [layer_id for layer_id in ids if layer_id in bucket]
        for layer_id in removed:
            del bucket[layer_id]
        return removed

    async def clear_chart(self, workspace_id, chart_id):
        bucket = self.instances.pop((workspace_id, chart_id), {})
        return len(bucket)

    def bucket(self, workspace_id, chart_id) -> dict[str, dict]:
        return self.instances.get((workspace_id, chart_id), {})


def _patch_db(db: FakeChartDB):
    """Patch the DB functions the tool module imported with the fake's methods."""
    return (
        patch("src.tools.chart_annotation.tools.list_layers", db.list_layers),
        patch("src.tools.chart_annotation.tools.remove_layers", db.remove_layers),
        patch("src.tools.chart_annotation.tools.clear_chart", db.clear_chart),
    )


@pytest.fixture
def fake_db():
    db = FakeChartDB()
    patchers = _patch_db(db)
    for p in patchers:
        p.start()
    try:
        yield db
    finally:
        for p in patchers:
            p.stop()


# --------------------------------------------------------------------------- #
# manage_chart_layers tool
# --------------------------------------------------------------------------- #


class TestManageChartLayers:
    @staticmethod
    def _seed_layer(fake_db, layer_id="nvda-setup", timeframe="1day"):
        fake_db.seed(
            ChartRef.of("ws_abc", "NVDA", timeframe).layer(layer_id),
            {"title": "Setup", "levels": [{"price": 1}], "drawings": [{}, {}]},
        )

    def test_args_validate_action(self):
        # valid
        ManageChartLayersArgs(symbol="NVDA", action="list")
        ManageChartLayersArgs(symbol="NVDA", action="remove", ids=["a"])
        # invalid action, and ids that do not fit the action
        for bad in (
            {"action": "flush"},
            {"action": "list", "ids": ["a"]},
            {"action": "remove"},
            {"action": "remove", "ids": []},
            {"action": "clear_all", "ids": ["a"]},
        ):
            with pytest.raises(ValidationError):
                ManageChartLayersArgs(symbol="NVDA", **bad)

    @pytest.mark.asyncio
    async def test_list_names_each_layer(self, fake_db):
        self._seed_layer(fake_db)
        result = await manage_chart_layers.ainvoke(
            _tool_call("manage_chart_layers", {"symbol": "nvda", "action": "list"}),
            config=_config(),
        )

        assert result.content == (
            "1 layer(s) on NVDA:1day.\n- nvda-setup 'Setup': 1 level, 2 drawings"
        )
        assert "annotations" not in result.artifact
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert result.artifact["symbol"] == "NVDA"
        assert result.artifact["timeframe"] == "1day"
        # Listed by id and size, never whole: the artifact is kept in the thread.
        assert result.artifact["layers"] == [
            {
                "layer_id": "nvda-setup",
                "title": "Setup",
                "elements": "1 level, 2 drawings",
            }
        ]

    @pytest.mark.asyncio
    async def test_list_scoped_to_timeframe(self, fake_db):
        """list returns only the requested instance, not other timeframes."""
        self._seed_layer(fake_db, "daily")
        self._seed_layer(fake_db, "hourly", timeframe="1hour")

        hourly = await manage_chart_layers.ainvoke(
            _tool_call(
                "manage_chart_layers",
                {"symbol": "NVDA", "timeframe": "1hour", "action": "list"},
            ),
            config=_config(),
        )

        assert hourly.artifact["chart_id"] == "NVDA:1hour"
        assert [lyr["layer_id"] for lyr in hourly.artifact["layers"]] == ["hourly"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("spelled", ["BRK.B", "brk-b"])
    async def test_class_share_either_spelling_names_one_chart(self, fake_db, spelled):
        """A layer stored on BRK-B is found, and removed, from either spelling,
        and the event's symbol is the one its chart_id carries."""
        fake_db.seed(LayerRef("ws_abc", "BRK-B:1day", "BRK-B", "1day", "a"), {"title": "T"})
        listed = await manage_chart_layers.ainvoke(
            _tool_call("manage_chart_layers", {"symbol": spelled, "action": "list"}),
            config=_config(),
        )
        assert listed.artifact["chart_id"] == "BRK-B:1day"
        assert listed.artifact["symbol"] == "BRK-B"
        assert [lyr["layer_id"] for lyr in listed.artifact["layers"]] == ["a"]

        removed = await manage_chart_layers.ainvoke(
            _tool_call(
                "manage_chart_layers", {"symbol": spelled, "action": "remove", "ids": ["a"]}
            ),
            config=_config(),
        )
        assert removed.artifact["removed"] == 1
        assert removed.artifact["symbol"] == "BRK-B"

    @pytest.mark.asyncio
    async def test_list_empty_chart(self, fake_db):
        result = await manage_chart_layers.ainvoke(
            _tool_call("manage_chart_layers", {"symbol": "NVDA", "action": "list"}),
            config=_config(),
        )
        assert result.content == "0 layer(s) on NVDA:1day."
        assert result.artifact["layers"] == []

    @pytest.mark.asyncio
    async def test_list_does_not_accept_ids(self, fake_db):
        result = await manage_chart_layers.ainvoke(
            _tool_call(
                "manage_chart_layers",
                {"symbol": "NVDA", "action": "list", "ids": ["l1"]},
            ),
            config=_config(),
        )
        assert result.content == "Error: 'list' takes no 'ids'."
        assert result.artifact is None

    @pytest.mark.asyncio
    async def test_remove_deletes_specific_layers(self, fake_db):
        self._seed_layer(fake_db, "a")
        self._seed_layer(fake_db, "b")
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await manage_chart_layers.ainvoke(
                _tool_call(
                    "manage_chart_layers",
                    {"symbol": "NVDA", "action": "remove", "ids": ["a"]},
                ),
                config=_config(),
            )

        assert result.artifact["removed"] == 1
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert list(fake_db.bucket("ws_abc", "NVDA:1day")) == ["b"]
        # One SSE event, for the layer, carrying identity.
        writer.assert_called_once()
        event = writer.call_args[0][0]
        assert event["artifact_type"] == "chart_layer"
        assert event["payload"] == {
            "op": "remove",
            "workspace_id": "ws_abc",
            "chart_id": "NVDA:1day",
            "symbol": "NVDA",
            "timeframe": "1day",
            "ids": ["a"],
        }

    @pytest.mark.asyncio
    async def test_remove_takes_the_layer_id_the_agent_published(self, fake_db):
        """The id a layer was published under is the id that removes it."""
        self._seed_layer(fake_db, "q4-range")
        result = await manage_chart_layers.ainvoke(
            _tool_call(
                "manage_chart_layers",
                {"symbol": "NVDA", "action": "remove", "ids": ["q4-range", "gone"]},
            ),
            config=_config(),
        )
        # An id that matched nothing is named, so a typo is not read as done.
        assert result.content == (
            "Removed 1 layer(s) from NVDA:1day. "
            "No layer 'gone' on this chart; action='list' shows the ids."
        )
        assert fake_db.bucket("ws_abc", "NVDA:1day") == {}

    @pytest.mark.asyncio
    async def test_remove_requires_non_empty_ids(self, fake_db):
        result = await manage_chart_layers.ainvoke(
            _tool_call("manage_chart_layers", {"symbol": "NVDA", "action": "remove"}),
            config=_config(),
        )
        assert result.content == (
            "Error: 'remove' needs the layer ids to delete. "
            "action='list' shows them; action='clear_all' deletes every layer."
        )
        assert result.artifact is None

    @pytest.mark.asyncio
    async def test_clear_all_removes_everything(self, fake_db):
        for layer_id in ("a", "b", "c"):
            self._seed_layer(fake_db, layer_id)
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await manage_chart_layers.ainvoke(
                _tool_call(
                    "manage_chart_layers", {"symbol": "NVDA", "action": "clear_all"}
                ),
                config=_config(),
            )

        assert result.artifact["cleared"] == 3
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert fake_db.bucket("ws_abc", "NVDA:1day") == {}
        writer.assert_called_once()
        event = writer.call_args[0][0]
        assert event["artifact_type"] == "chart_layer"
        assert event["payload"]["op"] == "clear"
        assert event["payload"]["chart_id"] == "NVDA:1day"
        assert event["payload"]["workspace_id"] == "ws_abc"

    @pytest.mark.asyncio
    async def test_clear_all_rejects_ids(self, fake_db):
        result = await manage_chart_layers.ainvoke(
            _tool_call(
                "manage_chart_layers",
                {"symbol": "NVDA", "action": "clear_all", "ids": ["l1"]},
            ),
            config=_config(),
        )
        assert result.content == "Error: 'clear_all' takes no 'ids'; action='remove' deletes some layers."
        assert result.artifact is None

    @pytest.mark.asyncio
    async def test_a_store_failure_deletes_nothing_and_says_so(self, fake_db):
        with patch(
            "src.tools.chart_annotation.tools.clear_chart", side_effect=RuntimeError("db down")
        ):
            result = await manage_chart_layers.ainvoke(
                _tool_call("manage_chart_layers", {"symbol": "NVDA", "action": "clear_all"}),
                config=_config(),
            )
        assert result.content == (
            "Could not clear the chart (persistence unavailable). "
            "Nothing was deleted. Try again in a moment."
        )


# --------------------------------------------------------------------------- #
# Registry visibility
# --------------------------------------------------------------------------- #


class TestSkillRegistryVisibility:
    def test_chart_annotation_registered_as_a_listed_guide(self):
        from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY

        skill = SKILL_REGISTRY["chart-annotation"]
        # The drawing guide, discoverable by the agent. The tools themselves
        # are bound on every main-agent build, so the skill gates none.
        assert not skill.hidden
        assert skill.tool_names == ()

    def test_appears_in_default_listing(self):
        """chart-annotation must be in the manifest the LLM sees.

        ``list_skills`` drives what gets injected into the system prompt via
        SkillsMiddleware. The agent can only choose to load a skill it can see,
        so a discoverable skill must appear here.
        """
        from ptc_agent.agent.middleware.skills.registry import list_skills

        assert "chart-annotation" in {s["name"] for s in list_skills()}

    def test_synced_to_sandbox(self):
        """SKILL.md is uploaded so the agent can self-load by reading it."""
        from ptc_agent.agent.middleware.skills.registry import (
            SKILL_REGISTRY,
            skills_outside_sandbox,
        )

        assert "chart-annotation" in SKILL_REGISTRY
        assert "chart-annotation" not in skills_outside_sandbox()

    def test_reachable_by_name(self):
        from ptc_agent.agent.middleware.skills.registry import get_skill

        skill = get_skill("chart-annotation")
        assert skill is not None
        assert skill.name == "chart-annotation"
