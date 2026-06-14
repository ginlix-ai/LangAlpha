"""Unit tests for chart annotation tools and schemas."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.tools.chart_annotation.schemas import (
    DrawChartAnnotationArgs,
    EventAnnotation,
    FibRetracementAnnotation,
    ManageChartAnnotationsArgs,
    MarkerAnnotation,
    PriceLineAnnotation,
    RectangleAnnotation,
    TextAnnotation,
    TrendlineAnnotation,
    VerticalLineAnnotation,
)
from src.tools.chart_annotation.tools import (
    draw_chart_annotation,
    manage_chart_annotations,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _tool_call(name: str, args: dict, call_id: str = "call_test") -> dict:
    return {"name": name, "args": args, "id": call_id, "type": "tool_call"}


def _drawn(result) -> dict:
    """The single annotation just drawn, read from the inline-card artifact.

    ``draw_chart_annotation`` returns a ``chart_annotation`` wrapper artifact
    whose ``annotations`` list holds the chart instance's full current set. The
    happy-path tests draw exactly one into a fresh instance, so [0] is it.
    """
    return result.artifact["annotations"][0]


def _config(workspace_id: str | None = "ws_abc") -> dict:
    configurable: dict = {"user_id": "user_xyz"}
    if workspace_id is not None:
        configurable["workspace_id"] = workspace_id
    return {"configurable": configurable}


class FakeChartDB:
    """In-memory stand-in for ``src.server.database.chart_annotation``.

    Mirrors the real instance semantics: rows keyed by
    ``(workspace_id, chart_id, annotation_id)``, insertion order preserved per
    instance, upsert on a duplicate annotation_id.
    """

    def __init__(self, fail_on_add: bool = False):
        # (workspace_id, chart_id) -> {annotation_id: payload}
        self.instances: dict[tuple[str, str], dict[str, dict]] = {}
        self.fail_on_add = fail_on_add

    async def add_annotation(self, workspace_id, chart_id, symbol, timeframe, annotation):
        if self.fail_on_add:
            raise RuntimeError("simulated db failure")
        bucket = self.instances.setdefault((workspace_id, chart_id), {})
        bucket[annotation["annotation_id"]] = dict(annotation)

    async def list_annotations(self, workspace_id, chart_id):
        return list(self.instances.get((workspace_id, chart_id), {}).values())

    async def remove_annotations(self, workspace_id, chart_id, ids):
        bucket = self.instances.get((workspace_id, chart_id), {})
        removed = 0
        for ann_id in ids:
            if ann_id in bucket:
                del bucket[ann_id]
                removed += 1
        return removed

    async def clear_chart(self, workspace_id, chart_id):
        bucket = self.instances.pop((workspace_id, chart_id), {})
        return len(bucket)

    def bucket(self, workspace_id, chart_id) -> dict[str, dict]:
        return self.instances.get((workspace_id, chart_id), {})


def _patch_db(db: FakeChartDB):
    """Patch the DB functions the tool module imported with the fake's methods."""
    return (
        patch("src.tools.chart_annotation.tools.add_annotation", db.add_annotation),
        patch("src.tools.chart_annotation.tools.list_annotations", db.list_annotations),
        patch("src.tools.chart_annotation.tools.remove_annotations", db.remove_annotations),
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


@pytest.fixture
def failing_db():
    """Fake DB whose add_annotation always raises (fail-closed path)."""
    db = FakeChartDB(fail_on_add=True)
    patchers = _patch_db(db)
    for p in patchers:
        p.start()
    try:
        yield db
    finally:
        for p in patchers:
            p.stop()


# --------------------------------------------------------------------------- #
# Schema validation
# --------------------------------------------------------------------------- #


class TestAnnotationSchemas:
    def test_price_line_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={"type": "price_line", "price": 205.0, "label": "Resistance"},
        )
        assert isinstance(args.annotation, PriceLineAnnotation)
        assert args.annotation.price == 205.0
        assert args.annotation.style == "solid"  # default

    def test_trendline_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="AAPL",
            annotation={
                "type": "trendline",
                "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.2},
                "point2": {"time": "2024-12-20T00:00:00Z", "price": 138.7},
            },
        )
        assert isinstance(args.annotation, TrendlineAnnotation)
        assert args.annotation.point1.price == 145.2

    def test_marker_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="TSLA",
            annotation={
                "type": "marker",
                "time": "2024-11-14T00:00:00Z",
                "shape": "arrowUp",
            },
        )
        assert isinstance(args.annotation, MarkerAnnotation)
        assert args.annotation.position == "aboveBar"  # default

    def test_vertical_line_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={
                "type": "vertical_line",
                "time": "2024-11-14T00:00:00Z",
                "label": "Earnings",
            },
        )
        assert isinstance(args.annotation, VerticalLineAnnotation)
        assert args.annotation.style == "dashed"  # default

    def test_rectangle_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={
                "type": "rectangle",
                "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.2},
                "point2": {"time": "2024-12-20T00:00:00Z", "price": 138.7},
                "label": "Demand zone",
            },
        )
        assert isinstance(args.annotation, RectangleAnnotation)
        assert args.annotation.point1.price == 145.2
        assert args.annotation.point2.time == "2024-12-20T00:00:00Z"

    def test_text_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={
                "type": "text",
                "time": "2024-11-14T00:00:00Z",
                "price": 200.0,
                "text": "Breakout",
            },
        )
        assert isinstance(args.annotation, TextAnnotation)
        assert args.annotation.text == "Breakout"

    def test_text_requires_text_field(self):
        with pytest.raises(ValidationError):
            DrawChartAnnotationArgs(
                symbol="NVDA",
                annotation={
                    "type": "text",
                    "time": "2024-11-14T00:00:00Z",
                    "price": 200.0,
                },
            )

    def test_event_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={
                "type": "event",
                "time": "2024-11-14T00:00:00Z",
                "price": 205.0,
                "title": "Q3 earnings beat",
                "detail": "Beat EPS by $0.15 and raised full-year guidance ~5%.",
            },
        )
        assert isinstance(args.annotation, EventAnnotation)
        assert args.annotation.title == "Q3 earnings beat"
        assert args.annotation.color is None  # default

    def test_event_requires_title_and_detail(self):
        with pytest.raises(ValidationError):
            DrawChartAnnotationArgs(
                symbol="NVDA",
                annotation={
                    "type": "event",
                    "time": "2024-11-14T00:00:00Z",
                    "price": 205.0,
                    "title": "Earnings",  # missing detail
                },
            )

    def test_fib_retracement_variant_roundtrip(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={
                "type": "fib_retracement",
                "point1": {"time": "2024-10-16T00:00:00Z", "price": 100.0},
                "point2": {"time": "2024-12-20T00:00:00Z", "price": 200.0},
            },
        )
        assert isinstance(args.annotation, FibRetracementAnnotation)
        assert args.annotation.point2.price == 200.0

    def test_invalid_discriminator_rejected(self):
        with pytest.raises(ValidationError):
            DrawChartAnnotationArgs(
                symbol="NVDA",
                annotation={"type": "bogus", "price": 1.0},
            )

    def test_trendline_missing_point2_rejected(self):
        with pytest.raises(ValidationError):
            DrawChartAnnotationArgs(
                symbol="NVDA",
                annotation={
                    "type": "trendline",
                    "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.0},
                },
            )

    def test_timeframe_defaults_to_1day(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            annotation={"type": "price_line", "price": 1.0},
        )
        assert args.timeframe == "1day"

    def test_timeframe_accepts_valid_interval(self):
        args = DrawChartAnnotationArgs(
            symbol="NVDA",
            timeframe="1hour",
            annotation={"type": "price_line", "price": 1.0},
        )
        assert args.timeframe == "1hour"

    def test_timeframe_rejects_unknown_interval(self):
        with pytest.raises(ValidationError):
            DrawChartAnnotationArgs(
                symbol="NVDA",
                timeframe="2hour",
                annotation={"type": "price_line", "price": 1.0},
            )

    def test_schema_emits_oneof_with_all_variants(self):
        """The LLM sees a discriminated oneOf with exactly our variants."""
        schema = DrawChartAnnotationArgs.model_json_schema()
        ann = schema["properties"]["annotation"]
        assert "oneOf" in ann
        assert ann["discriminator"]["propertyName"] == "type"
        mapping = ann["discriminator"]["mapping"]
        assert set(mapping.keys()) == {
            "price_line",
            "trendline",
            "marker",
            "vertical_line",
            "rectangle",
            "text",
            "event",
            "fib_retracement",
        }

    def test_manage_args_validates_action(self):
        # valid
        ManageChartAnnotationsArgs(symbol="NVDA", action="list")
        ManageChartAnnotationsArgs(symbol="NVDA", action="remove", ids=["a"])
        # invalid action
        with pytest.raises(ValidationError):
            ManageChartAnnotationsArgs(symbol="NVDA", action="flush")


# --------------------------------------------------------------------------- #
# draw_chart_annotation tool
# --------------------------------------------------------------------------- #


class TestDrawChartAnnotation:
    @pytest.mark.asyncio
    async def test_price_line_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "nvda",
                        "annotation": {
                            "type": "price_line",
                            "price": 205.0,
                            "label": "Resistance",
                        },
                    },
                ),
                config=_config(),
            )

        # content mentions price and symbol upper-cased
        assert "205" in result.content
        assert "NVDA" in result.content

        # artifact is the chart_annotation wrapper carrying the full set + identity
        assert result.artifact["type"] == "chart_annotation"
        assert result.artifact["op"] == "add"
        assert result.artifact["symbol"] == "NVDA"
        assert result.artifact["timeframe"] == "1day"
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert result.artifact["workspace_id"] == "ws_abc"
        annotation_id = result.artifact["annotation_id"]
        assert annotation_id.startswith("ann_")
        assert _drawn(result)["type"] == "price_line"

        # DB got the write under the (workspace, chart_id) instance
        bucket = fake_db.bucket("ws_abc", "NVDA:1day")
        assert annotation_id in bucket
        assert bucket[annotation_id]["price"] == 205.0
        assert bucket[annotation_id]["label"] == "Resistance"

        # Stream writer emitted the artifact event with op=add + identity
        writer.assert_called_once()
        payload = writer.call_args[0][0]
        assert payload["artifact_type"] == "chart_annotation"
        assert payload["payload"]["op"] == "add"
        assert payload["payload"]["symbol"] == "NVDA"
        assert payload["payload"]["timeframe"] == "1day"
        assert payload["payload"]["chart_id"] == "NVDA:1day"
        assert payload["payload"]["workspace_id"] == "ws_abc"
        assert payload["payload"]["annotation_id"] == annotation_id

    @pytest.mark.asyncio
    async def test_trendline_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "trendline",
                            "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.2},
                            "point2": {"time": "2024-12-20T00:00:00Z", "price": 138.7},
                            "label": "Channel top",
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "trendline"
        assert _drawn(result)["point1"]["price"] == 145.2
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_marker_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "marker",
                            "time": "2024-11-14T00:00:00Z",
                            "shape": "arrowUp",
                            "text": "Earnings beat",
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "marker"
        assert _drawn(result)["shape"] == "arrowUp"
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_vertical_line_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "vertical_line",
                            "time": "2024-11-14T00:00:00Z",
                            "label": "Earnings",
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "vertical_line"
        assert _drawn(result)["style"] == "dashed"  # default persisted
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_rectangle_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "rectangle",
                            "point1": {"time": "2024-10-16T00:00:00Z", "price": 145.2},
                            "point2": {"time": "2024-12-20T00:00:00Z", "price": 138.7},
                            "label": "Demand zone",
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "rectangle"
        assert _drawn(result)["point1"]["price"] == 145.2
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_text_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "text",
                            "time": "2024-11-14T00:00:00Z",
                            "price": 200.0,
                            "text": "Breakout",
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "text"
        assert _drawn(result)["text"] == "Breakout"
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_event_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "event",
                            "time": "2024-11-14T00:00:00Z",
                            "price": 205.0,
                            "title": "Q3 earnings beat",
                            "detail": "Beat EPS and raised guidance.",
                        },
                    },
                ),
                config=_config(),
            )

        assert "Q3 earnings beat" in result.content
        drawn = _drawn(result)
        assert drawn["type"] == "event"
        assert drawn["title"] == "Q3 earnings beat"
        assert drawn["detail"] == "Beat EPS and raised guidance."
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_fib_retracement_happy_path(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {
                            "type": "fib_retracement",
                            "point1": {"time": "2024-10-16T00:00:00Z", "price": 100.0},
                            "point2": {"time": "2024-12-20T00:00:00Z", "price": 200.0},
                        },
                    },
                ),
                config=_config(),
            )

        assert _drawn(result)["type"] == "fib_retracement"
        assert _drawn(result)["point2"]["price"] == 200.0
        writer.assert_called_once()

    @pytest.mark.asyncio
    async def test_result_artifact_carries_full_cumulative_set(self, fake_db):
        """Each draw's inline-card artifact holds the instance's full set."""
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {"symbol": "NVDA", "annotation": {"type": "price_line", "price": 200.0}},
                ),
                config=_config(),
            )
            second = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {"symbol": "NVDA", "annotation": {"type": "price_line", "price": 210.0}},
                ),
                config=_config(),
            )

        assert second.artifact["type"] == "chart_annotation"
        prices = sorted(a["price"] for a in second.artifact["annotations"])
        assert prices == [200.0, 210.0]

    @pytest.mark.asyncio
    async def test_timeframe_creates_distinct_instance(self, fake_db):
        """Same ticker, different timeframe = a separate chart instance."""
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            daily = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {"symbol": "NVDA", "annotation": {"type": "price_line", "price": 200.0}},
                ),
                config=_config(),
            )
            hourly = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "timeframe": "1hour",
                        "annotation": {"type": "price_line", "price": 210.0},
                    },
                ),
                config=_config(),
            )

        assert daily.artifact["chart_id"] == "NVDA:1day"
        assert hourly.artifact["chart_id"] == "NVDA:1hour"
        # Two separate instances, each with exactly its own annotation.
        assert len(daily.artifact["annotations"]) == 1
        assert len(hourly.artifact["annotations"]) == 1
        assert len(fake_db.bucket("ws_abc", "NVDA:1day")) == 1
        assert len(fake_db.bucket("ws_abc", "NVDA:1hour")) == 1

    @pytest.mark.asyncio
    async def test_missing_workspace_returns_error_no_write(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {"type": "price_line", "price": 1.0},
                    },
                ),
                config=_config(workspace_id=None),
            )

        assert "workspace_id" in result.content.lower()
        assert result.artifact == {}
        writer.assert_not_called()
        assert fake_db.instances == {}

    @pytest.mark.asyncio
    async def test_db_failure_is_fail_closed(self, failing_db):
        """When the DB write fails we must NOT emit an SSE event (no ghost draw)."""
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {"type": "price_line", "price": 205.0},
                    },
                ),
                config=_config(),
            )

        assert "persistence" in result.content.lower() or "could not save" in result.content.lower()
        assert result.artifact == {}
        writer.assert_not_called()


# --------------------------------------------------------------------------- #
# manage_chart_annotations tool
# --------------------------------------------------------------------------- #


class TestManageChartAnnotations:
    @pytest.mark.asyncio
    async def test_list_returns_stored_annotations(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            # seed one
            await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {"type": "price_line", "price": 205.0},
                    },
                ),
                config=_config(),
            )

            result = await manage_chart_annotations.ainvoke(
                _tool_call(
                    "manage_chart_annotations",
                    {"symbol": "NVDA", "action": "list"},
                ),
                config=_config(),
            )

        assert result.artifact["symbol"] == "NVDA"
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert result.artifact["timeframe"] == "1day"
        assert len(result.artifact["annotations"]) == 1
        assert result.artifact["annotations"][0]["price"] == 205.0

    @pytest.mark.asyncio
    async def test_list_scoped_to_timeframe(self, fake_db):
        """list returns only the requested instance, not other timeframes."""
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {"symbol": "NVDA", "annotation": {"type": "price_line", "price": 200.0}},
                ),
                config=_config(),
            )
            await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "timeframe": "1hour",
                        "annotation": {"type": "price_line", "price": 210.0},
                    },
                ),
                config=_config(),
            )

            hourly = await manage_chart_annotations.ainvoke(
                _tool_call(
                    "manage_chart_annotations",
                    {"symbol": "NVDA", "timeframe": "1hour", "action": "list"},
                ),
                config=_config(),
            )

        assert hourly.artifact["chart_id"] == "NVDA:1hour"
        assert len(hourly.artifact["annotations"]) == 1
        assert hourly.artifact["annotations"][0]["price"] == 210.0

    @pytest.mark.asyncio
    async def test_list_does_not_accept_ids(self, fake_db):
        result = await manage_chart_annotations.ainvoke(
            _tool_call(
                "manage_chart_annotations",
                {"symbol": "NVDA", "action": "list", "ids": ["ann_1"]},
            ),
            config=_config(),
        )
        assert "does not accept" in result.content.lower()
        assert result.artifact == {}

    @pytest.mark.asyncio
    async def test_remove_deletes_specific_ids(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            draw_result = await draw_chart_annotation.ainvoke(
                _tool_call(
                    "draw_chart_annotation",
                    {
                        "symbol": "NVDA",
                        "annotation": {"type": "price_line", "price": 205.0},
                    },
                ),
                config=_config(),
            )
            ann_id = draw_result.artifact["annotation_id"]
            writer.reset_mock()

            result = await manage_chart_annotations.ainvoke(
                _tool_call(
                    "manage_chart_annotations",
                    {"symbol": "NVDA", "action": "remove", "ids": [ann_id]},
                ),
                config=_config(),
            )

        assert result.artifact["removed"] == 1
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert ann_id not in fake_db.bucket("ws_abc", "NVDA:1day")
        # SSE remove event emitted with identity
        writer.assert_called_once()
        payload = writer.call_args[0][0]["payload"]
        assert payload["op"] == "remove"
        assert payload["chart_id"] == "NVDA:1day"
        assert payload["workspace_id"] == "ws_abc"
        assert payload["ids"] == [ann_id]

    @pytest.mark.asyncio
    async def test_remove_requires_non_empty_ids(self, fake_db):
        result = await manage_chart_annotations.ainvoke(
            _tool_call(
                "manage_chart_annotations",
                {"symbol": "NVDA", "action": "remove"},
            ),
            config=_config(),
        )
        assert "requires" in result.content.lower()
        assert result.artifact == {}

    @pytest.mark.asyncio
    async def test_clear_all_removes_everything(self, fake_db):
        writer = MagicMock()
        with patch("langgraph.config.get_stream_writer", return_value=writer):
            for price in (200.0, 210.0, 220.0):
                await draw_chart_annotation.ainvoke(
                    _tool_call(
                        "draw_chart_annotation",
                        {
                            "symbol": "NVDA",
                            "annotation": {"type": "price_line", "price": price},
                        },
                    ),
                    config=_config(),
                )
            writer.reset_mock()

            result = await manage_chart_annotations.ainvoke(
                _tool_call(
                    "manage_chart_annotations",
                    {"symbol": "NVDA", "action": "clear_all"},
                ),
                config=_config(),
            )

        assert result.artifact["cleared"] == 3
        assert result.artifact["chart_id"] == "NVDA:1day"
        assert fake_db.bucket("ws_abc", "NVDA:1day") == {}
        writer.assert_called_once()
        payload = writer.call_args[0][0]["payload"]
        assert payload["op"] == "clear"
        assert payload["chart_id"] == "NVDA:1day"
        assert payload["workspace_id"] == "ws_abc"

    @pytest.mark.asyncio
    async def test_clear_all_rejects_ids(self, fake_db):
        result = await manage_chart_annotations.ainvoke(
            _tool_call(
                "manage_chart_annotations",
                {"symbol": "NVDA", "action": "clear_all", "ids": ["ann_1"]},
            ),
            config=_config(),
        )
        assert "does not accept" in result.content.lower()
        assert result.artifact == {}


# --------------------------------------------------------------------------- #
# Registry visibility
# --------------------------------------------------------------------------- #


class TestSkillRegistryVisibility:
    def test_chart_annotation_registered_for_both_modes(self):
        from src.ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY

        skill = SKILL_REGISTRY["chart-annotation"]
        # Discoverable in both modes so the agent can self-load it on demand
        # (including from the standalone chat page, where it renders a card).
        assert skill.exposure == "both"
        tool_names = skill.get_tool_names()
        assert set(tool_names) == {"draw_chart_annotation", "manage_chart_annotations"}

    def test_appears_in_default_listing(self):
        """chart-annotation must be in the manifest the LLM sees in every mode.

        ``list_skills`` drives what gets injected into the system prompt via
        SkillsMiddleware. The agent can only choose to load a skill it can see,
        so a discoverable skill must appear here.
        """
        from src.ptc_agent.agent.middleware.skills.registry import list_skills

        for mode in ("ptc", "flash", None):
            listed = {s["name"] for s in list_skills(mode=mode)}
            assert "chart-annotation" in listed, (
                f"chart-annotation missing from listing for mode={mode}"
            )

    def test_synced_to_sandbox(self):
        """exposure=both ⇒ SKILL.md is uploaded so PTC can self-load by reading it."""
        from src.ptc_agent.agent.middleware.skills.registry import (
            get_sandbox_skill_names,
        )

        assert "chart-annotation" in get_sandbox_skill_names()

    def test_reachable_by_name_in_every_mode(self):
        from src.ptc_agent.agent.middleware.skills.registry import get_skill

        for mode in ("ptc", "flash", None):
            skill = get_skill("chart-annotation", mode=mode)
            assert skill is not None, f"get_skill returned None for mode={mode}"
            assert skill.name == "chart-annotation"
