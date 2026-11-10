"""117 turns each chart's legacy annotations into a valid chart layer.

The payloads are the shapes ``draw_chart_annotation`` stored: the annotation
model's ``model_dump()`` (unset fields as None) plus the chart's keys.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from chartkit.schema import MAX_DRAWINGS, MAX_LABEL, ChartLayer

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "117_legacy_annotations_to_layers.py"
)


@pytest.fixture(scope="module")
def m():
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("alembic.op", MagicMock())
        spec = importlib.util.spec_from_file_location("migration_116", _PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def _stored(n: int, **fields) -> dict:
    return {
        "annotation_id": f"ann_{n:032x}",
        "symbol": "NVDA",
        "timeframe": "1day",
        "chart_id": "NVDA:1day",
        **fields,
    }


PRICE_LINE = _stored(1, type="price_line", price=205.5, label="Resistance 205", color="#ef5350", style="dashed")
TRENDLINE = _stored(
    2,
    type="trendline",
    point1={"time": "2024-10-16T00:00:00Z", "price": 145.2},
    point2={"time": "2024-12-20T00:00:00Z", "price": 138.7},
    label="Channel top",
    color=None,
)
MARKER = _stored(
    3, type="marker", time="2024-11-14T00:00:00Z", shape="arrowUp", position="belowBar",
    text="Earnings beat", color="green",
)
VERTICAL_LINE = _stored(4, type="vertical_line", time="2024-11-14T00:00:00Z", label="FOMC", color=None, style="dashed")
RECTANGLE = _stored(
    5,
    type="rectangle",
    point1={"time": "2024-10-16T00:00:00Z", "price": 150.0},
    point2={"time": "2024-11-20T00:00:00Z", "price": 140.0},
    label="Demand zone",
    color="rgba(79, 138, 214, 0.7)",
)
TEXT = _stored(6, type="text", time="2024-11-14T00:00:00Z", price=150.0, text="Gap fill", color=None)
EVENT = _stored(
    7, type="event", time="2024-11-14T00:00:00Z", price=148.9, title="Q3 earnings beat",
    detail="Revenue rose 94% on data-center demand; guidance above consensus.", color=None,
)
FIB = _stored(
    8,
    type="fib_retracement",
    point1={"time": "2024-10-16T00:00:00Z", "price": 100.0},
    point2={"time": "2024-12-20T00:00:00Z", "price": 200.0},
    label="Oct-Dec move",
    color=None,
)
EVERY_TYPE = [PRICE_LINE, TRENDLINE, MARKER, VERTICAL_LINE, RECTANGLE, TEXT, EVENT, FIB]


def _one(m, ann, timeframe="1day"):
    layers, dropped = m.chart_layers("NVDA", timeframe, [ann])
    assert dropped == 0
    assert len(layers) == 1
    return layers[0]


def test_every_legacy_type_maps_to_its_nearest_element(m):
    layers, dropped = m.chart_layers("NVDA", "1day", EVERY_TYPE)

    assert dropped == 0
    [layer] = layers
    ChartLayer.model_validate(layer)
    assert layer["id"] == "earlier-annotations"
    assert layer["title"] == "Earlier annotations"
    assert (layer["symbol"], layer["timeframe"]) == ("NVDA", "1day")
    assert layer["levels"] == [
        {"price": 205.5, "pane": "price", "title": "Resistance 205", "color": "#ef5350", "style": "dashed"}
    ]
    assert layer["markers"] == [
        {"time": "2024-11-14", "position": "belowBar", "shape": "arrowUp", "color": "#008000", "text": "Earnings beat"}
    ]
    assert layer["drawings"] == [
        {
            "type": "segment", "label": "Channel top",
            "p1": {"time": "2024-10-16", "price": 145.2}, "p2": {"time": "2024-12-20", "price": 138.7},
        },
        {"type": "vline", "label": "FOMC", "time": "2024-11-14", "style": "dashed"},
        {
            "type": "box", "label": "Demand zone", "color": "rgba(79, 138, 214, 0.7)",
            "p1": {"time": "2024-10-16", "price": 150.0}, "p2": {"time": "2024-11-20", "price": 140.0},
        },
        {"type": "label", "at": {"time": "2024-11-14", "price": 150.0}, "text": "Gap fill"},
        {
            "type": "label", "color": "#E9954A", "at": {"time": "2024-11-14", "price": 148.9},
            "text": "Q3 earnings beat",
            "note": "Revenue rose 94% on data-center demand; guidance above consensus.",
        },
        {
            "type": "fib", "label": "Oct-Dec move",
            "p1": {"time": "2024-10-16", "price": 100.0}, "p2": {"time": "2024-12-20", "price": 200.0},
        },
    ]


def test_loading_the_migration_does_not_load_chartkit():
    """``alembic history`` and a database with nothing to convert never pay
    for chartkit's schema; the converters load it on first use."""
    import subprocess
    import sys

    probe = (
        "import importlib.util, sys; from unittest.mock import MagicMock; "
        "import alembic; alembic.op = MagicMock(); "
        f"spec = importlib.util.spec_from_file_location('m116', {str(_PATH)!r}); "
        "mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); "
        "print('chartkit' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


@pytest.mark.parametrize(
    ("value", "timeframe", "expected"),
    [
        ("2024-11-14T00:00:00Z", "1day", "2024-11-14"),
        ("2024-11-14", "1day", "2024-11-14"),
        # The old chart put a time on the nearest bar.
        ("2024-11-14T15:00:00Z", "1day", "2024-11-15"),
        ("2024-11-14T20:00:00-05:00", "1day", "2024-11-15"),
        ("2024-11-14T14:30:00Z", "1hour", "2024-11-14T14:30:00"),
        ("2024-11-14T09:30:00-05:00", "5min", "2024-11-14T14:30:00"),
        ("2024-11-14T14:30:12.345+00:00", "1min", "2024-11-14T14:30:12"),
        ("2024-11-14T14:30", "15min", "2024-11-14T14:30:00"),
        ("next tuesday", "1day", None),
        ("", "1day", None),
        (None, "1day", None),
    ],
)
def test_times_land_where_the_old_chart_drew_them(m, value, timeframe, expected):
    assert m.layer_time(value, timeframe) == expected


def test_a_colour_the_layer_refuses_falls_back(m):
    layer = _one(m, {**PRICE_LINE, "color": "hsl(120, 50%, 50%)"})
    assert "color" not in layer["levels"][0]
    layer = _one(m, {**PRICE_LINE, "color": " Orange "})
    assert layer["levels"][0]["color"] == "#ffa500"


def test_words_longer_than_the_layer_allows_are_cut_and_kept_in_the_note(m):
    long = "Resistance from the March double top " * 5
    layer = _one(m, {**PRICE_LINE, "label": long[:200]})
    level = layer["levels"][0]
    assert len(level["title"]) == MAX_LABEL and level["title"].endswith("…")
    assert level["note"] == " ".join(long[:200].split())

    marker = _one(m, {**MARKER, "text": long[:200]})["markers"][0]
    assert len(marker["text"]) == MAX_LABEL and marker["note"].startswith("Resistance")


def test_an_event_without_a_title_shows_its_detail(m):
    label = _one(m, {**EVENT, "title": "  "})["drawings"][0]
    assert label["text"] == EVENT["detail"]


@pytest.mark.parametrize(
    "ann",
    [
        {**PRICE_LINE, "price": None},
        {**PRICE_LINE, "price": True},
        {**TRENDLINE, "point2": None},
        {**RECTANGLE, "point1": {"time": "garbage", "price": 1.0}},
        {**MARKER, "time": "not a time"},
        {**VERTICAL_LINE, "time": None},
        {**TEXT, "text": ""},
        {**EVENT, "title": "", "detail": ""},
        {**FIB, "point1": {"time": "2024-10-16T00:00:00Z"}},
        {"type": "arrow", "time": "2024-11-14"},
        "not an object",
    ],
)
def test_an_annotation_that_cannot_be_drawn_is_dropped(m, ann):
    layers, dropped = m.chart_layers("NVDA", "1day", [PRICE_LINE, ann])
    assert dropped == 1
    assert len(layers[0]["levels"]) == 1 and not layers[0]["drawings"] and not layers[0]["markers"]


def test_a_chart_with_nothing_drawable_gets_no_layer(m):
    assert m.chart_layers("NVDA", "1day", [{"type": "bogus"}]) == ([], 1)


def test_a_chart_no_layer_can_name_drops_its_annotations(m):
    assert m.chart_layers("NVDA", "1week", [PRICE_LINE, MARKER]) == ([], 2)


def test_more_drawings_than_a_layer_holds_spill_into_another(m):
    anns = [TRENDLINE] * (MAX_DRAWINGS + 5) + [PRICE_LINE]
    layers, dropped = m.chart_layers("NVDA", "1day", anns)
    assert dropped == 0
    assert [(lyr["id"], lyr["title"]) for lyr in layers] == [
        ("earlier-annotations", "Earlier annotations"),
        ("earlier-annotations-2", "Earlier annotations (2)"),
    ]
    assert [len(lyr["drawings"]) for lyr in layers] == [MAX_DRAWINGS, 5]
    assert [len(lyr["levels"]) for lyr in layers] == [0, 1]
    for lyr in layers:
        ChartLayer.model_validate(lyr)


@pytest.mark.parametrize(
    "payload",
    [[1, 2], "not json", "[1]", 7, None],
    ids=["jsonb-array", "text-not-json", "text-array", "scalar", "null"],
)
def test_a_payload_that_is_not_an_object_is_no_annotation(m, payload):
    assert m.stored_annotation(payload) is None


def test_a_payload_read_as_text_is_parsed(m):
    assert m.stored_annotation('{"type": "price_line"}') == {"type": "price_line"}


def test_a_chart_is_spelled_as_the_server_stores_it(m):
    assert m.stored_symbol("brk.b") == "BRK-B"
    assert m.stored_symbol("NVDA") == "NVDA"


def test_why_no_layer_can_name_a_chart(m):
    assert m.chart_refusal("NVDA", "1day") is None
    assert "symbol" in m.chart_refusal("BRK B", "1day")
    assert "timeframe" in m.chart_refusal("NVDA", "1week")


def test_layers_are_numbered_past_the_ids_the_chart_has(m):
    anns = [TRENDLINE] * (MAX_DRAWINGS + 5)
    layers, _ = m.chart_layers("NVDA", "1day", anns, taken={"earlier-annotations", "earlier-annotations-3"})
    assert [(lyr["id"], lyr["title"]) for lyr in layers] == [
        ("earlier-annotations-2", "Earlier annotations (2)"),
        ("earlier-annotations-4", "Earlier annotations (4)"),
    ]


# -- The conversion, on a stand-in connection -----------------------------------


class _Row(SimpleNamespace):
    pass


class _Bind:
    """Answers the conversion's statements: the legacy rows, the layer ids
    the charts have, and an insert that lands unless its id is in ``refuse``."""

    def __init__(self, rows, *, taken=(), refuse=()):
        self.rows, self.taken, self.refuse = rows, list(taken), set(refuse)
        self.inserted: list[dict] = []
        self.deleted: list[str] = []

    def execute(self, clause, params=None):
        sql = str(clause)
        if "SELECT workspace_id, chart_id, symbol" in sql:
            return MagicMock(fetchall=lambda: self.rows)
        if "SELECT c.workspace_id" in sql:
            return MagicMock(fetchall=lambda: self.taken)
        if "INSERT" in sql:
            landed = params["id"] not in self.refuse
            if landed:
                self.inserted.append(params)
            return MagicMock(rowcount=int(landed))
        if "DELETE" in sql:
            self.deleted.extend(params["id"])
        return MagicMock()


WS = "00000000-0000-4000-8000-000000000001"
T0 = datetime(2026, 1, 5, tzinfo=timezone.utc)


def _row(n: int, chart_id: str, payload, *, symbol=None) -> _Row:
    symbol = symbol or chart_id.split(":")[0]
    return _Row(
        workspace_id=WS, chart_id=chart_id, symbol=symbol, timeframe=chart_id.split(":")[1],
        annotation_id=f"ann_{n:032x}", payload=payload, created_at=T0 + timedelta(minutes=n),
    )


def test_a_chart_no_layer_can_name_is_logged_and_the_rest_convert(m, caplog):
    bind = _Bind([
        _row(1, "BRK B:1day", PRICE_LINE),
        _row(2, "NVDA:1day", PRICE_LINE),
    ])
    with caplog.at_level("WARNING", logger="alembic.runtime.migration"):
        m.convert_legacy(bind)
    [warning] = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert f"workspace_id={WS}" in warning and "chart_id='BRK B:1day'" in warning
    assert "symbol" in warning
    assert [(p["chart"], p["id"]) for p in bind.inserted] == [("NVDA:1day", "earlier-annotations")]
    # Dropped, as an undrawable annotation is.
    assert sorted(bind.deleted) == [f"ann_{1:032x}", f"ann_{2:032x}"]


def test_payloads_that_are_not_objects_are_dropped_not_fatal(m):
    bind = _Bind([
        _row(1, "NVDA:1day", [1, 2]),
        _row(2, "NVDA:1day", "not json"),
        _row(3, "NVDA:1day", json.dumps(PRICE_LINE)),
    ])
    m.convert_legacy(bind)
    [layer] = bind.inserted
    assert len(json.loads(layer["payload"])["levels"]) == 1
    assert len(bind.deleted) == 3


def test_two_spellings_of_one_ticker_become_one_chart(m):
    bind = _Bind([
        _row(1, "brk.b:1day", PRICE_LINE, symbol="brk.b"),
        _row(2, "BRK-B:1day", MARKER),
    ])
    m.convert_legacy(bind)
    [layer] = bind.inserted
    assert (layer["chart"], layer["symbol"]) == ("BRK-B:1day", "BRK-B")
    payload = json.loads(layer["payload"])
    assert payload["symbol"] == "BRK-B" and payload["levels"] and payload["markers"]
    assert len(bind.deleted) == 2


def test_a_run_again_numbers_past_the_charts_layer_and_deletes_only_what_it_wrote(m):
    """After a downgrade, the previous build's new drawings convert into a
    layer of their own; a chart whose layer could not be written keeps its
    annotation rows."""
    bind = _Bind(
        [_row(1, "NVDA:1day", PRICE_LINE), _row(2, "AAPL:1day", PRICE_LINE)],
        taken=[_Row(workspace_id=WS, chart_id="NVDA:1day", annotation_id="earlier-annotations")],
        refuse={"earlier-annotations"},
    )
    m.convert_legacy(bind)
    assert [(p["chart"], p["id"]) for p in bind.inserted] == [("NVDA:1day", "earlier-annotations-2")]
    assert json.loads(bind.inserted[0]["payload"])["title"] == "Earlier annotations (2)"
    # AAPL's insert collided: its row stays for a later run.
    assert bind.deleted == [f"ann_{1:032x}"]


def test_upgrade_commits_the_revisions_before_it_first(m, monkeypatch):
    """An upgrade runs every pending revision in one transaction; 116's ALTER
    on command_runs must not wait out the conversion's lock."""
    calls = []
    ctx = MagicMock()
    ctx.autocommit_block.return_value.__enter__.side_effect = lambda *a: calls.append("commit")
    monkeypatch.setattr(m, "op", MagicMock(get_context=lambda: ctx, get_bind=lambda: "bind"))
    monkeypatch.setattr(m, "convert_legacy", lambda bind: calls.append(("convert", bind)))
    m.upgrade()
    assert calls == ["commit", ("convert", "bind")]
