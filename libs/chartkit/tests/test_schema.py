"""ChartLayer v1: what the server accepts, and what it tells the agent when it
refuses. The messages are what the agent reads back in its tool result, so the
cases pin where each one points (element, field) as well as that it refuses."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from chartkit import schema as ls
from chartkit.schema import LayerRejected, parse_layer
from samples import T, sample

_TS = Path(__file__).resolve().parents[3] / "web/src/pages/MarketView/types/chartLayer.ts"


def _problems(layer: dict, **dumps) -> list[str]:
    with pytest.raises(LayerRejected) as caught:
        parse_layer(json.dumps(layer, **dumps))
    return caught.value.problems


@pytest.mark.parametrize("name", ["indicators", "drawings", "intraday"])
def test_samples_validate(name):
    layer = parse_layer(json.dumps(sample(name)))
    assert layer.id == sample(name)["id"]


def test_the_samples_cover_every_drawing_type():
    layer = parse_layer(json.dumps(sample("drawings")))
    assert {d.type for d in layer.drawings} == set(ls.DRAWING_TYPES)


def test_a_stored_layer_keeps_the_wire_names():
    """``from`` is a Python keyword, kept as ``from`` on the wire."""
    layer = parse_layer(json.dumps(sample("indicators")))
    body = layer.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert body["regimes"][0]["from"] == sample("indicators")["regimes"][0]["from"]
    assert parse_layer(json.dumps(body)) == layer


def _mutate(name, fn):
    layer = sample(name)
    fn(layer)
    return layer


@pytest.mark.parametrize(
    ("name", "mutate", "expected"),
    [
        (
            "drawings",
            lambda d: d["drawings"][12].update(stop=700),
            "drawings[12] (position 'long-1'): a long's stop 700 must be below its entry 658.52",
        ),
        (
            "drawings",
            lambda d: d["drawings"][12].update(targets=[650]),
            "drawings[12] (position 'long-1'): a long's targets must be above its entry 658.52; got [650.0]",
        ),
        (
            "drawings",
            lambda d: d["drawings"][0].update(type="ray"),
            "drawings[0]: unknown drawing type 'ray'; expected one of segment, vline",
        ),
        ("drawings", lambda d: d["drawings"][0].pop("type"), "drawings[0]: missing 'type'"),
        (
            "drawings",
            lambda d: d["drawings"][1].update(time="2026-10-28 09:30"),
            "drawings[1] (vline 'earnings').time: time '2026-10-28 09:30' must be 'YYYY-MM-DD' or",
        ),
        (
            "drawings",
            lambda d: d["drawings"][1].update(time="2026-10-28T09:30Z"),
            "drawings[1] (vline 'earnings').time: time '2026-10-28T09:30Z' must be",
        ),
        (
            "drawings",
            lambda d: d["drawings"][1].update(time="2026-02-30"),
            "drawings[1] (vline 'earnings').time: time '2026-02-30' is not a real calendar date/time",
        ),
        (
            "drawings",
            lambda d: d["drawings"][0].update(colour="#fff"),
            "drawings[0] (segment 'trend').colour: is not a field of this element",
        ),
        (
            "drawings",
            lambda d: d["drawings"][0].update(color="url(javascript:x)"),
            "drawings[0] (segment 'trend').color: color 'url(javascript:x)' must be a hex color",
        ),
        (
            "drawings",
            lambda d: d["drawings"][10].update(probability=1.5),
            "drawings[10] (forecast 'retest').probability: must be from 0 to 1, got",
        ),
        (
            "drawings",
            lambda d: d["drawings"][10]["target"].update(time=T[-1]),
            "drawings[10] (forecast 'retest'): target.time",
        ),
        (
            "drawings",
            lambda d: d["drawings"][11]["bars"][1].update(low=250.0),
            "drawings[11] (analog 'June base').bars[1]: bar at",
        ),
        (
            "drawings",
            lambda d: d["drawings"][11]["bars"].reverse(),
            "drawings[11] (analog 'June base'): bars must follow origin.time, in time order",
        ),
        (
            "drawings",
            lambda d: d["drawings"][11].update(source_end=T[10]),
            "drawings[11] (analog 'June base'): the replayed stretch ends at",
        ),
        (
            "drawings",
            lambda d: d["drawings"][11].update(bars=[]),
            "drawings[11] (analog 'June base').bars",
        ),
        (
            "drawings",
            lambda d: d["drawings"][7]["point_labels"].pop(),
            "drawings[7] (path 'H&S'): point_labels has 6 entries but points has 7",
        ),
        (
            "drawings",
            lambda d: d["drawings"][1].update(id="trend"),
            "drawings: id 'trend' is used more than once",
        ),
        ("drawings", lambda d: d.update(id="NVDA Setup"), "id: String should match pattern"),
        ("drawings", lambda d: d.update(timeframe="1d"), "timeframe: must be '1min'"),
        ("drawings", lambda d: d.update(version=2), "version: must be 1, got 2"),
        (
            "drawings",
            lambda d: d["drawings"][14]["rows"][3].update(low=200),
            "drawings[14] (profile).rows[3]: low 200 must be below high 178",
        ),
        (
            "drawings",
            lambda d: d.update(drawings=d["drawings"] * 25),
            "drawings: List should have at most 300 items",
        ),
        (
            "indicators",
            lambda d: d["series"][2].update(pane="macd"),
            "series[2].pane: 'macd' is not a declared pane (declared: price, rsi)",
        ),
        (
            "indicators",
            lambda d: d["fills"][0].update(b="ema50"),
            "fills[0].b: 'ema50' is not a series id in this layer",
        ),
        (
            "indicators",
            lambda d: d["series"][0]["data"].__setitem__(5, dict(d["series"][0]["data"][4])),
            "series[0]: data[5].time '2026-06-05' is not after data[4].time '2026-06-05'",
        ),
        (
            "indicators",
            lambda d: d["panes"].append({"id": "price"}),
            "panes[1]: 'price' is the implicit main pane",
        ),
        (
            "indicators",
            lambda d: d["panes"][0].update(height=0.6),
            "panes[0].height: must be a fraction of the chart height from 0.05 to 0.5, got",
        ),
        (
            "indicators",
            lambda d: d["panes"][0].update(height=0.01),
            "panes[0].height: must be a fraction of the chart height from 0.05 to 0.5, got",
        ),
        (
            "indicators",
            lambda d: d["panes"].extend([{"id": "b", "height": 0.4}, {"id": "c", "height": 0.4}]),
            "panes: lower panes take 1.05 of the chart height; keep their total at or under 0.75",
        ),
        (
            "indicators",
            lambda d: d["fills"][0].update(b="rsi14"),
            "fills[0]: series 'ema20' is in pane 'price' but 'rsi14' is in 'rsi'; a fill needs both in the same pane",
        ),
        (
            "indicators",
            lambda d: d["fills"][0].update(b="ema20"),
            "fills[0]: a and b are both 'ema20'",
        ),
        (
            "indicators",
            lambda d: d["markers"][1].pop("price"),
            "markers[1]: position 'atPrice' needs a price",
        ),
        (
            "indicators",
            lambda d: d["regimes"][0].update({"from": "2026-12-01"}),
            "regimes[0]: it ends at '2026-07-20', before it starts at '2026-12-01'",
        ),
        (
            "indicators",
            lambda d: d["levels"][0].update(note="x" * 601),
            "levels[0].note: String should have at most 600 characters",
        ),
    ],
)
def test_a_bad_layer_says_where_and_what(name, mutate, expected):
    problems = _problems(_mutate(name, mutate))
    assert any(p.startswith(expected) for p in problems), problems


def test_the_same_moment_twice_is_not_increasing():
    """``T09:30`` and ``T09:30:00`` are one moment, however they are spelled."""
    layer = sample("intraday")
    layer["series"] = [
        {
            "id": "vwap",
            "title": "VWAP",
            "data": [
                {"time": "2026-10-29T09:30", "value": 1.0},
                {"time": "2026-10-29T09:30:00", "value": 1.1},
            ],
        }
    ]
    assert _problems(layer)[0].startswith("series[0]: data[1].time '2026-10-29T09:30:00' is not after")


def test_non_finite_numbers_are_refused_even_though_json_lets_them_through():
    layer = sample()
    layer["drawings"][0]["p1"]["price"] = float("nan")
    assert _problems(layer, allow_nan=True) == [
        "drawings[0] (segment 'trend').p1.price: must be a finite number"
    ]


def test_every_cross_reference_problem_is_reported_at_once():
    layer = sample("indicators")
    layer["series"][2]["pane"] = "macd"
    layer["fills"][0]["b"] = "zz"
    layer["series"].append(dict(layer["series"][0]))
    problems = _problems(layer)
    assert len(problems) == 3
    assert problems[0] == "series: id 'ema20' is used more than once; ids must be unique within series"


def test_a_long_list_of_problems_is_cut_to_a_handful():
    layer = sample()
    for drawing in layer["drawings"]:
        drawing["colour"] = "#fff"
    problems = _problems(layer)
    assert len(problems) == 9 and problems[-1] == "... and 7 more problem(s)"


def test_a_layer_over_the_size_cap_is_refused_before_it_is_parsed():
    layer = sample("indicators")
    layer["summary"] = None
    layer["series"][0]["note"] = None
    layer["levels"] = [{"price": 1.0, "title": "x" * 100}] * 200
    line = json.dumps(layer) + " " * ls.MAX_LAYER_BYTES
    with pytest.raises(LayerRejected) as caught:
        parse_layer(line)
    assert caught.value.layer_id == "nvda-trend-kit"
    assert re.match(r"the layer is 1\.\d\d MB serialized, over the 1\.5 MB cap", caught.value.problems[0])


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("{not json", "the line is not valid JSON"),
        ("[1, 2]", "a layer must be a JSON object, got list"),
    ],
)
def test_a_line_that_is_not_a_layer(line, expected):
    with pytest.raises(LayerRejected) as caught:
        parse_layer(line)
    assert caught.value.problems[0].startswith(expected)
    assert caught.value.layer_id is None


@pytest.mark.parametrize(
    "color",
    ["#abc", "#abcd", "#26a69a", "#26a69a80", "rgb(1, 2, 3)", "rgba(1,2,3,0.5)", "rgba(1,2,3,.5)", "rgba(1,2,3,1)"],
)
def test_colors_accepted(color):
    layer = sample()
    layer["drawings"][0]["color"] = color
    parse_layer(json.dumps(layer))


@pytest.mark.parametrize(
    "color", ["red", "var(--x)", "rgb(1,2,3);background:url(x)", "hsl(1,2%,3%)", "#12345"]
)
def test_colors_refused(color):
    layer = sample()
    layer["drawings"][0]["color"] = color
    assert "must be a hex color" in _problems(layer)[0]


# -- The TypeScript mirror ------------------------------------------------------


def _ts_interfaces(text: str) -> dict[str, tuple[str | None, set[str]]]:
    """``{name: (extends, field names)}`` for every interface in the file."""
    found = {}
    for match in re.finditer(
        r"(?:export )?interface (\w+)(?: extends (\w+))? \{(.*?)\n\}", text, re.S
    ):
        fields = set(re.findall(r"^\s+(\w+)\??:", match.group(3), re.M))
        found[match.group(1)] = (match.group(2), fields)
    return found


def _ts_fields(interfaces, name: str) -> set[str]:
    parent, fields = interfaces[name]
    return fields | (_ts_fields(interfaces, parent) if parent else set())


_MIRRORS = {
    "ChartLayer": ls.ChartLayer,
    "LayerPane": ls.LayerPane,
    "LayerSeries": ls.LayerSeries,
    "LayerSeriesPoint": ls.LayerSeriesPoint,
    "LayerFill": ls.LayerFill,
    "LayerLevel": ls.LayerLevel,
    "LayerMarker": ls.LayerMarker,
    "LayerRegime": ls.LayerRegime,
    "LayerAlert": ls.LayerAlert,
    "LayerPoint": ls.LayerPoint,
    "ConeBand": ls.ConeBand,
    "AnalogBar": ls.AnalogBar,
    "ProfileRow": ls.ProfileRow,
    **{f"{m.__name__}": m for m in ls._DRAWINGS},
}


@pytest.mark.skipif(not _TS.exists(), reason="the frontend's types are not in this checkout")
@pytest.mark.parametrize("name", sorted(_MIRRORS))
def test_the_typescript_mirror_has_the_same_fields(name):
    """The frontend trusts a stored layer's shape: every field the server
    keeps, it declares, and it declares none the server would refuse."""
    interfaces = _ts_interfaces(_TS.read_text())
    model = _MIRRORS[name]
    server = {f.alias or n for n, f in model.model_fields.items()}
    assert _ts_fields(interfaces, name) == server


def test_counts_read_a_dict_or_a_parsed_layer_alike():
    layer = sample("indicators")
    kinds = ["series", "levels", "markers", "regimes", "drawings", "alerts"]
    assert ls.counts(layer) == {k: len(layer.get(k, [])) for k in kinds}
    assert ls.counts(layer)["series"] == 4 and ls.counts(layer) == ls.counts(parse_layer(json.dumps(layer)))
    assert ls.counts({}) == dict.fromkeys(kinds, 0)


def test_a_layer_id_is_sniffed_only_when_it_is_shaped_like_one():
    assert ls._sniff_id('{"id": "nvda-1", "x"') == "nvda-1"
    assert ls._sniff_id('{"id": "Bad Id"') is None
    assert ls._sniff_id('{"id": "' + "a" * 49 + '"') is None
    assert ls._sniff_id('{"id": "abc\\n"') is None
