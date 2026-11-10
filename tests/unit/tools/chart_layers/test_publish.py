"""Publishing a run's chart layers: stored, then streamed, then read back to
the agent with the coordinates each element carries; refused, failed or
unplaced layers say NOT published and why."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.jobs import Outbox
from src.server.models.chart_layer import LayerRef
from src.tools.chart_annotation import layers as publish
# The layer contract's own samples, kept with chartkit's suite.
from libs.chartkit.tests.samples import sample

WS = "00000000-0000-4000-8000-00000000aaaa"
_ADD = "src.tools.chart_annotation.layers.add_layers"


@pytest.fixture
def writer():
    w = MagicMock()
    with patch("langgraph.config.get_stream_writer", return_value=w):
        yield w


@pytest.fixture
def store():
    add = AsyncMock()
    with patch(_ADD, add):
        yield add


def _lines(*names: str) -> list[str]:
    return [json.dumps(sample(n)) for n in names]


async def _publish(lines, workspace_id=WS, *, truncated=False) -> publish.Published:
    return await publish.publish_outbox(Outbox(tuple(lines), truncated), workspace_id)


def _stored(store) -> list[tuple[LayerRef, dict]]:
    """Every row stored, across the store's calls."""
    return [row for call in store.await_args_list for row in call.args[0]]


@pytest.mark.asyncio
async def test_a_layer_is_stored_then_streamed_and_read_back(store, writer):
    result = await _publish(_lines("drawings"))

    store.assert_awaited_once()
    [(ref, body)] = _stored(store)
    # The row is keyed by the layer's own id and holds the layer alone.
    assert ref == LayerRef(WS, "NVDA:1day", "NVDA", "1day", "nvda-setup")
    assert body["id"] == "nvda-setup" and body["drawings"][14]["from"]

    event = writer.call_args.args[0]
    assert event["artifact_type"] == "chart_layer" and event["artifact_id"] == "nvda-setup"
    assert event["payload"] == {
        "op": "add",
        "workspace_id": WS,
        "chart_id": "NVDA:1day",
        "symbol": "NVDA",
        "timeframe": "1day",
        "layer_id": "nvda-setup",
        "layer": body,
    }
    assert result.layers == [
        {
            "chart_id": "NVDA:1day",
            "symbol": "NVDA",
            "timeframe": "1day",
            "workspace_id": WS,
            "layer_id": "nvda-setup",
            "title": "NVDA channel, long setup and a retest forecast",
            "summary": "Rising channel since June; long at the lower rail with 2 targets.",
            "as_of": "2026-10-28",
        }
    ]


@pytest.mark.asyncio
async def test_the_read_back_lists_each_element_with_its_coordinates(store, writer):
    result = await _publish(_lines("drawings", "indicators"))
    text = result.text
    assert text.startswith(
        "Published chart layer nvda-setup 'NVDA channel, long setup and a retest forecast' "
        "on NVDA:1day, as of 2026-10-28:\n"
    )
    for line in (
        "  segment 'trend': 140 @2026-06-08 → 190 @2026-10-05, extend right",
        "  channel 'chan': p1 148 @2026-06-01, p2 196.4 @2026-10-19, p3 182.1 @2026-07-27",
        "  box 'Demand zone': 170–176, 2026-08-24 → 2026-09-21",
        "  position 'long-1': long entry 658.52 @2026-10-28 stop 618.59 targets 738.38, 778.32 "
        "(R 2.0, 3.0) until 2026-12-03",
        "  forecast 'retest': 196 @2026-10-30 → 215 by 2026-11-12 (+9.7%), 35%",
        "  analog 'June base': 2026-06-29 → 2026-07-02 replayed from 196 @2026-10-30, 4 bars to 2026-11-05, ending 204 (+4.1%)",
        "  measure: 150.2 @2026-06-29 → 205 @2026-09-01 (+54.8, +36.5%)",
        "  profile: 2026-07-27 → 2026-10-30, 14 rows 170–198, POC 183, value area 176–190",
        "  series ema20 'EMA 20' (price, line): 110 points, 2026-06-01 → 2026-10-30, last 204.5",
        "  levels: 70 'OB' (rsi); 30 'OS' (rsi); 205.5 'Resistance'",
        "  markers: 2, 2026-06-29 → 2026-08-24",
        "  alert break-205: price crosses above 205.5",
    ):
        assert f"\n{line}\n" in f"{text}\n", line


@pytest.mark.asyncio
async def test_agent_text_reaches_the_read_back_quoted_and_bounded(store, writer):
    """Agent strings are quoted, so a NUL (which Postgres text refuses, and the
    handed-off read-back is stored) never lands raw; the whole is capped."""
    layer = sample("drawings")
    layer["drawings"] = [
        {"type": "path", "points": [{"time": "2026-06-01", "price": 1}, {"time": "2026-06-02", "price": 2}],
         "point_labels": ["a\u0000b", "c"]}
    ]
    text = (await _publish([json.dumps(layer)])).text
    assert "\x00" not in text and "'a\\x00b' 1 @2026-06-01" in text

    deep = "[" * 200_000 + "]" * 200_000
    text = (await _publish([deep])).text
    assert "NOT published" in text and "not valid JSON" in text

    many = [json.dumps({**sample("drawings"), "id": f"l{n}"}) for n in range(20)]
    text = (await _publish(many)).text
    assert len(text) <= publish.MAX_READ_BACK_CHARS + 200 and "cut at" in text


@pytest.mark.asyncio
async def test_a_refused_layer_says_not_published_and_why_and_the_rest_still_publish(store, writer):
    bad = sample()
    bad["id"] = "bad-one"
    bad["drawings"][12]["stop"] = 700
    result = await _publish([json.dumps(bad), *_lines("intraday")], workspace_id=WS
    )
    assert result.text.startswith(
        "Chart layer 'bad-one' NOT published (nothing was drawn):\n"
        "  drawings[12] (position 'long-1'): a long's stop 700 must be below its entry 658.52\n"
    )
    assert [layer["layer_id"] for layer in result.layers] == ["spy-orb"]
    # The symbol is stored in its one spelling, in the row and the layer alike.
    [(ref, body)] = _stored(store)
    assert ref.symbol == "SPY" and body["symbol"] == "SPY"
    assert writer.call_count == 1


@pytest.mark.asyncio
async def test_a_line_with_no_id_is_named_by_its_place(store, writer):
    result = await _publish(["{oops", *_lines("intraday")])
    assert result.text.startswith("Chart layer #1 of this run NOT published (nothing was drawn):\n")


@pytest.mark.asyncio
async def test_a_layer_that_could_not_be_saved_is_not_streamed(writer):
    with patch(_ADD, AsyncMock(side_effect=RuntimeError("db down"))):
        result = await _publish(_lines("drawings"))
    assert result.layers == []
    assert result.text == (
        "Chart layer 'nvda-setup' NOT published: it could not be saved (storage "
        "unavailable), so nothing was drawn. Publish it again in a moment."
    )
    writer.assert_not_called()


@pytest.mark.asyncio
async def test_a_run_s_layers_are_stored_all_or_none(writer):
    """One write stores every layer of the run; when it fails none is
    streamed and the agent is told every one of them is not published."""
    store = AsyncMock(side_effect=RuntimeError("db down"))
    with patch(_ADD, store):
        result = await _publish(_lines("drawings", "intraday"))
    store.assert_awaited_once()
    assert [ref.layer_id for ref, _ in store.await_args.args[0]] == ["nvda-setup", "spy-orb"]
    assert result.layers == []
    assert result.text == (
        "Chart layers 'nvda-setup', 'spy-orb' NOT published: they could not be saved "
        "(storage unavailable), so nothing was drawn. Publish them again in a moment."
    )
    writer.assert_not_called()


@pytest.mark.asyncio
async def test_layers_stream_only_after_the_write_commits(writer):
    order: list[str] = []
    writer.side_effect = lambda _: order.append("event")
    with patch(_ADD, AsyncMock(side_effect=lambda rows: order.append("store"))):
        await _publish(_lines("drawings", "intraday"))
    assert order == ["store", "event", "event"]


@pytest.mark.asyncio
async def test_publishing_never_raises(writer):
    """The job has ended either way: a bug in publishing is told, not raised
    into the call or the watcher's settle."""
    with patch.object(publish, "_publish", AsyncMock(side_effect=RuntimeError("bug"))):
        result = await _publish(_lines("drawings"))
    assert result == publish.Published(
        "Chart layers NOT published: they could not be processed. "
        "Publish them again in a moment."
    )


@pytest.mark.asyncio
async def test_a_run_with_no_workspace_publishes_nothing(store, writer):
    result = await _publish(_lines("drawings", "intraday"), None)
    assert result.text == "2 chart layers NOT published: this run has no workspace to keep them in."
    store.assert_not_awaited()
    writer.assert_not_called()


@pytest.mark.asyncio
async def test_an_id_published_twice_on_one_chart_keeps_the_last(store, writer):
    first, second = sample(), sample()
    second["title"] = "revised"
    result = await _publish([json.dumps(first), json.dumps(second)], workspace_id=WS
    )
    store.assert_awaited_once()
    assert result.layers[0]["title"] == "revised"
    assert result.text.startswith(
        "Chart layer 'nvda-setup' was published more than once on NVDA:1day in this run; "
        "only the last one stands."
    )


@pytest.mark.asyncio
async def test_a_run_publishes_at_most_its_cap(store, writer):
    lines = []
    for i in range(publish.MAX_LAYERS_PER_RUN + 3):
        layer = sample("intraday")
        layer["id"] = f"orb-{i}"
        lines.append(json.dumps(layer))
    result = await _publish(lines)
    assert len(_stored(store)) == publish.MAX_LAYERS_PER_RUN
    assert result.text.startswith(
        f"This run published {publish.MAX_LAYERS_PER_RUN + 3} chart layers; only the first "
        f"{publish.MAX_LAYERS_PER_RUN} were considered and the rest were NOT published."
    )


@pytest.mark.asyncio
async def test_a_cut_chart_file_says_so(store, writer):
    result = await _publish(_lines("intraday"), truncated=True)
    assert result.text.startswith(
        "Chart layers past the first 4 MB this run published were cut off and NOT published."
    )
    assert len(result.layers) == 1


@pytest.mark.asyncio
async def test_a_run_that_published_nothing_adds_nothing(store, writer):
    assert await _publish([]) == publish.Published()


def test_layer_counts_for_a_listing():
    assert publish.layer_counts(sample("indicators")) == (
        "4 series, 3 levels, 2 markers, 2 regimes, 1 alert"
    )
    assert publish.layer_counts({}) == "no elements"


@pytest.mark.asyncio
async def test_layers_are_validated_off_the_event_loop(store, writer):
    """Up to 4 MB of JSON through pydantic would hold the loop: the outbox
    is parsed, validated and dumped in a worker thread, once per outbox."""
    import threading

    seen: list[str] = []
    real = publish._stage

    def stage(*args):
        seen.append(threading.current_thread().name)
        return real(*args)

    with patch.object(publish, "_stage", side_effect=stage):
        await _publish(_lines("drawings", "intraday"))
        await publish.stage_outbox(Outbox(tuple(_lines("intraday"))), WS)
    assert len(seen) == 2 and threading.main_thread().name not in seen


@pytest.mark.asyncio
async def test_a_large_layers_add_names_it_without_its_body(store, writer):
    """Every event goes into the run's Redis buffer: a layer past
    ``MAX_STREAMED_LAYER_BYTES`` streams its address alone, and the chart
    refetches it; a small one streams whole."""
    from datetime import date, timedelta

    big = sample("indicators")
    big["id"] = "big"
    start = date(2020, 1, 1)
    big["series"][0]["data"] = [
        {"time": (start + timedelta(days=i)).isoformat(), "value": 100.0 + i} for i in range(2000)
    ]
    await _publish([json.dumps(big), *_lines("intraday")])

    events = {c.args[0]["artifact_id"]: c.args[0]["payload"] for c in writer.call_args_list}
    assert "layer" not in events["big"]
    assert events["big"] == {
        "op": "add", "workspace_id": WS, "chart_id": "NVDA:1day", "symbol": "NVDA",
        "timeframe": "1day", "layer_id": "big",
    }
    [small] = [p for k, p in events.items() if k != "big"]
    assert small["layer"]["id"] == small["layer_id"]
    # Stored whole all the same.
    assert len(dict((ref.layer_id, body) for ref, body in _stored(store))["big"]["series"][0]["data"]) == 2000
