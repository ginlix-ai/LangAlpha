"""117 against real PostgreSQL: each chart's legacy annotations become layers.

Seeds a database at 116 with the rows ``draw_chart_annotation`` wrote (every
legacy type, as its model dumped them, on charts in two workspaces and two
timeframes, beside a chart's existing layer), upgrades it, and reads back what
the chart now serves. Then the documented rollback, which changes nothing.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

from chartkit.schema import ChartLayer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database: this test downgrades the schema, which the shared
#: session-scoped one cannot survive.
SCRATCH_DB = "langalpha_migration_116"

USER = "user-117"
WS_A = str(uuid.uuid4())
WS_B = str(uuid.uuid4())
T0 = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)


def _uri(dbname: str) -> str:
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _alembic(uri: str, action: str, revision: str) -> None:
    """Threaded, as the suite's upgrade helper is: a migration calls
    ``asyncio.run`` internally."""
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1))
    await asyncio.to_thread({"upgrade": command.upgrade, "downgrade": command.downgrade}[action], cfg, revision)


@pytest_asyncio.fixture
async def scratch():
    admin = _uri("postgres")

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    try:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            # Earlier migrations read the bundled servers; keep them as shipped.
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            yield _uri(SCRATCH_DB)
    finally:
        await _recreate(create=False)


def _legacy(n: int, symbol: str, timeframe: str, **fields) -> tuple[str, dict]:
    """A row as ``draw_chart_annotation`` stored it: the chart's keys and the
    annotation model's ``model_dump()``, unset fields as None."""
    annotation_id = f"ann_{uuid.UUID(int=n).hex}"
    return annotation_id, {
        "annotation_id": annotation_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "chart_id": f"{symbol}:{timeframe}",
        **fields,
    }


NVDA_DAY = [
    _legacy(1, "NVDA", "1day", type="price_line", price=205.0, label="Resistance 205", color=None, style="solid"),
    _legacy(
        2, "NVDA", "1day", type="trendline",
        point1={"time": "2024-10-16T00:00:00Z", "price": 145.2},
        point2={"time": "2024-12-20T00:00:00Z", "price": 138.7}, label=None, color="#ff9800",
    ),
    _legacy(
        3, "NVDA", "1day", type="marker", time="2024-11-14T00:00:00Z", shape="arrowUp",
        position="aboveBar", text="Earnings beat", color=None,
    ),
    _legacy(4, "NVDA", "1day", type="vertical_line", time="2024-12-18T19:00:00Z", label="FOMC", color=None, style="dashed"),
    _legacy(
        5, "NVDA", "1day", type="rectangle",
        point1={"time": "2024-10-16T00:00:00Z", "price": 150.0},
        point2={"time": "2024-11-20T00:00:00Z", "price": 140.0}, label="Demand zone", color="blue",
    ),
    _legacy(6, "NVDA", "1day", type="text", time="2024-11-14T00:00:00Z", price=152.0, text="Gap fill", color=None),
    _legacy(
        7, "NVDA", "1day", type="event", time="2024-11-20T00:00:00Z", price=146.0,
        title="Q3 earnings", detail="Beat on revenue; guided above consensus.", color=None,
    ),
    _legacy(
        8, "NVDA", "1day", type="fib_retracement",
        point1={"time": "2024-10-16T00:00:00Z", "price": 100.0},
        point2={"time": "2024-12-20T00:00:00Z", "price": 150.0}, label="Oct-Dec", color=None,
    ),
    # Never drawable: the old chart skipped it too.
    _legacy(9, "NVDA", "1day", type="marker", time="sometime", shape="circle", position="aboveBar", text=None, color=None),
]
NVDA_HOUR = [
    _legacy(
        10, "NVDA", "1hour", type="marker", time="2024-11-14T09:30:00-05:00", shape="circle",
        position="belowBar", text="Open", color=None,
    ),
]
AAPL_DAY_B = [_legacy(11, "AAPL", "1day", type="price_line", price=230.5, label=None, color=None, style="dotted")]

# A layer already on a chart: its row id is its own id, its payload the layer.
EXISTING = {"setup": {"version": 1, "id": "setup", "title": "Setup"}}


async def _seed(uri: str) -> None:
    insert = (
        "INSERT INTO chart_annotations"
        " (workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at)"
        " VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)"
    )
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        await conn.execute("INSERT INTO users (user_id, email) VALUES (%s, %s)", (USER, "u115@example.com"))
        for ws, name in ((WS_A, "Research"), (WS_B, "Other")):
            await conn.execute(
                "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status)"
                " VALUES (%s, %s, %s, %s, 'running')",
                (ws, USER, name, name.lower()),
            )
        for k, (layer_id, payload) in enumerate(EXISTING.items()):
            await conn.execute(insert, (WS_A, "NVDA:1day", "NVDA", "1day", layer_id, json.dumps(payload), T0 - timedelta(days=1, minutes=k)))
        for ws, rows in ((WS_A, NVDA_DAY + NVDA_HOUR), (WS_B, AAPL_DAY_B)):
            for k, (annotation_id, payload) in enumerate(rows):
                await conn.execute(
                    insert,
                    (ws, payload["chart_id"], payload["symbol"], payload["timeframe"], annotation_id,
                     json.dumps(payload), T0 + timedelta(minutes=k)),
                )


async def _rows(uri: str) -> dict[tuple[str, str, str], tuple[dict, datetime]]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT workspace_id::text, chart_id, annotation_id, payload, created_at FROM chart_annotations"
        )
        return {(ws, chart, aid): (payload, at) for ws, chart, aid, payload, at in await cur.fetchall()}


async def test_each_charts_annotations_become_one_layer_and_the_rows_go(scratch):
    await _alembic(scratch, "upgrade", "116")
    await _seed(scratch)

    await _alembic(scratch, "upgrade", "117")
    rows = await _rows(scratch)

    assert set(rows) == {
        (WS_A, "NVDA:1day", "earlier-annotations"),
        (WS_A, "NVDA:1day", "setup"),
        (WS_A, "NVDA:1hour", "earlier-annotations"),
        (WS_B, "AAPL:1day", "earlier-annotations"),
    }
    # The chart's own layer is untouched.
    for layer_id, payload in EXISTING.items():
        assert rows[(WS_A, "NVDA:1day", layer_id)][0] == payload

    # Stored as a published layer is: the row id is the layer's id and the
    # payload the layer itself.
    payload, at = rows[(WS_A, "NVDA:1day", "earlier-annotations")]
    assert at == T0
    layer = ChartLayer.model_validate(payload)
    assert (layer.id, layer.title, layer.symbol, layer.timeframe) == (
        "earlier-annotations", "Earlier annotations", "NVDA", "1day",
    )
    assert [(lv.price, lv.title) for lv in layer.levels] == [(205.0, "Resistance 205")]
    assert [(m.time, m.text) for m in layer.markers] == [("2024-11-14", "Earnings beat")]
    assert [d.type for d in layer.drawings] == ["segment", "vline", "box", "label", "label", "fib"]
    vline, box, event = layer.drawings[1], layer.drawings[2], layer.drawings[4]
    assert (vline.time, vline.label) == ("2024-12-19", "FOMC")
    assert box.color == "#0000ff"
    assert (event.text, event.note) == ("Q3 earnings", "Beat on revenue; guided above consensus.")

    hour = ChartLayer.model_validate(rows[(WS_A, "NVDA:1hour", "earlier-annotations")][0])
    assert [(m.time, m.text, m.position) for m in hour.markers] == [("2024-11-14T14:30:00", "Open", "belowBar")]
    aapl = ChartLayer.model_validate(rows[(WS_B, "AAPL:1day", "earlier-annotations")][0])
    assert [(lv.price, lv.style) for lv in aapl.levels] == [(230.5, "dotted")]

    # The rollback changes nothing, and the way up again finds nothing to do.
    await _alembic(scratch, "downgrade", "116")
    assert await _rows(scratch) == rows
    await _alembic(scratch, "upgrade", "117")
    assert await _rows(scratch) == rows


async def _insert_raw(uri: str, ws: str, chart_id: str, symbol: str, timeframe: str, annotation_id: str, payload_sql: str, at: datetime) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        await conn.execute(
            "INSERT INTO chart_annotations"
            " (workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at)"
            f" VALUES (%s, %s, %s, %s, %s, {payload_sql}, %s)",
            (ws, chart_id, symbol, timeframe, annotation_id, at),
        )


async def test_odd_rows_never_abort_it_and_a_run_again_adds_what_is_new(scratch):
    """A payload that is no object, and a chart no layer can name, are
    dropped; a ticker spelled as the server no longer stores it lands on the
    chart the server reads. After a rollback, the previous build's new
    drawings become a layer of their own beside the first."""
    await _alembic(scratch, "upgrade", "116")
    await _seed(scratch)
    t1 = T0 + timedelta(hours=1)
    await _insert_raw(scratch, WS_A, "NVDA:1day", "NVDA", "1day", "ann_" + "a" * 32, "'[1, 2]'::jsonb", t1)
    await _insert_raw(scratch, WS_A, "NVDA:1day", "NVDA", "1day", "ann_" + "b" * 32, "'\"not json\"'::jsonb", t1)
    brk_space = _legacy(20, "BRK B", "1day", type="price_line", price=400.0, label=None, color=None, style=None)
    brk_dot = _legacy(21, "brk.b", "1day", type="price_line", price=410.0, label=None, color=None, style=None)
    for (annotation_id, payload), symbol in ((brk_space, "BRK B"), (brk_dot, "brk.b")):
        await _insert_raw(
            scratch, WS_B, f"{symbol}:1day", symbol, "1day", annotation_id,
            f"'{json.dumps(payload)}'::jsonb", t1,
        )

    await _alembic(scratch, "upgrade", "117")
    rows = await _rows(scratch)
    assert not [k for k in rows if k[2].startswith("ann_")]
    # The undrawable payloads dropped; the chart's drawable ones still converted.
    nvda = ChartLayer.model_validate(rows[(WS_A, "NVDA:1day", "earlier-annotations")][0])
    assert [lv.price for lv in nvda.levels] == [205.0]
    brk = ChartLayer.model_validate(rows[(WS_B, "BRK-B:1day", "earlier-annotations")][0])
    assert (brk.symbol, [lv.price for lv in brk.levels]) == ("BRK-B", [410.0])
    assert not [k for k in rows if k[1].startswith("BRK B")]

    await _alembic(scratch, "downgrade", "116")
    later = _legacy(30, "NVDA", "1day", type="price_line", price=190.0, label="New support", color=None, style=None)
    await _insert_raw(
        scratch, WS_A, "NVDA:1day", "NVDA", "1day", later[0], f"'{json.dumps(later[1])}'::jsonb",
        T0 + timedelta(days=2),
    )
    await _alembic(scratch, "upgrade", "117")
    again = await _rows(scratch)
    assert {k: v for k, v in again.items() if k[2] != "earlier-annotations-2"} == rows
    second = ChartLayer.model_validate(again[(WS_A, "NVDA:1day", "earlier-annotations-2")][0])
    assert (second.title, [(lv.price, lv.title) for lv in second.levels]) == (
        "Earlier annotations (2)", [(190.0, "New support")],
    )
