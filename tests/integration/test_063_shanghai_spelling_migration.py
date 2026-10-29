"""063 against real PostgreSQL: what each collision merges to, and the way back.

The unit test reads 063's statements off a mock bind, so none of its SQL ever
ran. This seeds a database at 062 with a listing stored under both Shanghai
spellings in every table 063 touches, upgrades it, and reads back the rows a
user would meet. Then it downgrades, the documented rollback, and upgrades
again; and re-runs the upgrade over rows already respelled.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database: this test downgrades the schema, which the shared
#: session-scoped one cannot survive.
SCRATCH_DB = "langalpha_migration_063"

USER = "user-063-a"
WORKSPACE = str(uuid.uuid4())
WATCHLIST = str(uuid.uuid4())
# Watchlist items and holdings by label; "old" rows hold .SS, "new" ones .SH.
ITEM = {
    label: str(uuid.uuid4())
    for label in ("old", "new", "lone", "hk_short", "hk_padded", "lower", "upper")
}
LOT = {
    label: str(uuid.uuid4())
    for label in ("old", "new", "usd", "cny", "uncosted", "costed", "hk_long", "hk_four")
}
AUTOMATION = str(uuid.uuid4())
HK_AUTOMATION = str(uuid.uuid4())
# Case twins in neither family: both passes must leave them as stored.
UNTOUCHED = {"aapl", "AAPL"}


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
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    run = {
        "upgrade": command.upgrade,
        "downgrade": command.downgrade,
        "stamp": command.stamp,
    }[action]
    await asyncio.to_thread(run, cfg, revision)


async def _execute(uri: str, statements: list[tuple[str, tuple]]) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        for sql, params in statements:
            await conn.execute(sql, params)


async def _fetch(uri: str, sql: str, params: tuple = ()) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


def _at(minute: int) -> str:
    return f"2026-01-01T00:{minute:02d}:00+00:00"


def _seed() -> list[tuple[str, tuple]]:
    item = (
        "INSERT INTO watchlist_items (watchlist_item_id, watchlist_id, user_id, symbol,"
        " instrument_type, name, notes, alert_settings, metadata, created_at)"
        " VALUES (%s, %s, %s, %s, 'stock', %s, %s, %s::jsonb, %s::jsonb, %s)"
    )
    lot = (
        "INSERT INTO user_portfolios (user_portfolio_id, user_id, symbol, instrument_type,"
        " quantity, average_cost, currency, account_name, notes, metadata,"
        " first_purchased_at, created_at)"
        " VALUES (%s, %s, %s, 'stock', %s, %s, %s, %s, %s, %s::jsonb, %s, %s)"
    )
    annotation = (
        "INSERT INTO chart_annotations"
        " (workspace_id, chart_id, symbol, timeframe, annotation_id, payload, created_at)"
        " VALUES (%s, %s, %s, '1day', %s, %s::jsonb, %s)"
    )

    def _payload(symbol: str, annotation_id: str, label: str) -> str:
        return json.dumps({
            "annotation_id": annotation_id, "symbol": symbol,
            "chart_id": f"{symbol}:1day", "type": "hline", "label": label,
        })

    return [
        ("INSERT INTO users (user_id, email) VALUES (%s, %s)", (USER, "a@example.com")),
        (
            "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status)"
            " VALUES (%s, %s, 'Research', 'research', 'running')",
            (WORKSPACE, USER),
        ),
        (
            "INSERT INTO watchlists (watchlist_id, user_id, name) VALUES (%s, %s, 'Main')",
            (WATCHLIST, USER),
        ),
        # The older entry has no note; the newer one's note and keys must survive it.
        (item, (ITEM["old"], WATCHLIST, USER, "600519.SS", "Kweichow Moutai", None,
                '{"above": 1800}', '{"origin": "agent", "pinned": true}', _at(1))),
        (item, (ITEM["new"], WATCHLIST, USER, "600519.SH", None, "earnings in August",
                '{"below": 1400}', '{"origin": "user", "tag": "core"}', _at(2))),
        (item, (ITEM["lone"], WATCHLIST, USER, "601318.SS", None, None, "{}", "{}", _at(3))),
        # Hong Kong in two paddings: one listing, stored at four digits.
        (item, (ITEM["hk_short"], WATCHLIST, USER, "700.HK", "Tencent", None,
                "{}", "{}", _at(4))),
        (item, (ITEM["hk_padded"], WATCHLIST, USER, "0700.HK", None, "cloud",
                "{}", "{}", _at(5))),
        (item, (ITEM["lower"], WATCHLIST, USER, "aapl", None, None, "{}", "{}", _at(6))),
        (item, (ITEM["upper"], WATCHLIST, USER, "AAPL", None, None, "{}", "{}", _at(7))),
        # One account-less position bought in two spellings.
        (lot, (LOT["old"], USER, "600519.SS", 100, 10, "CNY", None, "first lot",
               '{"broker": "a"}', _at(1), _at(1))),
        (lot, (LOT["new"], USER, "600519.SH", 300, 20, "CNY", None, None,
               '{"lots": 2}', _at(0), _at(2))),
        # Costs in two currencies: never averaged.
        (lot, (LOT["usd"], USER, "600519.SS", 10, 200, "USD", "Broker", None,
               "{}", None, _at(3))),
        (lot, (LOT["cny"], USER, "600519.SH", 5, 1500, "CNY", "Broker", None,
               "{}", None, _at(4))),
        # Only the newer lot has a cost, so its currency is the one that holds.
        (lot, (LOT["uncosted"], USER, "600519.SS", 10, None, "USD", "Margin", None,
               "{}", None, _at(5))),
        (lot, (LOT["costed"], USER, "600519.SH", 20, 1500, "CNY", "Margin", None,
               "{}", None, _at(6))),
        (lot, (LOT["hk_long"], USER, "00700.HK", 100, 300, "HKD", None, None,
               "{}", None, _at(7))),
        (lot, (LOT["hk_four"], USER, "0700.HK", 100, 500, "HKD", None, None,
               "{}", None, _at(8))),
        (annotation, (WORKSPACE, "600519.SS:1day", "600519.SS", "ann_shared",
                      _payload("600519.SS", "ann_shared", "older"), _at(1))),
        (annotation, (WORKSPACE, "600519.SH:1day", "600519.SH", "ann_shared",
                      _payload("600519.SH", "ann_shared", "newer"), _at(2))),
        (annotation, (WORKSPACE, "600519.SS:1day", "600519.SS", "ann_ss_only",
                      _payload("600519.SS", "ann_ss_only", "lone"), _at(3))),
        (annotation, (WORKSPACE, "00700.HK:1day", "00700.HK", "ann_hk",
                      _payload("00700.HK", "ann_hk", "tencent"), _at(4))),
        (
            "INSERT INTO automations (automation_id, user_id, name, trigger_type,"
            " trigger_config, instruction)"
            " VALUES (%s, %s, 'Moutai alert', 'price', %s::jsonb, 'Tell me')",
            (AUTOMATION, USER, json.dumps({"symbol": "600519.SS", "conditions": []})),
        ),
        (
            "INSERT INTO automations (automation_id, user_id, name, trigger_type,"
            " trigger_config, instruction)"
            " VALUES (%s, %s, 'Tencent alert', 'price', %s::jsonb, 'Tell me')",
            (HK_AUTOMATION, USER, json.dumps({"symbol": "700.HK", "conditions": []})),
        ),
    ]


async def _read(uri: str) -> dict[str, Any]:
    items = await _fetch(
        uri,
        "SELECT watchlist_item_id::text, symbol, name, notes, alert_settings, metadata"
        " FROM watchlist_items",
    )
    lots = await _fetch(
        uri,
        "SELECT user_portfolio_id::text, symbol, quantity, average_cost, currency,"
        " notes, metadata, first_purchased_at FROM user_portfolios",
    )
    annotations = await _fetch(
        uri,
        "SELECT chart_id, symbol, annotation_id, payload FROM chart_annotations"
        " ORDER BY annotation_id",
    )
    triggers = dict(await _fetch(
        uri, "SELECT automation_id::text, trigger_config->>'symbol' FROM automations",
    ))
    return {
        "items": {row[0]: row[1:] for row in items},
        "lots": {row[0]: row[1:] for row in lots},
        "annotations": annotations,
        "trigger": triggers[AUTOMATION],
        "hk_trigger": triggers[HK_AUTOMATION],
    }


@pytest_asyncio.fixture(scope="module")
async def run() -> dict[str, Any]:
    admin, uri = _uri("postgres"), _uri(SCRATCH_DB)

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    out: dict[str, Any] = {}
    try:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            # Earlier migrations read the bundled servers; keep them as shipped.
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            await _alembic(uri, "upgrade", "062")
            await _execute(uri, _seed())

            await _alembic(uri, "upgrade", "063")
            out["up"] = await _read(uri)

            # Rows already respelled: running the upgrade over them is a no-op.
            await _alembic(uri, "stamp", "062")
            await _alembic(uri, "upgrade", "063")
            out["rerun"] = await _read(uri)

            await _alembic(uri, "downgrade", "062")
            out["down"] = await _read(uri)

            await _alembic(uri, "upgrade", "063")
            out["again"] = await _read(uri)
        yield out
    finally:
        await _recreate(create=False)


def _symbols(state: dict[str, Any]) -> set[str]:
    return (
        {row[0] for row in state["items"].values()}
        | {row[0] for row in state["lots"].values()}
        | {row[1] for row in state["annotations"]}
        | {row[3]["symbol"] for row in state["annotations"]}
        | {state["trigger"], state["hk_trigger"]}
    )


class TestUpgrade:
    async def test_every_table_stores_sh(self, run):
        assert _symbols(run["up"]) == {"600519.SH", "601318.SH", "0700.HK", *UNTOUCHED}
        assert {row[0] for row in run["up"]["annotations"]} == {
            "600519.SH:1day", "0700.HK:1day",
        }
        assert {row[3]["chart_id"] for row in run["up"]["annotations"]} == {
            "600519.SH:1day", "0700.HK:1day",
        }

    async def test_a_watchlist_keeps_the_older_entry_with_the_newer_ones_note(self, run):
        items = run["up"]["items"]
        assert set(items) == {
            ITEM["old"], ITEM["lone"], ITEM["hk_short"], ITEM["lower"], ITEM["upper"],
        }
        symbol, name, notes, alerts, metadata = items[ITEM["old"]]
        assert (symbol, name, notes) == ("600519.SH", "Kweichow Moutai", "earnings in August")
        # Every key either entry carried; where both did, the survivor's.
        assert alerts == {"above": 1800, "below": 1400}
        assert metadata == {"origin": "agent", "pinned": True, "tag": "core"}

    async def test_a_holding_in_two_spellings_merges_as_the_upsert_would(self, run):
        lots = run["up"]["lots"]
        assert LOT["new"] not in lots
        symbol, qty, cost, currency, notes, metadata, first_at = lots[LOT["old"]]
        assert (symbol, qty, cost, currency) == (
            "600519.SH", Decimal("400"), Decimal("17.5"), "CNY",
        )
        assert notes == "first lot"
        assert metadata == {"broker": "a", "lots": 2}
        assert first_at == datetime(2026, 1, 1, tzinfo=timezone.utc)

    async def test_costs_in_two_currencies_leave_no_cost_and_keep_the_lots(self, run):
        lots = run["up"]["lots"]
        assert LOT["cny"] not in lots
        _symbol, qty, cost, currency, _notes, metadata, _first = lots[LOT["usd"]]
        assert (qty, cost, currency) == (Decimal("15"), None, "USD")
        assert metadata["merged_lots"] == [
            {"symbol": "600519.SS", "quantity": 10, "average_cost": 200, "currency": "USD"},
            {"symbol": "600519.SH", "quantity": 5, "average_cost": 1500, "currency": "CNY"},
        ]

    async def test_a_lot_without_a_cost_takes_the_costed_lots_currency(self, run):
        lots = run["up"]["lots"]
        assert LOT["costed"] not in lots
        _symbol, qty, cost, currency, *_ = lots[LOT["uncosted"]]
        assert (qty, cost, currency) == (Decimal("30"), Decimal("1500"), "CNY")

    async def test_a_chart_keeps_the_older_copy_of_a_shared_annotation(self, run):
        labels = {row[2]: row[3]["label"] for row in run["up"]["annotations"]}
        assert labels == {"ann_shared": "older", "ann_ss_only": "lone", "ann_hk": "tencent"}

    async def test_hong_kong_paddings_merge_into_one_four_digit_row(self, run):
        items, lots = run["up"]["items"], run["up"]["lots"]
        assert ITEM["hk_padded"] not in items
        assert items[ITEM["hk_short"]][:3] == ("0700.HK", "Tencent", "cloud")
        assert LOT["hk_four"] not in lots
        symbol, qty, cost, currency, *_ = lots[LOT["hk_long"]]
        assert (symbol, qty, cost, currency) == (
            "0700.HK", Decimal("200"), Decimal("400"), "HKD",
        )
        assert run["up"]["hk_trigger"] == "0700.HK"

    async def test_a_symbol_in_neither_family_keeps_its_case(self, run):
        items = run["up"]["items"]
        assert (items[ITEM["lower"]][0], items[ITEM["upper"]][0]) == ("aapl", "AAPL")

    async def test_rerunning_the_upgrade_changes_nothing(self, run):
        assert run["rerun"] == run["up"]


class TestRollback:
    async def test_the_downgrade_hands_back_ss_and_keeps_merges(self, run):
        down = run["down"]
        # Hong Kong stays at four digits, a spelling the previous build resolves.
        assert _symbols(down) == {"600519.SS", "601318.SS", "0700.HK", *UNTOUCHED}
        assert {row[0] for row in down["annotations"]} == {"600519.SS:1day", "0700.HK:1day"}
        assert {row[3]["chart_id"] for row in down["annotations"]} == {
            "600519.SS:1day", "0700.HK:1day",
        }
        # Only the spelling moved.
        respelled = {
            key: (row[0].replace(".SS", ".SH"), *row[1:])
            for key, row in down["lots"].items()
        }
        assert respelled == run["up"]["lots"]
        assert set(down["items"]) == set(run["up"]["items"])

    async def test_upgrading_again_lands_where_the_first_upgrade_did(self, run):
        assert run["again"] == run["up"]
