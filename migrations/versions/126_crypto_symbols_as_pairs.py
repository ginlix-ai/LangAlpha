"""Migration 126: store each coin held or watched as its pair.

Every reader of a holding or watchlist row quotes its symbol alone, through
the stocks endpoint, and a bare coin there resolves as a US ticker: ``BTC``
read as a bitcoin ETF at about $30 a share, ``ETH`` as an ether ETF and
``USDC`` as a delisted OTC stock that flagged the whole menu-bar panel stale.
Writes now store a ``crypto`` row as its pair (``normalize_symbol``); this
rewrites the rows written before, ``instrument_type`` lowercased with them.

A row whose pair is already held in the same account, or already on the same
watchlist, merges with it as 063 merged respelled listings, each group once
from its original rows: a holding's quantities sum, its cost is the weighted
average when the costed lots share a currency (else none, every lot kept under
``metadata.merged_lots``) and the earliest purchase date stands; both kinds
keep every distinct note, fill a missing name or exchange, and gain the
metadata and alert keys they lacked.

The previous build writes bare coins, so this release deploys in place, not
blue/green: a row it writes while draining lands after the upgrade and stays
bare. The downgrade changes nothing: the bare spellings are the bug, and the
wider ``quantity`` (merges sum lots) suits the previous build as well.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

from alembic import op
from sqlalchemy import text

revision = "126"
down_revision = "125"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

# Both tables' symbol column is VARCHAR(50) (001_initial_schema).
_SYMBOL_MAX_LEN = 50


def _pair(symbol: str) -> str | None:
    """The pair a coin's row stores, or None to leave the row as it is.

    ``normalize_symbol``'s crypto rule, restated rather than imported, since a
    migration does not import the app.
    """
    from market_protocol import AssetClass, display_spelling, to_canonical

    spelled = display_spelling(symbol)
    try:
        ref = to_canonical(symbol, asset_class=AssetClass.EQUITY)
    except ValueError:
        return None
    if ref.asset_class is AssetClass.CRYPTO:
        pair = ref.symbol if "-" in ref.symbol else f"{ref.symbol}-{ref.price_currency}"
    elif spelled.isascii() and spelled.isalnum():
        pair = f"{spelled}-USD"
    else:
        return None
    return pair if len(pair) <= _SYMBOL_MAX_LEN else None


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value)


def _first(rows: list[dict], column: str) -> Any:
    return next((r[column] for r in rows if r[column]), None)


def _notes(rows: list[dict]) -> str | None:
    notes = dict.fromkeys(r["notes"].strip() for r in rows if r["notes"] and r["notes"].strip())
    return "\n\n".join(notes) or None


def _keys(rows: list[dict], column: str) -> Any:
    """Every key of the group's *column* objects, the keeper's value winning."""
    merged: dict = {}
    for row in reversed(rows):
        if isinstance(row[column], dict):
            merged.update(row[column])
    return merged or rows[0][column]


def _groups(rows: list[dict], key) -> list[tuple[str, list[dict]]]:
    """Rows by the row they become, the one already stored as the pair first.

    Each group is merged once from its original rows, so three spellings of
    one coin average and keep their lots as two would.
    """
    groups: dict[tuple, tuple[str, list[dict]]] = {}
    for row in rows:
        target = _pair(row["symbol"]) or row["symbol"]
        groups.setdefault(key(row, target), (target, []))[1].append(row)
    for target, group in groups.values():
        group.sort(key=lambda r: not (r["symbol"] == target and r["instrument_type"] == "crypto"))
    return list(groups.values())


def _holding_cost(rows: list[dict]) -> tuple[Decimal | None, str, bool]:
    """``(average_cost, currency, mixed)`` across the lots, as ``upsert_portfolio_holding`` merges."""
    costed = [r for r in rows if r["average_cost"] is not None]
    currencies = {(r["currency"] or "").upper() for r in costed}
    if len(currencies) != 1:
        return None, rows[0]["currency"], len(currencies) > 1
    quantity = sum((r["quantity"] for r in costed), Decimal(0))
    total = sum((r["quantity"] for r in rows), Decimal(0))
    cost = sum(r["quantity"] * r["average_cost"] for r in costed) / quantity if quantity and total else None
    return cost, currencies.pop() or rows[0]["currency"], False


def _rewrite_holdings(conn) -> None:
    rows = conn.execute(text(
        "SELECT * FROM user_portfolios WHERE lower(instrument_type) = 'crypto' "
        "ORDER BY created_at, user_portfolio_id"
    )).mappings().all()
    for target, group in _groups(
        [dict(r) for r in rows], lambda r, t: (r["user_id"], t, r["account_name"]),
    ):
        keep, others = group[0], group[1:]
        if not others:
            if (keep["symbol"], keep["instrument_type"]) != (target, "crypto"):
                conn.execute(text(
                    "UPDATE user_portfolios SET symbol = :symbol, instrument_type = 'crypto' "
                    "WHERE user_portfolio_id = :id"
                ), {"symbol": target, "id": keep["user_portfolio_id"]})
            continue
        cost, currency, mixed = _holding_cost(group)
        # merged_lots is read from the rows in SQL, so their numbers stay exact.
        conn.execute(text("""
            UPDATE user_portfolios SET
                symbol = :symbol, instrument_type = 'crypto',
                quantity = :quantity, average_cost = :cost, currency = :currency,
                first_purchased_at = :first_at, name = :name, exchange = :exchange,
                notes = :notes, updated_at = NOW(),
                metadata = CASE WHEN :mixed THEN
                    coalesce(CAST(:metadata AS jsonb), '{}'::jsonb) || jsonb_build_object(
                        'merged_lots', (
                            SELECT jsonb_agg(jsonb_build_object(
                                'symbol', l.symbol, 'quantity', l.quantity,
                                'average_cost', l.average_cost, 'currency', l.currency
                            ) ORDER BY l.created_at, l.user_portfolio_id)
                            FROM user_portfolios l
                            WHERE l.user_portfolio_id = ANY(CAST(:ids AS uuid[]))
                        ))
                    ELSE CAST(:metadata AS jsonb) END
            WHERE user_portfolio_id = :id
        """), {
            "symbol": target, "quantity": sum(r["quantity"] for r in group), "cost": cost,
            "currency": currency, "mixed": mixed,
            "first_at": min((r["first_purchased_at"] for r in group if r["first_purchased_at"]), default=None),
            "name": _first(group, "name"), "exchange": _first(group, "exchange"),
            "notes": _notes(group), "metadata": _json(_keys(group, "metadata")),
            "ids": [str(r["user_portfolio_id"]) for r in group], "id": keep["user_portfolio_id"],
        })
        conn.execute(text("DELETE FROM user_portfolios WHERE user_portfolio_id = ANY(CAST(:ids AS uuid[]))"),
                     {"ids": [str(r["user_portfolio_id"]) for r in others]})
        logger.info("126: %d holdings merged into %s (%s)", len(others), keep["user_portfolio_id"], target)


def _rewrite_watchlist_items(conn) -> None:
    rows = conn.execute(text(
        "SELECT * FROM watchlist_items WHERE lower(instrument_type) = 'crypto' "
        "ORDER BY created_at, watchlist_item_id"
    )).mappings().all()
    for target, group in _groups([dict(r) for r in rows], lambda r, t: (r["watchlist_id"], t)):
        keep, others = group[0], group[1:]
        if not others and (keep["symbol"], keep["instrument_type"]) == (target, "crypto"):
            continue
        if others:
            conn.execute(text("DELETE FROM watchlist_items WHERE watchlist_item_id = ANY(CAST(:ids AS uuid[]))"),
                         {"ids": [str(r["watchlist_item_id"]) for r in others]})
        conn.execute(text("""
            UPDATE watchlist_items SET
                symbol = :symbol, instrument_type = 'crypto', name = :name,
                exchange = :exchange, notes = :notes, updated_at = NOW(),
                alert_settings = CAST(:alerts AS jsonb), metadata = CAST(:metadata AS jsonb)
            WHERE watchlist_item_id = :id
        """), {
            "symbol": target, "name": _first(group, "name"), "exchange": _first(group, "exchange"),
            "notes": _notes(group), "alerts": _json(_keys(group, "alert_settings")),
            "metadata": _json(_keys(group, "metadata")), "id": keep["watchlist_item_id"],
        })


def upgrade() -> None:
    conn = op.get_bind()
    # A merged holding sums its lots, and DECIMAL(18, 8) holds ten whole
    # digits, which two lots of a meme coin can pass. Widening precision at
    # the same scale leaves every stored value as it is, with no table rewrite.
    conn.execute(text("ALTER TABLE user_portfolios ALTER COLUMN quantity TYPE NUMERIC(28, 8)"))
    _rewrite_holdings(conn)
    _rewrite_watchlist_items(conn)


def downgrade() -> None:
    pass
