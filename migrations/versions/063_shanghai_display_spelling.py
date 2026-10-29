"""Migration 063: store Shanghai listings as ``.SH``.

Shanghai has two spellings, ``.SS`` (Yahoo, FMP) and ``.SH`` (the exchange,
Tushare, Chinese brokers). Both are accepted on input; ``.SH`` is the one
stored from here on. Rows written before hold ``.SS``, which leaves one listing
under two keys: a watchlist can hold ``600519.SS`` beside ``600519.SH``, and a
chart drawn on one spelling does not show on the other.

Each table is respelled. Where a respelled row collides with a row already in
the new spelling, the oldest row survives. A chart annotation keeps its older
copy whole; a watchlist entry or holding also takes what only the others
carry: their notes are appended, a missing name or exchange is filled, and
metadata and alert keys it lacks are added. A holding then merges as
``upsert_portfolio_holding`` does: quantities sum, cost is the weighted
average, and the earliest purchase date stands. A holding's currency is
whatever its writer passed, USD by default, so two lots of one listing can
disagree; cost is never averaged across currencies. When the costed lots
disagree no cost is invented: the merged holding's cost is left empty, and
every lot as it stood is kept under ``metadata.merged_lots``.

Hong Kong codes collapse the same way. HKEX writes a code with and without
leading zeros (``700.HK``, ``00700.HK``); all name one listing, stored as the
four-digit ``0700.HK`` from here on, so rows in another padding are respelled
and merged by the same rules.

The downgrade respells Shanghai back to ``.SS``, the only Shanghai spelling the
previous code resolves: it reads ``600519.SH`` as an unknown venue on the US
calendar, which no provider quotes. Hong Kong stays at four digits, which the
previous code resolves too. Rows merged on the way up stay merged. Rolling back
therefore means ``alembic downgrade 062`` run from this build before the
previous one starts; that build cannot start on a revision it does not know,
and stamping back would leave it ``.SH`` rows. Since the previous build writes
``.SS``, this release deploys in place, not blue/green: a row it writes while
draining lands after the upgrade and stays ``.SS``.

Every statement only touches rows whose spelling changes, or the groups they
collide with, so re-running either direction changes nothing.
"""

from typing import Callable

from alembic import op

revision = "063"
down_revision = "062"
branch_labels = None
depends_on = None


Spelling = Callable[[str], str]


def _shanghai(src: str, dst: str) -> Spelling:
    """SQL for a column with its ``.{src}`` suffix spelled ``.{dst}``. Any other
    symbol is left as stored: uppercasing it too would respell ``aapl`` beside
    an ``AAPL`` and abort on the unique key."""
    return lambda col: (
        rf"CASE WHEN upper({col}) ~ '\.{src}$' THEN"
        rf" regexp_replace(upper({col}), '\.{src}$', '.{dst}') ELSE {col} END"
    )


def _hong_kong(col: str) -> str:
    """SQL for *col* with an HKEX code at four digits, as ``display_spelling``
    writes it: leading zeros dropped, then padded back to four. ``lpad`` would
    truncate a five-digit code, so a long one is left as it is."""
    stem = rf"ltrim(split_part(upper({col}), '.', 1), '0')"
    return rf"""CASE WHEN upper({col}) ~ '^[0-9]+\.HK$' THEN
        CASE WHEN length({stem}) >= 4 THEN {stem} ELSE lpad({stem}, 4, '0') END || '.HK'
        ELSE {col} END"""


def _ranked(table: str, key: str, owner: str, target: str, cols: str, scope: str) -> str:
    """Rows whose spelling matches *scope*, numbered oldest first within each
    collision.

    ``note_rn`` numbers rows sharing one note text, so a note two rows repeat
    is carried once.
    """
    group = f"{owner}, {target}"
    return rf"""
        SELECT {key}, {cols},
               first_value({key}) OVER w AS keeper,
               row_number() OVER w AS rn,
               row_number() OVER (
                   PARTITION BY {group}, btrim(notes)
                   ORDER BY created_at, {key}
               ) AS note_rn
        FROM {table}
        WHERE {target} ~ '{scope}'
        WINDOW w AS (PARTITION BY {group} ORDER BY created_at, {key})
    """


def _first(col: str) -> str:
    """The survivor's *col*, else the oldest merged row's that has one."""
    return f"(array_agg({col} ORDER BY rn) FILTER (WHERE btrim({col}) <> ''))[1]"


# Every distinct note in the group, oldest first.
_NOTES = (
    r"string_agg(notes, E'\n\n' ORDER BY rn) "
    "FILTER (WHERE note_rn = 1 AND btrim(notes) <> '')"
)


def _keys(col: str) -> str:
    """Every key in the group's *col* objects, the survivor's value winning."""
    return f"""(
        SELECT jsonb_object_agg(kv.key, kv.value ORDER BY g.rn DESC)
        FROM ranked g
        CROSS JOIN LATERAL jsonb_each(
            CASE WHEN jsonb_typeof(g.{col}) = 'object' THEN g.{col} ELSE '{{}}'::jsonb END
        ) kv
        WHERE g.keeper = r.keeper
    )"""


def _respell(spell: Spelling, scope: str) -> None:
    """Respell every stored symbol through *spell*, merging collisions.

    *scope* is a regex every respelled symbol in the family matches, which
    bounds the rows a collision can involve.
    """
    op.execute("SET LOCAL lock_timeout = '5s'")

    # -- watchlist_items: UNIQUE (watchlist_id, symbol, instrument_type) --
    target = spell("symbol")
    ranked = _ranked(
        "watchlist_items",
        "watchlist_item_id",
        "watchlist_id, instrument_type",
        target,
        "name, exchange, notes, alert_settings, metadata",
        scope,
    )
    op.execute(rf"""
        WITH ranked AS ({ranked}),
        merged AS (
            SELECT r.keeper,
                   {_first("name")} AS name,
                   {_first("exchange")} AS exchange,
                   {_NOTES} AS notes,
                   {_keys("alert_settings")} AS alert_settings,
                   {_keys("metadata")} AS metadata
            FROM ranked r
            GROUP BY r.keeper
            HAVING count(*) > 1
        ),
        kept AS (
            UPDATE watchlist_items w
            SET name = coalesce(m.name, w.name),
                exchange = coalesce(m.exchange, w.exchange),
                notes = coalesce(m.notes, w.notes),
                alert_settings = coalesce(m.alert_settings, w.alert_settings),
                metadata = coalesce(m.metadata, w.metadata)
            FROM merged m
            WHERE w.watchlist_item_id = m.keeper
            RETURNING w.watchlist_item_id
        )
        DELETE FROM watchlist_items w
        USING ranked r
        WHERE w.watchlist_item_id = r.watchlist_item_id
          AND r.rn > 1
          AND r.keeper IN (SELECT watchlist_item_id FROM kept)
    """)
    op.execute(rf"""
        UPDATE watchlist_items SET symbol = {target}
        WHERE symbol <> {target}
    """)

    # -- user_portfolios: one holding per (user, symbol, type, account) --
    # account_name matches IS NOT DISTINCT FROM, as the upsert does;
    # PARTITION BY already groups NULLs together. Cost averages over the lots
    # that carry one, and only when they share a currency, compared ignoring
    # case as the upsert compares it; the survivor then takes it: a lot with no
    # cost says nothing about its currency. Costs in two currencies have no
    # common average, so none is stored and the lots are kept as they stood.
    ranked = _ranked(
        "user_portfolios",
        "user_portfolio_id",
        "user_id, instrument_type, account_name",
        target,
        "symbol, quantity, average_cost, currency, first_purchased_at,"
        " name, exchange, notes, metadata",
        scope,
    )
    op.execute(rf"""
        WITH ranked AS ({ranked}),
        merged AS (
            SELECT r.keeper,
                   sum(quantity) AS qty,
                   sum(quantity * average_cost) FILTER (WHERE average_cost IS NOT NULL)
                       / NULLIF(sum(quantity) FILTER (WHERE average_cost IS NOT NULL), 0)
                       AS cost,
                   count(DISTINCT upper(coalesce(currency, '')))
                       FILTER (WHERE average_cost IS NOT NULL) AS cost_currencies,
                   (array_agg(upper(currency) ORDER BY rn)
                       FILTER (WHERE average_cost IS NOT NULL))[1] AS cost_currency,
                   jsonb_agg(jsonb_build_object(
                       'symbol', symbol, 'quantity', quantity,
                       'average_cost', average_cost, 'currency', currency
                   ) ORDER BY rn) AS lots,
                   min(first_purchased_at) AS first_at,
                   {_first("name")} AS name,
                   {_first("exchange")} AS exchange,
                   {_NOTES} AS notes,
                   {_keys("metadata")} AS metadata
            FROM ranked r
            GROUP BY r.keeper
            HAVING count(*) > 1
        ),
        kept AS (
            UPDATE user_portfolios p
            SET quantity = m.qty,
                average_cost = CASE
                    WHEN m.qty = 0 THEN NULL
                    WHEN m.cost_currencies > 1 THEN NULL
                    ELSE m.cost
                END,
                currency = CASE
                    WHEN m.cost_currencies = 1 THEN m.cost_currency
                    ELSE p.currency
                END,
                first_purchased_at = m.first_at,
                name = coalesce(m.name, p.name),
                exchange = coalesce(m.exchange, p.exchange),
                notes = coalesce(m.notes, p.notes),
                metadata = CASE
                    WHEN m.cost_currencies > 1 THEN
                        coalesce(m.metadata, '{{}}'::jsonb)
                        || jsonb_build_object('merged_lots', m.lots)
                    ELSE coalesce(m.metadata, p.metadata)
                END
            FROM merged m
            WHERE p.user_portfolio_id = m.keeper
            RETURNING p.user_portfolio_id
        )
        DELETE FROM user_portfolios p
        USING ranked r
        WHERE p.user_portfolio_id = r.user_portfolio_id
          AND r.rn > 1
          AND r.keeper IN (SELECT user_portfolio_id FROM kept)
    """)
    op.execute(rf"""
        UPDATE user_portfolios SET symbol = {target}
        WHERE symbol <> {target}
    """)

    # -- chart_annotations: PRIMARY KEY (workspace_id, chart_id, annotation_id),
    # chart_id = "{SYMBOL}:{timeframe}", and the payload repeats both. --
    op.execute(rf"""
        WITH ranked AS (
            SELECT workspace_id, chart_id, annotation_id,
                   row_number() OVER (
                       PARTITION BY workspace_id, timeframe, annotation_id, {target}
                       ORDER BY created_at, chart_id
                   ) AS rn
            FROM chart_annotations
            WHERE {target} ~ '{scope}'
        )
        DELETE FROM chart_annotations a
        USING ranked r
        WHERE a.workspace_id = r.workspace_id
          AND a.chart_id = r.chart_id
          AND a.annotation_id = r.annotation_id
          AND r.rn > 1
    """)
    op.execute(rf"""
        UPDATE chart_annotations
        SET symbol = {target},
            chart_id = {target} || ':' || timeframe,
            payload = payload || jsonb_build_object(
                'symbol', {target}, 'chart_id', {target} || ':' || timeframe
            )
        WHERE symbol <> {target}
    """)

    # -- automations: a price trigger names its symbol in trigger_config --
    trigger_symbol = spell("trigger_config->>'symbol'")
    op.execute(rf"""
        UPDATE automations
        SET trigger_config = jsonb_set(
            trigger_config, '{{symbol}}', to_jsonb({trigger_symbol})
        )
        WHERE trigger_type = 'price' AND trigger_config->>'symbol' <> {trigger_symbol}
    """)


def upgrade() -> None:
    _respell(_shanghai("SS", "SH"), r"\.SH$")
    _respell(_hong_kong, r"^[0-9]+\.HK$")


def downgrade() -> None:
    _respell(_shanghai("SH", "SS"), r"\.SS$")
