"""Migration 107: holding costs keep every digit, and pence costs read as pounds.

``user_portfolios.average_cost`` was NUMERIC(18,4), so a cost under 0.0001
(a coin at 0.00001234) was stored as 0, and a profile file save that still
held the written cost differed from the row on every save. The column becomes
unconstrained NUMERIC, which stores what was written and reads it back
exactly. It is the only column that holds a holding's cost. Dropping the
scale needs no table rewrite.

Search labels London listings in pence (GBp), and the add dialog stored that
label, while every quote served for them is in pounds. Writers now store a
pence cost as GBP divided by 100 (``canonical_holding_cost``). Rows already
labeled GBp, or GBX in any case, are relabeled GBP with the cost unchanged: the
web values these costs as pounds, so the relabel changes no figure it shows,
where dividing by 100 would change every one of them. Every other code is
trimmed and upper-cased, since the merge guard now compares codes exactly; a
blank one is left as it is, since the API reads a holding's currency as text.

Rollback: ``alembic downgrade 105`` narrows the column back to NUMERIC(18,4),
rounding every cost to four places (a cost under 0.00005 becomes 0), and fails
if any cost has more than 14 digits before the point. Currency labels stay as
this migration left them, since the codes they had are not kept.
"""

from alembic import op

revision = "107"
down_revision = "105"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE user_portfolios ALTER COLUMN average_cost TYPE NUMERIC")
    op.execute(
        "UPDATE user_portfolios SET currency = 'GBP' "
        "WHERE BTRIM(currency) = 'GBp' OR UPPER(BTRIM(currency)) = 'GBX'"
    )
    op.execute(
        "UPDATE user_portfolios SET currency = UPPER(BTRIM(currency)) "
        "WHERE BTRIM(currency) <> '' AND currency <> UPPER(BTRIM(currency))"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE user_portfolios ALTER COLUMN average_cost TYPE NUMERIC(18, 4)")
