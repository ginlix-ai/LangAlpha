"""Migration 136: a grant kind for a connector the organization sets up.

``org_mcp`` grants let the relay reach a connector the organization defines
for its members. The grant names the connector by its sandbox name, and, for a
connector each member signs in to, the member's own connection; a connector
whose credential the auth service holds has no connection, so either may be
NULL. The check constraint rejects any kind it does not list, so it is rebuilt
with the new kind added and the three existing kinds, as 131 left them,
unchanged.

Online-safe the way 086 is: the drop and the NOT VALID add are one catalog
change under a short ACCESS EXCLUSIVE lock, and the validation runs in an
autocommit block under SHARE UPDATE EXCLUSIVE, which lets the relay's reads and
the grant sync's writes go on. Every row already stored is one of the three
kinds the old constraint allowed, which the new one still allows, so the
validation cannot fail. The previous build never writes an ``org_mcp`` row,
and refuses one it reads: a shared grant has no connection, so its status
check finds none; a member grant's connection is servable, but its credential
lookup finds no catalog row named ``org:<id>`` and answers ``needs_reauth``.

Rollback: roll the code back first, then ``alembic downgrade 135`` deletes the
``org_mcp`` rows and restores 131's constraint. They are derived state the
next session resolve rebuilds; a sandbox still holding one of their grant ids
gets the relay's unknown-grant refusal until then.
"""

from alembic import op

revision = "136"
down_revision = "135"
branch_labels = None
depends_on = None

_CONSTRAINT = "sandbox_egress_grants_kind_reference"

_OAUTH = "(kind = 'oauth_mcp' AND connection_id IS NOT NULL AND server_name IS NULL)"
_NAMED = "(kind IN ('header_mcp', 'platform_mcp') AND connection_id IS NULL AND server_name IS NOT NULL)"
_ORG = "(kind = 'org_mcp' AND server_name IS NOT NULL)"


def _swap_check(*clauses: str) -> str:
    # ADD CONSTRAINT has no IF NOT EXISTS; the DROP makes the ALTER re-runnable
    # after an abort in the autocommit block that validates it.
    return f"""
        ALTER TABLE sandbox_egress_grants
            DROP CONSTRAINT IF EXISTS {_CONSTRAINT},
            ADD CONSTRAINT {_CONSTRAINT} CHECK ({" OR ".join(clauses)}) NOT VALID
    """


def _validate_check() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(
            f"ALTER TABLE sandbox_egress_grants VALIDATE CONSTRAINT {_CONSTRAINT}"
        )


def upgrade() -> None:
    # The relay reads this table on its hot path: a wait behind an in-flight
    # grant sync would queue every reader behind the ACCESS EXCLUSIVE request,
    # so give up and let the deploy retry instead.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(_swap_check(_OAUTH, _NAMED, _ORG))
    # Every existing row already satisfies the narrower check it replaces.
    _validate_check()


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DELETE FROM sandbox_egress_grants WHERE kind = 'org_mcp'")
    op.execute(_swap_check(_OAUTH, _NAMED))
    # The DELETE above left no row outside the old vocabulary.
    _validate_check()
