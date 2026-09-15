"""A second grant kind: a catalog row authenticated by its own static headers.

``oauth_mcp`` is keyed by the connection whose token the relay spends, and the
connection row is both the credential's identity and its lifecycle. A row that
authenticates with an API key in a header has no connection: the credential
lives in the user's vault and the row says which header carries it, so the
grant has to name the row instead. ``server_name`` is that reference, and it is
all the grant stores -- the value is resolved per request, so a rotated secret
takes effect with no resync and a row that is deleted, disabled or repointed
stops resolving rather than leaving a live grant behind.

The uniqueness key grows by that column for the same reason. Every
``header_mcp`` row carries a NULL ``connection_id``, and under NULLS NOT
DISTINCT the old three-column key would have let one workspace hold exactly one
header grant. ``oauth_mcp`` rows keep a NULL ``server_name``, so their key is
the one they already had.

This is the expand half of an expand/contract pass, and the three-column key
survives it on purpose. The previous release upserts with
``ON CONFLICT (workspace_id, kind, connection_id)``, and PostgreSQL infers that
only from an index over exactly those columns, so dropping the key here would
break every grant sync on the colour still serving through a blue/green
cutover. The key is rebuilt with the default NULLS DISTINCT instead: the old
release never writes a NULL ``connection_id``, so nothing it stores changes,
while the header rows, which all do, no longer collide on it. The contract
migration that drops it belongs to a release no old colour can be running. A
rollback to the previous release leaves the ``header_mcp`` rows active -- its
retirement sweep is filtered to ``oauth_mcp`` and its relay refuses them, so
they sit inert but not retired until this release runs again or the downgrade
here deletes them.

The check constraint is what keeps the two halves from being mixed: each kind
names exactly one reference, and a kind nothing here lists cannot be stored at
all.

Revision ID: 045
Revises: 044
"""

from alembic import op


revision = "045"
down_revision = "044"
branch_labels = None
depends_on = None

OLD_KEY = "sandbox_egress_grants_workspace_id_kind_connection_id_key"


def upgrade() -> None:
    # Every ALTER below takes ACCESS EXCLUSIVE on a table the relay reads on
    # its hot path; a wait behind an in-flight grant sync would queue every
    # reader behind it, so give up and let the deploy retry instead.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        ADD COLUMN IF NOT EXISTS server_name VARCHAR(255)
    """)
    op.execute(f"""
        ALTER TABLE sandbox_egress_grants
        DROP CONSTRAINT IF EXISTS {OLD_KEY}
    """)
    op.execute(f"""
        ALTER TABLE sandbox_egress_grants
        ADD CONSTRAINT {OLD_KEY}
        UNIQUE (workspace_id, kind, connection_id)
    """)
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        ADD CONSTRAINT sandbox_egress_grants_scope_key
        UNIQUE NULLS NOT DISTINCT (workspace_id, kind, connection_id, server_name)
    """)
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        DROP CONSTRAINT IF EXISTS sandbox_egress_grants_kind_reference
    """)
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        ADD CONSTRAINT sandbox_egress_grants_kind_reference CHECK (
            (kind = 'oauth_mcp'
                AND connection_id IS NOT NULL AND server_name IS NULL)
            OR (kind = 'header_mcp'
                AND connection_id IS NULL AND server_name IS NOT NULL)
        )
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        DROP CONSTRAINT IF EXISTS sandbox_egress_grants_kind_reference
    """)
    # The header rows go first: they are the ones the old key cannot hold.
    # They are derived state that the next session resolve rebuilds, but a
    # sandbox still holding one of these grant ids gets the relay's
    # unknown-grant refusal until then.
    op.execute("DELETE FROM sandbox_egress_grants WHERE kind = 'header_mcp'")
    op.execute("""
        ALTER TABLE sandbox_egress_grants
        DROP CONSTRAINT IF EXISTS sandbox_egress_grants_scope_key
    """)
    op.execute(f"""
        ALTER TABLE sandbox_egress_grants
        DROP CONSTRAINT IF EXISTS {OLD_KEY}
    """)
    op.execute(f"""
        ALTER TABLE sandbox_egress_grants
        ADD CONSTRAINT {OLD_KEY}
        UNIQUE NULLS NOT DISTINCT (workspace_id, kind, connection_id)
    """)
    op.execute("ALTER TABLE sandbox_egress_grants DROP COLUMN IF EXISTS server_name")
