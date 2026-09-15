"""Storage for the probe's last word, apart from the tools it last brought back.

A snapshot row answers two different questions with one status, and they part
company the moment a working server starts refusing. The cached tools stay
valid under an unchanged config, so a failing re-probe must not wipe them: the
upsert keeps ``tools``/``status``/``observed_meta``/``discovered_at`` on a
same-config downgrade. That rule is right for serving and wrong for reporting,
because it also keeps the *verdict*, so a server that discovered fine and now
answers 401 still reads ``ok`` with an auth state from before the refusal, and
the page has nothing to draw a warning from.

``last_probe`` is the second answer, written on every guarded completion and
never kept: whatever the last probe concluded, in the vocabulary
``ProbeResult`` publishes. Both tiers carry the column because one statement
builder writes both; only the host-side probe fills it in.

``probe_kicked_at`` is the throttle behind the list route's self-heal, moved
onto a row every worker can see. The kick it rate-limits happens when there is
no snapshot row yet, which is why it lives on the catalog row rather than the
snapshot: that is the row that always exists.

Revision ID: 044
Revises: 043
"""

from alembic import op


revision = "044"
down_revision = "043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    for table in ("user_mcp_tool_schemas", "workspace_mcp_tool_schemas"):
        op.execute(f"""
            ALTER TABLE {table}
            ADD COLUMN IF NOT EXISTS last_probe JSONB NOT NULL DEFAULT '{{}}'
        """)
    op.execute("""
        ALTER TABLE user_mcp_servers
        ADD COLUMN IF NOT EXISTS probe_kicked_at TIMESTAMPTZ
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE user_mcp_servers DROP COLUMN IF EXISTS probe_kicked_at")
    for table in ("user_mcp_tool_schemas", "workspace_mcp_tool_schemas"):
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS last_probe")
