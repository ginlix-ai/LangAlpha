"""Migration 149: a shared workspace's managers may keep members' own context
out, and the key a member's memory aliases are made with.

An organization may let a member's own answer preferences and personal memory
reach the agent on that member's messages in its shared workspaces. That grant
is the organization's, read from its features on each turn; nothing of it is
stored here. ``member_context`` is a workspace's managers' answer to it:
``true`` follows the organization's grant, ``false`` keeps every member's own
context out of this workspace whatever the organization grants. It is read at
the start of each turn, so a change holds from the next one.

``member_memory_alias`` keys the opaque names a member's memory files go by in
a shared workspace. Minted by Postgres, as 049 minted the first server key, so
every worker makes the same alias without configuration.

Online-safe: one column with a constant default on a small table, which
Postgres adds without rewriting it; every existing row reads ``true``, which
changes nothing until an organization grants it. Rollback: roll the code back
first, then ``alembic downgrade 148``, which drops the column and with it
each workspace's choice. The key stays: an upgrade after that keeps it, so a
memory read a thread kept from before is filled back in as before.
"""

from alembic import op

revision = "149"
down_revision = "148"
branch_labels = None
depends_on = None

_KEY = "member_memory_alias"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE shared_workspaces "
        "ADD COLUMN IF NOT EXISTS member_context BOOLEAN NOT NULL DEFAULT TRUE"
    )
    # pgcrypto has been present since 001.
    op.execute(f"""
        INSERT INTO server_keys (name, secret)
        VALUES ('{_KEY}', gen_random_bytes(32))
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE shared_workspaces DROP COLUMN IF EXISTS member_context")
    # The key stays, so a re-upgrade aliases as before (``ON CONFLICT``).
