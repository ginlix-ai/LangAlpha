"""Migration 138: who a file link opens to, and the member grant keys.

``share_links.audience`` is NULL for a link that opens to anyone (every link
so far) and ``organization`` for one that opens only to members of its
owner's organization. A thread link keeps its audience in
``share_permissions`` and needs no column.

Two keys sign the grant a member's page carries to the routes that take no
bearer: one for a thread link's replay and files, one for a file link's
frames. Minted by Postgres, as 049 minted the first, so every worker signs
with the same key without configuration.

Online-safe the way 136 is: the column is nullable with no default, a catalog
change under a short lock, and its check is added NOT VALID and validated in
an autocommit block under SHARE UPDATE EXCLUSIVE; every row holds NULL, so the
validation cannot fail. The previous build never reads the column, so a link
set to the organization audience while it still serves would open to anyone
until the deploy finishes; nothing sets one until this build serves.

Rollback: roll the code back first, then ``alembic downgrade 137`` drops the
column and both keys. Links set to the organization audience open to anyone
again under the previous build, so stop sharing them first.
"""

from alembic import op

revision = "138"
down_revision = "137"
branch_labels = None
depends_on = None

_CHECK = "share_links_audience_known"
_KEYS = ("member_share_grant", "artifact_member_grant")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE share_links ADD COLUMN IF NOT EXISTS audience TEXT")
    op.execute(f"""
        ALTER TABLE share_links
            DROP CONSTRAINT IF EXISTS {_CHECK},
            ADD CONSTRAINT {_CHECK}
                CHECK (audience IS NULL OR audience = 'organization') NOT VALID
    """)
    for name in _KEYS:
        # pgcrypto has been present since 001.
        op.execute(f"""
            INSERT INTO server_keys (name, secret)
            VALUES ('{name}', gen_random_bytes(32))
            ON CONFLICT (name) DO NOTHING
        """)
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE share_links VALIDATE CONSTRAINT {_CHECK}")
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE share_links DROP COLUMN IF EXISTS audience")
    op.execute(
        "DELETE FROM server_keys WHERE name IN ("
        + ", ".join(f"'{name}'" for name in _KEYS)
        + ")"
    )
