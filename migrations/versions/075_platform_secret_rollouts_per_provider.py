"""Migration 075: one platform-secret rollout per provider kind.

A deployment serves every computer it holds, and a computer keeps the provider
it was created on, so after a provider switch two kinds of computer coexist and
each needs its own provider's Secrets. The rollout is keyed by
``(provider, secret_key)`` so both kinds' identities live side by side, and each
computer is certified against its own kind's rows.

Generations now come from one sequence shared by every kind, seeded above every
generation and every version already stamped. A version stamped under one
kind's identity, or before this migration, can then never equal a generation
issued for another identity, which is what lets a computer's version be
compared with its own kind's generation alone.

No rows are rewritten: the rows already present keep their provider and their
generation, so that kind's computers stay current, and a second kind registers
fresh rows above everything at its next boot.

Rollback: ``alembic downgrade 074`` before redeploying the previous build, which
upserts on ``secret_key`` alone and needs that key unique again. ``downgrade``
keeps the rows of the provider that registered last, restores that key and
drops the sequence; the next upgrade reseeds it above anything stamped meanwhile.
"""

from alembic import op

revision = "075"
down_revision = "074"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE platform_secret_rollouts "
        "DROP CONSTRAINT IF EXISTS platform_secret_rollouts_pkey"
    )
    op.execute(
        "ALTER TABLE platform_secret_rollouts "
        "ADD CONSTRAINT platform_secret_rollouts_pkey "
        "PRIMARY KEY (provider, secret_key)"
    )
    op.execute(
        "CREATE SEQUENCE IF NOT EXISTS platform_secret_generation_seq "
        "AS INTEGER MINVALUE 1"
    )
    # is_called = false: the next nextval returns exactly this value.
    op.execute("""
        SELECT setval('platform_secret_generation_seq', GREATEST(
            (SELECT COALESCE(MAX(generation), 0) FROM platform_secret_rollouts),
            (SELECT COALESCE(MAX(platform_secret_version), 0) FROM computers),
            (SELECT COALESCE(MAX(platform_secret_version), 0) FROM workspaces)
        ) + 1, false)
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # The previous build holds one provider's rows; keep the kind registered
    # last. It treats any other deployment provider as an identity change.
    op.execute("""
        DELETE FROM platform_secret_rollouts
        WHERE provider <> (
            SELECT provider FROM platform_secret_rollouts
            ORDER BY generation DESC, provider
            LIMIT 1
        )
    """)
    op.execute(
        "ALTER TABLE platform_secret_rollouts "
        "DROP CONSTRAINT IF EXISTS platform_secret_rollouts_pkey"
    )
    op.execute(
        "ALTER TABLE platform_secret_rollouts "
        "ADD CONSTRAINT platform_secret_rollouts_pkey PRIMARY KEY (secret_key)"
    )
    op.execute("DROP SEQUENCE IF EXISTS platform_secret_generation_seq")
