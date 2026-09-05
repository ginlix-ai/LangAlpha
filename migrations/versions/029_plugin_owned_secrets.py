"""Which plugin introduced a vault secret, so it can keep referencing it.

Declaring a secret is a request; the grant is deciding it against the vault,
and the grant's durable record is the component rows that already carry the
reference. That record has a hole: a plugin whose entries were all held back
at install (every one an sse entry awaiting the upgrade probe, or skipped by
policy) has no rows, so the key the user filled in through that plugin's own
wizard reads afterwards as a credential the vault holds for something else,
and the plugin is refused it for the rest of its life.

This column closes the hole from the other side: the secret remembers the
plugin that introduced it, and the grant is the union of the two records. Only
a create stamps it, because only a create is the plugin introducing the name;
writing a new value into a name that was already there proves nothing about
whose it is.

ON DELETE SET NULL, not RESTRICT: uninstalling a plugin must never be blocked
by a credential, and the secret itself is the user's to keep. The stamp going
with it is deliberate: an uninstalled plugin's claim on a vault name should not
outlive it. The action names plugin_id explicitly, because a composite key
nulls every column it lists, and user_id is NOT NULL: without the column list
the uninstall aborts on the constraint it was meant to satisfy.

Revision ID: 029
Revises: 028
"""

from alembic import op


revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # user_vault_secrets is read on every sandbox push. Same reasoning as 028.
    op.execute("SET lock_timeout = '5s'")

    op.execute("""
        ALTER TABLE user_vault_secrets
        ADD COLUMN IF NOT EXISTS plugin_id UUID NULL
    """)
    op.execute("""
        ALTER TABLE user_vault_secrets
        DROP CONSTRAINT IF EXISTS fk_user_vault_secrets_plugin
    """)
    # (user_id, plugin_id) as a pair, per 028: a secret can only be claimed by
    # a plugin belonging to the same user, whatever a future query forgets.
    op.execute("""
        ALTER TABLE user_vault_secrets
        ADD CONSTRAINT fk_user_vault_secrets_plugin
        FOREIGN KEY (user_id, plugin_id)
        REFERENCES user_plugins(user_id, user_plugin_id)
        ON DELETE SET NULL (plugin_id)
    """)
    # Grant resolution asks for one plugin's claims on every lifecycle step.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_vault_secrets_plugin
        ON user_vault_secrets(plugin_id)
        WHERE plugin_id IS NOT NULL
    """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE user_vault_secrets "
        "DROP CONSTRAINT IF EXISTS fk_user_vault_secrets_plugin"
    )
    op.execute("DROP INDEX IF EXISTS idx_user_vault_secrets_plugin")
    op.execute(
        "ALTER TABLE user_vault_secrets DROP COLUMN IF EXISTS plugin_id"
    )
