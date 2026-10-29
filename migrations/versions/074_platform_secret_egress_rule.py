"""Migration 074: the egress rule a platform Secret's sandboxes are told.

On Daytona a Secret's hosts live on the Secret, so a sandbox holds only its
placeholder. On E2B each sandbox carries the rule itself (which hosts, which
header), so a change to the rule has to reach every sandbox, and the rollout
identity has to see it. ``egress_rule_sha256`` is the hash of that rule, empty
where the provider keeps the routing.

Existing rows default to empty, which is exactly what a Daytona rollout
registers, so the upgrade bumps no generation and re-pends no computer.
Adding a column with a constant default rewrites no rows.

Rollback: ``alembic stamp 073`` first, then redeploy the previous build; it
never reads this column, and its inserts take the default. ``downgrade`` drops
the column.
"""

from alembic import op

revision = "074"
down_revision = "073"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE platform_secret_rollouts "
        "ADD COLUMN IF NOT EXISTS egress_rule_sha256 VARCHAR(64) NOT NULL DEFAULT ''"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE platform_secret_rollouts DROP COLUMN IF EXISTS egress_rule_sha256"
    )
