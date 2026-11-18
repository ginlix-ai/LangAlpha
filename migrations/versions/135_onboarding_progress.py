"""Migration 135: where each user is in the first-run onboarding.

``onboarding_progress`` holds a user's two onboarding answers (which models
they use, and where the agent runs), whether the plan step is behind them,
their answer to pairing the Mac the desktop app runs on, which surface runs
setup, the last setup step finished, and when setup was completed, each
written by whichever onboarding surface the user is on. One row per user,
written only by ``PATCH /api/v1/users/me/onboarding``; no row reads as every
field unset.

``user_id`` references ``users`` with ``ON DELETE CASCADE ON UPDATE CASCADE``,
so a row follows ``migrate_user_id`` to the user's new id and goes when the
user does. Resetting preferences leaves it, as it leaves every per-user table
but ``user_preferences``. The previous build never reads it.

Rollback: ``alembic downgrade 134`` drops the table, and every user then
reads as having no answers and no setup progress.
"""

from alembic import op

revision = "135"
down_revision = "134"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS onboarding_progress (
            user_id            TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            models             TEXT NULL CHECK (models IN ('claude_code', 'subscription_or_key', 'langalpha_models')),
            runs_on            TEXT NULL CHECK (runs_on IN ('cloud', 'own_computer')),
            plan_step_done_at  TIMESTAMPTZ NULL,
            this_mac           TEXT NULL CHECK (this_mac IN ('paired', 'declined', 'unavailable')),
            setup_on           TEXT NULL CHECK (setup_on IN ('browser', 'mac_pending', 'app')),
            setup_step         TEXT NULL CHECK (setup_step IN ('computer', 'model', 'default')),
            setup_completed_at TIMESTAMPTZ NULL,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS onboarding_progress")
