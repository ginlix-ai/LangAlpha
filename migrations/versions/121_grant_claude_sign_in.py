"""Migration 121: grant the Claude sign-in to the users who have one stored.

The ``claude_oauth`` feature says who may start a new Claude sign-in. A
deployment that stops offering it to new users sets its gate to ``grant``,
which honors only an override the user cannot set. This writes that override,
``feature_overrides.claude_oauth = true`` in ``other_preference``, for every
user with a Claude sign-in stored, working or not, so they can still reconnect
it. A user with no preferences row gets one; a token whose user is gone is
skipped. Under any other gate the override is read by nothing.

Rows are written in raw SQL, past the application's cache, so the cached
preferences shape moves with it (``_USER_PREFS_SHAPE``).

Rollback: ``alembic downgrade 120`` removes the override from every row that
holds it, including one written since, which the previous build ignores.
"""

from alembic import op

revision = "121"
down_revision = "120"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        INSERT INTO user_preferences (user_id, other_preference)
        SELECT DISTINCT t.user_id, '{"feature_overrides": {"claude_oauth": true}}'::jsonb
        FROM user_oauth_tokens t
        JOIN users u ON u.user_id = t.user_id
        WHERE t.provider = 'claude-oauth'
        ON CONFLICT (user_id) DO UPDATE SET other_preference = jsonb_set(
            CASE WHEN jsonb_typeof(user_preferences.other_preference) = 'object'
                 THEN user_preferences.other_preference ELSE '{}'::jsonb END,
            '{feature_overrides}',
            CASE WHEN jsonb_typeof(user_preferences.other_preference -> 'feature_overrides') = 'object'
                 THEN user_preferences.other_preference -> 'feature_overrides' ELSE '{}'::jsonb END
            || '{"claude_oauth": true}'::jsonb
        )
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE user_preferences
        SET other_preference = jsonb_set(
            other_preference, '{feature_overrides}', (other_preference -> 'feature_overrides') - 'claude_oauth'
        )
        WHERE jsonb_typeof(other_preference -> 'feature_overrides') = 'object'
          AND other_preference -> 'feature_overrides' ? 'claude_oauth'
    """)
