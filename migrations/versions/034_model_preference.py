"""Give model configuration its own column, and a per-model layer inside it.

``other_preference`` had become a junk drawer: thirteen langalpha keys spanning
four unrelated concerns, sharing a column with five keys another service writes
through the platform API. This lifts the ones that are model configuration into
``model_preference`` and leaves the rest where they are.

The keys move rather than being copied because two columns claiming the same
setting is worse than either one alone. langalpha reads ``model_preference``
with a fallback read of ``other_preference`` for one release, so a rollback
inside that window strands nothing; the fallback goes away in the next.

``feature_overrides``, ``search_provider`` and ``search_depth`` stay put, as do
ginlix-integration's channel keys (``llm_model``, ``agent_mode``,
``workspace_id``, ``telegram_mode``, ``telegram_workspace_id``) — the strip is
key-specific precisely so no other service's writes are disturbed.

Revision ID: 034
Revises: 033
"""

from alembic import op


revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


# The keys that are model configuration. Everything not named here is left in
# ``other_preference`` untouched. ``prompt_guidance`` is the odd one: no release
# ever wrote it there, and it lands there only when this revision's own
# downgrade carries it back, so naming it is what closes the round trip.
MOVED_KEYS = (
    "preferred_model",
    "preferred_flash_model",
    "compaction_model",
    "summarization_model",
    "fetch_model",
    "fallback_models",
    "custom_models",
    "custom_providers",
    "compaction_profile",
    "reasoning_effort",
    "fast_mode",
    "prompt_guidance",
)

_KEY_ARRAY = "ARRAY[" + ", ".join(f"'{k}'" for k in MOVED_KEYS) + "]::text[]"


def upgrade() -> None:
    # ``DEFAULT`` backfills the existing rows in the same statement (PG11+), so
    # a reader never sees NULL and never has to guess what NULL meant.
    op.execute("""
        ALTER TABLE user_preferences
        ADD COLUMN IF NOT EXISTS model_preference JSONB NOT NULL DEFAULT '{}'::jsonb
    """)

    # Backfill and strip in one statement so no row is ever visible with the
    # setting in both columns. ``jsonb_exists_any`` is the function spelling of
    # the ``?|`` operator — the operator's ``?`` confuses SQLAlchemy's
    # parameter parsing on the way through ``op.execute``.
    op.execute(f"""
        UPDATE user_preferences p
        SET
            model_preference = COALESCE(p.model_preference, '{{}}'::jsonb) || COALESCE(
                (
                    SELECT jsonb_object_agg(e.key, e.value)
                    FROM jsonb_each(COALESCE(p.other_preference, '{{}}'::jsonb)) e
                    WHERE e.key = ANY({_KEY_ARRAY})
                ),
                '{{}}'::jsonb
            ),
            other_preference = COALESCE(p.other_preference, '{{}}'::jsonb) - {_KEY_ARRAY},
            updated_at = NOW()
        WHERE jsonb_exists_any(COALESCE(p.other_preference, '{{}}'::jsonb), {_KEY_ARRAY})
    """)


def downgrade() -> None:
    # Carry the whole bag back, not just the keys that came from here: the
    # column also grew ``prompt_guidance``, which is a top-level setting with
    # no key list of its own, and the old shape ignores keys it does not know.
    # ``profiles`` is the one loss — a per-model map has nowhere to live in a
    # flat bag — so it is subtracted rather than folded in.
    #
    # The guard is on the column being non-empty rather than on a key list: a
    # row holding only new keys would otherwise be skipped here and dropped
    # with the column on the next statement.
    op.execute("""
        UPDATE user_preferences p
        SET
            other_preference = COALESCE(p.other_preference, '{}'::jsonb)
                || (p.model_preference - 'profiles'),
            updated_at = NOW()
        WHERE p.model_preference IS NOT NULL AND p.model_preference <> '{}'::jsonb
    """)

    op.execute("ALTER TABLE user_preferences DROP COLUMN IF EXISTS model_preference")
