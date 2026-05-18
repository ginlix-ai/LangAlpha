"""Backfill conversation_threads.platform = 'web' for pre-tracking rows.

Before this commit, only ginlix-integration set `platform` (telegram/slack/
discord/feishu). All web-originated threads (ChatAgent and MarketView) wrote
NULL. From this commit on, web clients send 'web' or 'market_view:<SYMBOL>'.

This migration tags every NULL-platform web row as 'web' so the new prefix
filter (e.g. WHERE platform LIKE 'market_view%') sees a uniform namespace.

`external_id IS NULL` is the safety guard: channel-integration rows always
write `external_id` (chat_id:topic_id), so excluding them protects any
legacy channel row that happens to be missing platform from being
mis-tagged as 'web'.

Revision ID: 011
"""

from alembic import op


revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE conversation_threads
        SET platform = 'web'
        WHERE platform IS NULL
          AND external_id IS NULL
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE conversation_threads
        SET platform = NULL
        WHERE platform = 'web'
    """)
