"""Add template_entries table for the template system.

Templates are upper-layer "applications" (e.g. sirius-valuation) that group a
set of workspaces under a shared dashboard / schema. Each entry binds 1:1 to a
real workspace (its sandbox runs the template's analysis); CASCADE on
workspace deletion keeps everything consistent — no orphan entries.

Revision ID: 011
Revises: 010
Create Date: 2026-05-17
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS template_entries (
            entry_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         VARCHAR(255) NOT NULL,
            template_id     VARCHAR(64)  NOT NULL,
            workspace_id    UUID NOT NULL UNIQUE
                            REFERENCES workspaces(workspace_id) ON DELETE CASCADE,

            -- 模板内业务主键（sirius 用股票代码；未来其他模板可用别的）
            entry_key       VARCHAR(128) NOT NULL,
            display_name    VARCHAR(255),

            -- 状态机：pending → analyzing → completed / failed
            status          VARCHAR(16)  NOT NULL DEFAULT 'pending',

            -- 模板自定义的进度结构（如 {"D1":"completed","D2":"running",...}）
            progress        JSONB        NOT NULL DEFAULT '{}'::jsonb,

            -- 看板列表展示用的精简字段（4-8 个数字/标签）
            summary         JSONB        NOT NULL DEFAULT '{}'::jsonb,

            -- 完整结构化结果（详情页用）
            payload         JSONB        NOT NULL DEFAULT '{}'::jsonb,

            -- 失败时存原因
            error_message   TEXT,

            -- 模板内业务参数（初始化时传入的 market、reference_code 等）
            params          JSONB        NOT NULL DEFAULT '{}'::jsonb,

            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            completed_at    TIMESTAMPTZ,

            -- 同一用户的同一模板内，业务主键唯一（防止同一只票分析两次）
            UNIQUE(user_id, template_id, entry_key)
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_template_entries_user_template
        ON template_entries(user_id, template_id, updated_at DESC)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_template_entries_status
        ON template_entries(status)
        WHERE status IN ('pending', 'analyzing')
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_template_entries_status")
    op.execute("DROP INDEX IF EXISTS idx_template_entries_user_template")
    op.execute("DROP TABLE IF EXISTS template_entries")
