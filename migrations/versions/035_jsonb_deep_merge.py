"""Teach the database to merge a JSONB patch at any depth.

``model_preference`` holds ``profiles``, a map keyed by model name, so a patch
for one model has to leave its siblings standing. ``||`` merges one level only,
so the write path had grown a hand-built clause that peeled ``profiles`` out,
re-read the row three times and depended on being concatenated *after* the
shallow merge to win. That ordering was the only thing protecting a sibling
model and nothing asserted it. One recursive function says the same thing in a
form that cannot be assembled wrong.

A JSON ``null`` in the patch deletes its key at whatever depth it appears,
which is the semantics the write path already had.

Revision ID: 035
Revises: 034
"""

from alembic import op


revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # plpgsql rather than SQL because the body calls itself, and a SQL body is
    # resolved against the catalog at creation time, when it does not yet exist.
    op.execute("""
        CREATE OR REPLACE FUNCTION jsonb_deep_merge(a jsonb, b jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        IMMUTABLE
        STRICT
        AS $$
        DECLARE
            merged jsonb;
            k text;
        BEGIN
            IF jsonb_typeof(a) <> 'object' OR jsonb_typeof(b) <> 'object' THEN
                RETURN b;
            END IF;

            merged := a;
            FOR k IN SELECT jsonb_object_keys(b) LOOP
                IF b -> k = 'null'::jsonb THEN
                    merged := merged - k;
                ELSIF jsonb_typeof(b -> k) = 'object' THEN
                    -- Recurse even when the key is new, so a null nested in a
                    -- branch the row does not have yet still reads as a delete
                    -- rather than being stored as a literal null.
                    merged := jsonb_set(
                        merged,
                        ARRAY[k],
                        jsonb_deep_merge(
                            CASE WHEN jsonb_typeof(merged -> k) = 'object'
                                 THEN merged -> k ELSE '{}'::jsonb END,
                            b -> k
                        ),
                        true
                    );
                ELSE
                    merged := jsonb_set(merged, ARRAY[k], b -> k, true);
                END IF;
            END LOOP;

            RETURN merged;
        END;
        $$
    """)


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS jsonb_deep_merge(jsonb, jsonb)")
