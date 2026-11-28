"""A Drive file's record made while a purge takes its entry: the route checked
the file is live, the purge commits, and the insert meets the record's foreign
key. That one refusal is a file gone; any other is not this race."""

from __future__ import annotations

import psycopg
import pytest

from src.server.database.artifacts import ArtifactNotFound
from src.server.database.artifacts import records as records_db

_HOME = "00000000-0000-0000-0000-00000000000f"
_ENTRY = "00000000-0000-0000-0000-0000000000d0"


@pytest.fixture(autouse=True)
def _db(artifacts_db):
    return artifacts_db


def _violation(constraint: str) -> psycopg.errors.ForeignKeyViolation:
    class Violation(psycopg.errors.ForeignKeyViolation):
        @property
        def diag(self):
            return type("Diag", (), {"constraint_name": constraint})()

    return Violation()


@pytest.mark.asyncio
async def test_an_entry_purged_before_the_insert_is_not_found(mock_cursor):
    mock_cursor.execute.side_effect = _violation("share_links_drive_entry_id_fkey")

    with pytest.raises(ArtifactNotFound):
        await records_db.ensure_record(_HOME, "drive", _ENTRY)


@pytest.mark.asyncio
async def test_another_foreign_key_s_refusal_is_not_taken_for_it(mock_cursor):
    mock_cursor.execute.side_effect = _violation("share_links_workspace_id_fkey")

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await records_db.ensure_record(_HOME, "drive", _ENTRY)
