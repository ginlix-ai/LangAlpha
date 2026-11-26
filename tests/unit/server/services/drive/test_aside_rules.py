"""The rules of a rename through a computer's mount that need no database
(``drive/aside.py``); the SQL side is in tests/integration/test_drive_aside_db.py."""

from __future__ import annotations

import dataclasses

import pytest

from src.server.database.drive.aside import _beside, keeps_source, moved_onto_memo
from tests.unit.server.services.drive.test_memo_rules import MARKED, _entry


@pytest.mark.parametrize(
    ("source", "target", "kept"),
    [
        (MARKED, None, True),
        (MARKED, dataclasses.replace(MARKED, marked=False), True),
        (MARKED, MARKED, False),
        (None, MARKED, False),
        (None, None, False),
        (dataclasses.replace(MARKED, marked=False), None, False),
    ],
)
def test_only_a_memo_moved_onto_an_ordinary_file_keeps_its_identity(source, target, kept):
    assert keeps_source(_entry(memo=source), _entry(memo=target)) is kept


def test_the_line_for_a_memo_moved_onto_a_memo():
    assert moved_onto_memo("notes/a.md", "notes/b.md", "1b2c") == (
        "notes/a.md was a memo: moving it onto notes/b.md, also a memo, saved its text as "
        "that memo's next version and put notes/a.md with its record in the Drive's trash. "
        "To bring it back: la drive trash restore 1b2c"
    )


@pytest.mark.parametrize(
    ("moved", "backup"),
    [
        ("notes/q3.md.bak", True),
        ("notes/q3.md~", True),
        ("notes/Q3.MDbak", True),
        ("notes/orig_q3.md", True),
        ("notes/.q3.md.swp", True),
        ("notes/xq3.md", False),
        ("notes/q3.md", False),
        ("notes/old.md", False),
        ("q3.md.bak", False),
        ("notes/archive/q3.md.bak", False),
    ],
)
def test_a_backup_sits_beside_the_name_and_holds_all_of_it(moved, backup):
    """``data.md`` only ends in ``a.md``: a name of its own, so a memo
    moved there was moved on purpose."""
    assert _beside(moved, "notes/q3.md") is backup
    assert _beside("notes/data.md", "notes/a.md") is False
