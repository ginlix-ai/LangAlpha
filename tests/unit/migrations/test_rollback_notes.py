"""A migration's rollback note names the revision below it.

A migration renumbered after another branch took its number gets a new
``down_revision``, and a note left naming the old one sends an operator's
downgrade one migration too far.
"""

from __future__ import annotations

import re
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

_ROOT = Path(__file__).resolve().parents[3]
_ROLLBACK = re.compile(r"Rollback: ``alembic downgrade (\w+)``")


def test_every_rollback_note_names_the_revision_below():
    config = Config(str(_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(_ROOT / "migrations"))
    wrong = {}
    for script in ScriptDirectory.from_config(config).walk_revisions():
        note = _ROLLBACK.search(script.module.__doc__ or "")
        if note and note.group(1) != script.down_revision:
            wrong[script.revision] = (note.group(1), script.down_revision)

    assert not wrong, wrong
