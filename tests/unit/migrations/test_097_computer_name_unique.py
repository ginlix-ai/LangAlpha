"""097's dedupe and index, pinned.

The app tells a name refusal from any other unique violation by the index's
name, and reads a holder with the index's fold, so the two must not drift.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.server.database import computer_names as N
from src.server.database.sql_fences import FENCE_NOT_DELETED

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


def _load():
    spec = importlib.util.spec_from_file_location(
        "migration_097", _VERSIONS / "097_computer_name_unique.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _flat(sql) -> str:
    return re.sub(r"\s+", " ", str(sql)).strip()


@pytest.fixture
def migration(monkeypatch):
    """The module over a fake database holding ``taken``, folded as the index folds."""
    module = _load()
    events: list[str] = []
    taken: set[tuple[str, str]] = set()
    duplicates: list[tuple[str, str, str]] = []

    def execute(sql, params=None):
        statement = _flat(sql)
        events.append(statement)
        if "row_number()" in statement:
            return SimpleNamespace(fetchall=lambda: list(duplicates))
        if statement.startswith("SELECT EXISTS"):
            key = (params["user_id"], " ".join(params["name"].split()).lower())
            return SimpleNamespace(scalar=lambda: key in taken)
        if statement.startswith("UPDATE computers"):
            events[-1] = f"rename {params['computer_id']} {params['name']}"
            user = next(u for c, u, _b in duplicates if c == params["computer_id"])
            taken.add((user, " ".join(params["name"].split()).lower()))
        return SimpleNamespace()

    op = MagicMock()
    op.get_bind.return_value.execute.side_effect = execute
    op.execute.side_effect = lambda sql: events.append(_flat(sql))
    monkeypatch.setattr(module, "op", op)
    return SimpleNamespace(
        module=module, events=events, taken=taken, duplicates=duplicates
    )


def test_it_follows_096_in_a_linear_chain(migration):
    assert (migration.module.revision, migration.module.down_revision) == ("097", "096")


def test_the_index_is_the_one_the_app_reads_refusals_by(migration):
    migration.module.upgrade()

    index = migration.events[-1]
    assert index.startswith(f"CREATE UNIQUE INDEX IF NOT EXISTS {N.COMPUTER_NAME_INDEX}")
    assert f"(user_id, {N._NAME_KEY.format('name')})" in index
    assert index.endswith(f"WHERE {FENCE_NOT_DELETED}")


def test_later_duplicates_take_the_first_free_number_and_the_oldest_keeps_its_name(
    migration,
):
    """A typed "Cloud computer 2" is skipped by the first later duplicate; the
    oldest "Cloud computer" is never among the duplicates at all."""
    migration.taken.update(
        {("u1", "cloud computer"), ("u1", "cloud computer 2"), ("u2", "lab")}
    )
    migration.duplicates.extend(
        [("c2", "u1", "Cloud computer"), ("c3", "u1", "cloud computer"), ("c4", "u2", "Lab")]
    )

    migration.module.upgrade()

    renames = [e for e in migration.events if e.startswith("rename")]
    assert renames == [
        "rename c2 Cloud computer 3",
        "rename c3 cloud computer 4",
        "rename c4 Lab 2",
    ]


def test_nothing_is_read_before_writers_are_held_and_the_index_comes_last(migration):
    migration.duplicates.append(("c2", "u1", "Lab"))
    migration.taken.add(("u1", "lab"))

    migration.module.upgrade()

    events = migration.events
    lock = events.index("LOCK TABLE computers IN SHARE ROW EXCLUSIVE MODE")
    read = next(i for i, e in enumerate(events) if "row_number()" in e)
    rename = next(i for i, e in enumerate(events) if e.startswith("rename"))
    index = next(i for i, e in enumerate(events) if e.startswith("CREATE UNIQUE INDEX"))
    assert lock < read < rename < index


def test_a_number_never_pushes_a_long_name_past_the_column(migration):
    migration.duplicates.append(("c2", "u1", "x" * 255))
    migration.taken.add(("u1", "x" * 255))

    migration.module.upgrade()

    rename = next(e for e in migration.events if e.startswith("rename"))
    name = rename.split(" ", 2)[2]
    assert name == "x" * 253 + " 2"


def test_downgrade_drops_only_the_index(migration):
    migration.module.downgrade()

    assert migration.events[-1] == f"DROP INDEX IF EXISTS {N.COMPUTER_NAME_INDEX}"
    assert not any(e.startswith("UPDATE") for e in migration.events)
