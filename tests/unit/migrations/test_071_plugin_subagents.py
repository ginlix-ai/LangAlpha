"""071's DDL, pinned as SQL: a plugin's subagents go with its row, and a name
is one user's once."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "migration_071", _VERSIONS / "071_plugin_subagents.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _flat(op) -> str:
    return re.sub(
        r"\s+", " ", " ".join(str(c.args[0]) for c in op.execute.call_args_list)
    )


def test_it_follows_070(migration):
    module, _op = migration
    assert (module.revision, module.down_revision) == ("071", "070")


def test_the_rows_cascade_with_their_plugin(migration):
    # CASCADE where the other component tables RESTRICT: a build that never
    # reads this table can still uninstall a plugin that has rows in it.
    module, op = migration
    module.upgrade()
    assert (
        "FOREIGN KEY (user_id, plugin_id) REFERENCES user_plugins(user_id, user_plugin_id) "
        "ON DELETE CASCADE" in _flat(op)
    )


def test_a_name_is_one_users_once(migration):
    module, op = migration
    module.upgrade()
    assert "CONSTRAINT uq_plugin_subagents_user_name UNIQUE (user_id, name)" in _flat(op)
