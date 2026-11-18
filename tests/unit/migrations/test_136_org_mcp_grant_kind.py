"""136 rebuilds the grant kind check without holding the relay's table lock
through the validation scan.

The relay reads ``sandbox_egress_grants`` on every call, so the scan has to run
after the transaction that took ACCESS EXCLUSIVE for the swap has committed.
"""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"
_BEGIN, _COMMIT = "<autocommit>", "</autocommit>"
_VALIDATE = (
    "ALTER TABLE sandbox_egress_grants VALIDATE CONSTRAINT "
    "sandbox_egress_grants_kind_reference"
)


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "migration_136", _VERSIONS / "136_org_mcp_grant_kind.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events: list[str] = []

    @contextmanager
    def autocommit_block():
        events.append(_BEGIN)
        yield
        events.append(_COMMIT)

    op = MagicMock()
    op.get_context.return_value.autocommit_block = autocommit_block
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    monkeypatch.setattr(module, "op", op)
    return module, events


def _alter(events: list[str]) -> int:
    return next(i for i, e in enumerate(events) if "ADD CONSTRAINT" in e)


def test_it_follows_135_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("136", "135")


@pytest.mark.parametrize("step", ["upgrade", "downgrade"])
def test_the_check_is_validated_outside_the_lock_that_adds_it(migration, step):
    module, events = migration

    getattr(module, step)()

    alter = _alter(events)
    begin, commit = events.index(_BEGIN), events.index(_COMMIT)
    assert events.count(_BEGIN) == 1
    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    assert alter < begin
    # One statement, re-runnable after an abort between it and the validation.
    assert (
        "DROP CONSTRAINT IF EXISTS sandbox_egress_grants_kind_reference"
        in events[alter]
    )
    assert events[alter].endswith("NOT VALID")
    assert not any("VALIDATE" in e for e in events[:begin])
    assert events[begin + 1 : commit] == ["SET lock_timeout = 0", _VALIDATE]
    assert commit == len(events) - 1


def test_the_downgrade_deletes_org_rows_before_the_narrower_check(migration):
    module, events = migration

    module.downgrade()

    delete = next(i for i, e in enumerate(events) if e.startswith("DELETE"))
    assert "WHERE kind = 'org_mcp'" in events[delete]
    assert delete < _alter(events)
    assert "org_mcp" not in events[_alter(events)]


@pytest.mark.parametrize("step", ["upgrade", "downgrade"])
def test_both_directions_keep_the_kinds_the_previous_check_allowed(migration, step):
    module, events = migration

    getattr(module, step)()

    check = events[_alter(events)]
    for kind in ("oauth_mcp", "header_mcp", "platform_mcp"):
        assert f"'{kind}'" in check
