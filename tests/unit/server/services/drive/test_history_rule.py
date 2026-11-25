"""The history rule's settings as the rule's one definition takes them
(``records.history_sql``). Which versions go, and when, is SQL and was
verified live against the stack's Postgres (rewrites in parts, counts, age,
the grace); this pins the defaults and the hand-off."""

from __future__ import annotations

from src.config.models import DriveConfig
from src.server.database.drive.records import history_sql
from src.server.services import drive

MIB = 1024 * 1024


def test_defaults():
    h = DriveConfig().history
    assert (h.max_bytes, h.keep_days, h.keep_max, h.replaced_grace_seconds) == (100 * MIB, 30, 10, 3600)


def test_the_config_reaches_the_rule_as_its_params(monkeypatch):
    monkeypatch.setattr(
        drive, "_config", lambda: DriveConfig(history={"max_bytes": None, "keep_days": 7, "keep_max": None})
    )
    rule = drive.history_rule()
    assert rule.params == {"max": None, "keep": None, "days": 7}
    assert rule.grace_seconds == 3600


def test_every_scope_names_the_params_it_takes():
    for scope, own in (("entry", "%(entry)s"), ("space", "%(space)s"), ("spaces", "%(spaces)s")):
        sql = history_sql(scope)
        for param in ("%(max)s", "%(keep)s", "%(days)s"):
            assert param in sql
        if own:
            assert own in sql
