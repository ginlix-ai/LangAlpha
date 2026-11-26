"""What an ordinary file keeps of its history, as config hands it to the
rule (``records.HistoryRule``). Which versions go, and when, is SQL, pinned
against Postgres in ``tests/integration/test_drive_db.py``."""

from __future__ import annotations

from src.config.models import DriveConfig
from src.server.services import drive

MIB = 1024 * 1024


def test_defaults():
    h = DriveConfig().history
    assert (h.max_bytes, h.undo_days, h.replaced_grace_seconds) == (100 * MIB, 7, 3600)


def test_the_config_reaches_the_rule(monkeypatch):
    monkeypatch.setattr(
        drive, "_config", lambda: DriveConfig(history={"max_bytes": None, "undo_days": 3})
    )
    rule = drive.history_rule()
    assert (rule.max_bytes, rule.undo_days, rule.grace_seconds) == (None, 3, 3600)


def test_an_overlay_still_naming_the_old_keys_loads():
    """``keep_days`` and ``keep_max`` are gone; a deployment overlay that
    still sets them must not stop the server."""
    h = DriveConfig(history={"keep_days": 30, "keep_max": 10}).history
    assert h.undo_days == 7
