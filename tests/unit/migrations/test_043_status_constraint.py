"""The 043 CHECK constraint is a copy of ``AttemptStatus``, kept readable in psql.

A state added to the enum without a migration surfaces as a constraint
violation on the order path, where a failed write strands the attempt in
``submitting``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.server.services.brokerage_orders import AttemptStatus

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "043_order_attempts.py"
)


def test_the_check_constraint_names_every_attempt_status():
    spec = importlib.util.spec_from_file_location("migration_043", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert set(module._STATUSES) == {s.value for s in AttemptStatus}
