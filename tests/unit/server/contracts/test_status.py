"""Locks the canonical run-status vocabulary (F14).

TERMINAL_STATUSES is the one internal terminal set: both run ledgers import
it, and migration 020's CHECK constraint must enumerate exactly the same
outcomes (plus the live 'in_progress'). The CHECK-binding test parses the
migration SQL so drift between the Python constant and the schema fails the
default unit suite — no live database required.
"""

import re
from pathlib import Path

from src.server.contracts.status import (
    LIVE_PUBLIC_STATUSES,
    PUBLIC_STATUSES,
    RAW_LIVE_STATUSES,
    RAW_TERMINAL_SNAPSHOT_STATUSES,
    TERMINAL_PUBLIC_STATUSES,
    TERMINAL_STATUSES,
    is_live,
    is_terminal,
    to_public,
)

MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[4] / "migrations" / "versions"
)


class TestTerminalSet:
    def test_terminal_statuses_members(self):
        assert set(TERMINAL_STATUSES) == {
            "completed",
            "interrupted",
            "error",
            "cancelled",
        }

    def test_ledgers_share_the_constant(self):
        from src.server.database.runs import subagent_runs as sr_db
        from src.server.database.runs import lifecycle as tl_db

        assert tl_db.TERMINAL_STATUSES is TERMINAL_STATUSES
        assert sr_db.TERMINAL_STATUSES is TERMINAL_STATUSES


class TestPredicates:
    def test_is_terminal(self):
        for status in TERMINAL_STATUSES:
            assert is_terminal(status)
        assert not is_terminal("in_progress")
        assert not is_terminal("active")
        assert not is_terminal(None)
        assert not is_terminal("bogus")

    def test_is_live(self):
        assert is_live("in_progress")
        assert is_live("active")
        for status in TERMINAL_STATUSES:
            assert not is_live(status)
        assert not is_live(None)

    def test_enum_like_values_unwrap(self):
        class Raw:
            value = "interrupted"

        assert is_terminal(Raw())
        assert not is_live(Raw())

    def test_every_terminal_maps_into_public_vocabulary(self):
        # 'error' -> 'failed'; the rest pass through unchanged.
        assert {to_public(s) for s in TERMINAL_STATUSES} == {
            "completed",
            "interrupted",
            "failed",
            "cancelled",
        }


class TestPartitions:
    """The classification tuples must stay a partition of their vocabulary.

    The feed classifies twice — in SQL (RAW_* IN-lists, threads_read /
    threads_write) and in Python (`to_public` + TERMINAL_PUBLIC_STATUSES,
    thread_lifecycle). These pins make the two agree BY CONTRACT: a raw
    status added to one family and not the other, or a raw→public mapping
    that crosses families, fails here instead of drifting silently.
    """

    def test_raw_tuples_partition_the_run_row_vocabulary(self):
        raw_live = set(RAW_LIVE_STATUSES)
        raw_terminal = set(RAW_TERMINAL_SNAPSHOT_STATUSES)
        assert raw_live.isdisjoint(raw_terminal)
        assert raw_live | raw_terminal == {"in_progress", *TERMINAL_STATUSES}

    def test_public_vocabulary_partitions_into_families(self):
        live = set(LIVE_PUBLIC_STATUSES)
        terminal = set(TERMINAL_PUBLIC_STATUSES)
        assert live.isdisjoint(terminal)
        assert live | terminal | {"idle", "interrupted"} == PUBLIC_STATUSES

    def test_raw_families_map_into_matching_public_families(self):
        # SQL's `live` branch must agree with what the Python projection
        # would call live (or awaiting input); ditto terminal → unseen.
        for status in RAW_LIVE_STATUSES:
            assert to_public(status) in {*LIVE_PUBLIC_STATUSES, "interrupted"}
        for status in RAW_TERMINAL_SNAPSHOT_STATUSES:
            assert to_public(status) in TERMINAL_PUBLIC_STATUSES


class TestMigrationCheckBinding:
    def test_subagent_runs_check_matches_constant(self):
        sql = (MIGRATIONS_DIR / "020_subagent_run_ledger.py").read_text()
        match = re.search(r"CHECK \(status IN\s*\(([^)]*)\)", sql)
        assert match, "status CHECK not found in migration 020"
        check_values = set(re.findall(r"'(\w+)'", match.group(1)))
        assert check_values == {"in_progress", *TERMINAL_STATUSES}
