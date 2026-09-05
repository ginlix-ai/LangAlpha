"""The orphan report must describe exactly what the sweep will act on.

An orphan is a blob no manifest row references, which is the same relation the
condemn pass and the reap re-check evaluate. Two spellings of it drift, and the
drift shows up as an operator chasing bytes the GC is never going to reclaim,
so the report negates the sweep's own predicate rather than restating it.
"""

from __future__ import annotations

import itertools

from scripts.ops.report_orphan_blobs import _GC_BACKLOG_SQL, _ORPHAN_PREDICATE
from src.server.database.blob_keys import REFERENCED_SQL

_EXISTS_BLOB = "EXISTS (SELECT 1 FROM workspace_files f JOIN workspaces w ON w.workspace_id = f.workspace_id WHERE f.blob_sha256 = b.sha256 AND w.user_id = b.user_id)"
_EXISTS_PACK = "EXISTS (SELECT 1 FROM workspace_files f JOIN workspaces w ON w.workspace_id = f.workspace_id WHERE f.pack_sha256 = b.sha256 AND w.user_id = b.user_id)"


def _evaluate(sql: str, *, blob_ref: bool, pack_ref: bool) -> bool:
    """Read the predicate as a boolean expression over its two EXISTS clauses."""
    expr = sql.replace(_EXISTS_BLOB, str(blob_ref)).replace(_EXISTS_PACK, str(pack_ref))
    assert "EXISTS" not in expr, f"unsubstituted subquery in {expr!r}"
    for sql_op, py_op in (("NOT", "not"), (" OR ", " or "), (" AND ", " and ")):
        expr = expr.replace(sql_op, py_op)
    return bool(eval(expr))  # noqa: S307 - operands are the booleans just substituted


def test_orphan_predicate_is_the_negation_of_referenced():
    for blob_ref, pack_ref in itertools.product([True, False], repeat=2):
        referenced = _evaluate(REFERENCED_SQL, blob_ref=blob_ref, pack_ref=pack_ref)
        orphan = _evaluate(_ORPHAN_PREDICATE, blob_ref=blob_ref, pack_ref=pack_ref)
        assert orphan is not referenced, (blob_ref, pack_ref)


def test_gc_backlog_counters_are_taken_over_orphans_only():
    """A referenced blob past the grace period is not condemnable and a
    condemned one that regained a reference is revived by the reap, so counting
    either reports a backlog the sweep will never work through."""
    assert _ORPHAN_PREDICATE in _GC_BACKLOG_SQL


def test_referenced_predicate_covers_both_pointer_columns():
    """A row referenced through either column is referenced; the pack column is
    the one a reader forgets, and forgetting it deletes live chunks."""
    assert _evaluate(REFERENCED_SQL, blob_ref=False, pack_ref=True)
    assert _evaluate(REFERENCED_SQL, blob_ref=True, pack_ref=False)
