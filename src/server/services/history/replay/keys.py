"""Whether a turn's stored lines still stand for it: the lines key.

A key is the deploy's ``lines_epoch()``, a dot, and a digest of the rows the
turn was projected from. Nothing here decides what a line holds, so this
module is not among the ``projection_sources()`` it digests.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.server.services.history import slices
from src.server.services.runs.sse_producer import resolve_token_threshold

if TYPE_CHECKING:
    from src.server.services.history.reader import TurnAnchor
    from src.server.services.history.replay.turn import Inputs

# Bump to re-project every stored turn when its output changes for a reason
# the digested sources below do not show (a ptc_agent helper's output).
LINES_VERSION = 1

_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parents[3]

# The package modules that decide what a turn's lines hold. The others decide
# only whether and when lines are stored and read: the root (pairing, paging,
# serving stored lines), cold (batching and stores), keys, errors, refresh,
# and stored_events (the fallback, whose lines are never stored).
_PROJECTION_MODULES = (
    "turn",
    "items",
    "facts",
    "legacy",
    "stored_merge",
    "run_lane",
    "lines",
    "widgets",
    "stopped",
    "lanes",
)


def projection_sources() -> list[Path]:
    """The code that turns slices into lines. A test holds this list closed
    over its ``src`` imports, so a helper a projection module starts calling
    lands here or in that test's reasoned exclusions."""
    return sorted(
        [
            *(_HERE / f"{name}.py" for name in _PROJECTION_MODULES),
            _HERE.parent / "projector.py",
            _HERE.parent / "task_status.py",
            # Helpers that shape the projected text and payloads. The
            # sanitizer is here so a scrubbing fix reaches lines stored before
            # it, the credit payload so a field it hides stays hidden.
            _SRC / "server/utils/error_sanitization.py",
            _SRC / "server/utils/content_normalizer.py",
            _SRC / "server/utils/text_phase.py",
            _SRC / "server/database/provenance.py",
            _SRC / "server/services/runs/credit_usage.py",
            _SRC / "server/contracts/status.py",
            _SRC / "llms/content_utils.py",
            _SRC / "llms/token_counter.py",
        ]
    )


def _projection_source_digest() -> str:
    """Digest of the projection's code: stored lines carry no expiry, so a
    deploy that changes the projection must not keep serving what the old
    code projected."""
    digest = hashlib.blake2b(digest_size=8)
    for path in projection_sources():
        digest.update(str(path.relative_to(_SRC)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


_PROJECTION_VERSION = (
    f"{LINES_VERSION}.{slices.SLICE_KEY}.{_projection_source_digest()}"
)


def lines_epoch() -> str:
    """The part of every lines key a deploy decides: the projection code and
    the config it reads. Every key starts with it and a dot, so the lines a
    deploy made stale are found in SQL without recomputing each turn's key."""
    digest = hashlib.blake2b(digest_size=8)
    digest.update(f"{_PROJECTION_VERSION}\x1e{resolve_token_threshold()}".encode())
    return digest.hexdigest()


def _row_generation(row: dict[str, Any] | None, id_column: str) -> Any:
    """A row reduced to its identity plus its Postgres row version: any
    rewrite lands a new tuple with a new ``xmin``, so the version stands in
    for every column without serializing megabyte-scale ones. Rows read
    without ``xmin`` hash whole."""
    if not row or "xmin" not in row:
        return row
    return {id_column: row.get(id_column), "xmin": row["xmin"]}


def lines_key(
    inputs: Inputs,
    turn_index: Any,
    anchor: TurnAnchor,
    response: dict[str, Any] | None,
) -> str:
    """The deploy's ``lines_epoch()``, then a digest of every input a turn's
    lines depend on beyond its slice and its runs' slices (which only store
    once final). ``response`` is the row the lines were projected from, which
    may be newer than the one the read started with. The facts of the runs
    it launched are in it: they are recorded after a run settles.

    Each source is hashed whole rather than a chosen subset of fields: an
    over-inclusive key only costs an extra projection when something
    irrelevant changes, while an under-inclusive one serves stale lines
    forever.
    """
    response_id = str(response["conversation_response_id"]) if response else None
    digest = hashlib.blake2b(digest_size=16)
    for source in (
        _row_generation(response, "conversation_response_id"),
        [
            _row_generation(q, "conversation_query_id")
            for q in inputs.queries_by_turn.get(turn_index, [])
        ],
        inputs.provenance_by_response.get(response_id) or [],
        inputs.usage_by_response.get(response_id),
        anchor.ending_interrupts,
        inputs.run_facts.for_turn(turn_index),
    ):
        digest.update(json.dumps(source, default=str, sort_keys=True).encode())
        digest.update(b"\x1e")
    return f"{lines_epoch()}.{digest.hexdigest()}"
