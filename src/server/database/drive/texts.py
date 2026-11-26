"""What the Drive reads out of its files, cached: text extracted from Word
and PDF files (``drive_extracts``, by digest), lines added and removed
between two contents (``drive_line_counts``, by the two digests), and
which version wrote each line (``drive_attributions``, by version).

All are filled on demand outside the space's lock and never counted
against the allowance; losing a row costs one recompute. Extracts and
counts are keyed by content, which never changes, so they never go stale.
An attribution depends on every version before it, so it is kept under
the file's version epoch (``drive_version_epochs``), which a trigger on
``drive_versions`` moves before any version's content changes or any
version goes, deleting the file's attributions as it does (migration 154):
a row holds exactly while its epoch is the file's.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from psycopg import errors

from src.server.database.drive.reads import fetch
from src.server.database.drive.records import (
    VERSION_COLUMNS,
    DriveError,
    Version,
    version_from_row,
)

if TYPE_CHECKING:
    # The child process's own list, which it keeps free of the server's imports.
    from src.server.services.drive.extract_child import Status

#: A ``failed`` extraction is tried again after this long.
FAILED_RETRY_SQL = "interval '1 hour'"


# --- extracts -------------------------------------------------------------


@dataclass(frozen=True)
class Extract:
    kind: str
    status: Status
    text: str | None
    truncated: bool
    #: A ``failed`` row old enough to try again.
    retry: bool


async def extracts(space_id: str, shas: list[str]) -> dict[str, Extract]:
    """The cached extracts of ``shas`` the space has."""
    if not shas:
        return {}
    rows = await fetch(
        "SELECT sha256, kind, status, text, truncated,"
        f" (status = 'failed' AND extracted_at < now() - {FAILED_RETRY_SQL}) AS retry"
        " FROM drive_extracts WHERE space_id = %(space)s AND sha256 = ANY(%(shas)s)",
        {"space": space_id, "shas": list(set(shas))},
    )
    return {r.pop("sha256"): Extract(**r) for r in rows}


async def put_extract(
    space_id: str, sha256: str, kind: str, status: Status, text: str | None, truncated: bool
) -> None:
    """Cache an extraction; an ``ok`` row is replaced only by ``ok``, so a
    later reading that timed out (two workers may read one digest) does not
    undo it. Content freed since it was read keeps nothing."""
    try:
        await fetch(
            "INSERT INTO drive_extracts (space_id, sha256, kind, status, text, truncated)"
            " VALUES (%(space)s, %(sha)s, %(kind)s, %(status)s, %(text)s, %(truncated)s)"
            " ON CONFLICT (space_id, sha256) DO UPDATE SET kind = EXCLUDED.kind,"
            " status = EXCLUDED.status, text = EXCLUDED.text,"
            " truncated = EXCLUDED.truncated, extracted_at = now()"
            " WHERE drive_extracts.status <> 'ok' OR EXCLUDED.status = 'ok'"
            " RETURNING 1",
            {
                "space": space_id,
                "sha": sha256,
                "kind": kind,
                "status": status,
                "text": text,
                "truncated": truncated,
            },
        )
    except errors.ForeignKeyViolation:
        pass


async def held_texts(space_id: str, shas: list[str]) -> dict[str, str]:
    """Text the database holds under ``shas``, decoded."""
    if not shas:
        return {}
    rows = await fetch(
        "SELECT sha256, content FROM drive_contents"
        " WHERE space_id = %(space)s AND sha256 = ANY(%(shas)s) AND content IS NOT NULL",
        {"space": space_id, "shas": list(set(shas))},
    )
    return {r["sha256"]: bytes(r["content"]).decode("utf-8", errors="replace") for r in rows}


# --- versions -------------------------------------------------------------


@dataclass(frozen=True)
class Stored:
    """One version as its text is read: which bytes, of what kind."""

    seq: int
    sha256: str
    is_text: bool
    content_type: str
    size: int


_STORED = "v.seq, v.sha256, v.is_text, v.content_type, v.byte_len AS size"


async def with_bases(entry_id: str, seqs: list[int]) -> list[tuple[Stored, Stored | None]]:
    """Versions ``seqs`` of a file, each with the version before it among
    all it keeps (None for its first)."""
    lagged = ", ".join(
        f"lag({col}) OVER w AS base_{name}"
        for col, name in (
            ("v.seq", "seq"),
            ("v.sha256", "sha256"),
            ("v.is_text", "is_text"),
            ("v.content_type", "content_type"),
            ("v.byte_len", "size"),
        )
    )
    rows = await fetch(
        f"SELECT * FROM (SELECT {_STORED}, {lagged} FROM drive_versions v"
        " WHERE v.entry_id = %(id)s WINDOW w AS (ORDER BY v.seq)) s"
        " WHERE s.seq = ANY(%(seqs)s) ORDER BY s.seq",
        {"id": entry_id, "seqs": seqs},
    )
    out = []
    for r in rows:
        base = {k.removeprefix("base_"): r.pop(k) for k in list(r) if k.startswith("base_")}
        out.append((Stored(**r), Stored(**base) if base["seq"] is not None else None))
    return out


async def versions(entry_id: str, seqs: list[int]) -> list[Version]:
    rows = await fetch(
        f"SELECT {VERSION_COLUMNS} FROM drive_versions v"
        " WHERE v.entry_id = %(id)s AND v.seq = ANY(%(seqs)s) ORDER BY v.seq DESC",
        {"id": entry_id, "seqs": seqs},
    )
    return [version_from_row(r) for r in rows]


# --- line counts ----------------------------------------------------------

#: A first version's base in ``drive_line_counts``.
NO_BASE = ""


async def line_counts(
    space_id: str, pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], tuple[int, int]]:
    """Cached ``(added, removed)`` by ``(base_sha256, sha256)``."""
    if not pairs:
        return {}
    rows = await fetch(
        "SELECT base_sha256, sha256, added, removed FROM drive_line_counts"
        " WHERE space_id = %(space)s"
        " AND (base_sha256, sha256) IN (SELECT * FROM unnest(%(bases)s::text[], %(shas)s::text[]))",
        {"space": space_id, "bases": [b for b, _ in pairs], "shas": [s for _, s in pairs]},
    )
    return {(r["base_sha256"], r["sha256"]): (r["added"], r["removed"]) for r in rows}


async def put_line_counts(space_id: str, base: str, sha: str, added: int, removed: int) -> None:
    try:
        await fetch(
            "INSERT INTO drive_line_counts (space_id, base_sha256, sha256, added, removed)"
            " VALUES (%(space)s, %(base)s, %(sha)s, %(added)s, %(removed)s)"
            " ON CONFLICT DO NOTHING RETURNING 1",
            {"space": space_id, "base": base, "sha": sha, "added": added, "removed": removed},
        )
    except errors.ForeignKeyViolation:
        pass  # its content was freed meanwhile


# --- attribution ----------------------------------------------------------


@dataclass(frozen=True)
class Chain:
    """A file's versions, oldest first, read in one snapshot with its epoch
    and which of them have an attribution cached under that epoch."""

    epoch: int
    versions: list[Stored]
    attributed: set[int]


async def chain(entry_id: str) -> Chain:
    """The file's versions and its epoch, one snapshot. Makes the epoch row
    first, so a version change after this has a row to move. A file purged
    since the caller read it is ``not_found``: either step finds no entry."""
    try:
        await fetch(
            "INSERT INTO drive_version_epochs (entry_id) VALUES (%(id)s)"
            " ON CONFLICT DO NOTHING RETURNING 1",
            {"id": entry_id},
        )
    except errors.ForeignKeyViolation:
        raise DriveError("not_found", "No such entry") from None
    rows = await fetch(
        f"SELECT {_STORED}, n.epoch, a.seq IS NOT NULL AS attributed"
        " FROM drive_versions v"
        " CROSS JOIN (SELECT epoch FROM drive_version_epochs WHERE entry_id = %(id)s) n"
        " LEFT JOIN drive_attributions a"
        " ON a.entry_id = v.entry_id AND a.seq = v.seq AND a.epoch = n.epoch"
        " WHERE v.entry_id = %(id)s ORDER BY v.seq",
        {"id": entry_id},
    )
    if not rows:
        # A file always has its head; the purge took the epoch row with it.
        raise DriveError("not_found", "No such entry")
    epoch = rows[0]["epoch"]
    attributed = {r["seq"] for r in rows if r.pop("attributed")}
    for r in rows:
        r.pop("epoch")
    return Chain(epoch=epoch, versions=[Stored(**r) for r in rows], attributed=attributed)


async def blame(entry_id: str, seq: int, epoch: int) -> list[int] | None:
    rows = await fetch(
        "SELECT blame FROM drive_attributions"
        " WHERE entry_id = %(id)s AND seq = %(seq)s AND epoch = %(epoch)s",
        {"id": entry_id, "seq": seq, "epoch": epoch},
    )
    return list(rows[0]["blame"]) if rows else None


#: Written only while the file's epoch is still ``epoch``, under one lock,
#: the epoch row's, so it never waits in a cycle with a version change: a
#: change that committed first moved the epoch, and nothing matches; one
#: that moved it and has not committed is waited for, then nothing matches;
#: one that comes after waits for this write, then moves the epoch and
#: deletes the row with every other of the file's. No version row is
#: locked: a delete of several versions holds the epoch row from its first.
_PUT_ATTRIBUTION_SQL = """
    WITH held AS (
        SELECT 1 FROM drive_version_epochs
         WHERE entry_id = %(id)s AND epoch = %(epoch)s
           FOR SHARE
    )
    INSERT INTO drive_attributions (entry_id, seq, epoch, blame)
    SELECT %(id)s::uuid, %(seq)s::int, %(epoch)s::bigint, %(blame)s::int[]
     WHERE EXISTS (SELECT 1 FROM held)
    ON CONFLICT (entry_id, seq) DO UPDATE SET epoch = EXCLUDED.epoch, blame = EXCLUDED.blame
    RETURNING 1
"""


async def put_attribution(entry_id: str, seq: int, epoch: int, blame: list[int]) -> bool:
    """Cache version ``seq``'s attribution, worked out under ``epoch``;
    whether it was kept (not when the versions changed meanwhile). The
    epoch row is there: ``chain`` makes it before ``epoch`` is read."""
    written = await fetch(
        _PUT_ATTRIBUTION_SQL,
        {"id": entry_id, "seq": seq, "epoch": epoch, "blame": blame},
    )
    return bool(written)
