"""Loads of the ``messages`` DeltaChannel the way the server performs them,
and the verified transaction that rewrites how a thread stores them.

``thin_message_snapshots`` plans against a thread's checkpoint tree and checks
its own writes by loading through the server's reader and saver inside the
writing transaction, before and after; these are that tree, those loads,
metered, and the transaction around them.
"""

from __future__ import annotations

import enum
import functools
import hashlib
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from scripts._errors import where

MESSAGES = "messages"

# The releases whose storage format and load path these scripts were checked
# against: the stage-1 walk stops at the first checkpoint whose
# ``channel_values`` has a ``messages`` key, a direct load joins the blob by
# channel version, and a flag without a blob seeds empty.
SUPPORTED_RELEASES = {
    "langgraph": "1.2.11",
    "langgraph-checkpoint": "4.1.1",
    "langgraph-checkpoint-postgres": "3.1.1",
}


def unsupported_saver() -> str | None:
    """Why the installed saver is not one these scripts may edit, or None."""
    from importlib.metadata import PackageNotFoundError, version

    for dist, wanted in SUPPORTED_RELEASES.items():
        try:
            installed = version(dist)
        except PackageNotFoundError:
            return f"{dist} is not installed"
        if installed != wanted:
            return f"{dist} {installed} is not a verified release ({wanted})"
    # Longer walks reach targets past the first stage-1 page, which only the
    # server's walk guard rebuilds correctly on these releases.
    import src.server.utils.checkpointer  # noqa: F401
    from langgraph.checkpoint.postgres.base import BasePostgresSaver

    if not getattr(BasePostgresSaver._try_advance_walks, "_waits_for_target", False):
        return "the server's delta walk guard is not installed"
    return None


class Mode(enum.Enum):
    DRY_RUN = "dry run"
    REHEARSE = "rehearse"
    APPLY = "apply"


@dataclass
class Walk:
    """What one load cost the saver: its stage-1 scan and the writes it replayed."""

    pages: int = 0
    rows: int = 0
    chain: int = 0
    writes: int = 0
    seed: bool = False

    def __str__(self) -> str:
        if not self.pages:
            return "no walk"
        return (
            f"pages={self.pages} stage1_rows={self.rows} chain={self.chain} "
            f"writes={self.writes} seed={'yes' if self.seed else 'no'}"
        )

    def no_longer_than(self, other: Walk) -> bool:
        return (
            self.pages <= other.pages
            and self.chain <= other.chain
            and self.writes <= other.writes
        )


_NODES_SQL = """
SELECT checkpoint_id, parent_checkpoint_id,
       checkpoint -> 'channel_versions' ->> 'messages' AS version,
       (checkpoint -> 'channel_values' -> 'messages') IS NOT NULL AS flagged,
       xmin::text AS xmin
FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = ''
ORDER BY checkpoint_id
"""

_BLOB_SIZES_SQL = """
SELECT version, length(blob) AS size FROM checkpoint_blobs
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages'
  AND type <> 'empty' AND blob IS NOT NULL
"""


@dataclass
class Node:
    checkpoint_id: str
    parent: str | None
    version: str | None
    flagged: bool
    xmin: str
    # messages versions along the path from the root: the updates a walk
    # from an ancestor replays.
    updates: int = 0


class Tree:
    """A thread's root-namespace checkpoints, parents before children."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.nodes: dict[str, Node] = {}
        self.order: list[str] = []
        self.children: dict[str, list[str]] = defaultdict(list)
        self.by_version: dict[str, list[str]] = defaultdict(list)
        self.problem: str | None = None
        for row in rows:
            cid = row["checkpoint_id"]
            self.nodes[cid] = Node(
                cid, row["parent_checkpoint_id"], row["version"], row["flagged"], row["xmin"]
            )
            self.order.append(cid)
        for cid in self.order:
            node = self.nodes[cid]
            parent = self.nodes.get(node.parent) if node.parent else None
            if node.parent is not None and parent is None:
                self.problem = "a checkpoint's parent is missing"
            elif parent is not None and node.parent >= cid:
                # The planners visit parents first, in id order.
                self.problem = "a parent sorts after its child"
            elif parent is not None:
                self.children[node.parent].append(cid)
                node.updates = parent.updates + (node.version != parent.version)
            if node.version is not None:
                self.by_version[node.version].append(cid)

    def leaves(self) -> list[str]:
        return [cid for cid in self.order if not self.children.get(cid)]

    def walk(self, checkpoint_id: str, flags: set[str]) -> tuple[str | None, list[str]]:
        """The saver's walk for a checkpoint that loads no blob: parents from
        its own up to and including the first flagged one, which seeds it."""
        chain: list[str] = []
        cur = self.nodes[checkpoint_id].parent
        while cur is not None:
            chain.append(cur)
            if cur in flags:
                return cur, chain
            cur = self.nodes[cur].parent
        return None, chain


async def read_tree(conn: psycopg.AsyncConnection, thread_id: str) -> tuple[Tree, dict[str, int]]:
    """The thread's tree, and the size of each stored ``messages`` blob by version."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_NODES_SQL, (thread_id,))
        tree = Tree(await cur.fetchall())
        await cur.execute(_BLOB_SIZES_SQL, (thread_id,))
        sizes = {r["version"]: r["size"] for r in await cur.fetchall()}
    return tree, sizes


@functools.cache
def _metered_saver_class():
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from langgraph.checkpoint.postgres.base import BasePostgresSaver

    class MeteredSaver(AsyncPostgresSaver):
        """The server's saver, counting what each delta rebuild reads.

        Bound to one connection, so a load inside a script's transaction sees
        what it has written but not yet committed.
        """

        def __init__(self, conn: psycopg.AsyncConnection) -> None:
            super().__init__(conn)
            self.walk = Walk()

        def _ingest_stage1_page(self, stage1_rows, *args):  # type: ignore[override]
            self.walk.pages += 1
            self.walk.rows += len(stage1_rows)
            return BasePostgresSaver._ingest_stage1_page(stage1_rows, *args)

        def _build_delta_channels_writes_history(self, **kwargs):
            histories = super()._build_delta_channels_writes_history(**kwargs)
            history = histories.get(MESSAGES) or {}
            self.walk.chain += len(kwargs["chain_by_ch"].get(MESSAGES, ()))
            self.walk.writes += len(history.get("writes", ()))
            self.walk.seed = self.walk.seed or "seed" in history
            return histories

    return MeteredSaver


class Loader:
    """Reads ``messages`` through the server's reader graph on one connection."""

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        # Importing the checkpointer module installs the server's walk guard on
        # the saver base class; the reader is what the history endpoints use.
        import src.server.utils.checkpointer  # noqa: F401
        from src.server.services.history.reader import CheckpointHistoryReader

        self.saver = _metered_saver_class()(conn)
        self.reader = CheckpointHistoryReader(self.saver)

    @property
    def serde(self) -> Any:
        return self.saver.serde

    @property
    def spec(self) -> Any:
        """The ``messages`` channel as the reader graph declares it."""
        return self.reader.graph.channels[MESSAGES]

    async def channel_value(self, thread_id: str, checkpoint_id: str) -> tuple[list, Walk]:
        """The channel's value at the checkpoint, before its own pending writes.

        This is what a snapshot must hold: a child replays the checkpoint's
        writes on top of it. ``aget_state`` would also apply null-task writes.
        """
        from langgraph.pregel._checkpoint import achannels_from_checkpoint

        self.saver.walk = Walk()
        config = {
            "configurable": {"thread_id": thread_id, "checkpoint_ns": "", "checkpoint_id": checkpoint_id}
        }
        saved = await self.saver.aget_tuple(config)
        if saved is None:
            raise LookupError(f"checkpoint {checkpoint_id} not found")
        graph = self.reader.graph
        graph._migrate_checkpoint(saved.checkpoint)  # as aget_state does first
        channels, _ = await achannels_from_checkpoint(
            graph.channels, saved.checkpoint, saver=self.saver, config=saved.config
        )
        return list(channels[MESSAGES].get()), self.saver.walk

    async def state_messages(self, thread_id: str, checkpoint_id: str) -> tuple[list, Walk]:
        """``messages`` as the history reader serves the checkpoint's state."""
        self.saver.walk = Walk()
        snapshot = await self.reader.aget_state(thread_id, checkpoint_id)
        return list(snapshot.values.get(MESSAGES) or []), self.saver.walk

    async def digest(self, thread_id: str, checkpoint_id: str) -> tuple[str, Walk]:
        messages, walk = await self.state_messages(thread_id, checkpoint_id)
        return messages_digest(self.serde, messages), walk


def messages_digest(serde: Any, messages: list) -> str:
    """A list's serialized bytes, hashed: two lists match when their digests
    do, without holding either. Ids, types, content and every field count."""
    tag, data = serde.dumps_typed(list(messages))
    digest = hashlib.blake2b(tag.encode(), digest_size=16)
    digest.update(b"\x1e")
    digest.update(data)
    return digest.hexdigest()


def describe(conninfo: str) -> str:
    params = conninfo_to_dict(conninfo)
    return f"{params.get('host', '?')}:{params.get('port', '5432')}/{params.get('dbname', '?')}"


# --------------------------------------------------------------------------
# One thread's verified rewrite
# --------------------------------------------------------------------------


class Status(enum.Enum):
    PLANNED = "would write"
    NOTHING = "nothing to do"
    REHEARSED = "rehearsed"
    WRITTEN = "written"
    SKIPPED = "skipped"
    MISMATCH = "mismatch"
    ERROR = "error"


class Skip(Exception):
    """Leave the thread as it is, for the reason given: raised by a plan, or by
    its write when a row is no longer what the plan read."""


@dataclass(frozen=True)
class Timeouts:
    statement_s: float = 120.0
    lock_s: float = 5.0

    async def set_on(self, conn: psycopg.AsyncConnection) -> None:
        statement = f"{int(self.statement_s * 1000)}ms"
        # Planning and loads run in Python with the transaction open, so the
        # session gets as long idle in it as one statement, and no longer.
        await conn.execute(
            "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false),"
            " set_config('idle_in_transaction_session_timeout', %s, false)",
            (statement, f"{int(self.lock_s * 1000)}ms", statement),
        )


R = TypeVar("R")


@dataclass
class Rewrite(Generic[R]):
    """A thread's planned change, which ``rewrite_thread`` carries out.

    ``write`` raises ``Skip`` to refuse; without one there is nothing to do.
    ``verify`` gets every check once loaded again and names what it finds
    broken, or returns None.
    """

    report: R
    checks: list[str] = field(default_factory=list)
    committed: str | None = None  # the check loaded again once committed
    write: Callable[[], Awaitable[None]] | None = None
    verify: Callable[[dict[str, Check]], Awaitable[str | None]] | None = None


@dataclass
class Check:
    checkpoint_id: str
    before: Walk
    digest: str  # the pre-write load, compared against after the write
    after: Walk | None = None
    match: bool | None = None


@dataclass
class Result(Generic[R]):
    thread_id: str
    status: Status
    reason: str = ""
    report: R | None = None
    checks: list[Check] = field(default_factory=list)
    committed: Check | None = None
    committed_load: Walk | None = None
    committed_match: bool | None = None

    @property
    def failed(self) -> bool:
        return self.status in (Status.MISMATCH, Status.ERROR) or self.committed_match is False


Planner = Callable[[psycopg.AsyncConnection, Loader], Awaitable[Rewrite[R]]]


async def rewrite_thread(
    conninfo: str, thread_id: str, mode: Mode, timeouts: Timeouts, plan: Planner[R]
) -> Result[R]:
    """Plan, write and verify one thread in its own REPEATABLE READ transaction.

    Every read, the write and both rounds of loads share one snapshot, so what
    is compared after the write is exactly what was read before it. A dry run
    only plans. The transaction commits only in apply mode once every check
    loads as it did before and ``verify`` finds nothing; any other ending
    rolls it back.
    """
    refusal = unsupported_saver()
    if refusal:
        return Result(thread_id, Status.ERROR, f"refused: {refusal}")
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as conn:
        await timeouts.set_on(conn)
        try:
            async with conn.transaction() as tx:
                isolation = "REPEATABLE READ" + (" READ ONLY" if mode is Mode.DRY_RUN else "")
                await conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
                result = await _rewrite(conn, thread_id, mode, plan)
                if not (mode is Mode.APPLY and result.status is Status.WRITTEN):
                    raise psycopg.Rollback(tx)
        except psycopg.errors.SerializationFailure:
            return Result(thread_id, Status.SKIPPED, "a checkpoint changed concurrently")
        except psycopg.errors.LockNotAvailable:
            return Result(thread_id, Status.SKIPPED, "a checkpoint row is locked by another writer")
        except psycopg.errors.QueryCanceled:
            return Result(thread_id, Status.SKIPPED, "statement timeout")
        except Exception as exc:  # one thread's failure must not stop the run
            return Result(thread_id, Status.ERROR, where(exc))

        if result.status is Status.WRITTEN and result.committed is not None:
            # The next load the way the server does it: a later transaction,
            # reading only committed rows.
            check = result.committed
            try:
                digest, walk = await Loader(conn).digest(thread_id, check.checkpoint_id)
            except Exception as exc:
                result.committed_match, result.reason = False, f"committed load failed: {where(exc)}"
            else:
                result.committed_load = walk
                assert check.after is not None
                result.committed_match = digest == check.digest and walk.no_longer_than(check.after)
    return result


async def _rewrite(
    conn: psycopg.AsyncConnection, thread_id: str, mode: Mode, plan: Planner[R]
) -> Result[R]:
    loader = Loader(conn)
    try:
        rewrite = await plan(conn, loader)
    except Skip as skip:
        return Result(thread_id, Status.SKIPPED, str(skip))
    result = Result(thread_id, Status.NOTHING, report=rewrite.report)
    if rewrite.write is None:
        return result
    if mode is Mode.DRY_RUN:
        result.status = Status.PLANNED
        return result

    ids = list(rewrite.checks)
    if rewrite.committed is not None and rewrite.committed not in ids:
        ids.insert(0, rewrite.committed)
    for cid in ids:
        digest, walk = await loader.digest(thread_id, cid)
        result.checks.append(Check(cid, walk, digest))
    try:
        await rewrite.write()
    except Skip as skip:
        result.status, result.reason = Status.SKIPPED, str(skip)
        return result

    for check in result.checks:
        digest, check.after = await loader.digest(thread_id, check.checkpoint_id)
        check.match = digest == check.digest
    checks = {c.checkpoint_id: c for c in result.checks}
    result.committed = checks.get(rewrite.committed) if rewrite.committed else None
    broken = await rewrite.verify(checks) if rewrite.verify else None
    if broken or not all(c.match for c in result.checks):
        result.status = Status.MISMATCH
        result.reason = f"rolled back: {broken}" if broken else "rolled back"
        return result
    result.status = Status.WRITTEN if mode is Mode.APPLY else Status.REHEARSED
    return result
