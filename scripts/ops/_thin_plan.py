"""The thinning planner: which ``messages`` blobs a thread keeps and flags.

It reads only the thread's tree, its stored blobs and its writes, and
computes loads the way the saver does to propose a plan. The plan is only a
proposal: what commits is decided by real loads before and after the write
(``_delta_loads.rewrite_thread``).
"""

from __future__ import annotations

from collections import Counter, OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from scripts.ops._delta_loads import Loader, Tree, messages_digest

# A checkpoint's ``messages`` writes as the saver replays them: (task id,
# channel, value), in ``writes_sort_key`` order.
Writes = dict[str, list[tuple[str, str, Any]]]


@dataclass
class Unit:
    """One stored ``messages`` blob and the checkpoints that load it directly
    (every checkpoint at its version)."""

    version: str
    size: int
    loaders: list[str]
    flagged: list[str]

    @property
    def is_snapshot(self) -> bool:
        return bool(self.flagged)


@dataclass
class Layout:
    """Which checkpoints flag a snapshot, and which versions hold a blob."""

    flags: set[str]
    stored: set[str]

    def copy(self) -> Layout:
        return Layout(set(self.flags), set(self.stored))


class Replayer:
    """Loads computed the way the saver computes them, from the rows the
    planning transaction read, under any layout."""

    def __init__(
        self,
        thread_id: str,
        tree: Tree,
        loader: Loader,
        read_blob: Callable[[str], Awaitable[tuple[str, bytes] | None]],
        read_writes: Callable[[], Awaitable[Writes]],
    ) -> None:
        self._thread_id = thread_id
        self._tree = tree
        self._spec = loader.spec
        self.serde = loader.serde
        self._read_blob = read_blob
        self._read_writes = read_writes
        self._writes: Writes | None = None
        self._raw: OrderedDict[str, Any] = OrderedDict()
        self._stored: dict[str, tuple[list[tuple[Any, Any]], str]] = {}

    async def writes(self) -> Writes:
        """Every ``messages`` write of the thread, decoded on first use: a
        plan that simulates no load never pays for it."""
        if self._writes is None:
            self._writes = await self._read_writes()
        return self._writes

    async def _stored_raw(self, version: str) -> Any:
        if version in self._raw:
            self._raw.move_to_end(version)
            return self._raw[version]
        typed = await self._read_blob(version)
        if typed is None:
            raise LookupError(f"messages blob at version {version} is gone")
        value = self.serde.loads_typed(typed)
        self._raw[version] = value
        if len(self._raw) > 8:
            self._raw.popitem(last=False)
        return value

    async def load(self, checkpoint_id: str, layout: Layout) -> list:
        from langgraph._internal._typing import MISSING

        node = self._tree.nodes[checkpoint_id]
        if node.version in layout.stored:
            channel = self._spec.from_checkpoint(await self._stored_raw(node.version))
        else:
            seed, chain = self._tree.walk(checkpoint_id, layout.stored, layout.flags)
            base: Any = MISSING
            seed_version = self._tree.nodes[seed].version if seed else None
            if seed_version is not None and seed_version in layout.stored:
                base = await self._stored_raw(seed_version)
            channel = self._spec.from_checkpoint(base)
            writes = await self.writes()
            channel.replay_writes([w for cid in reversed(chain) for w in writes.get(cid, ())])
        return list(channel.get()) if channel.is_available() else []

    async def stored(self, version: str) -> tuple[list[tuple[Any, Any]], str]:
        """The shape and digest of what loads of a stored blob return."""
        if version not in self._stored:
            value = list(self._spec.from_checkpoint(await self._stored_raw(version)).get())
            self._stored[version] = (_shape(value), messages_digest(self.serde, value))
        return self._stored[version]

    async def reproduces(self, checkpoint_id: str, layout: Layout, expected: tuple) -> bool:
        value = await self.load(checkpoint_id, layout)
        shape, digest = expected
        # Ids first: most differences show there, without serializing.
        return _shape(value) == shape and messages_digest(self.serde, value) == digest


def _shape(messages: list) -> list[tuple[Any, Any]]:
    return [(getattr(m, "type", None), getattr(m, "id", None)) for m in messages]


@dataclass
class Plan:
    before: Layout
    after: Layout
    drop: list[Unit] = field(default_factory=list)
    promote: list[Unit] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    kept: Counter[str] = field(default_factory=Counter)

    @property
    def changes(self) -> bool:
        return bool(self.drop or self.promote)


async def make_plan(
    tree: Tree,
    units: dict[str, Unit],
    replay: Replayer,
    protected: dict[str, str],
    spacing: int | None,
) -> Plan:
    """Decide each blob, oldest first, against the layout the decisions above
    it left, so every load is checked against the snapshot it will seed from.

    Every walk already stops at any stored blob, so a plan only ever removes
    places a walk stops, and no load is left crossing a new one. ``spacing``
    None keeps every blob and only flags the lists stored before DeltaChannel.
    """
    old = Layout({c for c, n in tree.nodes.items() if n.flagged}, set(units))
    new = old.copy()
    plan = Plan(old, new)

    forced: dict[str, str] = {}
    for cid, why in protected.items():
        version = tree.nodes[cid].version
        if version not in units:
            seed, _ = tree.walk(cid, old.stored, old.flags)
            version = tree.nodes[seed].version if seed else None
        if version in units:
            forced.setdefault(version, why)

    def keep_reason(unit: Unit) -> str | None:
        if spacing is None:
            return "keep all"
        if unit.version in forced:
            return forced[unit.version]
        seed, _ = tree.walk(unit.loaders[0], new.stored, new.flags)
        since = tree.nodes[unit.loaders[0]].updates - (tree.nodes[seed].updates if seed else 0)
        return "spacing" if since >= spacing else None

    async def droppable(unit: Unit) -> bool:
        expected = await replay.stored(unit.version)
        new.stored.discard(unit.version)
        new.flags.difference_update(unit.flagged)
        for cid in unit.loaders:
            if not await replay.reproduces(cid, new, expected):
                new.stored.add(unit.version)
                new.flags.update(unit.flagged)
                return False
        return True

    for unit in sorted(units.values(), key=lambda u: u.loaders[0]):
        reason = keep_reason(unit)
        if reason is None and await droppable(unit):
            plan.drop.append(unit)
            continue
        plan.kept[reason or "replay differs"] += 1
        if not unit.is_snapshot:
            # A list stored before DeltaChannel. The flag changes no load on
            # the verified releases; it stops the walks of the releases before
            # them, which passed such a list and replayed from the root.
            plan.promote.append(unit)
            plan.flags.extend(unit.loaders)
            new.flags.update(unit.loaders)
    return plan


def longest_walk(tree: Tree, layout: Layout, write_counts: dict[str, int]) -> tuple[int, int]:
    """The longest walk any checkpoint's load takes under a layout, as
    (checkpoints, writes) replayed."""
    span: dict[str, tuple[int, int]] = {}
    longest = (0, 0)
    for cid in tree.order:
        node = tree.nodes[cid]
        if node.parent is None:
            up = (0, 0)
        else:
            parent = tree.nodes[node.parent]
            stops = parent.version in layout.stored or node.parent in layout.flags
            above = (0, 0) if stops else span[node.parent]
            up = (above[0] + 1, above[1] + write_counts.get(node.parent, 0))
        span[cid] = up
        if node.version not in layout.stored:
            longest = max(longest, up)
    return longest
