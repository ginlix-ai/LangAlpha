"""The thinning planner: which ``messages`` blobs a thread keeps, flags and seeds.

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

from scripts.ops._delta_loads import Loader, Skip, Tree, messages_digest

# A checkpoint's ``messages`` writes as the saver replays them: (task id,
# channel, value), ordered by task id, then index.
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
class Seed:
    """A snapshot to store at a version that holds no blob: the value the
    topmost checkpoint of a walk across a newly flagged list loads today."""

    checkpoint_id: str
    version: str
    snapshot: Any  # a _DeltaSnapshot
    typed: tuple[str, bytes]  # serialized once, for the report and the write

    @classmethod
    def of(cls, checkpoint_id: str, version: str, value: list, serde: Any) -> Seed:
        from langgraph.checkpoint.serde.types import _DeltaSnapshot

        snapshot = _DeltaSnapshot(value)
        return cls(checkpoint_id, version, snapshot, serde.dumps_typed(snapshot))

    @property
    def size(self) -> int:
        return len(self.typed[1])


@dataclass
class Layout:
    """Which checkpoints flag a snapshot, and which versions hold a blob:
    one stored now, or a seed not yet written."""

    flags: set[str]
    stored: set[str]
    pending: dict[str, Seed] = field(default_factory=dict)

    def holds(self, version: str | None) -> bool:
        return version is not None and (version in self.stored or version in self.pending)

    def copy(self) -> Layout:
        return Layout(set(self.flags), set(self.stored), dict(self.pending))


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

    async def _blob(self, version: str, layout: Layout) -> Any:
        seed = layout.pending.get(version)
        return seed.snapshot if seed is not None else await self._stored_raw(version)

    async def load(self, checkpoint_id: str, layout: Layout) -> list:
        from langgraph._internal._typing import MISSING

        node = self._tree.nodes[checkpoint_id]
        if layout.holds(node.version):
            assert node.version is not None
            channel = self._spec.from_checkpoint(await self._blob(node.version, layout))
        else:
            seed, chain = self._tree.walk(checkpoint_id, layout.flags)
            base: Any = MISSING
            seed_version = self._tree.nodes[seed].version if seed else None
            if seed_version is not None and layout.holds(seed_version):
                base = await self._blob(seed_version, layout)
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
    seeds: list[Seed] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    kept: Counter[str] = field(default_factory=Counter)

    @property
    def changes(self) -> bool:
        return bool(self.drop or self.promote or self.seeds)


async def make_plan(
    tree: Tree,
    units: dict[str, Unit],
    replay: Replayer,
    protected: dict[str, str],
    spacing: int | None,
) -> Plan:
    """Decide each blob, oldest first, against the layout the decisions above
    it left, so every load is checked against the snapshot it will seed from.

    ``spacing`` None keeps every blob: lists stored before DeltaChannel are
    only flagged, and the walks that crossed them seeded.
    """
    old = Layout({c for c, n in tree.nodes.items() if n.flagged}, set(units))
    new = old.copy()
    plan = Plan(old, new)

    forced: dict[str, str] = {}
    for cid, why in protected.items():
        version = tree.nodes[cid].version
        if version in units:
            forced.setdefault(version, why)
            continue
        seed, _ = tree.walk(cid, old.flags)
        seed_version = tree.nodes[seed].version if seed else None
        if seed_version in units:
            forced.setdefault(seed_version, why)

    def keep_reason(unit: Unit) -> str | None:
        if spacing is None:
            return "keep all"
        if unit.version in forced:
            return forced[unit.version]
        seed, _ = tree.walk(unit.loaders[0], new.flags)
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

    by_age = sorted(units.values(), key=lambda u: u.loaders[0])

    # Lists stored before DeltaChannel: no walk stops at them yet.
    for unit in (u for u in by_age if not u.is_snapshot):
        reason = keep_reason(unit)
        if reason is None and await droppable(unit):
            plan.drop.append(unit)
            continue
        plan.kept[reason or "replay differs"] += 1
        plan.promote.append(unit)
        new.flags.update(unit.loaders)

    # Walks that today pass a list just flagged above would now stop at it.
    # Seed the topmost of each with what it loads today.
    added = new.flags - old.flags
    crosses: dict[str, bool] = {}
    for cid in tree.order:
        parent = tree.nodes[cid].parent
        crosses[cid] = parent is not None and (
            parent in added or (parent not in old.flags and crosses[parent])
        )
    exposed = {cid for cid in tree.order if crosses[cid] and not old.holds(tree.nodes[cid].version)}
    for cid in tree.order:
        if cid not in exposed or tree.nodes[cid].parent in exposed:
            continue
        version = tree.nodes[cid].version
        if version is None:
            raise Skip("a walk crossing a kept stored list has no messages version")
        if not new.holds(version):
            value = await replay.load(cid, old)
            expected = (_shape(value), messages_digest(replay.serde, value))
            for peer in tree.by_version[version]:
                if peer != cid and not await replay.reproduces(peer, old, expected):
                    raise Skip("a seed would change a load sharing its version")
            seed = Seed.of(cid, version, value, replay.serde)
            new.pending[version] = seed
            plan.seeds.append(seed)
        new.flags.add(cid)
        plan.flags.append(cid)

    # Flagged snapshots: DeltaChannel's own, and lists an earlier run flagged
    # (of this script, or of seed_delta_tips before --keep all replaced it).
    for unit in (u for u in by_age if u.is_snapshot):
        reason = keep_reason(unit)
        if reason is None and await droppable(unit):
            plan.drop.append(unit)
        else:
            plan.kept[reason or "replay differs"] += 1

    for unit in plan.promote:
        plan.flags.extend(cid for cid in unit.loaders if cid not in old.flags)
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
            above = (0, 0) if node.parent in layout.flags else span[node.parent]
            up = (above[0] + 1, above[1] + write_counts.get(node.parent, 0))
        span[cid] = up
        if not layout.holds(node.version):
            longest = max(longest, up)
    return longest
