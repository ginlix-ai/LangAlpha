"""The per-thread baseline: one frozen context block, re-rendered byte-identically.

Everything here used to be read on every model call by three separate
middlewares, each appending its own system block. Between the static prefix and
the conversation, that meant every agent.md write re-wrote the entire message
history at the write rate instead of the read rate.

The baseline instead reads once per turn in ``before_agent``, freezes the result
into checkpoint state, and re-renders from that state on every call. What the
model sees is stable for an epoch, so a cache breakpoint can sit on it. What
changed underneath reaches the model as a durable row written into history
(see ``epoch.py`` and ``durable.py``), not as an edit to a block it already has.

An epoch survives until compaction, because compaction is the one event that can
remove the block from the model's retained history: "the file did not change" is
not enough after a summarization dropped the injection it is measured against.

The split of work is the point. This module does the IO and the rendering; the
record itself, and the decision between rebuilding it and filing rows against
it, are pure and live in ``epoch.py``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html import escape
from typing import Any

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import SystemMessage

from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.middleware.provider_cache import (
    breakpoint_marker,
    tag_last_text_block,
)
from ptc_agent.agent.middleware.runtime_context.changes import SourceRead
from ptc_agent.agent.middleware.runtime_context.durable import (
    build_update_message,
    rows_in_view,
)
from ptc_agent.agent.middleware.runtime_context.epoch import (
    MEMO_DISPLAY_CAP,
    MEMORY_TIERS,
    BaselineEpoch,
    Identity,
    Observations,
    Workspace,
    advance_epoch,
    compaction_fingerprint,
)
from ptc_agent.agent.middleware.runtime_context.harness_blocks import (
    HARNESS_BLOCKS,
    harness_update_kind,
)
from ptc_agent.agent.middleware.runtime_context.profile import (
    ProfileSnapshot,
    profile_read_answered,
)
from ptc_agent.agent.middleware.runtime_context.state import STATE_BASELINE, state_get
from ptc_agent.agent.middleware.runtime_context.templates import render_template
from ptc_agent.agent.middleware.runtime_context.turn import NON_CHANGE_ROW_KINDS
from ptc_agent.agent.roles import AgentRole
from ptc_agent.agent.tools.context_file_policy import MAX_AGENT_MD_SIZE, MAX_MEMORY_BLOCK_SIZE
from ptc_agent.core.paths import (
    MEMO_INDEX_FILENAME,
    MEMO_USER_DIR,
    MEMORY_INDEX_FILENAME,
)

logger = logging.getLogger(__name__)

AGENT_MD_PATH = "/agent.md"


def _retained_change_rows(state: Any) -> tuple[str, ...]:
    """The kinds of the change rows the model can still read.

    A rebuilt row that kept earlier rows in force stands in for them: once a
    compaction has taken those rows and kept it, it is the only word left that
    their source was carried unread, and the next rebuild has to keep carrying
    it or fold it.
    """
    kinds: list[str] = []
    for update in rows_in_view(state):
        if update.kind in NON_CHANGE_ROW_KINDS:
            continue
        kinds.append(update.kind)
        if update.kind == "baseline_rebuilt":
            kinds.extend(
                kind
                for kind in update.provenance.get("carried", ())
                if isinstance(kind, str) and kind not in kinds
            )
    return tuple(kinds)


# A store or sandbox read at the turn boundary must not stall the turn.
_READ_TIMEOUT_S = 2.0

# One past the display cap, so a count at the ceiling can still be told apart
# from one above it.
_COUNT_QUERY_LIMIT = MEMO_DISPLAY_CAP + 1

# A long-running thread that never compacts would otherwise accumulate durable
# rows forever, each one measured from a baseline that drifted further from the
# truth. Folding the drift back into the block after this many rows keeps the
# diff honest and the history short. A judgement call, not a measured constant.
DEFAULT_REBUILD_AFTER_UPDATES = 20

NamespaceFactory = Callable[[], tuple[str, ...]]

#: Reads the current text of one harness-authored block from the turn's agent
#: state. ``None`` means the source has not answered yet, which freezes nothing
#: and marks the epoch incomplete; a string, empty included, is the text.
BlockReader = Callable[[Any], str | None]

# The harness-authored blocks, in the order they render (see harness_blocks.py).
BLOCK_KINDS: tuple[str, ...] = tuple(HARNESS_BLOCKS)


# ---------------------------------------------------------------------------
# Where the baseline reads from
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MemoryTierSource:
    """One memory tier: where it lives in the store, and how the model sees it."""

    namespace_factory: NamespaceFactory
    display_path: str


@dataclass(frozen=True, slots=True)
class MemoSource:
    """The memo catalog: where the count is read from, and where memos live."""

    namespace_factory: NamespaceFactory
    display_path: str = f"{MEMO_USER_DIR}/"
    index_key: str = MEMO_INDEX_FILENAME


@dataclass(frozen=True, slots=True)
class BaselineSources:
    """The store-backed tiers the baseline reads at the turn boundary.

    One value rather than a spray of constructor keywords, because every field
    comes from the same gate resolution and the same namespace closures at agent
    build, so they are decided together and are only ever passed together.
    """

    store: Any | None = None
    store_cache: Any | None = None
    memory: dict[str, MemoryTierSource] = field(default_factory=dict)
    index_key: str = MEMORY_INDEX_FILENAME
    memo: MemoSource | None = None


class BaselineContextMiddleware(AgentMiddleware):
    """Freezes the per-thread baseline in ``before_agent``, renders it per call.

    Args:
        session: Workspace session, the agent.md read path. None disables the
            agent.md tier entirely (Flash has no sandbox).
        workspace_name: Workspace name as its row had it at agent build.
        workspace_description: Workspace description, from the same read.
        sources: Store-backed tiers to read (memory, memo). None reads neither.
        blocks: Harness-authored text this flavor states, keyed by read kind
            (see :data:`BlockReader`). A kind with no reader is a block this
            build does not have, which renders nothing and files no row.
        user_profile: Identity plus the steering it carries (locale language
            rule, agent preferences, output routing).
        user_data_counts: Portfolio/watchlist/preference counts, snapshot at
            agent build. Counts drift within a turn, so the snapshot is what
            renders rather than a live read.
        sandbox_enabled: Whether the agent has a filesystem. It gates the
            filesystem-dependent steering in the user_profile component, and
            it is the build that has a workspace to read, so it decides
            whether a missing ``workspace_name`` is a read that did not answer
            or a build with no workspace.
        preferred_market: Market the identity block reports, from
            ``resolve_preferred_market``.
        timezone: The turn's zone, which the identity block states ahead of
            the profile's own, so it agrees with the stamp.
        guidance: Resolved prompt guidance level; None resolves it lazily.
        model_name: Model the turn resolved to, used only for guidance.
        rebuild_after_updates: Durable rows since the last epoch that force a
            rebuild. Zero disables the safety valve.
        read_timeout_s: Ceiling on any one turn-boundary read.
        files_mounted: Whether the file mount served as the agent was built,
            which a rebuild freezes for the static prompt to state. None for
            a build whose prompt says nothing about the mount.
        role: The role the agent runs in, for the wording of the blocks that
            differ by role. Fixed per build, so the block stays deterministic.
        channels_enabled: Whether the agent has the chat-app settings folder,
            which a rebuild freezes for the block to state. None for a build
            whose block says nothing about it.
    """

    def __init__(
        self,
        *,
        session: Any | None = None,
        workspace_name: str | None = None,
        workspace_description: str | None = None,
        sources: BaselineSources | None = None,
        blocks: dict[str, BlockReader] | None = None,
        user_profile: dict[str, Any] | None = None,
        user_data_counts: dict[str, Any] | None = None,
        sandbox_enabled: bool = False,
        preferred_market: str | None = None,
        timezone: str | None = None,
        guidance: str | None = None,
        model_name: str | None = None,
        rebuild_after_updates: int = DEFAULT_REBUILD_AFTER_UPDATES,
        read_timeout_s: float = _READ_TIMEOUT_S,
        files_mounted: bool | None = None,
        role: AgentRole = "analyst",
        channels_enabled: bool | None = None,
    ) -> None:
        super().__init__()
        self._session = session
        self._role = role
        self._files_mounted = files_mounted
        self._channels_enabled = channels_enabled
        # None is a read that did not answer (or a build with no workspace);
        # an empty string is a workspace with no name. Only the first is kept
        # out of change detection, and ``sandbox_enabled`` below tells the two
        # meanings of None apart at rebuild time.
        self._workspace_available = workspace_name is not None
        self._workspace_name = (workspace_name or "").strip()
        self._workspace_description = (workspace_description or "").strip()
        resolved = sources or BaselineSources()
        self._store = resolved.store
        self._store_cache = resolved.store_cache
        self._index_key = resolved.index_key
        # A tier without a store can only ever read as "unavailable", which
        # would mark every epoch incomplete and rebuild the block on every
        # turn. A factory with no store is a wiring bug; drop the tier instead.
        self._memory: dict[str, MemoryTierSource] = (
            {
                tier: resolved.memory[tier]
                for tier in MEMORY_TIERS
                if tier in resolved.memory
            }
            if resolved.store is not None
            else {}
        )
        self._memo = resolved.memo if resolved.store is not None else None
        self._blocks: dict[str, BlockReader] = {
            kind: reader
            for kind, reader in (blocks or {}).items()
            if kind in HARNESS_BLOCKS
        }
        # Both platform reads answer with a dict or None, and None is either
        # a failure or no user at all; neither is a profile the user cleared.
        self._profile_available = (
            profile_read_answered(user_profile) and user_data_counts is not None
        )
        self._user_profile = user_profile or {}
        self._user_data_counts = user_data_counts
        self._sandbox_enabled = sandbox_enabled
        self._preferred_market = preferred_market
        self._timezone = timezone
        self._guidance_value = guidance
        # Set once the turn-boundary read has run on this instance; see
        # ``abefore_model``.
        self._opened = False
        self._model_name = model_name
        self._rebuild_after_updates = max(0, rebuild_after_updates)
        self._read_timeout_s = read_timeout_s
        # Rendered block, memoized per epoch. The memo is what makes
        # byte-identity a property of the code rather than of the templates
        # happening to be deterministic.
        self._block_cache: tuple[Any, str] | None = None

    # -- rendering inputs ---------------------------------------------------

    def _guidance(self) -> str:
        if self._guidance_value is None:
            from ptc_agent.agent.prompts import resolve_prompt_guidance

            self._guidance_value = resolve_prompt_guidance(self._model_name)
        return self._guidance_value

    def _identity(self) -> Identity:
        profile = self._user_profile
        return Identity(
            name=profile.get("name") or "User",
            timezone=self._timezone or profile.get("timezone") or "UTC",
            locale=profile.get("locale") or "en-US",
            preferred_market=self._preferred_market or "US",
        )

    def _profile_snapshot(self) -> ProfileSnapshot:
        """The profile as this turn sees it, in the shape the epoch freezes.

        Frozen into the epoch rather than re-read per turn, so the block stays
        byte-identical for the epoch; a change underneath it arrives as a
        `profile_changed` row instead, the same way a memo count does.
        """
        return ProfileSnapshot(
            user_profile=dict(self._user_profile),
            user_data_counts=dict(self._user_data_counts or {}),
            preferred_market=self._preferred_market or "US",
        )

    # -- turn-boundary read -------------------------------------------------

    async def abefore_agent(
        self, state: Any, runtime: Any = None
    ) -> dict[str, Any] | None:
        """Freeze or refresh the baseline, and record what moved underneath it.

        Runs once per turn. Either it rebuilds the epoch (first turn, a new
        compaction event, or accumulated drift) or it leaves the frozen block
        alone and writes a durable row into history for every source that moved.

        The rows land after the turn's own user message, which is already in
        state by the time this hook runs. That ordering is what every carrier
        shape needs, so it is a property of the hook rather than of the carrier.
        """
        self._opened = True
        try:
            epoch = BaselineEpoch.from_state(state_get(state, STATE_BASELINE))
            advanced, rows = advance_epoch(epoch, await self.read_observations(state))
            if advanced is None:
                return None
            written: dict[str, Any] = {STATE_BASELINE: advanced.to_state()}
            if rows:
                # No ids: the Pregel path stamps them, and minting one here
                # would re-roll a different uuid on every replay.
                written["messages"] = [build_update_message(row) for row in rows]
            return written
        except Exception:  # noqa: BLE001 - context is never worth failing a turn for
            logger.warning("[Baseline] turn-boundary read failed", exc_info=True)
            return None

    def before_agent(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    async def abefore_model(
        self, state: Any, runtime: Any = None
    ) -> dict[str, Any] | None:
        # A turn that resumes an interrupt re-enters the graph at the
        # interrupted node, never at the entry node, so ``abefore_agent`` does
        # not run for it. The instance is built per request, so a first model
        # call on one that never opened a turn is that resume: the boundary
        # read runs here instead, after the tool result the resume produced.
        if self._opened:
            return None
        return await self.abefore_agent(state, runtime)

    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    async def read_observations(self, state: Any) -> Observations:
        """Everything one turn boundary reads, in one pass.

        The file sources and the memo count are gathered together because they
        answer to the same deadline and neither depends on the other; the
        profile and the compaction fingerprint are already in hand.
        """
        reads, memo_count = await asyncio.gather(
            self._read_sources(state), self._memo_count()
        )
        return Observations(
            now=datetime.now(tz=UTC),
            workspace=Workspace(
                name=self._workspace_name, description=self._workspace_description
            ),
            identity=self._identity(),
            profile=self._profile_snapshot(),
            reads=tuple(reads),
            memo_configured=self._memo is not None,
            memo_count=memo_count,
            memo_path=self._memo.display_path if self._memo is not None else "",
            compaction=compaction_fingerprint(state_get(state, "_summarization_event")),
            rebuild_after_updates=self._rebuild_after_updates,
            retained_rows=_retained_change_rows(state),
            profile_available=self._profile_available,
            workspace_available=self._workspace_available,
            workspace_configured=self._sandbox_enabled,
            files_mounted=self._files_mounted,
            channels_enabled=self._channels_enabled,
        )

    async def _read_sources(self, state: Any) -> list[SourceRead]:
        """Every source this turn boundary reads: the files concurrently, then the blocks.

        The block readers are in-memory by construction (what the harness
        already resolved for this turn), so they are called after the awaits
        rather than given a task each.
        """
        jobs: list[Awaitable[SourceRead]] = []
        if self._session is not None:
            jobs.append(self._read_agent_md())
        for tier in MEMORY_TIERS:
            if tier in self._memory:
                jobs.append(self._read_memory(tier))
        reads = list(await asyncio.gather(*jobs)) if jobs else []
        reads.extend(
            self._read_block(kind, state)
            for kind in BLOCK_KINDS
            if kind in self._blocks
        )
        return reads

    def _read_block(self, kind: str, state: Any) -> SourceRead:
        """One harness-authored block as this turn sees it."""
        read = SourceRead(
            kind=kind,
            update_kind=harness_update_kind(kind),
            path=HARNESS_BLOCKS[kind].label,
            available=False,
            provenance={"source": "harness"},
        )
        try:
            text = self._blocks[kind](state)
        except Exception:  # noqa: BLE001 - a source that threw is not an empty one
            logger.warning("[Baseline] %s read failed", kind, exc_info=True)
            return read
        if text is None:
            return read
        read.available = True
        read.text = text
        return read

    async def _read_agent_md(self) -> SourceRead:
        """agent.md straight from the sandbox, never from a per-process cache.

        The file has writers on other workers (the workspace-files API, a
        subagent, another turn of the same thread), and a cache in this
        process cannot see any of them. One uncached read per turn is what
        makes the content hash a cross-worker truth.
        """
        provenance: dict[str, Any] = {"source": "sandbox", "path": AGENT_MD_PATH}
        # Taken, not read: the stamp describes one write, and this read is the
        # first to observe it. Left in place it would label every later change,
        # including one made from another worker, with a writer it never had.
        take_writer = getattr(self._session, "take_agent_md_writer", None)
        writer = take_writer() if callable(take_writer) else None
        if isinstance(writer, dict) and writer:
            provenance = {**provenance, **writer}
        try:
            content = await asyncio.wait_for(
                _read_sandbox_text(self._session, "agent.md"),
                timeout=self._read_timeout_s,
            )
        except Exception:  # noqa: BLE001 - a failed read is not an empty file
            logger.warning("[Baseline] agent.md read failed", exc_info=True)
            return SourceRead(
                kind="agent_md",
                update_kind="agent_md_changed",
                path=AGENT_MD_PATH,
                available=False,
                provenance=provenance,
                cap=MAX_AGENT_MD_SIZE,
            )
        return SourceRead(
            kind="agent_md",
            update_kind="agent_md_changed",
            path=AGENT_MD_PATH,
            text=content,
            provenance=provenance,
            cap=MAX_AGENT_MD_SIZE,
        )

    async def _read_memory(self, tier: str) -> SourceRead:
        source = self._memory[tier]
        read = SourceRead(
            kind=f"memory:{tier}",
            update_kind=f"memory_changed:{tier}",
            path=source.display_path,
            available=False,
            provenance={"source": "store", "tier": tier},
            cap=MAX_MEMORY_BLOCK_SIZE,
        )
        try:
            namespace = source.namespace_factory()
        except Exception:  # noqa: BLE001
            logger.warning("[Baseline] memory namespace resolution failed", exc_info=True)
            return read
        getter = (
            (lambda: self._store_cache.aget(self._store, namespace, self._index_key))
            if self._store_cache is not None
            else (lambda: self._store.aget(namespace, self._index_key))
        )
        try:
            item = await asyncio.wait_for(getter(), timeout=self._read_timeout_s)
        except Exception:  # noqa: BLE001 - a failed read is not an empty tier
            logger.warning("[Baseline] memory.md read failed", exc_info=True)
            return read
        read.available = True
        if item is None:
            return read
        value = getattr(item, "value", None)
        read.text = _content_from_value(value)
        # The store value has no writer field, so the closest thing to a writer
        # is when it was last written. See the module note in changes.py.
        if isinstance(value, dict) and isinstance(value.get("modified_at"), str):
            read.provenance["modified_at"] = value["modified_at"]
        return read

    async def _memo_count(self) -> int | None:
        """Memos in the user's namespace, or None when the read did not answer."""
        if self._memo is None:
            return None
        try:
            namespace = self._memo.namespace_factory()
        except Exception:  # noqa: BLE001
            logger.warning("[Baseline] memo namespace resolution failed", exc_info=True)
            return None
        index_key = self._memo.index_key
        try:
            catalog = await asyncio.wait_for(
                self._store_cache.aget(self._store, namespace, index_key)
                if self._store_cache is not None
                else self._store.aget(namespace, index_key),
                timeout=self._read_timeout_s,
            )
            if catalog is not None and isinstance(catalog.value, dict):
                count = catalog.value.get("memo_count")
                if isinstance(count, int) and count >= 0:
                    return count
            results = await asyncio.wait_for(
                self._store.asearch(namespace, limit=_COUNT_QUERY_LIMIT, offset=0),
                timeout=self._read_timeout_s,
            )
        except Exception:  # noqa: BLE001 - a memo count is never worth a turn
            logger.warning("[Baseline] memo count read failed", exc_info=True)
            return None
        if not results:
            return 0
        return sum(1 for item in results if item.key != index_key)

    # -- rendering ----------------------------------------------------------

    def _render_block(self, epoch: BaselineEpoch) -> str:
        """The whole baseline block, in its fixed order.

        Reads the frozen epoch and nothing else, which is what makes the block
        byte-identical for the life of that epoch. The file blocks carry their
        own one-paragraph trust preface, rendered here rather than in the system
        prefix so the rule sits next to the text it governs; it is emitted only
        when there is a file block for it to govern, and the harness blocks go
        above it so nothing stands between the preface and the files it names.
        """
        guidance = self._guidance()
        parts: list[str] = []

        parts.append(_workspace_block(epoch.workspace))
        parts.append(self._user_profile_block(epoch.profile, epoch.channels_enabled))

        identity = epoch.identity or self._identity()
        parts.append(
            render_template(
                "envelope/baseline_identity.md.j2",
                guidance=guidance,
                **identity.to_state(),
            )
        )

        for kind in BLOCK_KINDS:
            entry = epoch.blocks.get(kind)
            if entry is None:
                continue
            parts.append(
                render_template(
                    HARNESS_BLOCKS[kind].template, content=entry.text, guidance=guidance
                )
            )

        if epoch.has_files:
            parts.append(
                render_template("envelope/baseline_files.md.j2", guidance=guidance)
            )

        if epoch.agent_md is not None:
            parts.append(
                render_template(
                    "envelope/baseline_agentmd.md.j2",
                    path=epoch.agent_md.path or AGENT_MD_PATH,
                    content=epoch.agent_md.text,
                    guidance=guidance,
                    role=self._role,
                )
            )

        for tier in MEMORY_TIERS:
            entry = epoch.memory.get(tier)
            if entry is None:
                continue
            parts.append(
                render_template(
                    "envelope/baseline_memory.md.j2",
                    path=entry.path or tier,
                    content=entry.text,
                    readonly=not self._sandbox_enabled,
                    guidance=guidance,
                )
            )

        if epoch.memo is not None and epoch.memo.display:
            parts.append(
                f'<memo-index count="{epoch.memo.display}" path="{epoch.memo.path}"/>'
            )

        return "\n\n".join(p for p in parts if p)

    def _user_profile_block(
        self, frozen: ProfileSnapshot | None, channels: bool | None = None
    ) -> str:
        """The ``<user_profile>`` component: identity plus the steering it carries.

        Rendered from the epoch's frozen snapshot so the block never moves
        inside an epoch. A preference or count that changes mid-thread reaches
        the model as a `profile_changed` row on the next turn. An epoch frozen
        before profiles were captured falls back to this turn's snapshot, and
        one that froze no ``channels`` to this build's.
        """
        snapshot = frozen if frozen is not None else self._profile_snapshot()
        if channels is None:
            channels = self._channels_enabled
        if not snapshot.user_profile and not snapshot.user_data_counts:
            return ""
        try:
            content = render_template(
                "components/user_profile.md.j2",
                user_profile=snapshot.user_profile,
                user_data_counts=snapshot.user_data_counts or None,
                sandbox_enabled=self._sandbox_enabled,
                profile_files=self._sandbox_enabled,
                channels_enabled=bool(channels),
            )
        except Exception:  # noqa: BLE001 - one component is not the whole block
            logger.warning("[Baseline] user_profile render failed", exc_info=True)
            return ""
        return f"<user_profile>\n{content}\n</user_profile>"

    # -- middleware hooks ---------------------------------------------------

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Append the frozen baseline as one system block and pin breakpoint 3.

        No IO here by construction: everything this renders was read at the turn
        boundary or captured at agent build. A read on this path is what made
        the old middlewares re-write the whole history at the write rate.
        """
        try:
            epoch = BaselineEpoch.from_state(
                state_get(getattr(request, "state", None), STATE_BASELINE)
            )
            block = self._cached_block(epoch)
        except Exception:  # noqa: BLE001 - context is never worth failing a turn for
            logger.warning("[Baseline] render failed; sending the call without it", exc_info=True)
            return await handler(request)
        if not block:
            return await handler(request)

        system_message = append_to_system_message(request.system_message, block)
        marker = breakpoint_marker(getattr(request, "model", None))
        if marker is not None:
            tagged = tag_last_text_block(system_message.content, *marker)
            if tagged is not None:
                system_message = SystemMessage(content=tagged)
        return await handler(request.override(system_message=system_message))

    def _cached_block(self, epoch: BaselineEpoch) -> str:
        key = (epoch.epoch, epoch.built_at)
        if self._block_cache is not None and self._block_cache[0] == key:
            return self._block_cache[1]
        block = self._render_block(epoch)
        self._block_cache = (key, block)
        return block


# ---------------------------------------------------------------------------
# Reading and rendering helpers (kept out of the class: pure over their inputs)
# ---------------------------------------------------------------------------


def _workspace_block(workspace: Workspace) -> str:
    """What the workspace is called, as element text.

    Text rather than attributes: a name is free text the user typed, and as text
    an escape of ``<`` and ``&`` is the whole obligation, with no quoting rule to
    get wrong and an apostrophe surviving as an apostrophe.
    """
    name = workspace.name.strip()
    if not name:
        return ""
    lines = [f"Name: {escape(name, quote=False)}"]
    description = workspace.description.strip()
    if description:
        lines.append(f"Description: {escape(description, quote=False)}")
    body = "\n".join(lines)
    return f"<workspace>\n{body}\n</workspace>"


async def _read_sandbox_text(session: Any, relative_path: str) -> str | None:
    """One workspace file's text, or None when the file does not exist.

    A session without a sandbox (Flash, a test double) reads as absent rather
    than as an error, since there is no file for it to have.
    """
    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        return None
    return await sandbox.aread_file_text(sandbox.normalize_path(relative_path))


def _content_from_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    raw = value.get("content")
    if isinstance(raw, str):
        return raw
    # Legacy v1 stored lines as list[str].
    if isinstance(raw, list):
        return "\n".join(raw)
    return None
