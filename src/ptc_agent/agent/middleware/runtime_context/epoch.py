"""The checkpointed baseline record: one frozen snapshot, one observation cursor.

The snapshot is what the model already has in front of it, so it never changes
inside an epoch. The cursor is what the turn-boundary read saw last, and it
moves whenever a source moves. Keeping the two apart is what lets a change be
reported once, as a durable row, without touching the block that row is measured
against.

Old checkpoints decode here and nowhere else. ``BaselineEpoch.from_state`` is
the one place that fills in a key an older build never wrote (an epoch frozen
before profiles existed, a memo count that never answered), so every reader
downstream works from typed fields instead of guessing at a dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from ptc_agent.agent.middleware.runtime_context.changes import (
    UPDATE_SCHEMA_VERSION,
    SourceRead,
    render_diff,
    sha256_text,
)
from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate
from ptc_agent.agent.middleware.runtime_context.harness_blocks import (
    harness_block_for,
)
from ptc_agent.agent.middleware.runtime_context.profile import (
    ProfileSnapshot,
    profile_diff_lines,
)
from ptc_agent.agent.middleware.runtime_context.state import as_dict

# Rendered in a fixed order so the block is byte-identical for equal inputs.
MEMORY_TIERS: tuple[str, ...] = ("user", "workspace")

# Cap the memo count so the rendered block stays constant-size no matter how
# many memos a user has.
MEMO_DISPLAY_CAP = 500


# ---------------------------------------------------------------------------
# The parts of one epoch
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Workspace:
    """What the workspace was called when the epoch froze."""

    name: str = ""
    description: str = ""

    @classmethod
    def from_state(cls, value: Any) -> Workspace:
        data = as_dict(value)
        return cls(
            name=str(data.get("name") or ""),
            description=str(data.get("description") or ""),
        )

    def to_state(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description}

    def sha(self) -> str:
        return sha256_text(f"{self.name}\n{self.description}")


def workspace_diff_lines(frozen: Workspace, current: Workspace) -> list[str]:
    lines: list[str] = []
    for label, was, now in (
        ("Name", frozen.name, current.name),
        ("Description", frozen.description, current.description),
    ):
        if was == now:
            continue
        if not was:
            lines.append(f"{label}: {now} (not in the frozen block)")
        elif not now:
            lines.append(f"{label}: no longer set (the frozen block says {was})")
        else:
            lines.append(f"{label}: {now} (the frozen block says {was})")
    return lines


@dataclass(frozen=True, slots=True)
class Identity:
    """The four identity fields the block states, each with a rendered default."""

    name: str = "User"
    timezone: str = "UTC"
    locale: str = "en-US"
    preferred_market: str = "US"

    @classmethod
    def from_state(cls, value: Any) -> Identity | None:
        data = as_dict(value)
        if not data:
            return None
        return cls(
            name=str(data.get("name") or "User"),
            timezone=str(data.get("timezone") or "UTC"),
            locale=str(data.get("locale") or "en-US"),
            preferred_market=str(data.get("preferred_market") or "US"),
        )

    def to_state(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "timezone": self.timezone,
            "locale": self.locale,
            "preferred_market": self.preferred_market,
        }


@dataclass(frozen=True, slots=True)
class FileEntry:
    """The frozen copy of one file source: what it said, and its hash.

    Hashed over the raw read rather than the truncated text, so an edit past the
    cap is still an edit and the next turn notices it even though it never
    rendered.
    """

    path: str = ""
    text: str = ""
    sha256: str = ""
    exists: bool = False

    @classmethod
    def from_state(cls, value: Any) -> FileEntry | None:
        data = as_dict(value)
        if not data:
            return None
        return cls(
            path=str(data.get("path") or ""),
            text=str(data.get("text") or ""),
            sha256=str(data.get("sha256") or ""),
            exists=bool(data.get("exists")),
        )

    def to_state(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "sha256": self.sha256,
            "path": self.path,
            "exists": self.exists,
        }


@dataclass(frozen=True, slots=True)
class MemoRef:
    """The memo pointer: a count, its display form, and where the memos live."""

    count: int | None = None
    display: str | None = None
    path: str = ""

    @classmethod
    def from_state(cls, value: Any) -> MemoRef | None:
        data = as_dict(value)
        if not data:
            return None
        count = data.get("count")
        display = data.get("display")
        return cls(
            count=count if isinstance(count, int) else None,
            display=str(display) if isinstance(display, str) and display else None,
            path=str(data.get("path") or ""),
        )

    def to_state(self) -> dict[str, Any]:
        return {"count": self.count, "display": self.display, "path": self.path}


@dataclass(frozen=True, slots=True)
class ObservationCursor:
    """What the detector saw last, and how far the block has drifted from it.

    Tracked apart from the frozen hashes so a source that changed once does not
    re-emit the same row every turn: the snapshot stays put because it is what
    the model has, while the cursor follows the world.
    """

    observed: dict[str, str] = field(default_factory=dict)
    drift_updates: int = 0
    # The profile behind ``observed["profile"]``. A turn whose profile reads
    # did not answer still knows the trading rule, and a row stating it has
    # to restate everything else as last seen, since a later profile row
    # supersedes the earlier ones whole.
    profile: ProfileSnapshot | None = None


@dataclass(frozen=True, slots=True)
class BaselineEpoch:
    """One epoch of the per-thread baseline, as it sits in the checkpoint."""

    epoch: int = 0
    built_at: str = ""
    reason: str = ""
    workspace: Workspace = field(default_factory=Workspace)
    identity: Identity | None = None
    profile: ProfileSnapshot | None = None
    memo: MemoRef | None = None
    agent_md: FileEntry | None = None
    memory: dict[str, FileEntry] = field(default_factory=dict)
    # Harness-authored text the block states verbatim (the MCP roster, the
    # skills manifest), keyed by read kind. One mapping rather than a field per
    # kind, the way the memory tiers are one mapping: a third of them is then a
    # line at the source, not a migration of the record.
    blocks: dict[str, FileEntry] = field(default_factory=dict)
    compaction_seen: str | None = None
    incomplete: bool = False
    cursor: ObservationCursor = field(default_factory=ObservationCursor)
    # Whether the static system prompt says the file mount serves. The prompt
    # is the cached prefix, so the mount coming up or going down mid-epoch
    # would miss the whole prefix; the value holds until the next rebuild,
    # and the tools work either way. None for a build that states nothing
    # about the mount (Flash) or an epoch stored before the value was.
    files_mounted: bool | None = None
    # Whether the block states the chat-app settings folder, held the same
    # way, since the block is cached per thread. None for a build that says
    # nothing of it or an epoch stored before the value was.
    channels_enabled: bool | None = None
    # False for the empty epoch of a thread's first turn, which is what tells
    # the first turn apart from an epoch that happens to carry no sources.
    stored: bool = False

    @classmethod
    def from_state(cls, value: Any) -> BaselineEpoch:
        """The epoch a checkpoint holds, with every missing key filled in."""
        data = as_dict(value)
        tiers = as_dict(data.get("memory"))
        memory: dict[str, FileEntry] = {}
        for tier in MEMORY_TIERS:
            entry = FileEntry.from_state(tiers.get(tier))
            if entry is not None:
                memory[tier] = entry
        blocks: dict[str, FileEntry] = {}
        for kind, stored in as_dict(data.get("blocks")).items():
            entry = FileEntry.from_state(stored)
            if entry is not None:
                blocks[str(kind)] = entry
        observed = {
            str(key): str(sha) for key, sha in as_dict(data.get("observed")).items()
        }
        seen = data.get("compaction_seen")
        mounted = data.get("files_mounted")
        channels = data.get("channels_enabled")
        return cls(
            epoch=int(data.get("epoch") or 0),
            built_at=str(data.get("built_at") or ""),
            reason=str(data.get("reason") or ""),
            workspace=Workspace.from_state(data.get("workspace")),
            identity=Identity.from_state(data.get("identity")),
            profile=ProfileSnapshot.from_state(data.get("profile")),
            memo=MemoRef.from_state(data.get("memo")),
            agent_md=FileEntry.from_state(data.get("agent_md")),
            memory=memory,
            blocks=blocks,
            compaction_seen=seen if isinstance(seen, str) else None,
            incomplete=bool(data.get("incomplete")),
            cursor=ObservationCursor(
                observed=observed,
                drift_updates=int(data.get("drift_updates") or 0),
                profile=ProfileSnapshot.from_state(data.get("profile_seen")),
            ),
            files_mounted=mounted if isinstance(mounted, bool) else None,
            channels_enabled=channels if isinstance(channels, bool) else None,
            stored=bool(data),
        )

    def to_state(self) -> dict[str, Any]:
        """The plain dict the checkpoint stores."""
        state: dict[str, Any] = {
            "epoch": self.epoch,
            "built_at": self.built_at,
            "reason": self.reason,
            "workspace": self.workspace.to_state(),
            "identity": (self.identity or Identity()).to_state(),
            "profile": self.profile.to_state() if self.profile is not None else {},
            "memo": self.memo.to_state() if self.memo is not None else {},
            "memory": {tier: entry.to_state() for tier, entry in self.memory.items()},
            "blocks": {kind: entry.to_state() for kind, entry in self.blocks.items()},
            "compaction_seen": self.compaction_seen,
            "observed": dict(self.cursor.observed),
            "drift_updates": self.cursor.drift_updates,
            "incomplete": self.incomplete,
        }
        if self.agent_md is not None:
            state["agent_md"] = self.agent_md.to_state()
        if self.files_mounted is not None:
            state["files_mounted"] = self.files_mounted
        if self.channels_enabled is not None:
            state["channels_enabled"] = self.channels_enabled
        if self.cursor.profile is not None and self.cursor.profile != self.profile:
            # Left out when it is the frozen copy, which the hash then names.
            state["profile_seen"] = self.cursor.profile.to_state()
        return state

    def source_entry(self, kind: str) -> FileEntry | None:
        """The frozen copy for one read source, or None when the epoch froze none."""
        if kind == "agent_md":
            return self.agent_md
        if kind.startswith("memory:"):
            return self.memory.get(kind.split(":", 1)[1])
        return self.blocks.get(kind)

    def source_kinds(self) -> set[str]:
        """Every read source this epoch froze a copy of."""
        kinds = {f"memory:{tier}" for tier in self.memory} | set(self.blocks)
        if self.agent_md is not None:
            kinds.add("agent_md")
        return kinds

    @property
    def has_files(self) -> bool:
        return self.agent_md is not None or bool(self.memory)


@dataclass(frozen=True, slots=True)
class Observations:
    """Everything one turn boundary saw, plus the settings it read under.

    Handed whole to :func:`advance_epoch` so that deciding between a rebuild and
    a set of rows is a pure function of the turn: nothing downstream of here
    reads a clock, a store or the sandbox.
    """

    now: datetime
    workspace: Workspace = field(default_factory=Workspace)
    identity: Identity = field(default_factory=Identity)
    profile: ProfileSnapshot = field(default_factory=ProfileSnapshot)
    reads: tuple[SourceRead, ...] = ()
    memo_configured: bool = False
    memo_count: int | None = None
    memo_path: str = ""
    compaction: str | None = None
    rebuild_after_updates: int = 0
    # The kinds of the change rows the compaction cutoff left where the model
    # can still read them, one entry per row. A rebuild folds their content
    # into the blocks, but the rows keep claiming to supersede the block, so a
    # rebuild with any of them in view files one more row that hands authority
    # back. The kinds say which rows a partial rebuild must leave in force.
    retained_rows: tuple[str, ...] = ()
    # False when the platform read behind ``profile`` or ``workspace`` did not
    # answer this turn. A failed read is not a cleared profile or an unnamed
    # workspace, so the change detector skips the source and a rebuild keeps
    # the previous copy, the way ``_entry`` does for a file. The trading rule
    # in ``profile`` is the exception: it comes off the binding plans, so it
    # is measured either way.
    profile_available: bool = True
    workspace_available: bool = True
    # False for a build with no workspace to read at all (Flash). Such a build
    # is not blind to the workspace, it has none, so a rebuild drops the one
    # an earlier PTC epoch froze instead of carrying it forward.
    workspace_configured: bool = True
    # Whether the file mount serves as this turn's agent was built. None for
    # a build whose prompt says nothing about it.
    files_mounted: bool | None = None
    # Whether this turn's agent has the chat-app settings folder. None for a
    # build whose block says nothing about it.
    channels_enabled: bool | None = None


# ---------------------------------------------------------------------------
# Advancing the epoch
# ---------------------------------------------------------------------------


def advance_epoch(
    epoch: BaselineEpoch, observations: Observations
) -> tuple[BaselineEpoch | None, list[DurableUpdate]]:
    """The epoch this turn leaves behind, and the rows it owes the model.

    One decision for all of it. Either the epoch is rebuilt, and no row is owed
    because the block itself now says what the sources hold, or the block stands
    and every source that moved since it was last observed files a row. Returns
    ``(None, [])`` when nothing moved, which is the common case and writes no
    state at all.
    """
    reason = _rebuild_reason(epoch, observations)
    if reason is not None:
        rebuilt = _freeze(epoch, observations, reason)
        # A rebuild that carried a source's old copy forward has not folded
        # that source's rows in, so they stay the current word on it and the
        # retiring row names them as the exception; every other row is
        # retired now, whether or not the epoch is complete, since a source
        # that stays unavailable would otherwise keep a row about a folded
        # source in force for as long as the retries run. A file read that
        # failed is retried by the incomplete epoch; a platform read that did
        # not answer is measured again by the change detector next turn.
        carried = _carried_row_kinds(observations)
        folded = [kind for kind in observations.retained_rows if kind not in carried]
        rows: list[DurableUpdate] = []
        if folded:
            still = sorted(
                {kind for kind in observations.retained_rows if kind in carried}
            )
            rows.append(_rebuilt_row(observations.now, folded, still))
        if not observations.profile_available:
            # The rebuilt block states this turn's trading rule, but a profile
            # row it leaves in force can still state the one before it.
            profile_row, seen = _profile_step(rebuilt, observations)
            if profile_row is not None and seen is not None:
                rows.append(profile_row)
                rebuilt = replace(
                    rebuilt,
                    cursor=ObservationCursor(
                        observed={**rebuilt.cursor.observed, "profile": seen.sha()},
                        drift_updates=rebuilt.cursor.drift_updates + 1,
                        profile=seen,
                    ),
                )
        return rebuilt, rows
    rows, cursor = _observe(epoch, observations)
    # An epoch stored before the cursor kept the profile has a hash that a row
    # since may have moved off the frozen copy. A read that answers says what
    # it names; kept, a later turn whose read does not answer can still
    # measure the trading rule against it.
    backfilled = (
        epoch.profile is not None
        and _seen_profile(epoch) is None
        and cursor.profile is not None
    )
    adopted = {}
    for name in ("files_mounted", "channels_enabled"):
        value = getattr(observations, name)
        if getattr(epoch, name) is None and value is not None:
            adopted[name] = value
    if adopted:
        # An epoch stored before it froze a value takes this turn's, as its
        # rebuild would have: no row, since no prompt stated another.
        epoch = replace(epoch, **adopted)
        backfilled = True
    if not rows and not backfilled:
        return None, []
    return replace(epoch, cursor=cursor), rows


def compaction_fingerprint(event: Any) -> str | None:
    """A stable id for one compaction event.

    ``CompactionEvent`` carries no id of its own, so the fingerprint is the
    boundary it drew plus the summary it wrote: both change together on a new
    compaction and neither changes without one.
    """
    if not isinstance(event, dict):
        return None
    summary = event.get("summary_message")
    summary_id = getattr(summary, "id", None)
    if summary_id is None and isinstance(summary, dict):
        summary_id = summary.get("id")
    return f"{event.get('cutoff_index')}:{event.get('anchor_message_id')}:{summary_id}"


def _carried_row_kinds(obs: Observations) -> set[str]:
    """The row kinds whose source this turn's rebuild carries forward unread."""
    kinds = {read.update_kind for read in obs.reads if not read.available}
    if obs.memo_configured and obs.memo_count is None:
        kinds.add("memo_changed")
    if not obs.profile_available:
        kinds.add("profile_changed")
    if obs.workspace_configured and not obs.workspace_available:
        kinds.add("workspace_changed")
    return kinds


#: The block a change row of each non-harness kind speaks for, as the retiring
#: row names it. The harness kinds name their block from ``HARNESS_BLOCKS``.
_ROW_BLOCKS: dict[str, str] = {
    "agent_md_changed": "<agentmd>",
    "memo_changed": "the memo count",
    "profile_changed": "<user_profile> and <user_identity>",
    "workspace_changed": "<workspace>",
}


def _row_block(kind: str) -> str:
    # The two memory tiers are two blocks with one tag, and a rebuild can fold
    # one while the other read fails, so the exception has to name the tier.
    if kind.startswith("memory_changed:"):
        return f"<memory> for the {kind.split(':', 1)[1]} tier"
    harness = harness_block_for(kind)
    if harness is not None:
        return harness.label
    return _ROW_BLOCKS.get(kind, kind)


def _rebuilt_row(now: datetime, folded: list[str], still: list[str]) -> DurableUpdate:
    """The row that retires the change rows a rebuild folded in.

    Rows are never deleted (a compaction's stored cutoff is positional), and
    each one says to trust it over the frozen block. Once the block is rebuilt
    that is backwards, so the rebuild files the one row that can outrank them:
    it is later than all of them, and it says the blocks are current again.
    ``still`` names the rows the rebuild could not fold in, by the block each
    speaks for, so the model keeps trusting those over their block.
    """
    provenance: dict[str, Any] = {"source": "harness"}
    if still:
        provenance["still_in_force"] = sorted({_row_block(kind) for kind in still})
        # The kinds behind the labels, for the next rebuild's retained-row
        # scan: a compaction can take the change rows this row keeps in force
        # while keeping this row, and the exception has to outlive them.
        provenance["carried"] = sorted(set(still))
    return DurableUpdate(
        kind="baseline_rebuilt",
        schema_version=UPDATE_SCHEMA_VERSION,
        text=f"{len(folded)} earlier change row(s) folded in.",
        provenance=provenance,
        created_at=now,
    )


def _rebuild_reason(epoch: BaselineEpoch, obs: Observations) -> str | None:
    if not epoch.stored:
        return "first_turn"
    if epoch.incomplete:
        return "incomplete"
    if any(read.available and epoch.source_entry(read.kind) is None for read in obs.reads):
        # A source this build reads that the stored epoch never froze: the
        # thread predates the source (a deploy added one), and without a
        # frozen copy there is neither a block nor a delta to file a row from.
        return "new_source"
    if epoch.source_kinds() - {read.kind for read in obs.reads}:
        # The inverse: the stored epoch froze a source this build does not
        # read (a PTC thread continued in flash mode has no agent.md, no
        # workspace memory and no roster). The block would keep rendering it
        # and the change detector would never look at it again.
        return "source_removed"
    if obs.compaction is not None and epoch.compaction_seen != obs.compaction:
        return "compaction"
    if (
        obs.rebuild_after_updates
        and epoch.cursor.drift_updates >= obs.rebuild_after_updates
    ):
        return "drift"
    return None


def _freeze(
    previous: BaselineEpoch, obs: Observations, reason: str
) -> BaselineEpoch:
    """The next epoch from this turn's reads, carrying failures forward."""
    incomplete = any(not read.available for read in obs.reads)

    # Every entry comes from this build's reads; a failed read carries the
    # previous copy forward through ``_entry``, a source with no read at all
    # is dropped, which is what a rebuild for ``source_removed`` is for.
    agent_md: FileEntry | None = None
    memory: dict[str, FileEntry] = {}
    blocks: dict[str, FileEntry] = {}
    for read in obs.reads:
        if read.kind == "agent_md":
            agent_md = _entry(read, previous.agent_md)
        elif read.kind.startswith("memory:"):
            tier = read.kind.split(":", 1)[1]
            entry = _entry(read, previous.memory.get(tier))
            if entry is not None:
                memory[tier] = entry
        else:
            # The file kinds are closed; every other source is harness-authored
            # text, frozen the same way and told apart only by the trust
            # preface, which the file kinds alone carry.
            entry = _entry(read, previous.blocks.get(read.kind))
            if entry is not None:
                blocks[read.kind] = entry

    # A read that did not answer leaves a hole the change detector can never
    # fill, since it only measures against a frozen entry. A memo count that
    # did not answer is the same hole: the epoch says so and rebuilds once more
    # instead of losing the source for the life of the epoch.
    if obs.memo_configured and obs.memo_count is None:
        incomplete = True
    # A build with no memo source (Flash) has none to carry: the pointer an
    # earlier PTC epoch froze names a path this build cannot reach.
    memo = previous.memo if obs.memo_configured else None
    memo_carried = memo is not None and obs.memo_count is None
    if obs.memo_configured and (obs.memo_count is not None or memo is None):
        memo = MemoRef(
            count=obs.memo_count,
            display=_memo_display(obs.memo_count),
            path=obs.memo_path,
        )

    workspace_carried = obs.workspace_configured and not obs.workspace_available
    if obs.workspace_available:
        workspace = obs.workspace
    elif workspace_carried:
        workspace = previous.workspace
    else:
        workspace = Workspace()
    profile = obs.profile
    identity = obs.identity
    profile_carried = not obs.profile_available and previous.profile is not None
    if profile_carried:
        profile = previous.profile.with_trading_of(obs.profile)
        # The identity block is derived from the same read, so a read that
        # did not answer keeps both: the defaults it would render otherwise
        # are nobody's name, zone or market.
        identity = previous.identity or identity

    observed: dict[str, str] = {}
    if agent_md is not None:
        observed["agent_md"] = agent_md.sha256
    for tier, entry in memory.items():
        observed[f"memory:{tier}"] = entry.sha256
    for kind, entry in blocks.items():
        observed[kind] = entry.sha256
    observed["profile"] = profile.sha()

    # A carried source measures from the last value the model can still read:
    # the previous cursor when its row survived the cutoff, the frozen copy
    # otherwise. A value that returns to the block during the outage then
    # files the row that takes the surviving one back, and a value the cutoff
    # took with its row is stated again rather than counted as seen.
    retained = set(obs.retained_rows)
    for key, carried, kind in (
        ("profile", profile_carried, "profile_changed"),
        ("workspace", workspace_carried, "workspace_changed"),
        ("memo", memo_carried, "memo_changed"),
    ):
        if carried and kind in retained and key in previous.cursor.observed:
            observed[key] = previous.cursor.observed[key]
    # The loop above may have kept the previous cursor's hash, and with it the
    # profile behind that hash.
    seen = profile if observed["profile"] == profile.sha() else _seen_profile(previous)

    return BaselineEpoch(
        epoch=previous.epoch + 1,
        built_at=obs.now.isoformat(),
        reason=reason,
        workspace=workspace,
        identity=identity,
        profile=profile,
        memo=memo,
        agent_md=agent_md,
        memory=memory,
        blocks=blocks,
        compaction_seen=obs.compaction,
        incomplete=incomplete,
        cursor=ObservationCursor(observed=observed, drift_updates=0, profile=seen),
        files_mounted=obs.files_mounted,
        channels_enabled=obs.channels_enabled,
        stored=True,
    )


def _observe(
    epoch: BaselineEpoch, obs: Observations
) -> tuple[list[DurableUpdate], ObservationCursor]:
    """Every row this turn owes, and where the cursor lands once they are written."""
    observed = dict(epoch.cursor.observed)
    rows: list[DurableUpdate] = []

    for read in obs.reads:
        row = _file_row(epoch, read, observed, obs.now)
        if row is not None:
            rows.append(row)

    memo_row = _memo_row(epoch, obs)
    if memo_row is not None:
        rows.append(memo_row)
        observed["memo"] = str(obs.memo_count)

    if obs.workspace_available:
        workspace_row = _workspace_row(epoch, obs, observed.get("workspace"))
        if workspace_row is not None:
            rows.append(workspace_row)
        observed["workspace"] = obs.workspace.sha()

    profile_row, seen = _profile_step(epoch, obs)
    if profile_row is not None:
        rows.append(profile_row)
    if seen is not None:
        # Advanced whether or not a row was written: a change the rows cannot
        # describe still counts as seen, and a later return to the frozen
        # values has to read as a change from what was seen last.
        observed["profile"] = seen.sha()

    return rows, ObservationCursor(
        observed=observed,
        drift_updates=epoch.cursor.drift_updates + len(rows),
        profile=seen or epoch.cursor.profile,
    )


def _file_row(
    epoch: BaselineEpoch, read: SourceRead, observed: dict[str, str], now: datetime
) -> DurableUpdate | None:
    """One file source's row, and the cursor entry it advances in place."""
    if not read.available:
        return None
    entry = epoch.source_entry(read.kind)
    if entry is None:
        # The epoch never froze this source, so there is no baseline to measure
        # a delta from. Nothing the model can act on.
        return None
    current_sha = sha256_text(read.content)
    if observed.get(read.kind) == current_sha:
        return None
    observed[read.kind] = current_sha
    if entry.sha256 == current_sha:
        # Back to what the baseline block already says. Still a row: it
        # supersedes an earlier "changed" notice that is now wrong.
        text = f"{read.path} matches its frozen copy again."
    else:
        # A harness block has no file behind it for the model to open.
        text = render_diff(
            entry.text,
            read.content,
            read.path,
            readable=harness_block_for(read.update_kind) is None,
        )
    return DurableUpdate(
        kind=read.update_kind,
        schema_version=UPDATE_SCHEMA_VERSION,
        text=text,
        provenance=dict(read.provenance),
        created_at=now,
    )


def _memo_row(epoch: BaselineEpoch, obs: Observations) -> DurableUpdate | None:
    """A row when the memo count moved since it was last observed.

    The memo pointer is a count, so there is nothing to diff: the row says the
    new number and leaves the frozen block alone. Without it a memo the user
    uploads mid-thread stays invisible until the next compaction.
    """
    if not obs.memo_configured or obs.memo_count is None:
        return None
    frozen = epoch.memo.count if epoch.memo is not None else None
    last = epoch.cursor.observed.get("memo")
    seen = int(last) if isinstance(last, str) and last.isdigit() else frozen
    if seen == obs.memo_count or (seen is None and obs.memo_count == 0):
        return None
    # ``frozen`` is None only in an epoch from before a missing count marked the
    # epoch incomplete; the block then carries no number.
    block = (
        f"the index in your baseline block says {frozen}"
        if isinstance(frozen, int)
        else "your baseline block carries no memo count"
    )
    return DurableUpdate(
        kind="memo_changed",
        schema_version=UPDATE_SCHEMA_VERSION,
        text=f"{obs.memo_count} memo(s) now under {obs.memo_path} ({block}).",
        provenance={"source": "store", "tier": "memo"},
        created_at=obs.now,
    )


def _workspace_row(
    epoch: BaselineEpoch, obs: Observations, last: str | None
) -> DurableUpdate | None:
    """A row when the workspace was renamed or redescribed since last seen.

    The static prompt names <workspace> as the one authoritative place for the
    name, so a rename has to reach the model without the block, and the cache
    behind it, being rewritten.
    """
    frozen = epoch.workspace
    current_sha = obs.workspace.sha()
    if (last or frozen.sha()) == current_sha:
        return None
    if current_sha == frozen.sha():
        text = "Name and description are back to what <workspace> says."
    else:
        text = "\n".join(workspace_diff_lines(frozen, obs.workspace))
    return DurableUpdate(
        kind="workspace_changed",
        schema_version=UPDATE_SCHEMA_VERSION,
        text=text,
        provenance={"source": "platform", "tier": "workspace"},
        created_at=obs.now,
    )


def _seen_profile(epoch: BaselineEpoch) -> ProfileSnapshot | None:
    """The profile the cursor last measured, or None when the epoch cannot say.

    An epoch stored before the cursor kept it has the hash alone, which names
    the frozen copy only when no row has moved it since.
    """
    if epoch.cursor.profile is not None:
        return epoch.cursor.profile
    frozen = epoch.profile
    if (
        frozen is not None
        and epoch.cursor.observed.get("profile", frozen.sha()) == frozen.sha()
    ):
        return frozen
    return None


def _profile_step(
    epoch: BaselineEpoch, obs: Observations
) -> tuple[DurableUpdate | None, ProfileSnapshot | None]:
    """The profile row this turn owes, if any, and the profile the cursor moves to.

    A turn whose profile reads did not answer still stamps its order tools off
    the binding plans the trading rule comes from, so the rule is measured
    against the rest of the profile as last seen rather than skipped with it.
    """
    if obs.profile_available:
        current = obs.profile
    else:
        seen = _seen_profile(epoch)
        if seen is None:
            return None, None
        current = seen.with_trading_of(obs.profile)
    return _profile_row(epoch, current, obs.now), current


def _profile_row(
    epoch: BaselineEpoch, current: ProfileSnapshot, now: datetime
) -> DurableUpdate | None:
    """A row when the profile or the data counts moved since they were last seen.

    The row lists each field as the frozen block has it and as it is now, so a
    preference the user changed mid-thread takes effect on the next turn without
    the block, and everything cached behind it, being rewritten.
    """
    frozen = epoch.profile
    if frozen is None:
        # An epoch from before profiles were frozen: nothing to measure against
        # until the next rebuild carries a snapshot.
        return None
    current_sha = current.sha()
    frozen_sha = frozen.sha()
    if (epoch.cursor.observed.get("profile") or frozen_sha) == current_sha:
        return None
    if current_sha == frozen_sha:
        # Back to what the block already says. Still a row: it supersedes an
        # earlier "changed" notice that is now wrong.
        text = "Every field is back to the value in <user_profile>."
    else:
        text = "\n".join(profile_diff_lines(frozen, current))
    return DurableUpdate(
        kind="profile_changed",
        schema_version=UPDATE_SCHEMA_VERSION,
        text=text,
        provenance={"source": "platform", "tier": "profile"},
        created_at=now,
    )


def _entry(read: SourceRead, previous: FileEntry | None) -> FileEntry | None:
    """The frozen copy for one source.

    A failed read carries the previous epoch's entry forward rather than
    freezing an absence: the model keeps what it had instead of being told to
    recreate a file the sandbox merely failed to hand over.
    """
    if not read.available:
        return previous
    raw = read.content
    return FileEntry(
        path=read.path,
        text=_truncate(raw, read.cap),
        sha256=sha256_text(raw),
        exists=read.text is not None,
    )


def _truncate(content: str, cap: int | None) -> str:
    if cap is None or len(content) <= cap:
        return content
    # Cut at a newline so the slice never lands mid-fence and tempts the model
    # to "repair" content it did not write.
    cut = content.rfind("\n", 0, cap)
    if cut <= 0:
        cut = cap
    return content[:cut] + "\n\n[... truncated ...]"


def _memo_display(count: int | None) -> str | None:
    if count is None or count <= 0:
        return None
    if count > MEMO_DISPLAY_CAP:
        return f"{MEMO_DISPLAY_CAP}+"
    return str(count)

