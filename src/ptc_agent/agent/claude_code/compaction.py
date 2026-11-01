"""How a Claude Code session compacts: when the CLI does it, and the launch
that has it compact between requests of ours.

The CLI compacts a session on its own once the context reaches the thread's
threshold, given the window ``cli.options_for`` sets from it. A session runs
on the pin it started on, and the CLI replays its record of that pin until it
compacts, whatever a later launch passes. So the build's prompt and tools
reach a session only through a compaction (``compact``): at a turn start the
CLI compacts under the old pin, and the next launch runs on the new one.

Whether a turn start does that follows the main agent's turn-start rebuild
(``middleware/compaction/rebuild.py``): a session at or over the thread's
threshold moves on any change, since the CLI would compact it before the
turn's first request anyway; under it, a pin below the build's
``rebuild_before`` moves whatever its size or cache, a deploy's change moves
once the session holds at least ``floor_tokens`` and its prompt cache has
expired, so the turn's first request would write the whole context again
anyway, and a change of per-turn inputs alone, or a rollback, never does.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeSDKClient,
    ProcessError,
    RateLimitEvent,
    ResultMessage,
    SystemMessage,
)
from langgraph.constants import TAG_NOSTREAM

from ptc_agent.agent.claude_code.cli import RuntimeSpec, compaction_failure, is_haiku, launch, received, turn_failure
from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay
from ptc_agent.agent.claude_code.session import (
    SETTLE_TIMEOUT_S,
    CheckpointSessionStore,
    SessionPin,
    SessionRecords,
    SessionWrite,
)
from ptc_agent.agent.claude_code.tools_bridge import ToolResults
from ptc_agent.agent.prompts import release
from ptc_agent.agent.transcript import Window
from ptc_agent.agent.transcript.pointer import aexport_transcript, transcript_note, transcript_target
from src.config.settings import get_compaction_timeout
from src.llms.cache_markers import ttl_seconds
from src.llms.claude_code import ClaudeTurnFailed

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.agent.middleware.runtime_context import TurnContext
    from ptc_agent.config.agent import CompactionConfig

logger = logging.getLogger(__name__)

#: What the runtime asks the CLI for (``CLAUDE_CODE_PROMPT_CACHE_TTL``), for
#: a session whose records show no cache write.
DEFAULT_TTL = ttl_seconds("1h")
#: The answer a Haiku session's compaction launch is handed after its last
#: one (``noted``).
NOTED = "Noted."


def same_pin(a: SessionPin, b: SessionPin) -> bool:
    """Whether two pins send the same prefix, whichever build recorded them."""
    return a["system_prompt"] == b["system_prompt"] and a["tools"] == b["tools"]


def _usage(entry: Mapping[str, Any]) -> Mapping[str, Any] | None:
    message = entry.get("message") if entry.get("type") == "assistant" else None
    return (message or {}).get("usage") if isinstance(message, Mapping) else None


def _since_compaction(entries: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
    start = next((i + 1 for i in range(len(entries) - 1, -1, -1) if entries[i].get("subtype") == "compact_boundary"), 0)
    return entries[start:]


def context_tokens(entries: Sequence[Mapping[str, Any]]) -> int:
    """The context the session's last request since its last compaction sent
    and got back, by its usage; 0 when none has run since."""
    usage = next((u for e in reversed(_since_compaction(entries)) if (u := _usage(e))), None) or {}
    return sum(usage.get(k) or 0 for k in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens"))


def cache_ttl(entries: Sequence[Mapping[str, Any]]) -> int:
    """The life in seconds of the newest cache entries the session wrote:
    five minutes when a request wrote both kinds, since that part expires first."""
    for e in reversed(entries):
        written = (_usage(e) or {}).get("cache_creation") or {}
        if written.get("ephemeral_5m_input_tokens"):
            return ttl_seconds("5m")
        if written.get("ephemeral_1h_input_tokens"):
            return ttl_seconds("1h")
    return DEFAULT_TTL


def _last_activity(entries: Sequence[Mapping[str, Any]]) -> float:
    for e in reversed(entries):
        stamp = e.get("timestamp")
        if isinstance(stamp, str):
            try:
                return datetime.fromisoformat(stamp).timestamp()
            except ValueError:
                continue
    return 0.0


@dataclass(frozen=True)
class CompactionRules:
    """A thread's compaction settings for this turn, the main agent's.

    Attributes:
        floor_tokens: The least context a deploy's change moves the session
            at; 0 never moves one.
        threshold: The context the thread compacts at, which the CLI is
            launched to compact at too; 0 leaves the CLI's own default.
        settled_at: When the thread's previous run settled, by the run
            ledger, a naive one read as UTC; None when it has none. A run
            that was cancelled or failed still used the cache, and left no
            record saying when.
        backend: The sandbox whose file tools a summary sends the agent to
            the thread's transcript with, in ``workspace_id``'s folder.
        enabled: False when the main agent's compaction is off, which then
            makes no turn-start rebuild either, so no session moves.
    """

    floor_tokens: int
    threshold: int = 0
    settled_at: datetime | None = None
    backend: SandboxBackend | None = None
    workspace_id: str | None = None
    enabled: bool = True

    @classmethod
    def of(cls, settings: CompactionConfig, turn: TurnContext | None, *, backend: SandboxBackend | None = None,
           workspace_id: str | None = None) -> CompactionRules:
        """The main agent's rules for a turn: its compaction config, when the
        thread's previous run settled, and where its transcripts are read."""
        return cls(
            settings.rebuild_floor_tokens,
            threshold=settings.token_threshold if settings.enabled else 0,
            settled_at=turn.previous_run_settled_at if turn else None,
            backend=backend,
            workspace_id=workspace_id,
            enabled=settings.enabled,
        )

    async def transcript_note(self, thread_id: str, state: Mapping[str, Any]) -> str:
        """What the compaction summaries in the thread's session records end
        on: the pointer an API thread's summary ends on, once the transcript
        of the thread's messages is saved where the file tools reach it, as
        the compaction middleware saves it before it points; else nothing."""
        entries = (state.get("claude_session") or {}).get("entries") or []
        if not any(e.get("isCompactSummary") for e in entries):
            return ""
        exported = await aexport_transcript(
            self.backend, transcript_target(self.backend, thread_id), state["messages"],
            workspace_id=self.workspace_id, window=Window.of(state),
        )
        return transcript_note(exported) if exported else ""

    def repin_plan(self, records: SessionRecords, current: SessionPin, now: float) -> tuple[bool, str] | None:
        """Whether the session moves to ``current`` at this turn start and why,
        by the turn-start rebuild's reasons and ``threshold`` for one the CLI
        would compact anyway; None when it already runs on ``current``.

        The cache counts as cold once its ttl has passed since the later of
        the session's newest record and the previous run's settling, both
        read after a request ended while the cache's life counts from its
        start, so the estimate errs towards warm.
        """
        kept = records.get("pinned")
        if not kept or same_pin(kept, current):
            return None
        if not self.enabled:
            return False, "off"
        entries = records.get("entries") or []
        tokens = context_tokens(entries)
        if self.threshold and tokens >= self.threshold:
            return True, "threshold"
        pinned = kept.get("prompt_version") or 0
        if release.prompt_version() < pinned:
            return False, "rollback"
        if pinned < release.rebuild_before():
            return True, "urgent"
        if kept.get("source_digest") == release.source_digest():
            return False, "inputs_only"
        if not self.floor_tokens:
            return False, "off"
        if tokens < self.floor_tokens:
            return False, "under_floor"
        settled = self.settled_at.replace(tzinfo=self.settled_at.tzinfo or UTC).timestamp() if self.settled_at else 0.0
        if now - max(_last_activity(entries), settled) <= cache_ttl(entries):
            return False, "warm"
        return True, "deploy_cold"


#: The rules of a turn that compacts by none of the main agent's settings.
NO_RULES = CompactionRules(floor_tokens=0)


async def compact(spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, *, thread_key: str, identity: str,
                  records: SessionRecords) -> list[SessionWrite]:
    """Have the CLI compact the session on the pin it runs on, in a launch of
    its own that takes no turn, and return the records it wrote; none when
    there was nothing to summarize. A launch or a summary request that fails
    raises as a turn's would, a refused login reported as a turn reports it,
    and the whole of it is held to the compaction budget an API thread's is.

    Its own launch, because the summary request is then the session's own
    prefix, thinking included, and a launch that does not compact leaves the
    session as it was.
    """
    budget = get_compaction_timeout()
    try:
        async with asyncio.timeout(budget):
            return await _compacted(spec, pin, launcher, thread_key=thread_key, identity=identity, records=records)
    except TimeoutError:
        raise ClaudeTurnFailed("api_error", f"the compaction did not finish within {budget:g}s") from None


async def _compacted(spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, *, thread_key: str, identity: str,
                     records: SessionRecords) -> list[SessionWrite]:
    store = CheckpointSessionStore()
    prepared, client, _ = await launch(
        spec, pin, launcher, thread_key=thread_key, identity=identity,
        records=noted(records) if is_haiku(spec.model) else records, store=store,
        results=ToolResults(), query="/compact", compaction=True,
    )
    try:
        boundary, result, failed = None, None, None
        errors: list[tuple[str, str]] = []
        limits: list[RateLimitEvent] = []
        try:
            async for m in received(client):
                if isinstance(m, SystemMessage) and m.subtype == "init" and m.data.get("model") != spec.model:
                    raise ClaudeTurnFailed("model_unavailable", spec.model)
                if isinstance(m, SystemMessage) and m.subtype == "compact_boundary":
                    boundary = m.data.get("uuid")
                elif isinstance(m, SystemMessage) and m.subtype == "status" and m.data.get("compact_result") == "failed":
                    failed = compaction_failure(str(m.data.get("compact_error") or ""))
                elif isinstance(m, AssistantMessage) and m.error and m.parent_tool_use_id is None:
                    errors.append((m.error, " ".join(getattr(b, "text", "") for b in m.content).strip()))
                elif isinstance(m, RateLimitEvent):
                    limits.append(m)
                elif isinstance(m, ResultMessage):
                    result = m
                    break
        except ProcessError as e:
            # After its init, so it took the launch: failed as its summary
            # request failing is, its stderr kept to the log.
            logger.warning("[ClaudeCode] the CLI exited during the compaction", exc_info=True)
            failed = failed or ClaudeTurnFailed("api_error", f"the CLI exited with code {e.exit_code} before it finished")
        if result is not None:
            await _metered(spec, result)
        if boundary is None:
            failed = failed or turn_failure(result, errors, limits)
            if failed is None:
                return []
            if failed.kind == "auth_failed":
                await launcher.refused()
            raise failed
        await store.arrived.wait([boundary], SETTLE_TIMEOUT_S)
    finally:
        await asyncio.shield(_end(client, launcher, prepared))
    return store.drain()


def noted(records: SessionRecords) -> SessionRecords:
    """``records`` ending on one more answer of Claude's, ``NOTED``, for a
    compaction we start: the CLI summarizes all but the newest answer, so
    the part it summarizes then ends on the session's own last answer rather
    than on the prompt that answer took, which Haiku would answer instead.
    Only the compaction's launch is handed it; the thread's records never
    hold it."""
    entries = records.get("entries") or []
    last = next((e for e in reversed(entries) if e.get("type") == "assistant" and e.get("message")), None)
    if last is None:
        return records
    message = {**last["message"], "id": f"msg_{uuid.uuid4().hex[:24]}", "content": [{"type": "text", "text": NOTED}],
               "stop_reason": "end_turn", "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 1}}
    answer = {**{k: v for k, v in last.items() if k != "requestId"}, "uuid": str(uuid.uuid4()),
              "parentUuid": next(e["uuid"] for e in reversed(entries) if e.get("uuid")), "message": message}
    return {**records, "entries": [*entries, answer]}


async def _end(client: ClaudeSDKClient, launcher: Launcher, prepared: Prepared) -> None:
    """Shielded by the caller, as a turn's close is: a second cancellation
    would otherwise leave the CLI and its config dir on the computer."""
    with contextlib.suppress(Exception):
        await client.disconnect()
    await launcher.release(prepared)


async def _metered(spec: RuntimeSpec, result: ResultMessage) -> None:
    """Report the summary request to the token tracker as the relay reports a
    turn's requests, off the messages stream. The CLI streams the request to
    no one and leaves the result's ``usage`` empty, but counts it in
    ``model_usage``, alone: a launch's config dir restores no earlier cost.
    Its cache write comes as one count, read as 5m: the CLI marks that
    request's cache with no ttl, whatever ``CLAUDE_CODE_PROMPT_CACHE_TTL`` says."""
    for counts in (result.model_usage or {}).values():
        usage = {"input_tokens": counts.get("inputTokens", 0), "output_tokens": counts.get("outputTokens", 0),
                 "cache_read_input_tokens": counts.get("cacheReadInputTokens", 0),
                 "cache_creation_input_tokens": counts.get("cacheCreationInputTokens", 0)}

        async def events():
            yield {"type": "message_start", "message": {"id": "", "type": "message", "role": "assistant", "model": spec.model,
                                                        "content": [], "stop_reason": None, "stop_sequence": None,
                                                        "usage": {"input_tokens": 0, "output_tokens": 0}}}
            yield {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": usage}
            yield {"type": "message_stop"}

        try:
            relay = ClaudeStreamRelay(model_name=spec.model, metadata=dict(spec.billing), cache=False)
            await relay.feed(events()).ainvoke([], {"tags": [TAG_NOSTREAM]})
        except Exception:
            logger.warning("[ClaudeCode] the compaction's usage was not reported", exc_info=True)
