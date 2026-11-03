"""How a Claude Code session compacts: when the CLI does it, the launch that
has it compact between requests of ours, and what a compaction reports.

The CLI compacts a session on its own once the context reaches the thread's
threshold, given the window ``cli.options_for`` sets from it. A session runs
on the pin it started on, and the CLI replays its record of that pin until it
compacts, whatever a later launch passes. So the build's prompt and tools
reach a session only through a compaction (``compact``): at a turn start, or
on a manual /compact, the CLI compacts under the old pin, and the next launch
runs on the new one.

Whether a turn start does that follows the main agent's turn-start rebuild
(``middleware/compaction/rebuild.py``): a session whose context reaches the
thread's threshold with the turn's prompt moves on any change, since the CLI
compacts it in the turn anyway, and so does one the CLI compacted on an old
pin (``repin_due``), since that compaction was the one the move was due;
under it, a pin below the build's ``rebuild_before`` moves whatever its size
or cache, a deploy's change moves once the context the turn sends is at
least ``floor_tokens`` and its prompt cache has expired, so the turn's first
request would write the whole context again anyway, and a change of per-turn
inputs alone, or a rollback, never does.

A turn start whose cache has expired also compacts a session on any pin once
the context the turn sends reaches ``cold_threshold``, unless the send keeps
the full context, as the main agent summarizes a cold Claude thread: the
turn's first request would write that context to the cache again otherwise.
Between turns, ``next_summary`` tells the composer what the next turn start
does, by the same rules.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Awaitable, Mapping, Sequence
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
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.config import get_stream_writer
from langgraph.constants import TAG_NOSTREAM

from ptc_agent.agent.claude_code.cli import RuntimeSpec, compaction_failure, compacts_at, is_haiku, launch, pointed, received, turn_failure
from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay
from ptc_agent.agent.claude_code.session import (
    SETTLE_TIMEOUT_S,
    CheckpointSessionStore,
    SessionPin,
    SessionRecords,
    SessionWrite,
    compacted,
)
from ptc_agent.agent.claude_code.tools_bridge import ToolResults
from ptc_agent.agent.middleware.compaction.rebuild import NextSummary, cold_threshold
from ptc_agent.agent.middleware.compaction.utils import attachment_tokens, stamp_context_threshold
from ptc_agent.agent.prompts import release
from ptc_agent.agent.transcript import Window
from ptc_agent.agent.transcript.pointer import aexport_transcript, transcript_note, transcript_target
from src.config.settings import get_compaction_timeout
from src.llms.cache_markers import ttl_seconds
from src.llms.claude_code import ClaudeTurnFailed
from src.llms.token_counter import extract_token_usage
from src.server.utils.error_sanitization import sanitize_error_text

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


#: The bytes of text a Claude token holds, taken low so a prompt's count errs
#: high: 2.5 to 2.8 in English, Chinese and Japanese prose and in code, 2.1 in
#: JSON, and 1.7 to 1.9 in number tables and hex ids, where it errs up to a
#: sixth low (``count_tokens_claude``, which is fitted to Claude's own count).
_PROMPT_BYTES_PER_TOKEN = 2


def prompt_tokens(prompt: str | list[dict[str, Any]]) -> int:
    """What a turn's prompt adds to the session's context before a request
    has measured it: its text by its size, and its attachments as the model
    is charged for them. Not by tiktoken, which fetches its encoding over the
    network on first use, and the turn start would wait on that."""
    blocks = [{"type": "text", "text": prompt}] if isinstance(prompt, str) else prompt
    text = sum(len(b["text"].encode()) for b in blocks if b.get("type") == "text")
    return text // _PROMPT_BYTES_PER_TOKEN + attachment_tokens([HumanMessage(blocks)])


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
    five minutes when a request wrote both kinds, since that part expires
    first, or when a compaction is newer than any request, since its summary
    request writes for five minutes (``_metered``)."""
    for e in reversed(entries):
        if compacted(e):
            return ttl_seconds("5m")
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
        threshold: The context the CLI is launched to compact at: the
            thread's threshold, as near as the CLI's window bounds allow
            (``compacts_at``); 0 leaves the CLI's own default.
        settled_at: When the thread's previous run settled, by the run
            ledger, a naive one read as UTC; None when it has none. A run
            that was cancelled or failed still used the cache, and left no
            record saying when.
        backend: The sandbox whose file tools a summary sends the agent to
            the thread's transcript with, in ``workspace_id``'s folder.
        enabled: False when the main agent's compaction is off, which then
            makes no turn-start rebuild either, so no session moves.
        sent_at: When the server received the turn's send (epoch seconds),
            when the cache's expiry is judged: the composer says a deploy's
            rebuild or a cold compaction is coming only once the cache has
            expired, so a send made before then meets neither, however long
            it waits to start. None judges it at the turn start.
        cold_tokens: The context a cold session compacts at when under
            ``threshold`` (``cold_threshold``); 0 never, as with the user's
            setting off.
        keep_full_context: The turn's send asked not to be compacted for a
            cold cache alone; a move to the build's pin still is.
    """

    floor_tokens: int
    threshold: int = 0
    settled_at: datetime | None = None
    backend: SandboxBackend | None = None
    workspace_id: str | None = None
    enabled: bool = True
    sent_at: float | None = None
    cold_tokens: int = 0
    keep_full_context: bool = False

    @classmethod
    def of(cls, settings: CompactionConfig, turn: TurnContext | None, *, backend: SandboxBackend | None = None,
           workspace_id: str | None = None) -> CompactionRules:
        """The main agent's rules for a turn: its compaction config, the user's
        settings included, when the thread's previous run settled and the turn
        was sent, whether the send keeps the full context, and where its
        transcripts are read."""
        return cls(
            settings.rebuild_floor_tokens,
            threshold=compacts_at(settings.token_threshold) if settings.enabled else 0,
            settled_at=turn.previous_run_settled_at if turn else None,
            backend=backend,
            workspace_id=workspace_id,
            enabled=settings.enabled,
            sent_at=turn.sent_at if turn else None,
            cold_tokens=settings.cold_compact_tokens,
            keep_full_context=bool(turn and turn.keep_full_context),
        )

    def reading(self) -> dict[str, Any]:
        """What ``next_summary`` reads of this turn's rules between turns
        (``RULES_KEY``), the user's settings included, and when it was read."""
        return {"threshold": self.threshold, "floor_tokens": self.floor_tokens, "cold_tokens": self.cold_tokens,
                "at": time.time()}

    @property
    def cold_at(self) -> int:
        """The context a cold session compacts at, or 0 where it compacts only
        at ``threshold``."""
        lowered = cold_threshold(self.threshold, self.cold_tokens)
        return lowered if self.enabled and lowered < self.threshold else 0

    def expires_at(self, entries: Sequence[Mapping[str, Any]]) -> float:
        """When the session's prompt cache expires: its ttl after the later of
        the session's newest record and the previous run's settling, both read
        after a request ended while the cache's life counts from its start, so
        the estimate errs towards warm."""
        settled = self.settled_at.replace(tzinfo=self.settled_at.tzinfo or UTC).timestamp() if self.settled_at else 0.0
        return max(_last_activity(entries), settled) + cache_ttl(entries)

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

    def repin_plan(self, records: SessionRecords, current: SessionPin, now: float,
                   pending: int = 0) -> tuple[bool, str] | None:
        """Whether this turn start compacts the session and why: to move it
        to ``current`` by the turn-start rebuild's reasons (``_move``), or for
        an expired cache once the context reaches ``cold_at``, on any pin,
        unless the send keeps the full context. None when the session runs on
        ``current`` and its cache is warm or its context under ``cold_at``.
        ``pending`` is what the turn's prompt adds to the context, which no
        request has measured yet; the thresholds and the floor read the
        context with it, as the main agent's read the context its turn's
        first call sends.

        The cache is judged at the turn's send (``sent_at``) when there is
        one, else at ``now`` (``expires_at``).
        """
        kept = records.get("pinned")
        if not kept:
            return None
        entries = records.get("entries") or []
        tokens = context_tokens(entries) + pending
        expired = (self.sent_at if self.sent_at is not None else now) > self.expires_at(entries)
        moves, reason = (False, None) if same_pin(kept, current) else self._move(records, tokens, expired=expired)
        if moves or not (expired and self.cold_at and tokens >= self.cold_at):
            return (moves, reason) if reason else None
        if self.keep_full_context:
            return False, "kept_full_context"
        return True, "cold"

    def _move(self, records: SessionRecords, tokens: int, *, expired: bool) -> tuple[bool, str]:
        """Whether a session on another pin moves at a turn start that sends
        ``tokens``, by the turn-start rebuild's reasons, and ``threshold`` for
        one the CLI would compact anyway."""
        kept = records.get("pinned") or {}
        if not self.enabled:
            return False, "off"
        if records.get("repin_due"):
            return True, "compacted"
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
        if not expired:
            return False, "warm"
        return True, "deploy_cold"

    def moves_mid_turn(self, records: SessionRecords, current: SessionPin, pending: int) -> bool:
        """Whether a turn's next query, which adds ``pending`` to a session on
        another pin, reaches the threshold, where the CLI would compact it on
        the pin it runs on: the turn then compacts it first and moves it, as
        a turn start would."""
        kept = records.get("pinned")
        tokens = context_tokens(records.get("entries") or []) + pending
        return bool(kept and self.enabled and self.threshold and not same_pin(kept, current) and tokens >= self.threshold)


def next_summary(rules: Mapping[str, Any], records: SessionRecords, *, settled_at: datetime | None) -> NextSummary | None:
    """What a Claude Code thread's next turn start compacts, read between
    turns as the main agent's ``next_summary`` reads an API thread's, by the
    turn start's own rules (``CompactionRules.repin_plan``): from the
    session's records and the ``rules`` its last turn ran under
    (``RULES_KEY``), the user's settings included. The prompt the next turn
    sends is not known yet, so the context is the last request's, and a pin
    from another build's sources reads as one due a move. Only ``cold`` gives
    way to a send that keeps the full context."""
    kept = records.get("pinned")
    judged = CompactionRules(rules.get("floor_tokens") or 0, threshold=rules.get("threshold") or 0, settled_at=settled_at,
                             cold_tokens=rules.get("cold_tokens") or 0)
    if not kept or not judged.threshold:
        return None
    entries = records.get("entries") or []
    tokens, at = context_tokens(entries), judged.expires_at(entries)
    moves, reason = judged._move(records, tokens, expired=False)
    if moves:
        return {"reason": "threshold"} if reason == "threshold" else {"reason": "rebuild"}
    if judged._move(records, tokens, expired=True)[0]:
        return {"reason": "rebuild", "at": at}
    if judged.cold_at and tokens >= judged.cold_at:
        return {"reason": "cold", "at": at}
    return None


#: The rules of a turn that compacts by none of the main agent's settings.
NO_RULES = CompactionRules(floor_tokens=0)


async def compact(spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, *, thread_key: str, identity: str,
                  records: SessionRecords, transcript_note: Awaitable[str] | None = None) -> list[SessionWrite]:
    """Have the CLI compact the session on the pin it runs on, in a launch of
    its own that takes no turn, and return the records it wrote; none when
    there was nothing to summarize. A launch or a summary request that fails
    raises as a turn's would, a refused login reported as a turn reports it,
    and the whole of it is held to the compaction budget an API thread's is,
    the transcript's save for ``transcript_note`` included: admission holds
    a send for about that budget.

    Its own launch, because the summary request is then the session's own
    prefix, thinking included, and a launch that does not compact leaves the
    session as it was.
    """
    budget = get_compaction_timeout()
    try:
        async with asyncio.timeout(budget):
            note = await transcript_note if transcript_note is not None else ""
            return await _compacted(spec, pin, launcher, thread_key=thread_key, identity=identity, records=records,
                                    transcript_note=note)
    except TimeoutError:
        raise ClaudeTurnFailed("api_error", f"the compaction did not finish within {budget:g}s") from None


async def _compacted(spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, *, thread_key: str, identity: str,
                     records: SessionRecords, transcript_note: str) -> list[SessionWrite]:
    store = CheckpointSessionStore()
    prepared, client, _ = await launch(
        spec, pin, launcher, thread_key=thread_key, identity=identity,
        records=noted(records) if is_haiku(spec.model) else records, store=store,
        results=ToolResults(), query="/compact", compaction=True, transcript_note=transcript_note,
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


def _messages(entries: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """The conversation messages a session's records hold since its last
    compaction, by the uuid of each record, one key per message: the CLI
    records an answer as one record per content block."""
    return {
        e["uuid"]: (e.get("message") or {}).get("id") or e["uuid"]
        for e in _since_compaction(entries)
        if e.get("type") in ("user", "assistant") and e.get("uuid") and not e.get("isMeta")
    }


@dataclass(frozen=True)
class SessionCompaction:
    """What a compaction between turns wrote, read off the session's records
    before it (``entries``) and the records it wrote."""

    entries: Sequence[Mapping[str, Any]]
    writes: list[SessionWrite]

    @property
    def summary_text(self) -> str:
        summary = next((e for w in self.writes for e in w["entries"] if e.get("isCompactSummary")), {})
        content = (summary.get("message") or {}).get("content")
        text = content if isinstance(content, str) else "".join(b.get("text", "") for b in content or [] if isinstance(b, dict))
        return pointed(text, "")

    @property
    def original_count(self) -> int:
        return len(set(_messages(self.entries).values()))

    @classmethod
    def at(cls, entries: Sequence[Mapping[str, Any]], boundary: str) -> SessionCompaction:
        """The compaction at the ``boundary`` record among a session's records."""
        cut = next((i for i, e in enumerate(entries) if e.get("uuid") == boundary), len(entries))
        return cls(entries[:cut], [{"session_id": "", "entries": [dict(e) for e in entries[cut:]]}])

    @property
    def _boundary(self) -> Mapping[str, Any]:
        return next((e for w in self.writes for e in w["entries"] if e.get("subtype") == "compact_boundary"), {})

    @property
    def boundary(self) -> str:
        """The uuid of the compaction's boundary record."""
        return self._boundary.get("uuid") or ""

    @property
    def new_count(self) -> int:
        """The summary and the messages the CLI kept after it."""
        kept = set(((self._boundary.get("compactMetadata") or {}).get("preservedMessages") or {}).get("uuids") or [])
        return 1 + len({key for uuid, key in _messages(self.entries).items() if uuid in kept})

    def notice(self) -> dict[str, Any]:
        """The summarize complete signal's payload, the shape the compaction
        middleware gives an API thread's, which replay reads off the reply
        that follows the compaction too."""
        text = self.summary_text
        return {"summary_length": len(text), "original_message_count": self.original_count, "summary_text": text,
                "source": "provider"}

    def update(self) -> dict[str, Any]:
        return {"claude_session": self.writes}


def context_signal(action: str, signal: str, **fields: Any) -> None:
    """Emit a ``context_window`` signal as the compaction middleware emits an
    API thread's. An ``error`` is the CLI's own text, which can quote a request."""
    if "error" in fields:
        fields["error"] = sanitize_error_text(str(fields["error"]))
    try:
        get_stream_writer()({"type": "context_window", "action": action, "signal": signal, **fields})
    except Exception:  # noqa: BLE001 - a signal is never worth failing a step for
        logger.debug("[ClaudeCode] %s %s signal not emitted", action, signal, exc_info=True)


def report_usage(ai: AIMessage, threshold: int) -> None:
    """Stamp ``ai`` with the threshold its context fills and emit its usage,
    with the payload the compaction middleware emits for an API thread's
    call, so the live ring and replay show the threshold the CLI compacts at."""
    if not threshold:
        return
    usage = extract_token_usage(ai)
    input_tokens, output_tokens = usage.get("input_tokens", 0), usage.get("output_tokens", 0)
    if input_tokens <= 0:
        return
    stamp_context_threshold(ai, threshold)
    context_signal("token_usage", "complete", input_tokens=input_tokens, output_tokens=output_tokens,
                   total_tokens=input_tokens + output_tokens, threshold=threshold)
