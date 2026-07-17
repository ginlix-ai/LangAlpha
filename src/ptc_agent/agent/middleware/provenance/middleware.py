"""Provenance middleware: emit a ``provenance`` stream event per data source.

Wraps every tool call (``awrap_tool_call``), runs the tool unchanged, then
dispatches on the tool name to a per-tool extractor that yields zero or more
:class:`ProvenanceSource` objects. Each source is emitted as a custom stream
event via ``get_stream_writer`` — the same mechanism FileOperationMiddleware
uses — so records accumulate into ``sse_events`` without entering LLM context.

Extraction NEVER breaks a tool call: the whole extraction path is wrapped in
try/except and the original tool result is always returned. Agent attribution
is omitted on purpose — the streaming handler resolves ``main`` vs ``task:{id}``
from the LangGraph namespace.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langgraph.config import get_stream_writer

from ptc_agent.agent.middleware.provenance.body_store import (
    drain_body_writes,
    schedule_body_write,
    store_bodies,
)
from ptc_agent.agent.provenance import (
    SNIPPET_MAX_CHARS,
    ProvenanceSource,
    build_provenance_event,
    fingerprint_result,
    fingerprint_result_with_body,
    hash_args,
    redact_args,
)
from ptc_agent.agent.provenance.types import (
    RESULT_BODY_MAX_BYTES,
)

logger = logging.getLogger(__name__)

# Filesystem path prefixes used to classify reads. These mirror the routes
# wired into CompositeFilesystemBackend in agent.py (paths.py constants
# MEMO_USER_DIR / MEMORY_USER_DIR / MEMORY_WORKSPACE_DIR).
_MEMO_PREFIX = ".agents/user/memo/"
_MEMORY_PREFIXES = (".agents/user/memory/", ".agents/workspace/memory/")

# Agent-infrastructure path roots whose reads are scaffolding the agent operates
# with — skill docs, generated tool/MCP wrapper modules, system trace files,
# spilled prior tool results — NOT external data its analysis is based on, so
# they emit no provenance. Roots mirror AGENT_SYSTEM_DIRS in paths.py; note
# .agents is split: its user/* + workspace/memory data subtrees stay tracked
# (memo_read / memory_read / file_read), only these infra subdirs are skipped.
_INFRA_PREFIXES = (
    ".system",
    "tools",
    "mcp_servers",
    ".self-improve",
    ".agents/skills",
    ".agents/threads",
    ".agents/large_tool_results",
)

# Agent-scaffolding FILES (not dirs) at the workspace root: injected context, not
# external data. agent.md is the per-workspace notebook auto-injected into every
# turn, so the agent reading it back is not a tracked data access.
_INFRA_FILES = ("agent.md",)

# Sandbox-root prefixes the agent emits on absolute paths (e.g.
# /home/workspace/.agents/...). Stripped before the infra/memo/memory prefix
# checks so classification works for both absolute and relative path forms —
# without this the agent's absolute paths bypass every prefix check.
_SANDBOX_ROOT_PREFIXES = ("home/workspace/", "home/daytona/")

# Market tools whose identifier is a single symbol-bearing arg.
_MARKET_SYMBOL_ARGS = ("symbol", "underlying")

# Market tool -> data-kind slug. Set as ProvenanceSource.detail on symbol-bearing
# rows so the Sources panel can group by ticker yet still distinguish, in a
# hover, which data products that ticker was accessed through (a single AAPL row
# may cover both company_overview and daily_prices). The slug is i18n-mapped by
# the frontend; symbol-less tools (screen_stocks, get_market_movers) already
# surface their tool name as the identifier.
_MARKET_DATA_KINDS = {
    "get_company_overview": "company_overview",
    "get_daily_prices": "daily_prices",
    "get_options_chain": "options_chain",
    "get_quote": "quote",
    # Symbol-bearing only when the caller passes an explicit `indices` list;
    # the no-arg form stays keyed by tool name (detail guard below).
    "get_market_overview": "market_overview",
}

# Market-data tools that all share _extract_market_data. Superset of
# _MARKET_DATA_KINDS — also covers the symbol-less ones (screen/movers/overview).
_MARKET_DATA_TOOLS = (
    "get_daily_prices",
    "get_company_overview",
    "get_market_overview",
    "screen_stocks",
    "get_options_chain",
    "get_market_movers",
    "get_quote",
)

# Host-side bounds on the in-sandbox MCP trace (LLM-authored, untrusted): the
# generated client is supposed to self-limit, but it's code the agent controls,
# so re-clamp here so a poisoned/huge trace can't amplify into DB/SSE/render.
# SNIPPET_MAX_CHARS is imported from provenance.types (the canonical cap shared
# with the in-sandbox client) so host- and sandbox-side truncation stay equal.
_SHA_MAX_CHARS = 128
_IDENT_PART_MAX_CHARS = 256
_MAX_TRACE_ENTRIES = 200
# Cap on the agent-controlled trace timestamp. A well-formed ISO-8601 value is
# ~25-35 chars; anything longer (or non-string) is a poisoned/buggy trace, so it
# falls back to now(). Bounds the one trace field that otherwise rode the SSE
# event unclamped (the DB writer already coerces a bad value to NULL).
_TIMESTAMP_MAX_CHARS = 64

# Sanity ceiling for the agent-claimed true result size (the one trace field that
# otherwise rode through to the SSE event and the BIGINT column unvalidated). It's
# the TRUE byte length of an MCP result, so it can legitimately exceed the 64 KiB
# body cap (a truncated body); this only rejects a non-numeric, negative, or absurd
# value a poisoned trace could forge. Generous — no honest single result approaches
# it (real maxima are low single-digit MB), but it keeps a fabricated exabyte size
# out of the record/event.
_RESULT_SIZE_MAX_BYTES = 256 * 1024 * 1024

# A tool result whose content begins with one of these is treated as failed, so
# we don't record a "source accessed" for data the tool never actually returned.
# Each prefix is qualified (not a bare verb) so legit content isn't misread as an
# error: "exception:"/"exception " not "exception" ("Exceptional returns ..."),
# and "failed to " not "failed" ("Failed Q4 earnings beat ..."). The real string
# errors that reach here are web_fetch's "Failed to fetch/process ...".
_ERROR_CONTENT_PREFIXES = ("[error]", "error:", "failed to ", "exception:", "exception ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


def _normalize_sandbox_path(path: str) -> str:
    """Strip the sandbox root + leading ``./`` and ``/`` so the infra/memo/memory
    prefix checks work whether the agent emitted an absolute
    (``/home/workspace/.agents/...``) or relative (``.agents/...``) path."""
    p = (path or "").lstrip("/").removeprefix("./")
    for prefix in _SANDBOX_ROOT_PREFIXES:
        if p.startswith(prefix):
            return p[len(prefix):]
    return p


def _classify_file_source_type(path: str) -> str:
    """Map a read path to memo_read / memory_read / file_read by prefix."""
    normalized = _normalize_sandbox_path(path)
    if normalized.startswith(_MEMO_PREFIX):
        return "memo_read"
    if any(normalized.startswith(prefix) for prefix in _MEMORY_PREFIXES):
        return "memory_read"
    return "file_read"


def _is_agent_infra_path(path: str) -> bool:
    """True for agent scaffolding (skills/tools/mcp/system/notebook) — not data.

    Dir prefixes match at a path-segment boundary so ``tools/x`` and the bare dir
    ``tools`` hit but a sibling like ``tools_analysis/x`` does not; the
    ``_INFRA_FILES`` notebook files (e.g. agent.md) match exactly.
    """
    normalized = _normalize_sandbox_path(path).rstrip("/")
    if normalized in _INFRA_FILES:
        return True
    return any(
        normalized == prefix or normalized.startswith(prefix + "/")
        for prefix in _INFRA_PREFIXES
    )


# Tools whose artifact carries an in-sandbox ``mcp_trace``. All run agent code
# in the sandbox: ExecuteCode directly, Bash when it runs a script importing the
# MCP wrappers, and BashOutput which surfaces a backgrounded script's trace when
# the command finishes. They share the trace extractor, the error-result
# exemption, and the mcp_trace strip.
_MCP_TRACE_TOOLS = ("ExecuteCode", "Bash", "BashOutput")


def _strip_mcp_trace(result: Any) -> None:
    """Drop the provenance-only mcp_trace key from an ExecuteCode/Bash artifact.

    Mutates the ToolMessage artifact in place so the raw in-sandbox trace never
    propagates outward onto tool_call_result or into persisted sse_events.
    """
    artifact = getattr(result, "artifact", None)
    if isinstance(artifact, dict):
        artifact.pop("mcp_trace", None)


def _truncate(value: Any, limit: int) -> Any:
    """Clamp a string field to ``limit`` chars; pass non-strings through."""
    return value[:limit] if isinstance(value, str) else value


def _clamp_body_bytes(value: Any, limit: int) -> str | None:
    """Byte-clamp an untrusted trace body; pass None / non-strings to None.

    Re-clamps the agent-authored ``result_body`` host-side (the sandbox already
    byte-caps it) on a byte boundary with ``errors="ignore"`` so a multibyte
    split is dropped, mirroring the sandbox's decode.
    """
    if not isinstance(value, str):
        return None
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def _coerce_result_size(value: Any) -> int | None:
    """Trust the agent-authored ``result_size`` only as a bounded non-negative int.

    Every other trace field is clamped on the way in; ``result_size`` rode through
    raw onto the SSE event and the BIGINT column. On a verified body
    ``_verify_source_sha`` overwrites it with the real length, so this guards the
    unverified path (a truncated body, or a forged trace): a non-numeric, negative,
    or absurd size becomes None rather than being emitted/persisted. ``bool`` is
    excluded since it is an ``int`` subclass.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or value > _RESULT_SIZE_MAX_BYTES:
        return None
    return value


def _is_error_result(result: Any) -> bool:
    """True when a tool call failed, so we don't attest a source it never returned.

    Host-side tools mostly catch errors and return an error payload with a
    success status (market tools return ``{"error": ...}``, web_fetch returns an
    ``[error] ...`` string), so inspect the content/artifact shape, not just
    ``ToolMessage.status``.
    """
    if getattr(result, "status", None) == "error":
        return True
    artifact = getattr(result, "artifact", None)
    if isinstance(artifact, dict) and artifact.get("error"):
        return True
    content = getattr(result, "content", result)
    if isinstance(content, str) and content.lstrip()[:32].lower().startswith(
        _ERROR_CONTENT_PREFIXES
    ):
        return True
    return False


class ProvenanceMiddleware(AgentMiddleware):
    """Emit a ``provenance`` event per external source read by a tool call.

    Shared-stack placement gives subagents coverage too. Extraction failures
    are swallowed; the tool result is always returned as-is.

    ``redactor`` (typically ``LeakDetectionMiddleware.redact``) scrubs known
    secret values from every snippet before it is emitted/persisted — provenance
    fingerprints the raw result/artifact, which the leak middleware's
    content-only scan never touches.
    """

    def __init__(
        self,
        redactor: Callable[[str | None], str | None] | None = None,
    ) -> None:
        super().__init__()
        self._redactor = redactor
        # Background body-store writes in flight (strong refs so they aren't GC'd
        # mid-flush), owned by this instance and drained best-effort at agent end.
        # Bounded concurrency + exception consumption live in body_store.
        self._body_flush_tasks: set[asyncio.Task[None]] = set()
        # tool name -> extractor(request, result) -> Iterable[ProvenanceSource]
        self._extractors: dict[
            str, Callable[[Any, Any], Iterable[ProvenanceSource]]
        ] = {
            "WebSearch": self._extract_web_search,
            "WebFetch": self._extract_web_fetch,
            "get_sec_filing": self._extract_sec_filing,
            "Read": self._extract_file_read,
            # Glob is pure path enumeration (a directory listing), not data the
            # analysis rests on, so it is intentionally NOT tracked. Read (content
            # consumed) and Grep (content matched) are.
            "Grep": self._extract_file_read,
            "ExecuteCode": self._extract_execute_code,
            # Bash shares the trace extractor: a script it runs (e.g. `python
            # analysis.py`) writes the same mcp_trace, surfaced on the artifact.
            "Bash": self._extract_execute_code,
            # BashOutput surfaces a backgrounded script's mcp_trace on the status
            # read that observes completion — same artifact contract as Bash.
            "BashOutput": self._extract_execute_code,
            **dict.fromkeys(_MARKET_DATA_TOOLS, self._extract_market_data),
        }

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Run the tool, then emit provenance events for its accessed sources."""
        result = await handler(request)

        try:
            tool_call = request.tool_call
            tool_name = tool_call.get("name")
            extractor = self._extractors.get(tool_name)
            if extractor is None:
                return result

            # Don't attest a source the tool never actually returned. The
            # MCP-trace tools (ExecuteCode/Bash) are exempt: the code/command may
            # "error" while individual in-sandbox MCP calls succeeded, and those
            # trace entries are guarded per-entry.
            if tool_name not in _MCP_TRACE_TOOLS and _is_error_result(result):
                return result

            try:
                sources = list(extractor(request, result))
            finally:
                # mcp_trace is provenance-only scaffolding carrying raw in-sandbox
                # args + snippets. Strip it from the artifact (after extraction
                # reads it) so it never rides the public tool_call_result event or
                # lands in persisted sse_events. In a finally so it runs even if
                # the extractor raises — the raw-args-never-persisted property must
                # not depend on extraction succeeding.
                if tool_name in _MCP_TRACE_TOOLS:
                    _strip_mcp_trace(result)

            if not sources:
                return result

            writer = get_stream_writer()
            if writer is None:
                return result

            body_batch: list[tuple] = []
            for source in sources:
                self._verify_source_sha(source)
                source.result_snippet = self._redact_snippet(source.result_snippet)
                try:
                    writer(build_provenance_event(source))
                except Exception:
                    logger.debug(
                        "[PROVENANCE] failed to emit event for %s",
                        tool_name,
                        exc_info=True,
                    )
                item = self._body_item(source)
                if item is not None:
                    body_batch.append(item)
            # One batched write per TOOL CALL, after this call's emits (best-effort,
            # never on the SSE event): collapses this call's per-source fan-out —
            # web_search's N URLs, the market fan-out's duplicate shas,
            # execute_code's N mcp_trace entries — into a single chunked upsert.
            # Scheduled OFF the tool-call critical path (background task, drained at
            # agent end) so a DB/object-store write never delays the tool result or
            # the next LLM step; the provenance events above already went out.
            if body_batch:
                await schedule_body_write(
                    self._body_flush_tasks, store_bodies(body_batch)
                )
        except Exception:
            # WARNING, not DEBUG: a broken extractor silently degrades the
            # feature to "no provenance"; surface it so it's observable.
            logger.warning(
                "[PROVENANCE] extraction failed; returning tool result unchanged",
                exc_info=True,
            )

        return result

    def _redact_snippet(self, snippet: str | None) -> str | None:
        """Scrub secrets from a snippet; drop it if redaction itself fails."""
        if self._redactor is None:
            return snippet
        try:
            return self._redactor(snippet)
        except Exception:
            logger.debug("[PROVENANCE] snippet redaction failed; dropping snippet")
            return None

    def _redact_body(self, body: str | None) -> str | None:
        """Scrub secrets from a FULL body — pure redaction, NO truncation.

        Uses the same deterministic ``self._redactor`` as ``_redact_snippet``
        (which only redacts; the snippet is truncated upstream at fingerprint
        time), so the body is redacted over its full length while staying
        deterministic for the shared content-addressed store's dedup. Returns
        None if redaction itself fails (don't store an unredacted body).
        """
        if body is None:
            return None
        if self._redactor is None:
            return body
        try:
            return self._redactor(body)
        except Exception:
            logger.debug("[PROVENANCE] body redaction failed; dropping body")
            return None

    def _verify_source_sha(self, source: ProvenanceSource) -> None:
        """Drop an agent-authored content-address the captured body doesn't hash to.

        For ``mcp_tool`` sources both ``result_sha256`` and ``result_body`` come from
        the in-sandbox trace, which the agent's own ``execute_code`` can forge (it
        shares ``MCP_TRACE_FILE``). The body store is GLOBAL + content-addressed, so a
        trusted sha is the only thing standing between one tenant and another's body:
        a forged ``(sha, body)`` pair would either poison the sha for every later
        fetch of the real content, or — paired with a fabricated record — read a body
        by an attacker-chosen address (IDOR-by-content-address).

        We trust the agent's sha only if the bytes we hold reproduce it. On a miss we
        null BOTH the sha and the body, so neither the body write nor the provenance
        record (built from this source's sha) trusts an address we can't verify. A
        self-consistent forgery still passes, but it can only claim its own hash —
        colliding with real content would require breaking SHA-256. Host-computed
        shas (web/sec/market) are produced by trusted code and left untouched.

        A legitimately truncated MCP body (>64 KiB) hashes to the head, not the full
        result, so it fails here and loses its body — honest, since we never hold the
        full bytes to verify or serve anyway.
        """
        if source.source_type != "mcp_tool":
            return
        body, sha = source.result_body, source.result_sha256
        if body and sha and hashlib.sha256(body.encode("utf-8")).hexdigest() == sha:
            # The head we hold IS the full verified body; size it honestly.
            source.result_size = len(body.encode("utf-8"))
            return
        source.result_sha256 = None
        source.result_body = None

    def _body_item(self, source: ProvenanceSource) -> tuple | None:
        """Build a ``(sha, redacted_body, byte_len, content_type)`` store tuple.

        Returns None unless the source carries both a body and the
        ``result_sha256`` it hashes to, or if redaction itself fails (we never
        store an unredacted body). Pure — no DB; the batch is flushed by a
        background task scheduled once per tool call.

        ``byte_len`` is the length of the body we actually store (post-redaction),
        NOT the raw pre-redaction ``result_size``. The read side derives truncation
        as ``byte_len > len(inline)``, so a body that redaction shrank below the
        inline cap is stored whole and reads back complete — the flag tracks what we
        hold, not the original size (which the provenance record keeps as
        ``result_size``).
        """
        if not source.result_body or not source.result_sha256:
            return None
        redacted = self._redact_body(source.result_body)
        if redacted is None:
            return None
        return (
            source.result_sha256,
            redacted,
            len(redacted.encode("utf-8")),
            "text/plain; charset=utf-8",
        )

    async def _drain_body_flushes(self) -> None:
        """Await body writes already scheduled at entry (idempotent best-effort)."""
        await drain_body_writes(self._body_flush_tasks)

    async def aafter_agent(self, state: Any, runtime: Any) -> dict | None:
        """Drain background body writes at agent end (best-effort quiescence)."""
        await self._drain_body_flushes()
        return None

    # ----- per-tool extractors ------------------------------------------

    def _extract_web_search(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """One source per result in ``artifact["results"]`` (shared tool_call_id)."""
        artifact = getattr(result, "artifact", None)
        if not isinstance(artifact, dict):
            return
        tool_call_id = request.tool_call.get("id")
        timestamp = _now_iso()
        for item in artifact.get("results") or []:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            if not url:
                continue
            sha256, size, snippet, body = fingerprint_result_with_body(item)
            yield ProvenanceSource(
                record_id=_new_id(),
                source_type="web_search",
                identifier=url,
                timestamp=timestamp,
                title=item.get("title"),
                provider=None,  # provider not exposed in artifact
                tool_call_id=tool_call_id,
                args=redact_args(request.tool_call.get("args")),
                result_sha256=sha256,
                result_size=size,
                result_snippet=snippet,
                # Body = the same per-item value that produced result_sha256.
                result_body=body,
            )

    def _extract_web_fetch(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """Identifier comes from args["url"] — the result is a bare string."""
        url = (request.tool_call.get("args") or {}).get("url")
        if not url:
            return
        content = getattr(result, "content", result)
        sha256, size, snippet, body = fingerprint_result_with_body(content)
        yield ProvenanceSource(
            record_id=_new_id(),
            source_type="web_fetch",
            identifier=url,
            timestamp=_now_iso(),
            tool_call_id=request.tool_call.get("id"),
            args=redact_args(request.tool_call.get("args")),
            result_sha256=sha256,
            result_size=size,
            result_snippet=snippet,
            result_body=body,
        )

    def _extract_sec_filing(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """One source per 8-K filing, else the top-level 10-K/10-Q source_url."""
        artifact = getattr(result, "artifact", None)
        if not isinstance(artifact, dict):
            return
        tool_call_id = request.tool_call.get("id")
        timestamp = _now_iso()
        symbol = artifact.get("symbol")
        filings = artifact.get("filings")

        if isinstance(filings, list) and filings:
            for filing in filings:
                if not isinstance(filing, dict):
                    continue
                url = filing.get("source_url")
                if not url:
                    continue
                sha256, size, snippet, body = fingerprint_result_with_body(filing)
                yield ProvenanceSource(
                    record_id=_new_id(),
                    source_type="sec_filing",
                    identifier=url,
                    timestamp=timestamp,
                    title=symbol,
                    provider="edgar",
                    tool_call_id=tool_call_id,
                    args=redact_args(request.tool_call.get("args")),
                    result_sha256=sha256,
                    result_size=size,
                    result_snippet=snippet,
                    result_body=body,
                )
            return

        source_url = artifact.get("source_url")
        if not source_url:
            return
        sha256, size, snippet, body = fingerprint_result_with_body(artifact)
        yield ProvenanceSource(
            record_id=_new_id(),
            source_type="sec_filing",
            identifier=source_url,
            timestamp=timestamp,
            title=symbol,
            provider="edgar",
            tool_call_id=tool_call_id,
            args=redact_args(request.tool_call.get("args")),
            result_sha256=sha256,
            result_size=size,
            result_snippet=snippet,
            result_body=body,
        )

    def _extract_market_data(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """One source per symbol; identifier from symbol/underlying/indices arg."""
        args = request.tool_call.get("args") or {}
        tool_name = request.tool_call.get("name")
        tool_call_id = request.tool_call.get("id")
        timestamp = _now_iso()

        identifiers: list[str] = []
        for key in _MARKET_SYMBOL_ARGS:
            value = args.get(key)
            if isinstance(value, str) and value:
                identifiers.append(value)
        indices = args.get("indices")
        if isinstance(indices, list):
            identifiers.extend(str(i) for i in indices if i)
        elif isinstance(indices, str) and indices:
            identifiers.append(indices)
        symbols = args.get("symbols")
        if isinstance(symbols, list):
            identifiers.extend(str(s).upper() for s in symbols if s)

        # No symbol arg (e.g. get_market_overview with no explicit indices,
        # screen_stocks, get_market_movers) — record one source keyed by the
        # tool name.
        if not identifiers:
            identifiers = [tool_name]

        # Only tag a data-kind when we have real ticker(s); for symbol-less tools
        # identifiers is just [tool_name], so a kind label would duplicate it.
        # Guarding on identifiers (not args truthiness) stays correct even if a
        # symbol-less tool is later added to _MARKET_DATA_KINDS.
        detail = (
            _MARKET_DATA_KINDS.get(tool_name)
            if identifiers != [tool_name]
            else None
        )

        artifact = getattr(result, "artifact", None)
        fingerprint_target = (
            artifact if artifact is not None else getattr(result, "content", result)
        )
        sha256, size, snippet, body = fingerprint_result_with_body(fingerprint_target)
        for identifier in identifiers:
            yield ProvenanceSource(
                record_id=_new_id(),
                source_type="market_data",
                identifier=identifier,
                timestamp=timestamp,
                detail=detail,
                provider="market_data_proxy",
                tool_call_id=tool_call_id,
                args=redact_args(args),
                result_sha256=sha256,
                result_size=size,
                result_snippet=snippet,
                result_body=body,
            )

    def _extract_file_read(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """Read/Grep — classify source_type by path prefix.

        Skips agent-infrastructure paths (skill docs, generated tool/MCP
        wrappers, system files): provenance tracks external data the analysis
        rests on, not the scaffolding the agent reads to operate.
        """
        args = request.tool_call.get("args") or {}
        path = args.get("file_path") or args.get("path")
        if not path:
            return
        if _is_agent_infra_path(path):
            return
        content = getattr(result, "content", result)
        sha256, size, snippet = fingerprint_result(content)
        yield ProvenanceSource(
            record_id=_new_id(),
            source_type=_classify_file_source_type(path),
            identifier=path,
            timestamp=_now_iso(),
            tool_call_id=request.tool_call.get("id"),
            args=redact_args(args),
            result_sha256=sha256,
            result_size=size,
            result_snippet=snippet,
        )

    def _extract_execute_code(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """One mcp_tool source per entry in the result artifact's ``mcp_trace``.

        Shared by ExecuteCode and Bash — both surface an ``mcp_trace`` artifact
        for the MCP calls their in-sandbox code made. Contract (Phase B2):
        artifact is a dict with key ``mcp_trace``, a list of ``{server, tool,
        args, result_sha256, result_size, result_snippet, timestamp}``. Yields
        nothing when the artifact/trace is absent.
        """
        artifact = getattr(result, "artifact", None)
        trace = artifact.get("mcp_trace") if isinstance(artifact, dict) else []
        if not isinstance(trace, list):
            return
        tool_call_id = request.tool_call.get("id")
        # Cap entries per execution; the trace is agent-authored and unbounded.
        for entry in trace[:_MAX_TRACE_ENTRIES]:
            if not isinstance(entry, dict):
                continue
            server = entry.get("server")
            tool = entry.get("tool")
            if not server or not tool:
                continue
            # Clamp the agent-controlled identifier parts and result fields so a
            # poisoned trace can't bloat the identifier/snippet/sha columns.
            server = str(server)[:_IDENT_PART_MAX_CHARS]
            tool = str(tool)[:_IDENT_PART_MAX_CHARS]
            # Clamp the agent-controlled timestamp like the other trace fields:
            # a non-string or oversized value would otherwise ride the SSE event.
            raw_ts = entry.get("timestamp")
            timestamp = (
                raw_ts[:_TIMESTAMP_MAX_CHARS]
                if isinstance(raw_ts, str) and raw_ts
                else _now_iso()
            )
            yield ProvenanceSource(
                record_id=_new_id(),
                source_type="mcp_tool",
                identifier=f"{server}:{tool}",
                timestamp=timestamp,
                provider=f"mcp:{server}",
                tool_call_id=tool_call_id,
                # Keep the readable redacted args alongside the legacy hash; the
                # deny-list redactor strips any secrets/PII before they persist.
                args_fingerprint=hash_args(entry.get("args")),
                args=redact_args(entry.get("args")),
                result_sha256=_truncate(entry.get("result_sha256"), _SHA_MAX_CHARS),
                result_size=_coerce_result_size(entry.get("result_size")),
                result_snippet=_truncate(
                    entry.get("result_snippet"), SNIPPET_MAX_CHARS
                ),
                # The per-call body the sandbox captured for THIS MCP call (the
                # root fix: NOT the aggregate ExecuteCode stdout). Re-clamped
                # host-side as the trace is agent-authored. May be absent once
                # the sandbox's per-execution body budget is exhausted.
                result_body=_clamp_body_bytes(
                    entry.get("result_body"), RESULT_BODY_MAX_BYTES
                ),
            )
