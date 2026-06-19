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

import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langgraph.config import get_stream_writer

from ptc_agent.agent.provenance import (
    SNIPPET_MAX_CHARS,
    ProvenanceSource,
    build_provenance_event,
    fingerprint_result,
    hash_args,
    redact_args,
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
# the frontend; symbol-less tools (screen_stocks, get_market_movers,
# get_sector_performance) already surface their tool name as the identifier.
_MARKET_DATA_KINDS = {
    "get_company_overview": "company_overview",
    "get_stock_daily_prices": "daily_prices",
    "get_options_chain": "options_chain",
    "get_market_indices": "market_index",
}

# Market-data tools that all share _extract_market_data. Superset of
# _MARKET_DATA_KINDS — also covers the symbol-less ones (screen/movers/sector).
_MARKET_DATA_TOOLS = (
    "get_stock_daily_prices",
    "get_company_overview",
    "get_market_indices",
    "get_sector_performance",
    "screen_stocks",
    "get_options_chain",
    "get_market_movers",
)

# Host-side bounds on the in-sandbox MCP trace (LLM-authored, untrusted): the
# generated client is supposed to self-limit, but it's code the agent controls,
# so re-clamp here so a poisoned/huge trace can't amplify into DB/SSE/render.
# SNIPPET_MAX_CHARS is imported from provenance.types (the canonical cap shared
# with the in-sandbox client) so host- and sandbox-side truncation stay equal.
_SHA_MAX_CHARS = 128
_IDENT_PART_MAX_CHARS = 256
_MAX_TRACE_ENTRIES = 200

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


def _strip_mcp_trace(result: Any) -> None:
    """Drop the provenance-only mcp_trace key from an ExecuteCode artifact.

    Mutates the ToolMessage artifact in place so the raw in-sandbox trace never
    propagates outward onto tool_call_result or into persisted sse_events.
    """
    artifact = getattr(result, "artifact", None)
    if isinstance(artifact, dict):
        artifact.pop("mcp_trace", None)


def _truncate(value: Any, limit: int) -> Any:
    """Clamp a string field to ``limit`` chars; pass non-strings through."""
    return value[:limit] if isinstance(value, str) else value


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

            # Don't attest a source the tool never actually returned. ExecuteCode
            # is exempt: its code may "error" while individual in-sandbox MCP
            # calls succeeded, and those trace entries are guarded per-entry.
            if tool_name != "ExecuteCode" and _is_error_result(result):
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
                if tool_name == "ExecuteCode":
                    _strip_mcp_trace(result)

            if not sources:
                return result

            writer = get_stream_writer()
            if writer is None:
                return result

            for source in sources:
                source.result_snippet = self._redact_snippet(source.result_snippet)
                try:
                    writer(build_provenance_event(source))
                except Exception:
                    logger.debug(
                        "[PROVENANCE] failed to emit event for %s",
                        tool_name,
                        exc_info=True,
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
            sha256, size, snippet = fingerprint_result(item)
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
            )

    def _extract_web_fetch(
        self, request: Any, result: Any
    ) -> Iterator[ProvenanceSource]:
        """Identifier comes from args["url"] — the result is a bare string."""
        url = (request.tool_call.get("args") or {}).get("url")
        if not url:
            return
        content = getattr(result, "content", result)
        sha256, size, snippet = fingerprint_result(content)
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
                sha256, size, snippet = fingerprint_result(filing)
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
                )
            return

        source_url = artifact.get("source_url")
        if not source_url:
            return
        sha256, size, snippet = fingerprint_result(artifact)
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

        # No symbol arg (e.g. get_sector_performance, screen_stocks,
        # get_market_movers) — record one source keyed by the tool name.
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
        sha256, size, snippet = fingerprint_result(fingerprint_target)
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

        Contract (Phase B2): artifact is a dict with key ``mcp_trace``, a list
        of ``{server, tool, args, result_sha256, result_size, result_snippet,
        timestamp}``. Yields nothing when the artifact/trace is absent.
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
            yield ProvenanceSource(
                record_id=_new_id(),
                source_type="mcp_tool",
                identifier=f"{server}:{tool}",
                timestamp=entry.get("timestamp") or _now_iso(),
                provider=f"mcp:{server}",
                tool_call_id=tool_call_id,
                # Keep the readable redacted args alongside the legacy hash; the
                # deny-list redactor strips any secrets/PII before they persist.
                args_fingerprint=hash_args(entry.get("args")),
                args=redact_args(entry.get("args")),
                result_sha256=_truncate(entry.get("result_sha256"), _SHA_MAX_CHARS),
                result_size=entry.get("result_size"),
                result_snippet=_truncate(
                    entry.get("result_snippet"), SNIPPET_MAX_CHARS
                ),
            )
