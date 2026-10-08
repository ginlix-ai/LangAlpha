"""Market watch middleware: injects live prices for watched tickers.

One `<market-watch>` block is contributed per model request as a row under
``REQUEST_CALL_UPDATES`` on the request state (``awrap_model_call`` +
``request.override``), and the tail envelope renders it. That key is
request-only and never a state channel, so the stamp reaches the provider but
never enters durable state and checkpoints, replay, and history stay clean. An in-memory throttle
re-injects the cached block between refreshes so the model keeps a price view
without a provider fetch every call, and a skip-when-already-quoted guard keeps
the fetch off the hot path; any failure degrades to contributing nothing so the
turn is never broken.

This middleware must sit OUTSIDE ``TailEnvelopeMiddleware``, which reads the
request key it writes, and it owns no cache breakpoint of its own: the envelope
carries the whole volatile tail and pins the breakpoint there.

Every injection is also attested in the turn's provenance stream (one
``market_data`` record per watched symbol). Provenance rides sse_events
persistence, so it records what the model saw even though the stamp itself is
never checkpointed.
"""

import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.config import get_config

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse

from ptc_agent.agent.middleware.provenance.body_store import (
    drain_body_writes,
    schedule_body_write,
    store_body,
)
from ptc_agent.agent.middleware.runtime_context import (
    REQUEST_CALL_UPDATES,
    RUNTIME_UPDATE_SOURCE,
    DurableUpdate,
)
from ptc_agent.agent.provenance.types import (
    ProvenanceSource,
    build_provenance_event,
    fingerprint_result_with_body,
)
from src.config.settings import get_market_watch_min_interval
from src.data_client.instrument_clock import clock_for_ref
from src.data_client.registry import get_market_data_provider
from market_protocol import AssetClass, MarketPhase
from src.tools.market_data.display import resolve_ref
from src.tools.market_data.quote_format import current_price, format_quote_block
from src.utils.market_watch import get_watchlist

logger = logging.getLogger(__name__)

_STAMP_OPEN = "<market-watch>"
_STAMP_CLOSE = "</market-watch>"
_STAMP_NOTE = "Automated live-price feed (not a user message)."
# Bump when the block's shape changes: a row outlives the code that wrote it.
_STAMP_SCHEMA_VERSION = 1

# Phases in which a watched price can move. A lunch break holds the morning
# close and an index's extended hours hold the regular close, so a stamp then
# would only repeat a price the venue is not changing.
_MOVING_PHASES = (MarketPhase.PRE, MarketPhase.REGULAR, MarketPhase.POST)

# Direct tools whose call already puts a fresh price for the symbol in front of
# the model; when the current batch has one for a watched ticker we skip the
# redundant injection. A stale/renamed entry only loses this optimization.
_QUOTE_TOOL_NAMES = {"get_quote", "get_company_overview"}


def _configurable() -> dict:
    """The current run's configurable dict; empty outside a runnable context."""
    try:
        return get_config().get("configurable") or {}
    except Exception:
        return {}


def _any_venue_open(symbols: list[str]) -> bool:
    """True if any watched symbol's price can be moving now.

    Each symbol is read on its own instrument clock, so a watchlist of only
    ``0700.HK`` stamps during Hong Kong hours even while US markets are shut,
    and goes quiet over its lunch break. An unresolvable symbol counts as open
    (fail-open → proceed to fetch); a clock lookup that raises propagates to
    the injection guard, which degrades to injecting nothing. Symbols read as
    the stocks they are fetched as, so a watched COMP is the company.
    """
    now = datetime.now(timezone.utc)
    for sym in symbols:
        ref = resolve_ref(sym, AssetClass.EQUITY)
        if ref is None:
            return True
        if clock_for_ref(ref).phase(now) in _MOVING_PHASES:
            return True
    return False


def _trailing_tool_batch(messages: list[Any]) -> list[ToolMessage]:
    """The run of consecutive ToolMessages at the tail (the just-completed batch)."""
    batch: list[ToolMessage] = []
    for msg in reversed(messages):
        if isinstance(msg, ToolMessage):
            batch.append(msg)
        else:
            break
    batch.reverse()
    return batch


def _tool_call_symbols(args: Any) -> set[str]:
    """Uppercased tickers referenced by a quote tool_call's args (`symbols`/`symbol`)."""
    out: set[str] = set()
    if not isinstance(args, dict):
        return out
    listed = args.get("symbols")
    if isinstance(listed, list):
        out |= {s.upper() for s in listed if isinstance(s, str)}
    single = args.get("symbol")
    if isinstance(single, str):
        out.add(single.upper())
    return out


def _batch_quotes_watched(
    messages: list[Any], batch: list[ToolMessage], symbols: list[str]
) -> bool:
    """True if the AIMessage that made the batch quoted EVERY watched symbol.

    Partial overlap must not suppress the stamp: a direct quote for one
    watched symbol would otherwise hide fresh prices for the rest of the list
    for that model call.
    """
    idx = len(messages) - len(batch) - 1
    if idx < 0:
        return False
    tool_calls = getattr(messages[idx], "tool_calls", None) or []
    watched = {s.upper() for s in symbols}
    quoted: set[str] = set()
    for tc in tool_calls:
        name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", None)
        if name not in _QUOTE_TOOL_NAMES:
            continue
        args = tc.get("args") if isinstance(tc, dict) else getattr(tc, "args", None)
        quoted |= _tool_call_symbols(args)
    return bool(watched) and watched <= quoted


class MarketWatchMiddleware(AgentMiddleware):
    """Injects live prices for watched tickers; no-op when the watch list is empty.

    Contributes one `<market-watch>` row per model call via
    ``request.override``; nothing is persisted. Any failure (Redis, provider,
    formatting) degrades to contributing nothing; it must never break the turn.
    """

    def __init__(self, min_interval_seconds: int | None = None) -> None:
        self._min_interval = (
            min_interval_seconds
            if min_interval_seconds is not None
            else get_market_watch_min_interval()
        )
        # awrap_model_call runs once per model call, sequentially — no lock needed.
        self._last_injected_at: float | None = None
        self._last_prices: dict[str, float] = {}
        self._last_block: str | None = None
        # Watchlist the cached block was fetched for (uppercased): a throttled
        # round only replays the block when the CURRENT list still matches it —
        # a mid-window mutation refetches for the new list instead.
        self._last_symbols: set[str] = set()
        # Provenance for the last fetched block: (symbol, provider) per line
        # plus the fetch time — replays attest the original access, not the
        # replay clock.
        self._last_sources: list[tuple[str, str | None]] = []
        self._last_fetch_ts: str | None = None
        self._last_body_sha: str | None = None
        self._body_tasks: set = set()

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        stamp = await self._stamp_for(request.messages, request.runtime)
        if stamp is None:
            return await handler(request)
        # Request-scoped state, not a state update: the row is handed to the
        # tail envelope for this one call and is never written back, which is
        # what keeps the stamp out of checkpoints, replay, and history.
        state = dict(getattr(request, "state", None) or {})
        update = DurableUpdate(
            kind="market_watch",
            schema_version=_STAMP_SCHEMA_VERSION,
            text=stamp,
            provenance={
                "source": RUNTIME_UPDATE_SOURCE,
                "writer": "market_watch",
                "symbols": sorted(self._last_symbols),
                "fetched_at": self._last_fetch_ts,
            },
        )
        state[REQUEST_CALL_UPDATES] = [
            *(state.get(REQUEST_CALL_UPDATES) or []),
            update.to_dict(),
        ]
        return await handler(request.override(state=state))

    async def _stamp_for(
        self, messages: list[Any] | None, runtime: Any
    ) -> str | None:
        """The `<market-watch>` block to inject for this call, or None to skip.

        The tail-shape guard runs first (no I/O), then the watchlist read (one
        Redis GET). Past that, the venue gate and throttle are re-evaluated on the
        CURRENT list every call: a venue close stops the replay and a mid-window
        watchlist mutation refetches — only an unchanged list is throttled away.
        """
        messages = messages or []
        if not messages:
            return None
        tail = messages[-1]
        if not isinstance(tail, (HumanMessage, ToolMessage)):
            return None

        configurable = _configurable()
        thread_id = configurable.get("thread_id")
        if not thread_id:
            return None
        try:
            symbols = await get_watchlist(thread_id)
        except Exception:
            logger.warning("[MarketWatch] watchlist read failed", exc_info=True)
            return None
        if not symbols:
            return None

        # Outer net past the cheap guards: any failure (venue lookup, provider,
        # formatting) degrades to injecting nothing — it must never break the
        # turn. Inner `return None`s below are normal control flow.
        try:
            # Venue gate: stamp when ANY watched symbol's exchange is open. Pure
            # CPU (symbol resolve + clock phase). Above the throttle so a
            # mid-window venue close stops the cached replay.
            if not _any_venue_open(symbols):
                return None

            # Skip-if-already-quoted: the model just fetched a spot price for a
            # watched symbol, so don't fetch/inject a second one.
            batch = _trailing_tool_batch(messages) if isinstance(tail, ToolMessage) else []
            if batch and _batch_quotes_watched(messages, batch, symbols):
                return None

            # Throttle (no provider I/O): within the window re-inject the cached
            # block so the model keeps a price view between refreshes — but only
            # while the watchlist is unchanged. A mid-window mutation falls
            # through to a fresh fetch: replaying would resurface removed
            # symbols, and going silent would blind the model to a just-watched
            # one for the rest of the window. Venue re-checked above.
            watched = {s.upper() for s in symbols}
            if (
                self._last_injected_at is not None
                and time.monotonic() - self._last_injected_at < self._min_interval
                and self._last_block
                and watched == self._last_symbols
            ):
                stamp = self._wrap(self._last_block)
                await self._emit_provenance(runtime, stamp)
                return stamp

            block = await self._fetch_block(symbols, configurable.get("user_id"))
            if block is None:
                return None
            self._last_block = block
            self._last_symbols = watched
            try:
                runtime.stream_writer(
                    {
                        "type": "market_watch_update",
                        # The authoritative watch list, not the priced subset —
                        # a provider timeout on one symbol must not make the
                        # chip claim the others are unwatched.
                        "symbols": sorted(watched),
                        "content": block,
                        "timestamp": time.time(),
                    }
                )
            except Exception:
                logger.debug("[MarketWatch] stream_writer unavailable, skipping SSE event")
            stamp = self._wrap(block)
            await self._emit_provenance(runtime, stamp)
            return stamp
        except Exception:
            logger.warning("[MarketWatch] injection failed", exc_info=True)
            return None

    async def _fetch_block(self, symbols: list[str], user_id: Any) -> str | None:
        """Fetch + format a quote block; None on failure/empty. Updates throttle state."""
        try:
            provider = get_market_data_provider()
            if inspect.isawaitable(provider):
                provider = await provider
            snaps = await provider.get_snapshots(
                symbols, asset_type="stocks", user_id=user_id
            )
            if not snaps:
                return None
            block = format_quote_block(
                snaps, prev_prices=self._last_prices, asset_class=AssetClass.EQUITY
            )
        except Exception:
            logger.warning("[MarketWatch] snapshot fetch failed", exc_info=True)
            return None
        self._last_injected_at = time.monotonic()
        self._last_prices = {
            s["symbol"]: current_price(s)
            for s in snaps
            if s.get("symbol") and current_price(s) is not None
        }
        # Every snap renders a line in the block, so every snap is provenance.
        self._last_sources = [
            (s["symbol"], s.get("source")) for s in snaps if s.get("symbol")
        ]
        self._last_fetch_ts = datetime.now(timezone.utc).isoformat()
        return block

    async def _emit_provenance(self, runtime: Any, stamp: str) -> None:
        """Attest the injected stamp in the turn's provenance stream.

        Fires on every injection — fresh fetch or throttled replay — so a turn's
        provenance records exactly what the model saw, independent of the stamp
        never being checkpointed. Persist-side dedup collapses same-content
        replays within a turn; never raises.
        """
        if not self._last_sources:
            return
        try:
            sha256, size, snippet, body = fingerprint_result_with_body(stamp)
            timestamp = self._last_fetch_ts or datetime.now(timezone.utc).isoformat()
            for symbol, provider_name in self._last_sources:
                runtime.stream_writer(
                    build_provenance_event(
                        ProvenanceSource(
                            record_id=str(uuid4()),
                            source_type="market_data",
                            identifier=symbol,
                            timestamp=timestamp,
                            detail="market_watch",
                            provider=provider_name or "market_data_proxy",
                            result_sha256=sha256,
                            result_size=size,
                            result_snippet=snippet,
                        )
                    )
                )
            await self._store_body_once(sha256, body)
        except Exception:
            logger.debug("[MarketWatch] provenance emit failed", exc_info=True)

    async def _store_body_once(self, sha256: str, body: str) -> None:
        """Persist the stamp body to the content-addressed store, once per block.

        Deliberately unredacted: the stamp is deterministic live-price text with
        no user data, so it needs none of ProvenanceMiddleware's redaction.
        Scheduled off the model-call critical path; replays reference the
        already-stored body through the sha.
        """
        if sha256 == self._last_body_sha:
            return
        self._last_body_sha = sha256
        await schedule_body_write(
            self._body_tasks,
            store_body(
                sha256, body, len(body.encode("utf-8")), "text/plain; charset=utf-8"
            ),
            name="market_watch_body",
        )

    async def aafter_agent(self, state: Any, runtime: Any) -> dict | None:
        """Drain background body writes at agent end (best-effort quiescence)."""
        await drain_body_writes(self._body_tasks)
        return None

    @staticmethod
    def _wrap(block: str) -> str:
        return f"{_STAMP_OPEN}\n{_STAMP_NOTE}\n{block}\n{_STAMP_CLOSE}"
