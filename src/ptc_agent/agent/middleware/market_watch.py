"""Market watch middleware: stamps live prices for watched tickers.

One <market-watch> block is injected before each model call (`abefore_model`)
onto a single best carrier — the incoming human message, or, after a tool round,
the best message in the just-completed batch (a TodoWrite result, else the
shortest non-error result, else the tail). An in-memory throttle pre-check and a
skip-when-already-quoted guard keep the provider fetch off the hot path; any
failure degrades to injecting nothing so the turn is never broken.
"""

import inspect
import logging
import time
from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime

from langchain.agents.middleware.types import AgentMiddleware, AgentState

from src.config.settings import get_market_watch_min_interval
from src.data_client.registry import get_market_data_provider
from src.market_protocol import MarketPhase
from src.market_protocol.calendars import get_calendar
from src.tools.market_data.display import resolve_ref
from src.tools.market_data.quote_format import current_price, format_quote_block
from src.utils.market_watch import get_watchlist

logger = logging.getLogger(__name__)

_STAMP_OPEN = "<market-watch>"

# Direct tools whose call already puts a fresh price for the symbol in front of
# the model; when the current batch has one for a watched ticker we skip the
# redundant injection. A stale/renamed entry only loses this optimization.
_QUOTE_TOOL_NAMES = {"get_quote", "get_company_overview"}


def _any_venue_open(symbols: list[str]) -> bool:
    """True if any watched symbol's listing venue is not CLOSED.

    Each symbol is priced against its own exchange calendar, so a watchlist of
    only ``0700.HK`` stamps during Hong Kong hours even while US markets are
    shut. An unresolvable symbol counts as open (fail-open → proceed to fetch);
    a calendar lookup that raises propagates to ``abefore_model``'s outer guard,
    which degrades to injecting nothing.
    """
    now = datetime.now(timezone.utc)
    for sym in symbols:
        ref = resolve_ref(sym)
        if ref is None:
            return True
        if get_calendar(ref.calendar_id).phase_at(now) != MarketPhase.CLOSED:
            return True
    return False


def _content_text(content: Any) -> str:
    """Flatten str-or-list message content to text for a substring check."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            p.get("text", "") for p in content if isinstance(p, dict)
        )
    return ""


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
    """True if the AIMessage that made the batch quoted a watched symbol directly."""
    idx = len(messages) - len(batch) - 1
    if idx < 0:
        return False
    tool_calls = getattr(messages[idx], "tool_calls", None) or []
    watched = {s.upper() for s in symbols}
    for tc in tool_calls:
        name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", None)
        if name not in _QUOTE_TOOL_NAMES:
            continue
        args = tc.get("args") if isinstance(tc, dict) else getattr(tc, "args", None)
        if _tool_call_symbols(args) & watched:
            return True
    return False


def _select_carrier(batch: list[ToolMessage]) -> ToolMessage | None:
    """Best carrier in a tool batch: a TodoWrite result, else the shortest non-error
    result. None when no string-content candidate exists (caller falls back to the tail).
    """
    candidates = [
        (i, m)
        for i, m in enumerate(batch)
        if isinstance(m.content, str) and m.status != "error"
    ]
    if not candidates:
        return None
    for _, m in candidates:
        if m.name == "TodoWrite":  # neutral dead space; card reads args, not content
            return m
    return min(candidates, key=lambda im: (len(im[1].content), im[0]))[1]


class MarketWatchMiddleware(AgentMiddleware):
    """Stamps live prices for watched tickers; no-op when the watch list is empty.

    One <market-watch> block per model call is appended to the best carrier in
    the current tool batch (or the incoming human message). Any failure (Redis,
    provider, formatting) degrades to injecting nothing — it must never break
    the turn.
    """

    def __init__(
        self,
        min_interval_seconds: int | None = None,
    ) -> None:
        self._min_interval = (
            min_interval_seconds
            if min_interval_seconds is not None
            else get_market_watch_min_interval()
        )
        # abefore_model is the single call site and runs sequentially before each
        # model call, so these fields need no lock (the parallel-caller hazard only
        # existed for the removed per-tool-result awrap_tool_call path).
        self._last_injected_at: float | None = None
        self._last_prices: dict[str, float] = {}

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
            block = format_quote_block(snaps, prev_prices=self._last_prices)
        except Exception:
            logger.warning("[MarketWatch] snapshot fetch failed", exc_info=True)
            return None
        self._last_injected_at = time.monotonic()
        self._last_prices = {
            s["symbol"]: current_price(s)
            for s in snaps
            if s.get("symbol") and current_price(s) is not None
        }
        return block

    async def abefore_model(
        self, state: AgentState, runtime: Runtime, *, config: RunnableConfig
    ) -> dict[str, Any] | None:
        """Stamp one <market-watch> block onto the best carrier before a model call.

        Cheap in-memory guards run first (tail shape, throttle) so no-watch and
        throttled rounds pay no Redis/provider I/O; only past those does it read
        the watch list and, at most once per interval, fetch quotes.
        """
        messages = state.get("messages") or []
        if not messages:
            return None
        tail = messages[-1]
        if not isinstance(tail, (HumanMessage, ToolMessage)):
            return None

        # Throttle pre-check (no I/O): within the window the prior stamp is still
        # in context, so skip the Redis read AND the provider fetch this round.
        if (
            self._last_injected_at is not None
            and time.monotonic() - self._last_injected_at < self._min_interval
        ):
            return None

        configurable = config.get("configurable") or {}
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

        # Outer net past the cheap in-memory guards: any failure (session lookup,
        # provider, formatting, stamping) degrades to injecting nothing — it must
        # never break the turn. Inner `return None`s below are normal control flow.
        try:
            # Venue gate: stamp when ANY watched symbol's exchange is open. Pure
            # CPU (symbol resolve + calendar phase), so it stays on the hot path
            # after the throttle/watchlist guards.
            if not _any_venue_open(symbols):
                return None

            batch = _trailing_tool_batch(messages) if isinstance(tail, ToolMessage) else []
            # Skip-if-already-quoted: the model just fetched a spot price for a
            # watched symbol, so don't fetch/inject a second one.
            if batch and _batch_quotes_watched(messages, batch, symbols):
                return None

            block = await self._fetch_block(symbols, configurable.get("user_id"))
            if block is None:
                return None

            carrier = tail if isinstance(tail, HumanMessage) else (_select_carrier(batch) or tail)
            # Idempotency: a tool-result tail recurs across model calls (unlike the
            # one-shot human tail), so never double-stamp a carrier on re-entry/retry.
            content = carrier.content
            if _STAMP_OPEN in _content_text(content):
                return None

            stamp = f"{_STAMP_OPEN}\n{block}\n</market-watch>"
            if isinstance(content, list):
                new_content = content + [{"type": "text", "text": stamp}]
            else:
                new_content = f"{content}\n\n{stamp}"
            try:
                runtime.stream_writer(
                    {
                        "type": "market_watch_update",
                        "symbols": sorted(self._last_prices),
                        "content": block,
                        "timestamp": time.time(),
                    }
                )
            except Exception:
                logger.debug("[MarketWatch] stream_writer unavailable, skipping SSE event")
            return {"messages": [carrier.model_copy(update={"content": new_content})]}
        except Exception:
            logger.warning("[MarketWatch] injection failed", exc_info=True)
            return None
