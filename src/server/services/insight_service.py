"""InsightService — Schedule-based market insights from a research agent.

Schedule (all times Eastern / America/New_York):
  - 9:00 AM  pre_market    — Overnight + pre-market news
  - 10–20    market_update  — Hourly news summaries (weekdays only)
  - 8:30 PM  post_market   — End-of-day recap

Weekends: pre_market and post_market only — no hourly updates.
"""

import asyncio
import contextvars
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from src.config.settings import get_config, get_langsmith_metadata
from src.server.database import market_insight as insight_db

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

# Staleness windows: skip a job if a matching insight already exists within this window
STALENESS_WINDOWS = {
    "pre_market": timedelta(hours=4),
    "market_update": timedelta(minutes=30),
    "post_market": timedelta(hours=4),
}

# Max symbols to include in personalized prompt context
DEFAULT_MAX_SYMBOLS = 20
DEFAULT_GENERATION_TIMEOUT = 600
DEFAULT_DEDUP_WINDOW_MINUTES = 5

# A generating row older than the generation timeout plus this is abandoned.
# The timeout bounds research and extraction; this covers the writes around it.
IN_FLIGHT_SLACK = timedelta(minutes=1)
# How often a worker that lost the claim to a run in flight claims again.
HOLDER_POLL_SECONDS = 15


_TAIL = (
    "Only include genuinely noteworthy stories. "
    "Report facts, not predictions or recommendations. US market focus."
)

_JSON_GUIDELINES = """
You MUST respond with ONLY a valid JSON object (no markdown, no explanation, no preamble).
The JSON must have exactly these fields:
{
  "headline": "Concise headline capturing the dominant market theme (max 120 chars)",
  "summary": "2-3 sentence overview of the most important developments",
  "news_items": [
    {"title": "Short factual headline", "body": "2-4 sentence factual summary", "url": "source URL or null"}
  ],
  "topics": [
    {"text": "Topic name (1-2 words)", "trend": "up|down|neutral"}
  ]
}
Include 4-8 news_items and 3-5 topics. Respond with ONLY the JSON object.
"""


def _build_instruction(insight_type: str, now_et: datetime) -> str:
    """Build the research instruction for the given job type."""
    time_str = now_et.strftime("%A, %B %-d, %Y %-I:%M %p ET")
    today_date = now_et.strftime("%A, %B %-d, %Y")

    if insight_type == "pre_market":
        yesterday = now_et - timedelta(days=1)
        yesterday_date = yesterday.strftime("%A, %B %-d, %Y")
        return (
            f"Current time: {time_str}\n\n"
            f"Curate the most significant US financial market news "
            f"from last night ({yesterday_date} ~8 PM ET) through this morning. "
            f"For each story, provide a short headline and a 2-4 sentence "
            f"factual summary of what happened. {_TAIL}"
        )

    if insight_type == "market_update":
        window_end = now_et.strftime("%-I:%M %p")
        window_start = (now_et - timedelta(hours=1)).strftime("%-I:%M %p")
        return (
            f"Current time: {time_str}\n\n"
            f"Curate the most significant US financial market news from the "
            f"past hour ({window_start} – {window_end} ET). "
            f"For each story, provide a short headline and a 2-4 sentence "
            f"factual summary of what happened. {_TAIL}"
        )

    # post_market
    return (
        f"Current time: {time_str}\n\n"
        f"Curate the most significant US financial market news from today "
        f"({today_date}) for an end-of-day recap. "
        f"For each story, provide a short headline and a 2-4 sentence "
        f"factual summary of what happened. {_TAIL}"
    )


def _build_personalized_instruction(
    symbols_context: str, now_et: datetime
) -> str:
    """Build a personalized insight prompt with watchlist/portfolio context."""
    time_str = now_et.strftime("%A, %B %-d, %Y %-I:%M %p ET")
    return (
        f"Current time: {time_str}\n\n"
        f"Generate a personalized market brief focused on the user's portfolio "
        f"and watchlist. Cover the most significant recent news, price movements, "
        f"and developments for these holdings:\n\n{symbols_context}\n\n"
        f"For each relevant story, provide a short headline and a 2-4 sentence "
        f"factual summary. Prioritize stories that directly affect the listed "
        f"symbols. {_TAIL}"
    )


def _extract_json_string(text: str) -> str | None:
    """Extract a JSON object string from text that may contain reasoning.

    Tries in order:
    1. Entire text as JSON (clean model output)
    2. ```json ... ``` fenced block (model wraps JSON in markdown)
    3. Outermost { ... } pair (JSON embedded in reasoning text)
    """
    stripped = text.strip()

    # 1. Entire text is JSON
    if stripped.startswith("{"):
        return stripped

    # 2. Fenced JSON block (search anywhere, not just whole-string)
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\s*\n?```", stripped, re.DOTALL)
    if fence_match:
        block = fence_match.group(1).strip()
        start = block.find("{")
        end = block.rfind("}")
        if start != -1 and end > start:
            return block[start : end + 1]

    # 3. Outermost braces
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end > start:
        return stripped[start : end + 1]

    return None


def _extract_structured_output(raw_text: str) -> dict:
    """Extract structured insight output from agent response text.

    The agent is prompted to include JSON in its response. This function
    finds and parses that JSON, validating against InsightOutputSchema.
    """
    from src.llms.content_utils import extract_json_from_content
    from src.server.models.market_insight import InsightOutputSchema

    # Strip reasoning/thinking content blocks (for structured content)
    cleaned = extract_json_from_content(raw_text)
    if not isinstance(cleaned, str) or not cleaned.strip():
        raise ValueError("No text content after stripping reasoning blocks")

    # Find and parse JSON from the text
    json_str = _extract_json_string(cleaned)
    if json_str:
        try:
            data = json.loads(json_str)
            result = InsightOutputSchema(**data)
            return result.model_dump()
        except (json.JSONDecodeError, Exception):
            pass

    raise ValueError(
        "Direct JSON parsing failed — caller should invoke LLM fallback"
    )


async def _llm_extract_fallback(raw_text: str, user_id: str | None) -> dict:
    """Extract structured insight data via the canonical one-shot LLM wrapper.

    Routes through ``LLMService.complete`` so BYOK / OAuth credentials are
    respected (or the platform default is used when ``user_id`` is None).
    ``user_id`` is required so callers must be explicit about the platform path.
    """
    from src.server.models.market_insight import InsightOutputSchema
    from src.server.services.llm.service import LLMService
    from src.server.app import setup

    svc = LLMService(agent_config=setup.agent_config, logger=logger)
    result = await svc.complete(
        user_id=user_id,
        system_prompt=(
            "You are a structured data extractor. Extract the market insight data "
            "from the provided text into the required JSON schema. "
            "Include all news items and topics mentioned."
        ),
        user_prompt=raw_text,
        response_schema=InsightOutputSchema,
        slot="background",
    )
    return result.model_dump() if hasattr(result, "model_dump") else dict(result)


async def _research(
    prompt: str,
    user_id: str | None = None,
    *,
    insight_type: str,
    requested_by: str | None = None,
) -> str:
    """Run one research brief and return the last AI message's text.

    *user_id* resolves the model through the user's OAuth / BYOK chain, so a
    self-hosted deployment without a system key can still generate insights.
    *requested_by* is the person who asked for the brief, None for a scheduled
    one: a scheduled brief may run on a user's credentials without being theirs.
    """
    from langchain_core.messages import HumanMessage

    from ptc_agent.agent.insight_graph import build_insight_graph
    from src.observability.langsmith_tracer import with_run_tracer
    from src.server.app import setup
    from src.server.services.llm.config import apply_fetch_override

    agent_config = setup.agent_config
    if not agent_config:
        raise RuntimeError("Agent config not initialized")

    config = agent_config
    if user_id:
        try:
            from src.server.services.llm.config import resolve_llm_config

            config = await resolve_llm_config(
                config, user_id, request_model=None, is_byok=True, slot="background",
                user_facing=False,
            )
        except Exception as exc:
            logger.warning(f"[MARKET_INSIGHT] Could not resolve user LLM, falling back to system: {exc}")

    if config.llm is None:
        raise ValueError("No LLM configured — set a model in agent_config.yaml or select one in Settings")

    graph = build_insight_graph(config)
    input_state = {"messages": [HumanMessage(content=prompt)]}
    # WebFetch reads the user's fetch model from the context, as in a chat
    # turn. The run gets a context of its own because the scheduler runs one
    # brief after another in a single task, and a fetch client set for one
    # must not outlive it.
    run_context = contextvars.copy_context()
    run_context.run(apply_fetch_override, config)
    # A research run has no step cap, so it names a tracer that does not hold
    # its run tree, as a chat turn does. Tagged so its spend and failures can
    # be told apart from chat turns, which share the project.
    metadata = get_langsmith_metadata(user_id=requested_by, workflow_type="market_insight")
    metadata.update(origin="user" if requested_by else "system", insight_type=insight_type)
    run_config = with_run_tracer({"metadata": metadata, "tags": ["workflow:insight"]})
    result = await asyncio.create_task(
        graph.ainvoke(input_state, run_config), context=run_context,
    )

    # Extract text from the last AI message
    messages = result.get("messages", [])
    for msg in reversed(messages):
        if hasattr(msg, "type") and msg.type == "ai" and msg.content:
            content = msg.content
            if isinstance(content, list):
                text_parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text_parts.append(block["text"])
                    elif isinstance(block, str):
                        text_parts.append(block)
                return "\n".join(text_parts)
            return str(content)

    raise ValueError("Insight research returned no AI message")


class InsightService:
    """Singleton background service that generates market insights on a schedule."""

    _instance: Optional["InsightService"] = None

    @classmethod
    def get_instance(cls) -> "InsightService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        # Defaults (overridden by config in start())
        self._enabled = True
        self._tz = ET
        self._generation_timeout = DEFAULT_GENERATION_TIMEOUT
        self._max_symbols = DEFAULT_MAX_SYMBOLS
        self._dedup_window_minutes = DEFAULT_DEDUP_WINDOW_MINUTES
        # Schedule times (overridden by config)
        self._pre_market = datetime.strptime("04:00", "%H:%M").time()
        self._post_market = datetime.strptime("20:30", "%H:%M").time()
        self._update_start = datetime.strptime("10:00", "%H:%M").time()
        self._update_end = datetime.strptime("20:00", "%H:%M").time()
        self._update_interval_min = 60

    async def start(self) -> None:
        """Load config and start the schedule loop."""
        config = get_config("market_insight") or {}
        self._enabled = config.get("enabled", True)
        self._generation_timeout = config.get(
            "generation_timeout", DEFAULT_GENERATION_TIMEOUT
        )
        self._max_symbols = config.get("max_symbols_context", DEFAULT_MAX_SYMBOLS)
        self._dedup_window_minutes = config.get(
            "dedup_window_minutes", DEFAULT_DEDUP_WINDOW_MINUTES
        )

        tz_name = config.get("timezone", "America/New_York")
        self._tz = ZoneInfo(tz_name)

        schedule = config.get("schedule", {})
        if schedule:
            self._pre_market = datetime.strptime(
                schedule.get("pre_market", "04:00"), "%H:%M"
            ).time()
            self._post_market = datetime.strptime(
                schedule.get("post_market", "20:30"), "%H:%M"
            ).time()
            self._update_start = datetime.strptime(
                schedule.get("market_update_start", "10:00"), "%H:%M"
            ).time()
            self._update_end = datetime.strptime(
                schedule.get("market_update_end", "20:00"), "%H:%M"
            ).time()
            self._update_interval_min = schedule.get(
                "market_update_interval", 60
            )

        if not self._enabled:
            logger.info("[MARKET_INSIGHT] Disabled by config")
            return

        # Check that agent config is available (replaces TAVILY_API_KEY check)
        from src.server.app import setup

        if not setup.agent_config:
            logger.warning(
                "[MARKET_INSIGHT] Agent config not initialized — service disabled"
            )
            return

        self._shutdown_event.clear()

        self._task = asyncio.create_task(
            self._schedule_loop(), name="market_insight_loop"
        )

        now_et = datetime.now(self._tz)
        next_job = self._next_job(now_et)
        if next_job:
            run_at, job_type = next_job
            logger.info(
                f"[MARKET_INSIGHT] Service started, "
                f"next job: {job_type} at {run_at.strftime('%H:%M')} ET"
            )
        else:
            logger.info(
                "[MARKET_INSIGHT] Service started, "
                "no more jobs today"
            )

    async def shutdown(self) -> None:
        """Gracefully stop the schedule loop."""
        logger.info("[MARKET_INSIGHT] Shutting down...")
        self._shutdown_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[MARKET_INSIGHT] Shutdown complete")

    # ------------------------------------------------------------------
    # Per-user on-demand generation
    # ------------------------------------------------------------------

    async def generate_for_user(self, user_id: str) -> dict:
        """Request personalized insight generation for a user.

        Returns the DB row immediately (status='generating').
        The actual agent work runs in a background task.
        """
        # Dedup: check for recently completed personalized insight
        recent = await insight_db.get_user_recent_completed_insight(
            user_id, within_minutes=self._dedup_window_minutes
        )
        if recent:
            logger.info(
                f"[MARKET_INSIGHT] Returning recent insight for user {user_id}: "
                f"{recent['market_insight_id']}"
            )
            return recent

        # Fetch context
        symbols_context = await self._build_user_context(user_id)
        now_et = datetime.now(self._tz)

        if symbols_context:
            prompt = _build_personalized_instruction(symbols_context, now_et)
        else:
            # Empty watchlist/portfolio — fall back to generic brief
            prompt = _build_instruction("market_update", now_et)

        # Atomic idempotency: try to insert, handle conflict from partial unique index
        row = await insight_db.create_market_insight_if_not_generating(
            model="background",
            type="personalized",
            user_id=user_id,
            metadata={"schema_version": 3, "has_context": bool(symbols_context)},
        )
        if row is None:
            # Another request already created a generating row
            existing = await insight_db.get_user_generating_insight(user_id)
            if existing:
                raise InsightAlreadyGeneratingError(existing)
            # Edge case: generating row completed between our conflict and here.
            # Check for the just-completed insight instead of returning None.
            recent = await insight_db.get_user_recent_completed_insight(
                user_id, within_minutes=1
            )
            if recent:
                return recent
            # Truly unknown state — ask caller to retry
            raise InsightAlreadyGeneratingError({"retry": True})

        insight_id = row["market_insight_id"]

        # Fire background task and return immediately
        asyncio.create_task(
            self._run_personalized_generation(user_id, insight_id, prompt),
            name=f"insight_gen_{insight_id}",
        )

        logger.info(
            f"[MARKET_INSIGHT] Started personalized generation for user {user_id}: "
            f"id={insight_id}"
        )
        return row

    async def _run_personalized_generation(
        self, user_id: str, insight_id: str, prompt: str
    ) -> None:
        """Background task: run the research graph and persist the result."""
        start_time = time.monotonic()
        try:
            raw_text = await asyncio.wait_for(
                _research(
                    prompt + "\n\n" + _JSON_GUIDELINES,
                    user_id=user_id,
                    insight_type="personalized",
                    requested_by=user_id,
                ),
                timeout=self._generation_timeout,
            )
            parsed = await self._extract_with_fallback(raw_text, user_id=user_id)
            elapsed_ms = int((time.monotonic() - start_time) * 1000)

            await insight_db.complete_market_insight(
                market_insight_id=insight_id,
                headline=parsed["headline"],
                summary=parsed["summary"],
                content=parsed["news_items"],
                topics=parsed["topics"],
                sources=[],
                generation_time_ms=elapsed_ms,
            )

            logger.info(
                f"[MARKET_INSIGHT] Personalized insight completed for user {user_id}: "
                f"id={insight_id}, time={elapsed_ms}ms"
            )

        except (asyncio.TimeoutError, asyncio.CancelledError):
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                f"[MARKET_INSIGHT] Personalized insight timed out for {user_id}: "
                f"id={insight_id}, time={elapsed_ms}ms"
            )
            try:
                await asyncio.shield(
                    insight_db.fail_market_insight(insight_id, f"Timeout after {elapsed_ms}ms")
                )
            except Exception:
                logger.warning(f"[MARKET_INSIGHT] Failed to mark {insight_id} as failed")
        except Exception as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                f"[MARKET_INSIGHT] Personalized insight failed for {user_id}: "
                f"id={insight_id}, error={e}",
                exc_info=True,
            )
            try:
                await asyncio.shield(
                    insight_db.fail_market_insight(insight_id, str(e))
                )
            except Exception:
                logger.warning(f"[MARKET_INSIGHT] Failed to mark {insight_id} as failed")

    async def _build_user_context(self, user_id: str) -> str:
        """Build symbols context from user's watchlists and portfolio, capped."""
        from src.server.database.portfolio import get_user_portfolio
        from src.server.database.watchlist import (
            get_user_watchlists,
            get_watchlist_items,
        )

        seen_symbols: set[str] = set()
        lines: list[str] = []

        # Watchlist items (concurrent fetch to avoid N+1)
        try:
            watchlists = await get_user_watchlists(user_id)
            items_lists = await asyncio.gather(*[
                get_watchlist_items(wl["watchlist_id"], user_id) for wl in watchlists
            ])
            for items in items_lists:
                for item in items:
                    sym = item.get("symbol", "").upper()
                    if sym and sym not in seen_symbols and len(seen_symbols) < self._max_symbols:
                        seen_symbols.add(sym)
                        name = item.get("name", "")
                        itype = item.get("instrument_type", "")
                        lines.append(f"- {sym} ({name}) [{itype}] [watchlist]")
        except Exception as e:
            logger.warning(f"[MARKET_INSIGHT] Failed to fetch watchlists for {user_id}: {e}")

        # Portfolio holdings
        try:
            holdings = await get_user_portfolio(user_id)
            for h in holdings:
                sym = h.get("symbol", "").upper()
                if sym and sym not in seen_symbols and len(seen_symbols) < self._max_symbols:
                    seen_symbols.add(sym)
                    name = h.get("name", "")
                    qty = h.get("quantity", "")
                    avg_cost = h.get("average_cost", "")
                    lines.append(
                        f"- {sym} ({name}) [portfolio: {qty} shares @ ${avg_cost}]"
                    )
                elif sym in seen_symbols:
                    # Symbol already from watchlist — add portfolio info
                    qty = h.get("quantity", "")
                    avg_cost = h.get("average_cost", "")
                    lines.append(
                        f"  ^ also in portfolio: {qty} shares @ ${avg_cost}"
                    )
        except Exception as e:
            logger.warning(f"[MARKET_INSIGHT] Failed to fetch portfolio for {user_id}: {e}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Schedule computation
    # ------------------------------------------------------------------

    def _todays_schedule(self, now_et: datetime) -> list[tuple[datetime, str]]:
        """Compute all scheduled jobs for today (full day), sorted by time."""
        date = now_et.date()
        is_weekday = date.weekday() < 5  # Mon=0 .. Fri=4
        jobs: list[tuple[datetime, str]] = []

        # pre_market — every day
        jobs.append((
            datetime.combine(date, self._pre_market, tzinfo=self._tz),
            "pre_market",
        ))

        # market_update — weekdays only
        if is_weekday:
            t = datetime.combine(date, self._update_start, tzinfo=self._tz)
            end = datetime.combine(date, self._update_end, tzinfo=self._tz)
            while t <= end:
                jobs.append((t, "market_update"))
                t += timedelta(minutes=self._update_interval_min)

        # post_market — every day
        jobs.append((
            datetime.combine(date, self._post_market, tzinfo=self._tz),
            "post_market",
        ))

        jobs.sort(key=lambda x: x[0])
        return jobs

    def _remaining_jobs(
        self, now_et: datetime
    ) -> list[tuple[datetime, str]]:
        """Return today's jobs that are still in the future (>= now)."""
        return [
            (t, jtype) for t, jtype in self._todays_schedule(now_et) if t >= now_et
        ]

    def _next_job(
        self, now_et: datetime
    ) -> Optional[tuple[datetime, str]]:
        """Return the next job to run (today or tomorrow)."""
        remaining = self._remaining_jobs(now_et)
        if remaining:
            return remaining[0]

        # No more jobs today — return first job tomorrow
        tomorrow = now_et.date() + timedelta(days=1)
        tomorrow_start = datetime.combine(
            tomorrow, datetime.min.time(), tzinfo=self._tz
        )
        tomorrow_jobs = self._todays_schedule(tomorrow_start)
        return tomorrow_jobs[0] if tomorrow_jobs else None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _catchup_if_needed(self) -> None:
        """Generate an insight if nothing recent exists. Single attempt, no retry.

        On failure, the schedule loop will naturally retry at the next
        scheduled time — no tight retry loop for broken infrastructure.
        """
        try:
            now_et = datetime.now(self._tz)
            hour = now_et.hour
            if hour < 10:
                catchup_type = "pre_market"
            elif hour < 20:
                catchup_type = "market_update"
            else:
                catchup_type = "post_market"

            # A restart on a weekend afternoon must not run the hourly update
            # the weekend schedule leaves out.
            if catchup_type not in {t for _, t in self._todays_schedule(now_et)}:
                logger.info(
                    f"[MARKET_INSIGHT] No {catchup_type} scheduled today "
                    f"so no catch-up"
                )
                return

            await self._generate_insight(catchup_type, now_et)
        except Exception as e:
            logger.warning(
                f"[MARKET_INSIGHT] Catch-up failed (non-fatal): {e} — "
                f"schedule loop will retry at next scheduled time"
            )

    async def _schedule_loop(self) -> None:
        """Main loop: sleep until next scheduled job, execute, repeat."""
        await self._catchup_if_needed()

        while not self._shutdown_event.is_set():
            now_et = datetime.now(self._tz)
            next_job = self._next_job(now_et)

            if not next_job:
                logger.warning(
                    "[MARKET_INSIGHT] No scheduled jobs found, retrying in 1h"
                )
                if await self._sleep_until_or_shutdown(
                    datetime.now(self._tz) + timedelta(hours=1)
                ):
                    return
                continue

            run_at, job_type = next_job
            wait_seconds = (run_at - datetime.now(self._tz)).total_seconds()

            if wait_seconds > 0:
                logger.info(
                    f"[MARKET_INSIGHT] Next: {job_type} at "
                    f"{run_at.strftime('%H:%M')} ET "
                    f"(in {wait_seconds:.0f}s)"
                )
                if await self._sleep_until_or_shutdown(run_at):
                    return  # Shutdown requested

            try:
                await self._generate_insight(job_type, datetime.now(self._tz))
            except Exception as e:
                logger.error(
                    f"[MARKET_INSIGHT] {job_type} generation failed: {e}",
                    exc_info=True,
                )

    async def _sleep_until_or_shutdown(self, target: datetime) -> bool:
        """Sleep until target time. Returns True if shutdown was requested."""
        now = datetime.now(self._tz)
        wait = max((target - now).total_seconds(), 0)
        if wait <= 0:
            return False
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=wait)
            return True  # Shutdown requested
        except asyncio.TimeoutError:
            return False  # Timer elapsed normally

    # ------------------------------------------------------------------
    # Insight generation (system scheduled)
    # ------------------------------------------------------------------

    async def _generate_insight(
        self, job_type: str, now_et: datetime
    ) -> None:
        """Generate a single market insight, unless another worker has the slot
        or a recent insight of this type already covers it."""
        instruction = _build_instruction(job_type, now_et)
        claim = await self._claim(job_type, instruction)
        if claim.holder_in_flight:
            claim = await self._outlast(claim, job_type, instruction)
        if claim.row is None:
            logger.info(
                f"[MARKET_INSIGHT] Skipping {job_type}: insight "
                f"{claim.holder_id} already covers it"
            )
            return
        insight_id = claim.row["market_insight_id"]

        logger.info(f"[MARKET_INSIGHT] Starting {job_type}: id={insight_id}")
        start_time = time.monotonic()

        # When auth is off (local dev / self-hosted), use the local dev user's
        # credentials so insights work without a system API key.
        from src.config.settings import HOST_MODE, LOCAL_DEV_USER_ID
        system_user_id = None if HOST_MODE == "platform" else LOCAL_DEV_USER_ID

        async def research_and_extract() -> dict:
            raw_text = await _research(
                instruction + "\n\n" + _JSON_GUIDELINES,
                user_id=system_user_id,
                insight_type=job_type,
            )
            return await self._extract_with_fallback(raw_text, user_id=system_user_id)

        try:
            # Extraction is inside the timeout so a run never outlives the
            # in-flight hold its claim gives it.
            parsed = await asyncio.wait_for(
                research_and_extract(), timeout=self._generation_timeout
            )
            elapsed_ms = int((time.monotonic() - start_time) * 1000)

            await insight_db.complete_market_insight(
                market_insight_id=insight_id,
                headline=parsed["headline"],
                summary=parsed["summary"],
                content=parsed["news_items"],
                topics=parsed["topics"],
                sources=[],
                generation_time_ms=elapsed_ms,
            )

            logger.info(
                f"[MARKET_INSIGHT] {job_type} completed: "
                f"id={insight_id}, time={elapsed_ms}ms"
            )

        except (asyncio.TimeoutError, asyncio.CancelledError):
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                f"[MARKET_INSIGHT] {job_type} timed out: "
                f"id={insight_id}, time={elapsed_ms}ms"
            )
            try:
                await asyncio.shield(
                    insight_db.fail_market_insight(
                        insight_id, f"Timeout after {elapsed_ms}ms"
                    )
                )
            except Exception:
                logger.warning(f"[MARKET_INSIGHT] Failed to mark {insight_id} as failed")
        except Exception as e:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                f"[MARKET_INSIGHT] {job_type} failed for {insight_id}: {e}",
                exc_info=True,
            )
            try:
                await asyncio.shield(
                    insight_db.fail_market_insight(insight_id, str(e))
                )
            except Exception:
                logger.warning(f"[MARKET_INSIGHT] Failed to mark {insight_id} as failed")

    def _in_flight_within(self) -> timedelta:
        return timedelta(seconds=self._generation_timeout) + IN_FLIGHT_SLACK

    async def _claim(self, job_type: str, instruction: str) -> insight_db.SystemInsightClaim:
        return await insight_db.claim_system_insight(
            job_type,
            fresh_within=STALENESS_WINDOWS.get(job_type, timedelta(minutes=30)),
            in_flight_within=self._in_flight_within(),
            model="background",
            metadata={"instruction": instruction, "schema_version": 3},
        )

    async def _outlast(
        self, claim: insight_db.SystemInsightClaim, job_type: str, instruction: str
    ) -> insight_db.SystemInsightClaim:
        """Claim again until the run holding the type completes, fails, or
        outlives its hold, and return the claim that ends the wait.

        Losing the claim is not final: the other workers' runs used to be the
        retry when one failed, and a deploy that stops the winning worker
        marks its run failed. The wait also ends when another worker takes
        the type over, so a failed run is retried once, not once per worker.
        """
        holder_id = claim.holder_id
        while claim.holder_in_flight and claim.holder_id == holder_id:
            await asyncio.sleep(HOLDER_POLL_SECONDS)
            claim = await self._claim(job_type, instruction)
        return claim

    async def _extract_with_fallback(self, raw_text: str, user_id: str | None) -> dict:
        """Extract structured output from raw text, falling back to LLM if direct parse fails."""
        try:
            return _extract_structured_output(raw_text)
        except ValueError:
            logger.info("[MARKET_INSIGHT] Direct JSON parse failed, trying LLM fallback")
            return await _llm_extract_fallback(raw_text, user_id=user_id)


class InsightAlreadyGeneratingError(Exception):
    """Raised when user already has an in-progress insight generation."""

    def __init__(self, existing_insight: dict):
        self.existing_insight = existing_insight
        super().__init__("Insight generation already in progress")
