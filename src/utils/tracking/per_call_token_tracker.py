"""
Per-call token usage tracker for accurate tiered pricing.

This module provides a custom callback handler that tracks token usage with per-call
granularity, enabling accurate cost calculation for models with tiered pricing,
2D matrix pricing, or input-dependent pricing.

Unlike LangChain's default UsageMetadataCallbackHandler which aggregates tokens
immediately, this tracker preserves per-call records before aggregation, allowing
accurate pricing where rates vary based on token counts.
"""

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.messages import AIMessage
from langchain_core.messages.ai import UsageMetadata, add_usage
from langchain_core.outputs import ChatGeneration, LLMResult

from src.llms.token_counter import extract_token_usage
from src.utils.tracking.core import calculate_cost_from_per_call_records

logger = logging.getLogger(__name__)

# Every model field is stored per call and persisted into a JSON column, so all of
# them are bounded before they land. Long enough for any real model name plus a
# versioned snapshot suffix (the longest identity we ship is 30 characters); short
# enough that a repeated echo cannot amplify the row.
_MODEL_FIELD_MAX_LEN = 256


def _as_text(value: Any) -> Optional[str]:
    """Keep a model field only when it is actually a string.

    Guards both untrusted directions: the provider's echo and the run metadata a
    caller can set. A non-string reaching the record becomes a dict key downstream
    and raises where nothing can catch it usefully.
    """
    return value if isinstance(value, str) else None


def _as_identity(value: Any) -> Optional[str]:
    """Keep a stamped billing identity only if it could plausibly be one.

    Bounded like the vendor echo but rejected rather than truncated: a cut manifest
    key matches no rate card and bills zero, which is the exact failure this
    attribution exists to fix, whereas dropping it degrades to the echo and the
    working fallback behind it.
    """
    text = _as_text(value)
    return text if text and len(text) <= _MODEL_FIELD_MAX_LEN else None


def collapse_repeated_name(name: str) -> str:
    """Collapse a model name that streaming has written N times over.

    langchain merges ``response_metadata`` across streamed chunks by concatenating
    conflicting strings and does not exempt ``model_name``, so a provider repeating
    the field on every chunk yields the name N times, which matches no pricing entry
    and bills as zero. The shortest repeating unit wins: taking the longest would
    return ``name * (N / smallest_prime_factor(N))`` and so only repair prime N,
    while over-collapsing would need a real model name that is itself a perfect
    repetition, which no manifest key or model_id is.
    """
    if not name:
        return name
    # Smallest rotation mapping the string onto itself, i.e. its shortest period,
    # in one linear scan.
    period = (name + name).find(name, 1)
    if 0 < period < len(name) and len(name) % period == 0:
        # Logged because the repair is not provably right: at this point a vendor model
        # whose name is itself a perfect repetition is indistinguishable from a corrupted
        # one, and no manifest key or model_id we ship is such a name. Not collapsing is
        # the worse default, since the uncollapsed name matches no card and bills zero,
        # so the substitution is made observable rather than avoided. served_model keeps
        # the raw echo either way.
        logger.warning(
            f"Collapsed a repeated model name (x{len(name) // period}) to "
            f"{name[:period][:_MODEL_FIELD_MAX_LEN]!r}"
        )
        return name[:period]
    return name


class PerCallTokenTracker(BaseCallbackHandler):
    """
    Tracks LLM token usage with per-call granularity for accurate tiered pricing.

    This callback handler captures token usage from each individual LLM call before
    aggregation, enabling accurate cost calculation for models with:
    - Tiered pricing (different rates based on token count thresholds)
    - 2D matrix pricing (rates vary by both input and output token counts)
    - Input-dependent output pricing (output rate based on input tier)

    The tracker maintains both:
    1. per_call_records: List of individual call records for accurate pricing
    2. usage_metadata: Aggregated usage by model for backward compatibility

    Example:
        >>> tracker = PerCallTokenTracker()
        >>> # Use in LangGraph workflow
        >>> result = workflow.invoke(input, config={"callbacks": [tracker]})
        >>> # Calculate accurate costs from per-call records
        >>> costs = calculate_cost_from_per_call_records(tracker.get_per_call_records())

    ``per_call_records`` is the private buffer; ``get_per_call_records()`` is the
    reader-facing snapshot. Background subagents share one tracker, so anything
    that iterates the buffer has to go through the accessor.
    """

    def __init__(self) -> None:
        """Initialize the tracker with empty per-call records and aggregated metadata."""
        super().__init__()
        self._lock = threading.Lock()
        self.per_call_records: List[Dict[str, Any]] = []
        self.usage_metadata: Dict[str, UsageMetadata] = {}
        # Running USD total of platform-billed calls. A reader prices only the
        # records appended since the last read, through the batch pass that
        # bills the turn, so the gate's arithmetic is the biller's arithmetic
        # rather than a copy of it. Advisory by design: the billed figure
        # remains that finalize-time pass, so a pricing failure here costs
        # gating precision, never money.
        self._platform_usd: float = 0.0
        self._priced_upto: int = 0
        self._pricing_error_logged = False
        # Maps run_id → what is only knowable at dispatch: the billing attribution
        # stamped on the client that issued the call (see LLM.get_llm) — billing_type,
        # the manifest key, the pricing id/provider that key resolves to — plus the
        # request's start time. One dict rather than one per field so every terminal
        # path drains the whole entry at once.
        self._run_attribution: Dict[UUID, Dict[str, str]] = {}

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Capture billing attribution from LLM metadata before the call runs."""
        self._capture_run_metadata(run_id, metadata)

    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Capture billing attribution from chat model metadata (preferred over on_llm_start)."""
        self._capture_run_metadata(run_id, metadata)

    _ATTRIBUTION_KEYS = ("billing_type", "manifest_model", "pricing_model_id", "pricing_provider")

    def _capture_run_metadata(
        self, run_id: UUID, metadata: Optional[Dict[str, Any]]
    ) -> None:
        """Stash the per-run billing attribution stamped on the client.

        Read at start rather than at end because a fallback swaps the whole client
        mid-turn; each attempt is its own run_id carrying its own stamp. The start
        time is stamped even when the client carries none of that attribution: it
        anchors peak hour pricing, which an unstamped client is equally subject
        to, and a streamed call can open and close in different rate windows.
        """
        # Validated like the vendor echo, and for a sharper reason: run metadata is
        # not only ours. A consumer-supplied client, or any caller passing
        # config={"metadata": ...}, can land anything here, and a bad manifest_model
        # becomes the billing key — a non-string poisons the record and makes the
        # turn's pricing pass raise on an unhashable key, an unbounded one inflates
        # every record and the row they persist to. Logged rather than dropped
        # quietly: the fallback is the vendor echo, which is the very attribution
        # this stamp exists to override.
        captured: Dict[str, str] = {}
        for key in self._ATTRIBUTION_KEYS:
            raw = metadata.get(key) if metadata else None
            if raw is None:
                continue  # an unstamped client, not a malformed stamp
            text = _as_identity(raw)
            if text is None:
                logger.warning(
                    f"Ignoring unusable {key} stamp on run {run_id}; "
                    "billing falls back to the provider echo"
                )
                continue
            captured[key] = text
        with self._lock:
            entry = self._run_attribution.setdefault(run_id, {})
            entry.update(captured)
            # First start wins: on_llm_start and on_chat_model_start are
            # alternatives, but a handler that saw both would otherwise push the
            # anchor later than the request actually went out.
            entry.setdefault("started_at", datetime.now(timezone.utc).isoformat())

    def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        """
        Callback invoked after an LLM completes.

        Captures token usage metadata from the response and stores both:
        1. Per-call record with full metadata
        2. Aggregated usage by model (for compatibility)

        Args:
            response: The LLM response containing usage metadata
            run_id: Unique identifier for this LLM run
            parent_run_id: Identifier of the parent run (if nested)
            **kwargs: Additional callback arguments
        """
        # Drain the run's attribution before any early return below can skip it.
        # The entry is dead either way once the run ends, and four of the five
        # exits here never reach the append.
        with self._lock:
            attribution = self._run_attribution.pop(run_id, {})

        if not response.generations or not response.generations[0]:
            return

        generation = response.generations[0][0]
        if not isinstance(generation, ChatGeneration):
            return

        message = generation.message
        if not isinstance(message, AIMessage):
            return

        # Use extract_token_usage() for robust token extraction across providers
        # Handles Anthropic, OpenAI, Gemini formats with proper field normalization
        usage_metadata = extract_token_usage(message)
        if not usage_metadata:
            return

        # What the provider says it served. Useful on its own — it is how a silent
        # substitution or an alias-to-snapshot resolution becomes visible — but it is
        # the vendor's string, so it is not what we bill on. Bounded because it is
        # upstream-controlled and lands in a JSON column: the same chunk-merge that
        # motivates collapse_repeated_name makes its length scale with output tokens.
        # Type-guarded because this is provider-parsed JSON, not our own value: a
        # non-string here would raise inside the callback, and langchain swallows
        # callback exceptions, so the call would complete having metered nothing.
        served_model = _as_text(message.response_metadata.get("model_name"))
        if not served_model and response.llm_output:
            served_model = _as_text(response.llm_output.get("model_name"))

        # Bill on our own key when we have one. Consumer-supplied clients carry no
        # stamp, so the vendor echo is the fallback, with any streaming repetition
        # collapsed before it reaches pricing. Collapse first, then bound: the
        # repetition this repairs is longer than the cap, so cutting first would
        # strand a name the repair would otherwise have recovered.
        billing_type = attribution.get("billing_type", "platform")
        model_name = attribution.get("manifest_model") or (
            collapse_repeated_name(served_model)[:_MODEL_FIELD_MAX_LEN]
            if served_model
            else None
        )
        if served_model:
            served_model = served_model[:_MODEL_FIELD_MAX_LEN]

        if not model_name:
            logger.warning(
                f"No model_name found in response metadata for run {run_id}, "
                "skipping token tracking"
            )
            return

        record = {
            "model_name": model_name,
            "served_model": served_model,
            "pricing_model_id": attribution.get("pricing_model_id"),
            "pricing_provider": attribution.get("pricing_provider"),
            "usage": usage_metadata,
            "billing_type": billing_type,
            "started_at": attribution.get("started_at"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": str(run_id),
            "parent_run_id": str(parent_run_id) if parent_run_id else None,
        }

        with self._lock:
            # Store per-call record
            self.per_call_records.append(record)

            # Also maintain aggregated usage for backward compatibility
            if model_name not in self.usage_metadata:
                self.usage_metadata[model_name] = usage_metadata
            else:
                self.usage_metadata[model_name] = add_usage(
                    self.usage_metadata[model_name], usage_metadata
                )

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        with self._lock:
            self._run_attribution.pop(run_id, None)

    def platform_usd_total(self) -> float:
        """Running USD total of platform-billed calls, for mid-run gating.

        Prices only the records appended since the last read, through the same
        batch pass that bills the turn — one pricing implementation rather than
        a second one that has to be kept agreeing with it. A tail that raises
        is skipped rather than retried: the alternative freezes this figure for
        the rest of the turn, and a record that cannot be priced here cannot be
        billed at finalize either, so it is already a money bug elsewhere.
        """
        # The lock guards the records and the cursor, not the pricing pass:
        # this is called on every model boundary and on a two-second
        # heartbeat, and holding it across the pass stalls the callbacks
        # appending the records being priced.
        with self._lock:
            tail = self.per_call_records[self._priced_upto:]
            self._priced_upto = len(self.per_call_records)
            if not tail:
                return self._platform_usd
        try:
            priced = calculate_cost_from_per_call_records(tail)["platform_cost"]
        except Exception:
            priced = 0.0
            if not self._pricing_error_logged:
                self._pricing_error_logged = True
                logger.warning(
                    "Failed to price %d record(s) into the running total; "
                    "the mid-run spend figure undercounts them",
                    len(tail),
                    exc_info=True,
                )
        with self._lock:
            self._platform_usd += priced
            return self._platform_usd

    def get_aggregated_usage(self) -> Dict[str, UsageMetadata]:
        """
        Get aggregated token usage by model.

        This method provides backward compatibility with code expecting
        aggregated usage data.

        Returns:
            Dictionary mapping model names to aggregated UsageMetadata
        """
        with self._lock:
            return self.usage_metadata.copy()

    def get_per_call_records(self) -> List[Dict[str, Any]]:
        """Snapshot of the per-call records, never the live buffer.

        Background subagents append to the same tracker while a turn winds down, so
        every reader needs its own copy taken under the lock. In a record
        ``model_name`` is the billing key and ``served_model`` the raw vendor echo,
        diagnostic only and never priced; the append site documents the rest.
        """
        with self._lock:
            return self.per_call_records.copy()

    def record_count(self) -> int:
        """How many calls have been recorded, without copying the buffer."""
        with self._lock:
            return len(self.per_call_records)

    def reset(self) -> None:
        """
        Reset all tracked data.

        Clears both per-call records and aggregated usage metadata.
        Useful for reusing the same tracker across multiple workflow runs.
        """
        with self._lock:
            self.per_call_records.clear()
            self.usage_metadata.clear()
            self._run_attribution.clear()
            self._platform_usd = 0.0
            self._priced_upto = 0
            self._pricing_error_logged = False

    def __repr__(self) -> str:
        """String representation showing number of calls and models tracked."""
        with self._lock:
            num_calls = len(self.per_call_records)
            num_models = len(self.usage_metadata)
        return f"PerCallTokenTracker(calls={num_calls}, models={num_models})"
