"""Token usage and cost calculation utilities."""

import copy
import logging
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, TypedDict

logger = logging.getLogger(__name__)

# An unstamped record's model name is the vendor's string; bound it before it
# reaches a log line.
_LOGGED_NAME_MAX_LEN = 120

# Which key space ``by_model`` uses, read off the row rather than inferred from
# created_at — during a blue/green drain both shapes are written concurrently,
# which is exactly when a timestamp boundary stops working. Derived per payload
# rather than fixed, because one turn can carry both a stamped client and a
# consumer-supplied one and land two key spaces in the same dict. Nothing
# consumes it yet; it is written now because a row that predates the marker can
# never acquire one.
KEY_SHAPE_MANIFEST = "manifest"
KEY_SHAPE_VENDOR = "vendor"
KEY_SHAPE_MIXED = "mixed"


# ============================================================================
# Cost and Token Utilities
# ============================================================================


def _stamp_applied_rates(
    entry: Dict[str, Any],
    pricing: Optional[Dict[str, Any]],
    window: Optional[str],
    cost: float,
    billing_type: str = "platform",
    straddled: bool = False,
) -> None:
    """Record on the row itself which rate card this call was billed at.

    A row holding only token counts is repriced by any later manifest edit, so a
    rate change silently restates turns that were metered correctly at the time.
    The resolved card is stored verbatim rather than summarized: a tiered or matrix
    schedule does not reduce to a pair of numbers, and a summary would be the thing
    that silently drifts.

    Deduped by card rather than keyed by window, because ``by_model`` is keyed by
    model and adding a second dimension to that key forks the key space again.
    One entry is the normal case; a turn straddling a schedule boundary gets two.
    """
    if not pricing:
        return

    rates = entry.setdefault("rates", [])
    for applied in rates:
        if (
            applied.get("window") == window
            and applied["billing_type"] == billing_type
            and applied["pricing"] == pricing
        ):
            applied["call_count"] += 1
            applied["cost"] += cost
            if straddled:
                applied["straddled_calls"] = applied.get("straddled_calls", 0) + 1
            return

    applied = {
        "call_count": 1,
        "cost": cost,
        # by_model is keyed by model alone, so a model used both platform-routed
        # and BYOK in one turn lands two genuinely different cards in one entry.
        # Without this the platform-billed half is not recoverable from the row.
        "billing_type": billing_type,
        "pricing": copy.deepcopy(pricing),
    }
    if window:
        applied["window"] = window
    if straddled:
        applied["straddled_calls"] = 1
    rates.append(applied)


def empty_usage_payload() -> Dict[str, Any]:
    """The zero-usage row, in the same shape a priced turn produces.

    Kept separate from the pricing pass because the degraded exits that need it
    run precisely when that pass has just raised, so they cannot call back into
    it. Omitting a key here instead would make a row that failed to price
    indistinguishable from one an older writer produced.
    """
    return {
        "by_model": {},
        "model_key_shape": KEY_SHAPE_MANIFEST,
        "total_cost": 0.0,
        "platform_cost": 0.0,
        # The populated path drops zero components, so an empty turn names none.
        "cost_breakdown": {},
        "per_call_costs": [],
    }


class CallRecord(TypedDict):
    """One model call as metered and not yet priced (``build_call_record``)."""

    # The billing key. The vendor's echo is kept apart and never priced.
    model_name: str
    served_model: Optional[str]
    # The client's own stamp, which outranks a lookup by name.
    pricing_model_id: Optional[str]
    pricing_provider: Optional[str]
    # Flattened by extract_token_usage: input, output, cached, cache write tokens.
    usage: Dict[str, Any]
    billing_type: str
    started_at: Optional[str]
    timestamp: str
    run_id: str
    parent_run_id: Optional[str]


class PricedCall(NamedTuple):
    card: Optional[Dict[str, Any]]
    window: Optional[str]
    straddled: bool
    cost: float
    breakdown: Dict[str, Any]
    # Whether the manifest had a card at all: a seat-priced card is found and
    # still bills as None, which a caller has to tell apart from "unknown".
    listed: bool


_ResolutionKey = Tuple[str, str, Optional[str], Optional[str]]


class PricingResolver:
    """Resolve and price a model once per key, not once per call.

    A turn carries hundreds of records across a handful of models, and both
    lookups walk the manifest and log on a miss. Memoizing turns the miss
    into one warning per model per turn instead of one per call. The stamped
    pair is part of the key because a fallback can swap the route mid-turn,
    so two records can share a display name and still bill differently.
    """

    def __init__(self) -> None:
        self._resolved: Dict[_ResolutionKey, Tuple[Optional[Dict[str, Any]], bool]] = {}

    def card_for(self, record: CallRecord) -> Tuple[Optional[Dict[str, Any]], bool]:
        """The billable card for the record's call, and whether one was listed."""
        from src.llms.pricing_utils import find_model_pricing, resolve_pricing_identity

        model_name = record["model_name"]
        billing_type = record.get("billing_type", "platform")
        stamped_id = record.get("pricing_model_id")
        stamped_provider = record.get("pricing_provider")
        cache_key = (model_name, billing_type, stamped_id, stamped_provider)
        if cache_key in self._resolved:
            return self._resolved[cache_key]

        if stamped_id:
            # The client that issued the call is the authority on what it routed
            # to, so its own stamp outranks a lookup by name. For a built-in the
            # two agree — provider is already reassigned to system_provider on the
            # platform route — but a user-defined model may deliberately shadow a
            # built-in name to send it through a variant's key, and resolving that
            # name against the manifest would price the model it replaced.
            provider, pricing_id = stamped_provider, stamped_id
        else:
            # No stamp: a consumer-supplied client. Fall back to the manifest key
            # the record carries — system_provider for platform billing, base
            # provider for BYOK/OAuth.
            provider, pricing_id = resolve_pricing_identity(
                model_name, billing_type=billing_type
            )

        pricing = find_model_pricing(pricing_id, provider=provider)
        listed = pricing is not None

        if pricing is None:
            # A miss on a platform call is a money event, not a reporting gap: the
            # tokens still aggregate but contribute nothing to platform_cost, which
            # is the figure the turn is billed on. find_model_pricing already warns
            # on the lookup itself; this line adds the context that makes it actionable.
            # !r on both names: an unstamped record carries the vendor's string,
            # which can hold newlines and forge log records downstream. pricing_id
            # needs the same treatment as model_name rather than less — on a
            # manifest miss the resolver hands back the name it was given, so this
            # is that same untrusted string a second time.
            miss = (
                f"No pricing found for model: {model_name[:_LOGGED_NAME_MAX_LEN]!r} "
                f"(pricing id: {pricing_id[:_LOGGED_NAME_MAX_LEN]!r}, provider: {provider!r}), "
                "recording tokens with zero cost"
            )
            if billing_type == "platform":
                logger.warning(miss)
            else:
                logger.debug(miss)
        elif pricing.get("pricing_type") == "subscription":
            # Seat-priced plans meter tokens but bill nothing per token.
            logger.debug(f"Subscription pricing for {model_name!r}, recording tokens with zero cost")
            pricing = None

        self._resolved[cache_key] = (pricing, listed)
        return self._resolved[cache_key]


def price_call(record: CallRecord, resolve: Optional[PricingResolver] = None) -> PricedCall:
    """One per-call record priced, picking the rate window first.

    The batch pass prices every record through here, so a caller pricing one
    call cannot disagree with the bill. ``resolve`` carries a memo across
    calls for a caller that prices them one at a time.
    """
    from src.llms.pricing_utils import (
        calculate_total_cost,
        has_schedule,
        parse_stamp,
        resolve_schedule,
        schedule_anchor,
    )

    pricing, listed = (resolve or PricingResolver()).card_for(record)
    usage = record["usage"]

    # Extract token counts from this specific call
    input_tokens = usage.get('input_tokens', 0)
    output_tokens = usage.get('output_tokens', 0)

    # Extract cached tokens and cache creation tokens
    # Uses flattened structure from extract_token_usage()
    cached_tokens = usage.get('cached_tokens', 0)
    cache_5m_tokens = usage.get('cache_5m_tokens', 0)
    cache_1h_tokens = usage.get('cache_1h_tokens', 0)

    # A model on peak hour pricing resolves per record, after the memoized
    # lookup: the card depends on when this call ran, which the batch cannot
    # share. Anchored on whichever end of the call the manifest names.
    window = None
    straddled = False
    card = pricing
    if has_schedule(pricing):
        anchor = schedule_anchor(pricing)
        billed_on = "started_at" if anchor == "request" else "timestamp"
        other_end = "timestamp" if anchor == "request" else "started_at"
        anchor_at = parse_stamp(record.get(billed_on))
        other_at = parse_stamp(record.get(other_end))

        # Records written before the start stamp existed carry only one end.
        # The other end is a real observation of the same call, so it places
        # the call far better than the no-stamp default of charging peak.
        card, window = resolve_schedule(pricing, anchor_at or other_at)

        # A streamed call can open in one window and close in another, and
        # the whole call bills at one rate either way — the windows differ by
        # 2x, so the unbilled end is the error. Recorded rather than acted on:
        # no vendor says which end it charges, so the size of the disagreement
        # has to be measurable before anyone can argue about it. Compares the
        # two ends only, so a call crossing out of a window and back reads as
        # unstraddled; that needs a call longer than the trough between two
        # peak blocks, which is hours. Both ends have to be real: claiming a
        # straddle off one stamp and one default would inflate the very count
        # this exists to size, and every pre-branch record has one end.
        if anchor_at is not None and other_at is not None:
            straddled = resolve_schedule(pricing, other_at)[1] != window

    # Cost is 0 when pricing is unavailable or seat-priced, but token counts
    # are still aggregated so usage is never lost.
    if card:
        # Calculate cost for THIS CALL ONLY (accurate tiered pricing)
        cost_result = calculate_total_cost(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            cache_5m_tokens=cache_5m_tokens,
            cache_1h_tokens=cache_1h_tokens,
            pricing=card
        )
        return PricedCall(
            card,
            window,
            straddled,
            cost_result['total_cost'],
            cost_result.get('breakdown', {}),
            listed,
        )
    return PricedCall(card, window, straddled, 0.0, {}, listed)


def calculate_cost_from_per_call_records(
    per_call_records: List[CallRecord], resolve: Optional[PricingResolver] = None
) -> Dict[str, Any]:
    """
    Calculate accurate cost from per-call token usage records.

    This function calculates costs by processing each LLM call individually before
    aggregating, enabling accurate pricing for models with tiered pricing, 2D matrix
    pricing, or input-dependent pricing.

    Pricing the aggregate instead would push two small calls into a tier neither
    of them reached, so the records stay separate until after each is priced.

    Example:
        With tiered pricing (0-32k: $0.80, 32k-128k: $1.20):

        Incorrect (aggregated tokens): Two 30k calls → 60k total → Tier 2 pricing
        Correct (per-call): Two 30k calls → Each in Tier 1 → Accurate pricing

    Args:
        per_call_records: The records PerCallTokenTracker collected.
        resolve: The memo of earlier passes over the same turn, for the
            mid-run spend gate. The bill resolves afresh.

    Returns:
        The shape below is persisted verbatim into the ``token_usage`` JSON
        column, so it is a stored contract rather than prose.

        {
            "by_model": {
                model_name: {
                    input_tokens: int,
                    output_tokens: int,
                    total_tokens: int,
                    cached_tokens: int,
                    call_count: int,
                    total_cost: float,
                    # Absent entirely when nothing was billed — an unpriced or
                    # seat-priced model aggregates tokens but has no card:
                    rates: [               # the card(s) actually billed, verbatim
                        {
                            call_count: int,
                            cost: float,
                            pricing: dict,
                            window: str,           # omitted unless a schedule chose it
                            straddled_calls: int,  # opened in the other window
                        },
                        ...
                    ],
                }
            },
            "model_key_shape": str,   # which key space by_model uses
            "total_cost": float,
            "platform_cost": float,
            "cost_breakdown": {
                input_cost: float,
                output_cost: float,
                cached_cost: float,
                ...
            },
            "per_call_costs": [
                {
                    model_name: str,
                    served_model: Optional[str],
                    input_tokens: int,
                    output_tokens: int,
                    cost: float,
                    breakdown: dict,
                    billing_type: str,
                    timestamp: str,
                    run_id: str,
                    # Peak hour models only, so every other call stays as it was:
                    window: str,
                    started_at: Optional[str],
                    straddled: bool,   # omitted when the call sat in one window
                },
                ...
            ]
        }
    """
    if not per_call_records:
        return empty_usage_payload()

    total_cost = 0.0
    aggregated_breakdown = {
        "input_cost": 0.0,
        "output_cost": 0.0,
        "cached_cost": 0.0,
        "cache_storage_cost": 0.0,
        "cache_5m_cost": 0.0,
        "cache_1h_cost": 0.0
    }
    per_call_costs = []
    by_model: Dict[str, Dict[str, Any]] = {}
    resolve = resolve or PricingResolver()

    # A stamped client names its own key; an unstamped one leaves the vendor echo as
    # the key. Counted so the marker describes the dict actually built rather than
    # the one this writer intends.
    #
    # KEY_SHAPE_MANIFEST means the key came from our side, not that it is present in
    # models.json: a user-defined model's key is its own alias, which is off-manifest
    # by definition and still ours. A reader wanting manifest membership has to check
    # the manifest; this marker only separates our keys from the vendor's.
    stamped_records = 0

    # Process each call individually
    for record in per_call_records:
        model_name = record["model_name"]
        usage = record["usage"]

        billing_type = record.get("billing_type", "platform")
        if record.get("pricing_model_id"):
            stamped_records += 1
        card, window, straddled, call_cost, call_breakdown, _ = price_call(record, resolve)

        input_tokens = usage.get('input_tokens', 0)
        output_tokens = usage.get('output_tokens', 0)
        cached_tokens = usage.get('cached_tokens', 0)

        # Store per-call cost record
        call_cost_record = {
            "model_name": model_name,
            "served_model": record.get("served_model"),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "cost": call_cost,
            "breakdown": call_breakdown,
            "billing_type": record.get("billing_type", "platform"),
            "timestamp": record.get("timestamp"),
            "run_id": record.get("run_id"),
            "parent_run_id": record.get("parent_run_id"),
        }
        if window:
            # Only for models priced by the hour, so the row does not carry a
            # window and a second stamp on every call of every other model.
            call_cost_record["window"] = window
            call_cost_record["started_at"] = record.get("started_at")
            if straddled:
                call_cost_record["straddled"] = True
        per_call_costs.append(call_cost_record)

        # Aggregate total cost
        total_cost += call_cost

        # Aggregate breakdown across all calls
        if 'input' in call_breakdown:
            aggregated_breakdown['input_cost'] += call_breakdown['input']['cost']
        if 'cached_input' in call_breakdown:
            aggregated_breakdown['cached_cost'] += call_breakdown['cached_input']['cost']
        if 'output' in call_breakdown:
            aggregated_breakdown['output_cost'] += call_breakdown['output']['cost']
        if 'cache_storage' in call_breakdown:
            aggregated_breakdown['cache_storage_cost'] += call_breakdown['cache_storage']['cost']
        if 'cache_5m_creation' in call_breakdown:
            aggregated_breakdown['cache_5m_cost'] += call_breakdown['cache_5m_creation']['cost']
        if 'cache_1h_creation' in call_breakdown:
            aggregated_breakdown['cache_1h_cost'] += call_breakdown['cache_1h_creation']['cost']

        # Aggregate usage by model (for reporting)
        if model_name not in by_model:
            by_model[model_name] = {
                'input_tokens': 0,
                'output_tokens': 0,
                'total_tokens': 0,
                'cached_tokens': 0,
                'call_count': 0,
                'total_cost': 0.0,
            }

        by_model[model_name]['input_tokens'] += input_tokens
        by_model[model_name]['output_tokens'] += output_tokens
        by_model[model_name]['total_tokens'] += input_tokens + output_tokens
        by_model[model_name]['cached_tokens'] += cached_tokens
        by_model[model_name]['call_count'] += 1
        by_model[model_name]['total_cost'] += call_cost
        _stamp_applied_rates(
            by_model[model_name], card, window, call_cost, billing_type, straddled
        )

    # Clean up zero-value breakdown entries
    cost_breakdown = {k: v for k, v in aggregated_breakdown.items() if v > 0}

    # Sum cost for platform-served calls only (used for credit deduction).
    # BYOK/OAuth calls are paid by the user's own key — no credits consumed.
    platform_cost = sum(
        c["cost"] for c in per_call_costs if c.get("billing_type") == "platform"
    )

    if stamped_records == len(per_call_records):
        model_key_shape = KEY_SHAPE_MANIFEST
    elif stamped_records == 0:
        model_key_shape = KEY_SHAPE_VENDOR
    else:
        model_key_shape = KEY_SHAPE_MIXED

    return {
        "by_model": by_model,
        "model_key_shape": model_key_shape,
        "total_cost": total_cost,
        "platform_cost": platform_cost,
        "cost_breakdown": cost_breakdown,
        "per_call_costs": per_call_costs,
    }


# Public API
__all__ = [
    'CallRecord',
    'PricedCall',
    'PricingResolver',
    'calculate_cost_from_per_call_records',
    'price_call',
]
