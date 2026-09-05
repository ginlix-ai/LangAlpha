"""Server wiring for the runtime credit gate.

The gate middleware (``ptc_agent.agent.middleware.credit_gate``) is
deployment-agnostic: it talks to a duck-typed port. This module is the
platform-mode implementation — leases against the quota service's
``/api/auth/billing/leases`` endpoints, heartbeats into this service's own
run ledgers — and the factory that decides whether a run gets a gate at
all. With platform gating inactive (OSS mode or no ``AUTH_SERVICE_URL``)
the factory returns None and the gate never engages.
"""

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ptc_agent.agent.middleware.credit_gate import (
    LEASE_RENEW_MARGIN_SECONDS,
    CreditGateState,
    CreditLease,
    LeaseVerdict,
)

from src.config.env import USD_TO_CREDITS_RATE
from src.config.settings import AUTH_SERVICE_URL
from src.llms.pricing_utils import chunk_multiplier
from src.server.database.runs import credit_ledger
from src.server.dependencies import usage_limits

logger = logging.getLogger(__name__)

# Stand-in when the service does not say when the lease expires. Comfortably
# above the gate's renew margin on purpose: the renewal cadence degrades to a
# slow poll rather than a hot one, and the gate keeps enforcing the ceiling it
# was granted in the meantime.
_UNKNOWN_TTL_SECONDS = 300.0

# A figure this large is a metering bug on our side, not a spend. Declining to
# send it fails open for one tick, which the next heartbeat corrects — cheaper
# than asking for a ceiling sized off a number we do not believe.
_MAX_REPORTABLE_SPEND = 1_000_000.0

# Bounds on the multiplier we are willing to send. The floor keeps a bad rate
# from asking for a reservation too small to cover a single call; the ceiling
# keeps one from reserving a user's whole balance against one turn. A figure
# outside them is a manifest or arithmetic fault, not a price.
# The unit both services count in. Not a deployment knob while a quota
# service is comparing against it: see the boot check below.
_CANONICAL_CREDIT_UNIT = 1000
_MIN_RATE_MULTIPLIER = 0.1
_MAX_RATE_MULTIPLIER = 100.0


def _reportable_multiplier(value: Any) -> float:
    """Clamp to something a reservation can be sized from. Never raises: this
    sits on the acquire path, where a bad number must cost precision rather
    than the verdict itself."""
    try:
        m = float(value)
    except (TypeError, ValueError):
        return 1.0
    if not math.isfinite(m):
        return 1.0
    return min(max(m, _MIN_RATE_MULTIPLIER), _MAX_RATE_MULTIPLIER)


# A lost release is not free: the reservation holds the entire ceiling it was
# granted until the TTL expires, and that is exactly the window a user who has
# just topped up spends trying to resume. Worth a couple of seconds of teardown
# to avoid, and no more — the TTL is the backstop either way. One waits between
# attempts, so the attempt count is this plus the first try.
_RELEASE_BACKOFF_SECONDS = (0.5, 1.5)
_RELEASE_ATTEMPTS = len(_RELEASE_BACKOFF_SECONDS) + 1

# The ceiling that makes "and no more" true. Retries and backoffs bound the
# attempts but not the wait: a service that accepts connections and then does
# not answer gives every attempt the shared client's full timeout, and the
# teardown this sits in is what ends the user's stream. Sized so a service that
# is merely failing still gets all three attempts, while one that is hanging
# costs a single client timeout instead of three plus the backoffs.
_RELEASE_BUDGET_SECONDS = 5.0

# Bounded separately from the shared client's default: this one runs inside
# startup, where a slow answer delays every worker's boot.
_CAPABILITY_TIMEOUT_SECONDS = 5.0


def _reportable_spend(value: Any, run_ref: str) -> Optional[float]:
    """The figure, or None when it is a metering fault rather than a spend.

    The lease and the ledger refuse the same values, and the ledger is the
    reason they must: its write is monotone and Postgres ranks NaN above every
    real numeric, so an unusable figure that lands once can never be lowered by
    a later correct one, and it carries through the per-user SUM to every other
    run that user has open.
    """
    try:
        spent = float(value)
    except (TypeError, ValueError):
        spent = float("nan")
    if not math.isfinite(spent) or spent > _MAX_REPORTABLE_SPEND:
        logger.error(
            "[CreditGate] not reporting spend %r for run %s — outside the "
            "range this deployment will record",
            value,
            run_ref,
        )
        return None
    return spent


def _token_hint() -> str:
    """Names the one acquire failure that no amount of retrying resolves."""
    return (
        "; INTERNAL_SERVICE_TOKEN is not set"
        if usage_limits.service_token_missing()
        else ""
    )


def _log_acquire_failure(status_code: int, run_ref: str, text: str) -> None:
    """Fail open either way, but say which failure it was."""
    if status_code in (401, 403):
        # This deployment cannot ask at all, so the gate is off for every run
        # on it and stays off until someone notices.
        logger.error(
            "[CreditGate] lease acquire rejected with %d — the runtime credit "
            "gate is inactive for every run on this deployment%s",
            status_code,
            _token_hint(),
        )
    elif status_code == 404:
        # Ambiguous on its own: an unknown principal and a route this build is
        # looking for in the wrong place answer the same way. The startup
        # capability check is what separates them.
        logger.error(
            "[CreditGate] lease acquire returned 404 for run %s — an unknown "
            "user, or the lease route is elsewhere in this deployment. Neither "
            "resolves by asking again.",
            run_ref,
        )
    else:
        logger.warning(
            "[CreditGate] lease acquire returned %d: %s", status_code, text[:200]
        )


def _ttl_seconds(body: dict) -> float:
    """Seconds until the grant lapses.

    ``expires_in_seconds`` wins over differencing ``expires_at``: the service
    differences it against its own clock, so the answer carries none of the skew
    between the two hosts. Subtracting an absolute stamp from a local ``now()``
    does, and skew big enough to eat the renew margin parks every lease inside
    its renew window for the whole turn.

    Falls back to a stand-in rather than to zero: an unreadable expiry read as
    "expires now" has exactly that effect.
    """
    remaining = body.get("expires_in_seconds")
    if isinstance(remaining, (int, float)) and not isinstance(remaining, bool):
        return max(float(remaining), 0.0)
    expires_at = body.get("expires_at")
    if not expires_at:
        return _UNKNOWN_TTL_SECONDS
    try:
        expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        return max((expiry - datetime.now(timezone.utc)).total_seconds(), 0.0)
    except Exception:
        # Naive timestamps land here: they parse, then the subtraction raises
        # because the other side is aware.
        logger.warning(
            "[CreditGate] lease expires_at unreadable (%r); assuming a %.0fs TTL",
            expires_at,
            _UNKNOWN_TTL_SECONDS,
        )
        return _UNKNOWN_TTL_SECONDS


def _normalize_verdict(body: Any) -> Optional[LeaseVerdict]:
    """The service's answer as the gate's own type, or None when it is not one.

    An unreadable answer is not an answer: collapsing it into ``granted=False``
    records a denial with no ceiling, which stops the turn at its first model
    boundary.
    """
    if not isinstance(body, dict) or "granted" not in body:
        logger.warning("[CreditGate] lease acquire body unusable: %.200s", body)
        return None
    # Strictly the boolean the contract declares. bool() would read the string
    # "false", or any non-empty string, as a grant.
    granted = body.get("granted")
    if not isinstance(granted, bool):
        logger.warning(
            "[CreditGate] lease acquire 'granted' not a boolean: %r", granted
        )
        return None
    ceiling = 0.0
    if granted:
        # A grant whose ceiling will not read as a number is not usable as one.
        try:
            ceiling = float(body.get("ceiling_credits") or 0.0)
        except (TypeError, ValueError):
            logger.warning(
                "[CreditGate] lease acquire ceiling unusable: %r",
                body.get("ceiling_credits"),
            )
            return None
    # Anything but a plain integer leaves that release unfenced, which is the
    # shape before the service offered a fence, not a failure. ``bool`` is an
    # ``int`` in Python and is not one of these.
    generation = body.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int):
        generation = None
    quota = body.get("quota")
    return LeaseVerdict(
        granted=granted,
        ceiling_credits=ceiling,
        ttl_seconds=_ttl_seconds(body),
        generation=generation,
        quota=quota if isinstance(quota, dict) else None,
    )


class PlatformCreditGatePort:
    """Lease + heartbeat operations for one deployment. Stateless."""

    def _url(self, path: str) -> str:
        return f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/billing/leases/{path}"

    async def acquire(
        self,
        user_id: str,
        run_ref: str,
        spent_credits: float,
        rate_multiplier: float = 1.0,
        byok: bool = False,
    ) -> Optional[LeaseVerdict]:
        """One lease acquire/extend attempt. None = no verdict (fail open):
        transport failure, any non-200, an unreadable body. A 200 is the
        service's answer — grant or denial — normalized for the gate.

        ``rate_multiplier`` is how much one model boundary on this turn costs
        against the baseline the service sizes reservations in. Advisory: the
        service decides what to do with it, and a build that sends nothing gets
        whatever default it already applied.

        ``byok`` travels for the same reason admission sends it, and on the same
        terms: the service cannot reproduce its own admission verdict without
        knowing whether the turn runs on a key the user pays for. Sent only when
        true, so the payload is unchanged for the platform-funded case.
        """
        spent = _reportable_spend(spent_credits, run_ref)
        if spent is None:
            # No verdict rather than a rejected request: the gate keeps
            # enforcing the ceiling it already holds, and nothing unusable
            # reaches the reservation.
            return None
        client = await usage_limits.get_http_client()
        body: dict[str, Any] = {
            "user_id": user_id,
            "run_ref": run_ref,
            "spent_credits": max(spent, 0.0),
            "rate_multiplier": _reportable_multiplier(rate_multiplier),
        }
        if byok:
            body["byok"] = True
        try:
            resp = await client.post(
                self._url("acquire"),
                json=body,
                headers=usage_limits.service_headers(),
            )
        except Exception as e:
            logger.warning("[CreditGate] lease service unreachable: %s", e)
            return None
        if resp.status_code != 200:
            _log_acquire_failure(resp.status_code, run_ref, resp.text)
            return None
        try:
            body = resp.json()
        except Exception:
            return None
        return _normalize_verdict(body)

    async def release(
        self, user_id: str, run_ref: str, generation: Optional[int] = None
    ) -> None:
        """Retire the run's reservation. Best-effort, but not one-shot.

        ``generation`` fences it against a lane that rejoined and re-acquired
        while this teardown was in flight: the release names the grant it is
        retiring, so it cannot retire one this teardown never held. Omitted,
        the release is unconditional and that race is live.

        Retried only for the failures a second attempt can fix. A 4xx is an
        answer — including the 404 that means there was nothing to release,
        which is the idempotent case, not an error.

        Bounded by a whole-teardown deadline rather than per attempt, because
        it is the total that the caller waits out: this runs from the lane's
        ``aclose``, so until it returns the turn's stream has stopped producing
        but has not ended.
        """
        client = await usage_limits.get_http_client()
        payload: dict[str, Any] = {"user_id": user_id, "run_ref": run_ref}
        if generation is not None:
            payload["generation"] = generation
        detail = "no attempt made"
        deadline = time.monotonic() + _RELEASE_BUDGET_SECONDS
        for attempt in range(_RELEASE_ATTEMPTS):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                detail = f"{detail}; teardown budget spent"
                break
            try:
                resp = await client.post(
                    self._url("release"),
                    json=payload,
                    headers=usage_limits.service_headers(),
                    timeout=remaining,
                )
                if resp.status_code < 500:
                    return
                detail = f"HTTP {resp.status_code}"
            except Exception as e:
                detail = str(e)
            if attempt < len(_RELEASE_BACKOFF_SECONDS):
                backoff = min(
                    _RELEASE_BACKOFF_SECONDS[attempt],
                    max(0.0, deadline - time.monotonic()),
                )
                await asyncio.sleep(backoff)
        logger.warning(
            "[CreditGate] lease release failed for run %s after %d attempts "
            "(%s); its reservation stands until the lease expires",
            run_ref,
            _RELEASE_ATTEMPTS,
            detail,
        )

    async def capability(self) -> Optional[dict]:
        """What the lease service reports about itself, or None if it did not
        answer. Read-only on both sides, so every worker asking at boot costs
        nothing."""
        client = await usage_limits.get_http_client()
        try:
            resp = await client.get(
                self._url("capability"),
                headers=usage_limits.service_headers(),
                timeout=_CAPABILITY_TIMEOUT_SECONDS,
            )
        except Exception as e:
            logger.warning("[CreditGate] capability check unreachable: %s", e)
            return None
        if resp.status_code != 200:
            logger.error(
                "[CreditGate] capability check returned %d — the runtime "
                "credit gate cannot reach its lease service at %s, so every "
                "long-running turn on this deployment goes ungated%s",
                resp.status_code,
                self._url("capability"),
                _token_hint(),
            )
            return None
        try:
            body = resp.json()
        except Exception:
            body = None
        if not isinstance(body, dict):
            logger.error("[CreditGate] capability body unusable: %s", resp.text[:200])
            return None
        return body

    async def heartbeat(
        self, kind: credit_ledger.RunKind, run_ref: str, credits: float
    ) -> Optional[bool]:
        """True when the row took the value, False when it is no longer open,
        None when there was nothing worth writing.

        Validated on the same terms as ``acquire``: a beat is the write that
        actually has to hold the line, since the ledger keeps whatever lands
        on it.
        """
        spent = _reportable_spend(credits, run_ref)
        if spent is None:
            return None
        return await credit_ledger.heartbeat(kind, run_ref, spent)


_port = PlatformCreditGatePort()


def build_run_credit_gate(
    user_id: str,
    run_id: str,
    token_callback: Any,
    tool_tracker: Any,
    model_name: Optional[str] = None,
    is_byok: bool = False,
) -> Optional[CreditGateState]:
    """The main lane's gate state, or None when platform gating is inactive.

    Also mints the turn's lease, which this lane and every subagent it
    spawns share. Admission (``enforce_credit_limit``) has already allowed
    this run by the time this is called, which is the gate's seed verdict —
    the first model boundary proceeds on it while the lease's first acquire
    is in flight.

    No gate without a principal: a heartbeat onto a ``user_id IS NULL`` row
    would spend against an aggregate that can never see it.
    """
    if not user_id or not usage_limits.platform_gating_active():
        return None
    return CreditGateState(
        run_ref=run_id,
        kind="run",
        port=_port,
        lease=CreditLease(
            user_id=user_id, run_ref=run_id, port=_port, is_byok=is_byok
        ),
        tracker=token_callback,
        tool_tracker=tool_tracker,
        rate_multiplier=_model_rate_multiplier(model_name, is_byok),
    )


def _model_rate_multiplier(model_name: Optional[str], is_byok: bool = False) -> float:
    """What one boundary on this model costs against the baseline, or 1.0.

    A BYOK turn pays its tokens to the user's own vendor account, so the only
    thing left to meter is infrastructure, and an infrastructure credit is
    computed from tool-use counts alone -- there is no model term anywhere in
    ``calculate_infrastructure_credits``. So the rate is not a large estimate
    of that spend, it is an unrelated one, and scaling the reservation by it
    sizes the ask on a quantity the spend cannot vary with.

    Resolved per turn rather than cached: the answer moves with the manifest,
    and for a model on hourly pricing it moves with the clock. Never raises —
    an unreadable rate costs the reservation its sizing, not the turn.
    """
    if is_byok or not model_name:
        return 1.0
    try:
        return chunk_multiplier(model_name) or 1.0
    except Exception:
        logger.warning(
            "[CreditGate] could not size a rate for %r; reserving at baseline",
            model_name,
            exc_info=True,
        )
        return 1.0


async def verify_credit_gate_wiring() -> None:
    """Startup assertion for the runtime credit gate. Never raises.

    The gate has two ways to die without producing a single error at request
    time. A lease route that is not where this build looks leaves every
    long-running turn ungated, and a granted TTL at or below the renew margin
    puts each lease inside the renew window the moment it is issued, so the
    refresher re-acquires for the whole turn — load with nothing to show for
    it. One GET answers both at boot instead of in a bill.

    Silent when gating is inactive: OSS builds have no service to ask.
    """
    if not usage_limits.platform_gating_active():
        return
    body = await _port.capability()
    if body is None:
        return  # the check logged what it found
    if not body.get("enabled"):
        logger.info(
            "[CreditGate] lease reservations are switched off on the quota "
            "service; turns run without a runtime credit gate"
        )
        return
    ttl = body.get("lease_ttl_seconds")
    if not isinstance(ttl, (int, float)) or isinstance(ttl, bool):
        logger.error("[CreditGate] capability reported no usable TTL: %r", ttl)
        return
    if ttl <= LEASE_RENEW_MARGIN_SECONDS:
        logger.error(
            "[CreditGate] lease TTL is %ss against a %ss renew margin — every "
            "lease arrives already due for renewal, so runs will re-acquire "
            "continuously instead of gating",
            ttl,
            LEASE_RENEW_MARGIN_SECONDS,
        )
        return
    lo = body.get("rate_multiplier_min")
    hi = body.get("rate_multiplier_max")
    if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
        if float(lo) != _MIN_RATE_MULTIPLIER or float(hi) != _MAX_RATE_MULTIPLIER:
            logger.error(
                "[CreditGate] multiplier bounds disagree: the service accepts "
                "[%s, %s], this build clamps to [%s, %s]. A multiplier outside "
                "the service's range is rejected, and a rejected acquire is no "
                "verdict, which leaves the turn ungated.",
                lo,
                hi,
                _MIN_RATE_MULTIPLIER,
                _MAX_RATE_MULTIPLIER,
            )
    # The service counts in credits and has no view of what one cost to
    # produce, so it cannot detect a build that converts at a different rate:
    # halving this reports half the credits for the same work, which from there
    # is indistinguishable from a cheaper turn. The check only exists here.
    if USD_TO_CREDITS_RATE != _CANONICAL_CREDIT_UNIT:
        logger.error(
            "[CreditGate] USD_TO_CREDITS_RATE is %s, not the canonical %s, so "
            "every spend this build reports is scaled %.4gx against the "
            "ceilings it is compared with.",
            USD_TO_CREDITS_RATE,
            _CANONICAL_CREDIT_UNIT,
            USD_TO_CREDITS_RATE / _CANONICAL_CREDIT_UNIT,
        )
    logger.info("[CreditGate] lease service ready (TTL %ss)", ttl)
