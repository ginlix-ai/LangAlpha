#!/usr/bin/env python3
"""Live pass: one reservation per turn, against the real lease service.

    docker exec -e PYTHONPATH=/app:/app/src <backend> \\
        /app/.venv/bin/python scripts/e2e/credit_family_pass.py <user_id>


Exercises the exact shape that broke — a parent lane and a subagent lane on a
small balance — and asserts the child never asks for its own reservation, the
family total is what gets reported, and the lease survives the parent's close
until the last child leaves. Releases the lease on every exit path, so a
failed run does not leave one standing for its whole TTL.
"""

import asyncio
import sys
import uuid

from ptc_agent.agent.middleware.credit_gate import CreditGateState, CreditLease
from src.server.services.credit_gate_port import PlatformCreditGatePort

if len(sys.argv) != 2:
    sys.exit("usage: credit_family_pass.py <user_id>")
USER_ID = sys.argv[1]
# Real UUIDs: a run ref IS a run's conversation_response_id, and the lease
# service parses it as one. A readable fixture string gets a 422 here, which
# the gate correctly fails open on — and a fail-open pass proves nothing.
RUN_REF = str(uuid.uuid4())
CHILD_REF = str(uuid.uuid4())


class Meter:
    def __init__(self, usd):
        self.usd = usd

    def platform_usd_total(self):
        return self.usd


class SpyPort(PlatformCreditGatePort):
    """The real port, with every acquire/release recorded."""

    def __init__(self):
        self.acquires = []
        self.releases = []
        self.heartbeats = []

    async def acquire(self, user_id, run_ref, spent_credits, rate_multiplier=1.0,
                      byok=False):
        verdict = await super().acquire(
            user_id, run_ref, spent_credits, rate_multiplier, byok
        )
        self.acquires.append((run_ref, spent_credits, rate_multiplier, byok, verdict))
        return verdict

    async def release(self, user_id, run_ref, generation=None):
        self.releases.append((run_ref, generation))
        return await super().release(user_id, run_ref, generation)

    async def heartbeat(self, kind, run_ref, credits):
        # Recorded, not performed. The ledger heartbeat writes to this
        # service's own run rows through a pool only the app lifespan opens,
        # and these refs have no row anyway — the lease is what this pass is
        # here to prove.
        self.heartbeats.append((kind, run_ref, credits))
        return True


def check(label, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {label}{'  ' + detail if detail else ''}")
    return ok


async def main():
    port = SpyPort()
    lease = CreditLease(user_id=USER_ID, run_ref=RUN_REF, port=port)
    parent = CreditGateState(
        run_ref=RUN_REF, kind="run", port=port, lease=lease, tracker=Meter(0.002)
    )
    ok = True
    try:
        await parent.start()
        await asyncio.sleep(3.0)  # let the lease's first acquire land

        granted = [v for *_, v in port.acquires if v and v.granted]
        ok &= check(
            "lease granted to the parent",
            bool(granted) and lease.ceiling_credits > 0,
            f"ceiling={lease.ceiling_credits}",
        )
        ok &= check("parent not denied", lease.denial is None)

        child = parent.spawn_child(
            run_ref=CHILD_REF, tracker=Meter(0.004), tool_tracker=None
        )
        await child.start()
        await asyncio.sleep(4.0)

        child_asks = [r for r, *_ in port.acquires if r == CHILD_REF]
        ok &= check(
            "child never asks for its own reservation",
            child_asks == [],
            f"child acquires={child_asks}",
        )
        ok &= check(
            "child is not denied against its own parent",
            lease.denial is None and child.lease is lease,
        )

        fam, p, c = lease.spend(), parent.spend(), child.spend()
        ok &= check(
            "family spend sums both lanes",
            abs(fam - (p + c)) < 1e-9 and c > 0,
            f"family={fam:.4f} parent={p:.4f} child={c:.4f}",
        )
        # Force a renewal: under the ceiling the lease would not re-ask on its
        # own, and a check that never triggers one proves nothing.
        before = len(port.acquires)
        lease._lease_deadline = 0.0
        await asyncio.sleep(3.0)
        renewed = port.acquires[before:]
        ok &= check(
            "a renewal happens and reports the family total, under the turn's ref",
            bool(renewed)
            and all(r == RUN_REF for r, *_ in renewed)
            and abs(renewed[-1][1] - fam) < 1e-6,
            f"renewals={[(r, round(s, 4)) for r, s, *_ in renewed]} family={fam:.4f}",
        )

        await parent.aclose()
        ok &= check(
            "parent close does not release while a child is live",
            port.releases == [],
            f"releases={port.releases}",
        )

        await child.aclose()
        ok &= check(
            "the last lane out releases",
            [r for r, _ in port.releases] == [RUN_REF],
            f"releases={port.releases}",
        )
        # The fence, against the live service rather than a stub: a release
        # that carries no generation is unconditional, which is the race it
        # exists to close.
        ok &= check(
            "the release is fenced on the grant it retires",
            bool(port.releases) and port.releases[-1][1] is not None,
            f"generation={port.releases[-1][1] if port.releases else None}",
        )
    finally:
        # Never leave a reservation standing, whatever happened above. Unfenced
        # on purpose: this is the backstop, and by here there is no grant left
        # worth protecting.
        await port.release(USER_ID, RUN_REF)
    print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


sys.exit(asyncio.run(main()))
