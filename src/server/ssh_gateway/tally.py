"""Events on the machines' address too frequent to log one by one.

Anyone may connect there, scanners included, so a line per refused
connection or login would let a stranger write the gateway's log as fast as
it can connect.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter

REPORT_EVERY_S = 60


class Tally:
    """Events counted by reason and logged summed at most every
    REPORT_EVERY_S, each reason with its latest example. The first after a
    quiet spell is logged at once, so a single refusal still shows up when it
    happens, and the rest when the interval is up.

    ``reason`` must come from a fixed set: each one is kept until the next
    report.
    """

    def __init__(self, logger: logging.Logger, level: int, summary: str) -> None:
        self._logger = logger
        self._level = level
        # With one %d, for the total.
        self._summary = summary
        self._counts: Counter[str] = Counter()
        self._latest: dict[str, str] = {}
        self._logged_at = float("-inf")
        self._report_due: asyncio.TimerHandle | None = None

    def add(self, reason: str, example: str) -> None:
        self._counts[reason] += 1
        self._latest[reason] = example
        if self._report_due is not None:
            return
        wait = self._logged_at + REPORT_EVERY_S - time.monotonic()
        if wait <= 0:
            self._report()
        else:
            self._report_due = asyncio.get_running_loop().call_later(wait, self._report)

    def _report(self) -> None:
        self._report_due = None
        self._logger.log(
            self._level,
            "%s: %s",
            self._summary % self._counts.total(),
            ", ".join(
                f"{n} {reason} (latest {self._latest[reason]})"
                for reason, n in self._counts.most_common()
            ),
        )
        self._counts.clear()
        self._latest.clear()
        self._logged_at = time.monotonic()
