"""What ``claim_probe_kick`` hands back, which is the fence its probe writes under.

The throttle itself is pinned in ``tests/unit/server/services/test_mcp_discovery_schedule.py``;
this is the other half of the claim, the stamp it wrote. A probe fences its
snapshot write on that stamp because the discovery fingerprint hashes
``${vault:NAME}`` refs and never their values: a rotated secret leaves the
fingerprint identical, so only this clock separates the probe that dialled with
the old value from the one the rotation scheduled.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.mcp_servers import claim_probe_kick

STAMP = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def claim():
    """The UPDATE's RETURNING, swappable per test, plus the statement it ran."""
    state = {"row": (STAMP,), "statements": []}
    cursor = AsyncMock()

    async def _execute(sql, params=None):
        state["statements"].append(sql)

    cursor.execute = AsyncMock(side_effect=_execute)
    cursor.fetchone = AsyncMock(side_effect=lambda: state["row"])

    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_connection(conn_in=None):
        yield conn

    with patch(
        "src.server.database.mcp_servers.get_db_connection", new=_fake_connection
    ):
        yield state


@pytest.mark.asyncio
async def test_a_won_claim_returns_the_stamp_it_wrote(claim):
    assert await claim_probe_kick("u1", "authy") is STAMP


@pytest.mark.asyncio
async def test_a_refused_claim_still_reads_falsy(claim):
    """The callers test the answer for truth, and a throttled kick the clock
    turned down updated no row."""
    claim["row"] = None

    assert await claim_probe_kick("u1", "authy", throttle_s=120.0) is None


@pytest.mark.asyncio
async def test_the_stamp_comes_back_from_the_statement_that_wrote_it(claim):
    """A re-read afterwards would hand back whatever a sibling worker stamped
    in between, which is the one value this fence must not be given."""
    await claim_probe_kick("u1", "authy")

    (sql,) = claim["statements"]
    assert "RETURNING probe_kicked_at" in sql
