"""Sticky origin-meta resolution (Codex 2.3 round-12 F3).

``_resolve_origin_meta`` is the single source for a run's watching-flash
identity AND its dispatch generation: the dispatch POST supplies them,
follow-up turns (HITL resume, user continuation — public requests with no
origin fields) inherit them from the previous attempt's durable metadata.
Both the START txn metadata and the tracker re-mark consume this one
resolution — a re-mark from the raw request field would wipe the admission
stamp to None on every public follow-up and make the live admitted run
read as unadmitted to the fenced-teardown probe.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.chat.ptc_run import _resolve_origin_meta


def _request(**fields):
    base = {"origin_flash_thread_id": None, "origin_dispatch_gen": None}
    base.update(fields)
    return SimpleNamespace(**base)


@pytest.mark.asyncio
async def test_dispatch_post_supplies_both_fields():
    req = _request(origin_flash_thread_id="flash-1", origin_dispatch_gen="g-1")
    meta = await _resolve_origin_meta(req, "ptc-1")
    assert meta == {
        "origin_flash_thread_id": "flash-1",
        "origin_dispatch_gen": "g-1",
    }


@pytest.mark.asyncio
async def test_public_follow_up_inherits_from_previous_attempt():
    """HITL resume / continuation carries no origin fields — the stamp stays
    sticky via the previous attempt's durable metadata, generation included."""
    req = _request()
    prev = {
        "metadata": {
            "origin_flash_thread_id": "flash-1",
            "origin_dispatch_gen": "g-1",
        }
    }
    with patch(
        "src.server.database.runs.lifecycle.get_latest_attempt",
        AsyncMock(return_value=prev),
    ):
        meta = await _resolve_origin_meta(req, "ptc-1")
    assert meta == {
        "origin_flash_thread_id": "flash-1",
        "origin_dispatch_gen": "g-1",
    }


@pytest.mark.asyncio
async def test_origin_less_thread_resolves_none():
    """A genuinely origin-less thread (foreground-born, no dispatch lineage)
    resolves both fields to None — correctly foreign to any prober."""
    req = _request()
    with patch(
        "src.server.database.runs.lifecycle.get_latest_attempt",
        AsyncMock(return_value=None),
    ):
        meta = await _resolve_origin_meta(req, "ptc-1")
    assert meta == {
        "origin_flash_thread_id": None,
        "origin_dispatch_gen": None,
    }
