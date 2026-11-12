"""The finalize marks a completed run of a user with idle compaction on, and
that mark alone makes the run row owe the ``idle_compact`` job."""

from types import SimpleNamespace

import pytest

from src.server.database.runs.hook_jobs import IDLE_COMPACTION_META, build_finalize_jobs_from_run_row
from src.server.services.runs.finalization import assemble_finalize_artifacts
from src.server.services.runs.sse_producer import RunSSEProducer

_KEY = ("t-idle", "r-idle")


async def _finalized_metadata(agent_config) -> dict:
    handler = RunSSEProducer(thread_id=_KEY[0], run_id=_KEY[1])
    handler.agent_config = agent_config
    outcome = await assemble_finalize_artifacts(
        _KEY,
        metadata={"handler": handler},
        status="completed",
        phase="completed",
        interrupt_reason=None,
        pause_message=None,
        error=None,
        handler=handler,
        cancelled_by_user=False,
        workspace_id=None,
        user_id=None,
    )
    return outcome.metadata


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("agent_config", "owed"),
    [
        (SimpleNamespace(feature_enabled=lambda key: key == "idle_compaction"), True),
        (SimpleNamespace(feature_enabled=lambda key: False), False),
        (None, False),
    ],
)
async def test_the_users_flag_at_the_finalize_decides_the_idle_compaction_job(agent_config, owed):
    metadata = await _finalized_metadata(agent_config)
    assert (metadata.get(IDLE_COMPACTION_META) is True) is owed

    run = {
        "conversation_response_id": _KEY[1],
        "conversation_thread_id": _KEY[0],
        "run_seq": 1,
        "metadata": {"user_id": "u-1", **metadata},
    }
    jobs = build_finalize_jobs_from_run_row(run)("completed")
    assert ("idle_compact" in {job.hook_type for job in jobs}) is owed
