"""A chart's comments against a real Postgres: one record per chart, which
holds comments and nothing a file's record shares or versions, and whose
comments go only to threads in the chart's own workspace."""

from __future__ import annotations

import uuid

import psycopg
import pytest
import pytest_asyncio

from src.server.database import artifacts
from src.server.database.artifacts import DestinationUnavailable
from src.server.database.home_workspace import get_home_workspace_id

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

ANCHOR = {"kind": "chart", "marks": [{"type": "point", "time": "2026-10-03", "price": 118.4}]}


async def _thread(workspace_id: str) -> str:
    from src.server.database.conversation.threads_write import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(thread_id, workspace_id, "completed")
    return thread_id


@pytest_asyncio.fixture(loop_scope="session")
async def owner(patched_get_db_connection, test_db_pool):
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"chart-{uuid.uuid4().hex[:10]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com")
    ws = str((await create_workspace(user_id=user_id, name="NVDA", status="running"))["workspace_id"])
    other = str((await create_workspace(user_id=user_id, name="AMD", status="running"))["workspace_id"])
    home = str(
        (
            await create_workspace(
                user_id, "Home", workspace_id=get_home_workspace_id(user_id), status="unbound"
            )
        )["workspace_id"]
    )
    try:
        yield {"user": user_id, "ws": ws, "other": other, "home": home}
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM workspaces WHERE user_id = %s", (user_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def test_a_chart_has_one_record_beside_a_file_of_the_same_name(owner):
    first = await artifacts.ensure_record(owner["ws"], "chart", "NVDA:1day")
    again = await artifacts.ensure_record(owner["ws"], "chart", "NVDA:1day")
    assert (first.artifact_id, first.kind, first.path) == (again.artifact_id, "chart", "NVDA:1day")
    assert (await artifacts.find_record(owner["ws"], "chart", "NVDA:1day")).artifact_id == first.artifact_id
    assert await artifacts.find_record(owner["other"], "chart", "NVDA:1day") is None

    file = await artifacts.ensure_record(owner["ws"], "file", "NVDA:1day")
    assert file.kind == "file" and file.artifact_id != first.artifact_id
    # A read by id says which it is; a file's writes never take a chart.
    found = await artifacts.get_artifact(first.artifact_id, user_id=owner["user"])
    assert found.kind == "chart"
    with pytest.raises(artifacts.ArtifactNotFound):
        await artifacts.set_keep_updated(
            user_id=owner["user"], artifact_id=first.artifact_id, keep_updated=False
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "shared_at = NOW(), files = ARRAY[path]",
        "kept_at = NOW()",
        "keep_updated = TRUE",
        "audience = 'organization'",
        "title = 'NVDA'",
    ],
)
async def test_a_chart_record_is_never_shared_kept_or_versioned(owner, test_db_pool, assignment):
    chart = await artifacts.ensure_record(owner["ws"], "chart", "NVDA:1day")
    with pytest.raises(psycopg.errors.CheckViolation):
        async with test_db_pool.connection() as conn:
            await conn.execute(
                f"UPDATE share_links SET {assignment} WHERE artifact_id = %s",
                (chart.artifact_id,),
            )


async def test_a_chart_comment_waits_only_in_its_own_workspace(owner):
    chart = await artifacts.ensure_record(owner["ws"], "chart", "NVDA:1day")
    own, elsewhere, home = (
        await _thread(owner["ws"]),
        await _thread(owner["other"]),
        await _thread(owner["home"]),
    )
    for thread_id in (elsewhere, home):
        with pytest.raises(DestinationUnavailable):
            await artifacts.create_comment(
                user_id=owner["user"], artifact_id=chart.artifact_id, anchor=ANCHOR,
                body="why here?", delivery="queued", thread_id=thread_id, caller=owner["user"],
            )
    comment = await artifacts.create_comment(
        user_id=owner["user"], artifact_id=chart.artifact_id, anchor=ANCHOR,
        body="why here?", delivery="queued", thread_id=own, caller=owner["user"],
    )
    assert (comment.kind, comment.path, comment.number) == ("chart", "NVDA:1day", 1)
    assert (comment.file_sha256, comment.title, comment.thread_id) == (None, None, own)

    with pytest.raises(DestinationUnavailable):
        await artifacts.bind(
            user_id=owner["user"], artifact_id=chart.artifact_id, thread_id=home,
            caller=owner["user"],
        )
    bound = await artifacts.bind(
        user_id=owner["user"], artifact_id=chart.artifact_id, thread_id=own,
        caller=owner["user"],
    )
    assert bound.bound_thread_id == own

    found = await artifacts.destinations(
        user_id=owner["user"], artifact_id=chart.artifact_id, beside_thread_id=home,
        caller=owner["user"],
    )
    assert (found.beside.usable, found.beside.home) == (False, True)
    assert found.default[0].thread_id == own

    (queued,) = await artifacts.list_queued_comments(user_id=owner["user"], thread_id=own)
    assert (queued.kind, queued.workspace_id) == ("chart", owner["ws"])

    # A send from Home carries nothing of it; a send from its workspace does.
    assert await artifacts.claim_comments(
        user_id=owner["user"], thread_id=home, comment_ids=[comment.comment_id],
        request_key=str(uuid.uuid4()), caller=owner["user"],
    ) == []
    (claimed,) = await artifacts.claim_comments(
        user_id=owner["user"], thread_id=own, comment_ids=[comment.comment_id],
        request_key=str(uuid.uuid4()), caller=owner["user"],
    )
    assert (claimed.kind, claimed.path, claimed.anchor) == ("chart", "NVDA:1day", ANCHOR)

    reply = await artifacts.add_agent_reply(
        user_id=owner["user"], thread_id=own, comment_id=comment.comment_id,
        body="Drawn.", resolve=True,
    )
    assert (reply.kind, reply.path, reply.resolved) == ("chart", "NVDA:1day", True)


async def test_a_file_comment_still_reaches_home(owner):
    file = await artifacts.ensure_record(owner["ws"], "file", "report.html")
    home = await _thread(owner["home"])
    comment = await artifacts.create_comment(
        user_id=owner["user"], artifact_id=file.artifact_id,
        anchor={"kind": "text", "quote": "Revenue"}, body="check", delivery="queued",
        thread_id=home, caller=owner["user"],
    )
    assert (comment.kind, comment.thread_id) == ("file", home)
