"""Who sees a shared workspace's private thread, through every door, against
real PostgreSQL.

Maya started two threads in the Semis desk, a workspace that lists her, Lee
and Nate (a manager) and names the Semis team, where Teo is: one she keeps
private, one shared with the workspace. Ada (an organization admin outside
the team), Ray (removed from the list) and Olga (another organization) do not
reach the workspace; Pat has a personal workspace of their own. For each of
them and each door (the thread by id, the lists, the thread's folder through
the file routes, its transcript, a feed event, an open stream) the answer is
the thread rule's: the private thread is Maya's alone, the shared one is
everyone's the workspace reaches, and someone the workspace does not reach
learns nothing, by a 404 rather than a 403.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ORG, TEAM = "org-matrix", "team-semis"
MAYA, LEE, NATE, TEO = "mx-maya", "mx-lee", "mx-nate", "mx-teo"
ADA, RAY, OLGA, PAT = "mx-ada", "mx-ray", "mx-olga", "mx-pat"
HOLDER = "shared-00000000-0000-4000-8000-0000000000dd"
VIEWERS = (MAYA, LEE, NATE, TEO, ADA, RAY, OLGA, PAT)
#: Those the Semis desk reaches: listed, or in its team.
REACH = {MAYA, LEE, NATE, TEO}
ROOT = "/home/workspace"


@pytest.fixture(autouse=True)
def _tuple_rows(patched_get_db_connection, monkeypatch):
    from src.server.database import pool, thread_transcripts
    from tests.integration.test_steer_outbox_db import _app_rows

    _app_rows(monkeypatch)
    # The transcript reads index their rows by position, as the app's pool hands them.
    monkeypatch.setattr(thread_transcripts, "get_db_connection", pool.get_db_connection)


@pytest.fixture
def seats(monkeypatch):
    """Each person's account as the auth service would answer it; Pat's with
    the ``teams`` door shut, so it reads as no organization."""
    from src.server.services import membership
    from src.server.services.account_contract import Team
    from src.server.services.shared_workspaces import access

    known = {
        who: access.Seat(ORG) for who in (MAYA, LEE, NATE, ADA, RAY)
    } | {
        TEO: access.Seat(ORG, (Team(TEAM, "Semis", "member"),)),
        OLGA: access.Seat("org-elsewhere"),
    }

    async def caller_seat(caller):
        return known.get(caller)

    monkeypatch.setattr(access, "caller_seat", caller_seat)
    monkeypatch.setattr(membership, "teams_door_open", lambda: True)


async def _seed(conn) -> SimpleNamespace:
    ws, mine = uuid.uuid4(), uuid.uuid4()
    private, shared, own = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with conn.cursor() as cur:
        for uid in VIEWERS:
            await cur.execute("INSERT INTO users (user_id, name) VALUES (%s, %s)", (uid, uid))
        await cur.execute(
            "INSERT INTO users (user_id, auth_provider) VALUES (%s, 'shared_workspace')", (HOLDER,)
        )
        for workspace, owner, name in ((ws, HOLDER, "desk"), (mine, PAT, "mine")):
            await cur.execute(
                "INSERT INTO workspaces (workspace_id, user_id, name, dir_name, status)"
                " VALUES (%s, %s, %s, %s, 'running')",
                (workspace, owner, name, name),
            )
        await cur.execute(
            "INSERT INTO shared_workspaces (workspace_id, holder_id, org_id, created_by,"
            " team_id, team_since) VALUES (%s, %s, %s, %s, %s, now())",
            (ws, HOLDER, ORG, MAYA, TEAM),
        )
        for uid, role in ((MAYA, "member"), (LEE, "member"), (NATE, "manager")):
            await cur.execute(
                "INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)"
                " VALUES (%s, %s, %s, %s)",
                (ws, uid, role, MAYA),
            )
        for index, (thread, workspace, starter, open_) in enumerate(
            ((private, ws, MAYA, False), (shared, ws, MAYA, True), (own, mine, None, False))
        ):
            await cur.execute(
                "INSERT INTO conversation_threads (conversation_thread_id, workspace_id,"
                " current_status, thread_index, title, created_by, workspace_shared_at)"
                " VALUES (%s, %s, 'completed', %s, 'Orchid', %s, CASE WHEN %s THEN now() END)",
                (thread, workspace, index, starter, open_),
            )
            await cur.execute(
                "INSERT INTO thread_transcripts (conversation_thread_id, prefix, fingerprint)"
                " VALUES (%s, 'turns', 'f')",
                (thread,),
            )
            await cur.execute(
                "INSERT INTO thread_transcript_files (conversation_thread_id, prefix, path,"
                " user_id, sha256, byte_len, content) VALUES (%s, 'turns', 'turns/0001.jsonl',"
                " %s, %s, 2, '{}')",
                (thread, HOLDER, "0" * 64),
            )
    return SimpleNamespace(
        ws=str(ws), mine=str(mine), private=str(private), shared=str(shared), own=str(own)
    )


def _sees(s) -> dict[str, set[str]]:
    """The threads each viewer may see: the whole truth table."""
    return {
        MAYA: {s.private, s.shared},
        LEE: {s.shared},
        NATE: {s.shared},
        TEO: {s.shared},
        ADA: set(),
        RAY: set(),
        OLGA: set(),
        PAT: {s.own},
    }


async def _status(check) -> int:
    try:
        await check
    except HTTPException as e:
        return e.status_code
    return 200


async def test_the_thread_by_id_and_the_lists(patched_get_db_connection, cleanup_tables, seats):
    from src.server.database.conversation.viewer_threads import (
        get_thread_for_viewer,
        threads_with_shared,
        workspace_threads_for_member,
    )
    from src.server.database.search.read import search_threads
    from src.server.database.workspace import get_workspace
    from src.server.services.shared_workspaces.access import thread_access, workspace_access

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    sees = _sees(s)
    ids = lambda rows: {str(r["conversation_thread_id"]) for r in rows}  # noqa: E731

    for who in VIEWERS:
        for thread in (s.private, s.shared):
            want = 200 if thread in sees[who] else 404
            assert await _status(thread_access(thread, who)) == want, (who, thread)
            # The read by id a list row opens, for whoever reaches the workspace.
            if who in REACH:
                assert bool(await get_thread_for_viewer(thread, who)) == (want == 200), who
        # Someone else's own workspace keeps the owner check's 403.
        assert await _status(thread_access(s.own, who)) == (200 if who == PAT else 403), who

        reached = await _status(workspace_access(await get_workspace(s.ws), who))
        assert reached == (200 if who in REACH else 404), who
        if who not in REACH:
            continue
        page, _ = await workspace_threads_for_member(s.ws, who)
        recents, _ = await threads_with_shared(who, [s.ws])
        found = await search_threads(who, ["Orchid"], limit=20, shared=[s.ws])
        assert ids(page) == sees[who], who
        assert ids(recents) & {s.private, s.shared} == sees[who], who
        assert {m.thread_id for m in found} & {s.private, s.shared} == sees[who], who
    page, _ = await threads_with_shared(PAT, [])
    assert ids(page) == {s.own}


STORED = ("notes.md", "{p}/request.md", "{s}/request.md")


@pytest.fixture
def files(monkeypatch):
    """The file routes on a sandbox that holds ``STORED`` for whichever
    workspace is asked, with access, the workspace row and the thread folder
    rule read from the database."""
    from src.server.app.workspace_files import _shared, crud
    from src.server.database.workspace import get_workspace
    from src.server.services.persistence.sync_result import SyncResult, UnsavedFile
    from src.server.services.workspace_file_containment import ContainedFile

    stored: list[str] = []
    sandbox = MagicMock()
    sandbox.is_ready.return_value = True
    sandbox.validate_path.return_value = True
    sandbox.virtualize_path = lambda p: p
    sandbox.aglob_files = AsyncMock(side_effect=lambda *a, **k: [f"{ROOT}/{p}" for p in stored])

    @asynccontextmanager
    async def acquire(workspace_id, _caller):
        yield sandbox, await get_workspace(workspace_id)

    monkeypatch.setattr(crud, "acquire_sandbox_to_change", acquire)
    for module in (crud, _shared):
        monkeypatch.setattr(module, "owner_work_dir", lambda _ws: ROOT)
        monkeypatch.setattr(module, "owner_layout", lambda _ws: SimpleNamespace(workspace=ROOT))
    monkeypatch.setattr(crud, "_virtual_files_in_scope", AsyncMock(return_value=[]))
    monkeypatch.setattr(crud, "get_vault_secrets_for_redaction", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        crud, "contained_sandbox_path", AsyncMock(side_effect=lambda _s, p, work_dir: p)
    )
    monkeypatch.setattr(
        crud, "_read_contained_target",
        AsyncMock(side_effect=lambda _s, path, *_a, **_k: ContainedFile(
            canonical=f"{ROOT}/{path}", size=2, content=b"hi"
        )),
    )
    monkeypatch.setattr(
        crud.FilePersistenceService, "list_sandbox_files",
        AsyncMock(side_effect=lambda *a, **k: {p: {"file_size": 1, "mtime": 1.0} for p in stored}),
    )
    monkeypatch.setattr(
        crud.FilePersistenceService, "sync_to_db",
        AsyncMock(side_effect=lambda *a, **k: SyncResult(
            unsaved=[UnsavedFile(p, "too_large", 9) for p in stored]
        )),
    )
    return SimpleNamespace(crud=crud, stored=stored)


async def test_a_threads_folder_through_the_file_routes(
    patched_get_db_connection, cleanup_tables, seats, files
):
    """Read, list, backup status and backup, for each thread's folder."""
    crud = files.crud
    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    sees = _sees(s)
    folder = {t: f".agents/threads/{t[:8]}" for t in (s.private, s.shared, s.own)}

    async def answers(who: str, ws: str, threads: tuple[str, ...]) -> dict[str, object]:
        files.stored[:] = ["notes.md", *(f"{folder[t]}/request.md" for t in threads)]
        out: dict[str, object] = {}
        for t in threads:
            out[f"read {t}"] = await _status(crud.read_workspace_file(
                workspace_id=ws, x_user_id=who, path=f"{folder[t]}/request.md",
                offset=0, limit=10, unlimited=True,
            ))
        try:
            listed = (await crud.list_workspace_files(
                workspace_id=ws, x_user_id=who, path=".", include_system=True,
                pattern="**/*", wait_for_sandbox=False, auto_start=False,
            ))["files"]
            status = await crud.get_backup_status(workspace_id=ws, x_user_id=who)
            backup = await crud.backup_workspace_files(workspace_id=ws, x_user_id=who)
        except HTTPException as e:
            out["routes"] = e.status_code
            return out
        out["listed"] = {t for t in threads if f"{folder[t]}/request.md" in listed}
        out["status"] = {t for t in threads if f"{folder[t]}/request.md" in status["untracked"]}
        out["backup"] = {
            t for t in threads
            if f"{folder[t]}/request.md" in {f["path"] for f in backup["unsaved"]}
        }
        return out

    for who in VIEWERS:
        ws_threads = (s.private, s.shared)
        got = await answers(who, s.ws, ws_threads)
        if who not in REACH:
            assert got == {f"read {t}": 404 for t in ws_threads} | {"routes": 404}, who
            continue
        seen = sees[who] & set(ws_threads)
        assert got == {
            **{f"read {t}": 200 if t in seen else 404 for t in ws_threads},
            "listed": seen, "status": seen, "backup": seen,
        }, who

    got = await answers(PAT, s.mine, (s.own,))
    assert got == {f"read {s.own}": 200, "listed": {s.own}, "status": {s.own}, "backup": {s.own}}
    # Another's own workspace answers with the owner check's 403.
    assert (await answers(MAYA, s.mine, (s.own,)))["routes"] == 403


def _served_on_computer(workspace_id: str, layout, owner: str):
    """The transcripts a computer's agents read for one workspace folder,
    whoever owns it."""
    from src.server.services.livefs.history_files import TranscriptFiles

    return TranscriptFiles(workspace_id, layout)


async def test_a_threads_transcript(patched_get_db_connection, cleanup_tables, seats):
    """The computer's agents read the shared thread's transcript and none
    kept private; a turn in the private thread reads it only as Maya's."""
    from ptc_agent.core.paths import SandboxLayout
    from src.server.services.livefs.history_files import private_thread_history

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    sees = _sees(s)
    machine = SimpleNamespace(computer_root="/home/daytona")

    async def listed(route) -> set[str]:
        paths = await route.aglob_paths("**/*")
        return {t for t in (s.private, s.shared, s.own) for p in paths if f"/{t[:8]}/" in p}

    for ws, folder, owner, want in (
        (s.ws, "desk", HOLDER, {s.shared}),
        (s.mine, "mine", PAT, {s.own}),
    ):
        layout = SandboxLayout.for_root(machine.computer_root).for_workspace(folder)
        assert await listed(_served_on_computer(ws, layout, owner)) == want

    for who in VIEWERS:
        for thread in (s.private, s.shared):
            history = await private_thread_history(s.ws, "desk", thread, who)
            reads = set()
            if history is not None:
                for route in history.routes(machine):
                    reads |= await listed(route)
            # Only a thread kept private comes this way, and only to its starter.
            want = {thread} if (thread == s.private and thread in sees[who]) else set()
            assert reads == want, (who, thread)


@pytest.fixture
def published(monkeypatch):
    """Every event put on a Redis channel, as each feed connection would
    receive it."""
    from src.server.services import thread_lifecycle_feed

    sent: list[dict] = []

    async def publish(channel, message):
        sent.append(json.loads(message))

    cache = SimpleNamespace(enabled=True, client=SimpleNamespace(publish=publish))
    monkeypatch.setattr(thread_lifecycle_feed, "get_cache_client", lambda: cache)
    return sent


async def test_a_feed_event(patched_get_db_connection, cleanup_tables, seats, published):
    """A title change tells only those who see the thread; a comment's tells
    everyone the workspace reaches and names the thread only to them."""
    from src.server.services.artifacts.events import publish_comment_change
    from src.server.services.shared_workspaces.feed import SharedFeed
    from src.server.services.thread_lifecycle_feed import publish_thread_title

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    sees = _sees(s)

    for thread in (s.private, s.shared):
        published.clear()
        await publish_thread_title(
            user_id=None, thread_id=thread, workspace_id=s.ws, title="Orchid", shared_org=ORG
        )
        await publish_comment_change(
            HOLDER, artifact_id="a1", workspace_id=s.ws, path="memo.md", thread_id=thread,
            comment_ids=["c1"],
        )
        title, comment = published
        for who in VIEWERS:
            feed = await SharedFeed.open(who)
            if who in (PAT, OLGA):
                # No organization here: the feed listens to no shared channel.
                assert feed is None or feed.reach.org_id != ORG, who
                continue
            told = [f for e in (title, comment) for f in await feed.on_event(dict(e))]
            types = {f["type"]: f for f in told}
            if who not in REACH:
                assert told == [], who
                continue
            assert ("thread_title" in types) == (thread in sees[who]), (who, thread)
            assert types["artifact_comment"]["thread_id"] == (
                thread if thread in sees[who] else None
            ), (who, thread)
            assert all("private_to" not in f for f in told), who


async def _frames(stream, n: int) -> list[str]:
    out = []
    async for frame in stream:
        out.append(frame)
        if len(out) == n:
            break
    return out


async def test_an_open_stream(patched_get_db_connection, cleanup_tables, seats, monkeypatch):
    """Everyone who sees the shared thread opens its task stream; once Maya
    makes it private, every stream but hers ends at its next frame. The
    private thread's stream opens for Maya alone."""
    from src.server.app.threads import tasks
    from src.server.database.conversation import thread_reach
    from src.server.handlers.chat import task_run_sse_reader

    async with patched_get_db_connection() as conn:
        s = await _seed(conn)
    sees = _sees(s)
    flipped = asyncio.Event()

    async def frames(thread_id, task_id, last_event_id):
        yield "frame 1"
        await flipped.wait()
        # Past any per-frame recheck interval, as a turn's next token is.
        await asyncio.sleep(2.5)
        yield "frame 2"
        yield "frame 3"

    monkeypatch.setattr(task_run_sse_reader, "stream_task_run_sse", frames)

    async def open_(thread: str, who: str):
        return await tasks.stream_subagent_task(
            thread_id=thread, task_id="t1", x_user_id=who, last_event_id=None,
            last_event_id_header=None,
        )

    for who in VIEWERS:
        want = 200 if s.private in sees[who] else 404
        assert await _status(open_(s.private, who)) == want, who

    streams = {}
    for who in VIEWERS:
        if s.shared in sees[who]:
            streams[who] = (await open_(s.shared, who)).body_iterator
        else:
            assert await _status(open_(s.shared, who)) == 404, who
    assert set(streams) == {MAYA, LEE, NATE, TEO}
    for stream in streams.values():
        assert await _frames(stream, 1) == ["frame 1"]
    assert (await thread_reach.set_shared(s.shared, MAYA, False))[0] == "done"
    flipped.set()
    rest = await asyncio.gather(*(_frames(stream, 2) for stream in streams.values()))
    assert dict(zip(streams, rest)) == {
        MAYA: ["frame 2", "frame 3"], LEE: [], NATE: [], TEO: [],
    }
