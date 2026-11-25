"""A turn in a thread its sender keeps private in a shared workspace reads
that thread's own transcript where a summary's pointer names it.

A shared workspace's computer serves only threads shared with the workspace,
through its mount and through its in-process routes alike. The turn's own
route is rooted at the directory the pointer prints, so the file tools answer
there whatever serves the computer, and every read it makes names the thread
and the sender, which the store checks against the thread row. The turn's
reach is that route, not the mount, so Bash is never sent for the files.
Nothing else changes: no other thread, no other sender, not the mount.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.filesystem_routes import IdentityGates, build_filesystem_backend
from ptc_agent.agent.transcript import Window
from ptc_agent.agent.transcript.pointer import aexport_transcript, shell_reads_transcripts
from ptc_agent.agent.transcript.store import TranscriptTarget
from ptc_agent.core.sandbox.livefs_mount import TranscriptReach
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.path_fold import resolve_agent_path, virtual_agent_path
from src.server.database import thread_transcripts as db
from src.server.database.conversation import thread_reach
from src.server.database.shared_workspaces import HOLDER_PREFIX
from src.server.services.livefs import history_files
from src.server.services.livefs.history_files import (
    ComputerHistory,
    ThreadFiles,
    ThreadHistory,
    TranscriptFiles,
    private_thread_history,
)
from src.server.services.livefs.tree import POINTS
from src.server.services.shared_workspaces.turn import TurnIdentity
from src.server.services.thread_agent import AgentInputs

ROOT, FOLDER = "/home/workspace", "/home/workspace/Team"
LAYOUT = SandboxLayout(ROOT).for_workspace("Team")
WS = "11111111-1111-1111-1111-111111111111"
OWN = "aaaa1111-0000-0000-0000-000000000001"
OTHER = "bbbb2222-0000-0000-0000-000000000002"
ANN, LEE = "ann", "lee"
HOLDER = f"{HOLDER_PREFIX}team"

# Where a summary in the thread, and one in its subagent's run, sends Grep.
POINTER = TranscriptTarget.for_agent(OWN).directory
TASK_POINTER = TranscriptTarget.for_agent(OWN, "task:t1").directory


def _sandbox() -> MagicMock:
    """The shared computer's own files, as its mount shows them: nothing of
    a thread kept private."""
    sb = MagicMock()
    sb.computer_root = ROOT
    sb.workspace_dir = FOLDER
    sb.normalize_path.side_effect = lambda p: resolve_agent_path(
        p, workspace=FOLDER, root=ROOT, allowed=[ROOT]
    )
    sb.virtualize_path.side_effect = lambda p: virtual_agent_path(p, workspace=FOLDER, root=ROOT)
    sb.validate_path.return_value = True
    sb.filesystem_config.enable_path_validation = True
    sb.aglob_paths = AsyncMock(return_value=[])
    sb.agrep_rich = AsyncMock(return_value=[])
    sb.aread_range = AsyncMock(return_value=None)
    return sb


def _files(thread_id: str, sender: str):
    composite, _ = build_filesystem_backend(
        backend=_sandbox(),
        gates=IdentityGates(*([False] * 7)),
        store=None,
        user_id=HOLDER,
        workspace_id=WS,
        layout=LAYOUT,
        history_routes=ThreadHistory(WS, "Team", thread_id, sender).routes(
            SimpleNamespace(computer_root=ROOT)
        ),
    )
    return composite


@pytest.fixture
def rows(monkeypatch):
    """The thread's stored transcript, a turn and a subagent run."""
    listed = [
        db.ListedFile(OWN, OWN[:8], path, f"d{n}", 20, HOLDER)
        for n, path in enumerate(["turn-0001.jsonl", "tasks/t1/run-0001.jsonl"])
    ]
    calls = {
        "list_transcript_tree": AsyncMock(return_value=listed),
        "load_transcript_contents": AsyncMock(
            side_effect=lambda files: {f: f"needle in {f.path}\n".encode() for f in files}
        ),
        "load_transcript_file": AsyncMock(
            return_value=db.StoredFile(HOLDER, "d0", b"needle in turn-0001.jsonl\n")
        ),
    }
    for name, mock in calls.items():
        monkeypatch.setattr(db, name, mock)
    return calls


@pytest.mark.asyncio
async def test_the_threads_own_turn_lists_searches_and_reads_it_at_the_pointers_path(rows):
    files = _files(OWN, ANN)
    turn = f"{FOLDER}/{POINTER}/turn-0001.jsonl"
    run = f"{FOLDER}/{TASK_POINTER}/run-0001.jsonl"

    assert set(await files.agrep_rich("needle", path=files.normalize_path(POINTER))) == {run, turn}
    assert set(await files.aglob_paths("**/*.jsonl", files.normalize_path(POINTER))) == {run, turn}
    assert await files.aread_range(files.normalize_path(f"{POINTER}/turn-0001.jsonl")) == (
        "needle in turn-0001.jsonl\n"
    )
    # A subagent's summary points inside the thread's directory.
    found = await files.agrep_rich(
        "needle", path=files.normalize_path(TASK_POINTER), output_mode="content"
    )
    assert found == [f"{run}:1:needle in tasks/t1/run-0001.jsonl"]
    # Every read is the thread's, as its sender reaches it, never the
    # computer's view.
    for name in ("list_transcript_tree", "load_transcript_file"):
        assert {c.kwargs["scope"] for c in rows[name].await_args_list} == {db.own_thread(OWN, ANN)}


@pytest.mark.asyncio
async def test_a_grep_of_the_transcripts_folder_finds_the_threads_own(rows):
    """Where the mount serves transcripts, a Grep of their folder follows its
    link into every thread's directory; this thread's own is found there too,
    as Glob finds it, and not from further up, where rg does not follow it."""
    files = _files(OWN, ANN)
    turn = f"{FOLDER}/{POINTER}/turn-0001.jsonl"
    folder = POINTER.rsplit("/", 1)[0]

    assert turn in await files.agrep_rich("needle", path=files.normalize_path(folder))
    assert turn in await files.aglob_paths("**/*.jsonl", files.normalize_path(folder))
    assert await files.agrep_rich("needle", path=files.normalize_path(".agents")) == []


@pytest.mark.asyncio
async def test_a_turn_in_another_thread_does_not_reach_this_one(rows):
    files = _files(OTHER, ANN)

    assert files.route_for(f"{POINTER}/turn-0001.jsonl") is None
    assert await files.aread_range(files.normalize_path(f"{POINTER}/turn-0001.jsonl")) is None
    rows["load_transcript_file"].assert_not_awaited()


@pytest.mark.parametrize(
    "private_to, sender, served",
    [
        (ANN, ANN, True),
        # A colleague's turn: the thread is not theirs.
        (ANN, LEE, False),
        # Shared with the workspace, or no such thread: the mount serves it
        # as it serves any.
        (None, ANN, False),
    ],
)
@pytest.mark.asyncio
async def test_only_the_starters_turn_in_a_private_thread_gets_the_route(
    monkeypatch, private_to, sender, served
):
    monkeypatch.setattr(thread_reach, "private_to", AsyncMock(return_value=private_to))

    history = await private_thread_history(WS, "Team", OWN, sender)

    if not served:
        assert history is None
        return
    [route] = history.routes(SimpleNamespace(computer_root=ROOT + "/"))
    assert isinstance(route, ThreadFiles)
    assert route.root_prefix == f"{FOLDER}/{POINTER}/"
    assert history.serves(WS) and not history.serves("another-workspace")


@pytest.mark.asyncio
async def test_the_turn_saves_and_points_at_the_transcript_where_its_file_tools_read_it(
    monkeypatch,
):
    from src.server.services import transcripts

    save = AsyncMock(side_effect=lambda target, messages, *, window, reader: target)
    monkeypatch.setattr(transcripts, "save_live", save)
    mount = MagicMock()
    mount.ready = AsyncMock(return_value=False)
    sandbox = MagicMock(transcripts=TranscriptReach(mount, shell=True), livefs=mount)
    history = ThreadHistory(WS, "Team", OWN, ANN)
    backend = SandboxBackend(sandbox, ROOT, history=history)
    target = TranscriptTarget.for_agent(OWN)

    assert shell_reads_transcripts(backend) is False
    got = await aexport_transcript(backend, target, [], workspace_id=WS, window=Window())

    assert got == target
    save.assert_awaited_once()
    # Neither saved through the mount nor held up by its links.
    mount.ready.assert_not_awaited()
    mount.save_transcript.assert_not_called()
    # Without the thread's history the computer's reach stands, as before.
    assert SandboxBackend(sandbox, ROOT).transcripts.home is mount


def test_the_mount_and_the_computers_routes_serve_what_the_computer_serves():
    sandbox = SimpleNamespace(computer_root=ROOT)
    [mounted] = [p for p in POINTS if p.name == "transcripts"]
    unmounted = ComputerHistory("c-1", [(WS, "Team")]).routes(sandbox)

    assert mounted.history("c-1", WS, LAYOUT).scope == db.ON_COMPUTER
    assert [r._route.scope for r in unmounted if isinstance(r, TranscriptFiles)] == [
        db.ON_COMPUTER
    ]
    # Decided by the workspace's own row: a personal workspace's every thread,
    # a shared one's once shared with it.
    assert "workspace_shared_at IS NOT NULL OR NOT EXISTS" in db.ON_COMPUTER.condition


@pytest.mark.parametrize("owner, read", [(HOLDER, True), ("user-owner", False)])
@pytest.mark.asyncio
async def test_a_turn_asks_for_the_route_only_in_a_shared_workspace(monkeypatch, owner, read):
    thread_route = AsyncMock(return_value=MagicMock(name="history"))
    monkeypatch.setattr(history_files, "private_thread_history", thread_route)
    manager = MagicMock()
    manager.tool_view.return_value = MagicMock(trading_rule=None)
    with (
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.services.workspace_layout.resolve_project_context",
            AsyncMock(return_value=None),
        ),
        patch("src.server.services.egress.direct_tools.direct_tools_for_turn", AsyncMock()),
    ):
        inputs = await AgentInputs.read(
            SimpleNamespace(computer_id="c-1"),
            identity=TurnIdentity(ANN, owner, ANN),
            workspace_id=WS,
            thread_id=OWN,
            role="analyst",
            user_profile=None,
        )

    assert (inputs.build_kwargs()["thread_history"] is not None) is read
    assert thread_route.await_count == int(read)
