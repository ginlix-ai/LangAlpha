"""Which store route a mount path reaches, and what the mount may do there.

The daemon trusts these answers as a filesystem: a listing's size and
version must match what a read returns, a directory's writable flag decides
whether create is refused up front, and a refusal's code becomes the errno a
program sees. Past threads are renders of server state, so they refuse every
change, and a workspace path reaches only the user's own workspaces on this
computer. An
automation's file is a row in Postgres, so its version is the route's own, not
a hash of the content, a save takes its defaults from the command's context,
and a move within its folder renames the file, not the automation. The
sandbox links each directory in where the file tools show the same files.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock

import psycopg
import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.db_json_route import Plan
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime.protocol import INLINE_MAX_BYTES
from src.server.database import workspace as workspace_db
from src.server.database.thread_transcripts import ListedFile, StoredFile
from src.server.database.workspace_folders import MOVING_DIR
from src.server.services.automations.file import (
    AutomationFile,
    Document,
    FilePlan,
    _Create,
    _Delete,
    is_file_name,
)
from src.server.services.livefs.routes import LivefsError
from src.server.services.livefs.tokens import LivefsIdentity
from src.server.services.livefs.tree import LivefsTree

ROOT = "/home/workspace"
USER = "user-1"
COMPUTER = "computer-1"
HERE = "ws-here"  # on this computer, folder "research"
ELSEWHERE = "ws-elsewhere"  # the user's, on another computer
STRANGER = "ws-stranger"  # another user's
SECOND = "ws-second"  # on this computer, folder "second"
WORKSPACES = {
    workspace_id: {
        "workspace_id": workspace_id,
        "user_id": user_id,
        "computer_id": computer_id,
        "dir_name": dir_name,
    }
    for workspace_id, user_id, computer_id, dir_name in (
        (HERE, USER, COMPUTER, "research"),
        (ELSEWHERE, USER, "computer-2", "notes"),
        (STRANGER, "user-2", COMPUTER, "theirs"),
        (SECOND, USER, COMPUTER, "second"),
    )
}
T1 = "11111111-0000-0000-0000-000000000000"
T2 = "22222222-0000-0000-0000-000000000000"
SHORT = T1[:8]
THREADS = {HERE: T1, STRANGER: T2}
# Non-ASCII on purpose: a size counted in characters would pass on ASCII.
TRANSCRIPT = {
    "manifest.json": json.dumps({"thread_id": T1}, ensure_ascii=False),
    "turn-0001.jsonl": '{"role": "user", "content": "résumé 市场"}\n',
    "tasks/k1/meta.json": '{"description": "价格"}',
    "tasks/k1/run-0001.jsonl": '{"role": "ai", "content": "✓"}\n',
}
NOW = datetime(2026, 9, 26, tzinfo=timezone.utc)
FILE_NAME = "morning-brief.json"
AUTOMATIONS = f"user/automations/{FILE_NAME}"
SERVED = '{"name": "Morning brief", "status": "active"}\n'
EDITED = '{"name": "Morning brief", "status": "paused"}\n'
ROWS_VERSION = "sha256:rows-v1"
REPORT = 'Saved morning-brief.json: updated "Morning brief": paused'
DOCUMENT = Document(fields={}, shown=None, timezone="UTC", model_pref=None)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _thread_row(thread_id: str) -> dict:
    return {
        "conversation_thread_id": thread_id,
        "title": "市场研究",
        "workspace_id": HERE,
        "workspace_name": "Research",
        "dir_name": "research",
        "created_at": NOW,
        "updated_at": NOW,
        "has_transcript": True,
    }


async def _refusal(attempt) -> LivefsError:
    with pytest.raises(LivefsError) as caught:
        await attempt()
    return caught.value


async def _files(tree: LivefsTree, path: str):
    """Every file under ``path`` with the listing entry it was found by."""
    for entry in (await tree.list(path)).entries:
        child = f"{path}/{entry['name']}"
        if entry["type"] == "dir":
            async for found in _files(tree, child):
                yield found
        else:
            yield child, entry


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def tree(store, monkeypatch):
    monkeypatch.setattr(
        workspace_db, "get_workspace_placement", AsyncMock(side_effect=WORKSPACES.get)
    )
    monkeypatch.setattr(
        workspace_db,
        "get_live_workspace_folders_for_computer",
        AsyncMock(return_value=[{**WORKSPACES[HERE], "name": "Research"}]),
    )
    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: SimpleNamespace(enabled=True),
    )
    return LivefsTree(LivefsIdentity(COMPUTER, USER, ROOT), store)


def _new_file(name: str) -> MagicMock:
    """A file name no automation has, which a save creates one under."""
    new = MagicMock(spec=AutomationFile(name))
    new.unchanged = None
    new.rows = None
    new.fetch = AsyncMock(side_effect=lambda user_id, conn=None: new.rows)
    new.render = MagicMock(side_effect=lambda rows: None if rows is None else (EDITED, "sha256:new-v1"))
    new.parse = AsyncMock(return_value=DOCUMENT)
    new.plan = MagicMock(return_value=Plan(FilePlan(create=_Create(data=MagicMock(), pause=False))))
    new.hold = AsyncMock(return_value=None)

    async def commit(user_id, changes, conn):
        new.rows = ["row"]
        return f'Saved {name}: created "Morning brief"'

    new.commit = AsyncMock(side_effect=commit)
    return new


@pytest.fixture
def automations(monkeypatch):
    """The user's one automation as its file serves and saves it. Every read,
    the save's own included, renders the file as ``content`` and ``version``
    stand at that moment, so a save leaves the file as a read serves it. Any
    other valid name is in ``others``, holding no automation until a save
    there creates one."""
    conn = MagicMock()

    @asynccontextmanager
    async def _open(*_, **__):
        yield conn

    conn.transaction = conn.cursor = _open
    monkeypatch.setattr(db_json_route, "get_db_connection", _open)
    rows = MagicMock(spec=AutomationFile(FILE_NAME))
    rows.unchanged = None
    rows.content, rows.version = SERVED, ROWS_VERSION
    rows.fetch = AsyncMock(return_value=[])
    rows.render = MagicMock(side_effect=lambda _rows: (rows.content, rows.version))
    rows.parse = AsyncMock(return_value=DOCUMENT)
    rows.plan = MagicMock(return_value=Plan(FilePlan()))
    rows.hold = AsyncMock(side_effect=lambda user_id, changes, planned, conn: rows.version if changes else None)
    rows.commit = AsyncMock(return_value=REPORT)
    rows.others = {}

    def file_named(cls, name: str):
        if name == FILE_NAME:
            return rows
        if not is_file_name(name):
            return None
        return rows.others.setdefault(name, _new_file(name))

    async def rendered(cls, user_id: str):
        return {FILE_NAME: rows.render(await rows.fetch(user_id))}

    monkeypatch.setattr(AutomationsBackend, "file_named", classmethod(file_named))
    monkeypatch.setattr(AutomationsBackend, "rendered", classmethod(rendered))
    monkeypatch.setattr(AutomationsBackend, "names", classmethod(AsyncMock(return_value=[FILE_NAME])))
    return rows


@pytest.fixture
def history(monkeypatch):
    """One stored thread per workspace in THREADS, as its rows hold it. Only
    the database is faked, so the service's own read path serves the files."""
    content = {path: text.encode() for path, text in TRANSCRIPT.items()}

    def workspace_transcripts(workspace_id):
        return [THREADS[workspace_id]] if workspace_id in THREADS else []

    def found(workspace_id, short_id):
        thread_id = THREADS.get(workspace_id)
        return bool(thread_id) and thread_id[:8] == short_id

    def list_transcript(workspace_id, short_id):
        if not found(workspace_id, short_id):
            return None
        return {path: (_sha(data), len(data)) for path, data in content.items()}

    def load_transcript_file(workspace_id, short_id, path):
        if not found(workspace_id, short_id) or path not in content:
            return None
        return StoredFile(USER, _sha(content[path]), content[path])

    def list_transcript_tree(workspace_id, short_id, limit):
        thread_id = THREADS.get(workspace_id)
        if thread_id is None or short_id not in (None, thread_id[:8]):
            return []
        listed = [
            ListedFile(thread_id, thread_id[:8], path, sha256, size)
            for path, (sha256, size) in sorted(list_transcript(workspace_id, thread_id[:8]).items())
        ]
        return listed[:limit]

    def load_transcript_contents(files):
        return {f: content[f.path] for f in files if _sha(content[f.path]) == f.sha256}

    mocks = SimpleNamespace(
        workspace_transcripts=AsyncMock(side_effect=workspace_transcripts),
        list_transcript=AsyncMock(side_effect=list_transcript),
        load_transcript_file=AsyncMock(side_effect=load_transcript_file),
        list_transcript_tree=AsyncMock(side_effect=list_transcript_tree),
        load_transcript_contents=AsyncMock(side_effect=load_transcript_contents),
    )
    for name, mock in vars(mocks).items():
        monkeypatch.setattr(f"src.server.database.thread_transcripts.{name}", mock)
    monkeypatch.setattr(
        "src.server.database.conversation.list_computer_threads",
        AsyncMock(side_effect=lambda _, rows=True: ("digest-1", [_thread_row(T1)] if rows else [])),
    )
    return mocks


# -- the layout ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_root_holds_user_workflows_workspaces_and_computer(tree):
    entries = (await tree.list("")).entries
    names = {e["name"] for e in entries}
    assert names == {"user", "workflows", "workspaces", "computer"}
    assert {e["type"] for e in entries} == {"dir"}


@pytest.mark.asyncio
async def test_user_holds_the_automations_folder_which_takes_new_files(tree, automations):
    entries = (await tree.list("user")).entries
    assert [e["name"] for e in entries] == ["memory", "memo", "profile", "automations"]

    listing = await tree.list("user/automations")
    assert [(e["name"], e["writable"]) for e in listing.entries] == [
        ("README.md", False),
        (FILE_NAME, True),
    ]
    assert listing.writable is True


@pytest.mark.asyncio
async def test_workspaces_lists_only_this_computers_live_workspaces(tree):
    entries = (await tree.list("workspaces")).entries
    assert [e["name"] for e in entries] == [HERE]


@pytest.mark.asyncio
async def test_a_workspace_path_serves_the_users_workspaces_on_this_computer(tree, store):
    store.put((USER, "workspaces", SECOND, "memory"), "plan.md", {"content": "p"})
    content, _, path = await tree.read(f"workspaces/{SECOND}/memory/plan.md")
    assert content == "p"
    assert path == f"{ROOT}/second/.agents/memory/plan.md"


@pytest.mark.asyncio
async def test_the_users_workspace_on_another_computer_is_not_found(tree, store, history):
    store.put((USER, "workspaces", ELSEWHERE, "memory"), "plan.md", {"content": "p"})
    for attempt in (
        lambda: tree.read(f"workspaces/{ELSEWHERE}/memory/plan.md"),
        lambda: tree.list(f"workspaces/{ELSEWHERE}/transcripts"),
        lambda: tree.write(
            f"workspaces/{ELSEWHERE}/memory/new.md", b"x", if_match=None, if_none_match="*"
        ),
    ):
        assert (await _refusal(attempt)).code == "not_found"
    history.workspace_transcripts.assert_not_awaited()


@pytest.mark.asyncio
async def test_another_users_workspace_is_not_found(tree, history):
    # Transcripts are keyed by workspace alone, so ownership is the only fence.
    for path in (
        f"workspaces/{STRANGER}",
        f"workspaces/{STRANGER}/transcripts",
        f"workspaces/{STRANGER}/transcripts/{T2[:8]}",
        "workspaces/ws-missing",
    ):
        assert (await _refusal(lambda: tree.list(path))).code == "not_found"
    history.workspace_transcripts.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_memory_is_saved_under_that_workspaces_folder(tree, store):
    saved = await tree.write(
        f"workspaces/{HERE}/memory/notes.md",
        b"# notes",
        if_match=None,
        if_none_match="*",
    )
    assert saved.path == f"{ROOT}/research/.agents/memory/notes.md"
    assert saved.as_sent
    item = store.get((USER, "workspaces", HERE, "memory"), "notes.md")
    assert item.value["content"] == "# notes"
    assert store.search((USER, "memory")) == []


@pytest.mark.asyncio
async def test_new_files_are_accepted_only_where_the_file_tools_write(tree, history, automations):
    # The daemon refuses create up front in the rest, since a refusal that
    # only arrives at close() goes unseen by most programs.
    directories = (
        "",
        "user",
        "user/memory",
        "user/memo",
        "user/automations",
        "workflows",
        "workspaces",
        f"workspaces/{HERE}",
        f"workspaces/{HERE}/memory",
        f"workspaces/{HERE}/transcripts",
        f"workspaces/{HERE}/transcripts/{SHORT}",
        "computer",
    )
    listings = {path: await tree.list(path) for path in directories}
    writable = {path for path, listing in listings.items() if listing.writable}
    assert writable == {"user/memory", "user/automations", "workflows", f"workspaces/{HERE}/memory"}
    # The daemon keeps a structural listing until the host runs ``link`` again,
    # so one a route serves (threads.jsonl moves under ``computer``) is not.
    structural = {path for path, listing in listings.items() if listing.structural}
    assert structural == {"", "user", "workspaces", f"workspaces/{HERE}"}


# -- where the sandbox links it -------------------------------------------------


def _live(monkeypatch, *folders: dict) -> None:
    monkeypatch.setattr(
        workspace_db,
        "get_live_workspace_folders_for_computer",
        AsyncMock(return_value=list(folders)),
    )


@pytest.mark.asyncio
async def test_each_live_folder_links_the_user_files_its_memory_and_transcripts(
    tree, monkeypatch
):
    """Bash runs in the folder, so the user's files are linked there too, at
    the relative path the file tools fold onto the root."""
    _live(monkeypatch, WORKSPACES[HERE], WORKSPACES[SECOND])

    links = await tree.links()

    here, second = f"{ROOT}/research/.agents", f"{ROOT}/second/.agents"
    assert links == {
        None: [
            ("user", f"{ROOT}/.agents/user"),
            ("workflows", f"{ROOT}/.agents/workflows"),
            ("computer/threads.jsonl", f"{ROOT}/.agents/threads.jsonl"),
        ],
        HERE: [
            ("user", f"{here}/user"),
            ("workflows", f"{here}/workflows"),
            (f"workspaces/{HERE}/memory", f"{here}/memory"),
            (f"workspaces/{HERE}/transcripts", f"{here}/transcripts"),
        ],
        SECOND: [
            ("user", f"{second}/user"),
            ("workflows", f"{second}/workflows"),
            (f"workspaces/{SECOND}/memory", f"{second}/memory"),
            (f"workspaces/{SECOND}/transcripts", f"{second}/transcripts"),
        ],
    }


@pytest.mark.asyncio
async def test_a_workspace_that_owns_the_root_shares_the_roots_user_link(tree, monkeypatch):
    _live(monkeypatch, {**WORKSPACES[HERE], "dir_name": None})

    links = await tree.links()

    assert [
        link for group in links.values() for link in group if link[0] == "user"
    ] == [("user", f"{ROOT}/.agents/user")]


@pytest.mark.asyncio
async def test_without_a_store_a_folder_links_its_transcripts_but_no_memory(tree):
    links = await LivefsTree(LivefsIdentity(COMPUTER, USER, ROOT), None).links()

    linked = {source for source, _ in links[HERE]}
    assert f"workspaces/{HERE}/transcripts" in linked
    assert f"workspaces/{HERE}/memory" not in linked


@pytest.mark.asyncio
async def test_a_folder_staged_mid_move_is_linked_once_it_lands(tree, monkeypatch):
    staged = {**WORKSPACES[SECOND], "dir_name": f"{MOVING_DIR}/{SECOND}"}
    _live(monkeypatch, WORKSPACES[HERE], staged)

    links = await tree.links()

    assert links.keys() == {None, HERE}
    assert all(MOVING_DIR not in target for group in links.values() for _, target in group)


# -- what a listing promises ---------------------------------------------------


class _SizeCache:
    """The Redis the thread index keeps its sizes in."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    async def set(self, key: str, value, ex=None) -> bool:
        self.data[key] = str(value).encode()
        return True


@pytest.mark.asyncio
async def test_every_file_lists_the_size_version_and_content_its_read_returns(
    tree, store, history, automations, monkeypatch
):
    """The daemon keeps a file's bytes by (path, version) and skips the read
    when a listing names a version it holds, and takes a listed file's
    content in place of a read, so each route has to agree with its read."""
    from ptc_agent.agent.backends.user_data import UserDataBackend
    from ptc_agent.agent.backends.workflows import build_workflow_value
    from src.server.services.livefs import cache as livefs_cache

    big = "é" * (INLINE_MAX_BYTES // 2 + 1)
    store.put((USER, "memory"), "notes.md", {"content": "résumé 市场"})
    store.put((USER, "memory"), "big.md", {"content": big})
    store.put((USER, "workspaces", HERE, "memory"), "plan.md", {"content": "计划"})
    store.put((USER, "memos"), "brief.md", {"content": "kept ✓"})
    store.put((USER, "workflows"), "daily.js", build_workflow_value("run('✓')", None))

    def profile(filename: str) -> SimpleNamespace:
        return SimpleNamespace(
            fetch=AsyncMock(return_value=filename),
            render=lambda rows: (f'{{"file": "{rows}", "note": "价格"}}\n', f"rows:{rows}"),
        )

    for filename in list(UserDataBackend.files):
        monkeypatch.setitem(UserDataBackend.files, filename, profile(filename))
    sizes = _SizeCache()
    monkeypatch.setattr(
        livefs_cache, "get_cache_client", lambda: SimpleNamespace(enabled=True, client=sizes)
    )

    async def walk() -> dict:
        return {
            path: entry
            for top in ("user", "workflows", "workspaces", "computer")
            async for path, entry in _files(tree, top)
        }

    # Cold, the thread index is rendered for its size; warm, the size is kept.
    for _ in range(2):
        listed = await walk()
        transcripts = f"workspaces/{HERE}/transcripts/{SHORT}/"
        assert set(listed) == {
            "user/memory/notes.md",
            "user/memory/big.md",
            "user/memo/brief.md",
            *(f"user/profile/{name}" for name in (*UserDataBackend.data_files, "README.md")),
            "user/automations/README.md",
            AUTOMATIONS,
            "workflows/daily.js",
            f"workspaces/{HERE}/memory/plan.md",
            *(transcripts + name for name in TRANSCRIPT),
            "computer/threads.jsonl",
        }
        for path, entry in listed.items():
            content, version, _ = await tree.read(path)
            assert entry["size"] == len(content.encode()), path
            assert entry["version"] == version, path
            assert entry.get("content", content) == content, path
    assert "content" not in listed["user/memory/big.md"]
    assert "content" not in listed["computer/threads.jsonl"]
    assert listed["user/profile/README.md"]["content"]


@pytest.mark.asyncio
async def test_a_transcript_listing_carries_every_folder_below_it_whole(tree, history):
    # The daemon answers the rest of the command from these, so a folder
    # missing a file hides it, and carried content stands in for its read.
    listing = await tree.list(f"workspaces/{HERE}/transcripts")

    assert [e["name"] for e in listing.entries] == [SHORT]
    assert set(listing.below) == {SHORT, f"{SHORT}/tasks", f"{SHORT}/tasks/k1"}
    assert not any(below["writable"] for below in listing.below.values())
    files = {
        f"{folder}/{e['name']}": e
        for folder, below in listing.below.items()
        for e in below["entries"]
        if e["type"] == "file"
    }
    assert set(files) == {f"{SHORT}/{path}" for path in TRANSCRIPT}
    for path, text in TRANSCRIPT.items():
        data = text.encode()
        entry = files[f"{SHORT}/{path}"]
        assert (entry["size"], entry["version"], entry["content"]) == (
            len(data),
            _sha(data)[:32],
            text,
        )


@pytest.mark.asyncio
async def test_a_thread_past_the_tree_limit_is_named_and_listed_on_its_own(
    tree, history, monkeypatch
):
    from src.server.services.livefs import history as history_route

    monkeypatch.setattr(history_route, "_TREE_MAX_FILES", len(TRANSCRIPT) - 1)
    root = f"workspaces/{HERE}/transcripts"

    listing = await tree.list(root)
    assert [e["name"] for e in listing.entries] == [SHORT]
    assert listing.below == {}
    thread = await tree.list(f"{root}/{SHORT}")
    assert [e["name"] for e in thread.entries] == ["manifest.json", "tasks", "turn-0001.jsonl"]
    assert thread.below == {}


@pytest.mark.asyncio
async def test_a_transcript_directory_name_must_be_a_short_thread_id(tree, history):
    for name in (".git", SHORT[:7], SHORT + "0", "ABCDEF12"):
        path = f"workspaces/{HERE}/transcripts/{name}"
        assert (await _refusal(lambda: tree.list(path))).code == "not_found"
    history.list_transcript.assert_not_awaited()


# -- refusals ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_past_threads_refuse_every_change_as_read_only(tree, history):
    turn = f"workspaces/{HERE}/transcripts/{SHORT}/turn-0001.jsonl"
    _, turn_version, _ = await tree.read(turn)
    _, index_version, _ = await tree.read("computer/threads.jsonl")
    attempts = (
        lambda: tree.write(turn, b"x", if_match=turn_version, if_none_match=None),
        lambda: tree.write(
            f"workspaces/{HERE}/transcripts/{SHORT}/new.md",
            b"x",
            if_match=None,
            if_none_match="*",
        ),
        lambda: tree.delete(turn),
        lambda: tree.rename(turn, "user/memory/turn.jsonl"),
        lambda: tree.write(
            "computer/threads.jsonl", b"x", if_match=index_version, if_none_match=None
        ),
    )
    for attempt in attempts:
        refused = await _refusal(attempt)
        assert (refused.status, refused.code) == (403, "read_only")


@pytest.mark.asyncio
async def test_memos_refuse_changes_as_read_only(tree, store):
    store.put((USER, "memos"), "brief.md", {"content": "kept"})
    _, version, _ = await tree.read("user/memo/brief.md")
    for attempt in (
        lambda: tree.write(
            "user/memo/brief.md", b"changed", if_match=version, if_none_match=None
        ),
        lambda: tree.delete("user/memo/brief.md"),
    ):
        refused = await _refusal(attempt)
        assert (refused.status, refused.code) == (403, "read_only")
    assert (await tree.read("user/memo/brief.md"))[0] == "kept"


@pytest.mark.asyncio
async def test_dot_segments_are_refused_before_any_route_is_reached(tree, history):
    for path in (
        f"user/memory/../../workspaces/{STRANGER}/memory/x.md",
        f"workspaces/./{HERE}/memory/x.md",
        f"workspaces/{HERE}/transcripts/{SHORT}/../../memory/x.md",
    ):
        refused = await _refusal(lambda: tree.read(path))
        assert (refused.status, refused.code) == (422, "invalid")
    history.load_transcript_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_the_store_cannot_key_is_refused_as_invalid(tree, store):
    refused = await _refusal(
        lambda: tree.write(
            "user/memory/bad name!.md", b"x", if_match=None, if_none_match="*"
        )
    )
    assert (refused.status, refused.code) == (422, "invalid")
    assert store.search((USER, "memory")) == []


class _YieldingStore(InMemoryStore):
    """Yields at every read and write, as a store over a connection does, so
    concurrent saves interleave there."""

    async def aget(self, *args, **kwargs):
        await asyncio.sleep(0)
        return await super().aget(*args, **kwargs)

    async def aput(self, *args, **kwargs):
        await asyncio.sleep(0)
        return await super().aput(*args, **kwargs)


SHIPPED = "shipped\n"


@pytest.fixture
def racing(tree, monkeypatch):
    """The mount over a yielding store holding a memory file and a saved
    workflow, beside a shipped workflow no save has forked yet."""
    from ptc_agent.agent.backends.workflows import build_workflow_value

    monkeypatch.setattr(
        "src.config.settings.get_workflow_orchestration_config",
        lambda: SimpleNamespace(enabled=True, max_script_bytes=64 * 1024),
    )
    monkeypatch.setattr(
        "ptc_agent.agent.filesystem_routes.get_prebuilt_workflows",
        lambda: SimpleNamespace(files=lambda: {"shipped.js": SHIPPED}),
    )
    store = _YieldingStore()
    store.put((USER, "memory"), "notes.md", {"content": "base"})
    store.put((USER, "workflows"), "brief.js", build_workflow_value("base", None))
    return LivefsTree(LivefsIdentity(COMPUTER, USER, ROOT), store)


@pytest.mark.parametrize("path", ["user/memory/notes.md", "workflows/brief.js", "workflows/shipped.js"])
@pytest.mark.asyncio
async def test_two_saves_over_one_version_land_once_and_refuse_the_other(racing, path):
    """Both name the version they read, and each is checked under the lock
    its write holds, so the second finds the first one's save rather than
    landing over it. A shipped workflow's first save forks it."""
    _, version, _ = await racing.read(path)

    results = await asyncio.gather(
        *(
            racing.write(path, body, if_match=f'"{version}"', if_none_match=None)
            for body in (b"from A", b"from B")
        ),
        return_exceptions=True,
    )

    saved = [r for r in results if not isinstance(r, BaseException)]
    refused = [r for r in results if isinstance(r, LivefsError)]
    assert len(saved) == len(refused) == 1, results
    assert (refused[0].status, refused[0].code) == (412, "changed")
    content, version, _ = await racing.read(path)
    assert content in ("from A", "from B")
    assert version == saved[0].version


@pytest.mark.asyncio
async def test_two_creates_of_one_new_file_land_once_and_refuse_the_other(racing):
    path = "user/memory/new.md"
    results = await asyncio.gather(
        *(racing.write(path, body, if_match=None, if_none_match="*") for body in (b"A", b"B")),
        return_exceptions=True,
    )

    refused = [r for r in results if isinstance(r, LivefsError)]
    assert len(refused) == 1, results
    assert (refused[0].status, refused[0].code) == (412, "exists")


@pytest.mark.asyncio
async def test_a_create_over_a_shipped_workflow_is_refused_as_exists(racing):
    """The mount serves the shipped script where no save has forked it."""
    refused = await _refusal(
        lambda: racing.write("workflows/shipped.js", b"mine", if_match=None, if_none_match="*")
    )

    assert (refused.status, refused.code) == (412, "exists")
    assert (await racing.read("workflows/shipped.js"))[0] == SHIPPED


@pytest.mark.parametrize("target", ["workflows/mine.js", "user/memory/mine.js"])
@pytest.mark.asyncio
async def test_a_shipped_workflow_is_refused_a_move_as_its_delete_is(racing, target):
    """A move ends in deleting the source, which a script no save has
    forked refuses, so it is refused before the copy at the target is made."""
    removal = await _refusal(lambda: racing.delete("workflows/shipped.js"))
    move = await _refusal(lambda: racing.rename("workflows/shipped.js", target))

    assert (move.status, move.code) == (removal.status, removal.code) == (403, "read_only")
    assert (await racing.read("workflows/shipped.js"))[0] == SHIPPED
    assert (await _refusal(lambda: racing.read(target))).code == "not_found"


@pytest.mark.asyncio
async def test_a_forked_shipped_workflow_moves_and_the_shipped_one_shows_again(racing):
    """The move deletes the user's fork, as removing it does."""
    _, version, _ = await racing.read("workflows/shipped.js")
    await racing.write("workflows/shipped.js", b"mine", if_match=f'"{version}"', if_none_match=None)

    await racing.rename("workflows/shipped.js", "workflows/mine.js")

    assert (await racing.read("workflows/mine.js"))[0] == "mine"
    assert (await racing.read("workflows/shipped.js"))[0] == SHIPPED


@pytest.mark.parametrize(
    ("target", "before"),
    [("workflows/moved.js", None), ("user/memory/moved.md", None), ("workflows/kept.js", "kept")],
    ids=["into-workflows", "within-memory", "over-a-file"],
)
@pytest.mark.asyncio
async def test_a_save_landing_on_a_file_mid_move_is_kept_and_the_move_refused(
    racing, target, before
):
    """The save lands after the move copied the file and before it deletes
    it. The delete goes over the version the move copied, so it is refused
    rather than deleting the saved bytes, and the copy is taken back."""
    source = "user/memory/notes.md"
    if before is not None:
        await racing.write(target, before.encode(), if_match=None, if_none_match="*")
    _, version, _ = await racing.read(source)

    moved, saved = await asyncio.gather(
        racing.rename(source, target),
        racing.write(source, b"saved mid-move", if_match=f'"{version}"', if_none_match=None),
        return_exceptions=True,
    )

    assert not isinstance(saved, BaseException), saved
    assert isinstance(moved, LivefsError), moved
    assert (moved.status, moved.code) == (412, "changed")
    assert (await racing.read(source))[0] == "saved mid-move"
    if before is None:
        assert (await _refusal(lambda: racing.read(target))).code == "not_found"
    else:
        assert (await racing.read(target))[0] == before


# -- the channels folder -------------------------------------------------------
#
# The user's chat-app settings are for the file tools only: never a point of
# the mount, whatever the deployment has configured.

CHANNELS = (
    "user/channels",
    "user/channels/channels.json",
    "user/channels/available.json",
)


@pytest.fixture
def messaging_configured(monkeypatch):
    """A deployment with the channel gateway, whose settings no mount call
    may ask for."""
    from src.config import env
    from src.server.services import channel_settings

    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "http://gateway.test")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "svc-token")
    monkeypatch.setattr(
        channel_settings.messaging,
        "gateway_request",
        AsyncMock(side_effect=AssertionError("the mount reached the channel settings")),
    )


async def _unreachable(tree: LivefsTree) -> None:
    assert "channels" not in [e["name"] for e in (await tree.list("user")).entries]
    for path in CHANNELS:
        assert (await _refusal(lambda: tree.list(path))).code == "not_found"
        assert (await _refusal(lambda: tree.read(path))).code == "not_found"
        refused = await _refusal(
            lambda: tree.write(path, b"{}", if_match=None, if_none_match="*")
        )
        assert refused.code == "not_found"
        assert (await _refusal(lambda: tree.delete(path))).code == "not_found"


@pytest.mark.asyncio
async def test_the_channels_folder_is_not_on_the_mount(tree, messaging_configured):
    await _unreachable(tree)


@pytest.mark.asyncio
async def test_the_channels_route_stays_off_the_mount_even_when_built(
    tree, messaging_configured, monkeypatch
):
    """Were the mount's composite ever built with the channels route, the
    tree still never serves it."""
    from ptc_agent.agent import filesystem_routes
    from ptc_agent.agent.backends.channels import ChannelsBackend
    from src.server.services.livefs import tree as tree_module

    def with_channels(**kwargs):
        return filesystem_routes.resolve_identity_gates(**kwargs, channels=True)

    monkeypatch.setattr(tree_module, "resolve_identity_gates", with_channels)
    built = tree._build(tree._computer())
    route = built.route_for(f"{ROOT}/.agents/user/channels/channels.json")
    assert isinstance(route, ChannelsBackend)

    await _unreachable(tree)


# -- the automations folder ----------------------------------------------------


async def _save(tree: LivefsTree, if_match: str, body: str = EDITED):
    return await tree.write(
        AUTOMATIONS, body.encode(), if_match=f'"{if_match}"', if_none_match=None
    )


async def _create(tree: LivefsTree, path: str, body: str = EDITED):
    return await tree.write(path, body.encode(), if_match=None, if_none_match="*")


@pytest.mark.asyncio
async def test_a_save_over_the_rows_version_lands_and_returns_its_report(
    tree, automations
):
    """No Read tool call stands behind a program's save, so the version is
    the whole check."""
    _, version, _ = await tree.read(AUTOMATIONS)
    assert version == ROWS_VERSION

    saved = await _save(tree, version)

    assert (saved.path, saved.report) == (f"{ROOT}/.agents/{AUTOMATIONS}", REPORT)
    # Stored as the server renders it, which the sender has to read back.
    assert saved.size == len(SERVED.encode())
    assert not saved.as_sent
    automations.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_state_moving_under_a_read_never_makes_its_save_conflict(tree, automations):
    """A run moves ``state``, which the save ignores; only the rows' own
    version names a change the program did not see."""
    _, version, _ = await tree.read(AUTOMATIONS)
    automations.content = SERVED.replace("}\n", ', "state": {"failure_count": 1}}\n')

    saved = await _save(tree, version)

    assert saved.report == REPORT


@pytest.mark.asyncio
async def test_a_save_over_a_stale_version_is_refused_as_changed(tree, automations):
    refused = await _refusal(lambda: _save(tree, "sha256:rows-v0"))

    assert (refused.status, refused.code) == (412, "changed")
    automations.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_writer_landing_after_the_check_is_caught_under_the_route_lock(
    tree, automations
):
    automations.version = "sha256:rows-v2"

    refused = await _refusal(lambda: _save(tree, ROWS_VERSION))

    assert (refused.status, refused.code) == (412, "changed")
    automations.plan.assert_not_called()
    automations.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_server_failure_while_saving_is_refused_as_unavailable(tree, automations):
    automations.commit.side_effect = RuntimeError("connection reset")

    refused = await _refusal(lambda: _save(tree, ROWS_VERSION))

    assert (refused.status, refused.code) == (503, "unavailable")
    assert "nothing was saved" in refused.message


@pytest.mark.asyncio
async def test_a_save_answers_with_the_file_it_left_without_reading_it_back(
    tree, automations
):
    """Read back after the commit, a read that failed would answer a save
    that landed as NOT SAVED, and a program rerun on that would create its
    automation twice."""
    async def fetch(user_id, conn=None):
        if conn is None:
            raise RuntimeError("connection reset")
        return []

    automations.fetch.side_effect = fetch

    saved = await _save(tree, ROWS_VERSION)

    assert saved.report == REPORT
    assert (saved.version, saved.size) == (ROWS_VERSION, len(SERVED.encode()))


@pytest.mark.asyncio
async def test_a_new_name_in_the_folder_creates_an_automation(tree, automations):
    saved = await _create(tree, "user/automations/evening-wrap.json")

    assert saved.report == 'Saved evening-wrap.json: created "Morning brief"'
    assert (saved.version, saved.size) == ("sha256:new-v1", len(EDITED.encode()))
    automations.others["evening-wrap.json"].commit.assert_awaited_once()
    automations.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_create_over_an_automations_file_is_refused_as_exists(tree, automations):
    refused = await _refusal(lambda: _create(tree, AUTOMATIONS))

    assert (refused.status, refused.code) == (412, "exists")
    automations.commit.assert_not_awaited()


@pytest.mark.parametrize("name", ["notes.txt", "brief.json.tmp", ".brief.json.swp"])
@pytest.mark.asyncio
async def test_a_name_no_automation_may_take_is_refused_as_invalid(tree, automations, name):
    """Where an editor's temporary file lands, so a save in place fails
    loudly rather than creating an automation."""
    refused = await _refusal(lambda: _create(tree, f"user/automations/{name}"))

    assert (refused.status, refused.code) == (422, "invalid")
    assert "1 to 64 letters, digits, - or _" in refused.message


@pytest.mark.asyncio
async def test_status_deleted_answers_with_no_file(tree, automations):
    automations.plan.return_value = Plan(FilePlan(delete=_Delete(automation_id="id-1", name="Morning brief")))

    async def delete(user_id, changes, conn):
        automations.render.side_effect = lambda _rows: None
        return 'Deleted "Morning brief" (morning-brief.json), with its run history'

    automations.commit.side_effect = delete

    saved = await _save(tree, ROWS_VERSION, '{"status": "deleted"}')

    assert (saved.removed, saved.version, saved.size, saved.as_sent) == (True, "", 0, False)
    assert saved.report.startswith('Deleted "Morning brief"')


@pytest.mark.asyncio
async def test_removing_an_automations_file_deletes_it_and_reports_that(tree, automations):
    automations.plan_delete.return_value = Plan(FilePlan(delete=_Delete(automation_id="id-1", name="Morning brief")))
    automations.commit.return_value = 'Deleted "Morning brief" (morning-brief.json), with its run history'

    path, report = await tree.delete(AUTOMATIONS)

    assert path == f"{ROOT}/.agents/{AUTOMATIONS}"
    assert report == 'Deleted "Morning brief" (morning-brief.json), with its run history'


@pytest.mark.parametrize(
    ("path", "status", "code"),
    [("user/automations/gone.json", 404, "not_found"), ("user/automations/README.md", 403, "read_only")],
    ids=["no-automation", "readme"],
)
@pytest.mark.asyncio
async def test_removing_what_is_no_automations_file_is_refused(tree, automations, path, status, code):
    refused = await _refusal(lambda: tree.delete(path))

    assert (refused.status, refused.code) == (status, code)
    automations.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_move_within_the_folder_renames_the_file_and_keeps_the_automation(tree, automations):
    automations.rename = AsyncMock(return_value="Renamed morning-brief.json to brief.json")

    moved = await tree.rename(AUTOMATIONS, "user/automations/brief.json")

    assert moved == (
        f"{ROOT}/.agents/{AUTOMATIONS}",
        f"{ROOT}/.agents/user/automations/brief.json",
        "Renamed morning-brief.json to brief.json",
    )
    automations.rename.assert_awaited_once_with(USER, "brief.json", ANY)
    automations.others["brief.json"].commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_move_onto_another_automations_file_is_refused_as_exists(tree, automations):
    taken = automations.others["evening-wrap.json"] = _new_file("evening-wrap.json")
    taken.rows = ["row"]

    refused = await _refusal(lambda: tree.rename(AUTOMATIONS, "user/automations/evening-wrap.json"))

    assert (refused.status, refused.code) == (412, "exists")
    automations.rename.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_file_moved_into_the_folder_creates_an_automation(tree, store, automations):
    store.put((USER, "memory"), "draft.json", {"content": EDITED})

    _, _, report = await tree.rename("user/memory/draft.json", "user/automations/draft.json")

    assert report == 'Saved draft.json: created "Morning brief"'
    assert store.search((USER, "memory")) == []


@pytest.mark.parametrize(
    "target", ["user/memory/morning-brief.json", "workflows/morning-brief.json"]
)
@pytest.mark.asyncio
async def test_an_automations_file_cannot_be_moved_out_of_its_folder(tree, store, automations, target):
    """Refused before loading it, so a database outage cannot turn the
    answer into not found."""
    refused = await _refusal(lambda: tree.rename(AUTOMATIONS, target))

    assert (refused.status, refused.code) == (403, "read_only")
    assert store.search((USER, "memory")) == []
    automations.fetch.assert_not_awaited()
    automations.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_profile_files_still_move_nowhere(tree):
    """Their names are fixed, so a move within their folder is refused as
    before, without reading the rows."""
    refused = await _refusal(lambda: tree.rename("user/profile/portfolio.json", "user/profile/mine.json"))

    assert (refused.status, refused.code) == (403, "read_only")


@pytest.mark.parametrize(
    "context",
    [CallContext(workspace_id=HERE, thread_id=T1, timezone="Asia/Tokyo"), None],
    ids=["filed", "nothing-filed"],
)
@pytest.mark.asyncio
async def test_a_save_takes_its_defaults_from_the_commands_context(store, automations, context):
    """Where a new automation runs, which thread ``"current"`` names, and its
    clock come from the conversation that ran the command."""
    tree = LivefsTree(LivefsIdentity(COMPUTER, USER, ROOT), store, context)

    await _save(tree, ROWS_VERSION)

    # A program sends no copy of what it read (None), so the rows stand for it.
    automations.parse.assert_awaited_once_with(USER, context or CallContext(), ANY, None)
    assert automations.plan.call_args.args[0] == (context or CallContext())


def test_a_failure_line_names_the_error_without_quoting_the_row(caplog):
    """The log format prints the message alone, so the line itself says what
    failed once the traceback, which would quote the row, is left out."""
    from src.server.services.livefs import tree

    refused = psycopg.errors.CheckViolation("Failing row contains (Sell all TSLA).")
    with caplog.at_level(logging.ERROR, logger=tree.logger.name):
        tree._log_failure(
            logging.ERROR, "livefs request failed", "/mnt/livefs/user/automations/sell-tsla.json", refused
        )

    (record,) = caplog.records
    assert record.getMessage() == (
        "livefs request failed: path=/mnt/livefs/user/automations error_type=CheckViolation sqlstate=23514"
    )
    assert record.exc_info is None
