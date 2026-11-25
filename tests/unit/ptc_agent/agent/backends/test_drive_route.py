"""The Drive as the file tools see it.

The route answers in process on every kind of computer, so what is pinned
here is its contract with the tools: who a save is recorded as made by (the
agent, the run and the sitting the mount shares), parallel calls on one file
never losing a save, a save the Drive refuses coming back as the tool's
ordinary error, a search of one folder reading only that folder, a Drive
that fails mid-search leaving the rest of the answer, a build without the
Drive refusing its paths rather than writing to the disk.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.runnables.config import var_child_runnable_config

from ptc_agent.agent import filesystem_routes
from ptc_agent.agent.backends import drive as drive_route
from ptc_agent.agent.backends.drive import DriveAbsent, DriveRefused, DriveRoute
from ptc_agent.agent.filesystem_routes import (
    build_filesystem_backend,
    resolve_identity_gates,
)
from ptc_agent.agent.tools.file_ops import create_filesystem_tools
from ptc_agent.agent.tools.glob import create_glob_tool
from ptc_agent.agent.tools.grep import create_grep_tool
from ptc_agent.core.path_fold import resolve_agent_path, virtual_agent_path
from ptc_agent.core.sandbox.drive_mount import DRIVE_NOT_HERE, DriveDisk, drive_refusal
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database.drive.records import Content, DriveError, Entry

ROOT = "/home/workspace"
FOLDER = f"{ROOT}/Research"
USER = "user-1"
NOTE = ".agents/drive/personal/notes/a.md"

T0 = datetime(2026, 11, 1, tzinfo=UTC)


def _entry(path: str, data: bytes | None, *, at: datetime = T0, seq: int = 1) -> Entry:
    folder = data is None
    text = not folder and b"\x00" not in data
    return Entry(
        entry_id=hashlib.md5(path.encode()).hexdigest(),
        space_id="space-1",
        path=path,
        parent=path.rpartition("/")[0],
        name=path.rpartition("/")[2],
        kind="folder" if folder else "file",
        head_seq=None if folder else seq,
        sha256=None if folder else hashlib.sha256(data).hexdigest(),
        size=None if folder else len(data),
        content_type=None if folder else ("text/markdown" if text else "application/octet-stream"),
        is_text=None if folder else text,
        created_at=at,
        modified_at=at,
        changed_seq=1,
        deleted_at=None,
        trash_id=None,
        deleted_by=None,
        modified_by=None,
    )


class FakeDrive:
    """The service, over a dict of files, recording what each call asked.

    A save yields between checking its condition and storing, as a save
    awaiting the database does, so two saves let run at once interleave."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.writes: list[dict] = []
        self.trees: list[str] = []
        self.unders: list[str | None] = []
        # Set, a text read stops before its second file as a database fetch
        # that never answers would, and ``fetching`` counts the reads open.
        self.stalled: asyncio.Event | None = None
        self.fetching = 0
        self.page_size = 200

    async def reach(self, user_id, ref, *, write=False):
        return SimpleNamespace(space_id="space-1", user_id=user_id)

    async def stat(self, space, path):
        if path in self.files:
            return _entry(path, self.files[path])
        if any(p.startswith(path + "/") for p in self.files):
            return _entry(path, None)
        raise DriveError("not_found", f"Nothing at {path}")

    async def content(self, space, path):
        entry = await self.stat(space, path)
        if entry.kind == "folder":
            raise DriveError("is_folder", f"{path} is a folder")
        data = self.files[path]
        return entry, Content("space-1", entry.sha256, len(data), entry.is_text, entry.content_type, None)

    def _bytes(self, content) -> bytes:
        return next(d for d in self.files.values() if hashlib.sha256(d).hexdigest() == content.sha256)

    async def read_range(self, content, offset, length):
        return self._bytes(content)[offset : offset + length]

    async def stream(self, content, offset, length):
        yield self._bytes(content)[offset:]

    async def write(self, space, path, data, *, actor, if_match=None, **_):
        self.writes.append({"path": path, "actor": actor, "if_match": if_match})
        if if_match is not None:
            held = self.files.get(path)
            if held is None or hashlib.sha256(held).hexdigest() != if_match:
                raise DriveError("changed", "The file changed")
        await asyncio.sleep(0)
        self.files[path] = data
        return _entry(path, data, seq=len(self.writes))

    async def tree(self, space, path=""):
        self.trees.append(path)
        return [
            _entry(p, d)
            for p, d in sorted(self.files.items())
            if not path or p.startswith(path + "/")
        ]

    async def _texts(self, entry_ids, under):
        self.unders.append(under)
        self.fetching += 1
        try:
            sent = 0
            # In id order, as the service pages them, which is not path order.
            for path, data in sorted(self.files.items(), key=lambda f: _entry(*f).entry_id):
                entry = _entry(path, data)
                if under and not path.startswith(under + "/"):
                    continue
                if entry.is_text and (entry_ids is None or entry.entry_id in entry_ids):
                    if sent and self.stalled is not None:
                        self.stalled.set()
                        await asyncio.Event().wait()
                    sent += 1
                    yield entry, data
        finally:
            self.fetching -= 1

    async def text_pages(self, space, entry_ids=None, *, under=None):
        page = []
        async for found in self._texts(entry_ids, under):
            page.append(found)
            if len(page) >= self.page_size:
                yield page
                page = []
        if page:
            yield page


@pytest.fixture
def fake(monkeypatch) -> FakeDrive:
    service = FakeDrive()
    monkeypatch.setattr(filesystem_routes, "drive_service", lambda: service)
    return service


def _sandbox() -> MagicMock:
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
    sb.awrite_text = AsyncMock(return_value=True)
    sb.aread_text = AsyncMock(return_value=None)
    sb.sandbox.drive.on_disk = AsyncMock(return_value=DriveDisk.OFF)
    return sb


def _build(*, drive: bool = True):
    gates = resolve_identity_gates(
        store=None, user_id=USER, own_data=True, workspace_id="ws-1",
        disable_subagents=False, drive=drive,
    )
    return build_filesystem_backend(
        backend=_sandbox(), gates=gates, store=None, user_id=USER, workspace_id="ws-1",
        call=CallContext(workspace_id="ws-1", thread_id="thread-1"),
    )


def _tools(*, drive: bool = True):
    backend, sources = _build(drive=drive)
    return backend, sources, create_filesystem_tools(backend)


@pytest.fixture
def run():
    """Calls made in a run: the main agent's, or a subagent's under its task,
    whose metadata keeps the turn's run id as LangGraph copies it down."""

    def _in(run_id: str, *, subagent: str | None = None, task: str = "abc123") -> dict:
        config = {
            "configurable": {
                "thread_id": "thread-1",
                "checkpoint_ns": f"tools:1|task:{task}" if subagent else "",
            },
            "metadata": {"run_id": run_id, **({"subagent_type": subagent} if subagent else {})},
        }
        var_child_runnable_config.set(config)
        return config

    token = var_child_runnable_config.set(None)
    yield _in
    var_child_runnable_config.reset(token)


class TestGates:
    def test_the_drive_needs_the_feature_a_user_and_their_own_data(self):
        def gate(**kw):
            args = dict(store=None, user_id=USER, own_data=True, workspace_id="ws",
                        disable_subagents=False, drive=True)
            return resolve_identity_gates(**{**args, **kw}).drive

        assert gate()
        assert not gate(drive=False)
        assert not gate(user_id=None)
        # A shared workspace's turn reaches no one member's files.
        assert not gate(own_data=False)

    def test_a_build_without_it_mounts_the_refusal(self):
        backend, _ = _build(drive=False)
        assert isinstance(backend.route_for(NOTE), DriveAbsent)

    def test_a_build_with_no_other_route_still_mounts_the_refusal(self, monkeypatch):
        # No store, no user and no workflows: nothing else is routed.
        monkeypatch.setattr(
            "src.config.settings.get_workflow_orchestration_config",
            lambda: SimpleNamespace(enabled=False),
        )
        gates = resolve_identity_gates(
            store=None, user_id=None, own_data=True, workspace_id=None,
            disable_subagents=True, drive=False,
        )
        backend, _ = build_filesystem_backend(
            backend=_sandbox(), gates=gates, store=None, user_id=None, workspace_id=None,
        )
        assert isinstance(backend.route_for(NOTE), DriveAbsent)

    def test_a_folder_spelling_reaches_the_computers_drive(self):
        backend, _, _ = _tools()
        for spelled in (NOTE, f"{FOLDER}/{NOTE}", f"/{NOTE}"):
            assert isinstance(backend.route_for(spelled), DriveRoute), spelled


class TestNotHere:
    """With the feature off the Drive's paths are refused, never written to
    the computer's disk, where a real folder would sit in the mount's way."""

    @pytest.mark.asyncio
    async def test_write_edit_and_read_are_refused_in_one_line(self, fake, run):
        backend, _, (read, write, edit) = _tools(drive=False)
        run("run-1")

        # The same line Bash's guard gives a command naming the Drive here.
        assert DRIVE_NOT_HERE == drive_refusal(DriveDisk.OFF)
        assert await write.ainvoke({"file_path": NOTE, "content": "x"}) == f"ERROR: {DRIVE_NOT_HERE}"
        out = await edit.ainvoke({"file_path": NOTE, "old_string": "a", "new_string": "b"})
        assert DRIVE_NOT_HERE in out
        assert await read.ainvoke({"file_path": NOTE}) == f"ERROR: {DRIVE_NOT_HERE}"

        backend.sandbox.awrite_text.assert_not_called()
        assert fake.writes == []

    @pytest.mark.asyncio
    async def test_a_read_that_attaches_bytes_is_refused_too(self):
        # A PNG or PDF Read takes the capped download, not the text read.
        backend, _ = _build(drive=False)
        backend.sandbox.adownload_file_bytes_capped = AsyncMock(return_value=b"\x89PNG")

        with pytest.raises(DriveRefused) as refused:
            await backend.adownload_file_bytes_capped(".agents/drive/personal/c.png", lambda _: 1 << 20)

        assert str(refused.value) == DRIVE_NOT_HERE
        backend.sandbox.adownload_file_bytes_capped.assert_not_called()



class TestSeen:
    @pytest.mark.asyncio
    async def test_a_file_deleted_since_it_was_read_can_be_written_again(self, fake, run):
        _, _, (read, write, _edit) = _tools()
        run("run-1")
        await write.ainvoke({"file_path": NOTE, "content": "one\n"})
        await read.ainvoke({"file_path": NOTE})
        del fake.files["notes/a.md"]

        assert (await read.ainvoke({"file_path": NOTE})).startswith("ERROR: File not found")
        out = await write.ainvoke({"file_path": NOTE, "content": "two\n"})

        assert out.startswith("Saved"), out
        assert fake.files["notes/a.md"] == b"two\n"


class TestWhoSaved:
    @pytest.mark.asyncio
    async def test_a_save_records_the_sitting_the_mount_would(self, fake, run):
        _, _, (_read, write, edit) = _tools()
        config = run("run-1")
        await write.ainvoke({"file_path": NOTE, "content": "one\n"})
        await edit.ainvoke({"file_path": NOTE, "old_string": "one", "new_string": "two"})

        # What Bash and code file a mount save under, for the same call.
        mount = CallContext(workspace_id="ws-1", thread_id="thread-1").for_call(config)
        assert len(fake.writes) == 2
        for w in fake.writes:
            actor = w["actor"]
            assert (actor.kind, actor.via, actor.user_id) == ("agent", "tool", USER)
            assert (actor.agent, actor.run_id, actor.sitting) == (
                mount.agent, mount.run_id, mount.sitting,
            )
            assert (actor.thread_id, actor.workspace_id) == ("thread-1", "ws-1")
        assert mount.sitting == "thread-1:run-1"

    @pytest.mark.asyncio
    async def test_a_subagent_saves_as_itself(self, fake, run):
        _, _, (_read, write, _edit) = _tools()
        run("run-1", subagent="research")
        await write.ainvoke({"file_path": NOTE, "content": "x\n"})

        actor = fake.writes[0]["actor"]
        assert (actor.agent, actor.run_id, actor.sitting) == ("research", "run-1", "thread-1:run-1")

    @pytest.mark.asyncio
    async def test_one_runs_read_does_not_condition_anothers_write(self, fake, run):
        _, _, (read, write, _edit) = _tools()
        fake.files["notes/a.md"] = b"first\n"
        run("run-1", subagent="research", task="one")
        await read.ainvoke({"file_path": NOTE})
        run("run-1", subagent="research", task="two")
        await write.ainvoke({"file_path": NOTE, "content": "second\n"})

        assert fake.writes[0]["if_match"] is None


class TestParallelCalls:
    @pytest.mark.asyncio
    async def test_two_edits_at_once_both_land(self, fake, run):
        _, _, (_read, _write, edit) = _tools()
        fake.files["notes/a.md"] = b"alpha\nbeta\n"
        run("run-1")

        outs = await asyncio.gather(
            edit.ainvoke({"file_path": NOTE, "old_string": "alpha", "new_string": "ALPHA"}),
            edit.ainvoke({"file_path": NOTE, "old_string": "beta", "new_string": "BETA"}),
        )

        assert all(o.startswith(f"Edited {NOTE}") for o in outs), outs
        assert fake.files["notes/a.md"] == b"ALPHA\nBETA\n"

    @pytest.mark.asyncio
    async def test_an_edit_is_conditional_on_the_text_it_read(self, fake, run):
        _, _, (read, _write, edit) = _tools()
        fake.files["notes/a.md"] = b"first\n"
        run("run-1")
        await read.ainvoke({"file_path": NOTE})
        fake.files["notes/a.md"] = b"the person's edit\n"

        out = await edit.ainvoke({"file_path": NOTE, "old_string": "edit", "new_string": "note"})

        assert out.startswith(f"Edited {NOTE}")
        assert fake.writes[0]["if_match"] == hashlib.sha256(b"the person's edit\n").hexdigest()

    @pytest.mark.asyncio
    async def test_a_read_and_a_write_at_once_lose_nothing(self, fake, run):
        _, _, (read, write, _edit) = _tools()
        fake.files["notes/a.md"] = b"first\n"
        run("run-1")

        await asyncio.gather(
            read.ainvoke({"file_path": NOTE}),
            write.ainvoke({"file_path": NOTE, "content": "second\n"}),
        )
        out = await write.ainvoke({"file_path": NOTE, "content": "third\n"})

        assert out.startswith(f"Saved 6 bytes to {NOTE}")
        assert fake.files["notes/a.md"] == b"third\n"


class TestRefusals:
    @pytest.mark.asyncio
    async def test_a_write_over_a_file_changed_since_its_read_is_refused(self, fake, run):
        _, _, (read, write, _edit) = _tools()
        fake.files["notes/a.md"] = b"first\n"
        run("run-1")
        await read.ainvoke({"file_path": NOTE})
        fake.files["notes/a.md"] = b"the person's edit\n"

        out = await write.ainvoke({"file_path": NOTE, "content": "stale\n"})

        assert out == (
            f"ERROR: {NOTE} changed in the Drive since you last read it, so it was "
            "not saved. Read it again and make your change on what it holds now."
        )
        assert fake.files["notes/a.md"] == b"the person's edit\n"

    @pytest.mark.asyncio
    async def test_a_read_conditions_a_write_of_the_same_name_spelled_otherwise(self, fake, run):
        # One name to the Drive: composed and decomposed é, in another case.
        _, _, (read, write, _edit) = _tools()
        fake.files["notes/caf\u00e9.md"] = b"first\n"
        run("run-1")
        await read.ainvoke({"file_path": ".agents/drive/personal/notes/caf\u00e9.md"})

        await write.ainvoke(
            {"file_path": ".agents/drive/personal/notes/CAFE\u0301.md", "content": "second\n"}
        )

        assert fake.writes[0]["if_match"] == hashlib.sha256(b"first\n").hexdigest()

    @pytest.mark.asyncio
    async def test_a_file_never_read_is_written_as_asked(self, fake, run):
        _, _, (_read, write, _edit) = _tools()
        run("run-1")
        out = await write.ainvoke({"file_path": NOTE, "content": "new\n"})
        assert out.startswith(f"Saved 4 bytes to {NOTE} in the Drive")
        assert fake.writes[0]["if_match"] is None

    @pytest.mark.asyncio
    async def test_a_file_outside_a_space_folder_says_where_files_go(self, fake, run):
        _, _, (_read, write, _edit) = _tools()
        run("run-1")
        out = await write.ainvoke({"file_path": ".agents/drive/x.md", "content": "x"})
        assert out == (
            "ERROR: .agents/drive/x.md is not a file in the Drive: its files go "
            "under .agents/drive/personal/."
        )

    @pytest.mark.asyncio
    async def test_a_save_over_the_size_limit_names_it_as_the_server_does(self, fake, run, monkeypatch):
        async def refuse(*_a, **_k):
            raise DriveError("too_large", "A file is at most 50 MB")

        monkeypatch.setattr(fake, "write", refuse)
        _, _, (_read, write, _edit) = _tools()
        run("run-1")
        out = await write.ainvoke({"file_path": NOTE, "content": "x"})
        assert out == (
            f"ERROR: {NOTE} was not saved: a file is at most 50 MB in one save. Write it in the "
            "workspace, then copy it in with `cp` where Bash reaches the Drive, or `la drive put`."
        )

    @pytest.mark.parametrize(
        ("code", "says"),
        [
            ("over_quota", "they can delete files or clear a file's earlier versions"),
            ("too_many_entries", "they can empty its trash or remove files"),
            ("invalid_path", "Pick another name."),
            ("unavailable", "try again shortly"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_refused_save_tells_the_agent_what_to_do(self, fake, run, monkeypatch, code, says):
        async def refuse(*_a, **_k):
            raise DriveError(code, "the reason")

        monkeypatch.setattr(fake, "write", refuse)
        _, _, (_read, write, _edit) = _tools()
        run("run-1")
        out = await write.ainvoke({"file_path": NOTE, "content": "x"})
        assert out.startswith(f"ERROR: {NOTE}") or out.startswith("ERROR: The Drive")
        assert says in out

    @pytest.mark.parametrize(
        ("serving", "how"),
        [
            (True, "Open it with code at this same path, which reads it in place, so a large file needs no copy first."),
            (False, "Copy it into the workspace with `la drive get data/x.bin`, then open it with code."),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_binary_file_points_to_code_only_where_the_drive_is_on_disk(
        self, fake, run, serving, how
    ):
        sandbox = _sandbox()
        sandbox.sandbox.drive.on_disk = AsyncMock(
            return_value=DriveDisk.ON if serving else DriveDisk.UNAVAILABLE
        )
        prefix = f"{ROOT}/.agents/drive/"
        route = DriveRoute(service=fake, user_id=USER, sandbox_backend=sandbox, root_prefix=prefix)
        fake.files["data/x.bin"] = b"\x00\x01\x02"
        run("run-1")

        with pytest.raises(DriveRefused) as refused:
            await route.aread_text(f"{prefix}personal/data/x.bin")

        assert str(refused.value).endswith(how)


class TestLargeFiles:
    """A Drive file can be as large as the allowance; one call holds a
    bounded part of it whatever its shape."""

    MB = 1024 * 1024

    def _huge(self, fake, monkeypatch, chunk: bytes, chunks: int, *, allowed: int) -> dict:
        """A file of ``chunks`` copies of ``chunk`` that is never allocated:
        the stream fails once more than ``allowed`` bytes are taken from it."""
        taken = {"bytes": 0}
        size = len(chunk) * chunks
        entry = _entry("data/big.jsonl", b"x")
        entry = dataclasses.replace(entry, size=size, is_text=False, content_type="application/x-ndjson")

        async def content(space, path):
            return entry, Content("space-1", "f" * 64, size, False, entry.content_type, None)

        async def stream(found, start=0, end=None):
            for _ in range(chunks):
                taken["bytes"] += len(chunk)
                assert taken["bytes"] <= allowed, "the route held the whole file"
                yield chunk

        monkeypatch.setattr(fake, "content", content)
        monkeypatch.setattr(fake, "stream", stream)
        return taken

    @pytest.mark.asyncio
    async def test_a_read_of_a_huge_single_line_file_holds_a_bounded_part(self, fake, run, monkeypatch):
        taken = self._huge(fake, monkeypatch, b"a" * self.MB, 400, allowed=4 * self.MB)
        _, _, (read, _write, _edit) = _tools()
        run("run-1")

        out = await read.ainvoke({"file_path": ".agents/drive/personal/data/big.jsonl"})

        assert out == (
            "ERROR: Line 1 of .agents/drive/personal/data/big.jsonl is longer than Read shows "
            "at once (160000 characters). Copy it into the workspace with "
            "`la drive get data/big.jsonl`, then read it in parts with code."
        )
        assert taken["bytes"] <= self.MB

    @pytest.mark.asyncio
    async def test_a_read_past_long_lines_shows_its_own_window(self, fake, run, monkeypatch):
        line = b"b" * (64 * 1024 - 1) + b"\n"
        self._huge(fake, monkeypatch, line, 400, allowed=400 * len(line))
        _, _, (read, _write, _edit) = _tools()
        run("run-1")

        out = await read.ainvoke(
            {"file_path": ".agents/drive/personal/data/big.jsonl", "offset": 200, "limit": 5}
        )

        assert out.startswith("   201\tbbb")
        assert "You saw lines 201..202." in out

    @pytest.mark.asyncio
    async def test_a_read_far_into_a_huge_file_scans_a_bounded_part(self, fake, run, monkeypatch):
        line = b"c" * (64 * 1024 - 1) + b"\n"
        taken = self._huge(fake, monkeypatch, line * 16, 3000, allowed=20 * self.MB)
        _, _, (read, _write, _edit) = _tools()
        run("run-1")

        out = await read.ainvoke(
            {"file_path": ".agents/drive/personal/data/big.jsonl", "offset": 40000, "limit": 10}
        )

        assert out == (
            "ERROR: Line 40001 of .agents/drive/personal/data/big.jsonl is past the first 16.0 MB, "
            "as far into a Drive file as Read goes. Copy it into the workspace with "
            "`la drive get data/big.jsonl`, then read that part with code, such as DuckDB or a few "
            "lines of Python."
        )
        assert taken["bytes"] <= 17 * self.MB

    @pytest.mark.parametrize(
        ("chunk", "offset", "line"),
        [
            # Lines end as str.splitlines ends them, as a file held as text is read.
            (b"one\rtwo\rthree\r", 1, "two"),
            # A CRLF cut between two chunks is one line end.
            (b"\nx\r", 2, "x"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_streamed_read_splits_lines_as_a_held_one_does(
        self, fake, run, monkeypatch, chunk, offset, line
    ):
        self._huge(fake, monkeypatch, chunk, 3, allowed=3 * len(chunk))
        _, _, (read, _write, _edit) = _tools()
        run("run-1")

        out = await read.ainvoke(
            {"file_path": ".agents/drive/personal/data/big.jsonl", "offset": offset, "limit": 1}
        )

        assert out.split("\n")[0] == f"{offset + 1:6}\t{line}"

    @pytest.mark.asyncio
    async def test_an_edit_of_a_file_too_large_to_hold_reads_none_of_it(self, fake, run, monkeypatch):
        self._huge(fake, monkeypatch, b"a" * self.MB, 400, allowed=0)
        _, _, (_read, _write, edit) = _tools()
        run("run-1")

        out = await edit.ainvoke(
            {"file_path": ".agents/drive/personal/data/big.jsonl", "old_string": "a", "new_string": "b"}
        )

        assert out == (
            "ERROR: .agents/drive/personal/data/big.jsonl is 400.0 MB, more than the file tools "
            "take whole from the Drive (16.0 MB); Read pages through only its first 16.0 MB. Copy "
            "it into the workspace with `la drive get data/big.jsonl`, change it with code, and "
            "send it back with `la drive put`."
        )


class TestSearch:
    @pytest.mark.asyncio
    async def test_grep_searches_inside_the_drive_and_not_from_the_folder(self, fake, run):
        backend, _, _ = _tools()
        fake.files.update({"notes/a.md": b"ZEPH here\n", "notes/b.csv": b"ZEPH,1\n"})
        grep = create_grep_tool(backend)
        run("run-1")

        inside = await grep.ainvoke({"pattern": "ZEPH", "path": ".agents/drive/personal", "glob": "*.md"})
        from_folder = await grep.ainvoke({"pattern": "ZEPH"})

        assert inside.splitlines()[1:] == ["/.agents/drive/personal/notes/a.md"]
        assert from_folder.startswith("No matches found")

    @pytest.mark.asyncio
    async def test_a_grep_names_the_data_files_it_did_not_search(self, fake, run):
        backend, _, _ = _tools()
        fake.files.update({"notes/a.md": b"ZEPH\n", "data/p.parquet": b"PAR1\x00ZEPH", "img/x.png": b"\x00"})
        run("run-1")

        out = await create_grep_tool(backend).ainvoke({"pattern": "ZEPH", "path": ".agents/drive/personal"})

        assert out.endswith(
            "[Grep does not search Drive data files (.agents/drive/personal/data/p.parquet); "
            "query them with code if they may hold the match.]"
        )
        assert "x.png" not in out

    @pytest.mark.asyncio
    async def test_grep_pages_follow_path_order_whatever_order_the_drive_sends(self, fake, run):
        backend, _, _ = _tools()
        # The service sends z.md first: its id sorts before a.md's.
        fake.files.update({"notes/a.md": b"ZEPH\n", "notes/z.md": b"ZEPH\n"})
        grep = create_grep_tool(backend)
        run("run-1")

        pages = [
            (await grep.ainvoke(
                {"pattern": "ZEPH", "path": ".agents/drive/personal", "head_limit": 1, "offset": at}
            )).splitlines()[1:]
            for at in (0, 1)
        ]

        assert pages == [["/.agents/drive/personal/notes/a.md"], ["/.agents/drive/personal/notes/z.md"]]

    @pytest.mark.asyncio
    async def test_a_search_of_one_folder_reads_only_that_folder(self, fake, run):
        backend, _, _ = _tools()
        fake.files.update({"notes/a.md": b"ZEPH\n", "other/b.md": b"ZEPH\n"})
        run("run-1")

        globbed = await create_glob_tool(backend).ainvoke(
            {"pattern": ".agents/drive/personal/notes/*.md"}
        )
        grepped = await create_grep_tool(backend).ainvoke(
            {"pattern": "ZEPH", "path": ".agents/drive/personal/notes"}
        )

        assert globbed.splitlines()[1:] == ["/.agents/drive/personal/notes/a.md"]
        assert grepped.splitlines()[1:] == ["/.agents/drive/personal/notes/a.md"]
        assert fake.trees == ["notes", "notes"]
        assert fake.unders == ["notes"]

    @pytest.mark.asyncio
    async def test_bytes_the_store_cannot_hand_over_refuse_the_read(self, fake, run, monkeypatch):
        backend, _, _ = _tools()
        fake.files["c.png"] = b"\x89PNG"

        async def down(*_a, **_k):
            raise DriveError("unavailable", "down")

        monkeypatch.setattr(fake, "read_range", down)
        run("run-1")
        with pytest.raises(DriveRefused, match="try again shortly"):
            await backend.adownload_file_bytes_capped(".agents/drive/personal/c.png", lambda _: 1 << 20)

    @pytest.mark.asyncio
    async def test_a_drive_that_does_not_answer_leaves_the_rest_of_a_glob(self, fake, run, monkeypatch):
        backend, _, _ = _tools()
        backend.sandbox.aglob_paths = AsyncMock(return_value=[f"{ROOT}/.agents/x.md"])

        async def down(*_a, **_k):
            raise DriveError("unavailable", "down")

        monkeypatch.setattr(fake, "tree", down)
        run("run-1")
        out = await create_glob_tool(backend).ainvoke({"pattern": f"{ROOT}/{{.agents/x.md,.agents/drive/personal/**/*.md}}"})

        assert "/.agents/x.md" in out
        assert out.endswith(drive_route.UNSEARCHED)

    @pytest.mark.asyncio
    async def test_a_grep_the_drive_stops_answering_keeps_what_it_found(self, fake, run, monkeypatch):
        backend, _, _ = _tools()
        fake.files.update({"notes/a.md": b"ZEPH\n", "notes/b.md": b"ZEPH\n"})
        fake.page_size = 1
        real = fake.text_pages

        async def breaking(*a, **k):
            async for page in real(*a, **k):
                yield page
                raise DriveError("unavailable", "down")

        monkeypatch.setattr(fake, "text_pages", breaking)
        run("run-1")
        out = await create_grep_tool(backend).ainvoke(
            {"pattern": "ZEPH", "path": ".agents/drive/personal/notes"}
        )

        # One page came before the Drive stopped: the file it held, alone.
        assert ("notes/a.md" in out) != ("notes/b.md" in out)
        assert drive_route.STOPPED in out

    @pytest.mark.asyncio
    async def test_a_grep_cancelled_mid_fetch_leaves_nothing_reading(self, fake, run):
        backend, _, _ = _tools()
        fake.files.update({f"notes/{i}.md": b"ZEPH\n" for i in range(3)})
        # A page a file, so the fetch that never answers is the one read ahead.
        fake.page_size = 1
        fake.stalled = asyncio.Event()
        run("run-1")
        others = asyncio.all_tasks()
        grep = asyncio.ensure_future(
            create_grep_tool(backend).ainvoke({"pattern": "ZEPH", "path": ".agents/drive/personal"})
        )
        await asyncio.wait_for(fake.stalled.wait(), 5)

        grep.cancel()
        with pytest.raises(asyncio.CancelledError):
            await grep

        assert fake.fetching == 0
        assert not [t for t in asyncio.all_tasks() - others if not t.done()]
