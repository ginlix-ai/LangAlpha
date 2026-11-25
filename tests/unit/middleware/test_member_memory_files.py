"""Settled contract: a member's memory files reach their own calls alone.

On a member's turn in a shared workspace whose organization grants it, the
main agent's Read and Glob reach that member's personal memory, read only,
at ``.agents/members/<short id>/memory/``. The thread keeps a stub of every
such read; only calls in that member's streak are sent what it returned.
Grep is refused there, no search from above lists it, Bash and code cannot
name it, and every tool but the main agent's own Read, Write, Edit and Glob
is built without it.
"""

from __future__ import annotations

import re

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import Field

from ptc_agent.agent.backends import user_memory_namespace
from ptc_agent.agent.backends.composite import CompositeFilesystemBackend
from ptc_agent.agent.filesystem_routes import subagent_backend
from ptc_agent.agent.middleware.compaction.summary_request import _summarizable
from ptc_agent.agent.middleware.member_context import (
    MEMBER_READ_KEY,
    MemberContext,
    MemberContextMiddleware,
    member_memory,
)
from ptc_agent.agent.middleware.member_context.files import MEMORY_READ_CHARS, alias_of, close_members
from ptc_agent.agent.middleware.runtime_context import RowCarrierMiddleware
from ptc_agent.agent.middleware.sender_naming import SenderNamingMiddleware
from ptc_agent.agent.tools.grep import create_grep_tool
from ptc_agent.agent.transcript.render import render_segment
from ptc_agent.agent.transcript.sender import MEMBER_SENDER_KEY
from ptc_agent.core.paths import WorkspaceLayout

ANA = "a0000000-0000-4000-8000-000000000001"
BO = "b0000000-0000-4000-8000-000000000002"
ROOT = "/home/workspace"
LAYOUT = WorkspaceLayout(ROOT, "")
SECRET = "the code word is PERIWINKLE"
KEY = b"test-deployment-key".ljust(32, b"\0")
MEMBER = MemberContext(user_id=ANA, name="Ana", handle="1a2b3c", workspace_id="ws-1", alias_key=KEY)
FOLDER = ".agents/members/1a2b3c/memory/"
#: ``codeword.md`` under its alias, the only name the agent is shown.
ALIAS = "3c3602b8bead953a.md"
FILE = f"{FOLDER}{ALIAS}"
#: ``plans/launch.md``, a file in a folder, which names ``codeword.md``.
NOTES = "plans/launch.md"
STUB = (
    "[Read from Ana's memory. What it returned is shown to you only on Ana's messages while "
    "this workspace uses members' own context; where it is not shown here, it was there when "
    "read and is hidden now.]"
)


def _normalize(path: str) -> str:
    if path in (".", ""):
        return ROOT
    return path if path.startswith("/") else f"{ROOT}/{path.removeprefix('./')}"


def _sandbox() -> MagicMock:
    sb = MagicMock()
    sb.computer_root = ROOT
    sb.workspace_dir = ROOT
    sb.normalize_path.side_effect = _normalize
    sb.virtualize_path.side_effect = lambda p: p[len(ROOT) + 1 :] if p.startswith(f"{ROOT}/") else p
    sb.validate_path.return_value = True
    sb.filesystem_config.enable_path_validation = False
    sb.aread_range = AsyncMock(return_value=None)
    sb.aread_text = AsyncMock(return_value=None)
    sb.aglob_paths = AsyncMock(return_value=[f"{ROOT}/report.md"])
    sb.agrep_rich = AsyncMock(return_value=[])
    return sb


async def _files(sandbox: MagicMock | None = None):
    store = InMemoryStore()
    await store.aput(user_memory_namespace(ANA), "memory.md", {"content": "- [Code word](codeword.md)"})
    await store.aput(user_memory_namespace(ANA), "codeword.md", {"content": SECRET})
    await store.aput(user_memory_namespace(ANA), NOTES, {"content": "See codeword.md before the launch."})
    sandbox = sandbox or _sandbox()
    shared = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
    mount = member_memory(MEMBER, store=store, files=shared, sandbox=sandbox, layout=LAYOUT)
    # Grep over the backend the memory is mounted on, though the agent's is
    # built without it, to pin that no search from above reaches it.
    tools = {"Read": mount.read, "Write": mount.write, "Edit": mount.edit, "Glob": mount.glob,
             "Grep": create_grep_tool(mount.files)}
    middleware = MemberContextMiddleware(MEMBER, mount=mount)
    return middleware, tools, mount.files


async def _call(middleware, tools, name: str, args: dict, call_id: str) -> ToolMessage:
    async def handler(request):
        content = await tools[name].ainvoke(request.tool_call["args"])
        return ToolMessage(content=content, tool_call_id=call_id, name=name)

    request = SimpleNamespace(tool_call={"name": name, "args": args, "id": call_id})
    return await middleware.awrap_tool_call(request, handler)


def _says(text: str, user_id: str = ANA, name: str = "Ana", *, mid: str) -> HumanMessage:
    return HumanMessage(content=text, id=mid, additional_kwargs={MEMBER_SENDER_KEY: {"user_id": user_id, "name": name}})


def _read_round(call_id: str, stub: ToolMessage) -> list[Any]:
    return [AIMessage("", tool_calls=[{"id": call_id, "name": "Read", "args": {"file_path": FILE}}]), stub]


class TestAReadIsStoredAsAStub:
    @pytest.mark.asyncio
    async def test_the_thread_keeps_a_stub_and_the_streak_is_sent_the_file(self):
        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        assert stub.content == STUB
        assert stub.additional_kwargs[MEMBER_READ_KEY] == {
            "user_id": ANA, "tool": "Read", "args": {"file_path": FILE},
        }
        assert SECRET not in repr(stub.model_dump())

        history = [_says("what is my code word?", mid="m1"), *_read_round("c1", stub)]
        sent = await middleware.compose(history)
        assert SECRET in sent[-1].content
        assert MEMBER_READ_KEY not in sent[-1].additional_kwargs
        # The thread's own copy is untouched.
        assert history[-1].content == STUB

    @pytest.mark.asyncio
    async def test_a_later_turn_of_the_streak_reads_it_again_to_the_same_bytes(self):
        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        history = [_says("one", mid="m1"), *_read_round("c1", stub), AIMessage("done"), _says("two", mid="m2")]
        first = await middleware.compose(history)
        later, _, _ = await _files()
        assert await later.compose(history) == first

    @pytest.mark.asyncio
    async def test_another_members_turn_sees_only_the_stub(self):
        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        history = [_says("one", mid="m1"), *_read_round("c1", stub), AIMessage("done"),
                   _says("what is Ana's code word?", BO, "Bo", mid="m2")]
        assert await middleware.compose(history) is None
        # Nor does a new streak of Ana's after Bo fill a read from before him.
        history.append(_says("again", mid="m3"))
        sent = await middleware.compose(history)
        assert sent[2].content == STUB

    @pytest.mark.asyncio
    async def test_a_read_that_fails_again_leaves_the_stub(self):
        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        history = [_says("one", mid="m1"), *_read_round("c1", stub), AIMessage("done"), _says("two", mid="m2")]
        # A later turn, the file since renamed: its old alias reads nothing.
        later, _, _ = await _files()
        gone = FILE.replace("/memory/", "/memory/missing-")
        renamed = stub.model_copy(update={"additional_kwargs": {
            MEMBER_READ_KEY: {"user_id": ANA, "tool": "Read", "args": {"file_path": gone}},
        }})
        sent = await later.compose([history[0], *_read_round("c1", renamed), *history[3:]])
        assert sent[2].content == STUB

    @pytest.mark.asyncio
    async def test_a_read_that_raises_stores_no_text_of_it(self):
        middleware, tools, _ = await _files()

        async def handler(request):
            raise RuntimeError(f"no such key {SECRET}")

        request = SimpleNamespace(tool_call={"name": "Read", "args": {"file_path": FILE}, "id": "c1"})
        result = await middleware.awrap_tool_call(request, handler)
        assert result.status == "error"
        assert SECRET not in result.content

    @pytest.mark.asyncio
    async def test_aliasing_that_fails_stores_no_text_of_it(self, monkeypatch):
        middleware, tools, _ = await _files()

        ran = []

        async def failing(_text, *, source=None):
            ran.append(source)
            raise RuntimeError(f"listing failed at {SECRET}")

        monkeypatch.setattr(middleware._mount.memory, "aliased_text", failing)
        result = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        assert ran, "the aliasing ran and raised"
        assert result.status == "error" and SECRET not in result.content
        assert not any(call_id == "c1" for call_id, _ in middleware._reads)

    @pytest.mark.asyncio
    async def test_a_read_that_finds_nothing_is_kept_as_its_error_not_a_stub(self):
        middleware, tools, _ = await _files()
        result = await _call(middleware, tools, "Read", {"file_path": f"{FOLDER}0000000000000000.md"}, "c1")
        assert result.status == "error" and result.content.startswith("ERROR")
        assert MEMBER_READ_KEY not in (result.additional_kwargs or {})
        assert not any(call_id == "c1" for call_id, _ in middleware._reads)

    @pytest.mark.asyncio
    async def test_a_glob_of_the_folder_is_a_stub_too(self):
        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Glob", {"pattern": "*.md", "path": FOLDER}, "c1")
        assert stub.content.startswith("[Listed Ana's memory.")
        history = [_says("one", mid="m1"),
                   AIMessage("", tool_calls=[{"id": "c1", "name": "Glob", "args": {"pattern": "*.md", "path": FOLDER}}]),
                   stub]
        listed = (await middleware.compose(history))[-1].content
        assert ALIAS in listed and "codeword" not in listed and "launch" not in listed

    @pytest.mark.asyncio
    async def test_two_reads_under_one_call_id_each_get_their_own_file(self):
        middleware, tools, _ = await _files()
        notes = f"{FOLDER}{alias_of(KEY, MEMBER, NOTES)}"
        first = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        second = await _call(middleware, tools, "Read", {"file_path": notes}, "c1")
        history = [
            _says("one", mid="m1"), *_read_round("c1", first),
            AIMessage("", tool_calls=[{"id": "c1", "name": "Read", "args": {"file_path": notes}}]), second,
        ]
        for composer in (middleware, MemberContextMiddleware(MEMBER, mount=middleware._mount)):
            sent = await composer.compose(history)
            assert SECRET in sent[2].content and "before the launch" not in sent[2].content
            assert "before the launch" in sent[4].content and SECRET not in sent[4].content

    @pytest.mark.asyncio
    async def test_a_failure_the_tool_caught_is_kept_without_its_text(self):
        middleware, _, _ = await _files()
        leaked = {"Read": "ERROR: Failed to read file: key 'secret plans.md': boom",
                  "Glob": "ERROR: Failed to glob files: key 'secret plans.md': boom"}

        for tool, text in leaked.items():
            async def handler(request, text=text, tool=tool):
                return ToolMessage(content=text, tool_call_id="c1", name=tool)

            arg = {"Read": "file_path", "Glob": "path"}[tool]
            request = SimpleNamespace(tool_call={"name": tool, "args": {arg: FILE if tool == "Read" else FOLDER}, "id": "c1"})
            result = await middleware.awrap_tool_call(request, handler)
            assert result.status == "error" and "secret" not in result.content
            assert result.content == "ERROR: reading Ana's memory failed. Try again."

    @pytest.mark.asyncio
    async def test_any_other_read_is_left_as_it_is(self):
        middleware, tools, _ = await _files()
        result = await _call(middleware, tools, "Read", {"file_path": "report.md"}, "c1")
        assert MEMBER_READ_KEY not in result.additional_kwargs


@pytest.mark.asyncio
async def test_a_workspace_file_naming_the_folder_is_no_read_of_it():
    sandbox = _sandbox()
    text = f"Ana keeps hers at {FOLDER}codeword.md\n"
    sandbox.aread_range = AsyncMock(return_value=text)
    sandbox.aread_text = AsyncMock(return_value=text)
    middleware, tools, _ = await _files(sandbox)
    result = await _call(middleware, tools, "Read", {"file_path": "report.md"}, "c1")
    assert MEMBER_READ_KEY not in result.additional_kwargs and "Ana keeps hers" in result.content
    # Nor is a search that found that file refused.
    found = f"report.md:1:{text}"

    async def grep(request):
        return ToolMessage(content=found, tool_call_id="c2", name="Grep")

    request = SimpleNamespace(tool_call={"name": "Grep", "args": {"pattern": "hers"}, "id": "c2"})
    assert (await middleware.awrap_tool_call(request, grep)).content == found


class TestOnlyReadAndGlobReachIt:
    @pytest.mark.asyncio
    async def test_grep_of_the_folder_is_refused_without_running(self):
        middleware, tools, _ = await _files()
        ran = AsyncMock()
        request = SimpleNamespace(tool_call={"name": "Grep", "args": {"pattern": "code", "path": FOLDER}, "id": "c1"})
        refused = await middleware.awrap_tool_call(request, ran)
        ran.assert_not_awaited()
        assert refused.status == "error" and "Grep cannot search Ana's memory" in refused.content

    @pytest.mark.asyncio
    async def test_no_search_from_above_lists_it(self):
        _, tools, files = await _files()
        parent = ".agents/members/1a2b3c"
        for pattern, path in (("**/*.md", ROOT), (f"{FOLDER}*", ROOT), ("**/*", parent)):
            assert await files.aglob_paths(pattern, path) == [f"{ROOT}/report.md"]
            listed = await tools["Glob"].ainvoke({"pattern": pattern, "path": path})
            # The tool echoes the pattern; what it lists is the folder's own.
            assert listed.splitlines()[1:] == ["report.md"]
        for path in (ROOT, ".agents", parent):
            found = await files.agrep_rich("PERIWINKLE", path, output_mode="content")
            assert SECRET not in repr(found)
            found = await tools["Grep"].ainvoke({"pattern": "PERIWINKLE", "path": path})
            assert found.startswith("No matches") and SECRET not in found

    @pytest.mark.asyncio
    async def test_a_file_left_on_disk_under_members_is_not_searched_from_above(self):
        # The folder is closed in every shared build; whatever a command left
        # there on disk is no member's memory, and a search above it says so
        # by not listing it.
        sandbox = _sandbox()
        planted = f"{ROOT}/.agents/members/1a2b3c/memory/codeword.md"
        sandbox.aglob_paths = AsyncMock(return_value=[f"{ROOT}/report.md", planted])
        sandbox.agrep_rich = AsyncMock(return_value=[f"{ROOT}/report.md", planted])
        files = close_members(
            CompositeFilesystemBackend(sandbox=sandbox, routes=[]),
            store=InMemoryStore(), sandbox=sandbox, layout=LAYOUT,
        )
        assert await files.aglob_paths("**/*.md", ROOT) == [f"{ROOT}/report.md"]
        assert await files.agrep_rich("PERIWINKLE", ".agents") == [f"{ROOT}/report.md"]
        # Nor a search aimed inside it with a filter the walk cannot follow,
        # which goes to the disk as rg's own answer.
        from ptc_agent.agent.backends.search_match import RgRefused, RgWalk

        odd = "{*.md,{*.txt,*.json}}"
        with pytest.raises(RgRefused):
            RgWalk(odd, None, ROOT)
        found = await files.agrep_rich("PERIWINKLE", ".agents/members", glob=odd)
        assert planted not in found and sandbox.agrep_rich.await_args.kwargs["glob"] == odd

    @pytest.mark.asyncio
    async def test_a_file_left_on_disk_under_members_is_not_attached(self):
        # A PNG or PDF Read takes the capped download, not the text read.
        sandbox = _sandbox()
        sandbox.adownload_file_bytes_capped = AsyncMock(return_value=b"\x89PNG")
        closed = close_members(
            CompositeFilesystemBackend(sandbox=sandbox, routes=[]),
            store=InMemoryStore(), sandbox=sandbox, layout=LAYOUT,
        )
        _, _, mounted = await _files(sandbox)

        for files, path in ((closed, ".agents/members/x/memory/c.png"), (mounted, f"{FOLDER}c.png")):
            assert await files.adownload_file_bytes_capped(path, lambda _: 1 << 20) is None
        sandbox.adownload_file_bytes_capped.assert_not_called()

    @pytest.mark.asyncio
    async def test_writes_are_refused_with_who_to_ask(self):
        _, tools, _ = await _files()
        for name, args in (
            ("Write", {"file_path": FILE, "content": "changed"}),
            ("Edit", {"file_path": FILE, "old_string": "PERIWINKLE", "new_string": "x"}),
        ):
            result = await tools[name].ainvoke(args)
            assert "Ana's memory is read-only in a shared workspace" in result

    @pytest.mark.asyncio
    async def test_only_the_main_agents_own_backend_has_it(self):
        store, sandbox = InMemoryStore(), _sandbox()
        shared = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=shared, sandbox=sandbox, layout=LAYOUT)
        assert mount.files.route_for(FILE) is mount.memory
        assert mount.inside(FILE) and mount.inside(FOLDER.rstrip("/"))
        assert not mount.inside("report.md") and not mount.inside(f"{FOLDER}../../x")
        # Every other tool is built over the backend it was mounted on.
        assert shared.route_for(FILE) is None
        assert subagent_backend(shared, LAYOUT).route_for(FILE) is None

    def test_the_mount_never_serves_it(self):
        from ptc_agent.agent.middleware.member_context import MemberMemory

        assert MemberMemory.mountable is False


class TestEveryEntryGoesByAnAlias:
    """No file or folder in a member's memory is shown by its real name."""

    def test_the_same_entry_has_the_same_alias_every_time(self):
        again = MemberContext(user_id=ANA, name="Ana B", handle="1a2b3c", workspace_id="ws-1",
                              alias_key=bytes(KEY))
        assert alias_of(KEY, MEMBER, "codeword.md") == alias_of(again.alias_key, again, "codeword.md") == ALIAS

    def test_two_members_files_of_one_name_differ(self):
        bo = MemberContext(user_id=BO, name="Bo", handle="4d5e6f", workspace_id="ws-1", alias_key=KEY)
        elsewhere = MemberContext(user_id=ANA, name="Ana", handle="1a2b3c", workspace_id="ws-2", alias_key=KEY)
        aliases = {alias_of(KEY, m, "codeword.md") for m in (MEMBER, bo, elsewhere)}
        assert len(aliases) == 3
        # Nor does another deployment's key give the same one.
        assert alias_of(b"another-deployment", MEMBER, "codeword.md") != ALIAS

    def test_folders_go_by_aliases_and_a_plain_extension_is_kept(self):
        alias = alias_of(KEY, MEMBER, "plans/q3/launch.md")
        folder, sub, name = alias.split("/")
        assert name.endswith(".md") and "plans" not in alias and "q3" not in alias and "launch" not in alias
        assert alias_of(KEY, MEMBER, "plans/other.md").split("/")[0] == folder
        # A folder's alias is never its file namesake's.
        assert alias_of(KEY, MEMBER, "plans") != folder
        assert alias_of(KEY, MEMBER, "memory.md") == "memory.md"
        for odd in ("notes.Ana-Lopez", "notes.AnaLopez", "notes.pdf.tar.gz-backup", "noext", "scan.PDF"):
            assert "." not in alias_of(KEY, MEMBER, odd)
        assert alias_of(KEY, MEMBER, "data.JSON").endswith(".JSON")

    @pytest.mark.asyncio
    async def test_a_real_name_or_an_unknown_alias_is_not_found(self):
        _, tools, files = await _files()
        for path in (f"{FOLDER}codeword.md", f"{FOLDER}0000000000000000.md", f"{FOLDER}{NOTES}"):
            assert "File not found" in await tools["Read"].ainvoke({"file_path": path})
            assert await files.aread_text(path) is None
        assert await files.aread_text(FILE) == SECRET

    @pytest.mark.asyncio
    async def test_an_absolute_pattern_in_the_folder_lists_it(self):
        _, _, files = await _files()
        listed = await files.aglob_paths(f"{ROOT}/{FOLDER}*.md", FOLDER)
        assert f"{ROOT}/{FILE}" in listed

    @pytest.mark.asyncio
    async def test_a_glob_lists_aliases_and_a_real_name_finds_nothing(self):
        _, tools, files = await _files()
        listed = await files.aglob_paths("**/*", FOLDER)
        notes = alias_of(KEY, MEMBER, NOTES)
        assert sorted(p.removeprefix(f"{ROOT}/{FOLDER}") for p in listed) == sorted(["memory.md", ALIAS, notes])
        assert await files.aglob_paths("codeword*", FOLDER) == []
        assert await files.aglob_paths("plans/*", FOLDER) == []
        assert "launch" not in await tools["Glob"].ainvoke({"pattern": "**/*", "path": FOLDER})

    @pytest.mark.asyncio
    async def test_a_file_naming_another_is_sent_its_alias(self):
        middleware, tools, _ = await _files()
        notes = f"{FOLDER}{alias_of(KEY, MEMBER, NOTES)}"
        stub = await _call(middleware, tools, "Read", {"file_path": notes}, "c1")
        assert stub.additional_kwargs[MEMBER_READ_KEY]["args"] == {"file_path": notes}
        history = [_says("one", mid="m1"),
                   AIMessage("", tool_calls=[{"id": "c1", "name": "Read", "args": {"file_path": notes}}]), stub]
        text = (await middleware.compose(history))[-1].content
        assert f"See {ALIAS} before the launch." in text and "codeword" not in text


    @pytest.mark.asyncio
    async def test_a_folder_named_on_its_own_is_sent_its_alias(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "taxes/2026/return.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        top, year = alias_of(KEY, MEMBER, "taxes/2026/return.md").split("/")[:2]
        text = await memory.aliased_text(
            "See taxes/ and ./taxes/2026/ (taxes/2026/receipts.md is gone). Taxes are due."
        )
        assert text == (
            f"See {top}/ and ./{top}/{year}/ ({top}/{year}/receipts.md is gone). Taxes are due."
        )

    @pytest.mark.asyncio
    async def test_a_link_relative_to_a_folder_is_sent_the_aliass_tail(self):
        store = InMemoryStore()
        for key in ("plans/taxes.pdf", "plans/q3/notes.md", "work/notes.md", "top.md"):
            await store.aput(user_memory_namespace(ANA), key, {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        taxes = alias_of(KEY, MEMBER, "plans/taxes.pdf").split("/")[1]
        q3, notes = alias_of(KEY, MEMBER, "plans/q3/notes.md").split("/")[1:]
        text = await memory.aliased_text("See taxes.pdf, ../taxes.pdf, q3/notes.md and notes.md.")
        # ``notes.md`` is the tail of two entries, so no alias is right.
        assert text == f"See {taxes}, ../{taxes}, {q3}/{notes} and [a name in their memory]."
        plans = alias_of(KEY, MEMBER, "plans/taxes.pdf").split("/")[0]
        assert await memory.aliased_text("[Taxes](/plans/taxes.pdf)") == f"[Taxes](/{plans}/{taxes})"
        # A path through their own memory folder points where it is mounted.
        for own in (".agents/user/memory/plans/taxes.pdf", "/home/workspace/Desk/.agents/user/memory/plans/taxes.pdf"):
            assert await memory.aliased_text(f"see {own}") == f"see {FOLDER}{plans}/{taxes}"
        assert await memory.aliased_text("in .agents/user/memory.") == f"in {FOLDER.rstrip('/')}."

    @pytest.mark.asyncio
    async def test_a_link_to_a_folder_without_a_slash_is_sent_its_alias(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "plans/q3/launch.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        plans, q3 = alias_of(KEY, MEMBER, "plans/q3/launch.md").split("/")[:2]
        text = await memory.aliased_text('[Plans](plans), [Q3](./plans/q3 "t") and my plans.\n[p]: <plans>')
        # Only a link's target: the bare word elsewhere is the member's prose.
        assert text == f'[Plans]({plans}), [Q3](./{plans}/{q3} "t") and my plans.\n[p]: <{plans}>'

    @pytest.mark.asyncio
    async def test_a_percent_encoded_link_is_sent_its_alias(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "tax returns/2026 é.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        alias = alias_of(KEY, MEMBER, "tax returns/2026 é.md")
        for written in ("tax%20returns/2026%20%C3%A9.md", "tax%20returns/2026 %c3%a9.md", "tax returns/2026 é.md",
                        "%74ax returns/2026 é%2Emd"):
            assert await memory.aliased_text(f"[Taxes]({written})") == f"[Taxes]({alias})"

    @pytest.mark.parametrize("held", [[], ["kept.md"]])
    @pytest.mark.asyncio
    async def test_a_link_to_an_entry_the_store_no_longer_holds_is_hidden(self, held):
        store = InMemoryStore()
        for key in held:
            await store.aput(user_memory_namespace(ANA), key, {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        text = await memory.aliased_text(
            "- [Plan](secret-plan.md) and [Q](<q3 secret/notes.md>)\n"
            "[old]: ./gone/secret.md\n"
            "- [Index](memory.md), [Site](https://example.com/a.md), [Up](#top)\n"
            "[^1]: a footnote\n"
        )
        assert "secret" not in text
        assert text.startswith("- [Plan]([a name in their memory]) and [Q](<[a name in their memory]>)\n"
                               "[old]: [a name in their memory]\n")
        assert "[Index](memory.md), [Site](https://example.com/a.md), [Up](#top)\n[^1]: a footnote" in text
        if held:
            kept = alias_of(KEY, MEMBER, "kept.md")
            assert await memory.aliased_text("[K](kept.md) [F](.agents/user/memory/kept.md)") == (
                f"[K]({kept}) [F]({FOLDER}{kept})"
            )

    @pytest.mark.asyncio
    async def test_a_link_is_read_whole_as_markdown_spells_it(self):
        store = InMemoryStore()
        for key in ("taxes(2026).md", "tax%20plan notes.md"):
            await store.aput(user_memory_namespace(ANA), key, {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        taxes, plan = alias_of(KEY, MEMBER, "taxes(2026).md"), alias_of(KEY, MEMBER, "tax%20plan notes.md")
        # Escaped and balanced parentheses, and a literal ``%20`` beside an
        # encoded space, each find the entry they spell.
        for written in ("taxes\\(2026\\).md", "taxes(2026).md", "taxes%282026%29.md"):
            assert await memory.aliased_text(f"[T]({written})") == f"[T]({taxes})"
        assert await memory.aliased_text("[P](tax%20plan%20notes.md)") == f"[P]({plan})"
        # Escaped in prose too, as Markdown renders it the same name.
        assert await memory.aliased_text("See taxes\\(2026\\).md now.") == f"See {taxes} now."
        # A removed entry's whole destination is hidden, parentheses and all.
        for gone in ("(private).md", "old\\(secret\\).md", "my (old) plan.md"):
            text = await memory.aliased_text(f"[Old]({gone}) after")
            assert text == "[Old]([a name in their memory]) after", gone

    @pytest.mark.asyncio
    async def test_an_html_character_reference_spells_the_name_too(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "plans/taxes&returns.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        alias = alias_of(KEY, MEMBER, "plans/taxes&returns.md")
        for written in ("plans&#47;taxes&amp;returns.md", "plans&#x2F;taxes&#38;returns.md", "plans&sol;taxes&amp;returns.md"):
            assert await memory.aliased_text(f"See {written} now.") == f"See {alias} now.", written
            assert await memory.aliased_text(f"[T]({written})") == f"[T]({alias})", written

    @pytest.mark.asyncio
    async def test_a_reference_for_two_characters_spells_the_name_too(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "reports/\u223e\u0333 fjord.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        alias = alias_of(KEY, MEMBER, "reports/\u223e\u0333 fjord.md")
        for written in ("reports/&acE; &fjlig;ord.md", "reports/\u223e\u0333 fjord.md"):
            assert await memory.aliased_text(f"See {written} now.") == f"See {alias} now.", written

    @pytest.mark.asyncio
    async def test_a_raw_html_link_or_image_is_read_as_a_link(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "kept.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        kept = alias_of(KEY, MEMBER, "kept.md")
        text = await memory.aliased_text(
            '<a href="old-secret.md">x</a> <img src=\'pics/secret.png\'> <a HREF="kept.md">k</a>'
            ' <a href="https://example.com/secret.md">out</a>'
        )
        assert text == (
            '<a href="[a name in their memory]">x</a> <img src=\'[a name in their memory]\'>'
            f' <a HREF="{kept}">k</a> <a href="https://example.com/secret.md">out</a>'
        )

    @pytest.mark.asyncio
    async def test_a_url_keeps_a_name_in_its_query(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "report.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        report = alias_of(KEY, MEMBER, "report.md")
        text = await memory.aliased_text(
            "[src](https://example.com/get?file=report.md#report.md) and https://x.io/?f=report.md, see report.md"
        )
        assert text == (
            f"[src](https://example.com/get?file=report.md#report.md) and https://x.io/?f=report.md, see {report}"
        )

    @pytest.mark.asyncio
    async def test_a_read_its_aliases_lengthen_is_cut_again(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "a.md", {"content": "x"})
        body = "\n".join("a.md " * 15 for _ in range(500))  # ~38k raw, ~5x once aliased
        await store.aput(user_memory_namespace(ANA), "big.md", {"content": body})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT)
        path = f"{FOLDER}{alias_of(KEY, MEMBER, 'big.md')}"
        args = {"file_path": path}
        read = await mount.aliased("Read", args, await mount.read.ainvoke(args))
        assert len(read) <= MEMORY_READ_CHARS and not re.search(r"(?<![\w.-])a\.md", read)
        shown = [line for line in read.split("\n") if line.strip() and line.strip()[0].isdigit()]
        last = int(shown[-1].split("\t")[0])
        assert 1 < last < 500
        assert read.endswith(
            f"You saw lines 1..{last}. Call Read(file_path='{path}', offset={last}, limit=2000) "
            "to continue, or pass a smaller limit to keep each chunk shorter.]"
        )

    @pytest.mark.asyncio
    async def test_a_long_listing_is_cut_to_what_a_call_can_carry(self):
        store = InMemoryStore()
        for n in range(1000):
            await store.aput(user_memory_namespace(ANA), f"d{n}/e{n}.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT)
        args = {"pattern": "**/*.md", "path": FOLDER}
        listed = await mount.aliased("Glob", args, await mount.glob.ainvoke(args))
        assert len(listed) <= MEMORY_READ_CHARS
        assert listed.startswith("Found 1000 file(s)")
        shown = listed.split("\n")[1:-2]
        assert len(shown) > 100 and all(line.startswith(FOLDER) for line in shown)
        assert f"[showing first {len(shown)} of 1000 matches" in listed
        assert listed.endswith("of 1000 matches \u2014 narrow the pattern or pass a subdirectory path to see the rest]")

    @pytest.mark.asyncio
    async def test_a_link_resolves_against_the_file_it_is_in(self):
        store = InMemoryStore()
        for key in ("notes.md", "work/notes.md", "work/index.md"):
            await store.aput(user_memory_namespace(ANA), key, {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        root, (work, sibling) = alias_of(KEY, MEMBER, "notes.md"), alias_of(KEY, MEMBER, "work/notes.md").split("/")
        assert await memory.aliased_text("[N](notes.md)", source="work/index.md") == f"[N]({sibling})"
        assert await memory.aliased_text("[N](../notes.md)", source="work/index.md") == f"[N](../{root})"
        assert await memory.aliased_text("[N](notes.md)", source="memory.md") == f"[N]({root})"
        assert await memory.aliased_text("[W](work)", source="memory.md") == f"[W]({work})"

    @pytest.mark.asyncio
    async def test_a_link_through_a_dot_dot_inside_it_resolves(self):
        store = InMemoryStore()
        for key in ("notes.md", "work/notes.md", "work/index.md"):
            await store.aput(user_memory_namespace(ANA), key, {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        root, sibling = alias_of(KEY, MEMBER, "notes.md"), alias_of(KEY, MEMBER, "work/notes.md").split("/")[1]
        src = "work/index.md"
        assert await memory.aliased_text("[N](./reports/../notes.md)", source=src) == f"[N](./{sibling})"
        assert await memory.aliased_text("[N](reports/../../notes.md)", source=src) == f"[N](../{root})"
        assert await memory.aliased_text("[N](../../../x/secret.md)", source=src) == "[N]([a name in their memory])"

    @pytest.mark.asyncio
    async def test_a_reference_definition_in_a_block_quote_is_hidden_too(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "kept.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        kept = alias_of(KEY, MEMBER, "kept.md")
        text = await memory.aliased_text(
            "> [old]: deleted-secret.md\n>> [k]: kept.md\n     3\t> [n]:\n     4\t> gone-secret.md\n"
            "> [site]: https://example.com/a.md\n"
        )
        assert "secret" not in text
        assert f">> [k]: {kept}" in text and "[site]: https://example.com/a.md" in text

    @pytest.mark.asyncio
    async def test_a_read_removed_since_still_resolves_its_links_beside_it(self):
        store = InMemoryStore()
        for key in ("notes.md", "work/notes.md", "work/index.md"):
            await store.aput(user_memory_namespace(ANA), key, {"content": "[N](notes.md)"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT)
        index = f"{FOLDER}{alias_of(KEY, MEMBER, 'work/index.md')}"
        text = await mount.memory.aread_text(f"{ROOT}/{index}")
        await store.adelete(user_memory_namespace(ANA), "work/index.md")
        await mount.memory.keys(fresh=True)  # another read lists the memory without it
        sibling = alias_of(KEY, MEMBER, "work/notes.md").split("/")[1]
        assert await mount.aliased("Read", {"file_path": index}, text) == f"[N]({sibling})"

    @pytest.mark.asyncio
    async def test_a_label_with_an_escaped_bracket_and_a_network_path(self):
        store = InMemoryStore()
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        assert await memory.aliased_text("[old\\]]: secret-plan.md") == "[old\\]]: [a name in their memory]"
        kept = "[source](//example.com/report.md)"
        assert await memory.aliased_text(kept) == kept

    @pytest.mark.asyncio
    async def test_a_large_memory_file_is_read_a_window_at_a_time(self):
        from ptc_agent.agent.middleware.member_context.files import MEMORY_READ_CHARS
        from ptc_agent.agent.middleware.member_context.middleware import FILL_BUDGET_CHARS

        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "big.md", {"content": "\n".join(f"line {i} " + "x" * 60 for i in range(1_500))})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT)
        text = await mount.read.coroutine(file_path=f"{FOLDER}{alias_of(KEY, MEMBER, 'big.md')}")
        # Read's own clip, at the budget a call fills back in.
        assert MEMORY_READ_CHARS == FILL_BUDGET_CHARS and len(text) <= MEMORY_READ_CHARS
        assert "[Read truncated" in text
        # Elsewhere Read keeps its own cap.
        assert files.read_cap(f"{ROOT}/report.md") is None

    @pytest.mark.asyncio
    async def test_a_memory_md_beside_the_source_is_not_the_index(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "work/memory.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        nested = alias_of(KEY, MEMBER, "work/memory.md").split("/")[1]
        assert await memory.aliased_text("[M](memory.md)", source="work/index.md") == f"[M]({nested})"
        assert await memory.aliased_text("[I](memory.md)", source="memory.md") == "[I](memory.md)"
        assert await memory.aliased_text("[I](../memory.md)", source="work/index.md") == "[I](../memory.md)"

    @pytest.mark.asyncio
    async def test_a_reference_whose_destination_is_on_the_next_line_is_hidden(self):
        store = InMemoryStore()
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        assert await memory.aliased_text("[old]:\n  deleted-note.md\n") == "[old]:\n  [a name in their memory]\n"
        assert await memory.aliased_text("     1\t[old]:\n     2\t  deleted-note.md") == (
            "     1\t[old]:\n     2\t  [a name in their memory]"
        )
        assert await memory.aliased_text("[site]:\n  https://example.com/a.md\n") == "[site]:\n  https://example.com/a.md\n"

    @pytest.mark.asyncio
    async def test_a_reference_in_reads_numbered_output_is_hidden_too(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "kept.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        text = await memory.aliased_text("     1\t# Index\n     2\t[old]: ./gone/secret plan.md \"t\"\n")
        assert text == '     1\t# Index\n     2\t[old]: [a name in their memory] "t"\n'

    @pytest.mark.asyncio
    async def test_an_entry_added_mid_turn_is_sent_its_alias(self):
        store = InMemoryStore()
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        await memory.keys()  # the turn has listed the memory once already
        await store.aput(user_memory_namespace(ANA), "new secret.md", {"content": "x"})
        assert await memory.aliased_text("see new secret.md") == f"see {alias_of(KEY, MEMBER, 'new secret.md')}"

    @pytest.mark.asyncio
    async def test_an_entry_removed_after_the_read_keeps_its_alias(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "memory.md", {"content": "- [Gone](gone secret.md)"})
        await store.aput(user_memory_namespace(ANA), "gone secret.md", {"content": "x"})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        memory = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT).memory
        text = await memory.aread_text(f"{ROOT}/{FOLDER}memory.md")
        await store.adelete(user_memory_namespace(ANA), "gone secret.md")  # between read and listing
        gone = alias_of(KEY, MEMBER, "gone secret.md")
        # A read beside it lists the memory again, without the entry, first.
        assert await memory.aread_text(f"{ROOT}/{FOLDER}{gone}") is None
        assert await memory.aliased_text(text) == f"- [Gone]({gone})"

    @pytest.mark.asyncio
    async def test_an_image_or_pdf_entry_is_read_as_the_text_it_is(self):
        store = InMemoryStore()
        await store.aput(user_memory_namespace(ANA), "scan.PDF", {"content": SECRET})
        sandbox = _sandbox()
        files = CompositeFilesystemBackend(sandbox=sandbox, routes=[])
        mount = member_memory(MEMBER, store=store, files=files, sandbox=sandbox, layout=LAYOUT)
        alias = alias_of(KEY, MEMBER, "scan.PDF")
        assert "." not in alias and "." not in alias_of(KEY, MEMBER, "chart.png")
        assert SECRET in await mount.read.ainvoke({"file_path": f"{FOLDER}{alias}"})


class _Recording(BaseChatModel):
    script: list[AIMessage] = Field(default_factory=list)
    requests: list[list[AnyMessage]] = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "recording"

    def bind_tools(self, tools: Any, **kwargs: Any) -> _Recording:
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.requests.append(list(messages))
        return ChatResult(generations=[ChatGeneration(message=self.script.pop(0))])


def _turn(text: str, user_id: str = ANA, name: str = "Ana", *, mid: str) -> dict:
    return {"messages": [{"role": "user", "content": text, "id": mid, MEMBER_SENDER_KEY: {"user_id": user_id, "name": name}}]}


class TestThroughAGraph:
    """Two members' turns through a real graph build with the Read tool."""

    @pytest.mark.asyncio
    async def test_nothing_of_the_file_is_stored_streamed_or_summarized(self):
        notes = f"{FOLDER}{alias_of(KEY, MEMBER, NOTES)}"
        model = _Recording(script=[
            AIMessage("", tool_calls=[{"id": "c0", "name": "Glob", "args": {"pattern": "**/*", "path": FOLDER}}]),
            AIMessage("", tool_calls=[{"id": "c1", "name": "Read", "args": {"file_path": FILE}}]),
            AIMessage("", tool_calls=[{"id": "c2", "name": "Read", "args": {"file_path": notes}}]),
            AIMessage("Found it."),
            AIMessage("I can't see that."),
        ])
        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "t-1"}}
        events: list[Any] = []

        ana, tools, _ = await _files()
        graph = create_agent(
            model, tools=[tools["Read"], tools["Glob"]], middleware=[SenderNamingMiddleware(), ana, RowCarrierMiddleware()], checkpointer=saver
        )
        async for event in graph.astream(_turn("what is my code word?", mid="m1"), config, stream_mode=["messages", "updates"]):
            events.append(event)
        # Bo's turn, built as the server builds it for him: no member context.
        _, bo_tools, _ = await _files()
        bo = create_agent(
            model, tools=[bo_tools["Read"], bo_tools["Glob"]], middleware=[SenderNamingMiddleware(), RowCarrierMiddleware()], checkpointer=saver
        )
        async for event in bo.astream(_turn("what is Ana's code word?", BO, "Bo", mid="m2"), config, stream_mode=["messages", "updates"]):
            events.append(event)

        sent = [repr([m.content for m in request]) for request in model.requests]
        assert "PERIWINKLE" not in sent[0]
        assert "PERIWINKLE" in sent[2] and "PERIWINKLE" in sent[3]  # Ana's own calls after her read
        assert ALIAS in sent[1]  # her listing, by alias
        assert "PERIWINKLE" not in sent[4] and STUB in sent[4]  # Bo's call
        # No call is sent a file's real name, Ana's included.
        for real in ("codeword", "plans", "launch.md"):
            assert all(real not in each for each in sent)

        messages = (await graph.aget_state(config)).values["messages"]
        stored = {
            "events": repr(events),
            "checkpoint": repr([m.model_dump() for m in messages]),
            "summarizer input": repr([m.model_dump() for m in _summarizable(messages)]),
            "transcript": repr(render_segment(1, messages).lines),
        }
        for where, text in stored.items():
            for real in ("PERIWINKLE", "codeword", "plans", "launch.md"):
                assert real not in text, f"{real} in the {where}"
        assert ALIAS in stored["checkpoint"]  # what the stubs keep is the alias


@pytest.mark.asyncio
async def test_the_index_deadline_covers_the_listing(monkeypatch):
    import asyncio

    from ptc_agent.agent.middleware.member_context import files as files_module

    middleware, _, _ = await _files()
    memory = middleware._mount.memory

    async def slow(**_):
        await asyncio.sleep(5)

    monkeypatch.setattr(memory, "_all_items", slow)
    monkeypatch.setattr(files_module, "INDEX_READ_TIMEOUT_S", 0.05)
    assert await asyncio.wait_for(middleware._mount.index(), timeout=1) is None


@pytest.mark.asyncio
async def test_a_listing_cut_short_leaves_the_memory_out(monkeypatch):
    from ptc_agent.agent.backends.langgraph_store import StoreListingIncomplete

    middleware, _, _ = await _files()
    memory = middleware._mount.memory
    listing = AsyncMock(side_effect=StoreListingIncomplete("timed out"))
    monkeypatch.setattr(memory, "_all_items", listing)
    # A partial map would leave the missed entries' real names in the index.
    assert await middleware._mount.index() is None
    assert listing.await_args.kwargs == {"strict": True}


@pytest.mark.asyncio
async def test_the_block_names_the_folder_where_it_is_mounted():
    middleware, _, _ = await _files()
    block = await middleware.block()
    assert (
        "## Ana's memory\n\nRead-only here. Read the files the index points to with Read at "
        "`.agents/members/1a2b3c/memory/`; Bash, ExecuteCode and Grep cannot reach that folder, "
        "and a change to it is Ana's to make from their own workspace.\n\n"
        f'<memory path=".agents/members/1a2b3c/memory/memory.md">\n- [Code word]({ALIAS})\n</memory>'
    ) in block


class TestASummaryIsWrittenFromTheStubs:
    """Compaction sends its summary through the agent's own handler, so the
    member's middleware sees that call too, and the thread keeps the summary
    for every member's later calls. The provider's own compaction request is
    left as it is; the summaries asked in a trailing harness message get
    nothing, since that message, which no member wrote, ends the streak."""

    @staticmethod
    async def _summary_request(summary, history_head: list[Any] | None = None) -> list[Any]:
        from langchain.agents.middleware.types import ModelRequest, ModelResponse

        middleware, tools, _ = await _files()
        stub = await _call(middleware, tools, "Read", {"file_path": FILE}, "c1")
        history = [*(history_head or []), _says("what is my code word?", mid="m1"), *_read_round("c1", stub)]
        sent: list[Any] = []

        async def model(request):
            sent.extend(request.messages)
            return ModelResponse(result=[AIMessage("the summary")])

        async def handler(request):
            return await middleware.awrap_model_call(request, model)

        request = ModelRequest(
            model=SimpleNamespace(), messages=history, system_message=None, tools=[], state={}, runtime=None,
        )
        await summary(request, handler, history)
        return sent

    @staticmethod
    def _holds_none_of_it(sent: list[Any]) -> None:
        assert sent and "PERIWINKLE" not in repr(sent) and "member_context" not in repr(sent)
        assert STUB in repr(sent) or not any(isinstance(m, ToolMessage) for m in sent)

    @pytest.mark.asyncio
    async def test_the_providers_summary_holds_none_of_their_memory(self):
        from ptc_agent.agent.middleware.compaction import CompactionMiddleware

        async def summary(request, handler, history):
            with pytest.raises(ValueError):  # the stand-in model writes no compaction
                await CompactionMiddleware._provider_summary(request, handler, history, True, notes=False)

        sent = await self._summary_request(summary)
        self._holds_none_of_it(sent)
        assert STUB in repr(sent)

    @pytest.mark.asyncio
    async def test_the_agents_own_summary_holds_none_of_their_memory(self):
        from ptc_agent.agent.middleware.compaction import CompactionMiddleware

        async def summary(request, handler, history):
            with pytest.raises(ValueError):  # the stand-in model's summary has no closing line
                await CompactionMiddleware._self_summary(request, handler, history, True, notes=False)

        sent = await self._summary_request(summary)
        self._holds_none_of_it(sent)
        assert STUB in repr(sent)

    @pytest.mark.asyncio
    async def test_a_hand_off_summary_holds_none_of_their_memory(self, monkeypatch):
        from ptc_agent.agent.middleware.compaction import CompactionMiddleware
        from ptc_agent.agent.middleware.compaction import middleware as compaction

        # A summary only the provider can read, written out as text.
        monkeypatch.setattr(compaction, "provider_only", lambda message: True)
        head = HumanMessage("the summary so far", additional_kwargs={"lc_source": "summarization"})

        async def summary(request, handler, history):
            with pytest.raises(ValueError):  # the stand-in head takes no summary text
                await CompactionMiddleware._hand_off(request, handler, history, notes=False)

        sent = await self._summary_request(summary, [head])
        self._holds_none_of_it(sent)
        assert sent[0] is head

    @pytest.mark.asyncio
    async def test_their_own_calls_still_get_it(self):
        async def summary(request, handler, history):
            await handler(request)

        assert "PERIWINKLE" in repr(await self._summary_request(summary))
