"""Past threads served to the file tools of a computer the mount never serves.

Grep runs here over the stored rows, so the entries it returns match what rg
reports: a path and, in content mode, the line with its number, a long line cut
to windows around its matches. The routes are read-only, and a folder staged
mid-move is not one of the computer's folders.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.backends import ReadOnlyStoreError
from ptc_agent.agent.backends.langgraph_store import UnsupportedPattern, compile_grep, grep_texts
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.grep_render import GREP_LINE_CHARS, GrepLine
from src.server.database import thread_transcripts as db
from src.server.services.livefs import history_files
from src.server.services.livefs.history_files import (
    ComputerHistory,
    IndexFile,
    TranscriptFiles,
    _ReadOnlyFiles,
    computer_history,
)

A = "/r/Home/.agents/transcripts/abcd1234/turn-0001.jsonl"
B = "/r/Home/.agents/transcripts/abcd1234/turn-0002.jsonl"
FILES = [(A, "alpha\nneedle one\nneedle two\n"), (B, "beta\ngamma\n")]
NEEDLE = compile_grep("needle", False)


def _search(files, compiled, output_mode, show_line_numbers):
    """The search the history routes run."""
    return grep_texts(
        files, compiled, output_mode, show_line_numbers=show_line_numbers, cut_long_lines=True
    )


class TestSearch:
    def test_files_with_matches_lists_each_matching_file_once(self):
        assert _search(FILES, NEEDLE, "files_with_matches", True) == [A]

    def test_count_gives_the_matching_lines_of_each_file(self):
        assert _search(FILES, NEEDLE, "count", True) == [(A, 2)]

    def test_content_lines_carry_path_and_number(self):
        found = _search(FILES, NEEDLE, "content", True)

        assert found == [f"{A}:2:needle one", f"{A}:3:needle two"]
        assert all(isinstance(entry, GrepLine) and entry.path == A for entry in found)

    def test_content_without_numbers_drops_them(self):
        found = _search(FILES, NEEDLE, "content", False)

        assert found == [f"{A}:needle one", f"{A}:needle two"]

    def test_a_long_line_comes_back_as_windows_around_the_match(self):
        line = "p" * 2_000 + "needle" + "q" * 2_000
        found = _search([(A, line)], NEEDLE, "content", True)

        assert len(found) == 1
        assert "needle" in found[0]
        assert len(found[0]) < GREP_LINE_CHARS

    @pytest.mark.parametrize("output_mode", ["files_with_matches", "content"])
    def test_a_pattern_that_backtracks_finishes_over_a_long_line(self, output_mode):
        # `re` takes seconds here, holding the GIL and so the event loop.
        line = "a" * 256 * 1024

        started = time.perf_counter()
        found = _search([(A, line)], compile_grep(".*x", False), output_mode, True)

        assert found == []
        assert time.perf_counter() - started < 0.5


class TestCompiled:
    def test_a_pattern_re2_refuses_is_refused_with_its_reason(self):
        with pytest.raises(UnsupportedPattern) as refused:
            compile_grep("(?x) a b", False)

        assert refused.value.reason == "invalid perl operator: (?x"

    def test_case_insensitive_matches_either_case(self):
        assert compile_grep("needle", True).search("NEEDLE")
        assert not compile_grep("needle", False).search("NEEDLE")


class TestReadOnlyRoutes:
    layout = SandboxLayout.for_root("/home/workspace").for_workspace("Home")

    @pytest.fixture(
        params=["transcripts", "index"],
    )
    def route(self, request):
        if request.param == "transcripts":
            return TranscriptFiles("ws-1", self.layout)
        return IndexFile("computer-1", "/home/workspace")

    @pytest.mark.asyncio
    async def test_a_write_is_refused(self, route):
        with pytest.raises(ReadOnlyStoreError):
            await route.awrite_text(route.root_prefix + "x", "text")

    @pytest.mark.asyncio
    async def test_a_pattern_re2_refuses_is_refused_before_any_read(self, route):
        # The composite answers it with a note: no matches would read as
        # nothing there.
        with pytest.raises(UnsupportedPattern):
            await route.agrep_rich("(?x) a b", route.root_prefix, "content")

    @pytest.mark.asyncio
    async def test_an_edit_is_refused(self, route):
        with pytest.raises(ReadOnlyStoreError):
            await route.aedit_text(route.root_prefix + "x", "a", "b")

    def test_the_route_is_marked_read_only_and_searched_only_inside(self, route):
        assert route.refuses_writes is True
        assert route.searched_only_inside is True

    @pytest.mark.asyncio
    async def test_aread_range_slices_lines(self):
        class Fixed(_ReadOnlyFiles):
            async def aread_text(self, file_path):
                return "a\nb\nc\nd\n"

        assert await Fixed().aread_range("p", 1, 2) == "b\nc\n"
        assert await Fixed().aread_range("p", 3, 10) == "d\n"

    @pytest.mark.asyncio
    async def test_aread_range_of_a_missing_file_is_none(self):
        class Missing(_ReadOnlyFiles):
            async def aread_text(self, file_path):
                return None

        assert await Missing().aread_range("p") is None


class TestTranscriptGrep:
    layout = SandboxLayout.for_root("/home/workspace").for_workspace("Home")

    @pytest.mark.asyncio
    async def test_a_grep_matches_the_stored_rows(self, monkeypatch):
        files = TranscriptFiles("ws-1", self.layout)
        listed = [
            db.ListedFile("t-1", "abcd1234", "turn-0001.jsonl", "d1", 20, "u-1"),
            db.ListedFile("t-1", "abcd1234", "turn-0002.jsonl", "d2", 10, "u-1"),
        ]
        monkeypatch.setattr(db, "list_transcript_tree", AsyncMock(return_value=listed))
        monkeypatch.setattr(
            db,
            "load_transcript_contents",
            AsyncMock(return_value={listed[0]: b"hay\nneedle\n", listed[1]: b"hay\n"}),
        )

        found = await files.agrep_rich("needle", files.root_prefix, "count")

        assert found == [(f"{files.root_prefix}abcd1234/turn-0001.jsonl", 1)]

    @pytest.mark.asyncio
    async def test_a_content_search_stops_fetching_at_its_line_cap(self, monkeypatch):
        files = TranscriptFiles("ws-1", self.layout)
        listed = [
            db.ListedFile("t-1", "abcd1234", f"turn-000{n}.jsonl", f"d{n}", 20, "u-1")
            for n in (1, 2, 3)
        ]
        contents = AsyncMock(side_effect=lambda batch: {f: b"needle\nneedle\n" for f in batch})
        monkeypatch.setattr(db, "list_transcript_tree", AsyncMock(return_value=listed))
        monkeypatch.setattr(db, "load_transcript_contents", contents)
        # A batch per file, and a cap the second one reaches.
        monkeypatch.setattr(history_files, "_BATCH_BYTES", 20)
        monkeypatch.setattr(history_files, "_MAX_LINES", 3)

        found = await files.agrep_rich("needle", files.root_prefix, "content")

        assert found == [
            f"{files.root_prefix}abcd1234/turn-0001.jsonl:1:needle",
            f"{files.root_prefix}abcd1234/turn-0001.jsonl:2:needle",
            f"{files.root_prefix}abcd1234/turn-0002.jsonl:1:needle",
        ]
        assert contents.await_count == 2

    @pytest.fixture
    def three_threads(self, monkeypatch):
        """Three threads newest first, a match in each, and a batch per file."""
        listed = [
            db.ListedFile(f"t-{n}", f"abcd000{n}", "turn-0001.jsonl", f"d{n}", 20, "u-1")
            for n in (1, 2, 3)
        ]
        tree = AsyncMock(return_value=listed)
        contents = AsyncMock(side_effect=lambda batch: {f: b"needle\n" for f in batch})
        monkeypatch.setattr(db, "list_transcript_tree", tree)
        monkeypatch.setattr(db, "load_transcript_contents", contents)
        monkeypatch.setattr(history_files, "_BATCH_BYTES", 20)
        return tree, contents

    @pytest.mark.asyncio
    async def test_a_search_stops_at_its_byte_limit_and_says_so(self, three_threads, monkeypatch):
        tree, contents = three_threads
        monkeypatch.setattr(history_files, "_SEARCH_BYTES", 40)
        files = TranscriptFiles("ws-1", self.layout)

        found = await files.agrep_rich("needle", files.root_prefix)

        assert found == [f"{files.root_prefix}abcd000{n}/turn-0001.jsonl" for n in (1, 2)]
        assert contents.await_count == 2
        assert tree.await_args.kwargs["newest_first"] is True
        assert found.notes and "searched 2 of 3 transcript files" in found.notes[0]

    @pytest.mark.asyncio
    async def test_a_head_limit_stops_fetching_once_it_is_met(self, three_threads):
        _, contents = three_threads
        files = TranscriptFiles("ws-1", self.layout)

        found = await files.agrep_rich("needle", files.root_prefix, head_limit=1)

        assert found == [f"{files.root_prefix}abcd0001/turn-0001.jsonl"]
        assert contents.await_count == 1
        assert not getattr(found, "notes", ())

    @pytest.mark.asyncio
    async def test_a_file_the_walk_skips_is_never_fetched(self, three_threads):
        _, contents = three_threads
        files = TranscriptFiles("ws-1", self.layout)

        found = await files.agrep_rich(
            "needle", files.root_prefix, searches=lambda path: "abcd0002" in path
        )

        assert found == [f"{files.root_prefix}abcd0002/turn-0001.jsonl"]
        assert contents.await_count == 1


class TestComputerHistory:
    @pytest.mark.asyncio
    async def test_a_folder_with_no_name_or_staged_mid_move_is_dropped(self, monkeypatch):
        from src.server.database import workspace

        rows = [
            {"workspace_id": "w1", "dir_name": "Home"},
            {"workspace_id": "w2", "dir_name": ""},
            {"workspace_id": "w3", "dir_name": None},
            {"workspace_id": "w4", "dir_name": "_internal/Staged"},
        ]
        monkeypatch.setattr(
            workspace, "get_live_workspace_folders_for_computer", AsyncMock(return_value=rows)
        )

        history = await computer_history("c-1")

        assert isinstance(history, ComputerHistory)
        assert history._folders == (("w1", "Home"),)

    def test_a_route_per_folder_and_the_index(self):
        history = ComputerHistory("c-1", [("w1", "Home"), ("w2", "Research")])
        sandbox = type("S", (), {"computer_root": "/home/workspace/"})()

        routes = history.routes(sandbox)

        assert [type(r) for r in routes] == [TranscriptFiles, TranscriptFiles, IndexFile]
        assert routes[0].root_prefix != routes[1].root_prefix

    @pytest.mark.asyncio
    async def test_a_save_goes_through_the_transcript_store(self, monkeypatch):
        save = AsyncMock(return_value=True)
        monkeypatch.setattr(history_files.transcripts, "save_live", save)

        saved = await ComputerHistory("c-1", []).save_transcript("target", ["m"], window="w")

        assert saved is True
        save.assert_awaited_once_with("target", ["m"], window="w")
