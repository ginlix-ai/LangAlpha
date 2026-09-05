"""Behavioral tests for `SandboxBackend`.

Covers:
- Protocol methods (als, aread, awrite[overwrite=], aedit, agrep, aglob, aexecute).
- Rich adapter-extension methods (path helpers, agrep_rich, aexecute_bash, etc.).
- Sync-stub removal: assert direct overrides of every async method so the
  protocol base class's async-dispatches-to-sync default can never fire.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from deepagents.backends.protocol import (
    EditResult,
    ExecuteResponse,
    GlobResult,
    GrepResult,
    LsResult,
    ReadResult,
    WriteResult,
)

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.core.sandbox.runtime import SandboxTransientError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_sandbox(working_dir: str = "/home/workspace") -> MagicMock:
    """Create a mock PTCSandbox with realistic config and async method stubs."""
    sandbox = MagicMock()
    sandbox.config.filesystem.working_directory = working_dir
    sandbox.config.filesystem.enable_path_validation = True
    sandbox.sandbox_id = "sbx-abc123"
    sandbox.skills_manifest = {"skills": {}}
    # normalize_path: default pass-through for test simplicity
    sandbox.normalize_path.side_effect = lambda p: p if p.startswith("/") else f"{working_dir}/{p}"
    sandbox.virtualize_path.side_effect = lambda p: p.replace(working_dir, "") or "/"
    sandbox.validate_path.return_value = True
    # Async stubs — tests override return_value / side_effect as needed
    sandbox.als_directory = AsyncMock(return_value=[])
    sandbox.aread_file_range = AsyncMock(return_value=None)
    sandbox.aread_file_text = AsyncMock(return_value=None)
    sandbox.awrite_file_text = AsyncMock(return_value=True)
    sandbox.aedit_file_text = AsyncMock(return_value={"success": True, "occurrences": 1})
    sandbox.agrep_content = AsyncMock(return_value=[])
    sandbox.aglob_files = AsyncMock(return_value=[])
    sandbox.adownload_file_bytes = AsyncMock(return_value=None)
    sandbox.aupload_file_bytes = AsyncMock(return_value=True)
    sandbox.execute_bash_command = AsyncMock(
        return_value={"success": True, "stdout": "", "stderr": "", "exit_code": 0}
    )
    sandbox.execute = AsyncMock()
    sandbox.stop_background_command = AsyncMock(return_value=True)
    sandbox.get_background_command_status = AsyncMock(return_value={})
    sandbox.start_and_get_preview_url = AsyncMock()
    return sandbox


@pytest.fixture
def sandbox():
    return _make_sandbox()


@pytest.fixture
def backend(sandbox):
    return SandboxBackend(sandbox)


# ---------------------------------------------------------------------------
# Structural guards
# ---------------------------------------------------------------------------


class TestStructure:
    def test_id_property_falls_back_to_unknown(self, sandbox):
        sandbox.sandbox_id = None
        backend = SandboxBackend(sandbox)
        assert backend.id == "unknown"

    def test_id_property_returns_sandbox_id(self, backend):
        assert backend.id == "sbx-abc123"

    def test_every_async_method_is_directly_overridden(self):
        """If any async method were inherited from SandboxBackendProtocol, its
        default would dispatch to a sync method — which we don't implement.
        Guarantee all async methods are overridden on SandboxBackend itself.
        """
        required = [
            "als", "aread", "awrite", "aedit", "agrep", "aglob",
            "aexecute", "aupload_files", "adownload_files",
        ]
        for method in required:
            assert method in SandboxBackend.__dict__, (
                f"{method} must be overridden directly on SandboxBackend, "
                "not inherited from the protocol base"
            )


# ---------------------------------------------------------------------------
# Protocol: als / aread / awrite / aedit / agrep / aglob / aexecute
# ---------------------------------------------------------------------------


class TestAls:
    @pytest.mark.asyncio
    async def test_happy_path_returns_LsResult_with_entries(self, sandbox, backend):
        sandbox.als_directory.return_value = [
            {"path": "/home/workspace/a.txt", "is_dir": False},
            {"path": "/home/workspace/data", "is_dir": True},
        ]
        result = await backend.als("/home/workspace")
        assert isinstance(result, LsResult)
        assert result.error is None
        assert len(result.entries) == 2
        assert result.entries[0]["path"] == "/home/workspace/a.txt"
        assert result.entries[1]["is_dir"] is True

    @pytest.mark.asyncio
    async def test_empty_dir_returns_empty_entries_not_error(self, sandbox, backend):
        sandbox.als_directory.return_value = []
        result = await backend.als("/home/workspace/empty")
        assert result.error is None
        assert result.entries == []

    @pytest.mark.asyncio
    async def test_sandbox_exception_becomes_error(self, sandbox, backend):
        sandbox.als_directory.side_effect = RuntimeError("boom")
        result = await backend.als("/home/workspace")
        assert result.error is not None
        assert "boom" in result.error


class TestAread:
    @pytest.mark.asyncio
    async def test_happy_path_returns_ReadResult_file_data(self, sandbox, backend):
        sandbox.aread_file_range.return_value = "line1\nline2"
        result = await backend.aread("/home/workspace/file.txt", offset=0, limit=100)
        assert isinstance(result, ReadResult)
        assert result.error is None
        assert result.file_data["content"] == "line1\nline2"
        assert result.file_data["encoding"] == "utf-8"

    @pytest.mark.asyncio
    async def test_not_found_returns_file_not_found_error(self, sandbox, backend):
        sandbox.aread_file_range.return_value = None
        result = await backend.aread("/nope")
        assert result.error == "file_not_found"
        assert result.file_data is None

    @pytest.mark.asyncio
    async def test_offset_and_limit_forwarded_to_range_read(self, sandbox, backend):
        sandbox.aread_file_range.return_value = ""
        await backend.aread("/home/workspace/big.txt", offset=50, limit=10)
        sandbox.aread_file_range.assert_awaited_once()
        args = sandbox.aread_file_range.call_args.args
        # (normalized_path, offset, limit)
        assert args[1] == 50
        assert args[2] == 10


class TestAwrite:
    @pytest.mark.asyncio
    async def test_default_overwrite_false_fails_if_file_exists(self, sandbox, backend):
        # `set -C; > path` returns exit_code 1 when path already exists.
        sandbox.execute_bash_command.return_value = {
            "success": False, "stdout": "", "stderr": "", "exit_code": 1
        }
        result = await backend.awrite("/home/workspace/existing.txt", "new content")
        assert isinstance(result, WriteResult)
        assert result.error is not None
        assert "already exists" in result.error
        sandbox.awrite_file_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_default_overwrite_false_succeeds_when_new_file(self, sandbox, backend):
        # `set -C; > path` returns exit_code 0 when reservation succeeds.
        sandbox.execute_bash_command.return_value = {
            "success": True, "stdout": "", "stderr": "", "exit_code": 0
        }
        result = await backend.awrite("/home/workspace/new.txt", "hello")
        assert result.error is None
        assert result.path == "/home/workspace/new.txt"
        sandbox.awrite_file_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_overwrite_true_skips_existence_check(self, sandbox, backend):
        result = await backend.awrite(
            "/home/workspace/anything.txt", "content", overwrite=True
        )
        assert result.error is None
        # The atomic reservation (`set -C`) should NOT have fired.
        for call in sandbox.execute_bash_command.call_args_list:
            assert "set -C" not in call.kwargs.get("command", "")
        sandbox.awrite_file_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reservation_uses_shlex_quoting_for_path(self, sandbox, backend):
        """Shell-metacharacter paths must be quoted, not interpolated."""
        sandbox.execute_bash_command.return_value = {
            "success": True, "stdout": "", "stderr": "", "exit_code": 0
        }
        evil_path = "/home/workspace/a b; rm -rf /"
        await backend.awrite(evil_path, "content")
        cmd = sandbox.execute_bash_command.call_args.kwargs["command"]
        assert cmd.startswith("set -C; > ")
        # The quoted arg must wrap the whole path — no unescaped metacharacters.
        assert cmd == f"set -C; > '{evil_path}'"

    @pytest.mark.asyncio
    async def test_reservation_bash_exception_returns_error(self, sandbox, backend):
        sandbox.execute_bash_command.side_effect = RuntimeError("sandbox dead")
        result = await backend.awrite("/home/workspace/file.txt", "x")
        assert result.error is not None
        assert "Failed to reserve" in result.error
        sandbox.awrite_file_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_failure_returns_error(self, sandbox, backend):
        sandbox.awrite_file_text.return_value = False
        result = await backend.awrite("/home/workspace/file.txt", "x", overwrite=True)
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_operation_callback_fires_on_successful_write(self, sandbox):
        """awrite must invoke the operation callback with line_count and content."""
        cb = MagicMock()
        backend = SandboxBackend(sandbox, operation_callback=cb)
        await backend.awrite("/home/workspace/f.txt", "line1\nline2\nline3", overwrite=True)
        assert cb.call_count == 1
        payload = cb.call_args.args[0]
        assert payload["operation"] == "write_file"
        assert payload["line_count"] == 3
        assert payload["content"] == "line1\nline2\nline3"

    @pytest.mark.asyncio
    async def test_operation_callback_not_fired_on_failure(self, sandbox):
        """Callback must not fire when the underlying write fails."""
        sandbox.awrite_file_text.return_value = False
        cb = MagicMock()
        backend = SandboxBackend(sandbox, operation_callback=cb)
        await backend.awrite("/home/workspace/f.txt", "x", overwrite=True)
        cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_operation_callback_exception_is_swallowed(self, sandbox):
        """A raising callback must not break the write."""
        cb = MagicMock(side_effect=RuntimeError("boom"))
        backend = SandboxBackend(sandbox, operation_callback=cb)
        result = await backend.awrite("/home/workspace/f.txt", "x", overwrite=True)
        assert result.error is None


class TestAedit:
    @pytest.mark.asyncio
    async def test_success_returns_EditResult_with_occurrences(self, sandbox, backend):
        sandbox.aedit_file_text.return_value = {"success": True, "occurrences": 3}
        result = await backend.aedit("/f.py", "old", "new", replace_all=True)
        assert isinstance(result, EditResult)
        assert result.error is None
        assert result.occurrences == 3

    @pytest.mark.asyncio
    async def test_failure_returns_error(self, sandbox, backend):
        sandbox.aedit_file_text.return_value = {"success": False, "error": "string not found"}
        result = await backend.aedit("/f.py", "x", "y")
        assert result.error == "string not found"

    @pytest.mark.asyncio
    async def test_operation_callback_fires_with_post_edit_content(self, sandbox):
        """aedit must re-read the file post-edit and pass it to the callback."""
        sandbox.aedit_file_text.return_value = {"success": True, "occurrences": 2}
        sandbox.aread_file_text.return_value = "post-edit content"
        cb = MagicMock()
        backend = SandboxBackend(sandbox, operation_callback=cb)
        await backend.aedit("/f.py", "old", "new", replace_all=True)
        assert cb.call_count == 1
        payload = cb.call_args.args[0]
        assert payload["operation"] == "edit_file"
        assert payload["occurrences"] == 2
        assert payload["replace_all"] is True
        assert payload["old_string"] == "old"
        assert payload["new_string"] == "new"
        assert payload["content"] == "post-edit content"

    @pytest.mark.asyncio
    async def test_operation_callback_not_fired_on_failed_edit(self, sandbox):
        sandbox.aedit_file_text.return_value = {"success": False, "error": "nope"}
        cb = MagicMock()
        backend = SandboxBackend(sandbox, operation_callback=cb)
        await backend.aedit("/f.py", "x", "y")
        cb.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failed_readback_does_not_fail_a_committed_edit(self, sandbox):
        """The read now raises where it used to return None.

        The edit is already committed by the time the callback re-reads, so
        letting a sandbox that died in between propagate would fail the whole
        tool call over a snapshot the callback can do without.
        """
        sandbox.aedit_file_text.return_value = {"success": True, "occurrences": 2}
        sandbox.aread_file_text.side_effect = SandboxTransientError("sandbox went away")
        cb = MagicMock()
        backend = SandboxBackend(sandbox, operation_callback=cb)

        result = await backend.aedit("/f.py", "old", "new")

        assert result.error is None
        assert result.occurrences == 2
        assert cb.call_args.args[0]["content"] is None


class TestAdownloadAupload:
    """Protocol-surface batch file transfer methods."""

    @pytest.mark.asyncio
    async def test_adownload_files_empty_list(self, backend):
        result = await backend.adownload_files([])
        assert result == []

    @pytest.mark.asyncio
    async def test_adownload_not_found_maps_to_file_not_found(self, sandbox, backend):
        sandbox.adownload_file_bytes.return_value = None
        [r] = await backend.adownload_files(["/missing.txt"])
        assert r.error == "file_not_found"

    @pytest.mark.asyncio
    async def test_adownload_exception_is_not_reported_as_not_found(
        self, sandbox, backend
    ):
        """A transport failure must not read as "this file does not exist"."""
        sandbox.adownload_file_bytes.side_effect = RuntimeError("net")
        [r] = await backend.adownload_files(["/x.txt"])
        assert r.error is not None
        assert r.error != "file_not_found"
        assert "net" in r.error

    @pytest.mark.asyncio
    async def test_adownload_mixed_batch_preserves_order(self, sandbox, backend):
        sandbox.adownload_file_bytes.side_effect = [b"hello", None, b"bye"]
        results = await backend.adownload_files(["/a", "/b", "/c"])
        assert len(results) == 3
        assert results[0].content == b"hello"
        assert results[1].error == "file_not_found"
        assert results[2].content == b"bye"

    @pytest.mark.asyncio
    async def test_aupload_files_empty_list(self, backend):
        result = await backend.aupload_files([])
        assert result == []

    @pytest.mark.asyncio
    async def test_aupload_failure_maps_to_permission_denied(self, sandbox, backend):
        sandbox.aupload_file_bytes.return_value = False
        [r] = await backend.aupload_files([("/x.txt", b"x")])
        assert r.error == "permission_denied"

    @pytest.mark.asyncio
    async def test_aupload_success_returns_path_record(self, sandbox, backend):
        sandbox.aupload_file_bytes.return_value = True
        [r] = await backend.aupload_files([("/x.txt", b"x")])
        assert r.error is None
        # Response preserves the caller-provided path, not the normalized one.
        assert r.path == "/x.txt"

    @pytest.mark.asyncio
    async def test_aupload_mixed_batch(self, sandbox, backend):
        sandbox.aupload_file_bytes.side_effect = [True, False, True]
        results = await backend.aupload_files([
            ("/a", b"a"), ("/b", b"b"), ("/c", b"c")
        ])
        assert results[0].error is None
        assert results[1].error == "permission_denied"
        assert results[2].error is None


class TestAgrep:
    @pytest.mark.asyncio
    async def test_matches_parsed_into_GrepResult(self, sandbox, backend):
        sandbox.agrep_content.return_value = [
            {"path": "/home/workspace/a.py", "line": 5, "text": "hit"},
        ]
        result = await backend.agrep("pattern", path="/home/workspace")
        assert isinstance(result, GrepResult)
        assert result.error is None
        assert len(result.matches) == 1
        assert result.matches[0]["path"] == "/home/workspace/a.py"
        assert result.matches[0]["line"] == 5

    @pytest.mark.asyncio
    async def test_empty_matches_returns_empty_list(self, sandbox, backend):
        sandbox.agrep_content.return_value = []
        result = await backend.agrep("nope")
        assert result.error is None
        assert result.matches == []

    @pytest.mark.asyncio
    async def test_exception_returns_error(self, sandbox, backend):
        sandbox.agrep_content.side_effect = OSError("disk")
        result = await backend.agrep("x")
        assert result.error is not None
        assert "disk" in result.error


class TestAglob:
    @pytest.mark.asyncio
    async def test_paths_wrapped_in_GlobResult(self, sandbox, backend):
        sandbox.aglob_files.return_value = ["/home/workspace/a.py", "/home/workspace/b.py"]
        result = await backend.aglob("*.py", "/home/workspace")
        assert isinstance(result, GlobResult)
        assert len(result.matches) == 2
        assert result.matches[0]["path"] == "/home/workspace/a.py"

    @pytest.mark.asyncio
    async def test_exception_returns_error(self, sandbox, backend):
        sandbox.aglob_files.side_effect = RuntimeError("rg gone")
        result = await backend.aglob("*.py")
        assert result.error is not None


class TestAexecute:
    @pytest.mark.asyncio
    async def test_timeout_none_omits_kwarg(self, sandbox, backend):
        """When timeout=None, we MUST NOT forward it — PTCSandbox expects int."""
        await backend.aexecute("ls")
        call = sandbox.execute_bash_command.call_args
        assert "timeout" not in call.kwargs

    @pytest.mark.asyncio
    async def test_timeout_int_forwarded(self, sandbox, backend):
        await backend.aexecute("ls", timeout=30)
        call = sandbox.execute_bash_command.call_args
        assert call.kwargs["timeout"] == 30

    @pytest.mark.asyncio
    async def test_exception_returns_error_exit_code(self, sandbox, backend):
        sandbox.execute_bash_command.side_effect = RuntimeError("sandbox down")
        result = await backend.aexecute("ls")
        assert isinstance(result, ExecuteResponse)
        assert result.exit_code == 1
        assert "sandbox down" in result.output


# ---------------------------------------------------------------------------
# Rich extension methods (passthrough verification)
# ---------------------------------------------------------------------------


class TestPathHelpers:
    def test_normalize_path_delegates(self, sandbox, backend):
        sandbox.normalize_path.return_value = "/home/workspace/x"
        assert backend.normalize_path("x") == "/home/workspace/x"
        sandbox.normalize_path.assert_called_with("x")

    def test_virtualize_path_delegates(self, sandbox, backend):
        sandbox.virtualize_path.return_value = "/x"
        assert backend.virtualize_path("/home/workspace/x") == "/x"

    def test_validate_path_delegates(self, sandbox, backend):
        sandbox.validate_path.return_value = False
        assert backend.validate_path("/etc") is False

    def test_filesystem_config_delegates(self, sandbox, backend):
        assert backend.filesystem_config is sandbox.config.filesystem

    def test_sandbox_id_returns_raw_attribute(self, sandbox, backend):
        assert backend.sandbox_id == "sbx-abc123"

    def test_skills_manifest_returns_raw(self, sandbox, backend):
        assert backend.skills_manifest == {"skills": {}}


class TestRichFileOps:
    @pytest.mark.asyncio
    async def test_aread_range_forwards_all_args(self, sandbox, backend):
        sandbox.aread_file_range.return_value = "content"
        result = await backend.aread_range("/f.txt", 5, 20)
        assert result == "content"
        args = sandbox.aread_file_range.call_args.args
        assert args[1] == 5
        assert args[2] == 20

    @pytest.mark.asyncio
    async def test_aread_text_delegates(self, sandbox, backend):
        sandbox.aread_file_text.return_value = "full content"
        result = await backend.aread_text("/f.txt")
        assert result == "full content"

    @pytest.mark.asyncio
    async def test_awrite_text_delegates(self, sandbox, backend):
        await backend.awrite_text("/f.txt", "data")
        sandbox.awrite_file_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aedit_text_forwards_replace_all(self, sandbox, backend):
        await backend.aedit_text("/f.py", "old", "new", replace_all=True)
        call = sandbox.aedit_file_text.call_args
        assert call.kwargs["replace_all"] is True


class TestAgrepRich:
    @pytest.mark.asyncio
    async def test_forwards_all_10_kwargs(self, sandbox, backend):
        await backend.agrep_rich(
            pattern="re",
            path="/src",
            output_mode="content",
            glob="*.py",
            type="py",
            case_insensitive=True,
            show_line_numbers=False,
            lines_after=2,
            lines_before=3,
            lines_context=4,
            multiline=True,
            head_limit=100,
            offset=5,
        )
        kwargs = sandbox.agrep_content.call_args.kwargs
        assert kwargs["pattern"] == "re"
        assert kwargs["output_mode"] == "content"
        assert kwargs["type"] == "py"
        assert kwargs["case_insensitive"] is True
        assert kwargs["show_line_numbers"] is False
        assert kwargs["lines_after"] == 2
        assert kwargs["lines_before"] == 3
        assert kwargs["lines_context"] == 4
        assert kwargs["multiline"] is True
        assert kwargs["head_limit"] == 100
        assert kwargs["offset"] == 5


class TestRichExec:
    @pytest.mark.asyncio
    async def test_aexecute_bash_forwards_options(self, sandbox, backend):
        await backend.aexecute_bash(
            "npm test",
            working_dir="/src",
            timeout=300,
            background=True,
            thread_id="abc12345",
        )
        kwargs = sandbox.execute_bash_command.call_args.kwargs
        assert kwargs["command"] == "npm test"
        assert kwargs["working_dir"] == "/src"
        assert kwargs["timeout"] == 300
        assert kwargs["background"] is True
        assert kwargs["thread_id"] == "abc12345"

    @pytest.mark.asyncio
    async def test_astop_background_command_delegates(self, sandbox, backend):
        sandbox.stop_background_command.return_value = True
        result = await backend.astop_background_command("bash_0001")
        assert result is True
        sandbox.stop_background_command.assert_awaited_with("bash_0001")

    @pytest.mark.asyncio
    async def test_aget_background_command_status_delegates(self, sandbox, backend):
        sandbox.get_background_command_status.return_value = {"is_running": True}
        result = await backend.aget_background_command_status("bash_0001")
        assert result == {"is_running": True}

    @pytest.mark.asyncio
    async def test_aexecute_code_forwards_thread_id(self, sandbox, backend):
        await backend.aexecute_code("print(1)", thread_id="th12345")
        sandbox.execute.assert_awaited_once()
        assert sandbox.execute.call_args.kwargs["thread_id"] == "th12345"


class TestMiscRich:
    @pytest.mark.asyncio
    async def test_adownload_file_bytes_delegates(self, sandbox, backend):
        sandbox.adownload_file_bytes.return_value = b"bytes"
        result = await backend.adownload_file_bytes("/f.png")
        assert result == b"bytes"

    @pytest.mark.asyncio
    async def test_astart_preview_url_delegates(self, sandbox, backend):
        sandbox.start_and_get_preview_url.return_value = MagicMock(url="https://x")
        result = await backend.astart_preview_url("node server.js", 3000)
        assert result.url == "https://x"
