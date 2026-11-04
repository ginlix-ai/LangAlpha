"""The agent's own tools refuse a call that names a protected path and warn on
output that shows one; a Claude Code setup token in any output is redacted
by its shape, whether or not the output names the file it came from."""

import secrets
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.tool.leak_detection import LeakDetectionMiddleware
from ptc_agent.agent.middleware.tool.protected_path import ProtectedPathMiddleware
from ptc_agent.core.paths import LANGALPHA_CONFIG_SPELLINGS

CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"


def _request(**args):
    return SimpleNamespace(tool_call={"id": "call-1", "name": "Bash", "args": args})


async def _ran(command: str) -> bool:
    async def handler(request):
        return "ran"

    middleware = ProtectedPathMiddleware(["/home/workspace/_internal"], (LANGALPHA_CONFIG_SPELLINGS,))
    return await middleware.awrap_tool_call(_request(command=command), handler) == "ran"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command",
    [
        f"cat ~/.config/langalpha/claude-code/{CID}",
        "grep -r . ~/.config/langalpha",
        "ls ~/.config/langalpha/",
        "ls $XDG_CONFIG_HOME/langalpha",
        "ls ${XDG_CONFIG_HOME}/langalpha/claude-code",
        "base64 $XDG_CONFIG_HOME/langalpha/claude-code/*",
        "base64 ~/.config/langalpha/claude-code/*",
        f"cat /data/cfg/langalpha/claude-code/{CID}",
        f"cat langalpha/claude-code/.staged.{CID}.Ab12Cd",
        f"cat ~/.Config/LangAlpha/Claude-Code/{CID.upper()}",
        "cat /home/workspace/_INTERNAL/x",
    ],
)
async def test_a_call_naming_the_claude_code_tokens_never_runs(command):
    assert not await _ran(command)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command",
    [
        "ls ~/langalpha",
        "ls ~/langalpha/claude-code/notes.md",
        "cat ~/langalpha/claude-code-token/a.csv",
        "ls ~/.config/langalpha-old",
        "ls ~/.config/other",
        "ls /data/cfg/langalpha/claude-code/",
    ],
)
async def test_a_call_naming_neither_runs_even_in_a_workspace_named_like_the_tokens(command):
    assert await _ran(command)


async def _output(output: str, command: str = "grep -r sk-ant /data") -> ToolMessage:
    """``output`` through the two wrappers as the agent's stack orders them:
    the protected-path check outside, the leak scan inside it."""

    async def handler(request):
        return ToolMessage(content=output, tool_call_id="call-1")

    async def scanned(request):
        return await LeakDetectionMiddleware().awrap_tool_call(request, handler)

    middleware = ProtectedPathMiddleware(["/home/workspace/_internal"], (LANGALPHA_CONFIG_SPELLINGS,))
    return await middleware.awrap_tool_call(_request(command=command), scanned)


def _setup_token() -> str:
    """A string of a setup token's shape, made up per run."""
    return "-".join(["sk", "ant", "oat01", secrets.token_urlsafe(48)])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output",
    [
        f"/data/cfg/langalpha/claude-code/{CID}:sk-ant\n/data/cfg/notes.md:sk-ant\n",
        f"/home/u/.config/langalpha/claude-code/.staged.{CID}.Ab12Cd\n",
    ],
)
async def test_output_showing_a_token_file_is_warned_by_the_same_patterns(output):
    result = await _output(output)
    assert result.content.startswith(output) and "system_warning" in result.content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "line"),
    [
        ("grep -r . ~/.config", f"/home/u/.config/langalpha/claude-code/{CID}:{{token}}"),
        ("cat ~/.config/*/claude-code/*", "{token}"),
        ("grep -rh . ~/.config", "{token}"),
    ],
)
async def test_a_setup_token_in_output_is_redacted_with_or_without_its_path(command, line):
    token = _setup_token()
    result = await _output(f"/home/u/.config/other/settings:x\n{line.format(token=token)}\n", command)
    assert token not in result.content and "[REDACTED:CLAUDE_SETUP_TOKEN]" in result.content
    assert result.content.startswith("/home/u/.config/other/settings:x\n"), "the rest of the output stays"


@pytest.mark.asyncio
async def test_a_listing_above_the_tokens_is_kept_whole():
    listing = f"/home/u/.config\n/home/u/.config/other\n/home/u/.config/langalpha/claude-code/{CID}\n"
    result = await _output(listing, "find ~/.config -maxdepth 3")
    assert result.status != "error" and result.content.startswith(listing)


@pytest.mark.asyncio
async def test_output_showing_another_protected_path_is_kept_with_a_warning():
    result = await _output("/home/workspace/_internal/x.py\n")
    assert result.content.startswith("/home/workspace/_internal/x.py") and "system_warning" in result.content


@pytest.mark.asyncio
async def test_output_showing_neither_is_untouched():
    assert (await _output("/data/cfg/notes.md\n")).content == "/data/cfg/notes.md\n"
