"""The machine's token as the backend reaches it, run here through a real Link
on a local stand-in connection: the writes and reads a user's computer runs,
the one rule set that says whether a token file is usable, and where the
file goes under the login's config home.
"""

from __future__ import annotations

import asyncio
import os
import pwd
import shutil
import sys
import stat
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.claude_code import cli_install, machine_token, ssh_launcher
from ptc_agent.agent.claude_code.launcher import TOKEN_FD_ENV
from ptc_agent.agent.claude_code.machine_token import (
    STREAM_BOOT,
    TOKEN_DIR_SH,
    discard_staged_token,
    machine_state,
    promote_machine_token,
    remove_machine_token,
    stage_machine_token,
)
from ptc_agent.agent.claude_code.ssh_launcher import _PID_MARK, SSHLauncher
from ptc_agent.core import claude_code_managed
from ptc_agent.core.claude_code_managed import local_managed
from ptc_agent.core.paths import CLAUDE_CODE_TOKEN_DIR
from ptc_agent.core.sandbox.providers._ssh_transport import Machine
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.llms.claude_code import ClaudeTurnFailed
from tests.unit.ptc_agent.agent.claude_code.bsd_userland import bsd_path, on_linux
from tests.unit.ptc_agent.agent.claude_code.local_link import local_link

pytestmark = [
    pytest.mark.macos_scripts,
    pytest.mark.skipif(not (shutil.which("perl") and shutil.which("bash")), reason="the machine side needs perl and bash"),
]

TOKEN = "sk-ant-oat01-" + "3" * 80
OLD = "sk-ant-oat01-" + "4" * 80
CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
OTHER = "5d0c1f2e-8a7b-4c3d-9e1f-0a2b3c4d5e6f"


def test_the_vendored_scripts_parse_as_they_are_sent(tmp_path):
    install = tmp_path / "install.sh"
    install.write_text(cli_install._script(cli_install.pinned(), "/m/root", cli_install.RELEASES_URL))
    subprocess.run(["bash", "-n", str(install)], check=True)
    subprocess.run(["bash", "-n", str(cli_install._SCRIPT_FILE)], check=True)
    for name, script in (("shim", machine_token.shim()), ("state", machine_token.state_script())):
        path = tmp_path / f"{name}.pl"
        path.write_text(script)
        done = subprocess.run(["perl", "-c", str(path)], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr


async def _readied(link, monkeypatch) -> tuple[bool, bool]:
    """What a launcher readying the machine finds there before it installs
    anything: whether it refuses the machine for a policy installed there,
    and whether it has perl, which a refused machine is not asked."""
    monkeypatch.setattr(cli_install, "ensure_cli", AsyncMock(return_value="/root/cli/claude"))
    on_blocked = AsyncMock()
    launcher = SSHLauncher(Machine(AsyncMock(return_value=link), lambda: "/root"), identity="user-1", token_file=CID,
                           on_blocked=on_blocked)
    try:
        await launcher.ready()
    except ClaudeTurnFailed as e:
        if e.kind == "needs_perl":
            return False, False
        assert e.kind == "policy_blocked" and on_blocked.await_count == 1
        return True, True
    return False, True


def test_the_shim_names_what_the_launcher_reads():
    assert TOKEN_FD_ENV in machine_token.shim() and _PID_MARK.strip() in machine_token.shim()


def _token_dir(home: Path) -> Path:
    return home / ".config" / "langalpha" / "claude-code"


async def write(link, token: str, computer_id: str = CID) -> None:
    await promote_machine_token(link, computer_id, await stage_machine_token(link, computer_id, token))


@pytest.mark.asyncio
async def test_a_written_token_is_private_reads_as_usable_and_goes_on_removal(tmp_path):
    link = local_link(HOME=str(tmp_path))
    assert (await machine_state(link, CID)).file == "none"
    await write(link, TOKEN)
    path = _token_dir(tmp_path) / CID
    assert path.read_text().strip() == TOKEN
    modes = [stat.S_IMODE(p.stat().st_mode) for p in (path, path.parent, path.parent.parent)]
    assert modes == [0o600, 0o700, 0o700]
    assert sorted(os.listdir(path.parent)) == [CID], "nothing staged is left behind"
    assert (await machine_state(link, CID)).file == "file"
    assert await remove_machine_token(link, CID) and not path.exists()
    assert not await remove_machine_token(link, CID)


@pytest.mark.asyncio
async def test_two_computers_on_one_login_each_keep_their_own_token(tmp_path):
    link = local_link(HOME=str(tmp_path))
    await write(link, TOKEN, CID)
    await write(link, OLD, OTHER)
    assert (_token_dir(tmp_path) / CID).read_text().strip() == TOKEN
    assert (_token_dir(tmp_path) / OTHER).read_text().strip() == OLD
    assert await remove_machine_token(link, CID)
    assert (await machine_state(link, CID)).file == "none" and (await machine_state(link, OTHER)).file == "file"


@pytest.mark.asyncio
async def test_a_staged_token_leaves_the_one_in_use_until_it_is_promoted(tmp_path):
    link = local_link(HOME=str(tmp_path))
    await write(link, OLD)
    staged = await stage_machine_token(link, CID, TOKEN)
    assert staged.startswith(f".staged.{CID}.")
    assert (_token_dir(tmp_path) / CID).read_text().strip() == OLD
    await discard_staged_token(link, staged)
    assert sorted(os.listdir(_token_dir(tmp_path))) == [CID]
    assert (_token_dir(tmp_path) / CID).read_text().strip() == OLD


@pytest.mark.asyncio
async def test_removing_a_computers_token_takes_its_staged_ones_and_leaves_the_others(tmp_path):
    link = local_link(HOME=str(tmp_path))
    await write(link, TOKEN)
    await stage_machine_token(link, CID, OLD)
    other = await stage_machine_token(link, OTHER, OLD)
    assert await remove_machine_token(link, CID)
    assert os.listdir(_token_dir(tmp_path)) == [other]


@pytest.mark.asyncio
async def test_a_status_read_sweeps_only_the_computers_stale_staged_tokens(tmp_path):
    link = local_link(HOME=str(tmp_path))
    stale, fresh, others = [await stage_machine_token(link, cid, OLD) for cid in (CID, CID, OTHER)]
    for name in (stale, others):
        hour_ago = (_token_dir(tmp_path) / name).stat().st_mtime - 3600
        os.utime(_token_dir(tmp_path) / name, (hour_ago, hour_ago))
    assert (await machine_state(link, CID)).file == "none"
    assert sorted(os.listdir(_token_dir(tmp_path))) == sorted([fresh, others])


@pytest.mark.asyncio
async def test_staged_tokens_a_lost_connection_left_are_swept_by_the_next_write(tmp_path):
    link = local_link(HOME=str(tmp_path))
    old = await stage_machine_token(link, CID, OLD)
    fresh = await stage_machine_token(link, OTHER, OLD)
    hour_ago = (_token_dir(tmp_path) / old).stat().st_mtime - 3600
    os.utime(_token_dir(tmp_path) / old, (hour_ago, hour_ago))
    await write(link, TOKEN)
    assert sorted(os.listdir(_token_dir(tmp_path))) == sorted([CID, fresh])


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["../secret", "claude-code-token", ".staged.x", f"{CID}/x"])
async def test_a_name_that_is_not_a_token_files_is_refused_before_anything_runs(tmp_path, name):
    with pytest.raises(ValueError):
        await discard_staged_token(local_link(HOME=str(tmp_path)), name)
    with pytest.raises(ValueError):
        await remove_machine_token(local_link(HOME=str(tmp_path)), name)


@pytest.mark.asyncio
@pytest.mark.parametrize(("config_home", "under"), [("xdg", "xdg"), ("relative/cfg", "home/.config")])
async def test_the_token_goes_under_the_logins_config_home_when_it_is_absolute(tmp_path, monkeypatch, config_home, under):
    """A relative $XDG_CONFIG_HOME is ignored, as the XDG spec has it."""
    monkeypatch.chdir(tmp_path)
    xdg = str(tmp_path / config_home) if config_home == "xdg" else config_home
    link = local_link(HOME=str(tmp_path / "home"), XDG_CONFIG_HOME=xdg)
    await write(link, TOKEN)
    assert (tmp_path / under / "langalpha" / "claude-code" / CID).read_text().strip() == TOKEN
    assert (await machine_state(link, CID)).file == "file"


def _shim_announces(env: dict[str, str], cli: Path, computer_id: str = CID) -> str:
    """What the shim with relay off announces for the computer, run as a turn's command runs it."""
    done = subprocess.run(
        ["sh", "-c", STREAM_BOOT], capture_output=True, timeout=30, env={"PATH": os.environ["PATH"], **env},
        input=f'{TOKEN_DIR_SH}exec perl -e {machine_token.word(machine_token.shim())} 0 "$d/{computer_id}" {cli}\n'.encode(),
    )
    return done.stdout.decode().splitlines()[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(("text", "mode", "state"), [
    (TOKEN, 0o600, "file"),
    (f"  {TOKEN}\n", 0o600, "file"),
    ("", 0o600, "invalid"),
    (" \t\n ", 0o600, "invalid"),
    (TOKEN, 0o640, "invalid"),
])
async def test_the_status_check_reads_the_file_by_the_rule_a_turn_uses(tmp_path, text, mode, state):
    path = _token_dir(tmp_path) / CID
    path.parent.mkdir(parents=True)
    path.write_text(text)
    path.chmod(mode)
    assert (await machine_state(local_link(HOME=str(tmp_path)), CID)).file == state
    cli = tmp_path / "cli"
    cli.write_text("#!/bin/sh\nexit 0\n")
    cli.chmod(0o755)
    assert _shim_announces({"HOME": str(tmp_path)}, cli) == f"langalpha-token {'file' if state == 'file' else 'none'}"
    assert _shim_announces({"HOME": str(tmp_path)}, cli, OTHER) == "langalpha-token none", "another computer's token is not this one's"


@pytest.mark.asyncio
async def test_the_state_never_carries_the_token(tmp_path):
    link = local_link(HOME=str(tmp_path))
    await write(link, TOKEN)
    script = f'{TOKEN_DIR_SH}{machine_token.FIND_PERL}exec "$p" -e {machine_token.word(machine_token.state_script())} "$d/{CID}"'
    status, out, err = await machine_token._run_stream(link, script)
    assert (status, out) == (0, "file") and TOKEN not in err


@pytest.mark.asyncio
async def test_a_machine_without_perl_has_no_token_a_turn_could_use(tmp_path, monkeypatch):
    bare = tmp_path / "bin"
    bare.mkdir()
    for tool in ("bash", "sh", "cat", "mkdir", "chmod", "mktemp", "mv", "find"):
        (bare / tool).symlink_to(shutil.which(tool))
    link = local_link(str(bare), HOME=str(tmp_path))
    await write(link, TOKEN)
    assert not (await _readied(link, monkeypatch))[1] and (await _readied(local_link(HOME=str(tmp_path)), monkeypatch))[1]
    assert (await machine_state(link, CID)).file == "no_perl"


@pytest.mark.asyncio
@pytest.mark.parametrize("perl", [True, False])
async def test_a_machine_on_musl_is_told_so_before_whether_it_has_perl(tmp_path, monkeypatch, perl):
    """One registered before the probe refused musl: no build of the CLI runs
    there, which installing perl would not change, so that is what the status
    check and the launcher both find, and the launcher installs nothing."""
    musl = tmp_path / "musl"
    musl.mkdir()
    (musl / "getconf").write_text("#!/bin/sh\nexit 1\n")
    (musl / "ldd").write_text("#!/bin/sh\necho 'musl libc (x86_64)' >&2\nexit 1\n")
    (musl / "uname").write_text('#!/bin/sh\ncase "$1" in -s) echo Linux ;; *) echo x86_64 ;; esac\n')
    for tool in musl.iterdir():
        tool.chmod(0o755)
    bare = tmp_path / "bin"
    bare.mkdir()
    for tool in ("bash", "sh", "cat", "mkdir", "chmod", "mktemp", "mv", "find", "ls", "grep"):
        (bare / tool).symlink_to(shutil.which(tool))
    link = local_link(f"{musl}:{os.environ['PATH'] if perl else bare}", HOME=str(tmp_path))
    assert (await machine_state(link, CID))[:2] == ("unsupported", False)
    await write(local_link(HOME=str(tmp_path)), TOKEN)
    assert (await machine_state(link, CID))[:2] == ("unsupported", True)
    ensure = AsyncMock(return_value="/root/cli/claude")
    monkeypatch.setattr(cli_install, "ensure_cli", ensure)
    on_unsupported, on_no_perl = AsyncMock(), AsyncMock()
    launcher = SSHLauncher(
        Machine(AsyncMock(return_value=link), lambda: "/root"), identity="user-1", token_file=CID,
        on_unsupported=on_unsupported, on_no_perl=on_no_perl,
    )
    with pytest.raises(ClaudeTurnFailed) as refused:
        await launcher.ready()
    assert (refused.value.kind, refused.value.detail) == ("computer_unsupported", MUSL_UNSUPPORTED)
    on_unsupported.assert_awaited_once_with(True)
    assert not on_no_perl.await_count and not ensure.await_count


@pytest.mark.asyncio
async def test_a_machine_without_perl_still_says_whether_a_token_file_is_there(tmp_path):
    """So Settings still offers to remove it, and a removal that cannot reach the machine still tells of it."""
    bare = tmp_path / "bin"
    bare.mkdir()
    for tool in ("bash", "sh", "cat", "mkdir", "chmod", "mktemp", "mv", "find"):
        (bare / tool).symlink_to(shutil.which(tool))
    link = local_link(str(bare), HOME=str(tmp_path))

    async def a_turn_finds() -> bool:
        on_no_perl = AsyncMock()
        launcher = SSHLauncher(Machine(AsyncMock(return_value=link), lambda: "/root"), identity="user-1", token_file=CID,
                               on_no_perl=on_no_perl)
        with pytest.raises(ClaudeTurnFailed) as refused:
            await launcher.ready()
        assert refused.value.kind == "needs_perl"
        (present,) = on_no_perl.await_args.args
        return present

    assert (await machine_state(link, CID))[:2] == ("no_perl", False) and not await a_turn_finds()
    await write(link, TOKEN)
    assert (await machine_state(link, CID))[:2] == ("no_perl", True) and await a_turn_finds()
    assert (await machine_state(local_link(HOME=str(tmp_path)), CID))[:2] == ("file", True)


@pytest.mark.asyncio
@pytest.mark.parametrize(("xdg", "folder"), [
    (None, f"~/.config/{CLAUDE_CODE_TOKEN_DIR}"),
    ("relative/cfg", f"~/.config/{CLAUDE_CODE_TOKEN_DIR}"),
    ("{home}/cfg", f"~/cfg/{CLAUDE_CODE_TOKEN_DIR}"),
    ("{tmp}/elsewhere", f"{{tmp}}/elsewhere/{CLAUDE_CODE_TOKEN_DIR}"),
])
async def test_the_status_check_names_the_tokens_folder_with_the_logins_home_as_tilde(tmp_path, xdg, folder):
    """Where a removal that cannot reach the machine tells its owner to delete the token by hand."""
    home = tmp_path / "home"
    env = {"HOME": str(home)} | ({"XDG_CONFIG_HOME": xdg.format(home=home, tmp=tmp_path)} if xdg else {})
    state = await machine_state(local_link(**env), CID)
    assert state.folder == folder.format(tmp=tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(("xdg", "folder"), [
    (None, f"~/.config/{CLAUDE_CODE_TOKEN_DIR}"),
    ("{tmp}/elsewhere", f"{{tmp}}/elsewhere/{CLAUDE_CODE_TOKEN_DIR}"),
])
async def test_a_token_put_in_place_names_its_folder_as_the_status_check_does(tmp_path, xdg, folder):
    """A save kept before any status check still tells a removal that cannot
    reach the machine where the token is."""
    env = {"HOME": str(tmp_path / "home")} | ({"XDG_CONFIG_HOME": xdg.format(tmp=tmp_path)} if xdg else {})
    link = local_link(**env)
    named = await promote_machine_token(link, CID, await stage_machine_token(link, CID, TOKEN))
    assert named == folder.format(tmp=tmp_path) == (await machine_state(link, CID)).folder


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["", ">&2"])
async def test_a_machine_that_never_stops_printing_fails_the_script_at_once(tmp_path, stream):
    """A token script's output is bounded as a command's is, so a login that
    prints without end fails the script rather than filling the worker's
    memory until the script's time runs out."""
    bare = tmp_path / "bin"
    bare.mkdir()
    for tool in ("bash", "sh", "yes"):
        (bare / tool).symlink_to(shutil.which(tool))
    (bare / "mkdir").write_text(f"#!/bin/sh\nexec yes {stream}\n")
    (bare / "mkdir").chmod(0o755)
    with pytest.raises(RuntimeError, match=f"printed more than {machine_token.OUTPUT_CAP} bytes"):
        await asyncio.wait_for(stage_machine_token(local_link(str(bare), HOME=str(tmp_path)), CID, TOKEN), 10)


@pytest.mark.asyncio
async def test_a_token_that_is_not_one_line_is_never_sent(tmp_path):
    with pytest.raises(ValueError):
        await stage_machine_token(local_link(HOME=str(tmp_path)), CID, "two words")
    assert not (tmp_path / ".config").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["symlink", "config-not-a-folder"])
async def test_a_refused_write_says_why_without_the_token(tmp_path, how):
    config = tmp_path / ".config"
    link = local_link(HOME=str(tmp_path))
    if how == "symlink":
        _token_dir(tmp_path).mkdir(parents=True)
        (tmp_path / "elsewhere").write_text("keep")
        (_token_dir(tmp_path) / CID).symlink_to(tmp_path / "elsewhere")
    else:
        config.write_text("not a folder")
    with pytest.raises(RuntimeError) as refused:
        await write(link, TOKEN)
    assert TOKEN not in str(refused.value) and TOKEN not in repr(refused.value.__context__)
    if how == "symlink":
        assert (tmp_path / "elsewhere").read_text() == "keep"
        staged = [p for p in _token_dir(tmp_path).iterdir() if p.name.startswith(".staged.")]
        for p in staged:
            await discard_staged_token(link, p.name)
    assert not any(TOKEN in p.read_text() for p in tmp_path.rglob("*") if p.is_file() and not p.is_symlink())


@on_linux
@pytest.mark.asyncio
async def test_every_token_script_runs_on_a_macs_userland(tmp_path):
    """Stage, rename in, read, discard, sweep and remove, with BSD tools."""
    home = tmp_path / "home"
    link = local_link(bsd_path(tmp_path), HOME=str(home))
    await write(link, OLD)
    staged = await stage_machine_token(link, CID, TOKEN)
    assert (await machine_state(link, CID)).file == "file"
    await discard_staged_token(link, staged)
    await write(link, TOKEN)
    assert (_token_dir(home) / CID).read_text().strip() == TOKEN
    stale = await stage_machine_token(link, CID, OLD)
    hour_ago = (_token_dir(home) / stale).stat().st_mtime - 3600
    os.utime(_token_dir(home) / stale, (hour_ago, hour_ago))
    assert (await machine_state(link, CID)).file == "file" and os.listdir(_token_dir(home)) == [CID]
    await stage_machine_token(link, CID, OLD)
    assert await remove_machine_token(link, CID) and os.listdir(_token_dir(home)) == []

def _managed_dirs(tmp_path: Path, monkeypatch) -> tuple[Path, Path, Path]:
    """Where the scripts look for the organization's managed settings, moved
    under ``tmp_path``, with the spaces a Mac's paths have."""
    linux, mac, prefs = tmp_path / "etc" / "claude-code", tmp_path / "Application Support" / "ClaudeCode", tmp_path / "Managed Preferences"
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_LINUX", str(linux))
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_MAC", str(mac))
    monkeypatch.setattr(claude_code_managed, "MAC_MANAGED_PREFERENCES_DIR", str(prefs))
    return linux, mac, prefs


def _host_managed_dir(tmp_path: Path, monkeypatch) -> Path:
    """The managed folder of the system the test runs on, which the probe picks by ``uname -s``."""
    linux, mac, _ = _managed_dirs(tmp_path, monkeypatch)
    return mac if sys.platform == "darwin" else linux


def _place(folder: Path, *names: str) -> None:
    """Managed files at ``names`` under ``folder``, none of them readable: only their names may be tested."""
    for name in names:
        path = folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}")
        path.chmod(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(("files", "managed"), [
    ((), ()),
    (("managed-settings.json",), ("settings",)),
    (("managed-settings.d/10-org.json",), ("dropins",)),
    (("managed-settings.d/notes.txt",), ()),
    (("managed-settings.d/.org.json",), ()),
    (("managed-mcp.json",), ("mcp",)),
    (("managed-mcp.json", "managed-settings.d/a.json", "managed-settings.d/b.json", "managed-settings.json"), ("settings", "dropins", "mcp")),
])
async def test_the_status_check_names_the_organizations_managed_settings_without_reading_them(tmp_path, monkeypatch, files, managed):
    _place(_host_managed_dir(tmp_path, monkeypatch), *files)
    assert await local_managed() == managed
    link = local_link(HOME=str(tmp_path / "home"))
    assert (await machine_state(link, CID))[:2] == ("policy" if managed else "none", False)
    assert await _readied(link, monkeypatch) == (bool(managed), True)
    await write(link, TOKEN)
    assert (await machine_state(link, CID))[:2] == ("policy" if managed else "file", True), "a token there still named"


def _as_mac(tmp_path: Path) -> str:
    """A PATH with a Mac's userland whose ``uname -s`` says Darwin."""
    folder = tmp_path / "darwin-bin"
    folder.mkdir()
    (folder / "uname").write_text(f'#!/bin/sh\n[ "$1" = -s ] && {{ echo Darwin; exit 0; }}\nexec {shutil.which("uname")} "$@"\n')
    (folder / "uname").chmod(0o755)
    return f"{folder}:{bsd_path(tmp_path)}"


@on_linux
@pytest.mark.asyncio
@pytest.mark.parametrize(("files", "managed"), [
    ((), ()),
    (("mac/managed-settings.json", "mac/managed-mcp.json"), ("settings", "mcp")),
    (("mac/managed-settings.d/org.json",), ("dropins",)),
    (("prefs/com.anthropic.claudecode.plist",), ("profile",)),
    (("prefs/{login}/com.anthropic.claudecode.plist",), ("profile",)),
    # The CLI names the per-user profile by the login (os.userInfo()), never by $USER.
    (("prefs/someone-else/com.anthropic.claudecode.plist",), ()),
    (("linux/managed-mcp.json",), ()),
])
async def test_a_mac_is_read_in_its_own_places_with_its_userland(tmp_path, monkeypatch, files, managed):
    linux, mac, prefs = _managed_dirs(tmp_path, monkeypatch)
    folders = {"linux": linux, "mac": mac, "prefs": prefs}
    for file in files:
        where, _, name = file.format(login=pwd.getpwuid(os.geteuid()).pw_name).partition("/")
        _place(folders[where], name)
    mac_path = _as_mac(tmp_path)
    monkeypatch.setenv("PATH", mac_path)
    monkeypatch.setenv("USER", "someone-else")
    assert await local_managed() == managed
    link = local_link(mac_path, HOME=str(tmp_path / "home"), USER="someone-else")
    assert (await machine_state(link, CID)).file == ("policy" if managed else "none")
    assert await _readied(link, monkeypatch) == (bool(managed), True)


@pytest.mark.asyncio
async def test_a_folder_named_like_a_drop_in_is_not_one(tmp_path, monkeypatch):
    """The CLI reads only files (or links) ending in .json from managed-settings.d."""
    (_host_managed_dir(tmp_path, monkeypatch) / "managed-settings.d" / "org.json").mkdir(parents=True)
    assert await local_managed() == ()


@pytest.mark.asyncio
async def test_a_managed_source_not_known_here_is_left_out(tmp_path, monkeypatch):
    """A newer probe may name a source this server does not know: the check still answers."""
    for module in (machine_token, ssh_launcher, claude_code_managed):
        monkeypatch.setattr(module, "claude_code_managed_sh", lambda: 'cm=" future mcp settings"; ')
    assert await local_managed() == ("settings", "mcp")
    link = local_link(HOME=str(tmp_path))
    assert (await machine_state(link, CID)).file == "policy"
    assert await _readied(link, monkeypatch) == (True, True)
    for module in (machine_token, ssh_launcher, claude_code_managed):
        monkeypatch.setattr(module, "claude_code_managed_sh", lambda: 'cm=" future"; ')
    assert await local_managed() == () and (await machine_state(link, CID)).file == "none"
    assert await _readied(link, monkeypatch) == (False, True)


@pytest.mark.asyncio
async def test_a_machine_that_fails_the_launchers_probe_says_so(tmp_path, monkeypatch):
    monkeypatch.setattr(ssh_launcher, "claude_code_managed_sh", lambda: "exit 3; ")
    with pytest.raises(RuntimeError, match="managed settings"):
        await _readied(local_link(HOME=str(tmp_path)), monkeypatch)
