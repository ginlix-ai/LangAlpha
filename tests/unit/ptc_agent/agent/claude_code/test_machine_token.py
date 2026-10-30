"""The machine's token as the backend reaches it, run here through a real Link
on a local stand-in connection: the writes and reads a user's computer runs,
the one rule set that says whether a token file is usable, and where the
file goes under the login's config home.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

from ptc_agent.agent.claude_code import cli_install, machine_token
from ptc_agent.agent.claude_code.launcher import TOKEN_FD_ENV
from ptc_agent.agent.claude_code.machine_token import (
    STREAM_BOOT,
    TOKEN_DIR_SH,
    discard_staged_token,
    has_perl,
    machine_token_state,
    promote_machine_token,
    remove_machine_token,
    stage_machine_token,
)
from ptc_agent.agent.claude_code.ssh_launcher import _PID_MARK
from tests.unit.ptc_agent.agent.claude_code.bsd_userland import bsd_path, on_linux
from tests.unit.ptc_agent.agent.claude_code.local_link import local_link

pytestmark = pytest.mark.skipif(not (shutil.which("perl") and shutil.which("bash")), reason="the machine side needs perl and bash")

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


def test_the_shim_names_what_the_launcher_reads():
    assert TOKEN_FD_ENV in machine_token.shim() and _PID_MARK.strip() in machine_token.shim()


def _token_dir(home: Path) -> Path:
    return home / ".config" / "langalpha" / "claude-code"


async def write(link, token: str, computer_id: str = CID) -> None:
    await promote_machine_token(link, computer_id, await stage_machine_token(link, computer_id, token))


@pytest.mark.asyncio
async def test_a_written_token_is_private_reads_as_usable_and_goes_on_removal(tmp_path):
    link = local_link(HOME=str(tmp_path))
    assert await machine_token_state(link, CID) == "none"
    await write(link, TOKEN)
    path = _token_dir(tmp_path) / CID
    assert path.read_text().strip() == TOKEN
    modes = [stat.S_IMODE(p.stat().st_mode) for p in (path, path.parent, path.parent.parent)]
    assert modes == [0o600, 0o700, 0o700]
    assert sorted(os.listdir(path.parent)) == [CID], "nothing staged is left behind"
    assert await machine_token_state(link, CID) == "file"
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
    assert await machine_token_state(link, CID) == "none" and await machine_token_state(link, OTHER) == "file"


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
    assert await machine_token_state(link, CID) == "none"
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
    assert await machine_token_state(link, CID) == "file"


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
    assert await machine_token_state(local_link(HOME=str(tmp_path)), CID) == state
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
async def test_a_machine_without_perl_has_no_token_a_turn_could_use(tmp_path):
    bare = tmp_path / "bin"
    bare.mkdir()
    for tool in ("bash", "sh", "cat", "mkdir", "chmod", "mktemp", "mv", "find"):
        (bare / tool).symlink_to(shutil.which(tool))
    link = local_link(str(bare), HOME=str(tmp_path))
    await write(link, TOKEN)
    assert not await has_perl(link) and await has_perl(local_link(HOME=str(tmp_path)))
    assert await machine_token_state(link, CID) == "none"


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
    assert await machine_token_state(link, CID) == "file"
    await discard_staged_token(link, staged)
    await write(link, TOKEN)
    assert (_token_dir(home) / CID).read_text().strip() == TOKEN
    stale = await stage_machine_token(link, CID, OLD)
    hour_ago = (_token_dir(home) / stale).stat().st_mtime - 3600
    os.utime(_token_dir(home) / stale, (hour_ago, hour_ago))
    assert await machine_token_state(link, CID) == "file" and os.listdir(_token_dir(home)) == [CID]
    await stage_machine_token(link, CID, OLD)
    assert await remove_machine_token(link, CID) and os.listdir(_token_dir(home)) == []
