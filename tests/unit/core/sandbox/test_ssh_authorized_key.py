"""The deployment's key line on a machine, how each start keeps it current,
and how a removal takes it away.

The converge and remove scripts run here against a real file, since their
whole job is leaving the account's other keys exactly as they were while they
rewrite ours.
"""

import os
import shlex
import subprocess
import sys
import time

import pytest

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers._ssh_authorized_key import (
    admits,
    authorized_key_line,
    converge_script,
    remove_script,
)
from ptc_agent.core.sandbox.providers._ssh_shell import home_shared_script

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")

_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl langalpha"
_OTHERS = [
    "ssh-rsa AAAAold old@host",
    'from="198.51.100.4" ssh-ed25519 AAAAmine me@laptop',
]


def _sh(home, script: str) -> str:
    run = subprocess.run(
        ["sh", "-c", script],
        env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=True,
    )
    return run.stdout.strip()


def _converge(home, line: str) -> str:
    return _sh(home, converge_script(line))


def _remove(home) -> str:
    return _sh(home, remove_script(_KEY))


def _keys(home, *lines: str):
    path = home / ".ssh" / "authorized_keys"
    path.parent.mkdir(exist_ok=True)
    path.write_text("".join(f"{line}\n" for line in lines))
    path.chmod(0o600)
    return path


class TestConverge:
    @pytest.mark.parametrize(
        "stale",
        [[_KEY], [authorized_key_line(_KEY), authorized_key_line(_KEY, ["192.0.2.1"])]],
        ids=["bare", "two-older-lines"],
    )
    def test_our_lines_become_the_one_current_line_and_the_rest_stay(
        self, tmp_path, stale
    ):
        line = authorized_key_line(_KEY, ["203.0.113.7"])
        path = _keys(tmp_path, _OTHERS[0], *stale, _OTHERS[1])

        assert _converge(tmp_path, line) == "updated"
        assert path.read_text() == "".join(f"{x}\n" for x in [*_OTHERS, line])
        assert path.stat().st_mode & 0o777 == 0o600
        assert _converge(tmp_path, line) == "current"

    def test_a_file_grep_reads_as_binary_keeps_every_other_line(self, tmp_path):
        """A NUL byte has grep print "Binary file matches", or stop there, and
        exit 0, which once replaced every other key with that."""
        path = _keys(tmp_path, _OTHERS[0])
        path.write_bytes(
            path.read_bytes()
            + f"bad\0paste\n{authorized_key_line(_KEY)}\n{_OTHERS[1]}".encode()
        )
        line = authorized_key_line(_KEY, ["203.0.113.7"])

        assert _converge(tmp_path, line) == "updated"
        assert path.read_bytes() == (
            f"{_OTHERS[0]}\nbad\0paste\n{_OTHERS[1]}\n{line}\n".encode()
        )

    @pytest.mark.parametrize(
        "theirs",
        [
            f"no-port-forwarding,no-agent-forwarding,no-X11-forwarding {_KEY}",
            f'from="203.0.113.7",command="true",no-agent-forwarding,no-X11-forwarding {_KEY}',
            f"# {authorized_key_line(_KEY)}",
        ],
        ids=["narrowed", "options-inside-from", "commented-out"],
    )
    def test_a_line_the_owner_changed_is_theirs(self, tmp_path, theirs):
        path = _keys(tmp_path, _OTHERS[0], theirs)

        assert _converge(tmp_path, authorized_key_line(_KEY, ["203.0.113.7"])) == (
            "skipped"
        )
        assert path.read_text() == f"{_OTHERS[0]}\n{theirs}\n"

    def test_a_file_without_our_key_is_left_alone(self, tmp_path):
        """The login came by another file sshd reads, which is not ours to edit."""
        path = _keys(tmp_path, *_OTHERS)

        assert _converge(tmp_path, authorized_key_line(_KEY)) == "absent"
        assert path.read_text() == "".join(f"{x}\n" for x in _OTHERS)

    def test_a_linked_file_is_left_alone(self, tmp_path):
        """Replacing it would cut the link to whatever manages the real one."""
        real = _keys(tmp_path, _KEY)
        link = tmp_path / ".ssh" / "linked"
        real.rename(link)
        real.symlink_to(link)

        assert _converge(tmp_path, authorized_key_line(_KEY)) == "skipped"
        assert link.read_text() == f"{_KEY}\n"
        assert real.is_symlink()


class TestRemove:
    @pytest.mark.parametrize(
        "ours",
        [
            [_KEY],
            [authorized_key_line(_KEY), authorized_key_line(_KEY, ["192.0.2.1"])],
        ],
        ids=["bare", "two-lines"],
    )
    def test_only_our_lines_go(self, tmp_path, ours):
        path = _keys(tmp_path, _OTHERS[0], ours[0], _OTHERS[1], *ours[1:])

        assert _remove(tmp_path) == "removed"
        assert path.read_text() == "".join(f"{x}\n" for x in _OTHERS)
        assert path.stat().st_mode & 0o777 == 0o600
        assert _remove(tmp_path) == "absent"

    def test_a_file_left_with_no_line_stays(self, tmp_path):
        """sshd reads a missing file and an empty one alike; the owner may
        keep the file, and its mode, for keys they add later."""
        path = _keys(tmp_path)
        path.write_text(authorized_key_line(_KEY))  # no newline at the end

        assert _remove(tmp_path) == "removed"
        assert path.read_bytes() == b""
        assert path.stat().st_mode & 0o777 == 0o600

    def test_a_file_grep_reads_as_binary_keeps_every_other_line(self, tmp_path):
        path = _keys(tmp_path, _OTHERS[0])
        path.write_bytes(
            path.read_bytes()
            + f"bad\0paste\n{authorized_key_line(_KEY)}\n{_OTHERS[1]}\n".encode()
        )

        assert _remove(tmp_path) == "removed"
        assert path.read_bytes() == f"{_OTHERS[0]}\nbad\0paste\n{_OTHERS[1]}\n".encode()

    @pytest.mark.parametrize(
        "theirs",
        [
            f"no-port-forwarding,no-agent-forwarding,no-X11-forwarding {_KEY}",
            f'from="203.0.113.7",command="true",no-agent-forwarding,no-X11-forwarding {_KEY}',
            f"# {authorized_key_line(_KEY)}",
        ],
        ids=["narrowed", "options-inside-from", "commented-out"],
    )
    def test_a_line_the_owner_changed_keeps_the_whole_file(self, tmp_path, theirs):
        """Our own line beside it stays too: half a removal would read as done."""
        path = _keys(tmp_path, _OTHERS[0], authorized_key_line(_KEY), theirs)

        assert _remove(tmp_path) == "skipped"
        assert path.read_text() == (
            f"{_OTHERS[0]}\n{authorized_key_line(_KEY)}\n{theirs}\n"
        )

    def test_a_file_without_our_key_is_left_alone(self, tmp_path):
        path = _keys(tmp_path, *_OTHERS)

        assert _remove(tmp_path) == "absent"
        assert path.read_text() == "".join(f"{x}\n" for x in _OTHERS)

    def test_a_linked_file_is_left_alone(self, tmp_path):
        real = _keys(tmp_path, _KEY)
        link = tmp_path / ".ssh" / "linked"
        real.rename(link)
        real.symlink_to(link)

        assert _remove(tmp_path) == "skipped"
        assert link.read_text() == f"{_KEY}\n"
        assert real.is_symlink()

    @pytest.mark.parametrize("ssh_dir", [True, False], ids=["no-file", "no-dir"])
    def test_no_file_is_skipped(self, tmp_path, ssh_dir):
        if ssh_dir:
            (tmp_path / ".ssh").mkdir()

        assert _remove(tmp_path) == "skipped"
        assert not (tmp_path / ".ssh" / "authorized_keys").exists()

    def test_a_key_line_of_another_shape_is_refused(self):
        with pytest.raises(ValueError):
            remove_script(f'command="sh" {_KEY}')

    def test_a_home_another_computer_keeps_its_folder_in_is_left_alone(self, tmp_path):
        """Hosts that mount one home read one file, so a computer on another
        host logs in through the line too; its folder in the home says so."""
        path = _keys(tmp_path, _OTHERS[0], authorized_key_line(_KEY))
        (tmp_path / ".langalpha" / "computers" / "b2").mkdir(parents=True)

        shared = _sh(tmp_path, home_shared_script(["a1", "b2"]) + remove_script(_KEY))

        assert shared == "shared b2"
        assert path.read_text() == f"{_OTHERS[0]}\n{authorized_key_line(_KEY)}\n"
        assert _sh(tmp_path, home_shared_script(["a1"]) + remove_script(_KEY)) == (
            "removed"
        )
        assert home_shared_script([]) == ""


class TestTurns:
    """Two accounts' removals, or a start's converge, can reach one login at once."""

    _SECOND = (
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGjvFCxPkqvLhGmK1FKCXvG1YwVYW0NQ5vJWbg2pDcJw"
        " langalpha"
    )

    def test_overlapping_removals_each_keep_the_others_removal(self, tmp_path):
        for attempt in range(10):
            home = tmp_path / str(attempt)
            home.mkdir()
            path = _keys(
                home, _OTHERS[0], authorized_key_line(_KEY), _OTHERS[1], self._SECOND
            )
            runs = [
                subprocess.Popen(
                    ["sh", "-c", remove_script(key)],
                    env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
                    stdout=subprocess.PIPE,
                    text=True,
                )
                for key in (_KEY, self._SECOND)
            ]

            assert [run.communicate()[0].strip() for run in runs] == ["removed"] * 2
            assert path.read_text() == "".join(f"{x}\n" for x in _OTHERS)
            assert not (home / ".ssh" / "authorized_keys.langalpha.lock").exists()

    def test_a_lock_left_by_a_run_that_died_is_broken(self, tmp_path):
        path = _keys(tmp_path, _OTHERS[0], _KEY)
        lock = tmp_path / ".ssh" / "authorized_keys.langalpha.lock"
        lock.mkdir()
        os.utime(lock, (time.time() - 300, time.time() - 300))

        assert _remove(tmp_path) == "removed"
        assert path.read_text() == f"{_OTHERS[0]}\n"
        assert not lock.exists()

    def test_a_line_added_meanwhile_keeps_the_file_as_it_is(self, tmp_path):
        """An install appending while the file is rewritten loses nothing."""
        path = _keys(tmp_path, _OTHERS[0], _KEY)
        stub = tmp_path / "bin"
        stub.mkdir()
        # The second read, just before the rewrite, finds a line added since.
        (stub / "cksum").write_text(
            '#!/bin/sh\nn=$(cat "$HOME/n" 2>/dev/null); echo "$((n + 1))" > "$HOME/n"\n'
            f'[ "$n" = 1 ] && echo {shlex.quote(_OTHERS[1])} >> "$HOME/.ssh/authorized_keys"\n'
            "exec /usr/bin/cksum\n"
        )
        (stub / "cksum").chmod(0o755)

        run = subprocess.run(
            ["sh", "-c", remove_script(_KEY)],
            env={"HOME": str(tmp_path), "PATH": f"{stub}:/usr/bin:/bin"},
            capture_output=True,
            text=True,
        )

        assert run.returncode == 1
        assert path.read_text() == f"{_OTHERS[0]}\n{_KEY}\n{_OTHERS[1]}\n"

    def test_a_folder_that_takes_no_file_is_still_read(self, tmp_path):
        _keys(tmp_path, authorized_key_line(_KEY))
        (tmp_path / ".ssh").chmod(0o500)
        try:
            assert _converge(tmp_path, authorized_key_line(_KEY)) == "current"
        finally:
            (tmp_path / ".ssh").chmod(0o700)


class TestAdmits:
    @pytest.mark.parametrize(
        "login_from,address,admitted",
        [
            ([], "198.51.100.9", True),
            (["203.0.113.7"], "203.0.113.7", True),
            (["203.0.113.0/24"], "203.0.113.200", True),
            (["203.0.113.7"], "::ffff:203.0.113.7", True),
            (["203.0.113.7"], "127.0.0.1", True),
            (["203.0.113.7"], "::1", True),
            (["2001:db8::/32"], "2001:db8::5%eth0", True),
            (["203.0.113.7"], "203.0.113.8", False),
            (["203.0.113.7"], "", False),
        ],
        ids=[
            "unset",
            "listed",
            "in-network",
            "v4-mapped",
            "tunnel-v4",
            "tunnel-v6",
            "zoned",
            "unlisted",
            "unknown",
        ],
    )
    def test_a_line_admits_what_sshd_would(self, login_from, address, admitted):
        assert admits(login_from, address) is admitted


class TestLine:
    def test_unset_carries_only_the_forwarding_options(self):
        assert (
            authorized_key_line(_KEY) == f"no-agent-forwarding,no-X11-forwarding {_KEY}"
        )

    def test_set_admits_the_machine_itself_for_its_tunnel(self):
        assert authorized_key_line(_KEY, ["203.0.113.7"]).startswith(
            'from="203.0.113.7,127.0.0.1,::1",'
        )

    @pytest.mark.parametrize(
        "public_key,login_from",
        [
            (f"{_KEY}\nssh-rsa AAAAx", ()),
            (f'command="sh" {_KEY}', ()),
            (_KEY, ['203.0.113.7",command="sh']),
        ],
        ids=["two-lines", "options-in-the-key", "quote-in-a-source"],
    )
    def test_anything_sshd_would_read_as_more_is_refused(self, public_key, login_from):
        with pytest.raises(ValueError):
            authorized_key_line(public_key, login_from)


class TestSetting:
    def test_sources_are_normalized_as_sshd_matches_them(self):
        config = SSHConfig(
            login_from=[
                "203.0.113.7/32",
                " 203.0.113.7",
                "198.51.100.5/24",
                "2001:db8::1",
            ]
        )

        assert config.login_from == ["203.0.113.7", "198.51.100.0/24", "2001:db8::1"]

    def test_a_host_name_is_refused(self):
        """sshd does not resolve clients by name unless told to, so it would never match."""
        with pytest.raises(ValueError, match="not an IP address or network"):
            SSHConfig(login_from=["egress.example.com"])
