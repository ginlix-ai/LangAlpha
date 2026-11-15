"""The shared proof (``_proc_sh``), run in every exec shell present over a
/proc moved into a temp dir, so a test sets which process each pid names. A
real process in its own group stands in for whatever group a pid names, so a
signal that reaches it shows."""

from __future__ import annotations

import os
import signal
import subprocess

import pytest

from ptc_agent.core.sandbox.providers._proc_sh import PROC_SH, ps_start_sh

_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"


def _shells() -> list[str]:
    found: dict[str, str] = {}
    for path in ("/bin/bash", "/bin/dash", "/bin/zsh", "/usr/bin/zsh"):
        name = os.path.basename(path)
        if name not in found and os.access(path, os.X_OK):
            found[name] = path
    return list(found.values())


def _stat_line(pid: int, start: int, *, pgrp: int, state: str) -> str:
    """A /proc/<pid>/stat line in ``state``, whose group (field 5) is ``pgrp``
    and start time (field 22) ``start``, behind a command name with the spaces
    and parens one may hold."""
    return f"{pid} (a) b (c) {state} 0 {pgrp} {' '.join(['0'] * 16)} {start} 0 0\n"


@pytest.mark.parametrize("shell", _shells(), ids=os.path.basename)
@pytest.mark.parametrize(
    ("z", "proven"), [("z", True), ("", False)], ids=["session-or-ssh", "job"]
)
def test_a_zombie_leader_reads_as_ended_and_proves_its_group_only_when_asked(
    shell, z, proven, tmp_path
):
    """A wrapper killed in a sandbox whose init reaps nothing stays a zombie,
    which still holds its pid and its group's id, so a session's or the SSH
    stop still reaches what it left in its group. A job's stop does not ask:
    its job reads as having vanished, and a lone zombie is not one running."""
    proc = tmp_path / "proc"
    (proc / "1").mkdir(parents=True)
    (proc / "1" / "stat").write_text(_stat_line(1, 100, pgrp=1, state="S"))
    victim = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        (proc / str(victim.pid)).mkdir()
        (proc / str(victim.pid) / "stat").write_text(
            _stat_line(victim.pid, 300, pgrp=victim.pid, state="Z")
        )
        record = tmp_path / "pid"
        record.write_text(f"{victim.pid} 300\n")
        script = (
            '[ -z "${ZSH_VERSION-}" ] || emulate sh\n'
            + PROC_SH
            + f"plive {record} && echo live\n"
            + f"psig TERM {victim.pid} 300 {z} && echo signalled\n"
        ).replace("/proc/", f"{proc}/")

        done = subprocess.run(
            [shell, "-c", script],
            capture_output=True,
            text=True,
            env={"PATH": _PATH},
            timeout=30,
        )

        if proven:
            assert done.stdout.split() == ["signalled"]
            assert victim.wait(timeout=5) == -signal.SIGTERM
        else:
            assert done.stdout.split() == []
            assert victim.poll() is None
    finally:
        victim.kill()
        victim.wait()


@pytest.mark.parametrize("shell", _shells(), ids=os.path.basename)
@pytest.mark.parametrize(
    ("in_the_grace", "killed"),
    [("", True), ("reused", False), ("gone", True)],
    ids=["same-leader", "id-reused", "leader-gone"],
)
def test_the_grace_kills_only_a_group_still_led_by_the_process_it_found(
    shell, in_the_grace, killed, tmp_path
):
    """A group that ends in the grace may give its id to another before the
    KILL, which must then leave it alone. One whose leader is gone while the
    group lives is still the stopped command's members, and goes."""
    proc = tmp_path / "proc"
    (proc / "1").mkdir(parents=True)
    (proc / "1" / "stat").write_text(_stat_line(1, 100, pgrp=1, state="S"))
    group = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        leader = proc / str(group.pid)
        leader.mkdir()
        (leader / "stat").write_text(_stat_line(group.pid, 300, pgrp=group.pid, state="S"))
        # The grace's first tick is where the pid changes hands.
        change = {
            "": ":",
            "reused": f"echo '{_stat_line(group.pid, 999, pgrp=group.pid, state='S').strip()}' > {leader}/stat",
            "gone": f"rm -r {leader}",
        }[in_the_grace]
        script = (
            '[ -z "${ZSH_VERSION-}" ] || emulate sh\n'
            + PROC_SH.replace("/proc/", f"{proc}/")
            + f'sleep() {{ [ -n "${{T-}}" ] || {{ T=1; {change}; }}; command sleep "$@"; }}\n'
            + f"pgrace 3 {group.pid}\n"
        )

        subprocess.run([shell, "-c", script], env={"PATH": _PATH}, timeout=30, check=True)

        if killed:
            assert group.wait(timeout=5) == -signal.SIGKILL
        else:
            assert group.poll() is None
    finally:
        group.kill()
        group.wait()


@pytest.mark.parametrize("shell", _shells(), ids=os.path.basename)
def test_a_time_zone_change_leaves_a_recorded_process_its_own(shell, tmp_path):
    """With no /proc the start is ps's, which prints local time: a Mac that
    travels must not read every process it recorded as gone."""
    no_proc = f"{tmp_path}/no-proc/"
    victim = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        record = tmp_path / "pid"

        def run(script: str, tz: str) -> str:
            return subprocess.run(
                [shell, "-c", '[ -z "${ZSH_VERSION-}" ] || emulate sh\n' + script],
                capture_output=True,
                text=True,
                env={"PATH": _PATH, "TZ": tz},
                timeout=30,
            ).stdout.strip()

        start = run(ps_start_sh(str(victim.pid)), "America/New_York")
        record.write_text(f"{victim.pid} {start}\n")
        check = PROC_SH.replace("/proc/", no_proc) + f"plive {record} && echo live"

        assert run(check, "Europe/Berlin") == "live"
    finally:
        victim.kill()
        victim.wait()


@pytest.mark.parametrize("shell", _shells(), ids=os.path.basename)
def test_an_older_local_time_record_is_never_matched_in_utc(shell, tmp_path):
    """A record from before starts were kept in UTC is compared in local time
    alone: a reused pid whose UTC start reads as that local time is not the
    process recorded."""
    no_proc = f"{tmp_path}/no-proc/"
    victim = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:

        def run(script: str, tz: str) -> str:
            return subprocess.run(
                [shell, "-c", '[ -z "${ZSH_VERSION-}" ] || emulate sh\n' + script],
                capture_output=True,
                text=True,
                env={"PATH": _PATH, "TZ": tz},
                timeout=30,
            ).stdout.strip()

        utc = run(ps_start_sh(str(victim.pid)), "Asia/Tokyo")
        record = tmp_path / "pid"
        # Untagged, as a local-time record is, but reading as the UTC start.
        record.write_text(f"{victim.pid} {utc.removesuffix('Z')}\n")
        check = PROC_SH.replace("/proc/", no_proc) + f"plive {record} && echo live"

        assert run(check, "Asia/Tokyo") == ""
        record.write_text(f"{victim.pid} {utc}\n")
        assert run(check, "Asia/Tokyo") == "live"
    finally:
        victim.kill()
        victim.wait()
