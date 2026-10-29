"""A computer's stop ends what its commands left running, and only that.

Run here against real processes, since the script's whole job is matching the
machine's process table. The generated client starts the supervisor in a
session of its own, which no exec record names, and that one outlived every
stop and delete of its computer.
"""

import os
import shlex
import subprocess
import sys
import time

import pytest

from ptc_agent.core.sandbox.providers._ssh_shell import framed_script, stop_script

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")

# Stands in for the supervisor: an MCP server in a group of its own, in the
# supervisor's session, as the daemon spawns them.
_SUPERVISOR = """\
import subprocess, sys, time
server = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(60)"], process_group=0
)
print(server.pid, flush=True)
time.sleep(60)
"""


def _start_supervisor(src, root) -> tuple[subprocess.Popen, int]:
    proc = subprocess.Popen(
        [sys.executable, "-m", "supervisor", str(root)],
        cwd=src,
        env={**os.environ, "PYTHONPATH": str(src)},
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    return proc, int(proc.stdout.readline())


def _gone(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def test_a_supervisor_in_its_own_session_ends_with_its_computer(tmp_path):
    src = tmp_path / "src"
    (src / "supervisor").mkdir(parents=True)
    (src / "supervisor" / "__main__.py").write_text(_SUPERVISOR)
    cdir = tmp_path / "computers" / "c1"
    cdir.mkdir(parents=True)
    root = tmp_path / "root"
    other_root = tmp_path / "root-other"

    supervisor, server = _start_supervisor(src, root)
    neighbour, neighbours_server = _start_supervisor(src, other_root)
    try:
        subprocess.run(
            [
                "bash",
                "-c",
                stop_script(str(cdir), root=False, computer_root=str(root)),
            ],
            check=True,
            timeout=30,
        )

        assert supervisor.wait(timeout=5) is not None
        assert _gone(server)
        assert neighbour.poll() is None
        assert not _gone(neighbours_server, timeout=0)
        assert (cdir / "state").read_text().strip() == "stopped"
    finally:
        for proc, child in ((supervisor, server), (neighbour, neighbours_server)):
            for pid in (child, proc.pid):
                try:
                    os.kill(pid, 9)
                except ProcessLookupError:
                    pass
            proc.wait(timeout=5)
            proc.stdout.close()


# A process that outlives the shell starting it. Not one of the system's own
# binaries, whose environment macOS keeps from ps.
_LINGER = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(60)' >/dev/null 2>&1 &"


def _stop(cdir) -> None:
    subprocess.run(
        ["bash", "-c", stop_script(str(cdir), root=False)], check=True, timeout=30
    )


def _kill(pid: int) -> None:
    try:
        os.kill(pid, 9)
    except ProcessLookupError:
        pass


def test_what_a_finished_command_left_running_ends_with_the_stop(tmp_path):
    """Its leader gone, the command's group is known by its mark, which what
    the command started inherits."""
    cdir = tmp_path / "c1"
    (cdir / "execs").mkdir(parents=True)
    left = tmp_path / "left.pid"
    script = framed_script(
        f"{_LINGER} echo $! > {shlex.quote(str(left))}",
        computer_dir=str(cdir),
        cwd=None,
        computer=False,
    )
    subprocess.run(["bash", "-c", script], check=True, capture_output=True, timeout=30)
    survivor = int(left.read_text())
    try:
        # The record stays while the group has a member.
        assert [p.name for p in (cdir / "execs").iterdir()] == [
            str(os.getpgid(survivor))
        ]

        _stop(cdir)

        assert _gone(survivor)
    finally:
        _kill(survivor)


def test_a_group_that_only_took_a_records_id_is_left_alone(tmp_path):
    """A record outlives its group when the shell running the command is cut
    off. Once the group is empty its id can go to someone else's group, whose
    leader may then be gone too."""
    cdir = tmp_path / "c1"
    (cdir / "execs").mkdir(parents=True)
    other = subprocess.run(
        ["sh", "-c", f"{_LINGER} echo $!"],
        capture_output=True,
        text=True,
        start_new_session=True,
        env={"PATH": os.environ["PATH"]},
        timeout=30,
    )
    member = int(other.stdout)
    pgid = os.getpgid(member)
    try:
        (cdir / "execs" / str(pgid)).write_text(f"{pgid} 12345\n")

        _stop(cdir)

        assert not _gone(member, timeout=1.5)
    finally:
        _kill(member)



def _stat_line(pid: int, start: int, state: str) -> str:
    """A /proc/<pid>/stat line in ``state`` for a group leader, whose start
    time (field 22) is ``start``."""
    return f"{pid} (a) b (c) {state} 0 {pid} {' '.join(['0'] * 16)} {start} 0 0\n"


def test_a_pid_a_zombie_took_between_the_rounds_is_not_trusted(tmp_path):
    """A group proven on the TERM round is trusted on the KILL round only
    while its pid names no process at all: a zombie there now is not the
    leader recorded. Over a /proc moved into a temp dir and rewritten between
    the rounds, with a real process that ignores TERM standing in for the
    group."""
    proc = tmp_path / "proc"
    (proc / "1").mkdir(parents=True)
    (proc / "1" / "stat").write_text(_stat_line(1, 100, "S"))
    cdir = tmp_path / "c1"
    (cdir / "execs").mkdir(parents=True)
    victim = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "print(flush=True); time.sleep(30)",
        ],
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        victim.stdout.readline()
        stat = proc / str(victim.pid) / "stat"
        stat.parent.mkdir()
        stat.write_text(_stat_line(victim.pid, 300, "S"))
        (cdir / "execs" / str(victim.pid)).write_text(f"{victim.pid} 300\n")
        rounds = "if sig TERM; then sleep 1; sig KILL; fi"
        script = stop_script(str(cdir), root=False).replace("/proc/", f"{proc}/")
        assert rounds in script
        reused = shlex.quote(_stat_line(victim.pid, 999, "Z"))
        script = script.replace(
            rounds, f"if sig TERM; then printf %s {reused} > {stat}; sig KILL; fi"
        )

        subprocess.run(["bash", "-c", script], check=True, timeout=30)

        assert victim.poll() is None
    finally:
        _kill(victim.pid)
        victim.wait()
        victim.stdout.close()
