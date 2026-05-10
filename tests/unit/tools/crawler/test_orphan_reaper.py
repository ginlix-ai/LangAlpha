"""Tests for SafeCrawlerWrapper._trigger_browser_reset.

The reaper walks /proc looking for orphaned (ppid=1) Chrome processes that
have been orphaned for > 5 seconds, and SIGKILLs them. This is the Tier 3b
safety net for Tier 1's tini reaper — when the crawler circuit breaker trips
we want to free PID slots and RAM immediately instead of waiting for tini.

We exercise the reaper with a fake /proc fixture so tests run on macOS too.
"""

from __future__ import annotations

import signal
from unittest.mock import patch

import pytest

from src.tools.crawler.safe_wrapper import SafeCrawlerWrapper


def _write_stat(proc_dir, pid: int, comm: str, ppid: int, start_ticks: int):
    """Write a minimal /proc/{pid}/stat in the expected format.

    Real stat format: pid (comm) state ppid pgrp ... starttime ...
    Fields are 1-indexed; our parser reads ppid at index 4 and starttime at 22.
    We fill enough fields to satisfy the slicing.
    """
    pid_dir = proc_dir / str(pid)
    pid_dir.mkdir()
    # Build a stat line with 52 fields (well past field 22)
    fields = [str(pid), f"({comm})", "S", str(ppid)]
    fields += ["0"] * 17            # pad up to field 21
    fields.append(str(start_ticks)) # field 22 (starttime)
    fields += ["0"] * 30
    (pid_dir / "stat").write_text(" ".join(fields))


@pytest.fixture
def fake_proc(tmp_path):
    """Build a fake /proc tree with a controllable uptime and PID 1 = tini."""
    proc = tmp_path / "proc"
    proc.mkdir()
    # uptime: 100 seconds (arbitrary, start_ticks scaled against this)
    (proc / "uptime").write_text("100.0 90.0\n")
    # PID 1 comm must be an init process or the reaper's safety guard aborts.
    pid1 = proc / "1"
    pid1.mkdir()
    (pid1 / "comm").write_text("tini\n")
    return proc


@pytest.fixture
def patched_proc(fake_proc, monkeypatch):
    """Wire safe_wrapper.Path to our fake /proc and return the fake root."""
    monkeypatch.setattr(
        "src.tools.crawler.safe_wrapper.Path",
        lambda p: (fake_proc if p == "/proc" else fake_proc / p.lstrip("/")),
    )
    return fake_proc


def _make_wrapper() -> SafeCrawlerWrapper:
    return SafeCrawlerWrapper(
        max_queue_size=10,
        default_timeout=5.0,
        circuit_failure_threshold=3,
        circuit_recovery_timeout=60.0,
        circuit_success_threshold=2,
        http_concurrency=20,
        browser_concurrency=6,
    )


class TestOrphanReaper:

    @pytest.mark.asyncio
    async def test_kills_ppid1_chrome(self, patched_proc):
        """An orphaned Chrome process older than 5s gets SIGKILL."""
        # Chrome started at tick 5000, on a 100-tick-per-sec clock → 50s old
        _write_stat(patched_proc, pid=1000, comm="chrome", ppid=1, start_ticks=5000)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert (1000, signal.SIGKILL) in kills

    @pytest.mark.asyncio
    async def test_kills_camoufox_orphan(self, patched_proc):
        """Tier 3 Camoufox (Firefox-based) orphans are also reaped."""
        _write_stat(patched_proc, pid=1100, comm="camoufox", ppid=1, start_ticks=5000)
        _write_stat(patched_proc, pid=1101, comm="firefox", ppid=1, start_ticks=5000)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert (1100, signal.SIGKILL) in kills
        assert (1101, signal.SIGKILL) in kills

    @pytest.mark.asyncio
    async def test_kills_chrome_with_spaces_in_comm(self, patched_proc):
        """comm can contain spaces inside parens — the rsplit(')', 1) handles it."""
        pid_dir = patched_proc / "1200"
        pid_dir.mkdir()
        # Write a stat line with spaces inside the comm parens
        fields = ["1200", "(chrome renderer)", "S", "1"]
        fields += ["0"] * 17
        fields.append("5000")  # field 22
        fields += ["0"] * 30
        (pid_dir / "stat").write_text(" ".join(fields))

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert (1200, signal.SIGKILL) in kills

    @pytest.mark.asyncio
    async def test_skips_young_chrome(self, patched_proc):
        """A Chrome process < 5s old is NOT killed (fork/exec window)."""
        # start_ticks=9900 → uptime 100 - 9900/100 = 1.0s old, under 5s
        _write_stat(patched_proc, pid=1001, comm="chrome", ppid=1, start_ticks=9900)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert kills == []

    @pytest.mark.asyncio
    async def test_skips_non_chrome(self, patched_proc):
        """Non-browser orphans (e.g. uv, python3) are not killed."""
        _write_stat(patched_proc, pid=1002, comm="python3", ppid=1, start_ticks=0)
        _write_stat(patched_proc, pid=1003, comm="uv", ppid=1, start_ticks=0)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert kills == []

    @pytest.mark.asyncio
    async def test_skips_non_orphan(self, patched_proc):
        """Chrome with non-1 ppid (still has a real parent) is NOT killed."""
        _write_stat(patched_proc, pid=1004, comm="chrome", ppid=500, start_ticks=0)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert kills == []

    @pytest.mark.asyncio
    async def test_permission_error_is_not_fatal(self, patched_proc):
        """PermissionError on os.kill is logged and skipped, not raised."""
        _write_stat(patched_proc, pid=1005, comm="chrome_crashpad", ppid=1, start_ticks=0)

        def raise_perm(*_, **__):
            raise PermissionError("no access")

        with patch("os.kill", side_effect=raise_perm), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            # Must not raise
            await wrapper._trigger_browser_reset()

    @pytest.mark.asyncio
    async def test_proc_stat_missing_mid_scan(self, patched_proc):
        """A pid dir with no stat file (race with process exit) is skipped."""
        (patched_proc / "3000").mkdir()  # pid dir with no stat file

        with patch("os.kill") as mock_kill, patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()  # must not raise

        mock_kill.assert_not_called()

    @pytest.mark.asyncio
    async def test_aborts_when_pid1_is_not_init(self, patched_proc):
        """CRITICAL: reaper must refuse to run if PID 1 is not tini.

        Without tini as PID 1, ppid==1 means direct child of our python worker
        — i.e. LIVE browsers of in-flight workflows. Reaping them would kill
        healthy crawls. This test locks in the safety short-circuit.
        """
        (patched_proc / "1" / "comm").write_text("python\n")
        _write_stat(patched_proc, pid=5000, comm="chrome", ppid=1, start_ticks=5000)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert kills == []  # safety guard engaged — no kills

    @pytest.mark.asyncio
    @pytest.mark.parametrize("init_comm", ["tini", "docker-init", "catatonit", "dumb-init"])
    async def test_accepts_all_supported_init_processes(self, patched_proc, init_comm):
        """REGRESSION: docker-init is what `init: true` actually launches.

        Observed in staging: Docker Desktop and Docker CE set PID 1 to
        `docker-init` (their bundled tini wrapper), not bare `tini`. The
        allowlist must include it or the reaper refuses to run on every
        standard Docker deployment.
        """
        (patched_proc / "1" / "comm").write_text(f"{init_comm}\n")
        _write_stat(patched_proc, pid=6000, comm="chrome", ppid=1, start_ticks=5000)

        kills = []
        with patch("os.kill", side_effect=lambda pid, sig: kills.append((pid, sig))), \
             patch("os.sysconf", return_value=100):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()

        assert (6000, signal.SIGKILL) in kills

    @pytest.mark.asyncio
    async def test_missing_proc_is_not_fatal(self, tmp_path):
        """On macOS (no /proc) the reaper logs debug and returns silently."""
        nonexistent = tmp_path / "proc"
        # Do NOT create it
        with patch(
            "src.tools.crawler.safe_wrapper.Path",
            lambda p: (nonexistent if p == "/proc" else nonexistent / p.lstrip("/")),
        ):
            wrapper = _make_wrapper()
            await wrapper._trigger_browser_reset()  # Must not raise
