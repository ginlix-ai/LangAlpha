"""Contract tests for SandboxRuntime.open_stream() -- one process streamed both ways."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import random
import shlex
import time

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# A number no other process on the machine sleeps for, so the leftover check
# sees only this run's processes.
_SLEEP_S = random.randint(600_000, 699_999)
_LOOP = f"trap '' TERM; echo ready; while :; do sleep 1; done; : {_SLEEP_S}"
_READY = b"ready\n"

# Every provider reports a process a signal ended the way its shell does.
_KILLED = 137
_TERMINATED = 143

# Allowances the StreamProcess contract makes (it is faithful to lines, not
# bytes), by the providers that take them. Daytona cannot close a process's
# stdin, and its log ends output that lacks a final newline with one.
_NO_STDIN_EOF = {"daytona"}
_ADDS_FINAL_NEWLINE = {"daytona"}


async def _read(reader: asyncio.StreamReader, size: int, timeout: float = 60) -> bytes:
    return await asyncio.wait_for(reader.readexactly(size), timeout)


async def _drain(reader: asyncio.StreamReader, timeout: float = 30) -> bytes:
    return await asyncio.wait_for(reader.read(), timeout)


async def _processes(runtime, pattern: str) -> str:
    """How many command lines match *pattern*, from a ``ps`` that must itself
    succeed: one that failed would list nothing and read as none left."""
    result = await runtime.exec(
        "listed=$(ps -eo args) || exit $?; "
        f'printf "%s\n" "$listed" | grep -c {shlex.quote(pattern)} || test $? -eq 1'
    )
    assert result.exit_code == 0, f"the process check failed: {result.stdout!r}"
    return result.stdout.strip()


async def _until_running(runtime, pattern: str, timeout: float = 30) -> None:
    async with asyncio.timeout(timeout):
        while await _processes(runtime, pattern) == "0":
            await asyncio.sleep(0.2)


@pytest_asyncio.fixture(scope="class", loop_scope="class")
async def stream_runtime(shared_runtime, provider_name):
    if provider_name == "ssh":
        pytest.skip("an SSH computer streams over its own Link")
    if (await shared_runtime.exec("command -v bash")).exit_code != 0:
        pytest.fail(
            f"{provider_name}'s sandbox image has no bash, which open_stream needs to start a "
            "process as a group of its own. Run this contract against the image the provider ships."
        )
    return shared_runtime


@pytest.mark.asyncio(loop_scope="class")
class TestRuntimeStream:
    """SandboxRuntime.open_stream() -- stdin and stdout for as long as the process runs."""

    async def test_echo_through_cat(self, stream_runtime, timed):
        stream = await stream_runtime.open_stream("cat")
        try:
            for i in range(20):
                line = (json.dumps({"type": "ping", "i": i, "pad": "x" * 80}) + "\n").encode()
                async with timed("stream", "small_line_rtt"):
                    await stream.write(line)
                    assert await _read(stream.stdout, len(line)) == line

            rnd = random.Random(7)
            body = "".join(rnd.choice("abcdefé中文\"\\{}") for _ in range(3 * 1024 * 1024))
            big = (json.dumps({"type": "big", "body": body}, ensure_ascii=False) + "\n").encode()
            assert len(big) > 4 * 1024 * 1024

            async def send() -> None:
                # Chunks end mid-character, as a caller's writes may.
                for start in range(0, len(big), 65_537):
                    await stream.write(big[start : start + 65_537])

            async with timed("stream", "4mb_multibyte_line"):
                got, _ = await asyncio.gather(
                    _read(stream.stdout, len(big), timeout=300), send()
                )
            assert hashlib.sha256(got).digest() == hashlib.sha256(big).digest()
            assert stream.returncode is None
        finally:
            await stream.close()

    async def test_write_eof_ends_cat(self, stream_runtime, provider_name, timed):
        stream = await stream_runtime.open_stream("cat")
        try:
            await stream.write(b"last\n")
            assert await _read(stream.stdout, 5) == b"last\n"
            async with timed("stream", "eof_to_exit"):
                closed = await stream.write_eof()
                if closed:
                    assert await asyncio.wait_for(stream.wait(), 30) == 0
            assert closed is (provider_name not in _NO_STDIN_EOF)
            if closed:
                assert stream.returncode == 0
                assert await _drain(stream.stdout) == b""
        finally:
            await stream.close()

    async def test_kill_ends_a_process_that_ignores_term(self, stream_runtime, timed):
        stream = await stream_runtime.open_stream(_LOOP)
        try:
            assert await _read(stream.stdout, len(_READY)) == _READY
            await stream.terminate()
            await asyncio.sleep(2)
            assert stream.returncode is None
            async with timed("stream", "kill_to_exit"):
                await stream.kill()
                code = await asyncio.wait_for(stream.wait(), 5)
            assert code == _KILLED
            await stream.kill()
            await stream.terminate()
        finally:
            await stream.close()

    async def test_terminate_ends_sleep(self, stream_runtime, timed):
        stream = await stream_runtime.open_stream(f"sleep {_SLEEP_S}")
        try:
            await asyncio.sleep(1)
            async with timed("stream", "terminate_to_exit"):
                await stream.terminate()
                code = await asyncio.wait_for(stream.wait(), 5)
            assert code == _TERMINATED
        finally:
            await stream.close()

    @pytest.mark.parametrize("end", ["terminate", "kill", "close"])
    @pytest.mark.parametrize("shape", ["{} | cat", "{}; echo done"])
    async def test_ending_reaches_every_process_it_started(self, stream_runtime, shape, end):
        sleeper = f"sleep {_SLEEP_S + random.randint(1, 99_999)}"
        stream = await stream_runtime.open_stream(shape.format(sleeper))
        try:
            await _until_running(stream_runtime, f"[s]{sleeper[1:]}")
            if end == "close":
                await stream.close()
            else:
                await getattr(stream, end)()
                want = _TERMINATED if end == "terminate" else _KILLED
                assert await asyncio.wait_for(stream.wait(), 5) == want
        finally:
            await stream.close()
        await asyncio.sleep(1)
        assert await _processes(stream_runtime, f"[s]{sleeper[1:]}") == "0"

    async def test_stderr_stays_separate(self, stream_runtime):
        stream = await stream_runtime.open_stream("echo out; echo err >&2; echo out2")
        try:
            assert await asyncio.wait_for(stream.wait(), 30) == 0
            assert await _drain(stream.stdout) == b"out\nout2\n"
            assert await _drain(stream.stderr) == b"err\n"
        finally:
            await stream.close()

    async def test_both_readers_reached_eof_when_wait_returns(self, stream_runtime):
        for _ in range(10):
            stream = await stream_runtime.open_stream("echo out; echo err >&2")
            try:
                assert await asyncio.wait_for(stream.wait(), 30) == 0
                assert await stream.stdout.read(64) == b"out\n" and stream.stdout.at_eof()
                assert await stream.stderr.read(64) == b"err\n" and stream.stderr.at_eof()
            finally:
                await stream.close()

    async def test_exit_status_and_after(self, stream_runtime):
        stream = await stream_runtime.open_stream("exit 3")
        assert await asyncio.wait_for(stream.wait(), 30) == 3
        assert stream.returncode == 3
        with pytest.raises(ConnectionError):
            await stream.write(b"too late\n")
        await stream.kill()
        await stream.terminate()
        await stream.close()
        await stream.close()
        assert stream.returncode == 3

    async def test_no_profile_runs_first(self, stream_runtime, provider_name):
        if provider_name == "memory":
            pytest.skip("the memory provider's shell is this machine's own")
        rc_files = "~/.bashrc ~/.profile ~/.bash_profile ~/.bash_login"
        marker = f"/tmp/stream-rc-{_SLEEP_S}"
        # Each rc file records the command line of every shell that runs it.
        line = f'echo "$BASH_EXECUTION_STRING" >> {marker}'
        try:
            # Each file's prior state is recorded before it is touched, as a
            # copy or as a note that it did not exist, so cleanup restores
            # exactly what setup changed, however far setup got.
            setup = await stream_runtime.exec(
                f"for f in {rc_files}; do "
                f'if [ -e "$f" ]; then cp -p "$f" "$f.stream-test" || exit 1; '
                f'else touch "$f.stream-test-absent" || exit 1; fi; '
                f'echo {shlex.quote(line)} >> "$f" || exit 1; done'
            )
            assert setup.exit_code == 0, f"rc file setup failed: {setup.stdout!r}"
            stream = await stream_runtime.open_stream(f": rc-probe-{_SLEEP_S}")
            try:
                assert await asyncio.wait_for(stream.wait(), 30) == 0
            finally:
                await stream.close()
            seen = await stream_runtime.exec(f"cat {marker} 2>/dev/null || true")
            assert f"rc-probe-{_SLEEP_S}" not in seen.stdout
        finally:
            await stream_runtime.exec(
                f"for f in {rc_files}; do "
                f'if [ -e "$f.stream-test" ]; then mv -f "$f.stream-test" "$f"; '
                f'elif [ -e "$f.stream-test-absent" ]; then rm -f "$f" "$f.stream-test-absent"; fi; '
                f"done; rm -f {marker}"
            )

    async def test_output_that_is_not_utf8_reads_as_replacement(self, stream_runtime):
        stream = await stream_runtime.open_stream(r"printf 'a\377b\n'")
        try:
            assert await asyncio.wait_for(stream.wait(), 30) == 0
            assert await _drain(stream.stdout) == "a\ufffdb\n".encode()
        finally:
            await stream.close()

    async def test_output_without_a_final_newline(self, stream_runtime, provider_name):
        stream = await stream_runtime.open_stream(r"printf 'a\nb'")
        try:
            assert await asyncio.wait_for(stream.wait(), 30) == 0
            want = b"a\nb\n" if provider_name in _ADDS_FINAL_NEWLINE else b"a\nb"
            assert await _drain(stream.stdout) == want
        finally:
            await stream.close()

    async def test_a_line_arrives_whole_however_it_was_split(self, stream_runtime):
        stream = await stream_runtime.open_stream("cat")
        try:
            line = '{"text": "中文é"}\n'.encode()
            for i in range(len(line)):
                await stream.write(line[i : i + 1])
            assert await _read(stream.stdout, len(line)) == line
        finally:
            await stream.close()

    async def test_close_kills_a_running_process(self, stream_runtime):
        stream = await stream_runtime.open_stream(_LOOP)
        assert await _read(stream.stdout, len(_READY)) == _READY
        started = time.monotonic()
        await stream.close()
        assert time.monotonic() - started < 15
        assert stream.returncode is not None

    async def test_a_cancelled_close_ends_readers_a_process_outside_it_holds_open(
        self, stream_runtime
    ):
        holder = f"sleep {_SLEEP_S + random.randint(100_000, 199_999)}"
        stream = await stream_runtime.open_stream(f"setsid {holder} & {_LOOP}")
        try:
            assert await _read(stream.stdout, len(_READY)) == _READY
            await _until_running(stream_runtime, f"^{holder}")
            close = asyncio.create_task(stream.close())
            await asyncio.sleep(0)
            close.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await close
            assert await _drain(stream.stdout) == b""
            await _drain(stream.stderr)
        finally:
            await stream.close()
            await stream_runtime.exec(f"pkill -f {shlex.quote(f'^{holder}')}")
        assert await _processes(stream_runtime, f"^{holder}") == "0"

    async def test_no_process_left_behind(self, stream_runtime):
        await asyncio.sleep(2)
        assert await _processes(stream_runtime, f"[s]leep {_SLEEP_S}\\|[:] {_SLEEP_S}") == "0"
