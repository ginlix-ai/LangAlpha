"""Per-path write serialisation and read-back verification in ``sandbox/files.py``.

One sandbox already serves concurrent threads and subagent fan-out, and after the
computer/workspace split several workspaces share one computer, so two edits of the
same file can interleave. These tests run real concurrency against an in-memory
runtime: every operation has an await point, which is what lets an unlocked
read-modify-write lose an update.
"""

import asyncio
import base64
import shlex
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox import path_locks as path_locks_module
from ptc_agent.core.sandbox.files import SandboxWriteVerificationError
from ptc_agent.core.sandbox.path_locks import _PathLockRegistry
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    SandboxFailureKind,
    SandboxProvider,
)

WORK_DIR = "/home/workspace"


def _make_config() -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(working_directory=WORK_DIR),
    )


class _FakeRuntime:
    """In-memory sandbox filesystem that yields the event loop on every op.

    The await points are the whole point: without one between a read and its
    write, a single-threaded loop never interleaves two edits and the lost-update
    the lock exists to prevent cannot be reproduced.
    """

    def __init__(self, files: dict[str, bytes] | None = None, delay: float = 0.0):
        self.files: dict[str, bytes] = dict(files or {})
        self.id = "fake-runtime"
        self.working_dir = WORK_DIR
        self.uploads: list[tuple[str, bytes]] = []
        self.exec_calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self._delay = delay

    def _resolve(self, path: str) -> str:
        """Relative paths resolve against the working dir, as a real sandbox does."""
        return path if path.startswith("/") else f"{self.working_dir}/{path}"

    async def download_file(self, path: str) -> bytes:
        await asyncio.sleep(self._delay)
        resolved = self._resolve(path)
        if resolved not in self.files:
            raise FileNotFoundError(path)
        return self.files[resolved]

    async def upload_file(self, content: bytes, path: str) -> None:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self._delay)
            self._store(self._resolve(path), content)
            self.uploads.append((path, content))
        finally:
            self.in_flight -= 1

    async def exec(self, command: str, timeout: int | None = None) -> ExecResult:
        self.exec_calls += 1
        if "__LANGALPHA_TEXT_PAYLOAD__" in command:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                await asyncio.sleep(self._delay)
                lines = command.splitlines()
                target_word = shlex.split(lines[1])[0]
                target = target_word.split("=", 1)[1]
                start = next(
                    index
                    for index, line in enumerate(lines)
                    if line.startswith("base64 -d")
                )
                end = lines.index("__LANGALPHA_TEXT_PAYLOAD__", start + 1)
                content = base64.b64decode("".join(lines[start + 1 : end]))
                resolved = self._resolve(target)
                self._store(resolved, content)
                self.uploads.append((target, content))
                return ExecResult(
                    stdout=f"{len(self.files[resolved])}\n",
                    stderr="",
                    exit_code=0,
                )
            finally:
                self.in_flight -= 1

        await asyncio.sleep(0)
        resolved = self._resolve(command.split("<", 1)[1].strip().strip("'\""))
        if resolved not in self.files:
            return ExecResult(stdout="", stderr="no such file", exit_code=1)
        return ExecResult(
            stdout=f"{len(self.files[resolved])}\n", stderr="", exit_code=0
        )

    def _store(self, path: str, content: bytes) -> None:
        self.files[path] = content


class _TruncatingRuntime(_FakeRuntime):
    """Drops a byte off the first ``corrupt_writes`` uploads, as a dropped write would."""

    def __init__(self, corrupt_writes: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.corrupt_writes = corrupt_writes

    def _store(self, path: str, content: bytes) -> None:
        if self.corrupt_writes > 0:
            self.corrupt_writes -= 1
            content = content[:-1]
        self.files[path] = content


def _make_sandbox(runtime: _FakeRuntime):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    provider = AsyncMock(spec=SandboxProvider)
    provider.is_transient_error = MagicMock(return_value=False)
    provider.classify_error = MagicMock(
        side_effect=lambda exc: (
            SandboxFailureKind.PATH_ABSENT
            if isinstance(exc, FileNotFoundError)
            else SandboxFailureKind.TRANSIENT
        )
    )
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider", return_value=provider
    ):
        sandbox = PTCSandbox(config=_make_config())
    sandbox.runtime = runtime
    sandbox.sandbox_id = "sbx-1"
    return sandbox


@pytest.fixture(autouse=True)
def _isolated_registry(monkeypatch):
    """Each test gets its own registry so bounds and eviction stay observable."""
    registry = _PathLockRegistry()
    monkeypatch.setattr(path_locks_module, "_PATH_LOCKS", registry)
    return registry


@pytest.mark.asyncio
async def test_concurrent_edits_of_one_file_all_land():
    runtime = _FakeRuntime(files={f"{WORK_DIR}/notes.md": b"END\n"}, delay=0.001)
    sandbox = _make_sandbox(runtime)

    results = await asyncio.wait_for(
        asyncio.gather(
            *[
                sandbox.aedit_file_text("notes.md", "END", f"line{i}\nEND")
                for i in range(8)
            ]
        ),
        timeout=10,
    )

    assert all(r["success"] for r in results)
    final = runtime.files[f"{WORK_DIR}/notes.md"].decode()
    assert [line for line in final.splitlines() if line != "END"] != []
    for i in range(8):
        assert f"line{i}" in final, f"edit {i} was clobbered: {final!r}"


@pytest.mark.asyncio
async def test_concurrent_whole_file_writes_never_overlap():
    runtime = _FakeRuntime(delay=0.02)
    sandbox = _make_sandbox(runtime)

    await asyncio.wait_for(
        asyncio.gather(*[sandbox.awrite_file_text("a.txt", str(i)) for i in range(4)]),
        timeout=10,
    )

    assert runtime.max_in_flight == 1
    assert runtime.exec_calls == 4


@pytest.mark.asyncio
async def test_writes_to_different_paths_do_not_serialise():
    runtime = _FakeRuntime(delay=0.02)
    sandbox = _make_sandbox(runtime)

    await asyncio.wait_for(
        asyncio.gather(
            sandbox.awrite_file_text("a.txt", "a"),
            sandbox.awrite_file_text("b.txt", "b"),
        ),
        timeout=10,
    )

    assert runtime.max_in_flight == 2


@pytest.mark.asyncio
async def test_edits_of_different_paths_do_not_serialise():
    runtime = _FakeRuntime(
        files={f"{WORK_DIR}/a.txt": b"x", f"{WORK_DIR}/b.txt": b"x"}, delay=0.02
    )
    sandbox = _make_sandbox(runtime)

    await asyncio.wait_for(
        asyncio.gather(
            sandbox.aedit_file_text("a.txt", "x", "ax"),
            sandbox.aedit_file_text("b.txt", "x", "bx"),
        ),
        timeout=10,
    )

    assert runtime.max_in_flight == 2


@pytest.mark.asyncio
async def test_write_that_does_not_read_back_is_rewritten_once():
    runtime = _TruncatingRuntime(corrupt_writes=1)
    sandbox = _make_sandbox(runtime)

    assert await sandbox.awrite_file_text("report.md", "hello") is True
    assert len(runtime.uploads) == 2
    assert runtime.exec_calls == 2
    assert runtime.files[f"{WORK_DIR}/report.md"] == b"hello"


@pytest.mark.asyncio
async def test_write_that_never_reads_back_raises_naming_the_path():
    runtime = _TruncatingRuntime(corrupt_writes=99)
    sandbox = _make_sandbox(runtime)

    with pytest.raises(SandboxWriteVerificationError) as excinfo:
        await sandbox.awrite_file_text("report.md", "hello")

    assert f"{WORK_DIR}/report.md" in str(excinfo.value)
    assert len(runtime.uploads) == 2


@pytest.mark.asyncio
async def test_edit_propagates_a_verification_failure():
    runtime = _TruncatingRuntime(corrupt_writes=99, files={f"{WORK_DIR}/a.txt": b"x"})
    sandbox = _make_sandbox(runtime)

    with pytest.raises(SandboxWriteVerificationError):
        await sandbox.aedit_file_text("a.txt", "x", "y")


@pytest.mark.asyncio
async def test_byte_upload_does_not_read_back_by_default():
    # Bulk restore uploads a whole workspace through this path; it pays the lock,
    # never a second transfer per file.
    runtime = _TruncatingRuntime(corrupt_writes=99)
    sandbox = _make_sandbox(runtime)

    assert await sandbox.aupload_file_bytes("bulk.bin", b"payload") is True
    assert len(runtime.uploads) == 1


@pytest.mark.asyncio
async def test_small_text_write_uses_one_exec_request():
    runtime = _FakeRuntime()
    sandbox = _make_sandbox(runtime)

    assert await sandbox.awrite_file_text("report.md", "hello") is True

    assert runtime.exec_calls == 1
    assert runtime.uploads == [(f"{WORK_DIR}/report.md", b"hello")]


@pytest.mark.asyncio
async def test_large_text_write_keeps_upload_and_size_verification():
    runtime = _FakeRuntime()
    sandbox = _make_sandbox(runtime)
    content = "x" * (256 * 1024)

    assert await sandbox.awrite_file_text("report.md", content) is True

    assert runtime.exec_calls == 1
    assert len(runtime.uploads) == 1


@pytest.mark.asyncio
async def test_a_cancelled_waiter_releases_its_claim(_isolated_registry):
    # Cancelling a turn mid-write is routine; a waiter that leaves its claim
    # behind pins the entry in the registry forever.
    runtime = _FakeRuntime(delay=0.05)
    sandbox = _make_sandbox(runtime)

    holder = asyncio.create_task(sandbox.awrite_file_text("a.txt", "first"))
    await asyncio.sleep(0.01)
    waiter = asyncio.create_task(sandbox.awrite_file_text("a.txt", "second"))
    await asyncio.sleep(0.01)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await asyncio.wait_for(holder, timeout=10)

    entry = _isolated_registry.checkout(("sbx-1", f"{WORK_DIR}/a.txt"))
    try:
        assert entry.users == 1
    finally:
        _isolated_registry.checkin(entry)


@pytest.mark.asyncio
async def test_idle_locks_are_evicted_at_the_bound(monkeypatch):
    registry = _PathLockRegistry(capacity=4)
    monkeypatch.setattr(path_locks_module, "_PATH_LOCKS", registry)
    sandbox = _make_sandbox(_FakeRuntime())

    for i in range(20):
        await sandbox.awrite_file_text(f"f{i}.txt", "x")

    assert len(registry) == 4


@pytest.mark.asyncio
async def test_a_lock_in_use_is_never_evicted():
    registry = _PathLockRegistry(capacity=2)
    key = ("sbx-1", f"{WORK_DIR}/held.txt")
    held = registry.checkout(key)
    await held.lock.acquire()
    try:
        for i in range(20):
            registry.checkin(registry.checkout(("sbx-1", f"{WORK_DIR}/f{i}.txt")))
        assert registry.checkout(key) is held
        registry.checkin(held)
    finally:
        held.lock.release()
        registry.checkin(held)

    assert len(registry) <= 2


@pytest.mark.asyncio
async def test_two_sandboxes_sharing_a_path_do_not_serialise():
    # The key is (sandbox id, absolute path): two computers holding the same
    # relative path are unrelated files.
    runtime_a = _FakeRuntime(delay=0.02)
    runtime_b = _FakeRuntime(delay=0.02)
    sandbox_a = _make_sandbox(runtime_a)
    sandbox_b = _make_sandbox(runtime_b)
    sandbox_b.sandbox_id = "sbx-2"

    start = asyncio.get_running_loop().time()
    await asyncio.wait_for(
        asyncio.gather(
            sandbox_a.awrite_file_text("a.txt", "a"),
            sandbox_b.awrite_file_text("a.txt", "b"),
        ),
        timeout=10,
    )

    assert asyncio.get_running_loop().time() - start < 0.08
    assert runtime_a.max_in_flight == 1
    assert runtime_b.max_in_flight == 1


# ---------------------------------------------------------------------------
# The cross-process lease hook
# ---------------------------------------------------------------------------


class _LeaseLog:
    """A fake cross-process lease: records every take, serialises across registries."""

    def __init__(self):
        self.taken: list[tuple[str, str]] = []
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}

    def __call__(self, scope, path):
        return self._hold(scope, path)

    @asynccontextmanager
    async def _hold(self, scope, path):
        self.taken.append((scope, path))
        lock = self._locks.setdefault((scope, path), asyncio.Lock())
        async with lock:
            yield


@pytest.fixture
def _lease():
    lease = _LeaseLog()
    path_locks_module.install_cross_process_lock(lease)
    try:
        yield lease
    finally:
        path_locks_module.install_cross_process_lock(None)


@pytest.mark.asyncio
async def test_the_lease_is_taken_once_by_the_outermost_holder(
    _isolated_registry, _lease
):
    async with path_locks_module.path_write_lock("sb", "/a"):
        # The nested (re-entrant) acquire must not wait on its own lease.
        async with path_locks_module.path_write_lock("sb", "/a"):
            pass
    assert _lease.taken == [("sb", "/a")]


@pytest.mark.asyncio
async def test_without_a_lease_installed_the_lock_is_process_local(_isolated_registry):
    path_locks_module.install_cross_process_lock(None)
    async with path_locks_module.path_write_lock("sb", "/a"):
        pass


@pytest.mark.asyncio
async def test_two_registries_sharing_a_lease_serialise(monkeypatch, _lease):
    """Two workers are two registries; the lease is what keeps them apart."""
    order: list[str] = []

    async def writer(name, registry):
        monkeypatch.setattr(path_locks_module, "_PATH_LOCKS", registry)
        async with path_locks_module.path_write_lock("sb", "/a"):
            order.append(f"{name}:in")
            await asyncio.sleep(0.01)
            order.append(f"{name}:out")

    async def run():
        # Each writer swaps in its own registry before it acquires, so the two
        # never see each other's in-process lock.
        t1 = asyncio.create_task(writer("w1", _PathLockRegistry()))
        await asyncio.sleep(0)
        t2 = asyncio.create_task(writer("w2", _PathLockRegistry()))
        await asyncio.gather(t1, t2)

    await run()
    assert order == ["w1:in", "w1:out", "w2:in", "w2:out"]
