"""What the kernel suites share: the server (the fake, or a real one), the
daemon as the host drives it, programs run as the computer's user, and the
corpus. Runs inside the harness container as root.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass, field

KIT = os.path.dirname(os.path.abspath(__file__))
SRC = os.environ.get("DRIVE_SRC", "/opt/drive/livefs_runtime")
ROOT = "/home/workspace"
SHIPPED_PARENT = f"{ROOT}/_internal/src"
SHIPPED = f"{SHIPPED_PARENT}/livefs"
PRIVATE = "/var/lib/livefs/drive"
LINK = "/mnt/drive"
TOP = f"{LINK}/personal"
TOKEN = "lfs1.harness.secret"
USER = "workspace"
SPACE = "personal"

#: Daemon settings every start in this run adds (``--settings``).
EXTRA_SETTINGS: dict = {}


class Failure(Exception):
    """The mount or the server itself failed: the run cannot go on."""

    fatal = True


def log(*parts) -> None:
    print(*parts, flush=True)


def sh(cmd, check=False, **kw) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True, **kw)
    if check and r.returncode:
        raise Failure(f"{cmd} exited {r.returncode}: {r.stderr[-800:]}")
    return r


#: The mount under test, for programs run as the user to settle their saves.
MOUNTS: list = []


def _settled(r: subprocess.CompletedProcess, text: str, env: dict, settle: bool):
    if settle and MOUNTS and LINK in text:
        with contextlib.suppress(Failure):
            MOUNTS[0].run("drive-settle", "--call", env["LIVEFS_CALL_ID"])
    return r


def as_user(script: str, env: dict | None = None, check=True, cwd=None,
            settle: bool = True) -> subprocess.CompletedProcess:
    """A bash script run as the computer's user, as agent commands are: under
    a call id of its own, and when it names the Drive followed by
    ``drive-settle`` for that call, as the host does before the result.
    ``settle=False`` leaves its saves to the background."""
    env = {"LIVEFS_CALL_ID": "call-h-" + os.urandom(6).hex(), **(env or {})}
    r = sh(["runuser", "-u", USER, "--", "bash", "-c", script], check=check,
           env={**os.environ, "HOME": ROOT, **env}, cwd=cwd)
    return _settled(r, script, env, settle)


def user_python(code: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """``as_user`` for a Python program."""
    env = {"LIVEFS_CALL_ID": "call-h-" + os.urandom(6).hex(), **(env or {})}
    r = sh(["runuser", "-u", USER, "--", "python3", "-c", code], env={**os.environ, "HOME": ROOT, **env})
    return _settled(r, code, env, True)


# --- the server --------------------------------------------------------------

class FakeServer:
    """The fake endpoint, and its admin routes for changes made elsewhere."""

    def __init__(self, data: str, seed: str | None, port: int = 8790) -> None:
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        shutil.rmtree(data, ignore_errors=True)
        os.makedirs(data)
        cmd = [sys.executable, os.path.join(KIT, "fake_server.py"), "--port", str(port),
               "--token", TOKEN, "--data", data]
        if seed:
            cmd += ["--seed", seed]
        self.log = open(os.path.join(data, "server.log"), "w")
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 600
        while True:
            try:
                self.admin("GET", "stats")
                break
            except OSError:
                if self.proc.poll() is not None or time.monotonic() > deadline:
                    raise Failure("fake server did not start; see " + self.log.name) from None
                time.sleep(0.2)

    def admin(self, method: str, action: str, params: dict | None = None, body: bytes | None = None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        try:
            url = f"/admin/{action}?{urllib.parse.urlencode(params or {})}"
            conn.request(method, url, body=body or (b"" if method != "GET" else None))
            resp = conn.getresponse()
            data = resp.read()
        finally:
            conn.close()
        if resp.headers.get("Content-Type") == "application/json":
            return resp.status, json.loads(data)
        return resp.status, data

    def entry(self, path: str) -> dict | None:
        return self.admin("GET", "entry", {"path": path})[1]["entry"]

    def content(self, path: str) -> bytes | None:
        status, data = self.admin("GET", "content", {"path": path})
        return data if status == 200 else None

    def write(self, path: str, data: bytes) -> dict:
        return self.admin("POST", "write", {"path": path}, data)[1]

    def stop(self) -> None:
        self.proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            self.proc.wait(5)


class RealServer:
    """A running langalpha server in the fake's place: the daemon talks to its
    mount endpoint with a real computer's mount token, and changes made
    elsewhere go through the Drive's REST API. Only the suites that need no
    server-side knobs run against it (writers, feed)."""

    def __init__(self, base_url: str, token: str, rest: str, rest_token: str | None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        parts = urllib.parse.urlsplit(rest.rstrip("/"))
        self._host, self._port, self._prefix = parts.hostname, parts.port, parts.path
        self._rest_token = rest_token

    def _rest(self, method: str, action: str, params: dict | None = None,
              body: bytes | None = None, json_body: dict | None = None):
        conn = http.client.HTTPConnection(self._host, self._port, timeout=60)
        headers = {"Authorization": f"Bearer {self._rest_token}"} if self._rest_token else {}
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        try:
            url = f"{self._prefix}/{SPACE}/{action}?{urllib.parse.urlencode(params or {})}"
            conn.request(method, url, body=body if body is not None else (b"" if method != "GET" else None),
                         headers=headers)
            resp = conn.getresponse()
            data = resp.read()
        finally:
            conn.close()
        return resp.status, data

    def clear(self, *folders: str) -> None:
        """Trash what an earlier run left in these folders: the server keeps
        its files between runs, and a case finding its own names taken fails."""
        for folder in folders:
            self._rest("POST", "trash", json_body={"path": folder})

    def admin(self, method: str, action: str, params: dict | None = None, body: bytes | None = None):
        params = params or {}
        if action == "trash_list":
            status, data = self._rest("GET", "trash")
            items = json.loads(data).get("items", []) if status == 200 else []
            return status, [{"trash_id": i["trash_id"], "path": i["entry"]["path"]} for i in items]
        if action == "trash":
            status, data = self._rest("POST", "trash", json_body={"path": params["path"]})
            return status, json.loads(data or b"{}")
        if action == "move":
            status, data = self._rest("POST", "move", json_body={"from": params["from"], "to": params["to"]})
            return status, json.loads(data or b"{}")
        raise Failure(f"{action} has no counterpart on a real server")

    def entry(self, path: str) -> dict | None:
        status, data = self._rest("GET", "entry", {"path": path})
        return json.loads(data) if status == 200 else None

    def content(self, path: str) -> bytes | None:
        status, data = self._rest("GET", "content", {"path": path})
        return data if status == 200 else None

    def write(self, path: str, data: bytes) -> dict:
        return json.loads(self._rest("PUT", "content", {"path": path}, data)[1])

    def stop(self) -> None:
        pass



# --- the mount ---------------------------------------------------------------


class Mount:
    """The Drive daemon as the host drives it: ``boot`` then ``drive-start``."""

    def __init__(self, base_url: str, token: str = TOKEN) -> None:
        self.base_url = base_url
        self.token = token
        self.ship()

    def ship(self) -> None:
        shutil.rmtree(SHIPPED, ignore_errors=True)
        os.makedirs(SHIPPED_PARENT, exist_ok=True)
        shutil.copytree(SRC, SHIPPED, ignore=shutil.ignore_patterns("__pycache__"))
        sh(["chown", "-R", f"{USER}:{USER}", f"{ROOT}/_internal"])
        found = sh([sys.executable, "-I", "-c",
                    "import json,sys; sys.path.insert(0, sys.argv[1]);"
                    "from livefs import lifecycle;"
                    "print(json.dumps([lifecycle.code_version(), lifecycle.code_manifest()], sort_keys=True))",
                    SHIPPED_PARENT], check=True)
        self.code, manifest = json.loads(found.stdout)
        self.manifest = json.dumps(manifest, sort_keys=True)
        with open(os.path.join(SRC, "boot.py")) as f:
            self.boot = f.read()

    def run(self, action: str, *args: str, env: dict | None = None) -> dict:
        argv = [sys.executable, "-I", "-c", self.boot, PRIVATE, SHIPPED, self.code, self.manifest,
                action, "--root", ROOT, *args]
        r = sh(argv, env={**os.environ, **(env or {})}, cwd="/")
        try:
            return json.loads(r.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise Failure(f"{action} answered nothing: {r.stderr[-1500:]}") from None

    def start(self, settings: dict | None = None, fresh_store: bool = False, wait_ready=True) -> dict:
        if fresh_store:
            shutil.rmtree(os.path.join(PRIVATE, "store"), ignore_errors=True)
        settings = {**EXTRA_SETTINGS, **(settings or {})}
        config = json.dumps({"base_url": self.base_url, "token": self.token})
        t0 = time.monotonic()
        for _ in range(30):
            answer = self.run("drive-start", "--base-url", self.base_url, env={
                "LIVEFS_CONFIG": config, "DRIVE_MOUNT_SETTINGS": json.dumps(settings)})
            # A real server reloading its code answers nothing for a while.
            if answer.get("error") != "unreachable" or self.base_url.startswith("http://127.0.0.1"):
                break
            time.sleep(5)
        mounted = time.monotonic() - t0
        if not answer.get("ok"):
            raise Failure(f"drive-start: {answer}; log:\n{self.log_tail()}")
        out = {"mounted_s": round(mounted, 3)}
        if wait_ready:
            while True:
                status = self.status()
                if status and status.get("phase") == "ready":
                    break
                if time.monotonic() - t0 > 1800:
                    raise Failure("the daemon never got ready")
                time.sleep(0.2)
            out["ready_s"] = round(time.monotonic() - t0, 3)
            out["text"] = status["text"]
            out["warm"] = status["warm"]
            out["manifest_s"] = status.get("manifest_s")
        return out

    def stop(self) -> None:
        pid = self.pid()
        self.run("drive-stop")
        deadline = time.monotonic() + 30
        while pid and os.path.exists(f"/proc/{pid}") and time.monotonic() < deadline:
            time.sleep(0.05)

    def pid(self) -> int | None:
        r = sh(["pgrep", "-f", "livefs drive-serve"])
        pids = [int(p) for p in r.stdout.split()]
        return max(pids) if pids else None

    def status(self) -> dict | None:
        return self.run("drive-status").get("status")

    def ops(self) -> dict:
        status = self.status() or {}
        return dict(status.get("ops") or {})

    def log_tail(self) -> str:
        with contextlib.suppress(OSError), open(os.path.join(PRIVATE, "log"), errors="replace") as f:
            return f.read()[-3000:]
        return ""


def diff(after: dict, before: dict) -> dict:
    return {k: after.get(k, 0) - before.get(k, 0) for k in sorted(set(after) | set(before))
            if after.get(k, 0) - before.get(k, 0)}


def settle(mount: Mount, call: str, *args: str) -> dict:
    """What the host runs after a command naming the Drive (``drive-settle``)."""
    return mount.run("drive-settle", "--call", call, *args)


# --- what the fake server was asked and answered -----------------------------


@contextlib.contextmanager
def knobs(server: FakeServer, **values):
    """The fake server's fault knobs set for the block, and what they were
    put back however it ends, so a failing case leaves none set."""
    status, before = server.admin("POST", "limits", body=json.dumps(values).encode())
    if status != 200:
        raise Failure(f"knobs {values}: {before}")
    try:
        yield
    finally:
        server.admin("POST", "limits", body=json.dumps(before).encode())


def notes(server: FakeServer, call: str, kind: str) -> list:
    return [n for n in server.admin("GET", "notices")[1] if n.get("call") == call and n.get("kind") == kind]


def requests(server: FakeServer, action: str) -> int:
    return server.admin("GET", "stats")[1]["requests"].get(action, 0)


def outcomes(server: FakeServer, call: str) -> list:
    return server.admin("GET", "outcomes", {"call": call})[1]


def wake_feed(server: FakeServer, folder: str) -> None:
    """End the feed's request now open, so a knob set meanwhile governs the next."""
    server.write(f"{folder}/_wake.txt", os.urandom(8).hex().encode())
    time.sleep(0.6)


# --- small things ------------------------------------------------------------


def wait_for(pred, timeout: float = 5.0, step: float = 0.01):
    deadline = time.monotonic() + timeout
    while True:
        value = pred()
        if value or time.monotonic() > deadline:
            return value
        time.sleep(step)


def read_file(path: str) -> bytes | None:
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return None


def du(path: str) -> int:
    r = sh(["du", "-sb", path])
    return int(r.stdout.split()[0]) if r.returncode == 0 else 0


def drop_caches() -> bool:
    return sh("sync; echo 3 > /proc/sys/vm/drop_caches").returncode == 0


# --- the corpus --------------------------------------------------------------


def ensure_corpus(workdir: str, files: int, seed: int = 7) -> tuple[str, dict]:
    corpus = os.path.join(workdir, f"corpus-{files}-s{seed}")
    expected_path = corpus + ".expected.json"
    if not os.path.exists(expected_path):
        shutil.rmtree(corpus, ignore_errors=True)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(corpus + ".index.json")
        t0 = time.perf_counter()
        sh([sys.executable, os.path.join(KIT, "gen_corpus.py"), "--out", corpus, "--files", str(files),
            "--seed", str(seed), "--expected", expected_path], check=True)
        log(f"corpus generated in {time.perf_counter() - t0:.1f}s")
    with open(expected_path) as f:
        return corpus, json.load(f)


def corpus_files(corpus: str) -> list:
    files = []
    for dp, _, fns in os.walk(corpus):
        for fn in fns:
            p = os.path.join(dp, fn)
            files.append((os.path.relpath(p, corpus), os.path.getsize(p)))
    files.sort()
    return files


@dataclass
class Env:
    """What a kernel case runs against."""

    server: FakeServer | RealServer
    mount: Mount
    corpus: str
    files: list
    #: What a case measured, for the results file.
    report: dict = field(default_factory=dict)

    @property
    def fake(self) -> bool:
        return isinstance(self.server, FakeServer)

    def small_texts(self) -> list[str]:
        return [f for f, size in self.files if f.endswith((".md", ".txt", ".py")) and 0 < size < 65536]


# --- the blob store (``fake_blob.py``), for the blocks suite ----------------

#: Its plain and TLS bases, once ``trust_blob`` has run.
BLOB: dict = {}


def blob(method: str, path: str, body: dict | None = None):
    import urllib.request

    req = urllib.request.Request(BLOB["plain"] + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    return json.loads(data) if data[:1] in (b"{", b"[") else data


def trust_blob(plain: str, tls: str) -> None:
    """Point the blocks suite at the blob store, and have the daemon trust
    its certificate as it would a real store's from the system's."""
    BLOB.update(plain=plain, tls=tls)
    cert = b""
    for _ in range(150):
        try:
            cert = blob("GET", "/admin/cert")
            break
        except OSError:
            time.sleep(0.2)
    with open("/tmp/blob-ca.pem", "wb") as f:
        f.write(cert)
    os.environ["SSL_CERT_FILE"] = "/tmp/blob-ca.pem"
