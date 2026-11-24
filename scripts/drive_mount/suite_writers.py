"""Every way a program writes, checked against the server's copy. Each case
works in a folder of its own under ``wtest``."""

from __future__ import annotations

import hashlib
import os
import time

from checks import Results, case
from fixtures import TOP, Env, Failure, as_user, knobs, requests, wait_for

BASE = "wtest"


class Folder:
    """A case's folder, made here, with what the checks ask of it."""

    def __init__(self, env: Env, name: str) -> None:
        self.server = env.server
        self.rel = f"{BASE}/{name}"
        self.d = f"{TOP}/{self.rel}"
        as_user(f"mkdir -p {self.d}")

    def sh(self, script: str, **kw):
        return as_user(f"cd {self.d} && {script}", **kw)

    def entry(self, rel: str) -> dict:
        return self.server.entry(f"{self.rel}/{rel}") or {}

    def gone(self, rel: str) -> bool:
        return self.server.entry(f"{self.rel}/{rel}") is None

    def server_has(self, rel: str, want: bytes, timeout: float = 3.0) -> bool:
        return bool(wait_for(lambda: self.server.content(f"{self.rel}/{rel}") == want, timeout))

    def mount_has(self, rel: str, want: bytes) -> bool:
        with open(f"{self.d}/{rel}", "rb") as f:
            return f.read() == want

    def trashed(self, *names: str) -> list[str]:
        """What the trash holds under these names here (a replace leaves the
        old bytes as a version, never a trash item)."""
        wanted = tuple(f"{self.rel}/{n}" for n in names)
        return [t["path"] for t in self.server.admin("GET", "trash_list")[1] if t["path"].startswith(wanted)]


def writes_and_moves(server, before: tuple[int, int]) -> tuple[int, int]:
    return requests(server, "write") - before[0], requests(server, "move") - before[1]


@case("writers")
def mkdir_reaches_the_server(r: Results, env: Env) -> None:
    as_user(f"mkdir -p {TOP}/{BASE}")
    r.pin("mkdir -p reaches the server", (env.server.entry(BASE) or {}).get("kind") == "folder")


@case("writers")
def shell_redirects(r: Results, env: Env) -> None:
    f = Folder(env, "shell")
    f.sh("echo hello > a.txt")
    r.pin("echo > new file", f.server_has("a.txt", b"hello\n") and f.mount_has("a.txt", b"hello\n"))
    f.sh("echo more >> a.txt")
    r.pin("append >>", f.server_has("a.txt", b"hello\nmore\n") and f.mount_has("a.txt", b"hello\nmore\n"))
    f.sh("printf 'xy' > a.txt")
    r.pin("shorter rewrite with > (O_TRUNC) keeps no old tail",
          f.server_has("a.txt", b"xy") and f.mount_has("a.txt", b"xy"))


@case("writers")
def sed_in_place(r: Results, env: Env) -> None:
    f = Folder(env, "sed")
    f.sh("printf 'hello world\\nline two\\n' > s.txt && sed -i 's/hello/HELLO/' s.txt")
    want = b"HELLO world\nline two\n"
    sed_tmp = [e for e in os.listdir(f.d) if e.startswith("sed")]
    r.pin("sed -i (temp file renamed over the target)",
          f.server_has("s.txt", want) and f.mount_has("s.txt", want) and not sed_tmp,
          f"leftovers {sed_tmp}" if sed_tmp else "")
    in_trash = f.trashed("s.txt", "sed")
    r.pin("sed -i leaves nothing in the trash", not in_trash, str(in_trash))


@case("writers")
def saves_over_a_saved_file_are_one_write(r: Results, env: Env) -> None:
    if not env.fake:
        return
    f, server = Folder(env, "replace"), env.server
    f.sh("printf 'one\\ntwo\\n' > g.txt")
    f.server_has("g.txt", b"one\ntwo\n")
    g_id = f.entry("g.txt").get("id")
    before = (requests(server, "write"), requests(server, "move"))
    f.sh("sed -i 's/two/TWO/' g.txt")
    landed = f.server_has("g.txt", b"one\nTWO\n")
    time.sleep(0.6)
    writes, moves = writes_and_moves(server, before)
    r.pin("sed -i on a saved file reaches the server as one write over it, no move",
          landed and writes == 1 and moves == 0 and f.entry("g.txt").get("id") == g_id,
          f"writes {writes}, moves {moves}")

    f.sh("printf 'draft\\n' > e.txt")
    f.server_has("e.txt", b"draft\n")
    e_id = f.entry("e.txt").get("id")
    before = (requests(server, "write"), requests(server, "move"))
    f.sh("python3 -c \"import os; f=open('.e.txt.tmp','w'); f.write('final\\n'); "
         "f.close(); os.replace('.e.txt.tmp','e.txt')\"")
    landed = f.server_has("e.txt", b"final\n")
    time.sleep(0.6)
    writes, moves = writes_and_moves(server, before)
    r.pin("editor save without fsync (temp file renamed over) reaches the server as one "
          "replace, the target keeping its id",
          landed and writes == 1 and moves == 0 and f.gone(".e.txt.tmp") and f.entry("e.txt").get("id") == e_id,
          f"writes {writes}, moves {moves}")


@case("writers")
def python_file_modes(r: Results, env: Env) -> None:
    f = Folder(env, "python")
    f.sh("python3 -c \"f=open('p.txt','w'); f.write('0123456789'*100); f.close()\"")
    f.sh("python3 -c \"f=open('p.txt','w'); f.write('short'); f.close()\"")
    r.pin("python open/write/close, then a shorter rewrite", f.server_has("p.txt", b"short"))
    f.sh("python3 -c \"f=open('p.txt','a'); f.write('+tail'); f.close()\"")
    r.pin("python append mode", f.server_has("p.txt", b"short+tail"))
    f.sh("python3 -c \"f=open('p.txt','r+'); f.seek(2); f.write('OO'); f.close()\"")
    r.pin("python r+ write in the middle", f.server_has("p.txt", b"shOOt+tail"))
    f.sh("truncate -s 3 p.txt")
    r.pin("truncate -s", f.server_has("p.txt", b"shO"))
    # What the kernel caches after a local write is right too.
    st = os.stat(f"{f.d}/p.txt")
    r.pin("stat after writes shows the saved size", st.st_size == 3, str(st.st_size))


@case("writers")
def cp_mv_rm(r: Results, env: Env) -> None:
    f = Folder(env, "cpmv")
    with open("/etc/services", "rb") as fh:
        services = fh.read()
    as_user(f"cp /etc/services {f.d}/c.txt")
    r.pin("cp into the mount", f.server_has("c.txt", services) and f.mount_has("c.txt", services))
    first_id = f.entry("c.txt").get("id")
    f.sh("mv c.txt m.txt")
    r.pin("mv keeps the entry id, old name gone",
          f.entry("m.txt").get("id") == first_id and f.gone("c.txt")
          and not os.path.exists(f"{f.d}/c.txt") and f.mount_has("m.txt", services))
    f.sh("rm m.txt")
    trash = env.server.admin("GET", "trash_list")[1]
    r.pin("rm moves it to the trash", f.gone("m.txt")
          and any(t["path"] == f"{f.rel}/m.txt" for t in trash) and not os.path.exists(f"{f.d}/m.txt"))


@case("writers")
def folders(r: Results, env: Env) -> None:
    f = Folder(env, "folders")
    as_user(f"mkdir -p {f.d}/x/y/z && echo deep > {f.d}/x/y/z/f.txt")
    r.pin("mkdir -p of a new chain, then a file in it",
          f.entry("x/y/z").get("kind") == "folder" and f.server_has("x/y/z/f.txt", b"deep\n"))
    p = f.sh("rmdir x 2>&1", check=False)
    r.pin("rmdir of a non-empty folder is refused", p.returncode != 0 and "not empty" in p.stdout.lower(),
          p.stdout.strip())
    as_user(f"mkdir {f.d}/empty && rmdir {f.d}/empty")
    r.pin("rmdir of an empty folder", f.gone("empty") and not os.path.exists(f"{f.d}/empty"))
    x_id = f.entry("x").get("id")
    f.sh("mv x x2")
    r.pin("rename a folder with its contents",
          f.entry("x2").get("id") == x_id and f.server_has("x2/y/z/f.txt", b"deep\n")
          and f.gone("x/y/z/f.txt") and f.mount_has("x2/y/z/f.txt", b"deep\n"))


@case("writers")
def editor_save_with_fsync(r: Results, env: Env) -> None:
    f = Folder(env, "editor")
    f.sh("echo hello > a.txt")
    f.server_has("a.txt", b"hello\n")
    a_id = f.entry("a.txt").get("id")
    f.sh("python3 -c \"import os; f=open('.a.txt.swp','w'); f.write('edited\\n'); f.flush(); "
         "os.fsync(f.fileno()); f.close(); os.replace('.a.txt.swp','a.txt')\"")
    r.pin("editor save: temp file, fsync, rename over the target (target keeps its id)",
          f.server_has("a.txt", b"edited\n") and f.entry("a.txt").get("id") == a_id
          and f.gone(".a.txt.swp") and f.mount_has("a.txt", b"edited\n"))
    in_trash = f.trashed("a.txt", ".a.txt.swp")
    r.pin("an editor save leaves nothing in the trash", not in_trash, str(in_trash))


@case("writers")
def probe_file_never_reaches_the_server(r: Results, env: Env) -> None:
    f = Folder(env, "probe")
    before_trash = len(env.server.admin("GET", "trash_list")[1])
    f.sh("python3 -c \"import os; open('4913','w').close(); os.unlink('4913')\"")
    time.sleep(1.0)
    r.pin("a probe file made and deleted at once never reaches the server",
          f.gone("4913") and len(env.server.admin("GET", "trash_list")[1]) == before_trash)


@case("writers")
def two_appenders_lose_no_line(r: Results, env: Env) -> None:
    f, lines = Folder(env, "append"), 200
    f.sh(f": > ap.txt && (for i in $(seq {lines}); do echo A$i >> ap.txt; done & "
         f"for i in $(seq {lines}); do echo B$i >> ap.txt; done & wait)")
    got = wait_for(lambda: (env.server.content(f"{f.rel}/ap.txt") or b"").count(b"\n") == 2 * lines, 5)
    body = env.server.content(f"{f.rel}/ap.txt") or b""
    r.pin("two processes appending (O_APPEND) to one file at once lose no line",
          bool(got) and sorted(body.split()) == sorted(
              [f"A{i}".encode() for i in range(1, lines + 1)] + [f"B{i}".encode() for i in range(1, lines + 1)]),
          f"{len(body.splitlines())} lines")


@case("writers")
def rename_over_a_file_being_written(r: Results, env: Env) -> None:
    f = Folder(env, "renameover")
    f.sh("echo old > rw.txt && echo new > rw.new && mv rw.new rw.txt")
    r.pin("a file renamed over one still being written replaces it, and the temp name never lands",
          f.server_has("rw.txt", b"new\n") and f.gone("rw.new") and f.mount_has("rw.txt", b"new\n"))


@case("writers")
def touch_makes_an_empty_file(r: Results, env: Env) -> None:
    f = Folder(env, "touch")
    f.sh("touch t.txt")
    r.pin("touch makes an empty file (saved when the command ends)",
          bool(wait_for(lambda: f.entry("t.txt").get("size") == 0, 3)))


@case("writers")
def large_binary(r: Results, env: Env) -> None:
    f = Folder(env, "big")
    big = os.urandom(20 * 1024 * 1024)
    with open("/tmp/big.bin", "wb") as fh:
        fh.write(big)
    t0 = time.perf_counter()
    as_user(f"dd if=/tmp/big.bin of={f.d}/big.bin bs=1M status=none")
    took = time.perf_counter() - t0
    e = f.entry("big.bin")
    r.pin("20 MiB binary write (dd bs=1M)",
          e.get("sha256") == hashlib.sha256(big).hexdigest() and not e.get("is_text"),
          f"{took:.2f}s", seconds=round(took, 3))
    with open(f"{f.d}/big.bin", "rb") as fh:
        r.pin("20 MiB binary reads back", hashlib.sha256(fh.read()).digest() == hashlib.sha256(big).digest())


@case("writers")
def two_writers_at_once(r: Results, env: Env) -> None:
    f = Folder(env, "twowriters")
    script = ("import sys\n"
              "data = (sys.argv[2] * 4096).encode()\n"
              "with open(sys.argv[1], 'wb') as f:\n"
              "    for _ in range(512): f.write(data)\n")
    with open("/tmp/twowriters.py", "w") as fh:
        fh.write(script)
    t0 = time.perf_counter()
    f.sh("(python3 /tmp/twowriters.py one.bin A & python3 /tmp/twowriters.py two.bin B & wait)")
    took = time.perf_counter() - t0
    r.pin("two processes writing different files at once",
          f.server_has("one.bin", b"A" * 4096 * 512) and f.server_has("two.bin", b"B" * 4096 * 512),
          f"{took:.2f}s")


@case("writers")
def git_workflow(r: Results, env: Env) -> None:
    # Lock files, renames over, many small files, link() falling back.
    f = Folder(env, "git")
    t0 = time.perf_counter()
    p = f.sh(
        "mkdir repo && cd repo && git init -q . && git config user.email h@example.com "
        "&& git config user.name harness && echo one > README.md && git add README.md "
        "&& git commit -qm first && echo two >> README.md && git commit -qam second "
        "&& git log --oneline | wc -l && git status --porcelain | wc -l && git fsck --no-progress 2>&1 | wc -l",
        check=False)
    took = time.perf_counter() - t0
    out = p.stdout.split()
    r.pin("git init, two commits, log, status, fsck inside the mount",
          p.returncode == 0 and out[:3] == ["2", "0", "0"],
          f"{took:.2f}s exit {p.returncode} out {out} {p.stderr[-300:]}", seconds=round(took, 3))
    head = env.server.content(f"{f.rel}/repo/.git/HEAD")
    r.pin("git's files are on the server", head in (b"ref: refs/heads/master\n", b"ref: refs/heads/main\n"),
          repr(head))


@case("writers")
def journal_outlives_a_killed_daemon(r: Results, env: Env) -> None:
    # Written and the program gone before any save, then the daemon killed:
    # the bytes are in its journal, and the next one saves them.
    if not env.fake:
        return
    f, mount = Folder(env, "journal"), env.mount
    # The server answers busy without filing, so the background saver cannot
    # land the bytes before the kill and only a journal replay can.
    with knobs(env.server, busy_writes=100000):
        f.sh("echo survives > k.txt", settle=False)
        pid = mount.pid()
        if not pid:
            raise Failure("daemon not running")
        os.kill(pid, 9)
        wait_for(lambda: not os.path.exists(f"/proc/{pid}"), 5)
        r.pin("nothing reached the server before the kill", env.server.entry(f"{f.rel}/k.txt") is None)
    mount.start({"warm": False})
    r.pin("a file written and its program gone, then the daemon killed before the save, "
          "lands from the journal", f.server_has("k.txt", b"survives\n", 10))


@case("writers")
def rm_r_is_one_trash_item(r: Results, env: Env) -> None:
    # One ``rm -r`` is one thing to restore (the fake keeps no sittings).
    if env.fake:
        return
    f, server = Folder(env, "rmr"), env.server
    earlier = {t["trash_id"] for t in server.admin("GET", "trash_list")[1]}
    as_user(f"mkdir -p {f.d}/many && cd {f.d}/many && for i in $(seq 1 300); do echo $i > f$i.txt; done")
    wait_for(lambda: f.entry("many/f300.txt").get("size") == 4, 30)
    t0 = time.perf_counter()
    p = as_user(f"rm -r {f.d}/many", check=False)
    took = time.perf_counter() - t0
    # One item, or none new: deletes of one sitting within a minute join the
    # item it already made (every command here runs in one session).
    items = {t["trash_id"] for t in server.admin("GET", "trash_list")[1]} - earlier
    r.pin("rm -r of a folder of 300 files is one trash item",
          p.returncode == 0 and len(items) <= 1 and f.gone("many"),
          f"{len(items)} items, {took:.1f}s {p.stderr.strip()[:200]}", seconds=round(took, 3))
