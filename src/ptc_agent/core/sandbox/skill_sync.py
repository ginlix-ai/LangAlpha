"""Sandbox-side primitives for two-way workspace skill sync.

The server-side reconciler (``src/server/services/user_skills/reconcile.py``)
drives these; every ledger mutation happens here, flock-serialized inside the
sandbox, so concurrent script runs (two workers, or a reconcile racing a
manual refresh) can never interleave a read-modify-write of the lock file.

Three operations:

- :func:`report` — one exec: scan every skill dir and ledger entry, compute
  content tree hashes (with a git-index-style stat cache so unchanged trees
  cost one ``lstat`` per file), and return the full picture for the server's
  decision matrix. Read-only except the stat cache and janitorial GC of
  orphaned staging/trash dirs.
- :func:`apply_actions` — one exec: the write phase. Ledger stamps are
  guarded by ``expectTreeHash`` (stamp-if-unchanged), directory deletes
  re-verify the tree hash first (content beats deletion), and push-down
  lands as an atomic rename swap of a pre-staged dir.
- :func:`download_tree` — bounded, no-follow, regular-files-only zip of one
  skill dir for pull-up validation on the server.

The tree hash is defined over exactly the validator/upload projection
(``__pycache__`` dirs and ``LICENSE.txt`` excluded) so ignored files can
never produce a false-dirty loop.
"""

from __future__ import annotations

import base64
import json
import secrets
import shlex
import textwrap
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.agent.middleware.skills.lock import LOCK_FILE_VERSION

from .retry import RetryPolicy

if TYPE_CHECKING:
    from .ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

# How long an abandoned staging/trash dir lives before the next report GCs it.
# Generous on purpose: a dir younger than this may belong to an in-flight
# push from another worker.
ORPHAN_TTL_SECONDS = 3600


class SkillSyncError(Exception):
    """A sandbox-side sync primitive failed.

    ``code`` is the script's machine-readable classification (set for
    failures that depend only on the tree's content); the reconciler keys
    retry suppression on it rather than on message text.
    """

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.code = code


def skills_base(sandbox: "PTCSandbox") -> str:
    return f"{sandbox._work_dir}/.agents/skills"


# The script source is a plain string (no f-string) — arguments travel as a
# base64 JSON blob, and the whole script is itself base64-wrapped into the
# exec command line, which sidesteps shell quoting entirely.
_SCRIPT = textwrap.dedent(r'''
import base64, fcntl, hashlib, json, os, re, shutil, stat, sys, time

ARGS = json.loads(base64.b64decode("__ARGS_B64__").decode())
BASE = ARGS["base"]
MODE = ARGS["mode"]
LOCK_VERSION = ARGS["lockVersion"]
ORPHAN_TTL = ARGS["orphanTtl"]
LOCK_PATH = os.path.join(BASE, "skills-lock.json")
FLOCK_PATH = os.path.join(BASE, ".skills-sync.flock")
STAGING = os.path.join(BASE, ".staging")


def fail(msg, code=None):
    out = {"status": "error", "error": msg}
    if code:
        out["code"] = code
    print(json.dumps(out))
    sys.exit(1)


def acquire_flock():
    os.makedirs(BASE, exist_ok=True)
    fh = open(FLOCK_PATH, "w")
    deadline = time.time() + 15
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fh
        except OSError:
            if time.time() > deadline:
                fail("flock timeout")
            time.sleep(0.2)


def read_lock():
    empty = {"version": LOCK_VERSION, "skills": {}}
    if not os.path.isfile(LOCK_PATH):
        return empty
    try:
        with open(LOCK_PATH) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return empty
    if not isinstance(data, dict) or not isinstance(data.get("skills"), dict):
        return empty
    return data


def write_lock(data):
    tmp = LOCK_PATH + ".%d.tmp" % os.getpid()
    with open(tmp, "w") as f:
        json.dump(data, f, sort_keys=True, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, LOCK_PATH)


def projected_files(skill_dir):
    """Walk one skill dir: (relpath, lstat) for projection files, plus a flag
    for anything unsyncable (symlink/special/hardlinked files)."""
    out = []
    special = None
    for root, dirs, names in os.walk(skill_dir, followlinks=False):
        keep = []
        for d in dirs:
            if d == "__pycache__":
                continue
            # os.walk sorts entries with is_dir(), which follows the link, so a
            # symlink pointing at a directory lands here and is then never
            # descended into (followlinks=False). It would leave no trace in
            # the hash and none in `special` either, and the tree would read as
            # fully synced while holding something the projection cannot carry
            # -- the download would silently omit it and the next push would
            # delete it with the directory it replaces.
            dp = os.path.join(root, d)
            if os.path.islink(dp):
                rel = os.path.relpath(dp, skill_dir).replace(os.sep, "/")
                special = "symlinked directory: %s" % rel
                continue
            keep.append(d)
        dirs[:] = keep
        for n in names:
            # Mirror the host projection exactly (validate.py::_ignored): it
            # drops any path component named __pycache__, files included, so
            # skipping only the directory here would false-dirty forever.
            if n in ("LICENSE.txt", "__pycache__"):
                continue
            p = os.path.join(root, n)
            st = os.lstat(p)
            rel = os.path.relpath(p, skill_dir).replace(os.sep, "/")
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                special = "non-regular file: %s" % rel
                continue
            if st.st_nlink > 1:
                special = "hardlinked file: %s" % rel
                continue
            out.append((rel, st))
    out.sort()
    return out, special


def hash_file(path):
    h = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def tree_state(skill_dir, cache):
    """(tree_hash, new_cache, special_reason)."""
    files, special = projected_files(skill_dir)
    new_cache = {}
    parts = []
    for rel, st in files:
        cached = cache.get(rel)
        # ctime rides along with mtime and size. A write that restores the
        # mtime -- cp -p, an archive extract, an editor that preserves times --
        # and lands on the same byte count would otherwise hand back the
        # previous hash, and the tree would read as synced while holding
        # different content. ctime moves on any inode write and userspace has
        # no call to set it back. The length check retires the 3-field entries
        # written before this: they recompute once and are rewritten.
        if (
            cached
            and len(cached) == 4
            and cached[0] == st.st_mtime_ns
            and cached[1] == st.st_size
            and cached[2] == st.st_ctime_ns
        ):
            sha = cached[3]
        else:
            try:
                sha = hash_file(os.path.join(skill_dir, rel))
            except OSError:
                special = special or ("unreadable file: %s" % rel)
                continue
        new_cache[rel] = [st.st_mtime_ns, st.st_size, st.st_ctime_ns, sha]
        parts.append(rel + "\x00" + sha)
    tree_hash = hashlib.sha256("\n".join(parts).encode()).hexdigest()
    return tree_hash, new_cache, special


def parse_frontmatter(skill_dir):
    """Minimal frontmatter read: name and description. Returns None when
    SKILL.md is missing or has no frontmatter block."""
    md = os.path.join(skill_dir, "SKILL.md")
    if not os.path.isfile(md):
        return None
    try:
        fd = os.open(md, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "r", errors="replace") as f:
            content = f.read(1048576)
    except OSError:
        return None
    content = content.replace("\r\n", "\n")
    m = re.match(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", content, re.DOTALL)
    if not m:
        return None
    fm = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if ":" not in line or line.startswith("#"):
            continue
        k, _, v = line.partition(":")
        fm[k.strip()] = v.strip().strip("\"'")
    return {
        "name": fm.get("name", ""),
        "description": fm.get("description", ""),
    }


def gc_orphans():
    now = time.time()
    if os.path.isdir(STAGING):
        for entry in os.listdir(STAGING):
            p = os.path.join(STAGING, entry)
            try:
                if now - os.lstat(p).st_mtime > ORPHAN_TTL:
                    shutil.rmtree(p, ignore_errors=True)
            except OSError:
                pass
    for entry in os.listdir(BASE) if os.path.isdir(BASE) else []:
        if entry.startswith(".trash-"):
            p = os.path.join(BASE, entry)
            try:
                if now - os.lstat(p).st_mtime > ORPHAN_TTL:
                    shutil.rmtree(p, ignore_errors=True)
            except OSError:
                pass


def skill_dirs():
    out = set()
    if os.path.isdir(BASE):
        for name in os.listdir(BASE):
            if name.startswith("."):
                continue
            p = os.path.join(BASE, name)
            if os.path.isdir(p) and not os.path.islink(p) and os.path.isfile(os.path.join(p, "SKILL.md")):
                out.add(name)
    return out


def run_report():
    fh = acquire_flock()
    gc_orphans()
    lock_data = read_lock()
    entries = lock_data.get("skills", {})
    dirs = skill_dirs()
    names = dirs | set(entries.keys())
    result = {}
    cache_dirty = False
    for name in sorted(names):
        entry = entries.get(name)
        info = {"present": name in dirs, "entry": None}
        if entry is not None:
            info["entry"] = {
                "owner": entry.get("owner"),
                "sourceType": entry.get("sourceType"),
                "sync": {k: v for k, v in (entry.get("sync") or {}).items() if k != "statCache"} or None,
            }
        if name in dirs:
            skill_dir = os.path.join(BASE, name)
            cache = ((entry or {}).get("sync") or {}).get("statCache") or {}
            th, new_cache, special = tree_state(skill_dir, cache)
            fm = parse_frontmatter(skill_dir)
            well_formed = bool(fm and fm["name"] == name and fm["description"])
            reason = None
            if special:
                reason = special
            elif not well_formed:
                reason = "frontmatter missing, unparsable, or name mismatch"
            info.update({
                "treeHash": th if not special else None,
                "wellFormed": well_formed,
                "syncable": special is None,
                "reason": reason,
                "frontmatter": fm,
            })
            if entry is not None and "sync" in entry and new_cache != cache:
                entry["sync"]["statCache"] = new_cache
                cache_dirty = True
        result[name] = info
    if cache_dirty:
        write_lock(lock_data)
    fcntl.flock(fh, fcntl.LOCK_UN)
    print(json.dumps({"status": "ok", "skills": result}))


def _current_tree(name):
    """(state, treeHash) with state in 'missing' | 'unsyncable' | 'ok'.

    The two None cases are distinct decisions for a destructive op: a missing
    dir is nothing to protect, an unsyncable one is content we can't verify
    and therefore must not destroy.

    Deliberately hashes from bytes with no stat cache, and takes no entries to
    read one from. This hash is what a delete or an overwrite is checked
    against, so it answers "what is on disk right now"; the cache accelerates
    the bulk push, where a stale entry costs a redundant upload instead of a
    wrong decision.
    """
    skill_dir = os.path.join(BASE, name)
    if not os.path.isdir(skill_dir):
        return "missing", None
    th, _, special = tree_state(skill_dir, {})
    if special:
        return "unsyncable", None
    return "ok", th


def _drifted(name, action):
    """Whether the live tree still matches what the server decided against.

    A missing dir never blocks: the server's decision was about content it
    owns, and re-creating it is the heal. ``expectAbsent`` is the inverse
    guard for a create that must not clobber a dir that appeared since.
    """
    state, current = _current_tree(name)
    if action.get("expectAbsent"):
        return state != "missing", current
    expect = action.get("expectTreeHash")
    if expect is None or state == "missing":
        return False, current
    return current != expect, current


def run_apply():
    fh = acquire_flock()
    lock_data = read_lock()
    entries = lock_data.setdefault("skills", {})
    results = []
    for action in ARGS.get("actions", []):
        op = action.get("op")
        name = action.get("name")
        res = {"op": op, "name": name, "ok": True}
        try:
            if op == "set_entry":
                prior_cache = ((entries.get(name) or {}).get("sync") or {}).get("statCache")
                entry = action["entry"]
                if prior_cache and "sync" in entry and "statCache" not in entry["sync"]:
                    entry["sync"]["statCache"] = prior_cache
                entries[name] = entry
            elif op == "update_sync":
                entry = entries.get(name)
                if entry is None:
                    res.update(ok=False, error="no entry")
                else:
                    expect = action.get("expectTreeHash")
                    if expect is not None:
                        state, current = _current_tree(name)
                        if state != "ok" or current != expect:
                            res.update(ok=False, drift=True, treeHash=current)
                            results.append(res)
                            continue
                    new_sync = action.get("sync")
                    if new_sync is None:
                        entry.pop("sync", None)
                    else:
                        prior_cache = (entry.get("sync") or {}).get("statCache")
                        if prior_cache and "statCache" not in new_sync:
                            new_sync["statCache"] = prior_cache
                        entry["sync"] = new_sync
            elif op == "remove_entry":
                entries.pop(name, None)
            elif op == "delete_dir":
                drift, current = _drifted(name, action)
                if drift:
                    res.update(ok=False, drift=True, treeHash=current)
                else:
                    shutil.rmtree(os.path.join(BASE, name), ignore_errors=True)
                    entries.pop(name, None)
            elif op == "swap_staged":
                staged = action["staged"]
                drift, current = _drifted(name, action)
                if not os.path.isdir(staged):
                    res.update(ok=False, error="staged dir missing")
                elif drift:
                    res.update(ok=False, drift=True, treeHash=current)
                    shutil.rmtree(staged, ignore_errors=True)
                else:
                    live = os.path.join(BASE, name)
                    trash = os.path.join(BASE, ".trash-%s" % os.path.basename(staged))
                    displaced = os.path.lexists(live)
                    if displaced:
                        os.rename(live, trash)
                    try:
                        os.rename(staged, live)
                    except OSError:
                        # Never leave the name with nothing: put the live dir
                        # back before surfacing the failure.
                        if displaced:
                            os.rename(trash, live)
                        raise
                    entry = action["entry"]
                    th, new_cache, special = tree_state(live, {})
                    sync = entry.setdefault("sync", {})
                    sync["syncedTreeHash"] = th
                    sync["statCache"] = new_cache
                    entries[name] = entry
                    shutil.rmtree(trash, ignore_errors=True)
                    res["treeHash"] = th
            else:
                res.update(ok=False, error="unknown op")
        except Exception as e:
            res.update(ok=False, error="%s: %s" % (type(e).__name__, e))
        results.append(res)
    write_lock(lock_data)
    fcntl.flock(fh, fcntl.LOCK_UN)
    print(json.dumps({"status": "ok", "results": results}))


def run_download():
    import zipfile
    name = ARGS["name"]
    skill_dir = os.path.join(BASE, name)
    if not os.path.isdir(skill_dir):
        fail("skill dir missing")
    files, special = projected_files(skill_dir)
    if special:
        fail("unsyncable tree: %s" % special, "unsyncable")
    if len(files) > ARGS["maxFiles"]:
        fail("too many files: %d" % len(files), "too_many_files")
    total = sum(st.st_size for _, st in files)
    if total > ARGS["maxTotalBytes"]:
        fail("tree too large: %d bytes" % total, "tree_too_large")
    parts = []
    out = ARGS["out"]
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel, st in files:
            if st.st_size > ARGS["maxFileBytes"]:
                fail("file too large: %s" % rel, "file_too_large")
            fd = os.open(os.path.join(skill_dir, rel), os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as f:
                data = f.read(ARGS["maxFileBytes"] + 1)
            if len(data) > ARGS["maxFileBytes"]:
                fail("file too large: %s" % rel, "file_too_large")
            parts.append(rel + "\x00" + hashlib.sha256(data).hexdigest())
            zf.writestr(name + "/" + rel, data)
    tree_hash = hashlib.sha256("\n".join(parts).encode()).hexdigest()
    print(json.dumps({"status": "ok", "path": out, "bytes": os.path.getsize(out), "treeHash": tree_hash}))


def run_merge_authoritative():
    """merge_lock_files, but against a fresh read under the flock.

    The asset-sync path computes its authoritative (platform + managed)
    entries from a lock snapshot taken before a multi-second prune/upload;
    writing that merge back blind would clobber anything the reconciler
    committed in between. Re-reading here closes the window. Entries the
    authoritative set may never displace — agent-installed and linked, the
    same predicates as lock.py — always survive from the fresh read, and a
    racing claim on an authoritative name is reported rather than taken:
    the reconciler's next tree-hash check arbitrates the bytes.
    """
    fh = acquire_flock()
    entries = read_lock().get("skills", {})
    auth = ARGS["entries"]
    merged = {}
    skipped = []
    for name, entry in entries.items():
        # Mirrors lock.py is_agent_installed / is_linked.
        if (entry.get("owner") == "user" and entry.get("sourceType") != "langalpha-user") \
                or (entry.get("sync") or {}).get("linkedSkillId"):
            merged[name] = entry
            if name in auth:
                skipped.append(name)
    for name, entry in auth.items():
        if name not in skipped:
            merged[name] = entry
    write_lock({"version": LOCK_VERSION, "skills": merged})
    fcntl.flock(fh, fcntl.LOCK_UN)
    print(json.dumps({"status": "ok", "skills": merged, "skipped": skipped}))


if MODE == "report":
    run_report()
elif MODE == "apply":
    run_apply()
elif MODE == "download":
    run_download()
elif MODE == "merge_authoritative":
    run_merge_authoritative()
else:
    fail("unknown mode")
''')


def _build_command(args: dict[str, Any]) -> str:
    args = {"lockVersion": LOCK_FILE_VERSION, "orphanTtl": ORPHAN_TTL_SECONDS, **args}
    args_b64 = base64.b64encode(json.dumps(args).encode()).decode()
    script_b64 = base64.b64encode(
        _SCRIPT.replace("__ARGS_B64__", args_b64).encode()
    ).decode()
    return (
        'python3 -c "import base64;'
        f"exec(base64.b64decode('{script_b64}').decode())\""
    )


async def _run(
    sandbox: "PTCSandbox", args: dict[str, Any], *, retry_policy: RetryPolicy
) -> dict[str, Any]:
    assert sandbox.runtime is not None
    result = await sandbox._runtime_call(
        sandbox.runtime.exec,
        _build_command(args),
        retry_policy=retry_policy,
    )
    stdout = (getattr(result, "stdout", "") or "").strip()
    try:
        payload = json.loads(stdout.splitlines()[-1]) if stdout else {}
    except (json.JSONDecodeError, IndexError):
        raise SkillSyncError(f"unparsable skill-sync output: {stdout[:500]!r}")
    if payload.get("status") != "ok":
        raise SkillSyncError(
            payload.get("error") or "skill-sync script failed",
            code=payload.get("code"),
        )
    return payload


async def report(sandbox: "PTCSandbox") -> dict[str, Any]:
    """Scan the sandbox: ``{name: {present, wellFormed, syncable, treeHash,
    frontmatter, entry, ...}}`` for the union of skill dirs and ledger names."""
    payload = await _run(
        sandbox,
        {"mode": "report", "base": skills_base(sandbox)},
        retry_policy=RetryPolicy.SAFE,
    )
    return payload["skills"]


async def apply_actions(
    sandbox: "PTCSandbox", actions: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Execute the write phase; returns per-action results (``ok``/``drift``)."""
    if not actions:
        return []
    payload = await _run(
        sandbox,
        {"mode": "apply", "base": skills_base(sandbox), "actions": actions},
        retry_policy=RetryPolicy.UNSAFE,
    )
    return payload["results"]


async def merge_authoritative_entries(
    sandbox: "PTCSandbox", entries: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    """Fold the asset-sync path's authoritative entries into the live lock,
    flock-serialized against the reconciler. Returns ``(merged_lock_file,
    skipped_names)`` — the merged file is the post-write truth (fresh-read
    based, unlike a host-side merge over a stale snapshot), and ``skipped``
    names authoritative entries that lost to a racing agent/linked claim.
    Safe to retry: every run re-reads and re-merges.
    """
    payload = await _run(
        sandbox,
        {"mode": "merge_authoritative", "base": skills_base(sandbox), "entries": entries},
        retry_policy=RetryPolicy.SAFE,
    )
    merged = {"version": LOCK_FILE_VERSION, "skills": payload["skills"]}
    return merged, payload.get("skipped", [])


async def download_tree(
    sandbox: "PTCSandbox",
    name: str,
    *,
    max_files: int,
    max_file_bytes: int,
    max_total_bytes: int,
) -> tuple[bytes, str]:
    """Bounded zip of one skill dir → ``(zip_bytes, tree_hash)``.

    The hash is computed from the exact bytes zipped, so the caller can stamp
    the ledger against what it validated, not what a racing agent wrote since.
    """
    base = skills_base(sandbox)
    token = secrets.token_hex(8)
    out = f"{base}/.staging/download-{token}.zip"
    payload = await _run(
        sandbox,
        {
            "mode": "download",
            "base": base,
            "name": name,
            "out": out,
            "maxFiles": max_files,
            "maxFileBytes": max_file_bytes,
            "maxTotalBytes": max_total_bytes,
        },
        retry_policy=RetryPolicy.SAFE,
    )
    assert sandbox.runtime is not None
    raw = await sandbox._runtime_call(
        sandbox.runtime.download_file, out, retry_policy=RetryPolicy.SAFE
    )
    await sandbox._runtime_call(
        sandbox.runtime.exec, f"rm -f {out}", retry_policy=RetryPolicy.SAFE
    )
    if raw is None:
        raise SkillSyncError("download produced no bytes")
    data = raw if isinstance(raw, bytes) else raw.encode()
    if len(data) > max_total_bytes + 1_048_576:
        raise SkillSyncError("downloaded archive exceeds the transfer ceiling")
    return data, payload["treeHash"]


async def stage_skill_files(
    sandbox: "PTCSandbox", files: list[tuple[str, bytes]]
) -> str:
    """Upload a skill's files into a fresh staging dir; returns its path.

    ``files`` are ``(relpath, content)`` pairs from the validated archive.
    The subsequent ``swap_staged`` apply-op renames this dir into place
    atomically — the live dir is never rm -rf'd first.
    """
    assert sandbox.runtime is not None
    base = skills_base(sandbox)
    token = secrets.token_hex(8)
    staged = f"{base}/.staging/push-{token}"
    subdirs = {
        f"{staged}/{'/'.join(rel.split('/')[:-1])}" for rel, _ in files if "/" in rel
    }
    # Archive member paths reach this command line, so quote rather than
    # single-quote-wrap: a member named ``a'/$(id)/b`` would otherwise close
    # the quote and run in the sandbox shell.
    mkdir_cmd = "mkdir -p " + " ".join(
        shlex.quote(d) for d in sorted({staged, *subdirs})
    )
    await sandbox._runtime_call(
        sandbox.runtime.exec, mkdir_cmd, retry_policy=RetryPolicy.SAFE
    )
    await sandbox._runtime_call(
        sandbox.runtime.upload_files,
        [(content, f"{staged}/{rel}") for rel, content in files],
        retry_policy=RetryPolicy.SAFE,
    )
    return staged
