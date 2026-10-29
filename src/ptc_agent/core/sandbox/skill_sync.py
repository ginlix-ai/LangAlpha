"""Sandbox-side primitives for two-way workspace skill sync.

The server-side reconciler (``src/server/services/user_skills/reconcile.py``)
drives these; every ledger mutation happens here, flock-serialized inside the
sandbox, so concurrent script runs (two workers, or a reconcile racing a
manual refresh) can never interleave a read-modify-write of the lock file.

Every operation is aimed at one tier by its ``base``: the computer's shared
directory (:func:`skills_base`, platform and user-tier skills) or a
workspace's own (``WorkspaceLayout.skills``). Each tier keeps its own ledger
and its own flock, so two workspaces reconciling at once never share a lock
file.

Four operations:

- :func:`report` — one exec: scan every skill dir and ledger entry, compute
  content tree hashes (with a git-index-style stat cache so unchanged trees
  cost one ``lstat`` per file), and return the full picture for the server's
  decision matrix. Read-only except the stat cache, janitorial GC of
  orphaned staging/trash dirs, and finishing a delete in place cut short.
- :func:`apply_actions` — one exec: the write phase. Ledger stamps are
  guarded by ``expectTreeHash`` (stamp-if-unchanged), directory deletes
  re-verify the tree hash first (content beats deletion), and push-down
  lands as an atomic rename swap of a pre-staged dir.
- :func:`download_tree` — bounded, no-follow, regular-files-only zip of one
  skill dir for pull-up validation on the server.
- :func:`link_shared_skills` (one exec): point a workspace tier at the shared
  skills it should see, and prune the links it should not. First it drops the
  tier's own copies of skills the shared tier serves, with no tree-hash guard:
  those bytes are server-owned, so a copy can only be older.

The asset sync writes the shared tier through :func:`install_staged_skills`,
which swaps pre-staged dirs into place and merges its ledger entries in one
flock hold, and a link pass reads the shared tier under that flock, so it
never sees a shared skill missing mid-swap.

The tree hash is defined over exactly the validator/upload projection
(``__pycache__`` dirs, ``LICENSE.txt`` and macOS litter excluded) so ignored
files can never produce a false-dirty loop.
"""

from __future__ import annotations

import json
import posixpath
import secrets
import shlex
import textwrap
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from ptc_agent.agent.middleware.skills.lock import (
    LOCK_FILE_VERSION,
    MANAGED_SOURCE_TYPE,
)

from ..paths import SandboxLayout
from .retry import RetryPolicy
from .tree_upload import upload_tree

if TYPE_CHECKING:
    from .ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

# How long an abandoned staging/trash dir lives before the next report GCs it.
# Generous on purpose: a dir younger than this may belong to an in-flight
# push from another worker.
ORPHAN_TTL_SECONDS = 3600
_CLEANUP_TIMEOUT_SECONDS = 5


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
    """The computer's shared skill directory: platform and user-tier skills."""
    return SandboxLayout(sandbox._work_dir).skills


def shared_link_target(base: str, user_base: str) -> str:
    """The relative target a workspace-tier link uses for a shared skill dir.

    Relative rather than absolute so the link survives a computer whose root
    moves, and so the tree it points into reads the same from either tier.
    """
    return posixpath.relpath(user_base.rstrip("/"), base.rstrip("/"))


_SCRIPT = textwrap.dedent(r'''
import errno, fcntl, hashlib, json, os, re, shutil, stat, sys, time

ARGS = json.loads(__ARGS_JSON__)
BASE = ARGS["base"]
MODE = ARGS["mode"]
LOCK_VERSION = ARGS["lockVersion"]
MANAGED_SOURCE_TYPE = ARGS["managedSourceType"]
ORPHAN_TTL = ARGS["orphanTtl"]
LOCK_PATH = os.path.join(BASE, "skills-lock.json")
STAGING = os.path.join(BASE, ".staging")
DELETING = ".deleting-"


def fail(msg, code=None):
    out = {"status": "error", "error": msg}
    if code:
        out["code"] = code
    print(json.dumps(out))
    sys.exit(1)


def acquire_flock(base=None):
    base = base or BASE
    os.makedirs(base, exist_ok=True)
    fh = open(os.path.join(base, ".skills-sync.flock"), "w")
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
            if d in ("__pycache__", "__MACOSX"):
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
            # skipping only the directory here would false-dirty forever. The
            # rest is the litter macOS leaves (os_litter.is_os_litter).
            skipped = ("LICENSE.txt", "__pycache__", "__MACOSX", ".DS_Store")
            if n in skipped or n.startswith("._"):
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


def remove_path(p):
    # rmtree removes only a real dir, and an agent can leave a symlink or a
    # file at a skill's name.
    if os.path.islink(p) or not os.path.isdir(p):
        os.unlink(p)
        return
    # A folder in it the agent made read-only refuses a delete inside until it
    # is opened up again.
    open_up(p)
    for d, dirs, _ in os.walk(p):
        for name in dirs:
            open_up(os.path.join(d, name))
    shutil.rmtree(p, ignore_errors=True)


def open_up(p):
    try:
        st = os.lstat(p)
        if stat.S_ISDIR(st.st_mode) and st.st_mode & 0o700 != 0o700:
            os.chmod(p, stat.S_IMODE(st.st_mode) | 0o700)
    except OSError:
        pass


def delete_in_place(p):
    # SKILL.md first: a delete cut short leaves a dir that holds no skill, which
    # the next sync installs again, not one that reads as an edit made here and
    # goes up over the server's copy. The mark beside it outlasts the rest, so
    # the next report finishes the delete: left as it was, the dir would read
    # as one the agent made, which an install may not overwrite. With no mark
    # it is not begun: the skill stays whole, and the caller finds it there.
    mark = os.path.join(os.path.dirname(p), DELETING + os.path.basename(p))
    if os.path.isdir(p) and not os.path.islink(p):
        try:
            open(mark, "w").close()
        except OSError:
            return
        try:
            os.unlink(os.path.join(p, "SKILL.md"))
        except OSError:
            pass
    if os.path.lexists(p):
        remove_path(p)
    if not os.path.lexists(p) and os.path.lexists(mark):
        os.unlink(mark)


def replace_live(live, staged, trash):
    """Rename ``staged`` in at ``live``; True if the old dir went to ``trash``.

    A dir a Daytona sandbox restored from its archive sits in a read-only
    overlay layer that refuses to rename it (EXDEV), so it is deleted where it
    is instead, the name empty only under the flock a link pass reads it
    under. The staged dir, made since, still renames; should that rename
    fail, the name stays empty until the next sync installs the skill again.
    """
    displaced = os.path.lexists(live)
    if displaced:
        try:
            os.rename(live, trash)
        except OSError as e:
            if e.errno != errno.EXDEV:
                raise
            delete_in_place(live)
            if os.path.lexists(live):
                raise OSError(e.errno, "could not delete %s where it is" % live)
            displaced = False
    try:
        os.rename(staged, live)
    except OSError:
        # Never leave the name with nothing: put the live dir back before
        # surfacing the failure.
        if displaced:
            os.rename(trash, live)
        raise
    return displaced


def gc_orphans():
    now = time.time()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for name in os.listdir(script_dir):
        if not (
            (name.startswith(".skill-sync-") and name.endswith(".py"))
            or (name.startswith(".tree-upload-") and name.endswith(".tar.gz"))
        ):
            continue
        p = os.path.join(script_dir, name)
        try:
            if now - os.lstat(p).st_mtime > ORPHAN_TTL:
                os.unlink(p)
        except OSError:
            pass
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
                    remove_path(p)
            except OSError:
                pass
        elif entry.startswith(DELETING):
            # A delete in place runs only under this flock, so one marked
            # here was cut short. It is only ever of a skill's own name:
            # anything else, as "" or "..", would reach past one. And it took
            # SKILL.md first, so a dir that holds one is a skill made at the
            # name since, or one cut short before anything went: it stays,
            # and only the mark goes.
            name = entry[len(DELETING):]
            p = os.path.join(BASE, name)
            try:
                if (
                    name
                    and not name.startswith(".")
                    and os.path.isdir(p)
                    and not os.path.islink(p)
                    and not os.path.lexists(os.path.join(p, "SKILL.md"))
                ):
                    delete_in_place(p)
                else:
                    os.unlink(os.path.join(BASE, entry))
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
                    replace_live(live, staged, trash)
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


def linkable_dirs(path):
    """Top-level real skill dirs holding a SKILL.md, links excluded."""
    out = set()
    for name in (os.listdir(path) if os.path.isdir(path) else []):
        if name.startswith("."):
            continue
        p = os.path.join(path, name)
        if os.path.isdir(p) and not os.path.islink(p) and os.path.isfile(os.path.join(p, "SKILL.md")):
            out.add(name)
    return out


def is_linked_entry(entry):
    # Mirrors lock.py is_linked.
    sync = entry.get("sync")
    return isinstance(sync, dict) and bool(sync.get("linkedSkillId"))


def shared_tier(entry):
    # Mirrors lock.py is_shared_tier.
    if is_linked_entry(entry):
        return False
    return entry.get("owner") == "platform" or entry.get("sourceType") == MANAGED_SOURCE_TYPE


def unpin_shared_copies(served, own):
    """Drop this tier's copies of skills the shared tier serves, entries and
    dirs alike.

    They arrive with a backup taken while the workspace lived at the computer
    root, where the shared tier was its own. Left as real dirs they would
    outrank the shared skill below and never update again. A name the shared
    tier does not serve yet keeps its copy: a skill just promoted to the user
    tier is ledgered as delivered before the asset sync delivers it. A name
    in ``own`` is a workspace skill whatever its entry says, and the skill
    sync, not this pass, decides its bytes.
    """
    lock_data = read_lock()
    entries = lock_data["skills"]
    unpinned = []
    for name in sorted(entries):
        entry = entries[name]
        # Served names are real directory names, so they are safe to join.
        if (
            name not in served
            or name in own
            or not isinstance(entry, dict)
            or not shared_tier(entry)
        ):
            continue
        p = os.path.join(BASE, name)
        if os.path.isdir(p) and not os.path.islink(p):
            trash = os.path.join(BASE, ".trash-unpin-%s-%d" % (name, time.time_ns()))
            try:
                os.rename(p, trash)
            except OSError as e:
                if e.errno != errno.EXDEV:
                    continue
                # Restored from an archive, it cannot move: deleted where it is.
                delete_in_place(p)
            else:
                shutil.rmtree(trash, ignore_errors=True)
            if os.path.lexists(p):
                continue
        del entries[name]
        unpinned.append(name)
    if unpinned:
        write_lock(lock_data)
    return unpinned


def run_link():
    """Reconcile this tier's links into the computer's shared skill dir.

    The shared dir is read under its own flock too, so an install's swap is
    never seen half done and its skill's link pruned. The order is always
    this tier, then the shared one, and nothing holds the shared flock while
    it waits for a workspace tier's, so the two cannot deadlock.
    """
    user_base = ARGS["userBase"]
    prefix = ARGS["targetPrefix"]
    disabled = set(ARGS.get("disabled") or [])
    own = set(ARGS.get("own") or [])
    fh = acquire_flock()
    # The shared tier's own copies are the ones being linked to.
    same_tier = os.path.realpath(BASE) == os.path.realpath(user_base)
    if same_tier or not os.path.isdir(user_base):
        served = linkable_dirs(user_base)
    else:
        shared_fh = acquire_flock(user_base)
        try:
            served = linkable_dirs(user_base)
        finally:
            fcntl.flock(shared_fh, fcntl.LOCK_UN)
            shared_fh.close()
    unpinned = [] if same_tier else unpin_shared_copies(served, own)
    # A real dir here is the workspace's own skill, which outranks anything
    # it shares a name with in the shared tier. So does a workspace row whose
    # dir is not here: a link would take the name its restore brings back.
    owned = set()
    for name in (os.listdir(BASE) if os.path.isdir(BASE) else []):
        if name.startswith("."):
            continue
        p = os.path.join(BASE, name)
        if os.path.isdir(p) and not os.path.islink(p):
            owned.add(name)
    want = served - owned - own - disabled
    linked, relinked, pruned, blocked = [], [], [], []
    for name in sorted(want):
        link = os.path.join(BASE, name)
        target = prefix + "/" + name
        if os.path.islink(link):
            if os.readlink(link) == target:
                continue
            os.unlink(link)
            relinked.append(name)
        elif os.path.lexists(link):
            blocked.append(name)
            continue
        else:
            linked.append(name)
        os.symlink(target, link)
    for name in (os.listdir(BASE) if os.path.isdir(BASE) else []):
        link = os.path.join(BASE, name)
        if os.path.islink(link) and name not in want:
            os.unlink(link)
            pruned.append(name)
    fcntl.flock(fh, fcntl.LOCK_UN)
    print(json.dumps({
        "status": "ok",
        "linked": linked,
        "relinked": relinked,
        "pruned": sorted(pruned),
        "blocked": blocked,
        "unpinned": unpinned,
    }))


def protected_entry(entry):
    # Mirrors lock.py is_agent_installed / is_linked: content the asset sync
    # may never displace.
    return (
        entry.get("owner") == "user" and entry.get("sourceType") != MANAGED_SOURCE_TYPE
    ) or is_linked_entry(entry)


def merge_authoritative(auth):
    """merge_lock_files, but against a fresh read under the caller's flock.

    The asset-sync path computes its authoritative (platform + managed)
    entries from a lock snapshot taken before a multi-second prune/upload;
    writing that merge back blind would clobber anything the reconciler
    committed in between. Re-reading here closes the window. Entries the
    authoritative set may never displace always survive from the fresh read,
    and a racing claim on an authoritative name is reported rather than taken.
    """
    entries = read_lock().get("skills", {})
    merged = {}
    skipped = []
    for name, entry in entries.items():
        if protected_entry(entry):
            merged[name] = entry
            if name in auth:
                skipped.append(name)
    for name, entry in auth.items():
        if name not in skipped:
            merged[name] = entry
    write_lock({"version": LOCK_VERSION, "skills": merged})
    return merged, skipped


def run_install():
    """Swap the asset sync's staged skill dirs into place, then merge its
    entries, in one flock hold.

    A workspace link pass prunes every link the shared tier does not serve,
    so a skill must never be absent here while its new bytes upload. They
    land in a staging dir and replace the live dir by rename under the flock
    the link pass reads this tier under. A name claimed since the
    caller's snapshot keeps its bytes as well as its entry. A staged dir that
    is already gone was swapped by an earlier attempt of this same exec, so a
    retry is harmless.
    """
    staging = ARGS["staging"]
    tag = os.path.basename(staging)
    fh = acquire_flock()
    gc_orphans()
    entries = read_lock().get("skills", {})
    held = set()
    trashed = []
    for name in ARGS["names"]:
        if protected_entry(entries.get(name) or {}):
            held.add(name)
            continue
        staged = os.path.join(staging, name)
        if not os.path.isdir(staged):
            continue
        live = os.path.join(BASE, name)
        trash = os.path.join(BASE, ".trash-%s-%s" % (tag, name))
        try:
            displaced = replace_live(live, staged, trash)
        except OSError as e:
            fail("install %s: %s" % (name, e))
        if displaced:
            trashed.append(trash)
    merged, skipped = merge_authoritative(ARGS["entries"])
    fcntl.flock(fh, fcntl.LOCK_UN)
    # Outside the flock: a link pass waits on it, and a big tree takes a while
    # to delete. A trash dir nothing deletes is swept once it is an hour old.
    for trash in trashed:
        try:
            remove_path(trash)
        except OSError:
            pass
    shutil.rmtree(staging, ignore_errors=True)
    print(json.dumps({
        "status": "ok",
        "skills": merged,
        "skipped": sorted(held | set(skipped)),
    }))


if MODE == "report":
    run_report()
elif MODE == "apply":
    run_apply()
elif MODE == "download":
    run_download()
elif MODE == "install":
    run_install()
elif MODE == "link":
    run_link()
else:
    fail("unknown mode")
''')


def _build_script(args: dict[str, Any]) -> bytes:
    args = {
        "lockVersion": LOCK_FILE_VERSION,
        "managedSourceType": MANAGED_SOURCE_TYPE,
        "orphanTtl": ORPHAN_TTL_SECONDS,
        **args,
    }
    return _SCRIPT.replace("__ARGS_JSON__", repr(json.dumps(args))).encode()


async def _run(
    sandbox: "PTCSandbox", args: dict[str, Any], *, retry_policy: RetryPolicy
) -> dict[str, Any]:
    assert sandbox.runtime is not None
    script = f"{SandboxLayout(sandbox._work_dir).internal}/.skill-sync-{secrets.token_hex(8)}.py"
    try:
        await sandbox._runtime_call(
            sandbox.runtime.upload_file,
            _build_script(args),
            script,
            retry_policy=RetryPolicy.SAFE,
        )
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"python3 -I {shlex.quote(script)}",
            retry_policy=retry_policy,
        )
    finally:
        with anyio.move_on_after(_CLEANUP_TIMEOUT_SECONDS, shield=True):
            try:
                await sandbox._runtime_call(
                    sandbox.runtime.exec,
                    f"rm -f {shlex.quote(script)}",
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception:
                logger.debug("Could not remove skill-sync script", path=script)
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


async def report(
    sandbox: "PTCSandbox", *, base: str | None = None
) -> dict[str, Any]:
    """Scan one tier: ``{name: {present, wellFormed, syncable, treeHash,
    frontmatter, entry, ...}}`` for the union of skill dirs and ledger names.

    Links are not skill dirs here, so a workspace tier reports only the skills
    it owns and never the shared ones it points at.
    """
    payload = await _run(
        sandbox,
        {"mode": "report", "base": base or skills_base(sandbox)},
        retry_policy=RetryPolicy.SAFE,
    )
    return payload["skills"]


async def apply_actions(
    sandbox: "PTCSandbox",
    actions: list[dict[str, Any]],
    *,
    base: str | None = None,
) -> list[dict[str, Any]]:
    """Execute the write phase; returns per-action results (``ok``/``drift``)."""
    if not actions:
        return []
    payload = await _run(
        sandbox,
        {
            "mode": "apply",
            "base": base or skills_base(sandbox),
            "actions": actions,
        },
        retry_policy=RetryPolicy.UNSAFE,
    )
    return payload["results"]


async def install_staged_skills(
    sandbox: "PTCSandbox",
    staging: str,
    names: Iterable[str],
    entries: dict[str, Any],
    *,
    base: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Rename each of ``names`` from ``staging`` over its live dir, then fold
    the asset sync's authoritative ``entries`` into the live lock, all under
    the tier's flock.

    The asset sync's way to replace shared skills without ever leaving one
    missing for a link pass on another worker to prune against. Returns
    ``(merged_lock_file, skipped_names)``: the merged file is the post-write
    truth, read fresh rather than merged over the caller's stale snapshot,
    and a skipped name was claimed agent-installed or linked since that
    snapshot, so neither its bytes nor its entry were replaced. Removes
    ``staging`` once it succeeds.
    """
    payload = await _run(
        sandbox,
        {
            "mode": "install",
            "base": base or skills_base(sandbox),
            "staging": staging,
            "names": sorted(names),
            "entries": entries,
        },
        retry_policy=RetryPolicy.SAFE,
    )
    merged = {"version": LOCK_FILE_VERSION, "skills": payload["skills"]}
    return merged, payload.get("skipped", [])


async def link_shared_skills(
    sandbox: "PTCSandbox",
    *,
    base: str,
    user_base: str,
    disabled: Iterable[str] = (),
    own: Iterable[str] = (),
) -> dict[str, list[str]]:
    """Make ``base`` a complete view of the skills this workspace may use.

    Every shared skill the workspace has not disabled or shadowed gets a
    relative symlink rather than a copy, so the skill corpus's relative
    ``.agents/skills/<name>/...`` cross-references resolve from a workspace
    cwd exactly as they do from the computer root. Returns the names
    ``linked``, ``relinked``, ``pruned``, ``blocked`` (a non-directory
    already holds the name), and ``unpinned`` (a shared skill the tier's
    ledger still claimed, whose entry and any stale copy were dropped). A
    name in ``own``, the workspace's skill rows, is never unpinned or linked.

    A no-op when the two tiers are the same directory, which is what a
    computer holding a single workspace at its root looks like.
    """
    empty: dict[str, list[str]] = {
        "linked": [],
        "relinked": [],
        "pruned": [],
        "blocked": [],
        "unpinned": [],
    }
    if base.rstrip("/") == user_base.rstrip("/"):
        return empty
    payload = await _run(
        sandbox,
        {
            "mode": "link",
            "base": base,
            "userBase": user_base,
            "targetPrefix": shared_link_target(base, user_base),
            "disabled": sorted(disabled),
            "own": sorted(own),
        },
        retry_policy=RetryPolicy.SAFE,
    )
    return {key: payload.get(key) or [] for key in empty}


async def download_tree(
    sandbox: "PTCSandbox",
    name: str,
    *,
    max_files: int,
    max_file_bytes: int,
    max_total_bytes: int,
    base: str | None = None,
) -> tuple[bytes, str]:
    """Bounded zip of one skill dir → ``(zip_bytes, tree_hash)``.

    The hash is computed from the exact bytes zipped, so the caller can stamp
    the ledger against what it validated, not what a racing agent wrote since.
    """
    base = base or skills_base(sandbox)
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
        sandbox.runtime.exec,
        f"rm -f {shlex.quote(out)}",
        retry_policy=RetryPolicy.SAFE,
    )
    if raw is None:
        raise SkillSyncError("download produced no bytes")
    data = raw if isinstance(raw, bytes) else raw.encode()
    if len(data) > max_total_bytes + 1_048_576:
        raise SkillSyncError("downloaded archive exceeds the transfer ceiling")
    return data, payload["treeHash"]


async def stage_skill_files(
    sandbox: "PTCSandbox",
    files: list[tuple[str, bytes]],
    *,
    base: str | None = None,
) -> str:
    """Upload a skill's files into a fresh staging dir; returns its path.

    ``files`` are ``(relpath, content)`` pairs from the validated archive.
    The subsequent ``swap_staged`` apply-op renames this dir into place
    atomically — the live dir is never rm -rf'd first.
    """
    assert sandbox.runtime is not None
    base = base or skills_base(sandbox)
    token = secrets.token_hex(8)
    staged = f"{base}/.staging/push-{token}"
    subdirs = {
        f"{staged}/{'/'.join(rel.split('/')[:-1])}" for rel, _ in files if "/" in rel
    }
    await upload_tree(
        sandbox,
        [(content, f"{staged}/{rel}") for rel, content in files],
        dirs=sorted({staged, *subdirs}),
    )
    return staged
