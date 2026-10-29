"""Seam: moving workspace folders to follow their names, and clearing deleted ones.

One file of the ComputerManager split; see the package __init__.

A rename writes the name only. The folder follows the next time the computer
is acquired, before any folder on it is used: that is the one moment nothing
on the computer runs (while anything does, the folders wait for a later
acquisition) and every sandbox path is about to be resolved from the row anyway.
"""

import asyncio
import base64
import json
import logging
import shlex
import textwrap
import uuid
import zlib
from pathlib import Path
from typing import Any, Dict, Optional

import anyio
from ptc_agent.core.paths import SandboxLayout, workspace_root
from ptc_agent.core.sandbox.livefs_runtime import tree_runtime

from src.server.database.computer import DEFAULT_ROOT_DIR
from src.server.database.session_lock import await_settled
from src.server.database.workspace import (
    complete_workspace_folder_cleanup,
    defer_workspace_folder_cleanup,
    get_workspace_dir_name,
)
from src.server.database.workspace_folders import (
    FOLDER_LAYOUT_VERSION,
    LEFTOVERS_DIR,
    MOVING_DIR,
    accepted_landings,
    busy_workspace_ids,
    computer_command_running,
    computer_run_in_progress,
    hold_workspace_folder,
    is_top_level,
    leftovers_path,
    plan_folder_moves,
    read_folder_rows,
    record_folder_blocked,
    record_folder_landings,
    retarget_folder_cleanup,
    stage_folder_moves,
    workspace_folders_lock,
)
from src.server.services.persistence.transfer import SYNC_MARKER_NAME

logger = logging.getLogger(__name__)

_EXEC_TIMEOUT_S = 60
# The script ends itself this long after it starts: room for its 30 s wait on
# the ledger lock and then the moves, and inside the exec timeout, so a pass
# the host still waits for has reported or died by then.
_SCRIPT_LIMIT_S = 45
# From the exec reaching the sandbox to the script arming its limit.
_SCRIPT_START_S = 5

# Runs in the sandbox as one pass, holding the tool ledger's flock from the
# first rename to the ledger rewrite: the ledger records each claim's folder,
# and a sibling's sync reads a missing folder as a deleted workspace.
_ALARM = textwrap.dedent(r"""
    import signal, sys
    # First, so a pass the host lost track of is dead before the host lets its
    # locks go. The default action rather than a handler, which Python runs
    # only between bytecodes; dying anywhere leaves what a crash leaves, and
    # the journal below recovers that. Reset, since an ignored or blocked
    # signal survives exec.
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGALRM])
    signal.alarm(int(sys.argv[2]))
""")
_PASS = textwrap.dedent(r"""
    import datetime, fcntl, json, os, secrets, time
    # Copies and deletes in place stop here, and leave the rest of the limit
    # to the landings and the ledger; the next pass resumes them.
    DEADLINE = time.time() + int(sys.argv[2]) - 10
    # Entries a pass counts on deleting in place each second; one restored
    # sandbox measured three times this.
    DELETE_RATE = 10000
    with open(sys.argv[1], encoding="utf-8") as fh:
        A = json.load(fh)
    os.unlink(sys.argv[1])
    ROOT = A["root"]
    COPIES = A["moving"] + "/copies"

    def path(rel):
        return os.path.join(ROOT, rel)

    def exists(rel):
        return bool(rel) and os.path.lexists(path(rel))

    def rename(src, dst):
        os.makedirs(os.path.dirname(path(dst)), exist_ok=True)
        os.rename(path(src), path(dst))

    out = {
        "moves": {}, "tombstones": {}, "evicted": [], "errors": [],
        "progress": [], "blocked": {}, "pending": [], "deleted": [],
    }
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    class Pending(Exception):
        # A copy or a delete in place the deadline cut short; a later pass
        # takes it up where it stopped.
        pass

    class Leaving(Pending):
        # Past a copy's commit: the content is where the copy went, and the
        # folder it came from is part deleted.
        pass

    def aside_of(rel):
        # "e:<rel>" is a copy aside of the folder ``rel``, as [where it goes,
        # the folder's inode, the row it clears the way for], recorded before
        # the copy: a pass the copy or the delete after it outlasts leaves
        # it, and the next goes on from there rather than copying from the
        # start, into a second folder once the first is whole. Only a copy
        # under leftovers named for ``rel`` counts.
        entry = journal.get("e:" + rel)
        if not (isinstance(entry, list) and len(entry) == 3 and isinstance(entry[0], str)):
            return None
        away = entry[0]
        if os.path.dirname(away) == A["leftovers"] and os.path.basename(away).startswith(
            aside_base(rel) + "-"
        ):
            return entry
        return None

    def aside_base(rel):
        # Cut so the suffix still fits a 255-byte name, or the rename fails
        # on every pass and the row it clears the way for never lands.
        return os.path.basename(rel).encode("utf-8")[:200].decode("utf-8", "ignore")

    def out_of_room(owner, said):
        # A copy the disk had no room for goes before its row is blocked: a
        # blocked row runs no pass until the block ends, and the disk the copy
        # took would stay taken all that time. "n:<id>" marks it going, so a
        # pass the delete outlasts leaves the rest to the next, which deletes
        # it rather than copying on and then blocks the row.
        key = "n:" + owner
        if not isinstance(journal.get(key), str):
            journal[key] = str(said)
            save_journal()
        asides = {}
        for k in journal:
            entry = aside_of(k[2:]) if k.startswith("e:") else None
            if entry and entry[2] == owner:
                asides[k] = entry[0]
        for part in [COPIES + "/" + owner] + [away + ".copying" for away in asides.values()]:
            if not remove_tree(path(part), DEADLINE + 5):
                raise Pending("%s, deleting what it copied" % journal[key])
        for k, away in asides.items():
            if not exists(away):
                journal.pop(k)
        said = journal.pop(key)
        save_journal()
        raise NoRoom(said)

    def set_aside(rel, mid):
        if "n:" + mid in journal:
            out_of_room(mid, journal["n:" + mid])
        key = "e:" + rel
        entry = aside_of(rel)
        if entry is not None and os.lstat(path(rel)).st_ino == entry[1]:
            away, ino = entry[0], entry[1]
        else:
            # Another folder by now, or one an archive numbered anew, which
            # took any copy so far into the layer too, where it can no longer
            # be renamed whole; a part copy left goes with the next prune.
            journal.pop(key, None)
            away = "%s/%s-%s-%s" % (A["leftovers"], aside_base(rel), stamp, secrets.token_hex(3))
            try:
                rename(rel, away)
                out["evicted"].append([rel, away])
                return
            except OSError as exc:
                if not cannot_rename(exc):
                    raise
            ino = os.lstat(path(rel)).st_ino
        journal[key] = [away, ino, mid]
        save_journal()
        try:
            if not exists(away):
                try:
                    whole = copy_aside(path(rel), path(away), DEADLINE)
                except NoRoom:
                    out_of_room(mid, "no room to copy %s aside" % rel)
                if not whole:
                    raise Pending("copying %s aside" % rel)
            if not remove_tree(path(rel), DEADLINE):
                # Copied whole, with what the delete left still in the way.
                raise Pending("deleting %s where it is" % rel)
            journal.pop(key)
            save_journal()
        finally:
            # Past a copy's commit ``away`` holds all of it, whatever the
            # delete in place did after.
            if exists(away):
                out["evicted"].append([rel, away])

    os.makedirs(os.path.dirname(A["lock"]), exist_ok=True)
    lock = open(A["lock"], "a+")
    deadline = time.time() + 30
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if time.time() > deadline:
                # Nothing moved: a row staged this pass goes back to its folder.
                out["errors"].append("ledger flock timeout")
                for m in A["moves"]:
                    if not m.get("staged") and m.get("source"):
                        out["moves"][m["id"]] = m["source"]
                print(json.dumps(out))
                sys.exit(0)
            time.sleep(0.2)

    # Where each landing put a folder, written before the rename, taken back if
    # the rename fails, and dropped once a later pass moves the content on: a
    # pass whose landings never reached the database leaves its rows staged,
    # and by the next pass a rename may have changed their target, so only
    # this says where the content went. "t:<id>" is the same for a deleted
    # workspace's folder moved aside, kept while the row is still to clear,
    # this pass or a later one. An entry for a row the database no longer has
    # staged is stale, and goes before anything moves.
    journal_path = path(A["moving"] + "/landed.json")
    try:
        with open(journal_path, encoding="utf-8") as fh:
            journal = json.load(fh)
    except (OSError, ValueError):
        journal = {}
    if not isinstance(journal, dict):
        journal = {}

    def save_journal():
        os.makedirs(os.path.dirname(journal_path), exist_ok=True)
        tmp = "%s.%d.tmp" % (journal_path, os.getpid())
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(journal, fh)
        os.replace(tmp, journal_path)

    if A.get("prune"):
        keep = {r["id"] for r in A["moves"] + A["tombstones"] if r.get("staged")}
        keep |= {"c:" + k for k in keep}
        keep |= {"t:" + tid for tid in [r["id"] for r in A["tombstones"]] + A["deferred"]}
        # A move's copy whose delete was cut short, for its rollback to finish.
        keep |= {k for k in journal if k.startswith("c:move-")}
        # A copy aside whose delete was cut short, while what it left is
        # there, and one the deadline cut short, while that folder is there
        # and the row it clears the way for still has to move there. A row
        # this pass moves clears only its target and its staging now; one
        # renamed again since leaves the old target's part copy to go below.
        to_move = set(A.get("copying") or ())
        keep |= {"n:" + k for k in to_move}
        ways = {m["id"]: (m["target"], A["moving"] + "/" + m["id"]) for m in A["moves"]}
        for k in [k for k in journal if k.startswith("e:")]:
            entry = aside_of(k[2:])
            if entry and exists(k[2:]) and (
                exists(entry[0])
                or (entry[2] in to_move and k[2:] in ways.get(entry[2], (k[2:],)))
            ):
                keep.add(k)
        journal = {k: v for k, v in journal.items() if k in keep}
        save_journal()
        # A copy toward a name the row no longer moves to: what it wrote is
        # new, so removing it gives its disk back. One whose commit is
        # journaled stays, as only its being there says the commit never ran.
        copying = to_move | {k[2:] for k in journal if k.startswith("c:")}
        for name in os.listdir(path(COPIES)) if os.path.isdir(path(COPIES)) else ():
            if name not in copying and not remove_tree(path(COPIES + "/" + name), DEADLINE):
                break
        # The same for a copy aside no entry goes on from.
        going = {aside_of(k[2:])[0] + ".copying" for k in journal if k.startswith("e:")}
        leftovers = path(A["leftovers"])
        for name in os.listdir(leftovers) if os.path.isdir(leftovers) else ():
            left = A["leftovers"] + "/" + name
            if name.endswith(".copying") and left not in going and not remove_tree(path(left), DEADLINE):
                break

    def journaled(key, value, src, dst):
        journal[key] = value
        save_journal()
        try:
            rename(src, dst)
        except OSError:
            journal.pop(key, None)
            save_journal()
            raise

    def landing(key, planned=()):
        # Code in this sandbox can write the journal, and followed, an entry
        # naming another row's folder moves that folder into this row. So only
        # what a pass could have written counts: a deleted workspace's folder
        # goes to its own leftovers, a row onto a folder the database recorded
        # a pass planning for it. Anything else is as if never journaled.
        at = journal.get(key)
        if key.startswith("t:"):
            ok = at == A["leftovers"] + "/" + key[2:]
        else:
            ok = isinstance(at, str) and at in planned
        if at is not None and not ok:
            out["errors"].append("journal %s: ignored %r" % (key, at))
        return at if ok else None

    def former(row):
        return (
            (row.get("source"), A["moving"] + "/" + row["id"])
            + tuple(row.get("landings") or ())
        )

    def finish_leftover(row):
        # "c:<id>" is a copy that took a row's content where a rename could
        # not, as [the folder it came from, the copy's inode, where the copy
        # went]. The folder is what the copy's delete left. The copy is the
        # very folder by its inode, or, as an archive numbers every inode
        # anew, by being gone from where it was built: kept there until the
        # commit's rename, so a folder made later at that path never counts.
        # It names only a folder of the row's own, as a landing does. Raises
        # Leaving until the delete is done.
        key = "c:" + row["id"]
        entry = journal.get(key)
        if entry is None:
            return
        at, ino, copy_at = (
            entry if isinstance(entry, list) and len(entry) == 3 else (None, None, None)
        )
        if not isinstance(at, str):
            # A row with no source has None among its folders.
            at = None
        st = os.lstat(path(copy_at)) if isinstance(copy_at, str) and exists(copy_at) else None
        moved = st is not None and (st.st_ino == ino or not exists(COPIES + "/" + row["id"]))
        # A folder another row's landing names is that row's content.
        theirs = any(
            journal.get(r["id"]) == at for r in A["moves"] + A["tombstones"]
            if r["id"] != row["id"]
        )
        foreign = at is None or at not in former(row) or theirs
        if foreign or not moved:
            if foreign:
                out["errors"].append("journal %s: ignored %r" % (key, entry))
            journal.pop(key)
            save_journal()
            return
        try:
            # The sync marker first: cut short, what is left reads as a
            # folder that lost files, which the next attach restores, rather
            # than as whole.
            try:
                os.unlink(path(at + "/" + A["marker"]))
            except FileNotFoundError:
                pass
            gone = remove_tree(path(at), DEADLINE)
        except OSError as exc:
            gone = False
            out["errors"].append("deleting %s where it is: %s" % (at, exc))
        if not gone:
            held.add(at.casefold())
            raise Leaving("deleting %s where it is" % at)
        journal.pop(key)
        if journal.get(row["id"]) == at:
            journal.pop(row["id"])
        try:
            save_journal()
        except OSError as exc:
            # Past the commit whatever fails: the entry left on disk finds
            # nothing to delete next pass.
            raise Leaving("journal: %s" % exc)

    def copy_over(row, src, dest, also=None):
        # Moves the folder ``src`` to ``dest`` where a rename cannot, as a
        # copy and a delete in place. The copy builds under COPIES while
        # ``src`` stays whole and in use between passes, and becomes ``dest``
        # in one rename journaled first: past that ``dest`` is the content,
        # and the delete that follows resumes from the journal.
        copy = COPIES + "/" + row["id"]
        if "n:" + row["id"] in journal:
            out_of_room(row["id"], journal["n:" + row["id"]])
        os.makedirs(path(COPIES), exist_ok=True)
        try:
            synced = sync_tree(path(src), path(copy), DEADLINE)
        except NoRoom:
            # What it wrote is new, so removing it gives the space back.
            out_of_room(row["id"], "no room to copy %s" % src)
        if synced is None:
            raise Pending("copying %s" % src)
        entries, written = synced
        if written * 20 > entries and time.time() + entries / DELETE_RATE > DEADLINE:
            # A pass that did much of the copy leaves the delete to the next,
            # which finds the copy whole. One that only caught up on a folder
            # in use between passes goes on, or that folder would never move.
            raise Pending("copied %s, to delete it next pass" % src)
        keys = {"c:" + row["id"]: [src, os.lstat(path(copy)).st_ino, dest], **(also or {})}
        journal.update(keys)
        try:
            save_journal()
            rename(copy, dest)
        except OSError as exc:
            for key in keys:
                journal.pop(key, None)
            save_journal()
            if not cannot_rename(exc):
                raise
            # An archive since the copy began took it into the layer too.
            remove_tree(path(copy), DEADLINE)
            raise Pending("recopying %s" % src)
        finish_leftover(row)

    # Folders that must not be landed on this pass: a deleted workspace's row
    # still points at one it could not clear, a row that could not stage
    # still holds its own, and a copy's delete left part of one.
    held = set()

    # A staged row whose copy's delete was cut short has its content in
    # staging, and what is left of its old folder goes before any landing.
    leaving = set()
    for m in A["moves"]:
        if not m.get("staged"):
            continue
        try:
            finish_leftover(m)
        except Pending as exc:
            leaving.add(m["id"])
            out["progress"].append("stage %s: %s" % (m["id"], exc))
        except OSError as exc:
            leaving.add(m["id"])
            out["errors"].append("stage %s: %s" % (m["id"], exc))

    # Where each staged row's last landing left its content. That can be
    # another row's old folder (two rows traded names, or one took the other's
    # former name), so no row falls back to an old folder claimed here. No pass
    # lands two rows on one folder, so of two claims to one at least one is
    # false; nothing says which, and the folder is left to nobody.
    trusted, claims = {}, {}
    for m in A["moves"]:
        staged_here = m.get("staged") and m["id"] not in leaving
        at = landing(m["id"], m.get("landings") or ()) if staged_here else None
        if exists(at):
            trusted[m["id"]] = at
            claims.setdefault(at.casefold(), []).append(m["id"])
    claimed = {}
    for folded, ids in claims.items():
        claimed[folded] = ids[0] if len(ids) == 1 else None
        if len(ids) > 1:
            for mid in ids:
                out["errors"].append("journal %s: ignored %r" % (mid, trusted.pop(mid)))

    def unclaimed(rel, owner):
        return exists(rel) and claimed.get(rel.casefold(), owner) == owner

    def clear(t, src, dst):
        # Out of its name and under leftovers, by a rename. One a rename
        # cannot move goes as the host asked: "delete" where it is, as
        # nothing of a deleted workspace is kept, or "copy" under leftovers,
        # which leaves it as it was when the copy has no room, for the host
        # to decide.
        tid, stuck = t["id"], t.get("stuck")
        if exists(dst):
            try:
                rename(dst, dst + "-" + secrets.token_hex(3))
            except OSError as exc:
                if not (cannot_rename(exc) and stuck == "delete"):
                    raise
                # What an earlier clearing of this same workspace left.
                if not remove_tree(path(dst), DEADLINE):
                    raise Pending("deleting %s where it is" % dst)
        try:
            journaled("t:" + tid, dst, src, dst)
            return
        except OSError as exc:
            if not (cannot_rename(exc) and stuck in ("delete", "copy")):
                raise
        if stuck == "copy":
            copy_over(t, src, dst, also={"t:" + tid: dst})
            return
        if not remove_tree(path(src), DEADLINE):
            raise Pending("deleting %s where it is" % src)
        out["deleted"].append(tid)

    for t in A["tombstones"]:
        tid = t["id"]
        dst = A["leftovers"] + "/" + tid
        src = None
        try:
            # A copy took its content and its delete was cut short: the
            # rest goes first, wherever the content is now.
            finish_leftover(t)
            if t.get("stuck") == "finish":
                # A move's rollback: only what its copy left goes, the old
                # folder's remains or a copy that never landed.
                if not remove_tree(path(COPIES + "/" + tid), DEADLINE):
                    raise Pending("deleting the copy for %s" % tid)
                out["tombstones"][tid] = dst if exists(dst) else None
                continue
            done = landing("t:" + tid)
            if exists(done):
                # An earlier pass moved it and never recorded that: whatever
                # sits at its old folder now is another workspace's. An entry
                # whose folder is missing was written by a pass that died
                # before the rename, so the folder is still where the row says.
                out["tombstones"][tid] = done
                continue
            if t.get("staged"):
                # Mid-move: its last landing, its staging, or a source never staged.
                places = (landing(tid, t.get("landings") or ()), t["dir"], t.get("source"))
            else:
                places = (t["dir"],)
            # Its clearing ends in an rm, so never a folder a live row's landing claims.
            src = next((p for p in places if unclaimed(p, tid)), None)
            if src is not None:
                clear(t, src, dst)
            out["tombstones"][tid] = dst if exists(dst) else None
        except (Pending, NoRoom, OSError) as exc:
            if src:
                held.add(src.casefold())
            if isinstance(exc, NoRoom):
                out["blocked"][tid] = str(exc)
            if isinstance(exc, Pending):
                out["pending"].append(tid)
            if isinstance(exc, (Pending, NoRoom)):
                out["progress"].append("tombstone %s: %s" % (tid, exc))
            else:
                out["errors"].append("tombstone %s: %s" % (tid, exc))

    # Everything moving leaves first, so a folder one row vacates is free for
    # the next, and two rows can trade names.
    def to_stage(m, src, stage):
        try:
            rename(src, stage)
        except OSError as exc:
            if not cannot_rename(exc):
                raise
            copy_over(m, src, stage)

    staged, failed = {}, set()
    for m in A["moves"]:
        mid = m["id"]
        stage = A["moving"] + "/" + mid
        landed_at = trusted.get(mid)
        if mid in leaving:
            # Its content is in staging until what its copy left is gone.
            out["moves"][mid] = stage
            failed.add(mid)
            continue
        if m["target"].casefold() in held and (landed_at or not exists(stage)):
            # Staged now, it could only wait there, and a row in staging takes
            # no turns: it stays where its content is until the target clears.
            out["progress"].append("stage %s: %s is held this pass" % (mid, m["target"]))
            failed.add(mid)
            here = landed_at or m.get("source")
            if here:
                out["moves"][mid] = here
                held.add(here.casefold())
            continue
        try:
            if landed_at:
                # The last landing happened. A staging folder since then holds
                # only what an acquisition wrote to the row's staged path.
                if exists(stage):
                    set_aside(stage, mid)
                to_stage(m, landed_at, stage)
            else:
                if not m.get("staged") and exists(stage):
                    # Not this row's content: the row was not staged until now.
                    set_aside(stage, mid)
                if not exists(stage) and unclaimed(m.get("source"), mid):
                    to_stage(m, m["source"], stage)
            staged[mid] = exists(stage)
        except Leaving as exc:
            # Copied into staging, with its old folder still to delete.
            out["progress"].append("stage %s: %s" % (mid, exc))
            out["moves"][mid] = stage
            failed.add(mid)
        except (Pending, NoRoom, OSError) as exc:
            if isinstance(exc, NoRoom):
                out["blocked"][mid] = str(exc)
            if isinstance(exc, (Pending, NoRoom)):
                out["progress"].append("stage %s: %s" % (mid, exc))
            else:
                out["errors"].append("stage %s: %s" % (mid, exc))
            failed.add(mid)
            # Point the row at wherever its content still is, and keep every
            # landing off it; a row with its content staged stays staged.
            if landed_at:
                here = landed_at if exists(landed_at) else None
            else:
                # The one rename that can fail here takes its old folder into
                # staging, so the content is still there.
                here = m.get("source")
            if here:
                out["moves"][mid] = here
                held.add(here.casefold())
    # An entry left naming a folder the content has left would claim whatever
    # a later pass lands there; only one whose content is still in it stays.
    # So does one naming the target a copy landed on while that copy's delete
    # is still going, the one record of where the content went.
    def copied_to(mid):
        entry = journal.get("c:" + mid)
        return entry[2] if isinstance(entry, list) and len(entry) == 3 else None

    stale = [
        m["id"] for m in A["moves"]
        if m.get("staged") and m["id"] in journal
        and not (m["id"] in failed and m["id"] in trusted)
        and not (m["id"] in leaving and journal[m["id"]] == copied_to(m["id"]))
    ]
    if stale:
        for mid in stale:
            del journal[mid]
        try:
            save_journal()
        except OSError as exc:
            out["errors"].append("journal: %s" % exc)

    def land(m, stage, target):
        # By a rename, or by a copy where staging is a folder a rename cannot
        # move: one a row left staged across an archive.
        try:
            journaled(m["id"], target, stage, target)
        except OSError as exc:
            if not cannot_rename(exc):
                raise
            copy_over(m, stage, target, also={m["id"]: target})

    targets = {m["target"].casefold() for m in A["moves"]}

    def put_back(m, stage):
        # A row that cannot land this pass: back under its old name, which
        # staging left free, rather than staged and shut out of every turn
        # until it can. Never a name a row lands on.
        src = m.get("source")
        if not src or exists(src) or src.casefold() in targets | landed | held:
            return None
        try:
            rename(stage, src)
        except OSError:
            return None
        return src

    landed = set()
    for m in A["moves"]:
        mid, target = m["id"], m["target"]
        stage = A["moving"] + "/" + mid
        if mid in failed:
            continue
        if target.casefold() in held:
            out["errors"].append("land %s: %s is held this pass" % (mid, target))
            if staged.get(mid):
                out["moves"][mid] = put_back(m, stage) or stage
            continue
        try:
            if exists(target):
                if target.casefold() in landed:
                    raise OSError("target filled by another move in this pass")
                # No row holds it, or the plan would not land here: an unowned
                # folder, kept where nothing will mistake it for this workspace.
                set_aside(target, mid)
            if staged.get(mid):
                land(m, stage, target)
            # Otherwise nothing is anywhere, a folder never made or a sandbox
            # rebuilt empty, and the name alone is this row's.
            out["moves"][mid] = target
            landed.add(target.casefold())
        except Leaving as exc:
            # Copied onto its target, with what staging held still to delete.
            out["progress"].append("land %s: %s" % (mid, exc))
            out["moves"][mid] = stage
            landed.add(target.casefold())
        except (Pending, NoRoom, OSError) as exc:
            if isinstance(exc, NoRoom):
                out["blocked"][mid] = str(exc)
            if isinstance(exc, (Pending, NoRoom)):
                out["progress"].append("land %s: %s" % (mid, exc))
            else:
                out["errors"].append("land %s: %s" % (mid, exc))
            if staged.get(mid):
                # Nothing of the row is on its target before a copy's commit,
                # which raises Leaving past it.
                out["moves"][mid] = put_back(m, stage) or stage

    try:
        with open(A["ledger"], encoding="utf-8") as fh:
            ledger = json.load(fh)
    except FileNotFoundError:
        ledger = None
    except (OSError, ValueError) as exc:
        ledger = None
        out["errors"].append("ledger: %s" % exc)
    dirs = ledger.get("dirs") if isinstance(ledger, dict) else None
    if isinstance(dirs, dict):
        for claim, folder in list(out["moves"].items()) + list(out["tombstones"].items()):
            if claim in dirs and folder:
                dirs[claim] = folder
        tmp = "%s.%d.tmp" % (A["ledger"], os.getpid())
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(ledger, fh, sort_keys=True, indent=2)
            fh.write("\n")
        os.replace(tmp, A["ledger"])
    fcntl.flock(lock, fcntl.LOCK_UN)
    print(json.dumps(out))
""")


_SCRIPT = _ALARM + Path(tree_runtime.__file__).read_text(encoding="utf-8") + _PASS


def _command(args_path: str) -> str:
    # Compressed, as it carries ``tree_runtime`` whole, and the command is one
    # argument string the kernel caps at 128 KiB.
    code = base64.b64encode(zlib.compress(_SCRIPT.encode("utf-8"), 9)).decode("ascii")
    # Isolated, because the sandbox runs this from the computer root: a
    # workspace named ``json`` holding an ``__init__.py`` would be imported in
    # place of the standard library's, and fail every retry with rows staged.
    return (
        "python3 -I -c \"import base64,zlib;"
        f"exec(zlib.decompress(base64.b64decode('{code}')).decode())\" "
        f"{shlex.quote(args_path)} {_SCRIPT_LIMIT_S}"
    )


class FolderScriptNotStarted(RuntimeError):
    """The script never ran, so nothing on the disk moved."""


async def _outlast_script() -> None:
    """Keep the caller's folder locks until a script the host lost is dead.

    A timed-out Docker exec only stops reading, and a failed or cancelled call
    on either provider leaves the command running; it may have started as late
    as the moment the host gave up. Released sooner, the locks would let a file
    change or another settle use folders the script is still moving. The
    caller is often here because it was cancelled, and AnyIO cancels again at
    every await, so the wait is shielded.
    """
    with anyio.CancelScope(shield=True):
        await await_settled(
            asyncio.ensure_future(asyncio.sleep(_SCRIPT_LIMIT_S + _SCRIPT_START_S))
        )


async def _run_folder_script(
    runtime: Any,
    root: str,
    *,
    moves: list,
    tombstones: list,
    prune: bool,
    deferred: tuple[str, ...] = (),
    copying: tuple[str, ...] = (),
) -> Dict[str, Any]:
    layout = SandboxLayout.for_root(root)
    args = {
        "root": root,
        "moving": MOVING_DIR,
        "leftovers": LEFTOVERS_DIR,
        "ledger": layout.union_ledger,
        "lock": layout.union_lock,
        "tombstones": tombstones,
        "moves": moves,
        "deferred": list(deferred),
        "copying": list(copying),
        "marker": SYNC_MARKER_NAME,
        "prune": prune,
    }
    # The plan goes up as a file: a shell command is one argument string, which
    # the kernel caps at 128 KiB, and a plan of long non-Latin names passes that.
    args_path = f"{layout.internal}/.folders_args.{uuid.uuid4().hex}.json"
    try:
        await runtime.upload_file(json.dumps(args).encode("utf-8"), args_path)
    except Exception as e:
        raise FolderScriptNotStarted(f"could not upload the folder plan: {e}") from e
    try:
        result = await runtime.exec(_command(args_path), timeout=_EXEC_TIMEOUT_S)
    except (Exception, asyncio.CancelledError):
        await _outlast_script()
        raise
    lines = (result.stdout or "").strip().splitlines()
    if result.exit_code != 0 or not lines:
        if not (isinstance(result.exit_code, int) and result.exit_code >= 0):
            # No exit status (Docker's -1 on a timeout): it may still be running.
            await _outlast_script()
        raise RuntimeError(
            f"folder script exited {result.exit_code}: {(result.stderr or result.stdout or '')[:500]}"
        )
    report = json.loads(lines[-1])
    if not isinstance(report, dict):
        raise RuntimeError("folder script printed no report")
    return report


class FolderSettleMixin:
    async def _settle_folders(
        self,
        computer_id: str,
        runtime: Any,
        *,
        root: Optional[str] = None,
        own_run_id: Optional[str] = None,
        ignore_busy: bool = False,
    ) -> bool:
        """Move every folder on this computer that can follow its name now.

        True when a row changed. Never raises for a move it could not make: the
        workspace keeps its current folder and the next acquisition retries.
        ``ignore_busy`` is for a sandbox that was just built, where no process
        can be using any folder whatever the run ledger says.
        """
        version, rows = await read_folder_rows(computer_id)
        if (version or 0) < FOLDER_LAYOUT_VERSION or not plan_folder_moves(rows):
            return False
        if not ignore_busy and not plan_folder_moves(
            rows, await busy_workspace_ids(computer_id, rows, own_run_id=own_run_id)
        ):
            # Only a busy workspace would move: nothing to lock for yet.
            return False
        root = root or DEFAULT_ROOT_DIR
        async with workspace_folders_lock(computer_id) as conn:
            if conn is None:
                return False
            plan = await stage_folder_moves(
                conn, computer_id, own_run_id=own_run_id, ignore_busy=ignore_busy
            )
            if not plan:
                return False
            try:
                report = await _run_folder_script(
                    runtime,
                    root,
                    tombstones=[
                        {
                            "id": t.workspace_id,
                            "dir": t.dir_name,
                            "source": t.source,
                            "staged": t.staged,
                            "landings": list(t.landings),
                            # A folder a rename cannot move is deleted where
                            # it is, unless its owner kept it: that one is
                            # copied aside, or stays put with no room to.
                            "stuck": "copy" if t.kept else "delete",
                        }
                        for t in plan.tombstones
                    ],
                    moves=[
                        {
                            "id": m.workspace_id,
                            "source": m.source,
                            "target": m.target,
                            "staged": m.staged,
                            "landings": list(m.landings),
                        }
                        for m in plan.moves
                    ],
                    deferred=plan.deferred,
                    copying=plan.copying,
                    prune=True,
                )
            except FolderScriptNotStarted as e:
                # Nothing moved, as when the script cannot take the ledger lock:
                # a row staged this pass goes back to its folder.
                logger.warning(f"Folder settle on computer {computer_id} did not start: {e}")
                report = {"moves": {m.workspace_id: m.source for m in plan.moves if not m.staged}}
            except Exception as e:
                # The rows stay staged; the next settle finds the content.
                logger.warning(f"Folder settle on computer {computer_id} failed: {e}")
                return True
            for error in report.get("errors") or ():
                logger.warning(f"Folder settle on computer {computer_id}: {error}")
            for step in report.get("progress") or ():
                # A copy or a delete in place the next pass carries on.
                logger.info(f"Folder settle on computer {computer_id}: {step}")
            for folder, away in report.get("evicted") or ():
                logger.info(
                    f"Moved unowned folder {folder!r} on computer {computer_id} to {away}"
                )
            moves, tombstones = accepted_landings(plan, report)
            landed = await record_folder_landings(
                conn, computer_id, moves=moves, tombstones=tombstones
            )
            said = report.get("blocked") if isinstance(report.get("blocked"), dict) else {}
            blocked = {m.workspace_id: m.target for m in plan.moves if m.workspace_id in said}
            if blocked:
                await record_folder_blocked(conn, computer_id, blocked=blocked)
            logger.info(
                f"Settled folders on computer {computer_id}: "
                f"{sorted((m.workspace_id, moves[m.workspace_id]) for m in plan.moves if m.workspace_id in landed)}"
                f", cleared {len(tombstones)} deleted"
            )
            return True

    async def _remove_workspace_folder(
        self,
        workspace_id: str,
        workspace: Dict[str, Any],
        computer: Dict[str, Any],
    ) -> bool:
        """Do not boot stopped machines just to remove mirrored, unreferenced files.

        Recreation restores only live projects from the mirror, removing these
        leftovers without an extra provider round trip. A folder still at the
        top level moves under ``_internal/leftovers`` first, under the folder
        lock, so the name is free before the slow ``rm`` and a workspace that
        reuses it never shares the path being removed."""
        dir_name = (workspace.get("dir_name") or "").strip("/")
        if not (is_top_level(dir_name) or dir_name == leftovers_path(workspace_id)):
            # Without a project folder, the path is the machine runtime root: never unlink it.
            return False
        if dir_name in (".", ".."):
            return False
        provider_ref = computer.get("provider_ref")
        if not provider_ref or computer.get("status") != "running":
            return False

        computer_id = str(computer["computer_id"])
        root = computer.get("root_dir") or DEFAULT_ROOT_DIR
        binding = self._binding_from_computer(workspace_id, computer)
        try:
            # The local handle may be stale or absent; address the durable machine ref.
            async with self._detached_runtime(provider_ref, binding=binding) as runtime:
                from src.server.services.egress.session_binding import refresh_computer_grant_map

                try:
                    await refresh_computer_grant_map(
                        runtime,
                        root=root,
                        computer_id=computer_id,
                        user_id=computer["user_id"],
                    )
                except Exception:
                    logger.warning("Could not refresh egress map after workspace deletion", exc_info=True)
                if is_top_level(dir_name):
                    moved = await self._clear_tombstone_folder(
                        computer_id, runtime, root, workspace_id, dir_name
                    )
                    if moved is None:
                        # Nothing left: never on this disk, or deleted where it was.
                        await complete_workspace_folder_cleanup(
                            workspace_id, computer_id=computer_id, dir_name=dir_name,
                        )
                        return True
                    dir_name = moved
                target = workspace_root(root, dir_name)
                result = await runtime.exec(f"rm -rf {shlex.quote(target)}")
                if result.exit_code != 0:
                    raise RuntimeError(
                        f"folder removal exited {result.exit_code}: {result.stdout}"
                    )
            await complete_workspace_folder_cleanup(
                workspace_id, computer_id=computer_id, dir_name=dir_name,
            )
            logger.info(
                f"Removed folder {target} of workspace {workspace_id} from "
                f"sandbox {provider_ref}"
            )
            return True
        except Exception as e:
            # The tombstoned workspace carries the durable retry claim. Touching
            # it moves a broken candidate behind newer cleanup work.
            try:
                await defer_workspace_folder_cleanup(
                    workspace_id, computer_id=computer_id, dir_name=dir_name,
                )
            except Exception:
                logger.warning(
                    "Could not defer failed folder cleanup for workspace %s",
                    workspace_id,
                    exc_info=True,
                )
            logger.warning(
                f"Could not remove folder {dir_name} of workspace {workspace_id} "
                f"from sandbox {provider_ref}: {e}"
            )
            return False

    async def _clear_tombstone_folder(
        self, computer_id: str, runtime: Any, root: str, workspace_id: str, dir_name: str
    ) -> Optional[str]:
        """Move a deleted workspace's folder aside and record where; None once
        nothing is left, the folder absent or deleted where a rename could not
        move it."""
        async with workspace_folders_lock(computer_id) as conn:
            if conn is None:
                raise RuntimeError("the folder lock is busy")
            # The caller read the row before this lock. A settle since then may
            # have moved the folder aside and handed its name to a live
            # workspace, whose folder the script would otherwise take.
            current = await get_workspace_dir_name(workspace_id, conn=conn)
            if current != dir_name:
                if current not in (None, leftovers_path(workspace_id)):
                    raise RuntimeError(f"deleted workspace now records folder {current!r}")
                return current
            if await computer_run_in_progress(
                conn, computer_id
            ) or await computer_command_running(conn, computer_id):
                # A sibling's code reaches this folder as ../<folder>; a write
                # after the move would recreate the name for the next
                # workspace to take it. The settle defers tombstones the same way.
                raise RuntimeError("a run on the computer may be using the folder")
            if not await hold_workspace_folder(conn, workspace_id):
                # A change that read this folder before the delete is still
                # writing; moving it now would let that write recreate it.
                raise RuntimeError("a file change still holds the folder")
            report = await _run_folder_script(
                runtime,
                root,
                moves=[],
                tombstones=[
                    {
                        "id": workspace_id,
                        "dir": dir_name,
                        "source": dir_name,
                        "staged": False,
                        "stuck": "delete",
                    }
                ],
                prune=False,
            )
            tombstones = report.get("tombstones") or {}
            if not isinstance(tombstones, dict) or workspace_id not in tombstones:
                # A delete in place the time ran out on is retried like a failure.
                said = [*(report.get("errors") or ()), *(report.get("progress") or ())]
                raise RuntimeError("; ".join(map(str, said or ["no report"])))
            moved = tombstones[workspace_id]
            if moved not in (None, leftovers_path(workspace_id)):
                raise RuntimeError(f"folder script reported {moved!r} for a deleted workspace")
            if moved is not None:
                await retarget_folder_cleanup(
                    workspace_id, computer_id=computer_id, dir_name=dir_name, folder=moved
                )
            return moved
