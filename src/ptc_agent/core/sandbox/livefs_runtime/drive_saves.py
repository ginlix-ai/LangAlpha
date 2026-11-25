"""Saving files being written: when each is due, the request, and what its
answer or failure does.

A written file is saved once its writes go quiet (``IDLE_S``), at least
every ``MAX_DIRTY_S`` while they do not, at once on ``fsync``, and when the
command that wrote it ends (``settle``), so a refusal reaches that command's
result rather than its ``close``. One emptied or only made waits for one of
the last two, or for ``CLEAN_IDLE_S``: a shell's ``> file`` empties the file
before its command writes, and that must not save an empty version over the
real one.

Background and settle saves share one pool of ``SAVERS`` threads, so one
held up on the server holds up no other request and the server sees a
bounded number at once. Every failure goes through one step
(``_failed``): a refusal the server made and filed drops the edit; anything
else may not have reached a handler, so the bytes are kept, sent again with
back-off until they land or are refused, and the agent is told once that
the file is not saved yet (``drive_notices``).
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import os
import queue
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from .drive_client import DriveClient, Refused
from .drive_effects import Effects
from .drive_journal import Journal
from .drive_notices import Notices
from .drive_protocol import FILE_MAX_BYTES
from .drive_cache import Cache
from .drive_tree import entry_of
from .drive_tree import Space
from .drive_uploads import Abandoned, Changed, Failed, Restart, Stuck, Upload, Uploads, current
from .drive_notices import LOST, SAVED
from .drive_writes import GIVE_UP_S, KEPT_S, Pending, Phase, Tell, Writes, call_id

#: A written file is saved once this long passes with no change to it: a
#: program's run of writes, and a temp file renamed into place, reach the
#: server as one save.
IDLE_S = 0.3
#: One written to without pause is saved at least this often anyway,
#: counted from its last save. Long enough that a download or an export
#: still being written is not published in pieces (its bytes are in the
#: journal meanwhile), short enough that a log written all day is saved.
MAX_DIRTY_S = 30.0
#: An empty file not settled or synced is saved after this long, and a file
#: opened for writing and never changed is let go.
CLEAN_IDLE_S = 5.0
#: Saves under way at once at most.
SAVERS = 8
#: How often the journal is looked over for records a replaced daemon let go.
ADOPT_EVERY_S = 2.0
#: How often the saver looks for saves that came due.
TICK_S = 0.1


#: Times a save in parts begins again after its upload ended under it.
RESTARTS = 3
#: Saves in parts this daemon has under way at once. The server allows 8
#: open uploads a person, across every computer and the browser, so this
#: daemon leaves room for the others.
PARTS_AT_ONCE = 4
#: A save the server had no upload to spare for (``too_many_uploads``) waits
#: this long, or until one of this daemon's own uploads ends.
SLOT_WAIT_S = 30.0

#: What each code a Drive route answers a save with, or an upload ends
#: ``failed`` with, means for the bytes. These are the Drive refusing them:
#: the server filed the refusal for the command (NOT SAVED), and the edit is
#: dropped. Every other code, and an answer with none, is tried again: the
#: bytes are kept and sent from the begin again, bounded (``GIVE_UP_S``).
#:
#: try again: ``unavailable`` (the store did not answer, or could not read
#: the upload back), ``mismatch`` (the stored bytes are not the ones sent:
#: damaged on the way), ``too_many_uploads`` (wait for a free upload),
#: ``upload_ended``, ``parts_missing``, ``bad_part``, ``bad_digest`` (a step
#: of a save in parts), and anything not listed here.
REFUSED = {
    "too_large": "the file is larger than the Drive takes",
    "over_quota": "the Drive is full",
    "too_many_entries": "the Drive holds as many files as it may",
    "exists": "another file was made at its path",
    "changed": "the file was changed elsewhere since it was read",
    "is_folder": "a folder is at its path",
    "not_folder": "a file is where its folder would be",
    "not_found": "its folder or its Drive is gone",
    "invalid_path": "the Drive cannot hold a file at that path",
    "forbidden": "this computer may not write there",
}


def _unanswered(exc: OSError) -> str | None:
    """Why a failed save is sent again, or None for the Drive refusing its
    bytes (``REFUSED``), which the server filed. Only an answer carrying one
    of those codes is a refusal: any other (a proxy's page, a crash, a 404
    from no Drive route, a store that failed) is tried again."""
    if isinstance(exc, Stuck):
        return exc.reason
    if not isinstance(exc, Refused):
        return "the Drive's server did not answer"
    if exc.status == 401:
        return "the Drive's server did not accept this computer's token"
    if exc.code in REFUSED:
        return None
    if exc.code == "too_many_uploads":
        return "the Drive has as many uploads under way as it allows"
    if exc.code in (None, "unavailable") and exc.status in (429, 503):
        return "the Drive's server was busy"
    if exc.code is None:
        return f"the Drive's server answered {exc.status}, with no reason a Drive gives"
    return f"the Drive's server could not take it ({exc.code})"


@dataclass(frozen=True)
class _Ending:
    """An upload to end: where it would land, the bytes it carries, and
    whether it would make the file there (then taken back if it lands)."""

    space: Space
    upload_id: str
    rel: str
    sha: str
    made: bool


def request_max(p: Pending) -> int:
    """The most one ``write`` request carries; a larger file goes in parts."""
    return int(p.space.limits.get("file_bytes") or FILE_MAX_BYTES)


def ripe(p: Pending, now: float) -> bool:
    if now < p.retry_at:
        return False
    if p.phase is Phase.UNANSWERED:
        return now >= p.retry_at
    if p.phase is not Phase.DIRTY:
        return False
    if p.size == 0:
        return now - p.changed >= CLEAN_IDLE_S
    if p.size > request_max(p):
        # Saved in parts, and given up if written meanwhile: only once quiet.
        return now - p.changed >= IDLE_S
    return now - p.changed >= IDLE_S or now - p.first >= MAX_DIRTY_S


class Saves:
    def __init__(self, writes: Writes, client: DriveClient, cache: Cache, journal: Journal,
                 notices: Notices, effects: Effects, counts: Counter[str]) -> None:
        self.writes = writes
        self.names = writes.names
        self.client = client
        self.cache = cache
        self.journal = journal
        self.notices = notices
        self.effects = effects
        self.counts = counts
        self.uploads = Uploads(client, counts)
        self._queue: queue.SimpleQueue[Pending] = queue.SimpleQueue()
        #: Files queued or being saved by the pool, by id.
        self._busy: set[int] = set()
        self._mu = threading.Lock()
        #: Notified after each save the pool runs, for ``settle``.
        self._done = threading.Condition(self._mu)
        self._workers: list[threading.Thread] = []
        #: Files a settle is waiting on, by id: it tells the agent itself
        #: about those still unanswered when it ends.
        self._settling: Counter[int] = Counter()
        self._adopted_at = 0.0
        #: Files a renamed save made, whose take-back failed: tried again.
        self._taking_back: list[tuple[Space, str, str | None, dict]] = []
        #: Whether the cleanup thread runs: requests that may stall (ending
        #: uploads, taking files back, looking before bytes go) run there,
        #: never on the saver's turn, one pass at a time.
        self._cleaning = False
        #: This daemon's saves in parts under way (``PARTS_AT_ONCE``), the
        #: saves waiting for the server to free an upload, and the uploads
        #: still to end whose end the server did not answer.
        self._slots = threading.BoundedSemaphore(PARTS_AT_ONCE)
        self._waiting: set[Pending] = set()
        self._ending: list[_Ending] = []

    # --- one save ---------------------------------------------------------------

    def large(self, p: Pending) -> bool:
        return p.size > request_max(p)

    def save(self, p: Pending, call: str | None) -> None:
        """Send ``p`` to the server over the head it was edited from; raises
        what the save met."""
        if self.large(p):
            self._save_large(p, call)
            return
        with p.saving:
            self.save_held(p, call)

    def save_held(self, p: Pending, call: str | None) -> None:
        """``save`` with ``p.saving`` held, as a rename does before it moves
        the file. One too large for a request is only queued: its save holds
        ``saving`` only across each of its requests."""
        if self.large(p):
            self.queue(p)
            return
        call = call or p.call
        body = None
        try:
            snap = self.writes.snapshot(p)
            if snap is None:
                return
            body, rev = snap
            self.counts["save"] += 1
            answer = self._sent_once(p, lambda: self.client.write(p.space.ref, p.rel, body, p.base, call))
        except OSError as exc:
            self._failed(p, exc, call, digest=None if body is None else hashlib.sha256(body).hexdigest(),
                         sent=body is not None)
            raise
        self._commit(p, answer, rev, call, body)

    def abandon(self, files: list[Pending | None]) -> None:
        """Stop the saves in parts of ``files`` (being renamed or removed):
        none is waited for (``drive_uploads``). One that went unanswered and
        is not under way now has its upload ended, where it still can be, so
        it does not land after the file moved or went."""
        for p in files:
            if p is None:
                continue
            with p.lock:
                upload, parts = p.upload, p.parts
            if upload is not None:
                if not upload.abandoned.is_set():
                    upload.abandoned.set()
                    self.counts["save_parts_abandoned"] += 1
                continue
            if parts is not None:
                with p.lock:
                    if p.parts == parts:
                        p.parts = None
                self._end(_Ending(p.space, parts[0], p.rel, parts[1], p.base is None))

    def _end(self, ending: _Ending) -> None:
        """End an upload this daemon no longer wants, so it neither lands
        after its file moved or went nor holds one of the person's uploads.
        Ended too late (``done``), what it made is taken back; one the server
        did not answer for is tried again from ``tick`` until it does."""
        try:
            aborted = self.client.upload_cancel(ending.space.ref, ending.upload_id)
        except OSError:
            with self._mu:
                self._ending.append(ending)
            return
        if aborted:
            self.counts["save_parts_ended"] += 1
            self._slot_freed()
        elif ending.made:
            self._take_back(ending.space, ending.rel, ending.sha, {})

    def landed(self, p: Pending) -> dict | None:
        """The server's file at ``p``'s path when it is a save of ``p`` that
        went unanswered and landed after all (which the feed may not have
        shown yet). With ``saving`` held."""
        if not p.sent:
            return None
        there = self.client.entry(p.space.ref, p.rel)
        if there is not None and there.get("sha256") in p.sent:
            return there
        return None

    def _commit(self, p: Pending, answer: dict, rev: int, call: str | None, body: bytes | None,
                sha: str | None = None) -> None:
        """``answer`` names no entry when a save in parts landed on a file
        removed while the server read it back: saved (``sha``), and gone, as
        the feed will say."""
        wire = answer.get("entry")
        entry = entry_of(wire) if wire else None
        current, tells = self.names.commit_saved(p, entry, rev, call, sha)
        if entry is not None and entry.sha256 and body is not None:
            try:
                self.cache.put(entry.sha256, body, text=entry.is_text)
            except OSError as exc:
                print(f"drive: saved bytes not kept on disk: {exc}", file=sys.stderr, flush=True)
        if not current:
            # The feed already put a newer change here; what the kernel
            # cached of these bytes must go.
            self.effects.forget(self.names.key(p))
        for tell in tells:
            self._file(p, tell)

    def _save_large(self, p: Pending, call: str | None) -> None:
        """A file larger than one request: in parts from its journal record
        (``drive_uploads``). Given up when the file is written meanwhile, and
        saved again once quiet; abandoned when it is renamed or removed."""
        upload = Upload()
        if not self._slots.acquire(blocking=False):
            # As many under way as this daemon starts: due again shortly.
            self.writes.wait(p, 1.0)
            return
        try:
            self._save_large_slot(p, call, upload)
        finally:
            self._slots.release()
            self._slot_freed()

    def _save_large_slot(self, p: Pending, call: str | None, upload: Upload) -> None:
        with p.lock:
            if not p.unsaved or p.upload is not None:
                return
            rev, size, p.upload = p.rev, p.size, upload
            cached = p.digest[1] if p.digest is not None and p.digest[0] == rev else None
            earlier = p.parts
        call = call or p.call
        #: Where the begin sent it, and whether over nothing (a file it makes).
        target: list[tuple[str, bool]] = []

        def request(send):
            with p.saving:
                with p.lock:
                    current(p, rev, upload)
                if not target:
                    target.append((p.rel, p.base is None))
                return self._sent_once(p, send)

        def done() -> None:
            # Under ``saving``: a rename or removal either abandoned this
            # save before it ends here, or waits for it to end.
            with p.lock:
                if p.upload is upload:
                    p.upload = None

        sha = None
        try:
            self.counts["save_parts"] += 1
            sha = cached or self.uploads.digest(p, rev, upload, size)
            with p.lock:
                p.digest = (rev, sha)
            if earlier is not None and earlier[1] != sha:
                # An earlier upload of other bytes would stay open a day.
                with p.lock:
                    if p.parts == earlier:
                        p.parts = None
                self._end(_Ending(p.space, earlier[0], p.rel, earlier[1], p.base is None))
            answer = self._send_parts(p, rev, upload, size, sha, call, request)
        except (Changed, Abandoned):
            self.counts["save_parts_given_up"] += 1
            with p.saving:
                done()
            return
        except OSError as exc:
            with p.saving:
                done()
                if upload.abandoned.is_set() and upload.id is not None and target and sha is not None:
                    # Renamed or removed, and its upload never said it ended:
                    # it must not land after.
                    self._end(_Ending(p.space, upload.id, target[0][0], sha, target[0][1]))
                if not upload.abandoned.is_set():
                    if upload.id is not None and sha is not None:
                        with p.lock:
                            p.parts = (upload.id, sha)
                    # Bytes named by their sha256 may land once a begin took them.
                    self._failed(p, exc, call, digest=sha, sent=bool(target))
            raise
        except BaseException:
            with p.saving:
                done()
            raise
        with p.saving:
            done()
            rel, made = target[0]
            if upload.abandoned.is_set() and not (p.open and p.rel == rel):
                # Renamed or removed while the server read it back.
                if made:
                    self._take_back(p.space, rel, sha, answer)
                return
            kept = self._link_for_store(p, sha)
            self._commit(p, answer, rev, call, None, sha)
            # The linked bytes are ``sha``'s only while they are still the
            # save's rev and nothing can write them: let go, unchanged.
            store = not p.open and p.rev == rev
        if kept is not None:
            if store:
                self.cache.install(sha, kept, size)
            with contextlib.suppress(OSError):
                os.unlink(kept)

    def _send_parts(self, p: Pending, rev: int, upload: Upload, size: int, sha: str, call: str | None,
                    request: Callable) -> dict:
        """``send_large``, begun again when its upload ends under it. A
        refusal at the end as changed or exists is the server's head having
        moved: if it moved to these very bytes (an earlier save of them that
        went unanswered landed meanwhile), the file is saved."""
        for _ in range(RESTARTS):
            try:
                return self.uploads.send_large(p, rev, upload, size, sha, call, request)
            except Restart:
                self.counts["save_parts_restarted"] += 1
            except Failed as exc:
                if exc.code not in ("changed", "exists"):
                    raise
                there = self.client.entry(p.space.ref, p.rel)
                if there is None or there.get("sha256") != sha:
                    raise
                return {"state": "done", "entry": there}
        raise Stuck("the Drive's server kept ending the upload of it")

    def _take_back(self, space: Space, rel: str, sha: str | None, answer: dict) -> None:
        """A file made by a save in parts that landed after the file was
        renamed or removed here: it goes, only while ``rel`` still holds the
        very bytes that save sent, and the trash is conditional on them, so
        an edit saved there meanwhile, or between the look and the trash,
        stays."""
        made = (answer.get("entry") or {}).get("id")
        try:
            there = self.client.entry(space.ref, rel)
            if there is None or sha is None or there.get("sha256") != sha:
                return
            if made is not None and there.get("id") != made:
                return
            try:
                gone = self.client.trash(space.ref, rel, "file", None, if_match=sha)
            except Refused as exc:
                if exc.code in ("changed", "not_found"):
                    return
                raise
            self.names.commit_drop(space, there["id"], int(gone.get("cursor") or 0))
            self.counts["save_parts_taken_back"] += 1
        except Refused as exc:
            print(f"drive: a file a renamed save made not taken back: {exc}", file=sys.stderr, flush=True)
        except OSError:
            # Not answered: tried again from ``tick``, on the same conditions.
            with self._mu:
                self._taking_back.append((space, rel, sha, {"entry": {"id": made}}))

    def _link_for_store(self, p: Pending, sha: str | None) -> str | None:
        """A second name for the journal's bytes, so a save that closes the
        file hands them to the cache with no copy and no fetch back. Only
        taken by the cache if the save closed the file: a file still being
        written would change them."""
        if sha is None:
            return None
        link = os.path.join(self.cache.tmp, f"saved-{p.record}")
        try:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(link)
            os.link(self.journal.data_path(p.record), link)
        except OSError:
            return None
        return link

    def _sent_once(self, p: Pending, send: Callable[[], dict]) -> dict:
        """One request carrying the file (a write, or a begin of a save in
        parts), building on an earlier save of it that got no answer and
        landed after all. Its head is looked at first, as a refusal is filed
        for the command as not saved; and once more after a refusal, for one
        that committed only after that look. With ``saving`` held; ``send``
        reads the base when it runs."""
        def head() -> str | None:
            return (self.client.entry(p.space.ref, p.rel) or {}).get("sha256")

        if p.sent:
            p.head_landed(head())
        try:
            return send()
        except Refused as exc:
            if exc.code not in ("changed", "exists") or not p.sent:
                raise
            if not p.head_landed(head()):
                raise
            return send()

    def _failed(self, p: Pending, exc: OSError, call: str | None, *, digest: str | None, sent: bool) -> None:
        """The one step for a save that did not land. ``sent``: a request
        carried the bytes, named by ``digest``, which may have landed."""
        if not sent:
            why = f"it could not be read on this computer: {exc.strerror or exc}"
            if self.writes.unanswered(p, None, call):
                self._gave_up(p, why)
            elif not p.gave_up and not self._settling[id(p)]:
                self._tell_unsaved(p, why)
            return
        reason = _unanswered(exc)
        p.why = reason or ""
        if reason is None:
            # Refused, and filed by the server for the call, whose result
            # says so. The whole edit is refused: bytes written after the
            # ones sent build on the same refused start.
            self._end_parts(p)
            self.writes.drop_refused(p, exc.errno or errno.EIO)
            return
        if isinstance(exc, Refused) and exc.code == "too_many_uploads":
            # Not a failure of this save: it waits for an upload to end.
            self.counts["save_parts_waiting"] += 1
            with self._mu:
                self._waiting.add(p)
            self.writes.wait(p, SLOT_WAIT_S)
            return
        self.counts["unanswered"] += 1
        after = getattr(exc, "retry_after", None)
        # A server too busy for it refused it before applying anything.
        if self.writes.unanswered(p, None if after is not None else digest, call, after):
            self._gave_up(p, reason)
        elif not p.gave_up and not self._settling[id(p)]:
            self._tell_unsaved(p, reason)

    def _end_parts(self, p: Pending) -> None:
        with p.lock:
            parts, p.parts = p.parts, None
        if parts is not None:
            self._end(_Ending(p.space, parts[0], p.rel, parts[1], p.base is None))

    def _slot_freed(self) -> None:
        """One of this daemon's uploads ended: the saves waiting for one go."""
        with self._mu:
            waiting, self._waiting = self._waiting, set()
        for p in waiting:
            self.writes.wait(p, 0.0)

    def _gave_up(self, p: Pending, reason: str) -> None:
        """The save failed for good (``GIVE_UP_S``): every command that wrote
        it is told, and where its bytes are kept; each is told again if a
        late try saves it."""
        self.counts["save_given_up"] += 1
        self._end_parts(p)
        days = int(KEPT_S // 86400)
        why = (f"not saved after {int(GIVE_UP_S // 3600)} hours of tries ({reason}); its bytes stay on this "
               f"computer at this path for {days} days, tried again every hour and at a write to the file")
        calls = {*p.told_calls, call_id(p.call)} - {None}
        with p.lock:
            p.told_calls += [c for c in calls if c not in p.told_calls]
        for call in calls:
            with contextlib.suppress(OSError):
                self.notices.add(LOST, call, p.space.name, p.rel, why)
        self.effects.noticed()

    def _discard(self, p: Pending) -> None:
        """The bytes of a save that failed for good, kept ``KEPT_S``, go. When
        the server's head is these very bytes (a save of them landed with its
        answer lost), it is said saved instead. With ``saving`` held."""
        try:
            there = self.client.entry(p.space.ref, p.rel)
            # Only the bytes as they are: an earlier save that landed is not
            # this edit.
            landed = there is not None and there.get("sha256") == self._sha_now(p)
            why = "its bytes, kept on this computer since its save failed for good, are gone"
        except OSError:
            landed = False
            why = ("its bytes, kept on this computer since its save failed for good, are gone; "
                   "whether the Drive has them could not be asked")
        kind, err = LOST, errno.EIO
        if landed:
            kind, err, why = SAVED, 0, "a save of it had landed after all"
            self.counts["save_landed_late"] += 1
        else:
            self.counts["save_discarded"] += 1
        for call in {*p.told_calls, call_id(p.call)} - {None}:
            with contextlib.suppress(OSError):
                self.notices.add(kind, call, p.space.name, p.rel, why)
        self.effects.noticed()
        self.names.let_go(p, err)
        self.effects.forget(self.names.key(p))

    @staticmethod
    def _sha_now(p: Pending) -> str | None:
        """The sha256 of ``p``'s bytes as they are: from the digest of its
        last try when unchanged since, else read once."""
        with p.lock:
            if not p.open:
                return None
            if p.digest is not None and p.digest[0] == p.rev:
                return p.digest[1]
            h, at = hashlib.sha256(), 0
            while at < p.size:
                chunk = os.pread(p.fd, min(8 << 20, p.size - at), at)
                if not chunk:
                    break
                h.update(chunk)
                at += len(chunk)
            return h.hexdigest()

    # --- telling the agent ----------------------------------------------------

    def _tell_unsaved(self, p: Pending, reason: str) -> None:
        tell = p.tell_unsaved()
        if tell is not None:
            self._file(p, tell, reason)

    def _file(self, p: Pending, tell: Tell, reason: str = "") -> None:
        with contextlib.suppress(OSError):
            self.notices.add(tell.kind, tell.call, p.space.name, p.rel, reason)
        self.effects.noticed()

    # --- the pool ---------------------------------------------------------------

    def queue(self, p: Pending) -> None:
        """Save ``p`` on the pool, now, unless it is queued or being saved."""
        with self._mu:
            if id(p) in self._busy:
                return
            self._busy.add(id(p))
            if len(self._workers) < SAVERS:
                worker = threading.Thread(target=self._work, daemon=True, name="drive-save")
                self._workers.append(worker)
                worker.start()
        self._queue.put(p)

    def _work(self) -> None:
        while True:
            p = self._queue.get()
            try:
                self.save(p, None)
            except OSError:
                pass
            except Exception as exc:  # noqa: BLE001 - one file's bug must not end the pool
                print(f"drive: a save failed: {exc!r}", file=sys.stderr, flush=True)
            finally:
                with self._mu:
                    self._busy.discard(id(p))
                    self._done.notify_all()

    def run(self, stop: threading.Event) -> None:
        """The saver: ``tick`` until ``stop``. The saves run on the pool, so a
        server that stalls holds up nothing here."""
        while not stop.wait(TICK_S):
            self.tick(time.monotonic())

    def tick(self, now: float) -> None:
        """One turn of the saver: queue the files that came due, let go of
        files opened for writing that never changed, and take up journal
        records a replaced daemon let go."""
        expired: list[Pending] = []
        for p in self.names.writing():
            if p.gave_up and p.phase is Phase.UNANSWERED and now >= p.kept_until:
                expired.append(p)
                continue
            if ripe(p, now) and not p.saving.locked():
                self.queue(p)
            elif p.clean and now - p.changed >= CLEAN_IDLE_S and p.saving.acquire(blocking=False):
                try:
                    self.names.let_go_if_clean(p)
                finally:
                    p.saving.release()
        ending: list[_Ending] = []
        taking: list[tuple[Space, str, str | None, dict]] = []
        if now - self._adopted_at >= ADOPT_EVERY_S:
            self._adopted_at = now
            self.adopt()
            with self._mu:
                ending, self._ending = self._ending, []
                taking, self._taking_back = self._taking_back, []
        if ending or taking or expired:
            self._clean(ending, taking, expired)

    def _clean(self, ending: list[_Ending], taking: list[tuple[Space, str, str | None, dict]],
               expired: list[Pending]) -> None:
        """Run cleanup that asks the server on the cleanup thread; while a
        pass runs, the work waits for the next."""
        with self._mu:
            if self._cleaning:
                self._ending += ending
                self._taking_back += taking
                return
            self._cleaning = True

        def run() -> None:
            try:
                for e in ending:
                    self._end(e)
                for t in taking:
                    self._take_back(*t)
                for p in expired:
                    if p.saving.acquire(blocking=False):
                        try:
                            if p.open and p.gave_up and p.phase is Phase.UNANSWERED:
                                self._discard(p)
                        finally:
                            p.saving.release()
            except Exception as exc:  # noqa: BLE001 - one bug must not stop cleanup for good
                print(f"drive: cleanup failed: {exc!r}", file=sys.stderr, flush=True)
            finally:
                with self._mu:
                    self._cleaning = False

        threading.Thread(target=run, daemon=True, name="drive-cleanup").start()

    def settle(self, call: str, wait_s: float) -> int:
        """Send now what ``call`` wrote and is not saved yet, and wait up to
        ``wait_s`` for each save to land or be refused (the server files
        either for the call); one unanswered is sent again as its back-off
        comes due. What is still unanswered then is told to the agent as not
        saved yet. Returns how many that is."""
        deadline = time.monotonic() + wait_s
        mine = [p for p in self.names.writing() if p.unsaved and p.call == call]
        with self._mu:
            self._settling.update(id(p) for p in mine)
        try:
            for p in mine:
                self.queue(p)
            while True:
                now = time.monotonic()
                left = [p for p in mine if p.unsaved]
                if not left or now >= deadline:
                    break
                for p in left:
                    if ripe(p, now):
                        self.queue(p)
                with self._done:
                    self._done.wait(min(0.1, deadline - now))
        finally:
            with self._mu:
                self._settling.subtract(id(p) for p in mine)
                self._settling += Counter()
        for p in left:
            self._tell_unsaved(p, p.why or "the Drive's server has not answered yet")
        self.counts["settle"] += 1
        return len(left)

    def save_all(self) -> None:
        for p in self.names.writing():
            if p.unsaved:
                with contextlib.suppress(OSError):
                    self.save(p, None)

    def adopt(self) -> int:
        """Take up journal records no live daemon holds (one this daemon
        replaced that has since exited, or died)."""
        return self.writes.restore(self.journal.claim(self.names.records()))

    def unsaved(self) -> int:
        """Files a save of which went unanswered, still being sent."""
        return sum(1 for p in self.names.writing() if p.phase is Phase.UNANSWERED)

