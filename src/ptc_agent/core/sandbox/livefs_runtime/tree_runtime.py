"""Moving and removing directories a rename cannot move.

A Daytona sandbox restored from its archive comes back with everything it held
in one read-only overlay layer, and overlayfs without ``redirect_dir`` refuses
to rename a directory out of a lower layer (``EXDEV``). Files still rename, as
does any directory made since. So a folder that must keep its content under a
new name is copied into a new directory and deleted where it is, and one that
is only being thrown away is deleted where it is. A copy costs its size of
disk; deleting what the archive restored frees none, since the provider stopped
counting it.

Sent whole with each folder script that uses it, as ``glob_runtime`` is, and
kept in the livefs package, which ships it for the copies its links set aside.
Standard library only: the scripts run isolated, so a workspace's own
``shutil.py`` cannot shadow anything here.
"""

import errno
import os
import stat
import time

# Disk a copy leaves free for the agent's own work, at least: a disk filled to
# the last block fails every write the workspace makes next.
ROOM_RESERVE_MIN = 256 << 20
ROOM_RESERVE_FRACTION = 0.1
_CHUNK = 8 << 20
# Writes the disk refused for want of space, which a retry does not cure.
_FULL = (errno.ENOSPC, errno.EDQUOT)
# An extended attribute the filesystem has none of, or will not set for this
# user, as a file capability only root may: passed over, as shutil's copy does.
_NO_XATTR = (errno.ENOTSUP, errno.ENODATA, errno.EPERM, errno.EACCES, errno.EINVAL)
# Walks a copy takes to find a tree a process still at work in it has let be.
_WALKS = 5
# What a folder, a link or a pipe the copy makes takes of the reserve: a block,
# so a tree of empty folders cannot fill the disk without a file to measure.
_ENTRY_ROOM = 4096
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class NoRoom(Exception):
    """The disk cannot take the rest of a copy and keep its reserve free."""


def cannot_rename(exc):
    return isinstance(exc, OSError) and exc.errno == errno.EXDEV


def _past(deadline):
    return deadline is not None and time.time() >= deadline


def _lstat(path):
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _open_up(path, st):
    # A folder the agent made read-only refuses its owner a delete or a write
    # inside until it is opened up again, as ``chmod u+rwx`` would.
    if st.st_mode & 0o700 != 0o700:
        os.chmod(path, stat.S_IMODE(st.st_mode) | 0o700)


def _let_in(path, st, bits):
    """Whether ``path`` had to be given its owner ``bits`` for the copy to
    read it, which the caller takes away again. A rename reads nothing, so
    what its owner shut itself out of, as ``chmod 000`` does, still moves;
    root reads it as it is, and anyone else's is not ours to open."""
    if st.st_mode & bits == bits or os.geteuid() == 0 or st.st_uid != os.geteuid():
        return False
    os.chmod(path, stat.S_IMODE(st.st_mode) | bits)
    return True


def _open_dir(name, st, at=None):
    """The folder ``name``, in the one ``at`` holds open, held open itself and
    opened up for its owner to delete inside; None if it is no longer a
    folder. Opened by no link, so one swapped in since the stat leads nowhere."""
    shut = os.geteuid() != 0 and st.st_mode & 0o700 != 0o700
    for _ in range(2):
        try:
            fd = os.open(name, _DIR_FLAGS, dir_fd=at)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ELOOP, errno.ENOTDIR):
                return None
            if exc.errno != errno.EACCES or not shut:
                raise
            # One its owner cannot even read: opened up by its name, which
            # reaches no further than what that owner could open up itself.
            os.chmod(name, stat.S_IMODE(st.st_mode) | 0o700, dir_fd=at)
            shut = False
            continue
        if shut:
            os.fchmod(fd, stat.S_IMODE(st.st_mode) | 0o700)
        return fd
    return None


def _listed(fd):
    try:
        with os.scandir(fd) as entries:
            return iter([entry.name for entry in entries])
    except BaseException:
        os.close(fd)
        raise


def remove_tree(top, deadline=None):
    """Delete ``top`` where it is; True once it is gone, False if ``deadline``
    (epoch seconds) came first. The next call removes what is left.

    Each folder is held open while it is emptied, and what it holds is
    removed by name within it, as ``shutil.rmtree`` does: code at work in the
    sandbox that swaps a folder for a link midway leads the delete no further
    than the link, where a delete by path would follow it out of ``top``."""
    while True:
        st = _lstat(top)
        if st is None:
            return True
        if not stat.S_ISDIR(st.st_mode):
            os.unlink(top)
            return True
        fd = _open_dir(top, st)
        if fd is not None:
            break
    stack = [(fd, _listed(fd), None)]
    try:
        while stack:
            if _past(deadline):
                return False
            fd, names, name = stack[-1]
            entry = next(names, None)
            if entry is None:
                stack.pop()
                os.close(fd)
                if stack:
                    os.rmdir(name, dir_fd=stack[-1][0])
                else:
                    os.rmdir(top)
                continue
            try:
                est = os.stat(entry, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(est.st_mode):
                child = _open_dir(entry, est, fd)
                if child is not None:
                    stack.append((child, _listed(child), entry))
                    continue
            try:
                os.unlink(entry, dir_fd=fd)
            except FileNotFoundError:
                pass
    finally:
        for fd, _, _ in stack:
            os.close(fd)
    return True


def room_for(need, at):
    """Whether ``need`` more bytes fit under ``at`` and leave the reserve free."""
    fs = os.statvfs(at)
    free = fs.f_bavail * fs.f_frsize
    reserve = max(ROOM_RESERVE_MIN, int(fs.f_blocks * fs.f_frsize * ROOM_RESERVE_FRACTION))
    return need + reserve <= free


def _same_file(a, b):
    # The ctime too: a write that keeps the size and sets the mtime back, as
    # an archive extraction does, still moves the source's past the copy's.
    return (
        b is not None
        and stat.S_IFMT(a.st_mode) == stat.S_IFMT(b.st_mode)
        and a.st_size == b.st_size
        and a.st_mtime_ns == b.st_mtime_ns
        and stat.S_IMODE(a.st_mode) == stat.S_IMODE(b.st_mode)
        and a.st_ctime_ns <= b.st_ctime_ns
    )


def _resumes_at(a, b):
    """Where to go on copying ``a`` into ``b``: the end of what a copy the
    deadline cut short left, stamped with the mtime of a source unchanged
    since. 0 for anything else, which is copied again from the start."""
    if (
        b is not None
        and stat.S_ISREG(b.st_mode)
        and b.st_size < a.st_size
        and b.st_mtime_ns == a.st_mtime_ns
        and a.st_ctime_ns <= b.st_ctime_ns
    ):
        return b.st_size
    return 0


def _keep_owner(path, st):
    # Run as root, a copy would be root's, and an agent running as anyone
    # else could no longer write the files it made.
    if os.geteuid() == 0:
        os.lchown(path, st.st_uid, st.st_gid)


def _copy_xattrs(src, dst):
    # ACLs and user attributes, which a rename keeps. One the source has lost
    # since an earlier pass copied it goes too: a folder's copy outlives the
    # pass, and a revoked ACL must not come back with the move.
    if not hasattr(os, "listxattr"):
        return
    try:
        names = os.listxattr(src, follow_symlinks=False)
        stale = set(os.listxattr(dst, follow_symlinks=False)) - set(names)
    except OSError as exc:
        if exc.errno in _NO_XATTR:
            return
        raise
    for name in stale:
        try:
            os.removexattr(dst, name, follow_symlinks=False)
        except OSError as exc:
            if exc.errno not in _NO_XATTR:
                raise
    for name in names:
        try:
            value = os.getxattr(src, name, follow_symlinks=False)
            os.setxattr(dst, name, value, follow_symlinks=False)
        except OSError as exc:
            if exc.errno not in _NO_XATTR:
                raise


def _same_version(a, b):
    return (a.st_ino, a.st_size, a.st_mtime_ns, a.st_ctime_ns) == (
        b.st_ino, b.st_size, b.st_mtime_ns, b.st_ctime_ns
    )


def _copy_file(src, dst, st, deadline, offset=0):
    """Copy one file's bytes from ``offset`` and stamp the copy with ``st``,
    read before them. Returns whether the copy is whole, False leaving the
    rest to a later pass, and the version copied. Cut short, the part copied
    takes that mtime, so a file too large for one pass still gets copied. A
    file changed while it was copied, its bytes or anything else the copy
    takes from it, is copied again from the start, as its copy would be a
    version it never had, which the pass that made it may commit, and one
    still changing after a few copies is left with no copy. One gone, or no
    longer a file, has none either, nor a version, and the walk that follows
    finds what took its place. Raises NoRoom when the disk cannot take it."""
    for _ in range(3):
        let_in = _let_in(src, st, stat.S_IRUSR)
        try:
            fin = open(src, "rb")
        finally:
            if let_in:
                os.chmod(src, stat.S_IMODE(st.st_mode))
        done = False
        with fin:
            if let_in:
                # From the start, against the file as opened: putting its
                # mode back moved its ctime, as a write would have.
                if offset:
                    os.unlink(dst)
                st, offset = os.fstat(fin.fileno()), 0
            # By its length, as the copy writes every byte of a sparse file,
            # and once what a stale copy held is gone, which frees its blocks.
            # The copy goes no further than the length found room for: a file
            # still growing is copied again, at the length it has then.
            if not room_for(st.st_size - offset, os.path.dirname(dst)):
                raise NoRoom("no room to copy %s" % src)
            with open(dst, "r+b" if offset else "wb") as fout:
                fin.seek(offset)
                fout.seek(offset)
                while not _past(deadline):
                    chunk = fin.read(min(_CHUNK, st.st_size - fout.tell()))
                    if not chunk:
                        done = True
                        break
                    fout.write(chunk)
                # Or by its length, as the deadline can come between the last
                # chunk and the read that finds the end.
                done = done or fout.tell() == st.st_size
        if done:
            # The owner first, as a chown clears a setuid or setgid bit and a
            # file capability, and the mode last, which may shut out the rest.
            _keep_owner(dst, st)
            _copy_xattrs(src, dst)
            os.chmod(dst, stat.S_IMODE(st.st_mode))
        os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns))
        # After the copy's last write: a change to the source before it that
        # moves only its ctime, as to an xattr or its owner, leaves that under
        # the copy's, which a later pass takes for a copy made since.
        now = _lstat(src)
        if now is not None and _same_version(st, now):
            return done, st
        os.unlink(dst)
        st, offset = now, 0
        if st is None or not stat.S_ISREG(st.st_mode):
            return True, None
    return False, None


def _make_room_for_entry(path):
    if not room_for(_ENTRY_ROOM, os.path.dirname(path)):
        raise NoRoom("no room to make %s" % path)


def _discard(path, deadline):
    st = _lstat(path)
    if st is None:
        return True
    if stat.S_ISDIR(st.st_mode):
        return remove_tree(path, deadline)
    os.unlink(path)
    return True


def _sync_entry(entry, target, deadline, linked):
    """Bring one non-directory entry over; None if the deadline cut it off,
    else whether anything was written. ``linked`` maps a file with more than
    one name to its first name's copy, which the others are linked to, as a
    rename keeps them one file."""
    est = entry.stat(follow_symlinks=False)
    have = _lstat(target)
    kind = stat.S_IFMT(est.st_mode)
    key = (est.st_dev, est.st_ino)
    offset = 0
    if kind == stat.S_IFREG:
        first = linked.setdefault(key, target) if est.st_nlink > 1 else target
        fst = None if first == target else _lstat(first)
        if fst is not None:
            if have is not None and have.st_ino == fst.st_ino:
                return False
            if have is not None and not _discard(target, deadline):
                return None
            _make_room_for_entry(target)
            os.link(first, target)
            return True
        if first != target:
            # The name copied first went since the walk listed it, and its
            # copy with it: this one is copied in its place.
            linked[key] = target
        if _same_file(est, have):
            return False
        offset = _resumes_at(est, have)
    elif kind == stat.S_IFLNK:
        link = os.readlink(entry.path)
        # The ctime as for a file: an owner changed since the copy moves
        # nothing else a link has.
        if (
            have is not None
            and stat.S_ISLNK(have.st_mode)
            and have.st_mtime_ns == est.st_mtime_ns
            and est.st_ctime_ns <= have.st_ctime_ns
            and os.readlink(target) == link
        ):
            return False
    elif kind == stat.S_IFIFO:
        if (
            have is not None
            and stat.S_ISFIFO(have.st_mode)
            and stat.S_IMODE(have.st_mode) == stat.S_IMODE(est.st_mode)
            and have.st_mtime_ns == est.st_mtime_ns
            and est.st_ctime_ns <= have.st_ctime_ns
        ):
            return False
    else:
        # A socket or a device: nothing a copy can carry, and what an earlier
        # pass copied under its name goes.
        if have is None:
            return False
        return True if _discard(target, deadline) else None
    if have is not None and not offset and not _discard(target, deadline):
        return None
    if kind == stat.S_IFREG:
        done, copied = _copy_file(entry.path, target, est, deadline, offset)
        if (copied is None or copied.st_ino != est.st_ino) and linked.get(key) == target:
            # The name was replaced as it was copied, and its copy holds
            # another file: the next of the old one's names is copied anew.
            del linked[key]
        return True if done else None
    _make_room_for_entry(target)
    if kind == stat.S_IFLNK:
        os.symlink(link, target)
        _keep_owner(target, est)
    else:
        os.mkfifo(target)
        _keep_owner(target, est)
        # Set after, as mkfifo takes the umask off the mode it is given.
        os.chmod(target, stat.S_IMODE(est.st_mode))
    # A rename keeps a link's and a pipe's times as it does a file's.
    os.utime(target, ns=(est.st_atime_ns, est.st_mtime_ns), follow_symlinks=False)
    return True


def _held_in(path):
    """What the folder ``path`` holds, by name: each entry's lstat and, for a
    link, where it points. Let into for its owner as the copy is, then shut
    again; None if it is gone, or not ours to open."""
    st = _lstat(path)
    if st is None or not stat.S_ISDIR(st.st_mode):
        return None
    opened = _let_in(path, st, 0o500)
    try:
        found = {}
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    est = entry.stat(follow_symlinks=False)
                    found[entry.name] = (
                        est, os.readlink(entry.path) if stat.S_ISLNK(est.st_mode) else None
                    )
                except FileNotFoundError:
                    continue
        return found
    except (FileNotFoundError, PermissionError):
        return None
    finally:
        if opened:
            os.chmod(path, stat.S_IMODE(st.st_mode))


def _same_folder(a, b):
    # By its mode and, run as root, its owner, which the copy takes from it.
    # Not by its times, which the delete itself moves in each folder it
    # empties, nor its attributes, which a copy cannot always take: asked
    # after, either would have the folder copied again on every link. Run as
    # anyone else, a delete cut short may have left its owner let in.
    if b is None or not (stat.S_ISDIR(a.st_mode) and stat.S_ISDIR(b.st_mode)):
        return False
    mode, held = stat.S_IMODE(a.st_mode), stat.S_IMODE(b.st_mode)
    if os.geteuid() != 0:
        return mode in (held, held | 0o700)
    return mode == held and (a.st_uid, a.st_gid) == (b.st_uid, b.st_gid)


def holds(dst, src):
    """Whether ``dst``, a copy of ``src``, holds everything under ``src`` as
    it is now, so deleting ``src`` with ``remove_tree`` loses nothing: each
    folder with its mode, and each file, link and pipe. Not a socket or a
    device, which no copy carries. A folder not ours to open is passed over:
    the delete cannot enter it either."""
    top = _lstat(src)
    if top is not None and not _same_folder(top, _lstat(dst)):
        return False
    stack = [""]
    while stack:
        rel = stack.pop()
        here = _held_in(os.path.join(src, rel) if rel else src)
        if not here:
            continue
        copy = _held_in(os.path.join(dst, rel) if rel else dst) or {}
        for name, (st, points_at) in here.items():
            kind = stat.S_IFMT(st.st_mode)
            if kind not in (stat.S_IFDIR, stat.S_IFREG, stat.S_IFLNK, stat.S_IFIFO):
                continue
            got = copy.get(name)
            if kind == stat.S_IFDIR:
                if got is None or not _same_folder(st, got[0]):
                    return False
                stack.append(os.path.join(rel, name))
            elif got is None or not _same_file(st, got[0]) or got[1] != points_at:
                return False
    return True


def sync_tree(src, dst, deadline=None):
    """Make ``dst`` a copy of ``src``. Returns ``(entries, written)`` once it
    is whole, or None if ``deadline`` came first; raises NoRoom when the disk
    cannot take the rest.

    A file whose type, size, mode and mtime already match is not copied
    again, so a call cut short resumes where it stopped, and a call after the
    source changed writes only what changed and removes what has gone. Room
    is checked file by file rather than for the whole up front, as walking a
    large folder only to size it can take longer than a pass has. A walk
    that wrote anything is followed by another, and the copy is whole only
    once one finds nothing to write: a process still at work in the folder
    can change what a walk already passed, and the caller deletes the source
    of a whole copy. One still changing after a few walks is left to a later
    call, with None.
    """
    written = 0
    try:
        for _ in range(_WALKS):
            synced = _sync_tree(src, dst, deadline)
            if synced is None:
                return None
            entries, changed = synced
            written += changed
            if not changed:
                return entries, written
    except OSError as exc:
        if exc.errno in _FULL:
            raise NoRoom("the disk filled copying %s" % src) from exc
        raise
    return None


def _sync_tree(src, dst, deadline):
    shut = []
    try:
        return _walk(src, dst, deadline, shut)
    finally:
        # The deepest first, while the folders above still let it be reached.
        for path, st in reversed(shut):
            try:
                os.chmod(path, stat.S_IMODE(st.st_mode))
            except FileNotFoundError:
                pass


def _walk(src, dst, deadline, shut):
    dirs = []
    stack = [""]
    entries_seen = written = 0
    linked = {}
    while stack:
        rel = stack.pop()
        s = os.path.join(src, rel) if rel else src
        d = os.path.join(dst, rel) if rel else dst
        try:
            sst = os.lstat(s)
            opened = _let_in(s, sst, 0o500)
            if opened:
                shut.append((s, sst))
            listing = os.scandir(s)
        except FileNotFoundError:
            if not rel:
                raise
            # Gone since the folder above it was listed: its copy goes as a
            # file's does.
            if not _discard(d, deadline):
                return None
            written += 1
            continue
        with listing as entries:
            have = _lstat(d)
            if have is not None and not stat.S_ISDIR(have.st_mode):
                os.unlink(d)
                have = None
            if have is None:
                _make_room_for_entry(d)
                os.mkdir(d, 0o700)
                written += 1
            else:
                _open_up(d, have)
            dirs.append((s, d, sst, opened))
            names = set()
            for entry in entries:
                if _past(deadline):
                    return None
                names.add(entry.name)
                entries_seen += 1
                if entry.is_dir(follow_symlinks=False):
                    stack.append(os.path.join(rel, entry.name))
                    continue
                target = os.path.join(d, entry.name)
                try:
                    changed = _sync_entry(entry, target, deadline, linked)
                except FileNotFoundError:
                    if os.path.lexists(entry.path):
                        raise
                    # Gone since its folder was listed, as what a process
                    # still at work there deletes: so goes its copy, and the
                    # walk that follows finds the folder as it is now.
                    changed = True if _discard(target, deadline) else None
                if changed is None:
                    return None
                written += changed
        with os.scandir(d) as entries:
            gone = [entry.path for entry in entries if entry.name not in names]
        for path in gone:
            if not _discard(path, deadline):
                return None
            written += 1
    # Last, as a write into a folder moves its mtime, and its mode may shut
    # the copy's own writes out.
    for s, d, sst, opened in reversed(dirs):
        _keep_owner(d, sst)
        try:
            _copy_xattrs(s, d)
        except FileNotFoundError:
            pass
        os.chmod(d, stat.S_IMODE(sst.st_mode))
        os.utime(d, ns=(sst.st_atime_ns, sst.st_mtime_ns))
        # After its stamp, as for a file: a folder changed since the walk
        # listed it, in what it holds or in anything the stamp takes from it,
        # takes another walk, which finds it as it is then; one gone, a walk
        # that takes its copy away. Letting it in moved its mode and ctime.
        now = _lstat(s)
        if now is None or not (
            (sst.st_size, sst.st_mtime_ns) == (now.st_size, now.st_mtime_ns)
            if opened else _same_version(sst, now)
        ):
            written += 1
    return entries_seen, written


def copy_aside(src, dst, deadline=None):
    """Copy the folder ``src`` to ``dst``, made whole in one rename. True once
    it is; False if ``deadline`` came first, with the copy so far at ``dst +
    ".copying"`` for the next call to go on from. Raises NoRoom, having
    removed that copy, when the disk cannot take the rest. Deleting ``src`` is
    the caller's, after it has recorded the copy, so a delete cut short is
    finished rather than copied again.
    """
    tmp = dst + ".copying"
    try:
        if sync_tree(src, tmp, deadline) is None:
            return False
    except NoRoom:
        # Disk the copy took and will not use: what it wrote is new, so
        # removing it gives the space back.
        remove_tree(tmp, None if deadline is None else deadline + 5)
        raise
    try:
        os.rename(tmp, dst)
    except OSError as exc:
        if not cannot_rename(exc):
            raise
        # An archive since the copy began took it into the layer too: it
        # goes, and the next call copies anew.
        remove_tree(tmp, None if deadline is None else deadline + 5)
        return False
    return True
