"""Faults on the real kernel mount: saves unanswered, late or stalled,
names under folding, reads while the server stalls. Each needs the fake
server's knobs (``/admin/limits``)."""

from __future__ import annotations

import contextlib
import hashlib
import os
import random
import subprocess
import time
import unicodedata

from checks import Results, case
from fixtures import (
    PRIVATE,
    TOP,
    USER,
    Env,
    Failure,
    as_user,
    drop_caches,
    knobs,
    notes,
    read_file,
    requests,
    user_python,
    wait_for,
    wake_feed,
)

REL = "faults"
D = f"{TOP}/{REL}"


@case("faults")
def unanswered_save_is_sent_until_it_lands(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # A save that gets no answer.
    call = "call-faults-0001"
    with knobs(server, drop_writes=1):
        p = as_user(f"echo first > {D}/r1.txt", env={"LIVEFS_CALL_ID": call}, check=False, settle=False)
        told = wait_for(lambda: notes(server, call, "unsaved"), 5)
        landed = wait_for(lambda: server.content(f"{REL}/r1.txt") == b"first\n", 15)
        saved = wait_for(lambda: notes(server, call, "saved"), 5)
    r.pin("a save cut off unanswered is sent again until it lands; the agent is told it is "
          "not saved yet, then that it is",
          bool(told) and bool(landed) and bool(saved),
          f"exit {p.returncode}, told {bool(told)}, landed {bool(landed)}, saved {bool(saved)}")


@case("faults")
def token_refusal_is_reported_not_saved_and_lands(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    call = "call-faults-0002"
    with knobs(server, status_writes=[401, 2]):
        as_user(f"echo token > {D}/r1b.txt", env={"LIVEFS_CALL_ID": call}, check=False, settle=False)
        told = wait_for(lambda: notes(server, call, "unsaved"), 5)
        landed = wait_for(lambda: server.content(f"{REL}/r1b.txt") == b"token\n", 15)
    r.pin("a save the token check turned away (401) is reported not saved yet, never dropped, "
          "and lands once taken", bool(told) and bool(landed),
          f"told {bool(told)}, landed {bool(landed)}")


@case("faults")
def proxy_page_in_place_of_an_answer_is_sent_again(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    call = "call-faults-0003"
    with knobs(server, status_writes=[502, 1]):
        as_user(f"echo proxy > {D}/r1e.txt", env={"LIVEFS_CALL_ID": call}, check=False, settle=False)
        landed = wait_for(lambda: server.content(f"{REL}/r1e.txt") == b"proxy\n", 15)
    r.pin("a proxy's 502 page in place of an answer is sent again", bool(landed))


@case("faults")
def unsaved_file_outlives_a_daemon_restart(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    as_user(f"mkdir -p {D}")
    call = "call-faults-0004"
    with knobs(server, drop_writes=100000):
        as_user(f"echo kept > {D}/r1c.txt", env={"LIVEFS_CALL_ID": call}, check=False, settle=False)
        unsaved = wait_for(lambda: (mount.status() or {}).get("unsaved", 0) >= 1, 5)
        journal = sorted(os.listdir(os.path.join(PRIVATE, "unsaved"))) if os.path.isdir(
            os.path.join(PRIVATE, "unsaved")) else []
        mount.stop()
    mount.start({"warm": False})
    landed = wait_for(lambda: server.content(f"{REL}/r1c.txt") == b"kept\n", 15)
    r.pin("an unsaved file outlives a daemon restart (its bytes on disk) and lands after it",
          bool(unsaved) and bool(journal) and bool(landed),
          f"unsaved {bool(unsaved)}, journal {journal}, landed {bool(landed)}")


@case("faults")
def host_takes_notices_once_while_the_server_is_unreachable(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    as_user(f"mkdir -p {D}")
    call = "call-faults-0005"

    def take() -> list:
        try:
            return mount.run("drive-notices").get("notices") or []
        except Failure:
            return []

    with knobs(server, drop_writes=100000, notices_down=True):
        as_user(f"echo offline > {D}/r1d.txt", env={"LIVEFS_CALL_ID": call}, check=False, settle=False)
        time.sleep(1.0)
        taken = take()
        again = take()
    mine = [n for n in taken if n.get("call") == call and n.get("kind") == "unsaved"]
    landed = wait_for(lambda: server.content(f"{REL}/r1d.txt") == b"offline\n", 40)
    r.pin("with the server unreachable the host takes the notices from the computer "
          "(drive-notices) once, and the file lands when it is back",
          bool(mine) and not [n for n in again if n.get("call") == call] and bool(landed),
          f"taken {len(mine)}, again {len(again)}, landed {bool(landed)}")


@case("faults")
def late_answer_does_not_undo_a_newer_feed_change(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # A save's answer older than a change the feed already applied: a new
    # file, saved in the background once its writes went quiet, whose answer
    # comes back after the feed brought a later change.
    with knobs(server, answer_delay=2.0):
        as_user(f"echo mine > {D}/r2.txt", settle=False)
        wait_for(lambda: server.entry(f"{REL}/r2.txt") is not None, 3)
        time.sleep(0.3)
        server.write(f"{REL}/r2.txt", b"theirs\n")
        time.sleep(2.5)
    got = read_file(f"{D}/r2.txt")
    r.pin("a save's late answer does not undo a newer change the feed already applied",
          got == b"theirs\n" and server.content(f"{REL}/r2.txt") == b"theirs\n", repr(got))


@case("faults")
def rm_during_a_stalled_save_of_a_new_file_leaves_it_nowhere(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    with knobs(server, stall_writes=1.5):
        as_user(f"echo gone > {D}/r3.txt", settle=False)
        time.sleep(0.8)
        p = as_user(f"rm {D}/r3.txt", check=False, settle=False)
    time.sleep(2.5)
    r.pin("rm during a new file's save stalled on the server leaves it on neither side",
          p.returncode == 0 and server.entry(f"{REL}/r3.txt") is None and not os.path.exists(f"{D}/r3.txt"),
          f"rm {p.returncode} {p.stderr.strip()}, server {server.entry(f'{REL}/r3.txt') is not None}, "
          f"here {os.path.exists(f'{D}/r3.txt')}")


@case("faults")
def names_match_as_the_server_matches_them(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # Names as the server matches them.
    nfc = "naïve-r5.txt"
    nfd = unicodedata.normalize("NFD", nfc)
    as_user(f"echo composed > {D}/{nfc}")
    other = read_file(f"{D}/{nfd}")
    r.pin("a file reads under another normalization of its name",
          other == b"composed\n", repr(other))
    nfd2 = unicodedata.normalize("NFD", "café-r5.txt")
    user_python(f"open({(D + '/' + nfd2)!r}, 'w').write('decomposed\\n')")
    stored = server.content(f"{REL}/{unicodedata.normalize('NFC', nfd2)}")
    dropped = drop_caches()
    again = read_file(f"{D}/{nfd2}")
    r.pin("a decomposed name is stored composed and still reaches the file by the name used",
          stored == b"decomposed\n" and again == b"decomposed\n",
          f"stored {stored!r}, again {again!r}, caches dropped {dropped}")
    as_user(f"echo lower > {D}/readme-r5.md")
    p = user_python("import os, errno\n"
                    "try:\n"
                    f"    os.close(os.open({(D + '/README-R5.md')!r}, os.O_CREAT | os.O_EXCL | os.O_WRONLY))\n"
                    "    print('made')\n"
                    "except OSError as e:\n"
                    "    print(errno.errorcode[e.errno])")
    r.pin("a new file named as another case of a stored one fails at once with EEXIST",
          p.stdout.strip() == "EEXIST" and server.content(f"{REL}/readme-r5.md") == b"lower\n",
          p.stdout.strip() + p.stderr[-200:])
    with contextlib.suppress(OSError):
        os.stat(f"{D}/NOTES-R5.md")
    server.write(f"{REL}/notes-r5.md", b"noted\n")
    seen = wait_for(lambda: read_file(f"{D}/NOTES-R5.md") == b"noted\n", 5)
    r.pin("a name looked up as absent under another case finds the file made elsewhere",
          bool(seen))


@case("faults")
def text_not_on_disk_is_read_by_its_digest(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # A read of text not on disk whose content changed on the server.
    server.write(f"{REL}/r7.md", b"old text\n")
    wait_for(lambda: os.path.exists(f"{D}/r7.md"), 5)
    with knobs(server, feed_hold=True):
        wake_feed(server, REL)
        old = hashlib.sha256(b"old text\n").hexdigest()
        with contextlib.suppress(OSError):
            os.unlink(os.path.join(PRIVATE, "store", "objects", old[:2], old))
        server.write(f"{REL}/r7.md", b"new text, longer\n")
        got = read_file(f"{D}/r7.md")
    r.pin("text not on disk whose head moved on is read by its digest, not EIO",
          got == b"old text\n", repr(got))


@case("faults")
def reads_on_disk_do_not_wait_behind_a_stalled_server(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # Reads of bytes on disk while the server stalls.
    texts = env.small_texts()
    victims = random.Random(99).sample(texts, 6)
    with knobs(server, stall_writes=8):
        before = requests(server, "write")
        slow = subprocess.Popen(["runuser", "-u", USER, "--", "bash", "-c", f"echo slow > {D}/r9.txt"])
        if not wait_for(lambda: requests(server, "write") > before, 5, 0.02):
            raise Failure("the stalled request never reached the server")
        t0 = time.perf_counter()
        got = read_file(f"{TOP}/{victims[0]}")
        took = time.perf_counter() - t0
        with open(os.path.join(env.corpus, victims[0]), "rb") as f:
            expect = f.read()
        slow.wait()
    r.pin("a read of bytes on disk does not wait behind a save stalled on the server",
          got == expect and took < 0.5, f"{took * 1000:.0f} ms", seconds=round(took, 3))
    server.write(f"{REL}/r9.bin", os.urandom(3 << 20))
    wait_for(lambda: os.path.exists(f"{D}/r9.bin"), 5)
    with knobs(server, stall_content=8):
        before = requests(server, "content")
        slow = subprocess.Popen(["runuser", "-u", USER, "--", "bash", "-c", f"cat {D}/r9.bin > /dev/null"])
        if not wait_for(lambda: requests(server, "content") > before, 5, 0.02):
            raise Failure("the stalled request never reached the server")
        t0 = time.perf_counter()
        got = read_file(f"{TOP}/{victims[1]}")
        took = time.perf_counter() - t0
        with open(os.path.join(env.corpus, victims[1]), "rb") as f:
            expect = f.read()
        slow.wait()
    r.pin("a read of bytes on disk does not wait behind a binary fetch stalled on the server",
          got == expect and took < 0.5, f"{took * 1000:.0f} ms", seconds=round(took, 3))


@case("faults")
def unanswered_background_save_backs_off_and_lands(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    with knobs(server, drop_writes=100000):
        before = requests(server, "write")
        as_user(f"echo late > {D}/r10.txt", settle=False)
        time.sleep(6)
        tries = requests(server, "write") - before
    landed = wait_for(lambda: server.entry(f"{REL}/r10.txt") is not None, 40, 0.2)
    r.pin("a background save that gets no answer is sent again with back-off, and lands",
          1 <= tries <= 5 and bool(landed), f"{tries} tries in 6 s, landed {bool(landed)}")


@case("faults")
def folder_renamed_during_a_save_lands_under_the_new_name(r: Results, env: Env) -> None:
    server = env.server
    as_user(f"mkdir -p {D}")
    # Written to a staging folder, then the folder renamed into
    # place while the file's save is held up on the server.
    with knobs(server, stall_writes=2.0):
        as_user(f"mkdir -p {D}/stage12 && echo staged > {D}/stage12/x.txt", settle=False)
        time.sleep(0.8)  # its save is under way, stalled
        p = as_user(f"mv {D}/stage12 {D}/final12", check=False, settle=False)
    landed = wait_for(lambda: server.content(f"{REL}/final12/x.txt") == b"staged\n", 10)
    time.sleep(1.0)
    r.pin("a folder renamed while a file in it is being saved: the file lands under the new "
          "name and nothing is left under the old",
          p.returncode == 0 and bool(landed) and server.entry(f"{REL}/stage12") is None
          and server.entry(f"{REL}/stage12/x.txt") is None
          and read_file(f"{D}/final12/x.txt") == b"staged\n",
          f"mv {p.returncode} {p.stderr.strip()}, landed {bool(landed)}, "
          f"old folder {server.entry(f'{REL}/stage12') is not None}")
