"""The store on a disk too small for the Drive's text (a 512 KiB tmpfs)."""

from __future__ import annotations

import os
import random
import time

from checks import Results, case
from fixtures import PRIVATE, TOP, Env, as_user, du, read_file, requests, sh, wait_for

REL = "small"
D = f"{TOP}/{REL}"


@case("small_disk")
def store_too_small_for_the_text(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    store = os.path.join(PRIVATE, "store")
    mount.stop()
    os.makedirs(store, exist_ok=True)
    sh(["mount", "-t", "tmpfs", "-o", "size=512k", "tmpfs", store], check=True)
    try:
        before = requests(server, "texts")
        mount.start({"warm": False, "store_reserve": 0}, wait_ready=False)
        ready = wait_for(lambda: (mount.status() or {}).get("phase") == "ready", 60, 0.5)
        status = mount.status() or {}
        settled = requests(server, "texts")
        time.sleep(5)
        later = requests(server, "texts")
        held = du(store)
        r.pin("a store on a disk too small for the text gets ready, never fills it, and stops "
              "fetching", bool(ready) and later == settled and held <= 512 * 1024,
              f"ready {bool(ready)}, store {status.get('store')}, texts {before}->{settled}->{later}, "
              f"held {held}")
        texts = env.small_texts()
        sample = random.Random(5).sample(texts, 20)
        wrong = []
        for name in sample:
            with open(os.path.join(env.corpus, name), "rb") as f:
                if read_file(f"{TOP}/{name}") != f.read():
                    wrong.append(name)
        r.pin("text the store could not hold is read right, from the server",
              not wrong, f"{len(wrong)} of 20 wrong")
        big = os.urandom(700 * 1024)
        as_user(f"mkdir -p {D}")
        with open("/tmp/r4.bin", "wb") as f:
            f.write(big)
        r1 = as_user(f"cp /tmp/r4.bin {D}/r4.bin", check=False)
        r2 = as_user(f"echo more >> {D}/r4.bin", check=False)
        got = server.content(f"{REL}/r4.bin")
        here = read_file(f"{D}/r4.bin")
        r.pin("a save the store had no room to keep still moves the base; the next save lands",
              r1.returncode == 0 and r2.returncode == 0 and got == big + b"more\n" and here == got,
              f"cp {r1.returncode} {r1.stderr.strip()[:120]}, append {r2.returncode} "
              f"{r2.stderr.strip()[:120]}, server {len(got or b'')}, here {len(here or b'')}")
    finally:
        mount.stop()
        sh(["umount", "-l", store])
        mount.start({"warm": False})
