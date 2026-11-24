#!/usr/bin/env python3
"""Run the Drive mount harness on a real kernel, in one self-removing container.

    uv run python scripts/drive_mount/harness.py                 # every suite, 20,000 files
    uv run python scripts/drive_mount/harness.py --files 2000 --suites writers,refusal,feed
    uv run python scripts/drive_mount/harness.py --suites writers,feed \
        --server http://host.docker.internal:8580 --token lfs1.<computer>.<secret>

The container is the product's Docker sandbox image (it carries fuse3, rg and
git) with /dev/fuse and the mount capability. The package under test is this
checkout's ``livefs_runtime``, mounted read-only and shipped inside the way
the host ships it. The corpus is generated once into a named volume and
reused. Results land in ``tmp/drive_mount/`` as JSON; see ``inside.py`` for
the suites.
"""

from __future__ import annotations

import argparse
import os
import secrets
import subprocess
import sys

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
IMAGE = "langalpha-sandbox:latest"
VOLUME = "drivefs-harness-corpus"
#: Large files and the libraries that read them, for the blocks and live
#: suites (see each suite's docstring for what it holds).
DATA_VOLUME = "drivefs-large"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=20000)
    ap.add_argument("--suites", help="default: bench,conc,writers,refusal,feed,memory, or with --server "
                                     "the suites a real server can run")
    ap.add_argument("--levels", default="1,4,8")
    ap.add_argument("--only", default="", help="run only the cases whose names hold one of these (comma list)")
    ap.add_argument("--cpus", default="4", help="the container's CPU limit (a sandbox has 1 to 4)")
    ap.add_argument("--memory", default="8g")
    ap.add_argument("--image", default=IMAGE)
    ap.add_argument("--out", default=None, help="results file under tmp/drive_mount/")
    ap.add_argument("--settings", default="{}", help="daemon settings JSON for every start")
    ap.add_argument("--server", help="a running server in place of the fake (writers and feed only)")
    ap.add_argument("--token", help="a computer's mount token on --server")
    ap.add_argument("--rest", help="the Drive REST API, default <server>/api/v1/drive")
    ap.add_argument("--rest-token", help="a bearer token for --rest, where it needs one")
    ap.add_argument("--runtime", help="a livefs_runtime copy to test in place of this checkout's")
    ap.add_argument("--privileged", action="store_true",
                    help="a privileged container, which conc's dropped-cache passes need")
    a = ap.parse_args()
    real = []
    for flag in ("server", "token", "rest", "rest_token"):
        if getattr(a, flag):
            real += [f"--{flag.replace('_', '-')}", getattr(a, flag)]
    results = os.path.join(REPO, "tmp", "drive_mount")
    os.makedirs(results, exist_ok=True)
    name = f"drivefs-harness-{secrets.token_hex(4)}"
    out = a.out or f"drive_mount-{a.files}-{name[-8:]}.json"
    # Unnamed, the suites are inside.py's to choose: with --server it runs
    # only those a real server supports, and refuses others named outright.
    suites = (a.suites or "").split(",")
    data: list[str] = []
    blob = None
    if "blocks" in suites or "live" in suites:
        data = ["-v", f"{DATA_VOLUME}:/var/tmp/drive_large"]
    if "blocks" in suites:
        # The blob store the fake's links point at. On the default bridge,
        # reached by address: a network of its own needs a free address
        # pool, which a busy Docker may not have.
        blob = f"{name}-blob"
        subprocess.run([
            "docker", "run", "-d", "--rm", "--name", blob,
            "-v", f"{DATA_VOLUME}:/var/tmp/drive_large:ro", "-v", f"{REPO}/scripts/drive_mount:/opt/drive/harness:ro",
            "--entrypoint", "python3", a.image, "/opt/drive/harness/fake_blob.py",
        ], check=True, capture_output=True)
        ip = subprocess.run(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", blob],
                            check=True, capture_output=True, text=True).stdout.strip()
        data += ["--add-host", f"blob:{ip}"]
    cmd = [
        "docker", "run", "--rm", "--name", name,
        "--device", "/dev/fuse", "--cap-add", "SYS_ADMIN",
        "--security-opt", "apparmor:unconfined",
        *(["--privileged"] if a.privileged else []),
        "--add-host", "host.docker.internal:host-gateway",
        "--cpus", a.cpus, "--memory", a.memory, "--memory-swap", a.memory, "--user", "root", *data,
        "-v", f"{os.path.abspath(a.runtime) if a.runtime else REPO + '/src/ptc_agent/core/sandbox/livefs_runtime'}"
        ":/opt/drive/livefs_runtime:ro",
        "-v", f"{REPO}/scripts/drive_mount:/opt/drive/harness:ro",
        "-v", f"{REPO}/src:/opt/drive/repo/src:ro",  # the fake's path rules are the server's
        "-v", f"{VOLUME}:/var/tmp/drive_bench",
        "-v", f"{results}:/results",
        "-e", "PYTHONDONTWRITEBYTECODE=1",
        "--entrypoint", "python3", a.image,
        "/opt/drive/harness/inside.py",
        "--files", str(a.files), *(["--suites", a.suites] if a.suites else []), "--levels", a.levels,
        "--out", f"/results/{out}", "--settings", a.settings, "--only", a.only, *real,
        *(["--blob", "http://blob:8791,https://blob:8792"] if blob else []),
    ]
    print(f"container {name}; results tmp/drive_mount/{out}", flush=True)
    try:
        return subprocess.call(cmd)
    finally:
        if blob:
            subprocess.run(["docker", "rm", "-f", blob], capture_output=True)


if __name__ == "__main__":
    sys.exit(main())
