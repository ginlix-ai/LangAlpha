"""Lay out one release's update feed from the build artifacts, ready to upload.

    CHANNEL=latest FEED=https://... python desktop/scripts/feed-gather.py

Run from the directory holding `artifacts/`, which download-artifact fills with
every platform's build; writes `feed/`. Needs PyYAML, and runs in a step of its
own so that package never runs beside the feed's write key. With OVERWRITE=true
it publishes over a feed that already names a newer version.
"""

import glob
import os
import shutil
import sys
import urllib.error
import urllib.request

import yaml


def order(version):
    """A semver sort key: a release above its prereleases, numeric ids numerically."""
    main, _, pre = version.partition("-")
    ids = tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in pre.split(".")) if pre else ()
    return (tuple(int(x) for x in main.split(".")), 0 if pre else 1, ids)


def live_version(feed, name):
    """The version the feed's copy of a manifest names. Only a 404 counts as none."""
    # A user agent of its own: the edge refuses Python's default one.
    req = urllib.request.Request(f"{feed}/{name}", headers={"User-Agent": "langalpha-desktop-release"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return yaml.safe_load(r.read())["version"]
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        return None


def main():
    channel, feed = os.environ["CHANNEL"], os.environ["FEED"]
    os.makedirs("feed", exist_ok=True)
    for p in glob.glob("artifacts/**/*", recursive=True):
        if os.path.isfile(p) and not p.endswith(".yml"):
            shutil.copy(p, "feed/")

    # Each Mac builds on a runner of its own, so each writes a latest-mac.yml
    # naming only its own archives, and copied side by side one would replace the
    # other: every Mac of that architecture would be offered the other's build.
    # Same-named manifests are merged into the one a single build of both
    # architectures writes, x64 first, since the top-level `path` is what an
    # updater that predates per-architecture entries reads.
    groups = {}
    for p in sorted(glob.glob(f"artifacts/**/{channel}*.yml", recursive=True)):
        groups.setdefault(os.path.basename(p), []).append(p)
    if not groups:
        sys.exit(f"::error::the builds carry no {channel} manifest, so they could never update themselves")
    for name, paths in groups.items():
        docs = [yaml.safe_load(open(p)) for p in paths]
        if len({d["version"] for d in docs}) != 1:
            sys.exit(f"::error::the {name} manifests name different versions")
        merged = docs[0]
        if len(docs) > 1:
            files = {f["url"]: f for d in docs for f in d["files"]}.values()
            files = sorted(files, key=lambda f: (not f["url"].endswith(".zip"), "arm64" in f["url"]))
            merged["files"], merged["path"], merged["sha512"] = files, files[0]["url"], files[0]["sha512"]
            merged["releaseDate"] = max(d["releaseDate"] for d in docs)
        # A manifest already in the feed that names a newer version means this
        # run is older than one that published after it started.
        live = live_version(feed, name)
        if live and order(live) > order(merged["version"]) and os.environ.get("OVERWRITE") != "true":
            sys.exit(f"::error::the feed's {name} already names {live}, newer than {merged['version']}")
        with open(f"feed/{name}", "w") as out:
            yaml.safe_dump(merged, out, sort_keys=False)
        print(f"  {name}: {', '.join(f['url'] for f in merged['files'])}")


if __name__ == "__main__":
    main()
