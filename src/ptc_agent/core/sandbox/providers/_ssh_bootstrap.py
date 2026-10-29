"""The Python runtime an SSH computer's agent works with, installed without root.

Everything lands under ``~/.langalpha`` on the machine and nothing outside it
changes: a pinned uv, a uv-managed Python 3.12, a venv holding the sandbox's
default packages and a static ripgrep. No system package is installed.

No browser is downloaded: Chromium is about 600 MB on disk, and most machines
have a browser of their own. Where Playwright looks for its Chromium, the
install leaves a launcher that runs the machine's Chrome, Chromium, Edge or
Brave, found when it starts, so a browser installed later is used too and any
Playwright code works unchanged. A machine with none says so at launch. A
machine that got Chromium from an earlier install keeps it.

The install is stamped with a hash of this recipe, so a start finds a current
machine with one read, and a changed recipe reinstalls on the next start. A
reinstall redoes only the layers whose inputs changed: uv and ripgrep by their
version, Python by the venv's interpreter, the packages by a hash in
``.bootstrap.layers``. Concurrent installs (two computers on one
login, two workers) queue on a lock, and the one that waited usually finds
nothing left to do.

The script reports each phase as ``__phase <name> <epoch seconds>`` when it
begins, and ``__phase end`` when done, so the caller can time them.
"""

from __future__ import annotations

import hashlib
import json
import shlex
from dataclasses import dataclass

from ptc_agent.core.sandbox._defaults import (
    DEFAULT_DEPENDENCIES,
    SANDBOX_PLAYWRIGHT_VERSION,
)

# Everything this module installs, relative to the login's home.
STATE_DIR = ".langalpha"
STAMP = f"{STATE_DIR}/.bootstrap"
PHASE_MARK = "__phase "

UV_VERSION = "0.12.24"
RIPGREP_VERSION = "15.2.0"
PYTHON_VERSION = "3.12"
# By release target. Linux takes the musl builds, so they run on any glibc or
# none.
_UV_SHA256 = {
    "x86_64-unknown-linux-musl": "48170bd200a5430298c18f3b264485a1f3a8605f01088277daf6e37633edf0f5",
    "aarch64-unknown-linux-musl": "7f9ab4726d743b6a92e310477a7840c8a8a7b3938cb3c4657d541342fca75011",
    "x86_64-apple-darwin": "4fa82e37cb94767661f532b001e470b67a186c7260e305bd84ddb78fd545c0b6",
    "aarch64-apple-darwin": "0c4346de7abdb49495b393b9ec809fe387aa43e586be20fecb972216c1e71732",
}
_RIPGREP_SHA256 = {
    "x86_64-unknown-linux-musl": "33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c",
    "aarch64-unknown-linux-musl": "800b1e7206afe799dfb5a6901f23147cfaabe0e52210538100f61e86e1740915",
    "x86_64-apple-darwin": "af7825fcc69a2afc7a7aea55fc9af90e26421d8f20fe59df32e233c0b8a231c1",
    "aarch64-apple-darwin": "3750b2e93f37e0c692657da574d7019a101c0084da05a790c83fd335bad973e4",
}
# yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14, as in the
# sandbox image.
_OVERRIDES = ("curl_cffi>=0.14",)
# What the sandbox image's OS provides and a machine may not. uv's Python reads
# time zones from the OS's zoneinfo, which minimal images leave out, and
# zoneinfo falls back to this package.
_MACHINE_PACKAGES = ("tzdata",)
_PACKAGES = (*DEFAULT_DEPENDENCIES, *_MACHINE_PACKAGES)

# The scrape server rewords a launch that fails with this (_NO_BROWSER there).
NO_OWN_BROWSER = (
    "No Chrome, Chromium, Edge or Brave is installed on this machine, so "
    "Playwright has no browser to run. This is the user's own computer, and "
    "installing a browser downloads about 500 MB onto it, so ask the user "
    "before installing one."
)
# Stands where Playwright expects its Chromium and runs the machine's own
# browser with Playwright's arguments. Playwright reaches the browser over the
# pipes it opened, which exec keeps, and the browser still gets a profile of
# Playwright's own.
_LAUNCHER = f"""#!/bin/sh
# Written by langalpha in place of Playwright's Chromium.
for b in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
  "/Applications/Chromium.app/Contents/MacOS/Chromium" \\
  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge" \\
  "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"; do
  for d in "" "$HOME"; do [ -x "$d$b" ] && exec "$d$b" "$@"; done
done
for n in google-chrome google-chrome-stable chromium chromium-browser microsoft-edge brave-browser; do
  b=$(command -v "$n") && exec "$b" "$@"
done
echo "{NO_OWN_BROWSER}" >&2
exit 127
"""
# Prints the executables Playwright finds missing, by launching each Chromium
# it can choose: the headless shell by default, the full build by channel.
# Asking Playwright keeps the paths right across its versions.
_MISSING_BROWSERS = r"""import re
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    for options in ({}, {"channel": "chromium"}):
        try:
            p.chromium.launch(**options).close()
        except Exception as e:
            missing = re.search(r"Executable doesn't exist at ([^\r\n]+)", str(e))
            if missing is None:
                raise
            print(missing.group(1).strip())
"""


# What every script that changes the runtime holds while it does: an install,
# and an uninstall deleting it (``_ssh_shell.uninstall_script``). Each opens the
# file on descriptor 9 and then calls ``lock <seconds to wait>``.
LOCK_FILE = ".bootstrap.lock"
LOCK_SH = """lock() {
  if command -v flock >/dev/null 2>&1; then flock -w "$1" 9; return; fi
  # macOS has no flock(1). Perl takes the same flock(2) lock on descriptor 9,
  # and the lock stays with this shell's copy of it after perl exits.
  perl -MFcntl=:flock -e 'open(my $f, ">&=", 9) or exit 1; my $end = time + shift;
    until (flock($f, LOCK_EX | LOCK_NB)) { exit 1 if time >= $end; select(undef, undef, undef, 0.2) }' "$1"
}
"""


def recipe_hash() -> str:
    """Changes exactly when what the bootstrap installs changes."""
    recipe = {
        "uv": [UV_VERSION, _UV_SHA256],
        "python": PYTHON_VERSION,
        "packages": _PACKAGES,
        "overrides": _OVERRIDES,
        "ripgrep": [RIPGREP_VERSION, _RIPGREP_SHA256],
        "browsers": layer_hashes()["browsers"],
        "revision": 2,
    }
    return hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()[:16]


def _hash(*parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]


def layer_hashes() -> dict[str, str]:
    """What each stamped layer was installed from; a layer reinstalls when its
    hash changes."""
    return {
        "packages": _hash(_PACKAGES, _OVERRIDES),
        "browsers": _hash(SANDBOX_PLAYWRIGHT_VERSION, _LAUNCHER, _MISSING_BROWSERS),
    }


def phase_seconds(output: str) -> dict[str, float]:
    """How long each phase the script reported took, in order."""
    marks: list[tuple[str, float]] = []
    for line in output.splitlines():
        if line.startswith(PHASE_MARK):
            name, _, at = line[len(PHASE_MARK) :].partition(" ")
            try:
                marks.append((name, float(at)))
            except ValueError:
                continue
    return {
        name: round(max(0.0, marks[i + 1][1] - at), 1)
        for i, (name, at) in enumerate(marks[:-1])
    }


def last_phase(output: str) -> str | None:
    """The phase the script was in when it stopped."""
    for line in reversed(output.splitlines()):
        if line.startswith(PHASE_MARK):
            return line[len(PHASE_MARK) :].partition(" ")[0]
    return None


@dataclass(frozen=True)
class RuntimePaths:
    """Where the installed pieces live, as the install script and ``env.sh``
    both name them, so the two cannot drift apart."""

    base: str
    bin: str
    venv: str
    venv_bin: str
    python: str
    cache: str
    uv_cache: str
    browsers: str
    matplotlib: str

    @classmethod
    def under(cls, base: str) -> RuntimePaths:
        return cls(
            base=base,
            bin=f"{base}/bin",
            venv=f"{base}/venv",
            venv_bin=f"{base}/venv/bin",
            python=f"{base}/python",
            cache=f"{base}/cache",
            uv_cache=f"{base}/cache/uv",
            browsers=f"{base}/ms-playwright",
            matplotlib=f"{base}/matplotlib",
        )


def paths(home: str) -> RuntimePaths:
    """The installed pieces under *home*."""
    return RuntimePaths.under(f"{home}/{STATE_DIR}")


def bootstrap_script(*, lock_wait_s: int) -> str:
    """Bash that installs the runtime once, safe to run again at any time.

    Exits 0 at once when the stamp is current. Another install holding the
    lock is waited on for up to *lock_wait_s*, and usually leaves nothing to do.
    """
    want = recipe_hash()
    layers = layer_hashes()
    uv_sha = " ".join(f"{a}={s}" for a, s in _UV_SHA256.items())
    rg_sha = " ".join(f"{a}={s}" for a, s in _RIPGREP_SHA256.items())
    packages = " ".join(shlex.quote(p) for p in _PACKAGES)
    overrides = "\\n".join(_OVERRIDES)
    # The script names its folders under $L, which it sets to the base.
    p = RuntimePaths.under("$L")
    return f"""set -euo pipefail
L="{paths("$HOME").base}"
now() {{ if [ -n "${{EPOCHREALTIME:-}}" ]; then echo "${{EPOCHREALTIME/,/.}}"; else date +%s; fi; }}
phase() {{ echo "{PHASE_MARK}$1 $(now)"; }}
mkdir -p "{p.bin}" "{p.cache}"
chmod 700 "$L"
exec 9>"$L/{LOCK_FILE}"
{LOCK_SH}phase lock
lock {int(lock_wait_s)} || {{ echo "Another install of the runtime is still running." >&2; exit 75; }}
# Again, under the lock: a removal that held it may have taken them.
mkdir -p "{p.bin}" "{p.cache}"
phase check
if [ "$(cat "$L/.bootstrap" 2>/dev/null)" = {want} ]; then phase end; exit 0; fi
case "$(uname -m)" in
  x86_64|amd64) a=x86_64 ;;
  aarch64|arm64) a=aarch64 ;;
  *) echo "Unsupported processor architecture: $(uname -m)" >&2; exit 2 ;;
esac
case "$(uname -s)" in
  Linux) a=$a-unknown-linux-musl ;;
  Darwin) a=$a-apple-darwin ;;
  *) echo "Unsupported operating system: $(uname -s)" >&2; exit 2 ;;
esac
sha_for() {{ for kv in $2; do [ "${{kv%%=*}}" = "$1" ] && echo "${{kv#*=}}"; done; }}
fetch() {{
  if command -v curl >/dev/null 2>&1; then curl -fsSL --retry 3 -o "$2" "$1"
  else wget -q -O "$2" "$1"; fi
}}
verify() {{
  if command -v sha256sum >/dev/null 2>&1; then h=$(sha256sum "$1"); else h=$(shasum -a 256 "$1"); fi
  [ "${{h%% *}}" = "$2" ] || {{ echo "${{1##*/}} does not match its pinned SHA-256." >&2; return 1; }}
}}
current() {{ grep -qxF "$1=$2" "$L/.bootstrap.layers" 2>/dev/null; }}
record() {{
  {{ grep -v "^$1=" "$L/.bootstrap.layers" 2>/dev/null || true; [ -z "$2" ] || echo "$1=$2"; }} > "$L/.bootstrap.layers.new"
  mv "$L/.bootstrap.layers.new" "$L/.bootstrap.layers"
}}
t=$(mktemp -d "$L/.install.XXXXXX")
trap 'rm -rf "$t"' EXIT

if [ "$("{p.bin}/uv" --version 2>/dev/null | cut -d" " -f2)" != {UV_VERSION} ]; then
  phase uv
  echo "Installing uv {UV_VERSION}"
  n="uv-$a"
  fetch "https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/$n.tar.gz" "$t/uv.tgz"
  verify "$t/uv.tgz" "$(sha_for "$a" "{uv_sha}")"
  tar -xzf "$t/uv.tgz" -C "$t"
  install -m 0755 "$t/$n/uv" "$t/$n/uvx" "{p.bin}/"
fi

if [ "$("{p.bin}/rg" --version 2>/dev/null | head -1 | cut -d" " -f2)" != {RIPGREP_VERSION} ]; then
  phase ripgrep
  echo "Installing ripgrep {RIPGREP_VERSION}"
  n="ripgrep-{RIPGREP_VERSION}-$a"
  fetch "https://github.com/BurntSushi/ripgrep/releases/download/{RIPGREP_VERSION}/$n.tar.gz" "$t/rg.tgz"
  verify "$t/rg.tgz" "$(sha_for "$a" "{rg_sha}")"
  tar -xzf "$t/rg.tgz" -C "$t"
  install -m 0755 "$t/$n/rg" "{p.bin}/rg"
fi

export UV_PYTHON_INSTALL_DIR="{p.python}" UV_CACHE_DIR="{p.uv_cache}" UV_NO_CONFIG=1
# The venv is kept while its interpreter is the right Python and still runs.
if ! "{p.venv_bin}/python" -c "import sys; raise SystemExit('%d.%d' % sys.version_info[:2] != '{PYTHON_VERSION}')" 2>/dev/null; then
  phase python
  echo "Installing Python {PYTHON_VERSION}"
  # --no-bin: by default uv also links python3.X into ~/.local/bin, on the
  # owner's own PATH, where it would shadow theirs.
  "{p.bin}/uv" python install --no-bin {PYTHON_VERSION}
  rm -rf "{p.venv}"
  "{p.bin}/uv" venv --seed --python {PYTHON_VERSION} --python-preference only-managed "{p.venv}"
  record packages ""
fi

if ! current packages {layers["packages"]}; then
  phase packages
  echo "Installing packages"
  printf '{overrides}\\n' > "$t/overrides.txt"
  "{p.bin}/uv" pip install --python "{p.venv_bin}/python" --compile-bytecode \\
    --override "$t/overrides.txt" {packages}
  record packages {layers["packages"]}
fi

if ! current browsers {layers["browsers"]}; then
  phase browsers
  B="{p.browsers}"
  mkdir -p "$B"
  # Launchers from an earlier install go first, so the probe sees only
  # Playwright's own browsers. One that `playwright install` has since
  # replaced with the real build, marked complete, stays.
  for d in "$B"/*/; do
    if [ -f "$d.langalpha-launcher" ] && [ ! -f "${{d}}INSTALLATION_COMPLETE" ]; then rm -rf "$d"; fi
  done
  # Left unstamped when it fails, so the next reinstall tries it again, and
  # never failing the install: browsing alone goes without. set -e does not
  # reach into an if's condition, so each write's failure is checked. The
  # mark goes first, so a launcher left half written is cleared as one.
  if missing=$(PLAYWRIGHT_BROWSERS_PATH="$B" "{p.venv_bin}/python" -c {shlex.quote(_MISSING_BROWSERS)}) &&
    printf '%s\\n' "$missing" | while IFS= read -r f; do
      case "$f" in "$B"/*) ;; *) continue ;; esac
      rel="${{f#"$B"/}}"
      {{ mkdir -p "$(dirname "$f")" && : > "$B/${{rel%%/*}}/.langalpha-launcher" &&
        printf '%s' {shlex.quote(_LAUNCHER)} > "$f" && chmod 755 "$f"; }} || exit 1
    done; then
    record browsers {layers["browsers"]}
  else
    echo "Playwright's browser launcher was not set up." >&2
  fi
fi

if [ "$(uname -s)" = Darwin ]; then
  phase fonts
  # matplotlib 3.11 lists a Mac's fonts with system_profiler, which returns
  # none on macOS 27, and then cannot be imported. Its font cache, built here
  # from the font folders instead, spares it the call. Rebuilt every install,
  # so a new matplotlib never reads an older one's.
  rm -f "{p.matplotlib}"/fontlist-*.json
  MPLCONFIGDIR="{p.matplotlib}" "{p.venv_bin}/python" -c '
import subprocess
run = subprocess.check_output
def check_output(args, *a, **k):
    if list(args[:1]) == ["system_profiler"]:
        raise OSError("not used")
    return run(args, *a, **k)
subprocess.check_output = check_output
import matplotlib.font_manager' || echo "The matplotlib font cache was not built." >&2
fi

phase prune
"{p.bin}/uv" cache prune >/dev/null 2>&1 || true
echo {want} > "$L/.bootstrap.new" && mv "$L/.bootstrap.new" "$L/.bootstrap"
phase end
echo "Runtime ready"
"""


def stamp_check_script() -> str:
    """Prints ``current`` when the installed runtime matches this recipe."""
    return (
        f'[ "$(cat "$HOME/{STAMP}" 2>/dev/null)" = {recipe_hash()} ] '
        "&& echo current || echo stale\n"
    )
