"""The SSH runtime's scripts, run against shims: the Mac's exclusions, which
reach a machine whose install is already current as well as one being
installed, and the stamp a failed browser download must not write."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._ssh_bootstrap import STAMP, recipe_hash, stamp_check_script


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
def test_a_current_install_on_a_mac_is_still_kept_out_of_backups(tmp_path):
    """The stamp check returns before any install runs, so it applies them,
    and retries a Time Machine exclusion tmutil refused."""
    home = tmp_path / "home"
    (home / STAMP).parent.mkdir(parents=True)
    (home / STAMP).write_text(recipe_hash())
    shim = tmp_path / "shim"
    shim.mkdir()
    (shim / "uname").write_text("#!/bin/sh\necho Darwin\n")
    # Refuses once, as without Full Disk Access, then takes it.
    (shim / "tmutil").write_text(
        f'#!/bin/sh\n[ -e {tmp_path}/granted ] || {{ touch {tmp_path}/granted; exit 1; }}\n'
        f"echo \"$@\" >> {tmp_path}/excluded\n"
    )
    for tool in ("uname", "tmutil"):
        (shim / tool).chmod(0o755)
    base = (home / STAMP).parent

    def check() -> str:
        return subprocess.run(
            ["bash", "-c", stamp_check_script()],
            env={"PATH": f"{shim}:/usr/bin:/bin", "HOME": str(home)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    assert check() == "current\n"
    assert (base / ".metadata_never_index").exists()
    assert not (base / ".tm-excluded").exists()
    assert check() == "current\n"
    assert (base / ".tm-excluded").exists()
    check()
    assert (tmp_path / "excluded").read_text() == f"addexclusion {base}\n"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
def test_a_failed_chromium_download_leaves_no_stamp(tmp_path):
    """Else the stamp check reports current and the download is never tried again."""
    groups = bootstrap.selected_groups(["chromium"])
    home = tmp_path / "home"
    p = bootstrap.paths(str(home))
    shim, bin_dir, venv_bin = tmp_path / "shim", Path(p.bin), Path(p.venv_bin)
    for d in (shim, bin_dir, venv_bin):
        d.mkdir(parents=True)
    scripts = {
        shim / "uname": '#!/bin/sh\ncase "$1" in -m) echo x86_64 ;; *) echo Linux ;; esac\n',
        bin_dir / "uv": f'#!/bin/sh\n[ "$1" = --version ] && echo "uv {bootstrap.UV_VERSION}"\nexit 0\n',
        bin_dir / "rg": f"#!/bin/sh\necho ripgrep {bootstrap.RIPGREP_VERSION}\n",
        # The version check passes; the download passes only once allowed.
        venv_bin / "python": f'#!/bin/sh\n[ "$1" = -m ] && [ ! -e {tmp_path}/online ] && exit 1\nexit 0\n',
    }
    for path, text in scripts.items():
        path.write_text(text)
        path.chmod(0o755)
    layers = Path(p.base) / ".bootstrap.layers"
    layers.write_text(f"packages={bootstrap.layer_hashes(groups)['packages']}\n")

    def install() -> None:
        subprocess.run(
            ["bash", "-c", bootstrap.bootstrap_script(lock_wait_s=5, groups=groups)],
            env={"PATH": f"{shim}:/usr/bin:/bin", "HOME": str(home)},
            capture_output=True,
            text=True,
            check=True,
        )

    # An install that had another browsers recipe loses it to the cleanup, so
    # neither its stamp nor its record may survive the failed download.
    (home / STAMP).write_text(recipe_hash())
    layers.write_text(layers.read_text() + f"browsers={bootstrap.layer_hashes(None)['browsers']}\n")
    install()
    assert not (home / STAMP).exists()
    assert "browsers=" not in layers.read_text()
    (tmp_path / "online").touch()
    install()
    assert (home / STAMP).read_text().strip() == recipe_hash(groups)
    assert f"browsers={bootstrap.layer_hashes(groups)['browsers']}" in layers.read_text()
