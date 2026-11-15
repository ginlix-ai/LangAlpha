"""The Mac's exclusions of the SSH runtime, which reach a machine whose
install is already current as well as one being installed."""

from __future__ import annotations

import subprocess
import sys

import pytest

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
