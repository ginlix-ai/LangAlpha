"""Each deployment's tunnel on a machine goes by a name of its own, and taking
one away finds it by its id, never by a name another deployment may share."""

import re
import subprocess
import time

from src.server.services.ssh_tunnel_sweep import _uninstall_sh
from src.server.services.ssh_tunnels import tunnel_name

_GATEWAY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
_OTHER_GATEWAY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINtrK5WFrGlR2uf038jBnBgAvICChwc8DD1Mw46Bixsq"
)
_TUNNEL = "5b0f3c1e-8d2a-4c6b-9e7f-1a2b3c4d5e6f"
_OTHER_TUNNEL = "7c1d2e3f-4a5b-4c6d-8e9f-0a1b2c3d4e5f"


def test_a_tunnel_is_named_by_its_gateways_key():
    name = tunnel_name(f"{_GATEWAY} langalpha-gateway")

    # The key alone: its comment is no part of it.
    assert name == tunnel_name(_GATEWAY)
    assert name != tunnel_name(_OTHER_GATEWAY)
    # It goes unquoted into a service definition and a crontab line.
    assert re.fullmatch(r"tunnel-[0-9a-f]{8}", name)


def test_taking_a_tunnel_away_runs_only_the_uninstall_holding_its_id(tmp_path):
    """The other deployment's tunnel beside it stays; one set up before folders
    were named per deployment is found all the same."""
    home = tmp_path / "home"
    folders = {
        "tunnel-aaaaaaaa": _TUNNEL,
        "tunnel-bbbbbbbb": _OTHER_TUNNEL,
        "tunnel": _TUNNEL,
    }
    for folder, tunnel_id in folders.items():
        d = home / ".langalpha" / folder
        d.mkdir(parents=True)
        (d / "tunnel_id").write_text(f"{tunnel_id}\n")
        (d / "uninstall.sh").write_text(f'touch "{tmp_path}/ran-{folder}"\n')

    run = subprocess.run(
        ["sh", "-c", _uninstall_sh(_TUNNEL)],
        env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
        timeout=30,
    )

    assert run.returncode == 0
    expected = {"ran-tunnel-aaaaaaaa", "ran-tunnel"}
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if expected <= {p.name for p in tmp_path.glob("ran-*")}:
            break
        time.sleep(0.05)
    time.sleep(0.2)
    assert {p.name for p in tmp_path.glob("ran-*")} == expected


def test_a_machine_with_no_tunnel_left_answers_as_done(tmp_path):
    run = subprocess.run(
        ["sh", "-c", _uninstall_sh(_TUNNEL)],
        env={"HOME": str(tmp_path), "PATH": "/usr/bin:/bin"},
        timeout=30,
    )

    assert run.returncode == 0
