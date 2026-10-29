import asyncio
import hashlib
import json
import shlex
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers._ssh_authorized_key import authorized_key_line
from ptc_agent.core.sandbox.providers.ssh import SSHProbe
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.models.computer import (
    ComputerSSHPairingReport,
    ComputerSSHTunnelRequest,
)
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded
from src.server.services.ssh_pairing import dial, flow, script, state
from src.server.services.ssh_tunnels import GatewayAddress

_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
)
_OTHER_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHVXzG7KIOqaXH6YHUnmlYDxqplyW8Ds"
# Another machine's, whole, for what parses a key rather than only stores it.
_SECOND_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINtrK5WFrGlR2uf038jBnBgAvICChwc8DD1Mw46Bixsq"
)
_TUNNEL = "5b0f3c1e-8d2a-4c6b-9e7f-1a2b3c4d5e6f"
# What this deployment's gateway key names its machines' tunnels.
_TUNNEL_NAME = "tunnel-0a1b2c3d"
_SETTINGS = SSHConfig()


def _machine(username="ubuntu", **fields):
    return state.ReportedMachine(
        username=username, host_keys=[_KEY], ports=[22], **fields
    )


def _report(**fields):
    return ComputerSSHPairingReport(**fields)


def _found(**overrides):
    fields = {
        "host_key": _KEY,
        "fingerprint": "SHA256:x",
        "os": "Linux",
        "arch": "x86_64",
        "distro": "ubuntu",
        "home": "/home/dev",
        "shell": "/bin/bash",
        "is_root": False,
        "has_sudo": False,
        "disk_free_bytes": None,
        "supported": True,
        "hostname": "box",
    }
    return SSHProbe(**{**fields, **overrides})


def test_a_report_keeps_what_can_be_dialed_and_drops_the_rest():
    machine = dial.parse_report(
        _report(
            user="ubuntu",
            hostname="box\x1b[2J",
            host_keys=f"{_KEY} root@box\nnot a key\n{_KEY}",
            ports="2222 x 70000 2222",
            addresses="198.51.100.7 0.0.0.0 ::ffff:203.0.113.10 nonsense",
        ),
        ["203.0.113.10"],
    )

    assert machine.host_keys == [_KEY]
    assert machine.ports == [2222]
    assert machine.hostname == "box[2J"
    # Where it reported from first: the address a VPS most often answers on.
    assert machine.addresses == ["203.0.113.10", "198.51.100.7"]


def test_a_report_naming_no_port_is_tried_on_22():
    assert dial.parse_report(_report(user="ubuntu"), []).ports == [22]


@pytest.mark.parametrize("user", ["", "a b", "-oProxyCommand=x"])
def test_a_report_naming_no_usable_account_is_refused(user):
    with pytest.raises(ValueError):
        dial.parse_report(_report(user=user), [])


@pytest.mark.asyncio
async def test_an_address_still_silent_after_the_wait_is_dropped(monkeypatch):
    cancelled = []
    monkeypatch.setattr(dial, "_PREFER_WAIT_S", 0.05)

    async def probe(private_key, *, host, **_):
        if host == "slow":
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.append(host)
                raise
        if host == "dead":
            raise SSHConnectError("unreachable", "no answer")
        return host

    monkeypatch.setattr(dial, "probe", probe)

    reached, missed = await dial.first_reachable(
        "k",
        _machine(),
        [
            dial.Candidate("slow", 22),
            dial.Candidate("dead", 22),
            dial.Candidate("good", 22),
        ],
        settings=_SETTINGS,
    )

    assert (reached.host, reached.found) == ("good", "good")
    assert [(m.host, m.failure.code) for m in missed] == [("dead", "unreachable")]
    assert cancelled == ["slow"]


@pytest.mark.asyncio
async def test_where_the_report_came_from_wins_over_a_bridge_that_answers_sooner(
    monkeypatch,
):
    async def probe(private_key, *, host, **_):
        await asyncio.sleep({"203.0.113.10": 0.05, "172.17.0.1": 0.0}[host])
        return host

    monkeypatch.setattr(dial, "probe", probe)

    reached, _ = await dial.first_reachable(
        "k",
        _machine(),
        [dial.Candidate("203.0.113.10", 22), dial.Candidate("172.17.0.1", 22)],
        settings=_SETTINGS,
    )

    assert reached.host == "203.0.113.10"


@pytest.mark.asyncio
async def test_a_tunnel_just_handed_out_is_waited_for(monkeypatch):
    monkeypatch.setattr(dial, "_TUNNEL_RETRY_S", 0.01)
    tries = []

    async def probe(private_key, *, tunnel_id, **_):
        tries.append(tunnel_id)
        if len(tries) < 3:
            raise SSHConnectError("not_connected", "no tunnel yet")
        return "found"

    monkeypatch.setattr(dial, "probe", probe)
    tunneled = [dial.Candidate("box", 22, _TUNNEL)]

    reached, _ = await dial.first_reachable(
        "k", _machine(), tunneled, settings=_SETTINGS, wait_s=1.0
    )
    assert reached.tunnel_id == _TUNNEL and len(tries) == 3

    tries.clear()
    reached, missed = await dial.first_reachable(
        "k", _machine(), tunneled, settings=_SETTINGS
    )
    assert reached is None and len(tries) == 1
    assert missed[0].failure.code == "not_connected"


def _missed(*codes):
    return [
        dial.Missed(f"198.51.100.{i}", 22, SSHConnectError(c, c))
        for i, c in enumerate(codes)
    ]


@pytest.mark.parametrize(
    "codes,expected",
    [
        # A refused key on the machine itself says more than any silent address.
        (("unreachable", "auth_failed", "host_key_mismatch"), "auth_failed"),
        (("host_key_mismatch",), "host_key_mismatch"),
        (("host_key_mismatch", "timeout"), "unreachable"),
        (("timeout", "timeout"), "timeout"),
        (("private_address",), "private_address"),
    ],
)
def test_the_diagnosis_names_the_most_telling_failure(codes, expected):
    error = dial.diagnosis(_machine(), _missed(*codes), tunneled=False)

    assert error.code == expected


def test_a_refused_root_login_says_how_to_allow_it():
    error = dial.diagnosis(_machine("root"), _missed("auth_failed"), tunneled=False)

    assert "PermitRootLogin" in error.message


def _pairing(*, token="abc", base="https://example.com", expires_in=15 * 60, **fields):
    return state.PairingState(
        token=token,
        user_id="u",
        base=base,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
        **fields,
    )


_REPORT_URL = "https://example.com/api/v1/computers/ssh/pairings/abc/report"
# Keeps its arguments, and the config it reads on its stdin when told to.
_FAKE_CURL = (
    'printf "%s\\n" "$@" >> "$HOME/curl-args"; '
    'case " $* " in *" -K - "*) cat > "$HOME/curl-config" ;; esac; '
)


def _serve(monkeypatch, pairing=None, tunnel_name=_TUNNEL_NAME):
    """The script's render for *pairing*, its key ours."""

    async def load(token):
        return pairing if pairing is not None else _pairing(token=token)

    async def key(user_id):
        return {"public_key": f"{_KEY} langalpha"}

    monkeypatch.setattr(state, "load", load)
    monkeypatch.setattr(script, "ensure_client_key", key)
    monkeypatch.setattr(
        script, "deployment_tunnel_name", AsyncMock(return_value=tunnel_name)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("login_from", [[], ["203.0.113.7", "198.51.100.0/24"]])
async def test_the_script_authorizes_the_key_once_and_reports_back(
    tmp_path, monkeypatch, login_from
):
    """The script runs with curl and sshd faked, as a machine would run it.

    The line it writes is the one each start converges to, so a machine
    paired under a ``login_from`` is not rewritten on its first start.
    """
    _serve(monkeypatch)
    rendered = await script.render_script("abc", SSHConfig(login_from=login_from))

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {
        "uname": "echo Linux",
        "curl": _FAKE_CURL + 'printf "Connected.\\n200"',
    }.items():
        (bin_dir / name).write_text(f"#!/bin/sh\n{body}\n")
        (bin_dir / name).chmod(0o755)
    env = {"HOME": str(tmp_path), "PATH": f"{bin_dir}:/usr/bin:/bin"}
    (tmp_path / ".ssh").mkdir()
    (tmp_path / ".ssh" / "authorized_keys").write_text("ssh-rsa OLD")

    for _ in range(2):
        run = subprocess.run(
            ["sh", "-c", rendered], env=env, capture_output=True, text=True
        )
        assert run.returncode == 0, run.stderr
    assert "Connected." in run.stdout

    keys = (tmp_path / ".ssh" / "authorized_keys").read_text()
    line = authorized_key_line(f"{_KEY} langalpha", login_from)
    assert keys == f"ssh-rsa OLD\n{line}\n"
    args = (tmp_path / "curl-args").read_text()
    # The token stays off curl's arguments, which any account can list.
    assert "abc" not in args
    assert "user=" in args
    # Each run names itself, so the server tells the two apart.
    runs = [a for a in args.splitlines() if a.startswith("run=")]
    assert len(runs) == 2 and len(set(runs)) == 2 and "run=" not in runs
    assert (tmp_path / "curl-config").read_text() == f'url = "{_REPORT_URL}"\n'


@pytest.mark.asyncio
async def test_a_script_that_stops_tells_the_server_why(tmp_path, monkeypatch):
    """Without it the dialog would wait on a run that has ended until it expires."""
    _serve(monkeypatch)
    rendered = await script.render_script("abc", _SETTINGS)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {"uname": "echo Linux", "curl": _FAKE_CURL}.items():
        (bin_dir / name).write_text(f"#!/bin/sh\n{body}\n")
        (bin_dir / name).chmod(0o755)
    # A file where the folder goes: the script cannot authorize the key.
    (tmp_path / ".ssh").write_text("")
    run = subprocess.run(
        ["sh", "-c", rendered],
        env={"HOME": str(tmp_path), "PATH": f"{bin_dir}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )

    assert run.returncode == 1
    told = (tmp_path / "curl-args").read_text().splitlines()
    assert f"failed=Could not create {tmp_path}/.ssh." in told
    assert any(a.startswith("run=") and a != "run=" for a in told)
    assert (tmp_path / "curl-config").read_text() == f'url = "{_REPORT_URL}"\n'


# Stand-ins for what a tunneled run reaches on a Linux machine, the service
# managers among them, so nothing here starts, stops or reads a real service.
# The report answers 202 to send the run off to tunnel, and then 200.
_TUNNEL_FAKES = {
    "uname": "echo Linux",
    "curl": _FAKE_CURL
    + """
case $(cat "$HOME/curl-config") in
  *'/tunnel"'*) printf 'tunnel_id=%s\\ngateway_host=gw.example\\ngateway_port=2022\\n' "$TID"
    printf 'known_hosts=[gw.example]:2022 ssh-ed25519 AAAAgateway\\n200' ;;
  *) n=$(( $(cat "$HOME/reports" 2>/dev/null || echo 0) + 1 )); echo $n > "$HOME/reports"
    if [ $((n % 2)) = 1 ]; then printf 'tunnel\\nIt connects out.\\n202'; else printf 'Connected.\\n200'; fi ;;
esac""",
    "ssh-keyscan": "echo localhost ssh-ed25519 AAAAmachine",
    "ssh-keygen": """
y=
while [ $# -gt 0 ]; do case $1 in -y) y=1 ;; -f) f=$2; shift ;; esac; shift; done
if [ -n "$y" ]; then echo "ssh-ed25519 AAAA$(cksum < "$f" | cut -d' ' -f1)"
else echo "key $$" > "$f" && echo "ssh-ed25519 AAAAx" > "$f.pub"; fi""",
    "ssh": "exit 0",
    "systemctl": 'echo "$*" >> "$HOME/systemctl.log"',
    "loginctl": "exit 0",
    "crontab": "exit 0",
    "launchctl": 'echo "$*" >> "$HOME/launchctl.log"; exit 1',
}


@pytest.mark.asyncio
async def test_tunnels_to_two_deployments_live_side_by_side(tmp_path, monkeypatch):
    """One login paired with two deployments keeps a tunnel to each, and
    pairing again with one replaces only its own, keeping its key."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in _TUNNEL_FAKES.items():
        (bin_dir / name).write_text(f"#!/bin/sh\n{body}\n")
        (bin_dir / name).chmod(0o755)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(script, "tunnels_enabled", lambda: True)
    units = home / ".config" / "systemd" / "user"
    deployments = {
        "tunnel-aaaaaaaa": "11111111-1111-4111-8111-111111111111",
        "tunnel-bbbbbbbb": "22222222-2222-4222-8222-222222222222",
    }

    async def pair(name):
        _serve(monkeypatch, tunnel_name=name)
        run = subprocess.run(
            ["sh", "-c", await script.render_script("abc", _SETTINGS)],
            env={
                "HOME": str(home),
                "PATH": f"{bin_dir}:/usr/bin:/bin",
                "TID": deployments[name],
            },
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert run.returncode == 0, run.stdout + run.stderr

    await pair("tunnel-aaaaaaaa")
    key = (home / ".langalpha" / "tunnel-aaaaaaaa" / "id_ed25519").read_text()
    await pair("tunnel-bbbbbbbb")
    await pair("tunnel-aaaaaaaa")

    for name, tunnel_id in deployments.items():
        d = home / ".langalpha" / name
        assert (d / "tunnel_id").read_text() == f"{tunnel_id}\n"
        uninstall = (d / "uninstall.sh").read_text()
        assert f"label=ai.langalpha.{name}\n" in uninstall
        assert f"unit=langalpha-{name}.service\n" in uninstall
        assert f"ExecStart=/bin/sh {d}/run.sh\n" in (
            units / f"langalpha-{name}.service"
        ).read_text()
    assert (home / ".langalpha" / "tunnel-aaaaaaaa" / "id_ed25519").read_text() == key
    log = (home / "systemctl.log").read_text().splitlines()
    assert log.count("--user enable --now langalpha-tunnel-aaaaaaaa.service") == 2
    assert log.count("--user enable --now langalpha-tunnel-bbbbbbbb.service") == 1
    # Pairing again took down its own service, and never the other's.
    assert log.count("--user disable --now langalpha-tunnel-aaaaaaaa.service") == 1
    assert "--user disable --now langalpha-tunnel-bbbbbbbb.service" not in log


@pytest.mark.asyncio
@pytest.mark.parametrize("tunnels", [False, True])
async def test_the_script_parses(monkeypatch, tunnels):
    _serve(monkeypatch)
    monkeypatch.setattr(script, "tunnels_enabled", lambda: tunnels)
    rendered = await script.render_script("abc", _SETTINGS)

    run = subprocess.run(["sh", "-n"], input=rendered, capture_output=True, text=True)

    assert run.returncode == 0, run.stderr
    assert ("setup.lock" in rendered) is tunnels


# Every script as rendered, by sha256, pinned when its parts moved out of
# Python strings: the parts are reassembled, never rewritten. Changed since:
# the key line, which carries its options, the URLs, which reach curl as
# config on its stdin, the host keys asked of the machine's own sshd when no
# file has them, the tunnel loop's waits, drawn at random, the run each
# report and stop names, and the tunnel's folder and service, named for the
# deployment (named ``tunnel``, the tunneled ones render as pinned before).
_RENDERED = {
    ("https://langalpha.example.com", False): (
        "164822f6f1dd3da3692d25d8303c49b61072679dcd8ca7f2cdaf75bb167738ea"
    ),
    ("https://langalpha.example.com", True): (
        "dc6bcf339ca15117ed8521385a2fbf840f38921ddf7d067fbcc0c97cbb6120cb"
    ),
    ("http://127.0.0.1:8000", False): (
        "075b2891f7524801fa0f587509217b062dfac0790f4d8b04e9b5abfa77cac524"
    ),
    ("http://127.0.0.1:8000", True): (
        "666c9d86ed18ad54bee6237ae08848a594a08adb32248e7422a10b1f24995965"
    ),
    ("http://[::1]:8000", False): (
        "2b70537889e0496254c0999740ee53fc4bec8f71d9307c85dcde00e176327d1e"
    ),
    ("http://[::1]:8000", True): (
        "953fbfde166535d4b1452d5be43f8d198a596e210f1ba7718fd7b285886cc3bf"
    ),
    (None, False): "59d0a2e548d9fa61be969ee269ed92e8620e74903b4fc7f3357faf1a8c489c89",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("base,tunnels", list(_RENDERED))
async def test_the_script_renders_byte_for_byte_as_it_did(monkeypatch, base, tunnels):
    async def load(token):
        if base is None:
            return None
        return _pairing(token=token, base=base)

    async def key(user_id):
        assert user_id == "u"
        return {"public_key": f"{_KEY} langalpha"}

    monkeypatch.setattr(state, "load", load)
    monkeypatch.setattr(script, "ensure_client_key", key)
    monkeypatch.setattr(script, "tunnels_enabled", lambda: tunnels)
    monkeypatch.setattr(
        script, "deployment_tunnel_name", AsyncMock(return_value=_TUNNEL_NAME)
    )

    rendered = await script.render_script("0123456789abcdef0123456789abcdef", _SETTINGS)

    assert hashlib.sha256(rendered.encode()).hexdigest() == _RENDERED[(base, tunnels)]


@pytest.mark.asyncio
async def test_an_expired_command_says_so_instead_of_failing_silently(monkeypatch):
    async def load(token):
        return None

    monkeypatch.setattr(state, "load", load)
    run = subprocess.run(
        ["sh", "-c", await script.render_script("gone", _SETTINGS)],
        capture_output=True,
        text=True,
    )

    assert run.returncode == 1
    assert "expired" in run.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("login_from", [[], ["203.0.113.7"]])
async def test_the_helper_is_given_the_line_the_script_would_authorize(
    monkeypatch, login_from
):
    _serve(monkeypatch)
    settings = SSHConfig(login_from=login_from)

    line = await script.pairing_key("abc", settings)

    assert line == authorized_key_line(f"{_KEY} langalpha", login_from)
    assert shlex.quote(line) in await script.render_script("abc", settings)


@pytest.mark.asyncio
async def test_an_expired_pairing_gives_the_helper_no_key(monkeypatch):
    monkeypatch.setattr(state, "load", AsyncMock(return_value=None))

    assert await script.pairing_key("gone", _SETTINGS) is None


class _Redis:
    """The few hash commands a pairing uses, each transaction applied whole."""

    def __init__(self):
        self.hashes = {}

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def pipeline(self, transaction):
        assert transaction
        return _Pipeline(self)


class _Pipeline:
    def __init__(self, redis):
        self.redis, self.queued = redis, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __getattr__(self, command):
        return lambda *args, **kwargs: self.queued.append((command, args, kwargs))

    async def execute(self):
        hashes, results = self.redis.hashes, []
        for command, args, kwargs in self.queued:
            key = args[0]
            if command == "exists":
                results.append(int(key in hashes))
            elif command == "hset":
                fields = kwargs.get("mapping") or {args[1]: args[2]}
                hashes.setdefault(key, {}).update(
                    {f.encode(): v.encode() for f, v in fields.items()}
                )
                results.append(len(fields))
            elif command == "hsetnx":
                fields = hashes.setdefault(key, {})
                results.append(int(args[1].encode() not in fields))
                fields.setdefault(args[1].encode(), args[2].encode())
            elif command == "hmget":
                fields = hashes.get(key, {})
                results.append([fields.get(f.encode()) for f in args[1]])
            elif command == "hdel":
                for field in args[1:]:
                    hashes.get(key, {}).pop(field.encode(), None)
                results.append(len(args) - 1)
            elif command == "pexpireat":
                if args[1] <= time.time() * 1000:
                    hashes.pop(key, None)
                results.append(1)
        return results


@pytest.fixture
def redis(monkeypatch):
    fake = _Redis()
    monkeypatch.setattr(state, "_redis", lambda: fake)
    return fake


async def _store(*, expires_in=15 * 60, **fields):
    """A pairing as the mint left it, then the given fields as their writers would."""
    pairing = _pairing(expires_in=expires_in)
    minted = {
        "user_id": pairing.user_id,
        "base": pairing.base,
        "expires_at": pairing.expires_at.isoformat(),
    }
    await state._write(pairing, {"pairing": json.dumps(minted), **fields})
    return await state.load(pairing.token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "said,shown",
    [
        (
            "Could not make a key\nfor the tunnel.\x1b[2J",
            "Could not make a key for the tunnel.[2J",
        ),
        ("", "The command stopped on the machine before it finished. Run it again."),
    ],
)
async def test_a_script_that_stopped_fails_its_pairing_with_what_it_said(
    redis, said, shown
):
    machine = state.ReportedMachine(username="dev", hostname="box")
    tunneling = state.PairingResult(status="tunneling")
    await _store(
        machine=machine.model_dump_json(),
        **{state._result_field(""): tunneling.model_dump_json()},
    )

    status, message = await flow.report(
        "abc", _report(failed=said), sources=[], settings=_SETTINGS
    )

    assert (status, message) == (200, shown)
    view = (await state.load("abc")).view()
    assert view.status == "failed"
    assert view.error.model_dump() == {"code": "script_failed", "message": shown}
    assert view.machine.username == "dev"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stopping,fails",
    [("run-1", False), ("run-2", True), ("", True)],
    ids=["earlier-run", "followed-run", "unnamed"],
)
async def test_only_the_run_a_pairing_follows_can_stop_it(redis, stopping, fails):
    """The command run again, then the first terminal stopped, as by a
    Ctrl-C: the second run still sets up its tunnel. A script that does not
    name its run is taken at its word, as before runs were named."""
    machine = state.ReportedMachine(username="dev", hostname="box", run="run-2")
    tunneling = state.PairingResult(status="tunneling")
    await _store(
        machine=machine.model_dump_json(),
        **{state._result_field(""): tunneling.model_dump_json()},
    )

    status, message = await flow.report(
        "abc", _report(failed="Stopped.", run=stopping), sources=[], settings=_SETTINGS
    )

    assert (status, message) == (200, "Stopped.")
    pairing = await state.load("abc")
    assert pairing.view().status == ("failed" if fails else "tunneling")
    if not fails:
        assert (await flow._ask_to_connect_out(pairing))[0] == 202


@pytest.mark.asyncio
async def test_a_refused_report_fails_its_pairing_for_the_dialog(redis):
    """The script stops on the 422 without telling the server, as it does on any."""
    await _store()

    status, message = await flow.report(
        "abc", _report(user="dev@corp.example", hostname="box"), sources=[],
        settings=_SETTINGS,
    )

    assert status == 422
    view = (await state.load("abc")).view()
    assert (view.status, view.error.message) == ("failed", message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "computer_status,refused",
    [("stopped", False), ("starting", True), ("running", True)],
)
async def test_a_bound_report_is_refused_once_its_computer_started(
    redis, monkeypatch, computer_status, refused
):
    """Started meanwhile (another tab, a turn): pairing the machine again
    would drop the tunnel the running computer is reached through, and the
    reconnect naming the new one would be refused. Refused before the
    machine takes the new key and tunnel, which it does only on an answer."""
    pairing = _pairing()
    minted = {
        "user_id": pairing.user_id,
        "base": pairing.base,
        "expires_at": pairing.expires_at.isoformat(),
        "computer_id": "c-1",
    }
    await state._write(pairing, {"pairing": json.dumps(minted)})
    computer = {
        "computer_id": "c-1",
        "user_id": pairing.user_id,
        "kind": "ssh",
        "status": computer_status,
        "config": {},
    }
    monkeypatch.setattr(flow, "get_computer", AsyncMock(return_value=computer))

    status, message = await flow.report(
        "abc", _report(user="dev@corp.example", hostname="box"), sources=[],
        settings=_SETTINGS,
    )

    if refused:
        assert (status, message) == (409, "Stop the computer before you repair it.")
        view = (await state.load("abc")).view()
        assert (view.status, view.error.message) == ("failed", message)
    else:
        # Past the check, to the report's own refusal of that login.
        assert status == 422


@pytest.mark.asyncio
async def test_a_report_through_a_tunnel_being_taken_off_waits_for_it(
    redis, monkeypatch
):
    """Dialing through it would race the uninstall that is about to remove it."""
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(flow, "renew_tunnel", AsyncMock(return_value=False))
    await _store()

    status, message = await flow.report(
        "abc", _report(**_CHECKED_IN, tunnel_id=_TUNNEL), sources=[],
        settings=_SETTINGS,
    )

    assert (status, message) == (409, flow._TAKING_OFF)
    assert (await state.load("abc")).view().status == "failed"


@pytest.mark.asyncio
async def test_a_script_that_stopped_during_the_dial_is_not_sent_to_set_up_a_tunnel(
    redis,
):
    pairing = await _store(stopped="This command was stopped.")

    status, message = await flow._ask_to_connect_out(pairing)

    assert (status, message) == (409, "This command was stopped.")
    assert (await state.load("abc")).result is None


def _dial_meanwhile(monkeypatch, meanwhile, found=None):
    """A report's dial during which *meanwhile* runs, as another request would."""
    monkeypatch.setattr(flow, "client_key", AsyncMock(return_value="k"))
    monkeypatch.setattr(flow, "get_computers_for_user", AsyncMock(return_value=[]))

    async def probe(private_key, **_):
        await meanwhile()
        if found is None:
            raise SSHConnectError("unreachable", "no answer")
        return found

    monkeypatch.setattr(dial, "probe", probe)


_CHECKED_IN = {
    "user": "dev",
    "hostname": "box",
    "addresses": "203.0.113.10",
    "host_keys": _KEY,
}
# The documentation addresses the machine reports are not public ones.
_ANYWHERE = SSHConfig(allow_private_addresses=True)
# What a check-in of ``_CHECKED_IN`` binds the pairing to.
_BOUND = {"bound_user": "dev", state._bound_keys_field("dev"): json.dumps([_KEY])}


@pytest.mark.asyncio
async def test_a_stop_sent_during_the_dial_stands_over_its_failure(redis, monkeypatch):
    """The dial's result is written after the stop, and must not undo it."""

    async def stopped():
        await flow.report(
            "abc", _report(failed="Stopped."), sources=[], settings=_SETTINGS
        )

    _dial_meanwhile(monkeypatch, stopped)
    await _store()

    await flow.report("abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE)

    view = (await state.load("abc")).view()
    assert (view.status, view.error.code) == ("failed", "script_failed")
    assert view.machine.username == "dev"


@pytest.mark.asyncio
async def test_a_machine_reached_stands_over_a_stop_sent_meanwhile(redis, monkeypatch):
    """It is still there to add, as when the stop comes after the dial."""

    async def stopped():
        await flow.report(
            "abc", _report(failed="Stopped."), sources=[], settings=_SETTINGS
        )

    _dial_meanwhile(monkeypatch, stopped, found=_found())
    await _store()

    status, _ = await flow.report(
        "abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE
    )

    assert status == 200
    assert (await state.load("abc")).view().status == "ready"


@pytest.mark.asyncio
async def test_an_earlier_runs_dial_does_not_undo_a_later_runs_result(
    redis, monkeypatch
):
    """The command run again during the first run's dial: the first settles last."""
    dials = 0

    async def probe(private_key, **_):
        nonlocal dials
        dials += 1
        if dials == 1:
            await flow.report(
                "abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE
            )
            raise SSHConnectError("unreachable", "no answer")
        return _found()

    monkeypatch.setattr(flow, "client_key", AsyncMock(return_value="k"))
    monkeypatch.setattr(flow, "get_computers_for_user", AsyncMock(return_value=[]))
    monkeypatch.setattr(dial, "probe", probe)
    await _store()

    status, _ = await flow.report(
        "abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE
    )

    assert status == 422
    assert (await state.load("abc")).view().status == "ready"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "other,refused",
    [
        ({"user": "intruder"}, (409, flow._ANOTHER_LOGIN)),
        # The bound account's name, on a machine that is not the one bound.
        ({"host_keys": _SECOND_KEY}, (409, flow._ANOTHER_LOGIN)),
        # No run of the bound login reports an account SSH cannot log in as.
        (
            {"user": "-oProxyCommand=x"},
            (422, "'-oProxyCommand=x' is not an account name SSH can log in as."),
        ),
    ],
)
async def test_a_report_from_another_login_leaves_the_bound_run_standing(
    redis, other, refused
):
    """The token is a credential others may see, as in another account's
    process list or on a shared screen: their run must neither stand in for
    the machine being added nor fail it."""
    machine = state.ReportedMachine(username="dev", host_keys=[_KEY])
    tunneling = state.PairingResult(status="tunneling")
    await _store(
        machine=machine.model_dump_json(),
        attempt="a1",
        **{state._result_field("a1"): tunneling.model_dump_json()},
        **_BOUND,
    )

    answer = await flow.report(
        "abc", _report(**{**_CHECKED_IN, **other}), sources=[], settings=_ANYWHERE
    )

    assert answer == refused
    after = await state.load("abc")
    assert (after.attempt, after.view().status) == ("a1", "tunneling")


@pytest.mark.asyncio
async def test_a_later_run_is_dialed_with_the_bound_host_keys_alone(redis, monkeypatch):
    """A key named beside a bound one, as a machine elsewhere would name its own."""
    pinned = []

    async def probe(private_key, *, host_key, **_):
        pinned.append(host_key)
        return _found()

    monkeypatch.setattr(flow, "client_key", AsyncMock(return_value="k"))
    monkeypatch.setattr(flow, "get_computers_for_user", AsyncMock(return_value=[]))
    monkeypatch.setattr(dial, "probe", probe)
    await _store()

    for host_keys in (_KEY, f"{_SECOND_KEY}\n{_KEY}"):
        await flow.report(
            "abc",
            _report(**{**_CHECKED_IN, "host_keys": host_keys}),
            sources=[],
            settings=_ANYWHERE,
        )

    assert pinned == [_KEY, _KEY]


@pytest.mark.asyncio
async def test_a_machine_naming_no_host_keys_is_not_dialed(redis, monkeypatch):
    """Whatever answered at its address would be taken for the machine."""
    probe = AsyncMock()
    monkeypatch.setattr(dial, "probe", probe)
    await _store()

    status, _ = await flow.report(
        "abc",
        _report(**{**_CHECKED_IN, "host_keys": ""}),
        sources=[],
        settings=_ANYWHERE,
    )

    assert status == 422
    probe.assert_not_awaited()
    view = (await state.load("abc")).view()
    assert (view.status, view.error.code) == ("failed", "no_host_key")
    assert view.machine.username == "dev"


@pytest.mark.asyncio
async def test_a_report_naming_another_accounts_tunnel_is_refused_not_taken_over(
    redis, monkeypatch
):
    """One login paired from two accounts: a tunnel of this one's in its place
    would leave the other's computers on the machine unreachable."""
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(flow, "renew_tunnel", AsyncMock(return_value=None))
    monkeypatch.setattr(flow, "get_tunnel", AsyncMock(return_value={}))
    monkeypatch.setattr(
        flow, "deployment_tunnel_name", AsyncMock(return_value=_TUNNEL_NAME)
    )
    await _store()

    status, message = await flow.report(
        "abc",
        _report(**_CHECKED_IN, tunnel_id=_TUNNEL),
        sources=[],
        settings=_SETTINGS,
    )

    assert status == 409
    # This deployment's tunnel, the one the machine reported.
    assert f"sh ~/.langalpha/{_TUNNEL_NAME}/uninstall.sh," in message
    view = (await state.load("abc")).view()
    assert (view.status, view.error.message) == ("failed", message)


@pytest.mark.asyncio
async def test_a_tunnel_is_handed_out_for_one_key_per_attempt(redis, monkeypatch):
    """Else a token someone else saw mints a tunnel per key they make up."""
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(
        flow, "gateway_public_address", lambda: GatewayAddress("gw.example.com", 2022)
    )
    monkeypatch.setattr(
        flow, "gateway_host_key", AsyncMock(return_value=(f"{_KEY} gw", ""))
    )
    upsert = AsyncMock(return_value={"tunnel_id": _TUNNEL})
    monkeypatch.setattr(flow, "upsert_tunnel", upsert)
    tunneling = state.PairingResult(status="tunneling")
    await _store(
        attempt="a1", **{state._result_field("a1"): tunneling.model_dump_json()}
    )

    def ask(key):
        form = ComputerSSHTunnelRequest(public_key=f"{key} tunnel", hostname="box")
        return flow.issue_tunnel("abc", form)

    first = await ask(_KEY)

    assert first[0] == 200 and f"tunnel_id={_TUNNEL}" in first[1]
    # Asked again, as by a retried request: the same tunnel.
    assert await ask(_KEY) == first
    assert await ask(_SECOND_KEY) == (409, flow._ANOTHER_TUNNEL_KEY)
    assert upsert.await_count == 2


@pytest.mark.asyncio
async def test_the_owners_retry_stands_over_a_script_that_stopped(redis, monkeypatch):
    async def nothing():
        pass

    _dial_meanwhile(monkeypatch, nothing)
    machine = state.ReportedMachine(username="dev", hostname="box", host_keys=[_KEY])
    await _store(machine=machine.model_dump_json(), stopped="Stopped.", **_BOUND)

    view = await flow.connect(
        "u", "abc", host="203.0.113.10", port=2222, settings=_SETTINGS
    )

    assert (view.status, view.error.code) == ("failed", "unreachable")
    assert (await state.load("abc")).view() == view


@pytest.mark.asyncio
async def test_a_pairing_that_lapsed_during_the_dial_says_so_not_connected(
    redis, monkeypatch
):
    """Its terminal would otherwise send the owner back to a pairing that is gone."""

    async def lapsed():
        await asyncio.sleep(0.3)
        redis.hashes.clear()  # as Redis expires it

    _dial_meanwhile(monkeypatch, lapsed, found=_found())
    await _store(expires_in=0.2)

    answer = await flow.report(
        "abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE
    )

    assert answer == (404, state.EXPIRED)
    # The result's write made the key anew, already expired.
    assert redis.hashes == {}


@pytest.mark.asyncio
async def test_a_tunneled_machines_folder_skips_one_its_tunnel_already_has(
    redis, monkeypatch
):
    """By tunnel as the create's own check goes: the machine's host key alone
    may have changed since the first computer was made on it."""

    async def nothing():
        pass

    sibling = {
        "kind": "ssh",
        "name": "first",
        "root_dir": "/home/dev/langalpha",
        "provider_config": {"host": "box", "host_key": _OTHER_KEY, "tunnel": _TUNNEL},
    }
    _dial_meanwhile(monkeypatch, nothing, found=_found())
    monkeypatch.setattr(
        flow, "get_computers_for_user", AsyncMock(return_value=[sibling])
    )
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(flow, "renew_tunnel", AsyncMock(return_value=True))
    monkeypatch.setattr(flow, "set_tunnel_login", AsyncMock())
    await _store(tunnel_id=_TUNNEL)

    status, _ = await flow.report(
        "abc",
        _report(**_CHECKED_IN, tunnel_id=_TUNNEL),
        sources=[],
        settings=_SETTINGS,
    )

    assert status == 200
    view = (await state.load("abc")).view()
    assert view.target.tunnel_id == _TUNNEL
    assert view.probe.suggested_root_dir == "/home/dev/langalpha-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shell", [s for s in ("sh", "dash", "bash", "zsh") if shutil.which(s)]
)
async def test_the_command_hands_curl_its_url_on_stdin(
    tmp_path, monkeypatch, redis, shell
):
    """The token stays off every process's arguments, which any account can
    list, as the script's own requests keep it."""
    monkeypatch.setattr(state, "ensure_client_key", AsyncMock())
    monkeypatch.setattr(
        state, "effective_relay_base_url", lambda kind: "https://example.com"
    )
    pairing = await state.create_pairing("u", request_base="https://example.com")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "curl").write_text(f"#!/bin/sh\n{_FAKE_CURL}echo echo ran\n")
    (bin_dir / "curl").chmod(0o755)
    run = subprocess.run(
        [shell, "-c", pairing.command],
        env={"HOME": str(tmp_path), "PATH": f"{bin_dir}:/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )

    assert run.returncode == 0, run.stderr
    assert run.stdout == "ran\n"
    assert pairing.token not in (tmp_path / "curl-args").read_text()
    config = (tmp_path / "curl-config").read_text()
    assert config == f'url = "{pairing.script_url}"\n'


@pytest.mark.parametrize(
    "base,local",
    [
        ("http://localhost:8000", True),
        ("http://127.0.0.1:8000", True),
        ("http://[::1]:8000", True),
        ("http://app.localhost", True),
        ("http://192.168.1.20:8000", False),
        ("https://langalpha.example.com", False),
    ],
)
def test_a_loopback_base_makes_a_command_for_this_machine_only(base, local):
    assert state.base_is_local(base) is local


@pytest.mark.parametrize(
    "configured,expected",
    [
        ("http://host.docker.internal:8000", "http://127.0.0.1:8000"),
        ("http://host.containers.internal:8000/", "http://127.0.0.1:8000"),
        ("https://langalpha.example.com/", "https://langalpha.example.com"),
        ("http://192.168.1.20:8000", "http://192.168.1.20:8000"),
        ("", "http://127.0.0.1:8000"),
    ],
)
def test_a_base_only_containers_resolve_gives_way_to_the_browsers(
    monkeypatch, configured, expected
):
    monkeypatch.setattr(state, "effective_relay_base_url", lambda kind: configured)

    assert state.pairing_base("http://127.0.0.1:8000/") == expected


@pytest.fixture
def dial_budget(monkeypatch):
    """The budget replaced by a recorder, and the dial by a counter."""
    spent: list[int] = []
    dialed: list[str] = []
    refuse: list[bool] = []

    async def spend(user_id, attempts, *, settings):
        spent.append(attempts)
        if refuse:
            raise SSHDialBudgetExceeded(120)

    monkeypatch.setattr(flow, "spend_dials", spend)
    monkeypatch.setattr(flow, "client_key", AsyncMock(return_value="k"))
    monkeypatch.setattr(flow, "get_computers_for_user", AsyncMock(return_value=[]))

    async def probe(private_key, *, host, **_):
        dialed.append(host)
        raise SSHConnectError("unreachable", "no answer")

    monkeypatch.setattr(dial, "probe", probe)
    return SimpleNamespace(spent=spent, dialed=dialed, refuse=refuse)


@pytest.mark.asyncio
async def test_a_report_spends_one_dial_per_address_and_port(redis, dial_budget):
    await _store()
    form = {**_CHECKED_IN, "addresses": "203.0.113.10 203.0.113.11", "ports": "22 2222"}

    await flow.report("abc", _report(**form), sources=[], settings=_ANYWHERE)

    assert dial_budget.spent == [4]
    assert len(dial_budget.dialed) == 4


@pytest.mark.asyncio
async def test_a_report_past_the_budget_dials_nothing_and_fails_its_pairing(
    redis, dial_budget
):
    await _store()
    dial_budget.refuse.append(True)

    status, message = await flow.report(
        "abc", _report(**_CHECKED_IN), sources=[], settings=_ANYWHERE
    )

    assert status == 429
    assert "too many SSH connections" in message
    assert dial_budget.dialed == []
    # The dialog shows why instead of waiting on a machine that checked in.
    view = (await state.load("abc")).view()
    assert view.status == "failed"


@pytest.mark.asyncio
async def test_a_tunnel_candidate_costs_nothing(redis, dial_budget, monkeypatch):
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(flow, "renew_tunnel", AsyncMock(return_value=True))
    await _store()

    await flow.report(
        "abc",
        _report(**_CHECKED_IN, can_tunnel="1", tunnel_id=_TUNNEL),
        sources=[],
        settings=_ANYWHERE,
    )

    assert dial_budget.spent == [0]


@pytest.mark.asyncio
async def test_connect_by_address_spends_one_dial(redis, dial_budget):
    await _store(**_BOUND, machine=_machine("dev").model_dump_json())

    await flow.connect(
        _pairing().user_id, "abc", host="203.0.113.10", port=22, settings=_ANYWHERE
    )

    assert dial_budget.spent == [1]


@pytest.mark.asyncio
async def test_a_machine_with_no_ssh_server_is_sent_to_tunnel_undialed(
    redis, dial_budget, monkeypatch
):
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(dial, "tunnels_enabled", lambda: True)
    await _store()

    status, _ = await flow.report(
        "abc",
        _report(**_CHECKED_IN, tunnel_only="1"),
        sources=[],
        settings=_ANYWHERE,
    )

    assert status == 202
    assert dial_budget.spent == [0]
    assert dial_budget.dialed == []


@pytest.mark.asyncio
async def test_a_machine_with_no_ssh_server_is_refused_where_tunnels_are_off(
    redis, dial_budget, monkeypatch
):
    monkeypatch.setattr(flow, "tunnels_enabled", lambda: False)
    await _store()

    status, message = await flow.report(
        "abc",
        _report(**_CHECKED_IN, tunnel_only="1"),
        sources=[],
        settings=_ANYWHERE,
    )

    assert (status, message) == (422, flow._NO_TUNNEL)
    assert dial_budget.dialed == []
    assert (await state.load("abc")).view().status == "failed"


@pytest.mark.asyncio
async def test_a_pairing_for_a_computer_suggests_that_computers_folder(
    redis, monkeypatch
):
    """Its own mark is on that folder, which reads to a new computer as
    another's, so the next numbered folder would be the wrong suggestion."""
    computer = {
        "computer_id": "c-1",
        "user_id": "u",
        "kind": "ssh",
        "root_dir": "/home/dev/langalpha",
        "provider_config": {"host": "box", "host_key": _KEY},
    }
    monkeypatch.setattr(flow, "get_computer", AsyncMock(return_value=computer))
    monkeypatch.setattr(flow, "get_computers_for_user", AsyncMock(return_value=[computer]))
    pairing = await _store()
    pairing = pairing.model_copy(update={"computer_id": "c-1"})
    reached = dial.Reached(host="203.0.113.10", port=22, tunnel_id=None, found=_found())

    result = await flow._settle(pairing, _machine(), reached, [], tunnel_id=None)

    assert result.probe.suggested_root_dir == "/home/dev/langalpha"

    unbound = pairing.model_copy(update={"computer_id": None})
    result = await flow._settle(unbound, _machine(), reached, [], tunnel_id=None)
    assert result.probe.suggested_root_dir == "/home/dev/langalpha-2"


@pytest.mark.asyncio
async def test_a_bound_pairing_keeps_its_computer_across_workers(redis, monkeypatch):
    monkeypatch.setattr(state, "ensure_client_key", AsyncMock())
    monkeypatch.setattr(state, "effective_relay_base_url", lambda kind: "https://example.com")

    minted = await state.create_pairing(
        "u", request_base="https://example.com", computer_id="c-1"
    )

    assert (await state.load(minted.token)).computer_id == "c-1"
