"""A Mac's model name, read by the add-computer probe.

System Information's text report labels its fields in the Mac's language
("型号名称" on a Chinese Mac), so the probe reads its JSON, whose keys are not
translated.
"""

from __future__ import annotations

import json
import os
import platform
import shlex
import subprocess
import sys

import pytest

from ptc_agent.core import claude_code_managed
from ptc_agent.core.claude_code_managed import claude_code_managed_sh
from ptc_agent.core.sandbox.providers.ssh import (
    _PROBE_SCRIPT,
    IS_MUSL_SH,
    MUSL_UNSUPPORTED,
    _mac_model,
    _probe_result,
    read_machine,
)

_MODEL_LINE = next(
    line for line in _PROBE_SCRIPT.splitlines() if line.startswith('echo "model=')
)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize("name", ["MacBook Pro", 'Mac "Studio"'])
def test_the_model_is_read_from_the_json_on_a_mac_in_any_language(tmp_path, name):
    report = {
        "SPHardwareDataType": [
            {
                "_name": "hardware_overview",
                "chip_type": "Apple M3",
                "machine_model": "Mac15,3",
                "machine_name": name,
            }
        ]
    }
    fake = tmp_path / "system_profiler"
    fake.write_text(
        "#!/bin/sh\n"
        '[ "$1" = -json ] || { echo "型号名称: MacBook Pro"; exit 0; }\n'
        f"cat <<'EOF'\n{json.dumps(report, indent=2, ensure_ascii=False)}\nEOF\n"
    )
    fake.chmod(0o755)

    out = subprocess.run(
        ["bash", "-c", _MODEL_LINE],
        capture_output=True,
        text=True,
        env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
        check=True,
    ).stdout

    key, _, raw = out.strip().partition("=")
    assert key == "model"
    assert _mac_model(raw) == name


@pytest.mark.parametrize("raw", ["", "MacBook Pro", '"unterminated', "42"])
def test_anything_but_a_json_string_is_no_model(raw):
    assert _mac_model(raw) == ""


def test_the_machine_facts_keep_only_what_the_machine_answered():
    """A virtual machine's lscpu names its processor "-", and a Mac without
    sw_vers leaves the system's name alone; neither says anything."""
    model, facts = read_machine(
        'arch=aarch64\ndistro=macOS \nmodel="MacBook Pro"\ncpu=-\ncores=12\nmemory=0\n'
    )
    assert model == "MacBook Pro"
    assert facts == {"arch": "aarch64", "cores": 12}


_LINUX_FACTS = {
    "os": "Linux",
    "arch": "x86_64",
    "home": "/home/ada",
    "bash": "1",
    "fetch": "1",
    "bootid": "1",
}
_LIBC_LINE = next(line for line in _PROBE_SCRIPT.splitlines() if "libc=musl" in line)


def test_a_musl_machine_is_refused_with_a_sentence_for_its_owner():
    probe = _probe_result("key", "fp", {**_LINUX_FACTS, "libc": "musl"}, True)
    assert not probe.supported
    assert probe.problems == [MUSL_UNSUPPORTED]


def test_a_glibc_machine_is_supported():
    probe = _probe_result("key", "fp", _LINUX_FACTS, True)
    assert probe.supported
    assert MUSL_UNSUPPORTED not in probe.problems


@pytest.mark.skipif(platform.libc_ver()[0] != "glibc", reason="reads this machine's libc, which must be glibc")
def test_the_libc_line_says_nothing_on_a_glibc_machine():
    out = subprocess.run(["sh", "-c", IS_MUSL_SH + _LIBC_LINE], capture_output=True, text=True, check=True)
    assert out.stdout == ""


@pytest.mark.parametrize("system", ["Linux", "Darwin"])
def test_the_probe_names_the_organizations_managed_claude_code_settings(tmp_path, system):
    """By the status check's own snippet, run as the probe runs it on a
    machine whose uname says ``system``; what is not a managed source's
    name is dropped. The machine stays supported."""
    assert claude_code_managed_sh() in _PROBE_SCRIPT
    line = next(ln for ln in _PROBE_SCRIPT.splitlines() if ln.startswith(claude_code_managed_sh()))
    managed = tmp_path / "claude-code"
    (managed / "managed-settings.d").mkdir(parents=True)
    (managed / "managed-settings.d" / "org.json").write_text("{}")
    (managed / "managed-mcp.json").write_text("{}")
    for folder in (claude_code_managed.CLAUDE_CODE_MANAGED_DIR_LINUX, claude_code_managed.CLAUDE_CODE_MANAGED_DIR_MAC):
        line = line.replace(shlex.quote(folder), shlex.quote(str(managed)))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uname").write_text(f"#!/bin/sh\necho {system}\n")
    (bin_dir / "uname").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    out = subprocess.run(["bash", "-c", line], capture_output=True, text=True, check=True, env=env).stdout
    key, _, value = out.strip().partition("=")
    probe = _probe_result("key", "fp", {**_LINUX_FACTS, key: value + " bogus"}, True)
    assert probe.claude_code_blocked and probe.supported
    for source in claude_code_managed.CLAUDE_CODE_MANAGED:
        assert _probe_result("key", "fp", {**_LINUX_FACTS, key: source}, True).claude_code_blocked
    assert not _probe_result("key", "fp", {**_LINUX_FACTS, key: "bogus"}, True).claude_code_blocked
    assert not _probe_result("key", "fp", _LINUX_FACTS, True).claude_code_blocked
