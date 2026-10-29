"""A Mac's model name, read by the add-computer probe.

System Information's text report labels its fields in the Mac's language
("型号名称" on a Chinese Mac), so the probe reads its JSON, whose keys are not
translated.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from ptc_agent.core.sandbox.providers.ssh import _PROBE_SCRIPT, _mac_model, read_machine

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
