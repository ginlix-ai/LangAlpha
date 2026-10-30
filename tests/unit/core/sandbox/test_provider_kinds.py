"""The usage ledger's kinds are answered without loading any provider.

The ledger asks which kinds are metered, and which report metrics, at every
sweep and computer status write. Read off each class, that imported every
vendor's SDK on the event loop, Daytona's alone about 3s, on a deployment that
runs none of them. So the registry declares the two sets, and the classes,
which callers holding one kind still read, must agree with it.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from ptc_agent.core.sandbox.providers import (
    METERED_KINDS,
    METRIC_KINDS,
    PROVIDER_KINDS,
    provider_class,
)

_PROBE = """
import sys
from src.server.services.computer_usage.transitions import metered_kinds, metric_kinds
metered_kinds(), metric_kinds()
loaded = sorted(
    m for m in sys.modules
    if m.startswith("ptc_agent.core.sandbox.providers.")
    and m.rpartition(".")[2] in ("daytona", "docker", "e2b", "ssh")
)
print("LOADED=" + ",".join(loaded))
"""

# The probe imports ``src``, which resolves from the repo root only: pytest's
# ``pythonpath`` does not reach a child interpreter.
_REPO_ROOT = Path(__file__).resolve().parents[4]


def test_the_sets_name_only_known_kinds():
    assert set(METRIC_KINDS) <= set(METERED_KINDS) <= set(PROVIDER_KINDS)


@pytest.mark.parametrize("kind", PROVIDER_KINDS)
def test_the_registry_agrees_with_each_class(kind):
    cls = provider_class(kind)

    assert (kind in METERED_KINDS) is cls.metered
    assert (kind in METRIC_KINDS) is (cls.metered and cls.reports_metrics)


def test_the_ledgers_kinds_load_no_provider_module():
    done = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
        cwd=_REPO_ROOT,
    )
    (line,) = [ln for ln in done.stdout.splitlines() if ln.startswith("LOADED=")]
    assert line == "LOADED=", line
