"""CI guard: the backend's ``market_protocol`` is the libs/market-protocol package.

The core project's editable install puts ``src`` first on the path, so a copy
left under ``src/`` would shadow the package and the backend would run a
protocol other services never see.
"""

from __future__ import annotations

from pathlib import Path

import market_protocol

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_market_protocol_is_the_package_not_a_copy_under_src():
    package = Path(market_protocol.__file__).resolve()
    assert REPO_ROOT / "libs" / "market-protocol" in package.parents
    # Sources, not the directory: a checkout that switched branches keeps the
    # old copy's __pycache__, which shadows nothing.
    assert not list((REPO_ROOT / "src" / "market_protocol").rglob("*.py"))
