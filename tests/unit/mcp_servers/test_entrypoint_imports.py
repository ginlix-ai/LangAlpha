"""MCP server entrypoints must import in their real subprocess context.

The registry spawns these servers with only ``src/`` effectively on
``sys.path`` (uv editable-install .pth), so shared code is importable as
top-level ``data_client`` but NOT as ``src.*`` — each entrypoint's repo-root
bootstrap has to bridge that. A module-level ``src.`` import anywhere in the
entrypoint's import graph silently kills the server at startup (stderr is
discarded), so this is pinned here in an isolated subprocess; an in-process
import would not catch it because pytest already has the repo root on the path.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]

# yfinance servers import ``src.market_protocol`` at module level (via
# _yf_common), so they too need the _bootstrap repo-root bridge — the same
# regression class this gate exists to catch.
_ENTRYPOINT_SERVERS = [
    "price_data_mcp_server",
    "fundamentals_mcp_server",
    "macro_mcp_server",
    "options_mcp_server",
    "yf_price_mcp_server",
    "yf_market_mcp_server",
    "yf_analysis_mcp_server",
    "yf_fundamentals_mcp_server",
]


@pytest.mark.parametrize("module", _ENTRYPOINT_SERVERS)
def test_entrypoint_imports_without_repo_root_on_path(module, tmp_path):
    code = (
        "import sys; "
        f"sys.path = [p for p in sys.path if p not in ('', {str(_REPO_ROOT)!r})]; "
        f"sys.path.insert(0, {str(_REPO_ROOT / 'mcp_servers')!r}); "
        f"sys.path.insert(1, {str(_REPO_ROOT / 'src')!r}); "
        f"import {module}"
    )
    # Scrub PYTHONPATH: the child inherits the parent env, and a repo root
    # carried there (in any spelling) would make _bootstrap non-load-bearing
    # and pass this gate vacuously.
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, f"{module} failed to import:\n{result.stderr}"
