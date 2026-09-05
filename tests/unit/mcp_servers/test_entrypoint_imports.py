"""MCP server entrypoints must import in their real subprocess context.

The registry spawns these servers with only ``src/`` effectively on
``sys.path`` (uv editable-install .pth), so shared code is importable as
top-level ``data_client`` but NOT as ``src.*`` — each entrypoint's repo-root
bootstrap has to bridge that. A module-level ``src.`` import anywhere in the
entrypoint's import graph silently kills the server at startup (stderr is
discarded), so this is pinned here in an isolated subprocess; an in-process
import would not catch it because pytest already has the repo root on the path.

``sys.path[0]`` is the bundle the file sits in, so the ``_bootstrap`` it
imports first is that bundle's shim and the bridge runs from there. The
sandbox is a different shape -- every entrypoint flattened back into one
``mcp_servers/`` directory, reaching ``src.*`` through its own ``_internal``
mirror rather than through a repo root -- and it is not modelled here; the
files it stages are pinned in the sandbox sync tests instead.
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
_ENTRYPOINT_SERVERS = {
    "price_data_mcp_server": "langalpha_market_data",
    "fundamentals_mcp_server": "langalpha_market_data",
    "macro_mcp_server": "langalpha_market_data",
    "options_mcp_server": "langalpha_market_data",
    "yf_price_mcp_server": "yfinance",
    "yf_market_mcp_server": "yfinance",
    "yf_analysis_mcp_server": "yfinance",
    "yf_fundamentals_mcp_server": "yfinance",
    "scrape_mcp_server": "alternative_data",
}


def _import_with(module: str, launch_dir: Path, cwd: Path) -> subprocess.CompletedProcess:
    code = (
        "import sys; "
        f"sys.path = [p for p in sys.path if p not in ('', {str(_REPO_ROOT)!r})]; "
        f"sys.path.insert(0, {str(launch_dir)!r}); "
        f"sys.path.insert(1, {str(_REPO_ROOT / 'src')!r}); "
        f"import {module}"
    )
    # Scrub PYTHONPATH: the child inherits the parent env, and a repo root
    # carried there (in any spelling) would make _bootstrap non-load-bearing
    # and pass this gate vacuously.
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


@pytest.mark.parametrize("module", _ENTRYPOINT_SERVERS)
def test_entrypoint_imports_from_its_bundle(module, tmp_path):
    launch_dir = _REPO_ROOT / "plugins" / _ENTRYPOINT_SERVERS[module]
    result = _import_with(module, launch_dir, tmp_path)
    assert result.returncode == 0, f"{module} failed to import:\n{result.stderr}"


def test_every_declared_server_path_exists():
    """A bundle's ``mcp.json`` must name files that are actually there.

    Nothing else checks this. The list above is hand-written, so it pins the
    imports of the servers it names and says nothing about the paths the
    manifests point at; host-side a wrong one dies before the handshake, and
    in a sandbox ``assets.py`` logs a warning and carries on. One typo can
    therefore drop a server's tools everywhere while every suite stays green,
    and the paths now live in three manifests instead of one list.
    """
    import json

    missing = []
    for manifest in sorted((_REPO_ROOT / "plugins").glob("*/mcp.json")):
        entries = json.loads(manifest.read_text()).get("mcpServers") or {}
        for name, entry in entries.items():
            for arg in entry.get("args") or []:
                if arg.endswith(".py") and not (_REPO_ROOT / arg).is_file():
                    missing.append(f"{manifest.parent.name}/{name} -> {arg}")

    assert not missing, "mcp.json names files that do not exist: " + ", ".join(missing)


def test_entrypoint_basenames_are_unique_across_bundles():
    """Two bundles cannot ship entry points with the same file name.

    The sandbox flattens every entry point into one directory and rewrites
    argv to the bare basename, so a second `server.py` would overwrite the
    first and the surviving one would run under the other's environment. The
    bundles are separate directories, so nothing about the layout suggests the
    constraint; it holds today only because the names happen to differ.
    """
    import json
    from collections import defaultdict

    seen = defaultdict(list)
    for manifest in sorted((_REPO_ROOT / "plugins").glob("*/mcp.json")):
        entries = json.loads(manifest.read_text()).get("mcpServers") or {}
        for entry in entries.values():
            for arg in entry.get("args") or []:
                if arg.endswith(".py"):
                    seen[arg.rsplit("/", 1)[-1]].append(arg)

    clashes = {base: paths for base, paths in seen.items() if len(paths) > 1}
    assert not clashes, f"entry point basenames collide in the sandbox: {clashes}"
