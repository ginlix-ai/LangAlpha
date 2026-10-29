#!/usr/bin/env python3
"""Regenerate the exported market-data tool list (market_data.tools.json).

The staleness test (tests/unit/mcp_servers/test_market_data_tool_export.py) is
read-only by design; this script is the only writer. Run it after an
intentional tool, docstring or output-schema change and commit the file with it.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from tests.unit.mcp_servers.test_market_data_tool_export import (  # noqa: E402
    EXPORT_PATH,
    current_export,
    render,
)


def main() -> int:
    export = current_export()
    EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    EXPORT_PATH.write_text(render(export), encoding="utf-8")
    count = sum(len(server["tools"]) for server in export.values())
    print(f"wrote {count} tools -> {EXPORT_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
