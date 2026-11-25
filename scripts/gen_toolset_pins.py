#!/usr/bin/env python
"""Write the toolset's ``chrome-policy`` unit from the browser runtime.

The places Chrome must not reach are the browser runtime's lists of hosts and
ports (``protocol.chrome_policy``); the table carries the policy as literal
content, since a machine installs it with nothing else of ours on it. This
writes that content and the version that names it into ``units.json``, and a
unit test regenerates it in memory so drift fails CI.

    uv run python scripts/gen_toolset_pins.py            # write the table
    uv run python scripts/gen_toolset_pins.py --check    # fail if stale

A change to the table ships only with a higher ``revision``; this does not
raise it, so a write that changed anything says so.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# Both roots: ``ptc_agent`` is imported bare, and the package it pulls in
# reaches ``src.*`` from the repo root.
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from ptc_agent.core.sandbox.browser_runtime import protocol  # noqa: E402
from ptc_agent.core.sandbox.toolset_runtime import units  # noqa: E402

TABLE = Path(units.__file__).with_name("units.json")
POLICY_UNIT = "chrome-policy"


def pinned(text: str) -> str:
    """*text*, a table as written, with the policy unit's content and version
    taken from the browser runtime."""
    table = json.loads(text)
    unit = next(u for u in table["units"] if u["name"] == POLICY_UNIT)
    unit["content"] = protocol.chrome_policy()
    unit["version"] = units.content_version(unit)
    return json.dumps(table, indent=2) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if the table is stale")
    args = parser.parse_args()
    current = TABLE.read_text()
    wanted = pinned(current)
    if wanted == current:
        return 0
    if args.check:
        print(f"{TABLE} is stale: run scripts/gen_toolset_pins.py and raise its revision", file=sys.stderr)
        return 1
    TABLE.write_text(wanted)
    print(f"wrote {TABLE}; raise its revision before it ships")
    return 0


if __name__ == "__main__":
    sys.exit(main())
