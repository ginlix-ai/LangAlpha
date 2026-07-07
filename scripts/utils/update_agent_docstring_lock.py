#!/usr/bin/env python3
"""Regenerate the agent-facing docstring lock (agent_docstring_lock.json).

The pin test (tests/unit/mcp_servers/test_agent_contract.py) is read-only by
design; this script is the ONLY writer. Run it only for intentional, reviewed
docstring/signature changes and commit the lock together with the source edit.
"""

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from tests.unit.mcp_servers.test_agent_contract import (  # noqa: E402
    _COLLECTION_ERRORS,
    LOCK_PATH,
    current_pins,
)


def main() -> int:
    if _COLLECTION_ERRORS:
        print(
            "refusing to write a reduced lock — tool collection failed for: "
            f"{_COLLECTION_ERRORS}",
            file=sys.stderr,
        )
        return 1
    pins = current_pins()
    LOCK_PATH.write_text(
        json.dumps(pins, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )
    print(f"wrote {len(pins)} pins -> {LOCK_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
