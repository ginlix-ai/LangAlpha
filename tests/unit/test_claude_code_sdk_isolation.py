"""The Claude Code SDK loads only for a Claude Code turn.

``claude-agent-sdk`` is an optional extra that bundles a ~280 MB CLI, so the
backend's startup and every other turn must import nothing from it: a
deployment without the extra boots and runs, and one with it pays nothing until
a Claude Code turn starts. Checked in a fresh interpreter, since this suite may
already have imported the runtime.
"""

import subprocess
import sys

_PROBE = """
import sys
import src.server.app.setup
import src.server.handlers.chat.ptc_run
import ptc_agent.agent.agent
loaded = sorted(m for m in sys.modules if m.startswith(("claude_agent_sdk", "ptc_agent.agent.claude_code")))
print("LOADED=" + ",".join(loaded))
"""


def test_backend_startup_and_the_agent_build_never_import_the_sdk():
    done = subprocess.run([sys.executable, "-c", _PROBE], capture_output=True, text=True, timeout=300, check=True)
    (line,) = [ln for ln in done.stdout.splitlines() if ln.startswith("LOADED=")]
    assert line == "LOADED=", line
