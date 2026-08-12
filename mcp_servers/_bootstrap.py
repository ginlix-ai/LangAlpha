"""Put the repo root on ``sys.path`` for MCP server subprocesses.

Shared code imports as both ``src.*`` (the backend's spelling) and bare
``data_client.*``, but a launched server only gets ``/app/src`` on the path
(uv editable install). Importing this module first — its launcher directory is
``sys.path[0]`` — inserts the repo root so both spellings resolve.
"""

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
