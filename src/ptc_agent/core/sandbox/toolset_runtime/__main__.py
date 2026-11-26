"""``python3 -I <internal src>/toolset_runtime converge|status``: see ``cli``."""

import importlib
import sys
from pathlib import Path

# Run by its folder under -I, which puts the folder itself first on the path, so
# the package's parent goes there instead and the modules import as a package.
_package = Path(__file__).resolve().parent
sys.path[0] = str(_package.parent)
sys.exit(importlib.import_module(f"{_package.name}.cli").main(sys.argv[1:]))
