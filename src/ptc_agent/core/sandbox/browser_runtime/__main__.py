"""``python3 -I <computer root>/_internal/src/browser_runtime <command>``: see ``protocol.PACKAGE_NAME``."""

import importlib
import sys
from pathlib import Path

# Run by its folder under -I, which puts the folder itself first on the path, so
# the package's parent goes there instead and the modules import as a package.
_package = Path(__file__).resolve().parent
sys.path[0] = str(_package.parent)
_wrapper = sys.argv[1:2] == ["agent-browser"]
# The wrapper is imported alone, so that nothing the gate needs can keep a
# command from running.
_entry = importlib.import_module(f"{_package.name}.{'wrapper' if _wrapper else 'machine'}")
sys.exit(_entry.main(sys.argv[2:] if _wrapper else sys.argv[1:]))
