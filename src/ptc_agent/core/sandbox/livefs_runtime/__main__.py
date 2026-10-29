"""``python3 -m livefs {start,link,serve} ...``."""

import sys

from .daemon import main

if __name__ == "__main__":
    sys.exit(main())
