"""``python3 -m supervisor <computer_root>``."""

import sys

from .daemon import main

if __name__ == "__main__":
    sys.exit(main())
