"""``python3 -m livefs {start,link,serve} ...``, and the Drive mount's
``drive-{start,serve,link,stop,status,notices,settle}``."""

import sys

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("drive-"):
        from .drive_daemon import main
    else:
        from .daemon import main
    sys.exit(main())
