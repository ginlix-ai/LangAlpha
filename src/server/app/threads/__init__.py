"""
Unified Thread Router — all thread-related endpoints under /api/v1/threads.

Route definitions are thin; business logic lives in handlers/.
"""

from ._deps import router

# Leaf imports register routes on the shared router; order preserves the
# pre-split registration order.
from . import crud  # noqa: E402,F401
from . import messaging  # noqa: E402,F401
from . import tasks  # noqa: E402,F401
from . import sharing  # noqa: E402,F401
from . import feedback  # noqa: E402,F401
from . import provenance  # noqa: E402,F401

__all__ = ["router"]
