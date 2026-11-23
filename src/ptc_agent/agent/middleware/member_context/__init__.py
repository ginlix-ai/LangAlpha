"""A member's own context, on their own messages in a shared workspace.

A shared workspace's build carries nothing of any one member, because every
member reads what the agent writes there. Where the member's organization
grants it, the main agent is given, on that member's own messages only, their
answer preferences and their personal memory, read only. Nothing of it is
written into the thread: it is added to each call in the member's streak
(``context.py``) and read again on each turn, and a read of their memory
files (``files.py``) is kept in the thread as a stub (``middleware.py``).
"""

from ptc_agent.agent.middleware.member_context.context import MemberContext
from ptc_agent.agent.middleware.member_context.files import (
    MEMBER_READ_KEY,
    MemberMemory,
    MemberMount,
    MemberRead,
    MembersClosed,
    close_members,
    member_memory,
)
from ptc_agent.agent.middleware.member_context.middleware import MemberContextMiddleware

__all__ = [
    "MEMBER_READ_KEY",
    "MemberContext",
    "MemberContextMiddleware",
    "MemberMemory",
    "MemberMount",
    "MemberRead",
    "MembersClosed",
    "close_members",
    "member_memory",
]
