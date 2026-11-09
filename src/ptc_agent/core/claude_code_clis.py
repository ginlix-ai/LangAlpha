"""How many Claude Code CLIs a computer of the user's own runs at once.

Its owner picks the number, 1 to ``CLIS_MAX``; one never set runs
``CLIS_DEFAULT``. The SSH transport carries a login's CLIs on at most two
connections of ``CLI_SLOTS`` each, so the most is two connections' worth and
the default is one's.
"""

CLIS_DEFAULT = 4
CLIS_MAX = 8


def agents(n: int) -> str:
    """``n`` Claude Code agents, as a refusal names them."""
    return "1 agent" if n == 1 else f"{n} agents"
