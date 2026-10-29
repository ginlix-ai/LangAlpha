"""How long something ran, as the agent reads it."""


def format_duration(seconds: float) -> str:
    """``45s``, ``23m``, ``1h52m``, ``3h``: what a command's notices, its
    hand-off result and ThreadStatus all say, so one wording serves each."""
    total = max(int(seconds), 0)
    if total < 60:
        return f"{total}s"
    hours, rest = divmod(total, 3600)
    minutes = rest // 60
    if not hours:
        return f"{minutes}m"
    return f"{hours}h{minutes}m" if minutes else f"{hours}h"
