"""Which computers the idle sweep never stops, for every reader that follows
it: the sweep's own window, the command watcher's after-turn hour, a handed-off
command's result, and the data-token sweep's activity writes."""

from typing import Any, Mapping

from ptc_agent.core.sandbox.providers import is_machine_kind


def spared_by_idle_sweep(computer: Mapping[str, Any]) -> bool:
    """Whether the idle sweep never stops ``computer``: an always-on one, or a
    whole machine (``MACHINE_KINDS``). Nothing there stops a command an hour after
    its turn either.

    Raises:
        ValueError: It is not always-on and no provider knows its kind; each
            caller decides what such a computer gets.
    """
    return bool(computer.get("is_always_on")) or is_machine_kind(computer["kind"])
