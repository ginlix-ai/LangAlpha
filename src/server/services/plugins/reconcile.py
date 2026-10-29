"""The three-arm diff both component kinds reconcile through.

Servers and skills differ only in what their arms do; the shape is the same
and running it once is the point. The two kinds having their own copies is
how a post-edit policy step came to be applied to servers and not to skills.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

# (key, owned row ref) -> None
DeleteArm = Callable[[str, Any], Awaitable[None]]
# (key, owned row ref, incoming plan) -> None
UpdateArm = Callable[[str, Any, Any], Awaitable[None]]
# (plans that are new to this plugin) -> None
CreateArm = Callable[[list[Any]], Awaitable[None]]
# (key, incoming plan) -> True to report-and-skip instead of creating
DetachedArm = Callable[[str, Any], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class ReconcileArms:
    """What one component kind does with each side of the diff."""

    delete: DeleteArm
    update: UpdateArm
    create: CreateArm
    detached: DetachedArm


async def reconcile(
    owned: Mapping[str, Any],
    incoming: Mapping[str, Any],
    *,
    arms: ReconcileArms,
) -> None:
    """Diff by package key and run the matching arm over each side.

    Keyed by package identity (the mcp.json key / the skills directory name),
    never by row name, so a rename in the package is a delete plus a create.
    New keys are batched into one ``create`` call because both fan-outs cost a
    round of catalog reads per invocation.
    """
    for key in sorted(set(owned) - set(incoming)):
        await arms.delete(key, owned[key])

    for key in sorted(set(owned) & set(incoming)):
        await arms.update(key, owned[key], incoming[key])

    fresh = []
    for key in sorted(set(incoming) - set(owned)):
        plan = incoming[key]
        if not await arms.detached(key, plan):
            fresh.append(plan)
    if fresh:
        await arms.create(fresh)
