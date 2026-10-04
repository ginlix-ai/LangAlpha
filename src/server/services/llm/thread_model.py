"""A thread's own model, and the account default a thread without one follows.

Each thread runs the model a client last named for it
(``conversation_threads.llm_model``); NULL follows the account default for the
thread's mode. The send route reads it, never ``resolve_llm_config``:
automations call the run generators directly with their own model (null means
the account default), so keeping the read at the route keeps it off their turns
without a flag the resolver would have to trust.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from fastapi import HTTPException

from src.server.database.conversation import threads_read, threads_write

from . import logger, user_models
from .availability import model_resolves, raise_model_unavailable
from .config import _MODE_MODEL_MAP, saved_default_model, system_default_models

#: Values of ``model_preference.default_model_scope``: what a change of the
#: account default does to existing threads.
DEFAULT_MODEL_SCOPES = frozenset({"ask", "new_threads", "existing_threads"})

#: The preference keys an account default is read from.
DEFAULT_MODEL_KEYS = (
    *(pref_key for _field, pref_key in _MODE_MODEL_MAP.values()),
    "flash_follows",
)


@dataclass(frozen=True)
class NamedModel:
    """A model a send named for its thread, with the model the thread held
    when the send read it. The send keeps its model only while the thread
    still holds that one, so a pick saved while the turn was starting wins."""

    name: str
    seen: str | None


async def turn_model(
    user_id: str, thread_id: str, *, named: str | None, held: str | None
) -> str | None:
    """The model this turn runs: the one it names, else the one the thread
    holds; None runs the account default.

    A dead held model is cleared rather than refused, so the turn runs the
    default instead of every turn on the thread failing until someone picks
    another. A dead named model is returned for the resolver to refuse as
    ``model_removed`` (its test agrees with ``model_resolves``); when the thread
    holds it too, it is cleared with that refusal, so the client's refetch
    shows the default rather than resending it. A clear that loses to a pick
    saved since the send read the thread runs the pick, as the send's own
    write would yield to it.
    """
    if named and named != held:
        return named  # nothing of the thread's at stake; the resolver judges it
    model = named or held
    if not model:
        return None
    prefs = await user_models.get_model_preference(user_id)
    if model_resolves(prefs, model):
        return model
    if await threads_write.clear_thread_llm_model(thread_id, model):
        logger.info(
            f"[CHAT] Cleared thread {thread_id}'s model {model!r}: no longer available"
        )
    elif not named:
        meta = await threads_read.get_thread_auth_meta(thread_id)
        current = meta.get("llm_model") if meta else None
        if current and model_resolves(prefs, current):
            return current
    return named


#: Refusals of a model the catalog still lists but this user cannot run right
#: now: an account to reconnect, a key to add, a plan that does not cover it.
#: ``model_removed`` is here too, in case the availability check and the
#: resolver ever disagree about a model ``turn_model`` kept.
_UNSERVABLE_REFUSALS = frozenset({
    "oauth_required",
    "oauth_plan_unsupported",
    "byok_key_required",
    "model_unavailable",
    "model_removed",
})


def _runs_default_instead(exc: HTTPException, *, named: str | None, model: str | None) -> bool:
    """Whether a turn refused on the thread's own model may run the account default.

    Only a turn that named no model, which ran the thread's model because the
    thread holds it, not because anyone asked. The thread keeps its model, so
    turns return to it once the account does.
    """
    if named or not model or exc.status_code != 400:
        return False
    return isinstance(exc.detail, dict) and exc.detail.get("type") in _UNSERVABLE_REFUSALS


_Config = TypeVar("_Config")


async def resolve_turn_config(
    user_id: str,
    thread_id: str,
    *,
    named: str | None,
    held: str | None,
    fall_back: bool,
    resolve: Callable[[str | None], Awaitable[_Config]],
) -> _Config:
    """Resolve what a turn on this thread runs: ``turn_model``'s choice, or,
    with ``fall_back``, the account default when the thread's own model cannot
    run right now and the turn named none.

    ``fall_back`` is for turns a refusal would drop, with nobody at a client to
    reconnect the account or add the key; a person gets the refusal, which says
    what to fix. A send and a manual compaction both come through here, so a
    summary runs on the model and credential the thread's turns do. ``resolve``
    maps a model name (None for the account default) to the caller's config.
    """
    model = await turn_model(user_id, thread_id, named=named, held=held)
    try:
        return await resolve(model)
    except HTTPException as exc:
        if not (fall_back and _runs_default_instead(exc, named=named, model=model)):
            raise
        logger.info(
            f"[CHAT] Thread {thread_id}'s model {model!r} cannot run "
            f"({exc.detail.get('type')}); this turn runs the account default"
        )
        return await resolve(None)


async def require_selectable_model(user_id: str, name: str) -> None:
    """Refuse a name that resolves to no model before a thread keeps it, so
    the failure lands on the pick rather than on every later turn.

    Credentials are left to the turn: a lapsed connection comes back, a model
    missing from the catalog does not.
    """
    pref = await user_models.get_model_preference(user_id)
    if not model_resolves(pref, name):
        raise_model_unavailable(name)


def effective_default_models(model_pref: dict) -> dict[str, str | None]:
    """The model a thread following the account default runs, per mode.

    The composer's rule and the one a turn runs on (``select_model``), so
    "threads still on the old default" means what the user saw on them.
    """
    system = system_default_models()
    return {
        "ptc": saved_default_model(model_pref, "ptc") or system.get("default_model") or None,
        "flash": (
            saved_default_model(model_pref, "flash")
            or system.get("flash_model")
            or system.get("default_model")
            or None
        ),
    }


def default_moves(
    before: dict[str, str | None], after: dict[str, str | None]
) -> dict[str, tuple[str, str | None]]:
    """``{mode: (old, new)}`` for each mode whose default changed. A mode with
    no old default has no threads on it to move."""
    return {
        mode: (old, after.get(mode))
        for mode, old in before.items()
        if old and old != after.get(mode)
    }


async def carry_default_change(
    user_id: str, *, before: dict, after: dict, apply_to: str | None, conn=None
) -> int:
    """Carry a changed account default onto existing threads when asked to;
    returns how many threads moved.

    ``before`` and ``after`` are the model preferences either side of the
    write, and ``conn`` its transaction, so the threads move with it or not at
    all. ``apply_to`` is the write's one-shot answer; without one the saved
    ``default_model_scope`` decides, which is how a writer that cannot ask (a
    channel command, the setup wizard) honours the user's setting. Only
    ``existing_threads`` moves anything: ``ask`` with no one to ask means new
    threads only.
    """
    scope = apply_to or after.get("default_model_scope")
    if scope != "existing_threads":
        return 0
    moves = default_moves(effective_default_models(before), effective_default_models(after))
    if not moves:
        return 0
    moved = await threads_write.reassign_thread_llm_models(user_id, moves, conn=conn)
    logger.info(
        f"[PREFS] Moved {moved} thread(s) to the new default for user={user_id}: {moves}"
    )
    return moved
