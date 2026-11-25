"""File and chart comments reaching the agent: one chat send carries any
number of them.

The owner's typed note comes first, then a block per comment, as one message
the owner sends from the chat. The send takes the comments' unsent words under
its ``request_key`` before admission, because admission needs the message;
a send refused before any run took it gives them back, and one that was
admitted marks them again, in case a refused copy of it gave them back first.
``CommentSendScope`` holds that across the send's admission.

The query row (and a steer's payload) keeps ``artifact_comments`` beside the
rendered message, which is what a reader is shown in its place: the note and
each comment's pin and words, and what the agent's guidance for them says
(``guidance``). The turn's input message keeps it too (``stamp_send``), since
the agent is told that guidance once while it is in view rather than in every
message (``ptc_agent/agent/middleware/comment_guidance.py``).
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple, get_args

from fastapi import HTTPException

from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import ClaimedComment
from src.server.database.artifacts.records import RecordKind
from src.server.models.chart_layer import element_label, layer_number, point_text
from src.server.models.chat import (
    ChatMessage,
    ChatRequest,
    ContentItem,
    GuidancePart,
    ShownComment,
    ShownSend,
)
from src.server.services.artifacts.events import publish_comment_change

logger = logging.getLogger(__name__)

Reading = Literal["chat", "file"]

# A follow-up recalls the exchange it continues, briefly: the thread holds it
# too, unless it was compacted away.
_EARLIER_MESSAGES = 6
_EARLIER_CHARS = 1000
# What a reader's chip shows of each comment; the full text is on the file.
_SHOWN_CHARS = 500
_SHOWN_ANCHOR_CHARS = 200

_ANCHOR_KINDS = frozenset({"element", "text", "named", "block"})
_ANCHOR_FIELDS = (
    ("lines", "lines"),
    ("page", "page"),
    ("path", "path"),
    ("label", "label"),
    ("quote", "quote"),
    ("prefix", "before"),
    ("suffix", "after"),
    ("detail", "detail"),
)


@dataclass(frozen=True)
class FilePlace:
    """Where the turn's file tools reach a workspace's files: the path before
    a comment's own, and the computer to name when it is not Home's."""

    prefix: str
    computer: str | None = None


async def chief_of_staff_places(
    user_id: str, claimed: Iterable[ClaimedComment]
) -> dict[str, FilePlace]:
    """How a Chief of Staff turn, working in Home, reaches each commented
    workspace's folder; Home's own files need nothing before their path.

    A workspace on Home's computer is a sibling folder, one on another
    computer is its folder's whole path there, as ``<activity>`` lists it.
    """
    from src.server.database.home_workspace import get_home_workspace_id
    from src.server.database.workspace import get_workspace
    from src.tools.secretary.utils import read_computer_placement

    home_id = get_home_workspace_id(user_id)
    workspace_ids = sorted({c.workspace_id for c in claimed} - {home_id})
    if not workspace_ids:
        return {}
    placement = await read_computer_placement(user_id)
    places: dict[str, FilePlace] = {}
    for workspace_id in workspace_ids:
        workspace = await get_workspace(workspace_id)
        if workspace is None or not workspace.get("dir_name"):
            continue
        if path := placement.path(workspace):
            places[workspace_id] = FilePlace(f"{path}/", placement.computer(workspace))
        else:
            places[workspace_id] = FilePlace(f"../{workspace['dir_name']}/")
    return places


def _data(text: Any) -> str:
    """Text placed inside the message's blocks, unable to close one."""
    return str(text).replace("<", "&lt;")


def _attr(text: str) -> str:
    return _data(text).replace('"', "&quot;")


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _text(value: Any) -> str | None:
    return _data(value.strip()) if isinstance(value, str) and value.strip() else None


#: The guidance parts the tags written so far call for.
_Parts = set[GuidancePart]


def _render_area(
    anchor: dict[str, Any], _place: FilePlace | None, _parts: _Parts
) -> list[str]:
    """An area: its label, then each element listed, as an element anchor
    names one, and how many more it held."""
    lines = ['<anchor kind="area">']
    if label := _text(anchor.get("label")):
        lines.append(f"<label>{label}</label>")
    elements = [e for e in anchor.get("elements") or [] if isinstance(e, dict)]
    for element in elements:
        parts = [
            f"<{key}>{text}</{key}>"
            for key in ("lines", "path", "label", "quote")
            if (text := _text(element.get(key)))
        ]
        if parts:
            lines.append(f"<element>{''.join(parts)}</element>")
    count = anchor.get("count")
    if isinstance(count, int) and count > len(elements):
        lines.append(f'<more count="{count - len(elements)}"/>')
    lines.append("</anchor>")
    return lines


def _sheet(anchor: dict[str, Any]) -> str | None:
    """The sheet's name as the workbook spells it: spaces at its ends are part
    of it, and the agent opens the sheet by it."""
    sheet = anchor.get("sheet")
    return sheet if isinstance(sheet, str) and sheet.strip() else None


def _render_cells(
    anchor: dict[str, Any], _place: FilePlace | None, _parts: _Parts
) -> list[str]:
    """Cells: the sheet, the ranges together, and their values in that order,
    one per range, so a range that showed nothing still holds its place."""
    lines = ['<anchor kind="cells">']
    if sheet := _sheet(anchor):
        lines.append(f"<sheet>{_data(sheet)}</sheet>")
    ranges = [
        (name, _text(r.get("values")))
        for r in anchor.get("ranges") or []
        if isinstance(r, dict) and (name := _text(r.get("range")))
    ]
    if ranges:
        lines.append(f"<range>{', '.join(name for name, _ in ranges)}</range>")
    if any(values for _, values in ranges):
        shown = " · ".join(values or "(blank)" for _, values in ranges)
        lines.append(f"<values>{shown}</values>")
    lines.append("</anchor>")
    return lines if len(lines) > 2 else []


def _render_generic(
    anchor: dict[str, Any], _place: FilePlace | None, _parts: _Parts
) -> list[str]:
    kind = anchor.get("kind")
    lines = [f'<anchor kind="{kind}">' if kind in _ANCHOR_KINDS else "<anchor>"]
    for key, tag in _ANCHOR_FIELDS:
        value = anchor.get(key)
        # A PDF page is a number; every other field is text.
        if key == "page" and isinstance(value, int) and not isinstance(value, bool):
            value = str(value)
        if text := _text(value):
            lines.append(f"<{tag}>{text}</{tag}>")
    lines.append("</anchor>")
    return lines if len(lines) > 2 else []


def _fraction(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _picture_line(picture: str, place: FilePlace | None, parts: _Parts) -> str:
    """A picture written for the agent, named as a ``<file>`` in its
    workspace is: it sits in that workspace's folder."""
    prefix = place.prefix if place else ""
    parts.add("pictures")
    if place and place.computer:
        parts.add("computer")
        opening = f'<picture computer="{_attr(place.computer)}">'
    else:
        opening = "<picture>"
    return f"{opening}{_data(prefix + picture)}</picture>"


def _render_region(
    anchor: dict[str, Any], place: FilePlace | None, parts: _Parts
) -> list[str]:
    """Boxes on an image or a PDF's pages, each with the picture written for
    it, named as the comment's ``<file>`` is; a page is named where the boxes
    move to it, so one page reads once."""
    lines = ['<anchor kind="region">']
    page = None
    for box in anchor.get("boxes") or []:
        on = "of the image"
        if box.get("page") is not None:
            on = "of the page"
            if box["page"] != page:
                page = box["page"]
                lines.append(f"<page>{page}</page>")
        x, y, w, h = (box[k] for k in ("x", "y", "w", "h"))
        if w == 0:
            lines.append(
                f"<point>x {_fraction(x)}, y {_fraction(y)} {on}, from its top-left</point>"
            )
            continue
        lines.append(
            f"<box>x {_fraction(x)} to {_fraction(min(x + w, 1))}, "
            f"y {_fraction(y)} to {_fraction(min(y + h, 1))} {on}, from its top-left</box>"
        )
        if picture := box.get("picture"):
            lines.append(_picture_line(picture, place, parts))
    lines.append("</anchor>")
    return lines if len(lines) > 2 else []


def _chart_point(mark: dict[str, Any]) -> str:
    return _data(point_text(mark["price"], mark["time"]))


def _chart_element(mark: dict[str, Any]) -> str:
    return _data(
        element_label(
            mark["element_kind"],
            mark.get("element_type"),
            mark.get("element_id"),
            mark.get("element_name"),
        )
    )


def _render_chart(
    anchor: dict[str, Any], place: FilePlace | None, parts: _Parts
) -> list[str]:
    """A chart's marks in the order picked, in the layers' own terms: times
    as the chart's exchange writes them, points as ``price @time``, elements
    named as the layer read-back names them."""
    lines = ['<anchor kind="chart">']
    for mark in anchor.get("marks") or []:
        if not isinstance(mark, dict):
            continue
        kind = mark.get("type")
        if kind == "region":
            low, high = layer_number(mark["low"]), layer_number(mark["high"])
            span = f"{mark['start']} to {mark['end']}, price {low} to {high}"
            lines.append(f"<region>{_data(span)}</region>")
            if picture := mark.get("picture"):
                lines.append(_picture_line(picture, place, parts))
        elif kind == "point":
            lines.append(f"<point>{_chart_point(mark)}</point>")
        elif kind == "element":
            lines.append(
                f'<layer id="{_attr(mark["layer"])}">'
                f"{_data(mark.get('layer_title') or '')}</layer>"
            )
            lines.append(f"<element>{_chart_element(mark)}</element>")
            lines.append(f"<at>{_chart_point(mark)}</at>")
    lines.append("</anchor>")
    return lines if len(lines) > 2 else []


#: Each takes the file's place too, since a box's picture is named as its
#: <file> is, and the parts its tags call for.
_RENDERERS: dict[
    str, Callable[[dict[str, Any], FilePlace | None, _Parts], list[str]]
] = {
    "area": _render_area,
    "cells": _render_cells,
    "region": _render_region,
    "chart": _render_chart,
}


def _render_anchor(
    anchor: dict[str, Any], place: FilePlace | None, parts: _Parts
) -> list[str]:
    return _RENDERERS.get(anchor.get("kind"), _render_generic)(anchor, place, parts)


def _render_file(
    comment: ClaimedComment, place: FilePlace | None, parts: _Parts
) -> str:
    if comment.kind == "chart":
        # A chart is named by its id wherever the turn runs; only its
        # pictures are files.
        parts.add("chart")
        return f"<chart>{_data(comment.path)}</chart>"
    if place is None:
        return f"<file>{_data(comment.path)}</file>"
    if place.computer:
        parts.add("computer")
        opening = f'<file computer="{_attr(place.computer)}">'
    else:
        opening = "<file>"
    return f"{opening}{_data(place.prefix + comment.path)}</file>"


def _render_comment(
    comment: ClaimedComment, place: FilePlace | None, parts: _Parts
) -> str:
    lines = [
        f'<comment id="{comment.comment_id}" number="{comment.number}">',
        _render_file(comment, place, parts),
    ]
    if title := _text(comment.title):
        lines.append(f"<title>{title}</title>")
    lines.extend(_render_anchor(comment.anchor, place, parts))
    if comment.earlier:
        lines.append("<earlier>")
        for author, body in comment.earlier[-_EARLIER_MESSAGES:]:
            tag = "you" if author == "agent" else "user"
            lines.append(f"<{tag}>{_data(_clip(body, _EARLIER_CHARS))}</{tag}>")
        lines.append("</earlier>")
    lines.append(f"<message>{_data(chr(10).join(comment.delivered))}</message>")
    lines.append("</comment>")
    return "\n".join(lines)


def _place(
    comment: ClaimedComment, places: dict[str, FilePlace] | None
) -> FilePlace | None:
    return places.get(comment.workspace_id, FilePlace("")) if places is not None else None


class RenderedSend(NamedTuple):
    """The message the agent reads, and the parts of its guidance the tags
    written in it call for (``ShownSend.guidance``)."""

    text: str
    parts: list[GuidancePart]


def render_message(
    note: str,
    claimed: list[ClaimedComment],
    *,
    places: dict[str, FilePlace] | None = None,
) -> RenderedSend:
    """The message the agent reads: the owner's note, then the comments. How
    to answer them is the agent's runtime context, which states a part only
    where a tag here calls for it: a computer where a ``<file>`` or a
    ``<picture>`` names one, pictures, and charts.

    ``places`` is given on a Chief of Staff turn, which works from Home: each
    file is then named by the path its file tools take from there.
    """
    found: _Parts = set()
    blocks = "\n\n".join(
        _render_comment(c, _place(c, places), found) for c in claimed
    )
    body = f"<file-comments>\n{blocks}\n</file-comments>"
    text = f"{note.rstrip()}\n\n{body}" if note.strip() else body
    return RenderedSend(text, [p for p in get_args(GuidancePart) if p in found])


def _cells_place(anchor: dict[str, Any]) -> str | None:
    """Cells by where they are, as the sheet names them: "Model!B2:C2, D3:D6",
    or the ranges alone in a CSV, as the file's comment list says it."""
    names = ", ".join(
        r["range"]
        for r in anchor.get("ranges") or []
        if isinstance(r, dict) and isinstance(r.get("range"), str)
    )
    if not names:
        return None
    sheet = _sheet(anchor)
    if sheet is None:
        return names
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", sheet):
        sheet = "'" + sheet.replace("'", "''") + "'"
    return f"{sheet}!{names}"


def _shown_anchor(anchor: dict[str, Any], key: str) -> str | None:
    # Cells have no label of their own: their place is what names them.
    if key == "label" and anchor.get("kind") == "cells":
        value = _cells_place(anchor)
    else:
        value = anchor.get(key)
    if not isinstance(value, str) or not value.strip():
        return None
    return _clip(" ".join(value.split()), _SHOWN_ANCHOR_CHARS)


def shown_send(
    note: str,
    claimed: list[ClaimedComment],
    *,
    reading: Reading,
    parts: list[GuidancePart],
) -> ShownSend:
    """What a reader is shown for the message: the note and a chip per
    comment, with the parts of the agent's guidance it calls for
    (``RenderedSend.parts``) beside them."""
    return ShownSend(
        note=note,
        reading=reading,
        guidance=parts,
        comments=[
            ShownComment(
                comment_id=c.comment_id,
                artifact_id=c.artifact_id,
                workspace_id=c.workspace_id,
                path=c.path,
                kind=c.kind,
                title=c.title,
                number=c.number,
                text=_clip("\n".join(c.delivered), _SHOWN_CHARS),
                follow_up=bool(c.earlier),
                label=_shown_anchor(c.anchor, "label"),
                quote=_shown_anchor(c.anchor, "quote"),
            )
            for c in claimed
        ],
    )


def stamp_send(messages: list[dict[str, Any]], shown: ShownSend | None) -> None:
    """Record the send on a turn's input, in place, as a steer's delivery
    records it (``artifact_comments``), so the agent's guidance for the
    comments is decided from the thread's history alone. The last user
    message is the one carrying them (``_carrying``)."""
    if shown is None:
        return
    last = next(
        (m for m in reversed(messages) if m.get("role") in ("user", "human")), None
    )
    if last is not None:
        last["artifact_comments"] = shown.model_dump(mode="json")


def _carrying(request: ChatRequest, text: str, shown: ShownSend) -> ChatRequest:
    """``request`` with ``text`` as its message, where the note was."""
    messages = list(request.messages)
    last = messages[-1] if messages else None
    if last is not None and last.role in ("user", "human"):
        if isinstance(last.content, str):
            content: Any = text
        else:
            content = list(last.content)
            at = next(
                (
                    i
                    for i, item in enumerate(content)
                    if item.type == "text" and item.text
                ),
                None,
            )
            if at is None:
                content.insert(0, ContentItem(type="text", text=text))
            else:
                content[at] = ContentItem(type="text", text=text)
        messages[-1] = last.model_copy(update={"content": content})
    else:
        messages.append(ChatMessage(role="user", content=text))
    return request.model_copy(update={"messages": messages, "artifact_comments": shown})


#: Makes the thread a send needs before its claim, when missing, and says
#: whether this call made it.
MakeThread = Callable[[], Awaitable[bool]]


class CommentSendScope:
    """One chat send's file comments, from the claim until a run takes them.

    Entered around a send's admission. ``take`` claims the comments the
    request names and renders them into it; ``admitted`` holds the claim once
    a run or a steer took the send. Leaving on an exception before that
    refuses the send: its comments wait again, and a thread it made for them
    goes, since no turn ever ran there. A cancel does that on a task of its
    own, as every await in a cancelled task is cancelled again.
    """

    def __init__(
        self,
        *,
        thread_id: str,
        user_id: str,
        owner_id: str,
        chief_of_staff: bool,
        timezone: str | None,
    ) -> None:
        self._thread_id = thread_id
        # The comments are the workspace's owner's rows (a shared workspace's
        # holder's), while the sender is who the send's title is for.
        self._owner_id = owner_id
        self._user_id = user_id
        self._chief_of_staff = chief_of_staff
        self._timezone = timezone
        self._request_key: str | None = None
        #: None until a claim ran.
        self._claimed: list[ClaimedComment] | None = None
        self._made_thread = False
        self._row_ensured = False
        self._title_query: str | None = None
        self._taken = False

    @property
    def row_ensured(self) -> bool:
        """Whether ``take`` already ensured the thread's row, so the send's own
        ensure only has the status to set."""
        return self._row_ensured

    @property
    def made_thread(self) -> bool:
        """Whether this send made its thread, which then has no prior turn."""
        return self._made_thread

    async def __aenter__(self) -> CommentSendScope:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is None or self._taken:
            return
        if self._claimed is None and not self._made_thread:
            return
        if issubclass(exc_type, (asyncio.CancelledError, GeneratorExit)):
            from src.server.services.runs.coordinator import spawn_protected

            spawn_protected(
                self._give_back(), f"artifact-comments-release-{self._thread_id}"
            )
        else:
            await self._give_back()

    async def take(
        self, request: ChatRequest, note: str, *, make_thread: MakeThread
    ) -> tuple[ChatRequest, str]:
        """Claim the comments ``request`` names and render them into it.

        Returns the request carrying the rendered message, and that message
        as the turn's input text; a request naming none passes through.
        Comments already sent by another send, deleted, or on files this
        thread may not answer for are left out; with none left and no note
        there is nothing to send, which is refused before admission.
        """
        if not request.artifact_comment_ids:
            return request, note
        assert request.request_key and request.reading
        self._request_key = request.request_key
        # The claim checks the thread they go to, so a send that starts one
        # makes it first. Untitled: the rendered block is no title, and a
        # refused send should not cost a title call.
        self._made_thread = await make_thread()
        self._row_ensured = True
        self._claimed = await artifacts_db.claim_comments(
            user_id=self._owner_id,
            thread_id=self._thread_id,
            comment_ids=list(request.artifact_comment_ids),
            request_key=request.request_key,
            caller=self._user_id,
        )
        if self._made_thread:
            self._title_query = note.strip() or "\n\n".join(
                "\n".join(c.delivered) for c in self._claimed
            )
        if not self._claimed:
            if not note.strip():
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "comments_unavailable",
                        "message": (
                            "These comments were already sent or deleted. "
                            "Refresh the file's comments."
                        ),
                    },
                )
            cleared = {
                "artifact_comment_ids": None,
                "reading": None,
                "artifact_comments": None,
            }
            return request.model_copy(update=cleared), note
        places = (
            await chief_of_staff_places(self._user_id, self._claimed)
            if self._chief_of_staff
            else None
        )
        text, parts = render_message(note, self._claimed, places=places)
        await self._publish(_by_artifact(self._claimed))
        shown = shown_send(note, self._claimed, reading=request.reading, parts=parts)
        return _carrying(request, text, shown), text

    async def admitted(self) -> None:
        """A run or a steer took the send: hold the claim, and title a thread
        the send made. Never raises: the send went through, and only a refused
        copy's release could have undone the claim."""
        if self._taken:
            return
        self._taken = True
        if self._claimed:
            try:
                await artifacts_db.confirm_claim(
                    user_id=self._owner_id,
                    request_key=self._request_key,
                    thread_id=self._thread_id,
                    message_ids=[i for c in self._claimed for i in c.message_ids],
                )
            except Exception:
                logger.warning(
                    "Holding the comments of send %s failed",
                    self._request_key,
                    exc_info=True,
                )
        if self._made_thread and self._title_query:
            from src.server.services.thread_title import schedule_title_generation

            schedule_title_generation(
                thread_id=self._thread_id,
                user_id=self._user_id,
                first_query=self._title_query,
                expected_title=None,
                timezone=self._timezone,
            )

    async def run_never_began(self) -> None:
        """The run this send started ended before its workflow began, so no
        agent read what it carries, and the comments wait again. The turn stays
        on the thread with the note. Never raises."""
        if self._claimed:
            await give_back_unread(
                user_id=self._owner_id,
                thread_id=self._thread_id,
                request_key=self._request_key,
            )

    async def _give_back(self) -> None:
        """Undo a refused send. Never raises."""
        if self._claimed:
            await self._release()
        if not self._made_thread:
            return
        from src.server.database.conversation.threads_write import (
            delete_unused_thread,
        )

        try:
            await delete_unused_thread(self._thread_id, request_key=self._request_key)
        except Exception:
            logger.warning(
                "Removing thread %s after a refused comment send failed",
                self._thread_id,
                exc_info=True,
            )

    async def _release(self) -> None:
        """Give the comments back, unless a copy of the send with the same key
        reached a run, which carried them."""
        try:
            # The check for a copy that reached a run is the release's own,
            # under the comments' locks, so no copy confirms in between.
            released = await artifacts_db.release_claim(
                user_id=self._owner_id, request_key=self._request_key
            )
        except Exception:
            logger.warning(
                "Giving back the comments of send %s failed",
                self._request_key,
                exc_info=True,
            )
            return
        await self._publish(_grouped(released))

    async def _publish(self, changed: dict[str, list[str]]) -> None:
        await _publish_changes(
            self._owner_id,
            self._thread_id,
            changed,
            {
                c.artifact_id: (c.workspace_id, c.path, c.kind)
                for c in self._claimed or ()
            },
        )


async def _publish_changes(
    user_id: str,
    thread_id: str | None,
    changed: dict[str, list[str]],
    files: dict[str, tuple[str, str, RecordKind]] | None = None,
) -> None:
    """Tell the owner's tabs. Best effort: they read again at turn end.
    ``files`` is each record's workspace, path and kind where the caller
    holds them."""
    files = dict(files or {})
    for artifact_id, comment_ids in changed.items():
        try:
            if artifact_id not in files:
                artifact = await artifacts_db.get_artifact(
                    artifact_id, user_id=user_id
                )
                if artifact is None:
                    continue
                files[artifact_id] = (
                    artifact.workspace_id,
                    artifact.path,
                    artifact.kind,
                )
            workspace_id, path, kind = files[artifact_id]
            await publish_comment_change(
                user_id,
                artifact_id=artifact_id,
                workspace_id=workspace_id,
                path=path,
                kind=kind,
                thread_id=thread_id,
                comment_ids=comment_ids,
            )
        except Exception:
            logger.warning(
                "Publishing the comment change on artifact %s failed",
                artifact_id,
                exc_info=True,
            )


async def give_back_unread(
    *, user_id: str, thread_id: str | None, request_key: str
) -> None:
    """What a send carried, which no agent read, waits again. Never raises."""
    try:
        released = await artifacts_db.release_unread_send(
            user_id=user_id, request_key=request_key
        )
    except Exception:
        logger.warning(
            "Giving back the comments of send %s failed", request_key, exc_info=True
        )
        return
    await _publish_changes(user_id, thread_id, _grouped(released))


async def publish_returned_comments(thread_id: str, returned: list[dict]) -> None:
    """A stop gave these steers back, and the comments they carried with them,
    in the stamp's transaction. The stopping tab reconciles from its answer;
    every other tab hears here. Never raises."""
    changed: dict[str, list[str]] = {}
    files: dict[str, tuple[str, str, RecordKind]] = {}
    for entry in returned:
        for c in (entry.get("artifact_comments") or {}).get("comments") or ():
            changed.setdefault(c["artifact_id"], []).append(c["comment_id"])
            files[c["artifact_id"]] = (
                c["workspace_id"],
                c["path"],
                c.get("kind", "file"),
            )
    if not changed:
        return
    from src.server.database.conversation import get_thread_auth_meta

    try:
        meta = await get_thread_auth_meta(thread_id)
    except Exception:
        logger.warning(
            "Reading the owner of thread %s failed", thread_id, exc_info=True
        )
        return
    if meta:
        await _publish_changes(str(meta["user_id"]), thread_id, changed, files)


#: A send may wait behind a /compact or a cold computer before it is admitted,
#: and marking it again then undoes a release, so this errs long.
_ABANDONED_AFTER_SECONDS = 15 * 60
_ABANDONED_WITHIN_SECONDS = 2 * 3600


async def release_abandoned_claims(*, assume_dead: bool = False) -> int:
    """Give back the comments of sends whose worker died before a run or a
    steer took them, which nothing else would ever do. ``assume_dead`` (a lone
    worker's startup) takes them at once. Returns how many sends. Never raises."""
    try:
        claims = await artifacts_db.list_abandoned_claims(
            older_than_seconds=0 if assume_dead else _ABANDONED_AFTER_SECONDS,
            within_seconds=_ABANDONED_WITHIN_SECONDS,
        )
        for claim in claims:
            released = await artifacts_db.release_claim(
                user_id=claim.user_id, request_key=claim.request_key
            )
            await _publish_changes(claim.user_id, claim.thread_id, _grouped(released))
        return len(claims)
    except Exception:
        logger.warning("Giving back abandoned comment sends failed", exc_info=True)
        return 0


def _grouped(pairs: Iterable[tuple[str, str]]) -> dict[str, list[str]]:
    changed: dict[str, list[str]] = {}
    for artifact_id, comment_id in pairs:
        changed.setdefault(artifact_id, []).append(comment_id)
    return changed


def _by_artifact(claimed: Iterable[ClaimedComment]) -> dict[str, list[str]]:
    changed: dict[str, list[str]] = {}
    for c in claimed:
        changed.setdefault(c.artifact_id, []).append(c.comment_id)
    return changed
