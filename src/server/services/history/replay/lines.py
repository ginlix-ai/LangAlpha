"""Wire-ready replay lines: a turn's projection stored as the text it streams.

Each line is ``event \\t flags \\t data-json``, where the JSON is exactly what
the SSE frame carries. Most lines stream without being decoded; ``flags``
marks the few the endpoint must still stamp at read time. JSON escapes every
control character, so a line never contains a raw newline or tab.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from src.server.services.history.replay.widgets import _is_widget
from src.server.services.history.task_status import _artifact_task_id

# A task card: its status is stamped from liveness at read.
FLAG_TASK = "t"
# A widget whose data lives in object storage, inlined at read.
FLAG_WIDGET = "w"


@dataclass(slots=True)
class Line:
    event: str
    flags: str
    data_json: str

    def item(self) -> dict[str, Any]:
        return {"event": self.event, "data": json.loads(self.data_json)}


def _flags(event: str, data: dict[str, Any]) -> str:
    flags = ""
    if _artifact_task_id(data):
        flags += FLAG_TASK
    if _is_widget(event, data):
        payload = data.get("payload")
        if isinstance(payload, dict) and "data" not in payload and payload.get("data_ref"):
            flags += FLAG_WIDGET
    return flags


def line_of(item: dict[str, Any]) -> Line:
    data = item["data"]
    return Line(
        item["event"],
        _flags(item["event"], data),
        json.dumps(data, ensure_ascii=False, default=str),
    )


def encode(lines: list[Line]) -> str:
    return "\n".join(f"{ln.event}\t{ln.flags}\t{ln.data_json}" for ln in lines)


def decode(text: str) -> list[Line]:
    # split("\n"), never splitlines(): JSON leaves U+2028 and friends raw,
    # and splitlines() would break a line on them.
    out: list[Line] = []
    for raw in text.split("\n") if text else ():
        event, flags, data_json = raw.split("\t", 2)
        out.append(Line(event, flags, data_json))
    return out
