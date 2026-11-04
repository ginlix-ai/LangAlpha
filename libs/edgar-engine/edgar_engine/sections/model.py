"""Result types of the sections engine. Offsets index FilingText.text, the engine's own rendering."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

Status = Literal["present", "stub", "xref"]
TargetKind = Literal["in_document", "exhibit", "proxy", "annual_report", "other_filing", "none_stated"]


@dataclass(frozen=True)
class Span:
    start: int
    end: int


@dataclass
class Target:
    """Where a stub's content is. spans/text are filled only for in_document targets the engine resolved."""
    kind: TargetKind
    description: str = ""
    doc: str | None = None
    spans: list[Span] = field(default_factory=list)
    text: str | None = None


@dataclass
class Section:
    item: str
    status: Status
    text: str
    start: int
    end: int
    heading: str = ""
    combined_with: list[str] = field(default_factory=list)
    target: Target | None = None
    override: str | None = None


@dataclass
class FilingSections:
    accession: str | None
    form: str
    filing_date: str | None
    items: dict[str, Section]
    text: str
    engine_version: str
    documents: list[dict] = field(default_factory=list)

    def __getitem__(self, item: str) -> Section:
        return self.items[item]

    def get(self, item: str) -> Section | None:
        return self.items.get(item)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> FilingSections:
        def target(t):
            if t is None:
                return None
            return Target(**{**t, "spans": [Span(**s) for s in t.get("spans") or []]})
        items = {k: Section(**{**v, "target": target(v.get("target"))}) for k, v in d["items"].items()}
        return cls(**{**d, "items": items})
