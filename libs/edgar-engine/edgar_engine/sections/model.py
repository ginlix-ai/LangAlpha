"""Result types of the sections engine. Offsets index FilingText.text, the engine's own rendering."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

from .vocab import is_amendment

Status = Literal["present", "stub", "xref"]
TargetKind = Literal["in_document", "exhibit", "proxy", "annual_report", "other_filing", "none_stated"]
FilingStatus = Literal["original", "amendment"]
View = Literal["as_filed", "as_amended"]
Scope = Literal["complete", "partial", "exhibits", "cover"]

__all__ = ["DocumentInfo", "FilingSections", "FilingStatus", "Scope", "Section", "Span", "Status", "Target", "TargetKind",
           "View"]


@dataclass(frozen=True)
class Span:
    start: int
    end: int


@dataclass(frozen=True)
class DocumentInfo:
    """A document the engine read: its sequence, type and file name, and the lines it rendered to. In an
    as-amended view, accession names the filing it comes from."""
    seq: int
    type: str
    filename: str
    line_start: int
    line_end: int
    accession: str | None = None


@dataclass
class Target:
    """Where a stub's content is. spans/text are filled only for in_document targets the engine resolved."""
    kind: TargetKind
    description: str = ""
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
    source: str | None = None


@dataclass
class FilingSections:
    """One filing's items, or with view "as_amended" an original with its amendments merged in.

    filing_status follows the form ("/A" is an amendment). amends is the accession of the original an
    amendment amends, None when it cannot be found. scope says what an amendment restates: "complete",
    "partial", "exhibits" (only its exhibit list) or "cover" (no item). In an as-amended view, amendments
    lists the accessions merged in filing order, each item's source names the filing its text comes from,
    and text is the original's text followed by each amendment's, so offsets still index it.
    """
    accession: str | None
    form: str
    filing_date: str | None
    items: dict[str, Section]
    text: str
    engine_version: str
    documents: list[DocumentInfo] = field(default_factory=list)
    filing_status: FilingStatus | None = None
    amends: str | None = None
    view: View = "as_filed"
    amendments: list[str] = field(default_factory=list)
    scope: Scope | None = None

    def __post_init__(self):
        if self.filing_status is None:
            self.filing_status = "amendment" if is_amendment(self.form) else "original"

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
        return cls(**{**d, "items": items, "documents": [DocumentInfo(**x) for x in d.get("documents") or []]})
