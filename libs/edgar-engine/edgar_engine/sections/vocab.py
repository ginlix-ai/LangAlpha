"""What each form is: its items in order, the part each item belongs to, and the structural features the
engine reads it by. Every module asks a FormSpec instead of testing the form's name, so a new form or era is
one entry here."""
from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Mapping

TENK = ["1", "1A", "1B", "1C", "2", "3", "4", "5", "6", "7", "7A", "8", "9", "9A", "9B", "9C",
        "10", "11", "12", "13", "14", "15", "16"]
PART_OF_10K = {**{i: "I" for i in ["1", "1A", "1B", "1C", "2", "3", "4"]},
               **{i: "II" for i in ["5", "6", "7", "7A", "8", "9", "9A", "9B", "9C"]},
               **{i: "III" for i in ["10", "11", "12", "13", "14"]}, "15": "IV", "16": "IV"}

TENQ_PARTS = {"I": ["1", "2", "3", "4"], "II": ["1", "1A", "2", "3", "4", "5", "6"]}
TENQ = [f"{p}-{i}" for p, items in TENQ_PARTS.items() for i in items]

TWENTYF = ["1", "2", "3", "4", "4A", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
           "16A", "16B", "16C", "16D", "16E", "16F", "16G", "16H", "16I", "16J", "16K", "17", "18", "19"]
PART_OF_20F = {i: ("I" if int("".join(ch for ch in i if ch.isdigit())) <= 12 else
                   "II" if int("".join(ch for ch in i if ch.isdigit())) <= 16 else "III") for i in TWENTYF}

EIGHTK = ["1.01", "1.02", "1.03", "1.04", "1.05", "2.01", "2.02", "2.03", "2.04", "2.05", "2.06", "3.01",
          "3.02", "3.03", "4.01", "4.02", "5.01", "5.02", "5.03", "5.04", "5.05", "5.06", "5.07", "5.08",
          "6.01", "6.02", "6.03", "6.04", "6.05", "7.01", "8.01", "9.01"]
EIGHTK_OLD = [str(i) for i in range(1, 13)]
EIGHTK_NEW_FROM = "2004-08-23"

ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7, "VIII": 8, "IX": 9, "X": 10, "XI": 11,
         "XII": 12, "XIII": 13, "XIV": 14, "XV": 15, "XVI": 16, "XVII": 17, "XVIII": 18, "XIX": 19, "XX": 20}
PART_NUMERAL = {"I": "I", "II": "II", "III": "III", "IV": "IV", "1": "I", "2": "II", "3": "III", "4": "IV",
                "ONE": "I", "TWO": "II", "THREE": "III", "FOUR": "IV"}

_EXHIBIT_HEADING = re.compile(r"\bexhibits?\b", re.I)


@dataclass(frozen=True)
class FormSpec:
    """One form in one era. order is empty for a form found by titles (40-F), whose sections come in no fixed order."""
    family: str
    order: tuple[str, ...] = ()
    part_of: Mapping[str, str] = field(default_factory=dict)
    default_part: str | None = None
    required_parts: frozenset[str] = frozenset()
    parts_split: bool = False
    heading_order: tuple[str, ...] | None = None
    dotted_items: bool = False
    undotted_items: bool = False
    roman_items: bool = False
    lettered_16: bool = False
    section_dividers: bool = False
    statement_pages: bool = False
    included_statements: bool = False
    stacked_headings: bool = False
    stubs: bool = True
    xref_index: bool = False
    addressed_sentences: bool = False
    statement_items: frozenset[str] = frozenset()
    exhibit_items: frozenset[str] = frozenset()
    principal_documents: frozenset[str] = frozenset()
    exhibit_documents: tuple[str, ...] = ()
    amendment_documents: tuple[str, ...] = ()
    titled: bool = False

    @property
    def headings(self) -> list[str]:
        """The items a heading can name: a 10-Q heading names an item of its part ("Item 2"), not "II-2"."""
        return list(self.heading_order if self.heading_order is not None else self.order)

    def part_items(self, part: str) -> list[str]:
        """The items of one part as its headings number them ("2" for a 10-Q's "II-2")."""
        return [i.split("-", 1)[1] for i in self.order if i.split("-")[0] == part]

    def part(self, item: str) -> str | None:
        if self.parts_split:
            return item.split("-")[0]
        return self.part_of.get(item, self.default_part)

    def lists_exhibits(self, item: str, heading: str) -> bool:
        """The item listing the report's exhibits: its heading names exhibits (a 10-K's Item 14 in the numbering
        before 2003, Item 15 since), or with no such heading its number (10-K Item 15, 10-Q Part II Item 6,
        20-F Item 19, 8-K Item 9.01, Item 7 before August 2004)."""
        return item in self.exhibit_items or bool(_EXHIBIT_HEADING.search(heading))


_TENK = FormSpec("10-K", tuple(TENK), PART_OF_10K, required_parts=frozenset({"I", "II", "III"}), roman_items=True,
                 statement_pages=True, included_statements=True, xref_index=True, addressed_sentences=True,
                 statement_items=frozenset({"8"}), exhibit_items=frozenset({"15"}), exhibit_documents=("EX-13",))
_TENQ = FormSpec("10-Q", tuple(TENQ), required_parts=frozenset({"I", "II"}), parts_split=True,
                 heading_order=tuple(TENQ_PARTS["II"]), roman_items=True, statement_items=frozenset({"I-1"}),
                 exhibit_items=frozenset({"II-6"}))
_TWENTYF = FormSpec("20-F", tuple(TWENTYF), PART_OF_20F, required_parts=frozenset({"I", "II", "III"}), lettered_16=True,
                    statement_pages=True, xref_index=True, statement_items=frozenset({"17", "18"}),
                    exhibit_items=frozenset({"19"}))
_EIGHTK = FormSpec("8-K", tuple(EIGHTK), dotted_items=True, section_dividers=True, stacked_headings=True, stubs=False,
                   exhibit_items=frozenset({"9.01"}))
_EIGHTK_OLD = replace(_EIGHTK, order=tuple(EIGHTK_OLD), dotted_items=False, undotted_items=True,
                      exhibit_items=frozenset({"7"}))
_FORTYF_DOCUMENTS = frozenset({"AIF", "AFS", "MDA"})
_FORTYF = FormSpec("40-F", part_of={d: "documents" for d in _FORTYF_DOCUMENTS}, default_part="form",
                   required_parts=frozenset({"documents", "form"}), principal_documents=_FORTYF_DOCUMENTS,
                   amendment_documents=("EX-99",), titled=True)


FORM_VARIANTS = {"10-K": ("10-K", "10-K405")}


def is_amendment(form: str) -> bool:
    return form.upper().endswith("/A")


def base_form(form: str) -> str:
    """The form an amendment amends, with 10-K405 folded into 10-K: "10-K405/A" -> "10-K"."""
    f = form.upper().removesuffix("/A")
    return "10-K" if f == "10-K405" else f


def filed_forms(form: str) -> tuple[str, ...]:
    """The form names a base form is filed under: a 10-K also as 10-K405."""
    return FORM_VARIANTS.get(form, (form,))


_FAMILIES = {"10-K": "10-K", "10-K405": "10-K", "10-KT": "10-K", "10-KT405": "10-K", "10-Q": "10-Q", "10-QT": "10-Q",
             "8-K": "8-K", "8-K12B": "8-K", "8-K12G3": "8-K", "8-K15D5": "8-K", "20-F": "20-F", "40-F": "40-F"}


def form_family(form: str) -> str:
    """The form whose items a filing has: a variant (10-K405, a transition 10-KT, a successor's 8-K12B) reads as
    its form, and a small-business 10-KSB, whose items differ, is refused."""
    family = _FAMILIES.get(form.upper().removesuffix("/A"))
    if family is None:
        raise ValueError(f"unsupported form {form!r}")
    return family


def spec_for(form: str, filing_date: str | None = None) -> FormSpec:
    """The spec of a form (any variant: 10-K405, 10-Q/A) as filed on filing_date; the 8-K's items were renumbered
    in August 2004, and a filing with no date is read as current."""
    family = form_family(form)
    if family == "8-K":
        return _EIGHTK if (filing_date or "9999") >= EIGHTK_NEW_FROM else _EIGHTK_OLD
    return {"10-K": _TENK, "10-Q": _TENQ, "20-F": _TWENTYF, "40-F": _FORTYF}[family]
