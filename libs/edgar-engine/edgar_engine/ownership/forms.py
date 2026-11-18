"""What each ownership form is, so the statistics ask a spec instead of testing form names."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OwnershipForm:
    form: str
    amendment: bool
    in_statistics: bool


FORMS = {
    f.form: f for f in (
        OwnershipForm("3", False, False),
        OwnershipForm("3/A", True, False),
        OwnershipForm("4", False, True),
        OwnershipForm("4/A", True, True),
        OwnershipForm("5", False, False),
        OwnershipForm("5/A", True, False),
    )
}


def form_spec(form: object) -> OwnershipForm | None:
    return FORMS.get(form.strip().upper()) if isinstance(form, str) else None
