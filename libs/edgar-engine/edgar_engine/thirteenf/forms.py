"""What each 13F form is, so the amendment rules ask a spec instead of testing form names."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ThirteenFForm:
    form: str
    amendment: bool
    holdings: bool


FORMS = {
    f.form: f for f in (
        ThirteenFForm("13F-HR", False, True),
        ThirteenFForm("13F-HR/A", True, True),
        ThirteenFForm("13F-NT", False, False),
        ThirteenFForm("13F-NT/A", True, False),
    )
}


def form_spec(form: object) -> ThirteenFForm | None:
    return FORMS.get(form.strip().upper()) if isinstance(form, str) else None
