"""Forms get_sec_filing reads."""

from enum import Enum


class FilingType(str, Enum):
    FORM_10K = "10-K"
    FORM_10Q = "10-Q"
    FORM_20F = "20-F"
    FORM_40F = "40-F"
    FORM_8K = "8-K"


PERIODIC_FORMS = ("10-K", "10-Q", "20-F", "40-F")
FOREIGN_FORMS = ("20-F", "40-F")
