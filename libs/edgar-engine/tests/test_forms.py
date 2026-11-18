"""Item titles by era."""
import pytest

from edgar_engine.forms import item_order, item_title, mdna_item


@pytest.mark.parametrize(("form", "item", "filed", "title"), [
    ("10-K", "6", "2023-02-01", "Reserved"),
    ("10-K", "6", "2019-02-01", None),
    ("10-K", "4", "2008-02-01", None),
    ("10-K", "14", "2001-03-01", None),
    ("10-K405", "7", "2001-03-01", "Management's Discussion and Analysis of Financial Condition and Results of Operations"),
    ("10-Q/A", "II-4", "2024-05-01", "Mine Safety Disclosures"),
    ("8-K", "2.02", None, "Results of Operations and Financial Condition"),
    ("8-K", "5", "2001-01-01", None),
    ("8-K", "2.02", "2001-01-01", None),
    ("10-KSB", "1", "2001-01-01", None),
])
def test_an_item_takes_the_forms_title_only_in_the_era_it_has_it(form, item, filed, title):
    assert item_title(form, item, filed) == title


def test_the_mdna_item_of_each_periodic_form():
    assert [mdna_item(f) for f in ("10-K405", "10-Q", "20-F", "40-F", "8-K")] == ["7", "I-2", "5", "MDA", None]


def test_an_item_added_later_is_not_in_the_order_of_a_filing_made_before():
    assert "1C" not in item_order("10-K", "2023-03-01") and "1C" in item_order("10-K", "2024-03-01")
    assert {"1A", "9C", "16"} <= set(item_order("10-K", "2023-03-01"))
    assert not {"1A", "1B", "9B"} & set(item_order("10-K405", "2001-03-01"))
    assert "II-1A" not in item_order("10-Q", "2004-05-01") and "16K" in item_order("20-F")
