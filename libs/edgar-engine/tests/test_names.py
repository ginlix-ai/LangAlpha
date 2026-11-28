"""edgar_engine.thirteenf.names: managers by name."""
import pandas as pd

from edgar_engine.thirteenf import name_matches, name_matches_tokens, name_tokens


def test_a_managers_name_drops_case_punctuation_tags_and_legal_forms():
    assert name_tokens("BERKSHIRE HATHAWAY INC /DE/") == ("BERKSHIRE", "HATHAWAY")
    assert name_tokens("AQR Capital Management, LLC") == ("AQR", "CAPITAL", "MANAGEMENT")
    assert name_tokens("Smith & Wesson Co.") == ("SMITH", "AND", "WESSON")
    assert name_tokens("CO") == ("CO",)
    assert name_tokens("The Vanguard Group, Inc.") == ("VANGUARD", "GROUP")
    assert name_tokens("THE") == ("THE",)
    assert name_matches("The Vanguard Group", ["VANGUARD GROUP INC", "Other Capital LLC"]) == [0]
    assert name_tokens(None) == () == name_tokens(pd.NA) == name_tokens(float("nan"))
    assert name_tokens("O’Shaughnessy Asset Management") == ("OSHAUGHNESSY", "ASSET", "MANAGEMENT")


def test_an_exact_name_wins_over_names_holding_its_words():
    names = ["BERKSHIRE HATHAWAY INC", "Berkshire Hathaway Life Insurance Co of Nebraska", "Other Capital LLC"]
    assert name_matches("Berkshire Hathaway", names) == [0]
    assert name_matches("hathaway berkshire", names) == [0, 1]
    assert name_matches("Nobody", names) == []
    assert name_matches(" , ", names) == []


def test_words_made_once_match_by_the_same_rule():
    names = ["BERKSHIRE HATHAWAY INC", "Berkshire Hathaway Life Insurance Co of Nebraska", "Other Capital LLC"]
    index = [list(name_tokens(n)) for n in names]
    for query in ("Berkshire Hathaway", "hathaway berkshire", "Nobody", " , "):
        assert name_matches_tokens(name_tokens(query), index) == name_matches(query, names)
    assert name_matches_tokens(["HATHAWAY", "BERKSHIRE"], index) == [0, 1]
