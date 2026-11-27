"""A setting still under its former name fails the boot instead of being read."""

import pytest

from src.config.env import RENAMED_SETTINGS, _refuse_renamed

RENAMED = {"GINLIX_DATA_TEST_VAR": "LANGALPHA_DATA_TEST_VAR"}


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in (*RENAMED, *RENAMED.values()):
        monkeypatch.delenv(name, raising=False)


def test_the_former_name_alone_fails_and_names_the_new_one(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "http://data")
    with pytest.raises(
        RuntimeError,
        match="GINLIX_DATA_TEST_VAR was renamed to LANGALPHA_DATA_TEST_VAR: rename the line in your env file",
    ) as raised:
        _refuse_renamed(RENAMED)
    assert "http://data" not in str(raised.value)


def test_the_former_name_beside_the_new_one_fails_even_with_the_same_value(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "http://data")
    monkeypatch.setenv("LANGALPHA_DATA_TEST_VAR", "http://data")
    with pytest.raises(RuntimeError, match="which is also set: remove the GINLIX_DATA_TEST_VAR line"):
        _refuse_renamed(RENAMED)


def test_an_empty_former_name_is_ignored(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "")
    _refuse_renamed(RENAMED)


def test_a_whitespace_former_name_fails_since_it_was_read_as_set(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "  ")
    with pytest.raises(RuntimeError, match="GINLIX_DATA_TEST_VAR was renamed to LANGALPHA_DATA_TEST_VAR"):
        _refuse_renamed(RENAMED)


@pytest.mark.parametrize("blank", ["", "  "])
def test_a_blank_former_name_fails_where_blank_was_a_value(monkeypatch, blank):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", blank)
    with pytest.raises(RuntimeError, match="GINLIX_DATA_TEST_VAR was renamed to LANGALPHA_DATA_TEST_VAR"):
        _refuse_renamed(RENAMED, frozenset({"GINLIX_DATA_TEST_VAR"}))


def test_a_blank_sec_switch_fails_since_blank_turned_sec_reads_off(monkeypatch):
    for name in (*RENAMED_SETTINGS, *RENAMED_SETTINGS.values()):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GINLIX_DATA_SEC_ENABLED", "")
    with pytest.raises(RuntimeError, match="GINLIX_DATA_SEC_ENABLED was renamed to LANGALPHA_DATA_SEC_ENABLED"):
        _refuse_renamed(RENAMED_SETTINGS)


def test_the_new_name_alone_passes(monkeypatch):
    monkeypatch.setenv("LANGALPHA_DATA_TEST_VAR", "http://data")
    _refuse_renamed(RENAMED)
