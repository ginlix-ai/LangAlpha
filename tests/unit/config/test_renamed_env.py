"""A setting still under its former name fails the boot instead of being read."""

import pytest

from src.config.env import _refuse_renamed

RENAMED = {"GINLIX_DATA_TEST_VAR": "LANGALPHA_DATA_TEST_VAR"}


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in (*RENAMED, *RENAMED.values()):
        monkeypatch.delenv(name, raising=False)


def test_the_former_name_alone_fails_and_names_the_new_one(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "http://data")
    with pytest.raises(RuntimeError, match="GINLIX_DATA_TEST_VAR is renamed LANGALPHA_DATA_TEST_VAR"):
        _refuse_renamed(RENAMED)


def test_a_blank_former_name_alone_fails_too(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "")
    with pytest.raises(RuntimeError, match="is renamed"):
        _refuse_renamed(RENAMED)


def test_both_names_with_different_values_fail(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "http://old")
    monkeypatch.setenv("LANGALPHA_DATA_TEST_VAR", "http://new")
    with pytest.raises(RuntimeError, match="hold different values"):
        _refuse_renamed(RENAMED)


def test_both_names_with_one_value_pass_for_a_rollback_window(monkeypatch):
    monkeypatch.setenv("GINLIX_DATA_TEST_VAR", "http://data")
    monkeypatch.setenv("LANGALPHA_DATA_TEST_VAR", "http://data")
    _refuse_renamed(RENAMED)


def test_the_new_name_alone_passes(monkeypatch):
    monkeypatch.setenv("LANGALPHA_DATA_TEST_VAR", "http://data")
    _refuse_renamed(RENAMED)
