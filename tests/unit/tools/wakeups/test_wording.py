"""The wakeup tool's model-facing strings equal the approved wording byte for byte.

``approved.py`` holds the strings as the reviewed drafts had them; a change
to wording.py that drifts from them is a change to prompt surface.
"""

from src.tools.wakeups import wording
from tests.unit.tools.wakeups import approved


def test_tool_results_are_the_approved_strings():
    assert wording.TOOL_RESULTS == approved.TOOL_RESULTS


def test_every_template_has_an_approved_result():
    # A template added without its drafted example would be unreviewed.
    assert set(wording.TEMPLATES) == set(approved.TOOL_RESULTS)


def test_build_tool_results_are_the_approved_templates():
    assert wording.BUILD_TOOL_RESULTS == approved.BUILD_TOOL_RESULTS


def test_wake_lines_are_the_approved_templates():
    assert wording.WAKE_LINES == approved.WAKE_LINES
    assert wording.BUILD_WAKE_LINES == approved.BUILD_WAKE_LINES


def test_cancel_rows_and_quiet_hint_are_the_approved_strings():
    assert wording.CANCEL_ROW_ONE == approved.CANCEL_ROW_ONE
    assert wording.CANCEL_ROW_MANY == approved.CANCEL_ROW_MANY
    assert wording.CANCEL_ROW_SETTING_OFF == approved.CANCEL_ROW_SETTING_OFF
    assert wording.WAKE_QUIET_HINT == approved.WAKE_QUIET_HINT
    assert wording.WAKE_NOTE == approved.WAKE_NOTE
