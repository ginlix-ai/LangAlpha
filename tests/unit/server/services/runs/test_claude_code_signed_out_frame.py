"""A Claude Code token refused after admission reads as the admission refusal, not as this service failing."""

from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF
from src.server.services.runs.stream_errors import error_event_data, failure_kind_metadata


def _raised(inner: BaseException) -> BaseException:
    try:
        try:
            raise inner
        except BaseException as e:
            raise RuntimeError("graph step failed") from e
    except RuntimeError as wrapped:
        return wrapped


def test_a_refused_token_is_framed_with_the_signed_out_refusal_and_no_kind():
    exc = _raised(ClaudeTurnFailed("auth_failed", "401 invalid token"))
    frame = error_event_data("t-1", str(exc), exc=exc, model="claude-haiku-5-5-code")
    assert frame == {
        "thread_id": "t-1", "error": CLAUDE_CODE_OFF["signed_out"], "message": CLAUDE_CODE_OFF["signed_out"],
        "type": "claude_code_off", "model": "claude-haiku-5-5-code", "reason": "signed_out",
    }
    assert failure_kind_metadata(exc) == {}


def test_a_failure_anthropic_answered_is_the_providers_and_keeps_its_reason():
    spent = _raised(ClaudeTurnFailed("plan_exhausted", "five_hour limit reached"))
    frame = error_event_data("t-1", str(spent.__cause__), exc=spent, model="claude-haiku-5-5-code")
    assert (frame["error_kind"], frame["status_code"]) == ("upstream", 429)
    assert frame["hints"] == ["try_another_model"]
    assert "five_hour limit reached" in frame["error"]
    overloaded = _raised(ClaudeTurnFailed("api_error", "overloaded"))
    assert error_event_data("t-1", str(overloaded), exc=overloaded)["error_kind"] == "upstream"
    assert failure_kind_metadata(overloaded) == {"error_kind": "upstream"}


def test_a_cli_that_ended_without_a_result_is_this_services_failure():
    exc = _raised(ClaudeTurnFailed("no_result", "the CLI ended without a result"))
    assert error_event_data("t-1", str(exc), exc=exc)["error_kind"] == "internal"
