"""A request's chart views: validated on their own field, an invalid one
dropped rather than failing the send, never part of ``additional_context``."""

import pytest
from pydantic import ValidationError

from src.server.models.chart_view import ChartView
from src.server.models.chat import ChatRequest
from src.server.models.workflow import RetryRequest


def _view(**overrides):
    return {
        "symbol": "NVDA",
        "timeframe": "1day",
        "theme": "dark",
        "locale": "zh-CN",
        "from": "2026-01-02",
        "to": "2026-10-30T15:30",
        "width": 1200,
        "height": 600,
        **overrides,
    }


def _request(**fields) -> ChatRequest:
    return ChatRequest(workspace_id="w", messages=[{"role": "user", "content": "hi"}], **fields)


def test_reads_the_wire_names():
    view = ChartView.model_validate(_view())
    assert (view.from_, view.to) == ("2026-01-02", "2026-10-30T15:30")


def test_dumps_the_wire_names():
    assert ChartView.model_validate(_view(extra="dropped")).model_dump() == _view()


@pytest.mark.parametrize(("given", "kept"), [(50, 50), (9000, 9000), ("640", 640)])
def test_a_positive_size_is_kept_as_sent(given, kept):
    """chartkit bounds it, where the turn's jobs read it."""
    view = ChartView.model_validate(_view(width=given, height=given))
    assert (view.width, view.height) == (kept, kept)


@pytest.mark.parametrize(
    "bad",
    [
        {"timeframe": "1week"},
        {"theme": "sepia"},
        {"from": "yesterday"},
        {"to": "2026-10-30T15:30:00Z"},
        {"symbol": "X" * 33},
        {"symbol": ""},
        {"width": 0},
        {"height": -5},
        {"width": 812.6},
        {"height": None},
        {"width": float("nan")},
    ],
)
def test_invalid_rejected(bad):
    with pytest.raises(ValidationError):
        ChartView.model_validate(_view(**bad))


def test_an_invalid_view_is_dropped_not_the_request():
    request = _request(chart_views=[_view(theme="sepia"), "junk", _view()])
    assert request.chart_views == [ChartView.model_validate(_view())]
    assert request.additional_context is None


def test_a_retry_drops_an_invalid_view_too():
    assert RetryRequest(workspace_id="w", chart_views=[_view(width="wide")]).chart_views == []


def test_a_request_round_trips():
    request = _request(chart_views=[_view()])
    assert ChatRequest.model_validate(request.model_dump()).chart_views == request.chart_views


def test_a_view_is_not_an_additional_context():
    with pytest.raises(ValidationError):
        _request(additional_context=[{"type": "chart_view", **_view()}])


@pytest.mark.parametrize("given", ["junk", {"symbol": "NVDA"}, 7])
def test_views_that_are_not_a_list_are_none_not_a_422(given):
    assert _request(chart_views=given).chart_views == []


def test_no_views_stay_none():
    assert _request().chart_views is None
    assert _request(chart_views=None).chart_views is None


def test_a_long_list_is_cut_before_any_view_is_read(caplog):
    from src.server.models.chart_view import MAX_CHART_VIEWS

    views = [_view()] * MAX_CHART_VIEWS + ["junk"] * 10_000
    with caplog.at_level("WARNING", logger="src.server.models.chart_view"):
        request = _request(chart_views=views)
    assert len(request.chart_views) == MAX_CHART_VIEWS
    # The junk past the bound was never validated, so never logged one by one.
    assert len(caplog.records) == 1


def test_a_view_with_no_language_or_a_strange_one_keeps_the_rest():
    for locale in (None, "", "en_US; DROP", 7):
        view = ChartView.model_validate(_view(locale=locale))
        assert view.locale is None and view.symbol == "NVDA"
    assert "locale" not in ChartView.model_validate(_view(locale=None)).model_dump(exclude_none=True)
