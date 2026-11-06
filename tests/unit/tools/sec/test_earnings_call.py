"""Earnings-call matching for SEC filings: the call must report the filing's period."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data_client.fmp.fmp_client import CallDate, FMPClient, FMPRequestError
from src.tools.sec.earnings_call import EarningsCall, _select_call, fetch_matching_earnings_call
from src.tools.sec.tool import get_sec_filing_async
from src.tools.sec.types import FilingType

TEN_K, TEN_Q = FilingType.FORM_10K, FilingType.FORM_10Q

# A June fiscal year end whose FY2024 10-K and Q1 FY2025 10-Q were both filed
# late, on 2025-02-25, two weeks after the Q2 FY2025 call.
CALL_DATES = [
    CallDate(2, 2025, date(2025, 2, 11)),
    CallDate(1, 2025, date(2024, 11, 5)),
    CallDate(4, 2024, date(2024, 8, 6)),
    CallDate(3, 2024, date(2024, 4, 30)),
]


@pytest.mark.parametrize(
    ("filing_type", "period_end", "dates", "expected"),
    [
        (TEN_K, date(2024, 6, 30), CALL_DATES, CallDate(4, 2024, date(2024, 8, 6))),
        (TEN_Q, date(2024, 9, 30), CALL_DATES, CallDate(1, 2025, date(2024, 11, 5))),
        (TEN_Q, date(2024, 12, 31), CALL_DATES, CallDate(2, 2025, date(2025, 2, 11))),
        (TEN_K, date(2023, 12, 31), [CallDate(4, 2023, date(2024, 4, 1))], CallDate(4, 2023, date(2024, 4, 1))),
        (TEN_K, date(2023, 12, 31), [CallDate(4, 2023, date(2024, 5, 28))], CallDate(4, 2023, date(2024, 5, 28))),
        (
            TEN_Q,
            date(2026, 9, 30),
            [
                CallDate(quarter=3, fiscal_year=2026, date=date(2026, 10, 28)),
                CallDate(quarter=1, fiscal_year=2027, date=date(2026, 10, 28)),
                CallDate(quarter=4, fiscal_year=2026, date=date(2026, 6, 23)),
            ],
            CallDate(quarter=3, fiscal_year=2026, date=date(2026, 10, 28)),
        ),
        (
            TEN_K,
            date(2025, 12, 31),
            [CallDate(1, 2026, date(2026, 2, 10)), CallDate(4, 2025, date(2026, 2, 10))],
            CallDate(4, 2025, date(2026, 2, 10)),
        ),
        (
            TEN_Q,
            date(2026, 3, 31),
            [CallDate(4, 2026, date(2026, 4, 28)), CallDate(1, 2027, date(2026, 4, 28))],
            CallDate(1, 2027, date(2026, 4, 28)),
        ),
        (
            TEN_Q,
            date(2024, 12, 31),
            [
                CallDate(quarter=2, fiscal_year=2025, date=date(2025, 2, 11)),
                CallDate(quarter=1, fiscal_year=2025, date=date(2025, 1, 15)),
                CallDate(quarter=4, fiscal_year=2024, date=date(2024, 8, 6)),
            ],
            CallDate(quarter=2, fiscal_year=2025, date=date(2025, 2, 11)),
        ),
    ],
    ids=[
        "late-10k",
        "late-10q",
        "10q",
        "10k-call-92-days-after",
        "10k-call-149-days-after",
        "same-call-two-labels",
        "10k-same-date-q4-and-q1",
        "10q-same-date-q4-and-q1",
        "earlier-quarter-call-slipped-past-period-end",
    ],
)
def test_selects_first_call_after_period_end_that_fits_the_form(filing_type, period_end, dates, expected):
    assert _select_call(dates, filing_type, period_end) == expected


@pytest.mark.parametrize(
    ("filing_type", "period_end", "dates"),
    [
        (TEN_Q, date(2023, 3, 3), [CallDate(2, 2023, date(2023, 6, 15))]),
        (TEN_K, date(2024, 6, 30), [CallDate(1, 2025, date(2024, 8, 6))]),
        (TEN_Q, date(2024, 6, 30), CALL_DATES),
        (TEN_Q, date(2025, 3, 31), CALL_DATES),
        (TEN_K, date(2023, 6, 30), [CallDate(4, 2023, date(2008, 1, 28)), CallDate(1, 2024, date(2023, 10, 31))]),
        (TEN_K, date(2023, 6, 30), [CallDate(4, 2024, date(2024, 8, 6))]),
    ],
    ids=[
        "10q-call-missing-next-104-days-after",
        "10k-only-q1-call",
        "10q-q4-call-in-window-q1-too-late",
        "no-call-yet",
        "misdated-call",
        "10k-only-next-years-q4",
    ],
)
def test_no_call_for_period(filing_type, period_end, dates):
    assert _select_call(dates, filing_type, period_end) is None


def _fmp(transcript):
    client = MagicMock()
    client.get_earnings_call_dates = AsyncMock(return_value=CALL_DATES)
    client.get_earnings_call_transcript = AsyncMock(return_value=transcript)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return patch("src.tools.sec.earnings_call.FMPClient", return_value=client), client


@pytest.mark.asyncio
async def test_fetches_the_selected_calls_transcript():
    patcher, client = _fmp([{"content": "Operator: welcome."}])
    with patcher:
        result = await fetch_matching_earnings_call("ACME", TEN_K, "2024-06-30")

    assert result == EarningsCall("Operator: welcome.", 2024, 4, date(2024, 8, 6))
    client.get_earnings_call_transcript.assert_awaited_once_with("ACME", 2024, 4)


@pytest.mark.asyncio
async def test_selected_call_without_transcript_returns_none():
    patcher, _ = _fmp([{"content": ""}])
    with patcher:
        assert await fetch_matching_earnings_call("ACME", TEN_K, "2024-06-30") is None


@pytest.mark.asyncio
async def test_fmp_errors_propagate():
    with (
        patch("src.tools.sec.earnings_call.FMPClient", side_effect=ValueError("FMP API key required")),
        pytest.raises(ValueError),
    ):
        await fetch_matching_earnings_call("ACME", TEN_K, "2024-06-30")


@pytest.mark.asyncio
async def test_fmp_error_body_propagates(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    with (
        patch.object(FMPClient, "_make_request", AsyncMock(return_value={"Error Message": "Limit Reach"})),
        pytest.raises(FMPRequestError),
    ):
        await fetch_matching_earnings_call("ACME", TEN_K, "2024-06-30")


@pytest.mark.asyncio
async def test_sec_filing_matches_call_on_period_end_not_filing_date():
    metadata = {"symbol": "ACME", "filing_type": "10-K", "filing_date": "2025-02-25", "period_end": "2024-06-30"}
    fetch = AsyncMock(return_value=("Operator: welcome.", 2024, 4, date(2024, 8, 6)))
    with (
        patch("src.tools.sec.tool._fetch_sec_filing", AsyncMock(return_value=("# 10-K", "2025-02-25", metadata))),
        patch("src.tools.sec.tool.find_recent_8k_filings", AsyncMock(return_value=[])),
        patch("src.tools.sec.tool.fetch_matching_earnings_call", fetch),
    ):
        content, artifact = await get_sec_filing_async("ACME", "10-K")

    fetch.assert_awaited_once_with("ACME", "10-K", "2024-06-30")
    assert "**Matched Call:** Q4 FY2024" in content
    assert artifact["has_earnings_call"] is True
