"""An unknown account answers every route with the outage's 503."""

from src.server.services.account import AccountUnknown
from src.server.services.platform_client import quota_service_unavailable


def test_an_unknown_account_is_the_outage_503_exactly():
    unknown, outage = AccountUnknown(), quota_service_unavailable()
    assert (unknown.status_code, unknown.detail, unknown.headers) == (
        outage.status_code,
        outage.detail,
        outage.headers,
    )
