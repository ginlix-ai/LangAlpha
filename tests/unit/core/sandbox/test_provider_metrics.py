"""Only a backend with its own usage history reports metrics.

Docker and an SSH machine keep none, so a caller holding just the provider
class skips them and the base answers with nothing rather than raising.
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.providers._metrics import normalize_samples
from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
from ptc_agent.core.sandbox.providers.docker import DockerProvider
from ptc_agent.core.sandbox.providers.e2b import E2BProvider
from ptc_agent.core.sandbox.providers.ssh import SSHProvider


@pytest.mark.parametrize(
    ("provider_cls", "reports"),
    [
        (DaytonaProvider, True),
        (E2BProvider, True),
        (DockerProvider, False),
        (SSHProvider, False),
    ],
)
def test_reports_metrics_only_where_history_exists(provider_cls, reports):
    assert provider_cls.reports_metrics is reports


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_cls", [DockerProvider, SSHProvider])
async def test_a_backend_without_history_returns_no_samples(provider_cls):
    provider = provider_cls.__new__(provider_cls)

    assert await provider.metrics_since("sbx", datetime(2026, 1, 1, tzinfo=UTC)) == []


_T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _raw(**fields):
    values = {
        "timestamp": _T0 + timedelta(minutes=1),
        "cpu_count": 2,
        "cpu_used_pct": 10.0,
        "disk_total": 0,
        "disk_used": 0,
        "mem_total": 4 * 2**30,
        "mem_used": 2**29,
        "mem_cache": 2**28,
    }
    return SimpleNamespace(**{**values, **fields})


def test_a_provider_whose_used_excludes_cache_has_it_added_back():
    [sample] = normalize_samples([_raw()], _T0, None, used_excludes_cache=True)

    assert sample.mem_used_bytes == 2**29 + 2**28
    assert sample.mem_cache_bytes == 2**28


def test_used_is_left_alone_when_it_already_includes_the_cache():
    [sample] = normalize_samples([_raw()], _T0, None, used_excludes_cache=False)

    assert sample.mem_used_bytes == 2**29
    assert sample.mem_cache_bytes == 2**28


def test_an_unreported_used_stays_none_whatever_the_cache():
    [sample] = normalize_samples(
        [_raw(mem_used=None)], _T0, None, used_excludes_cache=True
    )

    assert sample.mem_used_bytes is None
    assert sample.mem_cache_bytes == 2**28


def test_a_missing_cache_adds_nothing_to_used():
    [sample] = normalize_samples(
        [_raw(mem_cache=None)], _T0, None, used_excludes_cache=True
    )

    assert sample.mem_used_bytes == 2**29
    assert sample.mem_cache_bytes is None


@pytest.mark.parametrize("cpu", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_cpu_reading_is_dropped(cpu):
    later = _T0 + timedelta(minutes=2)
    samples = normalize_samples(
        [_raw(cpu_used_pct=cpu), _raw(timestamp=later)], _T0, None
    )

    assert [s.timestamp for s in samples] == [later]
