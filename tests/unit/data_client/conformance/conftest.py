"""CMDP provider conformance suite.

Golden fixtures under ``fixtures/`` are live captures of raw provider
responses (see each file's ``_meta``) — they document TODAY's upstream
behavior, including known bugs, and are the ground truth Phase 1 normalizers
are built against.

Layers:
- ``test_fixture_facts``      — locks what the captures show (must stay green)
- ``test_protocol_checklist`` — protocol-level checklist items (green now)
- ``test_provider_conformance`` — normalizer contract; strict-xfail until
  Phase 1 implements ``normalize_series`` per provider, then the markers
  come off in the same PR.
"""

import importlib
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

_PROVIDER_MODULES = {
    "fmp": "src.data_client.fmp.data_source",
    "yfinance": "src.data_client.yfinance.data_source",
    "ginlix-data": "src.data_client.ginlix_data.data_source",
}


def load_fixture(name: str) -> dict:
    path = FIXTURES / name
    if not path.exists():
        pytest.skip(f"fixture {name} not captured")
    return json.loads(path.read_text())


def series_normalizer(provider: str):
    """Resolve a provider's ``normalize_series`` adapter (lands in Phase 1)."""
    mod = importlib.import_module(_PROVIDER_MODULES[provider])
    fn = getattr(mod, "normalize_series", None)
    if fn is None:
        raise NotImplementedError(f"{provider}.normalize_series lands in Phase 1")
    return fn


@pytest.fixture
def fmp_hk_raw() -> dict:
    return load_fixture("fmp_0700hk_intraday_1hour_raw.json")


@pytest.fixture
def fmp_vodl_daily_raw() -> dict:
    return load_fixture("fmp_vodl_daily_raw.json")


@pytest.fixture
def fmp_quotes_raw() -> dict:
    return load_fixture("fmp_quotes_raw.json")


@pytest.fixture
def yf_vodl_adjusted() -> dict:
    return load_fixture("yfinance_vodl_daily_adjusted.json")


@pytest.fixture
def yf_vodl_raw() -> dict:
    return load_fixture("yfinance_vodl_daily_raw.json")


@pytest.fixture
def yf_hk_1h() -> dict:
    return load_fixture("yfinance_0700hk_1h.json")
