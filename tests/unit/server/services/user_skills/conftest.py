"""Undo the suite-wide empty-bundle stub for this package.

``tests/unit/conftest.py`` stubs ``load_user_skill_bundle`` for every unit
test, because the turn path loads it from Postgres and unit tests have no
pool. The tests here are *about* that function, so they need the real one back.
"""

import pytest

from src.server.services import user_skills
from src.server.services.user_skills import materialize

_REAL_LOAD = materialize.load_user_skill_bundle


@pytest.fixture(autouse=True)
def real_user_skill_bundle(monkeypatch, _empty_user_skill_bundle):
    monkeypatch.setattr(user_skills, "load_user_skill_bundle", _REAL_LOAD)
    monkeypatch.setattr(materialize, "load_user_skill_bundle", _REAL_LOAD)
