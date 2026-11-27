import os
import subprocess
import sys
from datetime import date

import pytest

from edgar_engine.periods import expected_period, period_deadline, prior_period

# Each row matches the data service's rules.period_deadline and rules.expected_period; every day from 2001 to 2040
# was compared once against them.
DEADLINES = [("2025-12-31", "2026-02-17"),   # February 14 is a Saturday and the Monday is Presidents' Day
             ("2026-03-31", "2026-05-15"), ("2026-06-30", "2026-08-14"),
             ("2026-09-30", "2026-11-16"),   # November 14 is a Saturday
             ("2027-12-31", "2028-02-14"), ("2020-03-31", "2020-05-15"), ("2021-03-31", "2021-05-17")]
EXPECTED = [("2026-02-19", "2025-09-30"), ("2026-02-20", "2025-12-31"), ("2026-05-17", "2025-12-31"),
            ("2026-05-18", "2026-03-31"), ("2026-11-18", "2026-06-30"), ("2026-11-19", "2026-09-30"),
            ("2027-01-02", "2026-09-30"), ("2026-12-31", "2026-09-30"), ("2026-01-01", "2025-09-30")]


@pytest.mark.parametrize("period,due", DEADLINES)
def test_a_13f_is_due_45_days_after_its_quarter_on_a_business_day(period, due):
    assert period_deadline(period) == due


@pytest.mark.parametrize("today,period", EXPECTED)
def test_the_expected_period_is_the_latest_whose_deadline_is_three_days_past(today, period):
    assert expected_period(date.fromisoformat(today)) == period


def test_prior_period():
    assert [prior_period(p) for p in ("2026-03-31", "2026-12-31", "2026-08-14")] == \
        ["2025-12-31", "2026-09-30", "2026-06-30"]


def test_periods_import_without_edgartools():
    code = ("import importlib.abc, sys\n"
            "class Block(importlib.abc.MetaPathFinder):\n"
            "    def find_spec(self, name, path, target=None):\n"
            "        if name == 'edgar' or name.startswith('edgar.'):\n"
            "            raise ImportError('edgartools is blocked')\n"
            "sys.meta_path.insert(0, Block())\n"
            "from datetime import date\n"
            "from edgar_engine.periods import expected_period\n"
            "print(expected_period(date(2026, 2, 20)))\n")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    assert (out.returncode, out.stdout.strip()) == (0, "2025-12-31"), out.stderr
