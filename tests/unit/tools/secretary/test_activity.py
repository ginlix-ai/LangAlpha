"""The Chief of Staff's `<activity>` block.

The baseline files a row whenever the block's text changes, so the text has to
be a pure function of the rows: equal rows, equal bytes, and nothing that moves
with the clock alone.
"""

from __future__ import annotations

import copy
import typing
from contextlib import ExitStack
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.server.contracts.status import PUBLIC_STATUSES
from src.server.models.automation import FailureReason
from src.tools.secretary import activity
from src.tools.secretary.activity import render_activity
from src.tools.secretary.utils import ComputerPlacement

HOME = "home-0001"
HOME_COMPUTER = "computer-home"
NY = ZoneInfo("America/New_York")
TODAY = date(2026, 10, 5)


def _at(hour: int, minute: int = 0, day: int = 5) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=timezone.utc)


def _render(**rows) -> str:
    kwargs = dict(
        workspaces=[],
        workspace_total=0,
        threads=[],
        runs=[],
        holdings=[],
        home_id=HOME,
        placement=ComputerPlacement(),
        zone=NY,
        today=TODAY,
    )
    return render_activity(**{**kwargs, **rows})


def _thread(thread_id: str, *, run_status=None, cancel_requested_at=None, **row):
    """A row shaped like get_recent_threads_for_user's: the latest attempt's
    columns, which carry the status, and no thread-level status at all."""
    return {
        "conversation_thread_id": thread_id,
        "workspace_id": "ws-semis",
        "workspace_name": "Semis",
        "title": thread_id,
        "updated_at": _at(12),
        "latest_run_id": "r-1" if run_status else None,
        "latest_run_status": run_status,
        "latest_cancel_requested_at": cancel_requested_at,
        "latest_interrupt_reason": None,
        "latest_run_seq": 1 if run_status else None,
        "latest_run_started_at": _at(11) if run_status else None,
        **row,
    }


WORKSPACES = [
    {
        "workspace_id": "ws-semis",
        "name": "Semis",
        "description": "Chip makers",
        "computer_id": HOME_COMPUTER,
        "dir_name": "semis",
        "last_activity_at": _at(14),
    },
    {"workspace_id": "ws-macro", "name": "Macro", "updated_at": _at(9)},
]
THREADS = [
    _thread(
        "t-home",
        run_status="completed",
        workspace_id=HOME,
        workspace_name="Home",
        title="Morning catch-up",
        updated_at=_at(13),
    ),
    _thread("t-semis", run_status="interrupted", title=""),
]
RUNS = [
    {
        "automation_name": "Market pulse",
        "status": "completed",
        "excerpt": "Futures flat\n\nahead of CPI",
        "completed_at": _at(9, 42),
        "conversation_thread_id": "t-run",
    },
    {
        "automation_name": "Earnings sweep",
        "status": "failed",
        "failure_reason": "usage_limit",
        "error_message": "quota exhausted",
        "completed_at": _at(8),
        "conversation_thread_id": None,
    },
]
POSITIONS = [
    {"symbol": "NVDA", "quantity": 100, "average_cost": "420.10", "currency": "USD"},
    {"symbol": "7203", "quantity": 10, "average_cost": 2500, "currency": "JPY"},
]


def _full() -> str:
    return _render(
        workspaces=WORKSPACES,
        workspace_total=7,
        threads=THREADS,
        runs=RUNS,
        holdings=POSITIONS,
    )


def test_no_rows_render_nothing():
    """An empty block would read as a user with no workspaces at all."""
    assert _render() == ""


def test_each_section_is_headed_and_counted():
    sections = _full().split("\n\n")
    headers = (
        "Recently active workspaces (2 of 7):\n",
        "Recent threads, this one aside:\n",
        "Automation runs today, 2026-10-05 (America/New_York):\n",
        "Holdings by currency, largest cost basis first (2 of 2): ",
    )
    assert len(sections) == len(headers)
    for section, header in zip(sections, headers):
        assert section.startswith(header)


def test_every_workspace_carries_the_id_the_model_acts_on():
    lines = _full().splitlines()
    assert (
        "- Semis: Chip makers (last active 2026-10-05, folder `semis`, "
        "workspace_id `ws-semis`)"
    ) in lines
    # No activity stamp falls back to the row's update; no folder says none.
    assert "- Macro (last active 2026-10-05, workspace_id `ws-macro`)" in lines


def test_a_workspace_on_another_computer_names_its_folder():
    """Its folder sits at that computer's root, which the agent reaches by
    naming the computer."""
    text = _render(
        workspaces=[{**WORKSPACES[0], "computer_id": "computer-other"}],
        workspace_total=1,
    )
    assert text.splitlines()[1] == (
        "- Semis: Chip makers (last active 2026-10-05, folder `semis`, "
        "workspace_id `ws-semis`)"
    )


def test_with_two_computers_every_workspace_names_its_computer():
    """A user with one computer reads no computer at all (the tests above);
    with two, each line says which, beside its folder there."""
    home = {
        "computer_id": HOME_COMPUTER,
        "name": "Cloud computer",
        "is_home": True,
        "kind": "daytona",
        "provider_config": {},
        "root_dir": "/home/workspace",
    }
    other = {**home, "computer_id": "computer-other", "name": "Big box", "is_home": False}
    text = _render(
        workspaces=[
            WORKSPACES[0],
            {**WORKSPACES[0], "workspace_id": "ws-far", "name": "Far", "computer_id": "computer-other"},
        ],
        workspace_total=2,
        placement=ComputerPlacement.from_rows([home, other]),
    )
    lines = text.split("\n\n")[1].splitlines()
    assert lines[1] == (
        '- Semis: Chip makers (last active 2026-10-05, computer "Cloud computer", '
        "folder `semis`, workspace_id `ws-semis`)"
    )
    assert lines[2] == (
        '- Far: Chip makers (last active 2026-10-05, computer "Big box", '
        "folder `/home/workspace/semis`, workspace_id `ws-far`)"
    )


def test_a_computer_of_the_users_own_says_so_only_where_it_is_named():
    """With one computer, Home's own block says whose it is."""
    mac = {
        "computer_id": "computer-mac",
        "name": "Studio Mac",
        "is_home": False,
        "kind": "ssh",
        "provider_config": {"helper": True, "os": "Darwin"},
        "root_dir": "/Users/studio/langalpha",
    }
    hosted = {
        "computer_id": HOME_COMPUTER,
        "name": "Cloud computer",
        "is_home": True,
        "kind": "daytona",
        "provider_config": {},
        "root_dir": "/home/workspace",
    }
    on_mac = {**WORKSPACES[0], "workspace_id": "ws-mac", "computer_id": "computer-mac"}

    two = _render(
        workspaces=[WORKSPACES[0], on_mac],
        workspace_total=2,
        placement=ComputerPlacement.from_rows([hosted, mac]),
    ).split("\n\n")[1].splitlines()
    assert two[1] == (
        '- Semis: Chip makers (last active 2026-10-05, computer "Cloud computer", '
        "folder `semis`, workspace_id `ws-semis`)"
    )
    assert two[2] == (
        '- Semis: Chip makers (last active 2026-10-05, computer "Studio Mac", '
        "the user's own Mac, folder `/Users/studio/langalpha/semis`, "
        "workspace_id `ws-mac`)"
    )

    one = _render(
        workspaces=[on_mac],
        workspace_total=1,
        placement=ComputerPlacement.from_rows([{**mac, "is_home": True}]),
    ).splitlines()
    assert one[1] == (
        "- Semis: Chip makers (last active 2026-10-05, folder `semis`, "
        "workspace_id `ws-mac`)"
    )


def test_every_thread_carries_its_place_status_and_id():
    lines = _full().splitlines()
    assert (
        '- "Morning catch-up" in Home: finished, 2026-10-05 (thread_id `t-home`)'
    ) in lines
    assert (
        '- "Untitled" in Semis: waiting for an answer, 2026-10-05 (thread_id `t-semis`)'
    ) in lines


def test_runs_read_in_the_users_clock_and_failures_say_what_to_do():
    lines = _full().splitlines()
    assert (
        "- 05:42 Market pulse, finished: Futures flat ahead of CPI (thread_id `t-run`)"
    ) in lines
    # A usage limit is the user's to act on, so it reads as that rather than
    # as the raw error; a run with no thread names none.
    assert "- 04:00 Earnings sweep, failed: a usage limit stopped it" in lines


def test_equal_rows_render_equal_bytes_in_the_order_they_came():
    """The renderer keeps the database's order for everything it does not
    sort itself, and sorts holdings the same way from any input order."""
    first = _full()
    assert (
        _render(
            workspaces=copy.deepcopy(WORKSPACES),
            workspace_total=7,
            threads=copy.deepcopy(THREADS),
            runs=copy.deepcopy(RUNS),
            holdings=list(reversed(POSITIONS)),
        )
        == first
    )

    reordered = _render(
        workspaces=WORKSPACES[::-1],
        workspace_total=7,
        threads=THREADS[::-1],
        runs=RUNS[::-1],
        holdings=POSITIONS,
    )
    assert reordered != first
    for earlier, later in (
        ("ws-macro", "ws-semis"),
        ("t-semis", "t-home"),
        ("Earnings sweep", "Market pulse"),
    ):
        assert reordered.index(earlier) < reordered.index(later)


def test_thread_wording_covers_every_public_status():
    """The map is indexed directly, so a status it lacks would fail the read."""
    assert set(activity._THREAD_STATUS) == PUBLIC_STATUSES


def test_failure_wording_names_reasons_the_runs_record():
    """A renamed reason would fall back to the raw error, and the Chief of
    Staff would stop telling the user a limit or their own key stopped it."""
    assert set(activity._RUN_FAILURE) <= set(typing.get_args(FailureReason))


@pytest.mark.parametrize(
    ("run_status", "cancel_requested_at", "wording"),
    [
        (None, None, "not started"),
        ("in_progress", None, "running"),
        ("in_progress", _at(12, 5), "stopping"),
        ("interrupted", None, "waiting for an answer"),
        ("completed", None, "finished"),
        ("error", None, "failed"),
        ("cancelled", None, "stopped"),
    ],
)
def test_thread_status_is_the_latest_attempts(run_status, cancel_requested_at, wording):
    text = _render(
        threads=[
            _thread("t", run_status=run_status, cancel_requested_at=cancel_requested_at)
        ]
    )
    assert text.endswith(f'- "t" in Semis: {wording}, 2026-10-05 (thread_id `t`)')


def test_dates_are_the_users_not_utc():
    """Just past midnight UTC is still the evening before in New York."""
    text = _render(
        workspaces=[
            {"workspace_id": "w", "name": "W", "last_activity_at": _at(2, day=5)}
        ],
        workspace_total=1,
    )
    assert "last active 2026-10-04" in text


def test_an_unmapped_failure_falls_back_to_its_error_message():
    text = _render(
        runs=[
            {
                "automation_name": "Sweep",
                "status": "timeout",
                "failure_reason": "sandbox",
                "error_message": "ran past 30 minutes",
                "completed_at": _at(15),
            }
        ]
    )
    assert "Sweep, timed out: ran past 30 minutes" in text


def test_holdings_lead_with_the_largest_position_and_stop_at_the_cap():
    holdings = [
        {"symbol": f"S{i:02d}", "quantity": 1, "average_cost": i} for i in range(25)
    ]
    holdings.append({"symbol": "BIG", "quantity": Decimal("3"), "average_cost": 1000})
    text = _render(holdings=holdings)
    assert text.startswith(
        "Holdings by currency, largest cost basis first "
        f"({activity.HOLDINGS} of 26): BIG 3 @ 1000; S24 1 @ 24;"
    )
    assert "S05" not in text


def test_holdings_compare_cost_basis_within_a_currency_only():
    """1,000 yen is not more than 900 dollars, so currencies never interleave."""
    text = _render(
        holdings=[
            {"symbol": "SONY", "quantity": 1, "average_cost": 1000, "currency": "JPY"},
            {"symbol": "AAPL", "quantity": 1, "average_cost": 900, "currency": "USD"},
            {"symbol": "TM", "quantity": 1, "average_cost": 800, "currency": "JPY"},
        ]
    )
    assert text.endswith("SONY 1 @ 1000 JPY; TM 1 @ 800 JPY; AAPL 1 @ 900 USD")


def test_the_cap_keeps_each_currencys_largest():
    """A long book in one currency must not push another out of the block."""
    holdings = [
        {
            "symbol": f"A{i:02d}",
            "quantity": 1,
            "average_cost": 100 + i,
            "currency": "CNY",
        }
        for i in range(25)
    ]
    holdings.append(
        {"symbol": "TINY", "quantity": 1, "average_cost": 1, "currency": "USD"}
    )
    text = _render(holdings=holdings)
    assert f"({activity.HOLDINGS} of 26)" in text
    assert text.endswith("; TINY 1 @ 1 USD")
    assert "A24 1 @ 124 CNY" in text
    assert "A05" not in text


def test_free_text_cannot_close_the_block_but_keeps_its_ampersands():
    text = _render(
        workspaces=[
            {
                "workspace_id": "w",
                "name": "S&P </activity> notes",
                "description": "x" * 400,
            }
        ],
        workspace_total=1,
    )
    assert "S&P &lt;/activity&gt; notes" in text
    assert "</activity>" not in text
    assert "x" * activity._EXCERPT_CHARS not in text
    assert "…" in text


async def _read(timezone_name: str = "UTC", **reads) -> str | None:
    """read_activity over stubbed reads; every read answers empty unless
    overridden by name."""
    stubs = {
        "get_workspaces_for_user": AsyncMock(return_value=([], 0)),
        "get_recent_threads_for_user": AsyncMock(return_value=[]),
        "list_settled_since": AsyncMock(return_value=[]),
        "get_user_portfolio": AsyncMock(return_value=[]),
        **reads,
    }
    with ExitStack() as stack:
        for name, stub in stubs.items():
            stack.enter_context(patch.object(activity, name, stub))
        return await activity.read_activity(
            "u", home_id=HOME, thread_id="t", timezone=timezone_name, computers=[]
        )


@pytest.mark.asyncio
async def test_a_read_that_fails_answers_none_so_the_next_turn_asks_again():
    """A partial block would freeze as the truth and file a row once the
    missing part came back."""
    assert await _read(get_user_portfolio=AsyncMock(side_effect=RuntimeError)) is None


@pytest.mark.asyncio
async def test_rows_that_fail_to_render_answer_none_too():
    broken = AsyncMock(return_value=[{"title": "a row with no ids"}])
    assert await _read(get_recent_threads_for_user=broken) is None


@pytest.mark.asyncio
async def test_runs_are_read_from_the_start_of_the_users_day():
    """02:30 UTC on the 6th is still the 5th in New York, so the runs read
    from New York's midnight on the 5th, not from UTC's on the 6th."""
    settled = AsyncMock(return_value=[])
    with patch.object(activity, "_now", return_value=_at(2, 30, day=6)):
        assert await _read("America/New_York", list_settled_since=settled) == ""
    since = settled.await_args.args[1]
    assert since == datetime(2026, 10, 5, 4, tzinfo=timezone.utc)
    assert since.tzinfo == NY


_CLOUD = {
    "computer_id": HOME_COMPUTER,
    "name": "Cloud computer",
    "is_home": True,
    "kind": "daytona",
    "provider_config": {},
    "root_dir": "/home/workspace",
}
_STUDIO = {
    "computer_id": "computer-mac",
    "name": "Studio Mac",
    "is_home": False,
    "kind": "ssh",
    "provider_config": {"helper": True, "os": "Darwin"},
    "root_dir": "/Users/studio/langalpha",
}


def test_the_block_opens_with_the_computers_the_machine_tools_can_reach():
    block = _render(placement=ComputerPlacement.from_rows([_CLOUD, _STUDIO]))
    lines = block.splitlines()
    assert lines[0].startswith("The user's computers.")
    assert "Bash, ExecuteCode, Read, Write, Edit, Glob or Grep" in lines[0]
    assert lines[1] == '- "Cloud computer" (where you run, in Home, root `/home/workspace`)'
    assert lines[2] == (
        '- "Studio Mac" (the user\'s own Mac, the one they work on, '
        "root `/Users/studio/langalpha`)"
    )
    assert lines[3] == activity._OWN_MACHINE
    assert lines[4] == activity._MAC_TOOLS


_LAB = {
    "computer_id": "computer-lab",
    "name": "Lab VM",
    "is_home": False,
    "kind": "e2b",
    "provider_config": {},
    "root_dir": "/home/workspace",
}


def test_homes_own_machine_is_left_to_its_workspace_block():
    """With Home on the user's Mac, ``<workspace>`` already says what that
    machine asks of the agent, so only another of the user's own machines
    brings those lines here."""
    home_on_mac = [{**_CLOUD, "is_home": False}, {**_STUDIO, "is_home": True}]
    block = _render(placement=ComputerPlacement.from_rows(home_on_mac))
    assert activity._OWN_MACHINE not in block
    assert activity._MAC_TOOLS not in block

    lab_mac = {**_STUDIO, "computer_id": "computer-lab", "name": "Lab Mac", "is_home": False}
    block = _render(placement=ComputerPlacement.from_rows([*home_on_mac, lab_mac]))
    assert block.splitlines()[-2:] == [activity._OWN_MACHINE, activity._MAC_TOOLS]


def test_a_stopped_hosted_computer_is_marked_and_the_start_explained():
    rows = [{**_CLOUD, "status": "stopped"}, {**_LAB, "status": "stopped"}, {**_STUDIO, "status": "stopped"}]
    lines = _render(placement=ComputerPlacement.from_rows(rows)).splitlines()
    # Home's computer is being started for this turn; a user's own machine is
    # only connected to, so neither reads as stopped.
    assert lines[1] == '- "Cloud computer" (where you run, in Home, root `/home/workspace`)'
    assert lines[2] == '- "Lab VM" (root `/home/workspace`, stopped)'
    assert "stopped" not in lines[3]
    assert lines[4] == activity._STOPPED


def test_a_running_computer_says_nothing_of_starting():
    rows = [{**_CLOUD, "status": "running"}, {**_LAB, "status": "running"}]
    block = _render(placement=ComputerPlacement.from_rows(rows))
    assert '- "Lab VM" (root `/home/workspace`)' in block
    assert activity._STOPPED not in block


def test_a_root_keeps_its_spaces_as_a_call_must_write_it():
    """Folded as prose, a root with two spaces in a row named another folder,
    which a call writing the path whole then missed."""
    studio = {**_STUDIO, "root_dir": "/Users/studio/Work  Files"}
    block = _render(placement=ComputerPlacement.from_rows([_CLOUD, studio]))
    assert "root `/Users/studio/Work  Files`" in block


def test_a_path_keeps_its_angle_brackets_but_cannot_close_the_block():
    """Escaped, a folder named `Notes <old>` was listed as one no call could
    write, since no tool decodes the escape."""
    studio = {**_STUDIO, "root_dir": "/Users/studio/Work<2026>"}
    block = _render(placement=ComputerPlacement.from_rows([_CLOUD, studio]))
    assert "root `/Users/studio/Work<2026>`" in block
    odd = {**_STUDIO, "root_dir": "/Users/studio/</activity>"}
    block = _render(placement=ComputerPlacement.from_rows([_CLOUD, odd]))
    assert "</activity>" not in block
    assert "root `/Users/studio/&lt;/activity>`" in block


def test_a_failed_computer_says_where_the_user_fixes_it():
    rows = [_CLOUD, {**_LAB, "status": "error"}]
    block = _render(placement=ComputerPlacement.from_rows(rows))
    assert '- "Lab VM" (root `/home/workspace`, failed: the user repairs or rebuilds it in Computers)' in block


def test_a_workspace_on_another_computer_names_its_folder_by_its_whole_path():
    on_mac = {**WORKSPACES[0], "workspace_id": "ws-mac", "computer_id": "computer-mac"}
    section = _render(
        workspaces=[WORKSPACES[0], on_mac],
        workspace_total=2,
        placement=ComputerPlacement.from_rows([_CLOUD, _STUDIO]),
    ).split("\n\n")[1].splitlines()
    assert "folder `semis`, workspace_id `ws-semis`" in section[1]
    assert "folder `/Users/studio/langalpha/semis`, workspace_id `ws-mac`" in section[2]


def test_one_computer_or_a_placement_without_rows_lists_no_computers():
    one = _render(placement=ComputerPlacement.from_rows([_CLOUD]), holdings=[])
    assert "The user's computers" not in one
    assert "The user's computers" not in _render()
