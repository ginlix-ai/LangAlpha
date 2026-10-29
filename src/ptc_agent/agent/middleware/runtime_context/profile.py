"""The profile tier: the snapshot the epoch freezes, and how a change reads.

The profile moves underneath a thread from the platform side, so it follows the
same rule as a file: the block keeps the epoch's snapshot and a row carries the
delta. The hash is taken over the rendered fields rather than the raw profile,
so a field the block never shows cannot move the block.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Any

from ptc_agent.agent.middleware.runtime_context.changes import sha256_text
from ptc_agent.agent.middleware.runtime_context.state import as_dict
from ptc_agent.agent.middleware.runtime_context.templates import render_template
from src.server.models.profile_vocabulary import prompt_preferences, upgraded_agent_preference

_PROFILE_LABELS = {
    "name": "Name",
    "timezone": "Timezone",
    "locale": "Locale",
    "portfolio_count": "Portfolio holdings",
    "prefs_set": "Preferences set",
}

#: The investment profile's structured keys, in the order the block lists them,
#: each under the label its line and its row share.
INVESTMENT_PROFILE_LABELS = {
    "investor_type": "Investor type",
    "experience": "Experience",
    "help_with": "Wants help with",
    "familiar_instruments": "Has traded",
    "risk_level": "Risk level",
    "horizon": "Holding horizon",
    "markets": "Markets",
    "asset_classes": "Wants to trade",
    "approaches": "Analysis approaches",
    "styles": "Investing styles",
    "sectors": "Sectors",
    "brokerages": "Brokerages",
}

#: The keys the turn runner adds from the binding plans that stamp this turn's
#: order tools, not from the platform reads, so they answer when those do not.
TRADING_KEYS = ("trading_permission", "trading_asks_on", "trading_settings_url")


def line_text(value: Any) -> str | None:
    """A profile value as the block prints it on its line, or None when the
    key holds nothing (null, "", [] or {}) and has no line. A list reads
    joined in words; false and 0 are values, and print."""
    if not value and not isinstance(value, (int, float)):
        return None
    return ", ".join(map(str, value)) if isinstance(value, (list, tuple)) else str(value)


def investment_lines(investment: Any) -> list[tuple[str, str]]:
    """(label, text) for each investment-profile key that holds a value, in
    the order the block lists them: the block's lines, and the fields its
    change rows are named by."""
    values = as_dict(investment)
    return [
        (label, text)
        for key, label in INVESTMENT_PROFILE_LABELS.items()
        if (text := line_text(values.get(key))) is not None
    ]


@dataclass(frozen=True, slots=True)
class ProfileSwitches:
    """What the build has that the profile block's steering points to: the
    chat-app settings folder (``channels``, None for a build whose block says
    nothing of it) and ``schedule_wakeup`` (``wakeups``)."""

    channels: bool | None = None
    wakeups: bool = False


def render_user_profile(
    user_profile: dict[str, Any],
    user_data_counts: dict[str, Any] | None = None,
    *,
    main_agent_steering: bool,
    switches: ProfileSwitches = ProfileSwitches(),
    role: str = "analyst",
) -> str:
    """The ``components/user_profile.md.j2`` block for one profile.

    The profile is shaped here, at render time, so the template only prints:
    ``agent_preference`` splits into the keys printed as the user wrote them
    and the structured keys whose value selects guidance, and the investment
    profile into its labelled lines. A snapshot keeps the profile as it was
    read, so an epoch frozen before any of this renders the same way.
    """
    printed, steering = prompt_preferences(as_dict(user_profile.get("agent_preference")))
    return render_template(
        "components/user_profile.md.j2",
        user_profile=user_profile,
        preference_lines=[
            (key, text) for key, value in printed.items() if (text := line_text(value)) is not None
        ],
        agent_steering=steering,
        investment_lines=investment_lines(user_profile.get("investment_profile")),
        user_data_counts=user_data_counts,
        main_agent_steering=main_agent_steering,
        channels_enabled=bool(switches.channels),
        wakeups_enabled=switches.wakeups,
        role=role,
    )


def profile_read_answered(user_profile: dict[str, Any] | None) -> bool:
    """Whether the platform read behind ``user_profile`` answered.

    The trading rule is added either way, so a profile holding nothing else is
    a read that did not answer rather than a profile the user cleared.
    """
    return any(key not in TRADING_KEYS for key in user_profile or {})


@dataclass(frozen=True, slots=True)
class ProfileSnapshot:
    """The user's profile and data counts as one turn saw them."""

    user_profile: dict[str, Any] = field(default_factory=dict)
    user_data_counts: dict[str, Any] = field(default_factory=dict)
    # Resolved by the builder from the profile and the watchlist, and stated
    # in <user_identity>; carried here so a move in either input is a row.
    preferred_market: str = ""

    @classmethod
    def from_state(cls, value: Any) -> ProfileSnapshot | None:
        data = as_dict(value)
        if not data:
            return None
        return cls(
            user_profile=as_dict(data.get("user_profile")),
            user_data_counts=as_dict(data.get("user_data_counts")),
            preferred_market=str(data.get("preferred_market") or ""),
        )

    def to_state(self) -> dict[str, Any]:
        return {
            "user_profile": dict(self.user_profile),
            "user_data_counts": dict(self.user_data_counts),
            "preferred_market": self.preferred_market,
        }

    def fields(self) -> dict[str, str]:
        """Label to rendered value, one entry per line the block shows.

        Watchlist counts are split so a row can say what moved in words, and
        each agent preference and investment-profile key is its own field so
        one changed key names only itself.
        """
        rendered: dict[str, str] = {}
        profile = self.user_profile
        counts = self.user_data_counts
        for key in ("name", "timezone", "locale"):
            if profile.get(key):
                rendered[_PROFILE_LABELS[key]] = str(profile[key])
        if self.preferred_market:
            rendered["Preferred market"] = self.preferred_market
        agent_preference = upgraded_agent_preference(as_dict(profile.get("agent_preference")))
        for key, value in sorted(agent_preference.items()):
            # Every key, the ones that select guidance among them: a move in
            # one moves the block. A key holding nothing has no line there,
            # so it is no field here either.
            text = line_text(value)
            if text is not None:
                rendered[f"Preference {key}"] = text
        rendered.update(investment_lines(profile.get("investment_profile")))
        if profile.get("trading_permission"):
            # The whole rule rather than the level's name: a row is all the
            # model gets until the block is rebuilt, and a name alone would
            # move it to a level without the limits that come with it.
            rendered["Trading permission"] = " ".join(
                render_template(
                    "components/trading_permission.md.j2",
                    level=profile["trading_permission"],
                    asks_on=profile.get("trading_asks_on"),
                    trading_settings_url=profile.get("trading_settings_url"),
                ).split()
            )
        if counts.get("portfolio_count"):
            rendered[_PROFILE_LABELS["portfolio_count"]] = str(counts["portfolio_count"])
        summary = str(counts.get("watchlist_summary") or "")
        if ":" in summary and summary != "0:0":
            lists, symbols = summary.split(":", 1)
            rendered["Watchlists"] = f"{lists} list(s) with {symbols} symbol(s)"
        if counts.get("prefs_set"):
            rendered[_PROFILE_LABELS["prefs_set"]] = "yes"
        return rendered

    def sha(self) -> str:
        """Hash of what the block renders, so a field it never shows cannot move it."""
        return sha256_text(json.dumps(self.fields(), sort_keys=True, default=str))

    def with_trading_of(self, other: ProfileSnapshot) -> ProfileSnapshot:
        """This snapshot with ``other``'s trading rule in place of its own."""
        profile = {k: v for k, v in self.user_profile.items() if k not in TRADING_KEYS}
        profile.update(
            (k, other.user_profile[k]) for k in TRADING_KEYS if k in other.user_profile
        )
        return replace(self, user_profile=profile)


def profile_diff_lines(
    frozen: ProfileSnapshot, current: ProfileSnapshot
) -> list[str]:
    before = frozen.fields()
    after = current.fields()
    lines: list[str] = []
    for label in sorted(before.keys() | after.keys()):
        was, now = before.get(label), after.get(label)
        if was == now:
            continue
        if was is None:
            lines.append(f"{label}: {now} (not in the frozen block)")
        elif now is None:
            lines.append(f"{label}: no longer set (the frozen block says {was})")
        else:
            lines.append(f"{label}: {now} (the frozen block says {was})")
    return lines

