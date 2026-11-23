"""What each name the setup step manages holds in this process.

The saved settings reach existing code as environment values. Which of those
the operator set is fixed when the process starts, so it is read once, before
this module first writes, and never changes: a value set outside the app wins,
and every other one is the saved value, or what the process started with.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from types import MappingProxyType

from ptc_agent.config.core import kind_refusals
from src.server.services.setup_settings.catalog import (
    INSTALL_GROUPS_SETTING,
    PROVIDER_SETTING,
    SANDBOX_NAMESPACES,
    key_names,
)

_at_start: Mapping[str, str] | None = None


def _plain_names() -> tuple[str, ...]:
    return (*sorted(key_names()), *SANDBOX_NAMESPACES.values(), INSTALL_GROUPS_SETTING)


def take_snapshot() -> None:
    """Record what the operator set. Idempotent: the first call wins, and boot
    makes it before anything is written."""
    global _at_start
    if _at_start is None:
        names = (*_plain_names(), PROVIDER_SETTING)
        _at_start = MappingProxyType({n: os.environ[n] for n in names if n in os.environ})


def _snapshot() -> Mapping[str, str]:
    take_snapshot()
    assert _at_start is not None
    return _at_start


def set_outside_app(name: str) -> bool:
    """Set in the process environment or ``.env``. An empty value is unset, the
    shape every name has in a ``.env`` copied from the example."""
    return bool(_snapshot().get(name))


def _value(name: str, rows: Mapping[str, str]) -> str | None:
    at_start = _snapshot()
    if at_start.get(name):
        return at_start[name]
    # A saved empty value counts: no install groups is core alone.
    return rows[name] if name in rows else at_start.get(name)


def effective_values(rows: Mapping[str, str]) -> dict[str, str | None]:
    """Each managed name's value under ``rows``; None is absent.

    A saved sandbox choice stands only where its kind can run at all. A cloud
    kind chosen before its key arrives, or whose key was since removed, must not
    reach config: config requires the key of the provider it runs on, and a boot
    that fails there serves no computers at all. A kind the build does not offer
    goes the same way.
    """
    values = {name: _value(name, rows) for name in _plain_names()}
    choice = _value(PROVIDER_SETTING, rows)
    if choice and not set_outside_app(PROVIDER_SETTING):
        # After the keys, whose presence decides whether the choice stands.
        if kind_refusals(environ_with(values)).get(choice) is not None:
            choice = _snapshot().get(PROVIDER_SETTING)
    values[PROVIDER_SETTING] = choice
    return values


def environ_with(values: Mapping[str, str | None]) -> dict[str, str]:
    """The process environment with ``values`` laid over it."""
    environ = dict(os.environ)
    for name, value in values.items():
        if value is None:
            environ.pop(name, None)
        else:
            environ[name] = value
    return environ


def saved_choice(rows: Mapping[str, str], environ: Mapping[str, str]) -> bool:
    """Whether the provider in ``environ`` is the one saved in the app."""
    chosen = rows.get(PROVIDER_SETTING)
    return (
        bool(chosen)
        and not set_outside_app(PROVIDER_SETTING)
        and environ.get(PROVIDER_SETTING) == chosen
    )


def export(rows: Mapping[str, str]) -> set[str]:
    """Bring ``os.environ`` to the values under ``rows``; the names it changed."""
    changed = set()
    for name, value in effective_values(rows).items():
        if os.environ.get(name) == value:
            continue
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
        changed.add(name)
    return changed
