"""What the setup step's routes ask for, in the shape they answer with."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ptc_agent.config.core import (
    KIND_DAEMON_UNREACHABLE,
    KIND_NOT_OFFERED,
    kind_availability,
)
from ptc_agent.core.sandbox.providers._ssh_bootstrap import INSTALL_GROUPS, selected_groups
from src.server.database.computer import any_computer_exists
from src.server.database.deployment_settings import read_deployment_settings
from src.server.services.desktop_app import in_desktop_app
from src.server.services.setup_settings.apply import save
from src.server.services.setup_settings.catalog import (
    COMPLETED_SETTING,
    INSTALL_GROUPS_SETTING,
    PROVIDER_SETTING,
    SANDBOX_KEYS,
    key_names,
    key_specs,
)
from src.server.services.setup_settings.environment import (
    effective_values,
    environ_with,
    saved_choice,
    set_outside_app,
)

KINDS = ("local", "docker", *SANDBOX_KEYS)

_MAX_VALUE_LENGTH = 4096

_REFUSED_KIND = {
    ("local", KIND_NOT_OFFERED): "This computer is not offered on this deployment",
    ("docker", KIND_NOT_OFFERED): "Docker is not offered on this deployment",
    ("docker", KIND_DAEMON_UNREACHABLE): "The Docker daemon is not reachable",
}


class SetupRefused(ValueError):
    """A request the step refuses, with a reason the caller can act on."""


def _masked(value: str) -> str:
    """Enough to recognize a key by, and less of a short one: eight characters
    of sixteen would be half the secret."""
    if len(value) >= 24:
        return f"{value[:4]}...{value[-4:]}"
    return f"...{value[-2:]}" if len(value) >= 8 else "..."


def _key_status(rows: Mapping[str, str]) -> list[dict[str, Any]]:
    keys = []
    for spec in key_specs():
        entry: dict[str, Any] = {
            "name": spec.name,
            "group": spec.group,
            "provider": spec.provider,
        }
        if set_outside_app(spec.name):
            entry["state"] = "environment"
        elif spec.name in rows:
            entry["state"] = "saved"
            entry["masked"] = _masked(rows[spec.name])
        else:
            entry["state"] = "unset"
        keys.append(entry)
    return keys


def _provider_source(rows: Mapping[str, str], environ: Mapping[str, str]) -> str:
    from ptc_agent.config.file_utils import load_agent_config
    from ptc_agent.config.utils import configured_provider

    if set_outside_app(PROVIDER_SETTING):
        return "environment"
    if saved_choice(rows, environ):
        return "saved"
    return "config" if configured_provider(load_agent_config()) else "detected"


def _current_provider(environ: Mapping[str, str]) -> str:
    from ptc_agent.config.file_utils import load_agent_config
    from ptc_agent.config.utils import resolve_sandbox_provider

    return resolve_sandbox_provider(load_agent_config(), autodetect=True, environ=environ)


def _selected_groups(environ: Mapping[str, str]) -> list[dict[str, Any]]:
    saved = environ.get(INSTALL_GROUPS_SETTING)
    # An empty value is core alone, as the runtime reads it, not the defaults.
    chosen = {g.strip() for g in saved.split(",")} if saved is not None else None
    return [
        {
            "id": group_id,
            "required": bool(spec.get("required")),
            "selected": bool(spec.get("required"))
            or (group_id in chosen if chosen is not None else bool(spec.get("default"))),
            "download_mb": spec.get("download_mb"),
            "disk_mb": spec.get("disk_mb"),
        }
        for group_id, spec in INSTALL_GROUPS.items()
    ]


async def _sandbox_status(rows: Mapping[str, str], environ: Mapping[str, str]) -> dict[str, Any]:
    availability = await kind_availability(environ)
    options: list[dict[str, Any]] = [
        {
            "kind": "local",
            "available": availability["local"] is None,
            "detail": availability["local"],
            "install_groups": _selected_groups(environ),
        },
        {
            "kind": "docker",
            "available": availability["docker"] is None,
            "detail": availability["docker"],
        },
    ]
    # A cloud kind may be chosen before its key arrives.
    options.extend(
        {"kind": kind, "available": True, "detail": None, "key_set": availability[kind] is None}
        for kind in SANDBOX_KEYS
    )
    return {
        "provider": _current_provider(environ),
        "provider_source": _provider_source(rows, environ),
        "options": options,
    }


async def _predates_the_step() -> bool:
    """Set up some other way: a computer exists, or the operator chose in the
    environment. Such a deployment is not sent through the step."""
    if any(set_outside_app(n) for n in (PROVIDER_SETTING, *SANDBOX_KEYS.values())):
        return True
    return await any_computer_exists()


async def _status_from(rows: Mapping[str, str]) -> dict[str, Any]:
    """Read from ``rows`` rather than this worker's environment, so an answer
    never lags a save another worker took."""
    environ = environ_with(effective_values(rows))
    completed = COMPLETED_SETTING in rows
    return {
        "completed": completed,
        "needs_setup": not completed and not await _predates_the_step(),
        "sandbox": await _sandbox_status(rows, environ),
        "keys": _key_status(rows),
        # Inside the desktop app the server is the reader's own computer, so it
        # stops when that computer sleeps or the app quits; set by the app.
        "desktop_app": in_desktop_app(),
    }


async def read_status() -> dict[str, Any]:
    return await _status_from(await read_deployment_settings())


async def save_keys(keys: Mapping[str, str | None]) -> dict[str, Any]:
    """Save and remove keys. An empty value is a removal, as null is."""
    allowed = key_names()
    values: dict[str, str | None] = {}
    for name, raw in keys.items():
        if name not in allowed:
            raise SetupRefused(f"{name} is not a key this step holds")
        if set_outside_app(name):
            raise SetupRefused(f"{name} is set outside the app, so it is read-only here")
        value = raw.strip() if raw else None
        if value and (len(value) > _MAX_VALUE_LENGTH or not value.isprintable()):
            raise SetupRefused(f"{name} is not a usable key")
        values[name] = value or None
    rows = await save(values) if values else await read_deployment_settings()
    return await _status_from(rows)


async def save_sandbox(provider: str, install_groups: list[str] | None) -> dict[str, Any]:
    """Save where agent code runs. A cloud kind may be chosen before its key
    arrives: it takes effect once the key is there, whichever is saved first."""
    if provider not in KINDS:
        raise SetupRefused(f"{provider} is not a sandbox option")
    if provider not in SANDBOX_KEYS:
        why = (await kind_availability()).get(provider)
        if why is not None:
            raise SetupRefused(_REFUSED_KIND.get((provider, why), f"{provider} cannot run here"))
    values: dict[str, str | None] = {PROVIDER_SETTING: provider}
    if install_groups is not None:
        unknown = sorted(set(install_groups) - set(INSTALL_GROUPS))
        if unknown:
            raise SetupRefused(f"Unknown install groups: {', '.join(unknown)}")
        # As the runtime will install them, so status shows browsing on when
        # Chromium pulled it in.
        selected = selected_groups(install_groups)
        values[INSTALL_GROUPS_SETTING] = ",".join(g for g in INSTALL_GROUPS if g in selected)
    return await _status_from(await save(values))


async def complete() -> dict[str, Any]:
    return await _status_from(await save({COMPLETED_SETTING: "1"}))
