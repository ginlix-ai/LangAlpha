"""Build the Daytona snapshots ahead of the first sandbox that needs them.

Without this, the first computer created at each resource tier waits while its
snapshot builds, and if that build fails or times out the default tier falls
back to Daytona's bare base image. Names come from the same recipe hash the
create path computes, so a snapshot built here is the one a sandbox starts
from, and a change to the recipe means a new name. An active snapshot, or one
still on its way, is left alone; an inactive one is activated, and one whose
last build failed is built again.

    uv run python scripts/daytona_snapshots.py                 # every tier
    uv run python scripts/daytona_snapshots.py --tier standard
    uv run python scripts/daytona_snapshots.py --check         # report only

Run it with the same configuration as the server (``APP_ENV`` included), or
the names will not match the ones the server looks for.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ptc_agent.config.core import ResourceTier  # noqa: E402
from ptc_agent.config.loaders import load_core_from_files  # noqa: E402
from ptc_agent.core.sandbox.providers._image_recipe import (  # noqa: E402
    builtin_mcp_packages,
)
from ptc_agent.core.sandbox.providers.daytona import (  # noqa: E402
    _SNAPSHOT_IN_PROGRESS,
    DaytonaProvider,
)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tier",
        action="append",
        help="tier to build (repeatable); defaults to every configured tier",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report each tier's snapshot and change nothing; exits 1 unless all are active",
    )
    args = parser.parse_args()

    # Named outright: a bare dotenv search starts from the loader's own module,
    # not from this checkout, and so misses the key.
    env_file = PROJECT_ROOT / ".env"
    config = await load_core_from_files(
        env_file=env_file if env_file.exists() else None
    )
    settings = config.sandbox.daytona
    try:
        settings.require_api_key()
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2
    if not settings.snapshot_enabled:
        print(
            "Snapshots are off (sandbox.daytona.snapshot_enabled); sandboxes start from the base image."
        )
        return 0
    tiers = args.tier or list(settings.resource_tiers)
    unknown = sorted(set(tiers) - set(settings.resource_tiers))
    if unknown:
        print(
            f"Unknown tier(s): {', '.join(unknown)}. "
            f"Configured: {', '.join(settings.resource_tiers)}",
            file=sys.stderr,
        )
        return 2

    provider = DaytonaProvider(
        settings, working_dir=config.filesystem.working_directory
    )
    packages = builtin_mcp_packages(config)
    try:
        ready = [
            await prepare_tier(
                provider,
                tier,
                settings.resource_tiers[tier],
                packages,
                check=args.check,
            )
            for tier in tiers
        ]
    finally:
        await provider.close()
    return 0 if all(ready) else 1


async def prepare_tier(
    provider: DaytonaProvider,
    tier: str,
    preset: ResourceTier,
    packages: list[str],
    *,
    check: bool,
) -> bool:
    """Bring one tier's snapshot to active; False while it is not."""
    name = provider.snapshot_name(packages, tier=tier)
    try:
        state = await provider.snapshot_state(name)
        if state == "active":
            print(f"{tier}: {name} is active")
            return True
        if check:
            print(f"{tier}: {name} is {state or 'missing'}")
            return False
        if state == "inactive":
            print(f"{tier}: activating {name}")
            state = await provider.activate_snapshot(name)
        elif state not in _SNAPSHOT_IN_PROGRESS:
            print(
                f"{tier}: building {name} "
                f"({preset.cpu} vCPU, {preset.memory} GiB RAM, {preset.disk} GiB disk)"
            )
            await provider.build_snapshot(
                packages, tier=tier, on_log=lambda line: print(f"  {line}", flush=True)
            )
            state = "active"
    except Exception as e:
        print(f"{tier}: {e}", file=sys.stderr)
        return False
    if state != "active":
        print(f"{tier}: {name} is {state}; rerun once it settles")
        return False
    print(f"{tier}: {name} is ready")
    return True


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
