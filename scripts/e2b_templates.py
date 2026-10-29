"""Build the E2B sandbox templates ahead of the first sandbox that needs them.

Without this, the first computer created at each resource tier waits while its
template builds (several minutes). Names come from the same recipe hash the
create path computes, so a template built here is the one a sandbox starts from.
A ready template, or one already building, is left alone unless ``--rebuild``
is given; one whose last build failed is built again.

    uv run python scripts/e2b_templates.py                 # every tier
    uv run python scripts/e2b_templates.py --tier standard
    uv run python scripts/e2b_templates.py --rebuild --skip-cache
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ptc_agent.config.loaders import load_core_from_files  # noqa: E402
from ptc_agent.core.sandbox.providers._image_recipe import (  # noqa: E402
    builtin_mcp_packages,
)
from ptc_agent.core.sandbox.providers.e2b import _IN_PROGRESS, E2BProvider  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tier",
        action="append",
        help="tier to build (repeatable); defaults to every configured tier",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="build even when the template is ready or already building",
    )
    parser.add_argument(
        "--skip-cache",
        action="store_true",
        help="ignore E2B's layer cache and build every step again",
    )
    args = parser.parse_args()

    # Named outright: a bare dotenv search starts from the loader's own module,
    # not from this checkout, and so misses the key.
    env_file = PROJECT_ROOT / ".env"
    config = await load_core_from_files(env_file=env_file if env_file.exists() else None)
    settings = config.sandbox.e2b
    try:
        settings.require_api_key()
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2
    tiers = args.tier or list(settings.resource_tiers)
    unknown = sorted(set(tiers) - set(settings.resource_tiers))
    if unknown:
        print(
            f"Unknown tier(s): {', '.join(unknown)}. "
            f"Configured: {', '.join(settings.resource_tiers)}",
            file=sys.stderr,
        )
        return 2

    provider = E2BProvider(settings, working_dir=config.filesystem.working_directory)
    packages = builtin_mcp_packages(config)
    failed = []
    for tier in tiers:
        preset = settings.resource_tiers[tier]
        name = provider.template_name(packages, tier=tier, preset=preset)
        status = await provider.template_status(name)
        if not args.rebuild and status == "ready":
            print(f"{tier}: {name} is ready")
            continue
        if not args.rebuild and status in _IN_PROGRESS:
            print(f"{tier}: {name} is already building; rerun once it finishes")
            continue
        print(
            f"{tier}: building {name} "
            f"({preset.cpu} vCPU, {preset.memory} GiB RAM, {preset.disk} GiB disk)"
        )
        try:
            await provider.build_template(
                packages,
                tier=tier,
                preset=preset,
                on_log=lambda entry: print(f"  {entry}", flush=True),
                skip_cache=args.skip_cache,
            )
        except Exception as e:
            print(f"{tier}: build failed: {e}", file=sys.stderr)
            failed.append(tier)
            continue
        print(f"{tier}: built {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
