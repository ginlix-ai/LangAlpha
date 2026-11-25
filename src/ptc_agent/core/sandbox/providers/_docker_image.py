"""The Docker sandbox image: found on the daemon, pulled from the registry
its name points at, or built from Dockerfile.sandbox."""

from __future__ import annotations

import asyncio
import io
import os
import tarfile
from pathlib import Path
from typing import Any

import structlog

from ptc_agent.core.sandbox.providers._image_recipe import Copy, toolset_steps

logger = structlog.get_logger(__name__)

# The sandbox image is gigabytes, over whatever connection its owner has.
IMAGE_PULL_TIMEOUT_S = 20 * 60
# The checkout this module sits in, whose layout the Dockerfile's COPY lines
# name: the build context is rooted there wherever the Dockerfile was found.
_CHECKOUT = Path(__file__).resolve().parents[5]


async def ensure_image(client: Any, image: str) -> None:
    """Ensure the sandbox image exists: pulled if it names a registry, since
    building Dockerfile.sandbox is a very long first run, built otherwise or
    when the pull cannot deliver it."""
    try:
        await client.images.inspect(image)
        logger.debug("Docker image found", image=image)
        return
    except Exception:
        pass  # Image not found -- try to get it

    pull_failure = None
    if names_a_registry(image):
        pull_failure = await pull_image(client, image)
        if pull_failure is None:
            logger.info("Docker sandbox image pulled", image=image)
            return
        logger.warning(
            "Docker image pull failed, building it instead",
            image=image,
            reason=pull_failure,
        )

    try:
        await build_image(client, image)
    except RuntimeError as e:
        if pull_failure is None:
            raise
        raise RuntimeError(
            f"Could not pull Docker image {image!r} ({pull_failure}), "
            f"and could not build it either: {e}"
        ) from e


async def pull_image(client: Any, image: str) -> str | None:
    """Pull *image* within its time limit; what went wrong if
    that failed, None if it worked."""
    # With no tag the daemon pulls every tag the repository has.
    tag = None if names_a_version(image) else "latest"
    logger.info("Pulling Docker sandbox image", image=image)
    try:
        async with asyncio.timeout(IMAGE_PULL_TIMEOUT_S):
            # A registry's refusal can arrive as a line in a 200 stream.
            async for line in client.images.pull(image, tag=tag, stream=True):
                if isinstance(line, dict) and line.get("error"):
                    return str(line["error"])
    except TimeoutError:
        return f"no result within {IMAGE_PULL_TIMEOUT_S // 60} minutes"
    except Exception as e:
        return str(e) or type(e).__name__
    return None


async def build_image(client: Any, image: str) -> None:
    """Build *image* from Dockerfile.sandbox."""
    logger.info(
        "Docker image not found, attempting to build",
        image=image,
    )

    # Look for Dockerfile.sandbox relative to the repo root
    dockerfile_name = "Dockerfile.sandbox"
    # Try common locations
    search_paths = [
        os.path.join(os.getcwd(), dockerfile_name),
    ]
    # Also check relative to this file's location (up to repo root)
    module_dir = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):  # Walk up to 6 levels
        candidate = os.path.join(module_dir, dockerfile_name)
        search_paths.append(candidate)
        module_dir = os.path.dirname(module_dir)

    dockerfile_path = None
    for path in search_paths:
        if os.path.isfile(path):
            dockerfile_path = path
            break

    if dockerfile_path is None:
        raise RuntimeError(
            f"Docker image {image!r} not found and "
            f"{dockerfile_name} not found in any search path. "
            f"Build the image manually or place {dockerfile_name} in the repo root."
        )

    dockerfile_basename = os.path.basename(dockerfile_path)
    image_tag = image

    logger.info(
        "Building Docker sandbox image",
        dockerfile=dockerfile_path,
        tag=image_tag,
    )

    # Use aiodocker to build. It expects a tar stream via ``fileobj`` and
    # the Dockerfile path via ``path_dockerfile``.
    try:
        build_tar = io.BytesIO()
        with tarfile.open(fileobj=build_tar, mode="w") as tar:
            tar.add(dockerfile_path, arcname=dockerfile_basename)
            # The files the Dockerfile copies in, from where they sit in the
            # checkout.
            for step in toolset_steps():
                if isinstance(step, Copy):
                    for name in step.files:
                        path = step.source / name
                        tar.add(path, arcname=path.relative_to(_CHECKOUT).as_posix())
        build_tar.seek(0)

        async for log_line in client.images.build(
            fileobj=build_tar,
            path_dockerfile=dockerfile_basename,
            tag=image_tag,
            rm=True,
            stream=True,
            encoding="identity",
        ):
            if isinstance(log_line, dict) and "stream" in log_line:
                line = log_line["stream"].strip()
                if line:
                    logger.debug("Docker build", log=line)
            elif isinstance(log_line, dict) and "error" in log_line:
                raise RuntimeError(f"Docker build failed: {log_line['error']}")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to build Docker image {image_tag!r}: {e}") from e

    logger.info("Docker sandbox image built", image=image_tag)


def names_a_registry(image: str) -> bool:
    """Docker's own reading: the part before the first slash is a registry host
    if it has a dot or a port or is ``localhost``. ``app:tag`` and ``user/app`` are
    not, and a local tag has nowhere to be pulled from."""
    first, slash, _ = image.partition("/")
    return bool(slash) and ("." in first or ":" in first or first == "localhost")


def names_a_version(image: str) -> bool:
    return "@" in image or ":" in image.rpartition("/")[2]
