"""E2B sandbox provider: sandboxes as E2B microVMs, built from per-tier templates."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx
import structlog
from e2b import (
    E2B,
    BuildInfo,
    FileNotFoundException,
    RateLimitException,
    SandboxException,
    SandboxNotFoundException,
    ServiceBusyException,
    TimeoutException,
)
from e2b.api import encode_path_param, handle_api_exception
from e2b.api.client.api.templates import get_templates_aliases_alias
from e2b.api.client_async import get_api_client
from e2b.connection_config import ConnectionConfig

from ptc_agent.config.core import E2BConfig, PlatformSecretDefinition, ResourceTier
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox.providers._image_recipe import (
    BASE_IMAGE,
    Env,
    Run,
    Step,
    config_hash,
    image_identity,
    image_steps,
)
from ptc_agent.core.sandbox.providers._metrics import normalize_samples
from ptc_agent.core.sandbox.providers._tiers import resolve_tier
from ptc_agent.core.sandbox.providers.e2b_runtime import E2BRuntime, _window
from ptc_agent.core.sandbox.providers.e2b_secrets import (
    TLS_TRUST_ENV,
    E2BSecretReconciler,
    egress_rules,
    secret_marker,
)
from ptc_agent.core.sandbox.runtime import (
    MetricSample,
    SandboxFailureKind,
    SandboxProvider,
    SandboxRateLimitedError,
    SandboxTransientError,
)

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.platform_secrets import (
        ReconciledPlatformSecret,
        ResolvedPlatformSecret,
    )

logger = structlog.get_logger(__name__)

# Template names are lowercase letters, digits, dashes and underscores.
_TEMPLATE_NAME_INVALID = re.compile(r"[^a-z0-9_-]+")
# Bump when the E2B translation of the recipe changes in a way the recipe's own
# identity does not record, such as the default user or when it is set.
_TEMPLATE_REVISION = 2
# Build states in which a sandbox cannot start from the template yet.
_IN_PROGRESS = frozenset({"building", "waiting"})
# How long a create waits on a template some other process is building, as for
# a Daytona snapshot. A first build runs longer; the create then fails as
# transient and a retry waits again.
_TEMPLATE_WAIT_S = 300
_TEMPLATE_POLL_S = 5
# Past any real log, so a status read carries no build logs.
_NO_LOGS = 2**30
# Imported once by the template's start command. E2B snapshots the sandbox
# after that command, page cache included, so a new computer's first
# ExecuteCode reads these from memory rather than from a cold disk.
_WARM_IMPORTS = (
    "numpy",
    "pandas",
    "yfinance",
    "mcp",
    "matplotlib.pyplot",
    "httpx",
    "pydantic",
)
_WARM_DONE = "/tmp/.warm-done"


class E2BProvider(SandboxProvider):
    """Provider that manages sandboxes through the E2B SDK.

    Construction does no I/O: the server builds a provider per computer just to
    classify an error, and the client only dials when a call is made.

    ``platform_secrets`` is the deployment's catalog. E2B binds a Secret to
    each sandbox by host and header, so the provider and its runtimes route
    every binding through it.
    """

    label = "E2B"
    # E2B caps a sandbox's continuous runtime (24 h on Pro), and renewing the
    # timeout does not lift the cap.
    holds_always_on = False
    platform_secret_namespace_env = E2BConfig.ENV_FIELDS["secret_namespace"]
    # E2B fixes a sandbox's env at create, and its stop is a pause that keeps
    # every process, so only a replacement sandbox would be clean.
    scrubs_platform_secrets = False
    reports_metrics = True

    @classmethod
    def platform_secret_name(cls, name: str) -> str:
        # E2B stores a Secret name lowercased, and a sandbox names the Secret
        # it wants by that stored form.
        return name.lower()

    def __init__(
        self,
        config: E2BConfig,
        working_dir: str | None = None,
        *,
        platform_secrets: Sequence[PlatformSecretDefinition] = (),
    ) -> None:
        self._config = config
        self._working_dir = working_dir or DEFAULT_SANDBOX_ROOT
        self._secret_routes = {d.sandbox_env_var: d for d in platform_secrets}
        # Every connection setting is passed explicitly: the SDK falls back to
        # E2B_* environment variables for anything left out, and a stray one
        # would silently send traffic somewhere else.
        params: dict[str, Any] = {"api_key": config.api_key, "debug": False}
        if config.domain:
            params["domain"] = config.domain
        if config.api_url:
            params["api_url"] = config.api_url
        if config.sandbox_url:
            params["sandbox_url"] = config.sandbox_url
        self._api_params = params
        self._client = E2B(**params)

    # -- SandboxProvider interface --

    async def reconcile_platform_secrets(
        self, secrets: Sequence[ResolvedPlatformSecret]
    ) -> list[ReconciledPlatformSecret]:
        return list(await E2BSecretReconciler(self._client).reconcile(secrets))

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        platform_secret_bindings: dict[str, str] | None = None,
        mcp_packages: list[str] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        **kwargs: Any,
    ) -> E2BRuntime:
        """Create a sandbox from the tier's template, never from a bare image.

        E2B fixes cpu and memory when a template is built, so each tier has its
        own template. An unknown (removed-from-config) tier runs at the default
        tier's size so the workspace stays recoverable. Without its template a
        create fails rather than fall back: E2B's base image lacks what the
        dependency install assumes (``uv``, the apt packages), and a computer
        started from it would stay that broken across every pause and resume.

        A bound platform secret is set up here for the sandbox's whole life:
        its env holds the variable's marker, and its egress rules carry the
        Secret, which later rotations and renames reach without the env.
        Whether its ports take public traffic is also fixed here, for good.
        """
        envs = {**TLS_TRUST_ENV, **(env_vars or {})}
        network: dict[str, Any] = {}
        if platform_secret_bindings:
            envs.update({k: secret_marker(k) for k in platform_secret_bindings})
            network["rules"] = egress_rules(
                platform_secret_bindings, self._secret_routes
            )
        if not self._config.public_traffic:
            network["allow_public_traffic"] = False
        choice = resolve_tier(
            tier,
            default_tier=self._config.default_tier,
            resource_tiers=self._config.resource_tiers,
        )
        if choice.preset is not None:
            tier_name, preset = choice.name, choice.preset
        else:
            tier_name = self._config.default_tier
            preset = self._config.resource_tiers[tier_name]

        packages = mcp_packages or []
        window = _window(
            auto_stop_minutes,
            default_s=self._config.auto_stop_interval,
            max_s=self._config.max_timeout,
        )

        async def start(template: str) -> Any:
            return await self._client.AsyncSandbox.create(
                template=template,
                timeout=window,
                envs=envs,
                network=network or None,
                metadata={"created_by": "langalpha", "tier": tier_name},
                # Running out of time pauses rather than kills, and only a
                # deliberate start resumes it, so a stopped computer is never
                # woken by a stray request to one of its ports.
                lifecycle={"on_timeout": "pause", "auto_resume": False},
            )

        # The create goes first: a template name is a hash of its recipe, so
        # one that exists is the one asked for, and checking it first costs
        # three calls on every create. E2B answers 404 for a template never
        # built or still on its first build, and only then is it checked,
        # waited on or built.
        template = self.template_name(packages, tier=tier_name, preset=preset)
        try:
            sandbox = await start(template)
        except SandboxException as e:
            if e.status_code != 404:
                raise
            template = await self._ensure_template(
                packages, tier=tier_name, preset=preset
            )
            sandbox = await start(template)
        return E2BRuntime(
            self._client,
            sandbox.sandbox_id,
            sandbox=sandbox,
            default_working_dir=self._working_dir,
            window_s=window,
            max_timeout_s=self._config.max_timeout,
            secret_routes=self._secret_routes,
            public_traffic=self._config.public_traffic,
        )

    async def get(self, sandbox_id: str) -> E2BRuntime:
        # A lookup, never a connect: connecting resumes a paused sandbox.
        info = await self._client.AsyncSandbox.get_info(sandbox_id)
        return E2BRuntime(
            self._client,
            sandbox_id,
            info=info,
            default_working_dir=self._working_dir,
            window_s=_window(
                None,
                default_s=self._config.auto_stop_interval,
                max_s=self._config.max_timeout,
            ),
            max_timeout_s=self._config.max_timeout,
            secret_routes=self._secret_routes,
            public_traffic=self._config.public_traffic,
        )

    async def metrics_since(
        self,
        sandbox_id: str,
        since: datetime,
        *,
        until: datetime | None = None,
    ) -> list[MetricSample]:
        # The class-level variant is a plain API read; the instance one would
        # connect, which resumes a paused sandbox. The API takes whole seconds
        # and floors ``start``, so the sample at ``since`` comes back again and
        # ``normalize_samples`` drops it. Its ``mem_used`` leaves the page
        # cache out (it tracks MemTotal - MemAvailable, measured 2026-10-23).
        try:
            raw = await self._client.AsyncSandbox.get_metrics(
                sandbox_id, start=since, end=until
            )
        except SandboxException as e:
            if isinstance(e, RateLimitException) or e.status_code == 429:
                raise SandboxRateLimitedError(str(e)) from e
            raise
        return normalize_samples(raw, since, until, used_excludes_cache=True)

    async def close(self) -> None:
        # The SDK pools its connections per process and has nothing to close.
        return None

    def is_transient_error(self, exc: Exception) -> bool:
        """Whether *exc* may clear on retry, judged by type, never by message.

        ``TimeoutException`` covers a sandbox that paused or dropped mid-call
        (``SandboxNotRunningException``, ``SandboxUnreachableException``); the
        reconnect that follows decides whether it is merely paused or gone.
        """
        if isinstance(
            exc,
            (
                SandboxTransientError,
                TimeoutException,
                RateLimitException,
                ServiceBusyException,
                ConnectionError,
                TimeoutError,
                httpx.TransportError,
            ),
        ):
            return True
        if isinstance(exc, SandboxException):
            status = exc.status_code
            return status is not None and (status == 429 or status >= 500)
        return False

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        """Classify an E2B failure by exception type.

        A missing file and a missing sandbox are both 404s, told apart by the
        SDK's types. Only the control plane reports a sandbox as not found;
        envd reports an unreachable or paused one as a timeout, which stays
        transient so that the reconnect, not this classifier, decides between
        paused and gone. Authentication failures stay ``UNKNOWN``: gone
        authorizes replacing the sandbox, which a bad key must never do.
        """
        if isinstance(exc, (FileNotFoundError, FileNotFoundException)):
            return SandboxFailureKind.PATH_ABSENT
        if isinstance(exc, SandboxNotFoundException):
            return SandboxFailureKind.SANDBOX_GONE
        if self.is_transient_error(exc):
            return SandboxFailureKind.TRANSIENT
        return SandboxFailureKind.UNKNOWN

    # -- Templates --

    def _image_steps(
        self, mcp_packages: Sequence[str], preset: ResourceTier
    ) -> list[Step]:
        return image_steps(
            working_dir=self._working_dir, mcp_packages=mcp_packages, cpu=preset.cpu
        )

    def template_name(
        self, mcp_packages: Sequence[str], *, tier: str, preset: ResourceTier
    ) -> str:
        """``<prefix>-<tier>-<hash>``, where the hash covers the recipe and size."""
        identity = image_identity(self._image_steps(mcp_packages, preset), preset)
        identity["builder"] = {"e2b": _TEMPLATE_REVISION, "warm": list(_WARM_IMPORTS)}
        prefix = self._config.template_name or "langalpha"
        raw = f"{prefix}-{tier}-{config_hash(identity)}".lower()
        return _TEMPLATE_NAME_INVALID.sub("-", raw)

    def template_definition(
        self, mcp_packages: Sequence[str], *, preset: ResourceTier
    ) -> Any:
        """Translate the shared image recipe into E2B's template builder."""
        # Root from the first step: set_workdir creates the folder as the
        # builder's current user, and the file mount lays its links as the
        # working dir's owner, which must be the root every command runs as.
        # The last user set is also the sandbox's default.
        builder = self._client.AsyncTemplate().from_image(BASE_IMAGE).set_user("root")
        for step in self._image_steps(mcp_packages, preset):
            if isinstance(step, Run):
                for command in step.commands:
                    builder = builder.run_cmd(command, user="root")
            elif isinstance(step, Env):
                builder = builder.set_envs(dict(step.values))
            else:
                builder = builder.set_workdir(step.path)
        # ";" rather than "&&": a module that fails to import costs only its
        # warm-up, while a ready check that never passes would fail the build.
        warm = f"python3 -c 'import {', '.join(_WARM_IMPORTS)}'; touch {_WARM_DONE}"
        return builder.set_start_cmd(warm, f"test -f {_WARM_DONE}")

    async def template_status(self, name: str) -> str | None:
        """``ready``, ``building``, ``waiting`` or ``error``; None when never built.

        A template's name and its default tag both appear when its first build
        starts, so neither says a sandbox can start from it (create answers 404
        until the build finishes). Only the build behind the default tag does,
        and its status is addressed by the template's id: by name it resolves
        only while the build is running.
        """
        template_id = await self._template_id(name)
        if template_id is None:
            return None
        tags = await self._client.AsyncTemplate.get_tags(template_id)
        build_id = next((t.build_id for t in tags if t.tag == "default"), None)
        if build_id is None:
            return None
        build = await self._client.AsyncTemplate.get_build_status(
            BuildInfo(template_id=template_id, build_id=build_id, name=name, alias=name),
            logs_offset=_NO_LOGS,
        )
        return str(getattr(build.status, "value", build.status))

    async def _template_id(self, name: str) -> str | None:
        """The id behind a template name; None when no template has the name.

        The SDK resolves names this way itself but keeps the id to itself.
        """
        res = await get_templates_aliases_alias.asyncio_detailed(
            alias=encode_path_param(name),
            client=get_api_client(ConnectionConfig(**self._api_params)),
        )
        if res.status_code == 404:
            return None
        if res.status_code >= 300 or res.parsed is None:
            raise handle_api_exception(res)
        return res.parsed.template_id

    async def build_template(
        self,
        mcp_packages: Sequence[str],
        *,
        tier: str,
        preset: ResourceTier,
        on_log: Callable[[Any], None] | None = None,
        skip_cache: bool = False,
    ) -> str:
        """Build the tier's template and return its name; takes minutes."""
        name = self.template_name(mcp_packages, tier=tier, preset=preset)
        logger.info(
            "Building E2B template",
            template=name,
            cpu=preset.cpu,
            memory_gib=preset.memory,
            disk_gib=preset.disk,
        )
        await self._client.AsyncTemplate.build(
            self.template_definition(mcp_packages, preset=preset),
            name,
            cpu_count=preset.cpu,
            memory_mb=preset.memory * 1024,
            min_free_disk_mb=preset.disk * 1024,
            skip_cache=skip_cache,
            on_build_logs=on_log
            or (lambda entry: logger.debug("E2B template build", log=str(entry))),
        )
        logger.info("Built E2B template", template=name)
        return name

    async def _ensure_template(
        self, mcp_packages: Sequence[str], *, tier: str, preset: ResourceTier
    ) -> str:
        """The tier's template name once it is ready, building it if allowed."""
        name = self.template_name(mcp_packages, tier=tier, preset=preset)
        # A lookup that fails propagates rather than start a build that may
        # already be running: the caller's retry repeats the create, and on
        # another 404 this check.
        status = await self.template_status(name)
        deadline = time.monotonic() + _TEMPLATE_WAIT_S
        while status in _IN_PROGRESS and time.monotonic() < deadline:
            await asyncio.sleep(_TEMPLATE_POLL_S)
            status = await self.template_status(name)

        if status == "ready":
            return name
        if status in _IN_PROGRESS:
            raise SandboxTransientError(f"E2B template {name} is still building")
        if not self._config.template_auto_create:
            raise RuntimeError(
                f"E2B template {name} is not built; run `make e2b-templates` "
                "or set sandbox.e2b.template_auto_create"
            )
        if status == "error":
            logger.warning("Last E2B template build failed; rebuilding", template=name)
        try:
            return await self.build_template(mcp_packages, tier=tier, preset=preset)
        except Exception:
            # Another process can start the same build between the check and
            # this one, and E2B refuses the second ("build is not in waiting
            # state"). The caller's retry then waits on the build that won.
            if await self.template_status(name) not in (*_IN_PROGRESS, "ready"):
                raise
            raise SandboxTransientError(
                f"E2B template {name} is being built elsewhere"
            ) from None
