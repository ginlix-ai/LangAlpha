"""Seam: resolving a binding to a sandbox provider and its core config.

One file of the ComputerManager split; see the package __init__."""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.runtime import SandboxGoneError

from src.server.services.computer_manager._types import ComputerBinding

logger = logging.getLogger(__name__)


class ProviderMixin:
    def _provider_settings(self, kind: str, overrides: Optional[Dict[str, Any]]):
        """Validate overrides into a fresh model to prevent cross-machine aliasing.

        Providers hold config by reference; computers.provider_config lets machines
        on the same deployment use different settings for a provider kind."""
        base = getattr(self.config.to_core_config().sandbox, kind, None)
        if base is None:
            raise ValueError(f"Unknown sandbox provider: {kind!r}")
        if not overrides:
            return base
        unknown = sorted(set(overrides) - set(type(base).model_fields))
        if unknown:
            raise ValueError(
                f"provider_config for kind {kind!r} names unknown settings: "
                f"{', '.join(unknown)}"
            )
        return type(base).model_validate({**base.model_dump(), **overrides})

    def _provider_for(self, binding: Optional[ComputerBinding]):
        """Use the persisted backend so one process can serve multiple providers.

        Unbound workspaces fall back to the deployment configuration."""
        from ptc_agent.core.sandbox.providers import build_provider, create_provider

        if binding is None or not binding.kind:
            return create_provider(self.config.to_core_config())
        return build_provider(
            binding.kind,
            self._provider_settings(binding.kind, binding.provider_config),
            working_dir=(
                binding.root_dir or self.config.filesystem.working_directory
            ),
        )

    async def provider_for_workspace(self, workspace_id: str):
        """The caller must close this session-independent provider client."""
        return self._provider_for(await self.resolve_binding(workspace_id))

    async def provider_kind_for_workspace(self, workspace_id: str) -> Optional[str]:
        binding = await self.resolve_binding(workspace_id)
        return binding.kind

    def _core_config_for(self, binding: Optional[ComputerBinding]):
        """Rebuild FilesystemConfig when the root changes.

        Its model_post_init derives allowed and denied directories; copying or
        assigning the root would leave those lists stale."""
        core_config = self.config.to_core_config()
        if binding is None:
            return core_config
        if binding.kind and binding.kind != core_config.sandbox.provider:
            core_config.sandbox.provider = binding.kind
        root = binding.root_dir
        if root and root != core_config.filesystem.working_directory:
            from ptc_agent.config.core import FilesystemConfig

            core_config.filesystem = FilesystemConfig(
                working_directory=root,
                enable_path_validation=core_config.filesystem.enable_path_validation,
            )
        return core_config

    @asynccontextmanager
    async def _detached_runtime(
        self, sandbox_id: str, *, binding: Optional[ComputerBinding] = None
    ):
        """Lifecycle operations must reach the durable sandbox without a local session.

        Own and close the provider here to avoid leaking its SDK HTTP client.
        binding selects the machine backend, otherwise the deployment is used."""
        provider = self._provider_for(binding)
        try:
            yield await provider.get(sandbox_id)
        finally:
            await provider.close()

    def _is_sandbox_gone(
        self, exc: Exception, binding: Optional[ComputerBinding] = None
    ) -> bool:
        """Reuse classifiers because constructing providers opens SDK clients.

        This synchronous method cannot await close, so a provider per failed teardown
        would leak clients. Cache per backend because exception shapes differ."""
        from ptc_agent.core.sandbox.runtime import SandboxFailureKind

        if isinstance(exc, SandboxGoneError):
            return True
        key = (
            binding.provider_identity
            if binding is not None and binding.kind
            else (None, "{}")
        )
        classifier = self._error_classifiers.get(key)
        if classifier is None:
            classifier = self._provider_for(binding if key[0] else None)
            self._error_classifiers[key] = classifier
        return classifier.classify_error(exc) is SandboxFailureKind.SANDBOX_GONE
