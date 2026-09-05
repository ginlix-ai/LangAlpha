"""Agent Plugins (agent-plugins.org) install pipeline.

Validation, extraction, and install orchestration for plugin packages. The
plugin is a wrapper, never a fourth config tier: components fan into
user_mcp_servers / user_skills stamped with provenance, and the resolver
never learns plugins exist.

This module is the package's whole public surface. Callers import from here,
never from a submodule's private name: the fan-outs owe invalidation steps
that only their own orchestrators know to run, and reaching past them is how
a second install path came to skip one.
"""

from src.server.services.plugins.errors import (
    PluginAmbiguous,
    PluginFatal,
    PluginRejected,
)
from src.server.services.plugins.export import export_plugin_zip
from src.server.services.plugins.fetch import (
    MAX_PACKAGE_BYTES,
    compose_subdir_url,
    fetch_plugin_source,
)
from src.server.services.plugins.lifecycle import (
    install_plugin_package,
    uninstall_plugin,
)
from src.server.services.plugins.package import ValidatedPackage, validate_package
from src.server.services.plugins.post_install import (
    apply_bindings,
    apply_sse_upgrades,
)
from src.server.services.plugins.update import update_plugin_package

__all__ = [
    "MAX_PACKAGE_BYTES",
    "PluginAmbiguous",
    "PluginFatal",
    "PluginRejected",
    "ValidatedPackage",
    "apply_bindings",
    "apply_sse_upgrades",
    "compose_subdir_url",
    "export_plugin_zip",
    "fetch_plugin_source",
    "install_plugin_package",
    "uninstall_plugin",
    "update_plugin_package",
    "validate_package",
]
