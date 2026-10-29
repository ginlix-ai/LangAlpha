"""A mock provider that states the facts of a real provider kind.

The sandbox reads a provider's facts (``trusted_host``, ``mounts_livefs``,
``first_layout_version`` and the rest) off its class, where a plain mock has
none, and a mock attribute would read as true for every one of them.
``trusted_host`` is answered per deployment section, so a fact given for it is
the answer under any section.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from ptc_agent.core.sandbox.providers import provider_class
from ptc_agent.core.sandbox.runtime import SandboxProvider

FACTS = (
    *(
        name
        for name, hint in SandboxProvider.__annotations__.items()
        if "ClassVar" in str(hint)
    ),
    "trusted_host",
)


def fake_provider(kind: str = "daytona", **facts: Any) -> AsyncMock:
    """An ``AsyncMock`` spec'd on ``SandboxProvider`` whose class carries
    ``kind``'s facts, each of ``facts`` replacing one."""
    unknown = set(facts) - set(FACTS)
    if unknown:
        raise TypeError(f"not provider facts: {sorted(unknown)}")
    real = provider_class(kind)
    stated = {name: getattr(real, name) for name in FACTS if hasattr(real, name)}
    if "trusted_host" in facts:
        trusted = facts.pop("trusted_host")
        stated["trusted_host"] = classmethod(lambda cls, sandbox: trusted)
    cls = type(f"Fake{real.__name__}", (AsyncMock,), {**stated, **facts})
    provider = cls(spec=SandboxProvider)
    provider.is_transient_error = MagicMock(return_value=False)
    provider.is_host_unavailable = MagicMock(return_value=False)
    return provider
