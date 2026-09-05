"""plugin.json validation — the spec's failure ladder, implemented literally.

Errors are classified by where jsonschema reports them: only a root-level
``additionalProperties`` violation (unknown top-level key) and a non-object
``extensions`` value are non-fatal warnings (spec §5.2); every other schema
violation on plugin.json is fatal and nothing is written.
"""

from json import JSONDecodeError, loads
from typing import Any

from jsonschema.validators import validator_for
from referencing import Registry

from src.server.models.plugin import Diagnostic
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.schemas import (
    PLUGIN_SCHEMA,
    SCHEMA_URL_RE,
    SUPPORTED_SCHEMA_VERSIONS,
    describe_schema_error,
)

SPEC_URL = "https://agent-plugins.org/specification"

_NAME_RULE = (
    "is not a usable plugin name: use lowercase letters, digits, dots and "
    "hyphens, start and end with a letter or digit, and never repeat '-' "
    "or '.'"
)

# Empty registry: no $ref can resolve outside the vendored document, so the
# network-retrieval path is unreachable by construction.
_NO_REMOTE_REFS = Registry()

_validator_cls = validator_for(PLUGIN_SCHEMA)
_VALIDATOR = _validator_cls(PLUGIN_SCHEMA, registry=_NO_REMOTE_REFS)


def check_schema_version(value: Any, *, kind: str) -> str | None:
    """Return the declared spec version when it's a canonical-but-unsupported
    URL — the one case that deserves a better message than a const mismatch."""
    if not isinstance(value, str):
        return None
    match = SCHEMA_URL_RE.match(value)
    if match and match.group(2) == kind:
        version = match.group(1)
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            return version
    return None


def validate_manifest(raw: bytes) -> tuple[dict[str, Any], list[Diagnostic]]:
    """Validate plugin.json; return the verbatim document plus warnings.

    Raises PluginFatal on any violation past the two tolerated warns.
    """
    try:
        doc = loads(raw.decode("utf-8"))
    except (JSONDecodeError, UnicodeDecodeError) as e:
        raise PluginFatal(f"plugin.json is not valid JSON: {e}") from e
    if not isinstance(doc, dict):
        raise PluginFatal("plugin.json must be a JSON object")

    version = check_schema_version(doc.get("$schema"), kind="plugin")
    if version is not None:
        raise PluginFatal(
            f"plugin.json targets Agent Plugins {version}; this deployment "
            f"supports {', '.join(sorted(SUPPORTED_SCHEMA_VERSIONS))}"
        )

    warnings: list[Diagnostic] = []
    fatal: list[Diagnostic] = []
    for error in _VALIDATOR.iter_errors(doc):
        path = list(error.absolute_path)
        message = describe_schema_error(error)
        if error.validator == "additionalProperties" and not path:
            # In its own words rather than the validator's. Every dialect puts
            # something here that the canonical schema does not model, so this
            # is the warning nearly every real install shows, and "Additional
            # properties are not allowed" reads like a package that failed.
            # The spec's answer to an unmodelled key is to keep it and carry
            # on, which is precisely what happened, so the sentence says so.
            unknown = sorted(set(error.instance) - set(error.schema.get("properties", ())))
            warnings.append(
                Diagnostic(
                    scope="plugin",
                    code="unknown_root_key",
                    message=(
                        f"plugin.json carries {', '.join(repr(k) for k in unknown)}, "
                        "which this version of the spec does not define. They are "
                        "kept exactly as written and otherwise ignored, which is "
                        "what the spec asks for."
                    ),
                    spec_ref=SPEC_URL,
                )
            )
        elif path and path[0] == "extensions" and error.validator == "type":
            warnings.append(
                Diagnostic(
                    scope="plugin",
                    target=".".join(str(p) for p in path),
                    code="extensions_invalid",
                    message=message,
                    spec_ref=SPEC_URL,
                )
            )
        else:
            loc = ".".join(str(p) for p in path) or "plugin.json"
            if path == ["name"] and error.validator == "pattern":
                # This one is fatal, so it is the first wall a package author
                # hits, and the schema answers it with a lookahead regex.
                message = f"{error.instance!r} {_NAME_RULE}"
            fatal.append(
                Diagnostic(
                    level="error",
                    scope="plugin",
                    target=loc,
                    code="manifest_invalid",
                    message=f"{loc}: {message}",
                    spec_ref=SPEC_URL,
                )
            )
    if fatal:
        raise PluginFatal(
            "; ".join(d.message for d in fatal),
            diagnostics=fatal + warnings,
        )
    return doc, warnings


def manifest_extension(manifest: dict[str, Any], namespace: str) -> Any:
    """The extension payload for one namespace, or None.

    A non-object ``extensions`` (or member) was already downgraded to a warn —
    downstream must treat it as absent, which this accessor guarantees.
    """
    extensions = manifest.get("extensions")
    if not isinstance(extensions, dict):
        return None
    payload = extensions.get(namespace)
    return payload if isinstance(payload, dict) else None
