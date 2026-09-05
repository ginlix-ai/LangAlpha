"""Bundles that ship with the app, presented as installed plugins.

The same packages the config loader reads to compose the built-in MCP server
set (``ptc_agent.config.plugins``), shaped for the Plugins page: what a bundle
is called, who wrote it, and which servers and skills it owns. They are read
from disk rather than installed -- nothing fetches them, they occupy none of a
user's plugin slots, and uninstall has nothing to remove.

A bundle's components are its files: the servers ``mcp.json`` names, and the
directories under its own ``skills/``. Nothing here reads a skill declaration,
so a package cannot claim a skill it does not carry.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from ptc_agent.config.plugins import Bundle, bundles
from src.server.models.plugin import PluginComponentRef, PluginInfo

logger = logging.getLogger(__name__)

SOURCE_TYPE = "bundled"


def _name(bundle: Bundle) -> str | None:
    """The name a bundle is known by everywhere off disk.

    The manifest's, not the directory's. The directory is where the files sit;
    the manifest is what the package calls itself, and it is the name the API
    answers to. They agree in the tree today, and reading one here keeps a
    rename of the other from quietly detaching a user's disable, or a mark,
    from the package it names.
    """
    name = bundle.manifest.get("name")
    return name if isinstance(name, str) and name else None


def _text(value: object) -> str | None:
    """A manifest field the wire model types as a string, or None.

    A shipped ``plugin.json`` is never schema-validated -- an uploaded one is,
    at install -- so every field read here is whatever a hand-edit left in the
    file. Answering None for the wrong shape keeps one bad field from costing
    the package its row, and the row is what the reader would use to find it.
    """
    return value if isinstance(value, str) else None


def _server_names(bundle: Bundle) -> list[str]:
    return [name for name in bundle.servers if isinstance(name, str)]


def _skill_names(bundle: Bundle) -> tuple[list[str], bool]:
    """The bundle's skill names, and whether the directory answered at all.

    False is not "ships no skills". A directory that cannot be enumerated --
    permissions, or a bundle being replaced underneath the read -- leaves the
    ownership map incomplete, and the caller that withdraws things has to
    refuse rather than subtract too little. It is the same outcome an
    unreadable manifest already gets, which is what the empty case must not be
    confused with.
    """
    skills_dir = bundle.path / "skills"
    if not skills_dir.is_dir():
        return [], True
    try:
        names = sorted(
            d.name for d in skills_dir.iterdir() if (d / "SKILL.md").is_file()
        )
    except OSError as e:
        logger.error("bundled plugin: cannot read %s: %s", skills_dir, e)
        return [], False
    return names, True


def _components(bundle: Bundle) -> list[PluginComponentRef]:
    """What the package contributes, as rows the reader can actually open.

    Each ref is a link into the MCP or Skills tab, so a name with no row
    behind it is a promise the page cannot keep. Two kinds of skill directory
    would be exactly that: one the registry never publishes (shipped, but
    nothing loads it) and one it marks hidden, which is activated
    programmatically and never appears in a listing. ``list_skills`` is what
    the Skills route itself calls, so asking it is the only way both stay out.

    Ownership still counts them: ``_skill_names`` answers for what is on disk,
    because a disable has to subtract everything the bundle brought.
    """
    from ptc_agent.agent.middleware.skills import list_skills

    listed = {entry["name"] for entry in list_skills()}
    skills, _readable = _skill_names(bundle)
    return [
        PluginComponentRef(kind="mcp", name=name, key=name)
        for name in _server_names(bundle)
    ] + [
        PluginComponentRef(kind="skill", name=name, key=name)
        for name in skills
        if name in listed
    ]


class BundleOwnershipUnavailable(RuntimeError):
    """A switched-off bundle's components could not be enumerated.

    Raised rather than answered, because the caller subtracts what comes back
    and an incomplete answer subtracts too little -- which hands the user back
    the servers or skills they turned off, with nothing to see but a log line.
    """

    def __init__(self, names: Collection[str]) -> None:
        super().__init__(
            "cannot enumerate the components of switched-off bundle(s) "
            f"{sorted(names)}: their manifests are unreadable, so the disable "
            "cannot be enforced"
        )
        self.names = frozenset(names)


@dataclass(frozen=True)
class ComponentOwners:
    """Which bundle owns each shipped MCP server and skill, by name.

    Both directions come off one read of the directory, because both are
    wanted per request and ``bundles()`` re-reads disk on purpose: the
    listings need name -> owner for attribution, and the two resolvers need
    owner -> names to expand a switched-off bundle.
    """

    servers: dict[str, str]
    skills: dict[str, str]
    #: Bundles that named themselves and whose component list did not read.
    #: We know they are here; we do not know what they own.
    unreadable: frozenset[str] = frozenset()
    #: True when a bundle could not be named at all. Its name is what a
    #: disable row holds, so with the name gone any disable might be that one.
    #: A bundle that simply ships no ``plugin.json`` is not this: it never had
    #: a name for the page to show or a user to switch off.
    anonymous: bool = False

    def _unaccounted(self, names: Collection[str]) -> frozenset[str]:
        """Which of ``names`` this map cannot answer for."""
        if self.anonymous:
            return frozenset(names)
        return frozenset(names) & self.unreadable

    def owned_by(
        self, names: Collection[str]
    ) -> tuple[frozenset[str], frozenset[str]]:
        """(server names, skill names) belonging to the named bundles.

        The guard lives here rather than at each caller: all three ask this in
        order to withdraw something, and a caller that forgot to check would
        withdraw nothing and report success.
        """
        if not names:
            return frozenset(), frozenset()
        if blind := self._unaccounted(names):
            raise BundleOwnershipUnavailable(blind)
        return (
            frozenset(n for n, owner in self.servers.items() if owner in names),
            frozenset(n for n, owner in self.skills.items() if owner in names),
        )


def component_owners() -> ComponentOwners:
    """Ownership across every bundle on disk, right now.

    A component named by two bundles is a packaging mistake with no right
    answer, so the first declaration keeps it and the second is logged rather
    than silently reassigning the row's attribution on every request.

    This is the reader for the listings, which want an edited manifest to show
    up without a restart. Anything that withdraws something wants
    ``enforcement_owners`` instead.
    """
    servers: dict[str, str] = {}
    skills: dict[str, str] = {}
    unreadable: set[str] = set()
    anonymous = False
    for bundle in bundles():
        owner = _name(bundle)
        if owner is None:
            anonymous = anonymous or not bundle.readable
            continue
        skill_names, skills_readable = _skill_names(bundle)
        if not bundle.readable or not skills_readable:
            unreadable.add(owner)
        kinds = ((servers, _server_names(bundle)), (skills, skill_names))
        for target, names in kinds:
            for name in names:
                if name in target:
                    logger.warning(
                        "bundled plugin: %s and %s both claim %r",
                        target[name], owner, name,
                    )
                    continue
                target[name] = owner
    return ComponentOwners(
        servers=servers,
        skills=skills,
        unreadable=frozenset(unreadable),
        anonymous=anonymous,
    )


def enforcement_owners() -> ComponentOwners:
    """Ownership as of the read that composed the running server set.

    The servers that launch are frozen at startup; ``component_owners``
    re-reads disk on every call. A bundle that stops being readable as itself
    after boot -- removed, renamed -- therefore leaves the live map unable to
    name it while its server is still being launched, and a disable keyed on
    that bundle quietly stops applying: the user switched a plugin off and it
    came back on, with nothing to see. Reading the snapshot closes that window.

    It does not close all of it. The snapshot is a second read, taken beside
    the one that composed the servers rather than being that read, so a change
    landing between them is still unattributed. In a container it cannot
    happen -- ``plugins/`` is an image layer with no writer -- which is why
    the two are still separate; deriving both from one parse is the thing that
    would make this airtight.

    Falls back to a live read wherever no snapshot was taken (the CLI, tests),
    which is the map that process would have frozen anyway.
    """
    from src.server.app import setup

    return setup.bundle_owners or component_owners()


def _icon_site(bundle: Bundle) -> str | None:
    site = bundle.namespace.get("icon")
    return site if isinstance(site, str) and site.strip() else None


def _info(bundle: Bundle, disabled: Collection[str]) -> PluginInfo | None:
    manifest: dict[str, Any] = bundle.manifest
    name = _name(bundle)
    if name is None:
        logger.warning("bundled plugin: %s has no usable name", bundle.path)
        return None
    author = manifest.get("author")
    keywords = manifest.get("keywords")
    return PluginInfo(
        name=name,
        version=_text(manifest.get("version")),
        description=_text(manifest.get("description")) or "",
        author=_text(author.get("name")) if isinstance(author, dict) else None,
        homepage=_text(manifest.get("homepage")),
        source_type=SOURCE_TYPE,
        source_ref=None,
        enabled=name not in disabled,
        icon_url=f"/api/v1/plugins/{name}/icon" if _icon_site(bundle) else None,
        repository=_text(manifest.get("repository")),
        license=_text(manifest.get("license")),
        keywords=[
            k for k in (keywords if isinstance(keywords, list) else []) if isinstance(k, str)
        ],
        components=_components(bundle),
    )


def list_bundled(disabled: Collection[str] = ()) -> list[PluginInfo]:
    """Every bundle under ``plugins/``, name-ordered. Never raises.

    ``disabled`` is the caller's own switched-off set; a bundle absent from it
    is enabled, which is what makes a fresh account cost no rows.

    Never raises, per bundle: this answers one endpoint that lists every
    package a user has, so a manifest nobody validated must cost that package
    its row and nothing else. Field-level coercion covers the shapes we know;
    this covers the ones we do not.
    """
    out: list[PluginInfo] = []
    for bundle in bundles():
        try:
            info = _info(bundle, disabled)
        except (TypeError, ValueError) as e:  # pydantic's error is a ValueError
            logger.warning("bundled plugin: %s is unusable: %s", bundle.path, e)
            continue
        if info is not None:
            out.append(info)
    return out


def icon_site_for(name: str) -> str | None:
    """The site a bundle names as the owner of its mark, if it names one.

    Only a wrapper bundle needs this. Ours ship their logo with the frontend,
    because fetching our own mark from our own marketing site would put a
    network hop and a cache in front of a file that is already in the bundle,
    and would fail outright on an air-gapped self-host.

    ``name`` is looked up among the bundles actually on disk and is never
    joined onto a path: this feeds an unauthenticated route, and a name is the
    only input it has. ``tests/unit/server/services/test_bundled_plugins.py``
    holds that property down.
    """
    for bundle in bundles():
        if _name(bundle) == name:
            return _icon_site(bundle)
    return None


def bundled_names() -> frozenset[str]:
    """Every bundle this build ships, by the name the API answers to."""
    return frozenset(n for b in bundles() if (n := _name(b)) is not None)
