"""The plugin marketplace: the curated catalog the Discover tab lists.

A catalog is an ordinary marketplace repository, the same index the install
wizard's chooser reads, and it is fetched through the plugin source path so
every egress pin, size cap and token rule an install obeys applies here too.
Its index is data someone else wrote, so each entry is normalized field by
field: a malformed field is dropped on its own, and an entry is skipped only
when it has no usable name, has nothing to install, or breaks a reader.

An entry's ``source_url`` is what POST /api/v1/plugins installs. An in-repo
entry points into the catalog's own repository on the catalog's own branch,
so Update pulls whatever the catalog ships now; an external entry points at
its upstream repository at the commit the catalog pinned. Which is also how
an installed entry is known to be out of date: an in-repo one by the hash of
the tree it would install now, an external one by the commit.

The normalized listing is cached in Redis per configured URL, and a failure
briefly too, so neither a working nor a broken catalog is refetched on every
page view. Without Redis every call fetches live.
"""

import asyncio
import json
import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, ValidationError

from src.config.env import PLUGIN_MARKETPLACE_URL
from src.server.models.marketplace import (
    MarketplaceContents,
    MarketplaceCoverage,
    MarketplaceEntry,
    MarketplaceKey,
    MarketplacePublisher,
    MarketplaceTerm,
    MarketplaceVocabulary,
)
from src.server.services.plugins.archive import extract_plugin_archive
from src.server.services.plugins.discover import (
    ExternalSource,
    LocalSource,
    read_marketplace_index,
    resolve_entry_source,
    select_subtree,
)
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.extension import NAMESPACE
from src.server.services.plugins.fetch import (
    compose_subdir_url,
    compose_tree_url,
    fetch_plugin_source,
    PackageSite,
    forge_repository,
    in_catalog_tree,
    package_site,
)
from src.server.services.plugins.icons import (
    github_avatar,
    icon_value,
    vendored_icon,
)
from src.server.services.plugins.manifest import manifest_extension
from src.server.services.plugins.package import select_plugin_files
from src.server.services.plugins.schemas import PLUGIN_SCHEMA
from src.server.services.plugins.source import name_hint
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# A catalog changes when someone merges to it, so ten minutes is fresh enough
# for a page that lists it. A failure is remembered just long enough that a
# broken catalog costs one fetch a minute rather than one per page view.
CACHE_TTL_SECONDS = 600
FAILURE_TTL_SECONDS = 60
# Bumped when the listing or the tree hashes it keeps are computed differently,
# so no worker serves a listing an earlier build cached.
_CACHE_PREFIX = "plugin-marketplace:v4:"

# Runaway guards on fetched data, far above any real catalog.
MAX_ENTRIES = 500
MAX_KEYWORDS = 12
MAX_COVERAGE_KEYS = 32
MAX_CONTENTS = 64
_MAX_URL_CHARS = 2048

_VOCABULARY_PATH = "schemas/coverage.json"
_VOCABULARY_SECTIONS = ("assets", "regions", "data")
_AUTH_KINDS = ("oauth", "api-key", "none")

# An entry installs under its index name, so the index name has to be one the
# installer would accept: the manifest schema's own rule, not a copy of it.
_NAME_RULE = PLUGIN_SCHEMA["properties"]["name"]
_NAME_RE = re.compile(_NAME_RULE["pattern"])
_NAME_MAX = _NAME_RULE["maxLength"]

_KEY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,47}")


class Catalog(BaseModel):
    """A normalized catalog, as cached.

    Entries carry no viewer-specific state (``plugin``, ``installed`` and
    ``update_available`` are filled per request), and each entry's icon
    source is kept aside in ``icons``: the response names a handle on this
    origin, never a third-party host. A source is the site or image the entry
    names, or else its publisher's GitHub avatar. ``hashes`` holds each
    in-repo entry's tree as install would record it, which the response has
    no use for.
    """

    name: str | None = None
    description: str | None = None
    url: str | None = None
    vocabulary: MarketplaceVocabulary
    entries: list[MarketplaceEntry]
    icons: dict[str, str] = Field(default_factory=dict)
    hashes: dict[str, str] = Field(default_factory=dict)


@dataclass(frozen=True)
class _Home:
    """Where the catalog lives, which is where its in-repo entries live."""

    url: str
    repo: str | None  # None for a direct archive URL
    ref: str | None
    subdir: str | None

    def locate(self, rel: str) -> str | None:
        path = f"{self.subdir}/{rel}" if self.subdir else rel
        if self.repo is not None:
            return compose_tree_url(self.repo, path, self.ref)
        try:
            return compose_subdir_url(self.url, path)
        except ValueError:
            return None

    def view_url(self) -> str | None:
        if self.repo is None:
            return None
        if self.ref is None and self.subdir is None:
            return self.repo
        return compose_tree_url(self.repo, self.subdir, self.ref)


def _json(raw: bytes | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, RecursionError):
        return None


def _text(value: Any, limit: int) -> str | None:
    """Prose, cut to ``limit`` characters: a long description still lists."""
    if not isinstance(value, str) or not (value := value.strip()):
        return None
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _label(value: Any, limit: int) -> str | None:
    """A short single-line string, or None. Never cut: a truncated license or
    publisher name would claim something the catalog did not say."""
    if not isinstance(value, str) or not (value := value.strip()):
        return None
    if len(value) > limit or not value.isprintable():
        return None
    return value


def _https(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > _MAX_URL_CHARS:
        return None
    if not value.startswith("https://") or any(c.isspace() for c in value):
        return None
    try:
        host = urlsplit(value).hostname
    except ValueError:
        return None
    return value if host else None


def _keywords(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        word = _label(item, 32)
        if word is None or word.lower() in seen:
            continue
        seen.add(word.lower())
        out.append(word)
        if len(out) == MAX_KEYWORDS:
            break
    return out


def _publisher(value: Any) -> MarketplacePublisher | None:
    if not isinstance(value, dict):
        return None
    name = _label(value.get("name"), 80)
    if name is None:
        return None
    return MarketplacePublisher(
        name=name,
        url=_https(value.get("url")),
        official=value.get("official") is True,
    )


def _requires(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("summary")
    return _text(value, 160)


def _keys(value: Any, known: set[str] | None) -> list[str]:
    """Unique coverage keys in index order, restricted to ``known`` when the
    catalog defines that vocabulary section."""
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not _KEY_RE.fullmatch(item):
            continue
        if item in out or (known is not None and item not in known):
            continue
        out.append(item)
        if len(out) == MAX_COVERAGE_KEYS:
            break
    return out


def _coverage(
    value: Any, known: dict[str, set[str] | None]
) -> MarketplaceCoverage | None:
    if not isinstance(value, dict):
        return None
    markets: dict[str, list[str]] = {}
    raw_markets = value.get("markets")
    if isinstance(raw_markets, dict):
        for asset, regions in raw_markets.items():
            if not _KEY_RE.fullmatch(asset):
                continue
            if known["assets"] is not None and asset not in known["assets"]:
                continue
            # An asset whose regions all fell away still says what the
            # plugin covers; only the asset itself is the claim.
            markets[asset] = _keys(regions, known["regions"])
            if len(markets) == MAX_COVERAGE_KEYS:
                break
    data = _keys(value.get("data"), known["data"])
    if not markets and not data:
        return None
    return MarketplaceCoverage(markets=markets, data=data)


def _source_url(
    source: LocalSource | ExternalSource | None, home: _Home
) -> str | None:
    """The install URL for an index entry, by the chooser's own reading."""
    if isinstance(source, LocalSource):
        return home.locate(source.path)
    return source.url if source is not None else None


@dataclass(frozen=True)
class _InRepo:
    contents: MarketplaceContents
    mark: str | None
    content_hash: str


def _in_repo(path: str, tree: dict[str, bytes]) -> _InRepo | None:
    """What an in-repo entry installs, the mark its manifest vendors, and the
    hash install would record for it.

    Read by install's own steps, under the name install would give a package
    that does not name itself. Without it the catalog page has only the
    index's prose to go on, and a ``requires`` that says "nothing" sat beside
    a manifest that declares a key install refuses to go without. The mark is
    the file install inlines, so Discover and the installed row draw the same
    bytes. None when the files are not in the archive or do not read as a
    plugin: install would then say why, not this listing.
    """
    files = select_subtree(tree, path)
    if not files:
        return None
    try:
        plugin = select_plugin_files(files, name_hint=name_hint(None, path))
    except (PluginFatal, PluginAmbiguous):
        return None
    # collect_skills' own rule for what is a skill, without zipping each one
    # only to list its name: a skills/ child holding a SKILL.md, whose
    # directory name is the skill's name.
    skills = sorted(
        {
            p.split("/")[1]
            for p in plugin.files
            if p.startswith("skills/") and p.count("/") == 2 and p.endswith("/SKILL.md")
        }
    )
    connectors = [p.name for p in plugin.entry_plans if p.installable and p.name]
    contents = MarketplaceContents(
        connectors=connectors[:MAX_CONTENTS],
        skills=skills[:MAX_CONTENTS],
        keys=[
            MarketplaceKey(name=s.name, label=s.label)
            for s in plugin.extension.secrets[:MAX_CONTENTS]
        ],
    )
    mark = (manifest_extension(plugin.manifest, NAMESPACE) or {}).get("icon")
    # Only a file the package carries, which install has inlined by now: a
    # site the manifest names is the index's to say.
    if not (isinstance(mark, str) and mark.startswith("data:")):
        mark = None
    return _InRepo(contents, mark, plugin.content_hash)


@dataclass(frozen=True)
class _Listed:
    entry: MarketplaceEntry
    icon: str | None  # the source ``Catalog.icons`` keeps
    content_hash: str | None  # what ``Catalog.hashes`` keeps


def _entry(
    raw: dict[str, Any],
    home: _Home,
    known: dict[str, set[str] | None],
    tree: dict[str, bytes],
) -> _Listed | None:
    """One normalized entry, or None to skip it."""
    name = raw.get("name")
    if not isinstance(name, str) or len(name) > _NAME_MAX:
        return None
    if not _NAME_RE.fullmatch(name):
        return None
    source = resolve_entry_source(raw.get("source"))
    source_url = _source_url(source, home)
    if source_url is None:
        return None
    in_repo = _in_repo(source.path, tree) if isinstance(source, LocalSource) else None
    auth = raw.get("auth")
    publisher = _publisher(raw.get("publisher"))
    homepage = _https(raw.get("homepage"))
    # The publisher's own statements of who made it, never the source: an
    # in-repo entry's source is the catalog's repository, whose owner did
    # not write what it vendors.
    icon = (
        (in_repo.mark if in_repo else None)
        or vendored_icon(raw.get("icon"), tree)
        or icon_value(raw.get("icon"))
        or github_avatar(publisher.url if publisher else None)
        or github_avatar(homepage)
    )
    return _Listed(
        MarketplaceEntry(
            name=name,
            description=_text(raw.get("description"), 512) or "",
            category=_label(raw.get("category"), 48),
            license=_label(raw.get("license"), 64),
            publisher=publisher,
            homepage=homepage,
            keywords=_keywords(raw.get("keywords")),
            auth=auth if auth in _AUTH_KINDS else None,
            requires=_requires(raw.get("requires")),
            coverage=_coverage(raw.get("coverage"), known),
            contents=in_repo.contents if in_repo else None,
            source_url=source_url,
        ),
        icon,
        in_repo.content_hash if in_repo else None,
    )


def _read_vocabulary(
    raw: bytes | None,
) -> dict[str, list[MarketplaceTerm] | None]:
    """Each section's terms in file order; None for a section the catalog
    does not define, which then neither filters nor labels anything."""
    doc = _json(raw)
    out: dict[str, list[MarketplaceTerm] | None] = {}
    for section in _VOCABULARY_SECTIONS:
        terms = doc.get(section) if isinstance(doc, dict) else None
        if not isinstance(terms, dict):
            out[section] = None
            continue
        out[section] = []
        for key, spec in terms.items():
            if not _KEY_RE.fullmatch(key):
                continue
            spec = spec if isinstance(spec, dict) else {}
            out[section].append(
                MarketplaceTerm(
                    key=key,
                    label=_label(spec.get("label"), 48) or key,
                    label_zh=_label(spec.get("label_zh"), 48),
                )
            )
    return out


def _derived_terms(
    section: str, entries: list[MarketplaceEntry]
) -> list[MarketplaceTerm]:
    """The keys the entries use, first-seen order, labelled by key."""
    keys: list[str] = []
    for entry in entries:
        coverage = entry.coverage
        if coverage is None:
            continue
        if section == "assets":
            found = list(coverage.markets)
        elif section == "regions":
            found = [r for regions in coverage.markets.values() for r in regions]
        else:
            found = coverage.data
        keys.extend(k for k in found if k not in keys)
    return [MarketplaceTerm(key=k, label=k) for k in keys]


def parse_catalog(
    url: str, files: dict[str, bytes], subdir: str | None
) -> Catalog:
    """Normalize an extracted catalog archive.

    ``files`` is the archive root and ``subdir`` the part of it ``url`` names
    (as fetch_plugin_source returns them). Raises PluginFatal when there is
    no index to read.
    """
    tree = select_subtree(files, subdir) if subdir else files
    doc = read_marketplace_index(tree)
    if doc is None:
        raise PluginFatal("the catalog has no marketplace.json index")
    repo = forge_repository(url)
    home = _Home(
        url=url,
        repo=repo[0] if repo else None,
        ref=repo[1] if repo else None,
        subdir=subdir,
    )
    vocabulary = _read_vocabulary(tree.get(_VOCABULARY_PATH))
    known = {
        section: None if terms is None else {t.key for t in terms}
        for section, terms in vocabulary.items()
    }

    entries: list[MarketplaceEntry] = []
    icons: dict[str, str] = {}
    hashes: dict[str, str] = {}
    seen: set[str] = set()
    for raw in doc["plugins"]:
        try:
            listed = _entry(raw, home, known, tree)
        except Exception:
            # Every reader under here takes data someone else wrote, and one
            # that breaks on it in a way no rule foresaw costs that entry,
            # never the listing every user sees.
            logger.warning(
                "[marketplace] skipped the entry named %r", raw.get("name"), exc_info=True
            )
            continue
        if listed is None or listed.entry.name in seen:
            continue
        seen.add(listed.entry.name)
        entries.append(listed.entry)
        if listed.icon is not None:
            icons[listed.entry.name] = listed.icon
        if listed.content_hash is not None:
            hashes[listed.entry.name] = listed.content_hash
        if len(entries) == MAX_ENTRIES:
            break

    metadata = doc.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    return Catalog(
        name=_label(doc.get("name"), 100),
        description=(
            _text(metadata.get("description"), 512)
            or _text(doc.get("description"), 512)
        ),
        url=home.view_url(),
        vocabulary=MarketplaceVocabulary(
            **{
                section: (
                    _derived_terms(section, entries) if terms is None else terms
                )
                for section, terms in vocabulary.items()
            }
        ),
        entries=entries,
        icons=icons,
        hashes=hashes,
    )


@dataclass(frozen=True)
class Installed:
    """The plugin of this user's that an entry installed."""

    plugin: str
    # The catalog now offers something other than what the plugin holds.
    update_available: bool


def installed_from(
    catalog: Catalog, plugins: Iterable[Mapping[str, Any]]
) -> dict[str, Installed]:
    """Each entry's installed plugin, by entry name: the one fetched from the
    package the entry installs, at whatever revision.

    By source rather than by name: a catalog install whose name was coerced
    is still this entry, and a plugin of the same name from elsewhere is not.
    The revision is left out because an external entry's moves with every
    commit the catalog pins, and that move is the update, not another
    package. An in-repo entry always installs the catalog's current files, so
    its tree is the comparison. Either way a plugin that did not land whole
    recorded no tree, and reads as out of date until an Update lands it.
    """
    by_place: dict[tuple[str, str | None], tuple[PackageSite, Mapping[str, Any]]] = {}
    for row in plugins:
        if row.get("source_type") != "git":
            continue
        if (site := package_site(row.get("source_ref"))) is not None:
            by_place.setdefault(site.place, (site, row))
    out: dict[str, Installed] = {}
    for entry in catalog.entries:
        site = package_site(entry.source_url)
        if site is None or (found := by_place.get(site.place)) is None:
            continue
        held, row = found
        tree = catalog.hashes.get(entry.name)
        out[entry.name] = Installed(
            plugin=row["name"],
            update_available=(
                row.get("content_hash") != tree
                if tree is not None
                else held.ref != site.ref or not row.get("content_hash")
            ),
        )
    return out


def _extract_and_parse(url: str, raw: bytes, subdir: str | None) -> Catalog:
    files, _diagnostics = extract_plugin_archive(raw)
    return parse_catalog(url, files, subdir)


async def _fetch_catalog(url: str) -> Catalog:
    raw, subdir = await fetch_plugin_source(url, listed=True)
    # Inflating the archive is CPU work bounded only by the archive caps: off
    # the event loop, as validate_package is.
    return await asyncio.to_thread(_extract_and_parse, url, raw, subdir)


async def load_marketplace() -> Catalog | None:
    """The configured catalog, normalized; None when Discover is off.

    Raises PluginFatal with a user-facing message when the catalog cannot be
    fetched or read. The same message is replayed from the cache for a minute
    afterwards, so every worker answers a broken catalog alike without each
    refetching it.
    """
    url = PLUGIN_MARKETPLACE_URL.strip()
    if not url:
        return None
    cache = get_cache_client()
    key = f"{_CACHE_PREFIX}{url}"
    cached = await cache.get(key)
    if isinstance(cached, dict):
        if isinstance(cached.get("error"), str):
            raise PluginFatal(cached["error"])
        try:
            return Catalog.model_validate(cached.get("catalog"))
        except ValidationError:
            pass  # A shape this build cannot read: fetch again and overwrite.

    try:
        catalog = await _fetch_catalog(url)
    except Exception as e:
        # Remembered whatever broke: a catalog that fails a reader in a way no
        # rule foresaw fails it again on every view, and each would refetch it.
        message = "the plugin catalog could not be read"
        if isinstance(e, PluginFatal):
            message = f"{message}: {e}"
            logger.warning("[marketplace] %s (%s)", message, url)
        else:
            logger.exception("[marketplace] %s (%s)", message, url)
        await cache.set(key, {"error": message}, ttl=FAILURE_TTL_SECONDS)
        raise PluginFatal(message) from e
    await cache.set(
        key, {"catalog": catalog.model_dump(mode="json")}, ttl=CACHE_TTL_SECONDS
    )
    return catalog


async def lists_source(url: str) -> bool:
    """Whether the configured catalog offers ``url`` as one of its entries.

    What an install or update asks before fetching with the marketplace
    token, which reads the whole catalog repository: any signed-in user can
    name a source URL, and a folder the catalog does not list is not one it
    offers. A source outside the catalog's own tree never reads the catalog.
    """
    if not in_catalog_tree(url) or (site := package_site(url)) is None:
        return False
    try:
        catalog = await load_marketplace()
    except PluginFatal:
        return False
    return catalog is not None and any(
        package_site(entry.source_url) == site for entry in catalog.entries
    )
