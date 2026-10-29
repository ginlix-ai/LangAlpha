"""Wire models for the plugin marketplace (the Plugins page's Discover tab).

The catalog is fetched data, normalized before any of these is built: a field
an index entry omits or garbles arrives as None or empty rather than dropping
the entry, so most of what follows is optional by design.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class MarketplaceTerm(BaseModel):
    """One coverage key and how to show it."""

    key: str
    label: str
    label_zh: Optional[str] = None


class MarketplaceVocabulary(BaseModel):
    """The coverage terms the catalog uses, each list in display order.

    Read from the catalog's ``schemas/coverage.json`` when it ships one;
    otherwise derived from the keys its entries use, labelled by key.
    """

    assets: list[MarketplaceTerm] = Field(default_factory=list)
    regions: list[MarketplaceTerm] = Field(default_factory=list)
    data: list[MarketplaceTerm] = Field(default_factory=list)


class MarketplacePublisher(BaseModel):
    name: str
    url: Optional[str] = None
    # True when the vendor publishes the server itself, as the catalog claims.
    official: bool = False


class MarketplaceCoverage(BaseModel):
    """What a data plugin covers: asset class -> regions, plus data kinds."""

    markets: dict[str, list[str]] = Field(default_factory=dict)
    data: list[str] = Field(default_factory=list)


class MarketplaceKey(BaseModel):
    """A key the entry's manifest declares, which install asks for."""

    name: str
    label: str


class MarketplaceContents(BaseModel):
    """What installing an entry adds and asks for, read from its own files."""

    connectors: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    keys: list[MarketplaceKey] = Field(default_factory=list)


class MarketplaceEntry(BaseModel):
    """One installable catalog entry."""

    name: str
    description: str = ""
    category: Optional[str] = None
    license: Optional[str] = None
    publisher: Optional[MarketplacePublisher] = None
    homepage: Optional[str] = None
    # This origin's URL for the entry's mark; None when the catalog names none.
    icon_url: Optional[str] = None
    keywords: list[str] = Field(default_factory=list)
    auth: Optional[Literal["oauth", "api-key", "none"]] = None
    # One line on what the user needs before this works (an account, a key).
    requires: Optional[str] = None
    coverage: Optional[MarketplaceCoverage] = None
    # Known only for an entry whose files ship in the catalog repository; an
    # external entry's files are in its upstream repository, which listing
    # the catalog does not fetch.
    contents: Optional[MarketplaceContents] = None
    # What POST /api/v1/plugins installs: a forge tree URL into the catalog
    # repository, or into the upstream repository at a pinned commit.
    source_url: str
    # The name of this user's installed plugin that came from ``source_url``,
    # read the way install reads a source; None when none did. An install the
    # name was coerced on still matches, and one of the same name from
    # elsewhere does not.
    plugin: Optional[str] = None
    installed: bool = False  # ``plugin`` is set
    # ``plugin`` holds something other than what ``source_url`` installs now:
    # another tree for an entry in the catalog repository, another commit for
    # an upstream one. Updating it with ``source_url`` brings it level.
    update_available: bool = False


class MarketplaceResponse(BaseModel):
    """GET /api/v1/marketplace. ``configured`` false means Discover is off."""

    configured: bool
    name: Optional[str] = None
    description: Optional[str] = None
    # The catalog repository itself, for a "view catalog" link.
    url: Optional[str] = None
    vocabulary: MarketplaceVocabulary = Field(default_factory=MarketplaceVocabulary)
    entries: list[MarketplaceEntry] = Field(default_factory=list)
