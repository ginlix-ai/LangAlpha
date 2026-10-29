"""The plugin marketplace's settled contract.

Two halves. What a catalog index normalizes to: the index is fetched data
someone else wrote, so each field is kept or dropped on its own, an entry is
skipped only when it has no usable name or nothing to install, and its
``source_url`` is exactly what POST /api/v1/plugins will be handed. And where
the marketplace token may travel: GitHub's API, for the marketplace's own
repository, on the first request only.
"""

import base64
import hashlib
import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from src.server.services.plugins import fetch, marketplace
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.fetch import (
    compose_subdir_url,
    fetch_plugin_source,
    normalize_forge_url,
    package_site,
)
from src.server.services.plugins.marketplace import (
    installed_from,
    load_marketplace,
    parse_catalog,
)
from src.server.services.plugins.package import (
    _tree_hash,
    select_plugin_files,
    validate_package,
)
from src.server.services.plugins.source import (
    resolve_update_source,
    resolve_url_source,
)
from tests.unit.server.mcp_builders import zip_package

HOME = "https://github.com/acme/catalog"
SHA = "574ed3624aebd0418c7e96cd101262f30210ab26"


def catalog(plugins, *, url=HOME, subdir=None, vocabulary=None):
    """parse_catalog over an archive holding ``plugins`` as its index."""
    prefix = f"{subdir}/" if subdir else ""
    files = {
        f"{prefix}marketplace.json": json.dumps(
            {
                "name": "acme-catalog",
                "metadata": {"description": "Acme's picks."},
                "plugins": plugins,
            }
        ).encode()
    }
    if vocabulary is not None:
        files[f"{prefix}schemas/coverage.json"] = json.dumps(vocabulary).encode()
    return parse_catalog(url, files, subdir)


def only(plugins, **kwargs):
    result = catalog(plugins, **kwargs)
    assert len(result.entries) == 1
    return result.entries[0]


class TestSourceUrls:
    def test_an_in_repo_entry_installs_from_the_catalog_repository(self):
        entry = only([{"name": "wind", "source": "./plugins/wind"}])

        assert entry.source_url == f"{HOME}/tree/HEAD/plugins/wind"
        # The install endpoint parses it straight back to the catalog archive
        # and the entry's directory inside it.
        archive, subdir = normalize_forge_url(entry.source_url)
        assert archive == "https://codeload.github.com/acme/catalog/tar.gz/HEAD"
        assert subdir == "plugins/wind"

    def test_the_local_object_form_is_the_same_entry(self):
        entry = only(
            [{"name": "wind", "source": {"source": "local", "path": "plugins/wind"}}]
        )

        assert entry.source_url == f"{HOME}/tree/HEAD/plugins/wind"

    def test_a_pinned_branch_and_subdirectory_carry_into_in_repo_entries(self):
        # Update re-fetches the stored URL, so an entry has to name the same
        # branch the catalog was read from, or it would install from another.
        result = catalog(
            [{"name": "wind", "source": "./plugins/wind"}],
            url=f"{HOME}/tree/dev/catalog",
            subdir="catalog",
        )

        assert result.entries[0].source_url == (
            f"{HOME}/tree/dev/catalog/plugins/wind"
        )
        assert result.url == f"{HOME}/tree/dev/catalog"

    def test_an_unpinned_catalog_links_to_its_repository(self):
        assert catalog([]).url == HOME

    def test_an_external_entry_installs_from_upstream_at_the_pinned_commit(self):
        entry = only(
            [
                {
                    "name": "private-equity",
                    "source": {
                        "source": "github",
                        "repo": "anthropics/financial-services",
                        "path": "plugins/private-equity",
                        "sha": SHA,
                    },
                }
            ]
        )

        assert entry.source_url == (
            "https://github.com/anthropics/financial-services/tree/"
            f"{SHA}/plugins/private-equity"
        )

    def test_a_direct_archive_catalog_deep_links_with_a_fragment(self):
        result = catalog(
            [{"name": "wind", "source": "./plugins/wind"}],
            url="https://example.com/catalog.tar.gz",
        )

        source_url = result.entries[0].source_url
        assert source_url == "https://example.com/catalog.tar.gz#subdir=plugins/wind"
        assert normalize_forge_url(source_url)[1] == "plugins/wind"
        # No repository to send a person to.
        assert result.url is None

    @pytest.mark.parametrize(
        "source",
        [
            None,
            "../outside",
            "http://example.com/plugin",
            {"source": "git", "url": "https://example.com/repo.git"},
            {"source": "npm", "package": "x"},
        ],
    )
    def test_an_entry_with_nothing_installable_is_skipped(self, source):
        assert catalog([{"name": "x", "source": source}]).entries == []


def _upstream(sha):
    return {
        "name": "quant",
        "source": {
            "source": "github",
            "repo": "acme/agents",
            "path": "plugins/quant",
            "sha": sha,
        },
    }


def _row(name, source_ref, content_hash=""):
    return {
        "name": name, "source_type": "git", "source_ref": source_ref,
        "content_hash": content_hash,
    }


class TestInstalledPlugin:
    def test_an_entry_is_the_plugin_installed_from_its_source_whatever_its_name(self):
        result = catalog(
            [
                {"name": "wind", "source": "./plugins/wind"},
                {"name": "tide", "source": "./plugins/tide"},
            ]
        )
        plugins = [
            # The chooser's spelling of the same folder, under a coerced name.
            {
                "name": "wind-2", "source_type": "git",
                "source_ref": f"{HOME}/tree/HEAD#subdir=plugins/wind",
            },
            # Same name as an entry, from somewhere else.
            {
                "name": "tide", "source_type": "git",
                "source_ref": "https://github.com/someone/tide",
            },
            {"name": "upload", "source_type": "zip", "source_ref": "wind.zip"},
        ]

        installed = installed_from(result, plugins)
        assert {name: i.plugin for name, i in installed.items()} == {"wind": "wind-2"}

    def test_a_pick_from_a_folder_link_is_the_entry_for_that_plugin(self):
        # The chooser writes its pick, a path from the repository root, onto
        # the folder the pasted link named. The pick is the plugin's place,
        # and the fetch lands on it too.
        result = catalog(
            [
                {"name": "wind", "source": "./plugins/wind"},
                {"name": "tide", "source": "./plugins/tide"},
            ],
            url=f"{HOME}/tree/main",
        )
        entry = result.entries[0].source_url
        folder = f"{HOME}/tree/main/plugins"
        stored = compose_subdir_url(folder, "plugins/wind")

        assert package_site(stored) == package_site(entry)
        assert normalize_forge_url(stored) == normalize_forge_url(entry)
        assert package_site(stored) != package_site(
            compose_subdir_url(folder, "plugins/tide")
        )
        assert {
            name: i.plugin
            for name, i in installed_from(result, [_row("wind", stored)]).items()
        } == {"wind": "wind"}

    def test_an_upstream_entry_the_catalog_re_pinned_is_still_the_plugin(self):
        # The catalog moved the entry's commit after the install: the same
        # package at a newer revision, not a different one to install again.
        old = "4236bb91f8395b0435f1d8b8baf9e8e4c69a8620"
        result = catalog([_upstream(SHA)])
        installed_at = "https://github.com/Acme/agents/tree/{}/plugins/quant"

        whole = "f" * 64  # the tree a whole install stamps
        behind = installed_from(result, [_row("quant", installed_at.format(old), whole)])
        level = installed_from(result, [_row("quant", installed_at.format(SHA), whole)])

        assert behind["quant"].plugin == "quant"
        assert behind["quant"].update_available is True
        assert level["quant"].update_available is False

    def test_an_upstream_entry_that_did_not_land_whole_is_out_of_date(self):
        # Its ref is the catalog's, but the install or update left a component
        # out, so it recorded no tree.
        result = catalog([_upstream(SHA)])
        at = f"https://github.com/Acme/agents/tree/{SHA}/plugins/quant"

        partial = installed_from(result, [_row("quant", at, "")])

        assert partial["quant"].update_available is True

    def test_an_in_repo_entry_is_out_of_date_when_its_tree_is_not_the_installed_one(self):
        files = {
            "marketplace.json": json.dumps(
                {"plugins": [{"name": "wind", "source": "./plugins/wind"}]}
            ).encode(),
            "plugins/wind/plugin.json": json.dumps(
                {"name": "wind", "version": "1.0.0"}
            ).encode(),
            "plugins/wind/skills/brief/SKILL.md": b"---\nname: brief\n---\nBody.\n",
            "plugins/other/plugin.json": b'{"name": "other"}',
        }
        result = parse_catalog(HOME, files, None)
        url = result.entries[0].source_url
        # What install records for the same entry: its own read of the
        # catalog archive, under the folder the entry's URL names.
        installed = validate_package(
            zip_package(files), subdir="plugins/wind", name_hint="wind"
        ).content_hash

        def update_available(content_hash):
            return installed_from(result, [_row("wind", url, content_hash)])[
                "wind"
            ].update_available

        assert update_available(installed) is False
        assert update_available("0" * 64) is True
        # A partial install recorded no tree yet.
        assert update_available("") is True

    def test_a_folder_in_a_bare_skill_entry_hashes_as_install_names_it(self):
        # A SKILL.md at the folder's root is named after the folder by
        # install, whatever the index calls the entry; hashing under the
        # index's name would call every such install out of date.
        files = {
            "marketplace.json": json.dumps(
                {"plugins": [{"name": "brief-notes", "source": "./skills/brief"}]}
            ).encode(),
            "skills/brief/SKILL.md": b"---\ndescription: Notes.\n---\nBody.\n",
        }
        result = parse_catalog(HOME, files, None)
        installed = validate_package(
            zip_package(files), subdir="skills/brief", name_hint="brief"
        ).content_hash

        assert result.hashes["brief-notes"] == installed

    def test_a_folder_of_skills_installs_beside_the_catalogs_plugins(self):
        # The listing reads an entry's folder on its own. Install read the
        # whole repository, met the other entry's plugin.json, and answered
        # Install with a chooser offering only that other plugin.
        files = {
            "marketplace.json": json.dumps(
                {
                    "plugins": [
                        {"name": "wind", "source": "./plugins/wind"},
                        {"name": "notes", "source": "./plugins/notes"},
                    ]
                }
            ).encode(),
            "plugins/wind/plugin.json": b'{"name": "wind"}',
            "plugins/wind/skills/brief/SKILL.md": b"---\nname: brief\n---\nBody.\n",
            "plugins/notes/dcf/SKILL.md": b"---\nname: dcf\n---\nBody.\n",
            "plugins/notes/comps/SKILL.md": b"---\nname: comps\n---\nBody.\n",
        }
        result = parse_catalog(HOME, files, None)
        package = validate_package(
            zip_package(files), subdir="plugins/notes", name_hint="notes"
        )

        assert (package.name, package.synthesized) == ("notes", True)
        assert {p.dir for p in package.skill_plans} == {"dcf", "comps"}
        assert result.hashes["notes"] == package.content_hash


_WIND = {
    "plugin.json": b'{"name": "wind"}',
    "skills/brief/SKILL.md": b"---\nname: brief\n---\nBody.\n",
}
_SCREENER = {"agents/screener.md": b"---\ndescription: Screens.\n---\nBody.\n"}
# What a build before plugin subagents hashed these trees to.
_WIND_EARLIER = "ef49b173355e5c95acf2cbab75f1befc2fdf9abd73f7a8ba1b450f8d8f35b34f"
_WIND_SCREENER_EARLIER = "f7e47f539ce392b77e1ba4f6c375928cfff7482103ea32a51053e5e8e84a7ce6"


def _earlier_hash(files):
    """The tree hash as a build before plugin subagents took it, which is
    what an install by it stored on the row."""
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(path.encode() + b"\x00" + len(files[path]).to_bytes(8, "big"))
        digest.update(files[path])
    return digest.hexdigest()


class TestAgentsReadMark:
    @pytest.mark.parametrize(
        ("tree", "earlier"),
        [
            pytest.param(_WIND, _WIND_EARLIER, id="no-agents"),
            # Files under agents/ that no subagent is read from.
            pytest.param(
                {
                    **_WIND,
                    "agents/README.txt": b"Notes.\n",
                    "agents/team/lead.md": b"---\ndescription: Nested.\n---\n",
                },
                "160c7afb75561eea2d0ce8e716a8b33e2529c56c647664141ee1e631eaa3067e",
                id="no-subagent-files",
            ),
        ],
    )
    def test_a_tree_without_a_subagent_hashes_as_before(self, tree, earlier):
        assert _tree_hash(tree) == earlier

    def test_a_tree_with_a_subagent_hashes_apart_from_its_earlier_reading(self):
        tree = {**_WIND, **_SCREENER}

        assert _earlier_hash(tree) == _WIND_SCREENER_EARLIER
        assert _tree_hash(tree) != _WIND_SCREENER_EARLIER

    @pytest.mark.parametrize(
        ("folder", "behind"),
        [
            pytest.param({**_WIND, **_SCREENER}, True, id="agents"),
            pytest.param(_WIND, False, id="no-agents"),
        ],
    )
    def test_an_earlier_install_of_a_tree_with_subagents_reads_as_out_of_date(
        self, folder, behind
    ):
        files = {
            "marketplace.json": json.dumps(
                {"plugins": [{"name": "wind", "source": "./plugins/wind"}]}
            ).encode(),
            **{f"plugins/wind/{path}": content for path, content in folder.items()},
        }
        result = parse_catalog(HOME, files, None)
        url = result.entries[0].source_url
        # Install reads the folder as the listing does, plugin.json adapted.
        earlier = _earlier_hash(select_plugin_files(folder, name_hint="wind").files)
        installed_now = validate_package(
            zip_package(files), subdir="plugins/wind", name_hint="wind"
        ).content_hash

        def update_available(content_hash):
            return installed_from(result, [_row("wind", url, content_hash)])[
                "wind"
            ].update_available

        assert update_available(earlier) is behind
        # What this build's install records is what the listing compares
        # with, so the Update it offers clears once taken.
        assert update_available(installed_now) is False

    def test_a_folder_of_skills_and_subagents_hashes_as_install_reads_it(self):
        files = {
            "marketplace.json": json.dumps(
                {"plugins": [{"name": "notes", "source": "./plugins/notes"}]}
            ).encode(),
            "plugins/notes/dcf/SKILL.md": b"---\nname: dcf\n---\nBody.\n",
            **{f"plugins/notes/{path}": content for path, content in _SCREENER.items()},
        }
        result = parse_catalog(HOME, files, None)
        package = validate_package(
            zip_package(files), subdir="plugins/notes", name_hint="notes"
        )

        assert package.synthesized and [p.name for p in package.agent_plans] == ["screener"]
        assert result.hashes["notes"] == package.content_hash


class TestFieldNormalization:
    def test_well_formed_fields_pass_through(self):
        entry = only(
            [
                {
                    "name": "wind",
                    "description": "Stocks and macro.",
                    "category": "data",
                    "license": "MIT",
                    "publisher": {
                        "name": "Wind",
                        "url": "https://www.wind.com.cn",
                        "official": True,
                    },
                    "homepage": "https://market.windalice.com/",
                    "keywords": ["wind", "a-share"],
                    "auth": "api-key",
                    "requires": {"summary": "Wind key (credits)"},
                    "source": "./plugins/wind",
                }
            ]
        )

        assert entry.model_dump(exclude={"source_url"}) == {
            "name": "wind",
            "description": "Stocks and macro.",
            "category": "data",
            "license": "MIT",
            "publisher": {
                "name": "Wind",
                "url": "https://www.wind.com.cn",
                "official": True,
            },
            "homepage": "https://market.windalice.com/",
            "icon_url": None,
            "keywords": ["wind", "a-share"],
            "auth": "api-key",
            "requires": "Wind key (credits)",
            "coverage": None,
            "contents": None,
            "plugin": None,
            "installed": False,
            "update_available": False,
        }

    def test_a_malformed_field_is_dropped_without_dropping_the_entry(self):
        entry = only(
            [
                {
                    "name": "wind",
                    "description": "x" * 600,
                    "license": "L" * 65,
                    "publisher": {
                        "name": "Wind",
                        "url": "http://wind.com.cn",
                        "official": "yes",
                    },
                    "homepage": "http://market.windalice.com/",
                    "keywords": [f"k{i}" for i in range(20)] + ["k1", 7],
                    "auth": "token",
                    "requires": "x" * 200,
                    "source": "./plugins/wind",
                }
            ]
        )

        assert len(entry.description) == 512
        assert entry.license is None
        assert entry.publisher.url is None
        # Only a literal true is a claim of officialness.
        assert entry.publisher.official is False
        assert entry.homepage is None
        assert entry.keywords == [f"k{i}" for i in range(12)]
        assert entry.auth is None
        assert len(entry.requires) == 160

    @pytest.mark.parametrize(
        "name", ["Wind", "wind data", "a--b", "-wind", "w" * 65, 42, None]
    )
    def test_a_name_the_installer_would_refuse_skips_the_entry(self, name):
        assert catalog([{"name": name, "source": "./plugins/x"}]).entries == []

    def test_the_first_entry_of_a_name_wins(self):
        entry = only(
            [
                {"name": "wind", "description": "first", "source": "./a"},
                {"name": "wind", "description": "second", "source": "./b"},
            ]
        )

        assert entry.description == "first"

    @pytest.mark.parametrize(
        ("icon", "site"),
        [
            ("wind.com.cn", "wind.com.cn"),
            ("Wind.com.cn", "wind.com.cn"),
            # An address names the image itself, for a brand whose site
            # serves no icon to a fetch.
            ("https://wind.com.cn/logo.png", "https://wind.com.cn/logo.png"),
            ("http://wind.com.cn/logo.png", None),
            ("https://127.0.0.1/logo.png", None),
            ("wind.com.cn/logo.png", None),
            ("wind.com.cn:8443", None),
            ("127.0.0.1", None),
            ("localhost", None),
            ("wind .com", None),
            ("data:image/png;base64,AAAA", None),
        ],
    )
    def test_an_icon_is_a_bare_domain_or_an_https_image(self, icon, site):
        result = catalog([{"name": "wind", "icon": icon, "source": "./w"}])

        assert result.icons.get("wind") == site

    @pytest.mark.parametrize(
        ("entry", "icon"),
        [
            (
                {"publisher": {"name": "Anthropic", "url": "https://github.com/anthropics/x"}},
                "https://github.com/anthropics.png?size=128",
            ),
            (
                {"publisher": {"name": "Org", "url": "https://github.com/orgs/acme"}},
                "https://github.com/acme.png?size=128",
            ),
            (
                {"homepage": "https://github.com/someone/skills"},
                "https://github.com/someone.png?size=128",
            ),
            # A site the entry names wins over the account.
            (
                {"icon": "acme.com", "publisher": {"name": "A", "url": "https://github.com/acme"}},
                "acme.com",
            ),
            ({"publisher": {"name": "A", "url": "https://acme.com"}}, None),
            ({"homepage": "https://github.com/sponsors/someone"}, None),
            ({"homepage": "https://github.com/"}, None),
        ],
    )
    def test_an_entry_without_a_site_falls_back_to_its_github_account(self, entry, icon):
        result = catalog([{"name": "skills", "source": "./s", **entry}])

        assert result.icons.get("skills") == icon

    def test_an_in_repo_entry_draws_the_file_its_own_manifest_vendors(self):
        mark = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + (256).to_bytes(4, "big") * 2
        files = {
            "marketplace.json": json.dumps(
                {
                    "plugins": [
                        {"name": "vendor", "icon": "vendor.com", "source": "./plugins/vendor"},
                        {"name": "other", "icon": "other.com", "source": "./plugins/other"},
                    ]
                }
            ).encode(),
            "plugins/vendor/plugin.json": json.dumps(
                {"name": "vendor", "extensions": {"ai.langalpha": {"icon": "./icon.png"}}}
            ).encode(),
            "plugins/vendor/icon.png": mark,
            "plugins/other/plugin.json": json.dumps(
                {"name": "other", "extensions": {"ai.langalpha": {"icon": "./gone.png"}}}
            ).encode(),
        }

        icons = parse_catalog(HOME, files, None).icons

        # The same bytes an install keeps, so Discover and the installed rows
        # cannot drift; a file that is not there leaves the index's icon.
        assert icons["vendor"].startswith("data:image/png;base64,")
        assert base64.b64decode(icons["vendor"].split(",", 1)[1]) == mark
        assert icons["other"] == "other.com"

    def test_an_in_repo_entry_lists_what_its_own_files_declare(self):
        files = {
            "marketplace.json": json.dumps(
                {
                    "plugins": [
                        {"name": "screeners", "requires": "nothing", "source": "./plugins/s"},
                        {
                            "name": "upstream",
                            "source": {"source": "github", "repo": "acme/up", "sha": SHA},
                        },
                    ]
                }
            ).encode(),
            "plugins/s/plugin.json": json.dumps(
                {
                    "name": "screeners",
                    "extensions": {
                        "ai.langalpha": {
                            "secrets": [{"name": "FMP_API_KEY", "label": "FMP key"}]
                        }
                    },
                }
            ).encode(),
            "plugins/s/skills/pead-screener/SKILL.md": b"---\nname: pead-screener\n---\n",
            "plugins/s/skills/notes.md": b"loose",
        }

        entries = {e.name: e for e in parse_catalog(HOME, files, None).entries}

        # Install refuses until a declared key has a value, so the listing
        # carries it whatever the index's own "requires" prose says.
        contents = entries["screeners"].contents
        assert [(k.name, k.label) for k in contents.keys] == [("FMP_API_KEY", "FMP key")]
        assert contents.skills == ["pead-screener"]
        assert contents.connectors == []
        # Its files are in another repository, which listing never fetches.
        assert entries["upstream"].contents is None

    def test_an_index_icon_can_be_a_file_in_the_catalog(self):
        mark = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + (256).to_bytes(4, "big") * 2
        files = {
            "marketplace.json": json.dumps(
                {
                    "plugins": [
                        {
                            "name": "skills",
                            "icon": "./icons/skills.png",
                            "source": {"source": "github", "repo": "acme/skills", "sha": SHA},
                        },
                        {"name": "escape", "icon": "./../x.png", "source": "./s"},
                    ]
                }
            ).encode(),
            "icons/skills.png": mark,
        }

        icons = parse_catalog(HOME, files, None).icons

        # An upstream entry's package cannot carry the catalog's file, so the
        # index is where it is named.
        assert base64.b64decode(icons["skills"].split(",", 1)[1]) == mark
        assert "escape" not in icons

    def test_a_catalog_without_an_index_is_fatal(self):
        with pytest.raises(PluginFatal):
            parse_catalog(HOME, {"README.md": b"hello"}, None)


def _deep(depth):
    return b"[" * depth + b"]" * depth


class TestDataThatBreaksAParser:
    """Past what a reader rule checks, the parser itself can give up on what a
    catalog or its packages hold. That costs the value or the entry it is in,
    never the listing every user sees."""

    def test_an_entry_that_breaks_a_parser_costs_only_itself(self):
        files = {
            "marketplace.json": json.dumps(
                {
                    "plugins": [
                        {"name": "wind", "source": "./plugins/wind"},
                        {"name": "groups", "source": "./plugins/groups"},
                        {"name": "nested", "source": "./plugins/nested"},
                    ]
                }
            ).encode(),
            "plugins/wind/plugin.json": b'{"name": "wind"}',
            "plugins/wind/skills/brief/SKILL.md": b"---\nname: brief\n---\nBody.\n",
            # Compiling a key's pattern recurses once per group.
            "plugins/groups/plugin.json": json.dumps(
                {
                    "name": "groups",
                    "extensions": {
                        "ai.langalpha": {
                            "secrets": [
                                {"name": "K", "label": "k", "regex": "(" * 3000 + ")" * 3000}
                            ]
                        }
                    },
                }
            ).encode(),
            "plugins/nested/plugin.json": b'{"name": "nested", "x": ' + _deep(50_000) + b"}",
        }

        entries = {e.name: e for e in parse_catalog(HOME, files, None).entries}

        assert entries["wind"].contents.skills == ["brief"]
        # Listed from the index's prose; installing one says why it cannot.
        assert entries["groups"].contents is None
        assert entries["nested"].contents is None
        with pytest.raises(PluginFatal) as refused:
            validate_package(zip_package(files), subdir="plugins/groups")
        assert [d.code for d in refused.value.diagnostics] == ["unreadable"]

    @pytest.mark.asyncio
    async def test_a_catalog_that_breaks_a_parser_is_remembered_as_broken(
        self, monkeypatch
    ):
        archive = zip_package({"marketplace.json": b'{"plugins": ' + _deep(50_000) + b"}"})
        fetched = []

        async def _fetch(url, listed=False):
            fetched.append(url)
            return archive, None

        class _Cache:
            def __init__(self):
                self.store = {}

            async def get(self, key):
                return self.store.get(key)

            async def set(self, key, value, ttl=None):
                self.store[key] = value

        cache = _Cache()
        monkeypatch.setattr(marketplace, "PLUGIN_MARKETPLACE_URL", HOME)
        monkeypatch.setattr(marketplace, "fetch_plugin_source", _fetch)
        monkeypatch.setattr(marketplace, "get_cache_client", lambda: cache)

        for _ in range(2):
            with pytest.raises(PluginFatal):
                await load_marketplace()
        assert len(fetched) == 1

    def test_a_string_no_response_can_encode_is_dropped(self):
        # A JSON escape can spell a lone surrogate. It decodes, it caches, and
        # then every response that carries it fails to encode.
        files = {
            "marketplace.json": (
                b'{"plugins": ['
                b'{"name": "wind", "description": "Stocks \\ud800",'
                b' "homepage": "https://wind.com.cn/\\ud800",'
                b' "icon": "https://wind.com.cn/\\ud800.png",'
                b' "source": "./plugins/wind"},'
                b'{"name": "moved", "source": "./plugins/\\ud800"},'
                b'{"name": "keyed", "source": "./plugins/keyed"}]}'
            ),
            "plugins/keyed/plugin.json": (
                b'{"name": "keyed", "extensions": {"ai.langalpha":'
                b' {"secrets": [{"name": "K", "label": "key \\ud800"}]}}}'
            ),
            "plugins/keyed/skills/brief/SKILL.md": b"---\nname: brief\n---\nBody.\n",
        }

        result = parse_catalog(HOME, files, None)

        result.model_dump_json()
        entries = {e.name: e for e in result.entries}
        assert (entries["wind"].description, entries["wind"].homepage) == ("", None)
        assert "wind" not in result.icons
        assert "moved" not in entries
        assert entries["keyed"].contents is None


class TestCoverage:
    COVERAGE = {
        "markets": {"equity": ["cn", "hk", "cn"], "fund": ["cn"], "mars": ["x"]},
        "data": ["prices", "fundamentals", "prices", "gossip"],
    }
    VOCABULARY = {
        "assets": {
            "fund": {"label": "Fund", "label_zh": "基金"},
            "equity": {"label": "Equity", "label_zh": "股票"},
        },
        "regions": {"cn": {"label": "China"}, "us": {"label": "US"}},
        "data": {"prices": {"label": "Prices"}, "fundamentals": {}},
    }

    def test_without_a_vocabulary_keys_are_kept_and_the_vocabulary_derived(self):
        result = catalog(
            [{"name": "wind", "coverage": self.COVERAGE, "source": "./w"}]
        )

        coverage = result.entries[0].coverage
        assert coverage.markets == {
            "equity": ["cn", "hk"],
            "fund": ["cn"],
            "mars": ["x"],
        }
        assert coverage.data == ["prices", "fundamentals", "gossip"]
        assert [t.key for t in result.vocabulary.assets] == ["equity", "fund", "mars"]
        assert [t.key for t in result.vocabulary.regions] == ["cn", "hk", "x"]
        assert all(t.label == t.key for t in result.vocabulary.data)

    def test_a_vocabulary_drops_unknown_keys_and_supplies_labels(self):
        result = catalog(
            [{"name": "wind", "coverage": self.COVERAGE, "source": "./w"}],
            vocabulary=self.VOCABULARY,
        )

        coverage = result.entries[0].coverage
        assert coverage.markets == {"equity": ["cn"], "fund": ["cn"]}
        assert coverage.data == ["prices", "fundamentals"]
        # File order is display order, used or not.
        assert [
            (t.key, t.label, t.label_zh) for t in result.vocabulary.assets
        ] == [("fund", "Fund", "基金"), ("equity", "Equity", "股票")]
        assert [t.key for t in result.vocabulary.regions] == ["cn", "us"]
        assert [(t.key, t.label) for t in result.vocabulary.data] == [
            ("prices", "Prices"),
            ("fundamentals", "fundamentals"),
        ]

    def test_a_section_the_vocabulary_omits_neither_filters_nor_labels(self):
        result = catalog(
            [{"name": "wind", "coverage": self.COVERAGE, "source": "./w"}],
            vocabulary={"assets": self.VOCABULARY["assets"]},
        )

        coverage = result.entries[0].coverage
        assert coverage.markets == {"equity": ["cn", "hk"], "fund": ["cn"]}
        assert coverage.data == ["prices", "fundamentals", "gossip"]
        assert [t.key for t in result.vocabulary.data] == [
            "prices",
            "fundamentals",
            "gossip",
        ]

    def test_coverage_with_nothing_left_is_absent(self):
        entry = only(
            [
                {
                    "name": "wind",
                    "coverage": {"markets": {"mars": ["x"]}, "data": ["gossip"]},
                    "source": "./w",
                }
            ],
            vocabulary=self.VOCABULARY,
        )

        assert entry.coverage is None


def _recording(hops):
    """A client that answers like GitHub: the API redirects to codeload."""

    @asynccontextmanager
    async def _client(target, max_bytes=None):
        async def _get(url, headers=None):
            hops.append((url, dict(headers or {})))
            if url.startswith("https://api.github.com/"):
                return MagicMock(
                    status_code=302,
                    headers={
                        "location": "https://codeload.github.com/acme/catalog/"
                        "legacy.tar.gz/HEAD?token=short-lived"
                    },
                )
            return MagicMock(status_code=200, content=b"archive", headers={})

        yield MagicMock(get=_get)

    return _client


def _serve(monkeypatch, listing):
    """Answer load_marketplace with ``listing``, as the cache would."""

    async def _load():
        return listing

    monkeypatch.setattr(marketplace, "load_marketplace", _load)


@pytest.fixture
def hops(monkeypatch):
    async def _pin(url):
        return url

    recorded: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(fetch, "pin_public_url", _pin)
    monkeypatch.setattr(fetch, "pinned_stream_client", _recording(recorded))
    monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_URL", HOME)
    monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_TOKEN", "ghp_marketplace")
    return recorded


class TestMarketplaceToken:
    @pytest.mark.asyncio
    async def test_the_marketplace_repo_is_fetched_through_the_api_once_authorized(
        self, hops
    ):
        body, subdir = await fetch_plugin_source(
            f"{HOME}/tree/HEAD/plugins/wind", listed=True
        )

        assert body == b"archive"
        assert subdir == "plugins/wind"
        (first_url, first_headers), (second_url, second_headers) = hops
        assert first_url == "https://api.github.com/repos/acme/catalog/tarball/HEAD"
        assert first_headers == {
            "Authorization": "Bearer ghp_marketplace",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        # GitHub's own short-lived grant rides the redirect; ours never does.
        assert second_url.startswith("https://codeload.github.com/")
        assert second_headers == {}

    @pytest.mark.asyncio
    async def test_the_match_ignores_case_and_a_git_suffix_and_keeps_the_ref(
        self, hops, monkeypatch
    ):
        monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_URL", f"{HOME}/tree/v2")

        await fetch_plugin_source(
            "https://github.com/ACME/Catalog.git/tree/v2", listed=True
        )

        assert hops[0][0] == "https://api.github.com/repos/ACME/Catalog/tarball/v2"
        assert "Authorization" in hops[0][1]

    @pytest.mark.asyncio
    async def test_a_pinned_catalogs_own_entries_still_carry_it(
        self, hops, monkeypatch
    ):
        # Its in-repo entries are composed onto its ref, and an update fetches
        # the URL the install stored or the entry's own.
        pinned = f"{HOME}/tree/release"
        monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_URL", pinned)
        listing = catalog([{"name": "wind", "source": "./plugins/wind"}], url=pinned)
        entry = listing.entries[0]
        _serve(monkeypatch, listing)

        await fetch_plugin_source(entry.source_url, listed=True)
        await resolve_update_source(entry.source_url, None)
        await resolve_update_source(entry.source_url, entry.source_url)

        authorized = [url for url, headers in hops if "Authorization" in headers]
        assert authorized == [
            "https://api.github.com/repos/acme/catalog/tarball/release"
        ] * 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("catalog_url", "url", "ref"),
        [
            (f"{HOME}/tree/release", f"{HOME}/tree/main/plugins/wind", "main"),
            (f"{HOME}/tree/release", f"{HOME}/tree/{SHA}", SHA),
            (f"{HOME}/tree/release", HOME, "HEAD"),
            (HOME, f"{HOME}/tree/main/plugins/wind", "main"),
        ],
    )
    async def test_another_ref_of_the_marketplace_repo_never_sees_it(
        self, hops, monkeypatch, catalog_url, url, ref
    ):
        # Anyone signed in can name a source URL, so the token reads the one
        # tree the catalog is configured for and no other branch or commit.
        monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_URL", catalog_url)

        await fetch_plugin_source(url, listed=True)

        assert hops == [
            (f"https://codeload.github.com/acme/catalog/tar.gz/{ref}", {})
        ]

    @pytest.mark.asyncio
    async def test_a_redirect_back_to_the_api_does_not_carry_it(self, hops, monkeypatch):
        seen = hops

        @asynccontextmanager
        async def _moved(target, max_bytes=None):
            async def _get(url, headers=None):
                seen.append((url, dict(headers or {})))
                if len(seen) == 1:
                    # A renamed repository answers the API with a 301 to
                    # another API path.
                    return MagicMock(
                        status_code=301,
                        headers={
                            "location": "https://api.github.com/repositories/1/"
                            "tarball/HEAD"
                        },
                    )
                return MagicMock(status_code=200, content=b"archive", headers={})

            yield MagicMock(get=_get)

        monkeypatch.setattr(fetch, "pinned_stream_client", _moved)

        await fetch_plugin_source(HOME, listed=True)

        assert "Authorization" in seen[0][1]
        assert seen[1][0].startswith("https://api.github.com/")
        assert seen[1][1] == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("url", "archive"),
        [
            (
                "https://github.com/acme/other/tree/HEAD/plugins/wind",
                "https://codeload.github.com/acme/other/tar.gz/HEAD",
            ),
            (
                "https://github.com/acme-evil/catalog",
                "https://codeload.github.com/acme-evil/catalog/tar.gz/HEAD",
            ),
            (
                "https://gitlab.com/acme/catalog",
                "https://gitlab.com/api/v4/projects/acme%2Fcatalog/repository/"
                "archive.tar.gz",
            ),
            (
                "https://github.com/acme/catalog/archive/main.tar.gz",
                "https://github.com/acme/catalog/archive/main.tar.gz",
            ),
        ],
    )
    async def test_any_other_source_never_sees_it(self, hops, url, archive):
        await fetch_plugin_source(url, listed=True)

        assert hops == [(archive, {})]

    @pytest.mark.asyncio
    async def test_without_a_token_the_marketplace_repo_takes_the_codeload_path(
        self, hops, monkeypatch
    ):
        monkeypatch.setattr(fetch, "PLUGIN_MARKETPLACE_TOKEN", "")

        await fetch_plugin_source(f"{HOME}/tree/HEAD/plugins/wind", listed=True)

        assert hops == [
            ("https://codeload.github.com/acme/catalog/tar.gz/HEAD", {})
        ]

    @pytest.mark.asyncio
    async def test_a_marketplace_on_another_forge_never_sends_it(
        self, hops, monkeypatch
    ):
        monkeypatch.setattr(
            fetch, "PLUGIN_MARKETPLACE_URL", "https://gitlab.com/acme/catalog"
        )

        await fetch_plugin_source("https://gitlab.com/acme/catalog", listed=True)
        await fetch_plugin_source(HOME, listed=True)

        assert all(headers == {} for _url, headers in hops)

    @pytest.mark.asyncio
    async def test_an_install_carries_it_only_for_a_folder_the_catalog_lists(
        self, hops, monkeypatch
    ):
        # The tarball is the whole repository, and anyone signed in can name a
        # folder in it: the catalog's own listing is what vouches for one.
        _serve(monkeypatch, catalog([{"name": "wind", "source": "./plugins/wind"}]))

        await resolve_url_source(f"{HOME}/tree/HEAD/plugins/wind", None)
        await resolve_url_source(HOME, "plugins/wind")
        listed = list(hops)
        hops.clear()
        await resolve_url_source(f"{HOME}/tree/HEAD/drafts/unlisted", None)
        await resolve_url_source(HOME, "drafts/unlisted")
        await resolve_url_source(HOME, None)

        assert [("Authorization" in h) for _u, h in listed] == [True, False] * 2
        assert hops == [
            ("https://codeload.github.com/acme/catalog/tar.gz/HEAD", {})
        ] * 3

    @pytest.mark.asyncio
    async def test_a_catalog_that_cannot_be_read_vouches_for_nothing(
        self, hops, monkeypatch
    ):
        async def _broken():
            raise PluginFatal("the plugin catalog could not be read")

        monkeypatch.setattr(marketplace, "load_marketplace", _broken)

        await resolve_url_source(f"{HOME}/tree/HEAD/plugins/wind", None)

        assert hops == [
            ("https://codeload.github.com/acme/catalog/tar.gz/HEAD", {})
        ]
