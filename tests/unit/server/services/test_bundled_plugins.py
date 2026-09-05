"""The bundles on disk, as the Plugins page and the icon route see them.

``icon_site_for`` is the reason this file exists. It answers an unauthenticated
route whose only input is a name, and it is safe because it matches that name
against the bundles actually on disk rather than joining it onto a path. That
property is stated in a docstring and enforced by one loop; a later refactor to
``BUNDLES_DIR / name`` would read as a simplification and would be a traversal.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config import plugins as bundle_reader
from src.server.services.plugins.bundled import icon_site_for, list_bundled


def _write(root: Path, name: str, *, manifest: dict, mcp: dict | None = None) -> Path:
    bundle = root / name
    bundle.mkdir(parents=True)
    (bundle / "plugin.json").write_text(json.dumps(manifest))
    if mcp is not None:
        (bundle / "mcp.json").write_text(json.dumps(mcp))
    return bundle


def _write_skill(bundle: Path, name: str) -> None:
    (bundle / "skills" / name).mkdir(parents=True, exist_ok=True)
    (bundle / "skills" / name / "SKILL.md").write_text("# skill")


def _manifest(name: str, **extension) -> dict:
    return {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": name,
        "description": f"the {name} bundle",
        "version": "1.2.3",
        "author": {"name": "LangAlpha"},
        "keywords": ["finance", 7],
        "extensions": {"ai.langalpha": extension} if extension else {},
    }


@pytest.fixture
def bundles_dir(monkeypatch, tmp_path):
    root = tmp_path / "plugins"
    root.mkdir()
    monkeypatch.setattr(bundle_reader, "BUNDLES_DIR", root)
    return root


class TestIconSiteLookup:
    def test_a_declared_site_comes_back(self, bundles_dir):
        _write(bundles_dir, "yfinance", manifest=_manifest("yfinance", icon="finance.yahoo.com"))
        assert icon_site_for("yfinance") == "finance.yahoo.com"

    def test_a_bundle_that_declares_none_answers_none(self, bundles_dir):
        _write(bundles_dir, "ours", manifest=_manifest("ours"))
        assert icon_site_for("ours") is None

    def test_a_blank_site_is_not_a_site(self, bundles_dir):
        _write(bundles_dir, "ours", manifest=_manifest("ours", icon="   "))
        assert icon_site_for("ours") is None

    def test_an_unknown_name_answers_none(self, bundles_dir):
        _write(bundles_dir, "ours", manifest=_manifest("ours", icon="example.test"))
        assert icon_site_for("nope") is None

    @pytest.mark.parametrize(
        "name",
        ["..", "../outside", "../../outside", "./outside", "/etc", "outside/.."],
    )
    def test_a_traversal_never_reaches_a_manifest(self, bundles_dir, tmp_path, name):
        # A real bundle sitting one level up, so a path join would find it and
        # the test fails loudly instead of passing on an absent file.
        _write(tmp_path, "outside", manifest=_manifest("outside", icon="attacker.test"))
        _write(bundles_dir, "ours", manifest=_manifest("ours", icon="example.test"))
        assert icon_site_for(name) is None

    def test_the_manifest_name_is_the_key_not_the_directory(self, bundles_dir):
        # The name the route is reached with is the one the page put in the
        # href, and the page builds that from the manifest. Keying on the
        # directory instead served nothing at the only URL that is ever
        # requested, and matched a name no caller has.
        _write(
            bundles_dir, "dir-name",
            manifest=_manifest("other-name", icon="example.test"),
        )
        (info,) = list_bundled()
        assert info.icon_url == "/api/v1/plugins/other-name/icon"
        assert icon_site_for("other-name") == "example.test"
        assert icon_site_for("dir-name") is None


class TestListing:
    def test_a_bundle_reads_as_a_plugin(self, bundles_dir):
        bundle = _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        _write_skill(bundle, "dcf-model")
        (info,) = list_bundled()
        assert (info.name, info.version, info.source_type) == (
            "market", "1.2.3", "bundled",
        )
        assert info.author == "LangAlpha"
        assert info.enabled is True
        assert info.source_ref is None
        # Non-string keywords are dropped rather than failing the whole row.
        assert info.keywords == ["finance"]
        assert [(c.kind, c.name) for c in info.components] == [
            ("mcp", "price"), ("skill", "dcf-model"),
        ]

    def test_the_icon_url_appears_only_when_a_site_is_declared(self, bundles_dir):
        _write(bundles_dir, "ours", manifest=_manifest("ours"))
        _write(bundles_dir, "wrapper", manifest=_manifest("wrapper", icon="example.test"))
        urls = {i.name: i.icon_url for i in list_bundled()}
        assert urls == {"ours": None, "wrapper": "/api/v1/plugins/wrapper/icon"}

    def test_the_skill_directories_are_the_skills(self, bundles_dir):
        # A manifest claim is not one of them. The directory is the whole
        # answer, so a bundle cannot list a skill whose files it does not
        # carry, and a directory without a SKILL.md is not a skill.
        #
        # Asked of ownership rather than of the listing: a disable subtracts
        # everything the bundle brought, so this reader is the unfiltered one.
        # What the detail view advertises is a narrower question, and
        # TestAComponentLinkAlwaysLandsSomewhere holds that end.
        from src.server.services.plugins.bundled import component_owners

        bundle = _write(bundles_dir, "ours", manifest=_manifest("ours", skills=["claimed"]))
        _write_skill(bundle, "beta")
        _write_skill(bundle, "alpha")
        (bundle / "skills" / "no-skill-md").mkdir()
        assert component_owners().skills == {"alpha": "ours", "beta": "ours"}

    def test_a_nameless_manifest_is_skipped_not_fatal(self, bundles_dir):
        _write(bundles_dir, "broken", manifest={"description": "no name"})
        _write(bundles_dir, "ours", manifest=_manifest("ours"))
        assert [i.name for i in list_bundled()] == ["ours"]

    def test_an_unparseable_manifest_is_skipped_not_fatal(self, bundles_dir):
        broken = _write(bundles_dir, "broken", manifest=_manifest("broken"))
        (broken / "plugin.json").write_text("{ not json")
        _write(bundles_dir, "ours", manifest=_manifest("ours"))
        assert [i.name for i in list_bundled()] == ["ours"]

    @pytest.mark.parametrize(
        "bad",
        [
            {"keywords": 7},
            {"version": 7},
            {"homepage": [1, 2]},
            {"author": {"name": ["not", "a", "name"]}},
            {"description": {"long": "form"}},
        ],
    )
    def test_a_wrongly_typed_field_costs_the_field_not_the_row(
        self, bundles_dir, bad
    ):
        # A shipped plugin.json is read straight off disk -- an uploaded one is
        # schema-validated at install, this one never is. So the listing has to
        # survive a hand-edit that put the wrong type in a field. Only the name
        # is load-bearing: everything else here is decoration, and dropping the
        # decoration beats dropping the package the reader came to find.
        _write(bundles_dir, "broken", manifest={**_manifest("broken"), **bad})
        _write(bundles_dir, "ours", manifest=_manifest("ours"))
        assert {i.name for i in list_bundled()} == {"broken", "ours"}

    def test_a_missing_directory_is_survivable(self, monkeypatch, tmp_path):
        monkeypatch.setattr(bundle_reader, "BUNDLES_DIR", tmp_path / "gone")
        assert list_bundled() == []
        assert icon_site_for("anything") is None


class TestTheNameIsReservedAgainstInstalls:
    """A shipped name cannot also be an installed plugin's name.

    Nothing else stops it: a bundle has no row, so install's duplicate check
    looks in ``user_plugins`` and finds nothing. Both would then answer to the
    same name on every route that addresses a package by one, and the enable
    toggle would reach only whichever the endpoint tried first.
    """

    @pytest.mark.asyncio
    async def test_installing_over_a_shipped_name_is_refused(self, bundles_dir):
        from src.server.services.plugins.lifecycle import install_plugin_package

        _write(bundles_dir, "yfinance", manifest=_manifest("yfinance"))
        package = SimpleNamespace(name="yfinance")
        # No DB patched on purpose: the refusal has to come before the first
        # read, so a package that can never be installed never opens a
        # connection or takes the per-user advisory lock.
        with pytest.raises(ValueError, match="already ships"):
            await install_plugin_package(
                "u-1", package, source_type="zip", source_ref=None
            )

    @pytest.mark.asyncio
    async def test_a_free_name_reaches_the_duplicate_check(self, bundles_dir):
        from src.server.services.plugins import lifecycle

        _write(bundles_dir, "yfinance", manifest=_manifest("yfinance"))
        package = SimpleNamespace(name="not-shipped")
        with patch.object(
            lifecycle, "get_plugin", AsyncMock(return_value={"name": "not-shipped"})
        ):
            with pytest.raises(ValueError, match="already installed"):
                await lifecycle.install_plugin_package(
                    "u-1", package, source_type="zip", source_ref=None
                )


class TestTheShippedBundlesAreIntact:
    """The real ``plugins/`` directory, with no fixture in front of it.

    Every other class here builds its bundles in ``tmp_path``, so the tree we
    actually ship is only ever read at runtime. It fails quietly in both
    directions that matter. A ``plugin.json`` that stops parsing costs the
    package its row on the Plugins page while its servers keep loading from
    ``mcp.json`` -- the user sees a shorter list, not an error -- and it drops
    out of ``component_owners``, so its skills and servers lose the attribution
    the per-package switch is keyed on. A name claimed by two bundles keeps the
    first and logs the second. Both are one hand-edit away and neither is
    reachable from a fixture.
    """

    def test_every_bundle_on_disk_reaches_the_page(self):
        listed = {info.name for info in list_bundled()}
        on_disk = {b.name for b in bundle_reader.bundles()}
        assert len(listed) == len(on_disk), (
            f"{len(on_disk) - len(listed)} bundle(s) missing from the listing; "
            f"on disk {sorted(on_disk)}, listed {sorted(listed)}"
        )

    def test_no_two_bundles_claim_the_same_component(self):
        from src.server.services.plugins.bundled import component_owners

        claimed_servers, claimed_skills = [], []
        for bundle in bundle_reader.bundles():
            claimed_servers += list(bundle.servers)
            skills = bundle.path / "skills"
            if skills.is_dir():
                claimed_skills += [
                    d.name for d in skills.iterdir() if (d / "SKILL.md").is_file()
                ]

        owners = component_owners()
        assert len(owners.servers) == len(claimed_servers), (
            f"two bundles claim a server: {sorted(claimed_servers)}"
        )
        assert len(owners.skills) == len(claimed_skills), (
            f"two bundles claim a skill: {sorted(claimed_skills)}"
        )


class TestBothProjectionsAnswerTheSameQuestions:
    """One panel draws an installed package and a shipped one.

    ``PluginInfo`` is filled by two functions that share no code -- one reads a
    manifest off disk, the other reads the copy stored with the row -- and
    ``PluginDetail`` renders whichever it is handed. A field only one of them
    copies does not read as a gap in the code; it reads as a package that
    declared less than it did.
    """

    _MANIFEST = {
        "name": "same",
        "description": "one description",
        "version": "1.2.3",
        "author": {"name": "LangAlpha"},
        "homepage": "https://home.test",
        "repository": "https://repo.test",
        "license": "MIT",
        "keywords": ["finance"],
    }

    def test_the_same_manifest_projects_the_same_metadata(self, bundles_dir):
        from src.server.models.plugin import plugin_row_to_info

        _write(bundles_dir, "same", manifest=self._MANIFEST)
        (shipped,) = list_bundled()
        installed = plugin_row_to_info(
            {
                "name": "same",
                "version": "1.2.3",
                "manifest": self._MANIFEST,
                "source_type": "zip",
                "source_ref": None,
                "enabled": True,
            }
        )
        described = (
            "description", "version", "author", "homepage",
            "repository", "license", "keywords",
        )
        assert {f: getattr(installed, f) for f in described} == {
            f: getattr(shipped, f) for f in described
        }


class TestAnUnreadableBundleCannotSilentlyReEnableItself:
    """A disable that cannot be enforced has to say so, not answer "nothing".

    ``bundles()`` re-reads disk on every call while the MCP server set is
    frozen at startup, so the two can disagree: a manifest that stops reading
    after the process came up leaves an empty server map, and an empty map
    subtracts nothing. The user's switched-off server becomes callable again
    and the only trace is a log line. A partial write does it, and so does a
    transient OSError.
    """

    def _owners(self, bundles_dir):
        from src.server.services.plugins.bundled import component_owners

        return component_owners()

    def test_a_healthy_bundle_answers_for_its_own_components(self, bundles_dir):
        _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        servers, _ = self._owners(bundles_dir).owned_by({"market"})
        assert servers == frozenset({"price"})

    @pytest.mark.parametrize("broken", ["mcp.json", "plugin.json"])
    def test_an_unreadable_manifest_refuses_instead_of_answering_empty(
        self, bundles_dir, broken
    ):
        from src.server.services.plugins.bundled import BundleOwnershipUnavailable

        bundle = _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        (bundle / broken).write_text("{ not json")
        with pytest.raises(BundleOwnershipUnavailable):
            self._owners(bundles_dir).owned_by({"market"})

    def test_a_missing_mcp_json_is_not_a_read_failure(self, bundles_dir):
        # The ordinary skills-only bundle. Absent is an answer; unreadable is
        # not, and conflating them is what this whole class is about.
        bundle = _write(bundles_dir, "research", manifest=_manifest("research"))
        _write_skill(bundle, "dcf-model")
        _, skills = self._owners(bundles_dir).owned_by({"research"})
        assert skills == frozenset({"dcf-model"})

    def test_one_broken_bundle_does_not_block_a_disable_on_another(
        self, bundles_dir
    ):
        broken = _write(
            bundles_dir, "broken", manifest=_manifest("broken"),
            mcp={"mcpServers": {"x": {"type": "stdio", "command": "uv"}}},
        )
        (broken / "mcp.json").write_text("{ not json")
        _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        servers, _ = self._owners(bundles_dir).owned_by({"market"})
        assert servers == frozenset({"price"})

    def test_a_bundle_with_no_name_at_all_blocks_nothing(self, bundles_dir):
        # No plugin.json means no name, which means no page row and no disable
        # anyone could have written. That is not the anonymous case.
        (bundles_dir / "nameless").mkdir()
        _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        servers, _ = self._owners(bundles_dir).owned_by({"market"})
        assert servers == frozenset({"price"})


class TestEnforcementReadsTheBootSnapshot:
    """A withdrawal answers for the servers this process is actually running.

    ``component_owners`` re-reads disk on every call so the Plugins page shows
    an edited manifest at once, but the MCP server set was composed once at
    startup. A bundle that stops being readable as itself afterwards -- the
    directory removed, the whole plugin renamed -- leaves the live map unable
    to name it while its server is still being launched, so a disable keyed on
    that name subtracts nothing and the switched-off server comes back. The
    unreadable-manifest guard does not catch this one: a bundle that is simply
    gone is absent from ``unreadable`` too.
    """

    def test_a_bundle_removed_after_boot_still_enforces_its_disable(
        self, bundles_dir, monkeypatch
    ):
        from src.server.app import setup
        from src.server.services.plugins.bundled import (
            component_owners,
            enforcement_owners,
        )

        bundle = _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        monkeypatch.setattr(setup, "bundle_owners", component_owners())

        for child in sorted(bundle.iterdir()):
            child.unlink()
        bundle.rmdir()

        assert component_owners().owned_by({"market"}) == (
            frozenset(),
            frozenset(),
        )
        servers, _ = enforcement_owners().owned_by({"market"})
        assert servers == frozenset({"price"})

    def test_without_a_snapshot_it_reads_disk(self, bundles_dir, monkeypatch):
        """The CLI and the tests never run lifespan; both still get an answer."""
        from src.server.app import setup
        from src.server.services.plugins.bundled import enforcement_owners

        monkeypatch.setattr(setup, "bundle_owners", None)
        _write(
            bundles_dir, "market", manifest=_manifest("market"),
            mcp={"mcpServers": {"price": {"type": "stdio", "command": "uv"}}},
        )
        servers, _ = enforcement_owners().owned_by({"market"})
        assert servers == frozenset({"price"})


class TestASkillDirectoryThatWillNotOpen:
    """An unenumerable ``skills/`` is unreadable, not empty.

    ``component_owners`` runs during lifespan, so an ``OSError`` out of
    ``iterdir`` -- a directory the process cannot read, or one swapped out
    underneath it -- used to abort startup instead of costing one bundle. The
    quieter half is worse: answering "no skills" would let a disable keyed on
    that bundle subtract nothing and hand the skills back switched on.
    """

    def test_one_unreadable_directory_does_not_take_down_the_read(
        self, bundles_dir, monkeypatch
    ):
        from src.server.services.plugins.bundled import component_owners

        _write(bundles_dir, "market", manifest=_manifest("market"))
        _write_skill(bundles_dir / "market", "price-check")
        _write(bundles_dir, "research", manifest=_manifest("research"))
        _write_skill(bundles_dir / "research", "deep-dive")

        blocked = bundles_dir / "market" / "skills"
        real_iterdir = Path.iterdir

        def _iterdir(self):
            if self == blocked:
                raise PermissionError(13, "Permission denied")
            return real_iterdir(self)

        monkeypatch.setattr(Path, "iterdir", _iterdir)
        owners = component_owners()

        assert owners.skills == {"deep-dive": "research"}
        assert "market" in owners.unreadable

    def test_the_bundle_it_belongs_to_can_no_longer_be_enforced(
        self, bundles_dir, monkeypatch
    ):
        from src.server.services.plugins.bundled import (
            BundleOwnershipUnavailable,
            component_owners,
        )

        _write(bundles_dir, "market", manifest=_manifest("market"))
        _write_skill(bundles_dir / "market", "price-check")

        blocked = bundles_dir / "market" / "skills"
        real_iterdir = Path.iterdir

        def _iterdir(self):
            if self == blocked:
                raise PermissionError(13, "Permission denied")
            return real_iterdir(self)

        monkeypatch.setattr(Path, "iterdir", _iterdir)

        with pytest.raises(BundleOwnershipUnavailable):
            component_owners().owned_by({"market"})


class TestAComponentLinkAlwaysLandsSomewhere:
    """Every ref in the Components section is a link into another tab.

    Two directories would be a name with no row behind it: one the registry
    never publishes (shipped, but nothing loads it) and one it marks hidden,
    which is activated programmatically and never listed. The shipped set has
    one of each today -- ``alternative-data``'s ``x-api`` and
    ``langalpha-service``'s ``onboarding`` -- which is how the dead click got
    here, and why the filter asks the listing rather than the registry.
    """

    def test_a_skill_the_listing_does_not_carry_is_not_advertised(
        self, bundles_dir
    ):
        from src.server.services.plugins.bundled import list_bundled

        _write(bundles_dir, "alt", manifest=_manifest("alt"))
        _write_skill(bundles_dir / "alt", "pdf")
        _write_skill(bundles_dir / "alt", "not-in-the-registry")
        # In the registry, but hidden: activated programmatically, never a row.
        _write_skill(bundles_dir / "alt", "onboarding")

        listed = {b.name: b for b in list_bundled()}["alt"]
        names = {c.name for c in listed.components if c.kind == "skill"}

        assert names == {"pdf"}

    def test_ownership_still_counts_what_is_on_disk(self, bundles_dir):
        """A disable subtracts everything the bundle brought, listed or not."""
        from src.server.services.plugins.bundled import component_owners

        _write(bundles_dir, "alt", manifest=_manifest("alt"))
        _write_skill(bundles_dir / "alt", "not-in-the-registry")

        assert component_owners().skills == {"not-in-the-registry": "alt"}
