"""Where an installed plugin's mark comes from.

What its author wrote decides, in order: the icon the manifest names for the
server, then for the plugin, then its homepage, then its author's link. The
source it was fetched from counts only as a GitHub account, so a catalog that
vendors a plugin never lends it the catalog's own avatar. A ``./`` icon is a
file in the package, kept inline at install because the files are not kept.
"""

import base64
import io
import json
import struct
import zipfile

import pytest

from src.server.services.plugins import validate_package
from src.server.services.plugins.icons import plugin_icon_source, vendored_icon
from src.server.services.plugins.package import MAX_INLINED_ICON_CHARS

CATALOG = "https://github.com/acme/catalog/tree/HEAD/plugins/vendor"


def avatar(login):
    return f"https://github.com/{login}.png?size=128"


@pytest.mark.parametrize(
    ("manifest", "source_ref", "expected"),
    [
        (
            {
                "extensions": {"ai.langalpha": {"icon": "vendor.com"}},
                "homepage": "https://docs.other.com",
            },
            CATALOG,
            "vendor.com",
        ),
        ({"homepage": "https://docs.vendor.com/mcp"}, CATALOG, "docs.vendor.com"),
        ({"homepage": "https://github.com/someone/skills"}, CATALOG, avatar("someone")),
        (
            {"author": {"name": "A", "url": "https://github.com/acme/catalog"}},
            None,
            avatar("acme"),
        ),
        ({}, "https://github.com/someone/skills/tree/main", avatar("someone")),
        # Not a site: refused rather than handed to the resolver as a URL.
        ({"extensions": {"ai.langalpha": {"icon": "http://10.0.0.1/x.png"}}}, None, None),
        ({"homepage": "http://127.0.0.1:8000"}, None, None),
        # Another git host's icon is the host's brand.
        ({}, "https://gitlab.com/someone/skills", None),
        ({}, "skills.zip", None),
        (None, CATALOG, None),
    ],
)
def test_an_installed_plugin_wears_the_mark_its_author_points_at(
    manifest, source_ref, expected
):
    assert plugin_icon_source(manifest, source_ref) == expected


def test_a_server_wears_its_own_icon_before_its_plugins():
    manifest = {
        "extensions": {
            "ai.langalpha": {
                "icon": "vendor.com",
                "servers": {
                    "posts": {"icon": "x.com"},
                    "scrape": {"icon": "https://cdn.example.com/logo.png"},
                },
            }
        }
    }

    assert plugin_icon_source(manifest, None, "posts") == "x.com"
    assert plugin_icon_source(manifest, None, "scrape") == "https://cdn.example.com/logo.png"
    assert plugin_icon_source(manifest, None, "other") == "vendor.com"
    assert plugin_icon_source(manifest, None) == "vendor.com"


@pytest.mark.parametrize(
    "icon",
    ["http://cdn.example.com/logo.png", "https://10.0.0.1/logo.png", "https://cdn.example.com/a b.png"],
)
def test_an_icon_address_must_be_https_on_a_named_host(icon):
    manifest = {"extensions": {"ai.langalpha": {"icon": icon}}}

    assert plugin_icon_source(manifest, None) is None


def test_a_vendored_plugin_wears_its_homepage_not_the_catalog():
    manifest = {
        "author": {"name": "Catalog", "url": "https://github.com/acme/catalog"},
        "homepage": "https://vendor.com",
    }

    assert plugin_icon_source(manifest, CATALOG) == "vendor.com"


def png(side: int, padding: int = 0) -> bytes:
    """A PNG header claiming ``side`` px square: the size check reads only that."""
    return (
        b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR"
        + struct.pack(">II", side, side) + b"\x08\x06\x00\x00\x00"
        + b"\x00" * padding
    )


def data_uri(data: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(data).decode()


class TestVendoredIcon:
    def test_a_file_in_the_package_is_read_relative_to_its_directory(self):
        files = {"plugins/vendor/icon.png": png(256)}

        assert vendored_icon("./icon.png", files, "plugins/vendor") == data_uri(png(256))

    @pytest.mark.parametrize(
        ("value", "files"),
        [
            # A site or an address is not a file, and is left to icon_value.
            ("vendor.com", {"vendor.com": png(256)}),
            ("icon.png", {"icon.png": png(256)}),
            ("./missing.png", {}),
            ("./../secret.png", {"secret.png": png(256)}),
            ("./icon.png", {"icon.png": b"<html>not an image</html>"}),
            ("./icon.png", {"icon.png": png(16)}),
            ("./icon.png", {"icon.png": png(256, padding=65 * 1024)}),
        ],
    )
    def test_anything_but_a_usable_image_in_the_package_is_none(self, value, files):
        assert vendored_icon(value, files) is None

    def test_an_installed_manifest_draws_its_inlined_mark_first(self):
        manifest = {
            "extensions": {"ai.langalpha": {"icon": data_uri(png(256))}},
            "homepage": "https://vendor.com",
        }

        assert plugin_icon_source(manifest, CATALOG) == data_uri(png(256))


def package(namespace: dict, members: dict[str, bytes]) -> bytes:
    manifest = {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": "vendor",
        "version": "1.0.0",
        "description": "d",
        "homepage": "https://vendor.com",
        "extensions": {"ai.langalpha": namespace},
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("plugin.json", json.dumps(manifest))
        zf.writestr("skills/look/SKILL.md", "---\nname: look\ndescription: d\n---\nbody\n")
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


class TestInstallKeepsTheFile:
    def test_install_inlines_the_plugin_and_server_marks(self):
        validated = validate_package(
            package(
                {"icon": "./icon.png", "servers": {"posts": {"icon": "./art/x.png"}}},
                {"icon.png": png(256), "art/x.png": png(128)},
            )
        )

        namespace = validated.manifest["extensions"]["ai.langalpha"]
        assert namespace["icon"] == data_uri(png(256))
        assert namespace["servers"]["posts"]["icon"] == data_uri(png(128))
        assert plugin_icon_source(validated.manifest, None, "posts") == data_uri(png(128))
        assert not [d for d in validated.diagnostics if d.code == "icon_unusable"]

    def test_an_unusable_file_warns_and_the_homepage_draws(self):
        validated = validate_package(
            package({"icon": "./icon.png"}, {"icon.png": png(16)})
        )

        assert "icon" not in validated.manifest["extensions"]["ai.langalpha"]
        assert [d.code for d in validated.diagnostics if d.code == "icon_unusable"] == [
            "icon_unusable"
        ]
        assert plugin_icon_source(validated.manifest, None) == "vendor.com"

    def test_one_small_mark_under_many_keys_cannot_inflate_the_manifest(self):
        mark = png(64, padding=8 * 1024)
        servers = {f"s{i}": {"icon": "./x.png"} for i in range(500)}
        validated = validate_package(package({"servers": servers}, {"x.png": mark}))

        kept = validated.manifest["extensions"]["ai.langalpha"]["servers"]
        inlined = [m["icon"] for m in kept.values() if "icon" in m]
        assert inlined and all(icon == data_uri(mark) for icon in inlined)
        assert sum(map(len, inlined)) <= MAX_INLINED_ICON_CHARS
        assert len(inlined) < len(servers)
        crowded = [d for d in validated.diagnostics if d.code == "icon_unusable"]
        assert len(crowded) == 1
        assert "more" in crowded[0].message
