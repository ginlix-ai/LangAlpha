"""The Drive mount's daemon matches names by the server's key.

The daemon is stdlib only, so it carries its own copy of ``path_key``
(``livefs_runtime/drive_tree.py``); if the two drift, a name the server takes
for a stored file stops resolving to it on the computer, or a create the
server would merge goes through.
"""

import unicodedata

import pytest

from ptc_agent.core.sandbox.livefs_runtime import drive_protocol, drive_tree
from src.server.database.drive import keys
from src.server.services import drive

PATHS = [
    "notes/plan.md",
    "Notes/PLAN.md",
    "straße/STRASSE.txt",
    "ΣΊΣΥΦΟΣ/σίσυφος/ς",
    unicodedata.normalize("NFD", "café/naïve.md"),
    unicodedata.normalize("NFC", "café/naïve.md"),
    "ﬁle/ﬀ",
    "İstanbul/ǅ",
    "名前/ファイル.md",
    "Å/Å",  # angstrom sign and A with ring
    "a b/ c",
    "",
]


@pytest.mark.parametrize("path", PATHS)
def test_the_daemon_keys_a_path_as_the_server_does(path):
    assert drive_tree.path_key(path) == keys.path_key(path)


def test_names_the_server_takes_for_one_file_share_a_key():
    assert drive_tree.path_key("README.md") == drive_tree.path_key("readme.md")
    assert drive_tree.path_key(unicodedata.normalize("NFD", "café")) == drive_tree.path_key("café")
    assert drive_tree.path_key("a/b") != drive_tree.path_key("a/c")


def test_the_daemon_tells_text_by_the_servers_size_limit():
    """A file over the limit is binary to the server; the daemon, which
    cannot import it, judges a file from its size alone by its own copy."""
    assert drive_protocol.TEXT_MAX_BYTES == drive.TEXT_MAX_BYTES
