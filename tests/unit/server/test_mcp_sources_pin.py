"""``user_tier_rows`` is the only way to read the user's enabled servers.

The policy's ``user_connectors`` rule is applied there, so a second reader
of ``list_enabled_user_servers`` would put servers the policy withholds back
in a turn. Any mention of the name (an import or ``mcp_db.<name>``) counts.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[3] / "src"
NAME = "list_enabled_user_servers"
DEFINED_IN = "src/server/database/mcp_servers.py"
READER = "src/server/services/mcp_sources.py"


def _mentions(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == NAME:
            return True
        if isinstance(node, ast.Attribute) and node.attr == NAME:
            return True
        if isinstance(node, ast.ImportFrom) and any(a.name == NAME for a in node.names):
            return True
    return False


def test_only_mcp_sources_reads_the_users_enabled_servers():
    root = SRC.parent
    readers = {
        str(path.relative_to(root))
        for path in SRC.rglob("*.py")
        if _mentions(ast.parse(path.read_text(), filename=str(path)))
    }
    assert readers - {DEFINED_IN} == {READER}
