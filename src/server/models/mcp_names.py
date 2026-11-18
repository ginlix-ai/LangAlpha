"""What an MCP server's name and texts may be.

A leaf, so the account contract (``services.account_contract``) can check an
organization's connector by the same rules as the server form without
importing the server models, which reach the account through the services.
"""

from __future__ import annotations

import re
from typing import Optional

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}\Z")

# A server name is also the module its tool wrappers are generated into,
# imported from the sandbox's tools package beside the runtime's own
# ``mcp_client``. NAME_RE admits three kinds of name that cannot hold that
# role: the runtime's module, a dunder (``__init__`` is the package itself),
# and a hard keyword (``from tools.class import ...`` does not parse). Soft
# keywords stay legal, since ``from tools.match import ...`` parses. The
# keywords are Python 3.13's ``keyword.kwlist`` written out, so which names
# are refused does not move with the interpreter. The web form keeps a copy
# (``mcpSchemas.ts``).
RUNTIME_MODULE = "mcp_client"
PY_KEYWORDS = frozenset({
    "False", "None", "True", "and", "as", "assert", "async", "await", "break",
    "class", "continue", "def", "del", "elif", "else", "except", "finally",
    "for", "from", "global", "if", "import", "in", "is", "lambda", "nonlocal",
    "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
})


def sandbox_name_error(name: str) -> Optional[str]:
    """The refusal for a name the sandbox reserves, else ``None``.

    Checked where a name is introduced, not by ``McpServerInput``: a row saved
    before its name was reserved has to stay editable, since the only other
    way out is a delete that takes its OAuth connection, tool schemas,
    per-workspace switches and plugin ownership with it.
    """
    if name == RUNTIME_MODULE:
        return (
            f"name {name!r} is reserved: the sandbox's MCP runtime module "
            "already has it"
        )
    if name.startswith("__"):
        return (
            "name must not start with '__': a server name becomes a Python "
            "module in the sandbox, and those names are Python's own"
        )
    if name in PY_KEYWORDS:
        return (
            f"name {name!r} is a Python keyword, and a server name becomes a "
            "Python module in the sandbox"
        )
    return None


DESCRIPTION_MAX = 512
INSTRUCTION_MAX = 1024
