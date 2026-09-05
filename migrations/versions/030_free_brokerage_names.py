"""Rename rows that hold a shipped brokerage's name from before it was reserved.

The brokerage connectors are ordinary catalog rows whose name IS their identity:
the connection, the egress grant and the row the user ends up owning are all
keyed on it, and every surface joins a row to the shipped definition by name and
then draws it wearing that vendor. The write paths refuse the names from now on,
but a row that predates them cannot be repaired by a write-time rule -- it is
already there, and the same rule now refuses the rename that would fix it.

Left alone, such a row is not merely stale. At the workspace tier the resolver
skips it, so a server the user configured stops running with nothing on screen
to say why. At the user tier it is worse: the row keeps working but the
Brokerages tab presents it as the vendor, so the user's own address wears
Robinhood's name and label, and any OAuth connection sitting under that name
reads as a live broker connection.

So they are renamed rather than skipped or deleted: the row goes on doing what it
did, under a name its owner can see and edit, and the reserved name is free for
the connector. Everything keyed on the old name travels with it -- the OAuth
connection, the discovery cache, and the per-workspace tombstones that decide
where an inherited row is switched off.

A brokerage added after this migration needs its own pass; see brokerage_names().

Revision ID: 030
Revises: 029
"""

from alembic import op


revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None

# Pinned, not imported: a migration repairs the database as it was the day it
# ran, and BROKERAGES is free to grow afterwards.
#
# The address comes along because the name alone cannot tell a row that predates
# the reservation from the connector itself -- on any database where the feature
# has already run (a dev stack, a staging box) the connector IS a user-tier row
# under exactly this name, and renaming it would break the thing being shipped.
# A row pointing at the vendor's own host is the connector and is left alone;
# anything else under the name is somebody's own server and moves aside. Prefix
# rather than equality, matching how the app resolves a row to a vendor: the URL
# is the user's to edit once the row exists, and a sibling path on the vendor's
# host is still that vendor.
#
# Plugin-owned rows are the exception to that exemption, whatever they point at.
# The connector this migration protects is never owned by a plugin, so a
# plugin_id is proof the row is not it. Left in place, one at the vendor's own
# address would be adopted by the brokerage tab -- which joins by name -- and
# reach the OAuth start route directly, going around the plugin refusal that
# `set_brokerage_enabled` raises for exactly this row. It keeps working on the
# Connectors tab under the plugin that owns it, which is where it belongs. The
# rename is safe there because a plugin tracks its servers by
# `plugin_server_key`, not by name.
_SHIPPED = """(VALUES
    ('robinhood', 'https://agent.robinhood.com/'),
    ('ibkr', 'https://api.ibkr.com/')
) AS shipped(name, vendor_url)"""
_RESERVED = "('robinhood', 'ibkr')"


def upgrade() -> None:
    # --- user tier -------------------------------------------------------
    # The suffix is checked free against every table this rename writes a name
    # into, because each one has its own UNIQUE over it: the user row, the
    # workspace tombstone below (UNIQUE(workspace_id, name)), and the OAuth
    # connection (UNIQUE(user_id, server_name)). The connection is the one that
    # can be sitting there under a name nothing else holds -- disconnecting
    # revokes the row rather than dropping it, and deleting the catalog entry
    # leaves it alone -- so without its own check this migration moves a
    # connection onto one that already exists and aborts partway through.
    # The tool-schema tables need no check: every delete path drops their rows
    # in the same transaction as the server they belong to.
    #
    # The id-derived fallback is what all three fall back to, and it carries the
    # whole id rather than a prefix of it. This is the one branch with nothing
    # left to check the result against, so a prefix that happened to be taken
    # would abort the upgrade for every user on the box; the id itself never is,
    # and it never leaves the server to be copied into a name on purpose. The
    # full hex stays legal -- `robinhood_legacy_` plus 32 characters is 49, well
    # inside NAME_RE's 64 -- and is if anything even less likely than a prefix to
    # be mistaken for a name someone chose.
    #
    # Underscore rather than hyphen because the name has to stay a legal one:
    # NAME_RE (`^[A-Za-z_][A-Za-z0-9_]{0,63}$`, mirrored in the frontend schema
    # and again in the connect readback) admits no hyphen. A hyphenated rename
    # would leave a row that still runs but can never be edited again, which is
    # the opposite of what moving it aside is for.
    op.execute(f"""
        CREATE TEMP TABLE mcp_user_renames ON COMMIT DROP AS
        SELECT u.user_id,
               u.name AS old_name,
               CASE WHEN EXISTS (
                        SELECT 1 FROM user_mcp_servers x
                         WHERE x.user_id = u.user_id
                           AND x.name = u.name || '_legacy'
                    ) OR EXISTS (
                        SELECT 1
                          FROM workspace_mcp_servers w
                          JOIN workspaces ws USING (workspace_id)
                         WHERE ws.user_id = u.user_id
                           AND w.name = u.name || '_legacy'
                    ) OR EXISTS (
                        SELECT 1 FROM user_mcp_oauth_connections c
                         WHERE c.user_id = u.user_id
                           AND c.server_name = u.name || '_legacy'
                    )
                    THEN u.name || '_legacy_'
                         || replace(u.user_mcp_server_id::text, '-', '')
                    ELSE u.name || '_legacy'
               END AS new_name
          FROM user_mcp_servers u
          JOIN {_SHIPPED} ON shipped.name = u.name
         WHERE u.plugin_id IS NOT NULL
            OR u.url IS NULL
            OR u.url NOT LIKE shipped.vendor_url || '%'
    """)

    op.execute("""
        UPDATE user_mcp_servers u SET name = r.new_name
          FROM mcp_user_renames r
         WHERE u.user_id = r.user_id AND u.name = r.old_name
    """)
    # The connection is what makes the row look connected on the Plugins page,
    # so it has to move with it or the freed name inherits a live grant.
    op.execute("""
        UPDATE user_mcp_oauth_connections c SET server_name = r.new_name
          FROM mcp_user_renames r
         WHERE c.user_id = r.user_id AND c.server_name = r.old_name
    """)
    op.execute("""
        UPDATE user_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_user_renames r
         WHERE s.user_id = r.user_id AND s.server_name = r.old_name
    """)
    # source='user' rows are tombstones: they name an inherited user row that is
    # switched off in this workspace. Left behind, one would switch off the
    # brokerage the user connects later, in a workspace they never chose it for.
    # No address test here -- a tombstone carries no URL of its own, and it
    # follows whichever user row the map above decided to move.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_user_renames r, workspaces ws
         WHERE w.workspace_id = ws.workspace_id
           AND ws.user_id = r.user_id
           AND w.source = 'user'
           AND w.name = r.old_name
    """)

    # --- workspace tier --------------------------------------------------
    # Every one of them, with no address test: a brokerage connector only ever
    # exists at the user tier, so a workspace-local row under the name is always
    # somebody's own server and always the one the resolver has stopped running.
    op.execute(f"""
        CREATE TEMP TABLE mcp_workspace_renames ON COMMIT DROP AS
        SELECT w.workspace_mcp_server_id,
               w.workspace_id,
               w.name AS old_name,
               CASE WHEN EXISTS (
                        SELECT 1 FROM workspace_mcp_servers x
                         WHERE x.workspace_id = w.workspace_id
                           AND x.name = w.name || '_local'
                    )
                    THEN w.name || '_local_'
                         || replace(w.workspace_mcp_server_id::text, '-', '')
                    ELSE w.name || '_local'
               END AS new_name
          FROM workspace_mcp_servers w
         WHERE w.source = 'workspace' AND w.name IN {_RESERVED}
    """)

    # The name on the row wins over any name inside the config blob
    # (workspace_row_to_server_config), so the blob is left as it is.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_workspace_renames r
         WHERE w.workspace_mcp_server_id = r.workspace_mcp_server_id
    """)
    op.execute("""
        UPDATE workspace_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_workspace_renames r
         WHERE s.workspace_id = r.workspace_id AND s.server_name = r.old_name
    """)

    # Every workspace whose effective set just changed, so a session holding the
    # old version re-resolves instead of running the renamed row under its old
    # name until something else happens to bump it.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.workspace_id IN (SELECT workspace_id FROM mcp_workspace_renames)
            OR ws.user_id IN (SELECT user_id FROM mcp_user_renames)
    """)


def downgrade() -> None:
    # Deliberately empty. The renamed rows are indistinguishable from rows a
    # user named that way, and restoring the reserved name would re-create the
    # collision this migration exists to clear -- on a schema where the write
    # paths still refuse it.
    pass
