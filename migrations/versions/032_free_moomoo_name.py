"""Free the ``moomoo`` name for the brokerage connector shipped alongside it.

The pass ``030_free_brokerage_names`` did for ``robinhood`` and ``ibkr``, done
again for one more name. Its reasoning is unchanged and is not repeated here:
a brokerage's name IS its identity, every surface joins a row to the shipped
definition by name and then draws it wearing that vendor, and the write paths
that now refuse the name cannot repair a row that was already sitting on it.

What is worth saying is why this is a second migration rather than an edit to
the first. 030 already ran everywhere, so amending it would repair no database
that has one of these rows; and a migration is a record of what the schema
needed on the day it ran, which is exactly why 030 pinned its names as literals
instead of importing ``BROKERAGES``. Each brokerage added after it therefore
brings its own pass, on the same shape, for its own name only.

Revision ID: 032
Revises: 031
"""

from alembic import op


revision = "032"
down_revision = "031"
branch_labels = None
depends_on = None

# Pinned rather than imported, for the reason 030 gives: BROKERAGES is free to
# grow after this runs, and a later entry is that entry's migration to write.
#
# The address exemption is 030's and matters more here, not less. On every
# database where this feature already runs -- a dev stack, staging, anywhere the
# connector was connected before the reservation reached it -- the connector IS
# a user-tier row under exactly this name, and renaming it would break the thing
# being shipped. A row at the vendor's own host is the connector and stays; a
# plugin-owned row is never it, whatever it points at.
#
# "The vendor's own host" is asked as a host test rather than 030's textual
# prefix, because that is the question ``brokerage_for_url`` answers and the two
# have to agree: whatever this leaves under the name is drawn as that vendor by
# a page that joined on it. Parsing lowercases the host and drops the port, so
# ``HTTPS://MCP.MOOMOO.COM:443/mcp`` is the connector, while a prefix reads it as
# somebody else's server and renames it out from under a live connection. The
# trailing anchor is what keeps this a host test rather than a longer prefix: an
# address matches only where the host ends, so ``mcp.moomoo.com.evil.com`` is
# still somebody else.
_SHIPPED = r"""(VALUES
    ('moomoo', '^https?://mcp\.moomoo\.com(:[0-9]+)?([/?#]|$)')
) AS shipped(name, vendor_host)"""
_RESERVED = "('moomoo')"


def upgrade() -> None:
    # --- user tier -------------------------------------------------------
    # The scratch tables carry this revision in their names because ON COMMIT
    # DROP means "at the end of the transaction", and alembic runs the whole
    # upgrade in ONE transaction. A database coming from 029 or earlier still
    # holds 030's identically-named tables when this runs, so the bare names
    # abort every fresh install with "relation already exists". A migration
    # that borrows this shape needs its own suffix, not this one.
    #
    # The suffix is checked free against all three tables this rename writes a
    # name into, each of which has its own UNIQUE over it. The OAuth connection
    # is the one that can be holding a name nothing else holds, so it gets its
    # own check rather than riding on the server row's.
    op.execute(f"""
        CREATE TEMP TABLE mcp_user_renames_032 ON COMMIT DROP AS
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
            OR u.url !~* shipped.vendor_host
    """)

    op.execute("""
        UPDATE user_mcp_servers u SET name = r.new_name
          FROM mcp_user_renames_032 r
         WHERE u.user_id = r.user_id AND u.name = r.old_name
    """)
    # Moves with the row or the freed name inherits a live grant and the page
    # draws somebody else's server as a connected broker.
    op.execute("""
        UPDATE user_mcp_oauth_connections c SET server_name = r.new_name
          FROM mcp_user_renames_032 r
         WHERE c.user_id = r.user_id AND c.server_name = r.old_name
    """)
    op.execute("""
        UPDATE user_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_user_renames_032 r
         WHERE s.user_id = r.user_id AND s.server_name = r.old_name
    """)
    # source='user' rows are tombstones naming an inherited user row that is
    # switched off in this workspace. Left behind, one would switch off the
    # brokerage the user connects later, in a workspace they never chose it for.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_user_renames_032 r, workspaces ws
         WHERE w.workspace_id = ws.workspace_id
           AND ws.user_id = r.user_id
           AND w.source = 'user'
           AND w.name = r.old_name
    """)

    # --- workspace tier --------------------------------------------------
    # No address test: a brokerage connector only ever exists at the user tier,
    # so a workspace-local row under the name is always somebody's own server,
    # and always the one the resolver has already stopped running.
    op.execute(f"""
        CREATE TEMP TABLE mcp_workspace_renames_032 ON COMMIT DROP AS
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
          FROM mcp_workspace_renames_032 r
         WHERE w.workspace_mcp_server_id = r.workspace_mcp_server_id
    """)
    op.execute("""
        UPDATE workspace_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_workspace_renames_032 r
         WHERE s.workspace_id = r.workspace_id AND s.server_name = r.old_name
    """)

    # Every workspace whose effective set just changed, so a session holding the
    # old version re-resolves instead of running the renamed row under its old
    # name until something else happens to bump it.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.workspace_id IN (SELECT workspace_id FROM mcp_workspace_renames_032)
            OR ws.user_id IN (SELECT user_id FROM mcp_user_renames_032)
    """)


def downgrade() -> None:
    # Deliberately empty, as 030's is. A renamed row is indistinguishable from
    # one a user named that way, and restoring the reserved name would re-create
    # the collision this exists to clear, on a schema that still refuses it.
    pass
