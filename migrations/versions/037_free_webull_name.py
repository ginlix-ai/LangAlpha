"""Free the ``webull`` name for the connector shipped with it.

The pass ``030_free_brokerage_names`` did for ``robinhood`` and ``ibkr``, and
``032_free_moomoo_name`` did again for one more, done here for one more. The
reasoning is unchanged and is not repeated: a brokerage's name IS its identity,
every surface joins a row to the shipped definition by name and then draws it
wearing that vendor, and the write paths that now refuse the name cannot repair
a row that was already sitting on it.

Why a third migration rather than an edit to either: 030 and 032 have both run
everywhere, so amending one repairs no database that holds such a row, and a
migration records what the schema needed on the day it ran. That is why each of
them pinned its names as literals instead of importing ``BROKERAGES``, and why
each brokerage added afterwards brings its own pass for its own name only.

``webull`` is likelier to be already taken than the three before it: an ordinary
word a user could reasonably have named their own server, for a vendor that
published an MCP endpoint people were adding by hand well before there was a
connector to reserve the name. The rename is what keeps such a row working under
a name its owner can still edit.

Revision ID: 037
Revises: 036
"""

from alembic import op


revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None

# Pinned rather than imported, for the reason 030 gives: BROKERAGES is free to
# grow after this runs, and a later entry is that entry's migration to write.
#
# The address exemption is 030's, asked as 032's host test rather than 030's
# textual prefix because that is the question ``brokerage_for_url`` answers and
# the two have to agree: whatever this leaves under the name is drawn as that
# vendor by a page that joined on it. Parsing lowercases the host and drops the
# port, so ``HTTPS://API.WEBULL.COM:443/mcp`` is the connector, while a prefix
# reads it as somebody else's server and renames it out from under a live
# connection. The trailing anchor keeps it a host test rather than a longer
# prefix, so ``api.webull.com.evil.com`` is still somebody else.
_SHIPPED = r"""(VALUES
    ('webull', '^https?://api\.webull\.com(:[0-9]+)?([/?#]|$)')
) AS shipped(name, vendor_host)"""
_RESERVED = "('webull')"


def upgrade() -> None:
    # --- user tier -------------------------------------------------------
    # The scratch tables carry this revision in their names because ON COMMIT
    # DROP means "at the end of the transaction", and alembic runs the whole
    # upgrade in ONE transaction. A database coming from 029 or earlier still
    # holds 030's and 032's identically-shaped tables when this runs, so a bare
    # name aborts every fresh install with "relation already exists".
    #
    # The suffix is checked free against all three tables this rename writes a
    # name into, each of which has its own UNIQUE over it. The OAuth connection
    # is the one that can be holding a name nothing else holds, so it gets its
    # own check rather than riding on the server row's.
    op.execute(f"""
        CREATE TEMP TABLE mcp_user_renames_037 ON COMMIT DROP AS
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
          FROM mcp_user_renames_037 r
         WHERE u.user_id = r.user_id AND u.name = r.old_name
    """)
    # Moves with the row or the freed name inherits a live grant and the page
    # draws somebody else's server as a connected broker.
    op.execute("""
        UPDATE user_mcp_oauth_connections c SET server_name = r.new_name
          FROM mcp_user_renames_037 r
         WHERE c.user_id = r.user_id AND c.server_name = r.old_name
    """)
    op.execute("""
        UPDATE user_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_user_renames_037 r
         WHERE s.user_id = r.user_id AND s.server_name = r.old_name
    """)
    # source='user' rows are tombstones naming an inherited user row that is
    # switched off in this workspace. Left behind, one would switch off the
    # brokerage the user connects later, in a workspace they never chose it for.
    op.execute("""
        UPDATE workspace_mcp_servers w SET name = r.new_name
          FROM mcp_user_renames_037 r, workspaces ws
         WHERE w.workspace_id = ws.workspace_id
           AND ws.user_id = r.user_id
           AND w.source = 'user'
           AND w.name = r.old_name
    """)

    # --- consent for the rows this pass just vouched for ------------------
    # 036 backfills the three names 030 and 032 had already reserved, and could
    # not reach this one: until the rename above runs, a row called ``webull``
    # is as likely somebody's own server as the connector. Afterwards the ones
    # still holding the name are exactly the connector's -- the shipped row, and
    # the hand-added row at the vendor's own host that the address exemption
    # deliberately preserves, which is the shape this migration's docstring
    # calls the likeliest of the four.
    #
    # Without this they keep a NULL record, which every reader treats as consent
    # to nothing: all 71 curated tools refused, the row reporting "granted
    # nothing", and only a reconnect able to repair it. Same non-danger set and
    # same reasoning as 036's -- see the comment on its ``_BACKFILL``.
    op.execute("""
        UPDATE user_mcp_oauth_connections
           SET granted_capabilities =
               '["market_data","watchlists","account"]'::jsonb
         WHERE server_name = 'webull'
           AND granted_capabilities IS NULL
    """)

    # --- workspace tier --------------------------------------------------
    # No address test: a brokerage connector only ever exists at the user tier,
    # so a workspace-local row under the name is always somebody's own server,
    # and always the one the resolver has already stopped running.
    op.execute(f"""
        CREATE TEMP TABLE mcp_workspace_renames_037 ON COMMIT DROP AS
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
          FROM mcp_workspace_renames_037 r
         WHERE w.workspace_mcp_server_id = r.workspace_mcp_server_id
    """)
    op.execute("""
        UPDATE workspace_mcp_tool_schemas s SET server_name = r.new_name
          FROM mcp_workspace_renames_037 r
         WHERE s.workspace_id = r.workspace_id AND s.server_name = r.old_name
    """)

    # Every workspace whose effective set just changed, so a session holding the
    # old version re-resolves instead of running the renamed row under its old
    # name until something else happens to bump it.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.workspace_id IN (SELECT workspace_id FROM mcp_workspace_renames_037)
            OR ws.user_id IN (SELECT user_id FROM mcp_user_renames_037)
    """)


def downgrade() -> None:
    # Deliberately empty, as 030's and 032's are. A renamed row is
    # indistinguishable from one a user named that way, and restoring the
    # reserved name would re-create the collision this exists to clear, on a
    # schema that still refuses it.
    pass
