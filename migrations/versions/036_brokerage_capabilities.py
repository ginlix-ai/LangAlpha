"""Per-connection capability consent for brokerage connectors.

Two columns and a constraint. ``granted_capabilities`` records what the user
agreed a connection may do, as group keys rather than tool names: intent
outlives a vendor's tool list, and the expansion from keys to names is done
from source at grant-sync time, so a curation change ships with a deploy and
needs no data migration.

``policy_required`` exists because ``tool_allowlist IS NULL`` already means "no
policy, allow every tool" for a user's own OAuth server, and that reading has to
keep working. Once the allowlist is what holds the trading line, a derivation
bug that yields NULL would silently grant what the user declined. The flag plus
its CHECK turn that into a failed connect, which someone can see and retry,
instead of an open door discovered by a filled order.

Revision ID: 036
Revises: 035
"""

from alembic import op


revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None

# Pinned rather than imported, the reason 030 and 032 both give: a migration
# records what the schema needed the day it ran, and both BROKERAGES and the
# curation map are free to grow afterwards. A brokerage added later brings its
# own pass; a group added later is a code change with no backfill to do, because
# an existing connection's stored keys stay exactly what its user agreed to.
# Renaming one is the exception that does need a pass of its own: a stored key
# the map no longer names expands to no tools at all, so it reads as a
# connection that may do nothing rather than as the group it used to be.
#
# ``webull`` is absent because its name is not yet its own here -- 037 is what
# frees it, and only the rows that survive that pass are the connector's. Its
# backfill therefore runs there, immediately after the rename, for the same
# reason each name is only vouched for by the migration that reserved it.
#
# What each row gets is every group the vendor has EXCEPT the one that places
# real orders, which matches what the consent dialog itself offers as its
# default. Nobody was asked about these connections, so what is written here is
# not consent and must not be able to masquerade as it: the page renders a
# stored key as a choice the user made, and "you agreed to let an agent place
# live orders" is the one sentence this whole change exists to stop us putting
# in their mouth without asking. The earlier draft granted every group to
# preserve behaviour exactly, which was defensible while there was no consent
# surface and is not now that there is one.
#
# The cost is real and is the smaller one: someone who had the agent placing
# orders finds it declined until they reconnect and say so, once, on a screen
# that tells them what they are agreeing to.
_BACKFILL = """(VALUES
    ('robinhood', '["market_data","watchlists","scanners","account","order_preview"]'),
    ('ibkr',      '["market_data","watchlists","alerts","account","staged_orders"]'),
    ('moomoo',    '["market_data","watchlists","account","paper_trading"]')
) AS shipped(name, caps)"""


def upgrade() -> None:
    op.execute("""
        ALTER TABLE user_mcp_oauth_connections
          ADD COLUMN granted_capabilities JSONB
    """)

    # Nullable on purpose, and the three states are all meaningful: NULL is a
    # connection we curate no groups for (a user's own OAuth server), '[]' is a
    # brokerage granted nothing, and a populated array is a real choice. Folding
    # the first two together would make "not a brokerage" and "declined
    # everything" the same row, and only one of them may reach the vendor.
    op.execute(f"""
        UPDATE user_mcp_oauth_connections c
           SET granted_capabilities = shipped.caps::jsonb
          FROM {_BACKFILL}
         WHERE c.server_name = shipped.name
           AND c.granted_capabilities IS NULL
    """)

    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD COLUMN policy_required BOOLEAN NOT NULL DEFAULT false
    """)
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD CONSTRAINT sandbox_egress_grants_policy_present
          CHECK (NOT (policy_required AND tool_allowlist IS NULL))
    """)

    # Existing brokerage grants keep policy_required false and a NULL allowlist
    # until the next resolve rewrites them, so this migration leaves no grant
    # the CHECK would refuse. Bumping the version is what makes that next
    # resolve happen, rather than the old grant persisting until something
    # unrelated invalidates the workspace -- and it is also what makes the
    # backfill above bite, since a stored key nothing re-reads narrows nothing.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (
                   SELECT c.user_id
                     FROM user_mcp_oauth_connections c
                    WHERE c.granted_capabilities IS NOT NULL
               )
    """)


def downgrade() -> None:
    # Reversible, unlike 030's and 032's renames: dropping these columns puts
    # every grant back to "no policy", which is what the code before this
    # migration already meant by a NULL allowlist.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          DROP CONSTRAINT IF EXISTS sandbox_egress_grants_policy_present
    """)
    op.execute("ALTER TABLE sandbox_egress_grants DROP COLUMN policy_required")
    op.execute(
        "ALTER TABLE user_mcp_oauth_connections DROP COLUMN granted_capabilities"
    )
