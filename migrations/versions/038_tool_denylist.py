"""Invert the brokerage tool policy from an allowlist to a denial.

The policy regulates what we have curated and never blocks what we have not.
An allowlist cannot express that, because it has to enumerate, and the set it
would have to enumerate includes every tool a vendor publishes after our last
release. Under an allowlist those tools are refused; the connector quietly stops
covering what the broker offers, and the user's only remedy is to wait for a
deploy. Every other client these brokers can be connected to hands the model the
whole tool list, so a refusal here buys a guarantee nobody else offers at a cost
users feel immediately.

So the column now names what a grant refuses: exactly the curated tools whose
capability group the user declined. A tool in no group is in no denial.

``policy_required`` survives the inversion with a narrower job. It could once
prove the policy was not accidentally open, because an allowlist that failed to
derive came out NULL and served nothing. A denial that fails to derive comes out
empty and serves everything, so the flag now proves only that a policy was
computed for a connection that must have one. The CHECK moves with the column to
keep that much.

The flag is not the guard it reads as, and the relay is: ``fetch_grant_for_relay``
never selected ``policy_required``, so the CHECK constrains a column the
enforcement point does not look at. What actually closes the hole is
``prepare_relay`` refusing a NULL denial on a grant whose destination is a
shipped brokerage -- the one place that can see both the missing policy and the
address it was supposed to be derived from.

The column is ADDED rather than renamed, and the old one is left in place for a
later release to drop. See ``upgrade``.

Revision ID: 038
Revises: 037
"""

from alembic import op


revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Expand, never rename. Migrations run when a new container boots, and under
    # the blue/green cutover the old colour keeps serving every request until the
    # edge flips and then keeps draining turns for DRAIN_TIMEOUT after that. Its
    # code selects ``tool_allowlist``; renaming the column out from under it took
    # every brokerage relay call on the serving colour to an UndefinedColumn for
    # the length of the deploy. So the new column arrives beside the old one, the
    # old colour goes on reading a column that still exists and still holds what
    # it wrote, and dropping ``tool_allowlist`` is a later release's job once no
    # running code reads it.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD COLUMN tool_denylist JSONB
    """)

    # The CHECK names its column in its expression, so it is replaced rather
    # than left pointing at the old one.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          DROP CONSTRAINT IF EXISTS sandbox_egress_grants_policy_present
    """)

    # The flag has to be cleared before the CHECK can name the new column, and
    # clearing it is what the flag actually means. Every brokerage grant was
    # written with ``policy_required`` true against the OLD column, while the
    # new one starts NULL by design -- so re-adding the constraint over live
    # rows aborts the whole upgrade with a CheckViolation, on any database that
    # has ever connected a brokerage. It said so on the first real run.
    #
    # False is not a weakening here. The flag asserts "a denial was computed for
    # this grant", and at this instant none has been: the value that was
    # computed is an allowlist, in a different column, under a policy that has
    # been inverted. The version bump below is what makes the next resolve
    # recompute both halves together. Nothing rests on the flag in the meantime
    # -- ``fetch_grant_for_relay`` does not select it, and what refuses a
    # brokerage grant with no denial is ``prepare_relay`` reading the denial
    # itself against the destination.
    op.execute("""
        UPDATE sandbox_egress_grants
           SET policy_required = false, updated_at = NOW()
         WHERE policy_required
    """)

    # Either column satisfies it, because both versions write during a cutover
    # and they write different columns. Naming only the new one made the OLD
    # colour's grant sync fail outright: it writes ``policy_required`` true with
    # an allowlist and no denial, which a denial-only CHECK rejects, so the
    # draining colour could not record a grant at all. The guard this constraint
    # exists for survives -- a derivation bug that produced neither column is
    # still refused -- and the contract half of the expand/contract pass is what
    # narrows it back to the denial alone, once no old colour is left to serve.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD CONSTRAINT sandbox_egress_grants_policy_present
          CHECK (NOT (policy_required
                      AND tool_denylist IS NULL
                      AND tool_allowlist IS NULL))
    """)

    # No data conversion, and nothing is blanked. Every existing value is an
    # allowlist, and the honest complement of one needs the curation map as it
    # stood when that row was written, which is not something a migration can
    # reconstruct. So the new column starts NULL, meaning "no denial computed
    # yet", and the version bump below is what makes the next resolve compute
    # one.
    #
    # The window in between is not permissive, which is the part the first draft
    # of this migration got wrong: it left NULL to mean "deny nothing" while
    # running sandboxes still held their grant ids and relay JWTs, so an
    # in-flight turn could call the very tools its user had declined, for hours.
    # ``prepare_relay`` now refuses a brokerage grant whose denial is NULL
    # outright, so the window fails closed and announces itself as
    # ``policy_missing`` rather than quietly serving everything.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (
             SELECT DISTINCT user_id FROM user_mcp_oauth_connections
         )
    """)


def downgrade() -> None:
    # ``tool_allowlist`` was never touched on the way up, so it still holds
    # exactly what the pre-038 code wrote there and going back needs no data
    # recovery -- only the new column and its constraint removed. The earlier
    # draft NULLed every allowlist on the way down and called that "back to the
    # fail-closed reading", which was backwards twice over: a NULL allowlist is
    # read as NO policy rather than an empty one, and nothing forced a recompute
    # afterwards, so a rollback left every brokerage grant unrestricted until
    # some unrelated change happened to resync it. A rollback is what you reach
    # for when a deploy has already gone wrong; it is the worst moment to open
    # the gate.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          DROP CONSTRAINT IF EXISTS sandbox_egress_grants_policy_present
    """)

    # Fail the policy shut BEFORE the denial is destroyed, because the restored
    # code cannot read what this is about to drop and its own gate is
    # ``if allowlist is not None`` -- a NULL allowlist is not a strict policy
    # there, it is no policy at all. A grant written under 038 has exactly that,
    # so dropping the column first would have left every curated tool callable,
    # live orders included, on precisely the connections this feature exists to
    # gate. The empty list is the one value that says the same thing in the old
    # vocabulary: nothing is permitted until the restored code recomputes, which
    # the version bump below is what triggers.
    #
    # It covers the stale case too. A grant that predates 038 still holds the
    # allowlist its last pre-038 sync wrote, and a consent narrowing performed
    # while 038 was live never reached that column, so leaving it in place would
    # have silently restored a group the user had declined. Both populations get
    # the same treatment for the same reason: nothing served on a stale policy.
    op.execute("""
        UPDATE sandbox_egress_grants
           SET tool_allowlist = '[]'::jsonb,
               policy_required = true,
               updated_at = NOW()
         WHERE tool_denylist IS NOT NULL
    """)

    op.execute("""
        ALTER TABLE sandbox_egress_grants
          DROP COLUMN IF EXISTS tool_denylist
    """)
    op.execute("""
        ALTER TABLE sandbox_egress_grants
          ADD CONSTRAINT sandbox_egress_grants_policy_present
          CHECK (NOT (policy_required AND tool_allowlist IS NULL))
    """)
    # The allowlists still standing are as old as the upgrade, since the new
    # code wrote only the denial column. Bump so the restored code recomputes
    # them instead of enforcing a snapshot of whatever the map said back then.
    op.execute("""
        UPDATE workspaces ws SET mcp_config_version = ws.mcp_config_version + 1
         WHERE ws.user_id IN (
             SELECT DISTINCT user_id FROM user_mcp_oauth_connections
         )
    """)
