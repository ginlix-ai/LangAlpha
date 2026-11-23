import type { McpOauthStatus } from '../../utils/api';

/*
 * The OAuth predicates every MCP surface has to agree on, the sidebar's
 * connected-brokerage check included (see mcpState for the rest).
 *
 * Written as NEGATIONS of the states that need nothing, never as an
 * enumeration of the states that do — an enumeration is a list someone has to
 * remember to extend when a status is added, and the Plugins page had already
 * grown two of them.
 */

/**
 * The user-level OAuth connection is unusable (revoked / needs reauth / a
 * refresh whose outcome we can't prove). The only fix is reconnecting in
 * the Plugins page, so this dominates whatever workspace-local status is cached.
 */
export function isOauthBroken(
  status: McpOauthStatus | null | undefined,
): status is Exclude<McpOauthStatus, 'connected'> {
  return !!status && status !== 'connected';
}

/**
 * The next step for this connection is (re-)running the authorize flow. Stated
 * as the negation of the one good state so a new status can't be forgotten
 * here: every status that isn't a live connection — including none at all —
 * needs one.
 */
export function needsOauthConnect(status: McpOauthStatus | null | undefined): boolean {
  return status !== 'connected';
}

/**
 * There is a connection worth tearing down. Also a negation: only a
 * never-connected server (no status) and an already-revoked one have nothing
 * to disconnect.
 */
export function canDisconnectOauth(status: McpOauthStatus | null | undefined): boolean {
  return !!status && status !== 'revoked';
}
