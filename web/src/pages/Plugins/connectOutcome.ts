/**
 * What the OAuth callback said, read back off the URL it landed on.
 *
 * These params arrive in the address bar, so anyone who can get the user to
 * open a link can set them. Nothing here is rendered as markup, so the risk is
 * not injection but authorship: passed through, they put whatever prose a link
 * author chose inside this app's own destructive toast, under this app's own
 * title, on the page where connectors are managed. Both halves go through a
 * closed set instead.
 *
 * It also fixes the ordinary case. The reason is a slug the backend picked for
 * its own logs, and `token_exchange_failed` was being shown to users as-is, in
 * English, on a page that is otherwise translated.
 */

/** Catalog `NAME_RE`: what a server the user could have connected is called. */
const CATALOG_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

/**
 * Every reason the callback emits. Each has a sentence; anything else is the
 * generic one, which is what a build older or newer than this one will hit.
 *
 * Exported so a test can assert each sentence exists: the key is built by
 * template, which the tree-wide locale sweep cannot see, so nothing else would
 * notice twelve of them going missing from a catalog.
 */
export const CALLBACK_ERROR_REASONS = new Set([
  'missing_state',
  'invalid_state',
  'state_mismatch',
  'denied',
  'provider_error',
  'missing_code',
  'issuer_mismatch',
  'blocked_endpoint',
  'token_exchange_failed',
  'server_changed',
  'internal',
]);

export type ConnectOutcome =
  | { kind: 'connected'; server: string | null }
  | { kind: 'failed'; server: string | null; reasonKey: string };

/** A catalog name, or null for anything that could not be one. */
function serverName(value: string | null): string | null {
  return value && CATALOG_NAME.test(value) ? value : null;
}

/** The callback's verdict, or null if this landing is not one. */
export function readConnectOutcome(params: URLSearchParams): ConnectOutcome | null {
  const connected = params.get('mcp_connected');
  const reason = params.get('mcp_error');
  if (connected !== null) return { kind: 'connected', server: serverName(connected) };
  if (reason === null) return null;
  return {
    kind: 'failed',
    server: serverName(params.get('server')),
    reasonKey: `plugins.oauth.callbackError.${CALLBACK_ERROR_REASONS.has(reason) ? reason : 'unknown'}`,
  };
}
