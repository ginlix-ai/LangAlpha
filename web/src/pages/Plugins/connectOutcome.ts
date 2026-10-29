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

import type { TFunction } from 'i18next';
import { NAME_RE } from '@/pages/ChatAgent/components/mcp/mcpSchemas';

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
  'install_failed',
  'internal',
]);

export type ConnectOutcome =
  | { kind: 'connected'; server: string | null; plugin: string | null }
  | {
      kind: 'failed';
      server: string | null;
      plugin: string | null;
      reasonKey: string;
      /** Why a sign-in that was to save its server could not, as the server
       *  put it. Text a link author controls: shown after a fixed sentence,
       *  never as markup and never as the whole message. */
      detail: string | null;
    };

/** The server caps `detail` at this; a longer one did not come from it. */
export const DETAIL_MAX = 200;

const PLUGIN_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/i;

/**
 * What a server the user could have connected is called, or null. The shape
 * alone: a row saved before the sandbox reserved its name still connects, and
 * dropping its name here would leave its pending marker behind.
 */
function serverName(value: string | null): string | null {
  return value && NAME_RE.test(value) ? value : null;
}

/** The plugin a sign-in installed or was installing, or null. */
function pluginName(value: string | null): string | null {
  return value && PLUGIN_NAME_RE.test(value) ? value : null;
}

/**
 * The `detail` of an `install_failed` return as one line of plain text, or
 * null. Control characters go, whitespace collapses, and the length is held to
 * what the server ever sends, so a crafted link cannot fill the toast.
 */
function plainDetail(value: string | null): string | null {
  if (!value) return null;
  // eslint-disable-next-line no-control-regex
  const text = value.replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!text) return null;
  return text.length > DETAIL_MAX ? `${text.slice(0, DETAIL_MAX - 1)}\u2026` : text;
}

/** The callback's verdict, or null if this landing is not one. */
export function readConnectOutcome(params: URLSearchParams): ConnectOutcome | null {
  const connected = params.get('mcp_connected');
  const reason = params.get('mcp_error');
  const plugin = pluginName(params.get('plugin'));
  if (connected !== null) return { kind: 'connected', server: serverName(connected), plugin };
  if (reason === null) return null;
  const known = CALLBACK_ERROR_REASONS.has(reason);
  return {
    kind: 'failed',
    server: serverName(params.get('server')),
    plugin,
    reasonKey: `plugins.oauth.callbackError.${known ? reason : 'unknown'}`,
    detail: reason === 'install_failed' ? plainDetail(params.get('mcp_detail')) : null,
  };
}

/** Every param the callback may add, stripped once the landing is read. */
export const CALLBACK_PARAMS = ['mcp_connected', 'mcp_error', 'server', 'plugin', 'mcp_detail'];

/**
 * Why a sign-in came back without finishing: our sentence for the callback's
 * reason, then the server's own words where it sent any. Those words arrive
 * in the address bar, so they are a link author's as much as the server's:
 * plain text after a fixed lead, never markup and never the whole message.
 */
export function signInFailureParts(
  t: TFunction,
  reasonKey?: string,
  detail?: string | null,
): [reason: string, detail: string | null] {
  return [
    t(reasonKey ?? 'plugins.oauth.callbackError.unknown'),
    detail ? t('plugins.oauth.callbackDetail', { detail }) : null,
  ];
}
