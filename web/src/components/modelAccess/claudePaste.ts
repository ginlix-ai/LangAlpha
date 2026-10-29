/**
 * What a paste into the Claude sign-in field looks like, checked as it is
 * typed so a wrong paste is caught before it spends the single-use code. The
 * shapes are the ones the server's parser takes (`parse_callback_input`).
 */

export type PasteShape =
  | 'empty'
  /** `code#state`, what Claude's page shows. */
  | 'code_state'
  /** The whole callback address, with `code` and `state` in it. */
  | 'callback_url'
  /** `code=…&state=…` */
  | 'query'
  /** A Claude page address with no code in it: the tab, not the code. */
  | 'bare_page'
  /** A code without its `#state` half. */
  | 'code_only'
  | 'unknown';

const CLAUDE_HOSTS = /(^|\.)(claude\.ai|claude\.com|anthropic\.com)$/i;

export function checkClaudePaste(raw: string): { shape: PasteShape; ok: boolean } {
  const value = raw.trim();
  if (!value) return { shape: 'empty', ok: false };

  if (/^https?:\/\//i.test(value)) {
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      return { shape: 'unknown', ok: false };
    }
    const fragment = new URLSearchParams(url.hash.replace(/^#/, ''));
    const code = url.searchParams.get('code') ?? fragment.get('code');
    const state = url.searchParams.get('state') ?? fragment.get('state');
    if (code && state) return { shape: 'callback_url', ok: true };
    return { shape: CLAUDE_HOSTS.test(url.hostname) ? 'bare_page' : 'unknown', ok: false };
  }

  if (/(^|&)code=[^&]+/.test(value) && /(^|&)state=[^&]+/.test(value)) return { shape: 'query', ok: true };
  if (/^[^\s#]+#[^\s#]+$/.test(value)) return { shape: 'code_state', ok: true };
  if (/^[A-Za-z0-9_-]{16,}$/.test(value)) return { shape: 'code_only', ok: false };
  return { shape: 'unknown', ok: false };
}
