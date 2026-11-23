// Opens the connection to the auth server while index.html is still parsing.
//
// A visit whose session has expired, which is most visits after an hour away,
// refreshes it before the app renders anything (AuthContext waits on
// getSession). The auth server is its own origin, so that refresh first pays
// a DNS lookup and a TLS handshake, several round trips where the network is
// far from the server. A preconnect overlaps them with the entry's download.
// `crossorigin` because auth-js fetches without credentials, which the
// browser keeps on a connection of its own.

import { basename } from 'node:path';
import type { Plugin } from 'vite';

/** The origin to preconnect to, or null when this build signs in nowhere. */
export function authOrigin(hostMode: string | undefined, supabaseUrl: string | undefined): string | null {
  if (hostMode !== 'platform' || !supabaseUrl) return null;
  try {
    return new URL(supabaseUrl).origin;
  } catch {
    return null;
  }
}

export function authPreconnect(hostMode: string | undefined, supabaseUrl: string | undefined): Plugin {
  const origin = authOrigin(hostMode, supabaseUrl);
  return {
    name: 'la-auth-preconnect',
    transformIndexHtml: {
      order: 'post',
      handler: (html, ctx) =>
        origin && basename(ctx.filename) === 'index.html'
          ? { html, tags: [{ tag: 'link', attrs: { rel: 'preconnect', href: origin, crossorigin: '' }, injectTo: 'head-prepend' }] }
          : html,
    },
  };
}
