// Writes the app's frame policy into index.html, in dev and in the build, as a
// <meta http-equiv="Content-Security-Policy"> in place of the `frame-policy`
// marker. A meta needs no serving config, so a self-hosted build carries the
// same policy as any other. The marker sits in <head> ahead of every script, so
// the policy is in force before anything could create a frame.
//
// The build fails without the marker: a policy that quietly went missing would
// look exactly like one that works.

import type { Plugin } from 'vite';
import { appFrameSrc } from '../../src/lib/framePolicy.ts';

const MARKER = /<!--\s*frame-policy\b[^>]*-->/;

/** index.html with its marker replaced by the policy. */
export function withFramePolicy(html: string, apiBase: string | undefined): string {
  if (!MARKER.test(html)) throw new Error('index.html is missing its <!-- frame-policy --> marker');
  const meta = `<meta http-equiv="Content-Security-Policy" content="${appFrameSrc(apiBase)}" />`;
  return html.replace(MARKER, meta);
}

export function framePolicy(apiBase: string | undefined): Plugin {
  return {
    name: 'la-frame-policy',
    transformIndexHtml: {
      order: 'pre',
      handler: (html) => withFramePolicy(html, apiBase),
    },
  };
}
