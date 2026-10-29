import React from 'react';
import { Favicon } from '../Favicon';
import { messagingAppDomain } from './messageDelivery';

type Glyph = React.ComponentType<{ className?: string }>;

// One component per app, so a tab's glyph keeps its identity across renders.
const glyphs = new Map<string, Glyph>();

/** A glyph showing the app's favicon, for a slot that takes an icon component;
 *  null for an app with no known site. */
export function appFaviconGlyph(platform: string | null | undefined): Glyph | null {
  const domain = messagingAppDomain(platform);
  if (!domain) return null;
  let glyph = glyphs.get(domain);
  if (!glyph) {
    glyph = function AppFavicon() {
      return <Favicon domain={domain} size={14} />;
    };
    glyphs.set(domain, glyph);
  }
  return glyph;
}
