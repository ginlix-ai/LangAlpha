import React from 'react';
import { cn } from '@/lib/utils';
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
    // The slot's class (its size, and whatever it hides or fades the glyph
    // by) lands on a box around the image, which takes none of its own.
    glyph = function AppFavicon({ className }: { className?: string }) {
      return (
        <span className={cn('inline-flex', className)}>
          <Favicon domain={domain} size={14} />
        </span>
      );
    };
    glyphs.set(domain, glyph);
  }
  return glyph;
}
