import { useCallback, useState } from 'react';
import type { QuoteAnchor } from './quoteAnchor';

/**
 * One quote on its way to the composer: what it is anchored to and whether a
 * reply is being written. Held as one value so that a reply can never be open
 * with nothing to quote.
 */
export function useQuoteSession<A extends QuoteAnchor>() {
  const [session, setSession] = useState<{ anchor: A; composing: boolean } | null>(null);

  const show = useCallback((anchor: A) => setSession({ anchor, composing: false }), []);
  const beginReply = useCallback((anchor: A) => setSession({ anchor, composing: true }), []);
  const dismiss = useCallback(() => setSession(null), []);

  return { anchor: session?.anchor ?? null, composing: !!session?.composing, show, beginReply, dismiss };
}
