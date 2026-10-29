import { useEffect, useState } from 'react';

/** `?secret=NAME` ("Set up NAME") or `?edit=NAME` ("Replace key"). */
export interface SecretDeepLink {
  kind: 'prefill' | 'edit';
  name: string;
}

function sameLink(a: SecretDeepLink, b: SecretDeepLink | null): boolean {
  return b !== null && a.kind === b.kind && a.name === b.name;
}

/**
 * Returns the link on the one render that should act on it, then has the
 * caller strip it once that render is committed. Remembered until the param is
 * gone, so a link is acted on once however many renders happen while it is
 * being stripped. `ready` holds it until what it looks up has loaded.
 */
export function useSecretDeepLink({
  deepLink,
  ready,
  onConsumed,
}: {
  deepLink: SecretDeepLink | null;
  ready: boolean;
  onConsumed: () => void;
}): SecretDeepLink | null {
  const [handled, setHandled] = useState<SecretDeepLink | null>(null);
  const due = deepLink && ready && !sameLink(deepLink, handled) ? deepLink : null;
  if (due) setHandled(due);
  else if (!deepLink && handled) setHandled(null);
  useEffect(() => {
    if (handled) onConsumed();
  }, [handled, onConsumed]);
  return due;
}
