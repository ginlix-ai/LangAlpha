import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { revealError } from './secretErrors';

export type RevealCache = ReturnType<typeof useRevealCache>;

/**
 * Revealed values, by key name, fetched on demand. A value is dropped when its
 * key leaves the list, settled in render like the open form, so a key added
 * again under the name never shows the old one.
 */
export function useRevealCache(
  onReveal: (name: string) => Promise<string>,
  secrets: readonly UserVaultSecret[],
) {
  const { t } = useTranslation();
  const [revealed, setRevealed] = useState<Readonly<Record<string, string>>>({});
  const [revealing, setRevealing] = useState<string | null>(null);
  // A failed reveal is said under its own row, and only the latest one.
  const [failure, setFailure] = useState<{ name: string; message: string } | null>(null);
  // Bumped by every invalidation; a reveal resolving under an older epoch
  // discards its value instead of caching it.
  const epoch = useRef(0);

  const listed = new Set(secrets.map((s) => s.name));
  if (Object.keys(revealed).some((name) => !listed.has(name))) {
    setRevealed(Object.fromEntries(Object.entries(revealed).filter(([name]) => listed.has(name))));
  }

  function forget(name: string) {
    setRevealed((prev) => Object.fromEntries(Object.entries(prev).filter(([key]) => key !== name)));
  }

  async function toggle(name: string) {
    if (revealed[name] !== undefined) {
      forget(name);
      return;
    }
    setRevealing(name);
    setFailure(null);
    const at = epoch.current;
    try {
      const value = await onReveal(name);
      if (epoch.current === at) setRevealed((prev) => ({ ...prev, [name]: value }));
    } catch (err) {
      setFailure({ name, message: revealError(err, t) });
    }
    setRevealing(null);
  }

  /**
   * The key's value changed or it was deleted. Forgetting alone is not
   * enough: a reveal already in flight would cache the old plaintext when it
   * resolves, under a name a recreated key may take.
   */
  function invalidate(name: string) {
    epoch.current += 1;
    forget(name);
  }

  return { revealed, revealing, failure, toggle, invalidate };
}
