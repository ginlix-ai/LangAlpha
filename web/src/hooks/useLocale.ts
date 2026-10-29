import { useSyncExternalStore } from 'react';
import i18n from '@/i18n';

function subscribe(onChange: () => void): () => void {
  i18n.on('languageChanged', onChange);
  return () => i18n.off('languageChanged', onChange);
}

const readLocale = () => i18n.language;

/**
 * The active language as a render input, for the formatters in `lib/format.ts`
 * and anything else that formats for the reader. Read it here rather than off
 * the i18n instance: the React Compiler caches a call on its arguments, so a
 * helper that reads `i18n.language` itself keeps the old language after a
 * switch.
 */
export function useLocale(): string {
  return useSyncExternalStore(subscribe, readLocale);
}
