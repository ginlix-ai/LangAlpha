import { isFeatureEnabled, useFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import type { Computer } from '@/types/api';
import { hasBrowser } from '../utils/computerStatus';

/** The reader's own switch for the computer's browser, which they turn on in
 *  Settings. It reads off until the features load. */
export const BROWSER_FEATURE = 'browser';

/** Whether the reader is shown *computer*'s browser: it has one, and they
 *  turned the browser on. */
export function useBrowserHere(computer: Pick<Computer, 'kind'> | null | undefined, shared = false): boolean {
  return useFeatureEnabled(BROWSER_FEATURE) && hasBrowser(computer, shared);
}

/** Whether the reader is known to have no browser on *computer*: they turned it
 *  off, or the computer, once read, has none. Unlike `useBrowserHere`, a read
 *  still loading or failed says neither, so a Browser tab kept from before is
 *  not closed while the panel loads. */
export function useBrowserGone(computer: Pick<Computer, 'kind'> | null | undefined): boolean {
  const { data, isSuccess } = useFeatures();
  return (isSuccess && !isFeatureEnabled(data, BROWSER_FEATURE)) || (computer != null && !hasBrowser(computer));
}
