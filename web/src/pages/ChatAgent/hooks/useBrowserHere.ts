import { isFeatureEnabled, useFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import type { Computer } from '@/types/api';

/** The reader's own switch for the computer's browser, which they turn on in
 *  Settings. It reads off until the features load. */
export const BROWSER_FEATURE = 'browser';

/** Whether the reader is shown *computer*'s browser: every computer has one,
 *  and they turned the browser on. A member of a `shared` workspace is not sent
 *  its computer, which has one all the same. */
export function useBrowserHere(computer: Pick<Computer, 'kind'> | null | undefined, shared = false): boolean {
  return useFeatureEnabled(BROWSER_FEATURE) && (computer != null || shared);
}

/** Whether the reader is known to have turned the browser off. Unlike
 *  `useBrowserHere`, a read still loading or failed says neither, so a Browser
 *  tab kept from before is not closed while the panel loads. */
export function useBrowserGone(): boolean {
  const { data, isSuccess } = useFeatures();
  return isSuccess && !isFeatureEnabled(data, BROWSER_FEATURE);
}
