import { useState } from 'react';
import { APP_ENTRY_PATH } from '@/config/hostMode';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useStorageUser } from '@/lib/userStorage';
import { PAGE_TABS_FEATURE } from './pageTabsContext';
import { savedFrontTabPath } from './pageTabsStorage';

/**
 * Where an arrival at the app's entry goes while tabs are on: the page the tab
 * in front was showing. Coming back from outside the app (the account portal's
 * link back lands on the entry) then returns to that page, rather than putting
 * the dashboard in the tab. Null when there is nowhere to resume, and
 * undefined while the flags are read, which is waited for only when a strip
 * was saved, so a user who never opened tabs lands at once.
 */
export function useResumeTab(): string | null | undefined {
  const isMobile = useIsMobile();
  const user = useStorageUser();
  const [saved] = useState(() => savedFrontTabPath(user));
  const { data, isPending } = useFeatures();
  if (isMobile || !saved || saved.split(/[?#]/)[0] === APP_ENTRY_PATH) return null;
  if (isPending) return undefined;
  return isFeatureEnabled(data, PAGE_TABS_FEATURE) ? saved : null;
}
