import { useEffect, useState } from 'react';
import { APP_ENTRY_PATH } from '@/config/hostMode';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useStorageUser } from '@/lib/userStorage';
import { PAGE_TABS_FEATURE } from './pageTabsContext';
import { savedFrontTabPath } from './pageTabsStorage';

/** How long an arrival at the entry waits on the flags before taking the dashboard. */
const FLAGS_WAIT_MS = 2000;

/**
 * Where an arrival at the app's entry goes while tabs are on: the page the tab
 * in front was showing. Coming back from outside the app (the account portal's
 * link back lands on the entry) then returns to that page, rather than putting
 * the dashboard in the tab. Null when there is nowhere to resume, and
 * undefined while the flags are read, which is waited for only when a strip
 * was saved, so a user who never opened tabs lands at once. The redirect
 * cannot be undone, so it waits for the flags themselves rather than a kept
 * read, and for no more than `FLAGS_WAIT_MS`: a request that never answers
 * would otherwise leave the entry blank, and the dashboard is where it goes
 * with tabs off.
 */
export function useResumeTab(): string | null | undefined {
  const isMobile = useIsMobile();
  const user = useStorageUser();
  const [saved] = useState(() => savedFrontTabPath(user));
  const { data, isPending } = useFeatures();
  const [gaveUp, setGaveUp] = useState(false);
  const resumable = !isMobile && !!saved && saved.split(/[?#]/)[0] !== APP_ENTRY_PATH;
  const waiting = resumable && isPending;
  useEffect(() => {
    if (!waiting) return;
    const timer = setTimeout(() => setGaveUp(true), FLAGS_WAIT_MS);
    return () => clearTimeout(timer);
  }, [waiting]);
  if (!resumable) return null;
  if (isPending) return gaveUp ? null : undefined;
  return isFeatureEnabled(data, PAGE_TABS_FEATURE) ? saved : null;
}
