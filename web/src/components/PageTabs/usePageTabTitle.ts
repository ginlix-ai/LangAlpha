import { useEffect } from 'react';
import { useLocation } from 'react-router';
import { useIsPresent } from '@/lib/framer';
import { usePageTabActions } from './pageTabsContext';

/**
 * Names the tab this page is in, where the page knows better than its route
 * (a chat names its thread). A no-op while tabs are off. A page still
 * animating out already reads the address it is leaving for, so it stays
 * silent: its name would land on the page coming in. Kept apart from the tab
 * contexts, which the shell loads first, since only pages animate.
 */
export function usePageTabTitle(title: string | null | undefined) {
  const publish = usePageTabActions()?.publishTitle;
  const present = useIsPresent();
  const location = useLocation();
  const path = `${location.pathname}${location.search}`;
  useEffect(() => {
    if (publish && title && present) publish(title, path);
  }, [publish, title, path, present]);
}
