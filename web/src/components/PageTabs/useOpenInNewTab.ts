import { isDesktopShell } from '@/lib/desktop';
import { usePageTabActions } from './pageTabsContext';

/**
 * Opens an app path in a new tab: a page tab while tabs are on, else a browser
 * tab. Null in the desktop shell with tabs off, which hands a new window of
 * its own pages to the system browser, signed out.
 */
export function useOpenInNewTab(): ((path: string) => void) | null {
  const actions = usePageTabActions();
  if (actions) return (path) => actions.open(path);
  if (isDesktopShell()) return null;
  return (path) => void window.open(path, '_blank', 'noopener');
}
