/**
 * `/desktop/menubar`: the page the desktop shell keeps loaded in its hidden
 * menu bar window for as long as the app runs. Shown, it is the panel under
 * the icon; hidden, it is the app's background presence, holding the user
 * feed that notifications and the dock badge come from.
 *
 * None of the app shell: no sidebar, no setup gate, no onboarding, and no
 * sign-in of its own. A window this small cannot host a sign-in, and sending
 * it to the portal's page would take over the panel, so signed out it says so
 * and hands the sign-in to the main window.
 */
import { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useAuth } from '@/contexts/AuthContext';
import { desktop, hideMenubar, openInMain, setTrayItems } from '@/lib/desktop';
import { DesktopNotifier } from '@/lib/desktopNotifications/DesktopNotifier';
import { ThreadLifecycleFeed } from '@/lib/threadLifecycle/ThreadLifecycleFeed';
import { MenubarPanel } from './MenubarPanel';
import { useTrayItems } from './useTrayItems';
import './MenubarPage.css';

function SignedIn() {
  useTrayItems();
  return (
    <>
      <ThreadLifecycleFeed />
      <DesktopNotifier />
      <MenubarPanel />
    </>
  );
}

function SignedOut() {
  const { t } = useTranslation();
  // Lines left from the account that signed out would name its portfolio.
  useEffect(() => {
    setTrayItems([], { refreshMs: 300_000 });
  }, []);
  return (
    <div className="mb-signed-out">
      <h1 className="mb-signed-out-title title-font">{t('menubar.signedOut.title')}</h1>
      <p className="mb-signed-out-body">{t('menubar.signedOut.body')}</p>
      <button type="button" className="mb-open" onClick={() => openInMain('/')}>
        {t('menubar.signedOut.action')}
      </button>
    </div>
  );
}

export default function MenubarPage() {
  const { isLoggedIn, userId } = useAuth();

  // The shell closes its panel on Escape itself; this covers a shell that does not.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !event.defaultPrevented) hideMenubar();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  // Keyed on the account: another account signing in without a sign-out
  // between gets a fresh feed and notifier rather than the last one's.
  return (
    <div className="mb-root" data-look={desktop?.platform === 'darwin' ? 'mac' : undefined}>
      {isLoggedIn ? <SignedIn key={userId ?? ''} /> : <SignedOut />}
    </div>
  );
}
