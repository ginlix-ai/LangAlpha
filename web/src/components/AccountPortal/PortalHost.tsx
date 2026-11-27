import { createContext, useCallback, useContext, useEffect, useEffectEvent, useMemo, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate, useNavigationType, useSearchParams } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { usePageTabActions, usePageTabs } from '@/components/PageTabs/pageTabsContext';
import { useAuth } from '@/contexts/AuthContext';
import { useTheme } from '@/contexts/ThemeContext';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { PLATFORM_URL } from '@/config/supportLinks';
import { isDesktopShell } from '@/lib/desktop';
import { inAppPath } from '@/lib/inAppPath';
import { queryKeys } from '@/lib/queryKeys';
import { readPlanAfterChange } from '@/pages/ChatAgent/hooks/useTierQuota';
import { usePortalNav } from './portalNav';
import { portalPathOf, portalPathOfHref, portalSettingsPath, portalUrl, readPortalPage } from './portalPaths';
import { envelope, portalOrigin, readPortalMessage, type AppToPortal, type PortalNavGroup } from './portalMessages';
import { useSlotGeometry } from './useSlotGeometry';
import './PortalHost.css';

/** The portal may not be listening yet when its frame fires `load`. */
const HELLO_INTERVAL_MS = 500;
/** How long a portal page has to come on screen before Settings offers it at top level. */
const READY_TIMEOUT_MS = 10_000;

export type PortalStatus = 'loading' | 'ready' | 'stuck';

interface PortalHostValue {
  /** Start loading the portal, if it is not already: a menu that leads to it opened. */
  want: () => void;
  status: PortalStatus;
  /** The portal's pages as it last listed them. */
  nav: PortalNavGroup[] | null;
  /** The element the frame is laid over while a portal page is shown. */
  setSlot: (el: HTMLElement | null) => void;
}

const PortalHostContext = createContext<PortalHostValue | null>(null);

/** Null where the portal's pages do not open in Settings. */
export function usePortalHost(): PortalHostValue | null {
  return useContext(PortalHostContext);
}

/**
 * Whether the portal's pages open inside Settings. Not on a phone, where
 * Settings is a column of pills with no room for them, and not in the desktop
 * app, which shows the portal in its window as before.
 */
function usePortalInSettings(): boolean {
  const isMobile = useIsMobile();
  return !!PLATFORM_URL && !isMobile && !isDesktopShell();
}

function postToPortal(frame: HTMLIFrameElement | null, origin: string | null, message: AppToPortal) {
  if (origin) frame?.contentWindow?.postMessage(envelope(message), origin);
}

/**
 * The account portal's one frame, kept for the app's life once something asks
 * for it. It cannot live in Settings: a page tab or a section of its own would
 * reload it at every visit, and moving an iframe in the DOM reloads it too. So
 * it sits on the body and is laid over the slot Settings leaves for it, hidden
 * while no slot is there. Loading it as the account menu opens means the
 * portal is usually ready by the time its page is picked.
 *
 * Every plain click on a link into the portal opens that page in Settings
 * instead, wherever the link is drawn, so no caller needs to know. It listens
 * on the document, after the app's own handlers: a menu item has closed its
 * menu by then, and a handler that took the click (`defaultPrevented`) keeps
 * it. A link that opens a tab of its own keeps doing so: it was drawn that way
 * so the page it sits on, and anything typed there, stays.
 */
export function PortalHostProvider({ children }: { children: ReactNode }) {
  const enabled = usePortalInSettings();
  const { userId } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [nav, setNav] = usePortalNav();
  const [wanted, setWanted] = useState(false);
  const [status, setStatus] = useState<PortalStatus>('loading');
  const [slot, setSlot] = useState<HTMLElement | null>(null);
  const want = useCallback(() => setWanted(true), []);
  // An address that names a portal page starts it at once, alongside
  // Settings' own code rather than after it.
  if (enabled && !wanted && location.pathname === '/settings' && readPortalPage(new URLSearchParams(location.search)) !== null) {
    setWanted(true);
  }
  // The frame goes with a phone-width window and loads again after it, and
  // loads again for another user, whose portal it never showed.
  const [frameUser, setFrameUser] = useState(userId);
  if (frameUser !== userId) {
    setFrameUser(userId);
    setStatus('loading');
  }
  if (!enabled && status !== 'loading') setStatus('loading');

  const openLink = useEffectEvent((event: MouseEvent) => {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    const link = event.target instanceof Element ? event.target.closest('a[href]') : null;
    if (!(link instanceof HTMLAnchorElement) || link.hasAttribute('download') || (link.target && link.target !== '_self')) return;
    const path = portalPathOfHref(link.href);
    if (!path) return;
    event.preventDefault();
    setWanted(true);
    navigate(portalSettingsPath(path));
  });
  useEffect(() => {
    if (!enabled) return;
    const onClick = (event: MouseEvent) => openLink(event);
    document.addEventListener('click', onClick);
    return () => document.removeEventListener('click', onClick);
  }, [enabled]);

  const value = useMemo<PortalHostValue | null>(
    () => (enabled ? { want, status, nav, setSlot } : null),
    [enabled, want, status, nav],
  );
  return (
    <PortalHostContext.Provider value={value}>
      {children}
      {enabled && wanted && createPortal(<PortalFrame key={userId ?? ''} slot={slot} setStatus={setStatus} onNav={setNav} />, document.body)}
    </PortalHostContext.Provider>
  );
}

function PortalFrame({ slot, setStatus, onNav }: {
  slot: HTMLElement | null;
  setStatus: (status: PortalStatus) => void;
  onNav: (groups: PortalNavGroup[]) => void;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const navigationType = useNavigationType();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const tabs = usePageTabs();
  const actions = usePageTabActions();
  const { recheckSession } = useAuth();
  const locale = useLocale();
  const { theme } = useTheme();
  const { rect, layout } = useSlotGeometry(slot);
  // The portal page the address names, shown only while Settings has a slot for it.
  const named = location.pathname === '/settings' ? readPortalPage(searchParams) : null;
  const page = slot ? named : null;
  const here = `${location.pathname}${location.search}`;
  const frameRef = useRef<HTMLIFrameElement>(null);
  const [origin] = useState(() => portalOrigin(PLATFORM_URL ?? '', window.location.href));
  // Loaded at the page asked for, if there is one, and loaded again only to
  // retry a portal that stuck. Every other move is a message, so the portal
  // never reloads under the person.
  const [load, setLoad] = useState(() => ({ n: 0, src: portalUrl(named ?? '/') }));
  const [awaiting, setAwaiting] = useState(false);
  // The portal answered this load's hello.
  const [connected, setConnected] = useState(false);
  // The page the portal could not show (`''` once the person has left it),
  // and why: a page that lands late clears a timeout or a refused path, never
  // a signed-out portal. A refused path is one the address cannot carry.
  const [stuck, setStuck] = useState<{ page: string; why: 'timeout' | 'refused' | 'signed-out' } | null>(null);
  const [title, setTitle] = useState('');
  // The language the portal was last told, so a change after it is ready reaches it.
  const toldLocale = useRef(locale);
  // Where the portal says it is. The frame shows only once that is the page
  // asked for, or coming back from a native section would flash the portal
  // page last seen before it moved.
  const [at, setAt] = useState(named ?? '/');
  // Where the address is being moved to follow the portal, and the entry it
  // left (`from`), until the router lands there. The router applies a move
  // renders later than the portal makes it, so meanwhile the address still
  // names the page the portal left: the frame stays shown, since the portal
  // is where the person is, and that page is not sent back to it. A move the
  // person makes meanwhile (a push, Back) ends it; the follows are replaces.
  const [following, setFollowing] = useState<{ to: string; from: string } | null>(null);
  // The last `navigate` sent: an answer to an earlier one is stale.
  const lastNavigate = useRef(0);
  // The `navigate` on its way and the page it asked for. Until it is answered
  // the portal is moving, so the frame stays hidden even where the address is
  // back on the page the portal was showing.
  const [pending, setPending] = useState<{ id: number; to: string } | null>(null);

  // Leaving a page the portal could not show, then asking for any portal page,
  // tries it again, loaded afresh.
  if (stuck !== null && page === null && stuck.page !== '') setStuck({ ...stuck, page: '' });
  if (stuck !== null && page !== null && page !== stuck.page) {
    setStuck(null);
    setConnected(false);
    setPending(null);
    setAt(page);
    setLoad((prev) => ({ n: prev.n + 1, src: portalUrl(page) }));
  }

  // Done once the router lands there or the person moves the address, and on
  // a fresh load, which follows nobody: its `ready` says where it is.
  if (following !== null && (!connected || (location.key !== following.from && (page === following.to || navigationType !== 'REPLACE')))) {
    setFollowing(null);
  }

  // Hidden by visibility alone, which also keeps it out of focus and the
  // accessibility tree: the frame keeps rendering, so it shows at once.
  const visible = page !== null && connected && stuck === null && pending === null && rect !== null && at === (following?.to ?? page);
  const status: PortalStatus = stuck !== null ? 'stuck' : visible ? 'ready' : 'loading';
  useEffect(() => setStatus(status), [status, setStatus]);

  const follow = (raw: string, pageTitle: string) => {
    const path = portalPathOf(raw);
    // Somewhere the address cannot name: offered at top level, or the page
    // asked for would be sent again, answered the same way, without end.
    if (!path) {
      if (page !== null) setStuck({ page, why: 'refused' });
      return;
    }
    setAt(path);
    setTitle(pageTitle);
    // Hidden, the address is someone else's, which stays.
    if (page === null || (following?.to ?? page) === path) return;
    setFollowing({ to: path, from: following?.from ?? location.key });
    navigate(portalSettingsPath(path), { replace: true });
  };

  // The portal cannot read this session. Hidden at once, so its own sign-in
  // page never shows here. Signed out in the app as well, the shell goes to
  // sign-in on its own; still signed in, the portal is offered at top level,
  // where it can sign in for itself.
  const portalSignedOut = () => {
    setStuck({ page: page ?? '', why: 'signed-out' });
    void recheckSession();
  };

  const onMessage = useEffectEvent((event: MessageEvent) => {
    const message = readPortalMessage(event, { origin, source: frameRef.current?.contentWindow });
    if (!message) return;
    switch (message.type) {
      case 'ready':
        setAwaiting(false);
        setConnected(true);
        if (message.signedOut) portalSignedOut();
        else setStuck(null);
        // The portal may have moved on its own while it loaded (a redirect).
        // If the address moved too, the address wins and is sent below.
        if (page === null || page === at) follow(message.path, message.title);
        else setTitle(message.title);
        return;
      case 'location':
        if (message.id !== undefined) {
          // The answer to a move the person has since replaced with another,
          // sent or not yet: Back commits before the effect below sends its
          // move, and an answer landing between the two is still stale. Left
          // for a section of Settings' own, the move is still the last one,
          // and its answer is taken, so coming back to that page shows it.
          if (message.id !== lastNavigate.current || (page !== null && pending?.to !== page)) return;
          setPending(null);
        } else if (pending !== null) {
          // A move of the portal's own while one is on its way is overtaken
          // by it. A portal that sends no ids answers with the page asked for.
          if (portalPathOf(message.path) !== pending.to) return;
          setPending(null);
        }
        // A page that came on screen after the wait ran out, or after a
        // path the address could not carry.
        if (stuck !== null && stuck.why !== 'signed-out' && portalPathOf(message.path)) setStuck(null);
        follow(message.path, message.title);
        return;
      case 'nav':
        onNav(message.groups);
        return;
      case 'account-changed':
        // Often a plan bought or changed: read past the server's cached plan,
        // as a return from the plan page does, or a size the new plan opens
        // stays locked until that cache lapses.
        void readPlanAfterChange(queryClient);
        void queryClient.invalidateQueries({ queryKey: queryKeys.user.all });
        void queryClient.invalidateQueries({ queryKey: queryKeys.features.all });
        void queryClient.invalidateQueries({ queryKey: queryKeys.computers.providers() });
        // The chat apps an automation can deliver to, linked or unlinked there.
        void queryClient.invalidateQueries({ queryKey: queryKeys.automationDelivery.all });
        return;
      case 'open-app': {
        // Only from a page the person is looking at: a portal still loading,
        // stuck, or answering a move is hidden, and moving the app from it
        // would be a move nobody asked for.
        const path = visible ? inAppPath(message.path) : null;
        if (!path) return;
        if (!actions) {
          navigate(path);
          return;
        }
        const existing = tabs?.tabs.find((tab) => tab.path === path);
        if (existing) actions.select(existing.id);
        else actions.open(path);
        return;
      }
      case 'signed-out':
        portalSignedOut();
        return;
    }
  });
  useEffect(() => {
    const listener = (event: MessageEvent) => onMessage(event);
    window.addEventListener('message', listener);
    return () => window.removeEventListener('message', listener);
  }, []);

  // Hello on every load, until the portal answers.
  useEffect(() => {
    if (!awaiting) return;
    const hello = () => postToPortal(frameRef.current, origin, { type: 'hello', locale, theme, ...(layout && { layout }) });
    toldLocale.current = locale;
    hello();
    const timer = setInterval(hello, HELLO_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [awaiting, origin, locale, theme, layout]);

  // A page asked for and not on screen in time, whether the portal never
  // answered or never reached the page, is offered at top level instead.
  useEffect(() => {
    if (page === null || visible || stuck !== null) return;
    const timer = setTimeout(() => setStuck({ page, why: 'timeout' }), READY_TIMEOUT_MS);
    return () => clearTimeout(timer);
  }, [page, visible, stuck]);

  useEffect(() => {
    if (connected && layout) postToPortal(frameRef.current, origin, { type: 'layout', layout });
  }, [connected, origin, layout]);

  useEffect(() => {
    if (connected) postToPortal(frameRef.current, origin, { type: 'theme', theme });
  }, [connected, origin, theme]);

  // A hello again carries a new language: it is the one message that names it.
  useEffect(() => {
    if (!connected || locale === toldLocale.current) return;
    toldLocale.current = locale;
    postToPortal(frameRef.current, origin, { type: 'hello', locale, theme, ...(layout && { layout }) });
  }, [connected, origin, locale, theme, layout]);

  // The address moved under the portal: the rail, a link, Back.
  useEffect(() => {
    // A page the portal could not show is offered at top level, not asked for again.
    if (page === null || !connected || stuck !== null) return;
    if (pending !== null) {
      // Waiting on the answer to the move already sent for this page.
      if (pending.to === page) return;
    } else if (page === at) {
      if (title) actions?.publishTitle(title, here);
      return;
    }
    // Still landing where the portal went.
    if (following !== null) return;
    lastNavigate.current += 1;
    setPending({ id: lastNavigate.current, to: page });
    postToPortal(frameRef.current, origin, { type: 'navigate', path: page, id: lastNavigate.current });
  }, [page, at, connected, pending, following, stuck, title, here, actions, origin]);

  return (
    <iframe
      key={load.n}
      ref={frameRef}
      src={load.src}
      // The app's own copy, which the shell has loaded: Settings' may not be yet.
      title={title || t('accountPortal.title')}
      allow="clipboard-write"
      referrerPolicy="strict-origin-when-cross-origin"
      className="portal-frame"
      // A frame whose scheme differs from its parent's is painted on an
      // opaque backdrop instead of showing the page under it.
      style={{ colorScheme: theme, ...(rect ?? {}), visibility: visible ? 'visible' : 'hidden' }}
      onLoad={() => {
        // A load, the first or the portal reloading itself, starts over: a
        // move sent before it is lost with the page that took it.
        setAwaiting(true);
        setConnected(false);
        setPending(null);
      }}
    />
  );
}
