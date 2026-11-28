import { lazy, Suspense, useEffect, useRef } from 'react';
import { Navigate, useLocation, useSearchParams } from 'react-router';
import { useScrollMemory } from '@/lib/scrollMemory';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useTranslation } from 'react-i18next';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { AccountPanel } from './panels/AccountPanel';
import { InvestingProfilePanel } from './panels/InvestingProfilePanel';
import { GeneralPanel } from './panels/GeneralPanel';
import { AppearancePanel } from './panels/AppearancePanel';
import { ChatPanel } from './panels/ChatPanel';
import { ArtifactsPanel } from './panels/ArtifactsPanel';
import { DesktopPanel } from './panels/DesktopPanel';
import { AgentPanel } from './panels/AgentPanel';
import { ComputersPanel } from './panels/ComputersPanel';
import { ModelsPanel } from './panels/ModelsPanel';
import { TradingPermissionSection } from './panels/TradingPermissionSection';
import { ExperimentsPanel } from './panels/ExperimentsPanel';
import { PlanPanel } from './panels/PlanPanel';
import { IntegrationsPanel } from './panels/IntegrationsPanel';
import { SettingsRail } from './components/SettingsRail';
import { usePortalHost, type PortalStatus } from '@/components/AccountPortal/PortalHost';
import { ExternalRedirect } from '@/components/ExternalRedirect';
import { PORTAL_TAB, portalUrl } from '@/components/AccountPortal/portalPaths';
import { cn } from '@/lib/utils';
import ComputersDialogHost from '@/pages/ChatAgent/components/ComputersDialogHost';
import { closeComputerSpec, closeComputersPanel, useCloseComputersOnLeave } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { readSettingsView, SECTIONS, type Section } from './sections';
import { arrivesAway, noteFramed } from './arrivedAway';
import '@/styles/pageRail.css';
import './Settings.css';

// A self-hosted server's own settings, which a hosted build never opens.
const ServerPanel = lazy(() => import('./panels/ServerPanel'));

/** A section's rows while the account is read on a cold load: a name and a
 *  line under it on the left, the control on the right, as the rows lay theirs
 *  out. */
function SettingsSkeleton() {
  const { t } = useTranslation();
  return (
    <div className="settings-card" aria-busy="true">
      <span className="sr-only">{t('common.loading')}</span>
      {[30, 22, 36, 26].map((width) => (
        <div key={width} className="settings-row" aria-hidden="true">
          <div className="settings-row-text">
            <div className="text-[0.8125rem]"><SkeletonBar style={{ width: `${width}%` }} /></div>
            <div className="mt-1 text-xs"><SkeletonBar style={{ width: `${width + 25}%` }} /></div>
          </div>
          <SkeletonBar className="h-7 w-28 rounded-md" />
        </div>
      ))}
    </div>
  );
}

function SectionBody({ section }: { section: Section }) {
  switch (section) {
    case 'account': return <AccountPanel />;
    case 'preferences': return <InvestingProfilePanel />;
    case 'plan': return <PlanPanel />;
    case 'general': return <GeneralPanel />;
    case 'appearance': return <AppearancePanel />;
    case 'chat': return <ChatPanel />;
    case 'artifacts': return <ArtifactsPanel />;
    case 'desktop': return <DesktopPanel />;
    case 'agent': return <AgentPanel />;
    case 'computers': return <ComputersPanel />;
    case 'model': return <ModelsPanel />;
    case 'server': return <Suspense fallback={<SettingsSkeleton />}><ServerPanel /></Suspense>;
    case 'integrations': return <IntegrationsPanel />;
    case 'trading': return <TradingPermissionSection />;
    case 'experiments': return <ExperimentsPanel />;
  }
}

/**
 * Where the account portal's frame is laid (components/AccountPortal/PortalHost),
 * and what shows until the portal is ready: a section's head and rows, so a
 * cold start reads as the section loading rather than an empty page.
 */
function PortalSlot({ status, setSlot, page }: { status: PortalStatus; setSlot: (el: HTMLElement | null) => void; page: string }) {
  const { t } = useTranslation();
  return (
    <div ref={setSlot} className="settings-portal-slot">
      {/* A column like a native section's, never seen: the portal's page is
          laid out in its padding, so the two line up to the pixel. */}
      <div data-portal-probe className="settings-column settings-portal-probe" aria-hidden="true" />
      {status === 'stuck' ? (
        <div className="settings-portal-status">
          <p>{t('settings:portal.unavailable')}</p>
          <a href={portalUrl(page)} target="_blank" rel="noopener noreferrer">{t('settings:portal.openInNewTab')}</a>
        </div>
      ) : status !== 'ready' && (
        <div className="settings-column" aria-busy="true">
          <header className="settings-head" aria-hidden="true">
            <div className="page-head-title"><SkeletonBar style={{ width: '28%' }} /></div>
            <div className="settings-head-def mt-2"><SkeletonBar style={{ width: '52%' }} /></div>
          </header>
          <SettingsSkeleton />
        </div>
      )}
    </div>
  );
}

/**
 * A rail of sections beside one content column, as Plugins lays out. The URL
 * is the whole state: `?tab=` the section, and a hash the setting the search
 * jumped to, which is brought into view and marked for a moment.
 */
function Settings() {
  useCloseComputersOnLeave('settings');
  const [searchParams] = useSearchParams();
  const location = useLocation();
  const { hash } = location;
  const focusSetting = (location.state as { focusSetting?: boolean } | null)?.focusSetting === true;
  const { isLoading: isUserLoading } = useUser();
  const { isLoading: isPrefsLoading } = usePreferences();
  const { t } = useTranslation();
  const portal = usePortalHost();
  const view = readSettingsView(searchParams, hash, portal !== null);
  // Arriving on a portal page where its pages do not open here goes to the
  // portal itself. A window narrowed past the phone width on one keeps the
  // app, on Settings' own first section, rather than unloading it.
  const arrivedAway = arrivesAway(view, location.key);
  useEffect(() => {
    if (view.kind === 'portal') noteFramed(location.key);
  }, [view.kind, location.key]);
  const section = view.kind === 'section' ? view.section : null;
  // Settings lists the portal's pages, so the portal starts loading as it opens.
  const want = portal?.want;
  useEffect(() => want?.(), [want]);
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, `page:settings:${section ?? PORTAL_TAB}`);
  // A dialog opened from a section (Change spec's upgrade link) would cover
  // the portal page it led to.
  useEffect(() => {
    if (view.kind !== 'portal') return;
    closeComputersPanel();
    closeComputerSpec();
  }, [view.kind]);
  const isLoading = isUserLoading || isPrefsLoading;

  // The setting is brought into view as soon as it is drawn: a section that
  // waits on its own query (Experiments, Trading permission) draws it later. Keyed on
  // the navigation, not the hash alone, so finding the same setting twice
  // marks it twice.
  useEffect(() => {
    const anchor = hash.replace(/^#/, '');
    const page = pageRef.current;
    if (isLoading || !anchor || !page) return;
    let marked: HTMLElement | null = null;
    const mark = (target: HTMLElement) => {
      marked = target;
      target.scrollIntoView?.({ block: 'center' });
      if (focusSetting) {
        // The row itself where it is the control, else the control it names
        // as its own, else in a group of choices the one picked now, else
        // the first control. A body with none of its
        // own, like the investing answers, hands focus to its group's action.
        const control = 'input, select, button, a[href], [tabindex]:not([tabindex="-1"])';
        ((target.matches(control) ? target : null)
          ?? target.querySelector<HTMLElement>('[data-search-focus]')
          ?? target.querySelector<HTMLElement>('input:checked')
          ?? target.querySelector<HTMLElement>(control)
          ?? target.closest('.settings-group')?.querySelector<HTMLElement>(control))
          ?.focus({ preventScroll: true });
      }
      // Off and on again with a layout between, so a repeat restarts the mark.
      target.removeAttribute('data-found');
      void target.offsetWidth;
      target.setAttribute('data-found', '');
    };
    const drawn = document.getElementById(anchor);
    const observer = drawn ? null : new MutationObserver(() => {
      const target = document.getElementById(anchor);
      if (!target) return;
      observer?.disconnect();
      mark(target);
    });
    if (drawn) mark(drawn);
    else observer?.observe(page, { childList: true, subtree: true });
    return () => {
      observer?.disconnect();
      marked?.removeAttribute('data-found');
    };
  }, [location.key, hash, section, isLoading, focusSetting]);

  if (view.kind === 'away') return arrivedAway ? <ExternalRedirect to={portalUrl(view.page)} /> : <Navigate to="/settings" replace />;

  return (
    <div className="settings-page">
      {/* Doubles as the window titlebar in the desktop shell; inert
          elsewhere. Above the rail and the scroll port rather than inside
          them, so it stays put once the page is scrolled. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div className="settings-body">
        <SettingsRail view={view} />
        <div ref={pageRef} className={cn('settings-scroll', view.kind === 'portal' && 'settings-scroll--portal')}>
          {view.kind === 'section' && <div className="settings-column">
            <header className="settings-head">
              <h1 className="page-head-title">{t(SECTIONS[view.section].labelKey)}</h1>
              <p className="settings-head-def">{t(SECTIONS[view.section].descKey)}</p>
            </header>
            {/* Only a cold load waits here: the app shell has usually read
                both already. The section takes the placeholder rows' place in
                a crossfade, and a section switch swaps with a short fade. */}
            <SwapCell as="div" swapKey={isLoading ? 'loading' : 'ready'}>
              {isLoading ? (
                <SettingsSkeleton />
              ) : (
                <div key={view.section} className="settings-content">
                  <SectionBody section={view.section} />
                </div>
              )}
            </SwapCell>
          </div>}
          {view.kind === 'portal' && portal && <PortalSlot status={portal.status} setSlot={portal.setSlot} page={view.page} />}
        </div>
      </div>
      {/* The Computers panel, opened from the Computers section's rows, and
          Change spec, opened over Claude Code's view in Model access, so the
          user stays where they started while the computer changes. */}
      <ComputersDialogHost overSheet planInPlace />
    </div>
  );
}

export default Settings;
