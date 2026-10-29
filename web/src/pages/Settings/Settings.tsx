import { useEffect, useRef } from 'react';
import { useLocation, useSearchParams } from 'react-router';
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
import { AgentPanel } from './panels/AgentPanel';
import { ModelsPanel } from './panels/ModelsPanel';
import { TradingPermissionSection } from './panels/TradingPermissionSection';
import { ExperimentsPanel } from './panels/ExperimentsPanel';
import { PlanPanel } from './panels/PlanPanel';
import { IntegrationsPanel } from './panels/IntegrationsPanel';
import { SettingsRail } from './components/SettingsRail';
import { readSection, SECTIONS, type Section } from './sections';
import './Settings.css';

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
    case 'agent': return <AgentPanel />;
    case 'model': return <ModelsPanel />;
    case 'integrations': return <IntegrationsPanel />;
    case 'trading': return <TradingPermissionSection />;
    case 'experiments': return <ExperimentsPanel />;
  }
}

/**
 * A rail of sections beside one content column, as Plugins lays out. The URL
 * is the whole state: `?tab=` the section, and a hash the setting the search
 * jumped to, which is brought into view and marked for a moment.
 */
function Settings() {
  const [searchParams] = useSearchParams();
  const location = useLocation();
  const { hash } = location;
  const focusSetting = (location.state as { focusSetting?: boolean } | null)?.focusSetting === true;
  const { isLoading: isUserLoading } = useUser();
  const { isLoading: isPrefsLoading } = usePreferences();
  const { t } = useTranslation();
  const section = readSection(searchParams, hash);
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, `page:settings:${section}`);
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
        // The control a row names as its own, else in a group of choices the
        // one picked now, else the first control. A body with none of its
        // own, like the investing answers, hands focus to its group's action.
        const control = 'input, select, button, a[href], [tabindex]:not([tabindex="-1"])';
        (target.querySelector<HTMLElement>('[data-search-focus]')
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

  return (
    <div className="settings-page">
      {/* Doubles as the window titlebar in the desktop shell; inert
          elsewhere. Above the rail and the scroll port rather than inside
          them, so it stays put once the page is scrolled. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div className="settings-body">
        <SettingsRail section={section} />
        <div ref={pageRef} className="settings-scroll">
          <div className="settings-column">
            <header className="settings-head">
              <h1 className="settings-head-title">{t(SECTIONS[section].labelKey)}</h1>
              <p className="settings-head-def">{t(SECTIONS[section].descKey)}</p>
            </header>
            {/* Only a cold load waits here: the app shell has usually read
                both already. The section takes the placeholder rows' place in
                a crossfade, and a section switch swaps with a short fade. */}
            <SwapCell as="div" swapKey={isLoading ? 'loading' : 'ready'}>
              {isLoading ? (
                <SettingsSkeleton />
              ) : (
                <div key={section} className="settings-content">
                  <SectionBody section={section} />
                </div>
              )}
            </SwapCell>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Settings;
