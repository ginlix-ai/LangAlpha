/**
 * The panel under the menu bar icon: the market at a glance, the holdings'
 * day, the top of a watchlist, and what the agent is doing, in the order and
 * with the sections the user chose in Settings. Every row opens its page in
 * the main window; the panel itself navigates nowhere. What shows beside the
 * icon is chosen in Settings, not here.
 */
import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Settings } from 'lucide-react';
import { canDesktopSettings, openInMain, type PanelSectionId } from '@/lib/desktop';
import { DEFAULT_MENUBAR, DEFAULT_PANEL, useDesktopSettings } from '@/lib/desktopSettings';
import { AgentSection } from './AgentSection';
import { LiveQuotesProvider } from './liveQuotes';
import { LivenessBadge } from './LivenessBadge';
import { MarketSection } from './MarketSection';
import type { SectionProps } from './panelContext';
import { PortfolioSection } from './PortfolioSection';
import { useMarketStatusGlance, usePanelActive, useRefetchOnOpen } from './useGlanceData';
import { WatchlistSection } from './WatchlistSection';

const SECTIONS: Record<PanelSectionId, (props: SectionProps) => ReactNode> = {
  market: MarketSection,
  portfolio: PortfolioSection,
  watchlist: WatchlistSection,
  agent: AgentSection,
};

/** Where there is no shell to ask (a browser), the panel stands on the defaults. */
const DEFAULT_SETTINGS: SectionProps['settings'] = { menubar: DEFAULT_MENUBAR, panel: DEFAULT_PANEL };

export function MenubarPanel() {
  const { view, failed } = useDesktopSettings();
  // Drawn once the shell has said which sections to show, so a hidden one is never fetched.
  const settings = view ?? (failed ? DEFAULT_SETTINGS : null);
  if (!settings) return null;
  return <PanelBody settings={settings} />;
}

function PanelBody({ settings }: { settings: SectionProps['settings'] }) {
  const { t } = useTranslation();
  const active = usePanelActive();
  useRefetchOnOpen(active);
  const visible = settings.panel.sections.filter((s) => s.visible).map((s) => s.id);
  // Only the agent section goes without the session, so no status read for it alone.
  const marketStatus = useMarketStatusGlance(active && visible.some((id) => id !== 'agent'));
  const settingsPath = canDesktopSettings() ? '/settings?tab=desktop' : '/settings';

  return (
    <LiveQuotesProvider active={active}>
      <div className="mb-panel">
        {visible.map((id) => {
          const Body = SECTIONS[id];
          return <Body key={id} active={active} settings={settings} marketStatus={marketStatus} />;
        })}
        <footer className="mb-footer">
          <button type="button" className="mb-open" onClick={() => openInMain('/dashboard')}>
            {t('menubar.open')}
          </button>
          <LivenessBadge active={active} />
          <button
            type="button"
            className="mb-icon-btn"
            onClick={() => openInMain(settingsPath)}
            aria-label={t('menubar.settings')}
            title={t('menubar.settings')}
          >
            <Settings size={14} />
          </button>
        </footer>
      </div>
    </LiveQuotesProvider>
  );
}
