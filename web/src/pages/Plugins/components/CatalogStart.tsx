import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Compass } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useMarketplace, usePlugins } from '@/hooks/usePlugins';
import { useCatalogOn } from '../hooks/useCatalogOn';
import { useSectionNav } from '../hooks/useSectionNav';
import type { AddIntent } from '../utils/addParam';
import { inScope, SCOPE_TAB, type DiscoverScope } from '../utils/marketplace';
import { DiscoverCard } from './DiscoverCard';
import { CatalogInstallDialogs, InstallNotice, useCatalogInstall } from './discover/useCatalogInstall';
import { ADD_ROUTES, type InstallSection } from './shell/sections';
import './discover/discover.css';

/** Enough to choose from without turning the first visit into the catalog. */
const SHOWN = 4;

const SECTION_OF: Record<DiscoverScope, InstallSection> = {
  plugins: 'plugins',
  connectors: 'mcp',
  skills: 'skills',
};

const LEAD_KEYS: Record<DiscoverScope, string> = {
  plugins: 'plugins.start.lead.plugins',
  connectors: 'plugins.start.lead.connectors',
  skills: 'plugins.start.lead.skills',
};

const NO_CATALOG_LEAD_KEYS: Record<DiscoverScope, string> = {
  plugins: 'plugins.start.leadNoCatalog.plugins',
  connectors: 'plugins.start.leadNoCatalog.connectors',
  skills: 'plugins.start.leadNoCatalog.skills',
};

/**
 * A section with nothing of the user's in it yet starts from the catalog: a
 * line on where most things come from, a few entries that install from here,
 * the way into the rest, and then the routes for what the catalog does not
 * have. The built-in groups, when there are any, follow it unchanged. On a
 * server with no catalog the routes are the start, and the line says so. The
 * line waits for the server's answer, so it never names a catalog that the
 * server turns out not to have.
 *
 * The picks are what Discover features, the publishers' own entries the
 * account has not installed, so the first visit and the catalog agree.
 */
export function CatalogStart({
  scope,
  onAdd,
}: {
  scope: DiscoverScope;
  /** The section's own add flows: the Add menu's routes, here as well. */
  onAdd: (intent: AddIntent) => void;
}) {
  const { t } = useTranslation();
  const { openDetail, openDiscover } = useSectionNav();
  const { data } = useMarketplace();
  const catalogOn = useCatalogOn();
  const { data: pluginList } = usePlugins();
  // A sign-in an install starts here comes back to this same section. The
  // report's way forward is the installed plugin's page.
  const install = useCatalogInstall(`/plugins?tab=${SCOPE_TAB[scope]}`, {
    onOpenPlugin: (name) => openDetail({ kind: 'plugin', name }),
  });

  // Chosen once: an entry installed from here stays, now with its check,
  // rather than the row reshuffling under the pointer.
  const [pickNames, setPickNames] = useState<string[] | null>(null);
  if (pickNames === null && data?.configured) {
    const pool = data.entries.filter((e) => inScope(e, scope) && !e.installed);
    setPickNames(
      [...pool.filter((e) => e.publisher?.official), ...pool.filter((e) => !e.publisher?.official)]
        .slice(0, SHOWN)
        .map((e) => e.name),
    );
  }
  const picks = (pickNames ?? []).flatMap((name) => data?.entries.find((e) => e.name === name) ?? []);
  const pluginByName = new Map((pluginList?.plugins ?? []).map((p) => [p.name, p]));
  const failed = picks.find((e) => e.name === install.failure?.entry && !e.installed);

  return (
    <section
      className="catalog-start"
      aria-labelledby={catalogOn === null ? undefined : `catalog-start-${scope}`}
    >
      {catalogOn !== null && (
        <h2 id={`catalog-start-${scope}`} className="catalog-start-lead">
          {t((catalogOn ? LEAD_KEYS : NO_CATALOG_LEAD_KEYS)[scope])}
        </h2>
      )}
      {picks.length > 0 && (
        <div className="surface-grid discover-grid catalog-start-grid">
          {picks.map((entry) => (
            <DiscoverCard
              key={entry.name}
              entry={entry}
              plugin={entry.plugin ? pluginByName.get(entry.plugin) : undefined}
              install={install}
              onOpen={() => openDiscover(scope, entry.name)}
            />
          ))}
        </div>
      )}
      {/* An install that did not happen says why here, under the card it was
          started from, with its retry: Discover's entry page would hold it,
          but that page reads its own install state, not this one's. */}
      {failed && (
        <div className="catalog-start-notice">
          <InstallNotice install={install} entry={failed} />
        </div>
      )}
      <div className="catalog-start-foot">
        {catalogOn && (
          <HeaderButton variant="primary" icon={Compass} onClick={() => openDiscover(scope)}>
            {t('plugins.start.browse')}
          </HeaderButton>
        )}
        <div className="catalog-start-routes">
          {ADD_ROUTES[SECTION_OF[scope]].map((route) => (
            <button key={route.intent} type="button" onClick={() => onAdd(route.intent)}>
              {t(route.key)}
            </button>
          ))}
        </div>
      </div>
      <CatalogInstallDialogs install={install} />
    </section>
  );
}
