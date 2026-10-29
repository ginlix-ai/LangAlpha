import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { ChevronRight } from 'lucide-react';
import { useMcpCatalog } from '@/hooks/useMcpServers';
import { usePlugins } from '@/hooks/usePlugins';
import type { PluginInfo } from '@/pages/ChatAgent/utils/api';
import { groupBy, matchesFilter } from '../utils/groupOrigins';
import { hasLifecycle } from '../utils/pluginSurface';
import { pluginRowKey } from '../utils/rowKeys';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { usePluginBulkActions } from '../hooks/usePluginBulkActions';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useSectionNav } from '../hooks/useSectionNav';
import { useSignInOffer } from '../hooks/useSignInOffer';
import { useVendorOf } from '../hooks/useVendorOf';
import { useWizardSignInRetry } from '../hooks/useSignInRetry';
import { CatalogStart } from './CatalogStart';
import { PluginCard } from './PluginCard';
import { PluginDetail } from './PluginDetail';
import { PluginInstallWizard } from './PluginInstallWizard';
import { PluginListFrame } from './PluginListFrame';
import { CellModeProvider, SurfaceGrid, SurfaceSection } from './surface/Surface';
import { pluginOrigin, pluginStatus } from './pluginsPage/pluginStatus';
import { connectBlock } from '../brokerages';
import { connectRequestFor } from '../hooks/useMcpOauthActions';
import { useCatalogPlugins } from '../hooks/useCatalogPlugins';
import { useCatalogOn } from '../hooks/useCatalogOn';
import './pluginsPage/pluginsPage.css';

/**
 * Plugins → Yours: every package on the account as a cell, grouped by how it
 * got here. What needs the user comes first; then what they installed from
 * the catalog and what they uploaded, beside what ships with the app once the
 * column is wide enough that one grid would leave half a screen empty.
 *
 * Bundles are packages of the same shape and read as ordinary cells, but they
 * were never installed: nothing uninstalls them and they hold no slot against
 * the cap. Switching one off is the one verb they answer.
 */

const RETURN_TO = '/plugins?tab=plugins';

export function PluginsList() {
  const { t } = useTranslation();
  const { openDiscover } = useSectionNav();
  const pluginsQuery = usePlugins();
  const { data, isLoading, error } = pluginsQuery;
  const { data: catalog } = useMcpCatalog();
  const catalogPlugins = useCatalogPlugins();
  const catalogOn = useCatalogOn();
  const vendorOf = useVendorOf();
  const [wizardOpen, setWizardOpen] = useState(false);
  // A switch refused for want of a connection offers the sign-in, and a
  // cell's own Connect starts it; both come back here.
  const signIn = useSignInOffer(RETURN_TO);

  useAddIntent({ plugin: () => setWizardOpen(true) });
  // An install the wizard sent to a vendor's sign-in that came back unfinished.
  const signInRetry = useWizardSignInRetry(() => setWizardOpen(true));

  const plugins = data?.plugins ?? [];
  const servers = catalog?.servers ?? [];
  const maxPlugins = data?.max_plugins ?? 0;
  // The cap counts what the user installed. Bundles arrive with the app and
  // cannot be uninstalled, so counting them would spend an allowance nobody
  // asked for.
  const installed = plugins.filter(hasLifecycle);
  const atCap = maxPlugins > 0 && installed.length >= maxPlugins;
  const nearCap = maxPlugins > 0 && installed.length >= Math.floor(maxPlugins * 0.8);

  const detail = useDetailParam<PluginInfo>(
    'plugin',
    (ref) => plugins.find((p) => p.name === ref.name) ?? null,
    !isLoading && data !== undefined,
  );
  const detailPlugin = detail.target;

  // The rail's rule: a failed check, a missing key or a sign-in something
  // asked for, on a plugin that is on. A connector nobody has checked yet is
  // not a problem, and a switched-off plugin has nothing to fix until it is
  // back on.
  const needsUser = (p: PluginInfo) => pluginStatus(p, servers).needsUser;
  const list = usePluginListSurface({
    rows: [...plugins].sort((a, b) => a.name.localeCompare(b.name)),
    matches: (p, query) => matchesFilter(query, p.name, p.description, p.author, p.source_ref),
    key: pluginRowKey,
    name: (p) => p.name,
    isOn: (p) => p.enabled,
    needsUser,
    justAdded: 'plugin',
    // Nothing installed yet: what ships with the app is not the user's.
    startWhen: !!data && installed.length === 0,
  });
  const { visible } = list;

  const attention = visible.filter(needsUser);
  const byOrigin = groupBy(
    visible.filter((p) => !needsUser(p)),
    (p) => pluginOrigin(p, catalogPlugins),
  );
  const fromCatalog = byOrigin.get('catalog') ?? [];
  const fromGit = byOrigin.get('git') ?? [];
  const uploaded = byOrigin.get('uploaded') ?? [];
  const builtIn = byOrigin.get('builtIn') ?? [];

  function renderCell(plugin: PluginInfo) {
    const status = pluginStatus(plugin, servers);
    return (
      <CellModeProvider key={plugin.name} value={list.cellMode(plugin)}>
        <PluginCard
          plugin={plugin}
          status={status}
          onOpen={() => detail.open(plugin.name)}
          onConnect={() => {
            if (status.server) signIn.oauth.connect(connectRequestFor(status.server));
          }}
          connecting={
            !!status.server &&
            (signIn.oauth.connectingName === status.server.name ||
              connectBlock(vendorOf(status.server.url)) !== null)
          }
          onSignInNeeded={signIn.offer}
        />
      </CellModeProvider>
    );
  }

  function section(title: string, rows: PluginInfo[], extra?: { attention?: boolean }): ReactNode {
    if (rows.length === 0) return null;
    return (
      <SurfaceSection title={title} count={rows.length} attention={extra?.attention}>
        <SurfaceGrid>{rows.map(renderCell)}</SurfaceGrid>
      </SurfaceSection>
    );
  }

  const bulk = usePluginBulkActions(visible, list, signIn.start);
  const hasLeft = fromCatalog.length + fromGit.length + uploaded.length > 0;

  return (
    <>
      <PluginListFrame
        list={list}
        query={pluginsQuery}
        loadFailed={t('plugins.loadFailed.plugins')}
        noun={t('plugins.filter.nounPlugins')}
        catalogScope="plugins"
        bulk={bulk}
        hidden={!!detailPlugin}
        className="pp-list"
        footer={
          !isLoading && !error && plugins.length > 0 && !list.showStart && (nearCap || catalogOn) && (
            <div className="pp-foot">
              {/* The limit is only worth a word when it is close, as on Connectors;
                  "6 of 50" read beside Discover's catalog count looks like one. */}
              <span title={atCap ? t('plugins.list.atCap', { max: maxPlugins }) : undefined}>
                {nearCap ? t('plugins.pluginsPage.limit', { count: installed.length, max: maxPlugins }) : null}
              </span>
              {catalogOn && (
                <button type="button" onClick={() => openDiscover('plugins')}>
                  {t('plugins.pluginsPage.discoverMore')}
                  <ChevronRight className="h-3.5 w-3.5" aria-hidden />
                </button>
              )}
            </div>
          )
        }
      >
        {list.showStart && <CatalogStart scope="plugins" onAdd={() => setWizardOpen(true)} />}
        {section(t('plugins.pluginsPage.needsAttention'), attention, { attention: true })}
        {(hasLeft || builtIn.length > 0) && (
          <div className={hasLeft && builtIn.length > 0 ? 'pp-pair is-split' : 'pp-pair'}>
            {hasLeft && (
              <div className="pp-pair-col">
                {section(t('plugins.pluginsPage.fromCatalog'), fromCatalog)}
                {section(t('plugins.pluginsPage.sourceGit'), fromGit)}
                {section(t('plugins.pluginsPage.uploaded'), uploaded)}
              </div>
            )}
            {builtIn.length > 0 && (
              <div className="pp-pair-col">{section(t('plugins.pluginsPage.builtIn'), builtIn)}</div>
            )}
          </div>
        )}
      </PluginListFrame>

      {detailPlugin && (
        <PluginDetail
          key={detailPlugin.name}
          plugin={detailPlugin}
          onClose={detail.close}
          onSignInNeeded={signIn.offer}
        />
      )}

      <AnimatePresence>
        {wizardOpen && (
          <PluginInstallWizard
            initial={signInRetry.initial}
            onClose={() => {
              signInRetry.clear();
              setWizardOpen(false);
            }}
          />
        )}
      </AnimatePresence>
    </>
  );
}
