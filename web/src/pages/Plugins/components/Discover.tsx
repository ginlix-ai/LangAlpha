import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router';
import { ExternalLink, X } from 'lucide-react';
import { HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { useMcpCatalog } from '@/hooks/useMcpServers';
import { useMarketplace, usePlugins } from '@/hooks/usePlugins';
import { useLocale } from '@/hooks/useLocale';
import {
  formatApiErrorDetail,
  type MarketplaceEntry,
  type MarketplaceTerm,
  type PluginInfo,
} from '@/pages/ChatAgent/utils/api';
import { useDetailParam } from '../hooks/useDetailParam';
import { VIEW_PARAM } from '../utils/detailParam';
import {
  addsConnector,
  categoryCounts,
  entryCategory,
  featuredEntries,
  inScope,
  matchesQuery,
  namesRegion,
  SCOPE_TAB,
  termLabel,
  usedRegionTerms,
  type DiscoverCategory,
  type DiscoverScope,
} from '../utils/marketplace';
import { webLink } from '../utils/webLink';
import { DetailTrailStep } from './DetailPage';
import { DiscoverCard } from './DiscoverCard';
import { EmptyState } from './EmptyState';
import { PluginDetail } from './PluginDetail';
import { SectionLink, SurfaceGrid, SurfaceSection } from './surface/Surface';
import { CatalogDetail } from './discover/CatalogDetail';
import { CATEGORY_KEYS } from './discover/entryKeys';
import { FeaturedRow } from './discover/FeaturedRow';
import { CatalogInstallDialogs, useCatalogInstall } from './discover/useCatalogInstall';
import './discover/discover.css';

/**
 * Discover: the operator's plugin catalog, seen from the section it opens in.
 * Plugins shows every entry, Connectors the data vendors (entries that add a
 * connector), Skills the skill collections. Cards install in one click with
 * the same validate-first flow everywhere (`useCatalogInstall`); a card opens
 * the entry's page, or the installed plugin's page once it is installed.
 */

/** Rows each section shows on Plugins before "Show all": both kinds fit the
 *  first screen. The single-kind views show everything. */
const FOLD_ROWS = 2;
const RELATED = 6;

type SectionId = 'vendors' | 'collections';

export function Discover({ scope = 'plugins' }: { scope?: DiscoverScope }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data, isLoading, error, refetch } = useMarketplace();
  const { data: pluginList } = usePlugins();
  const { data: mcpCatalog } = useMcpCatalog();
  const [chip, setChip] = useState<string | null>(null);
  const [unfolded, setUnfolded] = useState<ReadonlySet<SectionId>>(() => new Set());

  const pluginByName = new Map((pluginList?.plugins ?? []).map((p) => [p.name, p]));
  const pluginOf = (entry: MarketplaceEntry) => (entry.plugin ? pluginByName.get(entry.plugin) : undefined);
  // An installed entry opens the same page Plugins does, here, so leaving it
  // comes back to the catalog as it was.
  const pluginDetail = useDetailParam<PluginInfo>(
    'plugin',
    (ref) => pluginList?.plugins.find((p) => p.name === ref.name) ?? null,
    pluginList !== undefined,
  );
  const catalogDetail = useDetailParam<MarketplaceEntry>(
    'catalog',
    (ref) => data?.entries.find((e) => e.name === ref.name) ?? null,
    data !== undefined,
  );
  const detailOpen = !!pluginDetail.target || !!catalogDetail.target;
  // A sign-in started here comes back to this same view. An install that
  // does not happen opens the entry's page, which says why beside Install.
  const install = useCatalogInstall(`/plugins?tab=${SCOPE_TAB[scope]}&${VIEW_PARAM}=discover`, {
    onFailed: (name) => {
      if (catalogDetail.target?.name !== name) catalogDetail.open(name);
    },
    onOpenPlugin: (name) => pluginDetail.open(name),
  });
  // The order the catalog first showed in, held for this visit: a card that
  // is installed here stays where the user found it rather than jumping to
  // the end under their cursor. Installed entries sink on the next visit.
  const [sunk, setSunk] = useState<ReadonlySet<string> | null>(null);
  if (sunk === null && data) {
    setSunk(new Set(data.entries.filter((e) => e.installed).map((e) => e.name)));
  }

  function openEntry(entry: MarketplaceEntry) {
    const plugin = pluginOf(entry);
    if (entry.installed && plugin) pluginDetail.open(plugin.name);
    else catalogDetail.open(entry.name);
  }

  function setParam(key: string, value: string | null) {
    const next = new URLSearchParams(searchParams);
    if (value === null) next.delete(key);
    else next.set(key, value);
    setSearchParams(next, { replace: true });
  }

  // A failed refetch keeps the answer it already had, so only a load that
  // never answered is an error screen.
  if (!data) {
    if (isLoading || !error) return <ListSkeleton />;
    return (
      <ListError>
        {t('plugins.discover.loadFailed')} {formatApiErrorDetail(error)}{' '}
        <button type="button" onClick={() => void refetch()} className="underline underline-offset-2">
          {t('common.retry')}
        </button>
      </ListError>
    );
  }
  // Not reached: on a server with no catalog the page shows Yours instead.
  if (!data.configured) return null;

  const query = searchParams.get('q')?.trim() ?? '';
  const scoped = data.entries.filter((e) => inScope(e, scope));
  const searched = scoped.filter((e) => matchesQuery(e, query));

  // Chips: the categories a view holds, or, on Connectors, where every entry
  // is a data vendor, the markets they name.
  const chips: { key: string; label: string; count: number }[] =
    scope === 'connectors'
      ? usedRegionTerms(scoped, data.vocabulary.regions).map((term: MarketplaceTerm) => ({
          key: term.key,
          label: termLabel(term, locale),
          count: searched.filter((e) => namesRegion(e, term.key)).length,
        }))
      : categoryCounts(searched).map(([c, count]) => ({
          key: c,
          label: t(CATEGORY_KEYS[c]),
          count,
        }));
  const shownChips = chips.filter((c) => c.count > 0 || c.key === chip);
  const chipMatch = (e: MarketplaceEntry) =>
    !chip ||
    (scope === 'connectors' ? namesRegion(e, chip) : entryCategory(e) === (chip as DiscoverCategory));
  const visible = searched.filter(chipMatch);
  const narrowed = query !== '' || chip !== null;

  // Installed sinks to the end: what the user already has is not news.
  const sinks = (e: MarketplaceEntry) => (sunk ? sunk.has(e.name) : e.installed);
  const ordered = [...visible.filter((e) => !sinks(e)), ...visible.filter(sinks)];
  const featured = narrowed ? undefined : featuredEntries(scoped, 1)[0];
  const mine = scoped.filter((e) => e.installed);
  // The featured entry is not repeated under it; its section still counts it.
  const sections = [
    { id: 'vendors' as SectionId, title: t('plugins.discoverPage.sectionVendors'), all: ordered.filter(addsConnector) },
    {
      id: 'collections' as SectionId,
      title: t('plugins.discoverPage.sectionCollections'),
      all: ordered.filter((e) => !addsConnector(e)),
    },
  ]
    .map((s) => ({ ...s, rows: s.all.filter((e) => e !== featured) }))
    .filter((s) => s.rows.length > 0);
  const foldable = scope === 'plugins' && !narrowed;
  const catalogUrl = webLink(data.url);

  function toggleFold(id: SectionId) {
    setUnfolded((prev) => {
      const next = new Set(prev);
      if (!next.delete(id)) next.add(id);
      return next;
    });
  }

  let content: React.ReactNode;
  if (scoped.length === 0) {
    content = (
      <EmptyState
        message={
          scope === 'connectors'
            ? t('plugins.discoverPage.emptyVendors')
            : scope === 'skills'
              ? t('plugins.discoverPage.emptyCollections')
              : t('plugins.discover.emptyCatalog')
        }
      />
    );
  } else if (visible.length === 0) {
    content = (
      <EmptyState
        message={
          query
            ? t('plugins.discoverPage.emptyQuery', { query })
            : t('plugins.discoverPage.emptyFilter')
        }
        action={
          <div className="flex items-center gap-2">
            {query && (
              <HeaderButton icon={X} onClick={() => setParam('q', null)}>
                {t('plugins.discoverPage.clearSearch')}
              </HeaderButton>
            )}
            {chip && (
              <HeaderButton onClick={() => setChip(null)}>
                {t('plugins.discoverPage.showEverything')}
              </HeaderButton>
            )}
          </div>
        }
      />
    );
  } else {
    content = sections.map((section) => {
      const isUnfolded = unfolded.has(section.id);
      const canFold = foldable && section.rows.length > FOLD_ROWS * 2;
      return (
        <SurfaceSection
          key={section.id}
          title={section.title}
          count={section.all.length}
          action={
            canFold ? (
              <SectionLink onClick={() => toggleFold(section.id)}>
                {isUnfolded ? t('plugins.discoverPage.showLess') : t('plugins.discoverPage.showAll')}
              </SectionLink>
            ) : undefined
          }
        >
          <SurfaceGrid
            className="discover-grid"
            rows={canFold ? FOLD_ROWS : undefined}
            expanded={isUnfolded}
            onExpandedChange={(next) => {
              if (next !== isUnfolded) toggleFold(section.id);
            }}
            hiddenNames={section.rows.map((e) => e.name)}
          >
            {section.rows.map((entry) => (
              <DiscoverCard
                key={entry.name}
                entry={entry}
                plugin={pluginOf(entry)}
                servers={mcpCatalog?.servers}
                install={install}
                onOpen={() => openEntry(entry)}
              />
            ))}
          </SurfaceGrid>
        </SurfaceSection>
      );
    });
  }

  const target = catalogDetail.target;
  const related = target
    ? data.entries
        .filter(
          (e) => e !== target && !e.installed && addsConnector(e) === addsConnector(target),
        )
        .slice(0, RELATED)
    : [];

  return (
    <div className="discover">
      <div hidden={detailOpen}>
        {shownChips.length > 1 && (
          <div
            className="discover-chips"
            role="group"
            aria-label={
              scope === 'connectors'
                ? t('plugins.discoverPage.marketsLabel')
                : t('plugins.discoverPage.chipsLabel')
            }
          >
            <button
              type="button"
              className={chip === null ? 'surface-chip is-on' : 'surface-chip'}
              aria-pressed={chip === null}
              onClick={() => setChip(null)}
            >
              {t('plugins.discoverPage.all')}
              <span className="surface-count">{searched.length}</span>
            </button>
            {shownChips.map((c) => (
              <button
                key={c.key}
                type="button"
                className={chip === c.key ? 'surface-chip is-on' : 'surface-chip'}
                aria-pressed={chip === c.key}
                onClick={() => setChip(chip === c.key ? null : c.key)}
              >
                {c.label}
                <span className="surface-count">{c.count}</span>
              </button>
            ))}
          </div>
        )}

        {featured && (
          <FeaturedRow
            featured={featured}
            mine={mine}
            pluginOf={pluginOf}
            servers={mcpCatalog?.servers}
            install={install}
            onOpen={openEntry}
            onManage={() => setParam(VIEW_PARAM, null)}
          />
        )}

        {content}

        <footer className="discover-foot">
          <span>{data.description}</span>
          {catalogUrl && (
            <a href={catalogUrl} target="_blank" rel="noopener noreferrer">
              {t('plugins.discover.viewCatalog')}
              <ExternalLink className="h-3 w-3" aria-hidden />
            </a>
          )}
        </footer>
      </div>

      {target && (
        <CatalogDetail
          key={target.name}
          entry={target}
          plugin={pluginOf(target)}
          vocabulary={data.vocabulary}
          related={related}
          pluginOf={pluginOf}
          install={install}
          onClose={catalogDetail.close}
          onOpenEntry={(name) => catalogDetail.open(name)}
          onOpenPlugin={(name) => pluginDetail.open(name)}
        />
      )}
      {pluginDetail.target && (
        <DetailTrailStep label={t('plugins.discoverPage.discover')} onClick={pluginDetail.close}>
          <PluginDetail
            key={pluginDetail.target.name}
            plugin={pluginDetail.target}
            onClose={pluginDetail.close}
            onSignInNeeded={install.offerSignIn}
          />
        </DetailTrailStep>
      )}
      <CatalogInstallDialogs install={install} />
    </div>
  );
}
