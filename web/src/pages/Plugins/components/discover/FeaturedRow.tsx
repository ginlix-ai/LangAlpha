import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';
import type { CatalogServer, MarketplaceEntry, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { readRequires } from '../../utils/marketplace';
import { pluginStatus } from '../pluginsPage/pluginStatus';
import { SectionLink } from '../surface/Surface';
import type { CatalogInstall } from './useCatalogInstall';
import { EntryInstallButton, EntryMark, OfficialMark } from './entryParts';

const MINE_NAMED = 3;

/**
 * The top of Discover: one entry worth a look, with its install one click
 * away, and, where the column has room, what already came from the catalog,
 * so the page answers "what do I have" before "what else is there".
 */
export function FeaturedRow({
  featured,
  mine,
  pluginOf,
  servers,
  install,
  onOpen,
  onManage,
}: {
  featured: MarketplaceEntry;
  /** Installed entries in this view, in catalog order. */
  mine: MarketplaceEntry[];
  /** The installed plugin that is an entry. */
  pluginOf: (entry: MarketplaceEntry) => PluginInfo | undefined;
  /** The connector catalog, for what the installed entries need. */
  servers?: CatalogServer[];
  install: CatalogInstall;
  onOpen: (entry: MarketplaceEntry) => void;
  /** Back to Yours, where installed plugins are managed. */
  onManage: () => void;
}) {
  const { t } = useTranslation();
  const requires = readRequires(featured.requires);
  const named = mine.slice(0, MINE_NAMED).map((e) => e.name);
  const rest = mine.length - named.length;
  // Counted the way Yours and the rail count attention, so a broken install
  // is not counted here as simply had, nor an unchecked one as broken.
  const needing = servers
    ? mine.filter((e) => {
        const plugin = pluginOf(e);
        return !!plugin && pluginStatus(plugin, servers).needsUser;
      })
    : [];

  return (
    <div className={mine.length > 0 ? 'discover-top has-mine' : 'discover-top'}>
      <section className="discover-feature" data-testid={`discover-featured-${featured.name}`}>
        <EntryMark
          entry={featured}
          plugin={pluginOf(featured)}
          size="lg"
          className="h-12 w-12 rounded-[11px]"
        />
        <div className="min-w-0">
          <div className="discover-feature-name">
            <button type="button" className="surface-cell-open" onClick={() => onOpen(featured)}>
              <span className="surface-cell-name">{featured.name}</span>
            </button>
            {featured.publisher?.official && <OfficialMark size={14} />}
            <span className="discover-feature-kick">{t('plugins.discoverPage.featured')}</span>
          </div>
          {featured.description && <div className="discover-feature-desc">{featured.description}</div>}
          <div className="discover-meta">
            {featured.publisher && (
              <span className="discover-by">
                {t('plugins.discoverPage.by', { publisher: featured.publisher.name })}
              </span>
            )}
            {requires && !requires.nothing && (
              <>
                {featured.publisher && <span className="discover-sep" aria-hidden>·</span>}
                <span className="discover-by">
                  {t('plugins.discoverPage.requires', { what: requires.text })}
                </span>
              </>
            )}
          </div>
        </div>
        <div className="discover-feature-action">
          <EntryInstallButton entry={featured} install={install} />
        </div>
      </section>
      {mine.length > 0 && (
        <aside className="discover-mine">
          <div className="discover-mine-label">
            {t('plugins.discoverPage.installedFromCatalog')}
            <span className="surface-count">{mine.length}</span>
          </div>
          <div className="discover-mine-stack">
            {mine.map((e) => (
              <span key={e.name} title={e.name}>
                <EntryMark entry={e} plugin={pluginOf(e)} size="sm" />
              </span>
            ))}
          </div>
          <div className="discover-mine-names">
            {named.join(t('plugins.discover.listSeparator'))}
            {rest > 0 && (
              <span className="is-faint"> {t('plugins.discoverPage.andMore', { count: rest })}</span>
            )}
          </div>
          {needing.length > 0 && (
            <button
              type="button"
              className="discover-mine-attn"
              // One opens its own page; several are listed together in Yours.
              onClick={() => (needing.length === 1 ? onOpen(needing[0]) : onManage())}
            >
              <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
              {needing.length === 1
                ? t('plugins.discoverPage.mineAttentionOne', { name: needing[0].name })
                : t('plugins.discoverPage.mineAttention', { count: needing.length })}
            </button>
          )}
          <SectionLink onClick={onManage}>{t('plugins.discoverPage.manageInYours')}</SectionLink>
        </aside>
      )}
    </div>
  );
}
