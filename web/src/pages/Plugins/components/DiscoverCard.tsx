import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import type { CatalogServer, MarketplaceEntry, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { CellState, StatusDot } from './surface/Surface';
import { EntryInstallSlot, EntryMark, EntryMeta, OfficialMark } from './discover/entryParts';
import type { CatalogInstall } from './discover/useCatalogInstall';
import { pluginStatus, statusLine } from './pluginsPage/pluginStatus';
import { entryCategory } from '../utils/marketplace';

/**
 * One catalog entry on Discover: the vendor's mark, its name with the
 * publisher check, two lines of what it is, who publishes it and how it signs
 * in, and one uniform "+" (a check once installed). The card opens the
 * entry's page; the "+" installs without leaving the grid.
 *
 * An installed entry with a problem says so in its own list cell's words,
 * in place of the publisher line, and that line opens the plugin's page,
 * where the fix is. A check there would call a broken plugin healthy. One the
 * catalog holds a newer version of says that the same way, beside its check,
 * and opens the page whose Update brings it in.
 */
export function DiscoverCard({
  entry,
  plugin,
  servers,
  install,
  onOpen,
}: {
  entry: MarketplaceEntry;
  /** The installed plugin that is this entry, whose mark the card wears. */
  plugin: PluginInfo | undefined;
  /** The connector catalog, for an installed plugin's state; undefined until
   *  it loads, when the card shows plain "Installed". */
  servers?: CatalogServer[];
  install: CatalogInstall;
  /** The entry's catalog page, or the installed plugin's page. */
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const status = entry.installed && plugin && servers ? pluginStatus(plugin, servers) : null;
  const update = entry.installed && entry.update_available;
  const line = plugin && status && (status.problem || update) ? statusLine(t, plugin, status, update) : null;
  let trailing: React.ReactNode;
  if (install.installing(entry) || !entry.installed) {
    // Ahead of installed: an install waiting on its sign-in is already in the
    // listing, and is not done until the sign-in is.
    trailing = <EntryInstallSlot entry={entry} install={install} />;
  } else if (line?.warn) {
    trailing = <CellState state="warn" label={line.text} />;
  } else if (line && status?.needsUser) {
    trailing = (
      <span className="discover-card-attn" title={line.text}>
        <StatusDot tone="attn" />
      </span>
    );
  } else if (plugin && !plugin.enabled) {
    trailing = <CellState state="off" />;
  } else {
    trailing = <CellState state="on" label={t('plugins.discover.installed')} />;
  }

  return (
    <article className="discover-card" data-testid={`discover-card-${entry.name}`}>
      <div className="surface-cell-mark">
        <EntryMark entry={entry} plugin={plugin} />
      </div>
      <div className="surface-cell-text">
        <div className="surface-cell-l1">
          <button type="button" className="surface-cell-open" onClick={onOpen}>
            <span className="surface-cell-name">{entry.name}</span>
          </button>
          {entry.publisher?.official && <OfficialMark />}
        </div>
        {entry.description && (
          <p className="surface-cell-l2 is-two discover-card-desc">{entry.description}</p>
        )}
        {line ? (
          <button
            type="button"
            className={cn('discover-card-state', line.warn && 'is-warn')}
            onClick={onOpen}
          >
            {line.text}
          </button>
        ) : (
          <EntryMeta entry={entry} category={entryCategory(entry)} />
        )}
      </div>
      <div className="surface-cell-end">{trailing}</div>
    </article>
  );
}
