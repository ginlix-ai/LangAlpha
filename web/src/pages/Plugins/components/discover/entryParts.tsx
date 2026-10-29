import { useTranslation } from 'react-i18next';
import { BadgeCheck, KeyRound, LogIn, Plus } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { Loader } from '@/components/ui/loader';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { TileSize } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import type { MarketplaceEntry, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { CellAdd } from '../surface/Surface';
import { ACCESS_KEYS, CATEGORY_KEYS, entryArt } from './entryKeys';
import type { CatalogInstall } from './useCatalogInstall';
import type { DiscoverCategory } from '../../utils/marketplace';

/** The small pieces every catalog surface draws the same way: the mark, the
 *  publisher check, how it signs in, and its install control. */

export function EntryMark({
  entry,
  plugin,
  size = 'md',
  className,
}: {
  entry: MarketplaceEntry;
  plugin: PluginInfo | undefined;
  size?: TileSize;
  className?: string;
}) {
  const { art, kind } = entryArt(entry, plugin);
  return <BrandMark name={entry.name} art={art} kind={kind} size={size} className={className} />;
}

/** The check beside an entry published by the vendor itself. */
export function OfficialMark({ size = 13 }: { size?: number }) {
  const { t } = useTranslation();
  const label = t('plugins.discover.officialTitle');
  return (
    <span className="discover-official" title={label} aria-label={label} role="img">
      <BadgeCheck style={{ width: size, height: size }} />
    </span>
  );
}

/** How an entry signs in, with its glyph; null for a skill collection. */
export function AccessLabel({ auth }: { auth: MarketplaceEntry['auth'] }) {
  const { t } = useTranslation();
  if (!auth) return null;
  const Icon = auth === 'oauth' ? LogIn : auth === 'api-key' ? KeyRound : null;
  return (
    <span className="discover-access">
      {Icon && <Icon className="h-3 w-3" aria-hidden />}
      {t(ACCESS_KEYS[auth])}
    </span>
  );
}

/** "by vendor · Sign in", or "by author · Quant" for a skill collection. */
export function EntryMeta({ entry, category }: { entry: MarketplaceEntry; category: DiscoverCategory }) {
  const { t } = useTranslation();
  const parts: React.ReactNode[] = [];
  if (entry.publisher) {
    parts.push(
      <span key="by" className="discover-by">
        {t('plugins.discoverPage.by', { publisher: entry.publisher.name })}
      </span>,
    );
  }
  parts.push(
    entry.auth ? (
      <AccessLabel key="access" auth={entry.auth} />
    ) : (
      <span key="category" className="discover-access">{t(CATEGORY_KEYS[category])}</span>
    ),
  );
  return (
    <div className="discover-meta">
      {parts.map((part, i) => (
        <span key={i} className="contents">
          {i > 0 && <span className="discover-sep" aria-hidden>·</span>}
          {part}
        </span>
      ))}
    </div>
  );
}

/** An install under way, said in words: a sign-in can take minutes. */
export function InstallBusy({ entry }: { entry: MarketplaceEntry }) {
  const { t } = useTranslation();
  return (
    <span role="status" className="discover-busy">
      <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
      {t(entry.auth === 'oauth' ? 'plugins.discover.signingIn' : 'plugins.discover.installing')}
    </span>
  );
}

/** An entry's install as a labelled button, for a page's or the featured
 *  row's header: "Sign in and install" where the vendor signs in. */
export function EntryInstallButton({ entry, install }: { entry: MarketplaceEntry; install: CatalogInstall }) {
  const { t } = useTranslation();
  if (install.installing(entry)) return <InstallBusy entry={entry} />;
  const oauth = entry.auth === 'oauth';
  return (
    <HeaderButton
      variant="primary"
      icon={oauth ? LogIn : Plus}
      onClick={() => install.start(entry)}
      disabled={install.held(entry)}
      title={install.holdReason(entry)}
      // The visible label is generic; the name says which entry.
      aria-label={t(oauth ? 'plugins.discoverPage.signInInstallAria' : 'plugins.discover.installAria', {
        plugin: entry.name,
      })}
      className="transition-opacity hover:opacity-90"
    >
      {t(oauth ? 'plugins.discoverPage.signInInstall' : 'plugins.discover.install')}
    </HeaderButton>
  );
}

/** An entry's install as a cell's round "+". */
export function EntryInstallSlot({ entry, install }: { entry: MarketplaceEntry; install: CatalogInstall }) {
  const { t } = useTranslation();
  if (install.installing(entry)) return <InstallBusy entry={entry} />;
  return (
    <CellAdd
      label={t('plugins.discover.installAria', { plugin: entry.name })}
      onClick={() => install.start(entry)}
      disabled={install.held(entry)}
      reason={install.holdReason(entry)}
    />
  );
}
