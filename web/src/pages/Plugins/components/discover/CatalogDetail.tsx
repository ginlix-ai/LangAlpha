import { useId } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Check, CreditCard, ExternalLink, KeyRound, LogIn, Sparkles } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import type {
  MarketplaceEntry,
  MarketplaceVocabulary,
  PluginInfo,
} from '@/pages/ChatAgent/utils/api';
import {
  addsConnector,
  coverageLine,
  dataLabels,
  entryCategory,
  readRequires,
} from '../../utils/marketplace';
import { webLink } from '../../utils/webLink';
import {
  DetailAsideBlock,
  DetailFacts,
  DetailHeader,
  DetailPage,
  DetailSection,
  DetailTrailStep,
} from '../DetailPage';
import { MetaLine } from '../pluginsPage/parts';
import { SurfaceCell, SurfaceGrid } from '../surface/Surface';
import { InstallNotice, type CatalogInstall } from './useCatalogInstall';
import { accessKey, CATEGORY_KEYS, entryArt } from './entryKeys';
import { EntryInstallButton, EntryInstallSlot, EntryMark, OfficialMark } from './entryParts';
import './discover.css';

/**
 * A catalog entry's own page: who publishes it, what installing it adds, what
 * it needs to work, and the install itself in the header. Everything here is
 * what the catalog says. An entry whose files ship in the catalog repository
 * also says what those files declare (its connectors, skills and keys); one
 * that lives upstream is not fetched just to be listed, so the page says
 * where its parts will appear rather than guessing their names.
 *
 * Once installed, the parts are the ones the plugin holds, and the copy
 * speaks of what is there and points to the plugin's page rather than
 * describing an install that already happened.
 */
export function CatalogDetail({
  entry,
  plugin,
  vocabulary,
  related,
  pluginOf,
  install,
  onClose,
  onOpenEntry,
  onOpenPlugin,
}: {
  entry: MarketplaceEntry;
  /** The installed plugin that is this entry, once there is one. */
  plugin: PluginInfo | undefined;
  vocabulary: MarketplaceVocabulary;
  /** More entries of the same kind, not installed, for the foot of the page. */
  related: MarketplaceEntry[];
  pluginOf: (entry: MarketplaceEntry) => PluginInfo | undefined;
  install: CatalogInstall;
  onClose: () => void;
  onOpenEntry: (name: string) => void;
  onOpenPlugin: (name: string) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const labelId = useId();
  const { art, kind } = entryArt(entry, plugin);
  const category = entryCategory(entry);
  const connector = addsConnector(entry);
  const installed = entry.installed;
  // The plugin's page opens by the name the catalog has for it, which is
  // known before the plugin list catches up with the install.
  const openPlugin = () => {
    if (entry.plugin) onOpenPlugin(entry.plugin);
  };
  // What landed, not what the files declared: a part can be held back (a
  // skill by that name already there), and an upstream entry declares none.
  const landed = installed && plugin ? plugin.components : null;
  const partNames = (kind: 'mcp' | 'skill', declared: string[] | undefined) =>
    landed ? landed.filter((c) => c.kind === kind).map((c) => c.name) : (declared ?? []);
  const connectorNames = partNames('mcp', entry.contents?.connectors);
  const skillNames = partNames('skill', entry.contents?.skills);
  const partsKnown = connectorNames.length + skillNames.length > 0;
  const vendor = entry.publisher?.name ?? entry.name;

  const controls =
    installed && !install.installing(entry) ? (
      <>
        <span className="discover-installed">
          <Check className="h-3.5 w-3.5" aria-hidden />
          {t('plugins.discover.installed')}
        </span>
        {entry.plugin && (
          <HeaderButton onClick={openPlugin}>{t('plugins.discoverPage.openPlugin')}</HeaderButton>
        )}
      </>
    ) : (
      <EntryInstallButton entry={entry} install={install} />
    );

  const meta = (
    <MetaLine
      parts={[
        entry.publisher ? t('plugins.discoverPage.by', { publisher: entry.publisher.name }) : null,
        entry.publisher?.official ? (
          <span key="official" className="discover-head-badge">
            <OfficialMark size={13} />
            {t('plugins.discoverPage.officialPublisher')}
          </span>
        ) : null,
        t(CATEGORY_KEYS[category]),
      ]}
    />
  );

  const markets = coverageLine(entry.coverage, vocabulary, locale);
  const kinds = dataLabels(entry, vocabulary.data, locale);
  const homepage = webLink(entry.homepage) ?? webLink(entry.publisher?.url);
  const source = webLink(entry.source_url);
  const access = accessKey(entry.auth);
  const covers = kinds.length > 0 ? kinds : entry.keywords;

  const aside = (
    <>
      <DetailAsideBlock title={t('plugins.discoverPage.details')}>
        <DetailFacts
          items={[
            [t('plugins.discoverPage.factPublisher'), entry.publisher?.name ?? null],
            [t('plugins.discoverPage.factCategory'), t(CATEGORY_KEYS[category])],
            [t('plugins.discoverPage.factAccess'), access ? t(access) : null],
            [t('plugins.discoverPage.factMarkets'), markets || null],
            [t('plugins.discoverPage.factLicense'), entry.license],
            [t('plugins.discoverPage.factHomepage'), homepage ? <OutLink key="homepage" href={homepage} /> : null],
            [t('plugins.discoverPage.factSource'), source ? <OutLink key="source" href={source} /> : null],
          ]}
        />
      </DetailAsideBlock>
      {covers.length > 0 && (
        <DetailAsideBlock title={t('plugins.discoverPage.covers')}>
          <div className="discover-tags">
            {covers.map((c) => (
              <span key={c} className="surface-tag">{c}</span>
            ))}
          </div>
        </DetailAsideBlock>
      )}
    </>
  );

  // The way back is Discover, not the section's own list.
  return (
    <DetailTrailStep label={t('plugins.discoverPage.discover')} onClick={onClose}>
      <DetailPage
        labelId={labelId}
        onClose={onClose}
        crumb={entry.name}
        aside={aside}
        header={
          <DetailHeader
            name={entry.name}
            labelId={labelId}
            kind={kind ?? 'plugin'}
            art={art}
            meta={meta}
            description={entry.description || undefined}
            controls={controls}
          />
        }
      >
        {/* Why the last install did not happen, under the Install it retries. */}
        <InstallNotice install={install} entry={entry} />
        <div className="discover-pair">
          <DetailSection title={t('plugins.discoverPage.adds')}>
            {(connector || !partsKnown) && (
              <div className="surface-grid is-one discover-static">
                {connector ? (
                  <SurfaceCell
                    mark={<EntryMark entry={entry} plugin={plugin} />}
                    name={entry.name}
                    meta={
                      connectorNames.length > 1
                        ? t('plugins.pluginsPage.connectorCount', { count: connectorNames.length })
                        : t('plugins.discoverPage.kindConnector')
                    }
                    line={kinds.length > 0 ? kinds.join(' · ') : markets || entry.description}
                  />
                ) : (
                  <SurfaceCell
                    name={entry.name}
                    meta={t('plugins.discoverPage.kindSkills')}
                    line={t(
                      installed
                        ? 'plugins.discoverPage.skillsOnPlugin'
                        : 'plugins.discoverPage.skillsAfterInstall',
                    )}
                  />
                )}
              </div>
            )}
            {connectorNames.length > 1 && <PartNames names={connectorNames} under />}
            {connector && (
              <p className="discover-note">
                {installed ? (
                  <Trans
                    i18nKey="plugins.discoverPage.toolsOnPlugin"
                    // Not `link`: the parser reads that as the void HTML
                    // element and drops the words it wraps.
                    components={{
                      open: (
                        <button
                          type="button"
                          className="pp-note-link"
                          onClick={openPlugin}
                        />
                      ),
                    }}
                  />
                ) : entry.auth === 'oauth' ? (
                  t('plugins.discoverPage.toolsAfterSignIn')
                ) : (
                  t('plugins.discoverPage.toolsAfterInstall')
                )}
              </p>
            )}
            {skillNames.length > 0 && (
              <PartNames
                label={t('plugins.card.skillCount', { count: skillNames.length })}
                names={skillNames}
              />
            )}
          </DetailSection>
          <DetailSection title={t('plugins.discoverPage.needs')}>
            <Needs entry={entry} vendor={vendor} installed={installed} />
          </DetailSection>
        </div>
        {related.length > 0 && (
          <DetailSection
            title={connector ? t('plugins.discoverPage.moreVendors') : t('plugins.discoverPage.moreCollections')}
          >
            <SurfaceGrid className="is-wide">
              {related.map((other) => (
                <SurfaceCell
                  key={other.name}
                  mark={<EntryMark entry={other} plugin={pluginOf(other)} />}
                  name={other.name}
                  meta={other.publisher?.official ? <OfficialMark /> : undefined}
                  line={other.description}
                  onOpen={() => onOpenEntry(other.name)}
                  trailing={<EntryInstallSlot entry={other} install={install} />}
                />
              ))}
            </SurfaceGrid>
          </DetailSection>
        )}
      </DetailPage>
    </DetailTrailStep>
  );
}

/** What an entry needs before it works, as the catalog states it. Installed,
 *  each need says where it now stands rather than how install asks for it. */
function Needs({
  entry,
  vendor,
  installed,
}: {
  entry: MarketplaceEntry;
  vendor: string;
  installed: boolean;
}) {
  const { t } = useTranslation();
  const requires = readRequires(entry.requires);
  const prose = requires && !requires.nothing ? requires.text : null;
  // Install refuses until every declared key has a value, whatever the
  // catalog's own line says, so the declared keys come first.
  const keys = entry.contents?.keys ?? [];
  // A skills-only package asks for its key but has no connector to check it on.
  const checked = entry.contents ? entry.contents.connectors.length > 0 : true;
  const keyBody = installed
    ? 'plugins.discoverPage.needKeyInstalled'
    : checked
      ? 'plugins.discoverPage.needKeyBody'
      : 'plugins.discoverPage.needKeyPlain';
  const rows: { key: string; icon: React.ReactNode; title: string; body: string }[] = [];
  const icon = (Icon: typeof KeyRound) => <Icon className="h-4 w-4" aria-hidden />;
  let proseShown = false;

  if (keys.length === 1 && prose && entry.auth !== 'oauth') {
    // The catalog's line is about that key (its plan, its price), so it heads
    // the row and the declared label says which key install asks for.
    proseShown = true;
    rows.push({
      key: `key:${keys[0].name}`,
      icon: icon(KeyRound),
      title: prose,
      body: t(
        installed
          ? 'plugins.discoverPage.needKeyNamedInstalled'
          : checked
            ? 'plugins.discoverPage.needKeyNamed'
            : 'plugins.discoverPage.needKeyNamedPlain',
        { key: keys[0].label },
      ),
    });
  } else if (keys.length > 0) {
    for (const key of keys) {
      rows.push({
        key: `key:${key.name}`,
        icon: icon(KeyRound),
        title: key.label,
        body: t(keyBody),
      });
    }
  } else if (entry.auth === 'api-key') {
    proseShown = !!prose;
    rows.push({
      key: 'key',
      icon: icon(KeyRound),
      title: prose ?? t('plugins.discoverPage.needKey'),
      body: t(installed ? 'plugins.discoverPage.needKeyInstalled' : 'plugins.discoverPage.needKeyBody'),
    });
  }
  if (prose && !proseShown) {
    rows.push({
      key: 'requires',
      icon: icon(CreditCard),
      title: prose,
      body: t('plugins.discoverPage.needRequiredBody'),
    });
  }
  if (entry.auth === 'oauth') {
    rows.push({
      key: 'sign-in',
      icon: icon(LogIn),
      title: t('plugins.discoverPage.needSignIn', { vendor }),
      body: t(
        installed ? 'plugins.discoverPage.needSignInInstalled' : 'plugins.discoverPage.needSignInBody',
        { vendor },
      ),
    });
  }
  if (rows.length === 0) {
    rows.push({
      key: 'nothing',
      icon: icon(Sparkles),
      title: t('plugins.discoverPage.needNothing'),
      body:
        requires?.nothing && requires.note
          ? requires.note
          : t(installed ? 'plugins.discoverPage.needNothingInstalled' : 'plugins.discoverPage.needNothingBody'),
    });
  }

  return (
    <div className="discover-needs">
      {rows.map((row) => (
        <div key={row.key} className="discover-need">
          <span className="discover-need-icon">{row.icon}</span>
          <div className="min-w-0">
            <div className="discover-need-title">{row.title}</div>
            <div className="discover-need-body">{row.body}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

/** The parts an entry adds, by name: what its files declare, or once
 *  installed what its plugin holds. Opening one is the plugin page's job. */
function PartNames({ label, names, under }: { label?: string; names: string[]; under?: boolean }) {
  return (
    <div className={cn('discover-parts', under && 'is-under')}>
      {label && <div className="discover-parts-label">{label}</div>}
      <div className="discover-tags">
        {names.map((name) => (
          <span key={name} className="surface-tag">{name}</span>
        ))}
      </div>
    </div>
  );
}

/** A link out, named by its site: "factset.com", "github.com". */
function OutLink({ href }: { href: string }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer" className="discover-link" title={href}>
      {siteName(href)}
      <ExternalLink className="h-3 w-3" aria-hidden />
    </a>
  );
}

function siteName(href: string): string {
  try {
    return new URL(href).host.replace(/^www\./, '');
  } catch {
    return href;
  }
}
