import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { useLocale } from '@/hooks/useLocale';
import { mediumDate } from '@/lib/format';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { secretEditHref, secretSetupHref } from '../../utils/secretParam';
import { DetailFacts } from '../DetailPage';

/** What a connector is and where it lives, for its page's Details block. */
export function ServerFacts({
  catalog,
  transport,
  plugin,
  onOpenPlugin,
  keyNames,
  stored,
}: {
  catalog: CatalogServer | null;
  transport: string;
  plugin: string | null;
  /** Opens the plugin's page; absent where the plugin is named only. */
  onOpenPlugin?: (plugin: string) => void;
  keyNames: string[];
  stored: ReadonlySet<string>;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  // Values built first: a bare element inside the tuple list reads to the
  // linter as a keyless list child.
  const pluginFact =
    plugin && onOpenPlugin ? (
      <button type="button" className="connector-head-link" onClick={() => onOpenPlugin(plugin)}>
        {plugin}
      </button>
    ) : (
      plugin
    );
  // The scheme says nothing a reader needs in a narrow column; the whole
  // address stays on hover.
  const urlFact = catalog?.url ? (
    <span className="connector-mono" title={catalog.url}>
      {catalog.url.replace(/^https?:\/\//, '')}
    </span>
  ) : null;
  const commandFact = catalog?.command ? (
    <span className="connector-mono">{[catalog.command, ...(catalog.args ?? [])].join(' ')}</span>
  ) : null;
  // Each key opens its own row in Secrets: a stored one in its edit form, a
  // missing one in the add form on its name.
  const keysFact =
    keyNames.length > 0 ? (
      <span className="flex flex-col items-start gap-1 min-w-0">
        {keyNames.map((key) => (
          <Link
            key={key}
            to={stored.has(key) ? secretEditHref(key) : secretSetupHref(key)}
            title={t('plugins.connectorsPage.callout.keyLinkTitle', { key })}
            className="connector-mono max-w-full truncate"
          >
            {key}
          </Link>
        ))}
      </span>
    ) : null;
  const facts: Array<readonly [string, ReactNode]> = [
    [t('plugins.connectorsPage.plugin'), pluginFact],
    [t('plugins.detail.transport'), transport],
    [t('plugins.detail.url'), urlFact],
    [t('plugins.detail.command'), commandFact],
    [t('plugins.connectorsPage.keys'), keysFact],
    [
      t('plugins.detail.created'),
      catalog?.created_at ? mediumDate(new Date(catalog.created_at), locale) : null,
    ],
    [
      t('plugins.detail.updated'),
      catalog?.updated_at ? mediumDate(new Date(catalog.updated_at), locale) : null,
    ],
  ];
  return <DetailFacts items={facts} />;
}
