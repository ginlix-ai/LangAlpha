import { useTranslation } from 'react-i18next';
import { useMarketplace } from '@/hooks/usePlugins';
import { inScope, type DiscoverScope } from '../../utils/marketplace';

/**
 * The Discover status line under the page title: how much of the catalog this
 * section shows and how much of it is already installed. Text only; the page
 * header lays it out.
 */
export function DiscoverSummary({ scope }: { scope: DiscoverScope }) {
  const { t } = useTranslation();
  const { data } = useMarketplace();
  if (!data?.configured) return null;
  const entries = data.entries.filter((e) => inScope(e, scope));
  const count = entries.length;
  const installed = entries.filter((e) => e.installed).length;
  const total =
    scope === 'connectors'
      ? t('plugins.discoverPage.vendorCount', { count })
      : scope === 'skills'
        ? t('plugins.discoverPage.collectionCount', { count })
        : t('plugins.discoverPage.catalogCount', { count });
  return (
    <>
      <span>{total}</span>
      {installed > 0 && (
        <>
          <span aria-hidden style={{ color: 'var(--color-text-quaternary)' }}>·</span>
          <span>{t('plugins.discoverPage.installedCount', { count: installed })}</span>
        </>
      )}
    </>
  );
}
