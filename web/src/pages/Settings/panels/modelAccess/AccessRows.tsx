import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import type { KeyPlace } from '@/hooks/useProviderCatalog';
import { useAvailableModels } from '@/hooks/useProviderModels';
import { AccessRow } from '@/components/modelAccess/AccessRow';
import { isKnownEndpoint, useKnownHosts } from '@/components/modelAccess/endpointTrust';
import { regionTag } from '@/components/modelAccess/optionLabels';
import type { RowStatus } from '@/components/modelAccess/parts';
import { keyDetail } from './keyDetail';

/** A key row's status: Connected, unless its own model list says the key was refused. */
export function KeyRow({ p, place, onManage, notAllowed = false }: {
  p: ConfiguredProvider;
  place?: KeyPlace;
  onManage: () => void;
  /** A rule keeps the user's own keys off: saved, unused, and removable. */
  notAllowed?: boolean;
}) {
  const { t } = useTranslation();
  const listable = !p.is_custom && p.access_type !== 'local';
  const list = useAvailableModels(listable && !notAllowed ? p.provider : null);
  const knownHosts = useKnownHosts();
  const status: RowStatus = notAllowed ? 'not_allowed' : list.isLoading
    ? 'working'
    : list.data?.status === 'fallback' && list.data.reason === 'unauthorized'
      ? 'refused'
      : 'connected';
  return (
    <AccessRow
      provider={place?.group.markProvider ?? p.provider}
      name={place?.group.name ?? p.display_name}
      tag={regionTag(t, place)}
      unverified={p.access_type !== 'local' && !!p.base_url && isKnownEndpoint(p.base_url, knownHosts) === false}
      detail={keyDetail(p, t)}
      status={status}
      action={<Button type="button" variant="ghost" size="sm" onClick={onManage}>{t('modelAccess.manage')}</Button>}
    />
  );
}
