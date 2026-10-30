import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import type { KeyPlace } from '@/hooks/useProviderCatalog';
import { useAvailableModels } from '@/hooks/useProviderModels';
import { isKnownEndpoint, useKnownHosts } from '@/components/modelAccess/endpointTrust';
import { regionTag } from '@/components/modelAccess/optionLabels';
import { QuietNote, StatusLabel, type AccessStatus } from '@/components/modelAccess/parts';
import { keyDetail } from './keyDetail';

/** One connection's row: its mark and name, what it is, its live status, Manage. */
export function AccessRow({
  provider,
  name,
  tag,
  unverified = false,
  detail,
  wrapDetail = false,
  status,
  action,
}: {
  provider: string;
  name: string;
  /** Which of the provider's plans and regions, where it has more than one. */
  tag?: string | null;
  /** Reached through an endpoint we cannot vouch for, most often a proxy. */
  unverified?: boolean;
  detail?: React.ReactNode;
  /** `withdrawn`: a key for a provider the catalog no longer has. */
  /** A detail worded as a sentence, which wraps rather than loses its end. */
  wrapDetail?: boolean;
  status: AccessStatus | 'refused' | 'withdrawn' | 'not_connected';
  action: React.ReactNode;
}) {
  const { t } = useTranslation();
  return (
    // Too narrow for the name and its detail beside the status and actions,
    // the status and actions take a line of their own under them.
    <li className="flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3">
      <BrandMark name={name} art={llmProviderArt(provider)} size="sm" className="rounded-md" />
      <div className="flex min-w-0 flex-[1_1_12rem] flex-col gap-0.5">
        <span className="flex min-w-0 items-baseline gap-2">
          <span className="truncate text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
          {tag && <span className="shrink-0 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{tag}</span>}
          {unverified && <span className="shrink-0 text-xs" style={{ color: 'var(--color-warning)' }}>{t('modelAccess.unverifiedTag')}</span>}
        </span>
        {detail && (
          // A masked key reads as one in the code face; an account or URL does not.
          <span className={cn('text-xs', wrapDetail ? 'wrap-break-word' : 'truncate', typeof detail === 'string' && detail.includes('...') && 'font-mono')} style={{ color: 'var(--color-text-tertiary)' }}>
            {detail}
          </span>
        )}
      </div>
      <div className="ml-auto flex shrink-0 items-center gap-3">
        {status === 'refused' || status === 'withdrawn' ? (
          <span className="text-xs" style={{ color: 'var(--color-warning)' }}>
            {t(status === 'refused' ? 'modelAccess.status.keyRefused' : 'modelAccess.status.withdrawn')}
          </span>
        ) : status === 'not_connected' ? (
          <span className="shrink-0 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.status.notConnected')}</span>
        ) : (
          <StatusLabel status={status} className="shrink-0" />
        )}
        <div className="shrink-0">{action}</div>
      </div>
    </li>
  );
}

/** A key row's status: Connected, unless its own model list says the key was refused. */
export function KeyRow({ p, place, onManage }: { p: ConfiguredProvider; place?: KeyPlace; onManage: () => void }) {
  const { t } = useTranslation();
  const listable = !p.is_custom && p.access_type !== 'local';
  const list = useAvailableModels(listable ? p.provider : null);
  const knownHosts = useKnownHosts();
  const status: AccessStatus | 'refused' = list.isLoading
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

export function Group({ title, children, empty }: { title: string; children: React.ReactNode[]; empty: string }) {
  return (
    <div className="flex flex-col gap-1.5">
      <h4 className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>{title}</h4>
      {children.length > 0 ? (
        <ul className="divide-y rounded-lg" style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)', borderColor: 'var(--color-border-muted)' }}>
          {children}
        </ul>
      ) : (
        <QuietNote>{empty}</QuietNote>
      )}
    </div>
  );
}
