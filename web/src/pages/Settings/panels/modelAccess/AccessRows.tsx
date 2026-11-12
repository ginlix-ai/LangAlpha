import { useContext } from 'react';
import { useTranslation } from 'react-i18next';
import type { ClaudeCodePlan } from '@/api/model';
import { Button } from '@/components/ui/button';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import type { KeyPlace } from '@/hooks/useProviderCatalog';
import { useAvailableModels } from '@/hooks/useProviderModels';
import { isKnownEndpoint, useKnownHosts } from '@/components/modelAccess/endpointTrust';
import { regionTag } from '@/components/modelAccess/optionLabels';
import { QuietNote, StatusActions, StatusLabel, type RowStatus } from '@/components/modelAccess/parts';
import { UsageCells, UsageDetail } from '@/components/modelAccess/PlanUsage';
import { UsageTableContext } from '@/components/modelAccess/usageSteps';
import { keyDetail } from './keyDetail';

/** One connection's row: its mark and name, what it is, its live status,
 *  Manage. In a usage table (`UsageTableContext`) it lays out on the table's
 *  columns, with the use of the Claude plan behind it (`plan`) in them;
 *  a row with no plan takes those columns for its text. */
export function AccessRow({
  provider,
  name,
  tag,
  unverified = false,
  detail,
  wrapDetail = false,
  plan,
  limited = false,
  status,
  action,
  notice,
}: {
  provider: string;
  name: string;
  /** Which of the provider's plans and regions, where it has more than one. */
  tag?: string | null;
  /** Reached through an endpoint we cannot vouch for, most often a proxy. */
  unverified?: boolean;
  detail?: React.ReactNode;
  /** A detail worded as a sentence, which wraps rather than loses its end. */
  wrapDetail?: boolean;
  /** The Claude plan whose use the row shows, on one line with its detail,
   *  and whether its detail names a limit, which stands alone there. */
  plan?: ClaudeCodePlan;
  limited?: boolean;
  status: RowStatus;
  action: React.ReactNode;
  /** A sentence under the detail that the user should act on, such as the
   *  day the connection stops working. */
  notice?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const steps = useContext(UsageTableContext);
  const noticeLine = notice && <span className="text-xs text-pretty wrap-break-word" style={{ color: 'var(--color-text-secondary)' }}>{notice}</span>;
  const nameLine = (
    <span className="flex min-w-0 items-baseline gap-2">
      <span className="truncate text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
      {tag && <span className="shrink-0 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{tag}</span>}
      {unverified && <span className="shrink-0 text-xs" style={{ color: 'var(--color-warning)' }}>{t('modelAccess.unverifiedTag')}</span>}
    </span>
  );
  // A masked key reads as one in the code face; an account or URL does not.
  const detailClass = cn('text-xs', wrapDetail ? 'wrap-break-word' : 'truncate', typeof detail === 'string' && detail.includes('...') && 'font-mono');
  if (steps) {
    return (
      <li className={cn('col-span-full flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3', steps.row)}>
        <BrandMark name={name} art={llmProviderArt(provider)} size="sm" className="rounded-md" />
        <div className={cn('flex min-w-0 flex-[1_1_12rem] flex-col gap-0.5', !plan && steps.spanUsage)}>
          {nameLine}
          {detail && (
            <span data-row-detail className={plan ? 'text-xs' : detailClass} style={{ color: 'var(--color-text-tertiary)' }}>
              {plan ? <UsageDetail text={detail} plan={plan} tail="bars" whole={limited} /> : detail}
            </span>
          )}
          {noticeLine}
        </div>
        {plan && <UsageCells plan={plan} />}
        <div className={cn('ml-auto flex shrink-0 items-center gap-3', steps.tail)}>
          <StatusLabel status={status} className="shrink-0 whitespace-nowrap" />
          <div className="shrink-0">{action}</div>
        </div>
      </li>
    );
  }
  return (
    // Too narrow for the name and its detail beside the status and actions,
    // the status and actions take a line of their own under them.
    <li className="flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3">
      <BrandMark name={name} art={llmProviderArt(provider)} size="sm" className="rounded-md" />
      <div className="flex min-w-0 flex-[1_1_12rem] flex-col gap-0.5">
        {nameLine}
        {detail && <span className={detailClass} style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>}
        {noticeLine}
      </div>
      <StatusActions status={status}>
        <div className="shrink-0">{action}</div>
      </StatusActions>
    </li>
  );
}

/** A key row's status: Connected, unless its own model list says the key was refused. */
export function KeyRow({ p, place, onManage }: { p: ConfiguredProvider; place?: KeyPlace; onManage: () => void }) {
  const { t } = useTranslation();
  const listable = !p.is_custom && p.access_type !== 'local';
  const list = useAvailableModels(listable ? p.provider : null);
  const knownHosts = useKnownHosts();
  const status: RowStatus = list.isLoading
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

export function Group({ title, children, empty, subgrid = false }: {
  title: string;
  children: React.ReactNode[];
  empty: string;
  /** Lays its rows on the columns of the grid it sits in, so the rows of
   *  cards stacked in that grid line up with each other. */
  subgrid?: boolean;
}) {
  return (
    <div className={subgrid ? 'col-span-full grid grid-cols-subgrid gap-y-1.5' : 'flex flex-col gap-1.5'}>
      {/* A section label, in the open card styles' label voice. */}
      <h3
        className={cn('text-xs font-medium open-style:label-text', subgrid && 'col-span-full')}
        style={{ color: 'var(--color-shell-label, var(--color-text-tertiary))' }}
      >
        {title}
      </h3>
      {children.length > 0 ? (
        // A card of rows: an open box in the open card styles, its rows parted
        // by a card's divider.
        <ul
          className={cn(
            'shell-box divide-y divide-[color:var(--color-shell-divider,hsl(var(--border)))] [&>*]:[border-style:var(--cs-rule-style,solid)]',
            subgrid && 'col-span-full grid grid-cols-subgrid',
          )}
        >
          {children}
        </ul>
      ) : (
        <QuietNote className={cn(subgrid && 'col-span-full')}>{empty}</QuietNote>
      )}
    </div>
  );
}
