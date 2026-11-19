import { useContext, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { ClaudeCodePlan } from '@/api/model';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';
import { StatusActions, StatusLabel, type RowStatus } from './parts';
import { UsageCells, UsageDetail } from './PlanUsage';
import { UsageTableContext } from './usageSteps';

/** A row's mark: the provider's, or a glyph for a row that is not a
 *  provider's, never neither. */
type AccessRowMark = { provider: string; icon?: never } | { icon: ReactNode; provider?: never };

/** One connection's row: its mark (the provider's, or a glyph on the name's
 *  tile for a row that is not a provider's), its name, what it is, its live
 *  status, Manage. In a usage table (`UsageTableContext`) it lays out on the table's
 *  columns, with the use of the Claude plan behind it (`plan`) in them;
 *  a row with no plan takes those columns for its text. */
export function AccessRow({
  provider,
  icon,
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
  testId,
}: AccessRowMark & {
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
  /** None for a row with nothing to report, such as a way to connect. */
  status?: RowStatus;
  action?: React.ReactNode;
  /** A sentence under the detail that the user should act on, such as the
   *  day the connection stops working. */
  notice?: React.ReactNode;
  testId?: string;
}) {
  const { t } = useTranslation();
  const mark = provider !== undefined
    ? <BrandMark name={name} art={llmProviderArt(provider)} size="sm" className="rounded-md" />
    : (
      <IdentityTile name={name} size="sm">
        <span className="flex [&>svg]:h-4 [&>svg]:w-4" style={{ color: 'var(--color-text-secondary)' }}>{icon}</span>
      </IdentityTile>
    );
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
      <li data-testid={testId} className={cn('col-span-full flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3', steps.row)}>
        {mark}
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
          {status && <StatusLabel status={status} className="shrink-0 whitespace-nowrap" />}
          <div className="shrink-0">{action}</div>
        </div>
      </li>
    );
  }
  return (
    // Too narrow for the name and its detail beside the status and actions,
    // the status and actions take a line of their own under them.
    <li data-testid={testId} className="flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-3">
      {mark}
      <div className="flex min-w-0 flex-[1_1_12rem] flex-col gap-0.5">
        {nameLine}
        {detail && <span className={detailClass} style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>}
        {noticeLine}
      </div>
      {status ? (
        <StatusActions status={status}>
          <div className="shrink-0">{action}</div>
        </StatusActions>
      ) : action && <div className="ml-auto shrink-0">{action}</div>}
    </li>
  );
}
