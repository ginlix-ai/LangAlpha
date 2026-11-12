import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { compactNumber, percent } from '@/lib/format';
import { cn } from '@/lib/utils';
import { useLocale } from '@/hooks/useLocale';
import { useContextBreakdown } from '@/pages/ChatAgent/hooks/useContextBreakdown';
import { hitRate, type TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
import { SwapCell } from './crossfade';
import { SkeletonBar } from './skeleton';
import { CONTEXT_PARTS, barSegments, contextRows, share, type ContextPart } from './context-panel.model';

/**
 * A hue per category (`--color-context-*`), so a legend row finds its stretch
 * of the bar. They name a part and never a state: how full the window is
 * stays the ring's to say.
 */
const PART_FILL: Record<ContextPart, string> = {
  system: 'bg-(--color-context-system)',
  tools: 'bg-(--color-context-tools)',
  mcp: 'bg-(--color-context-mcp)',
  skills: 'bg-(--color-context-skills)',
  memory: 'bg-(--color-context-memory)',
  environment: 'bg-(--color-context-environment)',
  conversation: 'bg-(--color-context-conversation)',
};

/** The fill of a window drawn without a breakdown. */
const USED_FILL = 'bg-(--color-text-primary)/60';

/** Placeholder label widths, uneven so the rows read as words to come. */
const PLACEHOLDER_WIDTHS = ['w-20', 'w-10', 'w-16', 'w-12', 'w-14', 'w-20', 'w-24'] as const;

/** One legend row's box, shared by the real rows and their placeholders so
 *  the counts land exactly where the placeholders sat. */
const LEGEND_ROW = 'flex h-4 items-center gap-2';

interface ContextPanelProps {
  tokenUsage: TokenUsage;
  /** The thread whose parts are counted; with none the panel is the window
   *  alone. */
  threadId: string | null | undefined;
}

/**
 * The ring's popover body. It mounts only while the popover is open, so its
 * count is asked for on open and never for a panel nobody is looking at.
 */
export function ContextPanel({ tokenUsage, threadId }: ContextPanelProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const breakdown = useContextBreakdown(threadId);
  const { totalInput, totalOutput, totalCached, lastInput, lastCached, total, threshold, contextLimit } = tokenUsage;

  const pct = share(total, threshold);
  const rows = breakdown.data?.parts ? contextRows(breakdown.data.parts, { total, lastInput }) : [];
  const segments = barSegments(rows, threshold);
  // Idle while pending means no thread to count, which is the window alone.
  const counting = breakdown.isPending && breakdown.fetchStatus !== 'idle';
  const stopped = breakdown.data?.reason === 'computer_stopped';

  const threadRate = hitRate(totalCached, totalInput);
  const lastRate = hitRate(lastCached, lastInput);

  return (
    <>
      <div className="flex items-baseline justify-between gap-3">
        <span className="text-sm font-medium">{t('chat.contextPanel.title')}</span>
        <span className="truncate text-(--color-text-tertiary)">
          {t('chat.contextPanel.used', {
            used: compactNumber(total, locale),
            threshold: compactNumber(threshold, locale),
            percent: percent(threshold > 0 ? total / threshold : 0, locale),
          })}
        </span>
      </div>

      <div className="mt-2.5 flex h-1.5 overflow-hidden rounded-full bg-(--color-border-muted)">
        {segments.length > 0
          ? segments.map(({ key, width }) => (
              <div key={key} className={cn('h-full flex-none', PART_FILL[key])} style={{ width: `${width * 100}%` }} />
            ))
          : <div className={cn('h-full flex-none', USED_FILL)} style={{ width: `${pct * 100}%` }} />}
      </div>

      <SwapCell as="div" swapKey={rows.length > 0 ? 'rows' : counting ? 'counting' : stopped ? 'stopped' : null}>
        {rows.length > 0 ? (
          <ul className="flex flex-col gap-1 pt-2.5">
            {rows.map(({ key, tokens }) => (
              <li key={key} className={LEGEND_ROW}>
                <span aria-hidden="true" className={cn('size-2 flex-none rounded-[2px]', PART_FILL[key])} />
                <span className="min-w-0 flex-1 truncate text-(--color-text-secondary)">{t(`chat.contextPanel.parts.${key}`)}</span>
                <span className="flex-none text-(--color-text-tertiary)">{compactNumber(tokens, locale)}</span>
              </li>
            ))}
          </ul>
        ) : counting ? (
          // As many rows as the legend can have, so the panel keeps its
          // height when the counts land.
          <ul aria-busy="true" aria-label={t('chat.contextPanel.counting')} className="flex flex-col gap-1 pt-2.5">
            {CONTEXT_PARTS.map((key, i) => (
              <li key={key} aria-hidden="true" className={LEGEND_ROW}>
                <span className="size-2 flex-none animate-pulse rounded-[2px] bg-(--color-border-default)" />
                <span className="min-w-0 flex-1"><SkeletonBar className={PLACEHOLDER_WIDTHS[i]} /></span>
                <span className="flex-none"><SkeletonBar className="w-7" /></span>
              </li>
            ))}
          </ul>
        ) : stopped ? (
          <p className="pt-2.5 text-(--color-text-tertiary)">{t('chat.contextPanel.computerStopped')}</p>
        ) : null}
      </SwapCell>

      <div className="mt-3 flex items-baseline justify-between gap-3 rounded-lg border border-(--color-border-muted) px-2.5 py-2">
        <span className="text-(--color-text-secondary)">{t('chat.contextPanel.compactAt')}</span>
        <span className="flex-none">
          <span className="font-medium">{compactNumber(threshold, locale)}</span>
          {contextLimit !== null && (
            <span className="text-(--color-text-tertiary)"> / {compactNumber(contextLimit, locale)}</span>
          )}
        </span>
      </div>

      <div className="mt-3 border-t border-(--color-border-muted) pt-2.5 text-(--color-text-tertiary)">
        <div className="mb-1 text-[0.6875rem] font-medium">{t('chat.contextPanel.thread')}</div>
        <dl className="flex flex-col gap-1">
          <Row label={t('chat.contextPanel.input')}>{compactNumber(totalInput, locale)}</Row>
          <Row label={t('chat.contextPanel.output')}>{compactNumber(totalOutput, locale)}</Row>
          {(threadRate !== null || lastRate !== null) && (
            <Row label={t('chat.contextPanel.cacheHitRate')}>
              {lastRate !== null && (
                <span className="text-(--color-text-quaternary)">
                  {t('chat.contextPanel.lastCall', { percent: percent(lastRate, locale) })}
                </span>
              )}
              {threadRate !== null && <span>{percent(threadRate, locale)}</span>}
            </Row>
          )}
        </dl>
      </div>
    </>
  );
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <dt className="min-w-0 truncate">{label}</dt>
      <dd className="flex flex-none items-baseline gap-2">{children}</dd>
    </div>
  );
}
