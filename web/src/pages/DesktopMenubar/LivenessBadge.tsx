/**
 * The footer's word for how live the panel's figures are (`feedLiveness`),
 * with the rows by state, the source and the oldest refresh behind a tooltip.
 * Reads the quote cache for exactly the quotes the sections show, so a hidden
 * section's rows never count.
 */
import { useCallback, useSyncExternalStore } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNetworkStatus } from '@/hooks/useNetworkStatus';
import { useNow } from '@/hooks/useNow';
import { freshnessSpecText, type FreshnessFields } from '@/lib/freshness';
import { mediumTime } from '@/lib/format';
import { indexName } from '@/lib/indices';
import { providerLabel } from '@/lib/providerLabels';
import { queryKeys } from '@/lib/queryKeys';
import type { QuoteRow } from '@/lib/quotes';
import { panelLiveness, rowLiveness, stateKey, type LivenessDot, type LivenessRow, type RowLiveness } from './feedLiveness';
import { useShownQuotes, type ShownQuote } from './liveQuotes';

/** Names listed per state before the rest are counted. */
const NAMES_LISTED = 4;

/** Faults first, then from the liveliest down, so the tooltip opens on what the badge says. */
const TOOLTIP_ORDER: readonly LivenessDot[] = ['warning', 'live', 'realtime', 'delayed', 'closed'];

interface CachedQuote {
  fields: FreshnessFields | null;
  source: string | null;
  updatedAt: number;
  fetching: boolean;
}

/**
 * The cached state of each shown quote, as one string per distinct state, so
 * the badge renders when a figure lands and not on every cache event.
 */
function useCachedQuotes(shown: readonly ShownQuote[]): Record<string, CachedQuote> {
  const queryClient = useQueryClient();
  const keys = shown.map((q) => q.key).join(',');
  const subscribe = useCallback((onChange: () => void) => queryClient.getQueryCache().subscribe(onChange), [queryClient]);
  const read = useCallback(() => {
    const out: Record<string, CachedQuote> = {};
    for (const key of keys ? keys.split(',') : []) {
      const state = queryClient.getQueryState<QuoteRow | null>(queryKeys.quote.detail(key));
      const fetching = state?.fetchStatus === 'fetching';
      if (!state?.data) {
        // Never answered because every read failed: that is no figure at all,
        // which says more than leaving the badge blank. A symbol no provider
        // has answers null, a success, and stays out.
        if (state?.status === 'error') out[key] = { fields: null, source: null, updatedAt: 0, fetching };
        continue;
      }
      out[key] = {
        fields: { tier: state.data.tier ?? null, freshness: state.data.freshness ?? null },
        source: state.data.freshness?.source ?? state.data.source ?? null,
        updatedAt: state.dataUpdatedAt,
        fetching,
      };
    }
    return JSON.stringify(out);
  }, [queryClient, keys]);
  return JSON.parse(useSyncExternalStore(subscribe, read, read)) as Record<string, CachedQuote>;
}

export function LivenessBadge({ active }: { active: boolean }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { online } = useNetworkStatus();
  const now = useNow(15_000, active);
  const { shown, live } = useShownQuotes();
  const cached = useCachedQuotes(shown);

  const rows: (LivenessRow & { name: string; source: string | null })[] = shown.flatMap((q) => {
    const c = cached[q.key];
    if (!c) return [];
    const name = (q.isIndex && indexName(q.key, 'short', t)) || q.key;
    return [{ ...q, ...c, name, streaming: !q.isIndex && live.has(q.key) }];
  });
  const textFor = (state: RowLiveness): string =>
    state.kind === 'streaming'
      ? t('marketView.quote.live')
      : state.kind === 'overdue'
        ? t('menubar.feed.overdue')
        : (freshnessSpecText(t, state.spec) ?? '');

  const groups = new Map<string, { dot: LivenessDot; text: string; names: string[] }>();
  for (const row of rows) {
    const state = rowLiveness(row, now);
    if (!state) continue;
    const key = stateKey(state);
    const group = groups.get(key) ?? { dot: state.dot, text: textFor(state), names: [] };
    group.names.push(row.name);
    groups.set(key, group);
  }
  const ordered = [...groups.values()].sort((a, b) => TOOLTIP_ORDER.indexOf(a.dot) - TOOLTIP_ORDER.indexOf(b.dot));
  const lines = ordered.map(({ text, names }) => {
    const rest = names.length - NAMES_LISTED;
    const listed = names.slice(0, NAMES_LISTED).join(', ');
    return `${text}: ${rest > 0 ? `${listed} ${t('menubar.feed.more', { count: rest })}` : listed}`;
  });
  const sources = [...new Set(rows.map((r) => providerLabel(r.source, t)).filter((s): s is string => !!s))];
  if (sources.length) lines.push(`${t('marketView.header.source')}: ${sources.join(', ')}`);
  const answered = rows.map((r) => r.updatedAt).filter((at) => at > 0);
  if (answered.length) lines.push(t('menubar.feed.updated', { time: mediumTime(Math.min(...answered), locale) }));

  const liveness = panelLiveness(rows, now);
  const badge = online
    ? liveness && { dot: liveness.dot, text: textFor(liveness.row) }
    : { dot: 'warning' as const, text: t('menubar.feed.offline') };
  if (!badge) return null;
  return (
    <span className="mb-live" data-dot={badge.dot} title={lines.join('\n') || undefined}>
      <span className="mb-live-dot" aria-hidden="true" />
      <span className="mb-live-label">{badge.text}</span>
    </span>
  );
}
