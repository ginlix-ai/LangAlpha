/**
 * "How the agent sees it" preview for a chart selection card.
 *
 * Opens when a sent-message selection card is clicked and renders the same
 * context the agent received for that selection: the chart id, the region /
 * price-level bounds, the user's note, and the OHLCV bars (the exact data the
 * agent analyzes), plus a note that it can draw its read back onto the chart.
 * Mirrors `WidgetContextPreview` — a Dialog over a structured, read-only body.
 */

import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { SquareDashedMousePointer, Ruler } from 'lucide-react';

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';

interface PreviewBar {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface SelectionPreviewShape {
  selectionType: 'region' | 'price_level';
  symbol: string;
  timeframe: string;
  priceLow: number;
  priceHigh: number;
  comment?: string;
  timeStart?: string;
  timeEnd?: string;
  bars?: PreviewBar[];
  barsTruncated?: boolean;
}

/** Rows shown in the OHLCV table; the rest collapse to a "+N more" note. */
const MAX_TABLE_ROWS = 60;

/** Price with 2 decimals (matching StockHeader's `.toFixed(2)`); `—` when absent. */
function fmtPrice(n: number | null | undefined): string {
  return n == null || !Number.isFinite(n) ? '—' : n.toFixed(2);
}

/** Volume with locale thousands separators; `—` when absent. */
function fmtVolume(n: number | null | undefined): string {
  return n == null || !Number.isFinite(n) ? '—' : Number(n).toLocaleString();
}

/** Format an ISO bar time in UTC (matching the agent's bar times): date, plus
 *  HH:MM for intraday bars. */
function fmtTime(iso?: string): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const date = d.toISOString().slice(0, 10);
  const hm = d.toISOString().slice(11, 16);
  return hm === '00:00' ? date : `${date} ${hm}`;
}

function MetaRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex gap-3 py-1">
      <span className="flex-shrink-0" style={{ width: 92, color: 'var(--color-text-tertiary)' }}>
        {label}
      </span>
      <span style={{ color: 'var(--color-text-primary)' }}>{value}</span>
    </div>
  );
}

export function SelectionContextPreview({
  selection,
  onClose,
}: {
  selection: SelectionPreviewShape | null;
  onClose: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const open = selection !== null;
  const isRegion = selection?.selectionType === 'region';
  const Icon = isRegion ? SquareDashedMousePointer : Ruler;

  const bars = useMemo(() => selection?.bars ?? [], [selection]);
  const shown = useMemo(() => bars.slice(0, MAX_TABLE_ROWS), [bars]);
  const price = isRegion
    ? `$${fmtPrice(selection?.priceLow)} – $${fmtPrice(selection?.priceHigh)}`
    : `$${fmtPrice(selection?.priceLow)}`;

  // Prefer the explicit bounds; fall back to the first/last candle of the
  // selected region when an older snapshot lacks them. Hide the row entirely
  // when neither is available rather than showing "— → —".
  const timeStart = selection?.timeStart ?? bars[0]?.time;
  const timeEnd = selection?.timeEnd ?? bars[bars.length - 1]?.time;
  const hasTimeRange = isRegion && (timeStart || timeEnd);

  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) onClose(); }}>
      <DialogContent
        className="max-w-2xl"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        <DialogHeader>
          <DialogTitle
            className="text-base flex items-center gap-2"
            style={{ color: 'var(--color-text-primary)' }}
          >
            <Icon className="h-4 w-4" style={{ color: 'var(--color-accent-light)' }} />
            {selection?.symbol}:{selection?.timeframe}
          </DialogTitle>
          <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('marketView.selection.previewSubtitle')}
          </DialogDescription>
        </DialogHeader>

        <div className="max-h-[65vh] overflow-auto text-sm">
          <div className="mb-3">
            <MetaRow
              label={t('marketView.selection.previewKind')}
              value={isRegion
                ? t('marketView.selection.cardRegionTitle')
                : t('marketView.selection.cardPriceTitle')}
            />
            {hasTimeRange && (
              <MetaRow
                label={t('marketView.selection.previewTimeRange')}
                value={`${fmtTime(timeStart)} → ${fmtTime(timeEnd)}`}
              />
            )}
            <MetaRow label={t('marketView.selection.previewPrice')} value={price} />
            {selection?.comment && (
              <MetaRow
                label={t('marketView.selection.previewNote')}
                value={<span style={{ color: 'var(--color-text-secondary)' }}>“{selection.comment}”</span>}
              />
            )}
          </div>

          {shown.length > 0 && (
            <div>
              <div
                className="flex items-center justify-between mb-1 text-xs"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                <span>{t('marketView.selection.previewBars')}</span>
                <span>
                  {bars.length > shown.length
                    ? t('marketView.selection.previewBarsShown', { shown: shown.length, total: bars.length })
                    : t('marketView.selection.previewBarsCount', { count: bars.length })}
                  {selection?.barsTruncated ? ` · ${t('marketView.selection.previewBarsTruncated')}` : ''}
                </span>
              </div>
              <div
                className="overflow-auto rounded-md border"
                style={{ borderColor: 'var(--color-border-muted)', maxHeight: '32vh' }}
              >
                <table className="w-full text-xs" style={{ borderCollapse: 'collapse' }}>
                  <thead>
                    <tr style={{ color: 'var(--color-text-tertiary)' }}>
                      {(['colTime', 'colOpen', 'colHigh', 'colLow', 'colClose', 'colVolume'] as const).map(
                        (col) => (
                          <th
                            key={col}
                            className="text-left font-medium px-2 py-1"
                            style={{ borderBottom: '1px solid var(--color-border-muted)' }}
                          >
                            {t(`marketView.selection.${col}`)}
                          </th>
                        ),
                      )}
                    </tr>
                  </thead>
                  <tbody style={{ color: 'var(--color-text-secondary)' }}>
                    {shown.map((b, i) => (
                      <tr key={i}>
                        <td className="px-2 py-1 whitespace-nowrap">{fmtTime(b.time)}</td>
                        <td className="px-2 py-1">{fmtPrice(b.open)}</td>
                        <td className="px-2 py-1">{fmtPrice(b.high)}</td>
                        <td className="px-2 py-1">{fmtPrice(b.low)}</td>
                        <td className="px-2 py-1">{fmtPrice(b.close)}</td>
                        <td className="px-2 py-1">{fmtVolume(b.volume)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <p className="mt-3 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('marketView.selection.previewDrawBack')}
          </p>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default SelectionContextPreview;
