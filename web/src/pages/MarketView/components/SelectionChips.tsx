import React from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { formatMoney, quoteCurrency } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { chartSelectionStore, type ChartSelection } from '../stores/chartSelectionStore';

/**
 * The regions and price levels the user picked on a chart, each with its note,
 * waiting to go out with the next send. A chip re-opens its note editor on the
 * chart; its X removes it. Shared by every composer a chart sends into.
 */
export function SelectionChips({ chips }: { chips: readonly ChartSelection[] }): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  if (chips.length === 0) return null;
  return (
    <div style={{ padding: '0 12px', marginBottom: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
      {chips.map((c) => {
        // The price arrives pre-formatted: the string carries no currency of its
        // own. It prints at the places the chart axis showed when it was drawn.
        const baseLabel = c.selectionType === 'region'
          ? t('marketView.selection.chipRegion', { symbol: c.symbol, timeframe: c.timeframe })
          : t('marketView.selection.chipPriceLevel', {
              price: Number.isFinite(c.priceLow)
                ? formatMoney(c.priceLow, quoteCurrency(null, c.symbol), locale, { decimals: c.decimals ?? 2 })
                : '—',
              symbol: c.symbol,
              timeframe: c.timeframe,
            });
        const label = c.comment ? `${baseLabel} · "${c.comment}"` : baseLabel;
        return (
          <span
            key={c.id}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              maxWidth: '100%',
              padding: '4px 6px 4px 10px',
              borderRadius: 6,
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-muted)',
              color: 'var(--color-text-secondary)',
              fontSize: '0.75rem',
            }}
          >
            <button
              type="button"
              title={t('marketView.selection.editChip')}
              onClick={() => chartSelectionStore.openEditor(c.id)}
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                maxWidth: 240,
                border: 'none',
                background: 'transparent',
                color: 'inherit',
                font: 'inherit',
                padding: 0,
                cursor: 'pointer',
              }}
            >
              {label}
            </button>
            <button
              type="button"
              aria-label={t('marketView.selection.removeChip')}
              onClick={() => chartSelectionStore.remove(c.id)}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
                width: 16,
                height: 16,
                padding: 0,
                border: 'none',
                background: 'transparent',
                color: 'var(--color-text-tertiary)',
                cursor: 'pointer',
              }}
            >
              <X style={{ width: 12, height: 12 }} />
            </button>
          </span>
        );
      })}
    </div>
  );
}
