import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney, quoteCurrency } from '@/lib/bars';
import { openInMain } from '@/lib/desktop';
import { QuoteRowButton, Section } from './primitives';
import { marketPath, type SectionProps } from './panelContext';
import { useWatchlistGlance } from './useGlanceData';

export function WatchlistSection({ active, settings, marketStatus }: SectionProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { watchlistId, watchlistRows } = settings.panel;
  const { rows, total, loaded } = useWatchlistGlance(active, watchlistId, watchlistRows, marketStatus);

  const aside =
    total > 0 ? (
      <button type="button" className="mb-aside-link" onClick={() => openInMain('/market')}>
        {t('menubar.watchlist.count', { shown: rows.length, total })}
      </button>
    ) : undefined;

  return (
    <Section title={t('menubar.watchlist.title')} aside={aside}>
      {loaded && total === 0 ? (
        <>
          <p className="mb-empty">{t('menubar.watchlist.empty')}</p>
          <button type="button" className="mb-link" onClick={() => openInMain('/market')}>
            {t('menubar.watchlist.add')}
          </button>
        </>
      ) : (
        <ul className="mb-rows">
          {rows.map((r) => {
            const code = quoteCurrency(r.currency, r.symbol, r.assetClass);
            const money = (value: number) => formatMoney(value, code, locale, { upTo: r.places });
            return (
              <QuoteRowButton
                key={r.symbol}
                symbol={r.symbol}
                price={r.price == null ? null : money(r.price)}
                pct={r.pct}
                ext={r.ext}
                extPrice={r.ext ? money(r.ext.price) : undefined}
                pending={r.pending}
                onOpen={() => openInMain(marketPath(r.symbol))}
              />
            );
          })}
        </ul>
      )}
    </Section>
  );
}
