import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Eye, EyeOff } from 'lucide-react';
import { useValuesHidden } from '@/components/orders/valuesHidden';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney } from '@/lib/bars';
import { openInMain } from '@/lib/desktop';
import { DASH, integer } from '@/lib/format';
import { changeDirection } from '@/pages/ChatAgent/components/charts/termChange';
import { formatPortfolioMoney } from '@/pages/Dashboard/utils/portfolioSummary';
import { summarizePortfolioDay } from './glanceSummary';
import { ExtendedLine, Move, Pending, QuoteRowButton, Section } from './primitives';
import { marketPath, type SectionProps } from './panelContext';
import { usePortfolioGlance } from './useGlanceData';

const TOP_HOLDINGS = 3;
const MASK = '••••••';

export function PortfolioSection({ active, marketStatus }: SectionProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { rows, count, loaded, isError, quotesPending } = usePortfolioGlance(active, marketStatus);
  const [hidden, toggleHidden] = useValuesHidden();
  const day = useMemo(() => summarizePortfolioDay(rows), [rows]);

  const eye = (
    <button
      type="button"
      className="mb-icon-btn"
      onClick={toggleHidden}
      aria-label={hidden ? t('menubar.portfolio.showValues') : t('menubar.portfolio.hideValues')}
      aria-pressed={hidden}
    >
      {hidden ? <EyeOff size={13} /> : <Eye size={13} />}
    </button>
  );

  if (loaded && count === 0) {
    return (
      <Section title={t('menubar.portfolio.title')}>
        <p className="mb-empty">{t('menubar.portfolio.empty')}</p>
        <button type="button" className="mb-link" onClick={() => openInMain('/dashboard')}>
          {t('menubar.portfolio.add')}
        </button>
      </Section>
    );
  }

  const changeMoney =
    day?.dayChange == null
      ? null
      : `${day.dayChange > 0 ? '+' : day.dayChange < 0 ? '-' : ''}${formatPortfolioMoney(Math.abs(day.dayChange), day.currency, locale)}`;

  return (
    <Section title={t('menubar.portfolio.title')} aside={count > 0 ? eye : undefined}>
      {day == null ? (
        (!loaded && !isError) || quotesPending ? (
          <div className="mb-hero-row" aria-busy="true">
            <Pending className="mb-hero" />
          </div>
        ) : (
          <div className="mb-hero-row">
            <span className="mb-hero mb-num mb-t3">{DASH}</span>
          </div>
        )
      ) : (
        <>
          <button type="button" className="mb-hero-row mb-row-btn" onClick={() => openInMain('/dashboard')}>
            <span className="mb-hero mb-num">{hidden ? MASK : formatPortfolioMoney(day.totalValue, day.currency, locale)}</span>
            <span className="mb-hero-figs">
              <span className="mb-hero-change mb-num">
                {!hidden && changeMoney && (
                  <span className="mb-move" data-dir={changeDirection(day.dayChangePct ?? 0)}>
                    {changeMoney}
                  </span>
                )}
                <Move value={day.dayChangePct} />
              </span>
              {day.ext && <ExtendedLine ext={day.ext} />}
            </span>
          </button>
          <ul className="mb-rows">
            {day.holdings.slice(0, TOP_HOLDINGS).map((h) => (
              <QuoteRowButton
                key={h.symbol}
                symbol={h.symbol}
                aside={<span className="mb-weight mb-num">{t('menubar.portfolio.weight', { pct: integer(h.weight, locale) })}</span>}
                price={formatMoney(h.price, h.currency, locale)}
                pct={h.changePercent}
                ext={h.ext}
                extPrice={h.ext ? formatMoney(h.ext.price, h.currency, locale) : undefined}
                onOpen={() => openInMain(marketPath(h.symbol))}
              />
            ))}
          </ul>
        </>
      )}
      <p className="mb-note">
        {t('menubar.portfolio.manualNote')}
        {day && day.otherCurrencies.length > 0 && (
          <> · {t('menubar.portfolio.otherCurrencies', { list: day.otherCurrencies.join(', ') })}</>
        )}
      </p>
    </Section>
  );
}
