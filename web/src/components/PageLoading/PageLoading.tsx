import React from 'react';
import { useTranslation } from 'react-i18next';
import './PageLoading.css';

/**
 * The quote wall behind the loader: index/futures/FX/rates/crypto symbols
 * with plausible static prices. Purely decorative set dressing — values are
 * frozen and marked aria-hidden, never live data.
 */
const QUOTES: Array<[string, string]> = [
  ['AAPL', '227.15'], ['MSFT', '512.30'], ['NVDA', '171.42'], ['GOOG', '201.88'],
  ['AMZN', '228.51'], ['META', '712.04'], ['TSLA', '316.70'], ['AVGO', '274.92'],
  ['JPM', '289.11'], ['XOM', '112.44'], ['V', '357.20'], ['WMT', '98.41'],
  ['LLY', '781.22'], ['MA', '559.43'], ['JNJ', '156.72'], ['PG', '161.33'],
  ['HD', '367.85'], ['COST', '989.52'], ['ORCL', '244.61'], ['CRM', '268.94'],
  ['BAC', '47.32'], ['KO', '70.14'], ['PEP', '134.55'], ['CSCO', '68.91'],
  ['MRK', '81.44'], ['ABBV', '188.23'], ['NFLX', '1287.40'], ['AMD', '158.33'],
  ['INTC', '22.91'], ['QCOM', '163.72'], ['TXN', '214.53'], ['IBM', '283.44'],
  ['GE', '254.11'], ['CAT', '414.22'], ['BA', '214.93'], ['SPX', '6285.30'],
  ['NDX', '22870.10'], ['DJI', '44650.70'], ['RUT', '2248.60'], ['VIX', '16.42'],
  ['ES', '6301.25'], ['NQ', '22945.50'], ['CL', '68.45'], ['GC', '3352.60'],
  ['SI', '36.92'], ['HG', '5.58'], ['ZN', '111.05'], ['BTC', '117850'],
  ['ETH', '2975.40'], ['EURUSD', '1.1690'], ['USDJPY', '147.42'], ['GBPUSD', '1.3490'],
  ['US10Y', '4.42'], ['US2Y', '3.88'], ['DAX', '23412.5'], ['N225', '39820.2'],
  ['HSI', '24102.8'], ['STOXX', '543.2'], ['FTSE', '8921.4'], ['KOSPI', '3182.5'],
];

/**
 * Rows must outsize any viewport (the component clips the excess): 120 rows
 * of ~560ch cover 4K in both axes at the 12px wall type size. A tiny seeded
 * LCG stands in for Math.random so the wall is identical on every render.
 */
function buildWall(): string {
  let seed = 11;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  const tokens = QUOTES.map(
    ([sym, px]) =>
      `${sym} ${px} ${rand() < 0.5 ? '▲' : '▼'}${(0.02 + rand() * 2.4).toFixed(2)}%`
  );
  const rows: string[] = [];
  for (let r = 0; r < 120; r++) {
    const shift = (r * 7) % tokens.length;
    const rotated = tokens.slice(shift).concat(tokens.slice(0, shift));
    rows.push(rotated.concat(rotated, rotated).join('   ').slice(0, 560));
  }
  return rows.join('\n');
}

const WALL = buildWall();

interface PageLoadingProps {
  /**
   * `screen` (default) fills the viewport on the page background — for the
   * auth gates and top-level route Suspense. `pane` fills its parent with a
   * transparent background — for Suspense fallbacks inside the app shell,
   * where the layout already paints the background.
   */
  variant?: 'screen' | 'pane';
}

/**
 * Branded loading state: the research terminal warming up. A dim full-bleed
 * wall of quotes with an ember scan band sweeping through it — the login
 * tape idiom at room scale, in pure CSS so it stays in the main bundle and
 * mounts instantly while route chunks load.
 */
function PageLoading({ variant = 'screen' }: PageLoadingProps) {
  const { t } = useTranslation();
  return (
    <div
      className={`page-loading${variant === 'pane' ? ' page-loading--pane' : ''}`}
      role="status"
    >
      <div className="page-loading__wall" aria-hidden="true">
        {WALL}
      </div>
      <div className="page-loading__scan" aria-hidden="true">
        <div className="page-loading__scan-inner">{WALL}</div>
      </div>
      <div className="page-loading__center">
        <p className="page-loading__label">
          {t('common.loading')}
          <span className="page-loading__cursor" aria-hidden="true">
            ▉
          </span>
        </p>
      </div>
    </div>
  );
}

export default PageLoading;
