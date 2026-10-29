import { afterEach, describe, it, expect, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

const { searchStocks } = vi.hoisted(() => ({ searchStocks: vi.fn() }));
vi.mock('@/lib/marketUtils', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, searchStocks };
});

import StockHeader from '../StockHeader';
import { deriveStockQuote, type StockQuoteInputs } from '../../hooks/useStockQuoteModel';
import { SYMBOL_SEARCH_DEBOUNCE_MS } from '@/hooks/useSymbolSearch';
import type { Freshness, SnapshotData } from '@/types/market';
import type { DataLevel } from '../../hooks/useMarketDataWS';

const baseInputs: StockQuoteInputs = {
  symbol: 'AMD',
  stockInfo: null,
  realTimePrice: null,
  quoteData: null,
  snapshot: null,
  marketStatus: { providers: ['ginlix-data', 'yfinance', 'fmp'] } as Record<string, unknown>,
  wsStatus: 'disconnected',
};

interface HeaderProps {
  inputs?: Partial<StockQuoteInputs>;
  wsDataLevel?: DataLevel;
  onSwitchSymbol?: (symbol: string, hit?: unknown) => void;
  chartFreshness?: Freshness | null;
}

// The header prints a model the host derives; the tests build it from the
// same inputs the host would, so each case still reads as raw quote data.
function Header({ inputs = {}, wsDataLevel, onSwitchSymbol, chartFreshness }: HeaderProps): React.ReactElement {
  const merged = { ...baseInputs, ...inputs };
  return (
    <StockHeader
      symbol={merged.symbol}
      quote={deriveStockQuote(merged)}
      chartMeta={null}
      onToggleOverview={() => {}}
      wsStatus={merged.wsStatus}
      wsHasData={merged.wsHasData}
      wsDataLevel={wsDataLevel}
      onSwitchSymbol={onSwitchSymbol}
      chartFreshness={chartFreshness}
    />
  );
}

/** The headline figure alone; the currency code rides beside it in its own span. */
const headlineFigure = (container: HTMLElement) => container.querySelector('.stock-price')?.firstChild?.textContent;

const snap = (source: string | null): SnapshotData => ({ symbol: 'AMD', price: 120.5, source });

describe('StockHeader source tooltip', () => {
  it('shows the snapshot-filling provider when not live', () => {
    render(<Header inputs={{ snapshot: snap('fmp') }} />);
    expect(screen.getByText('Quote: FMP · Freshness not declared')).toBeInTheDocument();
  });

  it('shows the WS feed provider when live', () => {
    render(
      <Header
        inputs={{ wsStatus: 'connected', wsHasData: true, snapshot: snap('fmp') }} // live price comes from WS, not this row
        wsDataLevel="second"
      />,
    );
    expect(screen.getByText(/Quote: Ginlix Data/)).toBeInTheDocument();
  });

  it('falls back to the enabled-provider list when the row has no source', () => {
    render(<Header inputs={{ snapshot: snap(null) }} />);
    expect(screen.getByText(/Quote: Ginlix Data, Yahoo Finance, FMP/)).toBeInTheDocument();
  });

  it('opens from a tab stop the lines describe, not only under a hovering pointer', () => {
    render(<Header inputs={{ snapshot: snap('fmp') }} />);
    const tooltip = screen.getByRole('tooltip', { hidden: true });
    expect(tooltip.textContent).toContain('Quote: FMP');
    const trigger = tooltip.parentElement!;
    expect(trigger.tabIndex).toBe(0);
    expect(trigger.getAttribute('aria-describedby')).toBe(tooltip.id);
  });
});

describe('StockHeader measured freshness', () => {
  const measured = (over: Partial<Freshness>): Freshness =>
    ({ label: 'live', measured: true, ...over }) as Freshness;
  // Same Shanghai day as the fixtures' 15:00 stamps, so they print as a bare time.
  const onFixtureDay = () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.UTC(2026, 8, 9, 7, 30));
  };

  it('the badge reads the measurement, not the declared tier', () => {
    // The row declares realtime; the boundary measured 14 minutes of lag.
    render(
      <Header
        inputs={{
          marketPhase: 'open',
          snapshot: {
            symbol: 'AMD', price: 120, source: 'fmp', tier: 'realtime',
            freshness: measured({ label: 'delayed', lag_s: 840, source: 'fmp' }),
          },
        }}
      />,
    );
    expect(screen.getByText('Delayed 14 min')).toBeInTheDocument();
    expect(screen.getByText('Quote: FMP · Delayed 14 min')).toBeInTheDocument();
  });

  it('an end-of-day row reads Last close, as the artifact badge does', () => {
    render(
      <Header
        inputs={{
          marketPhase: 'open',
          snapshot: {
            symbol: 'AMD', price: 120, source: 'daily', tier: 'eod',
            freshness: measured({ label: 'delayed', lag_s: 86_400, source: 'daily', interval: '1day' }),
          },
        }}
      />,
    );
    expect(screen.getByText('Last close')).toBeInTheDocument();
    expect(screen.queryByText(/1440/)).not.toBeInTheDocument();
  });

  it('a measured stale quote never reads Realtime', () => {
    render(
      <Header
        inputs={{
          marketPhase: 'open',
          snapshot: {
            symbol: 'AMD', price: 120, source: 'fmp', tier: 'realtime',
            freshness: measured({ label: 'stale', lag_s: 4000, source: 'fmp' }),
          },
        }}
      />,
    );
    expect(screen.getByText('Stale')).toBeInTheDocument();
  });

  it('an unmeasured row keeps the declared tier', () => {
    render(
      <Header
        inputs={{
          symbol: '0700.HK',
          marketPhase: 'open',
          snapshot: {
            symbol: '0700.HK', price: 434, source: 'fmp', tier: 'delayed_15m',
            freshness: { label: 'delayed', measured: false, source: 'fmp' },
          },
        }}
      />,
    );
    expect(screen.getByText('HK Delayed 15 min')).toBeInTheDocument();
  });

  it('the tooltip carries a second line for the chart, with its own provider', () => {
    // The quote is Tushare at 15:00; the chart is Yahoo and stopped at 14:56,
    // three minutes short of the Shanghai closing auction.
    onFixtureDay();
    render(
      <Header
        inputs={{
          symbol: '600519.SS',
          marketPhase: 'closed',
          snapshot: {
            symbol: '600519.SS', price: 1290.88, source: 'tushare', tier: 'realtime',
            as_of: Date.UTC(2026, 8, 9, 7, 0),
            freshness: measured({ label: 'live', lag_s: 0, source: 'tushare', closed: true }),
          },
        }}
        chartFreshness={measured({
          label: 'incomplete', lag_s: 180, source: 'yfinance', interval: '1min',
          actual_latest: Date.UTC(2026, 8, 9, 6, 56), closed: true,
        })}
      />,
    );
    // The closing print on a closed venue is the last close, not a live price.
    expect(screen.getByText('Quote: Tushare · Last close · 15:00')).toBeInTheDocument();
    expect(
      screen.getByText('Chart: Yahoo Finance · 3 min short of close · last bar 14:56'),
    ).toBeInTheDocument();
  });

  it('a live chart says so, and no chart freshness means no chart line', () => {
    onFixtureDay();
    const { rerender } = render(
      <Header
        inputs={{ symbol: '600519.SS' }}
        chartFreshness={measured({
          label: 'live', lag_s: 0, source: 'tushare', interval: '1min',
          actual_latest: Date.UTC(2026, 8, 9, 7, 0),
        })}
      />,
    );
    expect(screen.getByText('Chart: Tushare · live · last bar 15:00')).toBeInTheDocument();
    rerender(<Header inputs={{ symbol: '600519.SS' }} />);
    expect(screen.queryByText(/^Chart: /)).not.toBeInTheDocument();
  });

  it('a complete series on a closed venue reads last close, and dates a print from an earlier day', () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.UTC(2026, 8, 13, 2, 0)); // the Sunday after
    render(
      <Header
        inputs={{
          symbol: '600519.SS',
          marketPhase: 'closed',
          snapshot: {
            symbol: '600519.SS', price: 1290.88, source: 'tushare', tier: 'realtime',
            as_of: Date.UTC(2026, 8, 11, 7, 0),
            freshness: measured({ label: 'live', lag_s: 0, source: 'tushare', closed: true }),
          },
        }}
        chartFreshness={measured({
          label: 'live', lag_s: 0, source: 'tushare', interval: '1day', closed: true,
          actual_latest: Date.UTC(2026, 8, 10, 16, 0),
        })}
      />,
    );
    expect(screen.getByText(/^Quote: Tushare · Last close · .*11.*15:00$/)).toBeInTheDocument();
    expect(screen.getByText(/^Chart: Tushare · Last close · last bar .*$/)).toBeInTheDocument();
    expect(screen.queryByText(/Realtime|· live/)).not.toBeInTheDocument();
  });
});

describe('StockHeader price section (market convention)', () => {
  // Quote row after the close: official close = prev 100 + regular −4 = 96;
  // the after-hours move (−1% vs that close) renders on its own line.
  const postSnap: SnapshotData = {
    symbol: 'AMD',
    price: 96.5,
    previous_close: 100,
    regular_trading_change: -4,
    late_trading_change_percent: -1.0,
    source: 'x',
  };
  const closedStatus = { market: 'closed', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>;
  const quoteRow = { symbol: 'AMD', price: 96.5, open: 0, high: 0, low: 0, change: -3.5, changePercent: -3.5, volume: 0, previousClose: 100 };

  it('after the close, headlines the official close with a coherent change pair', () => {
    const { container } = render(
      <Header inputs={{ marketStatus: closedStatus, snapshot: postSnap, realTimePrice: quoteRow }} />,
    );
    expect(screen.getByText('96.00')).toBeInTheDocument();
    expect(screen.getByText('-4.00 -4.00%')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.04');
    expect(ext?.textContent).toContain('(-1.00%)');
  });

  it('labels the change cell as extended-hours when an ext line is showing', () => {
    render(<Header inputs={{ marketStatus: closedStatus, snapshot: postSnap, realTimePrice: quoteRow }} />);
    expect(screen.getByText('Change % incl. ext')).toBeInTheDocument();
  });

  it('keeps the plain change label with no ext line', () => {
    render(<Header inputs={{ snapshot: snap('fmp') }} />);
    expect(screen.getByText('Change %')).toBeInTheDocument();
  });

  it('the big close is refresh-stable: a quote-row price never replaces it', () => {
    // The row's own price field (96.5, a different tape moment) must not leak
    // into the big number — that mix was the refresh nondeterminism.
    render(<Header inputs={{ marketStatus: closedStatus, snapshot: postSnap, realTimePrice: quoteRow }} />);
    expect(screen.queryByText('96.50')).not.toBeInTheDocument();
  });

  it('a live WS tick updates the after-hours line, not the official close', () => {
    const tick = { ...quoteRow, price: 95.5, timestamp: 1700000000000 };
    const { container } = render(
      <Header inputs={{ marketStatus: closedStatus, snapshot: postSnap, realTimePrice: tick }} />,
    );
    expect(screen.getByText('96.00')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.50');
    expect(ext?.textContent).toContain('(-0.52%)');
  });

  it('renders the provider-exact close and full-precision pair when the row carries them', () => {
    // The rounded change fields say close = 96.00; the exact fields say 96.06
    // with the AH print at exactly 95.00 — the exact values must win.
    const exactSnap: SnapshotData = {
      ...postSnap,
      regular_close: 96.06,
      late_trading_change: -1.06,
      late_trading_change_percent: -1.1,
    };
    const { container } = render(
      <Header inputs={{ marketStatus: closedStatus, snapshot: exactSnap, realTimePrice: quoteRow }} />,
    );
    expect(headlineFigure(container)).toBe('96.06');
    expect(screen.getByText('-3.94 -3.94%')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.00');
    expect(ext?.textContent).toContain('-1.06');
    expect(ext?.textContent).toContain('(-1.10%)');
  });

  it('the after-hours line shows the aggregate close, matching the chart', () => {
    // last_minute_close (consolidated last sale) beats the provider's
    // odd-lot-tainted late change; the triple is re-derived from it.
    const aggSnap: SnapshotData = {
      ...postSnap,
      regular_close: 96.06,
      late_trading_change: -1.06,
      last_minute_close: 95.1,
    };
    const { container } = render(
      <Header inputs={{ marketStatus: closedStatus, snapshot: aggSnap, realTimePrice: quoteRow }} />,
    );
    expect(headlineFigure(container)).toBe('96.06');
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.10');
    expect(ext?.textContent).toContain('-0.96');
    expect(ext?.textContent).toContain('(-1.00%)');
  });

  it('pre-market headlines the previous close with the early move on its own line', () => {
    const preStatus = { market: 'open', afterHours: false, earlyHours: true, providers: [] } as Record<string, unknown>;
    const preSnap: SnapshotData = { symbol: 'AMD', price: 102, previous_close: 100, early_trading_change_percent: 2.0, source: 'x' };
    const { container } = render(
      <Header inputs={{ marketStatus: preStatus, snapshot: preSnap, realTimePrice: { ...quoteRow, price: 102 } }} />,
    );
    expect(headlineFigure(container)).toBe('100.00');
    expect(container.querySelector('.stock-change')).toBeNull();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('102.00');
    expect(ext?.textContent).toContain('(+2.00%)');
  });

  it('regular session renders the row price and change pair unchanged', () => {
    const openStatus = { market: 'open', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>;
    render(
      <Header
        inputs={{
          marketStatus: openStatus,
          snapshot: { symbol: 'AMD', price: 101.23, previous_close: 100, source: 'x' },
          realTimePrice: { ...quoteRow, price: 101.23, change: 1.23, changePercent: 1.23 },
        }}
      />,
    );
    expect(screen.getByText('101.23')).toBeInTheDocument();
    expect(screen.getByText('+1.23 +1.23%')).toBeInTheDocument();
  });

  it('a row without a change pair shows a dash, not a zero move', () => {
    const { container } = render(
      <Header inputs={{ stockInfo: { Symbol: 'AMD', Name: 'AMD', Price: 101.23 } as never }} />,
    );
    expect(headlineFigure(container)).toBe('101.23');
    expect(container.querySelector('.stock-change')?.textContent).toBe('—');
    expect(screen.queryByText('+0.00 +0.00%')).not.toBeInTheDocument();
  });
});

describe('StockHeader market status badge', () => {
  it('shows Closed with the venue prefix when the market phase is closed', () => {
    render(<Header inputs={{ symbol: '0700.HK', marketPhase: 'closed' }} />);
    expect(screen.getByText('HK Closed')).toBeInTheDocument();
  });

  it('shows a bare Closed for US symbols', () => {
    render(<Header inputs={{ marketPhase: 'closed' }} />);
    expect(screen.getByText('Closed')).toBeInTheDocument();
  });

  it('stays on Delayed during pre/post phases and when the phase is unknown', () => {
    const { rerender } = render(<Header inputs={{ symbol: '0700.HK', marketPhase: 'post' }} />);
    expect(screen.getByText('HK Delayed')).toBeInTheDocument();
    rerender(<Header inputs={{ symbol: '0700.HK', marketPhase: null }} />);
    expect(screen.getByText('HK Delayed')).toBeInTheDocument();
  });

  it('reads the session label from the provider-declared tier, never the transport', () => {
    const { rerender } = render(
      <Header inputs={{ symbol: '600519.SS', marketPhase: 'open', snapshot: { symbol: '600519.SS', price: 1290.88, source: 'tushare', tier: 'realtime', as_of: Date.UTC(2026, 8, 9, 7, 0) } }} />,
    );
    expect(screen.getByText('SH Realtime')).toBeInTheDocument();
    // The print time renders in the venue's clock (07:00Z = 15:00 Shanghai).
    expect(screen.getByText(/Quote: Tushare · Realtime · .*15:00/)).toBeInTheDocument();

    rerender(<Header inputs={{ symbol: '0700.HK', marketPhase: 'open', snapshot: { symbol: '0700.HK', price: 434, source: 'fmp', tier: 'delayed_15m' } }} />);
    expect(screen.getByText('HK Delayed 15 min')).toBeInTheDocument();
    expect(screen.getByText('Quote: FMP · Delayed 15 min')).toBeInTheDocument();

    rerender(<Header inputs={{ symbol: '920300.BJ', marketPhase: 'open', snapshot: { symbol: '920300.BJ', price: 9.93, source: 'daily', tier: 'eod' } }} />);
    expect(screen.getByText('BJ Last close')).toBeInTheDocument();
    expect(screen.getByText('Quote: Daily bars · Last close')).toBeInTheDocument();
  });

  it('never claims realtime for a row with no declared tier', () => {
    render(<Header inputs={{ marketPhase: 'open', snapshot: { symbol: 'AMD', price: 120, source: 'fmp' } }} />);
    expect(screen.getByText('Delayed')).toBeInTheDocument();
    expect(screen.getByText('Quote: FMP · Freshness not declared')).toBeInTheDocument();
  });

  it('a live WS feed wins over a stale closed phase', () => {
    render(
      <Header inputs={{ wsStatus: 'connected', wsHasData: true, marketPhase: 'closed' }} wsDataLevel="second" />,
    );
    expect(screen.getByText('Live')).toBeInTheDocument();
  });
});

describe('StockHeader volume cell', () => {
  const cellValue = (label: string) =>
    screen.getAllByText(label).map((el) => el.nextElementSibling?.textContent);

  it('labels the session volume as Volume', () => {
    render(
      <Header inputs={{ stockInfo: { Symbol: 'AMD', Name: 'AMD', Volume: 1_200_000 } as never, quoteData: { avgVolume: 2_500_000 } }} />,
    );
    expect(cellValue('Volume')).toEqual(['1.20M']);
    expect(cellValue('Avg Vol (3M)')).toEqual(['2.50M']);
  });

  it('says it is the 3-month average when that is what it falls back to', () => {
    render(<Header inputs={{ quoteData: { avgVolume: 2_500_000 } }} />);
    expect(screen.queryByText('Volume')).not.toBeInTheDocument();
    expect(cellValue('Avg Vol (3M)')).toEqual(['2.50M', '2.50M']);
  });

  it('colours Change % by the percent it prints, even with no absolute change', () => {
    render(<Header inputs={{ realTimePrice: { price: 118, change: null, changePercent: -1.5 } as never }} />);
    const value = screen.getByText('-1.50%');
    expect(value.className).toContain('negative');
  });
});

describe('StockHeader symbol switch', () => {
  it('keeps the ticker a label when nothing can be switched', () => {
    // By class, not by text: a listing with no known name falls back to its
    // symbol, so the ticker and the name read the same and a text query is
    // ambiguous.
    const { container } = render(<Header />);
    expect(container.querySelector('.stock-symbol')?.tagName).toBe('SPAN');
  });

  it('makes the ticker a control that opens the search', async () => {
    render(<Header onSwitchSymbol={() => {}} />);
    const ticker = screen.getByRole('button', { name: /AMD/ });
    fireEvent.click(ticker);
    expect(await screen.findByRole('combobox')).toBeTruthy();
  });

  it('names the control by what it does, not just the ticker', () => {
    render(<Header onSwitchSymbol={() => {}} />);
    expect(screen.getByRole('button', { name: 'Change symbol, currently AMD' })).toBeInTheDocument();
  });

  it('hands a picked hit to onSwitchSymbol with its name', async () => {
    searchStocks.mockResolvedValue({ query: 'goog', results: [{ symbol: 'GOOGL', name: 'Alphabet Inc.' }], count: 1 });
    const onSwitchSymbol = vi.fn();
    render(<Header onSwitchSymbol={onSwitchSymbol} />);
    fireEvent.click(screen.getByRole('button', { name: /AMD/ }));
    // The popover opens on real timers (findBy polls them); only the rest
    // period is faked.
    const input = await screen.findByRole('combobox');
    vi.useFakeTimers();
    fireEvent.change(input, { target: { value: 'goog' } });
    await act(async () => { await vi.advanceTimersByTimeAsync(SYMBOL_SEARCH_DEBOUNCE_MS); });
    fireEvent.click(screen.getByText('Alphabet Inc.'));
    expect(onSwitchSymbol).toHaveBeenCalledWith('GOOGL', expect.objectContaining({ name: 'Alphabet Inc.' }));
  });
});

describe('StockHeader display currency', () => {
  const row = { symbol: '600519.SS', price: 1500, open: 0, high: 0, low: 0, change: 10, changePercent: 0.67, volume: 0, previousClose: 1490 };
  const code = (container: HTMLElement) => container.querySelector('.stock-price-currency')?.textContent;

  it('names the snapshot currency once, beside a bare headline', () => {
    const { container } = render(
      <Header inputs={{ symbol: '600519.SS', snapshot: { symbol: '600519.SS', price: 1500, currency: 'CNY', source: 'tushare' }, realTimePrice: row }} />,
    );
    expect(headlineFigure(container)).toBe('1500.00');
    expect(code(container)).toBe('CNY');
    expect(screen.getByText('+10.00 +0.67%')).toBeInTheDocument();
  });

  it('falls back to the exchange suffix when the row carries no currency', () => {
    // Legacy cached snapshots predate the wire field; a CN listing must not
    // read as dollars.
    const { container } = render(
      <Header inputs={{ symbol: '600519.SS', snapshot: { symbol: '600519.SS', price: 1500, source: 'tushare' }, realTimePrice: row }} />,
    );
    expect(code(container)).toBe('CNY');
  });

  it('US listings name USD', () => {
    const { container } = render(
      <Header inputs={{ snapshot: snap('fmp'), realTimePrice: { ...row, symbol: 'AMD', price: 120.5 } }} />,
    );
    expect(headlineFigure(container)).toBe('120.50');
    expect(code(container)).toBe('USD');
  });

  it('prints no code while there is no price', () => {
    const { container } = render(<Header />);
    expect(code(container)).toBeUndefined();
  });
});

describe('StockHeader names', () => {
  it('shows the local and English names together for a CN listing', () => {
    render(
      <Header inputs={{ symbol: '600519.SS', stockInfo: { Symbol: '600519.SS', Name: 'Kweichow Moutai', NameLocal: '贵州茅台', NameEn: 'Kweichow Moutai' } as never }} />,
    );
    expect(screen.getByText('贵州茅台')).toBeInTheDocument();
    expect(screen.getByText('Kweichow Moutai')).toBeInTheDocument();
  });

  it('a clicked search hit names the listing without mixing in the previous stockInfo', () => {
    render(
      <Header
        inputs={{
          symbol: '000858.SZ',
          stockInfo: { Symbol: '600519.SS', Name: 'Kweichow Moutai', NameLocal: '贵州茅台' } as never,
          displayOverride: { name: 'Wuliangye', nameLocal: '五粮液', exchange: 'SZSE' },
        }}
      />,
    );
    expect(screen.getByText('五粮液')).toBeInTheDocument();
    expect(screen.queryByText('贵州茅台')).not.toBeInTheDocument();
  });
});

describe('StockHeader index row', () => {
  it('prints index points with no currency and the symbol once when no name is known', () => {
    const { container } = render(
      <Header
        inputs={{
          symbol: '000300.SH',
          snapshot: { symbol: '000300.SH', price: 4521.5, currency: 'CNY', asset_class: 'index', source: 'x' },
          realTimePrice: { symbol: '000300.SH', price: 4521.5, open: 0, high: 0, low: 0, change: -12.25, changePercent: -0.27, volume: 0, previousClose: 4533.75 },
        }}
      />,
    );
    expect(headlineFigure(container)).toBe('4521.50');
    expect(container.querySelector('.stock-price-currency')).toBeNull();
    expect(screen.getByText('-12.25 -0.27%')).toBeInTheDocument();
    expect(container.querySelector('.stock-names')).toBeNull();
  });

  it('renders a provided name beside the symbol', () => {
    render(
      <Header
        inputs={{
          symbol: '000300.SH',
          displayOverride: { name: 'CSI 300', exchange: 'SSE' },
          snapshot: { symbol: '000300.SH', price: 4521.5, currency: 'CNY', asset_class: 'index', source: 'x' },
        }}
      />,
    );
    expect(screen.getByText('CSI 300')).toBeInTheDocument();
  });
});

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
});
