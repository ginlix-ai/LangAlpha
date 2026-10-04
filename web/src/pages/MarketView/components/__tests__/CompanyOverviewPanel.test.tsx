import { afterEach, describe, it, expect, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import i18n from '@/i18n';

vi.mock('../../../ChatAgent/components/charts/MarketDataCharts', () => {
  const Stub = () => null;
  return {
    PerformanceBarChart: Stub,
    AnalystRatingsChart: Stub,
    QuarterlyRevenueChart: Stub,
    MarginsChart: Stub,
    EarningsSurpriseChart: Stub,
    CashFlowChart: Stub,
    RevenueBreakdownChart: Stub,
  };
});

import CompanyOverviewPanel from '../CompanyOverviewPanel';

const props = { symbol: '000300.SH', visible: true, onClose: () => {}, loading: false };

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

describe('CompanyOverviewPanel', () => {
  it('an index prints points bare, the symbol once, and says why fundamentals are absent', () => {
    render(
      <CompanyOverviewPanel
        {...props}
        data={{
          symbol: '000300.SH', currency: 'CNY', assetClass: 'index',
          quote: { price: 4439.14, change: -78.95, changePct: -1.75, yearLow: 4394.29, yearHigh: 5064.27 },
        }}
      />,
    );
    expect(screen.getByText('4,439.14')).toBeInTheDocument();
    expect(screen.getByText('-78.95 (-1.75%)')).toBeInTheDocument();
    expect(screen.getByText('4,394.29 - 5,064.27')).toBeInTheDocument();
    expect(screen.getAllByText('000300.SH')).toHaveLength(1);
    expect(screen.getByText(/apply to companies, not to an index/)).toBeInTheDocument();
  });

  it('an index the payload does not label is still read from its code', () => {
    render(<CompanyOverviewPanel {...props} data={{ symbol: '000300.SH', currency: 'CNY', quote: { price: 4439.14 } }} />);
    expect(screen.getByText('4,439.14')).toBeInTheDocument();
    expect(screen.queryAllByText(/¥/)).toHaveLength(0);
  });

  it('a company leads with both names and the ticker, and counts in the reader\'s locale', async () => {
    render(
      <CompanyOverviewPanel
        {...props}
        symbol="ACME.HK"
        data={{
          symbol: 'ACME.HK', name: '甲公司', nameEn: 'Acme Holdings', currency: 'HKD',
          quote: { price: 436.6, change: 3.4, changePct: 0.78, volume: 12_345_678, pe: 21.456 },
        }}
      />,
    );
    expect(screen.getByText('甲公司')).toBeInTheDocument();
    expect(screen.getByText('Acme Holdings')).toBeInTheDocument();
    expect(screen.getByText('ACME.HK')).toBeInTheDocument();
    expect(screen.getByText('HK$436.60')).toBeInTheDocument();
    expect(screen.getByText('+HK$3.40 (+0.78%)')).toBeInTheDocument();
    expect(screen.getByText('12.35M')).toBeInTheDocument();
    expect(screen.getByText('21.46')).toBeInTheDocument();

    await act(() => i18n.changeLanguage('zh-CN'));
    expect(screen.getByText('1234.57万')).toBeInTheDocument();
  });

  it('clips long names to an ellipsis but never the ticker', () => {
    render(
      <CompanyOverviewPanel
        {...props}
        symbol="ACME.HK"
        data={{
          symbol: 'ACME.HK', name: '甲公司', nameEn: 'Acme Holdings International Group Limited', currency: 'HKD',
          quote: { price: 436.6 },
        }}
      />,
    );
    const row = screen.getByText('ACME.HK').parentElement!;
    expect(row.style.minWidth).toBe('0px');
    for (const name of ['甲公司', 'Acme Holdings International Group Limited']) {
      const el = screen.getByText(name);
      expect(el.style.textOverflow).toBe('ellipsis');
      expect(el.style.minWidth).toBe('0px');
      expect(el).toHaveAttribute('title', name);
    }
    expect(screen.getByText('ACME.HK').style.flexShrink).toBe('0');
  });

  it('a company payload with nothing to draw reads as no data', () => {
    render(<CompanyOverviewPanel {...props} symbol="ACME" data={{ symbol: 'ACME' }} />);
    expect(screen.getByText('No data available')).toBeInTheDocument();
  });
});
