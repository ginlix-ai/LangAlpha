// @vitest-environment node
/**
 * The one reading of a company-overview quote block that the chat cards and
 * the MarketView panel share. Neutral placeholder symbols and fabricated
 * numbers only; the index codes are public benchmark spellings.
 */
import { describe, it, expect } from 'vitest';
import { deriveOverviewQuote } from '../overview';

describe('deriveOverviewQuote', () => {
  it('reads a snapshot-shaped quote by its regular session, not the blended price', () => {
    const q = deriveOverviewQuote(
      {
        name: 'Acme Corp',
        currency: 'USD',
        quote: {
          price: 101.25, change: 3.25, changePct: 3.32,
          regularClose: 100, regularChange: 2, regularChangePct: 2.04,
          marketStatus: 'open',
        },
      },
      'ACME',
    );
    expect(q).toMatchObject({
      displayPrice: 100, displayChange: 2, displayChangePct: 2.04,
      currency: 'USD', statementCurrency: 'USD', isIndex: false,
      isExtended: false, hasExtPrice: false,
    });
  });

  it('reads a provider-shaped quote, with the venue currency and the reported one apart', () => {
    const q = deriveOverviewQuote(
      { name: 'Acme Holdings', reportedCurrency: 'CNY', quote: { price: 436.6, change: -3.4, changePct: -0.77 } },
      'ACME.HK',
    );
    expect(q).toMatchObject({
      displayPrice: 436.6, displayChange: -3.4, displayChangePct: -0.77,
      // No `currency` on the payload: the suffix decides, never a default USD.
      currency: 'HKD', statementCurrency: 'CNY', isIndex: false,
    });
  });

  it('prints earnings in the trading currency, apart from the statements', () => {
    // An ADR's EPS is consensus per ADS, in dollars, though it reports in TWD.
    const q = deriveOverviewQuote({ name: 'Taiwan Semi', currency: 'USD', reportedCurrency: 'TWD' }, 'TSM');
    expect(q).toMatchObject({ statementCurrency: 'TWD', earningsCurrency: 'USD' });
  });

  it('prints an index level with no currency while its statements would still be money', () => {
    const family = deriveOverviewQuote(
      { name: 'S&P 500', currency: 'USD', assetClass: 'index', quote: { price: 5812.34, change: 12.3, changePct: 0.21 } },
      '^GSPC',
    );
    expect(family).toMatchObject({ isIndex: true, currency: null, statementCurrency: 'USD', displayPrice: 5812.34 });

    // A venue-listed index the payload does not label: the CN code range decides.
    const venue = deriveOverviewQuote({ currency: 'CNY', quote: { price: 4439.14 } }, '000300.SH');
    expect(venue).toMatchObject({ isIndex: true, currency: null, statementCurrency: 'CNY' });
  });

  it('splits an extended-hours print off the close only while it differs', () => {
    const late = deriveOverviewQuote(
      { quote: { regularClose: 100, regularChange: 1, lastTradePrice: 102.5, marketStatus: 'late_trading' } },
      'ACME',
    );
    expect(late).toMatchObject({
      marketStatus: 'late_trading', isExtended: true, hasExtPrice: true,
      extPrice: 102.5, extDiff: 2.5, extDiffPct: 2.5,
    });

    const flat = deriveOverviewQuote(
      { quote: { regularClose: 100, lastTradePrice: 100, marketStatus: 'early_trading' } },
      'ACME',
    );
    expect(flat).toMatchObject({ isExtended: true, hasExtPrice: false, extDiff: 0, extDiffPct: 0 });

    // In session the last trade is the price itself, not an extended move.
    const open = deriveOverviewQuote(
      { quote: { regularClose: 100, lastTradePrice: 101, marketStatus: 'open' } },
      'ACME',
    );
    expect(open).toMatchObject({ isExtended: false, hasExtPrice: false });
  });

  it('names a listing local-first and says when nothing names it', () => {
    expect(deriveOverviewQuote({ name: '甲公司', nameEn: 'Acme Holdings' }, 'ACME.HK').dualName).toEqual({
      primary: '甲公司', secondary: 'Acme Holdings', named: true,
    });
    // A name that only repeats the ticker names nothing, so the ticker prints once.
    expect(deriveOverviewQuote({ name: 'ACME' }, 'ACME').dualName).toEqual({
      primary: 'ACME', secondary: null, named: false,
    });
    expect(deriveOverviewQuote({}, 'ACME').dualName.named).toBe(false);
  });

  it('reads a payload with no quote as having no figures', () => {
    expect(deriveOverviewQuote({}, 'ACME')).toMatchObject({
      displayPrice: null, displayChange: null, displayChangePct: null,
      marketStatus: null, extPrice: null, hasExtPrice: false,
    });
  });
});
