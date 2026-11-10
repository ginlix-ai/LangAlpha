// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { StockSearchHit } from '@/lib/marketUtils';
import { boostOwnTickers, namesListing } from '../ownTickers';

const hit = (symbol: string, name?: string, extra: Partial<StockSearchHit> = {}): StockSearchHit => ({ symbol, name, ...extra });
const symbols = (rows: { symbol: string }[]) => rows.map((row) => row.symbol);

describe('namesListing', () => {
  it('reads a symbol prefix, punctuation aside, or the start of a name word', () => {
    expect(namesListing('nv', 'NVDA', [])).toBe(true);
    expect(namesListing('brk.b', 'BRK-B', [])).toBe(true);
    expect(namesListing('brkb', 'BRK.B', [])).toBe(true);
    expect(namesListing('600519', '600519.SH', [])).toBe(true);
    expect(namesListing('nord', 'NVO', ['Novo Nordisk A/S'])).toBe(true);
    expect(namesListing('bank of', 'BAC', ['Bank of America'])).toBe(true);
    expect(namesListing('茅台', '600519.SH', ['贵州茅台'])).toBe(true);
  });

  it('does not read a word from inside another', () => {
    expect(namesListing('ordisk', 'NVO', ['Novo Nordisk A/S'])).toBe(false);
    expect(namesListing('vda', 'NVDA', ['NVIDIA'])).toBe(false);
    expect(namesListing('co', 'XYZ', ['my_co'])).toBe(false);
    expect(namesListing(' ', 'NVDA', ['NVIDIA'])).toBe(false);
  });
});

describe('boostOwnTickers', () => {
  const hits = [
    hit('NVS', 'Novartis', { match: 'symbol_prefix' }),
    hit('NVO', 'Novo Nordisk', { match: 'symbol_prefix' }),
    hit('NVDA', 'NVIDIA', { match: 'symbol_prefix' }),
    hit('XNV', 'Envoy Co', { match: 'other' }),
  ];

  it('puts the own tickers the query names first, in the search order', () => {
    const own = [hit('NVDA'), hit('NVO')];
    expect(symbols(boostOwnTickers(hits, 'nv', own))).toEqual(['NVO', 'NVDA', 'NVS', 'XNV']);
  });

  it('does not boost an own ticker the server matched on neither its symbol nor a name word', () => {
    expect(symbols(boostOwnTickers(hits, 'nv', [hit('XNV')]))).toEqual(['NVS', 'NVO', 'NVDA', 'XNV']);
  });

  it('adds an own ticker the search did not return, after the boosted hits', () => {
    const own = [hit('NVTS', 'Navitas'), hit('NVDA'), hit('AAPL', 'Apple')];
    expect(symbols(boostOwnTickers(hits, 'nv', own))).toEqual(['NVDA', 'NVTS', 'NVS', 'NVO', 'XNV']);
  });

  it('compares display spellings and takes the server match for a returned one', () => {
    const rows = [hit('BRK-B', undefined, { match: 'name_word' }), hit('BRKR', 'Bruker', { match: 'other' })];
    expect(symbols(boostOwnTickers(rows, 'berk', [hit('brk.b', 'Berkshire Hathaway')]))).toEqual(['BRK-B', 'BRKR']);
  });

  it('keeps the exact listing ahead of the own tickers, so a cap never drops it', () => {
    const rows = [hit('A', 'Agilent', { match: 'exact' }), ...['AMD', 'AAPL', 'ABNB'].map((s) => hit(s, s, { match: 'symbol_prefix' }))];
    const own = [hit('AAPL', 'Apple'), hit('ABNB', 'Airbnb'), hit('AMZN', 'Amazon')];
    expect(symbols(boostOwnTickers(rows, 'a', own)).slice(0, 3)).toEqual(['A', 'AAPL', 'ABNB']);
  });

  it('keeps the search order with no own tickers', () => {
    expect(boostOwnTickers(hits, 'nv', [])).toEqual(hits);
  });
});
