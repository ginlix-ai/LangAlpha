import { describe, expect, it } from 'vitest';
import type { DesktopSettingsView, MenubarItem } from '@/lib/desktop';
import {
  DEFAULT_MENUBAR,
  DEFAULT_PANEL,
  PORTFOLIO_ITEM,
  choicesWith,
  hasItem,
  itemLabel,
  normalize,
  quoteRef,
  withItem,
} from '../desktopSettings';

const nvda: MenubarItem = { symbol: 'NVDA', kind: 'stock' };

describe('quoteRef', () => {
  it('reads a caret spelling as an index, asked for bare', () => {
    expect(quoteRef('^GSPC')).toEqual({ symbol: 'GSPC', kind: 'index' });
    expect(quoteRef('^n225')).toEqual({ symbol: 'N225', kind: 'index' });
  });

  it('reads the bare US index families as indices, as the shell does', () => {
    expect(quoteRef('vix')).toEqual({ symbol: 'VIX', kind: 'index' });
  });

  it('keeps anything else a stock in its spelling', () => {
    expect(quoteRef(' nvda ')).toEqual({ symbol: 'NVDA', kind: 'stock' });
    expect(quoteRef('600519.SH')).toEqual({ symbol: '600519.SH', kind: 'stock' });
  });

  it('takes the kind it is given', () => {
    expect(quoteRef('^gspc', 'index')).toEqual({ symbol: 'GSPC', kind: 'index' });
    expect(quoteRef(' brk.b ', 'stock')).toEqual({ symbol: 'BRK.B', kind: 'stock' });
    expect(quoteRef('VIX', 'stock')).toEqual({ symbol: 'VIX', kind: 'stock' });
  });
});

describe('withItem', () => {
  it('appends a new item', () => {
    expect(withItem<MenubarItem>([PORTFOLIO_ITEM], nvda, 8)).toEqual([PORTFOLIO_ITEM, nvda]);
  });

  it('holds each item once and stops at the cap', () => {
    expect(withItem<MenubarItem>([nvda], { symbol: 'NVDA', kind: 'stock' }, 8)).toEqual([nvda]);
    expect(withItem<MenubarItem>([nvda], PORTFOLIO_ITEM, 1)).toEqual([nvda]);
  });

  it('tells a stock from an index of the same name', () => {
    expect(withItem<MenubarItem>([{ symbol: 'VIX', kind: 'stock' }], { symbol: 'VIX', kind: 'index' }, 8)).toHaveLength(2);
    expect(hasItem([{ kind: 'index', symbol: 'VIX' }], { kind: 'stock', symbol: 'VIX' })).toBe(false);
  });

  it('knows whether the portfolio is there', () => {
    expect(hasItem([nvda], PORTFOLIO_ITEM)).toBe(false);
    expect(hasItem([nvda, PORTFOLIO_ITEM], PORTFOLIO_ITEM)).toBe(true);
  });
});

describe('itemLabel', () => {
  const t = (key: string) => `t:${key}`;

  it('names the portfolio and the known indices, and spells the rest', () => {
    expect(itemLabel(PORTFOLIO_ITEM, t)).toBe('t:settings.desktop.itemPortfolio');
    expect(itemLabel({ symbol: 'GSPC', kind: 'index' }, t)).toBe('t:marketIndex.GSPC.long');
    expect(itemLabel({ symbol: 'N225', kind: 'index' }, t)).toBe('^N225');
    expect(itemLabel(nvda, t)).toBe('NVDA');
  });
});

describe('choicesWith', () => {
  it('adds a value the shell holds that the choices leave out, in order', () => {
    expect(choicesWith([2, 4, 8], 4)).toEqual([2, 4, 8]);
    expect(choicesWith([2, 4, 8], 7)).toEqual([2, 4, 7, 8]);
  });
});

describe('normalize', () => {
  it('fills a group the shell left out with the defaults, and keeps what it sent', () => {
    const sent = { menubar: { ...DEFAULT_MENUBAR, items: [nvda] } } as unknown as DesktopSettingsView;
    const view = normalize(sent);
    expect(view.menubar.items).toEqual([nvda]);
    expect(view.panel).toEqual(DEFAULT_PANEL);
  });
});
