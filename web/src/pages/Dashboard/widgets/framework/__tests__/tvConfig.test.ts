import { describe, it, expect, beforeEach } from 'vitest';
import i18n from '@/i18n';
import { resolveScriptSrc, mapLocaleForTV, getTVCommonConfig } from '../tvConfig';

describe('resolveScriptSrc', () => {
  it('passes full https URLs through unchanged', () => {
    expect(resolveScriptSrc('https://example.com/foo.js')).toBe('https://example.com/foo.js');
  });

  it('passes full http URLs through unchanged', () => {
    expect(resolveScriptSrc('http://example.com/foo.js')).toBe('http://example.com/foo.js');
  });

  it('resolves known short keys against the TV CDN base', () => {
    expect(resolveScriptSrc('ticker-tape')).toBe(
      'https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js',
    );
    expect(resolveScriptSrc('symbol-info')).toBe(
      'https://s3.tradingview.com/external-embedding/embed-widget-symbol-info.js',
    );
    expect(resolveScriptSrc('etf-heatmap')).toBe(
      'https://s3.tradingview.com/external-embedding/embed-widget-etf-heatmap.js',
    );
  });

  it('throws for unknown short keys so registration typos surface at mount', () => {
    expect(() => resolveScriptSrc('not-a-real-key')).toThrow(/unknown script key/i);
  });
});

describe('mapLocaleForTV', () => {
  it('maps known BCP-47 locales to TV underscored variants', () => {
    expect(mapLocaleForTV('en-US')).toBe('en');
    expect(mapLocaleForTV('zh-CN')).toBe('zh_CN');
    expect(mapLocaleForTV('zh-TW')).toBe('zh_TW');
    expect(mapLocaleForTV('ja-JP')).toBe('ja');
    expect(mapLocaleForTV('pt-BR')).toBe('pt');
  });

  it('falls back to language prefix for unknown regions', () => {
    expect(mapLocaleForTV('en-AU')).toBe('en');
    expect(mapLocaleForTV('zh-HK')).toBe('zh_CN');
  });

  it('falls back to en for fully unknown locales', () => {
    expect(mapLocaleForTV('xx-YY')).toBe('en');
    expect(mapLocaleForTV('')).toBe('en');
  });
});

describe('getTVCommonConfig', () => {
  beforeEach(() => {
    i18n.changeLanguage('en-US');
  });

  it('reads the active locale at call time', () => {
    expect(getTVCommonConfig().locale).toBe('en');
    i18n.changeLanguage('zh-CN');
    expect(getTVCommonConfig().locale).toBe('zh_CN');
  });

  it('always emits transparent backgrounds and full size hints', () => {
    const cfg = getTVCommonConfig();
    expect(cfg.autosize).toBe(true);
    expect(cfg.isTransparent).toBe(true);
    expect(cfg.backgroundColor).toBe('rgba(0,0,0,0)');
    expect(cfg.width).toBe('100%');
    expect(cfg.height).toBe('100%');
  });

  it('routes "view full chart" to our /market route', () => {
    const cfg = getTVCommonConfig();
    expect(String(cfg.largeChartUrl)).toMatch(/\/market$/);
  });
});
