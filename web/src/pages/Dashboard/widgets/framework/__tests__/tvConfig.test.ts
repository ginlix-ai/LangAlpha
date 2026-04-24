import { describe, it, expect } from 'vitest';
import { resolveScriptSrc } from '../tvConfig';

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
