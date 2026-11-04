import { describe, it, expect } from 'vitest';
import { transformCitationBubbles, escapeHtmlAttr } from '../../utils/markdownSource';

describe('escapeHtmlAttr', () => {
  it('escapes ampersands', () => {
    expect(escapeHtmlAttr('a&b')).toBe('a&amp;b');
  });

  it('escapes double quotes', () => {
    expect(escapeHtmlAttr('a"b')).toBe('a&quot;b');
  });

  it('escapes angle brackets', () => {
    expect(escapeHtmlAttr('a<b')).toBe('a&lt;b');
  });

  it('escapes all special chars together', () => {
    expect(escapeHtmlAttr('x&y"z<w')).toBe('x&amp;y&quot;z&lt;w');
  });

  it('passes through clean strings', () => {
    expect(escapeHtmlAttr('hello world')).toBe('hello world');
  });
});

describe('transformCitationBubbles', () => {
  it('transforms a single citation', () => {
    const input = 'Oil prices surged ([nypost.com](https://nypost.com/2026/03/27/business))';
    const result = transformCitationBubbles(input);
    expect(result).toBe(
      'Oil prices surged <cite-bubble label="nypost.com" href="https://nypost.com/2026/03/27/business"></cite-bubble>'
    );
  });

  it('transforms multiple citations in the same text', () => {
    const input = 'Confirmed ([reuters.com](https://reuters.com/article)) ([bloomberg.com](https://bloomberg.com/news))';
    const result = transformCitationBubbles(input);
    expect(result).toContain('<cite-bubble label="reuters.com"');
    expect(result).toContain('<cite-bubble label="bloomberg.com"');
  });

  it('does not transform standard markdown links without outer parens', () => {
    const input = 'See [example](https://example.com) for more';
    const result = transformCitationBubbles(input);
    expect(result).toBe(input);
  });

  it('returns empty/falsy input unchanged', () => {
    expect(transformCitationBubbles('')).toBe('');
    expect(transformCitationBubbles(null as unknown as string)).toBe(null);
    expect(transformCitationBubbles(undefined as unknown as string)).toBe(undefined);
  });

  it('escapes special characters in label and URL', () => {
    const input = '([a&"b](https://example.com/q?a=1&b=2))';
    const result = transformCitationBubbles(input);
    expect(result).toContain('label="a&amp;&quot;b"');
    expect(result).toContain('href="https://example.com/q?a=1&amp;b=2"');
  });

  it('only matches http/https URLs', () => {
    const input = '([evil](javascript:alert(1)))';
    const result = transformCitationBubbles(input);
    expect(result).toBe(input); // no transform
  });

  it('preserves surrounding text', () => {
    const input = 'Before ([a.com](https://a.com/path)) after text.';
    const result = transformCitationBubbles(input);
    expect(result).toMatch(/^Before /);
    expect(result).toMatch(/ after text\.$/);
  });

  it('handles citation at start of text', () => {
    const input = '([a.com](https://a.com)) is the source.';
    const result = transformCitationBubbles(input);
    expect(result).toMatch(/^<cite-bubble/);
  });

  it('handles citation at end of text', () => {
    const input = 'Source: ([a.com](https://a.com))';
    const result = transformCitationBubbles(input);
    expect(result).toMatch(/cite-bubble>$/);
  });

  it('encodes $ as %24 in URLs to prevent escapeCurrencyDollars interference', () => {
    const input = '([example.com](https://example.com/q?price=$100&other=1))';
    const result = transformCitationBubbles(input);
    expect(result).toContain('href="https://example.com/q?price=%24100&amp;other=1"');
    expect(result).not.toContain('$100');
  });
});
