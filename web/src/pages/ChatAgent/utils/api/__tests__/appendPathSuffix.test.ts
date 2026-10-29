import { describe, it, expect } from 'vitest';
import { appendPathSuffix } from '../sandbox';

const BASE = 'https://8080-sandbox.example.com/preview?sig=abc';

describe('appendPathSuffix', () => {
  it('keeps a query and fragment as query and fragment, beside the signature', () => {
    expect(appendPathSuffix(BASE, '/timeline.html?tab=2#top'))
      .toBe('https://8080-sandbox.example.com/preview/timeline.html?sig=abc&tab=2#top');
  });

  it('roots a suffix written without a leading slash', () => {
    expect(appendPathSuffix(BASE, 'timeline.html')).toBe('https://8080-sandbox.example.com/preview/timeline.html?sig=abc');
  });

  it('leaves the URL alone with nothing to append, or when it is not a URL', () => {
    expect(appendPathSuffix(BASE, undefined)).toBe(BASE);
    expect(appendPathSuffix(BASE, '')).toBe(BASE);
    expect(appendPathSuffix('not a url', '/x.html')).toBe('not a url');
  });
});
