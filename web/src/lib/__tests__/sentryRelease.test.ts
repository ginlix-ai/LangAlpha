// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { releaseFromEntry } from '../sentry';

describe('releaseFromEntry: the release is the entry chunk the page loaded', () => {
  it('names a built entry after its content hash', () => {
    expect(releaseFromEntry('https://app.example.com/assets/index-Bjl2-chN.js')).toBe('web@index-Bjl2-chN');
  });

  it('ignores a query or fragment on the entry URL', () => {
    expect(releaseFromEntry('https://app.example.com/assets/index-Bjl2-chN.js?v=2')).toBe('web@index-Bjl2-chN');
  });

  it('names no release for the dev server, which serves source', () => {
    expect(releaseFromEntry('http://127.0.0.1:5173/src/main.tsx')).toBeUndefined();
    expect(releaseFromEntry('http://127.0.0.1:5173/src/main.tsx?t=123')).toBeUndefined();
  });
});
