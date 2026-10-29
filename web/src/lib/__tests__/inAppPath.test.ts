// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { inAppPath } from '../inAppPath';

describe('inAppPath', () => {
  it('keeps a path that stays inside the app', () => {
    expect(inAppPath('/chat/t/thread-1')).toBe('/chat/t/thread-1');
    expect(inAppPath('/chat/t/thread-1?panel=file#top')).toBe('/chat/t/thread-1?panel=file#top');
    expect(inAppPath('/account/plans')).toBe('/account/plans');
  });

  it.each([
    ['missing', null],
    ['empty', ''],
    ['protocol-relative', '//evil.example/x'],
    ['backslash host', '/\\evil.example'],
    ['double backslash', '\\\\evil.example'],
    ['absolute URL', 'https://evil.example/chat'],
    ['script scheme', 'javascript:alert(1)'],
    ['tab inside the slashes', '/\t/evil.example'],
    ['encoded slashes', '/%2F%2Fevil.example'],
    ['encoded backslash', '/%5Cevil.example'],
    ['dot segment before the slashes', '/..//evil.example'],
    ['encoded dot segment', '/%2e%2e//evil.example'],
    ['relative path', 'chat/t/thread-1'],
    ['malformed escape', '/chat/%E0%A4%A'],
  ])('drops a target that leaves the app: %s', (_label, value) => {
    expect(inAppPath(value)).toBeNull();
  });
});
