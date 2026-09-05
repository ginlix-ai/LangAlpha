import { describe, it, expect } from 'vitest';
import { webLink } from '../webLink';

/**
 * `homepage` and `repository` are free-form manifest strings that end up as
 * an anchor's `href`. `repository` shipped without the scheme check
 * `homepage` already had, which is the drift this helper exists to stop.
 */
describe('webLink', () => {
  it('keeps an ordinary web URL', () => {
    expect(webLink('https://github.com/acme/plugin')).toBe('https://github.com/acme/plugin');
    expect(webLink('http://example.test/docs')).toBe('http://example.test/docs');
    expect(webLink('HTTPS://Example.test')).toBe('HTTPS://Example.test');
  });

  it('refuses a scheme that runs instead of navigating', () => {
    expect(webLink('javascript:alert(1)')).toBeNull();
    expect(webLink('JavaScript:alert(1)')).toBeNull();
    expect(webLink('data:text/html,<script>alert(1)</script>')).toBeNull();
    expect(webLink('vbscript:msgbox(1)')).toBeNull();
  });

  it('refuses anything that is not an absolute web URL', () => {
    expect(webLink('github.com/acme/plugin')).toBeNull();
    expect(webLink('//evil.test')).toBeNull();
    expect(webLink('')).toBeNull();
    expect(webLink(null)).toBeNull();
    expect(webLink(undefined)).toBeNull();
  });
});
