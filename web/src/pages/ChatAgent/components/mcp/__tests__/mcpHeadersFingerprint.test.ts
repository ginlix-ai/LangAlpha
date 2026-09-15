import { describe, it, expect } from 'vitest';
import { headersFingerprint } from '../mcpHeadersFingerprint';

const SECRET = 'sk-live-9f3c1e7a4b2d';

describe('headersFingerprint', () => {
  it('reads the same for the same headers whatever order they were filled in', () => {
    expect(headersFingerprint({ Authorization: `Bearer ${SECRET}`, 'X-Org': 'acme' })).toBe(
      headersFingerprint({ 'X-Org': 'acme', Authorization: `Bearer ${SECRET}` }),
    );
  });

  it('reads differently once a value or a name changes', () => {
    const base = headersFingerprint({ Authorization: `Bearer ${SECRET}` });
    expect(headersFingerprint({ Authorization: 'Bearer sk-live-other' })).not.toBe(base);
    expect(headersFingerprint({ 'X-Api-Key': SECRET })).not.toBe(base);
    // A row the user is still filling in is its own question, not the finished
    // one, which is what stops a half-typed key from reading a stale verdict.
    expect(headersFingerprint({ Authorization: 'Bearer sk-live-9f3c1e7a4b2' })).not.toBe(base);
    expect(headersFingerprint({})).not.toBe(base);
  });

  it('does not carry the credential it stands for', () => {
    const digest = headersFingerprint({ Authorization: `Bearer ${SECRET}` });
    expect(digest).not.toContain(SECRET);
    expect(digest).not.toContain('Authorization');
    expect(digest).toMatch(/^[0-9a-f]{16}$/);
  });
});
