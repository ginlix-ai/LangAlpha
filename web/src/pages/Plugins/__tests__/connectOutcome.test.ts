import { describe, it, expect } from 'vitest';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import { CALLBACK_ERROR_REASONS, readConnectOutcome } from '../connectOutcome';

/**
 * The callback's outcome params, which arrive in the address bar and so are
 * whatever the person who wrote the link wanted them to be.
 */

function params(query: string) {
  return new URLSearchParams(query);
}
function sentence(catalog: object, key: string): unknown {
  return key.split('.').reduce<unknown>(
    (acc, part) =>
      acc && typeof acc === 'object' && part in acc
        ? (acc as Record<string, unknown>)[part]
        : undefined,
    catalog,
  );
}

describe('readConnectOutcome', () => {
  it('is silent on a landing that is not a callback', () => {
    expect(readConnectOutcome(params('tab=brokerages'))).toBeNull();
  });

  it('reports a success under the name of the server it names', () => {
    expect(readConnectOutcome(params('mcp_connected=robinhood'))).toEqual({
      kind: 'connected',
      server: 'robinhood',
    });
  });

  it('translates every reason the backend actually sends', () => {
    for (const reason of CALLBACK_ERROR_REASONS) {
      const outcome = readConnectOutcome(params(`mcp_error=${reason}`));
      expect(outcome).toMatchObject({ kind: 'failed', server: null });
      const key = (outcome as { reasonKey: string }).reasonKey;
      expect(key).toBe(`plugins.oauth.callbackError.${reason}`);
      // Both catalogs, because the key is built by template and the tree-wide
      // sweep that would otherwise catch this cannot read one.
      expect(typeof sentence(enUS, key)).toBe('string');
      expect(typeof sentence(zhCN, key)).toBe('string');
    }
  });

  it('has a sentence for a reason it has never heard of', () => {
    // A backend one version ahead is the ordinary case; a hand-written URL is
    // the other, and both land here rather than in the toast.
    for (const reason of ['brand_new_reason', 'Your session expired, sign in at evil.example']) {
      expect(readConnectOutcome(params(`mcp_error=${encodeURIComponent(reason)}`))).toEqual({
        kind: 'failed',
        server: null,
        reasonKey: 'plugins.oauth.callbackError.unknown',
      });
    }
    expect(typeof sentence(enUS, 'plugins.oauth.callbackError.unknown')).toBe('string');
    expect(typeof sentence(zhCN, 'plugins.oauth.callbackError.unknown')).toBe('string');
  });

  it('drops a server name that could not be one', () => {
    // Catalog names are `NAME_RE`. Anything else was not written by the
    // callback, and it is about to be printed above this app's own title.
    for (const bad of ['Contact support at evil.example', '9lives', 'has space', '']) {
      expect(readConnectOutcome(params(`mcp_connected=${encodeURIComponent(bad)}`))).toEqual({
        kind: 'connected',
        server: null,
      });
      expect(
        readConnectOutcome(params(`mcp_error=denied&server=${encodeURIComponent(bad)}`)),
      ).toMatchObject({ server: null });
    }
  });

  it('keeps a server name that is one', () => {
    expect(
      readConnectOutcome(params('mcp_error=denied&server=ibkr')),
    ).toMatchObject({ kind: 'failed', server: 'ibkr' });
  });
});
