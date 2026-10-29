// @vitest-environment node
import { describe, it, expect } from 'vitest';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import {
  CALLBACK_ERROR_REASONS,
  CALLBACK_PARAMS,
  DETAIL_MAX,
  readConnectOutcome,
} from '../connectOutcome';

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
      plugin: null,
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
        plugin: null,
        reasonKey: 'plugins.oauth.callbackError.unknown',
        detail: null,
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
        plugin: null,
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

  it('keeps a name saved before the sandbox reserved it', () => {
    // Such a row still connects, and a return that loses its name leaves the
    // pending marker behind.
    for (const legacy of ['class', 'mcp_client', '__init__']) {
      expect(readConnectOutcome(params(`mcp_connected=${legacy}`))).toEqual({
        kind: 'connected',
        server: legacy,
        plugin: null,
      });
    }
  });

  it('names the plugin a sign-in installed', () => {
    expect(readConnectOutcome(params('mcp_connected=alpha&plugin=market-tools'))).toEqual({
      kind: 'connected',
      server: 'alpha',
      plugin: 'market-tools',
    });
    expect(
      readConnectOutcome(params('mcp_error=install_failed&server=alpha&plugin=market.tools_2')),
    ).toMatchObject({ kind: 'failed', server: 'alpha', plugin: 'market.tools_2' });
  });

  it('drops a plugin name that could not be one', () => {
    // Printed beside the toast's title like the server name, so it gets the
    // same closed shape rather than whatever the link carried.
    for (const bad of [
      'Visit evil.example now',
      '-leading-dash',
      '.dotfirst',
      'has/slash',
      'a'.repeat(65),
      '',
    ]) {
      expect(
        readConnectOutcome(params(`mcp_connected=alpha&plugin=${encodeURIComponent(bad)}`)),
      ).toMatchObject({ kind: 'connected', plugin: null });
      expect(
        readConnectOutcome(params(`mcp_error=denied&plugin=${encodeURIComponent(bad)}`)),
      ).toMatchObject({ kind: 'failed', plugin: null });
    }
    expect(
      readConnectOutcome(params(`mcp_connected=alpha&plugin=${'a'.repeat(64)}`)),
    ).toMatchObject({ plugin: 'a'.repeat(64) });
  });

  it('carries a detail only for an install that failed', () => {
    expect(
      readConnectOutcome(params('mcp_error=install_failed&mcp_detail=Key%20rejected%20by%20vendor')),
    ).toMatchObject({ kind: 'failed', detail: 'Key rejected by vendor' });
    // Every other reason has its own sentence; a detail beside it was written
    // by someone other than the callback.
    for (const reason of ['denied', 'token_exchange_failed', 'brand_new_reason']) {
      expect(
        readConnectOutcome(params(`mcp_error=${reason}&mcp_detail=Call%20us%20at%20555`)),
      ).toMatchObject({ kind: 'failed', detail: null });
    }
    expect(readConnectOutcome(params('mcp_error=install_failed'))).toMatchObject({
      detail: null,
    });
  });

  it('keeps a detail as the plain text it arrived as', () => {
    const outcome = readConnectOutcome(
      params(`mcp_error=install_failed&mcp_detail=${encodeURIComponent('<b>x</b> & <i>y</i>')}`),
    );
    expect(outcome).toMatchObject({ detail: '<b>x</b> & <i>y</i>' });
  });

  it('strips control characters and folds whitespace in a detail', () => {
    const raw = 'line one\nline two\r\n\ttabbed\u0000nul\u007fdel   spaced';
    const outcome = readConnectOutcome(
      params(`mcp_error=install_failed&mcp_detail=${encodeURIComponent(raw)}`),
    );
    expect(outcome).toMatchObject({
      detail: 'line one line two tabbed nul del spaced',
    });
    // Nothing but control characters is no detail at all.
    expect(
      readConnectOutcome(
        params(`mcp_error=install_failed&mcp_detail=${encodeURIComponent('\u0000\n\t ')}`),
      ),
    ).toMatchObject({ detail: null });
  });

  it('caps a detail at what the server ever sends, with an ellipsis', () => {
    const exact = 'x'.repeat(DETAIL_MAX);
    expect(
      readConnectOutcome(params(`mcp_error=install_failed&mcp_detail=${exact}`)),
    ).toMatchObject({ detail: exact });

    const long = 'y'.repeat(DETAIL_MAX + 50);
    const detail = (
      readConnectOutcome(params(`mcp_error=install_failed&mcp_detail=${long}`)) as {
        detail: string;
      }
    ).detail;
    expect(DETAIL_MAX).toBe(200);
    expect(detail).toHaveLength(DETAIL_MAX);
    expect(detail.endsWith('\u2026')).toBe(true);
    expect(detail.slice(0, -1)).toBe('y'.repeat(DETAIL_MAX - 1));
  });

  it('lists every param the callback can add, so a landing strips them all', () => {
    expect([...CALLBACK_PARAMS].sort()).toEqual(
      ['mcp_connected', 'mcp_detail', 'mcp_error', 'plugin', 'server'].sort(),
    );
  });
});
