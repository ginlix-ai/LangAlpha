// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { buildMarketViewUrl, DEFAULT_MARKET_THREAD, MARKET_VIEW_ROUTE_PARAMS, readMarketViewRoute } from '../marketRoute';

const paramsOf = (url: string) => new URLSearchParams(url.slice(url.indexOf('?')));

describe('marketRoute', () => {
  it('round-trips every field through the URL', () => {
    const url = buildMarketViewUrl({
      symbol: 'AAPL',
      timeframe: '1hour',
      workspaceId: 'ws-7',
      threadId: 'thread-1',
      returnTo: '/chat/t/thread-1',
    });
    expect(url.startsWith('/market?')).toBe(true);
    expect(readMarketViewRoute(paramsOf(url))).toEqual({
      symbol: 'AAPL',
      timeframe: '1hour',
      workspaceId: 'ws-7',
      scope: 'workspace',
      threadId: 'thread-1',
      returnTo: '/chat/t/thread-1',
    });
  });

  it('selects the workspace scope only when a workspace is named', () => {
    const bare = readMarketViewRoute(paramsOf(buildMarketViewUrl({ symbol: 'AAPL' })));
    expect(bare).toEqual({ symbol: 'AAPL', timeframe: null, workspaceId: null, scope: null, threadId: null, returnTo: null });

    const withWs = paramsOf(buildMarketViewUrl({ symbol: 'AAPL', workspaceId: 'ws-7' }));
    expect(withWs.has('mode')).toBe(false);
    expect(readMarketViewRoute(withWs).scope).toBe('workspace');
  });

  it('omits the thread param for the default thread', () => {
    const url = buildMarketViewUrl({ symbol: 'AAPL', threadId: DEFAULT_MARKET_THREAD });
    expect(paramsOf(url).has('thread')).toBe(false);
    expect(readMarketViewRoute(paramsOf(url)).threadId).toBeNull();
  });

  it("reads an old link's mode as its scope, and an unknown one as none", () => {
    expect(readMarketViewRoute(new URLSearchParams('symbol=AAPL&mode=turbo')).scope).toBeNull();
    expect(readMarketViewRoute(new URLSearchParams('symbol=AAPL&mode=constructor')).scope).toBeNull();
    expect(readMarketViewRoute(new URLSearchParams('symbol=AAPL&mode=fast')).scope).toBe('all');
    expect(readMarketViewRoute(new URLSearchParams('symbol=AAPL&mode=ptc&ws=ws-7')).scope).toBe('workspace');
  });

  it('reads returnTo as a path inside the app', () => {
    const read = (value: string) => readMarketViewRoute(new URLSearchParams({ symbol: 'AAPL', returnTo: value })).returnTo;
    expect(read('/chat/t/thread-1?panel=file#top')).toBe('/chat/t/thread-1?panel=file#top');
    expect(read('/..//evil.example')).toBeNull();
  });

  it("lists the params the writer sets and an old link's mode, so a reader can clear exactly those", () => {
    const url = buildMarketViewUrl({ symbol: 'AAPL', timeframe: '1day', workspaceId: 'ws', threadId: 't', returnTo: '/x' });
    expect([...paramsOf(url).keys(), 'mode'].sort()).toEqual([...MARKET_VIEW_ROUTE_PARAMS].sort());
  });
});
