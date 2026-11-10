// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { parseAppMessage } from '../protocol';

describe('parseAppMessage', () => {
  it('reads a hello, dropping scopes the page does not know and repeats', () => {
    expect(parseAppMessage({ la: 1, type: 'hello', scopes: ['news', 'nonsense', 'news', 3], doc: 'd1' }))
      .toEqual({ type: 'hello', scopes: ['news'], doc: 'd1' });
  });

  it('reads scopes that are not a list as none', () => {
    expect(parseAppMessage({ la: 1, type: 'hello', scopes: 'news' })).toEqual({ type: 'hello', scopes: [], doc: null });
  });

  it('refuses a call with no id or no method', () => {
    expect(parseAppMessage({ la: 1, type: 'req', method: 'news.list' })).toBeNull();
    expect(parseAppMessage({ la: 1, type: 'sub', id: 's1' })).toBeNull();
    expect(parseAppMessage({ la: 1, type: 'unsub' })).toBeNull();
  });

  it('passes a method name through for the registry to own, prototype names included', () => {
    expect(parseAppMessage({ la: 1, type: 'req', id: 7, method: 'constructor', params: [1] }))
      .toEqual({ type: 'req', id: '7', method: 'constructor', params: {}, doc: null });
  });

  it('refuses another version, an unknown type, and what is not a message', () => {
    expect(parseAppMessage({ la: 2, type: 'bye' })).toBeNull();
    expect(parseAppMessage({ la: 1, type: 'shout' })).toBeNull();
    expect(parseAppMessage(null)).toBeNull();
    expect(parseAppMessage('hello')).toBeNull();
  });

  it('reads an open target as one of its three shapes, or not at all', () => {
    expect(parseAppMessage({ la: 1, type: 'open', target: { thread: 't1', workspace: 5 } }))
      .toEqual({ type: 'open', target: { thread: 't1' }, doc: null });
    expect(parseAppMessage({ la: 1, type: 'open', target: { symbol: 'AAPL' } }))
      .toEqual({ type: 'open', target: { symbol: 'AAPL' }, doc: null });
    expect(parseAppMessage({ la: 1, type: 'open', target: { symbol: 1 } })).toBeNull();
  });
});
