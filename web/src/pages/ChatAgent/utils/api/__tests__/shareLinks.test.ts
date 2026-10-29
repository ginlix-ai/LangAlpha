import { describe, expect, it } from 'vitest';
import { parseShareLinkHref, shareLinkHref } from '../shareLinks';

describe('parseShareLinkHref', () => {
  it('reads back what shareLinkHref writes', () => {
    expect(parseShareLinkHref(shareLinkHref('TnVftlXfU0nN', 'index.html'))).toEqual({ code: 'TnVftlXfU0nN', path: 'index.html' });
    expect(parseShareLinkHref(shareLinkHref('TnVftlXfU0nN'))).toEqual({ code: 'TnVftlXfU0nN', path: null });
  });

  // The agent's reply names the app's root as `?path=/`, which must keep
  // opening the root after the link's entry moves.
  it('keeps an explicit root apart from no page', () => {
    expect(parseShareLinkHref(`${window.location.origin}/a/TnVftlXfU0nN?path=/`)).toEqual({ code: 'TnVftlXfU0nN', path: '' });
    expect(parseShareLinkHref(shareLinkHref('TnVftlXfU0nN', '/'))).toEqual({ code: 'TnVftlXfU0nN', path: '' });
  });

  it('takes only an /a/ link on this origin', () => {
    expect(parseShareLinkHref('https://elsewhere.example.com/a/TnVftlXfU0nN')).toBeNull();
    expect(parseShareLinkHref(`${window.location.origin}/s/TnVftlXfU0nN`)).toBeNull();
    expect(parseShareLinkHref(`${window.location.origin}/a/TnVftlXfU0nN/extra`)).toBeNull();
    expect(parseShareLinkHref(undefined)).toBeNull();
    expect(parseShareLinkHref(`${window.location.origin}/a/TnVftlXfU0nN?as=visitor`)).toBeNull();
  });
});
