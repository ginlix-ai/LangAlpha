/**
 * The preview's receiver, run in a frame of its own: the markup posted so far
 * is drawn without running any of it, and a node already drawn keeps its
 * identity as the markup grows, so an image is never fetched twice. The real
 * browser run (scratchpad harness) covers what jsdom cannot: the policy, the
 * load events, the paint.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { STREAM_RECEIVER_SCRIPT } from '../buildHtmlSrcDoc';

let frame: HTMLIFrameElement;
let win: Window & typeof globalThis;

beforeEach(() => {
  frame = document.createElement('iframe');
  document.body.appendChild(frame);
  win = frame.contentWindow as Window & typeof globalThis;
  new win.Function(`${STREAM_RECEIVER_SCRIPT}();`)();
});

afterEach(() => frame.remove());

const draw = (html: string) =>
  win.dispatchEvent(new win.MessageEvent('message', { data: { type: 'widget:streamHtml', html }, source: window }));
const body = () => win.document.body;

describe('widget-stream receiver', () => {
  it('keeps every node it already drew as the markup grows', () => {
    draw('<style>.a{color:red}</style><div class="a"><img src="https://x.test/a.png"><p>Q');
    const img = body().querySelector('img')!;
    const style = body().querySelector('style')!;
    const p = body().querySelector('p')!;
    draw('<style>.a{color:red}</style><div class="a"><img src="https://x.test/a.png"><p>Q3 rev');
    draw('<style>.a{color:red}</style><div class="a"><img src="https://x.test/a.png"><p>Q3 revenue</p><table><tr><td>1');
    expect(body().querySelector('img')).toBe(img);
    expect(body().querySelector('style')).toBe(style);
    expect(body().querySelector('p')).toBe(p);
    expect(p.textContent).toBe('Q3 revenue');
    expect(body().querySelector('td')!.textContent).toBe('1');
  });

  it('patches a changed attribute in place and removes what the markup no longer has', () => {
    draw('<div id="a" class="x">one</div><span>two</span>');
    const div = body().querySelector('div')!;
    draw('<div id="a" class="y">one</div>');
    expect(body().querySelector('div')).toBe(div);
    expect(div.className).toBe('y');
    expect(body().querySelector('span')).toBeNull();
  });

  it('cuts a tag still being written', () => {
    draw('<p>done</p><img src="https://x.test/b.p');
    expect(body().innerHTML).toBe('<p>done</p>');
    draw('<p>a < b and more');
    expect(body().textContent).toBe('a < b and more');
  });

  it('draws no script, frame, handler or javascript: URL', () => {
    draw('<div onclick="evil()" onmouseover="evil()">x</div><a href=" javascript:evil()">l</a>'
      + '<iframe src="https://x.test"></iframe><meta http-equiv="refresh" content="0;url=https://x.test">'
      + '<svg><script>evil()</script><a href="https://ok.test">s</a></svg><script>evil()</script><script>half(');
    expect(body().querySelector('script, iframe, meta')).toBeNull();
    const div = body().querySelector('div')!;
    expect(div.getAttributeNames()).toEqual([]);
    expect(body().querySelector('a')!.hasAttribute('href')).toBe(false);
    expect(body().querySelector('svg a')!.getAttribute('href')).toBe('https://ok.test');
  });

  it('takes markup only from its parent', () => {
    win.dispatchEvent(new win.MessageEvent('message', { data: { type: 'widget:streamHtml', html: '<p>x</p>' }, source: win }));
    expect(body().innerHTML).toBe('');
  });
});
