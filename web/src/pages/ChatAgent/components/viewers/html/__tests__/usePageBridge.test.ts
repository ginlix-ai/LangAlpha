import { describe, it, expect, afterEach, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

import { usePageBridge } from '../usePageBridge';

const ORIGIN = 'http://pages.test';

function frame() {
  const iframe = document.createElement('iframe');
  document.body.appendChild(iframe);
  const win = iframe.contentWindow!;
  const sent = vi.spyOn(win, 'postMessage').mockImplementation(() => {});
  const hello = () => act(() => {
    window.dispatchEvent(new MessageEvent('message', { data: { type: 'page:ready' }, origin: ORIGIN, source: win }));
  });
  const posted = () => sent.mock.calls.map(([message]) => (message as { type: string }).type).filter((type) => type === 'page:pointer-watch');
  return { iframe, hello, posted };
}

describe('usePageBridge', () => {
  afterEach(() => {
    document.body.innerHTML = '';
    vi.restoreAllMocks();
  });

  // A frame whose document said hello took no more posts once anything ran
  // the bridge's effects again without reloading it: a hot update, or the
  // key coming back to the frame kept on screen. The comment layer then drew
  // the pins the page already knew and never told it about new ones.
  it('posts whenever it reads ready, including when the key comes back to a frame that said hello', () => {
    const { iframe, hello, posted } = frame();
    const frameRef = { current: iframe };
    const { result, rerender } = renderHook(
      ({ frameKey }: { frameKey: string | null }) => usePageBridge({ frameRef, origin: ORIGIN, frameKey }),
      { initialProps: { frameKey: 'live:a.html:1' } },
    );

    result.current.post({ type: 'page:pointer-watch' });
    expect(posted()).toHaveLength(0);

    hello();
    expect(result.current.ready).toBe(true);
    result.current.post({ type: 'page:pointer-watch' });
    expect(posted()).toHaveLength(1);

    // The next document has not said hello.
    rerender({ frameKey: 'live:a.html:2' });
    expect(result.current.ready).toBe(false);
    result.current.post({ type: 'page:pointer-watch' });
    expect(posted()).toHaveLength(1);

    rerender({ frameKey: 'live:a.html:1' });
    expect(result.current.ready).toBe(true);
    result.current.post({ type: 'page:pointer-watch' });
    expect(posted()).toHaveLength(2);
  });
});
