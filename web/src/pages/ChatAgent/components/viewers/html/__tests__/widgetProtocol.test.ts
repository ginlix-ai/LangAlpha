// @vitest-environment node
import { describe, expect, it, vi } from 'vitest';

import { readWidgetMessage } from '../widgetProtocol';

// The frame runs agent-written code, so what it posts is rebuilt from typed
// fields or refused, once, for every host.
describe('readWidgetMessage', () => {
  it('takes a finite, positive height only', () => {
    expect(readWidgetMessage({ type: 'widget:resize', height: 12.5 })).toEqual({ type: 'widget:resize', height: 12.5 });
    for (const height of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '300']) {
      expect(readWidgetMessage({ type: 'widget:resize', height })).toBeNull();
    }
  });

  it('names the prompt call, trims its text and bounds its data in UTF-8 bytes', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    expect(readWidgetMessage({ type: 'widget:startThread', text: ' go ', data: [1] }))
      .toEqual({ type: 'widget:startThread', call: 'startThread', text: 'go', data: [1] });
    expect(readWidgetMessage({ type: 'widget:sendPrompt', text: '   ' })).toBeNull();
    expect(readWidgetMessage({ type: 'widget:sendPrompt', text: 'big', data: '数'.repeat(12_000) }))
      .toEqual({ type: 'widget:sendPrompt', call: 'sendPrompt', text: 'big', data: undefined });
    expect(warn).toHaveBeenCalledTimes(1);
    warn.mockRestore();
  });

  it('shapes storage writes', () => {
    expect(readWidgetMessage({ type: 'widget:storage', clear: true })).toEqual({ type: 'widget:storage', write: { clear: true } });
    expect(readWidgetMessage({ type: 'widget:storage', key: 'k', value: null }))
      .toEqual({ type: 'widget:storage', write: { key: 'k', value: null } });
    expect(readWidgetMessage({ type: 'widget:storage', key: 'k', value: 1 })).toBeNull();
  });

  it('zooms only into what the lightbox may load', () => {
    expect(readWidgetMessage({ type: 'widget:zoom', src: 'data:image/png;base64,AA', alt: 'x'.repeat(600) }))
      .toEqual({ type: 'widget:zoom', src: 'data:image/png;base64,AA', alt: 'x'.repeat(500) });
    expect(readWidgetMessage({ type: 'widget:zoom', src: 'javascript:alert(1)' })).toBeNull();
    expect(readWidgetMessage({ type: 'widget:zoom', src: `https://x.test/${'a'.repeat(8192)}` })).toBeNull();
  });

  // An inline image is its bytes: an ordinary chart runs well past a URL's length.
  it('zooms into an ordinary data:image, and not one past a few MB', () => {
    const image = (n: number) => `data:image/png;base64,${'A'.repeat(n)}`;
    expect(readWidgetMessage({ type: 'widget:zoom', src: image(300 * 1024) })).toEqual({ type: 'widget:zoom', src: image(300 * 1024) });
    expect(readWidgetMessage({ type: 'widget:zoom', src: image(9 * 1024 * 1024) })).toBeNull();
  });

  it('rebuilds a diagnostic from its known fields', () => {
    expect(readWidgetMessage({ type: 'widget:diag', kind: 'error', message: 'boom', line: 3, extra: 1 }))
      .toEqual({ type: 'widget:diag', diag: { kind: 'error', message: 'boom', line: 3 } });
    expect(readWidgetMessage({ type: 'widget:diag', kind: 'blocked', directive: 'x'.repeat(200), url: 'u' }))
      .toEqual({ type: 'widget:diag', diag: { kind: 'blocked', directive: 'x'.repeat(100), url: 'u' } });
    expect(readWidgetMessage({ type: 'widget:diag', kind: 'other' })).toBeNull();
  });

  it('refuses anything else', () => {
    for (const data of [null, 'widget:resize', { type: 'page:theme' }, { type: 1 }]) {
      expect(readWidgetMessage(data)).toBeNull();
    }
  });
});
