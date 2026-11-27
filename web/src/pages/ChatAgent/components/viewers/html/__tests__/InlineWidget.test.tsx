import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';

import InlineWidget, { resetInlineWidgetHeightCache } from '../../InlineWidget';
import { WIDGET_SANDBOX, SERVED_HTML_PERMISSIONS } from '../sandbox';

// A prompt call or zoom is answered only after the user acted in the frame,
// which jsdom cannot produce; each test that posts one acts first.
const acted = vi.hoisted(() => ({ current: true }));
vi.mock('../frameActivation', () => ({
  userActedInFrame: () => acted.current,
  retryAfterRefusal: () => null,
  watchFrameActivation: () => {},
}));

// The height cache outlives unmounts by design — isolate every test from it.
beforeEach(() => {
  resetInlineWidgetHeightCache();
  acted.current = true;
  localStorage.clear();
});

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: vi.fn(),
}));

/** Dispatch a postMessage as if it came from the widget iframe's contentWindow. */
function postFromIframe(iframe: HTMLIFrameElement, data: unknown) {
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data, source: iframe.contentWindow }));
  });
}

describe('InlineWidget — sandbox bridge regressions', () => {
  beforeEach(() => {
    vi.stubGlobal('open', vi.fn(() => ({ print: vi.fn(), addEventListener: vi.fn() })));
    vi.stubGlobal('URL', { ...URL, createObjectURL: vi.fn(() => 'blob:x'), revokeObjectURL: vi.fn() });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('grows the iframe height on widget:resize', () => {
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    // Before any resize, height is the 150px placeholder and opacity 0,
    // with the loading glyph shown in the reserved box.
    expect(iframe.style.height).toBe('150px');
    expect(container.querySelector('.inline-widget-loading')).toBeTruthy();
    postFromIframe(iframe, { type: 'widget:resize', height: 420 });
    expect(iframe.style.height).toBe('420px');
    expect(iframe.style.opacity).toBe('1');
    expect(container.querySelector('.inline-widget-loading')).toBeNull();
  });

  it('reserves the last known height when the same widget remounts', () => {
    const first = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe = first.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:resize', height: 420 });
    first.unmount();

    // Re-open (e.g. thread revisit): box is reserved at the cached height and
    // visible immediately, but the loading glyph stays until the new document
    // actually reports.
    const second = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe2 = second.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe2.style.height).toBe('420px');
    expect(iframe2.style.opacity).toBe('1');
    expect(second.container.querySelector('.inline-widget-loading')).toBeTruthy();
    postFromIframe(iframe2, { type: 'widget:resize', height: 430 });
    expect(iframe2.style.height).toBe('430px');
    expect(second.container.querySelector('.inline-widget-loading')).toBeNull();
  });

  it('does not share cached heights across different widget content', () => {
    const first = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe = first.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:resize', height: 420 });
    first.unmount();

    const other = render(<InlineWidget base={null} html="<div>other content</div>" />);
    const iframe2 = other.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe2.style.height).toBe('150px');
    expect(iframe2.style.opacity).toBe('0');
  });

  it('calls onSendPrompt on widget:sendPrompt', () => {
    const onSendPrompt = vi.fn();
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt }} />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:sendPrompt', text: '  fix it  ' });
    // A call with no data still names its widget: the content key stands in for the id.
    expect(onSendPrompt).toHaveBeenCalledWith('fix it', {
      data: undefined, widgetId: expect.stringMatching(/^content:/), title: undefined,
    });
  });

  it('answers a prompt call only after the user acted in the frame', () => {
    acted.current = false;
    const onSendPrompt = vi.fn();
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt }} />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:sendPrompt', text: 'fix it' });
    expect(onSendPrompt).not.toHaveBeenCalled();
  });

  it('ignores messages from a foreign source', () => {
    const onSendPrompt = vi.fn();
    render(<InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt }} />);
    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', { data: { type: 'widget:sendPrompt', text: 'x' }, source: window }),
      );
    });
    expect(onSendPrompt).not.toHaveBeenCalled();
  });
});

describe('InlineWidget — hover action bar + fullscreen', () => {
  beforeEach(() => {
    vi.stubGlobal('open', vi.fn(() => ({ print: vi.fn(), addEventListener: vi.fn() })));
    vi.stubGlobal('URL', { ...URL, createObjectURL: vi.fn(() => 'blob:x'), revokeObjectURL: vi.fn() });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders the overlay action bar (fullscreen, open, more menu)', () => {
    render(<InlineWidget base={null} html="<div>hi</div>" title="My widget" />);
    expect(screen.getByLabelText('filePanel:fullscreen')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel:openInNewTab')).toBeInTheDocument();
    // Download/PDF are consolidated into the secondary "more" menu.
    expect(screen.getByLabelText('filePanel:moreActions')).toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.copySource')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel:downloadAsHtml')).not.toBeInTheDocument();
  });

  it('opens the fullscreen modal with a widget-fullscreen srcDoc iframe', () => {
    render(<InlineWidget base={null} html="<div>hi</div>" title="My widget" />);
    fireEvent.click(screen.getByLabelText('filePanel:fullscreen'));
    const frame = document.querySelector('iframe.html-fullscreen-frame') as HTMLIFrameElement;
    expect(frame).toBeTruthy();
    // Fullscreen variant uses a srcDoc (not a served src), scrollable body.
    expect(frame.getAttribute('src')).toBeNull();
    expect(frame.getAttribute('srcdoc')).toContain('overflow: auto; height: 100%;');
  });

  // The srcDoc bakes the theme in at build time and is built once, when the
  // widget mounts inline; the dialog makes a fresh document from that same
  // string on every open. A theme the user changed in between reaches the
  // inline frame through useHtmlSandbox's observer, but this frame did not
  // exist to be pushed to, so without a push on load it opens wearing the
  // theme the thread was first rendered in.
  it('pushes the live theme into the fullscreen document when it loads', () => {
    render(<InlineWidget base={null} html="<div>hi</div>" title="My widget" />);
    fireEvent.click(screen.getByLabelText('filePanel:fullscreen'));
    const frame = document.querySelector('iframe.html-fullscreen-frame') as HTMLIFrameElement;
    const postMessage = vi.fn();
    Object.defineProperty(frame, 'contentWindow', { value: { postMessage }, configurable: true });
    fireEvent.load(frame);
    // '*' and not a real origin: the frame is sandboxed without
    // allow-same-origin, so it has no origin that can be named.
    expect(postMessage).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'widget:themeUpdate' }),
      '*',
    );
  });

  it('offers no actions until its base is decided', () => {
    render(<InlineWidget base={undefined} html="<div>hi</div>" />);
    expect(screen.queryByLabelText('filePanel:fullscreen')).not.toBeInTheDocument();
  });

  it('closes the dialog on a prompt call from it, then answers the call', () => {
    const onDraftPrompt = vi.fn();
    render(<InlineWidget base={null} html="<div>hi</div>" title="Picker" widgetId="widget-c9" prompts={{ draftPrompt: onDraftPrompt }} />);
    fireEvent.click(screen.getByLabelText('filePanel:fullscreen'));
    const frame = document.querySelector('iframe.html-fullscreen-frame') as HTMLIFrameElement;
    postFromIframe(frame, { type: 'widget:draftPrompt', text: 'note', data: { a: 1 } });
    expect(onDraftPrompt).toHaveBeenCalledWith('note', { data: { a: 1 }, widgetId: 'widget-c9', title: 'Picker' });
    expect(document.querySelector('iframe.html-fullscreen-frame')).toBeNull();
  });

  it('opens a blob tab when open-in-new-tab is clicked', () => {
    render(<InlineWidget base={null} html="<div>hi</div>" />);
    fireEvent.click(screen.getByLabelText('filePanel:openInNewTab'));
    expect(window.open).toHaveBeenCalledWith('blob:x', '_blank', 'noopener,noreferrer');
  });
});

describe('InlineWidget — frame policy', () => {
  it('frames the widget as a served report is framed', () => {
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe.getAttribute('sandbox')).toBe(WIDGET_SANDBOX);
    expect(iframe.getAttribute('sandbox')).not.toContain('allow-same-origin');
    expect(iframe.getAttribute('sandbox')).not.toContain('allow-top-navigation');
    expect(iframe.getAttribute('allow')).toBe(SERVED_HTML_PERMISSIONS);
  });
});

describe('InlineWidget — bridge additions', () => {
  it('passes sendPrompt data on with the widget it came from', () => {
    const onSendPrompt = vi.fn();
    const { container } = render(
      <InlineWidget base={null} html="<div>hi</div>" title="Picker" widgetId="call_1" prompts={{ sendPrompt: onSendPrompt }} />,
    );
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:sendPrompt', text: 'Compare these', data: { tickers: ['NVDA', 'AMD'] } });
    expect(onSendPrompt).toHaveBeenCalledWith('Compare these', {
      data: { tickers: ['NVDA', 'AMD'] },
      widgetId: 'call_1',
      title: 'Picker',
    });
  });

  it('drops sendPrompt data over the cap and still sends the text', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const onSendPrompt = vi.fn();
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt }} />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:sendPrompt', text: 'go', data: { blob: 'x'.repeat(40_000) } });
    expect(onSendPrompt).toHaveBeenCalledWith('go', expect.objectContaining({ data: undefined }));
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });

  it('routes draftPrompt and startThread to their own handlers, with the widget their data came from', () => {
    const onSendPrompt = vi.fn();
    const onDraftPrompt = vi.fn();
    const onStartThread = vi.fn();
    const { container } = render(
      <InlineWidget
        base={null} html="<div>hi</div>" title="Picker" widgetId="call_3"
        prompts={{ sendPrompt: onSendPrompt, draftPrompt: onDraftPrompt, startThread: onStartThread }}
      />,
    );
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:draftPrompt', text: '  Add a note ', data: { scenario: 'bull' } });
    postFromIframe(iframe, { type: 'widget:startThread', text: 'Explore the bear case' });
    expect(onDraftPrompt).toHaveBeenCalledWith('Add a note', { data: { scenario: 'bull' }, widgetId: 'call_3', title: 'Picker' });
    expect(onStartThread).toHaveBeenCalledWith('Explore the bear case', { data: undefined, widgetId: 'call_3', title: 'Picker' });
    expect(onSendPrompt).not.toHaveBeenCalled();
  });

  it('drops a prompt call with no text, data over the cap, and one the host does not answer', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const onSendPrompt = vi.fn();
    const onStartThread = vi.fn();
    const { container } = render(
      <InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt, startThread: onStartThread }} />,
    );
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:startThread', text: '   ', data: { a: 1 } });
    postFromIframe(iframe, { type: 'widget:startThread', text: 7 });
    postFromIframe(iframe, { type: 'widget:startThread', text: 'go', data: { blob: 'x'.repeat(40_000) } });
    // No draft handler here: the call does nothing, and nothing else answers it.
    postFromIframe(iframe, { type: 'widget:draftPrompt', text: 'note' });
    expect(onStartThread.mock.calls).toEqual([['go', expect.objectContaining({ data: undefined })]]);
    expect(warn).toHaveBeenCalledWith('[widget] startThread data dropped: not JSON or over 32 KB serialized');
    expect(onSendPrompt).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it('keeps localStorage writes and seeds the next render with them', () => {
    const first = render(<InlineWidget base={null} html="<div>hi</div>" widgetId="call_2" />);
    const iframe = first.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:storage', key: 'pick', value: 'NVDA' });
    postFromIframe(iframe, { type: 'widget:storage', key: 'gone', value: 'x' });
    postFromIframe(iframe, { type: 'widget:storage', key: 'gone', value: null });
    // The frame that wrote it keeps its document: no reload per write.
    expect(iframe.getAttribute('srcdoc')).toContain('})({},');
    first.unmount();

    const second = render(<InlineWidget base={null} html="<div>hi</div>" widgetId="call_2" />);
    const iframe2 = second.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe2.getAttribute('srcdoc')).toContain('})({"pick":"NVDA"},');
  });

  // Each frame holds its own copy of the items: unless the other's writes reach
  // it, its next read-modify-write puts back what the other just changed.
  it('hands each frame the writes the other makes, once its document has loaded', () => {
    render(<InlineWidget base={null} html="<div>hi</div>" widgetId="call_sync" />);
    const inline = document.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    fireEvent.load(inline);
    const toInline = vi.spyOn(inline.contentWindow!, 'postMessage');
    fireEvent.click(screen.getByLabelText('filePanel:fullscreen'));
    const full = document.querySelector('iframe.html-fullscreen-frame') as HTMLIFrameElement;
    const toFull = vi.spyOn(full.contentWindow!, 'postMessage');
    const sync = (key: string, value: string) => ({ type: 'widget:storageSync', write: { key, value } });

    postFromIframe(inline, { type: 'widget:storage', key: 'early', value: '1' });
    expect(toFull).not.toHaveBeenCalledWith(sync('early', '1'), '*');
    fireEvent.load(full);
    expect(toFull).toHaveBeenCalledWith(sync('early', '1'), '*');

    postFromIframe(full, { type: 'widget:storage', key: 'pick', value: 'AMD' });
    expect(toInline).toHaveBeenCalledWith(sync('pick', 'AMD'), '*');
    expect(toFull).not.toHaveBeenCalledWith(sync('pick', 'AMD'), '*');
  });

  it('opens the lightbox on a zoom request, and only for an image URL', () => {
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:zoom', src: 'javascript:alert(1)' });
    expect(document.querySelector('img.max-w-\\[90vw\\]')).toBeNull();
    postFromIframe(iframe, { type: 'widget:zoom', src: 'https://example.com/a.png', alt: 'A' });
    const img = screen.getByAltText('A');
    expect(img.getAttribute('src')).toBe('https://example.com/a.png');
  });

  it('ignores diagnostics and inspect answers', () => {
    const onSendPrompt = vi.fn();
    const { container } = render(<InlineWidget base={null} html="<div>hi</div>" prompts={{ sendPrompt: onSendPrompt }} />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:diag', kind: 'error', message: 'boom' });
    postFromIframe(iframe, { type: 'widget:inspected', report: {} });
    expect(onSendPrompt).not.toHaveBeenCalled();
    expect(iframe.style.height).toBe('150px');
  });
});
