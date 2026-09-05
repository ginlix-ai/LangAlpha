import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';

import InlineWidget, { resetInlineWidgetHeightCache } from '../../InlineWidget';

// The height cache outlives unmounts by design — isolate every test from it.
beforeEach(() => {
  resetInlineWidgetHeightCache();
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
    const { container } = render(<InlineWidget html="<div>hi</div>" />);
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
    const first = render(<InlineWidget html="<div>hi</div>" />);
    const iframe = first.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:resize', height: 420 });
    first.unmount();

    // Re-open (e.g. thread revisit): box is reserved at the cached height and
    // visible immediately, but the loading glyph stays until the new document
    // actually reports.
    const second = render(<InlineWidget html="<div>hi</div>" />);
    const iframe2 = second.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe2.style.height).toBe('420px');
    expect(iframe2.style.opacity).toBe('1');
    expect(second.container.querySelector('.inline-widget-loading')).toBeTruthy();
    postFromIframe(iframe2, { type: 'widget:resize', height: 430 });
    expect(iframe2.style.height).toBe('430px');
    expect(second.container.querySelector('.inline-widget-loading')).toBeNull();
  });

  it('does not share cached heights across different widget content', () => {
    const first = render(<InlineWidget html="<div>hi</div>" />);
    const iframe = first.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:resize', height: 420 });
    first.unmount();

    const other = render(<InlineWidget html="<div>other content</div>" />);
    const iframe2 = other.container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    expect(iframe2.style.height).toBe('150px');
    expect(iframe2.style.opacity).toBe('0');
  });

  it('calls onSendPrompt on widget:sendPrompt', () => {
    const onSendPrompt = vi.fn();
    const { container } = render(<InlineWidget html="<div>hi</div>" onSendPrompt={onSendPrompt} />);
    const iframe = container.querySelector('iframe.inline-widget-frame') as HTMLIFrameElement;
    postFromIframe(iframe, { type: 'widget:sendPrompt', text: '  fix it  ' });
    expect(onSendPrompt).toHaveBeenCalledWith('fix it');
  });

  it('ignores messages from a foreign source', () => {
    const onSendPrompt = vi.fn();
    render(<InlineWidget html="<div>hi</div>" onSendPrompt={onSendPrompt} />);
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
    render(<InlineWidget html="<div>hi</div>" title="My widget" />);
    expect(screen.getByLabelText('filePanel.fullscreen')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.openInNewTab')).toBeInTheDocument();
    // Download/PDF are consolidated into the secondary "more" menu.
    expect(screen.getByLabelText('filePanel.moreActions')).toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.copySource')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.downloadAsHtml')).not.toBeInTheDocument();
  });

  it('opens the fullscreen modal with a widget-fullscreen srcDoc iframe', () => {
    render(<InlineWidget html="<div>hi</div>" title="My widget" />);
    fireEvent.click(screen.getByLabelText('filePanel.fullscreen'));
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
    render(<InlineWidget html="<div>hi</div>" title="My widget" />);
    fireEvent.click(screen.getByLabelText('filePanel.fullscreen'));
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

  it('opens a blob tab when open-in-new-tab is clicked', () => {
    render(<InlineWidget html="<div>hi</div>" />);
    fireEvent.click(screen.getByLabelText('filePanel.openInNewTab'));
    expect(window.open).toHaveBeenCalledWith('blob:x', '_blank', 'noopener,noreferrer');
  });
});
