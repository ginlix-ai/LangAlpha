/**
 * The preview of a ShowWidget call hands over to the finished widget without
 * a blank frame: the widget mounts at the preview's height, transparent over
 * it, and the preview goes only once the widget has drawn and faded in.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type React from 'react';
import { act, fireEvent, render } from '@testing-library/react';
import '@testing-library/jest-dom';

import WidgetBlock from '../../WidgetBlock';
import { STREAM_RECEIVER_SCRIPT } from '../buildHtmlSrcDoc';
import { resetInlineWidgetHeightCache } from '../../InlineWidget';
import { DURATION } from '@/lib/motion';
import { WorkspaceProvider } from '../../../../contexts/WorkspaceContext';

// The grant is a server round trip; each test says what it holds. With no
// workspace the base is decided (none) at once.
const grant = vi.hoisted(() => ({
  current: { data: undefined, isError: false } as { data?: { prefix: string }; isError: boolean },
}));
vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: (workspaceId: string | null) =>
    (workspaceId ? grant.current : { data: undefined, isError: false }),
}));

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

beforeEach(() => {
  resetInlineWidgetHeightCache();
  grant.current = { data: undefined, isError: false };
  vi.useFakeTimers();
});
afterEach(() => vi.useRealTimers());

function postFrom(iframe: HTMLIFrameElement, data: unknown) {
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data, source: iframe.contentWindow }));
  });
}

const previewFrame = (c: HTMLElement) =>
  c.querySelector('.widget-stream-container iframe') as HTMLIFrameElement | null;
const widgetFrame = (c: HTMLElement) =>
  c.querySelector('.inline-widget-container:not(.widget-stream-container) iframe') as HTMLIFrameElement | null;

describe('WidgetBlock', () => {
  it('shows the preparing line until the preview draws, then redraws it at most every 80ms', () => {
    const { container, rerender } = render(<WidgetBlock preview={{ html: null, title: null, argsLength: 30 }} widgetId="widget-c1" />);
    expect(container).toHaveTextContent('toolArtifact:preparing.drawingWidget');
    const frame = previewFrame(container)!;
    expect(frame.getAttribute('srcdoc')).toContain(STREAM_RECEIVER_SCRIPT);
    const send = vi.spyOn(frame.contentWindow!, 'postMessage');
    fireEvent.load(frame);

    rerender(<WidgetBlock preview={{ html: '<p>a', title: null, argsLength: 40 }} widgetId="widget-c1" />);
    expect(send).toHaveBeenLastCalledWith({ type: 'widget:streamHtml', html: '<p>a' }, '*');
    rerender(<WidgetBlock preview={{ html: '<p>ab', title: null, argsLength: 41 }} widgetId="widget-c1" />);
    rerender(<WidgetBlock preview={{ html: '<p>abc', title: null, argsLength: 42 }} widgetId="widget-c1" />);
    expect(send).toHaveBeenCalledTimes(1);
    act(() => vi.advanceTimersByTime(80));
    expect(send).toHaveBeenCalledTimes(2);
    expect(send).toHaveBeenLastCalledWith({ type: 'widget:streamHtml', html: '<p>abc' }, '*');

    postFrom(frame, { type: 'widget:resize', height: 240 });
    expect(frame.style.height).toBe('240px');
    expect(container).not.toHaveTextContent('toolArtifact:preparing.drawingWidget');
  });

  it('hands over in place: the widget holds the preview height and shows only once drawn', () => {
    const preview = { html: '<p>abc</p>', title: null, argsLength: 50 };
    const { container, rerender } = render(<WidgetBlock preview={preview} widgetId="widget-c1" />);
    const frame = previewFrame(container)!;
    fireEvent.load(frame);
    postFrom(frame, { type: 'widget:resize', height: 240 });

    rerender(<WidgetBlock widget={{ html: '<p>abc</p><script>x()</script>', title: 'T' }} widgetId="widget-c1" />);
    // The same preview frame, now under the widget.
    expect(previewFrame(container)).toBe(frame);
    expect(frame.closest('.widget-stream-container')).toHaveAttribute('data-overlay');
    const widget = widgetFrame(container)!;
    expect(widget.style.height).toBe('240px');
    expect(widget.style.opacity).toBe('0');
    // No spinner over a preview that is still showing.
    expect(container.querySelector('.inline-widget-loading')).toBeNull();

    postFrom(widget, { type: 'widget:resize', height: 260 });
    expect(widget.style.opacity).toBe('1');
    expect(previewFrame(container)).toBe(frame);
    act(() => vi.advanceTimersByTime(DURATION.quick * 1000));
    expect(previewFrame(container)).toBeNull();
    expect(widgetFrame(container)).toBe(widget);
  });

  it('draws a widget that arrives whole as it always has', () => {
    const { container } = render(<WidgetBlock widget={{ html: '<p>x</p>', title: 'T' }} widgetId="widget-c1" />);
    expect(previewFrame(container)).toBeNull();
    expect(widgetFrame(container)!.style.height).toBe('150px');
    expect(container.querySelector('.inline-widget-loading')).toBeTruthy();
  });

  it('skips the hand-over when the preview never drew', () => {
    const { container, rerender } = render(<WidgetBlock preview={{ html: null, title: null, argsLength: 5 }} widgetId="widget-c1" />);
    rerender(<WidgetBlock widget={{ html: '<p>x</p>', title: 'T' }} widgetId="widget-c1" />);
    expect(previewFrame(container)).toBeNull();
    expect(container.querySelector('.inline-widget-loading')).toBeTruthy();
  });

  it('names the preview frame by the title written so far', () => {
    const { container } = render(<WidgetBlock preview={{ html: '<p>a', title: 'Revenue', argsLength: 40 }} widgetId="widget-c1" />);
    expect(previewFrame(container)).toHaveAttribute('title', 'Revenue');
  });
});

describe('WidgetBlock — workspace files', () => {
  const inWorkspace = (ui: React.ReactElement) => (
    <WorkspaceProvider workspaceId="ws-1" downloadFile={null}>{ui}</WorkspaceProvider>
  );
  const whole = (html: string) => <WidgetBlock widget={{ html, title: 'T' }} widgetId="widget-c1" />;

  it('renders under the grant as its <base>', () => {
    grant.current = { data: { prefix: '/api/v1/wsfiles/g/tok-1' }, isError: false };
    const { container } = render(inWorkspace(whole("<img src='a.png'>")));
    expect(widgetFrame(container)!.getAttribute('srcdoc')).toContain(`<base href="${location.origin}/api/v1/wsfiles/g/tok-1/">`);
  });

  it('renders without a base where there is no workspace', () => {
    const { container } = render(whole('<div>hi</div>'));
    expect(widgetFrame(container)!.getAttribute('srcdoc')).not.toContain('<base');
  });

  it('waits for the grant, briefly, then renders without it', () => {
    const { container } = render(inWorkspace(whole('<div>hi</div>')));
    const iframe = widgetFrame(container)!;
    expect(iframe.getAttribute('srcdoc')).toBeNull();
    act(() => vi.advanceTimersByTime(2000));
    expect(iframe.getAttribute('srcdoc')).toContain('<!DOCTYPE html>');
    expect(iframe.getAttribute('srcdoc')).not.toContain('<base');
  });

  // A renewal is a new prefix; rebuilding the document for it would reload
  // the widget and lose what the user did in it.
  it('keeps its document when the grant renews', () => {
    grant.current = { data: { prefix: '/api/v1/wsfiles/g/tok-1' }, isError: false };
    const { container, rerender } = render(inWorkspace(whole('<div>hi</div>')));
    const iframe = widgetFrame(container)!;
    const before = iframe.getAttribute('srcdoc');
    grant.current = { data: { prefix: '/api/v1/wsfiles/g/tok-2' }, isError: false };
    rerender(inWorkspace(whole('<div>hi</div>')));
    expect(iframe.getAttribute('srcdoc')).toBe(before);
  });
});
