import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render } from '@testing-library/react';
import { useRef } from 'react';

import { useHtmlSandbox } from '../useHtmlSandbox';
import type { FramePrompts } from '../widgetProtocol';

const acted = vi.hoisted(() => ({ value: false, retryAt: null as number | null }));
vi.mock('../frameActivation', () => ({
  userActedInFrame: () => acted.value,
  retryAfterRefusal: () => acted.retryAt,
  watchFrameActivation: () => {},
}));

type Hooks = { prompts?: FramePrompts; onZoom?: (src: string, alt?: string) => void };

function Host(props: Hooks) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const { height, retryAt } = useHtmlSandbox({ iframeRef, autoHeight: true, ...props });
  return <iframe ref={iframeRef} title="widget" data-height={height ?? undefined} data-retry={retryAt ?? undefined} />;
}

let frame: HTMLIFrameElement;
const height = () => frame.getAttribute('data-height');

const fromFrame = (data: unknown, source: MessageEventSource | null = frame.contentWindow) =>
  act(() => { window.dispatchEvent(new MessageEvent('message', { data, source })); });

describe('useHtmlSandbox', () => {
  const sendPrompt = vi.fn();
  const draftPrompt = vi.fn();
  const onZoom = vi.fn();

  beforeEach(() => {
    acted.value = false;
    acted.retryAt = null;
    const view = render(<Host prompts={{ sendPrompt, draftPrompt }} onZoom={onZoom} />);
    frame = view.container.querySelector('iframe')!;
  });
  afterEach(() => vi.clearAllMocks());

  // Widget code can post these itself on load, past the frame's own check:
  // they act as the user, so only the user acting in the frame lets them through.
  it('ignores a prompt call or a zoom the user did not make', () => {
    fromFrame({ type: 'widget:sendPrompt', text: 'on load' });
    fromFrame({ type: 'widget:draftPrompt', text: 'on load', data: { a: 1 } });
    fromFrame({ type: 'widget:zoom', src: 'https://x.test/a.png' });
    expect(sendPrompt).not.toHaveBeenCalled();
    expect(draftPrompt).not.toHaveBeenCalled();
    expect(onZoom).not.toHaveBeenCalled();
  });

  it('answers them from a click in the frame, always as (text, data)', () => {
    acted.value = true;
    fromFrame({ type: 'widget:sendPrompt', text: ' more ' });
    fromFrame({ type: 'widget:draftPrompt', text: 'draft', data: { a: 1 } });
    fromFrame({ type: 'widget:zoom', src: 'https://x.test/a.png', alt: 'chart' });
    expect(sendPrompt).toHaveBeenCalledWith('more', undefined);
    expect(draftPrompt).toHaveBeenCalledWith('draft', { a: 1 });
    expect(onZoom).toHaveBeenCalledWith('https://x.test/a.png', 'chart');
  });

  it('hands the host when a refused click will count, until one is accepted', () => {
    acted.retryAt = 4200;
    fromFrame({ type: 'widget:sendPrompt', text: 'too soon' });
    expect(frame.getAttribute('data-retry')).toBe('4200');
    acted.value = true;
    fromFrame({ type: 'widget:sendPrompt', text: 'now' });
    expect(sendPrompt).toHaveBeenCalledWith('now', undefined);
    expect(frame.getAttribute('data-retry')).toBeNull();
  });

  it('leaves a call the host has no answer for unanswered', () => {
    acted.value = true;
    fromFrame({ type: 'widget:startThread', text: 'new' });
    expect(sendPrompt).not.toHaveBeenCalled();
    expect(draftPrompt).not.toHaveBeenCalled();
  });

  it('takes nothing from a window other than its own frame', () => {
    acted.value = true;
    fromFrame({ type: 'widget:sendPrompt', text: 'stranger' }, window);
    fromFrame({ type: 'widget:resize', height: 300 }, window);
    expect(sendPrompt).not.toHaveBeenCalled();
    expect(height()).toBeNull();
  });

  it('reports a bounded height, rounded up', () => {
    fromFrame({ type: 'widget:resize', height: 240.2 });
    expect(height()).toBe('241');
    fromFrame({ type: 'widget:resize', height: Number.POSITIVE_INFINITY });
    expect(height()).toBe('241');
  });
});
