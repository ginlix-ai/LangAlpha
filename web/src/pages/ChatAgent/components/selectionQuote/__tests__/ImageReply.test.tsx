import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { createRef } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ImageReply } from '../ImageReply';

const toast = vi.hoisted(() => vi.fn());
const hovered = vi.hoisted(() => ({ anchor: null as unknown, clear: () => {} }));
vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast }) }));
vi.mock('../useHoverAnchor', () => ({ useHoverAnchor: () => hovered }));

const image = Object.assign(document.createElement('img'), { src: 'https://example.com/chart.png', alt: 'chart' });
hovered.anchor = { id: 'a', kind: 'image', image, text: '' };

function replyTo(onAddContext: () => void) {
  const props = { surfaceRef: createRef<HTMLElement>(), agentName: 'Analyst', onAddContext };
  const view = render(<ImageReply {...props} enabled />);
  fireEvent.click(screen.getByLabelText('Reply to this image'));
  fireEvent.change(screen.getByLabelText('Reply to Analyst'), { target: { value: 'what is this?' } });
  fireEvent.keyDown(screen.getByLabelText('Reply to Analyst'), { key: 'Enter' });
  return { props, ...view };
}

describe('ImageReply with a slow image', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    toast.mockClear();
  });

  const slowImage = () => {
    let arrive = (_: Response) => {};
    vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>((resolve) => { arrive = resolve; })));
    return () => arrive(new Response(new Blob(['x'], { type: 'image/png' })));
  };

  it('adds it to the draft it was asked from', async () => {
    const arrive = slowImage();
    const onAddContext = vi.fn();
    replyTo(onAddContext);
    await act(async () => arrive());
    await waitFor(() => expect(onAddContext).toHaveBeenCalledOnce());
    expect(onAddContext.mock.calls[0][0]).toMatchObject({ kind: 'attachment', text: expect.stringContaining('what is this?') });
  });

  it('adds it to no other once the view has gone, and says so', async () => {
    const arrive = slowImage();
    const onAddContext = vi.fn();
    const { unmount } = replyTo(onAddContext);
    unmount();
    await act(async () => arrive());
    await waitFor(() => expect(toast).toHaveBeenCalledOnce());
    expect(onAddContext).not.toHaveBeenCalled();
  });
});
