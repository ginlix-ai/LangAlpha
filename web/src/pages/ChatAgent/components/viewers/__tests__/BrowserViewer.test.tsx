import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import '@/i18n';
import type { BrowserViewAnswer } from '../../../utils/api/sandbox';
import { BrowserViewSession } from '../../filePanel/browserStream';
import { BrowserViewer } from '../BrowserViewer';

let view: BrowserViewSession;
let stopWatching: (() => void) | null = null;

async function watched(answer: () => Promise<BrowserViewAnswer>, granted = false, computerName: string | null = null) {
  view = new BrowserViewSession();
  const ask = vi.fn(answer);
  render(<BrowserViewer view={view} computerName={computerName} />);
  await act(async () => { stopWatching = view.watch({ ask, control: async () => granted, fit: async () => ({ size: null, kept: false }), close: async () => null, navigate: async () => null, local: false, computer: null }); });
  return ask;
}

afterEach(() => {
  act(() => { stopWatching?.(); });
  stopWatching = null;
});

describe('the browser view with no page to show', () => {
  it.each([
    ['idle', 'Not open', 'Opens the browser and takes control from the agent. Shift+Esc hands it back.'],
    [
      'stopped',
      'Computer stopped',
      'Starts this computer, opens its browser and takes control from the agent. Shift+Esc hands it back.',
    ],
  ] as const)('offers a start page when the browser is %s, which opens it for where the person goes', async (state, status, note) => {
    const ask = await watched(async () => ({ state }), false, 'Cloud small');
    // Where the browser is, on which computer, above what going somewhere does.
    expect(screen.getByText('Cloud small')).toBeInTheDocument();
    expect(screen.getByText(status)).toBeInTheDocument();
    expect(screen.getByText(note)).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
    const field = screen.getByRole('textbox', { name: 'Search or enter an address' });
    fireEvent.change(field, { target: { value: 'example.com' } });
    await act(async () => { fireEvent.keyDown(field, { key: 'Enter' }); });
    expect(ask).toHaveBeenLastCalledWith('start');
  });

  it('stays while its visit starts the computer, with what was typed on its way', async () => {
    let asks = 0;
    await watched(() => (asks++ === 0 ? Promise.resolve({ state: 'stopped' as const }) : new Promise(() => {})));
    const field = screen.getByRole('textbox', { name: 'Search or enter an address' });
    fireEvent.change(field, { target: { value: 'apple earnings' } });
    await act(async () => { fireEvent.keyDown(field, { key: 'Enter' }); });
    expect(view.getSnapshot()).toMatchObject({ phase: 'starting', visiting: 'apple earnings' });
    const going = screen.getByRole('textbox', { name: 'Search or enter an address' });
    expect(going).toHaveValue('apple earnings');
    expect(going).toHaveAttribute('readonly');
    expect(screen.getByText('Computer stopped')).toBeInTheDocument();
  });

  it.each([
    ['outdated', "This computer doesn't have the browser"],
    ['unavailable', 'No browser on this computer'],
  ] as const)('says why when the browser is %s', async (state, title) => {
    await watched(async () => ({ state }));
    expect(screen.getByText(title)).toBeTruthy();
    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.queryByRole('textbox')).toBeNull();
  });

  it('offers to connect again when asking failed, without starting anything', async () => {
    const ask = await watched(async () => { throw new Error('down'); });
    expect(screen.getByText("The live view couldn't connect")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Try again' })); });
    expect(ask).toHaveBeenLastCalledWith('look');
  });

  it('shows a spinner and nothing to press while it looks', async () => {
    await watched(() => new Promise(() => {}));
    expect(screen.getAllByText('Checking the browser').length).toBeGreaterThan(0);
    expect(screen.queryByRole('button')).toBeNull();
  });
});

describe('the browser view on a blank page', () => {
  class FakeSocket {
    static OPEN = 1;
    static last: FakeSocket | null = null;
    readyState = FakeSocket.OPEN;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: ((event: { code: number }) => void) | null = null;
    constructor() { FakeSocket.last = this; }
    send() {}
    close() {}
  }

  const onPage = async (url: string, granted = false) => {
    vi.stubGlobal('WebSocket', FakeSocket);
    try {
      await watched(async () => ({ state: 'running', url: 'http://9224.preview.localhost/b/t' }), granted);
      await act(async () => {
        FakeSocket.last?.onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url }] }) });
      });
    } finally {
      vi.unstubAllGlobals();
    }
  };

  it('offers a start page instead of showing a white frame', async () => {
    await onPage('about:blank');
    expect(screen.getByRole('textbox', { name: 'Search or enter an address' })).toBeInTheDocument();
    expect(screen.getByText('Open')).toBeInTheDocument();
    expect(screen.getByRole('img', { hidden: true })).toHaveStyle({ visibility: 'hidden' });
  });

  it('shows the page once the browser is on one', async () => {
    await onPage('https://example.com/');
    expect(screen.queryByRole('textbox', { name: 'Search or enter an address' })).toBeNull();
    expect(screen.getByRole('img', { hidden: true })).not.toHaveStyle({ visibility: 'hidden' });
  });

  it('says what the agent is doing on the page while its call runs', async () => {
    await onPage('https://example.com/');
    act(() => { view.agentUsed({ id: 'a', command: 'agent-browser find text "Sign in" click', done: false, running: 'agent-browser find text "Sign in" click' }); });
    expect(screen.getByRole('status')).toHaveTextContent('Agent');
    expect(screen.getByText('Clicking “Sign in”')).toBeInTheDocument();
  });

  it('says how to use the page when someone watching clicks it', async () => {
    await onPage('https://example.com/');
    fireEvent.mouseDown(screen.getByRole('img', { hidden: true }));
    expect(screen.getByText('Take control to click and type here')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Take control' })).toBeInTheDocument();
  });

  it('says the agent waits while the person holds the page', async () => {
    await onPage('https://example.com/', true);
    await act(async () => { view.takeControl(); });
    expect(screen.getByText('You have the browser. The agent waits until you hand it back.')).toBeInTheDocument();
    expect(screen.getByText('Shift+Esc to hand back')).toBeInTheDocument();
  });

  it('hands the page back on Shift+Esc, which never reaches the page, and keeps focus on the view', async () => {
    await onPage('https://example.com/', true);
    await act(async () => { view.takeControl(); });
    const input = vi.spyOn(view, 'input');
    const keys = screen.getByRole('textbox', { hidden: true });
    fireEvent.keyDown(keys, { key: 'Escape', code: 'Escape', shiftKey: true });
    expect(view.getSnapshot().control).toBe('none');
    expect(input).not.toHaveBeenCalled();
    expect(document.activeElement).toHaveClass('browser-viewer-stage');
  });

  it('hands the page back on Shift+Esc from the start page field too, as its note says', async () => {
    await onPage('about:blank', true);
    await act(async () => { view.takeControl(); });
    const field = screen.getByRole('textbox', { name: 'Search or enter an address' });
    field.focus();
    fireEvent.keyDown(field, { key: 'Escape', code: 'Escape', shiftKey: true });
    expect(view.getSnapshot().control).toBe('none');
  });

  it('lets go of the keys still down on the page when typing moves elsewhere, and keeps the hold', async () => {
    await onPage('https://example.com/', true);
    await act(async () => { view.takeControl(); });
    const releaseKeys = vi.spyOn(view, 'releaseKeys');
    fireEvent.blur(screen.getByRole('textbox', { hidden: true }));
    expect(releaseKeys).toHaveBeenCalled();
    expect(view.getSnapshot().control).toBe('held');
  });

  it('sends Escape alone to the page as any other key', async () => {
    await onPage('https://example.com/', true);
    await act(async () => { view.takeControl(); });
    const input = vi.spyOn(view, 'input');
    fireEvent.keyDown(screen.getByRole('textbox', { hidden: true }), { key: 'Escape', code: 'Escape' });
    expect(view.getSnapshot().control).toBe('held');
    expect(input).toHaveBeenCalledWith(expect.objectContaining({ kind: 'key', event: 'down', key: 'Escape' }));
  });
});

describe('dragging on the page', () => {
  class FakeSocket {
    static OPEN = 1;
    static last: FakeSocket | null = null;
    readyState = FakeSocket.OPEN;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onclose: ((event: { code: number }) => void) | null = null;
    constructor() { FakeSocket.last = this; }
    send() {}
    close() {}
  }
  class FakeImage {
    naturalWidth = 100;
    naturalHeight = 100;
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    set src(_: string) { this.onload?.(); }
  }

  /** A held page with a 100 by 100 frame drawn in a 100 by 100 box. */
  async function held() {
    vi.stubGlobal('WebSocket', FakeSocket);
    vi.stubGlobal('Image', FakeImage);
    await watched(async () => ({ state: 'running', url: 'http://9224.preview.localhost/b/t' }), true);
    await act(async () => {
      FakeSocket.last?.onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://example.com/' }] }) });
      FakeSocket.last?.onmessage?.({ data: JSON.stringify({ type: 'frame', seq: 1, data: 'AAAA', metadata: { deviceWidth: 100 } }) });
    });
    await act(async () => { view.takeControl(); });
    const canvas = screen.getByRole('img', { hidden: true });
    canvas.getBoundingClientRect = () => ({ left: 0, top: 0, width: 100, height: 100, right: 100, bottom: 100, x: 0, y: 0, toJSON: () => ({}) });
    const input = vi.spyOn(view, 'input');
    return { canvas, input };
  }

  afterEach(() => { vi.unstubAllGlobals(); });

  it('sends a drag\'s last move before the release, under the button, even within one frame', async () => {
    const { canvas, input } = await held();
    fireEvent.mouseDown(canvas, { clientX: 10, clientY: 10, button: 0 });
    fireEvent.mouseMove(canvas, { clientX: 50, clientY: 60 });
    fireEvent.mouseUp(window, { clientX: 50, clientY: 60, button: 0 });
    expect(input.mock.calls.map(([event]) => event.kind === 'mouse' && [event.event, event.button])).toEqual([
      ['down', 'left'],
      ['move', 'left'],
      ['up', 'left'],
    ]);
  });

  it('lets go of a drag that ends off the frame where it last was on the page', async () => {
    const { canvas, input } = await held();
    fireEvent.mouseDown(canvas, { clientX: 10, clientY: 10, button: 0 });
    fireEvent.mouseMove(canvas, { clientX: 90, clientY: 60 });
    fireEvent.mouseUp(window, { clientX: 400, clientY: 60, button: 0 });
    const up = input.mock.calls.map(([event]) => event).find((event) => event.kind === 'mouse' && event.event === 'up');
    expect(up).toMatchObject({ x: 90, y: 60, button: 'left' });
  });

  it('lets go of each button on its own release, whatever else is down', async () => {
    const { canvas, input } = await held();
    fireEvent.mouseDown(canvas, { clientX: 10, clientY: 10, button: 0 });
    fireEvent.mouseDown(canvas, { clientX: 10, clientY: 10, button: 2 });
    fireEvent.mouseUp(window, { clientX: 10, clientY: 10, button: 0 });
    fireEvent.mouseUp(window, { clientX: 10, clientY: 10, button: 2 });
    const ups = input.mock.calls.map(([event]) => event).filter((event) => event.kind === 'mouse' && event.event === 'up');
    expect(ups.map((event) => event.kind === 'mouse' && event.button)).toEqual(['left', 'right']);
  });

  it('drops a drag\'s release once the hold it began under has ended', async () => {
    const { canvas, input } = await held();
    fireEvent.mouseDown(canvas, { clientX: 10, clientY: 10, button: 0 });
    await act(async () => { view.releaseControl(); });
    await act(async () => { view.takeControl(); });
    input.mockClear();
    fireEvent.mouseUp(window, { clientX: 50, clientY: 60, button: 0 });
    expect(input).not.toHaveBeenCalled();
  });
});
