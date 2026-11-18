// @vitest-environment node
import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { describeBrowserCommand, encodeInput, readStreamMessage, streamSocketUrl, type BrowserRelease } from '../browserProtocol';
import type { BrowserPhase } from '../browserPhase';
import { BrowserViewSession, type BrowserControl } from '../browserStream';
import { BrowserUseWatch } from '../../../session/stream/browserUse';
import type {
  BrowserControlAction,
  BrowserNavigation,
  BrowserNavigateRefusal,
  BrowserViewAnswer,
} from '../../../utils/api/sandbox';

let sockets: FakeSocket[] = [];

class FakeSocket {
  static OPEN = 1;
  readyState = FakeSocket.OPEN;
  sent: string[] = [];
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  closed = false;
  constructor(readonly url: string) { sockets.push(this); }
  send(data: string) { this.sent.push(data); }
  close() { this.closed = true; }
}

class FakeImage {
  naturalWidth = 960;
  naturalHeight = 433;
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  set src(_: string) { this.onload?.(); }
}

beforeEach(() => {
  sockets = [];
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
  vi.stubGlobal('WebSocket', FakeSocket);
  vi.stubGlobal('Image', FakeImage);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

/** Runs the next timer due, as a retry, a linger or a renewal would wait for. */
const nextTimer = () => { vi.advanceTimersToNextTimer(); };

function harness(answers: BrowserViewAnswer[]) {
  const ask = vi.fn(async (_start: boolean) => answers.shift() ?? { state: 'idle' as const });
  const control = vi.fn(async (_holder: string, action: BrowserControlAction, _releases?: BrowserRelease[]) => action !== 'release');
  /** The actions sent, which the hold's tests read in order. */
  const actions = () => control.mock.calls.map(([, action]) => action);
  const session = new BrowserViewSession();
  const phases: BrowserPhase[] = [];
  const controls: BrowserControl[] = [];
  session.subscribe(() => {
    const { phase, control } = session.getSnapshot();
    if (phases.at(-1) !== phase) phases.push(phase);
    if (controls.at(-1) !== control) controls.push(control);
  });
  const navigate = vi.fn(
    async (_holder: string, _navigation: BrowserNavigation): Promise<BrowserNavigateRefusal | null> => null,
  );
  const watch = () => session.watch({ ask, control, navigate });
  const settle = () => new Promise((resolve) => setImmediate(resolve));
  return { session, watch, sockets, phases, ask, control, navigate, actions, controls, settle };
}

const NO_MODIFIERS = { alt: false, ctrl: false, meta: false, shift: false };
const running = (url = 'https://view.example.com/b/tok'): BrowserViewAnswer => ({ state: 'running', url });
const frame = (seq = 1) => ({ data: JSON.stringify({ type: 'frame', seq, data: 'AAAA', metadata: { deviceWidth: 1280 } }) });

describe('BrowserViewSession', () => {
  it('goes live on a frame and acks it', async () => {
    const { watch, session, sockets, phases, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame(7));
    expect(phases.at(-1)).toBe('live');
    expect(JSON.parse(sockets[0].sent[0])).toEqual({ type: 'ack', seq: 7 });
    expect(session.getSnapshot().frame).toEqual({ width: 960, height: 433, pageWidth: 1280 });
  });

  it('goes live on the page address alone, since a blank page paints no frame', async () => {
    const { watch, session, sockets, phases, settle } = harness([running()]);
    watch();
    await settle();
    expect(phases.at(-1)).toBe('connecting');
    sockets[0].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'about:blank' }] }) });
    expect(phases.at(-1)).toBe('live');
    expect(session.getSnapshot().frame).toBeNull();
  });

  it('retries the same URL when a socket drops, which is a daemon restarting', async () => {
    const { watch, sockets, ask, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    sockets[0].onclose?.({ code: 1006 });
    nextTimer();
    expect(sockets[1].url).toBe(sockets[0].url);
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('drops a frame still decoding when its socket closed, on the old and the new connection', async () => {
    const decoding: FakeImage[] = [];
    vi.stubGlobal('Image', class extends FakeImage {
      constructor() { super(); decoding.push(this); }
      override set src(_: string) {}
    });
    const { watch, session, sockets, phases, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame(3));
    sockets[0].onclose?.({ code: 1006 });
    nextTimer();
    expect(sockets).toHaveLength(2);
    decoding[0].onload?.();
    expect(phases.at(-1)).toBe('reconnecting');
    expect(session.getSnapshot().frame).toBeNull();
    expect(sockets[1].sent).toEqual([]);
  });

  it('asks the server again at once when revoked, since no retry can help', async () => {
    const { watch, sockets, ask, settle } = harness([running(), running('https://view.example.com/b/new')]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    sockets[0].onclose?.({ code: 1008 });
    await settle();
    sockets[1].onmessage?.(frame());
    expect(vi.getTimerCount()).toBe(0);
    expect(ask).toHaveBeenCalledTimes(2);
    expect(sockets[1].url).toContain('/b/new');
  });

  it('drops a stream that opened and never said what page it is on, and dials again', async () => {
    const { watch, sockets, phases, ask, settle } = harness([running()]);
    watch();
    await settle();
    expect(sockets[0].closed).toBe(false);
    nextTimer();
    expect(sockets[0].closed).toBe(true);
    sockets[0].onclose?.({ code: 1005 });
    expect(phases.at(-1)).toBe('reconnecting');
    nextTimer();
    expect(sockets).toHaveLength(2);
    expect(sockets[1].url).toBe(sockets[0].url);
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('keeps waiting for a first message that shows something when the first frame does not decode', async () => {
    vi.stubGlobal('Image', class extends FakeImage {
      override set src(_: string) { this.onerror?.(); }
    });
    const { watch, sockets, phases, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame(2));
    // Acknowledged, so the next frame comes, but nothing is on screen yet.
    expect(JSON.parse(sockets[0].sent[0])).toEqual({ type: 'ack', seq: 2 });
    expect(phases.at(-1)).toBe('connecting');
    nextTimer();
    expect(sockets[0].closed).toBe(true);
  });

  it('asks the server again, not the old address, when the agent was just seen opening the browser', async () => {
    const { session, watch, sockets, settle } = harness([
      running(), { state: 'idle' }, running('https://view.example.com/b/new'),
    ]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.poke();
    sockets[0].onclose?.({ code: 1008 });
    await settle();
    expect(session.getSnapshot().phase).toBe('idle');
    nextTimer();
    await settle();
    expect(sockets.at(-1)?.url).toContain('/b/new');
  });

  it('shows the browser as closed once retries run out and the server says so', async () => {
    const { watch, sockets, phases, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    for (let i = 0; i < 3; i += 1) {
      sockets[i].onclose?.({ code: 1006 });
      nextTimer();
    }
    sockets[3].onclose?.({ code: 1006 });
    await settle();
    expect(phases.at(-1)).toBe('idle');
  });

  function closedBrowser() {
    const h = harness([running()]);
    return {
      ...h,
      async dropUntilIdle() {
        h.watch();
        await h.settle();
        h.sockets[0].onmessage?.(frame());
        for (let i = 0; i < 3; i += 1) {
          h.sockets[i].onclose?.({ code: 1006 });
          nextTimer();
        }
        h.sockets[3].onclose?.({ code: 1006 });
        await h.settle();
      },
    };
  }

  it('goes on trying the old URL behind idle, and goes live if a slow restart lands', async () => {
    const { session, sockets, phases, ask, dropUntilIdle } = closedBrowser();
    await dropUntilIdle();
    const asks = ask.mock.calls.length;
    nextTimer();
    expect(sockets[4].url).toBe(sockets[0].url);
    expect(phases.at(-1)).toBe('idle');
    sockets[4].onclose?.({ code: 1006 });
    nextTimer();
    expect(phases.at(-1)).toBe('idle');
    sockets[5].onmessage?.(frame());
    expect(phases.at(-1)).toBe('live');
    expect(ask.mock.calls.length).toBe(asks);
    expect(session.getSnapshot().frame).not.toBeNull();
  });

  it('gives the old URL up once its bridge must have gone', async () => {
    const { sockets, phases, ask, dropUntilIdle } = closedBrowser();
    await dropUntilIdle();
    const asks = ask.mock.calls.length;
    for (let i = 0; i < 20 && vi.getTimerCount() > 0; i += 1) {
      nextTimer();
      sockets.at(-1)?.onclose?.({ code: 1006 });
    }
    expect(vi.getTimerCount()).toBe(0);
    expect(sockets.length).toBeLessThanOrEqual(4 + 9);
    expect(phases.at(-1)).toBe('idle');
    expect(ask.mock.calls.length).toBe(asks);
  });

  it('stops trying the old URL when someone opens the browser', async () => {
    const { session, sockets, ask, settle, dropUntilIdle } = closedBrowser();
    await dropUntilIdle();
    session.open();
    await settle();
    expect(ask).toHaveBeenLastCalledWith(true);
    const count = sockets.length;
    nextTimer();
    expect(sockets.length).toBe(count);
  });

  it('stops asking when the stream never answers, and offers a button', async () => {
    const answers = Array.from({ length: 6 }, () => running());
    const { watch, sockets, phases, ask, settle } = harness(answers);
    watch();
    await settle();
    for (let round = 0; round < 12 && phases.at(-1) !== 'failed'; round += 1) {
      sockets.at(-1)?.onclose?.({ code: 1006 });
      nextTimer();
      await settle();
    }
    expect(phases.at(-1)).toBe('failed');
    expect(ask.mock.calls.length).toBeLessThanOrEqual(3);
  });

  it('says an open was refused for memory, and tries again on the button or the agent', async () => {
    const { watch, session, sockets, phases, ask, settle } = harness([
      { state: 'idle' }, { state: 'low_memory' }, { state: 'low_memory' }, running(),
    ]);
    watch();
    await settle();
    session.open();
    await settle();
    expect(phases.at(-1)).toBe('low_memory');
    expect(sockets).toHaveLength(0);
    session.open();
    await settle();
    expect(ask.mock.calls.map(([start]) => start)).toEqual([false, true, true]);
    session.poke();
    await settle();
    expect(phases.at(-1)).toBe('connecting');
  });

  it('never starts a browser unless told to', async () => {
    const { watch, session, ask, settle } = harness([{ state: 'idle' }]);
    watch();
    await settle();
    expect(ask).toHaveBeenLastCalledWith(false);
    session.open();
    await settle();
    expect(ask).toHaveBeenLastCalledWith(true);
  });

  it('wakes only a view that is not showing the browser when the agent is seen using it', async () => {
    const { watch, session, sockets, ask, settle } = harness([running(), running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.poke();
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('looks again while the agent has just used the browser and it is not up yet', async () => {
    const { watch, session, sockets, phases, ask, settle } = harness([{ state: 'idle' }, { state: 'idle' }, running()]);
    session.poke();
    watch();
    await settle();
    expect(phases.at(-1)).toBe('idle');
    nextTimer();
    await settle();
    expect(ask).toHaveBeenCalledTimes(2);
    // A look of its own says nothing new, so the card stays put meanwhile.
    expect(phases).not.toContain('checking');
    nextTimer();
    await settle();
    expect(ask).toHaveBeenCalledTimes(3);
    expect(sockets).toHaveLength(1);
    expect(phases.at(-1)).toBe('connecting');
  });

  it('stops looking again a little after the agent used the browser', async () => {
    const { watch, session, ask, settle } = harness([]);
    watch();
    await settle();
    session.poke();
    await settle();
    for (let i = 0; i < 10 && vi.getTimerCount() > 0; i += 1) {
      nextTimer();
      await settle();
    }
    expect(vi.getTimerCount()).toBe(0);
    // The poke's own look, then a few more inside the window.
    expect(ask.mock.calls.length).toBeGreaterThan(2);
    expect(ask.mock.calls.length).toBeLessThanOrEqual(2 + 15000 / 4000 + 1);
  });

  it('does not look again on a closed answer the agent had nothing to do with', async () => {
    const { watch, ask, settle } = harness([]);
    watch();
    await settle();
    expect(vi.getTimerCount()).toBe(0);
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('stops looking again once the tab is out of view, or on any other answer', async () => {
    const stopped = harness([{ state: 'idle' }, { state: 'stopped' }]);
    stopped.watch();
    await stopped.settle();
    stopped.session.poke();
    await stopped.settle();
    expect(vi.getTimerCount()).toBe(0);
    expect(stopped.phases.at(-1)).toBe('stopped');

    const hidden = harness([]);
    const stop = hidden.watch();
    await hidden.settle();
    hidden.session.poke();
    await hidden.settle();
    expect(vi.getTimerCount()).toBe(1);
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('starts each watch from checking with no frame, keeping the page it last saw', async () => {
    const { session, watch, sockets, settle } = harness([running(), running()]);
    const stop = watch();
    await settle();
    sockets[0].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://a.example' }] }) });
    sockets[0].onmessage?.(frame());
    stop();
    watch();
    expect(session.getSnapshot()).toEqual({ phase: 'checking', pageUrl: 'https://a.example', control: 'none', frame: null, agent: null });
  });

  it('closes its socket and answers nothing further once disposed', async () => {
    const { watch, session, sockets, ask, settle } = harness([running()]);
    const stop = watch();
    await settle();
    stop();
    expect(sockets[0].closed).toBe(true);
    session.retry();
    expect(ask).toHaveBeenCalledTimes(1);
  });
});

describe('streamSocketUrl', () => {
  it('keeps a signed query and asks for paced frames', () => {
    const url = new URL(streamSocketUrl('https://view.example.com/b/tok?signed=1'));
    expect(url.protocol).toBe('wss:');
    expect(url.pathname).toBe('/b/tok');
    expect(url.searchParams.get('signed')).toBe('1');
    expect(url.searchParams.get('pacing')).toBe('ack');
    expect(url.searchParams.get('maxFps')).toBe('8');
  });
});

describe('control of the page', () => {
  const click = { kind: 'mouse', event: 'down', x: 1, y: 2, button: 'left', modifiers: NO_MODIFIERS } as const;

  async function live() {
    const h = harness([running()]);
    const stop = h.watch();
    await h.settle();
    h.sockets[0].onmessage?.(frame());
    h.sockets[0].sent.length = 0;
    return { ...h, stop };
  }

  it('sends nothing of the reader to the page until they take control', async () => {
    const { session, sockets, controls, control, settle } = await live();
    session.input(click);
    expect(sockets[0].sent).toHaveLength(0);

    session.takeControl();
    expect(controls.at(-1)).toBe('taking');
    session.input(click);
    expect(sockets[0].sent).toHaveLength(0);
    await settle();
    expect(control).toHaveBeenLastCalledWith(session.holder, 'take');
    expect(controls.at(-1)).toBe('held');
    session.input(click);
    expect(sockets[0].sent).toHaveLength(1);
  });

  it('takes the page somewhere only for the person holding it, as its holder', async () => {
    const { session, navigate, settle } = await live();
    await expect(session.navigate({ action: 'open', url: 'example.com' })).resolves.toBe('not_held');
    expect(navigate).not.toHaveBeenCalled();

    session.takeControl();
    await settle();
    await expect(session.navigate({ action: 'open', url: 'example.com' })).resolves.toBeNull();
    expect(navigate).toHaveBeenLastCalledWith(session.holder, { action: 'open', url: 'example.com' });
    await session.navigate({ action: 'back' });
    expect(navigate).toHaveBeenLastCalledWith(session.holder, { action: 'back' });
  });

  it('lets go of a hold the machine says has lapsed', async () => {
    const { session, navigate, controls, settle } = await live();
    session.takeControl();
    await settle();
    navigate.mockResolvedValueOnce('not_held');
    await expect(session.navigate({ action: 'forward' })).resolves.toBe('not_held');
    expect(controls.at(-1)).toBe('none');
  });

  it('sends one navigation at a time, so they land on the machine in the order made', async () => {
    const { session, navigate, settle } = await live();
    session.takeControl();
    await settle();
    let finishFirst: (refusal: null) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { finishFirst = resolve; }));
    const first = session.navigate({ action: 'open', url: 'a.example.com' });
    const second = session.navigate({ action: 'back' });
    await settle();
    expect(navigate).toHaveBeenCalledTimes(1);

    finishFirst(null);
    await Promise.all([first, second]);
    expect(navigate.mock.calls.map(([, n]) => n)).toEqual([
      { action: 'open', url: 'a.example.com' },
      { action: 'back' },
    ]);
  });

  it('hands the hold back once the view gives up, so it does not keep the agent out', async () => {
    const { session, sockets, ask, actions, controls, settle } = await live();
    session.takeControl();
    await settle();
    ask.mockRejectedValueOnce(new Error('offline'));
    sockets[0].onclose?.({ code: 1008 });
    await settle();
    expect(session.getSnapshot().phase).toBe('failed');
    expect(controls.at(-1)).toBe('none');
    expect(actions().at(-1)).toBe('release');
  });

  it('takes again only once its own hand-back has landed, which would otherwise end the new hold', async () => {
    const { session, control, actions, settle } = await live();
    session.takeControl();
    await settle();
    let released: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { released = resolve; }));
    session.releaseControl();
    session.takeControl();
    await settle();
    expect(actions()).toEqual(['take', 'release']);
    released(false);
    await settle();
    expect(actions()).toEqual(['take', 'release', 'take']);
    expect(session.getSnapshot().control).toBe('held');
  });

  it('sends no take that was waiting out a hand-back once its watch has ended', async () => {
    const { session, control, actions, settle, stop } = await live();
    session.takeControl();
    await settle();
    let released: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { released = resolve; }));
    session.releaseControl();
    session.takeControl();
    stop();
    released(false);
    await settle();
    expect(actions().filter((action) => action === 'take')).toHaveLength(1);
    expect(session.getSnapshot().control).toBe('none');
  });

  it('lets a take\'s reply touch only that take, never a newer one', async () => {
    const { session, control, controls, settle } = await live();
    let failFirst: (error: Error) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((_, reject) => { failFirst = reject; }));
    session.takeControl();
    await settle();
    session.releaseControl();
    await settle();
    let grantSecond: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { grantSecond = resolve; }));
    session.takeControl();
    await settle();
    failFirst(new Error('timed out'));
    await settle();
    expect(controls.at(-1)).toBe('taking');
    grantSecond(true);
    await settle();
    expect(controls.at(-1)).toBe('held');
  });

  it('sends nowhere a navigation that waited out a hold the person ended and took again', async () => {
    const { session, navigate, settle } = await live();
    session.takeControl();
    await settle();
    let finishReload: (refusal: null) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { finishReload = resolve; }));
    void session.navigate({ action: 'reload' });
    await settle();
    const back = session.navigate({ action: 'back' });
    session.releaseControl();
    session.takeControl();
    await settle();
    finishReload(null);
    await expect(back).resolves.toBe('not_held');
    expect(navigate.mock.calls.map(([, n]) => n)).toEqual([{ action: 'reload' }]);
    expect(session.getSnapshot().control).toBe('held');
  });

  it('leaves a hold taken again alone when an older navigation finds the last one lapsed', async () => {
    const { session, navigate, controls, settle } = await live();
    session.takeControl();
    await settle();
    let answer: (refusal: 'not_held') => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { answer = resolve; }));
    const going = session.navigate({ action: 'reload' });
    await settle();
    session.releaseControl();
    session.takeControl();
    await settle();
    expect(controls.at(-1)).toBe('held');

    answer('not_held');
    await expect(going).resolves.toBe('not_held');
    expect(controls.at(-1)).toBe('held');
  });

  it('stays watching when the hold is not granted', async () => {
    const { session, sockets, controls, control, settle } = await live();
    control.mockResolvedValueOnce(false);
    session.takeControl();
    await settle();
    expect(controls.at(-1)).toBe('none');
    session.input(click);
    expect(sockets[0].sent).toHaveLength(0);
  });

  it('renews its own hold, hands it back, and renews nothing after', async () => {
    const { session, control, actions, controls, settle } = await live();
    session.takeControl();
    await settle();
    nextTimer();
    await settle();
    session.releaseControl();
    expect(actions()).toEqual(['take', 'renew', 'release']);
    expect(controls.at(-1)).toBe('none');
    nextTimer();
    await settle();
    expect(actions()).toEqual(['take', 'renew', 'release']);
    expect(new Set(control.mock.calls.map(([holder]) => holder))).toEqual(new Set([session.holder]));
  });

  it('hands back a hold granted after its watch ended, but not one a newer take wants', async () => {
    const { session, control, settle, stop, watch, actions } = await live();
    let grant: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { grant = resolve; }));
    session.takeControl();
    stop();
    grant(true);
    await settle();
    // The take landed after the view let go: it is released, not left to keep the agent out.
    expect(actions()).toEqual(['take', 'release', 'release']);

    // A take still in flight when its watch ends and the next watch takes again:
    // the late grant is the same holder's, and the newer take wants the hold, so
    // nothing is released and the newer take holds.
    const stopSecond = watch();
    await settle();
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { grant = resolve; }));
    session.takeControl();
    await settle();
    stopSecond();
    watch();
    await settle();
    let grantNewer: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { grantNewer = resolve; }));
    session.takeControl();
    await settle();
    expect(actions().at(-1)).toBe('take');
    const sent = actions().length;
    grant(true);
    await settle();
    expect(actions()).toHaveLength(sent);
    grantNewer(true);
    await settle();
    expect(session.getSnapshot().control).toBe('held');
  });

  it('takes again only once every hand-back still going has landed', async () => {
    const { session, control, settle, stop, watch, actions } = await live();
    let grant: (held: boolean) => void = () => {};
    let landFirst: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { grant = resolve; }));
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { landFirst = resolve; }));
    session.takeControl();
    stop();
    // The late grant is handed back too, and that second hand-back lands first.
    grant(true);
    await settle();
    expect(actions()).toEqual(['take', 'release', 'release']);

    watch();
    await settle();
    session.takeControl();
    await settle();
    // The first hand-back, landing after a take, would end the hold it granted.
    expect(actions()).toEqual(['take', 'release', 'release']);
    landFirst(false);
    await settle();
    expect(actions()).toEqual(['take', 'release', 'release', 'take']);
    expect(session.getSnapshot().control).toBe('held');
  });

  it('lets a late renewal reply touch only the hold it renewed', async () => {
    const { session, control, controls, settle } = await live();
    session.takeControl();
    await settle();
    let reply: (held: boolean) => void = () => {};
    control.mockImplementationOnce(() => new Promise<boolean>((resolve) => { reply = resolve; }));
    nextTimer();
    await settle();
    // Handed back and taken again while that renewal is out.
    session.releaseControl();
    session.takeControl();
    await settle();
    expect(controls.at(-1)).toBe('held');
    reply(false);
    await settle();
    expect(controls.at(-1)).toBe('held');
    expect(vi.getTimerCount()).toBe(1);
  });

  it('names a holder of its own, the same for its whole life', async () => {
    const { session, control, settle, stop, watch } = await live();
    session.takeControl();
    await settle();
    stop();
    watch();
    await settle();
    session.takeControl();
    await settle();
    expect(control.mock.calls.map(([holder]) => holder)).toEqual([session.holder, session.holder, session.holder]);
    expect(new BrowserViewSession().holder).not.toBe(session.holder);
  });

  it('shows the hold as gone when a renewal finds it gone', async () => {
    const { session, control, controls, settle } = await live();
    session.takeControl();
    await settle();
    control.mockResolvedValueOnce(false);
    nextTimer();
    await settle();
    expect(controls.at(-1)).toBe('none');
  });

  it('hands back a hold nobody has used for two minutes, and input keeps it', async () => {
    const { session, actions, controls, settle } = await live();
    session.takeControl();
    await settle();
    for (let i = 0; i < 5; i += 1) {
      nextTimer();
      await settle();
    }
    session.input(click);
    for (let i = 0; i < 5; i += 1) {
      nextTimer();
      await settle();
    }
    expect(controls.at(-1)).toBe('held');
    nextTimer();
    await settle();
    expect(controls.at(-1)).toBe('none');
    expect(actions().at(-1)).toBe('release');
  });

  it('hands the machine the keys and button still down with the hand-back, and only those', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    const shift = { kind: 'key', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: { ...NO_MODIFIERS, shift: true } } as const;
    session.input({ ...shift, event: 'down' });
    session.input({ kind: 'key', event: 'down', key: 'a', code: 'KeyA', keyCode: 65, modifiers: NO_MODIFIERS });
    session.input({ kind: 'key', event: 'up', key: 'a', code: 'KeyA', keyCode: 65, modifiers: NO_MODIFIERS });
    session.input(click);
    session.input({ kind: 'mouse', event: 'move', x: 30, y: 40, button: 'left', modifiers: NO_MODIFIERS });
    sockets[0].sent.length = 0;

    session.releaseControl();
    // Not on the stream, where they could land after the hand-back's own request.
    expect(sockets[0].sent).toEqual([]);
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual([
      'release',
      [{ key: 'Shift' }, { button: 'left', x: 30, y: 40 }],
    ]);

    // Nothing is down any more, so a later hand-back carries nothing.
    session.takeControl();
    await settle();
    session.releaseControl();
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual(['release', []]);
    expect(sockets[0].sent).toEqual([]);
  });

  it('lets go on the stream of what is still down when the hold lapses', async () => {
    const { session, sockets, control, controls, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ kind: 'key', event: 'down', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: { ...NO_MODIFIERS, shift: true } });
    sockets[0].sent.length = 0;
    control.mockResolvedValueOnce(false);
    nextTimer();
    await settle();
    expect(controls.at(-1)).toBe('none');
    expect(sockets[0].sent.map((wire) => JSON.parse(wire))).toEqual([
      expect.objectContaining({ type: 'input_keyboard', eventType: 'keyUp', key: 'Shift' }),
    ]);
  });

  it('sends a release made while the stream was down once the next connection speaks', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    const shift = { kind: 'key', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: NO_MODIFIERS } as const;
    session.input({ ...shift, event: 'down' });
    sockets[0].onclose?.({ code: 1006 });
    session.input({ ...shift, event: 'up' });
    // A press made meanwhile never reached the page, so nothing lets go of it.
    session.input({ kind: 'key', event: 'down', key: 'a', code: 'KeyA', keyCode: 65, modifiers: NO_MODIFIERS });

    nextTimer();
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://example.org/' }] }) });
    expect(sockets[1].sent.map((wire) => JSON.parse(wire)).filter((wire) => wire.type !== 'ack')).toEqual([
      expect.objectContaining({ type: 'input_keyboard', eventType: 'keyUp', key: 'Shift' }),
    ]);
    session.releaseControl();
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual(['release', []]);
  });

  it('asks again with what is still down when the hand-back goes unanswered', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ kind: 'key', event: 'down', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: NO_MODIFIERS });
    control.mockRejectedValue(new Error('503'));

    session.releaseControl();
    for (let i = 0; i < 3; i++) {
      await settle();
      nextTimer();
    }
    await settle();
    const handBacks = control.mock.calls.filter(([, action]) => action === 'release');
    expect(handBacks.map(([, , releases]) => releases)).toEqual(Array(3).fill([{ key: 'Shift' }]));

    // None answered, so the stream lets go once it next speaks.
    sockets[0].sent.length = 0;
    sockets[0].onmessage?.(frame(2));
    expect(sockets[0].sent.map((wire) => JSON.parse(wire)).filter((wire) => wire.type !== 'ack')).toEqual([
      expect.objectContaining({ type: 'input_keyboard', eventType: 'keyUp', key: 'Shift' }),
    ]);
  });

  it('asks again when the machine kept the hold because it could not let go', async () => {
    const { session, control, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ kind: 'key', event: 'down', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: NO_MODIFIERS });
    control.mockResolvedValueOnce(true);

    session.releaseControl();
    await settle();
    nextTimer();
    await settle();
    const handBacks = control.mock.calls.filter(([, action]) => action === 'release');
    expect(handBacks.map(([, , releases]) => releases)).toEqual([[{ key: 'Shift' }], [{ key: 'Shift' }]]);
    // Let go of this time, so nothing more is asked.
    nextTimer();
    await settle();
    expect(control.mock.calls.filter(([, action]) => action === 'release')).toHaveLength(2);
  });

  it('hands the machine the releases the stream could not deliver, with the hand-back', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ kind: 'key', event: 'down', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: { ...NO_MODIFIERS, shift: true } });
    session.input(click);
    sockets[0].onclose?.({ code: 1006 });
    session.releaseControl();
    // The page keeps the presses down until told, and no connection may follow.
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual([
      'release',
      [{ key: 'Shift' }, { button: 'left', x: 1, y: 2 }],
    ]);
    // So the next connection has nothing left to send.
    nextTimer();
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://example.org/' }] }) });
    expect(sockets[1].sent.map((wire) => JSON.parse(wire)).filter((wire) => wire.type !== 'ack')).toEqual([]);
  });

  it('sends the releases of a hold that lapsed while the stream was down once it speaks again', async () => {
    const { session, sockets, control, controls, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ kind: 'key', event: 'down', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: { ...NO_MODIFIERS, shift: true } });
    // Dropped, with its close still to come.
    sockets[0].readyState = 3;
    control.mockResolvedValueOnce(false);
    nextTimer();
    await settle();
    expect(controls.at(-1)).toBe('none');
    expect(control.mock.calls.map(([, action]) => action)).not.toContain('release');
    sockets[0].onclose?.({ code: 1006 });
    nextTimer();
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://example.org/' }] }) });
    const sent = () => sockets[1].sent.map((wire) => JSON.parse(wire)).filter((wire) => wire.type !== 'ack');
    expect(sent()).toEqual([expect.objectContaining({ type: 'input_keyboard', eventType: 'keyUp', key: 'Shift' })]);
    // Sent once.
    sockets[1].onmessage?.(frame(2));
    expect(sent()).toHaveLength(1);
  });

  it('lets go of every button the person still has down, each where it is', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    session.input({ ...click, button: 'left' });
    session.input({ ...click, button: 'right' });
    session.input({ ...click, event: 'up', button: 'left' });
    session.input({ kind: 'mouse', event: 'move', x: 30, y: 40, button: 'right', modifiers: NO_MODIFIERS });
    sockets[0].sent.length = 0;
    session.releaseControl();
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual(['release', [{ button: 'right', x: 30, y: 40 }]]);
  });

  it('points at the page only once the current connection has drawn it', async () => {
    const { session, sockets, settle } = await live();
    session.takeControl();
    await settle();
    session.input(click);
    // The stream drops and comes back on a page it has not drawn yet: the
    // frame on screen is the old page's.
    sockets[0].onclose?.({ code: 1006 });
    nextTimer();
    sockets[1].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://example.org/' }] }) });
    expect(session.getSnapshot().phase).toBe('live');
    session.input({ ...click, x: 5, y: 5 });
    session.input({ kind: 'key', event: 'down', key: 'a', code: 'KeyA', keyCode: 65, modifiers: NO_MODIFIERS });
    session.input({ ...click, event: 'up' });
    const sent = () => sockets[1].sent.map((wire) => JSON.parse(wire)).filter((wire) => wire.type !== 'ack');
    expect(sent()).toEqual([
      expect.objectContaining({ type: 'input_keyboard', eventType: 'keyDown' }),
      expect.objectContaining({ type: 'input_mouse', eventType: 'mouseReleased' }),
    ]);
    sockets[1].onmessage?.(frame(2));
    sockets[1].sent.length = 0;
    session.input({ ...click, x: 5, y: 5 });
    expect(sent()).toEqual([expect.objectContaining({ type: 'input_mouse', eventType: 'mousePressed' })]);
  });

  it('lets go of the keys, and only the keys, when the person\'s typing goes elsewhere', async () => {
    const { session, sockets, control, settle } = await live();
    session.takeControl();
    await settle();
    const shift = { kind: 'key', key: 'Shift', code: 'ShiftLeft', keyCode: 16, modifiers: { ...NO_MODIFIERS, shift: true } } as const;
    session.input({ ...shift, event: 'down' });
    session.input(click);
    sockets[0].sent.length = 0;
    session.releaseKeys();
    expect(sockets[0].sent.map((wire) => JSON.parse(wire))).toEqual([
      expect.objectContaining({ type: 'input_keyboard', eventType: 'keyUp', key: 'Shift' }),
    ]);
    expect(session.getSnapshot().control).toBe('held');
    session.releaseControl();
    expect(control.mock.calls.at(-1)?.slice(1)).toEqual(['release', [{ button: 'left', x: 1, y: 2 }]]);
  });

  it('hands the page back on leaving, so looking away never keeps the agent out', async () => {
    const { session, actions, settle, stop } = await live();
    session.takeControl();
    await settle();
    stop();
    expect(actions()).toEqual(['take', 'release']);
  });

  it('loses the hold when the browser goes', async () => {
    const { session, sockets, controls, settle } = await live();
    session.takeControl();
    await settle();
    for (let i = 0; i < 3; i += 1) {
      sockets[i].onclose?.({ code: 1006 });
      nextTimer();
    }
    sockets[3].onclose?.({ code: 1006 });
    await settle();
    expect(controls.at(-1)).toBe('none');
  });
});

describe('opening a link in the browser', () => {
  const URL = 'https://example.com/a';

  it('opens a closed browser, takes the page and goes there, as the person would', async () => {
    const { session, watch, sockets, ask, control, navigate, settle } = harness([{ state: 'idle' }, running()]);
    const visited = session.visit(URL);
    watch();
    await settle();
    expect(ask.mock.calls.map(([start]) => start)).toEqual([false, true]);
    sockets[0].onmessage?.(frame());
    await settle();
    expect(control).toHaveBeenLastCalledWith(session.holder, 'take');
    expect(navigate).toHaveBeenCalledWith(session.holder, { action: 'open', url: URL });
    await expect(visited).resolves.toBeNull();
    expect(session.getSnapshot().control).toBe('held');
  });

  it('goes at once on a page the person already holds', async () => {
    const { session, watch, sockets, control, navigate, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    await expect(session.visit(URL)).resolves.toBeNull();
    expect(control).toHaveBeenCalledTimes(1);
    expect(navigate).toHaveBeenCalledWith(session.holder, { action: 'open', url: URL });
  });

  it('ends on the address asked for last, and only that visit speaks', async () => {
    const { session, watch, sockets, navigate, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    let finishFirst: (refusal: BrowserNavigateRefusal | null) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { finishFirst = resolve; }));
    const first = session.visit('https://a.example');
    await settle();
    const second = session.visit('https://b.example');
    await settle();
    // The second waits for the first's navigation rather than racing it.
    expect(navigate).toHaveBeenCalledTimes(1);
    finishFirst('failed');
    await expect(first).resolves.toBeNull();
    await expect(second).resolves.toBeNull();
    expect(navigate.mock.calls.map(([, n]) => n)).toEqual([
      { action: 'open', url: 'https://a.example' },
      { action: 'open', url: 'https://b.example' },
    ]);
  });

  it.each([
    ['came back empty', (control: ReturnType<typeof harness>['control']) => control.mockResolvedValue(false)],
    ['never came back', (control: ReturnType<typeof harness>['control']) => control.mockRejectedValue(new Error('timed out'))],
  ])('says the computer did not answer when the take %s, and goes nowhere', async (_, answer) => {
    // Every viewer gets a hold of its own: the server says false only for a
    // computer that is stopped or did not answer.
    const { session, watch, sockets, control, navigate, settle } = harness([running()]);
    answer(control);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    await expect(session.visit(URL)).resolves.toBe('no_answer');
    expect(control).toHaveBeenCalledTimes(1);
    expect(navigate).not.toHaveBeenCalled();
  });

  it('drops a visit asked for before the person handed the page back', async () => {
    const { session, watch, sockets, control, navigate, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    let reloaded: (refusal: BrowserNavigateRefusal | null) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { reloaded = resolve; }));
    void session.navigate({ action: 'reload' });
    const visited = session.visit(URL);
    await settle();
    session.releaseControl();
    reloaded(null);
    await expect(visited).resolves.toBeNull();
    expect(control.mock.calls.map(([, action]) => action)).toEqual(['take', 'release']);
    expect(navigate).toHaveBeenCalledTimes(1);
  });

  it('takes the page again for a visit when a navigation still out finds the hold lapsed', async () => {
    const { session, watch, sockets, control, navigate, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    let refuse: (refusal: BrowserNavigateRefusal) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { refuse = resolve; }));
    void session.navigate({ action: 'reload' });
    const visited = session.visit(URL);
    await settle();
    refuse('not_held');
    await expect(visited).resolves.toBeNull();
    expect(control.mock.calls.map(([, action]) => action)).toEqual(['take', 'take']);
    expect(navigate).toHaveBeenLastCalledWith(session.holder, { action: 'open', url: URL });
  });

  it('goes there on the next watch when the one it was on ends under the navigation', async () => {
    const { session, watch, sockets, navigate, settle } = harness([running(), running()]);
    const stop = watch();
    await settle();
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    let refuse: (refusal: BrowserNavigateRefusal) => void = () => {};
    navigate.mockImplementationOnce(() => new Promise((resolve) => { refuse = resolve; }));
    const visited = session.visit(URL);
    await settle();
    // The panel remounts: its hand-back reaches the machine before the navigation.
    stop();
    refuse('not_held');
    watch();
    await settle();
    sockets[1].onmessage?.(frame());
    await expect(visited).resolves.toBeNull();
    expect(navigate).toHaveBeenCalledTimes(2);
  });

  it('tries opening a browser once, and leaves the tab to say why it could not', async () => {
    const { session, watch, ask, navigate, settle } = harness([{ state: 'idle' }, { state: 'low_memory' }]);
    const visited = session.visit(URL);
    watch();
    await settle();
    await expect(visited).resolves.toBe('not_running');
    expect(ask).toHaveBeenCalledTimes(2);
    expect(navigate).not.toHaveBeenCalled();
  });

  it('goes on through a watch that ends and another that starts, as a panel opening for it does', async () => {
    const { session, watch, sockets, navigate, settle } = harness([running(), running()]);
    const visited = session.visit(URL);
    watch()();
    watch();
    await settle();
    sockets.at(-1)?.onmessage?.(frame());
    await settle();
    await expect(visited).resolves.toBeNull();
    expect(navigate).toHaveBeenCalledWith(session.holder, { action: 'open', url: URL });
  });

  it('is dropped once nothing has watched for a while', async () => {
    const { session, watch, ask, navigate, settle } = harness([]);
    ask.mockImplementation(() => new Promise(() => {}));
    const visited = session.visit(URL);
    const stop = watch();
    await settle();
    stop();
    await settle();
    vi.advanceTimersByTime(10000);
    await expect(visited).resolves.toBeNull();
    expect(navigate).not.toHaveBeenCalled();
  });

  it('waits for the first watch, as a visit made before the panel opens does', async () => {
    const { session, watch, sockets, navigate, settle } = harness([running()]);
    const visited = session.visit(URL);
    vi.advanceTimersByTime(9000);
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    await settle();
    await expect(visited).resolves.toBeNull();
    expect(navigate).toHaveBeenCalledWith(session.holder, { action: 'open', url: URL });
  });

  it('drops a visit a newer one replaced, which alone goes', async () => {
    const { session, watch, sockets, navigate, settle } = harness([running()]);
    const first = session.visit('https://first.example');
    const second = session.visit(URL);
    await expect(first).resolves.toBeNull();
    watch();
    await settle();
    sockets[0].onmessage?.(frame());
    await settle();
    await expect(second).resolves.toBeNull();
    expect(navigate.mock.calls).toEqual([[session.holder, { action: 'open', url: URL }]]);
  });
});

describe('one notice per change', () => {
  it('tells its listeners once when an answer changes several things at once', async () => {
    const { session, watch, sockets, ask, settle } = harness([running()]);
    watch();
    await settle();
    sockets[0].onmessage?.({ data: JSON.stringify({ type: 'tabs', tabs: [{ active: true, url: 'https://a.example' }] }) });
    sockets[0].onmessage?.(frame());
    session.takeControl();
    await settle();
    expect(session.getSnapshot()).toMatchObject({ phase: 'live', control: 'held', pageUrl: 'https://a.example' });
    const seen: unknown[] = [];
    session.subscribe(() => seen.push(session.getSnapshot()));
    // Revoked, and the server says the browser closed: phase, page, frame and hold go together.
    ask.mockResolvedValueOnce({ state: 'stopped' });
    sockets[0].onclose?.({ code: 1008 });
    await settle();
    expect(seen.at(-1)).toEqual({ phase: 'stopped', pageUrl: null, control: 'none', frame: null, agent: null });
    expect(seen).toHaveLength(2);
    expect(seen[0]).toMatchObject({ phase: 'reconnecting', control: 'held' });
  });

  it('goes live on the first frame in one notice, with the frame', async () => {
    const { session, watch, sockets, settle } = harness([running()]);
    watch();
    await settle();
    const seen: unknown[] = [];
    session.subscribe(() => seen.push(session.getSnapshot()));
    sockets[0].onmessage?.(frame());
    expect(seen).toEqual([expect.objectContaining({ phase: 'live', frame: { width: 960, height: 433, pageWidth: 1280 } })]);
  });
});

describe('what the agent is doing', () => {
  const call = (id: string, command: string) => ({ tool_calls: [{ id, name: 'Bash', args: { command } }] });

  it('captions the latest call the stream says is still running, and clears a moment after the last ends', () => {
    const session = new BrowserViewSession();
    const stream = new BrowserUseWatch();
    const see = (type: string, event: object) => {
      const use = stream.seen(type, event);
      if (use) session.agentUsed(use);
    };
    see('tool_calls', call('a', 'agent-browser open example.com'));
    expect(session.getSnapshot().agent).toEqual({ key: 'opening', detail: 'example.com' });
    see('tool_calls', call('b', 'agent-browser snapshot -i'));
    expect(session.getSnapshot().agent?.key).toBe('reading');
    see('tool_call_result', { tool_call_id: 'b' });
    expect(session.getSnapshot().agent?.key).toBe('opening');
    see('tool_call_result', { tool_call_id: 'a' });
    expect(session.getSnapshot().agent?.key).toBe('opening');
    nextTimer();
    expect(session.getSnapshot().agent).toBeNull();
  });

  it('lets go of a call never heard to end, as a dropped stream leaves one', () => {
    const session = new BrowserViewSession();
    const command = 'agent-browser wait --text Results';
    session.agentUsed({ id: 'a', command, done: false, running: command });
    vi.advanceTimersByTime(59000);
    expect(session.getSnapshot().agent?.key).toBe('waiting');
    vi.advanceTimersByTime(1000);
    expect(session.getSnapshot().agent).toBeNull();
  });

  it.each([
    ['agent-browser open https://news.example.com/a?b=1', { key: 'opening', detail: 'news.example.com' }],
    ['cd /tmp && agent-browser --json click @e3 && agent-browser snapshot', { key: 'clicking', detail: null }],
    ['agent-browser find text "Continue with email" click', { key: 'clickingNamed', detail: 'Continue with email' }],
    ['agent-browser find role button', { key: 'reading', detail: null }],
    ["agent-browser fill @e5 'search terms'", { key: 'typing', detail: null }],
    ['agent-browser press Enter', { key: 'pressing', detail: 'Enter' }],
    ['agent-browser screenshot out.png', { key: 'screenshot', detail: null }],
    ['agent-browser frobnicate', { key: 'using', detail: null }],
    ['agent-browser open', { key: 'using', detail: null }],
    ['ls', null],
  ])('reads %s', (command, doing) => {
    expect(describeBrowserCommand(command)).toEqual(doing);
  });

  it('keeps a long name short', () => {
    expect(describeBrowserCommand(`agent-browser find text "${'x'.repeat(60)}" click`)?.detail).toHaveLength(40);
  });
});

describe('the stream contract', () => {
  it('reads a frame and what acknowledges it', () => {
    const raw = JSON.stringify({ type: 'frame', seq: 4, data: 'AA', metadata: { deviceWidth: 1280, deviceHeight: 720 } });
    expect(readStreamMessage(raw)).toEqual({ kind: 'frame', jpegBase64: 'AA', pageWidth: 1280, ack: '{"type":"ack","seq":4}' });
  });

  it('reads the active tab as the page, and ignores what the viewer has no use for', () => {
    const tabs = [{ active: false, url: 'https://a.example' }, { active: true, url: 'https://b.example' }];
    expect(readStreamMessage(JSON.stringify({ type: 'tabs', tabs }))).toEqual({ kind: 'page', url: 'https://b.example' });
    expect(readStreamMessage(JSON.stringify({ type: 'status' }))).toBeNull();
    expect(readStreamMessage('not json')).toBeNull();
  });

  it('encodes the mouse and keys the way the page needs them', () => {
    const shift = { ...NO_MODIFIERS, shift: true };
    const [mouse] = encodeInput({ kind: 'mouse', event: 'down', x: 1, y: 2, button: 'left', clickCount: 2, modifiers: shift });
    expect(JSON.parse(mouse)).toEqual({ type: 'input_mouse', eventType: 'mousePressed', x: 1, y: 2, button: 'left', clickCount: 2, modifiers: 8 });
    const [move] = encodeInput({ kind: 'mouse', event: 'move', x: 1, y: 2, modifiers: NO_MODIFIERS });
    expect(JSON.parse(move)).toEqual({ type: 'input_mouse', eventType: 'mouseMoved', x: 1, y: 2, button: 'none', modifiers: 0 });
    const [key] = encodeInput({ kind: 'key', event: 'down', key: 'Enter', code: 'Enter', keyCode: 13, modifiers: NO_MODIFIERS });
    expect(JSON.parse(key)).toEqual({ type: 'input_keyboard', eventType: 'keyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, text: '\r', modifiers: 0 });
  });

  const key = (event: 'down' | 'up', over: { key?: string; code?: string; keyCode?: number; ctrl?: boolean } = {}) => {
    const { ctrl = false, ...fields } = over;
    const [wire] = encodeInput({ kind: 'key', event, key: 'a', code: 'KeyA', keyCode: 65, ...fields, modifiers: { ...NO_MODIFIERS, ctrl } });
    return JSON.parse(wire);
  };

  it('gives a printable key its text on the way down only', () => {
    expect(key('down')).toMatchObject({ text: 'a', windowsVirtualKeyCode: 65 });
    expect(key('up')).not.toHaveProperty('text');
  });

  it('makes Enter act, and leaves a shortcut without text', () => {
    expect(key('down', { key: 'Enter', code: 'Enter', keyCode: 13 })).toMatchObject({ text: '\r' });
    expect(key('down', { key: 'Backspace', code: 'Backspace', keyCode: 8 })).not.toHaveProperty('text');
    expect(key('down', { ctrl: true })).toMatchObject({ modifiers: 2 });
    expect(key('down', { ctrl: true })).not.toHaveProperty('text');
  });

  const typed = (text: string) => encodeInput({ kind: 'text', text }).map((wire) => JSON.parse(wire).text);

  it('sends text a character at a time, since a longer char event never arrives', () => {
    expect(typed('ab 你')).toEqual(['a', 'b', ' ', '你']);
  });

  it('keeps an emoji whole and folds line endings', () => {
    expect(typed('😀\r\nx')).toEqual(['😀', '\n', 'x']);
  });

  it('has nothing to send for no text', () => {
    expect(typed('')).toEqual([]);
  });
});
