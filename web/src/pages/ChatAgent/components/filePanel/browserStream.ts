import { randomUUID } from '@/lib/randomUUID';
import type {
  BrowserCloseRefusal,
  BrowserControlAction,
  BrowserFit,
  BrowserFitAnswer,
  BrowserNavigateRefusal,
  BrowserNavigation,
  BrowserViewAnswer,
  BrowserViewWant,
} from '../../utils/api/sandbox';
import { isClosed, isTransient, type BrowserPhase } from './browserPhase';
import type { BrowserUse } from '../../session/stream/browserUse';
import {
  describeBrowserCommand,
  encodeInput,
  MAX_RELEASES,
  readStreamMessage,
  releaseOf,
  STREAM_FPS,
  streamSocketUrl,
  type BrowserDoing,
  type BrowserInput,
  type BrowserRelease,
  type FrameEvent,
  type Modifiers,
  type MouseButton,
} from './browserProtocol';

const NO_MODIFIERS: Modifiers = { alt: false, ctrl: false, meta: false, shift: false };

/**
 * One workspace's view of its computer's browser, with no React in it: what to do
 * when the socket ends is the whole of this, and it is easier to read and to test
 * as a plain machine. React reads its state as an external store.
 *
 * The server is the truth about whether a browser is open. A socket that ends
 * says only that this socket ended: a daemon restart or a pause closes it and the
 * same URL reconnects, a revoked capability closes it and no retry can help, and
 * from here a refused handshake looks the same whatever refused it. So a few
 * quick retries cover the first case, and past them, or at a revocation, the
 * server is asked again, which either hands over a new URL or says the browser
 * is gone. A browser that restarts slower than the retries reads as gone, so for
 * as long as its bridge stays up the old URL keeps being tried behind that answer.
 * Nothing polls the server: past that, a closed browser stays closed on screen
 * until someone opens it or the agent is seen using it, because asking runs a
 * command on the machine, and a tab left open must not keep a computer from stopping.
 * The one exception is bounded: just after the agent was seen using the browser,
 * an answer of closed is asked again a few times, since the command that opens
 * the browser may not have got that far yet.
 */

/** Whether the person holds the page. The agent is refused while they do. */
export type BrowserControl = 'none' | 'taking' | 'held';

/** What a frame's own metadata says of the page it shows. */
export interface FrameGeometry {
  /** The frame's pixels, a uniformly scaled copy of the page. */
  width: number;
  height: number;
  /** The page's width, the space input coordinates are in. Its height is the frame's
   *  shape at that width: a frame's `deviceHeight` is the window's, taller than the
   *  page by the toolbar headless Chrome never draws, and scaling by it lands clicks low. */
  pageWidth: number;
}

/** Revoked: a new capability is the only way back. */
const CLOSE_REVOKED = 1008;
const RETRY_DELAYS_MS: readonly number[] = [500, 1500, 3000];
/** Asks in a row that led to no stream before the view stops and offers a button. */
const MAX_ASKS_WITHOUT_STREAM = 2;
/** Tries of the old URL behind a closed answer, spanning the 45 seconds a bridge
 *  outlives its stream. Past that it has gone, and any new one has a new token. */
const LINGER_DELAYS_MS: readonly number[] = Array.from({ length: 9 }, () => 5000);
/** Well inside the minute a hold lasts on the machine, which lapses by itself for a
 *  viewer that vanishes. */
const CONTROL_RENEW_MS = 20000;
/** Renewals with no input before a hold is handed back: someone who walked away
 *  must not keep the agent out. */
const CONTROL_IDLE_RENEWALS = 6;
/** Waits before a hand-back the machine did not answer goes again. Few, since a
 *  try past the minute the hold could stand lets go of nothing. */
const RELEASE_RETRY_MS: readonly number[] = [1000, 3000];
/** How long after the agent was seen using the browser a closed answer is asked
 *  again, and how often: long enough for `agent-browser open` to bring Chrome up. */
const POKE_WINDOW_MS = 15000;
const POKE_RECHECK_MS = 4000;
/** How often a view of a machine downloading Chrome for Testing looks again. */
const INSTALL_RECHECK_MS = 5_000;
/** How long the caption stays once the agent's last call ends, so a quick one is
 *  readable, and the most it stays on a call whose end a dropped stream never told. */
const CAPTION_LINGER_MS = 1500;
const CAPTION_MAX_MS = 60000;
/** How long a stream that opened has to say what page it is on. One that stays
 *  silent is a daemon whose Chrome is gone, so it is dropped and dialed again, and
 *  the server, asked once those run out, says the browser is closed. */
const FIRST_MESSAGE_MS = 15000;
/** How long a visit waits for someone to watch: a panel opening for it watches
 *  within a moment, and one remounting watches again as fast. */
const WATCH_WAIT_MS = 10000;

/** What the panel shows of the browser. It outlives a watch, as what was on screen does. */
export interface BrowserViewState {
  phase: BrowserPhase;
  /** The page the browser is on, as the stream last said. */
  pageUrl: string | null;
  control: BrowserControl;
  /** The frame on the canvas; input is mapped through it. */
  frame: FrameGeometry | null;
  /** What the agent is doing on the page, while a call of its runs. */
  agent: BrowserDoing | null;
  /** What the person typed or clicked for the visit on its way there, which may
   *  start the computer and open the browser first. */
  visiting: string | null;
}

/** The server's side of one watch, bound to its workspace. Each may reject; the
 *  session reads a failure where it lands. */
export interface BrowserServer {
  /** `chrome`: a machine of the user's own with no browser starts downloading
   *  Chrome for Testing. */
  ask: (want: BrowserViewWant) => Promise<BrowserViewAnswer>;
  /** Act on `holder`'s hold on the page; resolves to whether it holds it now. */
  control: (holder: string, action: BrowserControlAction, releases?: BrowserRelease[]) => Promise<boolean>;
  /** Size the page to `viewer`'s view; resolves to what the page did with it. */
  fit: (viewer: string, size: BrowserFit) => Promise<BrowserFitAnswer>;
  /** Close the browser for `viewer`; resolves to why it is still open, or null. */
  close: (viewer: string) => Promise<BrowserCloseRefusal | null>;
  /** Take the page somewhere for `holder`; resolves to why nothing was done, or null. */
  navigate: (holder: string, navigation: BrowserNavigation) => Promise<BrowserNavigateRefusal | null>;
  /** The view runs on the computer itself, as the desktop app on this Mac does,
   *  so it opens the stream on the computer's loopback rather than through the
   *  server, and through the server only if that never comes up. */
  local: boolean;
  /** The computer the workspace is on, which the server's calls reach; null
   *  where the view does not know it yet. */
  computer: string | null;
}

/** Whether two watches are of one browser. Every watch of a view is of its
 *  workspace's browser, on the same computer unless the workspace moved; one
 *  whose computer is not known yet is taken to be the same. */
function sameBrowser(a: BrowserServer, b: BrowserServer): boolean {
  return a === b || a.computer === null || b.computer === null || a.computer === b.computer;
}

/** How long the view's size settles before the page follows it. */
const REFIT_MS = 250;
/** How long after an ask the page left as it was (someone else holds it, a
 *  command or a navigation ran), or that went unanswered, it is asked again, and
 *  how many times in a row: each ask runs a command on the computer, and the
 *  minute these span is as long as another viewer's hold lasts unused. */
const REFIT_AGAIN_MS = 10_000;
const MAX_FIT_RETRIES = 6;

/** The last time this view asked the page to take its size. Asked again once it
 *  no longer stands (another stream, another tab, another size), or a while after
 *  the page left it as it was. Never on the shape of its frames: two views of one
 *  page would each see the other's and take the page back and forth. */
interface FitAsk {
  socket: WebSocket;
  tab: string | null;
  size: BrowserFit;
  /** Asks just before this one, for the same, that the page left as it was or
   *  that went unanswered. */
  retries: number;
  /** Its retry came due while a resize settled, which asks in its place; `held`
   *  where it was a take's, which asks afresh, as the take would have. */
  due?: 'retry' | 'held';
}

/** Why a visit opened nothing: the browser could not be opened, or someone else
 *  holds the page. */
export type BrowserVisitRefusal = BrowserNavigateRefusal | 'no_answer';

/** One stream URL dialled again on a schedule. Behind a reconnecting phase the
 *  schedule running out asks the server; behind a closed answer it gives up. */
interface Redial {
  url: string;
  delays: readonly number[];
}

/** Why the server is asked: to look, to open the browser, or to look again just
 *  after the agent used it, which leaves on screen what is there meanwhile. */
type Ask = 'look' | 'start' | 'recheck' | 'chrome';

/** How a take ended; `gone` is a take its watch, or the hold's state, moved past. */
type Take = 'held' | 'refused' | 'failed' | 'gone';

export class BrowserViewSession {
  /** Whose hold this view's is, for its whole life: a renewal or a release from
   *  it never touches a hold another viewer took. */
  readonly holder = randomUUID();
  private state: BrowserViewState = {
    phase: 'checking', pageUrl: null, control: 'none', frame: null, agent: null, visiting: null,
  };
  private readonly listeners = new Set<() => void>();
  /** What a visit waits on, rechecked on every change. */
  private readonly waiters = new Set<() => void>();
  private canvas: HTMLCanvasElement | null = null;
  /** Set only while someone watches. Each watch has its own, so a reply that
   *  lands after its watch ended can tell. */
  private server: BrowserServer | null = null;
  private socket: WebSocket | null = null;
  private asksWithoutStream = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private askSeq = 0;
  private redial: Redial | null = null;
  /** The loopback never came up this watch, so its streams go through the server. */
  private localRefused = false;
  /** Where the socket being dialed goes if it closes before its stream comes
   *  up: the server's address behind a loopback one. Null once the stream is
   *  up, and for a redial, which closes as any other. */
  private fallback: string | null = null;
  private controlTimer: ReturnType<typeof setTimeout> | null = null;
  private idleRenewals = 0;
  /** The area the frame is shown in, in page pixels, whose shape the page
   *  takes, within what its computer allows. */
  private viewSize: BrowserFit | null = null;
  private refitTimer: ReturnType<typeof setTimeout> | null = null;
  private lastFit: FitAsk | null = null;
  private fitRetry: ReturnType<typeof setTimeout> | null = null;
  /** The tab this connection last said is showing. */
  private tab: string | null = null;
  /** The close under way, the watch it was sent on, and its answer, which a
   *  second close of the same browser shares. The stream it ends is not lost. */
  private closing: { server: BrowserServer; answer: Promise<BrowserCloseRefusal | null> } | null = null;
  /** The stream on the computer's loopback this watch was last told of. */
  private localUrl: string | null = null;
  /** When the agent was last seen using the browser. */
  private pokedAt = -Infinity;
  /** The person asked to open the browser, or to get Chrome for it, and it was
   *  still installing: the look that finds it installed opens it. */
  private opensWhenInstalled = false;
  /** Counts visits, so a newer one supersedes the last. */
  private visits = 0;
  /** The last navigation, which the next one waits out, so the page ends where
   *  the person, or a visit, asked last. */
  private going: Promise<unknown> = Promise.resolve();
  /** Counts holds, so a renewal's or a navigation's reply touches only the hold
   *  it was sent under. */
  private holds = 0;
  /** Counts takes, so a take's reply touches only the take that sent it. */
  private takes = 0;
  /** This view's last hand-back, which its next take waits out: the two reach
   *  the machine on separate requests, and a release landing after the take
   *  would end the hold the take just granted. */
  private releasing: Promise<void> | null = null;
  /** What the person has down on the page: keys by code, and mouse buttons
   *  where they are. Let go of when the hold ends, since the release itself would
   *  arrive after it and never reach the page: by the machine on a hand-back,
   *  and on the stream when the hold lapsed. */
  private readonly keysDown = new Map<string, Extract<BrowserInput, { kind: 'key' }>>();
  private readonly buttonsDown = new Map<MouseButton, Extract<BrowserInput, { kind: 'mouse' }>>();
  /** Releases let go of while the stream was down: a hand-back takes them to the
   *  machine, and otherwise the next connection sends them once it speaks. */
  private unsentUps: BrowserInput[] = [];
  /** The socket whose frame is on screen. A pointer lands where the person sees
   *  it, so until the current socket paints, the frame shown may be another
   *  page's and a click on it would land on this one. */
  private painted: WebSocket | null = null;
  /** Counts the person handing the page back, which has the last word over a
   *  visit they asked for before it. */
  private handBacks = 0;
  /** Ends a stream that opened and never spoke. */
  private silence: ReturnType<typeof setTimeout> | null = null;
  private captionTimer: ReturnType<typeof setTimeout> | null = null;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };

  getSnapshot = (): BrowserViewState => this.state;

  /** Where frames are painted, as a ref callback. */
  attachCanvas = (canvas: HTMLCanvasElement | null): void => {
    this.canvas = canvas;
  };

  /** The area the frame is shown in changed. The page follows it once it
   *  settles, as a window would, whoever holds it. */
  setViewSize = (size: BrowserFit | null): void => {
    const next = size && size.width > 0 && size.height > 0
      ? { width: Math.round(size.width), height: Math.round(size.height) }
      : null;
    if (next?.width === this.viewSize?.width && next?.height === this.viewSize?.height) return;
    this.viewSize = next;
    if (this.refitTimer !== null) clearTimeout(this.refitTimer);
    this.refitTimer = setTimeout(() => {
      this.refitTimer = null;
      this.fit();
    }, REFIT_MS);
  };

  /** Asks the machine to size the page to this view where the last ask no longer
   *  stands (`FitAsk`), once a stream is up to show it. `retry` asks the same
   *  again; `held` asks afresh, since a take can find the page at another view's
   *  shape, which no ask of this view's undid. */
  private fit(why: 'change' | 'retry' | 'held' = 'change'): void {
    const server = this.server;
    const size = this.viewSize;
    const socket = this.socket;
    if (!server || !size || !socket || this.state.phase !== 'live' || this.refitTimer !== null) return;
    const last = this.lastFit;
    const same = !!last && last.socket === socket && last.tab === this.tab
      && last.size.width === size.width && last.size.height === size.height;
    const again = why !== 'change' || !!last?.due;
    if (same && !again) return;
    const fresh = why === 'held' || last?.due === 'held';
    const ask: FitAsk = { socket, tab: this.tab, size, retries: same && !fresh ? last.retries + 1 : 0 };
    this.lastFit = ask;
    this.stopFitRetry();
    const settle = (answer: BrowserFitAnswer | null) => {
      // An older ask taken can have landed after a newer one, since separate
      // requests reach the machine in either order, and undone it. Its answer
      // means it is applied, so asking again now lands after it.
      const current = this.lastFit;
      if (answer?.size && current && current !== ask
        && (current.size.width !== size.width || current.size.height !== size.height)) {
        this.fitAgain('retry');
        return;
      }
      // Taken, or kept at a size of its own, the ask stands. Left as it was for
      // now, or unanswered: asked again a while later, while it is still what
      // this view last asked, a few times.
      if (answer?.size || answer?.kept || this.lastFit !== ask || ask.retries >= MAX_FIT_RETRIES) return;
      this.fitRetry = setTimeout(() => {
        this.fitRetry = null;
        if (this.lastFit !== ask) return;
        this.fitAgain('retry');
      }, REFIT_AGAIN_MS);
    };
    server.fit(this.holder, size).then(settle, () => settle(null));
  }

  /** Asks again now, or marks the last ask due: a resize still settling, or a
   *  view with no size, would swallow the ask, and may come back on this size. */
  private fitAgain(why: 'retry' | 'held'): void {
    if (this.refitTimer !== null || this.viewSize === null) {
      // A take's stays a take's, so its ask does not spend the old ask's retries.
      if (this.lastFit && this.lastFit.due !== 'held') this.lastFit.due = why;
    } else {
      this.fit(why);
    }
  }

  private stopFitRetry(): void {
    if (this.fitRetry !== null) clearTimeout(this.fitRetry);
    this.fitRetry = null;
  }

  /** The one way the state changes: one new state and one notice, however many
   *  fields moved, so nobody reads a half-applied change. */
  private update(patch: Partial<BrowserViewState>): void {
    const next = { ...this.state, ...patch };
    const keys = Object.keys(patch) as (keyof BrowserViewState)[];
    if (keys.every((key) => next[key] === this.state[key])) return;
    if (this.state.control === 'held' && next.control !== 'held') this.letGo();
    this.state = next;
    if (next.control !== 'held') this.stopRenewing();
    for (const listener of this.listeners) listener();
    this.wake();
  }

  private wake(): void {
    for (const waiter of [...this.waiters]) waiter();
  }

  /** What `read` finds once it finds anything, read again on every change; undefined
   *  once `ms` pass first. */
  private until<T>(read: () => T | undefined, ms?: number): Promise<T | undefined> {
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout> | null = null;
      const finish = (value: T | undefined) => {
        this.waiters.delete(check);
        if (timer !== null) clearTimeout(timer);
        resolve(value);
      };
      const check = () => {
        const value = read();
        if (value !== undefined) finish(value);
      };
      this.waiters.add(check);
      if (ms !== undefined) timer = setTimeout(() => finish(undefined), ms);
      check();
    });
  }

  /**
   * Hold the stream until the returned stop, which closes the socket and hands
   * back the page. A watch starts as a new viewer would, from checking and with no
   * frame, keeping only the page address the last one saw.
   */
  watch(server: BrowserServer): () => void {
    this.server = server;
    this.asksWithoutStream = 0;
    this.localRefused = false;
    this.update({ frame: null, phase: 'checking' });
    this.check();
    // A visit waits on the watch itself, which the state may not show.
    this.wake();
    return () => {
      if (this.server !== server) return;
      this.letControlGo();
      if (this.refitTimer !== null) clearTimeout(this.refitTimer);
      this.refitTimer = null;
      this.stopFitRetry();
      this.server = null;
      this.askSeq += 1;
      this.redial = null;
      this.dropSocket();
      this.wake();
    };
  }

  /** Look again; to `start`, open the browser, and its computer, if they are not up.
   *  `dropped` is the URL whose socket just ended, tried a while longer behind an
   *  answer that says closed. */
  private check(why: Ask = 'look', dropped: string | null = null): void {
    const server = this.server;
    if (!server) return;
    this.dropSocket();
    this.redial = dropped === null ? null : { url: dropped, delays: LINGER_DELAYS_MS };
    const start = why === 'start';
    if (start) this.asksWithoutStream = 0;
    const seq = ++this.askSeq;
    if (start) this.update({ phase: 'starting' });
    else if (why === 'chrome') this.update({ phase: 'installing' });
    else if (why === 'look' && this.state.phase !== 'reconnecting') this.update({ phase: 'checking' });
    server.ask(why === 'recheck' ? 'look' : why).then(
      (answer) => { if (seq === this.askSeq) this.land(answer, why); },
      () => {
        if (seq !== this.askSeq) return;
        this.redial = null;
        this.fail();
      },
    );
  }

  open(): void {
    this.check('start');
  }

  /** Download Chrome for Testing to a machine of the user's own that has no
   *  browser, asked for by them: it is several hundred MB on their machine. */
  getChrome(): void {
    this.check('chrome');
  }

  /** The agent was seen using the browser: only a view not already showing it has
   *  anything to learn, and one nobody watches looks anyway when it is next watched.
   *  A look that finds it closed in the moments after looks again (`land`). */
  poke(): void {
    this.pokedAt = Date.now();
    if (isClosed(this.state.phase)) this.check();
  }

  /** A browser call of the agent's started or ended: the view is poked, and the
   *  caption says the latest call still running, until a moment past the last. */
  agentUsed(use: BrowserUse): void {
    this.poke();
    if (this.captionTimer !== null) clearTimeout(this.captionTimer);
    const doing = use.running === null ? null : describeBrowserCommand(use.running);
    if (doing) this.update({ agent: doing });
    this.captionTimer = setTimeout(() => {
      this.captionTimer = null;
      this.update({ agent: null });
    }, doing ? CAPTION_MAX_MS : CAPTION_LINGER_MS);
  }

  /** Reconnect by hand, which also gives a stopped view a fresh run at it. */
  retry(): void {
    this.asksWithoutStream = 0;
    this.check();
  }

  /** What the person does on the page. It goes nowhere unless they hold it. */
  input(input: BrowserInput): void {
    if (this.state.control !== 'held') return;
    const socket = this.socket;
    if (socket?.readyState !== WebSocket.OPEN) {
      // A release still counts: the press it ends reached the page, which keeps
      // it down until told, by the next connection or the hand-back.
      if (input.kind !== 'text' && input.event === 'up' && this.track(input)) this.unsentUps.push(input);
      return;
    }
    // A release still goes: the press it ends already reached the page.
    if (input.kind === 'mouse' && input.event !== 'up' && this.painted !== socket) return;
    this.idleRenewals = 0;
    for (const wire of encodeInput(input)) socket.send(wire);
    this.track(input);
  }

  /** Notes what *input* leaves down; whether it let go of a press the page has. */
  private track(input: BrowserInput): boolean {
    if (input.kind === 'key') {
      if (input.event === 'up') return this.keysDown.delete(input.code || input.key);
      this.keysDown.set(input.code || input.key, input);
    } else if (input.kind === 'mouse') {
      const button = input.button ?? 'left';
      if (input.event === 'up') return this.buttonsDown.delete(button);
      if (input.event === 'down') this.buttonsDown.set(button, input);
      else if (input.event === 'move') {
        for (const [held, down] of this.buttonsDown) this.buttonsDown.set(held, { ...down, x: input.x, y: input.y });
      }
    }
    return false;
  }

  /** Release the keys the person still has down, as the page would see their
   *  hand come off: their key-ups go wherever focus went, never to the page. */
  releaseKeys(): void {
    this.sendAll(this.keyUps());
  }

  /** The ups of the keys the person has down, which no longer count as down. */
  private keyUps(): BrowserInput[] {
    const ups: BrowserInput[] = [...this.keysDown.values()].map((down) => ({ ...down, event: 'up', modifiers: NO_MODIFIERS }));
    this.keysDown.clear();
    return ups;
  }

  /** The ups of everything the person has down, which no longer counts as down. */
  private allUps(): BrowserInput[] {
    const buttons: BrowserInput[] = [...this.buttonsDown.values()].map((down) => ({ ...down, event: 'up', clickCount: 1, modifiers: NO_MODIFIERS }));
    this.buttonsDown.clear();
    return [...this.keyUps(), ...buttons];
  }

  /** Release what the person still has down, so nothing stays pressed for the agent. */
  private letGo(): void {
    this.sendAll(this.allUps());
  }

  /** Releases wait for the next connection when this one is down: the presses
   *  they end already reached the page, which keeps them down until told. */
  private sendAll(inputs: BrowserInput[]): void {
    const socket = this.socket;
    if (socket?.readyState !== WebSocket.OPEN) {
      this.unsentUps.push(...inputs);
      return;
    }
    for (const input of inputs) for (const wire of encodeInput(input)) socket.send(wire);
  }

  /** Close the browser for the person, held or not; resolves to why it is still
   *  open, or null. The stream the close ends is not dialed again: once closed,
   *  the view shows the browser closed, with their hold gone with it, and a
   *  close that left it open looks again if its stream ended meanwhile. */
  closeBrowser(): Promise<BrowserCloseRefusal | null> {
    const server = this.server;
    if (!server) return Promise.resolve('failed');
    // A visit still under way would open the browser again behind the close.
    this.visits += 1;
    this.update({ visiting: null });
    // One at a time, whichever watch asked: a second, sent while the first waits
    // out the agent's command, could land after a visit opened the browser
    // again and close it.
    const pending = this.closingFor(server);
    if (pending) return pending;
    const closing = { server, answer: this.closeOn(server) };
    this.closing = closing;
    void closing.answer.then(() => { if (this.closing === closing) this.closing = null; });
    return closing.answer;
  }

  /** The answer of a close under way on *server*'s browser. */
  private closingFor(server: BrowserServer | null): Promise<BrowserCloseRefusal | null> | null {
    const closing = this.closing;
    return closing && server && sameBrowser(closing.server, server) ? closing.answer : null;
  }

  private async closeOn(server: BrowserServer): Promise<BrowserCloseRefusal | null> {
    const refusal = await server.close(this.holder).catch(() => 'failed' as const);
    // Shown on the watch there now, which a rebind leaves on the same browser.
    if (!this.server || !sameBrowser(this.server, server)) return refusal;
    if (refusal === null) {
      this.askSeq += 1;
      this.redial = null;
      this.dropSocket();
      this.letControlGo();
      this.update({ phase: 'idle', frame: null, pageUrl: null });
    } else if (this.socket === null && this.timer === null) {
      this.check();
    }
    return refusal;
  }

  /** Take the page to an address, or back or forward, while the person holds it.
   *  Resolves to why nothing was done, or null; a hold that lapsed is let go here too. */
  async navigate(navigation: BrowserNavigation): Promise<BrowserNavigateRefusal | null> {
    const server = this.server;
    if (!server || this.state.control !== 'held') return 'not_held';
    this.idleRenewals = 0;
    const hold = this.holds;
    const ours = () => this.server === server && this.state.control === 'held' && this.holds === hold;
    // One at a time: two in flight can land in either order on the machine. One
    // that waited out a hold the person ended goes nowhere under their next.
    const going = this.going
      .then(() => (ours() ? server.navigate(this.holder, navigation) : 'not_held' as const))
      .catch(() => 'failed' as const);
    this.going = going;
    const refusal = await going;
    if (refusal === 'not_held' && ours()) this.update({ control: 'none' });
    return refusal;
  }

  /**
   * Open an address for the person, doing what they would by hand: open the browser
   * if it is closed, take the page, then go there. Each step is tried once on a
   * watch; one that ends under them starts them over on the next, waited for a
   * little while. Resolves to why nothing opened, or null when a later visit, or
   * nobody watching, dropped this one. *typed* is what the person typed or
   * clicked, which the computer reads as an address bar does.
   */
  async visit(typed: string): Promise<BrowserVisitRefusal | null> {
    const ticket = ++this.visits;
    const handBacks = this.handBacks;
    this.update({ visiting: typed });
    this.wake();
    const current = () => ticket === this.visits && handBacks === this.handBacks;
    try {
      for (;;) {
        const server = await this.until(() => (!current() ? null : this.server ?? undefined), WATCH_WAIT_MS);
        if (!server) return null;
        const outcome = await this.visitOn(server, typed, current, () => current() && this.server === server);
        if (outcome !== 'unwatched') return outcome;
      }
    } finally {
      if (ticket === this.visits) this.update({ visiting: null });
    }
  }

  /** A visit's steps on one watch; `unwatched` once the watch, or the visit, is over. */
  private async visitOn(
    server: BrowserServer,
    url: string,
    current: () => boolean,
    carried: () => boolean,
  ): Promise<BrowserVisitRefusal | null | 'unwatched'> {
    const settled = () => this.until<BrowserPhase | null>(() => {
      if (!carried()) return null;
      const { phase, control } = this.state;
      return isTransient(phase) || control === 'taking' ? undefined : phase;
    });
    // A close still on its way shows the page live and held, and the machine runs
    // it and this visit's navigation in either order, so one of them would undo
    // the other. Asked after the close, this visit goes once it lands, opening
    // the browser again if it closed, which the close shows on this watch even
    // when an earlier watch sent it.
    const closing = this.closingFor(server);
    if (closing) {
      await closing;
      if (!carried()) return 'unwatched';
    }
    // Each step goes on only while the watch it waited on is the one there now:
    // a step taken after it ended would start, or take, with nobody looking.
    let phase = await settled();
    if (phase && isClosed(phase) && carried()) {
      this.open();
      phase = await settled();
    }
    if (!phase || !carried()) return 'unwatched';
    if (phase !== 'live') return 'not_running';
    // A navigation still out can end the hold this visit would go under.
    await this.going;
    if (!carried()) return 'unwatched';
    if (this.state.control !== 'held') {
      const taken = await this.take(server);
      if (!carried()) return 'unwatched';
      // Every viewer gets a hold of its own, so a take that came back empty, or
      // never came back, is a computer that did not answer.
      if (taken === 'refused' || taken === 'failed') return 'no_answer';
      // The person handed back or took again while it was out: theirs is the last word.
      if (taken === 'gone') return this.state.phase === 'live' ? null : 'not_running';
    }
    await this.going;
    if (!carried()) return 'unwatched';
    const refusal = await this.navigate({ action: 'open', url });
    // A watch that ended under the navigation took the hold with it, which is
    // why it was refused: the next watch tries again.
    if (refusal && !carried()) return 'unwatched';
    // A visit asked for after this one has the last word, toast included.
    return current() ? refusal : null;
  }

  takeControl(): void {
    const server = this.server;
    if (!server || this.state.control !== 'none') return;
    void this.take(server);
  }

  /** A take waits out this view's own hand-back still on its way, which would
   *  otherwise land after it and end the hold it grants. */
  private take(server: BrowserServer): Promise<Take> {
    this.update({ control: 'taking' });
    const take = ++this.takes;
    // Sent only while this is still the take this watch is waiting on: a hold
    // granted to a view that let go keeps the agent out until its release lands.
    const send = () => this.server === server && take === this.takes && this.state.control === 'taking'
      ? server.control(this.holder, 'take')
      : Promise.resolve(false);
    return (this.releasing ? this.releasing.then(send) : send()).then(
      (held) => this.took(server, take, held ? 'held' : 'refused'),
      () => this.took(server, take, 'failed'),
    );
  }

  private took(server: BrowserServer, take: number, outcome: Exclude<Take, 'gone'>): Take {
    if (this.server !== server || this.state.control !== 'taking' || take !== this.takes) {
      // A hold granted after this view let go, as when its watch ended with the
      // take in flight and the release landed first, would keep the agent out
      // for its minute with nobody holding it. A newer take of this view's own,
      // in flight or held, keeps it.
      if (outcome === 'held' && this.state.control === 'none') this.release(server);
      return 'gone';
    }
    this.update({ control: outcome === 'held' ? 'held' : 'none' });
    if (outcome === 'held') {
      this.idleRenewals = 0;
      this.renewLater(server, ++this.holds);
      // The page may be at another viewer's shape, and while this hold lasts
      // the machine sizes it to this view's alone.
      this.fitAgain('held');
    }
    return outcome;
  }

  /** The person hands the page back. */
  releaseControl(): void {
    // A visit still opening the browser takes the page once it is up, so
    // handing back before that take drops the visit too.
    const visiting = this.state.visiting !== null;
    if (this.state.control === 'none' && !visiting) return;
    this.handBacks += 1;
    this.letControlGo();
    if (visiting) this.update({ visiting: null });
  }

  /** The hold goes back without the person asking: the view closed, gave up,
   *  or they left it idle. A visit they asked for still goes. */
  private letControlGo(): void {
    if (this.state.control === 'none') return;
    // What the person still has down goes with the hand-back, which the machine
    // lets go of before the hold ends. Sent on the stream, it could land after
    // the hand-back, which goes on a request of its own, or not at all.
    const ups = this.allUps();
    this.update({ control: 'none' });
    if (this.server) this.release(this.server, ups);
    else this.unsentUps.push(...ups);
  }

  private release(server: BrowserServer, ups: BrowserInput[] = []): void {
    // A take waits out every hand-back still going, not just the last: an older
    // one landing after the take would end the hold it granted, the holder
    // being the same.
    const releasing = Promise.all([this.releasing, this.handBack(server, [...this.unsentUps.splice(0), ...ups])])
      .then(() => {});
    this.releasing = releasing;
    void releasing.then(() => { if (this.releasing === releasing) this.releasing = null; });
  }

  /** Hands the hold back with *ups*, asking again while the machine does not
   *  answer, or keeps the hold because it could not let go of them: until then,
   *  what the person had down may still be down on the page. Asking again lets
   *  go of nothing twice, since the machine lets go only within the hold it
   *  ends. Ups never delivered wait for the next connection. */
  private async handBack(server: BrowserServer, ups: BrowserInput[]): Promise<void> {
    const releases = ups.flatMap((input) => releaseOf(input) ?? []).slice(0, MAX_RELEASES);
    for (const wait of [0, ...RELEASE_RETRY_MS]) {
      if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
      try {
        if (!(await server.control(this.holder, 'release', releases))) return;
      } catch {
        // Unanswered: it may not have reached the machine.
      }
    }
    this.unsentUps.push(...ups);
  }

  private stopRenewing(): void {
    if (this.controlTimer !== null) clearTimeout(this.controlTimer);
    this.controlTimer = null;
  }

  /** Renews hold number *hold* until it ends. A reply that lands after the
   *  person handed it back and took the page again belongs to the old hold,
   *  and touches neither the new one nor its timer. */
  private renewLater(server: BrowserServer, hold: number): void {
    const ours = () => this.server === server && this.state.control === 'held' && this.holds === hold;
    this.controlTimer = setTimeout(() => {
      this.controlTimer = null;
      this.idleRenewals += 1;
      if (this.idleRenewals >= CONTROL_IDLE_RENEWALS) {
        this.letControlGo();
        return;
      }
      server.control(this.holder, 'renew').then(
        (held) => {
          if (!ours()) return;
          if (held) this.renewLater(server, hold);
          else this.update({ control: 'none' });
        },
        () => { if (ours()) this.update({ control: 'none' }); },
      );
    }, CONTROL_RENEW_MS);
  }

  private endSilence(): void {
    if (this.silence !== null) clearTimeout(this.silence);
    this.silence = null;
  }

  private dropSocket(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    this.endSilence();
    const socket = this.socket;
    this.socket = null;
    this.fallback = null;
    if (socket) {
      socket.onmessage = null;
      socket.onclose = null;
      socket.close();
    }
  }

  private land(answer: BrowserViewAnswer, why: Ask): void {
    const opens = this.opensWhenInstalled;
    if (answer.state !== 'installing') this.opensWhenInstalled = false;
    else if (why === 'start' || why === 'chrome') this.opensWhenInstalled = true;
    // Only a start opens the browser, and the polls while it installed only looked.
    if (opens && why !== 'start' && answer.state === 'idle') {
      this.check('start');
      return;
    }
    if (answer.state === 'running') {
      this.redial = null;
      const local = this.server?.local && !this.localRefused ? answer.local_url : null;
      this.localUrl = local ?? null;
      this.fallback = local ? answer.url : null;
      this.connect(local ?? answer.url);
      return;
    }
    // No browser to hold, and the hold went with it.
    this.update({ phase: answer.state, frame: null, pageUrl: null, control: 'none' });
    // A download takes minutes, and nothing else says when it ends.
    if (answer.state === 'installing') {
      this.redial = null;
      this.timer = setTimeout(() => { this.timer = null; this.check('recheck'); }, INSTALL_RECHECK_MS);
      return;
    }
    // The agent was just seen opening it: the server is asked again, since the
    // browser it brings up may sit behind a new address, rather than the old one tried.
    if (answer.state === 'idle' && Date.now() - this.pokedAt < POKE_WINDOW_MS) {
      this.redial = null;
      this.timer = setTimeout(() => { this.timer = null; this.check('recheck'); }, POKE_RECHECK_MS);
      return;
    }
    if (answer.state === 'idle' && this.redial) {
      this.dialLater();
      return;
    }
    this.redial = null;
  }

  private connect(url: string): void {
    // A reconnect keeps saying so, and behind a closed answer the phase stays
    // until the stream says otherwise.
    if (this.redial === null && this.state.phase !== 'reconnecting') this.update({ phase: 'connecting' });
    const socket = new WebSocket(streamSocketUrl(url, this.maxFps(url)));
    this.socket = socket;
    this.tab = null;
    this.endSilence();
    this.silence = setTimeout(() => {
      this.silence = null;
      if (socket === this.socket) socket.close();
    }, FIRST_MESSAGE_MS);
    socket.onmessage = (event) => { if (socket === this.socket) this.receive(event.data); };
    socket.onclose = (event) => {
      if (socket !== this.socket) return;
      const fallback = this.fallback;
      this.fallback = null;
      // The loopback refused, or took too long to say anything: the same stream
      // through the server, at once and for the rest of this watch.
      if (fallback !== null) {
        this.socket = null;
        this.localRefused = true;
        this.connect(fallback);
        return;
      }
      this.closed(event.code, url);
    };
  }

  /** Frames a second *url*'s stream sends at most: the most when read on the
   *  machine's own loopback. */
  private maxFps(url: string): number {
    return url === this.localUrl ? STREAM_FPS.local : STREAM_FPS.remote;
  }

  private receive(raw: unknown): void {
    if (this.unsentUps.length > 0) {
      const ups = this.unsentUps;
      this.unsentUps = [];
      this.sendAll(ups);
    }
    const event = readStreamMessage(raw);
    if (event?.kind === 'page') {
      // The engine sizes each tab apart, so one the agent switched to or opened
      // shows at its own size until it is asked.
      if (event.tab !== null) {
        // The first tab a stream names is the one it showed all along, which
        // an ask made before it was named already sized.
        const last = this.lastFit;
        if (this.tab === null && last?.socket === this.socket && last.tab === null) last.tab = event.tab;
        this.tab = event.tab;
      }
      // Live on the page's address, not on a frame: a blank page paints none, and
      // every computer's browser starts on one.
      this.streamUp({ pageUrl: event.url });
      // The page takes the view's shape as soon as a stream is up to show it, a
      // browser that just opened being at the agent's size.
      this.fit();
    }
    if (event?.kind === 'frame' && this.socket) this.show(event, this.socket);
  }

  /** The stream is up once it names its page or a frame decodes: a frame that
   *  does not decode leaves the first-message deadline running. */
  private streamUp(patch: Partial<BrowserViewState> = {}): void {
    this.endSilence();
    this.asksWithoutStream = 0;
    this.redial = null;
    this.fallback = null;
    this.update({ ...patch, phase: 'live' });
  }

  /** A frame decodes after its socket may have closed, so it paints, and is
   * acknowledged, only on the connection it came on. */
  private show(frame: FrameEvent, socket: WebSocket): void {
    const ack = () => {
      if (frame.ack !== null && socket.readyState === WebSocket.OPEN) socket.send(frame.ack);
    };
    const image = new Image();
    image.onload = () => {
      if (this.socket !== socket) return;
      this.painted = socket;
      const geometry = this.paint(image, frame.pageWidth ?? image.naturalWidth);
      this.streamUp({ frame: geometry });
      this.fit();
      ack();
    };
    image.onerror = ack;
    image.src = `data:image/jpeg;base64,${frame.jpegBase64}`;
  }

  /** Draws the frame and returns its geometry, the same object while it is unchanged. */
  private paint(image: HTMLImageElement, pageWidth: number): FrameGeometry {
    const { naturalWidth: width, naturalHeight: height } = image;
    const shown = this.state.frame;
    const geometry = shown?.width === width && shown.height === height && shown.pageWidth === pageWidth
      ? shown
      : { width, height, pageWidth };
    const canvas = this.canvas;
    if (canvas) {
      if (canvas.width !== width) canvas.width = width;
      if (canvas.height !== height) canvas.height = height;
      canvas.getContext('2d')?.drawImage(image, 0, 0);
    }
    return geometry;
  }

  private closed(code: number, url: string): void {
    this.socket = null;
    if (this.closingFor(this.server)) return;
    const lingering = this.redial !== null && this.state.phase !== 'reconnecting';
    if (code === CLOSE_REVOKED) {
      this.redial = null;
      if (lingering) return;
      this.update({ phase: 'reconnecting' });
      this.askAgain(url);
      return;
    }
    if (!lingering) {
      this.update({ phase: 'reconnecting' });
      this.redial ??= { url, delays: RETRY_DELAYS_MS };
    }
    this.dialLater();
  }

  /** The next try on the schedule, or, once it has run out, the server asked
   *  again from a reconnect and the old URL let go behind a closed answer. */
  private dialLater(): void {
    const redial = this.redial;
    if (!redial) return;
    const [delay, ...rest] = redial.delays;
    if (delay === undefined) {
      this.redial = null;
      if (this.state.phase === 'reconnecting') this.askAgain(redial.url);
      return;
    }
    this.redial = { url: redial.url, delays: rest };
    this.timer = setTimeout(() => { this.timer = null; this.connect(redial.url); }, delay);
  }

  /** The view gave up. A hold would only keep the agent out of a page the
   *  person can no longer see or reach, so it goes back. */
  private fail(): void {
    this.letControlGo();
    this.update({ phase: 'failed' });
  }

  private askAgain(dropped: string): void {
    if (this.asksWithoutStream >= MAX_ASKS_WITHOUT_STREAM) {
      this.fail();
      return;
    }
    this.asksWithoutStream += 1;
    this.check('look', dropped);
  }
}
