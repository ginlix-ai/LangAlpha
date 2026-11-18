/**
 * The viewer's whole contract with whatever serves the browser's stream: frames
 * and the page's address coming in, mouse, keys and text going out, and the command
 * that shows the agent driving it. Everything else in the panel speaks these types,
 * which say what the reader did as the DOM says it, so this file is all a different
 * engine would change. Below them is what `agent-browser` serves and runs.
 */

export interface FrameEvent {
  kind: 'frame';
  jpegBase64: string;
  /** The page's width, the space input coordinates are in; null when unsaid. */
  pageWidth: number | null;
  /** What tells the stream this frame was shown, so it sends the next. */
  ack: string | null;
}

export interface PageEvent {
  kind: 'page';
  url: string | null;
}

export type StreamEvent = FrameEvent | PageEvent;

export interface Modifiers {
  alt: boolean;
  ctrl: boolean;
  meta: boolean;
  shift: boolean;
}

export type MouseButton = 'left' | 'middle' | 'right';

export type BrowserInput =
  | {
    kind: 'mouse';
    event: 'move' | 'down' | 'up' | 'wheel';
    x: number;
    y: number;
    /** The button pressed, released, or held through a move; none when absent. */
    button?: MouseButton;
    clickCount?: number;
    deltaX?: number;
    deltaY?: number;
    modifiers: Modifiers;
  }
  | {
    kind: 'key';
    event: 'down' | 'up';
    key: string;
    code: string;
    keyCode: number;
    modifiers: Modifiers;
  }
  | { kind: 'text'; text: string };

/** A key or mouse button the person still had down when they handed the page
 *  back and the stream could not deliver its release: the machine lets go of it
 *  before the hold ends, so nothing stays pressed for the agent. */
export type BrowserRelease = { key: string } | { button: MouseButton; x: number; y: number };

/** The machine takes no more than this many with one hand-back. */
export const MAX_RELEASES = 16;

/** The release *input* asks the machine for, or null for one that lets go of nothing. */
export function releaseOf(input: BrowserInput): BrowserRelease | null {
  if (input.kind === 'key' && input.event === 'up') return { key: input.key };
  if (input.kind === 'mouse' && input.event === 'up' && input.button) {
    return { button: input.button, x: Math.max(0, input.x), y: Math.max(0, input.y) };
  }
  return null;
}

/** Keeps a busy page near 2 Mbps with text as sharp as at 10, which a lower JPEG
 *  quality does not. A slower link is paced by the acks on top of this. */
const MAX_FPS = 8;
const MOUSE_EVENTS = { move: 'mouseMoved', down: 'mousePressed', up: 'mouseReleased', wheel: 'mouseWheel' } as const;
const KEY_EVENTS = { down: 'keyDown', up: 'keyUp' } as const;

/** The agent drives the browser from its shell, so a command naming the CLI is the only sign there is. */
const BROWSER_COMMAND = /(^|[\s;&|(])agent-browser(\s|$)/;

export function drivesBrowser(command: string): boolean {
  return BROWSER_COMMAND.test(command);
}

/** What the agent is doing on the page, as the caption under the view says it. */
export type BrowserDoingKey =
  | 'opening' | 'clicking' | 'clickingNamed' | 'typing' | 'pressing' | 'scrolling' | 'reading'
  | 'screenshot' | 'waiting' | 'script' | 'back' | 'forward' | 'reloading' | 'tabs' | 'pointing'
  | 'choosing' | 'uploading' | 'downloading' | 'closing' | 'using';

export interface BrowserDoing {
  key: BrowserDoingKey;
  /** The site opened, the key pressed or the words clicked, where the command names one. */
  detail: string | null;
}

const VERBS: Record<string, BrowserDoingKey> = {
  open: 'opening', goto: 'opening', navigate: 'opening',
  click: 'clicking', dblclick: 'clicking',
  fill: 'typing', type: 'typing', keyboard: 'typing',
  press: 'pressing', key: 'pressing',
  scroll: 'scrolling', scrollintoview: 'scrolling',
  snapshot: 'reading', get: 'reading', is: 'reading',
  screenshot: 'screenshot', pdf: 'screenshot',
  wait: 'waiting', eval: 'script',
  back: 'back', forward: 'forward', reload: 'reloading',
  tab: 'tabs', hover: 'pointing', mouse: 'pointing',
  select: 'choosing', check: 'choosing', uncheck: 'choosing',
  upload: 'uploading', download: 'downloading', close: 'closing',
};
const MAX_DETAIL = 40;

/** The words of the first browser command, quotes taken off, as a shell splits them. */
function browserWords(command: string): string[] | null {
  const match = BROWSER_COMMAND.exec(command);
  if (!match) return null;
  const rest = command.slice(match.index + match[0].length);
  const words: string[] = [];
  const word = /"((?:[^"\\]|\\.)*)"|'([^']*)'|([^\s;&|]+)|([;&|])/g;
  for (let m = word.exec(rest); m; m = word.exec(rest)) {
    if (m[4]) break;
    words.push(m[1] ?? m[2] ?? m[3]);
  }
  return words;
}

function shortened(text: string): string {
  return text.length > MAX_DETAIL ? `${text.slice(0, MAX_DETAIL - 1)}…` : text;
}

function siteOf(address: string | undefined): string | null {
  if (!address) return null;
  try {
    return new URL(/^[a-z][a-z\d+.-]*:/i.test(address) ? address : `https://${address}`).host || null;
  } catch {
    return null;
  }
}

/** Null for a command that does not run the browser CLI. */
export function describeBrowserCommand(command: string): BrowserDoing | null {
  const words = browserWords(command)?.filter((w) => !w.startsWith('-'));
  if (!words) return null;
  const [verb, ...args] = words;
  if (verb === 'find') {
    // `find <by> <value> <action>`: the words a click lands on are worth saying.
    const action = args[2];
    if (action === 'click' || action === 'dblclick') return { key: 'clickingNamed', detail: args[1] ? shortened(args[1]) : null };
    if (action === 'fill' || action === 'type') return { key: 'typing', detail: null };
    return { key: 'reading', detail: null };
  }
  const key = (verb && VERBS[verb.toLowerCase()]) || 'using';
  if (key === 'opening') {
    const site = siteOf(args[0]);
    return site ? { key, detail: site } : { key: 'using', detail: null };
  }
  if (key === 'pressing') return args[0] ? { key, detail: shortened(args[0]) } : { key: 'using', detail: null };
  return { key, detail: null };
}

export function streamSocketUrl(viewUrl: string): string {
  const url = new URL(viewUrl);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.searchParams.set('pacing', 'ack');
  url.searchParams.set('maxFps', String(MAX_FPS));
  return url.toString();
}

function activeTabUrl(tabs: unknown): string | null {
  if (!Array.isArray(tabs)) return null;
  const active = tabs.find((tab) => (tab as { active?: unknown })?.active === true) ?? tabs[0];
  const url = (active as { url?: unknown } | undefined)?.url;
  return typeof url === 'string' ? url : null;
}

/** A message off the stream, or null for one the viewer has no use for. */
export function readStreamMessage(raw: unknown): StreamEvent | null {
  let message: { type?: unknown; seq?: unknown; data?: unknown; metadata?: { deviceWidth?: unknown } | null; tabs?: unknown };
  try {
    message = JSON.parse(String(raw));
  } catch {
    return null;
  }
  if (message.type === 'tabs') return { kind: 'page', url: activeTabUrl(message.tabs) };
  if (message.type !== 'frame' || typeof message.data !== 'string') return null;
  const width = message.metadata?.deviceWidth;
  return {
    kind: 'frame',
    jpegBase64: message.data,
    pageWidth: typeof width === 'number' && width > 0 ? width : null,
    ack: typeof message.seq === 'number' ? JSON.stringify({ type: 'ack', seq: message.seq }) : null,
  };
}

/** The CDP bitfield. */
function modifierBits({ alt, ctrl, meta, shift }: Modifiers): number {
  return (alt ? 1 : 0) | (ctrl ? 2 : 0) | (meta ? 4 : 0) | (shift ? 8 : 0);
}

/** What a key down types: a printable key its character and Enter a return, a
 *  shortcut nothing. Chrome types only what a key event carries as text. */
function keyText({ event, key, modifiers }: Extract<BrowserInput, { kind: 'key' }>): string | undefined {
  if (event !== 'down' || modifiers.ctrl || modifiers.meta) return undefined;
  if (key.length === 1) return key;
  return key === 'Enter' ? '\r' : undefined;
}

/** The messages one input takes. */
export function encodeInput(input: BrowserInput): string[] {
  if (input.kind === 'text') {
    // A message a character: Chrome refuses a char event longer than three UTF-16
    // units and the stream drops it without a word, so a paste sent whole never
    // arrives. Iterating by code point keeps an emoji's two units together.
    return Array.from(input.text.replace(/\r\n?/g, '\n'), (text) => (
      JSON.stringify({ type: 'input_keyboard', eventType: 'char', text })
    ));
  }
  if (input.kind === 'key') {
    return [JSON.stringify({
      type: 'input_keyboard',
      eventType: KEY_EVENTS[input.event],
      key: input.key,
      code: input.code,
      // The virtual key code is what makes Enter and Backspace act.
      windowsVirtualKeyCode: input.keyCode,
      modifiers: modifierBits(input.modifiers),
      text: keyText(input),
    })];
  }
  return [JSON.stringify({
    type: 'input_mouse',
    eventType: MOUSE_EVENTS[input.event],
    x: input.x,
    y: input.y,
    button: input.button ?? 'none',
    clickCount: input.clickCount,
    deltaX: input.deltaX,
    deltaY: input.deltaY,
    modifiers: modifierBits(input.modifiers),
  })];
}
