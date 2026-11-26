import { runInNewContext } from 'node:vm';
import { describe, expect, it, vi } from 'vitest';

import { EARLY_SCRIPT } from '../frameScripts';

type Listener = (e: unknown) => void;

/** As the native one: `storageArea` is a prototype getter, and the init takes only a real Storage. */
class FakeStorageEvent {
  type: string;
  key: string | null;
  oldValue: string | null;
  newValue: string | null;
  constructor(type: string, init: { key?: string | null; oldValue?: string | null; newValue?: string | null; storageArea?: unknown } = {}) {
    if (init.storageArea != null) throw new TypeError("Failed to convert value to 'Storage'");
    this.type = type;
    this.key = init.key ?? null;
    this.oldValue = init.oldValue ?? null;
    this.newValue = init.newValue ?? null;
  }
  get storageArea(): unknown {
    return null;
  }
}

/** Runs the early program in its own realm as an opaque-origin frame sees it: storage and cookies refused. */
function boot(base = '') {
  const on: Record<string, Listener[]> = {};
  const posted: unknown[] = [];
  const dispatched: FakeStorageEvent[] = [];
  const parent = { postMessage: (m: unknown) => posted.push(m) };
  const clock = { now: 1_700_000_000_000 };
  const win: Record<string, unknown> = {
    parent,
    addEventListener: (type: string, f: Listener) => (on[type] ??= []).push(f),
    dispatchEvent: (e: FakeStorageEvent) => dispatched.push(e),
  };
  Object.defineProperty(win, 'localStorage', {
    configurable: true,
    get() {
      throw new Error('SecurityError');
    },
  });
  const doc: Record<string, unknown> = { addEventListener() {}, baseURI: base || 'about:srcdoc' };
  Object.defineProperty(doc, 'cookie', {
    configurable: true,
    get() {
      throw new Error('SecurityError');
    },
    set() {
      throw new Error('SecurityError');
    },
  });
  runInNewContext(`${EARLY_SCRIPT}({},${JSON.stringify(base)});`, {
    URL,
    window: win,
    document: doc,
    StorageEvent: FakeStorageEvent,
    Date: { now: () => clock.now, parse: Date.parse },
  });
  const fire = (type: string, e: unknown) => on[type]?.forEach((f) => f(e));
  return { win, doc, posted, dispatched, clock, fire, parent };
}

describe('early script storage stand-in', () => {
  it('tells the host of a delete or clear that finds nothing here to drop', () => {
    const { win, posted } = boot();
    const local = win.localStorage as Storage;
    const session = win.sessionStorage as Storage;
    local.removeItem('k');
    local.clear();
    session.removeItem('k');
    session.clear();
    expect(posted).toEqual([
      { type: 'widget:storage', key: 'k', value: null },
      { type: 'widget:storage', clear: true },
    ]);
  });

  it('names the stand-in as the storage area of a synced write', () => {
    const { win, dispatched, fire, parent } = boot();
    fire('message', { source: parent, data: { type: 'widget:storageSync', write: { key: 'k', value: '1' } } });
    expect(dispatched).toHaveLength(1);
    expect(dispatched[0].storageArea).toBe(win.localStorage);
    expect(dispatched[0]).toMatchObject({ key: 'k', oldValue: null, newValue: '1' });
  });
});

describe('early script cookie stand-in', () => {
  it('drops a cookie once its Max-Age or Expires has passed', () => {
    const { doc, clock } = boot();
    doc.cookie = 'a=1; Max-Age=1';
    doc.cookie = `b=2; expires=${new Date(clock.now + 1000).toUTCString()}`;
    doc.cookie = 'c=3';
    expect(doc.cookie).toBe('a=1; b=2; c=3');
    clock.now += 2000;
    expect(doc.cookie).toBe('c=3');
    doc.cookie = 'c=; Max-Age=0';
    expect(doc.cookie).toBe('');
  });
});

describe('early script submit guard', () => {
  const el = (attrs: Record<string, string>) => ({
    getAttribute: (n: string) => attrs[n] ?? null,
    hasAttribute: (n: string) => n in attrs,
  });
  function submit(form: Record<string, string>, submitter?: Record<string, string>, base = '') {
    const { fire } = boot(base);
    const preventDefault = vi.fn();
    fire('submit', { defaultPrevented: false, target: el(form), submitter: submitter && el(submitter), preventDefault });
    return preventDefault.mock.calls.length > 0;
  }

  it('lets a dialog-method submit close its dialog', () => {
    expect(submit({ method: 'dialog' })).toBe(false);
    expect(submit({ method: 'DIALOG' })).toBe(false);
    expect(submit({}, { formmethod: 'dialog' })).toBe(false);
  });

  it('still cancels a submit that would navigate the frame', () => {
    expect(submit({})).toBe(true);
    expect(submit({ method: 'post' })).toBe(true);
    expect(submit({ method: 'dialog' }, { formmethod: 'get' })).toBe(true);
    expect(submit({ target: '_blank' })).toBe(false);
    // An empty formtarget overrides the form's own: the frame itself.
    expect(submit({ target: '_blank' }, { formtarget: '' })).toBe(true);
  });

  it('cancels a new-tab submit to a workspace file, which would show the grant', () => {
    const base = 'https://app.example.com/wsfiles/g/tok-1/';
    expect(submit({ target: '_blank', action: 'private.csv' }, undefined, base)).toBe(true);
    expect(submit({ target: '_blank' }, { formaction: `${base}x.csv` }, base)).toBe(true);
    expect(submit({ target: '_blank', action: 'https://forms.example.org/new' }, undefined, base)).toBe(false);
    expect(submit({ target: '_blank' }, undefined, base)).toBe(false);
  });
});
