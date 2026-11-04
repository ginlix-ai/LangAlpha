// @vitest-environment node
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';

// The page's end of the bridge is plain script the server inlines as the
// first script of a framed document, so there is no module to import: it runs
// here from its parts, in a document of its own, wrapped as
// `preview_proxy/page_bridge.py` wraps the owner's live view, and in the order
// that file lists them. It runs on web's Vitest from this directory
// (`make test-bridge`), so the web package never reads outside web/.
const part = (name: string) => readFileSync(resolve(__dirname, `${name}.js`), 'utf8');
const quoted = (source: string) => [...source.matchAll(/"([\w-]+)"/g)].map((name) => name[1]);
const { PARTS, NESTED_PARTS } = (() => {
  const python = readFileSync(resolve(__dirname, '../page_bridge.py'), 'utf8');
  const view = /^_VIEW_PARTS = \(([^)]*)\)/m.exec(python);
  const comments = /"comments": \(\s*\*_VIEW_PARTS,([^)]*)\)/.exec(python);
  const nested = /"nested": \(([^)]*)\)/.exec(python);
  if (!view || !comments || !nested) throw new Error('page_bridge.py no longer lists its parts as this suite reads them');
  return { PARTS: [...quoted(view[1]), ...quoted(comments[1])], NESTED_PARTS: quoted(nested[1]) };
})();
const OPEN = "(function () {\n'use strict';\nif (!window.parent || window.parent === window) return;";
const BRIDGE = [OPEN, ...PARTS.map(part), 'hello();\n})();'].join('\n');
// A frame inside a page has no viewer to greet.
const NESTED = [OPEN, ...NESTED_PARTS.map(part), '})();'].join('\n');

type PageWindow = Window & typeof globalThis;
// jsdom ships no type declarations and @types/jsdom is not a dependency here.
// It is web's dependency, so it resolves from web's package, not from here.
const { JSDOM } = createRequire(resolve(__dirname, '../../../../web/package.json'))('jsdom') as {
  JSDOM: new (
    html: string,
    options: { url: string; runScripts: 'dangerously'; pretendToBeVisual: boolean; beforeParse: (w: PageWindow) => void },
  ) => { window: PageWindow };
};

const TEXT = 'The quarter ended 26 July with the company’s best текст result 汉字汉字 so far.';
const open: PageWindow[] = [];
afterEach(() => open.splice(0).forEach((w) => w.close()));

function page() {
  const sent: Record<string, unknown>[] = [];
  const viewer = { postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) };
  const { window: w } = new JSDOM(
    `<!doctype html><html><head><script>${BRIDGE}</script></head><body><p id="t">${TEXT}</p></body></html>`,
    {
      url: 'https://page-x.example.com/v/3/index.html',
      runScripts: 'dangerously',
      pretendToBeVisual: true,
      beforeParse(win) {
        Object.defineProperty(win, 'parent', { value: viewer, configurable: true });
        // jsdom lays nothing out, and the bridge offers no selection without a box.
        const box = { x: 1, y: 2, left: 1, top: 2, right: 31, bottom: 12, width: 30, height: 10, toJSON: () => ({}) };
        win.Range.prototype.getBoundingClientRect = () => box;
        win.Range.prototype.getClientRects = () => [box] as unknown as DOMRectList;
        Object.defineProperty(win.HTMLElement.prototype, 'innerText', {
          configurable: true,
          get(this: HTMLElement) { return this.textContent; },
        });
      },
    },
  );
  open.push(w);
  // The viewer names itself with its first message; nothing is sent before.
  const hello = new w.Event('message');
  Object.defineProperties(hello, {
    source: { value: viewer },
    origin: { value: 'https://app.example.com' },
    data: { value: { type: 'page:theme', css: '' } },
  });
  w.dispatchEvent(hello);

  const node = w.document.getElementById('t')!.firstChild!;
  return async function select(from: number, to: number) {
    sent.length = 0;
    w.getSelection()!.setBaseAndExtent(node, from, node, to);
    w.dispatchEvent(new w.Event('pointerup'));
    // The bridge checks a selection a beat after the release.
    await new Promise((done) => setTimeout(done, 180));
    const reported = sent.filter((m) => m.type === 'page:selection').pop() as
      | { anchor: { quote: string; prefix?: string; suffix?: string } }
      | undefined;
    return { anchor: reported?.anchor, live: w.getSelection()!.toString() };
  };
}

describe('the page bridge quotes whole words', () => {
  it('grows a selection that starts or ends inside a word to the whole word', async () => {
    const select = page();
    const { anchor, live } = await select(TEXT.indexOf('arter'), TEXT.indexOf('July') + 2);
    expect(anchor?.quote).toBe('quarter ended 26 July');
    // The words around it are read from the grown range too.
    expect(anchor?.prefix).toBe('The');
    expect(anchor?.suffix?.startsWith('with the')).toBe(true);
    // The reader's own selection stays as they made it.
    expect(live).toBe('arter ended 26 Ju');
  });

  it('keeps an apostrophe inside a word, and snaps other spaced scripts', async () => {
    const select = page();
    const at = TEXT.indexOf('company');
    expect((await select(at + 2, at + 4)).anchor?.quote).toBe('company’s');
    expect((await select(TEXT.indexOf('тек') + 1, TEXT.indexOf(' result'))).anchor?.quote).toBe('текст');
  });

  it('leaves CJK, whose characters stand alone, and a selection already on word edges', async () => {
    const select = page();
    const han = TEXT.indexOf('汉');
    expect((await select(han + 1, han + 3)).anchor?.quote).toBe('字汉');
    expect((await select(TEXT.indexOf('quarter'), TEXT.indexOf(' ended'))).anchor?.quote).toBe('quarter');
  });
});

// What the viewer sends for a commented element, and what the page answers.
function positions(body: string, anchor: Record<string, unknown>) {
  const sent: Record<string, unknown>[] = [];
  const viewer = { postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) };
  const { window: w } = new JSDOM(`<!doctype html><html><head><script>${BRIDGE}</script></head><body>${body}</body></html>`, {
    url: 'https://page-x.example.com/v/3/index.html',
    runScripts: 'dangerously',
    pretendToBeVisual: true,
    beforeParse(win) {
      Object.defineProperty(win, 'parent', { value: viewer, configurable: true });
      // jsdom lays nothing out; an element with no box reads as not found.
      const box = { x: 1, y: 2, left: 1, top: 2, right: 31, bottom: 12, width: 30, height: 10, toJSON: () => ({}) };
      win.Element.prototype.getBoundingClientRect = () => box;
      win.Element.prototype.getClientRects = () => [box] as unknown as DOMRectList;
      win.Range.prototype.getBoundingClientRect = () => box;
      win.Range.prototype.getClientRects = () => [box] as unknown as DOMRectList;
    },
  });
  open.push(w);
  const send = (data: unknown) => {
    const e = new w.Event('message');
    Object.defineProperties(e, {
      source: { value: viewer },
      origin: { value: 'https://app.example.com' },
      data: { value: data },
    });
    w.dispatchEvent(e);
  };
  send({ type: 'page:theme', css: '' });
  send({ type: 'page:anchors', items: [{ id: 'c1', anchor: { kind: 'element', label: 'x', ...anchor } }] });
  const last = sent.filter((m) => m.type === 'page:positions').pop() as { items: { id: string; rect: unknown }[] };
  return last.items[0].rect;
}

// A page in comment mode, with what it has sent the viewer.
function commentPage(body: string) {
  const sent: Record<string, unknown>[] = [];
  const viewer = { postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) };
  const { window: w } = new JSDOM(`<!doctype html><html><head><script>${BRIDGE}</script></head><body>${body}</body></html>`, {
    url: 'https://page-x.example.com/v/3/index.html',
    runScripts: 'dangerously',
    pretendToBeVisual: true,
    beforeParse(win) {
      Object.defineProperty(win, 'parent', { value: viewer, configurable: true });
      // jsdom lays nothing out: every element is drawn, and innerText, which
      // it lacks, breaks a line at each block's edges as a browser's does.
      const box = { x: 1, y: 2, left: 1, top: 2, right: 31, bottom: 12, width: 30, height: 10, toJSON: () => ({}) };
      win.Element.prototype.getBoundingClientRect = () => box;
      win.Element.prototype.getClientRects = () => [box] as unknown as DOMRectList;
      const rendered = (node: Node): string => {
        if (node.nodeType === 3) return (node as Text).data;
        if (node.nodeType !== 1) return '';
        const inner = Array.from(node.childNodes, rendered).join('');
        return /^(div|p|section|li|ul|h[1-6])$/.test((node as Element).localName) ? `\n${inner}\n` : inner;
      };
      Object.defineProperty(win.HTMLElement.prototype, 'innerText', {
        configurable: true,
        get(this: HTMLElement) { return rendered(this); },
      });
    },
  });
  open.push(w);
  const send = (data: unknown) => {
    const e = new w.Event('message');
    Object.defineProperties(e, {
      source: { value: viewer },
      origin: { value: 'https://app.example.com' },
      data: { value: data },
    });
    w.dispatchEvent(e);
  };
  send({ type: 'page:theme', css: '' });
  send({ type: 'page:comment-mode', on: true });
  return { w, sent, send };
}

// The label a click in comment mode picks the element at `selector` under.
function pickedLabel(body: string, selector: string): string | undefined {
  const { w, sent } = commentPage(body);
  w.document.querySelector(selector)!.dispatchEvent(new w.MouseEvent('click', { bubbles: true, button: 0, detail: 1 }));
  const pick = sent.filter((m) => m.type === 'page:pick').pop() as { anchor: { label?: string } } | undefined;
  return pick?.anchor.label;
}

describe('the page bridge names the document a pick is on', () => {
  it('sends the title the document has at the pick, which a comment keeps', () => {
    const { w, sent } = commentPage('<p id="p">Revenue grew on demand.</p>');
    const pick = () => {
      w.document.getElementById('p')!.dispatchEvent(new w.MouseEvent('click', { bubbles: true, button: 0, detail: 1 }));
      return sent.filter((m) => m.type === 'page:pick').pop();
    };
    // A title set after the bridge ran, as a <title> parsed after it is.
    w.document.title = '  Q3   margins ';
    expect(pick()?.title).toBe('Q3 margins');
    w.document.title = '';
    expect(pick()).not.toHaveProperty('title');
  });
});

describe('the page bridge names a container by its parts', () => {
  const tile = (label: string, value: string) =>
    `<div class="tile"><div>${label}</div><div>${value}</div><div>up 12.4% on the year before</div><div>Guide for the next quarter: 74.0%</div></div>`;
  const grid = `<div id="grid">${tile('Revenue (Q2)', '$46.7B')}${tile('Gross margin', '72.4%')}${tile('Operating income', '$28.4B')}${tile('Free cash flow', '$13.5B')}</div>`;

  it('lists the first lines of its first few children, not its first child’s', () => {
    expect(pickedLabel(grid, '#grid')).toBe('Revenue (Q2), Gross margin, Operating income…');
  });

  it('keeps a tile named by its caption, a first child of one line', () => {
    expect(pickedLabel(grid, '#grid > .tile:nth-child(2)')).toBe('Gross margin');
  });

  it('keeps an element with text of its own named by its own first line', () => {
    const passage = '<p id="p">Revenue grew <b>56%</b> on data-centre demand, with <i>gaming</i> flat over the quarter and the year.</p>';
    expect(pickedLabel(passage, '#p')).toBe('Revenue grew 56% on data-centre demand, with gaming flat…');
    expect(pickedLabel('<div id="d">Gross margin <span>72.4%</span></div>', '#d')).toBe('Gross margin 72.4%');
  });
});

describe('the page bridge pairs a pick with its own press', () => {
  it('numbers each primary press and puts the number on the pick its click makes', () => {
    const { w, sent } = commentPage('<p id="a">Revenue</p><p id="b">Gross margin</p>');
    const at = (id: string) => w.document.getElementById(id)!;
    // A browser follows a pointerdown with its mousedown, which carries the click count.
    const press = (id: string, button = 0) => {
      at(id).dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button }));
      at(id).dispatchEvent(new w.MouseEvent('mousedown', { bubbles: true, button, detail: 1 }));
    };
    const click = (id: string, detail: number) => at(id).dispatchEvent(new w.MouseEvent('click', { bubbles: true, button: 0, detail }));
    const of = (type: string) => sent.filter((m) => m.type === type);

    press('a');
    click('a', 1);
    press('b', 2);
    press('b');
    click('b', 1);
    // A click a key made (Enter on a focused element) ends no press.
    click('b', 0);
    expect(of('page:press').map((m) => m.press)).toEqual([1, 2]);
    expect(of('page:pick').map((m) => m.press)).toEqual([1, 2, undefined]);
  });

  it('counts a double click as one press and one pick, the later press and click being its continuation', () => {
    const { w, sent } = commentPage('<p id="a">Revenue</p>');
    const el = w.document.getElementById('a')!;
    const down = (detail: number) => {
      el.dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button: 0, detail: 0 }));
      el.dispatchEvent(new w.MouseEvent('mousedown', { bubbles: true, button: 0, detail }));
    };
    const click = (detail: number) => el.dispatchEvent(new w.MouseEvent('click', { bubbles: true, button: 0, detail }));
    down(1);
    click(1);
    down(2);
    click(2);
    expect(sent.filter((m) => m.type === 'page:press').map((m) => m.press)).toEqual([1]);
    expect(sent.filter((m) => m.type === 'page:pick').map((m) => m.press)).toEqual([1]);
  });

  it('cancels the selection a multi-click would start in comment mode only', () => {
    const on = commentPage('<p id="a">Revenue</p>');
    const off = commentPage('<p id="a">Revenue</p>');
    off.send({ type: 'page:comment-mode', on: false });
    const down = (w: typeof on.w, detail: number) => {
      const e = new w.MouseEvent('mousedown', { bubbles: true, cancelable: true, button: 0, detail });
      w.document.getElementById('a')!.dispatchEvent(e);
      return e.defaultPrevented;
    };
    expect([down(on.w, 1), down(on.w, 2), down(on.w, 3)]).toEqual([false, true, true]);
    expect(down(off.w, 2)).toBe(false);
  });

  it('presses for a pointerdown whose mousedown never comes, and again for a click after a pause', async () => {
    const { w, sent } = commentPage('<p id="a">Revenue</p>');
    const el = w.document.getElementById('a')!;
    const of = (type: string) => sent.filter((m) => m.type === type);
    el.dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button: 0 }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(of('page:press').map((m) => m.press)).toEqual([1]);
    el.dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button: 0 }));
    el.dispatchEvent(new w.MouseEvent('mousedown', { bubbles: true, button: 0, detail: 1 }));
    el.dispatchEvent(new w.MouseEvent('click', { bubbles: true, button: 0, detail: 1 }));
    expect(of('page:press').map((m) => m.press)).toEqual([1, 2]);
    expect(of('page:pick').map((m) => m.press)).toEqual([2]);
  });
});

describe('the page bridge keeps a label comment off a longer sentence', () => {
  // Reached by position, not by id, so a stranger at the path loses the pin.
  const moved = '<p>Something else entirely</p>';
  const anchor = { path: 'body > p:nth-of-type(1)', quote: 'Revenue' };

  it('finds no position when the only other match is a sentence that begins with the label', () => {
    expect(positions(`${moved}<p>Revenue beat the consensus by 4%</p>`, anchor)).toBeNull();
    expect(positions(`${moved}<p>Revenue by segment</p>`, anchor)).toBeNull();
  });

  it('finds an element that reads as the label with a word added', () => {
    expect(positions(`${moved}<p>Total revenue</p>`, anchor)).not.toBeNull();
    expect(positions(`${moved}<p>Gross margin (GAAP)</p>`, { path: 'body > p:nth-of-type(1)', quote: 'GROSS MARGIN' })).not.toBeNull();
  });
});

/** How wide each character is drawn on a page laid out with glyphs, in px. */
const CHAR = 6;

interface LayoutOptions {
  /**
   * Draw text too: each character `CHAR` px wide from its element's left
   * edge, as tall as the element, with the caret API a browser has and the
   * selection a drag over text makes. Without it the page is a browser with
   * no caret API, where every press is an element's or an area's.
   */
  glyphs?: boolean;
  /** The bridge's source, for a test of a one-line switch in it. */
  source?: string;
  /** A browser with the highlight API, whose highlights are kept here. */
  highlights?: Map<string, { ranges: Range[] }>;
  /** A browser without Intl.Segmenter. */
  noSegmenter?: boolean;
}

// A page in comment mode laid out by hand: an element with `data-box="x y w
// h"` is drawn there, and one without spans the elements inside it, as a
// container does. jsdom lays nothing out, and an area is all geometry.
function laidOutPage(body: string, { glyphs = false, source = BRIDGE, highlights, noSegmenter = false }: LayoutOptions = {}) {
  const sent: Record<string, unknown>[] = [];
  const viewer = { postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) };
  const { window: w } = new JSDOM(`<!doctype html><html><head><script>${source}</script></head><body>${body}</body></html>`, {
    url: 'https://page-x.example.com/v/3/index.html',
    runScripts: 'dangerously',
    pretendToBeVisual: true,
    beforeParse(win) {
      Object.defineProperty(win, 'parent', { value: viewer, configurable: true });
      type Box = { x: number; y: number; left: number; top: number; right: number; bottom: number; width: number; height: number; toJSON: () => object };
      const make = (left: number, top: number, right: number, bottom: number): Box => ({
        x: left, y: top, left, top, right, bottom, width: right - left, height: bottom - top, toJSON: () => ({}),
      });
      const boxOf = (el: Element): Box | null => {
        const spec = el.getAttribute('data-box');
        if (spec) {
          const [x, y, width, height] = spec.split(' ').map(Number);
          return make(x, y, x + width, y + height);
        }
        let box: Box | null = null;
        for (const child of Array.from(el.children)) {
          const b = boxOf(child);
          if (!b) continue;
          box = box ? make(Math.min(box.left, b.left), Math.min(box.top, b.top), Math.max(box.right, b.right), Math.max(box.bottom, b.bottom)) : b;
        }
        return box;
      };
      win.Element.prototype.getBoundingClientRect = function (this: Element) {
        return (boxOf(this) ?? make(0, 0, 0, 0)) as unknown as DOMRect;
      };
      win.Element.prototype.getClientRects = function (this: Element) {
        const box = boxOf(this);
        return (box && box.width ? [box] : []) as unknown as DOMRectList;
      };
      // The deepest drawn element at a point.
      win.Document.prototype.elementFromPoint = function (this: Document, x: number, y: number) {
        let hit: Element | null = null;
        for (const el of Array.from(this.body.querySelectorAll('*'))) {
          const b = boxOf(el);
          if (b && x >= b.left && x <= b.right && y >= b.top && y <= b.bottom) hit = el;
        }
        return hit;
      };
      if (glyphs) {
        // A text node directly in an element with a box is drawn in one line
        // from its left edge; any other text is not drawn.
        const lineOf = (t: Text) => {
          const spec = t.parentElement?.getAttribute('data-box');
          if (!spec) return null;
          const [x, y, , h] = spec.split(' ').map(Number);
          return { x, y, h };
        };
        const textRects = (range: Range): Box[] => {
          const doc = range.startContainer.ownerDocument!;
          const walker = doc.createTreeWalker(doc.body, 4);
          const out: Box[] = [];
          let on = false;
          for (let n = walker.nextNode() as Text | null; n; n = walker.nextNode() as Text | null) {
            if (n === range.startContainer) on = true;
            if (on) {
              const line = lineOf(n);
              const from = n === range.startContainer ? range.startOffset : 0;
              const to = n === range.endContainer ? range.endOffset : n.data.length;
              if (line && to > from) out.push(make(line.x + from * CHAR, line.y, line.x + to * CHAR, line.y + line.h));
            }
            if (n === range.endContainer) break;
          }
          return out;
        };
        win.Range.prototype.getClientRects = function (this: Range) {
          return textRects(this) as unknown as DOMRectList;
        };
        win.Range.prototype.getBoundingClientRect = function (this: Range) {
          const all = textRects(this);
          if (!all.length) return make(0, 0, 0, 0) as unknown as DOMRect;
          return make(
            Math.min(...all.map((r) => r.left)), Math.min(...all.map((r) => r.top)),
            Math.max(...all.map((r) => r.right)), Math.max(...all.map((r) => r.bottom)),
          ) as unknown as DOMRect;
        };
        // As a browser's: the caret nearest the point in the text of the
        // element there, wherever in its box the point is.
        Object.defineProperty(win.Document.prototype, 'caretPositionFromPoint', {
          configurable: true,
          value(this: Document, x: number, y: number) {
            const el = this.elementFromPoint(x, y);
            if (!el) return null;
            const text = Array.from(el.childNodes).find((n): n is Text => n.nodeType === 3 && /\S/.test((n as Text).data));
            if (!text) return { offsetNode: el, offset: 0 };
            const line = lineOf(text)!;
            return { offsetNode: text, offset: Math.max(0, Math.min(text.data.length, Math.round((x - line.x) / CHAR))) };
          },
        });
      }
      if (highlights) {
        Object.defineProperty(win.CSS, 'highlights', { value: highlights, configurable: true });
        (win as unknown as { Highlight: unknown }).Highlight = class {
          ranges: Range[] = [];
          add(range: Range) {
            this.ranges.push(range);
          }
        };
      }
      if (noSegmenter) Object.defineProperty(win.Intl, 'Segmenter', { value: undefined, configurable: true });
      const rendered = (node: Node): string => {
        if (node.nodeType === 3) return (node as Text).data;
        if (node.nodeType !== 1) return '';
        const inner = Array.from(node.childNodes, rendered).join('');
        return /^(div|p|section|li|ul|h[1-6])$/.test((node as Element).localName) ? `\n${inner}\n` : inner;
      };
      Object.defineProperty(win.HTMLElement.prototype, 'innerText', {
        configurable: true,
        get(this: HTMLElement) { return rendered(this); },
      });
    },
  });
  open.push(w);
  const send = (data: unknown) => {
    const e = new w.Event('message');
    Object.defineProperties(e, {
      source: { value: viewer },
      origin: { value: 'https://app.example.com' },
      data: { value: data },
    });
    w.dispatchEvent(e);
  };
  send({ type: 'page:theme', css: '' });
  send({ type: 'page:comment-mode', on: true });

  // Only a page drawn with glyphs has the caret API.
  const caret = (x: number, y: number) => (glyphs ? w.document.caretPositionFromPoint(x, y) : null);

  /**
   * A primary press dragged from one point to another and released, with the
   * click a browser fires after. On text, the browser selects from the press
   * to the pointer as it goes, unless the press was cancelled or the page
   * made text unselectable.
   */
  const drag = (from: [number, number], to: [number, number], { escape = false } = {}) => {
    sent.length = 0;
    const at = w.document.elementFromPoint(...from) ?? w.document.body;
    const event = (type: string, [x, y]: [number, number]) =>
      new w.MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX: x, clientY: y, detail: 1 });
    at.dispatchEvent(event('pointerdown', from));
    const down = event('mousedown', from);
    at.dispatchEvent(down);
    const base = glyphs && !down.defaultPrevented ? caret(...from) : null;
    const steps = 4;
    for (let i = 1; i <= steps; i++) {
      const point: [number, number] = [from[0] + (to[0] - from[0]) * i / steps, from[1] + (to[1] - from[1]) * i / steps];
      at.dispatchEvent(event('pointermove', point));
      const extent = caret(...point);
      const held = !!w.document.getElementById('__page_area_drag__');
      if (base?.offsetNode.nodeType === 3 && extent?.offsetNode.nodeType === 3 && !held) {
        w.getSelection()!.setBaseAndExtent(base.offsetNode, base.offset, extent.offsetNode, extent.offset);
      }
    }
    if (escape) w.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
    at.dispatchEvent(event('pointerup', to));
    at.dispatchEvent(event('click', to));
    const area = sent.find((m) => m.type === 'page:area') as
      | { press: number; anchor: { label?: string; count: number; point?: unknown; elements: { path?: string; label?: string; quote?: string }[]; rect: Record<string, number> } }
      | undefined;
    const picks = sent.filter((m) => m.type === 'page:pick') as { press?: number; anchor: { path?: string; label?: string; point?: unknown } }[];
    const words = sent.filter((m) => m.type === 'page:selection' && m.compose) as {
      press?: number; rect: Record<string, number>; anchor: { quote: string; label?: string; path?: string; prefix?: string; suffix?: string; point?: unknown };
    }[];
    return { area, picks, words, sent: [...sent], prevented: down.defaultPrevented };
  };

  /** A click that does not move, at a point. */
  const click = (point: [number, number]) => drag(point, point);

  /** The pointer moved to a point, onto whatever is there, and the hover a frame later. */
  const hover = async (point: [number, number]) => {
    const at = w.document.elementFromPoint(...point) ?? w.document.body;
    const event = (type: string) => new w.MouseEvent(type, { bubbles: true, clientX: point[0], clientY: point[1] });
    at.dispatchEvent(event('pointerover'));
    at.dispatchEvent(event('pointermove'));
    await new Promise((done) => setTimeout(done, 50));
    const last = sent.filter((m) => m.type === 'page:hover').pop() as { rect?: Record<string, number>; label?: string; word?: boolean; clear?: boolean };
    const rule = (w.document.getElementById('__page_comment_mode__') as HTMLStyleElement).sheet!.cssRules[0] as CSSStyleRule;
    return { ...last, cursor: rule.style.getPropertyValue('cursor') };
  };
  return { w, sent, send, drag, click, hover };
}

// Four tiles in a row, 100 x 80 with a 10 px gap: a caption, a value and a
// footnote line.
const tile = (i: number, label: string, value: string) => {
  const x = 18 + i * 110;
  return `<div class="tile" data-box="${10 + i * 110} 10 100 80"><div class="t-label" data-box="${x} 18 84 14">${label}</div>`
    + `<div class="t-value" data-box="${x} 36 84 24">${value}</div><div class="t-foot" data-box="${x} 68 84 14">vs last year</div></div>`;
};
const ROW = `<div id="grid">${tile(0, 'Revenue', '$46.7B')}${tile(1, 'Gross margin', '72.4%')}${tile(2, 'Operating income', '$28.4B')}${tile(3, 'Free cash flow', '$13.5B')}</div>`;

describe('the page bridge takes an area dragged around several elements', () => {
  it('holds the tiles the area covers, not the grid most of which it covers too', () => {
    const { drag } = laidOutPage(ROW);
    const { area, picks } = drag([4, 4], [336, 96]);
    expect(picks).toEqual([]);
    expect(area?.anchor.count).toBe(3);
    expect(area?.anchor.label).toBe('Revenue, Gross margin, Operating income');
    expect(area?.anchor.elements.map((e) => e.path)).toEqual([
      '#grid > div:nth-of-type(1)', '#grid > div:nth-of-type(2)', '#grid > div:nth-of-type(3)',
    ]);
    expect(area?.anchor.elements[0]).toMatchObject({ label: 'Revenue', quote: 'Revenue $46.7B vs last year' });
    // Its pin where the drag was let go: past the bottom-right corner, so on it.
    expect(area?.anchor.point).toEqual({ x: 1, y: 1 });
    // The press it began with, as a pick carries its click's.
    expect(area?.press).toBe(1);
  });

  it('keeps a tile whole when its footnote line falls outside, the line being no item of it', () => {
    const { drag } = laidOutPage(ROW);
    // Down to y 70: the caption and value inside, the footnote line (68 to 82) not.
    expect(drag([4, 4], [216, 70]).area?.anchor.elements.map((e) => e.label)).toEqual(['Revenue', 'Gross margin']);
  });

  it('splits a list mostly inside into its items when the area leaves one of them out', () => {
    const items = Array.from({ length: 10 }, (_, i) => `<li data-box="10 ${10 + i * 20} 100 16">Line ${i + 1}</li>`).join('');
    const { area, picks } = laidOutPage(`<ul id="list">${items}</ul>`).drag([4, 4], [120, 190]);
    expect(picks).toEqual([]);
    expect(area?.anchor.count).toBe(9);
  });

  it('takes a list the area holds whole as the list, as a lasso around one element is that element', () => {
    const items = Array.from({ length: 3 }, (_, i) => `<li data-box="10 ${10 + i * 20} 100 16">Line ${i + 1}</li>`).join('');
    const { area, picks } = laidOutPage(`<ul id="list">${items}</ul>`).drag([4, 4], [120, 80]);
    expect(area).toBeUndefined();
    expect(picks.map((p) => p.anchor.path)).toEqual(['#list']);
  });

  it('makes a lasso around one tile that tile’s comment, as a click on it would', () => {
    const { drag } = laidOutPage(ROW);
    const { area, picks } = drag([116, 4], [224, 96]);
    expect(area).toBeUndefined();
    expect(picks).toHaveLength(1);
    expect(picks[0].anchor).toMatchObject({ path: '#grid > div:nth-of-type(2)', quote: 'Gross margin 72.4% vs last year' });
    // Its pin where the drag was let go, the nearest the tile has to it.
    expect(picks[0].anchor.point).toEqual({ x: 1, y: 1 });
  });

  it('picks the element the press was on when the area holds none', () => {
    const { drag } = laidOutPage(ROW);
    const { area, picks } = drag([20, 40], [60, 50]);
    expect(area).toBeUndefined();
    expect(picks.map((p) => p.anchor.path)).toEqual(['#grid > div:nth-of-type(1) > div:nth-of-type(2)']);
  });

  it('names a wrapper’s one child rather than the wrapper, and leaves out spacers and refused elements', () => {
    const body = `<div id="grid"><div class="wrap">${tile(0, 'Revenue', '$46.7B')}</div>`
      + `<div class="spacer" data-box="110 10 10 80"></div>${tile(1, 'Gross margin', '72.4%')}`
      + `<div data-uncommentable data-box="230 10 100 80">Page control</div>${tile(3, 'Free cash flow', '$13.5B')}</div>`;
    const { area } = laidOutPage(body).drag([4, 4], [336, 96]);
    expect(area?.anchor.elements.map((e) => e.path)).toEqual(['#grid > div:nth-of-type(1) > div', '#grid > div:nth-of-type(3)']);
    expect(area?.anchor.count).toBe(2);
  });

  it('takes a spot a page lifts with data-comment-target whole', () => {
    const card = (id: string, x: number, words: string) =>
      `<section id="${id}" data-comment-target data-box="${x} 10 100 80"><p data-box="${x + 4} 14 40 10">${words}</p><p data-box="${x + 4} 30 40 10">Up</p></section>`;
    const body = `<div id="cards">${card('a', 10, 'Revenue')}${card('b', 120, 'Margin')}${card('c', 230, 'Cash')}</div>`;
    const { area } = laidOutPage(body).drag([4, 4], [226, 96]);
    expect(area?.anchor.elements.map((e) => e.path)).toEqual(['#a', '#b']);
  });

  it('keeps the pin where a drag up and to the left was let go', () => {
    const { area } = laidOutPage(ROW).drag([336, 96], [4, 4]);
    expect(area?.anchor.count).toBe(3);
    expect(area?.anchor.point).toEqual({ x: 0, y: 0 });
  });

  it('lists eight elements and counts the rest', () => {
    const many = Array.from({ length: 11 }, (_, i) => `<p data-box="10 ${10 + i * 20} 100 16">Line ${i + 1}</p>`).join('');
    const { area } = laidOutPage(`<div id="list">${many}</div>`).drag([4, 4], [120, 208]);
    expect(area?.anchor.count).toBe(10);
    expect(area?.anchor.elements).toHaveLength(8);
    expect(area?.anchor.label).toBe('Line 1, Line 2, Line 3, Line 4, Line 5, Line 6, Line 7, Line 8, Line 9, Line 10');
  });

  it('reports nothing for a drag called off with Esc, and swallows the click after it', () => {
    const { drag } = laidOutPage(ROW);
    const { area, picks, sent } = drag([4, 4], [336, 96], { escape: true });
    expect(area).toBeUndefined();
    expect(picks).toEqual([]);
    expect(sent.some((m) => m.type === 'page:key')).toBe(false);
  });

  it('keeps a press that never passes the threshold a click', () => {
    const { drag } = laidOutPage(ROW);
    const { area, picks } = drag([30, 40], [33, 42]);
    expect(area).toBeUndefined();
    expect(picks.map((p) => p.anchor.path)).toEqual(['#grid > div:nth-of-type(1) > div:nth-of-type(2)']);
  });

  it('finds an area again where its elements are, and not at all when none is', () => {
    const { w, sent, send } = laidOutPage(ROW);
    const positions = (anchor: Record<string, unknown>) => {
      sent.length = 0;
      send({ type: 'page:anchors', items: [{ id: 'c1', anchor }] });
      const last = sent.filter((m) => m.type === 'page:positions').pop() as { items: { rect: unknown }[] };
      return last.items[0].rect;
    };
    const elements = [
      { path: '#grid > div:nth-of-type(2)', quote: 'Gross margin 72.4%' },
      { path: '#grid > div:nth-of-type(3)', quote: 'Operating income $28.4B' },
    ];
    expect(positions({ kind: 'area', elements, count: 2 })).toEqual({ x: 120, y: 10, width: 210, height: 80 });
    // A tile gone: the area stands where the rest still do.
    w.document.querySelector('#grid > div:nth-of-type(3)')!.remove();
    expect(positions({ kind: 'area', elements, count: 2 })).toEqual({ x: 120, y: 10, width: 100, height: 80 });
    expect(positions({ kind: 'area', elements: [{ path: '#nowhere', quote: 'Nothing like it' }], count: 2 })).toBeNull();
  });
});

// A section heading over a sentence, then the row of tiles under it. Drawn
// with glyphs, "$46.7B" spans x 18 to 54 of its 84 px box, at y 36 to 60.
const SENTENCE = 'Revenue by market platform as presented in the filing';
const SECTION = `<h2 data-box="10 100 300 20">Segment revenue</h2><p id="p" data-box="10 130 600 16">${SENTENCE}</p>`;
/** A point on the left of a character of the sentence, where the caret goes before it. */
const inSentence = (i: number): [number, number] => [10 + i * CHAR + 1, 138];

describe('the page bridge selects words in comment mode', () => {
  it('makes a press on glyphs that drags a selection of the words, opened at once, not an area', () => {
    const { w, drag } = laidOutPage(SECTION + ROW, { glyphs: true });
    const from = SENTENCE.indexOf('rket');
    const to = SENTENCE.indexOf('resented');
    const { area, picks, words, sent, prevented } = drag(inSentence(from), inSentence(to));
    // The browser's own selection: nothing cancelled it.
    expect(prevented).toBe(false);
    expect(w.getSelection()!.toString()).toBe('rket platform as p');
    expect(area).toBeUndefined();
    expect(sent.some((m) => m.type === 'page:area-drag')).toBe(false);
    // Its click, which ended on another word, picks nothing.
    expect(picks).toEqual([]);
    expect(words).toHaveLength(1);
    expect(words[0].press).toBe(1);
    // Whole words, named by the section they are in, the pin where the drag was let go.
    expect(words[0].anchor).toMatchObject({ quote: 'market platform as presented', label: 'Segment revenue', path: '#p' });
    expect(words[0].anchor.prefix?.endsWith('Revenue by')).toBe(true);
    expect(words[0].anchor.suffix?.startsWith('in the filing')).toBe(true);
    const grown = { left: 10 + SENTENCE.indexOf('market') * CHAR, right: 10 + (SENTENCE.indexOf('presented') + 9) * CHAR };
    expect(words[0].anchor.point).toEqual({ x: Math.round(((inSentence(to)[0] - grown.left) / (grown.right - grown.left)) * 1000) / 1000, y: 0.5 });
  });

  it('draws an area from a press off glyphs, as before', () => {
    const { drag } = laidOutPage(SECTION + ROW, { glyphs: true });
    const { area, words } = drag([4, 4], [336, 96]);
    expect(words).toEqual([]);
    expect(area?.anchor.count).toBe(3);
  });

  it('takes the word a click on glyphs is on, named by its tile, and the element a click off them is on', () => {
    const { click } = laidOutPage(SECTION + ROW, { glyphs: true });
    const on = click([32, 48]);
    expect(on.picks).toEqual([]);
    expect(on.words).toHaveLength(1);
    expect(on.words[0].press).toBe(1);
    // A sign belongs to the number beside it; the pin stands where the click was.
    expect(on.words[0].anchor).toMatchObject({ quote: '$46.7B', label: 'Revenue', path: '#grid > div:nth-of-type(1) > div:nth-of-type(2)' });
    expect(on.words[0].anchor.point).toEqual({ x: Math.round((14 / 36) * 1000) / 1000, y: 0.5 });
    // In the value's box, right of its glyphs.
    const off = click([80, 48]);
    expect(off.words).toEqual([]);
    expect(off.picks.map((p) => [p.press, p.anchor.path])).toEqual([[2, '#grid > div:nth-of-type(1) > div:nth-of-type(2)']]);
  });

  it('picks the element a click on glyphs is on once the switch says element', () => {
    const source = BRIDGE.replace("var TEXT_CLICK = 'word';", "var TEXT_CLICK = 'element';");
    expect(source).not.toBe(BRIDGE);
    const { click } = laidOutPage(SECTION + ROW, { glyphs: true, source });
    const { words, picks } = click([32, 48]);
    expect(words).toEqual([]);
    expect(picks.map((p) => p.anchor.path)).toEqual(['#grid > div:nth-of-type(1) > div:nth-of-type(2)']);
  });

  it('pairs each selection with its own press, as a pick is', () => {
    const { drag, click, sent } = laidOutPage(SECTION + ROW, { glyphs: true });
    const presses: unknown[] = [];
    const first = drag(inSentence(2), inSentence(20));
    presses.push(...first.sent.filter((m) => m.type === 'page:press').map((m) => m.press));
    const second = click(inSentence(30));
    presses.push(...sent.filter((m) => m.type === 'page:press').map((m) => m.press));
    const third = click([4, 4]);
    expect(presses).toEqual([1, 2]);
    expect(first.words.map((m) => m.press)).toEqual([1]);
    expect(second.words.map((m) => [m.press, m.anchor.quote])).toEqual([[2, 'presented']]);
    // A click on no element and no glyphs: a press, and nothing picked.
    expect(third.sent.map((m) => m.type)).toEqual(['page:press']);
  });

  it('lets go of a selection called off with Esc, and opens nothing at the release', () => {
    const { w, drag } = laidOutPage(SECTION + ROW, { glyphs: true });
    const { words, picks, sent } = drag(inSentence(2), inSentence(30), { escape: true });
    expect(words).toEqual([]);
    expect(picks).toEqual([]);
    expect(sent.some((m) => m.type === 'page:key')).toBe(false);
    expect(w.getSelection()!.toString()).toBe('');
    // The page can be selected again after the release.
    expect(w.document.getElementById('__page_area_drag__')).toBeNull();
  });

  it('keeps a multi-click selecting nothing, the first click having taken its word', () => {
    const { w, sent } = laidOutPage(SECTION + ROW, { glyphs: true });
    const el = w.document.getElementById('p')!;
    const [x, y] = inSentence(14);
    const event = (type: string, detail: number) =>
      new w.MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX: x, clientY: y, detail });
    const prevented: boolean[] = [];
    for (const detail of [1, 2]) {
      el.dispatchEvent(event('pointerdown', 0));
      const down = event('mousedown', detail);
      el.dispatchEvent(down);
      prevented.push(down.defaultPrevented);
      el.dispatchEvent(event('pointerup', detail));
      el.dispatchEvent(event('click', detail));
    }
    expect(prevented).toEqual([false, true]);
    expect(sent.filter((m) => m.type === 'page:selection' && m.compose).map((m) => (m.anchor as { quote: string }).quote)).toEqual(['market']);
    expect(sent.filter((m) => m.type === 'page:press')).toHaveLength(1);
  });
});

describe('the page bridge hovers words in comment mode', () => {
  it('boxes the word under the pointer with the I-beam over glyphs, and outlines the element off them', async () => {
    const { hover } = laidOutPage(SECTION + ROW, { glyphs: true });
    expect(await hover([32, 48])).toMatchObject({ word: true, rect: { x: 18, y: 36, width: 36, height: 24 }, cursor: 'text' });
    const off = await hover([80, 48]);
    expect(off).toMatchObject({ label: '$46.7B', cursor: 'crosshair' });
    expect(off.word).toBeUndefined();
    // Over a space, the word on the pointer's side of it.
    const space = SENTENCE.indexOf(' market');
    expect(await hover([10 + space * CHAR + 1, 138])).toMatchObject({ word: true, rect: { x: 10 + (space - 2) * CHAR, width: 2 * CHAR } });
    expect(await hover([10 + space * CHAR + 5, 138])).toMatchObject({ word: true, rect: { x: 10 + (space + 1) * CHAR, width: 6 * CHAR } });
  });

  it('treats a control’s text and text made unselectable as the element’s, not words', async () => {
    const body = '<button id="b" data-box="10 10 60 20">Buy now</button><p id="n" style="user-select:none" data-box="10 40 200 16">Fixed caption</p>';
    const { hover, click } = laidOutPage(body, { glyphs: true });
    expect(await hover([14, 20])).toMatchObject({ label: 'Buy now', cursor: 'crosshair' });
    expect(await hover([14, 48])).toMatchObject({ label: 'Fixed caption', cursor: 'crosshair' });
    const { words, picks } = click([14, 20]);
    expect(words).toEqual([]);
    expect(picks.map((p) => p.anchor.path)).toEqual(['#b']);
  });

  it('finds words in Chinese with the segmenter, and between spaces without one', () => {
    const han = '<p id="h" data-box="10 10 200 16">数据中心收入增长</p>';
    expect(laidOutPage(han, { glyphs: true }).click([10 + 1 * CHAR + 2, 18]).words[0]?.anchor.quote).toBe('数据');
    const { click } = laidOutPage(SECTION + ROW, { glyphs: true, noSegmenter: true });
    expect(click([32, 48]).words[0]?.anchor.quote).toBe('$46.7B');
    expect(click(inSentence(SENTENCE.indexOf('by'))).words[0]?.anchor.quote).toBe('by');
  });
});

describe('the page bridge spaces a label where the page draws a gap', () => {
  // A tile's value on one line, written with no space: "$59.69" at 27 px,
  // its unit 2 px after it at 15 px, and a side note 9 px after the unit at
  // 12 px, set apart by margins alone.
  const TILE = '<div id="tile" data-box="10 10 200 80"><div data-box="18 18 84 14">Net income</div>'
    + '<div id="v" data-box="18 36 184 24" style="font-size:27px">$59.69'
    + '<span data-box="56 42 6 16" style="font-size:15px">B</span>'
    + '<span data-box="71 44 72 12" style="font-size:12px">62.0% margin</span></div></div>';

  it('puts a space at a gap that reads as one, and keeps a tight unit on its number', async () => {
    const { click, hover } = laidOutPage(TILE, { glyphs: true });
    // In the value's box, right of its glyphs.
    const { picks } = click([170, 38]);
    expect(picks).toHaveLength(1);
    expect(picks[0].anchor).toMatchObject({ path: '#v', label: '$59.69B 62.0% margin', quote: '$59.69B 62.0% margin' });
    // Off every line of the tile: the tile, its lines still apart.
    expect(await hover([180, 84])).toMatchObject({ label: 'Net income $59.69B 62.0% margin' });
  });
});

describe('the page bridge marks the words a comment is being written on', () => {
  const marked = (highlights: Map<string, { ranges: Range[] }>) =>
    [...highlights.values()].flatMap((h) => h.ranges.map((r) => r.toString()));

  it('marks picked words at once, lets the selection go, and leaves the mark to the draft the viewer opens', async () => {
    const highlights = new Map<string, { ranges: Range[] }>();
    const { w, drag, send } = laidOutPage(SECTION + ROW, { glyphs: true, highlights });
    const { words } = drag(inSentence(SENTENCE.indexOf('rket')), inSentence(SENTENCE.indexOf('resented')));
    expect(marked(highlights)).toEqual(['market platform as presented']);
    // The selection greys out once the box takes the focus; the mark does not.
    await new Promise((done) => setTimeout(done, 10));
    expect(w.getSelection()!.toString()).toBe('');
    send({ type: 'page:anchors', items: [{ id: '__draft__', anchor: words[0].anchor, mark: true }] });
    expect(marked(highlights)).toEqual(['market platform as presented']);
    // The draft gone: so is the mark.
    send({ type: 'page:anchors', items: [] });
    expect(marked(highlights)).toEqual([]);
  });

  it('drops the mark of words the viewer opened nothing on', () => {
    const highlights = new Map<string, { ranges: Range[] }>();
    const { click, send } = laidOutPage(SECTION + ROW, { glyphs: true, highlights });
    click([32, 48]);
    expect(marked(highlights)).toEqual(['$46.7B']);
    send({ type: 'page:clear-selection' });
    expect(marked(highlights)).toEqual([]);
  });

  it('keeps the selection instead where the browser cannot mark words', () => {
    const { w, click } = laidOutPage(SECTION + ROW, { glyphs: true });
    click([32, 48]);
    expect(w.getSelection()!.toString()).toBe('$46.7B');
  });
});

describe('the page bridge carries its view to the next file', () => {
  // A link under the view's prefix gets the document's inject value before
  // the browser follows it, so the next file arrives bridged; anywhere else
  // it is left as written.
  function linked(url: string) {
    const viewer = { postMessage: () => {} };
    const { window: w } = new JSDOM(
      `<!doctype html><html><head><script>${BRIDGE}</script></head><body>`
        + '<a id="sib" href="charts/next.html#q2">next</a><a id="out" href="/elsewhere/x.html">out</a></body></html>',
      {
        url,
        runScripts: 'dangerously',
        pretendToBeVisual: true,
        beforeParse(win) { Object.defineProperty(win, 'parent', { value: viewer, configurable: true }); },
      },
    );
    open.push(w);
    const follow = (id: string) => {
      const link = w.document.getElementById(id)!;
      // Cancelled here, so jsdom does not try to navigate.
      link.addEventListener('click', (e) => e.preventDefault());
      link.dispatchEvent(new w.MouseEvent('click', { bubbles: true, cancelable: true }));
      return link.getAttribute('href');
    };
    return follow;
  }

  it.each([
    ['the owner\'s file grant', 'https://app.example.com/api/v1/wsfiles/g/tok/reports/index.html?inject=comments', 'comments'],
    ['a shared thread\'s serve route', 'https://app.example.com/api/v1/public/shared/tok/files/serve/reports/index.html?inject=view', 'view'],
  ])('under %s', (_where, url, mode) => {
    const follow = linked(url);
    const sib = new URL(follow('sib')!);
    expect(sib.searchParams.get('inject')).toBe(mode);
    expect(sib.hash).toBe('#q2');
    expect(follow('out')).toBe('/elsewhere/x.html');
  });
});

// A file as the owner's grant serves it, with what the browser would report it
// loading under the test's control: jsdom keeps no resource timing.
const GRANT = 'https://api.example.com/api/v1/wsfiles/g/';
function served(url: string, body = '', prepare?: (win: PageWindow) => void) {
  const sent: Record<string, unknown>[] = [];
  const viewer = { postMessage: (message: unknown) => sent.push(JSON.parse(JSON.stringify(message))) };
  const entries = (names: string[]) => names.map((name) => ({ name }));
  const timing: { report: ((list: { getEntries: () => { name: string }[] }) => void) | null; waiting: string[] } = {
    report: null,
    waiting: [],
  };
  const { window: w } = new JSDOM(`<!doctype html><html><head><script>${BRIDGE}</script></head><body>${body}</body></html>`, {
    url,
    runScripts: 'dangerously',
    pretendToBeVisual: true,
    beforeParse(win) {
      Object.defineProperty(win, 'parent', { value: viewer, configurable: true });
      Object.defineProperty(win, 'PerformanceObserver', {
        configurable: true,
        value: class {
          constructor(report: NonNullable<typeof timing.report>) { timing.report = report; }
          observe() {}
          takeRecords() { return entries(timing.waiting.splice(0)); }
        },
      });
      prepare?.(win);
    },
  });
  open.push(w);
  const from = (source: unknown, data: unknown) => {
    const e = new w.Event('message');
    Object.defineProperties(e, {
      source: { value: source },
      origin: { value: 'https://app.example.com' },
      data: { value: data },
    });
    w.dispatchEvent(e);
  };
  return {
    w,
    viewer,
    sent,
    timing,
    from,
    // The viewer names itself with its first message; nothing is sent before.
    speak: () => from(viewer, { type: 'page:theme', css: '' }),
    loaded: (...names: string[]) => timing.report?.({ getEntries: () => entries(names) }),
    of: (type: string) => sent.filter((message) => message.type === type),
  };
}

describe('the page bridge carries the view on a form to its own files', () => {
  const send = (page: ReturnType<typeof served>, id: string) => {
    const form = page.w.document.getElementById(id) as HTMLFormElement;
    form.dispatchEvent(new page.w.Event('submit', { bubbles: true, cancelable: true }));
    return form;
  };
  const view = (form: HTMLFormElement) => (form.elements.namedItem('inject') as HTMLInputElement | null)?.value ?? null;

  it('as a field on a GET form here, and leaves the rest as written', () => {
    const page = served(
      `${GRANT}tok/reports/index.html?inject=comments`,
      '<form id="here" action="details.html"><input name="q"></form>'
        + '<form id="self"></form>'
        + '<form id="post" method="post" action="details.html"></form>'
        + '<form id="away" action="https://elsewhere.example.com/s"></form>',
    );
    expect(view(send(page, 'here'))).toBe('comments');
    expect(view(send(page, 'self'))).toBe('comments');
    expect(view(send(page, 'post'))).toBeNull();
    const away = send(page, 'away');
    expect(view(away)).toBeNull();
    expect(away.getAttribute('target')).toBe('_blank');
  });

  it('as the button that sent it says, over what its form says', () => {
    const page = served(
      `${GRANT}tok/reports/index.html?inject=comments`,
      '<form id="toget" method="post"><button id="g" formmethod="get" formaction="details.html"></button></form>'
        + '<form id="topost" action="details.html"><button id="p" formmethod="post"></button></form>'
        + '<form id="toaway" action="details.html"><button id="a" formaction="https://elsewhere.example.com/s"></button></form>',
    );
    const by = (formId: string, buttonId: string) => {
      const form = page.w.document.getElementById(formId) as HTMLFormElement;
      const submitter = page.w.document.getElementById(buttonId) as HTMLButtonElement;
      form.dispatchEvent(new page.w.SubmitEvent('submit', { bubbles: true, cancelable: true, submitter }));
      return { form, submitter };
    };
    expect(view(by('toget', 'g').form)).toBe('comments');
    expect(view(by('topost', 'p').form)).toBeNull();
    const away = by('toaway', 'a');
    expect(view(away.form)).toBeNull();
    expect(away.submitter.getAttribute('formtarget')).toBe('_blank');
    expect(away.form.hasAttribute('target')).toBe(false);
  });

  it('after a field of the page\'s own by the same name, where the server reads it', () => {
    const page = served(
      `${GRANT}tok/reports/index.html?inject=comments`,
      '<form id="own" action="details.html"><input name="inject" value="mine"></form>',
    );
    const form = send(page, 'own');
    send(page, 'own');
    const fields = [...form.querySelectorAll('input[name="inject"]')] as HTMLInputElement[];
    expect(fields.map((f) => f.value)).toEqual(['mine', 'comments']);
  });

  it('reads the view from the last of its address\'s values, as the server does', () => {
    const page = served(
      `${GRANT}tok/reports/index.html?inject=mine&inject=comments`,
      '<form id="here" action="details.html"></form>',
    );
    expect(view(send(page, 'here'))).toBe('comments');
  });

  it('to this frame by name, and leaves a form for another window as written', () => {
    const page = served(
      `${GRANT}tok/reports/index.html?inject=comments`,
      '<form id="self" target="_self" action="details.html"></form>'
        + '<form id="byself" target="pane"><button id="s" formtarget="_self" formaction="details.html"></button></form>'
        + '<form id="pane" target="pane" action="details.html"></form>',
    );
    expect(view(send(page, 'self'))).toBe('comments');
    const byself = page.w.document.getElementById('byself') as HTMLFormElement;
    const submitter = page.w.document.getElementById('s') as HTMLButtonElement;
    byself.dispatchEvent(new page.w.SubmitEvent('submit', { bubbles: true, cancelable: true, submitter }));
    expect(view(byself)).toBe('comments');
    expect(view(send(page, 'pane'))).toBeNull();
  });
});

describe('the page bridge hears only windows tied to the page', () => {
  function heardBy(page: ReturnType<typeof served>, source: unknown) {
    const heard: unknown[] = [];
    const listener = (e: Event) => heard.push((e as MessageEvent).data);
    // Capturing, the earliest a page's own listener can be.
    page.w.addEventListener('message', listener, true);
    page.from(source, { hello: 'page' });
    page.w.removeEventListener('message', listener, true);
    return heard.length > 0;
  }

  it('drops a stranger, a window with no tie to it, and a message with no source', () => {
    const page = served(`${GRANT}tok/reports/index.html`);
    const stranger = { parent: null, opener: null };
    const strangersChild = { parent: stranger, opener: null };
    expect(heardBy(page, stranger)).toBe(false);
    expect(heardBy(page, strangersChild)).toBe(false);
    expect(heardBy(page, null)).toBe(false);
  });

  it('hears itself, the viewer, a frame inside it and a window it opened', () => {
    const page = served(`${GRANT}tok/reports/index.html`);
    const opened: { parent: unknown; opener: unknown } = { parent: null, opener: page.w };
    opened.parent = opened;
    expect(heardBy(page, page.w)).toBe(true);
    expect(heardBy(page, page.viewer)).toBe(true);
    expect(heardBy(page, { parent: page.w })).toBe(true);
    expect(heardBy(page, opened)).toBe(true);
  });
});

describe('the page bridge reports what the page loads', () => {
  const GRANTED = `${GRANT}tok/`;

  it('holds the report until the document settles, naming the file it is', async () => {
    const page = served(`${GRANTED}reports/index.html?inject=view`);
    page.speak();
    // Its own file and another site's are no workspace file it loaded.
    page.loaded(`${GRANTED}data/a.csv`, `${GRANTED}reports/index.html`, 'https://cdn.example.com/lib.js');
    expect(page.of('page:loads')).toEqual([]);

    // What the browser has not handed over yet still goes in the settled report.
    page.timing.waiting.push(`${GRANTED}data/b%20c.csv`);
    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads')).toEqual([
      { type: 'page:loads', v: 1, path: 'reports/index.html', paths: ['data/a.csv', 'data/b c.csv'] },
    ]);
    const types = page.sent.map((message) => message.type);
    expect(types.indexOf('page:loads')).toBeLessThan(types.indexOf('page:settled'));

    // From then on a file only a click loaded goes out as it arrives.
    page.loaded(`${GRANTED}data/d.csv`);
    expect(page.of('page:loads').pop()?.paths).toEqual(['data/a.csv', 'data/b c.csv', 'data/d.csv']);
    page.loaded(`${GRANTED}data/d.csv`);
    expect(page.of('page:loads')).toHaveLength(2);
  }, 15_000);

  // No view of a workspace file serves the root of its host, so the review
  // warns of such a load by its path there, "/" first, never a file's.
  it('names a load from the root of its own host by its path there, and no other host\'s', async () => {
    const page = served(`${GRANTED}reports/index.html?inject=view`);
    page.speak();
    page.loaded(
      'https://api.example.com/assets/dark.json?v=2#top',
      `${GRANTED}data/a.csv`,
      'https://api.example.com/assets/a%20b.json',
      'http://api.example.com/assets/other-scheme.json',
      'https://api.example.com:8443/assets/other-port.json',
      'https://cdn.example.com/assets/lib.json',
    );
    // As a frame of the page's, or its worker, would send it.
    page.from({ parent: page.w }, { type: 'page:frame-loads', urls: ['https://api.example.com/fonts/x.woff2'] });
    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads').pop()?.paths).toEqual([
      '/assets/dark.json', 'data/a.csv', '/assets/a b.json', '/fonts/x.woff2',
    ]);
  }, 15_000);

  it('leaves out what the routes that serve files answer, another grant\'s included', async () => {
    const page = served(`${GRANTED}reports/index.html?inject=view`);
    page.speak();
    page.loaded(
      `${GRANT}other/data/b.csv`,
      'https://api.example.com/api/v1/public/shared/tok/files/serve/data/c.csv',
      'https://api.example.com/api/v1/artifacts/g/tok/v/3/data/d.csv',
      `${GRANTED}reports/index.html`,
      `${GRANTED}`,
    );
    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads')).toEqual([]);
  }, 15_000);

  it('watches nothing on a share, which has no workspace to review', () => {
    const page = served('https://api.example.com/api/v1/public/shared/tok/files/serve/reports/index.html?inject=view');
    expect(page.timing.report).toBeNull();
  });
});

describe('the page bridge moves to a renewed grant', () => {
  const at = (grant: string) => `${GRANT}${grant}/reports/index.html?inject=view`;

  it('takes the new address without loading again, and reads links under it', () => {
    const page = served(at('old'), '<a id="next" href="charts/next.html#q2">next</a>');
    page.speak();
    page.from(page.viewer, { type: 'page:address', src: at('new') });
    expect(page.w.location.href).toBe(at('new'));

    const next = page.w.document.getElementById('next')!;
    next.dispatchEvent(new page.w.MouseEvent('click', { bubbles: true, cancelable: true }));
    expect(page.of('page:open-file')).toEqual([
      { type: 'page:open-file', v: 1, path: 'reports/charts/next.html', fragment: 'q2' },
    ]);
  });

  it('leaves a link that carries a query to the frame, where the page reads it', () => {
    const page = served(at('g'), '<a id="detail" href="detail.html?ticker=MSFT#q2">MSFT</a>');
    page.speak();
    const detail = page.w.document.getElementById('detail')!;
    const click = new page.w.MouseEvent('click', { bubbles: true, cancelable: true });
    detail.dispatchEvent(click);
    expect(click.defaultPrevented).toBe(false);
    expect(page.of('page:open-file')).toEqual([]);
  });

  it('opens the same host over another scheme as another site, in a tab of its own', () => {
    const page = served(at('g'), '<a id="plain" href="http://api.example.com/api/v1/wsfiles/g/g/reports/a.html">a</a>');
    page.speak();
    const opened: unknown[][] = [];
    page.w.open = ((...args: unknown[]) => { opened.push(args); return null; }) as typeof page.w.open;
    const click = new page.w.MouseEvent('click', { bubbles: true, cancelable: true });
    page.w.document.getElementById('plain')!.dispatchEvent(click);
    expect(click.defaultPrevented).toBe(true);
    expect(opened).toEqual([['http://api.example.com/api/v1/wsfiles/g/g/reports/a.html', '_blank', 'noopener,noreferrer']]);
    expect(page.of('page:open-file')).toEqual([]);
  });

  it("moves a published version's page to its renewed grant, on either origin", () => {
    for (const base of ['https://api.example.com/api/v1/artifacts/g/', 'https://artifact-x.example.com/g/']) {
      const version = (grant: string) => `${base}${grant}/v/3/reports/index.html?inject=view`;
      const page = served(version('old'));
      page.speak();
      page.from(page.viewer, { type: 'page:address', src: version('new') });
      expect(page.w.location.href).toBe(version('new'));
    }
  });

  it('stays put for an address under the grant it already has', () => {
    const page = served(at('same'));
    page.speak();
    page.from(page.viewer, { type: 'page:address', src: `${GRANT}same/elsewhere.html` });
    expect(page.w.location.href).toBe(at('same'));
  });
});

describe('the page bridge finds what a comment was stored on', () => {
  it('knows an image by its alt, which its text does not hold', () => {
    const anchor = { path: 'body > img:nth-of-type(1)', quote: 'Revenue by quarter' };
    expect(positions('<img src="a.png" alt="Revenue by quarter">', anchor)).not.toBeNull();
    expect(positions('<img src="a.png" alt="Revenue by quarter, FY27">', anchor)).not.toBeNull();
    expect(positions('<img src="a.png" alt="Team photo">', anchor)).toBeNull();
  });

  it('finds a selection stored cut by its opening words', () => {
    const words = 'word '.repeat(500).trim();
    expect(positions(`<p>${words}</p>`, { kind: 'text', quote: `${words.slice(0, 1999)}…` })).not.toBeNull();
    // A quote that only ends in an ellipsis is still searched as it stands.
    expect(positions('<p>wait for it</p>', { kind: 'text', quote: 'wait…' })).toBeNull();
  });
});

// jsdom serves every document on its URL's origin; the browser puts a served
// file on an opaque one.
function opaque(win: PageWindow) {
  Object.defineProperty(win, 'origin', { value: 'null', configurable: true });
}

type Posted = { data: unknown; ports: unknown[] };
type FakeWorker = EventTarget & {
  url: string;
  options?: WorkerOptions;
  posted: Posted[];
  deliver: ((message: Posted) => void) | null;
};
// jsdom has no Worker: this one keeps the blob the stand-in starts it from, and
// its onmessage listens from the first time it is set, as a browser's does.
// What the page sends it waits, as a browser holds it, until the worker runs.
function stubWorkers(win: PageWindow, blobs: Blob[], workers: FakeWorker[]) {
  win.URL.createObjectURL = (blob: Blob) => {
    blobs.push(blob);
    return `blob:null/${blobs.length}`;
  };
  class Worker extends (win.EventTarget as typeof EventTarget) {
    url: string;
    options?: WorkerOptions;
    handler: ((e: Event) => void) | null = null;
    listening = false;
    posted: Posted[] = [];
    deliver: ((message: Posted) => void) | null = null;
    constructor(url: string, options?: WorkerOptions) {
      super();
      this.url = url;
      this.options = options;
      workers.push(this);
    }
    get onmessage() { return this.handler; }
    set onmessage(fn: ((e: Event) => void) | null) {
      if (!this.listening) this.addEventListener('message', (e) => this.handler?.(e));
      this.listening = true;
      this.handler = fn;
    }
    terminate() {}
    postMessage(data: unknown, ports: unknown[] = []) {
      if (this.deliver) this.deliver({ data, ports });
      else this.posted.push({ data, ports });
    }
  }
  Object.defineProperty(win, 'Worker', { value: Worker, configurable: true, writable: true });
}

// The blob's source run against a global of the test's, as the worker would run
// it, its messages arriving at the worker the page holds. `scripts` are what
// importScripts runs, by address.
async function runWorker(
  w: PageWindow, blob: Blob, worker: FakeWorker, scripts: Record<string, (self: any) => void> = {},
) {
  const source = await blob.text();
  const fetched: string[] = [];
  const imported: string[] = [];
  const opened: string[] = [];
  let report: ((list: { getEntries: () => { name: string }[] }) => void) | null = null;
  const scope = Object.assign(new w.EventTarget(), {
    postMessage: (data: unknown) => worker.dispatchEvent(new w.MessageEvent('message', { data })),
    fetch: (input: unknown) => {
      fetched.push(String(input));
      return Promise.resolve({ clone: () => ({ arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)) }) });
    },
    importScripts: (...urls: string[]) => {
      imported.push(...urls);
      urls.forEach((url) => scripts[url]?.(scope));
    },
    Request: class { url: string; constructor(input: unknown) { this.url = String(input); } },
    XMLHttpRequest: class {
      open(_method: string, url: string) { opened.push(url); }
      send() {}
      addEventListener() {}
      removeEventListener() {}
    },
    PerformanceObserver: class {
      constructor(callback: NonNullable<typeof report>) { report = callback; }
      observe() {}
    },
    // jsdom's takes no ports of Node's channel.
    MessageEvent: class extends w.Event {
      data: unknown;
      ports: unknown[];
      constructor(type: string, init: { data: unknown; ports: unknown[] }) {
        super(type);
        this.data = init.data;
        this.ports = init.ports;
      }
    },
  });
  worker.deliver = ({ data, ports }) => {
    const event = new w.Event('message');
    Object.defineProperties(event, { data: { value: data }, ports: { value: ports } });
    scope.dispatchEvent(event);
  };
  try {
    new Function('self', source)(scope);
  } finally {
    worker.posted.splice(0).forEach(worker.deliver);
  }
  const loaded = (...names: string[]) => report?.({ getEntries: () => names.map((name) => ({ name })) });
  return { scope, fetched, imported, opened, loaded };
}

describe('the page bridge starts a worker on an opaque origin', () => {
  const GRANTED = `${GRANT}tok/`;
  const BODY = '<script>window.heard = []; window.worker = new Worker("work/w.js");'
    + ' worker.onmessage = function (e) { heard.push(e.data); };</script>';
  function withWorker(body = BODY) {
    const blobs: Blob[] = [];
    const workers: FakeWorker[] = [];
    const page = served(`${GRANTED}reports/index.html?inject=view`, body, (win) => {
      opaque(win);
      stubWorkers(win, blobs, workers);
    });
    return { page, blobs, workers };
  }

  it('resolves what the worker asks for against its script, not the blob', async () => {
    const { page, blobs, workers } = withWorker();
    expect(workers[0].url).toMatch(/^blob:/);
    const run = await runWorker(page.w, blobs[0], workers[0]);
    expect(run.imported).toEqual([`${GRANTED}reports/work/w.js`]);

    run.scope.fetch('data/x.csv');
    run.scope.importScripts('lib/util.js');
    new run.scope.XMLHttpRequest().open('GET', '../table.json');
    expect(run.fetched).toEqual([`${GRANTED}reports/work/data/x.csv`]);
    expect(run.imported[1]).toBe(`${GRANTED}reports/work/lib/util.js`);
    expect(run.opened).toEqual([`${GRANTED}reports/table.json`]);
    expect(new run.scope.Request('q.csv').url).toBe(`${GRANTED}reports/work/q.csv`);
    // Its location is the script's, as a real worker's is, for a path built on it.
    const place = (run.scope as unknown as { location: URL }).location;
    expect(new URL('data/y.csv', place).href).toBe(`${GRANTED}reports/work/data/y.csv`);
    expect(String(place)).toBe(`${GRANTED}reports/work/w.js`);
    // An address of another site is left as written.
    run.scope.fetch('https://cdn.example.com/data.json');
    expect(run.fetched[1]).toBe('https://cdn.example.com/data.json');
  });

  it('starts a module worker as a classic one that imports it, which every engine allows', async () => {
    const { blobs, workers } = withWorker('<script>new Worker("work/m.js", { type: "module", name: "rows" });</script>');
    expect(workers[0].options).toEqual({ type: 'classic', name: 'rows' });
    // The script, this document's tag, that it is a module, and no shared one.
    const source = await blobs[0].text();
    expect(source).toMatch(/\)\("[^"]+\/reports\/work\/m\.js", "__page_worker_\w+", true, false\);$/);
  });

  it('settles once the worker has run, with what it loaded, which the page never hears', async () => {
    const { page, blobs, workers } = withWorker();
    page.speak();
    // Held while the worker's script has yet to run, as a request out is.
    await new Promise((done) => setTimeout(done, 600));
    expect(page.of('page:settled')).toEqual([]);

    const run = await runWorker(page.w, blobs[0], workers[0]);
    run.loaded(`${GRANTED}data/w.csv`);
    // The page's own messages still reach it.
    run.scope.postMessage({ rows: 3 });
    expect((page.w as unknown as { heard: unknown[] }).heard).toEqual([{ rows: 3 }]);

    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads').pop()?.paths).toEqual(['reports/work/w.js', 'data/w.csv']);
  }, 15_000);
});

describe('the page bridge starts a shared worker on an opaque origin', () => {
  const GRANTED = `${GRANT}tok/`;
  const SCRIPT = `${GRANTED}reports/work/s.js`;
  const channels: MessageChannel[] = [];
  afterEach(() => channels.splice(0).forEach((channel) => {
    channel.port1.close();
    channel.port2.close();
  }));
  function withShared(body: string) {
    const blobs: Blob[] = [];
    const workers: FakeWorker[] = [];
    const page = served(`${GRANTED}reports/index.html?inject=view`, body, (win) => {
      opaque(win);
      stubWorkers(win, blobs, workers);
      // What a browser offers there, refusing the page's script.
      Object.defineProperty(win, 'SharedWorker', {
        configurable: true,
        writable: true,
        value: class { constructor() { throw new Error('SecurityError'); } },
      });
      // jsdom has no channel; Node's is the browser's shape.
      Object.defineProperty(win, 'MessageChannel', {
        configurable: true,
        value: class extends MessageChannel {
          constructor() {
            super();
            channels.push(this);
          }
        },
      });
    });
    return { page, blobs, workers };
  }
  // A script that answers each connection on its port, and says how many it has.
  const answering = (self: any) => {
    let count = 0;
    self.onconnect = (event: MessageEvent) => {
      const port = event.ports[0];
      count += 1;
      port.onmessage = (message: MessageEvent) => port.postMessage(`${message.data} ${count}`);
    };
  };
  const reply = (port: MessagePort, data: string) => new Promise((done) => {
    port.onmessage = (message) => done(message.data);
    port.postMessage(data);
  });

  it("connects every construction for its script and name to one worker, each on a port of its own", async () => {
    const { page, blobs, workers } = withShared(
      '<script>window.a = new SharedWorker("work/s.js", "rows");'
        + ' window.b = new SharedWorker("work/s.js", { name: "rows" });'
        + ' window.c = new SharedWorker("work/s.js");</script>',
    );
    const win = page.w as unknown as { a: SharedWorker; b: SharedWorker; c: SharedWorker };
    expect(win.a).toBeInstanceOf(page.w.SharedWorker);
    expect(win.a).toBeInstanceOf(page.w.EventTarget);
    // One worker per name, each a classic one from a blob.
    expect(workers.map((worker) => worker.options)).toEqual([
      { name: 'rows', type: 'classic' },
      { type: 'classic' },
    ]);
    const source = await blobs[0].text();
    expect(source).toMatch(/\)\("[^"]+\/reports\/work\/s\.js", "__page_worker_\w+", false, true\);$/);

    // Connected before the script ran, heard once it has.
    const run = await runWorker(page.w, blobs[0], workers[0], { [SCRIPT]: answering });
    expect(run.imported).toEqual([SCRIPT]);
    expect(await reply(win.a.port, 'ping')).toBe('ping 2');
    expect(await reply(win.b.port, 'pong')).toBe('pong 2');

    // One more on a worker already running connects at once, by listener too.
    const connected: unknown[] = [];
    run.scope.addEventListener('connect', (event: Event) => connected.push((event as MessageEvent).ports.length));
    const d = new page.w.SharedWorker('work/s.js', 'rows');
    expect(workers).toHaveLength(2);
    expect(connected).toEqual([1]);
    expect(await reply(d.port, 'again')).toBe('again 3');
  });

  it('settles once its script has run, with what it loaded, and its messages are the page\'s own', async () => {
    const { page, blobs, workers } = withShared('<script>window.shared = new SharedWorker("work/s.js");</script>');
    page.speak();
    await new Promise((done) => setTimeout(done, 600));
    expect(page.of('page:settled')).toEqual([]);

    const run = await runWorker(page.w, blobs[0], workers[0], { [SCRIPT]: answering });
    run.loaded(`${GRANTED}data/s.csv`);
    run.scope.fetch('rows.json');
    expect(run.fetched).toEqual([`${GRANTED}reports/work/rows.json`]);
    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads').pop()?.paths).toEqual(['reports/work/s.js', 'data/s.csv']);
  }, 15_000);

  it('is an error to every connection when its script never runs, and starts again on the next', async () => {
    const { page, blobs, workers } = withShared(
      '<script>window.errors = []; window.a = new SharedWorker("work/s.js");'
        + ' a.onerror = function (e) { errors.push("a " + e.type); };'
        + ' a.addEventListener("error", function () { errors.push("a listener"); });'
        + ' window.b = new SharedWorker("work/s.js");'
        + ' b.onerror = function () { errors.push("b"); };</script>',
    );
    const missing = () => { throw new Error('NetworkError'); };
    await expect(runWorker(page.w, blobs[0], workers[0], { [SCRIPT]: missing })).rejects.toThrow('NetworkError');
    expect((page.w as unknown as { errors: string[] }).errors).toEqual(['a error', 'a listener', 'b']);
    new page.w.SharedWorker('work/s.js');
    expect(workers).toHaveLength(2);
  });
});

describe('the page bridge takes in what a frame of its own loaded', () => {
  const GRANTED = `${GRANT}tok/`;

  it('from a frame whose parent it is, out of the page\'s hearing, and from no one else', async () => {
    const page = served(`${GRANTED}reports/index.html?inject=view`);
    page.speak();
    const heard: unknown[] = [];
    page.w.addEventListener('message', (e) => heard.push((e as MessageEvent).data), true);
    const report = (source: unknown, file: string) => {
      const message = { type: 'page:frame-loads', urls: [`${GRANTED}${file}`] };
      page.from(source, message);
      return message;
    };
    const child = report({ parent: page.w }, 'reports/detail.html');
    report({ parent: null, opener: null }, 'data/stranger.csv');
    report(page.viewer, 'data/viewer.csv');
    expect(heard).not.toContainEqual(child);

    await vi.waitFor(() => expect(page.of('page:settled')).toHaveLength(1), { timeout: 10_000 });
    expect(page.of('page:loads').pop()?.paths).toEqual(['reports/detail.html']);
  }, 15_000);
});

describe('a workspace file a page frames', () => {
  it('sends its parent what it loaded on its own host, and what a frame inside it sends', () => {
    const posted: { message: { type: string; urls: string[] }; to: string }[] = [];
    const parent = {
      postMessage: (message: unknown, to: string) => posted.push({ message: JSON.parse(JSON.stringify(message)), to }),
    };
    let report: ((list: { getEntries: () => { name: string }[] }) => void) | null = null;
    const here = `${GRANT}tok/reports/detail.html`;
    const { window: w } = new JSDOM(`<!doctype html><html><head><script>${NESTED}</script></head><body></body></html>`, {
      url: here,
      runScripts: 'dangerously',
      pretendToBeVisual: true,
      beforeParse(win) {
        Object.defineProperty(win, 'parent', { value: parent, configurable: true });
        Object.defineProperty(win, 'PerformanceObserver', {
          configurable: true,
          value: class {
            constructor(callback: NonNullable<typeof report>) { report = callback; }
            observe() {}
          },
        });
      },
    });
    open.push(w);
    expect(NESTED).not.toContain('page:ready');
    // Each report names this document, which the viewer reads when the frame is its own.
    expect(posted).toEqual([{ message: { type: 'page:frame-loads', urls: [here], doc: here }, to: '*' }]);

    report!({ getEntries: () => [{ name: `${GRANT}tok/data/d.csv` }, { name: 'https://cdn.example.com/lib.js' }, { name: here }] });
    expect(posted.map((p) => p.message.urls)).toEqual([[here], [`${GRANT}tok/data/d.csv`]]);

    const heard: unknown[] = [];
    w.addEventListener('message', (e) => heard.push((e as MessageEvent).data), true);
    const from = (source: unknown, file: string) => {
      const e = new w.Event('message');
      const data = { type: 'page:frame-loads', urls: [`${GRANT}tok/${file}`] };
      Object.defineProperties(e, { source: { value: source }, data: { value: data } });
      w.dispatchEvent(e);
      return data;
    };
    const inner = from({ parent: w }, 'data/inner.csv');
    from({ parent: null }, 'data/stranger.csv');
    expect(posted.map((p) => p.message.urls).slice(2)).toEqual([[`${GRANT}tok/data/inner.csv`]]);
    expect(heard).not.toContainEqual(inner);
  });
});

describe('the page bridge on an opaque origin', () => {
  it('has IndexedDB read as absent, so a library that probes for it keeps to storage', () => {
    const page = served(`${GRANT}tok/reports/index.html`, '', (win) => {
      opaque(win);
      // What a browser offers there, refusing once it is opened.
      Object.defineProperty(win, 'indexedDB', {
        configurable: true,
        value: { open() { throw new Error('SecurityError'); } },
      });
    });
    expect(page.w.indexedDB).toBeUndefined();
    page.w.localStorage.setItem('theme', 'dark');
    expect(page.w.localStorage.getItem('theme')).toBe('dark');
  });

  it('ends a cookie on any Max-Age of zero or less, and on a past Expires only without one', () => {
    const page = served(`${GRANT}tok/reports/index.html`, '', (win) => {
      // A browser drops a cross-site frame's cookies without a word.
      Object.defineProperty(win.document, 'cookie', { configurable: true, get: () => '', set: () => {} });
    });
    const doc = page.w.document;
    doc.cookie = 'a=1';
    doc.cookie = 'b=2';
    doc.cookie = 'c=3';
    doc.cookie = 'a=; Max-Age=-1';
    doc.cookie = 'b=2; max-age=60; expires=Thu, 01 Jan 1970 00:00:00 GMT';
    doc.cookie = 'c=3; Max-Age=soon';
    expect(doc.cookie).toBe('b=2; c=3');
    doc.cookie = 'c=; Expires=Thu, 01 Jan 1970 00:00:00 GMT';
    expect(doc.cookie).toBe('b=2');
  });
});
