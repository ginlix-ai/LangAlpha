// @vitest-environment node
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

// The page's end of the bridge is plain script the server inlines as the
// first script of a framed document, so there is no module to import: it runs
// here from its parts, in a document of its own, wrapped as
// `preview_proxy/page_bridge.py` wraps the owner's live view (core, the
// request counter, then the comment parts). This suite is the repo's only JavaScript
// runner, hence the reach into the server's tree.
const part = (name: string) => readFileSync(
  resolve(__dirname, `../../../../../../../../../src/server/preview_proxy/bridge/${name}.js`),
  'utf8',
);
const BRIDGE = [
  "(function () {\n'use strict';\nif (!window.parent || window.parent === window) return;",
  part('core'),
  part('requests'),
  ...['text', 'areas', 'anchors', 'mode'].map((name) => part(`comments-${name}`)),
  'hello();\n})();',
].join('\n');

type PageWindow = Window & typeof globalThis;
// jsdom ships no type declarations and @types/jsdom is not a dependency here.
const { JSDOM } = createRequire(import.meta.url)('jsdom') as {
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
