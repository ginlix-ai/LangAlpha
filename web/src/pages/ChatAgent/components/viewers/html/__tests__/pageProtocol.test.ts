// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { parsePageMessage, type PageToViewer } from '../pageProtocol';

const RECT = { x: 10, y: 20, width: 30, height: 40 };

/** The message as the viewer reads it, narrowed to its type, or null. */
function read<T extends PageToViewer['type']>(type: T, fields: Record<string, unknown>): Extract<PageToViewer, { type: T }> | null {
  const message = parsePageMessage({ type, v: 1, ...fields });
  if (message && message.type !== type) throw new Error(`read ${message.type} for ${type}`);
  return message as Extract<PageToViewer, { type: T }> | null;
}

const pick = (anchor: unknown, fields: Record<string, unknown> = {}) => read('page:pick', { anchor, rect: RECT, ...fields });
const area = (anchor: unknown) => read('page:area', { anchor, rect: RECT });
const selection = (fields: Record<string, unknown>) => read('page:selection', fields)?.selection;

describe('parsePageMessage', () => {
  it('drops anything that is not a message it knows', () => {
    for (const data of [null, 'page:settled', [], {}, { type: 'page:nope' }, { type: 'widget:resize', height: 4 }]) {
      expect(parsePageMessage(data)).toBeNull();
    }
    expect(parsePageMessage({ type: 'page:settled', v: 1, extra: true })).toEqual({ type: 'page:settled' });
  });

  it('reads the path a hello names, and none it could not be', () => {
    expect(read('page:ready', { path: '/results/q3.html' })).toEqual({ type: 'page:ready', path: 'results/q3.html' });
    expect(read('page:ready', {})?.path).toBeNull();
    expect(read('page:ready', { path: 'a\u0000b' })?.path).toBeNull();
    expect(read('page:ready', { path: 'x'.repeat(1024) })?.path).toBe('x'.repeat(1024));
    expect(read('page:ready', { path: 'x'.repeat(1025) })?.path).toBeNull();
  });
});

describe('a box in the frame', () => {
  it('keeps a finite box and refuses what a viewport could not hold', () => {
    expect(read('page:area-drag', { rect: RECT })?.drag).toEqual({ rect: RECT, press: null });
    for (const rect of [null, [1, 2, 3, 4], { ...RECT, x: Number.NaN }, { ...RECT, y: Infinity }, { ...RECT, width: -1 }, { ...RECT, height: 2e6 }, { ...RECT, x: '10' }]) {
      expect(read('page:area-drag', { rect })).toBeNull();
    }
  });
});

describe('an element or text anchor', () => {
  it('keeps the known fields of the kind the message is for and drops the rest', () => {
    const anchor = { kind: 'element', path: '#tiles', label: 'Key numbers', point: { x: 0.25, y: 0.5 }, detail: 'x', onclick: 'y' };
    expect(pick(anchor)?.anchor).toEqual({ kind: 'element', path: '#tiles', label: 'Key numbers', point: { x: 0.25, y: 0.5 } });
  });

  it('refuses another kind than the message is for, a named spot included', () => {
    expect(pick({ kind: 'text', quote: 'Graphics' })).toBeNull();
    expect(pick({ kind: 'named', path: '#x' })).toBeNull();
  });

  it('needs words for a selection and a path or words for an element', () => {
    expect(selection({ anchor: { kind: 'text', path: '#seg' }, rect: RECT })).toBeUndefined();
    expect(pick({ kind: 'element', label: 'Only a label' })).toBeNull();
    expect(pick({ kind: 'element', quote: 'Revenue' })?.anchor).toEqual({ kind: 'element', quote: 'Revenue' });
  });

  it('strips control characters and treats blank text as absent', () => {
    expect(selection({ anchor: { kind: 'text', quote: '\u0000Data\u0007 Center ', label: ' \u0001 ' }, rect: RECT })?.anchor).toEqual({
      kind: 'text',
      quote: 'Data Center',
    });
  });

  it('cuts long text to the API limit without leaving half a surrogate pair', () => {
    const long = pick({ kind: 'element', path: '#a', label: 'x'.repeat(400) })?.anchor;
    expect(long?.label).toHaveLength(300);
    expect(long?.label?.endsWith('…')).toBe(true);

    // 298 plain characters, then an emoji whose high surrogate would end the cut.
    const label = `${'x'.repeat(298)}\u{1F600}tail`;
    expect(pick({ kind: 'element', path: '#a', label })?.anchor.label).toBe(`${'x'.repeat(298)}…`);
  });

  it('drops a point outside the element', () => {
    expect(pick({ kind: 'element', path: '#a', point: { x: 1.5, y: 0.5 } })?.anchor).toEqual({ kind: 'element', path: '#a' });
  });
});

describe('an area anchor', () => {
  const parts = [
    { path: '#grid > div:nth-of-type(1)', label: 'Revenue', quote: 'Revenue $46.7B' },
    { path: '#grid > div:nth-of-type(2)', label: 'Gross margin', quote: 'Gross margin 72.4%' },
  ];
  const anchor = {
    kind: 'area', label: 'Revenue, Gross margin', elements: parts, count: 5,
    point: { x: 1, y: 1 }, rect: { x: 0.1, y: 0.2, w: 0.5, h: 0.1 },
  };

  it('keeps its parts, its count, its pin’s point and where it sat, and none of an element’s fields', () => {
    // The file the bridge read it on is the comment's own `path`, not the anchor's.
    expect(area({ ...anchor, page_path: 'index.html', path: '#grid', quote: 'x', onclick: 'y' })?.anchor).toEqual(anchor);
    expect(area({ ...anchor, point: { x: 1.5, y: 1 } })?.anchor.point).toBeUndefined();
  });

  it('refuses an area of fewer than two parts or more than eight, or one counting fewer than it lists', () => {
    expect(area({ ...anchor, elements: parts.slice(0, 1), count: 1 })).toBeNull();
    expect(area({ ...anchor, elements: Array.from({ length: 9 }, (_, i) => ({ path: `#p${i}` })), count: 9 })).toBeNull();
    for (const count of [1, 501, 2.5]) expect(area({ ...anchor, count })).toBeNull();
    expect(area({ ...anchor, elements: 'two' })).toBeNull();
  });

  it('refuses the whole area for a part it could not find again, rather than a smaller one the reader never drew', () => {
    expect(area({ ...anchor, elements: [...parts, { label: 'Only a label' }] })).toBeNull();
    expect(area({ ...anchor, elements: [...parts, 'junk'] })).toBeNull();
  });

  it('cuts each part’s text to the API’s limits and drops a rect outside the page', () => {
    const long = area({ ...anchor, elements: [{ path: '#a', label: 'x'.repeat(400) }, parts[1]] })?.anchor;
    expect(long?.kind === 'area' && long.elements[0].label).toHaveLength(300);
    for (const rect of [{ x: 1.2, y: 0, w: 0.1, h: 0.1 }, { x: 0, y: 0, w: Number.NaN, h: 0.1 }, { x: 0, y: 0, w: 0.1 }, 'top']) {
      const read = area({ ...anchor, rect })?.anchor;
      expect(read?.count).toBe(5);
      expect(read?.rect).toBeUndefined();
    }
  });

  it('is read only as an area, and an area only where one is asked for', () => {
    expect(pick(anchor)).toBeNull();
    expect(area({ ...anchor, kind: 'element' })).toBeNull();
  });
});

describe('page:area and page:area-drag', () => {
  const anchor = { kind: 'area', elements: [{ path: '#a' }, { path: '#b' }], count: 2 };

  it('read an area like a pick, with the press it began with', () => {
    expect(read('page:area', { anchor, rect: RECT, press: 4 })).toEqual({ type: 'page:area', anchor, rect: RECT, title: null, press: 4 });
    expect(read('page:area', { anchor })).toBeNull();
    expect(area({ kind: 'element', path: '#a' })).toBeNull();
  });

  it('read the rectangle being dragged, and its end', () => {
    expect(read('page:area-drag', { rect: RECT, press: 4 })?.drag).toEqual({ rect: RECT, press: 4 });
    expect(read('page:area-drag', { clear: true })).toEqual({ type: 'page:area-drag', drag: null });
    expect(read('page:area-drag', { rect: { ...RECT, width: -2 } })).toBeNull();
  });
});

describe('page:hover', () => {
  it('reads the box and its label, and clears on clear', () => {
    expect(read('page:hover', { rect: RECT, label: 'Revenue' })?.hover).toEqual({ rect: RECT, label: 'Revenue', word: false });
    expect(read('page:hover', { rect: RECT, label: null })?.hover).toEqual({ rect: RECT, label: null, word: false });
    expect(read('page:hover', { clear: true, rect: RECT })?.hover).toBeNull();
    expect(read('page:hover', { rect: 'nowhere' })).toBeNull();
  });

  it('reads a word under the pointer, which carries no label', () => {
    expect(read('page:hover', { rect: RECT, word: true, label: 'Revenue' })?.hover).toEqual({ rect: RECT, label: null, word: true });
    expect(read('page:hover', { rect: RECT, word: 'yes' })?.hover?.word).toBe(false);
  });
});

describe('page:pick and page:selection', () => {
  it('need both a trusted anchor and a box', () => {
    expect(pick({ kind: 'element', path: '#a' })).toEqual({ type: 'page:pick', anchor: { kind: 'element', path: '#a' }, rect: RECT, title: null, press: null });
    expect(read('page:pick', { anchor: { kind: 'element', path: '#a' } })).toBeNull();
  });

  it('keeps the document title a pick names, for the comment it starts', () => {
    const at = { kind: 'element', path: '#a' };
    expect(pick(at, { title: '  Q3 margins\u0007 ' })?.title).toBe('Q3 margins');
    expect(pick(at, { title: 'x'.repeat(400) })?.title).toHaveLength(300);
    for (const title of ['', '   ', 7, null]) expect(pick(at, { title })?.title).toBeNull();
    expect(selection({ anchor: { kind: 'text', quote: 'Graphics' }, rect: RECT, title: 'Q3' })).toMatchObject({ title: 'Q3' });
  });

  it('keeps the number of the press a click ended, and no other kind of number', () => {
    const at = { kind: 'element', path: '#a' };
    expect(pick(at, { press: 7 })?.press).toBe(7);
    for (const press of [0, -1, 1.5, '7', Number.MAX_VALUE, null]) expect(pick(at, { press })?.press).toBeNull();
    expect(read('page:press', { press: 3 })).toEqual({ type: 'page:press', press: 3 });
    expect(read('page:press', {})).toBeNull();
  });

  it('tells a cleared selection from one not to trust', () => {
    expect(read('page:selection', { clear: true })).toEqual({ type: 'page:selection', selection: null });
    expect(selection({ anchor: { kind: 'text', quote: 'Graphics' }, rect: RECT })).toEqual({
      anchor: { kind: 'text', quote: 'Graphics' },
      rect: RECT,
      title: null,
      press: null,
      compose: false,
    });
    expect(read('page:selection', { anchor: { kind: 'text' }, rect: RECT })).toBeNull();
  });

  it('reads words picked in comment mode, with their press and where the pin stands', () => {
    const anchor = { kind: 'text', quote: 'Data Center', label: 'Segment revenue', point: { x: 0.9, y: 0.5 } };
    expect(selection({ anchor, rect: RECT, press: 3, compose: true })).toEqual({ anchor, rect: RECT, title: null, press: 3, compose: true });
    // Only a true flag opens the box; anything else is a selection the chip offers.
    expect(selection({ anchor, rect: RECT, press: 3, compose: 'yes' })).toMatchObject({ press: 3, compose: false });
    expect(selection({ anchor, rect: RECT, press: '3', compose: true })).toMatchObject({ press: null, compose: true });
  });
});

describe('page:positions', () => {
  it('keeps each item with an id, with null for one not on the page or not trusted', () => {
    const items = [{ id: 'a', rect: RECT }, { id: 'b', rect: null }, { id: 'c', rect: { x: 'bad' } }, { rect: RECT }, 'junk'];
    expect(read('page:positions', { items })?.items).toEqual([{ id: 'a', rect: RECT }, { id: 'b', rect: null }, { id: 'c', rect: null }]);
  });

  it('refuses a message without a list', () => {
    expect(read('page:positions', { items: { a: RECT } })).toBeNull();
  });
});

describe('page:key', () => {
  it('knows Esc and C only', () => {
    expect(read('page:key', { key: 'Escape' })?.key).toBe('Escape');
    expect(read('page:key', { key: 'c' })?.key).toBe('c');
    expect(read('page:key', { key: 'Enter' })).toBeNull();
    expect(read('page:key', {})).toBeNull();
  });
});

describe('page:scroll', () => {
  it('reads where the scroll stands and whether it is still moving', () => {
    expect(read('page:scroll', { x: 0, y: 720, moving: true })).toEqual({ type: 'page:scroll', x: 0, y: 720, moving: true });
    expect(read('page:scroll', { x: 0, y: 720, moving: false })?.moving).toBe(false);
  });

  it('refuses a report that does not say whether it stopped', () => {
    for (const fields of [{ x: 0, y: 720 }, { x: 0, y: 720, moving: 'no' }, { x: 0, y: Infinity, moving: false }, { y: 720, moving: false }]) {
      expect(read('page:scroll', fields)).toBeNull();
    }
  });
});

describe('page:ground', () => {
  it('keeps a colour as the browser computes one, and null for no ground', () => {
    expect(read('page:ground', { color: 'rgb(14, 17, 22)' })?.color).toBe('rgb(14, 17, 22)');
    expect(read('page:ground', { color: 'rgba(255, 255, 255, 0.5)' })?.color).toBe('rgba(255, 255, 255, 0.5)');
    expect(read('page:ground', { color: null })).toEqual({ type: 'page:ground', color: null });
  });

  it('does not trust anything else into a style', () => {
    for (const color of ['red', 'rgb(0, 0, 0); background-image: url(x)', 'url(https://example.com/x.png)', undefined]) {
      expect(read('page:ground', { color })).toBeNull();
    }
  });
});
