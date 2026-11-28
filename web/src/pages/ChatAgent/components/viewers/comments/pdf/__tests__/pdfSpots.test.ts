import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { TextAnchor } from '@/types/artifacts';
import { pdfSpots } from '../pdfSpots';

/** A page as react-pdf draws it: lines of words as spans, each line ended by a `<br>`, and the end marker once laid out. */
function layer(lines: string[], drawn = true): string {
  const words = lines.map((l) => `<span role="presentation">${l}</span><br role="presentation">`).join('');
  return `<div class="pdf-page-surface"><div class="react-pdf__Page"><canvas></canvas>`
    + `<div class="react-pdf__Page__textContent textLayer">${words}${drawn ? '<div class="endOfContent"></div>' : ''}</div></div></div>`;
}

function column(pages: (string | null)[]): HTMLElement {
  const root = document.createElement('div');
  root.className = 'pdf-pages';
  root.innerHTML = pages.map((p, i) => `<div class="pdf-page" data-pdf-slot="${i}">${p ?? ''}</div>`).join('');
  document.body.appendChild(root);
  return root;
}

function slot(root: Element, index: number): Element {
  return root.querySelector(`[data-pdf-slot="${index}"]`)!;
}

/** The range over `words` in the `nth` span holding them on page `index`. */
function wordsOn(root: Element, index: number, words: string, nth = 0): Range {
  const spans = [...slot(root, index).querySelectorAll('span')].filter((s) => s.textContent!.includes(words));
  const text = spans[nth].firstChild as Text;
  const at = text.data.indexOf(words);
  const range = document.createRange();
  range.setStart(text, at);
  range.setEnd(text, at + words.length);
  return range;
}

function pageOf(range: Range): number {
  const el = range.startContainer.parentElement!;
  return Number(el.closest('[data-pdf-slot]')!.getAttribute('data-pdf-slot')) + 1;
}

afterEach(() => {
  document.body.innerHTML = '';
});

describe('pdfSpots: words selected on a page', () => {
  it('saves the page and the words, a line break read as a space', () => {
    const root = column([layer(['Revenue rose 12% to', '$338M in Q3, ahead of', 'the consensus.'])]);
    const range = document.createRange();
    const lines = slot(root, 0).querySelectorAll('span');
    range.setStart(lines[0].firstChild!, 8);
    range.setEnd(lines[1].firstChild!, 5);
    expect(pdfSpots(root).words!(range)).toEqual({
      kind: 'text', page: 1, quote: 'rose 12% to $338M', prefix: 'Revenue', suffix: 'in Q3, ahead of the consensus.',
    });
  });

  it('keeps a selection running onto the next page to its first page', () => {
    const root = column([layer(['End of page one']), layer(['Start of page two'])]);
    const range = document.createRange();
    range.setStart(slot(root, 0).querySelector('span')!.firstChild!, 7);
    range.setEnd(slot(root, 1).querySelector('span')!.firstChild!, 5);
    expect(pdfSpots(root).words!(range)).toMatchObject({ page: 1, quote: 'page one' });
    // What the box and the mark are drawn on is that page's part too.
    const kept = pdfSpots(root).clip!(range);
    expect(slot(root, 0).contains(kept.endContainer)).toBe(true);
    expect(kept.toString()).toBe('page one');
  });

  it('takes no comment on words outside a page', () => {
    const root = column([layer(['Words'])]);
    const outside = document.createElement('p');
    outside.textContent = 'Not the file';
    root.before(outside);
    const range = document.createRange();
    range.selectNodeContents(outside);
    expect(pdfSpots(root).words!(range)).toBeNull();
  });
});

describe('pdfSpots: finding words again by page and quote', () => {
  const anchor = (page: number, quote: string, around: Partial<TextAnchor> = {}): TextAnchor => ({ kind: 'text', page, quote, ...around });

  it('finds them on their page, not the same words on another', () => {
    const root = column([layer(['Gross margin was 61.5%']), layer(['Gross margin was 61.5%'])]);
    const found = pdfSpots(root).find(anchor(2, 'margin was 61.5%'));
    expect(found && 'range' in found && pageOf(found.range)).toBe(2);
    expect(found && 'range' in found && found.range.toString()).toBe('margin was 61.5%');
  });

  it('tells the same words on one page apart by the words around them', () => {
    const root = column([layer(['In Q2 revenue grew 4%.', 'In Q3 revenue grew 4%.'])]);
    const second = wordsOn(root, 0, 'revenue grew', 1);
    const saved = pdfSpots(root).words!(second)!;
    const found = pdfSpots(root).find(saved);
    expect(found && 'range' in found && found.range.startContainer).toBe(second.startContainer);
  });

  it('parks on a page whose words are not drawn: unmounted, or still laying out', () => {
    const root = column([layer(['One']), null, layer(['Three'], false)]);
    expect(pdfSpots(root).find(anchor(2, 'anything'))).toEqual({ parked: slot(root, 1) });
    expect(pdfSpots(root).find(anchor(3, 'Three'))).toEqual({ parked: slot(root, 2) });
  });

  it('parks every comment while no page is laid out', () => {
    const root = column([]);
    expect(pdfSpots(root).find(anchor(4, 'anything'))).toEqual({ parked: root });
  });

  it('finds nothing past the last page, nor words gone from a drawn page', () => {
    const root = column([layer(['Only page'])]);
    expect(pdfSpots(root).find(anchor(2, 'Only page'))).toBeNull();
    expect(pdfSpots(root).find(anchor(1, 'Removed words'))).toBeNull();
  });
});

describe('pdfSpots: a pass of finds', () => {
  const anchor = (quote: string): TextAnchor => ({ kind: 'text', page: 1, quote });

  it('reads a page\'s words once for all the comments on it, and afresh in the next pass', () => {
    const root = column([layer(['Revenue rose 12% to', '$338M in Q3, ahead of', 'the consensus.'])]);
    const strategy = pdfSpots(root);
    const walks = vi.spyOn(document, 'createTreeWalker');
    strategy.fresh!();
    const found = ['rose 12%', 'in Q3', 'consensus'].map((quote) => strategy.find(anchor(quote)));
    strategy.settled!();
    // Walks over text alone: the page's words read for the quotes.
    const read = walks.mock.calls.filter((call) => call[1] === NodeFilter.SHOW_TEXT).length;
    walks.mockRestore();
    expect(read).toBe(1);
    expect(found.map((f) => f && 'range' in f && f.range.toString())).toEqual(['rose 12%', 'in Q3', 'consensus']);

    slot(root, 0).querySelectorAll('span')[2].textContent = 'the guidance.';
    strategy.fresh!();
    expect(strategy.find(anchor('consensus'))).toBeNull();
    const now = strategy.find(anchor('guidance'));
    expect(now && 'range' in now && now.range.toString()).toBe('guidance');
  });

  it('reads the radicals of one line as the reader sees them beside a line held as it is seen', () => {
    const root = column([layer(['Gross margin', '营收增⻓，⽑利率稳定'])]);
    const found = pdfSpots(root).find(anchor('margin营收增长'));
    expect(found && 'range' in found && found.range.toString()).toBe('margin营收增⻓');
  });
});

/** The range from `from` in the first line's span to `to` in the last's. */
function across(root: Element, from: number, to: number): Range {
  const lines = slot(root, 0).querySelectorAll('span');
  const range = document.createRange();
  range.setStart(lines[0].firstChild!, from);
  range.setEnd(lines[lines.length - 1].firstChild!, to);
  return range;
}

describe('pdfSpots: words as the reader sees them', () => {
  // A CJK font that maps its glyphs to radicals: the reader sees 长 and 毛,
  // the text layer holds U+2ED3 and U+2F51. A ligature reads as a copy does.
  const RAW = '营收增⻓，⽑利率稳定';
  const SEEN = '营收增长，毛利率稳定';

  it('quotes the characters the reader sees, the words around them too', () => {
    const root = column([layer([`本季度${RAW}，ﬁnal`])]);
    const range = wordsOn(root, 0, RAW);
    expect(pdfSpots(root).words!(range)).toEqual({ kind: 'text', page: 1, quote: SEEN, prefix: '本季度', suffix: '，final' });
  });

  it('finds again a comment saved as the reader sees the words', () => {
    const root = column([layer([`本季度${RAW}`])]);
    const found = pdfSpots(root).find({ kind: 'text', page: 1, quote: SEEN, prefix: '本季度' });
    expect(found && 'range' in found && found.range.toString()).toBe(RAW);
  });

  it('finds again a comment saved before, with the radicals as the layer holds them', () => {
    const root = column([layer([`本季度${RAW}`])]);
    const found = pdfSpots(root).find({ kind: 'text', page: 1, quote: RAW, prefix: '本季度' });
    expect(found && 'range' in found && found.range.toString()).toBe(RAW);
  });

  it('quotes adjacent ligatures as the search reads them, and finds them again', () => {
    const root = column([layer(['the ﬅﬅ mark'])]);
    const words = pdfSpots(root).words!(wordsOn(root, 0, 'ﬅﬅ'));
    expect(words).toMatchObject({ quote: 'ſtſt' });
    const found = pdfSpots(root).find(words as TextAnchor);
    expect(found && 'range' in found && found.range.toString()).toBe('ﬅﬅ');
  });
});

describe('pdfSpots: a line break in the words', () => {
  it('is no space between two Chinese characters', () => {
    const root = column([layer(['管理层上调了全年指引，并强', '调推理需求持续强劲。'])]);
    expect(pdfSpots(root).words!(across(root, 10, 4))).toMatchObject({ quote: '，并强调推理需' });
  });

  it('is a space between Latin words, and between Chinese and Latin either way', () => {
    const latin = column([layer(['gross margin', 'held steady'])]);
    expect(pdfSpots(latin).words!(across(latin, 6, 4))).toMatchObject({ quote: 'margin held' });
    document.body.innerHTML = '';
    const mixed = column([layer(['营收超过', 'consensus 预期'])]);
    expect(pdfSpots(mixed).words!(across(mixed, 2, 11))).toMatchObject({ quote: '超过 consensus 预' });
  });

  it('still finds a comment saved with the space a break used to read as', () => {
    const root = column([layer(['管理层上调了全年指引，并强', '调推理需求持续强劲。'])]);
    const found = pdfSpots(root).find({ kind: 'text', page: 1, quote: '，并强 调推理需' });
    expect(found && 'range' in found && wordsInFound(found.range)).toBe('，并强调推理需');
  });
});

function wordsInFound(range: Range): string {
  return pdfSpots(range.startContainer.ownerDocument!.body).words!(range)!.quote!;
}

describe('pdfSpots: the same words over and over on one page', () => {
  const FILLER = 'Lorem ipsum revenue growth margins valuation scenario analysis';
  // Laid out as on screen: the page `size` px wide and tall over its text
  // layer, each line one span `size / 400` px apart, as zoom scales them.
  let size = 800;
  const real = { range: Range.prototype.getBoundingClientRect, element: Element.prototype.getBoundingClientRect };
  const lineIndex = (node: Node) => {
    const span = (node instanceof Element ? node : node.parentElement)!.closest('span')!;
    return [...span.parentElement!.querySelectorAll('span')].indexOf(span);
  };
  beforeEach(() => {
    size = 800;
    Range.prototype.getBoundingClientRect = function (this: Range) {
      const line = size / 400;
      const first = lineIndex(this.startContainer);
      const last = lineIndex(this.endContainer);
      return new DOMRect(size / 8 + this.startOffset, 100 + first * line, size / 4, (last - first + 1) * line * 0.8);
    };
    Element.prototype.getBoundingClientRect = function (this: Element) {
      return this.classList.contains('textLayer') ? new DOMRect(0, 100, size, size) : new DOMRect(0, 0, 0, 0);
    };
  });
  afterEach(() => {
    // jsdom lays nothing out, and has no box for a range at all.
    if (real.range) Range.prototype.getBoundingClientRect = real.range;
    else delete (Range.prototype as Partial<Range>).getBoundingClientRect;
    Element.prototype.getBoundingClientRect = real.element;
  });
  const filler = (n: number) => column([layer(['Anthropic Initiation Report', ...Array.from({ length: n }, () => FILLER)])]);
  /** The words on line `n` (1-based, after the title). */
  const onLine = (root: Element, n: number, words = 'revenue growth') => wordsOn(root, 0, words, n - 1);
  const foundLine = (root: Element, anchor: TextAnchor) => {
    const found = pdfSpots(root).find(anchor);
    return found && 'range' in found ? lineIndex(found.range.startContainer) : null;
  };

  it('saves where the words sat, as fractions of their page', () => {
    const root = filler(30);
    const saved = pdfSpots(root).words!(onLine(root, 12))!;
    expect(saved.rect).toEqual({ x: 0.14, y: 0.03, w: 0.25, h: 0.002 });
  });

  it('finds them on the line they were selected on, whatever the zoom since', () => {
    const root = filler(30);
    const saved = pdfSpots(root).words!(onLine(root, 12))!;
    expect(foundLine(root, saved)).toBe(12);
    size = 1600;
    expect(foundLine(root, saved)).toBe(12);
    const across = document.createRange();
    across.setStart(onLine(root, 15).startContainer, 6);
    across.setEnd(onLine(root, 25).startContainer, 20);
    expect(foundLine(root, pdfSpots(root).words!(across)!)).toBe(15);
  });

  it('weighs every place on a page of hundreds, measuring only the tied ones', () => {
    const root = filler(260);
    const saved = pdfSpots(root).words!(onLine(root, 240))!;
    let measured = 0;
    const measure = Range.prototype.getBoundingClientRect;
    Range.prototype.getBoundingClientRect = function (this: Range) {
      measured++;
      return measure.call(this);
    };
    expect(foundLine(root, saved)).toBe(240);
    // The first and last lines' words around differ from the rest, so they never tie.
    expect(measured).toBeLessThanOrEqual(260);
  });

  it('goes by the words around before where the words sat', () => {
    const root = column([layer([FILLER, FILLER, 'Lorem ipsum revenue growth beat the plan', FILLER])]);
    const saved = pdfSpots(root).words!(wordsOn(root, 0, 'revenue growth', 2))!;
    // The page changed: the words moved up, and a filler line sits where they were.
    slot(root, 0).innerHTML = layer(['Lorem ipsum revenue growth beat the plan', FILLER, FILLER, FILLER]);
    expect(foundLine(root, saved)).toBe(0);
  });

  it('finds words saved before they kept their place as it always did: the first that fits', () => {
    const root = filler(30);
    const { rect: _rect, ...before } = pdfSpots(root).words!(onLine(root, 12))!;
    expect(foundLine(root, before)).toBe(2);
  });
});
