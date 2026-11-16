import { afterEach, describe, expect, it } from 'vitest';
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
