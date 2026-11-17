import { describe, expect, it } from 'vitest';
import { render } from '@testing-library/react';
import Markdown, { type RehypePlugins } from '../../../../Markdown';
import { prepareMarkdownSource } from '../../../../../utils/markdownSource';
import { joined, partModel } from '../../draftParts';
import { markdownSpots } from '../markdownSpots';
import { rehypeSourceLines } from '../sourceLines';

const SOURCE_LINES = (fileLine: (line: number) => number): RehypePlugins[number] => [rehypeSourceLines, fileLine];

const FILE = `# Q3 notes

## Revenue

Revenue came in at $338M, ahead of the $331M consensus.

| Metric | Q2 | Q3 |
|---|---|---|
| Revenue ($M) | 325 | 338 |
| Gross margin | 61.0% | 61.5% |

## Margin outlook

We model 61.5% for Q3, 50 bp under the guide,
as freight costs ease.

- Freight normalises by Q4
- Services mix rises
  - Renewals lead it

Revenue came in at $338M, ahead of the $331M consensus.
`;

function shown(content: string): HTMLElement {
  return render(<Markdown variant="panel" content={content} rehypeForBlock={SOURCE_LINES} />).container;
}

function linesOf(root: HTMLElement): string[] {
  return [...root.querySelectorAll('[data-source-lines]')].map((el) => `${el.localName} ${el.getAttribute('data-source-lines')}`);
}

describe('a Markdown block names its lines in the file', () => {
  it('marks each block the panel renders', () => {
    expect(linesOf(shown(FILE))).toEqual([
      'h1 1', 'h2 3', 'p 5', 'tr 7', 'tr 9', 'tr 10', 'h2 12', 'p 14-15', 'li 17', 'li 18', 'li 19', 'p 21',
    ]);
  });

  it('maps the prepared copy back past front matter and an equation given its own lines', () => {
    const original = '---\ntitle: x\n---\n# A\n\n$$x^2$$\n\nCost is $5 and $6.\n\nEnd';
    const { text, lineOf } = prepareMarkdownSource(original, null, []);
    expect(text).toBe('# A\n\n$$\nx^2\n$$\n\nCost is \\$5 and \\$6.\n\nEnd');
    expect(text.split('\n').map((_, i) => lineOf(i) + 1)).toEqual([4, 5, 6, 6, 6, 7, 8, 9, 10]);
  });
});

describe('markdownSpots', () => {
  const block = (root: Element, lines: string) => root.querySelector(`[data-source-lines="${lines}"]`)!;

  it('saves a paragraph by its lines, the heading above it and its words', () => {
    const root = shown(FILE);
    expect(markdownSpots(root).spots!.anchorOf([block(root, '14-15')])).toEqual({
      kind: 'block',
      lines: '14-15',
      label: 'Margin outlook',
      quote: 'We model 61.5% for Q3, 50 bp under the guide, as freight costs ease.',
    });
  });

  it('names a row by its first cell and quotes its cells apart', () => {
    const root = shown(FILE);
    expect(markdownSpots(root).spots!.anchorOf([block(root, '9')])).toEqual({
      kind: 'block', lines: '9', label: 'Revenue ($M)', quote: 'Revenue ($M) | 325 | 338',
    });
  });

  it('saves several blocks as an area listing each', () => {
    const root = shown(FILE);
    expect(markdownSpots(root).spots!.anchorOf([block(root, '9'), block(root, '10')])).toEqual({
      kind: 'area',
      label: 'Revenue ($M), Gross margin',
      count: 2,
      elements: [
        { lines: '9', label: 'Revenue ($M)', quote: 'Revenue ($M) | 325 | 338' },
        { lines: '10', label: 'Gross margin', quote: 'Gross margin | 61.0% | 61.5%' },
      ],
    });
  });

  it('lists the first 64 blocks of a longer area and counts them all', () => {
    const root = shown(Array.from({ length: 70 }, (_, i) => `Paragraph ${i + 1}.`).join('\n\n'));
    const anchor = markdownSpots(root).spots!.anchorOf([...root.querySelectorAll('[data-source-lines]')]);
    expect(anchor).toMatchObject({ kind: 'area', count: 70 });
    expect(anchor?.kind === 'area' && anchor.elements.length).toBe(64);
  });

  it('quotes an item without the items under it', () => {
    const root = shown(FILE);
    expect(markdownSpots(root).spots!.anchorOf([block(root, '18')])).toMatchObject({ lines: '18', quote: 'Services mix rises' });
  });

  it('finds a block again by its words once lines are added above it', () => {
    const before = shown(FILE);
    const anchor = markdownSpots(before).spots!.anchorOf([block(before, '14-15')])!;
    const after = shown(`Intro added.\n\nAnother line.\n\n${FILE}`);
    const found = markdownSpots(after).find(anchor);
    expect(found && 'parts' in found ? found.parts[0]?.[0].getAttribute('data-source-lines') : null).toBe('18-19');
  });

  it('tells apart blocks that read the same by the lines they were on', () => {
    const root = shown(FILE);
    const spots = markdownSpots(root);
    const later = spots.spots!.anchorOf([block(root, '21')])!;
    const earlier = spots.spots!.anchorOf([block(root, '5')])!;
    const at = (anchor: typeof later) => {
      const found = spots.find(anchor);
      return found && 'parts' in found ? found.parts[0]?.[0].getAttribute('data-source-lines') : null;
    };
    expect([at(earlier), at(later)]).toEqual(['5', '21']);
  });

  it('reads the content afresh for a new pass of lookups, and not before', () => {
    const view = render(<Markdown variant="panel" content={FILE} rehypeForBlock={SOURCE_LINES} />);
    const root = view.container;
    const spots = markdownSpots(root);
    const anchor = spots.spots!.anchorOf([block(root, '14-15')])!;
    const lineOf = () => {
      const found = spots.find(anchor);
      return found && 'parts' in found ? found.parts[0]?.[0].getAttribute('data-source-lines') : null;
    };
    expect(lineOf()).toBe('14-15');
    view.rerender(<Markdown variant="panel" content={`Intro added.\n\nAnother line.\n\n${FILE}`} rehypeForBlock={SOURCE_LINES} />);
    spots.fresh!();
    expect(lineOf()).toBe('18-19');
  });

  it('quotes a row and an item without the hidden copy of a formula in them', () => {
    const root = shown('| Metric | Value |\n|---|---|\n| Yield | $d/p$ |\n\n- Cover is $e/d$ times\n');
    const spots = markdownSpots(root).spots!;
    const row = root.querySelectorAll('tr')[1];
    const quote = (el: Element) => { const a = spots.anchorOf([el]); return a && 'quote' in a ? a.quote : null; };
    expect(quote(row)).not.toMatch(/x-tex|annotation/);
    expect(quote(root.querySelector('li')!)).not.toMatch(/x-tex|annotation/);
    expect(quote(row)).toBe(quote(row)!.replace(/(d\/p).*\1/, '$1'));
  });

  it('finds words again whose spaces are narrow no-break ones', () => {
    const root = shown('Shipped 1\u202f000 units, up from 1\u202f200.\n');
    const spots = markdownSpots(root);
    const text = root.querySelector('p')!.firstChild as Text;
    const range = document.createRange();
    range.setStart(text, text.data.indexOf('1'));
    range.setEnd(text, text.data.indexOf('units') + 'units'.length);
    const found = spots.find(spots.words!(range)!);
    expect(found && 'range' in found ? found.range.toString() : null).toBe('1\u202f000 units');
  });

  it('keeps an item beside the items under it, whose words it leaves out', () => {
    const root = shown('- Services mix rises\n  - Renewals lead it\n');
    const [parent, child] = [...root.querySelectorAll('li')];
    const picker = markdownSpots(root).spots!;
    expect(joined([], [parent, child], partModel(picker))).toEqual([parent, child]);
    const anchor = picker.anchorOf([parent, child]);
    expect(anchor && 'elements' in anchor ? anchor.elements?.map((e) => e.quote) : null).toEqual(['Services mix rises', 'Renewals lead it']);
  });

  it('leaves a block whose words are gone unfound', () => {
    const root = shown(FILE);
    expect(markdownSpots(root).find({ kind: 'block', lines: '14-15', quote: 'Nothing like this is in the file' })).toBeNull();
  });

  it('saves selected words with their lines and finds the right occurrence again', () => {
    const root = shown(FILE);
    const spots = markdownSpots(root);
    const text = block(root, '21').firstChild as Text;
    const range = document.createRange();
    range.setStart(text, text.data.indexOf('ahead'));
    range.setEnd(text, text.data.indexOf('consensus') + 'consensus'.length);
    const anchor = spots.words!(range)!;
    expect(anchor).toEqual({
      kind: 'text',
      quote: 'ahead of the $331M consensus',
      lines: '21',
      label: 'Margin outlook',
      prefix: 'Revenue came in at $338M,',
      suffix: '.',
    });
    const found = spots.find(anchor);
    expect(found && 'range' in found ? found.range.startContainer.parentElement?.getAttribute('data-source-lines') : null).toBe('21');
  });

  it('finds a single selected character again, by its lines', () => {
    const root = shown('Total = a + b\n\nAnother line with a in it\n');
    const spots = markdownSpots(root);
    const text = root.querySelector('[data-source-lines="3"]')!.firstChild as Text;
    const range = document.createRange();
    range.setStart(text, text.data.indexOf(' a ') + 1);
    range.setEnd(text, text.data.indexOf(' a ') + 2);
    const found = spots.find(spots.words!(range)!);
    expect(found && 'range' in found ? found.range.startContainer.parentElement?.getAttribute('data-source-lines') : null).toBe('3');
  });

  it('finds words that fold differently as a whole than one letter at a time', () => {
    const root = shown('The road, or ΟΔΟΣ, runs north.\n');
    const spots = markdownSpots(root);
    const text = root.querySelector('p')!.firstChild as Text;
    const range = document.createRange();
    range.setStart(text, text.data.indexOf('ΟΔΟΣ'));
    range.setEnd(text, text.data.indexOf('ΟΔΟΣ') + 4);
    const found = spots.find(spots.words!(range)!);
    expect(found && 'range' in found ? found.range.toString() : null).toBe('ΟΔΟΣ');
  });

  it('quotes a formula as it reads, without its hidden copy, and finds it again', () => {
    const root = shown('Yield is $d/p$ per share.\n');
    const spots = markdownSpots(root);
    const p = root.querySelector('p')!;
    const range = document.createRange();
    range.setStart(p.firstChild!, 0);
    range.setEnd(p.lastChild!, (p.lastChild as Text).data.length);
    const anchor = spots.words!(range)!;
    expect(anchor.kind === 'text' ? anchor.quote : null).not.toMatch(/annotation|application\/x-tex/);
    expect(spots.find(anchor)).toMatchObject({ range: expect.anything() });
  });
});
