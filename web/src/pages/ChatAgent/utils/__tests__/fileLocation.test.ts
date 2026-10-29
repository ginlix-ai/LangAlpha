// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  parseFragment,
  splitFileLocation,
  headingSlugs,
  findHeadingIndex,
  countLines,
  markdownLineProbe,
  markdownLineTarget,
  headingIndexAbove,
  countHeadings,
  hasLineSuffix,
} from '../fileLocation';

describe('parseFragment', () => {
  it('reads line and range fragments', () => {
    expect(parseFragment('L42')).toEqual({ line: 42 });
    expect(parseFragment('L40-L55')).toEqual({ line: 40, lineEnd: 55 });
    expect(parseFragment('L40-55')).toEqual({ line: 40, lineEnd: 55 });
    expect(parseFragment('L55-L40')).toEqual({ line: 40, lineEnd: 55 });
    expect(parseFragment('L0')).toBeNull();
  });

  it('reads a PDF page', () => {
    expect(parseFragment('page=12')).toEqual({ page: 12 });
    expect(parseFragment('zoom=100&page=3')).toEqual({ page: 3 });
    expect(parseFragment('page=0')).toBeNull();
  });

  it('reads a lowercase l-number as a heading anchor, not a line', () => {
    expect(parseFragment('l2')).toEqual({ anchor: 'l2' });
  });

  it('reads a spreadsheet cell or range, keeping the anchor beside it', () => {
    expect(parseFragment('B7')).toEqual({ cell: 'B7', anchor: 'B7' });
    expect(parseFragment('Model!B7')).toEqual({ cell: 'Model!B7', anchor: 'Model!B7' });
    expect(parseFragment('Model!B4:D9')).toEqual({ cell: 'Model!B4:D9', anchor: 'Model!B4:D9' });
    expect(parseFragment("'My Sheet'!B4:D9")).toEqual({
      cell: "'My Sheet'!B4:D9",
      anchor: "'My Sheet'!B4:D9",
    });
  });

  it('leaves prose that merely looks addressable as a plain anchor', () => {
    // Lowercase is prose far more often than it is a column, and `L42` is a
    // line fragment before it is ever a cell.
    expect(parseFragment('b7')).toEqual({ anchor: 'b7' });
    expect(parseFragment('L42')).toEqual({ line: 42 });
    expect(parseFragment('Q3')).toEqual({ cell: 'Q3', anchor: 'Q3' });
    expect(parseFragment('risks')).toEqual({ anchor: 'risks' });
    expect(parseFragment('B0')).toEqual({ anchor: 'B0' });
  });

  it('keeps anything else as a decoded anchor', () => {
    expect(parseFragment('risks-1')).toEqual({ anchor: 'risks-1' });
    expect(parseFragment('%E4%BC%B0%E5%80%BC')).toEqual({ anchor: '估值' });
    expect(parseFragment('')).toBeNull();
  });
});

describe('splitFileLocation', () => {
  it('splits a fragment off the path', () => {
    expect(splitFileLocation('results/report.md#valuation')).toEqual({
      path: 'results/report.md',
      location: { anchor: 'valuation' },
    });
    expect(splitFileLocation('filing.pdf#page=2')).toEqual({ path: 'filing.pdf', location: { page: 2 } });
    expect(splitFileLocation('models/dcf.xlsx#Model!B7')).toEqual({
      path: 'models/dcf.xlsx',
      location: { cell: 'Model!B7', anchor: 'Model!B7' },
    });
  });

  it('splits a line suffix, ignoring a column', () => {
    expect(splitFileLocation('work/model.py:42')).toEqual({ path: 'work/model.py', location: { line: 42 } });
    expect(splitFileLocation('model.py:40-55')).toEqual({ path: 'model.py', location: { line: 40, lineEnd: 55 } });
    expect(splitFileLocation('model.py:42:7')).toEqual({ path: 'model.py', location: { line: 42 } });
  });

  it('leaves a plain path, a port or an extensionless name alone', () => {
    expect(splitFileLocation('results/report.md')).toEqual({ path: 'results/report.md', location: null });
    expect(splitFileLocation('localhost:8000')).toEqual({ path: 'localhost:8000', location: null });
    expect(splitFileLocation('Makefile:12')).toEqual({ path: 'Makefile:12', location: null });
  });

  it('never reads a numeric host and port as a line', () => {
    expect(hasLineSuffix('127.0.0.1:8000')).toBe(false);
    expect(hasLineSuffix('10.0.0.12:443')).toBe(false);
    expect(hasLineSuffix('notes.v2:12')).toBe(true);
  });
});

describe('heading anchors', () => {
  it('slugs headings the way GitHub does, numbering repeats', () => {
    expect(headingSlugs(['Risks', 'Price & Volume', 'Risks', 'Risks'])).toEqual([
      'risks',
      'price--volume',
      'risks-1',
      'risks-2',
    ]);
  });

  it('finds a heading whose slug doubles a hyphen from a single-hyphen anchor', () => {
    expect(findHeadingIndex(['Overview', 'Revenue & Margin'], 'revenue-margin')).toBe(1);
  });

  it('finds a heading by slug, by written text, or through the sanitizer prefix', () => {
    const headings = ['Summary', 'Valuation', 'Risks', 'Risks'];
    expect(findHeadingIndex(headings, 'valuation')).toBe(1);
    expect(findHeadingIndex(headings, 'Valuation')).toBe(1);
    expect(findHeadingIndex(headings, 'risks-1')).toBe(3);
    expect(findHeadingIndex(headings, 'user-content-summary')).toBe(0);
    expect(findHeadingIndex(headings, 'missing')).toBe(-1);
  });
});

describe('countLines', () => {
  it('does not count a trailing newline as a line', () => {
    expect(countLines('')).toBe(0);
    expect(countLines('a\nb')).toBe(2);
    expect(countLines('a\nb\n')).toBe(2);
  });
});

describe('markdown line mapping', () => {
  const source = [
    '# Report', // 1
    '', // 2
    'Revenue grew **14%** on [strong demand](notes.md).', // 3
    '', // 4
    '## Valuation', // 5
    '', // 6
    '- [x] EV/EBITDA of 11.4x', // 7
    '', // 8
    '```python', // 9
    'print("## not a heading")', // 10
    '```', // 11
  ].join('\n');

  it('probes with the longest plain run of the source line', () => {
    expect(markdownLineProbe(source, 3)).toBe('on strong demand.');
    expect(markdownLineProbe(source, 5)).toBe('Valuation');
    expect(markdownLineProbe(source, 7)).toBe('EV/EBITDA of 11.4x');
  });

  it('skips forward over a blank line and keeps code verbatim', () => {
    expect(markdownLineProbe(source, 2)).toBe(markdownLineProbe(source, 3));
    expect(markdownLineProbe(source, 10)).toBe('print("## not a heading")');
    expect(markdownLineProbe(source, 99)).toBeNull();
  });

  it('finds the nearest heading above a line, ignoring fenced code', () => {
    expect(headingIndexAbove(source, 3)).toBe(0);
    expect(headingIndexAbove(source, 7)).toBe(1);
    expect(headingIndexAbove(source, 11)).toBe(1);
    expect(headingIndexAbove(source, 0)).toBe(-1);
  });
});

describe('a fence closes only on its own run', () => {
  // CommonMark closes a block on a run of the opener's own character, at least
  // as long, carrying no info string. Toggling on any fence-like line ended the
  // outer block at the first example inside it, and the example's headings then
  // counted as rendered ones, so a `#L` reference landed in the wrong section.
  const shorterRunInside = [
    '# Report',             // 1
    '',                     // 2
    '````md',               // 3
    '```python',            // 4
    '# not a heading',      // 5
    '```',                  // 6
    '````',                 // 7
    '',                     // 8
    '## Risks',             // 9
    '',                     // 10
    'Exposure is limited.', // 11
  ].join('\n');

  const otherFenceCharacter = [
    '# Report',             // 1
    '```',                  // 2
    '~~~',                  // 3
    '# not a heading',      // 4
    '~~~',                  // 5
    '```',                  // 6
    '',                     // 7
    '## Risks',             // 8
  ].join('\n');

  const runWithAnInfoString = [
    '# Report',             // 1
    '```',                  // 2
    '```python',            // 3
    '# not a heading',      // 4
    '```',                  // 5
    '',                     // 6
    '## Risks',             // 7
  ].join('\n');

  it('keeps a ``` example inside a ```` block', () => {
    expect(countHeadings(shorterRunInside)).toBe(2);
    expect(headingIndexAbove(shorterRunInside, 5)).toBe(0);
    expect(headingIndexAbove(shorterRunInside, 11)).toBe(1);
  });

  it('keeps a ~~~ run inside a ``` block', () => {
    expect(countHeadings(otherFenceCharacter)).toBe(2);
    expect(headingIndexAbove(otherFenceCharacter, 4)).toBe(0);
  });

  it('does not close on a run carrying an info string', () => {
    expect(countHeadings(runWithAnInfoString)).toBe(2);
    expect(headingIndexAbove(runWithAnInfoString, 5)).toBe(0);
  });

  it('still closes on a longer run of the same character', () => {
    // The control: the rule is at-least-as-long, not exactly-as-long, so this
    // block ends and the heading after it is a heading.
    const source = ['# Report', '````', 'x', '``````', '', '## Risks'].join('\n');
    expect(countHeadings(source)).toBe(2);
  });
});

describe('markdownLineTarget', () => {
  const source = [
    '# FY2024', // 1
    'Revenue grew 12% year over year.', // 2
    '# FY2025', // 3
    'Margins held.', // 4
    'Revenue grew 12% year over year.', // 5
    '', // 6
    'Revenue grew 12% year over year.', // 7
  ].join('\n');

  it('places a repeated sentence by its section and its position among repeats', () => {
    expect(markdownLineTarget(source, 2)).toEqual({
      probe: 'Revenue grew 12% year over year.', section: 0, repeatsInSection: 0, repeatsInDocument: 0,
    });
    expect(markdownLineTarget(source, 5)).toMatchObject({ section: 1, repeatsInSection: 0, repeatsInDocument: 1 });
    expect(markdownLineTarget(source, 6)).toMatchObject({ section: 1, repeatsInSection: 1, repeatsInDocument: 2 });
  });

  it('counts a paragraph wrapped over several lines as one block', () => {
    const wrapped = ['Revenue grew 12%', 'Revenue grew 12% again', '', 'Revenue grew 12%'].join('\n');
    expect(markdownLineTarget(wrapped, 2)).toMatchObject({ repeatsInDocument: 0 });
    expect(markdownLineTarget(wrapped, 4)).toMatchObject({ repeatsInDocument: 1 });
  });

  it('leaves front matter out of the heading count', () => {
    const withFrontMatter = ['---', '# owner: research', '---', '# Summary', 'Body text here.'].join('\n');
    expect(headingIndexAbove(withFrontMatter, 5)).toBe(0);
    expect(markdownLineTarget(withFrontMatter, 5)).toMatchObject({ section: 0 });
  });

  it('counts a heading line as the start of its own section', () => {
    expect(markdownLineTarget(source, 3)).toMatchObject({ probe: 'FY2025', section: 1, repeatsInSection: 0 });
  });
});
