/**
 * A rendered markdown file has no line numbers, so a line reference is found by
 * its text. Research notes repeat sentences across periods, and the highlight
 * has to land on the one the reference named rather than the first lookalike.
 */
import React, { useEffect, useRef } from 'react';
import { describe, it, expect } from 'vitest';
import { render, waitFor } from '@testing-library/react';
import { useFileFocus } from '../useFileFocus';
import type { FileLocation } from '../../../utils/fileLocation';

const SOURCE = [
  '# FY2024', // 1
  'Revenue grew 12% year over year.', // 2
  '# FY2025', // 3
  'Margins held.', // 4
  'Revenue grew 12% year over year.', // 5
  '![chart](c.png)', // 6
].join('\n');

const HTML = `<h1>FY2024</h1><p id="a">Revenue grew 12% year over year.</p>
<h1>FY2025</h1><p id="b">Margins held.</p><p id="c">Revenue grew 12% year over year.</p><p id="d"><img alt=""></p>`;
// One object per markup for every render: a fresh one makes React rewrite the markup and drop the highlight.
const MARKUP = { __html: HTML };
// Headings that do not line up with the source leave no section to fall back on.
const UNALIGNED = { __html: '<p>Margins held.</p><p><img alt=""></p>' };
const WITH_FOOTNOTES = {
  __html: `${HTML}<section data-footnotes class="footnotes"><h2 class="sr-only">Footnotes</h2><ol><li>Source.</li></ol></section>`,
};

interface ViewerProps {
  location: FileLocation;
  onChip: (chip: unknown) => void;
  markup?: { __html: string };
  truncated?: boolean;
}

function Viewer({ location, onChip, markup = MARKUP, truncated = false }: ViewerProps) {
  const ref = useRef<HTMLDivElement>(null);
  const focus = useFileFocus({
    selectedFile: 'notes.md',
    viewer: 'markdown',
    ready: true,
    editing: false,
    content: SOURCE,
    truncated,
    pageCount: null,
    containerRef: ref,
  });
  const { focusAt } = focus;
  useEffect(() => focusAt('notes.md', location), [focusAt, location]);
  onChip(focus.chip);
  return <div ref={ref} dangerouslySetInnerHTML={markup} />;
}

function highlighted(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll('.file-focus-block')).map((el) => el.id || el.tagName);
}

describe('useFileFocus on a markdown file', () => {
  it('highlights the repeated sentence in the section the line sits in', async () => {
    const { container } = render(<Viewer location={{ line: 5 }} onChip={() => {}} />);
    await waitFor(() => expect(highlighted(container)).toEqual(['c']));
  });

  it('still maps sections when GFM adds a footnote heading the source never wrote', async () => {
    const { container } = render(<Viewer location={{ line: 6 }} markup={WITH_FOOTNOTES} onChip={() => {}} />);
    await waitFor(() => expect(highlighted(container)).toEqual(['H1']));
    expect(container.querySelectorAll('h1')[1].classList.contains('file-focus-block')).toBe(true);
  });

  it('says a line past a truncated preview is out of reach instead of offering a jump', () => {
    let chip: unknown = null;
    render(<Viewer location={{ line: 25000 }} truncated onChip={(c) => { chip = c; }} />);
    expect(chip).toMatchObject({ kind: 'line', missing: true, beyond: true });
  });

  it('says a line it could not match stays unplaced instead of claiming a landing', async () => {
    let chip: unknown = null;
    render(<Viewer location={{ line: 6 }} markup={UNALIGNED} onChip={(c) => { chip = c; }} />);
    await waitFor(() => expect(chip).toMatchObject({ kind: 'line', line: 6, missing: true, unplaced: true }), { timeout: 3000 });
  });

  it('marks the landing as near when only the section could be found', async () => {
    let chip: unknown = null;
    const { container } = render(<Viewer location={{ line: 6 }} onChip={(c) => { chip = c; }} />);
    await waitFor(() => expect(highlighted(container)).toEqual(['H1']));
    await waitFor(() => expect(chip).toMatchObject({ kind: 'line', line: 6, near: true }));
  });

  it('ends the range it advertises at the last line there is', () => {
    // `#L2-L999` against a six-line file: the highlight was always going to stop
    // at line 6, and the chip used to say 999 anyway.
    let chip: unknown = null;
    render(<Viewer location={{ line: 2, lineEnd: 999 }} onChip={(c) => { chip = c; }} />);
    expect(chip).toMatchObject({ kind: 'line', line: 2, lineEnd: 6, missing: false });
  });

  it('drops a range whose end clamps back to its own start', () => {
    // One line is not a range, so the chip names the line rather than `6-6`.
    let chip: unknown = null;
    render(<Viewer location={{ line: 6, lineEnd: 999 }} onChip={(c) => { chip = c; }} />);
    expect(chip).toMatchObject({ kind: 'line', line: 6 });
    expect((chip as { lineEnd?: number }).lineEnd).toBeUndefined();
  });
});
