/**
 * A subagent type is the user's own name for it, so it can be any string,
 * including the name of something every object inherits.
 */
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { SubagentGlyph } from '../subagentGlyph';

const glyphOf = (type: string | undefined): SVGElement | null =>
  render(<SubagentGlyph type={type} className="glyph" />).container.querySelector('svg.glyph');

describe('SubagentGlyph', () => {
  it('draws a built-in role as its icon', () => {
    expect(glyphOf('research')?.classList.contains('lucide')).toBe(true);
  });

  it.each(['constructor', 'toString', '__proto__', 'hasOwnProperty', 'my-analyst', undefined])(
    'draws the rings for a type it has no icon of: %s',
    (type) => {
      const svg = glyphOf(type);
      expect(svg).not.toBeNull();
      expect(svg?.classList.contains('lucide')).toBe(false);
    },
  );
});
