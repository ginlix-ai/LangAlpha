import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SurfaceGrid } from '../components/surface/Surface';

/**
 * A group unfolds to show what was just added, and the mark that asks for it
 * fades after a few seconds. Folding back then pulled the new cell out from
 * under the pointer, so a reveal holds, as a pressed "See …" does.
 */

const NAMES = ['a', 'b', 'c', 'd', 'e'];

function grid(reveal: boolean) {
  // jsdom lays out no columns, so the fold counts one per row.
  return (
    <SurfaceGrid rows={2} hiddenNames={NAMES} reveal={reveal}>
      {NAMES.map((name) => (
        <div key={name}>cell {name}</div>
      ))}
    </SurfaceGrid>
  );
}

describe('SurfaceGrid reveal', () => {
  it('stays unfolded after the reveal is withdrawn', () => {
    const { rerender } = render(grid(false));
    expect(screen.queryByText('cell e')).toBeNull();

    rerender(grid(true));
    expect(screen.getByText('cell e')).toBeInTheDocument();

    rerender(grid(false));
    expect(screen.getByText('cell e')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });
});
