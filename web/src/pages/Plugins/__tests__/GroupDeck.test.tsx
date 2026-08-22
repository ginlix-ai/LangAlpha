import { describe, it, expect, beforeEach } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Server } from 'lucide-react';
import { renderWithProviders } from '@/test/utils';
import { GroupDeck } from '../components/GroupDeck';
import type { BulkSelection } from '../components/useBulkSelection';

/**
 * The origin-deck contract: small groups render flat, larger groups stack
 * into a summary card until expanded, and filter/select mode force every row
 * visible. Expansion persists per deck id in localStorage.
 */

function rows(n: number) {
  return Array.from({ length: n }, (_, i) => (
    <div key={i} data-testid={`row-${i}`}>
      row {i}
    </div>
  ));
}

function fakeSelection(selected: string[] = []): BulkSelection & { calls: string[] } {
  const calls: string[] = [];
  return {
    selecting: true,
    selected: new Set(selected),
    start: () => {},
    exit: () => {},
    toggle: () => {},
    setMany: (keys, on) => calls.push(`${on ? 'on' : 'off'}:${keys.join(',')}`),
    calls,
  };
}

beforeEach(() => localStorage.clear());

describe('GroupDeck', () => {
  it('renders nothing for an empty group', () => {
    renderWithProviders(
      <GroupDeck id="t:empty" title="Empty" icon={Server} count={0}>
        {rows(0)}
      </GroupDeck>,
    );
    expect(screen.queryByTestId('deck-t:empty')).not.toBeInTheDocument();
  });

  it('renders small groups flat, with no collapse affordance', () => {
    renderWithProviders(
      <GroupDeck id="t:small" title="Small" icon={Server} count={3}>
        {rows(3)}
      </GroupDeck>,
    );
    expect(screen.getByTestId('row-0')).toBeInTheDocument();
    expect(screen.getByTestId('row-2')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /collapse small/i })).not.toBeInTheDocument();
  });

  it('stacks a 4+ group by default and expands on click', () => {
    renderWithProviders(
      <GroupDeck id="t:big" title="Big group" icon={Server} count={5} enabledCount={2}>
        {rows(5)}
      </GroupDeck>,
    );
    // Collapsed: summary card with tally, rows hidden.
    expect(screen.queryByTestId('row-0')).not.toBeInTheDocument();
    expect(screen.getByText('2 on · 3 off')).toBeInTheDocument();
    expect(screen.getByTestId('deck-count-t:big')).toHaveTextContent('5');

    fireEvent.click(screen.getByRole('button', { name: /expand big group/i }));

    expect(screen.getByTestId('row-0')).toBeInTheDocument();
    expect(screen.getByTestId('row-4')).toBeInTheDocument();
    // The choice persists per deck id.
    expect(JSON.parse(localStorage.getItem('plugins.deckExpanded')!)).toEqual({
      't:big': true,
    });
  });

  it('collapses back from the expanded header', () => {
    renderWithProviders(
      <GroupDeck id="t:big" title="Big group" icon={Server} count={4} defaultExpanded>
        {rows(4)}
      </GroupDeck>,
    );
    expect(screen.getByTestId('row-0')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /collapse big group/i }));
    expect(screen.queryByTestId('row-0')).not.toBeInTheDocument();
  });

  it('forceExpanded overrides a stored collapse and locks the header open', () => {
    localStorage.setItem('plugins.deckExpanded', JSON.stringify({ 't:big': false }));
    renderWithProviders(
      <GroupDeck id="t:big" title="Big group" icon={Server} count={6} forceExpanded>
        {rows(6)}
      </GroupDeck>,
    );
    expect(screen.getByTestId('row-5')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /collapse big group/i })).toBeDisabled();
  });

  it('select-all box reflects partial selection and selects the remainder', () => {
    const selection = fakeSelection(['k1']);
    renderWithProviders(
      <GroupDeck
        id="t:sel"
        title="Sel"
        icon={Server}
        count={2}
        selection={selection}
        selectionKeys={['k1', 'k2']}
      >
        {rows(2)}
      </GroupDeck>,
    );
    const box = screen.getByRole('checkbox', { name: /select all in sel/i });
    expect(box).toHaveAttribute('aria-checked', 'mixed');
    fireEvent.click(box);
    expect(selection.calls).toEqual(['on:k1,k2']);
  });
});
