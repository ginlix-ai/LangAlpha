import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { BulkActionBar, type BulkAction } from '../components/BulkActionBar';

/**
 * Select mode opens with nothing selected, which used to put a row of actions
 * on screen that all read zero. The bar has to say what it wants first, and
 * only then offer the verbs.
 */

function bulkAction(id: string, label: string, extra: Partial<BulkAction> = {}): BulkAction {
  return { id, label, run: vi.fn(), ...extra };
}

// Built per test: a shared fixture carries its handlers' call history from one
// test into the next, so a run that never pressed a button can still see one.
const zeroActions = (): BulkAction[] => [
  bulkAction('enable', 'Enable 0'),
  bulkAction('delete', 'Delete 0', { destructive: true }),
];
const twoActions = (): BulkAction[] => [
  bulkAction('enable', 'Enable 2'),
  bulkAction('delete', 'Delete 2', { destructive: true, confirmMessage: 'Delete 2 items? This cannot be undone.' }),
];
const oneAction = (): BulkAction[] => [
  bulkAction('enable', 'Enable 1'),
  bulkAction('delete', 'Delete 1', { destructive: true, confirmMessage: 'Delete 1 item? This cannot be undone.' }),
];

describe('BulkActionBar with nothing selected', () => {
  it('asks for a selection instead of offering the zeroed actions', () => {
    render(<BulkActionBar count={0} selectionKey="" actions={zeroActions()} progress={null} onExit={vi.fn()} />);

    expect(screen.getByText('Select rows to act on them')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Enable 0' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Delete 0' })).toBeNull();
    expect(screen.queryByText('0 selected')).toBeNull();
  });

  it('keeps the way out of select mode', () => {
    const onExit = vi.fn();
    render(<BulkActionBar count={0} selectionKey="" actions={zeroActions()} progress={null} onExit={onExit} />);

    fireEvent.click(screen.getByRole('button', { name: /cancel/i }));
    expect(onExit).toHaveBeenCalled();
  });

  it('brings the actions back as soon as a row is picked', () => {
    const { rerender } = render(<BulkActionBar count={0} selectionKey="" actions={zeroActions()} progress={null} onExit={vi.fn()} />);
    rerender(<BulkActionBar count={2} selectionKey="a,b" actions={twoActions()} progress={null} onExit={vi.fn()} />);

    expect(screen.getByText('2 selected')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Enable 2' })).toBeInTheDocument();
    expect(screen.queryByText('Select rows to act on them')).toBeNull();
  });

  it('disarms a confirm the selection was emptied under', () => {
    const actions = twoActions();
    const { rerender } = render(
      <BulkActionBar count={2} selectionKey="a,b" actions={actions} progress={null} onExit={vi.fn()} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(screen.getByText('Delete 2 items? This cannot be undone.')).toBeInTheDocument();

    // Clearing the selection hides the strip behind the empty arm. Picking rows
    // again must come back to the verbs, not to a confirm nobody re-initiated.
    rerender(<BulkActionBar count={0} selectionKey="" actions={zeroActions()} progress={null} onExit={vi.fn()} />);
    rerender(<BulkActionBar count={2} selectionKey="a,b" actions={actions} progress={null} onExit={vi.fn()} />);

    expect(screen.queryByText('Delete 2 items? This cannot be undone.')).toBeNull();
    expect(screen.getByRole('button', { name: 'Enable 2' })).toBeInTheDocument();
    expect(actions[1].run).not.toHaveBeenCalled();
  });

  it('disarms when the selection moves out from under an armed confirm', () => {
    const onAB = twoActions();
    const { rerender } = render(
      <BulkActionBar count={2} selectionKey="a,b" actions={onAB} progress={null} onExit={vi.fn()} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(screen.getByText('Delete 2 items? This cannot be undone.')).toBeInTheDocument();

    // A and B are deselected and C picked. The selection never passes through
    // empty, so the strip used to stay open still naming two rows, and the red
    // button still held the delete built for A and B.
    const onC = oneAction();
    rerender(
      <BulkActionBar count={1} selectionKey="c" actions={onC} progress={null} onExit={vi.fn()} />,
    );
    // Not merely re-labelled: the strip is gone, because the user never armed
    // anything about C.
    expect(screen.queryByText(/cannot be undone/i)).toBeNull();

    // Confirming from here runs the action built for the row now selected.
    fireEvent.click(screen.getByRole('button', { name: 'Delete 1' }));
    fireEvent.click(screen.getByRole('button', { name: 'Delete 1' }));
    expect(onC[1].run).toHaveBeenCalled();
    expect(onAB[1].run).not.toHaveBeenCalled();
  });

  it('refuses an armed action the tab has since disabled', () => {
    const actions = twoActions();
    const { rerender } = render(
      <BulkActionBar count={2} selectionKey="a,b" actions={actions} progress={null} onExit={vi.fn()} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(screen.getByText('Delete 2 items? This cannot be undone.')).toBeInTheDocument();

    // Same rows, but delete is no longer offered on them: the confirm is a
    // press away from running an action the bar would not offer.
    const withdrawn = [actions[0], { ...actions[1], disabled: true }];
    rerender(
      <BulkActionBar count={2} selectionKey="a,b" actions={withdrawn} progress={null} onExit={vi.fn()} />,
    );
    expect(screen.queryByText('Delete 2 items? This cannot be undone.')).toBeNull();
    expect(screen.getByRole('button', { name: 'Delete 2' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(actions[1].run).not.toHaveBeenCalled();
  });

  it('still confirms a destructive action, cancel first', () => {
    const actions = twoActions();
    render(<BulkActionBar count={2} selectionKey="a,b" actions={actions} progress={null} onExit={vi.fn()} />);

    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(screen.getByText('Delete 2 items? This cannot be undone.')).toBeInTheDocument();
    expect(actions[1].run).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Delete 2' }));
    expect(actions[1].run).toHaveBeenCalled();
  });
});
