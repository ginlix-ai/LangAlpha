/**
 * The row says the summary is the plan, so the override is a quiet action, and
 * the cost shows only once the user has taken it. At the threshold the summary
 * has to run, and so does a rebuild, so there is nothing to take.
 */
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { NextSummaryRow } from '../NextSummaryRow';

describe('NextSummaryRow', () => {
  it('states the cold summary as the plan, with Skip beside it', () => {
    const onToggleSkip = vi.fn();
    renderWithProviders(<NextSummaryRow reason="cold" skipped={false} onToggleSkip={onToggleSkip} />);

    expect(screen.getByRole('status')).toHaveTextContent('Earlier messages will be summarized before your next one.');
    expect(screen.queryByText(/costs more/)).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Skip' }));
    expect(onToggleSkip).toHaveBeenCalledOnce();
  });

  it('after Skip, names the cost and offers Undo', () => {
    const onToggleSkip = vi.fn();
    renderWithProviders(<NextSummaryRow reason="cold" skipped onToggleSkip={onToggleSkip} />);

    expect(screen.getByRole('status')).toHaveTextContent(
      'Your next message will include the whole conversation, which costs more.',
    );
    fireEvent.click(screen.getByRole('button', { name: 'Undo' }));
    expect(onToggleSkip).toHaveBeenCalledOnce();
  });

  it.each(['threshold', 'rebuild'] as const)('for %s says so with nothing to skip', (reason) => {
    renderWithProviders(<NextSummaryRow reason={reason} skipped={false} onToggleSkip={() => {}} />);

    expect(screen.getByRole('status')).toHaveTextContent('Earlier messages will be summarized before your next one.');
    expect(screen.queryByRole('button')).toBeNull();
  });
});
