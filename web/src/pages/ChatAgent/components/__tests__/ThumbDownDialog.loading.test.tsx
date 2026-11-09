import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { ThumbDownDialog } from '../ThumbDownSentDialog';

vi.mock('@/hooks/useReportsEnabled', () => ({ useReportsState: () => true, useReportsEnabled: () => true }));

const props = { onSubmit: vi.fn(), onCancel: vi.fn(), onFollowUp: vi.fn(), onWithdrawn: vi.fn() };

// A rated turn reads as unrated until the thread's ratings load, so a dialog
// opened then waits instead of offering the form for a report already sent.
describe('ThumbDownDialog while the ratings load', () => {
  it('opens nothing until the turn\'s rating is known', () => {
    renderWithProviders(<ThumbDownDialog loading sent={null} {...props} />);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('offers the form once the turn is known to be unrated', () => {
    renderWithProviders(<ThumbDownDialog loading={false} sent={null} {...props} />);
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
