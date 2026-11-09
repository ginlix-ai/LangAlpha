import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import ThumbDownModal from '../ThumbDownModal';

const state = vi.hoisted(() => ({ reports: undefined as boolean | undefined }));
vi.mock('@/hooks/useReportsEnabled', () => ({ useReportsState: () => state.reports }));

function open(reports: boolean | undefined) {
  state.reports = reports;
  renderWithProviders(<ThumbDownModal isOpen onSubmit={vi.fn()} onCancel={vi.fn()} />);
}

// The server forwards a thumbs-down whenever the deployment takes reports, so
// the dialog says it goes to the team unless that is known not to be so.
describe('ThumbDownModal: what it says about where the rating goes', () => {
  it('says the team gets it while the setting is not known, with no review offered', () => {
    open(undefined);
    expect(screen.getByText(/goes to our team/)).toBeInTheDocument();
    expect(screen.queryByText('Request human review')).not.toBeInTheDocument();
  });

  it('offers a review where reports reach the team', () => {
    open(true);
    expect(screen.getByText(/goes to our team/)).toBeInTheDocument();
    expect(screen.getByText('Request human review')).toBeInTheDocument();
  });

  it('keeps it local where the deployment takes no reports', () => {
    open(false);
    expect(screen.getByText('Help us improve the quality of responses')).toBeInTheDocument();
    expect(screen.queryByText('Request human review')).not.toBeInTheDocument();
  });
});
