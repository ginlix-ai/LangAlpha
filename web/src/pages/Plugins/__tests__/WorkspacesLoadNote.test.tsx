import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

const query = vi.hoisted(() => ({
  data: undefined as { workspaces: unknown[] } | undefined,
  isError: false,
  refetch: vi.fn(),
}));

vi.mock('@/hooks/useAllWorkspaces', () => ({ useAllWorkspaces: () => query }));
vi.mock('@/hooks/useHomeWorkspace', () => ({ useHomeWorkspaceId: () => undefined }));

import { WorkspacesLoadNote } from '../components/WorkspacesLoadNote';

describe('WorkspacesLoadNote', () => {
  beforeEach(() => {
    query.data = undefined;
    query.isError = false;
    query.refetch.mockClear();
  });

  it('says why the scope controls are missing and retries the load', () => {
    // The controls stay hidden without a list, so a failed load has to be said.
    query.isError = true;
    renderWithProviders(<WorkspacesLoadNote />);

    expect(screen.getByText(/Couldn't load your workspaces/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(query.refetch).toHaveBeenCalledTimes(1);
  });

  it('stays out while the list is loading or in hand', () => {
    const { container, rerender } = renderWithProviders(<WorkspacesLoadNote />);
    expect(container).toBeEmptyDOMElement();

    query.data = { workspaces: [] };
    rerender(<WorkspacesLoadNote />);
    expect(container).toBeEmptyDOMElement();
  });
});
