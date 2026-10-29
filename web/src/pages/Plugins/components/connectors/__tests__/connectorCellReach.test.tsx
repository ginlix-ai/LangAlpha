import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { catalogServer } from '@/test/factories';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { ConnectorCell } from '../ConnectorCells';

vi.mock('@/hooks/useHomeWorkspace', () => ({ useHomeWorkspaceId: () => undefined }));

/**
 * A cell names where a connector is on only when that is not everywhere, in
 * the words its page's scope badge uses. One that new workspaces start without
 * is not everywhere even while every current workspace has it: the next
 * workspace will not.
 */
const workspaces = [
  { id: 'ws-a', name: 'Alpha' },
  { id: 'ws-b', name: 'Beta' },
  { id: 'ws-c', name: 'Gamma' },
];

function cell(over: Partial<CatalogServer>) {
  return renderWithProviders(
    <ConnectorCell
      server={catalogServer({ name: 'notes', ...over })}
      vendor={null}
      workspaces={workspaces}
      workspacesLoading={false}
      connecting={false}
      onOpen={() => {}}
      onConnect={() => {}}
    />,
  );
}

describe('a connector cell names its reach', () => {
  it('counts the workspaces it is on in when new workspaces start without it', () => {
    cell({ enabled_in_new_workspaces: false, disabled_workspace_ids: [] });
    expect(screen.getByText('3 workspaces')).toBeInTheDocument();
  });

  it('says nothing when it is on everywhere, new workspaces included', () => {
    cell({ enabled_in_new_workspaces: true, disabled_workspace_ids: [] });
    expect(screen.queryByText(/workspace/i)).toBeNull();
  });

  it('words an exception the way the scope badge does', () => {
    cell({ enabled_in_new_workspaces: true, disabled_workspace_ids: ['ws-b'] });
    expect(screen.getByText('All workspaces except 1')).toBeInTheDocument();
  });
});
