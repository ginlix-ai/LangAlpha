import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import type { CatalogServer, CatalogServerList } from '../../../utils/api';

// ---------------------------------------------------------------------------
// Drive the catalog list + mutation outcomes directly.
// ---------------------------------------------------------------------------

const mutateAsync = {
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
};

let catalog: CatalogServerList | undefined;

vi.mock('@/hooks/useMcpServers', () => ({
  useMcpCatalog: () => ({ data: catalog, isLoading: false, error: null }),
  useCreateMcpCatalogServer: () => ({ mutateAsync: mutateAsync.create, isPending: false }),
  useUpdateMcpCatalogServer: () => ({ mutateAsync: mutateAsync.update, isPending: false }),
  useDeleteMcpCatalogServer: () => ({ mutateAsync: mutateAsync.del, isPending: false }),
}));

// getVaultSecrets / createVaultSecret feed the secret picker inside the modal;
// keep them benign so nothing hits a real backend.
vi.mock('../../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, getVaultSecrets: vi.fn().mockResolvedValue([]), createVaultSecret: vi.fn() };
});

// Error feedback is a toast — mock the module so we can assert it was raised.
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

import { TemplatesView } from '../TemplatesView';
import { toast } from '@/components/ui/use-toast';

function makeTemplate(name: string, overrides: Partial<CatalogServer> = {}): CatalogServer {
  return {
    name,
    transport: 'stdio',
    command: 'npx',
    args: [],
    url: null,
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

function makeCatalog(servers: CatalogServer[], maxServers = 20): CatalogServerList {
  return { servers, max_servers: maxServers };
}

const baseProps = {
  workspaceId: 'ws-1',
  secretNames: [] as string[],
  onAddToWorkspace: vi.fn().mockResolvedValue(undefined),
  workspaceServerNames: new Set<string>(),
};

beforeEach(() => {
  vi.clearAllMocks();
  catalog = makeCatalog([]);
});

describe('TemplatesView — add-to-workspace error surfacing (FIX 3)', () => {
  it('surfaces a toast (and does not throw) when onAddToWorkspace rejects', async () => {
    catalog = makeCatalog([makeTemplate('svc')]);
    const onAddToWorkspace = vi
      .fn()
      .mockRejectedValue({ response: { data: { detail: 'add failed' } } });
    renderWithProviders(
      <TemplatesView {...baseProps} onAddToWorkspace={onAddToWorkspace} />,
    );

    fireEvent.click(await screen.findByRole('button', { name: /add to workspace/i }));

    await waitFor(() => expect(onAddToWorkspace).toHaveBeenCalledWith('svc'));
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({ variant: 'destructive', description: 'add failed' }),
      ),
    );
  });

  it('does not toast when onAddToWorkspace resolves', async () => {
    catalog = makeCatalog([makeTemplate('svc')]);
    renderWithProviders(<TemplatesView {...baseProps} />);

    fireEvent.click(await screen.findByRole('button', { name: /add to workspace/i }));

    await waitFor(() => expect(baseProps.onAddToWorkspace).toHaveBeenCalledWith('svc'));
    expect(toast).not.toHaveBeenCalled();
  });
});

describe('TemplatesView — delete-confirm error surfacing (FIX 3)', () => {
  it('surfaces a toast (and does not throw) when delete rejects', async () => {
    catalog = makeCatalog([makeTemplate('svc')]);
    mutateAsync.del.mockRejectedValue({ response: { data: { detail: 'delete failed' } } });
    renderWithProviders(<TemplatesView {...baseProps} />);

    // Open the delete confirm, then confirm.
    fireEvent.click(await screen.findByRole('button', { name: /delete svc/i }));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));

    await waitFor(() => expect(mutateAsync.del).toHaveBeenCalledWith('svc'));
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({ variant: 'destructive', description: 'delete failed' }),
      ),
    );
  });

  it('dismisses the confirm and does not toast when delete succeeds', async () => {
    catalog = makeCatalog([makeTemplate('svc')]);
    mutateAsync.del.mockResolvedValue({});
    renderWithProviders(<TemplatesView {...baseProps} />);

    fireEvent.click(await screen.findByRole('button', { name: /delete svc/i }));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));

    await waitFor(() => expect(mutateAsync.del).toHaveBeenCalledWith('svc'));
    // Confirm clears back to the trash affordance; no error toast.
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /delete svc/i })).toBeInTheDocument(),
    );
    expect(toast).not.toHaveBeenCalled();
  });
});
