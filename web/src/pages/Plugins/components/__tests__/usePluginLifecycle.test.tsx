import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import type { ReactNode } from 'react';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { ApiError } from '@/api/client';
import { toast } from '@/components/ui/use-toast';
import { getMarketplace, type MarketplaceResponse } from '@/pages/ChatAgent/utils/api/marketplace';
import {
  deletePlugin,
  updatePlugin,
  type PluginInfo,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api/plugins';
import type { SubagentView, SubagentsResponse } from '@/pages/ChatAgent/utils/api/subagents';
import { usePluginLifecycle } from '../pluginsPage/usePluginLifecycle';

vi.mock('@/pages/ChatAgent/utils/api/plugins', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/plugins')>()),
  updatePlugin: vi.fn(),
  deletePlugin: vi.fn(),
}));

vi.mock('@/pages/ChatAgent/utils/api/marketplace', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/marketplace')>()),
  getMarketplace: vi.fn(),
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

const plugin: PluginInfo = {
  name: 'acme',
  version: '1.0.0',
  description: '',
  author: null,
  homepage: null,
  source_type: 'git',
  source_ref: 'https://github.com/acme/acme-plugin',
  enabled: true,
  installed_at: null,
  updated_at: null,
  components: [{ kind: 'mcp', name: 'acme', key: 'acme' }],
};

const updated: PluginInstallResponse = {
  plugin: { ...plugin, version: '1.1.0' },
  report: {
    components: [],
    diagnostics: [],
    secrets_created: ['ACME_TOKEN'],
    secrets_required: [],
    dropped_files: [],
    servers_created: 0,
    skills_created: 0,
  },
};

function Harness() {
  const lifecycle = usePluginLifecycle(plugin);
  return (
    <>
      <button type="button" onClick={lifecycle.update}>
        Update acme
      </button>
      {lifecycle.updateAvailable && <p>newer in the catalog</p>}
      {lifecycle.dialogs}
    </>
  );
}

/** The catalog, listing `acme` as installed from `sourceUrl`. */
function catalogListing(sourceUrl: string, updateAvailable: boolean): MarketplaceResponse {
  return {
    configured: true,
    name: null,
    description: null,
    url: null,
    vocabulary: { assets: [], regions: [], data: [] },
    entries: [
      {
        name: 'acme',
        description: '',
        category: null,
        license: null,
        publisher: null,
        homepage: null,
        icon_url: null,
        keywords: [],
        auth: null,
        requires: null,
        coverage: null,
        source_url: sourceUrl,
        installed: true,
        plugin: 'acme',
        update_available: updateAvailable,
      },
    ],
  };
}

beforeEach(() => {
  vi.mocked(updatePlugin).mockReset();
  vi.mocked(deletePlugin).mockReset();
  vi.mocked(toast).mockReset();
  vi.mocked(getMarketplace).mockReset();
  vi.mocked(getMarketplace).mockResolvedValue({ ...catalogListing('', false), entries: [] });
});

describe('usePluginLifecycle: an update', () => {
  // A new version whose connector reads a key the plugin does not declare
  // comes back as a failed check naming that key. The update path used to
  // open its keys form only for declared keys, so this ended in a toast the
  // user could do nothing about.
  it('asks for a key the check finds missing that the new version does not declare', async () => {
    const user = userEvent.setup();
    vi.mocked(updatePlugin)
      .mockRejectedValueOnce(
        new ApiError('Request failed with status code 422', 'ERR_BAD_REQUEST', {
          data: {
            detail: {
              code: 'mcp_check_failed',
              message: 'acme did not pass its check.',
              failures: [
                {
                  server: 'acme',
                  verdict: 'missing_secrets',
                  reason: 'ACME_TOKEN is not set.',
                  missing_secrets: ['ACME_TOKEN'],
                },
              ],
            },
          },
          status: 422,
          statusText: '',
          headers: {},
        }),
      )
      .mockResolvedValueOnce(updated);
    renderWithProviders(<Harness />);

    await user.click(screen.getByRole('button', { name: 'Update acme' }));
    const dialog = await screen.findByRole('dialog', { name: 'Update acme' });
    expect(toast).not.toHaveBeenCalled();

    await user.type(within(dialog).getByLabelText(/ACME_TOKEN/), 'tok-1');
    await user.click(within(dialog).getByRole('button', { name: 'Check and update' }));
    await waitFor(() => expect(updatePlugin).toHaveBeenCalledTimes(2));
    expect(updatePlugin).toHaveBeenLastCalledWith('acme', { ACME_TOKEN: 'tok-1' }, null);
    expect(await screen.findByText('acme updated')).toBeInTheDocument();
  });

  // The catalog re-pinned the entry to a newer commit, which only it knows:
  // re-fetching the stored source would fetch the commit installed already.
  it('moves to the catalog entry when the catalog holds a newer version', async () => {
    const user = userEvent.setup();
    const pinned = 'https://github.com/acme/acme-plugin/tree/574ed36/plugins/acme';
    vi.mocked(getMarketplace).mockResolvedValue(catalogListing(pinned, true));
    vi.mocked(updatePlugin).mockResolvedValue(updated);
    renderWithProviders(<Harness />);

    expect(await screen.findByText('newer in the catalog')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Update acme' }));

    await waitFor(() => expect(updatePlugin).toHaveBeenCalledWith('acme', null, pinned));
  });

  it('re-fetches its own source when the catalog has nothing newer', async () => {
    const user = userEvent.setup();
    vi.mocked(getMarketplace).mockResolvedValue(
      catalogListing('https://github.com/acme/acme-plugin/tree/574ed36/plugins/acme', false),
    );
    vi.mocked(updatePlugin).mockResolvedValue(updated);
    renderWithProviders(<Harness />);

    await waitFor(() => expect(getMarketplace).toHaveBeenCalled());
    await user.click(screen.getByRole('button', { name: 'Update acme' }));

    await waitFor(() => expect(updatePlugin).toHaveBeenCalledWith('acme', null, null));
    expect(screen.queryByText('newer in the catalog')).not.toBeInTheDocument();
  });
});

describe('usePluginLifecycle: an uninstall', () => {
  function UninstallHarness() {
    const lifecycle = usePluginLifecycle(plugin);
    return (
      <button type="button" onClick={() => void lifecycle.uninstall()}>
        Uninstall acme
      </button>
    );
  }

  // The toast counts the subagents the server removed, and says the user's
  // changes to them went too, read from the list as it stood before.
  it('says the subagents it removed and the changes that went with them', async () => {
    const user = userEvent.setup();
    // The page keeps the list observed; here nothing does, so keep it cached.
    const queryClient = createTestQueryClient();
    queryClient.setDefaultOptions({ queries: { retry: false, gcTime: Infinity } });
    const edited = { name: 'screener', edited: true, source: 'plugin', plugin: { name: 'acme', display_name: 'Acme' } };
    queryClient.setQueryData(queryKeys.subagents.list(), {
      subagents: [edited as SubagentView],
    } as SubagentsResponse);
    vi.mocked(deletePlugin).mockResolvedValue({
      ok: true,
      deleted: { servers: ['acme'], skills: [], subagents: ['screener', 'writer'] },
    });
    renderWithProviders(<UninstallHarness />, { queryClient });

    await user.click(screen.getByRole('button', { name: 'Uninstall acme' }));

    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    const { description } = vi.mocked(toast).mock.calls[0][0] as { description: ReactNode };
    render(<>{description}</>);
    expect(screen.getByText('Removed 1 connector and 2 subagents.')).toBeInTheDocument();
    expect(screen.getByText('Your changes to its subagents were removed too.')).toBeInTheDocument();
  });
});
