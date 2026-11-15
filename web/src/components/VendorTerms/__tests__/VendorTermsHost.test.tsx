/**
 * The vendor terms dialog that opens on every page when the agent first needs
 * a vendor: what it says, how an answer goes out, and how it takes turns with
 * the agent's command requests.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { CliApproval } from '@/api/cliApprovals';
import type { PendingVendorTerms } from '@/api/vendorTerms';
import { queryKeys } from '@/lib/queryKeys';

const api = vi.hoisted(() => ({
  getPendingVendorTerms: vi.fn(),
  acceptVendorTerms: vi.fn(),
  dismissVendorTerms: vi.fn(),
}));
vi.mock('@/api/vendorTerms', () => api);

const cli = vi.hoisted(() => ({
  getPendingCliApprovals: vi.fn(),
  decideCliApproval: vi.fn(),
}));
vi.mock('@/api/cliApprovals', () => cli);

const toast = vi.hoisted(() => vi.fn());
vi.mock('@/components/ui/use-toast', () => ({ toast }));

import { CliApprovalHost } from '@/components/CliApproval/CliApprovalHost';
import { VendorTermsHost } from '../VendorTermsHost';

function terms(over: Partial<PendingVendorTerms> = {}): PendingVendorTerms {
  return {
    vendor: 'firecrawl',
    name: 'Firecrawl',
    terms_url: 'https://www.firecrawl.dev/terms-of-service',
    requested_at: '2026-11-15T00:00:00Z',
    ...over,
  };
}

function approval(): CliApproval {
  return {
    approval_id: 'ap-1',
    status: 'pending',
    action: 'workspace_delete',
    target: { id: 'w-1', name: 'Research' },
    details: { delete_files: false },
    requester: {
      kind: 'chief_of_staff',
      thread_id: null,
      thread_title: null,
      workspace_id: null,
      workspace_name: null,
      computer_name: null,
    },
    created_at: new Date().toISOString(),
    expires_at: new Date(Date.now() + 5 * 60_000).toISOString(),
  };
}

// The dialog is lazy, so its first render waits on a cold module load.
const LOAD = { timeout: 5000 };
const dialog = () => screen.findByRole('dialog', {}, LOAD);

beforeEach(() => {
  api.getPendingVendorTerms.mockReset();
  api.acceptVendorTerms.mockReset().mockResolvedValue({ vendor: 'firecrawl' });
  api.dismissVendorTerms.mockReset().mockResolvedValue(undefined);
  cli.getPendingCliApprovals.mockReset().mockResolvedValue([]);
  cli.decideCliApproval.mockReset().mockResolvedValue(undefined);
  toast.mockReset();
});

describe('VendorTermsHost: what it says', () => {
  it('names the vendor and links its terms in a new tab', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    renderWithProviders(<VendorTermsHost />);
    const box = await dialog();
    expect(within(box).getByRole('heading', { name: "Accept Firecrawl's terms" })).toBeInTheDocument();
    expect(box.querySelector('[data-slot="dialog-desc"]')?.textContent).toBe(
      'Your agent wants to use Firecrawl to read the web and its data catalog for you. LangAlpha sets up a Firecrawl account for you to do this, and Firecrawl asks you to accept its Terms of Service first.',
    );
    const link = within(box).getByRole('link', { name: "Read Firecrawl's Terms of Service" });
    expect(link).toHaveAttribute('href', 'https://www.firecrawl.dev/terms-of-service');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(within(box).getByRole('button', { name: 'Not now' })).toHaveFocus();
  });

  it('takes the name from the server, so another vendor reuses it', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms({ vendor: 'acme', name: 'Acme Data', terms_url: 'https://acme.example.com/tos' })]);
    renderWithProviders(<VendorTermsHost />);
    const box = await dialog();
    expect(within(box).getByRole('heading', { name: "Accept Acme Data's terms" })).toBeInTheDocument();
    expect(within(box).getByRole('link', { name: "Read Acme Data's Terms of Service" })).toHaveAttribute('href', 'https://acme.example.com/tos');
  });

  it('shows nothing when no terms wait', async () => {
    api.getPendingVendorTerms.mockResolvedValue([]);
    renderWithProviders(<VendorTermsHost />);
    await waitFor(() => expect(api.getPendingVendorTerms).toHaveBeenCalled());
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('opens once the pending list is read again and names a vendor', async () => {
    api.getPendingVendorTerms.mockResolvedValueOnce([]).mockResolvedValue([terms()]);
    const { queryClient } = renderWithProviders(<VendorTermsHost />);
    await waitFor(() => expect(api.getPendingVendorTerms).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.vendorTerms.pending() }));
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
  });
});

describe('VendorTermsHost: answering', () => {
  it('accepts, closing at once with a confirmation', async () => {
    api.getPendingVendorTerms.mockResolvedValueOnce([terms()]).mockResolvedValue([]);
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.acceptVendorTerms).toHaveBeenCalledWith('firecrawl');
    expect(api.dismissVendorTerms).not.toHaveBeenCalled();
    await waitFor(() => expect(toast).toHaveBeenCalledWith({ description: 'Accepted. Your agent can use Firecrawl now.' }));
  });

  it('says not now, closing at once', async () => {
    api.getPendingVendorTerms.mockResolvedValueOnce([terms()]).mockResolvedValue([]);
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Not now' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.dismissVendorTerms).toHaveBeenCalledWith('firecrawl');
    expect(api.acceptVendorTerms).not.toHaveBeenCalled();
    expect(toast).not.toHaveBeenCalled();
  });

  it('closed without an answer, says not now', async () => {
    api.getPendingVendorTerms.mockResolvedValueOnce([terms()]).mockResolvedValue([]);
    renderWithProviders(<VendorTermsHost />);
    fireEvent.keyDown(await dialog(), { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.dismissVendorTerms).toHaveBeenCalledWith('firecrawl');
  });

  // 409 is an account the vendor will not set up, 503 one it may later, 402 no credits.
  it.each([500, 409, 503, 402])('brings an accept that failed with %i back with a toast', async (status) => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    api.acceptVendorTerms.mockRejectedValueOnce({ response: { status, data: {} } });
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith({
        variant: 'destructive',
        description: "Couldn't set up your Firecrawl account. Try again later.",
      }),
    );
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
  });

  it('says nothing when the vendor is no longer known', async () => {
    api.getPendingVendorTerms.mockResolvedValueOnce([terms()]).mockResolvedValue([]);
    api.acceptVendorTerms.mockRejectedValueOnce({ response: { status: 404, data: {} } });
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    await waitFor(() => expect(api.getPendingVendorTerms).toHaveBeenCalledTimes(2));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(toast).not.toHaveBeenCalled();
  });

  it('brings the dialog back after a 404 from a worker that lacks the route', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    api.acceptVendorTerms.mockRejectedValueOnce({ response: { status: 404, data: {} } });
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    await waitFor(() => expect(api.getPendingVendorTerms).toHaveBeenCalledTimes(2));
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
    expect(toast).not.toHaveBeenCalled();
  });

  // The server still holds the same request, and the agent's next call only
  // renews it, so a failed not now left hidden would never be asked again.
  it('brings a failed not now back, without a toast', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    api.dismissVendorTerms.mockRejectedValueOnce({ response: { status: 500, data: {} } });
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Not now' }));
    await waitFor(() => expect(api.dismissVendorTerms).toHaveBeenCalledWith('firecrawl'));
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
    expect(toast).not.toHaveBeenCalled();
  });

  it('shows the next vendor once one is answered', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms(), terms({ vendor: 'acme', name: 'Acme Data' })]);
    renderWithProviders(<VendorTermsHost />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    expect(await screen.findByRole('heading', { name: "Accept Acme Data's terms" }, LOAD)).toBeInTheDocument();
  });
});

describe('VendorTermsHost: beside the command requests', () => {
  it('waits for a command request to be answered, then shows', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    cli.getPendingCliApprovals.mockResolvedValueOnce([approval()]).mockResolvedValue([]);
    renderWithProviders(<CliApprovalHost fallback={(held) => <VendorTermsHost held={held} />} />);
    const first = await dialog();
    expect(first).toHaveAttribute('data-cli-approval', 'ap-1');
    expect(screen.getAllByRole('dialog')).toHaveLength(1);
    fireEvent.click(within(first).getByRole('button', { name: 'Decline' }));
    const next = await screen.findByRole('heading', { name: "Accept Firecrawl's terms" }, LOAD);
    expect(next).toBeInTheDocument();
    expect(screen.getAllByRole('dialog')).toHaveLength(1);
    expect(cli.decideCliApproval).toHaveBeenCalledWith('ap-1', false);
  });

  it('shows when the command requests cannot be read', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    cli.getPendingCliApprovals.mockRejectedValue({ response: { status: 404, data: {} } });
    renderWithProviders(<CliApprovalHost fallback={(held) => <VendorTermsHost held={held} />} />);
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
  });

  it('gives way to a command request that arrives while it is open', async () => {
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    cli.getPendingCliApprovals.mockResolvedValueOnce([]).mockResolvedValue([approval()]);
    const { queryClient } = renderWithProviders(<CliApprovalHost fallback={(held) => <VendorTermsHost held={held} />} />);
    expect(await dialog()).toHaveAttribute('data-vendor-terms', 'firecrawl');
    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.cliApprovals.pending() }));
    await waitFor(() => expect(screen.getByRole('dialog')).toHaveAttribute('data-cli-approval', 'ap-1'));
    expect(screen.getAllByRole('dialog')).toHaveLength(1);
    expect(api.dismissVendorTerms).not.toHaveBeenCalled();
  });

  // Accepting sets up the account, which takes seconds; a command request
  // answered in that time must not bring the same terms back to answer twice.
  it('keeps an accept still under way answered across a command request', async () => {
    let finish!: () => void;
    api.acceptVendorTerms.mockReturnValueOnce(
      new Promise((resolve) => {
        finish = () => resolve({ vendor: 'firecrawl' });
      }),
    );
    api.getPendingVendorTerms.mockResolvedValue([terms()]);
    cli.getPendingCliApprovals.mockResolvedValueOnce([]).mockResolvedValueOnce([approval()]).mockResolvedValue([]);
    const { queryClient } = renderWithProviders(<CliApprovalHost fallback={(held) => <VendorTermsHost held={held} />} />);
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Accept' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());

    await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.cliApprovals.pending() }));
    const request = await dialog();
    expect(request).toHaveAttribute('data-cli-approval', 'ap-1');
    fireEvent.click(within(request).getByRole('button', { name: 'Decline' }));
    await waitFor(() => expect(cli.getPendingCliApprovals).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());

    await act(async () => finish());
    await waitFor(() => expect(toast).toHaveBeenCalledWith({ description: 'Accepted. Your agent can use Firecrawl now.' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(api.acceptVendorTerms).toHaveBeenCalledTimes(1);
  });
});
