/**
 * Creating a workspace is instant now: the backend inserts a row on a machine
 * the user already has and returns 201, so there is no provisioning to watch.
 * The 201 body below is the live one from `POST /api/v1/workspaces` on :8060,
 * which came back in 4.5ms already `running` because its computer was up.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

vi.mock('../../utils/api', () => ({
  uploadWorkspaceFile: vi.fn(),
  formatApiErrorDetail: (err: unknown) =>
    (err as { message?: string })?.message ?? 'Request failed',
}));

import { uploadWorkspaceFile } from '../../utils/api';
import CreateWorkspaceModal from '../CreateWorkspaceModal';

const mockUpload = uploadWorkspaceFile as Mock;

// POST /api/v1/workspaces, captured verbatim.
const LIVE_CREATED = {
  workspace_id: 'da7f8dc7-509a-4903-be94-13dc509944a1',
  user_id: 'wp13-final-1789525377',
  name: 'WP16 UI probe',
  description: '',
  sandbox_id: '91885905-79ed-48dc-81ac-0a3d2d608458',
  computer_id: '5319ad0e-7835-4ca6-9541-b7c2961a7bf6',
  dir_name: 'wp16-ui-probe-8707',
  status: 'running',
  config: {},
  is_pinned: false,
  sort_order: 0,
  resource_tier: 'standard',
  is_always_on: false,
};

beforeEach(() => {
  vi.clearAllMocks();
});

function setup(onCreate: Mock, onComplete = vi.fn(), onClose = vi.fn()) {
  render(
    <CreateWorkspaceModal
      isOpen
      onClose={onClose}
      onCreate={onCreate}
      onComplete={onComplete}
    />,
  );
  return { onComplete, onClose };
}

describe('CreateWorkspaceModal', () => {
  it('opens the workspace straight away, with no provisioning screen', async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    const { onComplete, onClose } = setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'WP16 UI probe');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() => expect(onComplete).toHaveBeenCalledWith(LIVE_CREATED.workspace_id));
    expect(onClose).toHaveBeenCalled();
    // No progress phase at all: nothing was provisioned and nothing uploaded.
    expect(screen.queryByText(/uploading files/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/^ready$/i)).not.toBeInTheDocument();
  });

  it('has no resource-tier picker: the tier belongs to the machine', async () => {
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    setup(onCreate);
    expect(screen.queryByRole('radiogroup')).not.toBeInTheDocument();
    expect(screen.queryByRole('radio', { name: /performance/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('radio', { name: /max/i })).not.toBeInTheDocument();
  });

  it('keeps the user on the form and relays the quota service\'s sentence on a 429', async () => {
    const user = userEvent.setup();
    const platformMessage =
      'You have reached your workspace limit. Upgrade your plan to add more.';
    const onCreate = vi.fn().mockRejectedValue(
      Object.assign(new Error('Request failed with status code 429'), {
        status: 429,
        rateLimitInfo: { message: platformMessage, type: 'workspace_limit' },
      }),
    );
    const { onComplete } = setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'Denied');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    expect(await screen.findByText(platformMessage)).toBeInTheDocument();
    expect(onComplete).not.toHaveBeenCalled();
    // Still on the form, with the typed name intact.
    expect(screen.getByPlaceholderText(/name/i)).toHaveValue('Denied');
  });

  it('shows the upload run only when files are queued, and never an "initializing" step', async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    mockUpload.mockResolvedValue({});
    const { onComplete } = setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'With files');
    const file = new File(['hello'], 'notes.txt', { type: 'text/plain' });
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    await user.upload(input, file);

    await user.click(screen.getByRole('button', { name: /^create$/i }));

    // The upload track appears; the old "Initializing workspace" step does not.
    expect(await screen.findByText(/uploading files/i)).toBeInTheDocument();
    expect(screen.queryByText(/initializing workspace/i)).not.toBeInTheDocument();
    await waitFor(() =>
      expect(mockUpload).toHaveBeenCalledWith(
        LIVE_CREATED.workspace_id,
        file,
        null,
        expect.any(Function),
      ),
    );
    // The workspace is only handed over when the user asks for it.
    expect(onComplete).not.toHaveBeenCalled();
  });
});
