/**
 * Creating a workspace is instant now: the backend inserts a row on a machine
 * the user already has and returns 201, so there is no provisioning to watch.
 * The 201 body below is the live one from `POST /api/v1/workspaces` on :8060,
 * which came back in 4.5ms already `running` because its computer was up.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('../../utils/api', () => ({
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(),
  uploadWorkspaceFile: vi.fn(),
  startWorkspace: vi.fn(),
  formatApiErrorDetail: (err: unknown) =>
    (err as { message?: string })?.message ?? 'Request failed',
}));

import { getComputerProviders, getComputers, startWorkspace, uploadWorkspaceFile } from '../../utils/api';
import CreateWorkspaceModal from '../CreateWorkspaceModal';

const mockUpload = uploadWorkspaceFile as Mock;
const mockGetComputers = getComputers as Mock;
const mockProviders = getComputerProviders as Mock;

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

const PRIMARY_ID = LIVE_CREATED.computer_id;
const SECOND_ID = '3b788b47-352e-4cfb-94f0-920b599ea227';

function machine(computer_id: string, name: string, extra: Record<string, unknown> = {}) {
  return {
    computer_id,
    user_id: LIVE_CREATED.user_id,
    kind: 'daytona',
    name,
    status: 'running',
    resource_tier: 'standard',
    is_always_on: false,
    is_primary: false,
    workspace_count: 1,
    root_dir: '/home/workspace',
    ...extra,
  };
}

const PRIMARY = machine(PRIMARY_ID, 'Alpha Research', { is_primary: true });
const SECOND = machine(SECOND_ID, 'Second machine', { status: 'stopped' });

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(startWorkspace).mockResolvedValue({ workspace_id: LIVE_CREATED.workspace_id, status: 'running' });
  mockGetComputers.mockResolvedValue({ computers: [PRIMARY], total: 1 });
  mockProviders.mockResolvedValue({ providers: [{ kind: 'daytona', is_default: true, allowed: null }] });
});

function setup(onCreate: Mock, onComplete = vi.fn(), onClose = vi.fn()) {
  const { queryClient } = renderWithProviders(
    <CreateWorkspaceModal
      isOpen
      onClose={onClose}
      onCreate={onCreate}
      onComplete={onComplete}
    />,
  );
  return { onComplete, onClose, queryClient };
}

// A computer whose first sandbox is still being built.
const PREPARING_PRIMARY = { ...PRIMARY, status: 'starting', provider_ref: null };
const NOTICE = "Alpha Research is being prepared. You can create a workspace on it once it's ready.";

describe('CreateWorkspaceModal', () => {
  it('opens the workspace straight away, with no provisioning screen', async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    const { onComplete, onClose } = setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'WP16 UI probe');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() => expect(onComplete).toHaveBeenCalledWith(LIVE_CREATED));
    expect(onClose).toHaveBeenCalled();
    expect(startWorkspace).not.toHaveBeenCalled();
    // No progress phase at all: nothing was provisioned and nothing uploaded.
    expect(screen.queryByText(/uploading files/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/^ready$/i)).not.toBeInTheDocument();
  });

  it.each([
    ['one computer', [PRIMARY]],
    ['one live computer beside a deleted one', [PRIMARY, { ...SECOND, status: 'deleted' }]],
  ])('offers no computer choice with %s, and names none', async (_case, computers) => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue({ computers, total: computers.length });
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    setup(onCreate);

    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    await user.type(screen.getByPlaceholderText(/name/i), 'Only machine');
    expect(screen.queryByRole('combobox', { name: 'Computer' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() => expect(onCreate).toHaveBeenCalledWith({ name: 'Only machine', description: '' }));
  });

  it('lets the user put the workspace on another computer, starting from the primary', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue({ computers: [SECOND, PRIMARY], total: 2 });
    const onCreate = vi.fn().mockResolvedValue({ ...LIVE_CREATED, computer_id: SECOND_ID });
    setup(onCreate);

    const choice = await screen.findByRole('combobox', { name: 'Computer' });
    expect(choice).toHaveValue(PRIMARY_ID);
    expect(screen.getByRole('option', { name: 'Alpha Research (Primary)' })).toBeInTheDocument();

    await user.selectOptions(choice, SECOND_ID);
    await user.type(screen.getByPlaceholderText(/name/i), 'On the second');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    await waitFor(() =>
      expect(onCreate).toHaveBeenCalledWith({ name: 'On the second', description: '', computerId: SECOND_ID }),
    );
  });

  it('names what each computer runs on once the providers differ', async () => {
    mockGetComputers.mockResolvedValue({
      computers: [PRIMARY, machine(SECOND_ID, 'Second machine', { kind: 'e2b' })],
      total: 2,
    });
    mockProviders.mockResolvedValue({
      providers: [
        { kind: 'daytona', is_default: true, allowed: null },
        { kind: 'e2b', is_default: false, allowed: true },
      ],
    });
    setup(vi.fn());

    expect(await screen.findByRole('option', { name: 'Alpha Research · Daytona (Primary)' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Second machine · E2B' })).toBeInTheDocument();
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

  it('keeps the user on the form and names the workspace already holding the name', async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockRejectedValue(
      Object.assign(new Error('Request failed with status code 409'), {
        response: {
          status: 409,
          data: {
            detail: {
              code: 'workspace_name_taken',
              message: 'A workspace named "Research" already exists.',
              name: 'Research',
              workspace_id: LIVE_CREATED.workspace_id,
            },
          },
        },
      }),
    );
    const { onComplete } = setup(onCreate);

    const field = screen.getByPlaceholderText(/name/i);
    await user.type(field, 'research');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    expect(
      await screen.findByText('A workspace named "Research" already exists. Choose another name.'),
    ).toBeInTheDocument();
    expect(onComplete).not.toHaveBeenCalled();
    expect(field).toHaveValue('research');
  });

  it('caps the name at 80 characters as the server counts them, so an emoji is one', async () => {
    const user = userEvent.setup();
    setup(vi.fn());

    const field = screen.getByPlaceholderText(/name/i) as HTMLInputElement;
    await user.click(field);
    await user.paste('\u{1F4C8}'.repeat(90));

    expect(field.value).toBe('\u{1F4C8}'.repeat(80));
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
  it('waits for start before uploading to a stopped computer', async () => {
    const user = userEvent.setup();
    let ready!: (value: { workspace_id: string; status: string }) => void;
    vi.mocked(startWorkspace).mockReturnValueOnce(new Promise((resolve) => { ready = resolve; }));
    mockUpload.mockResolvedValue({});
    setup(vi.fn().mockResolvedValue({ ...LIVE_CREATED, status: 'stopped' }));
    await user.type(screen.getByPlaceholderText(/name/i), 'Stopped upload');
    await user.upload(document.querySelector('input[type="file"]') as HTMLInputElement,
      new File(['hello'], 'notes.txt', { type: 'text/plain' }));
    await user.click(screen.getByRole('button', { name: /^create$/i }));
    await waitFor(() => expect(startWorkspace).toHaveBeenCalledWith(LIVE_CREATED.workspace_id));
    expect(mockUpload).not.toHaveBeenCalled();
    ready({ workspace_id: LIVE_CREATED.workspace_id, status: 'running' });
    await waitFor(() => expect(mockUpload).toHaveBeenCalledTimes(1));
  });

  it('retries a failed start without recreating the workspace', async () => {
    const user = userEvent.setup();
    vi.mocked(startWorkspace).mockRejectedValueOnce(new Error('Start failed'));
    mockUpload.mockResolvedValue({});
    const create = vi.fn().mockResolvedValue({ ...LIVE_CREATED, status: 'stopped' });
    setup(create);
    await user.type(screen.getByPlaceholderText(/name/i), 'Retry upload');
    await user.upload(document.querySelector('input[type="file"]') as HTMLInputElement,
      new File(['hello'], 'notes.txt', { type: 'text/plain' }));
    await user.click(screen.getByRole('button', { name: /^create$/i }));
    expect(await screen.findByText('Start failed')).toBeInTheDocument();
    expect(mockUpload).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: /retry/i }));
    await waitFor(() => expect(mockUpload).toHaveBeenCalledTimes(1));
    expect(startWorkspace).toHaveBeenCalledTimes(2);
    expect(create).toHaveBeenCalledTimes(1);
  });

});


it('blocks backdrop, close and cancel while creation is pending', async () => {
  const user = userEvent.setup();
  let finish!: (value: typeof LIVE_CREATED) => void;
  const pending = new Promise<typeof LIVE_CREATED>((resolve) => { finish = resolve; });
  const { onClose, onComplete } = setup(vi.fn(() => pending));
  await user.type(screen.getByPlaceholderText(/name/i), 'Pending');
  await user.click(screen.getByRole('button', { name: /^create$/i }));
  expect(screen.getByRole('button', { name: /cancel/i })).toBeDisabled();
  const close = document.querySelector<HTMLButtonElement>('.cwm-close-btn')!;
  expect(close).toBeDisabled();
  await user.click(document.querySelector<HTMLElement>('.cwm-overlay')!);
  await user.click(close);
  expect(onClose).not.toHaveBeenCalled();
  finish(LIVE_CREATED);
  await waitFor(() => expect(onComplete).toHaveBeenCalledWith(LIVE_CREATED));
  expect(onClose).toHaveBeenCalledOnce();
});

describe('CreateWorkspaceModal, a computer still being prepared', () => {
  it('holds the form while the only computer is prepared, and lets go once it is ready', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue({ computers: [PREPARING_PRIMARY], total: 1 });
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    const { queryClient } = setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'Too early');
    expect(await screen.findByText(NOTICE)).toBeInTheDocument();
    const create = screen.getByRole('button', { name: /^create$/i });
    expect(create).toBeDisabled();

    // What the status fan-out writes when the stream says running.
    act(() => {
      queryClient.setQueryData(queryKeys.computers.lists(), {
        computers: [{ ...PREPARING_PRIMARY, status: 'running' }],
        total: 1,
      });
    });
    await waitFor(() => expect(screen.queryByText(NOTICE)).not.toBeInTheDocument());
    expect(create).toBeEnabled();
    await user.click(create);
    await waitFor(() => expect(onCreate).toHaveBeenCalledWith({ name: 'Too early', description: '' }));
  });

  it('offers a computer being prepared as a choice it cannot take', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue({ computers: [PREPARING_PRIMARY, SECOND], total: 2 });
    const onCreate = vi.fn().mockResolvedValue({ ...LIVE_CREATED, computer_id: SECOND_ID });
    setup(onCreate);

    const choice = await screen.findByRole('combobox', { name: 'Computer' });
    expect(screen.getByRole('option', { name: 'Alpha Research (Primary, Preparing)' })).toBeDisabled();
    // The primary is still where it starts, so the hold shows until another is picked.
    expect(screen.getByText(NOTICE)).toBeInTheDocument();

    await user.selectOptions(choice, SECOND_ID);
    expect(screen.queryByText(NOTICE)).not.toBeInTheDocument();
    await user.type(screen.getByPlaceholderText(/name/i), 'On the second');
    await user.click(screen.getByRole('button', { name: /^create$/i }));
    await waitFor(() =>
      expect(onCreate).toHaveBeenCalledWith({ name: 'On the second', description: '', computerId: SECOND_ID }),
    );
  });

  it('suffixes a non-primary computer being prepared', async () => {
    mockGetComputers.mockResolvedValue({
      computers: [PRIMARY, { ...SECOND, status: 'creating', provider_ref: null }],
      total: 2,
    });
    setup(vi.fn());
    await screen.findByRole('combobox', { name: 'Computer' });
    expect(screen.getByRole('option', { name: 'Second machine (Preparing)' })).toBeDisabled();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('holds nothing for a user with no computer yet, whose first workspace makes one', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue({ computers: [], total: 0 });
    const onCreate = vi.fn().mockResolvedValue(LIVE_CREATED);
    setup(onCreate);

    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    await user.type(screen.getByPlaceholderText(/name/i), 'First');
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^create$/i }));
    await waitFor(() => expect(onCreate).toHaveBeenCalled());
  });

  it("words the server's refusal for a computer the list had not yet seen being prepared", async () => {
    const user = userEvent.setup();
    const onCreate = vi.fn().mockRejectedValue(
      Object.assign(new Error('Request failed with status code 409'), {
        response: {
          status: 409,
          data: { detail: { code: 'computer_preparing', message: 'Computer is still being prepared' } },
        },
      }),
    );
    setup(onCreate);

    await user.type(screen.getByPlaceholderText(/name/i), 'Raced');
    await user.click(screen.getByRole('button', { name: /^create$/i }));

    expect(
      await screen.findByText("This computer is still being prepared. Try again once it's ready."),
    ).toBeInTheDocument();
    expect(screen.queryByText('Computer is still being prepared')).not.toBeInTheDocument();
  });
});
