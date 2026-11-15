/**
 * The approval dialog the agent's command requests open on every page: what
 * each request says, who it names, how an answer goes out, and what happens
 * to the queue behind it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { Route, Routes, useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import type { CliApproval } from '@/api/cliApprovals';

const api = vi.hoisted(() => ({
  getPendingCliApprovals: vi.fn(),
  decideCliApproval: vi.fn(),
}));
vi.mock('@/api/cliApprovals', () => api);

const toast = vi.hoisted(() => vi.fn());
vi.mock('@/components/ui/use-toast', () => ({ toast }));

const approveAlways = vi.hoisted(() => vi.fn((then: () => void) => then()));
vi.mock('@/hooks/useAutoApprove', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useAutoApprove')>();
  return { AUTO_APPROVE: actual.AUTO_APPROVE, useApproveAlways: () => ({ approveAlways, saving: false }) };
});

import { CliApprovalHost } from '../CliApprovalHost';

let seq = 0;
function approval(over: Partial<CliApproval> = {}): CliApproval {
  seq += 1;
  return {
    approval_id: `ap-${seq}`,
    status: 'pending',
    action: 'workspace_delete',
    target: { id: 'w-1', name: 'Research' },
    details: { delete_files: false },
    requester: {
      kind: 'chief_of_staff',
      thread_id: 't-1',
      thread_title: 'Morning brief',
      workspace_id: null,
      workspace_name: null,
      computer_name: null,
    },
    created_at: new Date().toISOString(),
    expires_at: new Date(Date.now() + 5 * 60_000).toISOString(),
    ...over,
  } as CliApproval;
}

function requester(over: Partial<CliApproval['requester']>): CliApproval['requester'] {
  return { kind: 'chief_of_staff', thread_id: null, thread_title: null, workspace_id: null, workspace_name: null, computer_name: null, ...over };
}

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

function renderHost() {
  return renderWithProviders(
    <>
      <CliApprovalHost />
      <Routes>
        <Route path="*" element={<Where />} />
      </Routes>
    </>,
  );
}

// The dialog is lazy, so its first render waits on a cold module load.
const LOAD = { timeout: 5000 };
const dialog = () => screen.findByRole('dialog', {}, LOAD);
/** The description's text, with the thread link's text joined in. */
const sentence = async () => (await dialog()).querySelector('[data-slot="dialog-desc"]')?.textContent;

beforeEach(() => {
  api.getPendingCliApprovals.mockReset();
  api.decideCliApproval.mockReset();
  api.decideCliApproval.mockResolvedValue(undefined);
  toast.mockReset();
  approveAlways.mockClear();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('CliApprovalHost: what each request says', () => {
  it.each<[string, Partial<CliApproval>, string, string]>([
    [
      'thread_delete',
      { action: 'thread_delete', target: { id: 't-9', name: 'Old notes' }, details: {} },
      'Delete thread “Old notes”?',
      'Your Chief of Staff asked in “Morning brief” to delete this thread.',
    ],
    [
      'workspace_delete with its files',
      { action: 'workspace_delete', details: { delete_files: true } },
      'Delete workspace “Research”?',
      'Your Chief of Staff asked in “Morning brief” to delete this workspace, its threads and its files.',
    ],
    [
      'workspace_delete keeping its files',
      { action: 'workspace_delete', details: { delete_files: false } },
      'Delete workspace “Research”?',
      'Your Chief of Staff asked in “Morning brief” to delete this workspace and its threads.',
    ],
    [
      'workspace_create on a computer',
      {
        action: 'workspace_create',
        target: { id: null, name: 'NVDA' },
        details: { computer_id: 'c-2', computer_name: 'My Mac', description: 'Track Nvidia' },
      },
      'Create workspace “NVDA”?',
      'Your Chief of Staff asked in “Morning brief” to create this workspace on “My Mac”.',
    ],
    [
      'automation_delete',
      { action: 'automation_delete', target: { id: 'a-1', name: 'Daily digest' }, details: {} },
      'Delete automation “Daily digest”?',
      'Your Chief of Staff asked in “Morning brief” to delete this automation.',
    ],
    [
      'computer_remove with its workspaces',
      {
        action: 'computer_remove',
        target: { id: 'c-1', name: 'Old box' },
        details: { remove_key: false, delete_files: false, delete_workspaces: ['w-1'], delete_workspace_names: ['Research'] },
      },
      'Remove computer “Old box”?',
      'Your Chief of Staff asked in “Morning brief” to remove this computer and delete the workspaces on it.',
    ],
    [
      'an unnamed target',
      { action: 'thread_delete', target: { id: 't-9', name: null }, details: {} },
      'Delete this thread?',
      'Your Chief of Staff asked in “Morning brief” to delete this thread.',
    ],
  ])('%s', async (_name, over, title, body) => {
    api.getPendingCliApprovals.mockResolvedValue([approval(over)]);
    renderHost();
    expect(await screen.findByRole('heading', { name: title }, LOAD)).toBeInTheDocument();
    expect(await sentence()).toBe(body);
  });

  it.each<[string, CliApproval['requester'], string]>([
    ['the Chief of Staff outside a thread', requester({ kind: 'chief_of_staff' }), 'Your Chief of Staff asked to delete this workspace and its threads.'],
    [
      'an analyst in its workspace and thread',
      requester({ kind: 'analyst', workspace_id: 'w-2', workspace_name: 'NVDA', thread_id: 't-2', thread_title: 'Q3 prep' }),
      'The analyst in “NVDA” asked in “Q3 prep” to delete this workspace and its threads.',
    ],
    ['an analyst with no workspace named', requester({ kind: 'analyst' }), 'An analyst asked to delete this workspace and its threads.'],
    ['a command on a named computer', requester({ kind: 'command', computer_name: 'My Mac' }), 'A command on “My Mac” asked to delete this workspace and its threads.'],
    [
      'a command from a thread',
      requester({ kind: 'command', computer_name: 'My Mac', thread_id: 't-3', thread_title: 'Cleanup' }),
      'A command from “Cleanup” on “My Mac” asked to delete this workspace and its threads.',
    ],
    ['a command on no named computer', requester({ kind: 'command' }), 'A command on one of your computers asked to delete this workspace and its threads.'],
  ])('names %s', async (_name, who, body) => {
    api.getPendingCliApprovals.mockResolvedValue([approval({ requester: who })]);
    renderHost();
    expect(await sentence()).toBe(body);
  });

  it('says what else a computer removal takes, and that it cannot be undone', async () => {
    api.getPendingCliApprovals.mockResolvedValue([
      approval({
        action: 'computer_remove',
        target: { id: 'c-1', name: 'Box' },
        details: { delete_files: true, remove_key: true, delete_workspaces: [], delete_workspace_names: [] },
      }),
    ]);
    renderHost();
    const box = await dialog();
    expect(within(box).getByText("LangAlpha's files on the machine are deleted too.")).toBeInTheDocument();
    expect(within(box).getByText('Our key is removed from ~/.ssh/authorized_keys too.')).toBeInTheDocument();
    expect(within(box).getByText("This can't be undone.")).toBeInTheDocument();
    expect(within(box).getByRole('button', { name: 'Remove' })).toHaveAttribute('data-variant', 'destructive');
  });

  it('opens the thread that asked', async () => {
    api.getPendingCliApprovals.mockResolvedValue([approval()]);
    renderHost();
    fireEvent.click(await screen.findByRole('link', { name: '“Morning brief”' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/t-1');
  });
});

describe('CliApprovalHost: requests that start work on another computer', () => {
  const analyst = requester({ kind: 'analyst', workspace_id: 'w-2', workspace_name: 'NVDA', thread_id: 't-2', thread_title: 'Earnings prep' });
  const message = 'Pull the latest guidance.\nThen compare it with last quarter.';
  const prompt = 'Summarize overnight moves in my holdings.';

  it.each<[string, Partial<CliApproval>, string, string, string | null, string]>([
    [
      'a message to a thread',
      {
        action: 'thread_send',
        target: { id: 't-5', name: 'Q3 prep' },
        details: { kind: 'send', message_preview: message, workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
      },
      'Send a message to “Q3 prep”?',
      'The analyst in “NVDA” asked in “Earnings prep” to send a message to this thread on “Laptop”.',
      message,
      'Send',
    ],
    [
      'a retry, to an unnamed thread on no named computer',
      {
        action: 'thread_send',
        target: { id: 't-5', name: null },
        details: { kind: 'retry', message_preview: null, workspace_name: null, computer_name: null, in_home: true },
      },
      'Retry the failed turn in this thread?',
      "The analyst in “NVDA” asked in “Earnings prep” to run this thread's failed turn again.",
      null,
      'Retry',
    ],
    [
      'a new thread in a workspace',
      {
        action: 'thread_create',
        target: { id: 'w-3', name: 'AMD' },
        details: { message_preview: message, workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
      },
      'Start a thread in “AMD”?',
      'The analyst in “NVDA” asked in “Earnings prep” to start a thread in this workspace on “Laptop”.',
      message,
      'Start',
    ],
    [
      'a new thread in Home',
      {
        action: 'thread_create',
        target: { id: 'w-home', name: null },
        details: { message_preview: message, workspace_name: null, computer_name: 'Laptop', in_home: true },
      },
      'Start a thread with your Chief of Staff?',
      'The analyst in “NVDA” asked in “Earnings prep” to start a thread with your Chief of Staff on “Laptop”.',
      message,
      'Start',
    ],
    [
      'a new automation',
      {
        action: 'automation_change',
        target: { id: null, name: 'Morning brief' },
        details: { kind: 'create', automation_name: 'Morning brief', prompt_preview: prompt, workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
      },
      'Create automation “Morning brief”?',
      'The analyst in “NVDA” asked in “Earnings prep” to create this automation, which runs in “AMD” on “Laptop”.',
      prompt,
      'Create',
    ],
    [
      'a change to an automation in Home',
      {
        action: 'automation_change',
        target: { id: 'a-1', name: 'Morning brief' },
        details: { kind: 'edit', automation_name: 'Morning brief', prompt_preview: prompt, workspace_name: null, computer_name: 'Laptop', in_home: true },
      },
      'Change automation “Morning brief”?',
      'The analyst in “NVDA” asked in “Earnings prep” to change this automation, which runs with your Chief of Staff on “Laptop”.',
      prompt,
      'Save',
    ],
    [
      'a run of an automation',
      {
        action: 'automation_change',
        target: { id: 'a-1', name: 'Morning brief' },
        details: { kind: 'trigger', automation_name: 'Morning brief', prompt_preview: null, workspace_name: 'AMD', computer_name: 'Laptop', in_home: false },
      },
      'Run automation “Morning brief”?',
      'The analyst in “NVDA” asked in “Earnings prep” to run this automation now, in “AMD” on “Laptop”.',
      null,
      'Run',
    ],
  ])('%s', async (_name, over, title, body, preview, label) => {
    api.getPendingCliApprovals.mockResolvedValue([approval({ ...over, requester: analyst })]);
    renderHost();
    expect(await screen.findByRole('heading', { name: title }, LOAD)).toBeInTheDocument();
    expect(await sentence()).toBe(body);
    const box = await dialog();
    if (preview) expect(within(box).getByTestId('cli-approval-preview').textContent).toBe(preview);
    else expect(within(box).queryByTestId('cli-approval-preview')).not.toBeInTheDocument();
    expect(within(box).getByRole('button', { name: label })).toHaveAttribute('data-variant', 'default');
    expect(within(box).queryByText("This can't be undone.")).not.toBeInTheDocument();
    expect(within(box).queryByRole('button', { name: 'Always allow' })).not.toBeInTheDocument();
  });
});

describe('CliApprovalHost: answering', () => {
  it('approves, closing at once', async () => {
    api.getPendingCliApprovals.mockResolvedValueOnce([approval({ approval_id: 'ap-a' })]).mockResolvedValue([]);
    renderHost();
    const remove = within(await dialog()).getByRole('button', { name: 'Delete' });
    expect(remove).toHaveAttribute('data-variant', 'destructive');
    fireEvent.click(remove);
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.decideCliApproval).toHaveBeenCalledWith('ap-a', true);
  });

  it('declines', async () => {
    api.getPendingCliApprovals.mockResolvedValueOnce([approval({ approval_id: 'ap-d' })]).mockResolvedValue([]);
    renderHost();
    const decline = within(await dialog()).getByRole('button', { name: 'Decline' });
    expect(decline).toHaveFocus();
    fireEvent.click(decline);
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.decideCliApproval).toHaveBeenCalledWith('ap-d', false);
  });

  it('creates a workspace, or creates it and every later one', async () => {
    api.getPendingCliApprovals.mockResolvedValue([
      approval({
        approval_id: 'ap-c',
        action: 'workspace_create',
        target: { id: null, name: 'NVDA' },
        details: { description: 'Track Nvidia', computer_id: null, computer_name: null },
      }),
    ]);
    renderHost();
    const box = await dialog();
    expect(within(box).getByText('Track Nvidia')).toBeInTheDocument();
    expect(within(box).queryByText("This can't be undone.")).not.toBeInTheDocument();
    expect(within(box).getByRole('button', { name: 'Create' })).toHaveAttribute('data-variant', 'default');
    fireEvent.click(within(box).getByRole('button', { name: 'Always allow' }));
    expect(approveAlways).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(api.decideCliApproval).toHaveBeenCalledWith('ap-c', true));
  });

  it('brings a failed answer back with a toast', async () => {
    api.getPendingCliApprovals.mockResolvedValue([approval({ approval_id: 'ap-f' })]);
    api.decideCliApproval.mockRejectedValueOnce({ response: { status: 500, data: {} } });
    renderHost();
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive' })));
    expect(await dialog()).toHaveAttribute('data-cli-approval', 'ap-f');
  });

  it('reads the list again, with no toast, when it was answered elsewhere', async () => {
    api.getPendingCliApprovals.mockResolvedValueOnce([approval({ approval_id: 'ap-x' })]).mockResolvedValue([]);
    api.decideCliApproval.mockRejectedValueOnce({ response: { status: 409, data: { detail: { type: 'approval_decided' } } } });
    renderHost();
    fireEvent.click(within(await dialog()).getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(api.getPendingCliApprovals).toHaveBeenCalledTimes(2));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(toast).not.toHaveBeenCalled();
  });

  it('closed without an answer, sends none and stays closed', async () => {
    api.getPendingCliApprovals.mockResolvedValue([approval()]);
    renderHost();
    fireEvent.keyDown(await dialog(), { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(api.decideCliApproval).not.toHaveBeenCalled();
  });
});

describe('CliApprovalHost: the queue', () => {
  it('shows the oldest first with how many wait behind it, then the next', async () => {
    api.getPendingCliApprovals.mockResolvedValue([
      approval({ approval_id: 'ap-1', target: { id: 'w-1', name: 'First' } }),
      approval({ approval_id: 'ap-2', target: { id: 'w-2', name: 'Second' } }),
      approval({ approval_id: 'ap-3', target: { id: 'w-3', name: 'Third' } }),
    ]);
    renderHost();
    expect(await screen.findByRole('heading', { name: 'Delete workspace “First”?' })).toBeInTheDocument();
    expect(screen.getByTestId('cli-approval-more')).toHaveTextContent('+2 more waiting');
    fireEvent.click(screen.getByRole('button', { name: 'Decline' }));
    expect(await screen.findByRole('heading', { name: 'Delete workspace “Second”?' })).toBeInTheDocument();
    expect(screen.getByTestId('cli-approval-more')).toHaveTextContent('+1 more waiting');
  });

  it('counts down to the expiry and leaves once the server no longer lists it', async () => {
    api.getPendingCliApprovals
      .mockResolvedValueOnce([
        approval({ approval_id: 'ap-soon', expires_at: new Date(Date.now() + 1_500).toISOString() }),
      ])
      .mockResolvedValue([]);
    renderHost();
    expect(await screen.findByTestId('cli-approval-expiry')).toHaveTextContent(/^Expires in 0:0[12]$/);
    expect(screen.queryByTestId('cli-approval-more')).not.toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument(), { timeout: 3_000 });
  });

  it('shows minutes and seconds left', async () => {
    api.getPendingCliApprovals.mockResolvedValue([approval({ expires_at: new Date(Date.now() + 90_000).toISOString() })]);
    renderHost();
    expect(await screen.findByTestId('cli-approval-expiry')).toHaveTextContent('Expires in 1:30');
  });

  it('keeps a request the server still lists though this clock says it expired', async () => {
    // A browser clock ahead of the server's must not hide what still waits.
    api.getPendingCliApprovals.mockResolvedValue([approval({ expires_at: new Date(Date.now() - 1_000).toISOString() })]);
    renderHost();
    expect(await screen.findByTestId('cli-approval-expiry')).toHaveTextContent('Expires in 0:00');
    await waitFor(() => expect(api.getPendingCliApprovals).toHaveBeenCalledTimes(2));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});
