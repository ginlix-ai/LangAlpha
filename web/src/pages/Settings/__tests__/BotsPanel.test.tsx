/**
 * The Bots settings section: the list grouped by workspace, the derived Chief
 * of Staff drawn read-only, and the three writes the section offers. The hooks
 * are stubbed so the assertions are about what the section asks them to do,
 * not about React Query again (that contract lives in useBots.test.tsx).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, within, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { BotInfo } from '@/api/bots';
import type { Workspace } from '@/types/api';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  bots: [] as BotInfo[],
  workspaces: [] as Workspace[],
  create: vi.fn(async (_input: unknown) => ({})),
  update: vi.fn(async (_args: unknown) => ({})),
  remove: vi.fn(async (_botId: string) => ({})),
  toast: vi.fn(),
  botsError: null as unknown,
  botsRefetch: vi.fn(),
  botsLoading: false,
  botsFailed: false,
  workspacesLoading: false,
  workspacesError: null as unknown,
  workspacesFailed: false,
  workspacesRefetch: vi.fn(),
  navigate: vi.fn(),
  createThread: vi.fn(),
}));

vi.mock('react-router', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router')>()),
  useNavigate: () => h.navigate,
}));

vi.mock('@/pages/ChatAgent/utils/api/threads', () => ({
  createThreadWithTitle: h.createThread,
}));

vi.mock('@/hooks/useBots', async () => {
  // The real mutation objects, so a refusal lands in `error` and the dialog's
  // banner is the thing under test (a stub returning error: null could hide it).
  const { useMutation } = await import('@tanstack/react-query');
  return {
    useBots: () => ({
      data: { bots: h.bots, max_bots: 20 },
      isLoading: h.botsLoading,
      isError: h.botsFailed,
      error: h.botsError,
      refetch: h.botsRefetch,
    }),
    useCreateBot: () => useMutation({ mutationFn: (input: unknown) => h.create(input) }),
    useUpdateBot: () => useMutation({ mutationFn: (args: unknown) => h.update(args) }),
    useDeleteBot: () => useMutation({ mutationFn: (botId: string) => h.remove(botId) }),
  };
});

vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: () => ({
    data: h.workspacesFailed ? undefined : { workspaces: h.workspaces, total: h.workspaces.length },
    isLoading: h.workspacesLoading,
    isError: h.workspacesFailed,
    error: h.workspacesError,
    refetch: h.workspacesRefetch,
  }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: h.toast }),
}));

import { BotsPanel } from '../panels/BotsPanel';

const HOME: Workspace = { workspace_id: 'ws-home', name: 'Home', is_home: true };
const RESEARCH: Workspace = { workspace_id: 'ws-research', name: 'Research', is_home: false };

function bot(overrides: Partial<BotInfo> & Pick<BotInfo, 'bot_id' | 'name' | 'role' | 'workspace_id'>): BotInfo {
  return {
    user_id: 'user-1',
    persona: '',
    avatar: {},
    provider: null,
    model: null,
    skills: [],
    tools_allow: [],
    tools_deny: [],
    approval: 'ask',
    memory_scopes: ['workspace'],
    status: 'active',
    primary_thread_id: null,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

const CHIEF = bot({ bot_id: 'bot-cos', name: 'Chief of Staff', role: 'chief_of_staff', workspace_id: 'ws-home' });
const ANALYST = bot({ bot_id: 'bot-1', name: 'Macro analyst', role: 'analyst', workspace_id: 'ws-research' });

function renderPanel() {
  return renderWithProviders(<BotsPanel />);
}

beforeEach(() => {
  h.bots = [CHIEF, ANALYST];
  h.workspaces = [HOME, RESEARCH];
  h.create.mockReset();
  h.create.mockResolvedValue({});
  h.update.mockReset();
  h.update.mockResolvedValue({});
  h.remove.mockReset();
  h.remove.mockResolvedValue({});
  h.toast.mockReset();
  h.botsError = null;
  h.botsRefetch.mockReset();
  h.botsLoading = false;
  h.botsFailed = false;
  h.workspacesLoading = false;
  h.workspacesError = null;
  h.workspacesFailed = false;
  h.workspacesRefetch.mockReset();
  h.navigate.mockReset();
  h.createThread.mockReset();
});

describe('BotsPanel list', () => {
  it('groups every bot under its workspace', () => {
    renderPanel();

    expect(screen.getByRole('heading', { name: 'Your bots' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Home' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Research' })).toBeInTheDocument();
    expect(screen.getByText('Macro analyst')).toBeInTheDocument();
    expect(screen.getByText('Chief of Staff')).toBeInTheDocument();
  });

  it('draws the derived Chief of Staff read-only, with no row actions', () => {
    renderPanel();

    const row = screen.getByText('Chief of Staff').closest('.settings-row');
    expect(row).not.toBeNull();
    expect(within(row as HTMLElement).getByText('Read-only')).toBeInTheDocument();
    // The derived bot can be talked to, but not edited, paused or deleted here.
    expect(within(row as HTMLElement).getByRole('button', { name: 'Chat with Chief of Staff' })).toBeInTheDocument();
    expect(within(row as HTMLElement).queryByRole('button', { name: /Edit|Delete|Pause/ })).toBeNull();
    // Every row wears its BotMark, the derived one included.
    expect((row as HTMLElement).querySelector('[data-bot-mark]')).not.toBeNull();
  });

  it('offers edit, pause and delete on an analyst row', () => {
    renderPanel();

    const row = screen.getByText('Macro analyst').closest('.settings-row') as HTMLElement;
    expect(within(row).getByRole('button', { name: 'Delete Macro analyst' })).toBeInTheDocument();
    expect(within(row).getByRole('button', { name: 'Edit Macro analyst' })).toBeInTheDocument();
    expect(within(row).getByRole('button', { name: 'Pause Macro analyst' })).toBeInTheDocument();
  });

  it('explains a workspace needs a bot when there are none', () => {
    h.bots = [];
    renderPanel();

    expect(screen.getByText('No bots yet')).toBeInTheDocument();
    expect(screen.getByText(/A workspace needs a bot/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'New bot' })).toBeEnabled();
  });
});

describe('BotsPanel writes', () => {
  it('creates a bot in the workspace the dialog picks', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'New bot' }));
    fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'Rates analyst' } });
    // Only Research is offered: Home holds the derived Chief of Staff.
    expect(screen.getByLabelText('Workspace')).toHaveValue('ws-research');
    fireEvent.click(screen.getByRole('button', { name: 'Create' }));

    await waitFor(() => expect(h.create).toHaveBeenCalledWith({
      workspace_id: 'ws-research',
      name: 'Rates analyst',
      role: 'analyst',
      persona: '',
      provider: null,
      model: null,
      approval: 'ask',
    }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
  });

  it('sends only the changed name when nothing else was edited', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'Macro strategist' } });
    // The approval is stored and shown for now; the dialog says so.
    expect(screen.getByText(/v1 does not auto-approve/)).toBeInTheDocument();
    // Memory scopes are stored for a later version, and the form says so.
    expect(screen.getByText(/does not enforce memory scopes/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    // persona/model/approval are versioned server-side: a rename alone must not
    // restate them into a new bot_versions row.
    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: { name: 'Macro strategist' },
    }));
  });

  it('clears the model with an explicit null when the field empties', async () => {
    h.bots = [CHIEF, { ...ANALYST, model: 'claude-sonnet-4' }];
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Model'), { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: { name: 'Macro analyst', model: null },
    }));
  });

  it('sends persona and approval only when they changed', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Persona'), { target: { value: 'Reads the tape.' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Approval' }), { target: { value: 'edits' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: { name: 'Macro analyst', persona: 'Reads the tape.', approval: 'edits' },
    }));
  });

  it('edits the skill and tool lists and memory scopes, keeping a scope it does not show', async () => {
    h.bots = [CHIEF, { ...ANALYST, memory_scopes: ['workspace', 'legacy'] }];
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Skills'), { target: { value: 'macro, rates, macro' } });
    fireEvent.change(screen.getByLabelText('Allowed tools'), { target: { value: ' web_search , quote ' } });
    fireEvent.change(screen.getByLabelText('Blocked tools'), { target: { value: 'trading' } });
    fireEvent.click(screen.getByLabelText('User memory'));
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: {
        name: 'Macro analyst',
        skills: ['macro', 'rates'],
        tools_allow: ['web_search', 'quote'],
        tools_deny: ['trading'],
        // The scope this UI does not offer is carried through untouched.
        memory_scopes: ['workspace', 'legacy', 'user'],
      },
    }));
  });

  it('saves a chosen colour, shape and glyph as the avatar', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.click(screen.getByRole('button', { name: 'Mark' }));
    fireEvent.click(screen.getByRole('button', { name: 'Teal' }));
    fireEvent.change(screen.getByLabelText('Shape'), { target: { value: 'hex' } });
    fireEvent.change(screen.getByLabelText('Glyph (optional)'), { target: { value: 'Z' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: expect.objectContaining({
        avatar: { kind: 'mark', shape: 'hex', color: 'teal', glyph: 'Z' },
      }),
    }));
  });

  it('leaves an untouched avatar out of the patch', async () => {
    h.bots = [CHIEF, { ...ANALYST, avatar: { kind: 'image', url: 'https://example.com/a.png' } }];
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'Renamed' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalled());
    const patch = (h.update.mock.calls[0][0] as { patch: Record<string, unknown> }).patch;
    expect(patch).not.toHaveProperty('avatar');
  });

  it('keeps keys the avatar UI does not own when one of its fields changes', async () => {
    h.bots = [CHIEF, { ...ANALYST, avatar: { kind: 'mark', shape: 'circle', color: 'blue', note: 'keep' } }];
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.click(screen.getByRole('button', { name: 'Rose' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: expect.objectContaining({
        avatar: { note: 'keep', kind: 'mark', shape: 'circle', color: 'rose' },
      }),
    }));
  });

  it('uses the automatic mark when asked to reset', async () => {
    h.bots = [CHIEF, { ...ANALYST, avatar: { kind: 'emoji', emoji: '🦊' } }];
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.click(screen.getByRole('button', { name: 'Use automatic mark' }));
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith({
      botId: 'bot-1',
      patch: expect.objectContaining({ avatar: {} }),
    }));
  });

  it('pauses an active bot through the status write', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Pause Macro analyst' }));

    await waitFor(() => expect(h.update).toHaveBeenCalledWith(
      { botId: 'bot-1', patch: { status: 'paused' } },
    ));
  });

  it('deletes a bot only after the confirmation dialog', async () => {
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Delete Macro analyst' }));
    expect(screen.getByRole('heading', { name: 'Delete bot' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Delete bot' }));

    await waitFor(() => expect(h.remove).toHaveBeenCalledWith('bot-1'));
  });

  it('shows the server\'s refusal sentence when an edit is refused', async () => {
    h.update.mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'A bot with that name already exists in this workspace.' } },
      }),
    );
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Edit Macro analyst' }));
    fireEvent.change(screen.getByLabelText('Name'), { target: { value: 'Macro analyst' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(await screen.findByText('A bot with that name already exists in this workspace.')).toBeInTheDocument();
  });

  it('does not report "no bots yet" while the bots read has failed', () => {
    h.bots = [];
    h.botsFailed = true;
    h.botsError = new Error('Network Error');
    renderPanel();

    expect(screen.queryByText('No bots yet')).toBeNull();
    expect(screen.getByText("Couldn't load your bots.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(h.botsRefetch).toHaveBeenCalled();
  });

  it('does not report "no bots yet" while the workspace read has failed', () => {
    h.bots = [];
    h.workspacesFailed = true;
    h.workspacesError = new Error('Network Error');
    renderPanel();

    expect(screen.getByText("Couldn't load your bots.")).toBeInTheDocument();
    expect(screen.queryByText('No bots yet')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(h.workspacesRefetch).toHaveBeenCalled();
  });

  it('shows loading instead of an empty state while the reads are in flight', () => {
    h.botsLoading = true;
    renderPanel();

    expect(screen.getByRole('status')).toHaveTextContent('Loading');
    expect(screen.queryByText('No bots yet')).toBeNull();
  });
  it('opens a conversation bound to the bot the row names', async () => {
    h.createThread.mockResolvedValue({ thread_id: 'thread-9', workspace_id: 'ws-research' });
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Chat with Macro analyst' }));

    await waitFor(() => expect(h.createThread).toHaveBeenCalledWith({
      workspaceId: 'ws-research',
      firstQuery: '',
      title: 'Macro analyst',
      botId: 'bot-1',
    }));
    expect(h.navigate).toHaveBeenCalledWith('/chat/t/thread-9', {
      state: { workspaceId: 'ws-research' },
    });
  });

  it('opens a conversation with the derived Chief of Staff too', async () => {
    h.createThread.mockResolvedValue({ thread_id: 'thread-cos', workspace_id: 'ws-home' });
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Chat with Chief of Staff' }));

    await waitFor(() => expect(h.createThread).toHaveBeenCalledWith(expect.objectContaining({
      workspaceId: 'ws-home',
      botId: 'bot-cos',
    })));
    expect(h.navigate).toHaveBeenCalledWith('/chat/t/thread-cos', {
      state: { workspaceId: 'ws-home' },
    });
  });

  it('cannot chat with a paused bot', () => {
    h.bots = [CHIEF, { ...ANALYST, status: 'paused' }];
    renderPanel();

    const chat = screen.getByRole('button', { name: 'Chat with Macro analyst' });
    expect(chat).toBeDisabled();
    fireEvent.click(chat);

    expect(h.createThread).not.toHaveBeenCalled();
    expect(h.navigate).not.toHaveBeenCalled();
  });

  it('shows the server\'s refusal and stays on the page when a chat cannot open', async () => {
    h.createThread.mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 409'), {
        response: { status: 409, data: { detail: 'Resume the bot before starting a conversation.' } },
      }),
    );
    renderPanel();

    fireEvent.click(screen.getByRole('button', { name: 'Chat with Macro analyst' }));

    expect(await screen.findByText('Resume the bot before starting a conversation.')).toBeInTheDocument();
    expect(h.navigate).not.toHaveBeenCalled();
  });});
