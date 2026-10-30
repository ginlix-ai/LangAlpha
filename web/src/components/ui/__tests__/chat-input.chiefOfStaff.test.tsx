/**
 * The composer's host passes a scope (Chief of Staff or a workspace), every
 * conversation runs on the Default model, and the second model slot (the
 * Background model) is never shown. These pin that normalization and the
 * scope picker.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, fireEvent, screen, within, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputProps } from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';
import { getSkills } from '@/pages/ChatAgent/utils/api';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'running', is_home: true }),
}));

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  mutateAsync: vi.fn(),
  models: {} as Record<string, { models: string[] }>,
  slotModels: {} as Record<string, { models: string[] }>,
  metadata: {} as Record<string, { provider: string; sdk?: string }>,
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: true }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: mocks.models,
    slotModels: mocks.slotModels,
    claudeCodeState: 'off',
    payers: {},
    validModelNames: new Set(),
    metadata: mocks.metadata,
    isLoading: false,
    systemDefaults: { default_model: 'model-default', background_model: 'model-background-default' },
    offPlanModels: new Set(),
    platform: { model_tier: 0, byok_providers: [], oauth_providers: [] },
    rawApiResponse: {},
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync, mutate: vi.fn() }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(() => ({ id: 't', dismiss: vi.fn(), update: vi.fn() })),
}));

vi.mock('../chat-input.modelMenu', () => ({
  ChatInputModelMenu: ({ onSelectModel, selectedModel, groups }: {
    onSelectModel: (m: string) => void;
    selectedModel: string | null;
    groups: Array<{ connections: Array<{ models: string[] }> }>;
  }) => (
    <>
      <span>{`pill:${selectedModel}`}</span>
      <span>{`offered:${groups.flatMap((g) => g.connections.flatMap((c) => c.models)).join(',')}`}</span>
      <button type="button" onClick={() => onSelectModel('model-beta')}>pick-beta</button>
    </>
  ),
  ChatInputEffortMenu: () => null,
  ModelTriggerMeasure: () => null,
}));

let queryClient: QueryClient;

// The toolbar also paints every item into an aria-hidden measure row, so the
// visible controls are found by role, which skips it.
const pill = (name: string) => screen.getByRole('button', { name });
const pickerRow = (name: string) => within(screen.getByRole('menu')).getByRole('menuitemradio', { name });

function renderInput(props: Partial<ChatInputProps> = {}) {
  queryClient.setQueryData(queryKeys.user.preferences(), mocks.preferences);
  return render(composer(props));
}

function composer(props: Partial<ChatInputProps> = {}) {
  return (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput
          onSend={vi.fn()}
          scope="all"
          onScopeChange={vi.fn()}
          workspaces={[
            { workspace_id: 'ws-1', name: 'Alpha desk' },
            { workspace_id: 'ws-2', name: 'Beta desk' },
          ] as ChatInputProps['workspaces']}
          selectedWorkspaceId="ws-1"
          onWorkspaceChange={vi.fn()}
          {...props}
        />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('ChatInput scope picker', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mocks.preferences = {
      model_preference: { preferred_model: 'model-alpha', preferred_background_model: 'model-bg' },
    };
    mocks.mutateAsync.mockReset().mockResolvedValue(undefined);
    mocks.models = {};
    mocks.slotModels = {};
    mocks.metadata = {};
    vi.mocked(getSkills).mockClear();
  });

  // There a saved Claude Code Default reads as unset and a named one is
  // refused, so the composer shows and sends the deployment's Default instead,
  // whether the composer or a Home thread holds it.
  it("shows and sends the deployment's Default over a Claude Code one on the Chief of Staff", () => {
    mocks.metadata = { 'claude-x-code': { provider: 'claude-code', sdk: 'claude-code' } };
    mocks.preferences = { model_preference: { preferred_model: 'claude-x-code' } };
    const onSend = vi.fn();
    const { rerender } = renderInput({ onSend });
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();
    const box = screen.getByRole('textbox');
    fireEvent.change(box, { target: { value: 'hello' } });
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend.mock.calls[0][3]).toMatchObject({ model: 'model-default' });

    rerender(composer({ scope: 'workspace', inHome: true, model: 'claude-x-code', onSend }));
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();
    rerender(composer({ scope: 'workspace', onSend }));
    expect(screen.getByText('pill:claude-x-code')).toBeInTheDocument();
  });

  // The server refuses Claude Code on a Chief of Staff turn, so its composer
  // never offers one: neither a send to all workspaces nor a Home thread's.
  it('offers no Claude Code model on the Chief of Staff', () => {
    mocks.models = { anthropic: { models: ['claude-x'] }, 'claude-code': { models: ['claude-x-code'] } };
    mocks.slotModels = { anthropic: { models: ['claude-x'] } };
    mocks.metadata = {
      'claude-x': { provider: 'anthropic' },
      'claude-x-code': { provider: 'claude-code', sdk: 'claude-code' },
    };
    const offered = () => screen.getByText(/^offered:/).textContent;
    const { rerender } = renderInput();
    expect(offered()).toBe('offered:claude-x');
    rerender(composer({ scope: 'workspace', inHome: true }));
    expect(offered()).toBe('offered:claude-x');
    rerender(composer({ scope: 'workspace' }));
    expect(offered()).toBe('offered:claude-x,claude-x-code');
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('starts on the Default model on the Chief of Staff, never the background slot', () => {
    renderInput();
    expect(screen.getByText('pill:model-alpha')).toBeInTheDocument();

    // A pick rides the next send; it no longer saves a preference.
    fireEvent.click(screen.getByText('pick-beta'));
    expect(screen.getByText('pill:model-beta')).toBeInTheDocument();
    expect(mocks.mutateAsync).not.toHaveBeenCalled();
  });

  it('offers a scope picker beside the Subagents switch, and no agent mode toggle', () => {
    renderInput();
    expect(screen.queryByText('PTC')).not.toBeInTheDocument();
    expect(pill('Chief of Staff')).toHaveAttribute('aria-haspopup', 'dialog');
    expect(pill('Subagents')).toBeInTheDocument();
  });

  it('picks a workspace before switching the scope to it', () => {
    const calls: string[] = [];
    renderInput({
      onScopeChange: (scope) => calls.push(`scope:${scope}`),
      onWorkspaceChange: (id) => calls.push(`ws:${id}`),
    });
    fireEvent.click(pill('Chief of Staff'));
    fireEvent.click(pickerRow('Beta desk'));
    expect(calls).toEqual(['ws:ws-2', 'scope:workspace']);
  });

  it('switches back to the Chief of Staff from a workspace, keeping the Subagents switch', () => {
    const onScopeChange = vi.fn();
    renderInput({ scope: 'workspace', onScopeChange });
    expect(pill('Subagents')).toBeInTheDocument();
    fireEvent.click(pill('Alpha desk'));
    fireEvent.click(pickerRow('Chief of Staff'));
    expect(onScopeChange).toHaveBeenCalledWith('all');
  });

  it('names each section of the picker for a screen reader', () => {
    renderInput();
    fireEvent.click(pill('Chief of Staff'));
    const groups = within(screen.getByRole('menu')).getAllByRole('group');
    expect(groups.map((g) => g.getAttribute('aria-label'))).toEqual(['Chief of Staff', 'Workspaces']);
  });

  it('offers the Chief of Staff to a user with no workspace yet', () => {
    renderInput({ workspaces: [], selectedWorkspaceId: null, emptyWorkspacesHint: 'Make one first' });
    fireEvent.click(pill('Chief of Staff'));
    expect(screen.getByRole('dialog')).toHaveTextContent('Make one first');
  });

  it('picks a workspace from the keyboard when the list is too short to search', async () => {
    const user = userEvent.setup();
    const calls: string[] = [];
    renderInput({
      onScopeChange: (scope) => calls.push(`scope:${scope}`),
      onWorkspaceChange: (id) => calls.push(`ws:${id}`),
    });
    pill('Chief of Staff').focus();
    await user.keyboard('{Enter}');
    await waitFor(() => expect(document.activeElement).toBe(pickerRow('Chief of Staff')));
    await user.keyboard('{ArrowDown}{ArrowDown}{Enter}');
    expect(calls).toEqual(['ws:ws-2', 'scope:workspace']);
    // A keyboard user keeps their place on the pill.
    await waitFor(() => expect(document.activeElement).toBe(pill('Chief of Staff')));
  });

  it('returns to the draft after a pick made with the pointer', async () => {
    const user = userEvent.setup();
    renderInput();
    await user.click(pill('Chief of Staff'));
    await user.click(pickerRow('Beta desk'));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await user.keyboard('hello');
    expect(screen.getByRole('textbox')).toHaveValue('hello');
  });

  it('keeps the draft focused through a pick started from it', async () => {
    const user = userEvent.setup();
    renderInput();
    const draft = screen.getByRole('textbox');
    await user.click(draft);
    await user.click(pill('Chief of Staff'));
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    await waitFor(() => expect(document.activeElement).toBe(draft));
  });

  it('leaves the page live while open and closes on a press outside it', async () => {
    const user = userEvent.setup();
    renderInput();
    await user.click(pill('Chief of Staff'));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(document.documentElement.style.overflow).not.toBe('hidden');
    await user.click(document.body);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('closes when its pill is pressed again', async () => {
    const user = userEvent.setup();
    renderInput();
    await user.click(pill('Chief of Staff'));
    await user.click(pill('Chief of Staff'));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('stays shut when its pill comes back after leaving the toolbar mid-pick', async () => {
    const user = userEvent.setup();
    const picking = { scope: 'workspace' as const, onScopeChange: vi.fn() };
    const view = renderInput(picking);
    await user.click(pill('Alpha desk'));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    view.rerender(composer({ ...picking, onScopeChange: null }));
    view.rerender(composer(picking));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('keeps the picker open when its search field is clicked', async () => {
    const user = userEvent.setup();
    const workspaces = Array.from({ length: 8 }, (_, i) => ({ workspace_id: `ws-${i}`, name: `Desk ${i}` }));
    renderInput({ workspaces: workspaces as ChatInputProps['workspaces'], selectedWorkspaceId: 'ws-0' });
    await user.click(pill('Chief of Staff'));
    const search = screen.getByRole('searchbox');
    await user.click(search);
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(document.activeElement).toBe(search);
  });

  it("lists Home's skills on the Chief of Staff, where the turn runs", async () => {
    renderInput();
    await waitFor(() => {
      expect(getSkills).toHaveBeenLastCalledWith(expect.objectContaining({ workspaceId: 'home-1' }));
    });
  });
});
