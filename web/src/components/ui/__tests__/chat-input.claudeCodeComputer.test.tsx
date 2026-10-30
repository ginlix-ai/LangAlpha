/**
 * The picker's Claude Code rows follow where the workspace's turn would run
 * them: on one of the user's own computers, that computer's token, and its
 * fix links Settings to the workspace, which opens that computer's sheet;
 * on a hosted computer where no saved token runs, nowhere, which the rows
 * say with nothing to fix.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputProps } from '../chat-input';
import type { ModelRowStatus } from '../chat-input.modelPicker';
import type { WorkspaceClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { claudeCodeTokenUrl } from '@/lib/claudeCode';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'running', is_home: true }),
}));

const mocks = vi.hoisted(() => ({ where: { state: 'signed_in', computerId: null, known: true } as WorkspaceClaudeCode, asked: [] as unknown[] }));

vi.mock('@/hooks/useWorkspaceClaudeCode', () => ({
  useWorkspaceClaudeCode: (...args: unknown[]) => {
    mocks.asked = args;
    return mocks.where;
  },
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: { model_preference: { preferred_model: 'claude-x' } }, isLoading: false, isLoaded: true }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: { anthropic: { models: ['claude-x'] }, 'claude-code': { models: ['claude-x-code'] } },
    slotModels: { anthropic: { models: ['claude-x'] } },
    payers: {},
    validModelNames: new Set(['claude-x', 'claude-x-code']),
    metadata: {
      'claude-x': { provider: 'anthropic' },
      'claude-x-code': { provider: 'claude-code', sdk: 'claude-code' },
    },
    isLoading: false,
    systemDefaults: { default_model: 'claude-x', background_model: 'claude-x' },
    offPlanModels: new Set(),
    platform: { model_tier: 0, byok_providers: [], oauth_providers: [] },
    rawApiResponse: {},
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: vi.fn(), mutate: vi.fn() }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(() => ({ id: 't', dismiss: vi.fn(), update: vi.fn() })),
}));

// The menu stands in for the picker: it shows what the composer says of each
// row, and runs a row's fix as a press on it would.
vi.mock('../chat-input.modelMenu', () => ({
  ChatInputModelMenu: ({ rowStatus }: { rowStatus: (m: string) => ModelRowStatus }) => {
    const read = (m: string) => {
      const s = rowStatus(m);
      return 'unavailable' in s ? `held:${s.unavailable}${s.fix ? ':fix' : ''}` : 'pick';
    };
    const code = rowStatus('claude-x-code');
    return (
      <>
        <span>{`claude-x:${read('claude-x')}`}</span>
        <span>{`claude-x-code:${read('claude-x-code')}`}</span>
        {'unavailable' in code && code.fix && <button type="button" onClick={code.fix}>fix</button>}
      </>
    );
  },
  ChatInputEffortMenu: () => null,
  ModelTriggerMeasure: () => null,
}));

function Where() {
  const { pathname, search } = useLocation();
  return <span>{`at:${pathname}${search}`}</span>;
}

function renderInput() {
  return render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter>
        <ChatInput
          onSend={vi.fn()}
          scope="workspace"
          onScopeChange={vi.fn()}
          workspaces={[{ workspace_id: 'ws-1', name: 'Alpha desk' }] as ChatInputProps['workspaces']}
          selectedWorkspaceId="ws-1"
          onWorkspaceChange={vi.fn()}
        />
        <Where />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const codeRow = () => screen.getByText(/^claude-x-code:/).textContent;

describe("ChatInput: Claude Code rows by the workspace's computer", () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it('asks after the selected workspace', () => {
    renderInput();
    expect(mocks.asked).toEqual(['ws-1']);
  });

  it.each([
    ['no_token', 'Add a setup token in Settings to use this model'],
    ['rejected', 'Claude refused your setup token. Add a new one in Settings to use this model'],
  ] as const)("on an own computer while %s, leads to the workspace's sheet", (state, words) => {
    mocks.where = { state, computerId: 'cmp-mac', known: true };
    renderInput();
    expect(codeRow()).toBe(`claude-x-code:held:${words}:fix`);
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(screen.getByText(/^at:/)).toHaveTextContent(`at:${claudeCodeTokenUrl('ws-1')}`);
  });

  it('waits while the computer is checked, and is a pick once it is signed in or offline', () => {
    mocks.where = { state: 'checking', computerId: 'cmp-mac', known: false };
    const { unmount } = renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Checking');
    unmount();
    for (const state of ['signed_in', 'unverified', 'offline'] as const) {
      mocks.where = { state, computerId: 'cmp-mac', known: true };
      const view = renderInput();
      expect(codeRow()).toBe('claude-x-code:pick');
      view.unmount();
    }
  });

  it('holds them on a hosted computer where no saved token runs, with nothing to fix', () => {
    mocks.where = { state: 'needs_own_computer', computerId: null, known: true };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Runs only on workspaces on your own computers');
    expect(screen.queryByRole('button', { name: 'fix' })).toBeNull();
    expect(screen.getByText(/^claude-x:/)).toHaveTextContent('claude-x:pick');
  });
});
