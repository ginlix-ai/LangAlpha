/**
 * The picker's Claude Code rows follow where the workspace's turn would run
 * them: on one of the user's own computers, that computer's token, and its
 * fix links Settings to the workspace, which opens that computer's sheet;
 * on a hosted computer where no saved token runs, nowhere, which the rows
 * say with nothing to fix, unless the user chose how it runs there: then
 * Settings fixes it. Where the CLI runs on an own computer the server picks,
 * the rows say none is online, or name the one that is offline. The Chief
 * of Staff's rows follow where the server places its CLI.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputProps } from '../chat-input';
import type { ModelRowStatus } from '../chat-input.modelPicker';
import type { WorkspaceClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { CLAUDE_CODE_CHOICE_URL, claudeCodeTokenUrl } from '@/lib/claudeCode';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'running', is_home: true }),
}));

const mocks = vi.hoisted(() => ({
  where: { state: 'signed_in', computerId: null, known: true } as WorkspaceClaudeCode, asked: [] as unknown[], planUrl: null as string | null,
  medium: null as { kind: 'resize' | 'plan' | 'resizing'; go: () => void } | null, mediumAsked: undefined as unknown, go: vi.fn(),
}));

vi.mock('@/config/supportLinks', async (original) => ({
  ...(await original<typeof import('@/config/supportLinks')>()),
  get PLAN_URL() { return mocks.planUrl; },
}));

// The hold's own copy is pinned in useMediumAction.test; here, that the row
// carries it and its fix.
vi.mock('@/pages/ChatAgent/hooks/useMediumAction', () => ({
  useMediumHold: (computerId: string | null) => {
    mocks.mediumAsked = computerId;
    return mocks.medium && { unavailable: `hold:${mocks.medium.kind}`, fix: mocks.medium.go };
  },
}));

vi.mock('@/hooks/useWorkspaceClaudeCode', () => ({
  useTurnClaudeCode: (...args: unknown[]) => {
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
    payers: { 'claude-x': 'credits', 'claude-x-code': 'subscription:claude-code' },
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
  ChatInputModelMenu: ({ rowStatus, payers, groups }: {
    rowStatus: (m: string, now: number) => ModelRowStatus;
    payers: Record<string, string>;
    groups: { id: string; connections: { payer?: string }[] }[];
  }) => {
    const read = (m: string) => {
      const s = rowStatus(m, Date.now());
      return 'unavailable' in s ? `held:${s.unavailable}${s.fix ? ':fix' : ''}` : 'pick';
    };
    const code = rowStatus('claude-x-code', Date.now());
    return (
      <>
        <span>{`claude-x:${read('claude-x')}`}</span>
        <span>{`claude-x-code:${read('claude-x-code')}`}</span>
        <span>{`paid:${payers['claude-x']},${payers['claude-x-code']}`}</span>
        <span>{`tab:${groups.find((g) => g.id === 'claude-code')?.connections.map((c) => c.payer).join(',')}`}</span>
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

function renderInput(scope: ChatInputProps['scope'] = 'workspace') {
  return render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter>
        <ChatInput
          onSend={vi.fn()}
          scope={scope}
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
    mocks.planUrl = null;
    mocks.medium = null;
    mocks.mediumAsked = undefined;
    mocks.go.mockReset();
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it('asks after the selected workspace', () => {
    renderInput();
    expect(mocks.asked).toEqual([false, 'ws-1']);
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

  it('on a hosted computer while the choice for cloud computers is off, leads to Settings', () => {
    mocks.where = { state: 'hosted_off', computerId: null, known: true };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Choose in Settings how it runs on cloud computers:fix');
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(screen.getByText(/^at:/).textContent).toBe(`at:${CLAUDE_CODE_CHOICE_URL}`);
  });

  it('holds them on a cloud computer too small for Claude Code, with nothing to fix here', () => {
    mocks.where = { state: 'needs_bigger_computer', computerId: null, known: true };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Needs a Medium or larger cloud computer, or a computer of your own');
    expect(screen.queryByRole('button', { name: 'fix' })).toBeNull();
  });

  it.each(['resize', 'plan', 'resizing'] as const)('on a named cloud computer too small for Claude Code, holds the row with the fix when that is to %s, and does it', (kind) => {
    mocks.medium = { kind, go: mocks.go };
    mocks.where = { state: 'needs_bigger_computer', computerId: null, known: true, smallComputerId: 'cmp-small' };
    renderInput();
    expect(mocks.mediumAsked).toBe('cmp-small');
    expect(codeRow()).toBe(`claude-x-code:held:hold:${kind}:fix`);
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(mocks.go).toHaveBeenCalledTimes(1);
    expect(screen.getByText(/^at:/).textContent).toBe('at:/');
  });

  it('asks after no computer unless the workspace\'s is too small for Claude Code', () => {
    mocks.where = { state: 'signed_in', computerId: 'cmp-mac', known: true, smallComputerId: 'cmp-small' };
    renderInput();
    expect(mocks.mediumAsked).toBeNull();
  });

  it('falls back to the plain hold where nothing gets the named computer to Medium', () => {
    mocks.where = { state: 'needs_bigger_computer', computerId: null, known: true, smallComputerId: 'cmp-small' };
    renderInput();
    expect(mocks.mediumAsked).toBe('cmp-small');
    expect(codeRow()).toBe('claude-x-code:held:Needs a Medium or larger cloud computer, or a computer of your own');
  });

  it('where the build names a plan page, leads a cloud computer too small for Claude Code to it', () => {
    mocks.planUrl = '/account/plans';
    const opened = vi.spyOn(window, 'open').mockReturnValue(null);
    mocks.where = { state: 'needs_bigger_computer', computerId: null, known: true };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Needs a Medium or larger cloud computer: change its spec or upgrade your LangAlpha plan, or use a computer of your own:fix');
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(opened).toHaveBeenCalledWith('/account/plans', '_blank', 'noopener,noreferrer');
    opened.mockRestore();
  });

  it('holds them where no computer of the user\'s own with its own token is online, leading to Claude Code in Settings', () => {
    mocks.where = { state: 'no_own_computer', computerId: null, known: true };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Needs a computer of yours with its own setup token online:fix');
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(screen.getByText(/^at:/).textContent).toBe(`at:${CLAUDE_CODE_CHOICE_URL}`);
  });

  it('holds them while the computer that runs Claude Code is offline, naming it, with nothing to fix', () => {
    mocks.where = { state: 'cli_computer_offline', computerId: null, known: true, cliComputerName: 'Studio Mac' };
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Studio Mac is offline');
    expect(screen.queryByRole('button', { name: 'fix' })).toBeNull();
  });

  it("asks after the Chief of Staff's turn, and words Home's computer too small without a move to an own computer", () => {
    mocks.where = { state: 'needs_bigger_computer', computerId: null, known: true };
    renderInput('all');
    expect(mocks.asked[0]).toBe(true);
    expect(codeRow()).toBe('claude-x-code:held:Needs a Medium or larger cloud computer');
  });

  it("runs them on the user's own computer, where the subscription pays for them", () => {
    mocks.where = { state: 'signed_in', computerId: 'cmp-mac', known: true };
    renderInput();
    expect(screen.getByText(/^paid:/)).toHaveTextContent('paid:credits,subscription:claude-code');
    expect(screen.getByText(/^tab:/)).toHaveTextContent('tab:subscription:claude-code');
  });
});
