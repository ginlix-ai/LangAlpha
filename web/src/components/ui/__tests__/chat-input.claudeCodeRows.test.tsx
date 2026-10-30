/**
 * What the composer's picker says of a Claude Code model in each state the
 * server reports: a token that works, or one the check could not reach (which
 * admission runs), is a pick; one being checked waits; none, or a refused
 * one, says so and leads to where a token is added.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputProps } from '../chat-input';
import type { ModelRowStatus } from '../chat-input.modelPicker';
import type { ClaudeCodeState } from '@/api/model';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { CLAUDE_CODE_TOKEN_URL } from '@/lib/claudeCode';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'running', is_home: true }),
}));

const mocks = vi.hoisted(() => ({ state: 'signed_in' as ClaudeCodeState }));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: { model_preference: { preferred_model: 'claude-x' } }, isLoading: false, isLoaded: true }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: { anthropic: { models: ['claude-x'] }, 'claude-code': { models: ['claude-x-code'] } },
    slotModels: { anthropic: { models: ['claude-x'] } },
    claudeCodeState: mocks.state,
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
  render(
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

describe('ChatInput: Claude Code rows by state', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it.each(['signed_in', 'unverified'] as const)('offers the models as a pick while %s', (state) => {
    mocks.state = state;
    renderInput();
    expect(codeRow()).toBe('claude-x-code:pick');
  });

  it('holds them, with nothing to fix, while the token is checked', () => {
    mocks.state = 'checking';
    renderInput();
    expect(codeRow()).toBe('claude-x-code:held:Checking');
  });

  it.each([
    ['no_token', 'Add a setup token in Settings to use this model'],
    ['rejected', 'Claude refused your setup token. Add a new one in Settings to use this model'],
  ] as const)('says why while %s, and leads to adding a token', (state, words) => {
    mocks.state = state;
    renderInput();
    expect(codeRow()).toBe(`claude-x-code:held:${words}:fix`);
    fireEvent.click(screen.getByRole('button', { name: 'fix' }));
    expect(screen.getByText(/^at:/)).toHaveTextContent(`at:${CLAUDE_CODE_TOKEN_URL}`);
  });

  it('leaves every other model alone', () => {
    mocks.state = 'rejected';
    renderInput();
    expect(screen.getByText(/^claude-x:/)).toHaveTextContent('claude-x:pick');
  });
});
