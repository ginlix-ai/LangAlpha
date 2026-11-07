/**
 * The picker's Claude plan meter as the composer feeds it: it heads Claude
 * Code's tab while the token the workspace's turns run on works or is being
 * checked, and its Check now icon shows only while that token works.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputProps } from '../chat-input';
import type { ClaudeCodePlan, ClaudeCodeState } from '@/api/model';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', status: 'running', is_home: true }),
}));

const mocks = vi.hoisted(() => ({
  state: 'signed_in' as ClaudeCodeState,
  plan: undefined as ClaudeCodePlan | undefined,
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({
    preferences: { model_preference: { preferred_model: 'claude-x-code' }, other_preference: {} },
    isLoading: false,
    isLoaded: true,
  }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: { anthropic: { display_name: 'Anthropic', models: ['claude-x'] }, 'claude-code': { display_name: 'Claude Code', models: ['claude-x-code'] } },
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

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ enabled: mocks.state !== 'off', runsHere: mocks.state !== 'off', state: mocks.state, plan: mocks.plan, checkAfter: null }),
  useClaudeCodeComputers: () => ({ computers: [], cloudRefusals: {}, isLoading: false, checkAfterOf: () => null }),
}));
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { computer_id: 'cmp-hosted' }, isPending: false }) }));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: vi.fn(), mutate: vi.fn() }),
}));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

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
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

/** The picker, opened on Claude Code's tab. */
function openClaudeCodeTab() {
  fireEvent.click(screen.getByTitle('Select model'));
  const dialog = screen.getByRole('dialog');
  fireEvent.click(within(within(dialog).getByRole('radiogroup', { name: 'Model groups' })).getByRole('radio', { name: 'Claude Code' }));
  return dialog;
}

describe('ChatInput: the picker\'s Claude plan meter', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    mocks.plan = {
      five_hour: { used: 0.4, resets_at: '2099-01-01T00:00:00Z' }, seven_day: null, limits: [], credits: null, observed_at: new Date().toISOString(),
    };
  });

  it.each(['signed_in', 'unverified'] as const)('shows the meter with its Check now icon while the token is %s', (state) => {
    mocks.state = state;
    renderInput();
    const dialog = openClaudeCodeTab();
    const meter = dialog.querySelector('[data-plan-meter]') as HTMLElement;
    expect(meter).toHaveTextContent('Claude plan on your setup token');
    expect(meter).toHaveTextContent('40%');
    expect(within(meter).getByRole('button', { name: 'Check now' })).toBeInTheDocument();
  });

  it('shows the meter without the Check now icon while the token is being checked', () => {
    mocks.state = 'checking';
    renderInput();
    const dialog = openClaudeCodeTab();
    const meter = dialog.querySelector('[data-plan-meter]') as HTMLElement;
    expect(meter).toHaveTextContent('Claude plan on your setup token');
    expect(within(meter).queryByRole('button')).toBeNull();
  });

  it.each(['no_token', 'rejected', 'policy_blocked'] as const)('shows no meter while the token is %s', (state) => {
    mocks.state = state;
    renderInput();
    const dialog = openClaudeCodeTab();
    expect(dialog.querySelector('[data-plan-meter]')).toBeNull();
    expect(within(dialog).queryByRole('button', { name: 'Check now' })).toBeNull();
  });
});
