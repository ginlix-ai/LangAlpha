import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Hoisted mutable state — vary preferences / mutation per test. Stable refs
// (rebuilt in beforeEach) keep Settings' prefs-sync effect from looping.
const h = vi.hoisted(() => ({
  platformMode: false,
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  user: null as Record<string, unknown> | null,
  preferences: null as Record<string, unknown> | null,
  validModelNames: new Set<string>(),
}));

vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() {
    return h.platformMode;
  },
}));

vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => ({ logout: vi.fn() }),
}));

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: h.user, isLoading: false }),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false }),
}));

const mutationStub = { mutateAsync: h.mutateAsync };
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => mutationStub,
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', preference: 'dark', setTheme: vi.fn() }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    payers: {},
    systemDefaults: { fallback_models: [] },
    validModelNames: h.validModelNames,
    compactionProfiles: null,
    searchProviders: null,
    isLoading: false,
  }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

vi.mock('@/hooks/useDebouncedSave', () => ({
  useDebouncedSave: (saveFn: () => Promise<void>) => ({
    trigger: () => { setTimeout(() => { void saveFn(); }, 0); },
    flush: () => { setTimeout(() => { void saveFn(); }, 0); },
    status: 'idle',
  }),
}));

vi.mock('@/components/model/ModelTierConfig', () => ({
  ModelTierConfig: () => <div data-testid="model-tier-config-stub" />,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updateCurrentUser: vi.fn(async () => ({})),
  clearPreferences: vi.fn(async () => ({})),
  uploadAvatar: vi.fn(async () => ({ avatar_url: '' })),
  getUserApiKeys: vi.fn(async () => ({ providers: [] })),
}));

vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(async () => ({
    level: 'approve_each',
    agreement_version: 1,
    agreed_at: null,
    updated_at: null,
  })),
  updateTradingPermission: vi.fn(async () => ({})),
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'ws-home' })),
}));

// Onboarding — Settings renders replay/reset buttons; no provider in this harness.
vi.mock('@/pages/Onboarding', () => ({
  useOnboarding: () => ({ replayGuides: vi.fn(), resetOnboarding: vi.fn() }),
}));

// Import after mocks are registered.
import Settings from '../Settings';

function renderPreferencesTab() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/settings?tab=agent']}>
        <Settings />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  h.platformMode = false;
  h.validModelNames = new Set<string>();
  h.mutateAsync.mockClear();
  h.mutateAsync.mockResolvedValue({});
});

function setupAndRender(agentPreference: Record<string, unknown> = {}) {
  h.user = {
    id: 'u-1',
    email: 'tester@example.com',
    name: 'Tester',
    onboarding_completed: true,
  };
  h.preferences = { agent_preference: agentPreference };
  return renderPreferencesTab();
}

const lastWrite = () => (h.mutateAsync.mock.calls.at(-1)![0] as { agent_preference: Record<string, unknown> }).agent_preference;
const ranked = () => screen.getAllByTestId(/^output-format-/).map((row) => row.dataset.testid!.replace('output-format-', ''));

describe('Settings: output format', () => {
  it('shows the forms in the server order, left to the agent, when none is ranked', async () => {
    setupAndRender();
    await screen.findByText('Output format');
    expect(ranked()).toEqual(['visual', 'markdown', 'text']);
    expect(screen.getByText('The agent decides for now. Reorder to set your preference.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Let the agent decide' })).not.toBeInTheDocument();
  });

  it('shows a saved ranking in its order', async () => {
    setupAndRender({ output_format: ['text', 'visual', 'markdown'] });
    await screen.findByText('Output format');
    expect(ranked()).toEqual(['text', 'visual', 'markdown']);
  });

  it('fades the forms a partial ranking leaves out, which the agent gets no rank for', async () => {
    setupAndRender({ output_format: ['text'] });
    await screen.findByText('Output format');
    expect(ranked()).toEqual(['text', 'visual', 'markdown']);
    expect(screen.getAllByTestId(/^output-format-/).map((row) => row.dataset.ranked === 'true')).toEqual([true, false, false]);
  });

  it('reads a row from before the ranking as the server migrated it', async () => {
    setupAndRender({ output_format: 'html', deliverable: 'text' });
    await screen.findByText('Output format');
    expect(ranked()).toEqual(['visual', 'markdown', 'text']);
    expect(screen.getByRole('button', { name: 'Let the agent decide' })).toBeInTheDocument();
  });

  it('reads a list with a form outside the ranking as the user\'s words, not a ranking', async () => {
    setupAndRender({ output_format: ['visual', 'pdf'] });
    expect(await screen.findByText('visual, pdf')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Let the agent decide' })).not.toBeInTheDocument();
  });

  it('saves the whole order when a form moves, with only the keys it owns', async () => {
    setupAndRender({ tone: 'terse' });
    fireEvent.click(await screen.findByRole('button', { name: 'Move Chat up' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(lastWrite()).toEqual({ output_format: ['visual', 'text', 'markdown'], deliverable: null });
  });

  it('cannot move the first form up or the last one down', async () => {
    setupAndRender({ output_format: ['visual', 'markdown', 'text'] });
    expect(await screen.findByRole('button', { name: 'Move Visual up' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Move Chat down' })).toBeDisabled();
  });

  it('hands the choice back to the agent', async () => {
    setupAndRender({ output_format: ['markdown', 'text', 'visual'] });
    fireEvent.click(await screen.findByRole('button', { name: 'Let the agent decide' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(lastWrite()).toEqual({ output_format: null, deliverable: null });
  });
});

describe('Settings: research depth and reaching out', () => {
  it('shows each saved choice with its own line, and Agent decides when there is none', async () => {
    setupAndRender({ research_depth: 'thorough' });
    const depth = (await screen.findByRole('combobox', { name: 'Research depth' })) as HTMLSelectElement;
    expect(depth.value).toBe('thorough');
    expect(screen.getByText('Data and evidence checked before every answer. Takes longer and uses more credits.')).toBeInTheDocument();
    const reaching = screen.getByRole('combobox', { name: 'Reaching out' }) as HTMLSelectElement;
    expect(reaching.value).toBe('');
    expect(screen.getByText('When the agent gets in touch without being asked.')).toBeInTheDocument();
  });

  it('writes a pick, and null for Agent decides', async () => {
    setupAndRender({ proactivity: 'proactive' });
    fireEvent.change(await screen.findByRole('combobox', { name: 'Research depth' }), { target: { value: 'light' } });
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(lastWrite()).toEqual({ research_depth: 'light' });
    fireEvent.change(screen.getByRole('combobox', { name: 'Reaching out' }), { target: { value: '' } });
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(2));
    expect(lastWrite()).toEqual({ proactivity: null });
  });
});

describe("Settings: the user's instructions", () => {
  it('lists the keys the agent reads as the user\'s words, and none of the ones that steer it', async () => {
    setupAndRender({
      tone: 'plain words',
      research_depth: 'deep dives please',
      proactivity: 'on_request',
      output_format: ['visual'],
      avoid: ['jargon', 'emoji'],
    });
    expect(await screen.findByText('Your instructions')).toBeInTheDocument();
    expect(screen.getByRole('textbox', { name: 'Tone' })).toHaveValue('plain words');
    // A value outside its vocabulary reaches the agent as written, so it is one
    // too, but only to remove: the server refuses words as an edit to it.
    expect(screen.getByText('deep dives please')).toBeInTheDocument();
    expect(screen.queryByRole('textbox', { name: 'Research depth' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove Research depth' })).toBeInTheDocument();
    expect(screen.getByText('jargon, emoji')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Remove Proactivity' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Remove Output format' })).not.toBeInTheDocument();
  });

  it('lists an old deliverable beside an output format in the user\'s words, which the agent reads as written', async () => {
    setupAndRender({ output_format: 'pdf', deliverable: 'visual' });
    expect(await screen.findByRole('button', { name: 'Remove Deliverable' })).toBeInTheDocument();
    expect(screen.getByText('visual')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Let the agent decide' })).not.toBeInTheDocument();
  });

  it('lists an old deliverable beside a list in the user\'s words too', async () => {
    setupAndRender({ output_format: ['visual', 'pdf'], deliverable: 'text' });
    expect(await screen.findByRole('button', { name: 'Remove Deliverable' })).toBeInTheDocument();
  });

  it('saves an edit on leaving the field, and removes one with its button', async () => {
    setupAndRender({ tone: 'plain words', avoid: ['jargon'] });
    const tone = await screen.findByRole('textbox', { name: 'Tone' });
    fireEvent.change(tone, { target: { value: 'terse' } });
    fireEvent.blur(tone);
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(lastWrite()).toEqual({ tone: 'terse' });
    fireEvent.click(screen.getByRole('button', { name: 'Remove Avoid' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(2));
    expect(lastWrite()).toEqual({ avoid: null });
  });

  it('puts an emptied instruction back rather than saving it blank', async () => {
    setupAndRender({ tone: 'plain words' });
    const tone = await screen.findByRole('textbox', { name: 'Tone' });
    fireEvent.change(tone, { target: { value: '  ' } });
    fireEvent.blur(tone);
    expect(tone).toHaveValue('plain words');
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });

  it('draws no group when there are none', async () => {
    setupAndRender({ research_depth: 'light', output_format: ['text'] });
    await screen.findByText('Output format');
    expect(screen.queryByText('Your instructions')).not.toBeInTheDocument();
  });
});
