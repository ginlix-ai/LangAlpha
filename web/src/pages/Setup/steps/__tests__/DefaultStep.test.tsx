/**
 * The default step saves what the user leaves picked, and only that: the
 * conversation model, the Background model beside it (Auto as Settings saves
 * it), and routing only where it was changed. Continue then sets the user up
 * in steps and hands over to /welcome.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const codex = ['gpt-astra', 'gpt-sol', 'gpt-luna'];
const claudeCode = ['cc-fable', 'cc-opus', 'cc-sonnet'];
const h = vi.hoisted(() => ({
  models: 'subscription_or_key',
  preferences: {} as object,
  systemDefaults: null as null | object,
  payers: {} as Record<string, string>,
  gone: [] as string[],
  mutateAsync: vi.fn(),
  finish: vi.fn(),
  toWelcome: vi.fn(),
  home: vi.fn(),
  prefsUnread: false,
  retryPrefs: vi.fn(),
  catalogUnread: false,
  retryFacts: vi.fn(),
  next: 'default' as string | null,
  progress: null as null | object,
  policy: null as null | object,
  platform: false,
  byok: true,
  providers: [] as object[],
  userId: 'u1',
  nextListeners: new Set<() => void>(),
  codex: ['gpt-astra', 'gpt-sol', 'gpt-luna'],
}));
vi.mock('@/pages/Onboarding/welcome/Welcome', () => ({ default: () => null }));
vi.mock('@/hooks/useHomeWorkspace', () => ({
  homeWorkspaceQuery: () => ({ queryKey: ['home'], queryFn: h.home }),
}));
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: { 'codex-oauth': { models: h.codex }, 'claude-code': { models: claudeCode } },
    slotModels: { 'codex-oauth': { models: h.codex } },
    payers: h.payers,
    metadata: {
      'gpt-astra': { display_name: 'GPT Astra', intelligence: 5, speed: 2 },
      'gpt-sol': { display_name: 'GPT Sol', intelligence: 5, speed: 3 },
      'gpt-luna': { display_name: 'GPT Luna', intelligence: 3, speed: 5 },
      'cc-fable': { display_name: 'Claude Fable', sdk: 'claude-code', provider: 'claude-code', model_id: 'claude-fable-5', intelligence: 5, speed: 3 },
      'cc-opus': { display_name: 'Claude Opus', sdk: 'claude-code', provider: 'claude-code', model_id: 'claude-opus-5', intelligence: 5, speed: 3 },
      'cc-sonnet': { display_name: 'Claude Sonnet', sdk: 'claude-code', provider: 'claude-code', model_id: 'claude-sonnet-5', intelligence: 4, speed: 4 },
    },
    customModels: [],
    cannotRun: (m: string) => h.gone.includes(m),
    systemDefaults: h.systemDefaults,
    isLoading: false,
    unread: h.catalogUnread,
  }),
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.prefsUnread ? null : h.preferences, isLoaded: !h.prefsUnread, unread: h.prefsUnread, retry: h.retryPrefs }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync, isPending: false, isError: false }),
}));
vi.mock('@/hooks/useConfiguredProviders', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useConfiguredProviders')>()),
  useConfiguredProviders: () => ({ providers: h.providers, byokEnabled: h.byok }),
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { user_id: h.userId } }) }));
vi.mock('@/hooks/useAccount', () => ({ useAccount: () => ({ policy: h.policy }) }));
vi.mock('@/hooks/useProviderCatalog', () => ({ useProviderCatalog: () => ({ connectionOf: (p: string) => p }) }));
vi.mock('@/config/hostMode', () => ({ get isPlatformMode() { return h.platform; }, HOST_MODE: 'oss', APP_ENTRY_PATH: '/app' }));
vi.mock('../../answers/flow', async () => {
  const { useSyncExternalStore } = await import('react');
  const subscribe = (listener: () => void) => {
    h.nextListeners.add(listener);
    return () => h.nextListeners.delete(listener);
  };
  return {
    useAnswerFlow: () => ({
      isLoading: false,
      answers: { models: h.models, runsOn: 'cloud' },
      first: false,
      tokenTarget: null,
      next: useSyncExternalStore(subscribe, () => h.next),
      progress: h.progress,
      finish: h.finish,
      retryFacts: h.retryFacts,
      toWelcome: h.toWelcome,
      finishing: false,
      finishFailed: false,
      back: null,
    }),
  };
});
/** Where setup resumes, moved as a refetch of the facts would move it. */
const setNext = (next: string) => {
  h.next = next;
  h.nextListeners.forEach((l) => l());
};

import DefaultStep from '../DefaultStep';

function Where() {
  return <p data-testid="where">{useLocation().pathname}</p>;
}

const client = new QueryClient();
const stepTree = () => (
  <QueryClientProvider client={client}>
    <MemoryRouter initialEntries={['/setup/default']}>
      <Routes>
        <Route path="/setup/default" element={<DefaultStep />} />
        <Route path="*" element={<Where />} />
      </Routes>
    </MemoryRouter>
  </QueryClientProvider>
);
const renderStep = () => render(stepTree());
const row = (name: string) => screen.getAllByText(name).map((el) => el.closest('li')).find((li) => li !== null) ?? null;
const saved = () => (h.mutateAsync.mock.calls[0] as unknown as [{ model_preference: Record<string, unknown> }])[0].model_preference;

describe('DefaultStep', () => {
  beforeEach(() => {
    h.models = 'subscription_or_key';
    h.preferences = { model_preference: { preferred_model: 'gpt-sol' } };
    h.systemDefaults = null;
    h.payers = {};
    h.gone = [];
    h.mutateAsync = vi.fn(async () => ({}));
    h.finish = vi.fn(async () => true);
    h.toWelcome = vi.fn();
    h.home = vi.fn(async () => ({ workspace_id: 'home' }));
    h.prefsUnread = false;
    h.retryPrefs = vi.fn();
    h.catalogUnread = false;
    h.retryFacts = vi.fn();
    h.next = 'default';
    h.progress = null;
    h.policy = null;
    h.platform = false;
    h.byok = true;
    h.providers = [{ provider: 'codex-oauth', access_type: 'oauth', state: 'connected' }];
    h.userId = 'u1';
    h.codex = [...codex];
    client.clear();
  });

  it('offers to read the preferences again where the first read failed, instead of loading for good', async () => {
    h.prefsUnread = true;
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retryPrefs).toHaveBeenCalled();
  });

  it('holds on Try again where the models could not be read, and never finishes setup on none', async () => {
    h.catalogUnread = true;
    renderStep();
    expect(screen.queryByRole('button', { name: 'Continue' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retryFacts).toHaveBeenCalled();
    expect(h.finish).not.toHaveBeenCalled();
  });

  it('goes back to a step whose outcome goes once the form is up, instead of letting Continue finish setup', () => {
    renderStep();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeEnabled();
    act(() => setNext('model'));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/model');
    expect(h.finish).not.toHaveBeenCalled();
  });

  it('does not finish setup where a step behind it turned up while Continue ran', async () => {
    let saveLanded!: () => void;
    h.mutateAsync = vi.fn(() => new Promise<void>((resolve) => { saveLanded = resolve; }));
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    act(() => setNext('model'));
    await act(async () => saveLanded());
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/model'));
    expect(h.finish).not.toHaveBeenCalled();
  });

  it('goes back to a step whose outcome is gone, as /setup would, so setup is not finished without it', () => {
    h.next = 'model';
    renderStep();
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/model');
    expect(h.finish).not.toHaveBeenCalled();
  });

  it('shows the recommended setup with its reasons and the ratings', () => {
    renderStep();
    expect(screen.getByText('Recommended setup')).toBeInTheDocument();
    expect(screen.getByText('Highest intelligence rating of your models, and the fastest of those.')).toBeInTheDocument();
    expect(screen.getByText('Fastest of your other models, which is what background work needs.')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /GPT Sol/ })).toBeChecked();
    expect(screen.getAllByRole('img', { name: 'Intelligence: 5 of 5' })).toHaveLength(2);
  });

  it('saves the model picked over the recommended one, with the Background model beside it and nothing else', async () => {
    h.systemDefaults = { fallback_models: ['gpt-luna'], fetch_model: 'gpt-luna' };
    renderStep();
    await userEvent.click(screen.getByRole('radio', { name: /GPT Astra/ }));
    expect(screen.getByRole('button', { name: 'Use recommended' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(h.mutateAsync).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'gpt-astra', preferred_background_model: 'gpt-luna', background_follows: null },
    });
    await waitFor(() => expect(h.toWelcome).toHaveBeenCalled());
    expect(h.finish).toHaveBeenCalled();
  });

  it('goes back to the recommended setup', async () => {
    renderStep();
    await userEvent.click(screen.getByRole('radio', { name: /GPT Astra/ }));
    await userEvent.click(screen.getByRole('button', { name: 'Use recommended' }));
    expect(screen.getByRole('radio', { name: /GPT Sol/ })).toBeChecked();
    expect(screen.queryByRole('button', { name: 'Use recommended' })).toBeNull();
  });

  it('keeps what is saved: an explicit Background, and routing it leaves unwritten', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', preferred_background_model: 'gpt-astra', fallback_models: ['gpt-luna'] } };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Customize' }));
    expect(screen.getByText('Web fetch model')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'gpt-astra', background_follows: null });
  });

  it('keeps a saved Background left unlisted only because its connection\'s status could not be read', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', preferred_background_model: 'grok-fast' } };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'grok-fast', background_follows: null });
  });

  it('shows under Customize the saved Background it keeps unlisted, which Continue saves', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', preferred_background_model: 'grok-fast' } };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Customize' }));
    expect(screen.getByLabelText('Background model')).toHaveValue('grok-fast');
  });

  it('drops a saved Background confirmed not to run any more for the recommended one', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', preferred_background_model: 'gpt-gone' } };
    h.gone = ['gpt-gone'];
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'gpt-luna', background_follows: null });
  });

  it('saves the recommended model in place of a picked one a refetch no longer offers', async () => {
    const view = renderStep();
    await userEvent.click(screen.getByRole('radio', { name: /GPT Astra/ }));
    h.codex = ['gpt-sol', 'gpt-luna'];
    view.rerender(stepTree());
    expect(screen.getByRole('radio', { name: /GPT Sol/ })).toBeChecked();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'gpt-luna', background_follows: null });
  });

  it('saves the recommended Background in place of one picked under Customize that a refetch no longer offers', async () => {
    const view = renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Customize' }));
    await userEvent.selectOptions(screen.getByLabelText('Background model'), 'gpt-astra');
    h.codex = ['gpt-sol', 'gpt-luna'];
    view.rerender(stepTree());
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'gpt-luna', background_follows: null });
  });

  it('saves no routing model picked under Customize that a refetch no longer offers', async () => {
    const view = renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Customize' }));
    await userEvent.selectOptions(screen.getByLabelText('Web fetch model'), 'gpt-astra');
    h.codex = ['gpt-sol', 'gpt-luna'];
    view.rerender(stepTree());
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).not.toHaveProperty('fetch_model');
  });

  it('recommends nothing from a key BYOK is switched off for', () => {
    h.platform = true;
    h.byok = false;
    h.providers = [{ provider: 'codex-oauth', access_type: 'api_key', state: 'connected', masked_key: 'sk-...abcd' }];
    renderStep();
    expect(screen.getByText(/No models are available to you yet/)).toBeInTheDocument();
  });

  it('starts over for another account signing in here', async () => {
    const view = renderStep();
    await userEvent.click(screen.getByRole('radio', { name: /GPT Astra/ }));
    h.userId = 'u2';
    view.rerender(stepTree());
    expect(screen.getByRole('radio', { name: /GPT Sol/ })).toBeChecked();
  });

  it('holds Continue for a key or plan answer that leaves no model to save', () => {
    h.codex = [];
    renderStep();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
  });

  it('recommends nothing from a connection the account\'s rules keep off', () => {
    h.policy = { own_model_access: { api_keys: true, subscriptions: false } };
    renderStep();
    expect(screen.getByText(/No models are available to you yet/)).toBeInTheDocument();
  });

  it('saves nothing once setup was finished, or taken, by the other surface', () => {
    h.progress = { setup_on: 'browser', setup_completed_at: '2026-11-19T00:00:00Z' };
    const view = renderStep();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
    view.unmount();
    h.progress = { setup_on: 'app', setup_completed_at: null };
    renderStep();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
  });

  it('saves the recommended model where the form opened on none and a refetch offers some', async () => {
    h.codex = [];
    const view = renderStep();
    h.codex = [...codex];
    view.rerender(stepTree());
    expect(screen.getByRole('radio', { name: /GPT Sol/ })).toBeChecked();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toMatchObject({ preferred_model: 'gpt-sol' });
  });

  it('drops a saved Background confirmed not to run once the form is up', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', preferred_background_model: 'gpt-astra' } };
    const view = renderStep();
    h.gone = ['gpt-astra'];
    view.rerender(stepTree());
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toEqual({ preferred_model: 'gpt-sol', preferred_background_model: 'gpt-luna', background_follows: null });
  });

  it('keeps an Auto Background saved in Settings as Auto', async () => {
    h.systemDefaults = { background_model: 'gpt-luna' };
    h.preferences = { model_preference: { preferred_model: 'gpt-sol', background_follows: 'deployment' } };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toMatchObject({ preferred_background_model: null, background_follows: 'deployment' });
  });

  it('leaves a Claude Code Default\'s Background to the deployment, named as such', async () => {
    h.models = 'claude_code';
    h.systemDefaults = { background_model: 'gpt-luna' };
    h.preferences = {};
    renderStep();
    expect(screen.getByText('Claude Code runs conversations only, so background work runs on GPT Luna.')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Claude Sonnet/ })).toBeChecked();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toMatchObject({ preferred_model: 'cc-sonnet', preferred_background_model: null, background_follows: 'deployment' });
  });

  it('recommends Claude Code\'s Sonnet, which suits any Claude plan, over a Fable or Opus rated higher', () => {
    h.models = 'claude_code';
    h.preferences = {};
    renderStep();
    expect(screen.getByRole('radio', { name: /Claude Sonnet/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: /Claude Sonnet/ })).toHaveAccessibleName(/Recommended/);
    expect(screen.getByText('A good fit for any Claude plan.')).toBeInTheDocument();
  });

  it('says a Claude Code Default\'s Background bills credits where the deployment\'s model does', () => {
    h.models = 'claude_code';
    h.systemDefaults = { background_model: 'gpt-luna' };
    h.payers = { 'gpt-luna': 'credits' };
    h.preferences = {};
    renderStep();
    expect(screen.getByText('Claude Code runs conversations only, so background work runs on GPT Luna, a LangAlpha model that uses your credits.')).toBeInTheDocument();
  });

  it('resets a Background changed under Customize to the recommended Auto', async () => {
    h.models = 'claude_code';
    h.systemDefaults = { background_model: 'gpt-luna' };
    h.preferences = {};
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Customize' }));
    await userEvent.selectOptions(screen.getByLabelText('Background model'), 'gpt-astra');
    await userEvent.click(screen.getByRole('button', { name: 'Use recommended' }));
    expect(screen.queryByRole('button', { name: 'Use recommended' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(saved()).toMatchObject({ preferred_background_model: null, background_follows: 'deployment' });
  });

  it('sets the user up in steps it ticks off, Home included, before handing over to /welcome', async () => {
    let homeReady: (value: { workspace_id: string }) => void = () => {};
    h.home = vi.fn(() => new Promise((resolve) => { homeReady = resolve; }));
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('Setting you up')).toBeInTheDocument();
    await waitFor(() => expect(row('Getting Home ready')).toHaveAttribute('data-step-state', 'active'));
    expect(screen.getByRole('status')).toHaveTextContent('Getting Home ready');
    expect(row('Saving your models')).toHaveAttribute('data-step-state', 'done');
    expect(h.finish).not.toHaveBeenCalled();
    homeReady({ workspace_id: 'home' });
    await waitFor(() => expect(h.toWelcome).toHaveBeenCalled());
    expect(row('Finishing setup')).toHaveAttribute('data-step-state', 'done');
  });

  it('stays on the step when its models could not be saved', async () => {
    h.mutateAsync = vi.fn(async () => { throw new Error('nope'); });
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByRole('radio', { name: /GPT Sol/ })).toBeInTheDocument();
    expect(h.finish).not.toHaveBeenCalled();
    expect(h.toWelcome).not.toHaveBeenCalled();
  });

  it('comes back to the form when setup could not be finished', async () => {
    h.finish = vi.fn(async () => false);
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByRole('radio', { name: /GPT Sol/ })).toBeInTheDocument();
    expect(h.toWelcome).not.toHaveBeenCalled();
  });

  it('hands nothing over once the user has left the step', async () => {
    let homeReady: (value: { workspace_id: string }) => void = () => {};
    h.home = vi.fn(() => new Promise((resolve) => { homeReady = resolve; }));
    const { unmount } = renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await screen.findByText('Setting you up');
    unmount();
    homeReady({ workspace_id: 'home' });
    await new Promise((resolve) => setTimeout(resolve, 600));
    expect(h.finish).not.toHaveBeenCalled();
    expect(h.toWelcome).not.toHaveBeenCalled();
  });
});
