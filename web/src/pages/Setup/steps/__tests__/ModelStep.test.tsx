/**
 * Claude Code answered where this account can't turn it on: the model step
 * says so and leads back to the questions instead of a token sheet that
 * could never save.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const h = vi.hoisted(() => ({
  setup: null as null | object,
  unread: false,
  change: (() => {}) as () => void,
  models: 'claude_code',
  providers: [] as object[],
  tokenTarget: null as null | object,
  facts: null as null | object,
  custody: null as null | object,
  choose: vi.fn(),
  finish: vi.fn(),
  custodyUnread: false,
  retry: vi.fn(),
  features: [] as object[] | undefined,
  turnOn: vi.fn(),
  turnOnFailed: false,
  turnOnSucceeded: false,
  platform: true,
  refetchFeatures: vi.fn(),
  custodyListeners: new Set<() => void>(),
  settled: (() => true) as (provider: string) => boolean,
  pick: null as string | null,
  saveDefault: vi.fn(async () => true),
  retrySetup: vi.fn(),
  featuresLoading: false,
  policy: null as null | object,
}));
vi.mock('@/hooks/useAccount', async () => {
  const { allows } = await import('@/lib/accountRules');
  const who = { start: 'Your organization', mid: 'your organization' };
  return {
    useAccount: () => ({ policy: h.policy, setterName: () => who }),
    useRule: (rule: Parameters<typeof allows>[1]) => ({ allowed: allows(h.policy as Parameters<typeof allows>[0], rule), who }),
  };
});
/** Changes the choice as a read landing would, re-rendering whoever reads it. */
const setCustody = (custody: object) => {
  h.custody = custody;
  h.custodyListeners.forEach((l) => l());
};
vi.mock('@/api/model', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/model')>()),
  startClaudeCodeSignIn: vi.fn(async () => ({ authorize_url: 'https://claude.ai/oauth', expires_at: new Date(Date.now() + 600_000).toISOString() })),
  saveClaudeCodeComputerToken: vi.fn(async (computerId: string) => ({ computer_id: computerId, state: 'signed_in', source: 'machine' })),
  startClaudeCodeComputerSignIn: vi.fn(async () => ({ authorize_url: 'https://claude.ai/oauth', expires_at: new Date(Date.now() + 600_000).toISOString() })),
  submitClaudeCodeComputerSignInCode: vi.fn(),
}));
vi.mock('@/hooks/useConfiguredProviders', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useConfiguredProviders')>()),
  useConfiguredProviders: () => ({ providers: h.providers }),
}));
vi.mock('@/hooks/useProviderCatalog', () => ({ useProviderCatalog: () => ({ planGroups: [] }) }));
vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return h.platform; },
  get HOST_MODE() { return h.platform ? 'platform' : 'oss'; },
  APP_ENTRY_PATH: '/app',
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: h.setup, isLoading: false, unread: h.unread, retry: h.retrySetup }),
  updateComputers: vi.fn(),
}));
vi.mock('@/hooks/useClaudeCodeCustody', async () => {
  const { useSyncExternalStore } = await import('react');
  const subscribe = (listener: () => void) => {
    h.custodyListeners.add(listener);
    return () => h.custodyListeners.delete(listener);
  };
  return {
    useClaudeCodeCustody: () => {
      const custody = useSyncExternalStore(subscribe, () => h.custody);
      return { custody, choose: h.choose, choosing: null, isLoading: false, unread: h.custodyUnread, retry: h.retry };
    },
  };
});
vi.mock('@/hooks/useFeatures', () => ({
  useFeatures: () => ({
    data: h.featuresLoading ? undefined : h.features,
    isLoading: h.featuresLoading,
    isError: !h.featuresLoading && h.features === undefined,
    refetch: h.refetchFeatures,
  }),
  useSetFeatureOverride: () => ({ mutate: h.turnOn, isPending: false, isError: h.turnOnFailed, isSuccess: h.turnOnSucceeded }),
}));
vi.mock('../../answers/connectedDefault', () => ({
  useSaveConnectedDefault: () => ({ modelFor: () => h.pick, settled: (p: string) => h.settled(p), save: h.saveDefault, saving: false }),
}));
vi.mock('../../answers/flow', () => ({
  useAnswerFlow: () => ({
    isLoading: false,
    answers: { models: h.models, runsOn: 'own_computer' },
    first: false,
    tokenTarget: h.tokenTarget,
    facts: h.facts,
    finish: h.finish,
    finishing: false,
    finishFailed: false,
    back: null,
  }),
  useChangeAnswers: () => h.change,
}));

import { saveClaudeCodeComputerToken, startClaudeCodeSignIn, submitClaudeCodeComputerSignInCode } from '@/api/model';
import { runAuthResets } from '@/lib/authResets';
import { CLAUDE_CODE_DISCLOSURE, CLAUDE_CODE_FEATURE } from '@/lib/claudeCode';
import ModelStep from '../ModelStep';

function Where() {
  return <p data-testid="where">{useLocation().pathname}</p>;
}

beforeEach(() => {
  h.policy = null;
  h.unread = false;
  h.retrySetup = vi.fn();
  h.featuresLoading = false;
  h.features = [];
  h.turnOn = vi.fn();
});

function renderStep() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/setup/model']}>
        <Routes>
          <Route path="/setup/model" element={<ModelStep />} />
          <Route path="*" element={<Where />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('ModelStep, Claude Code answered', () => {
  beforeEach(() => {
    h.setup = null;
    h.unread = false;
    h.change = vi.fn();
    h.models = 'claude_code';
    h.providers = [];
    h.tokenTarget = null;
    h.facts = null;
    h.custody = null;
    h.choose = vi.fn(async () => ({}));
    h.finish = vi.fn(async () => true);
  });

  it('says Claude Code is not available and offers to change the answers', async () => {
    renderStep();
    expect(screen.getByText("Claude Code isn't available to you")).toBeInTheDocument();
    expect(screen.queryByText('Add your Claude setup token')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Change my answers' }));
    expect(h.change).toHaveBeenCalled();
  });

  it('goes on to the token step where Claude Code is offered', () => {
    h.setup = { on: false };
    renderStep();
    expect(screen.queryByTestId('setup-claude-code-unavailable')).toBeNull();
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/computer');
  });

  it('reads the setup again where it could not be read, rather than saying unavailable or asking for a token', async () => {
    h.unread = true;
    h.tokenTarget = { kind: 'computer', computerId: 'c1', name: 'lab-server' };
    renderStep();
    expect(screen.queryByTestId('setup-claude-code-unavailable')).toBeNull();
    expect(screen.queryByLabelText('Setup token')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retrySetup).toHaveBeenCalled();
  });
});

describe('ModelStep, Claude Code on a computer of the user\'s own', () => {
  beforeEach(() => {
    h.models = 'claude_code';
    h.setup = { on: true };
    h.tokenTarget = { kind: 'computer', computerId: 'c1', name: 'lab-server' };
    h.facts = { claudeCodeToken: true };
    h.custody = { mode: 'stored', offered: ['stored'] };
    h.choose = vi.fn(async () => ({}));
    h.finish = vi.fn(async () => true);
  });

  it('keeps a token stored for the cloud, which only Settings deletes, on the user\'s confirmation', async () => {
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(h.choose).not.toHaveBeenCalled();
    expect(h.finish).toHaveBeenCalled();
  });

  it('saves the computer\'s token without touching the one stored for the cloud', async () => {
    h.facts = { claudeCodeToken: false };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    await userEvent.type(screen.getByLabelText('Setup token'), 'sk-ant-oat01-test');
    await userEvent.click(screen.getByRole('button', { name: 'Save token' }));
    expect(saveClaudeCodeComputerToken).toHaveBeenCalledWith('c1', 'sk-ant-oat01-test');
    // Saved: the form lets the token go.
    await waitFor(() => expect(screen.getByLabelText('Setup token')).toHaveValue(''));
    expect(h.choose).not.toHaveBeenCalled();
  });

  it('starts the sheet over where Home moves to another computer while it is open', async () => {
    h.facts = { claudeCodeToken: false };
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const tree = () => (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={['/setup/model']}>
          <Routes>
            <Route path="/setup/model" element={<ModelStep />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
    const view = render(tree());
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    await userEvent.type(screen.getByLabelText('Setup token'), 'sk-ant-oat01-test');
    h.tokenTarget = { kind: 'computer', computerId: 'c2', name: 'studio' };
    view.rerender(tree());
    expect(screen.getByRole('button', { name: 'Already have a setup token? Paste it' })).toHaveAttribute('aria-expanded', 'false');
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    expect(screen.getByLabelText('Setup token')).toHaveValue('');
  });

  it('holds Replace token\'s Cancel while a pasted token is saved', async () => {
    let land = () => {};
    vi.mocked(saveClaudeCodeComputerToken).mockImplementationOnce((computerId: string) => new Promise((resolve) => {
      land = () => resolve({ computer_id: computerId, state: 'signed_in', source: 'machine' } as never);
    }));
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Replace token' }));
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeEnabled();
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    await userEvent.type(screen.getByLabelText('Setup token'), 'sk-ant-oat01-test');
    await userEvent.click(screen.getByRole('button', { name: 'Save token' }));
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled();
    await act(async () => land());
    await waitFor(() => expect(screen.getByTestId('setup-token-saved')).toBeInTheDocument());
  });

  it('holds Replace token\'s Cancel while a code from signing in is saved, and lets it go if that fails', async () => {
    let refuse = () => {};
    vi.mocked(submitClaudeCodeComputerSignInCode).mockImplementationOnce(() => new Promise((_, reject) => {
      refuse = () => reject(new Error('down'));
    }));
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Replace token' }));
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    await userEvent.type(await screen.findByLabelText('Code from Claude'), 'code-123{Enter}');
    await waitFor(() => expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled());
    await act(async () => refuse());
    await waitFor(() => expect(screen.getByRole('button', { name: 'Cancel' })).toBeEnabled());
  });

  it('takes back a Replace token without unfolding the paste form', async () => {
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Replace token' }));
    expect(screen.queryByTestId('setup-token-saved')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.getByTestId('setup-token-saved')).toBeInTheDocument();
  });

  it('opens with the pasted token folded, and folded again each time the step opens', async () => {
    h.facts = { claudeCodeToken: false };
    const view = renderStep();
    const toggle = () => screen.getByRole('button', { name: 'Already have a setup token? Paste it' });
    expect(toggle()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByLabelText('Setup token')).toBeNull();
    await userEvent.click(toggle());
    expect(toggle()).toHaveAttribute('aria-expanded', 'true');
    view.unmount();
    renderStep();
    expect(toggle()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByLabelText('Setup token')).toBeNull();
  });

  it('shows no token form while Claude Code is turning on, nor while the flags that say whether to are read', () => {
    h.facts = { claudeCodeToken: false };
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    const view = renderStep();
    expect(h.turnOn).toHaveBeenCalledWith({ key: CLAUDE_CODE_FEATURE, enabled: true });
    expect(screen.queryByRole('button', { name: 'Already have a setup token? Paste it' })).toBeNull();
    view.unmount();
    h.featuresLoading = true;
    renderStep();
    expect(screen.queryByRole('button', { name: 'Already have a setup token? Paste it' })).toBeNull();
  });

  it('neither turns Claude Code on nor asks for its token where a rule keeps the user\'s own subscriptions off', () => {
    h.facts = { claudeCodeToken: false };
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    h.policy = { own_model_access: { api_keys: true, subscriptions: false } };
    renderStep();
    expect(screen.getByText("Your organization doesn't allow your own subscriptions.")).toBeInTheDocument();
    expect(h.turnOn).not.toHaveBeenCalled();
  });

  it('turns Claude Code on again for an account that signs in in place of the one it was turned on for', () => {
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const tree = () => (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={['/setup/model']}>
          <Routes>
            <Route path="/setup/model" element={<ModelStep />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
    const view = render(tree());
    expect(h.turnOn).toHaveBeenCalledTimes(1);
    runAuthResets();
    h.setup = { on: true };
    view.rerender(tree());
    h.setup = { on: false };
    view.rerender(tree());
    expect(h.turnOn).toHaveBeenCalledTimes(2);
  });

  it('chooses nothing where no token is stored for the cloud', async () => {
    h.custody = { mode: 'local', offered: ['stored'] };
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(h.choose).not.toHaveBeenCalled();
    expect(h.finish).toHaveBeenCalled();
  });
});

describe('ModelStep, Claude Code on the cloud computer', () => {
  beforeEach(() => {
    h.models = 'claude_code';
    h.setup = { on: true };
    h.tokenTarget = { kind: 'saved' };
    h.facts = { claudeCodeToken: false };
    h.custody = { mode: 'local', offered: ['stored'] };
    h.choose = vi.fn(async () => ({}));
    h.custodyUnread = false;
    h.retry = vi.fn();
    h.features = [];
    h.turnOn = vi.fn();
    h.turnOnFailed = false;
    h.turnOnSucceeded = false;
    h.platform = true;
    h.refetchFeatures = vi.fn();
    vi.mocked(startClaudeCodeSignIn).mockClear();
  });

  /** A consent write the test answers when it chooses. */
  const pendingChoice = () => {
    let answer!: { resolve: (v: unknown) => void; reject: (e: unknown) => void };
    h.choose = vi.fn(() => new Promise((resolve, reject) => { answer = { resolve, reject }; }));
    return () => answer;
  };
  const agree = () => screen.getByRole('checkbox', { name: 'I agree to let LangAlpha store my setup token' });

  it('offers to turn Claude Code on again where turning it on failed, instead of loading for good', async () => {
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    h.turnOnFailed = true;
    renderStep();
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.turnOn).toHaveBeenCalledTimes(2);
    expect(h.turnOn).toHaveBeenLastCalledWith({ key: CLAUDE_CODE_FEATURE, enabled: true });
  });

  it('offers to turn Claude Code on again where it reads off after turning on went through', async () => {
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    h.turnOnSucceeded = true;
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.turnOn).toHaveBeenLastCalledWith({ key: CLAUDE_CODE_FEATURE, enabled: true });
  });

  it('refuses storing the token on the hosted server where it offers no choice', () => {
    h.custody = null;
    renderStep();
    expect(screen.getByTestId('setup-token-refused')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Change my answers' })).toBeInTheDocument();
  });

  it('opens the sheet with no agreement asked on a self-hosted server that offers no choice', () => {
    h.platform = false;
    h.custody = null;
    renderStep();
    expect(screen.queryByTestId('setup-token-refused')).toBeNull();
    expect(screen.queryByTestId('token-consent')).toBeNull();
    expect(screen.getByRole('button', { name: 'Sign in with Claude' })).toBeEnabled();
  });

  it('keeps the box ticked and locked while the agreement is recorded, and lets it go only if that fails', async () => {
    const answer = pendingChoice();
    renderStep();
    await userEvent.click(agree());
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    expect(agree()).toBeChecked();
    expect(agree()).toBeDisabled();
    await act(async () => answer().reject(new Error('refused')));
    expect(agree()).toBeChecked();
    expect(agree()).toBeEnabled();
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
  });

  it('records the agreement once for two ways in at a time, and keeps the box locked until that one write ends', async () => {
    const answer = pendingChoice();
    renderStep();
    await userEvent.click(agree());
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    await userEvent.type(screen.getByLabelText('Setup token'), 'sk-ant-oat01-test');
    await userEvent.click(screen.getByRole('button', { name: 'Save token' }));
    expect(h.choose).toHaveBeenCalledTimes(1);
    expect(agree()).toBeDisabled();
    await act(async () => answer().reject(new Error('refused')));
    expect(h.choose).toHaveBeenCalledTimes(1);
    expect(agree()).toBeEnabled();
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
  });

  it('clears the box where the agreement is withdrawn elsewhere, so it must be ticked again', () => {
    renderStep();
    act(() => { agree().click(); });
    expect(agree()).toBeChecked();
    act(() => setCustody({ mode: 'stored', offered: ['stored'] }));
    expect(agree()).toBeDisabled();
    act(() => setCustody({ mode: 'local', offered: ['stored'] }));
    expect(agree()).not.toBeChecked();
    expect(agree()).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Sign in with Claude' })).toBeDisabled();
  });

  it('offers to read the flags again where they could not be read, rather than refusing', async () => {
    h.features = undefined;
    renderStep();
    expect(screen.queryByTestId('setup-token-refused')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.refetchFeatures).toHaveBeenCalled();
  });

  it('asks for no sign-in where the step closed while the agreement was recorded', async () => {
    const answer = pendingChoice();
    const { unmount } = renderStep();
    await userEvent.click(agree());
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    unmount();
    await act(async () => answer().resolve({}));
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
  });

  it('shows no sheet while Claude Code is turning on, so nothing is added before the agreement is known', () => {
    h.setup = { on: false };
    h.features = [{ key: CLAUDE_CODE_FEATURE, gate: 'opt_in' }];
    renderStep();
    expect(h.turnOn).toHaveBeenCalledWith({ key: CLAUDE_CODE_FEATURE, enabled: true });
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    expect(screen.queryByLabelText('Setup token')).toBeNull();
  });

  it('offers to read the choice again where it could not be read', async () => {
    h.custodyUnread = true;
    renderStep();
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    expect(h.retry).toHaveBeenCalled();
  });

  it('leads back to the questions where this server runs Claude Code on no cloud computer', () => {
    h.custody = { mode: 'stored', offered: ['stored'], refusal: 'needs_own_computer' };
    renderStep();
    expect(screen.getByTestId('setup-token-refused')).toHaveTextContent('Paused on cloud computers on this server.');
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Change my answers' })).toBeInTheDocument();
  });

  it('says so and leads back to the questions where storing the token is not open to the user', () => {
    h.custody = { mode: 'local', offered: [] };
    renderStep();
    expect(screen.getByTestId('setup-token-refused')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Change my answers' })).toBeInTheDocument();
  });

  it('holds both ways to add a token until the user agrees: no custody write, no sign-in', async () => {
    renderStep();
    expect(screen.getByTestId('token-consent')).toBeInTheDocument();
    const agree = screen.getByRole('checkbox', { name: 'I agree to let LangAlpha store my setup token' });
    expect(agree).not.toBeChecked();
    const signIn = screen.getByRole('button', { name: 'Sign in with Claude' });
    expect(signIn).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: 'Already have a setup token? Paste it' }));
    await userEvent.type(screen.getByLabelText('Setup token'), 'sk-ant-oat01-test');
    expect(screen.getByRole('button', { name: 'Save token' })).toBeDisabled();
    await userEvent.click(signIn);
    expect(h.choose).not.toHaveBeenCalled();
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
  });

  it('records the agreement, once ticked, before signing in', async () => {
    renderStep();
    await userEvent.click(screen.getByRole('checkbox', { name: 'I agree to let LangAlpha store my setup token' }));
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    expect(h.choose).toHaveBeenCalledWith({ mode: 'stored', disclosure: CLAUDE_CODE_DISCLOSURE });
    expect(startClaudeCodeSignIn).toHaveBeenCalled();
    expect(h.choose.mock.invocationCallOrder[0]).toBeLessThan(vi.mocked(startClaudeCodeSignIn).mock.invocationCallOrder[0]);
  });

  it('asks for no sign-in where the agreement is refused', async () => {
    h.choose = vi.fn(async () => { throw new Error('refused'); });
    renderStep();
    await userEvent.click(screen.getByRole('checkbox', { name: 'I agree to let LangAlpha store my setup token' }));
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
  });

  it('shows an agreement already recorded as ticked for good, and asks for it no more', async () => {
    h.custody = { mode: 'stored', offered: ['stored'] };
    renderStep();
    const agree = screen.getByRole('checkbox', { name: 'I agree to let LangAlpha store my setup token' });
    expect(agree).toBeChecked();
    expect(agree).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    expect(h.choose).not.toHaveBeenCalled();
    expect(startClaudeCodeSignIn).toHaveBeenCalled();
  });
});

describe('ModelStep, a subscription or an API key answered', () => {
  beforeEach(() => {
    h.models = 'subscription_or_key';
    h.providers = [];
    h.facts = null;
    h.settled = () => true;
    h.pick = null;
    h.saveDefault = vi.fn(async () => true);
  });

  function renderConnected(settledNow: boolean) {
    h.facts = { providerConnected: true };
    h.settled = () => settledNow;
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const tree = () => (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[{ pathname: '/setup/model', state: { connected: 'openai' } }]}>
          <Routes>
            <Route path="/setup/model" element={<ModelStep />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
    return { view: render(tree()), tree };
  }

  it('holds Continue on a key just connected until its default is decided, then saves it', async () => {
    const { view, tree } = renderConnected(false);
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
    h.settled = () => true;
    h.pick = 'gpt-x';
    view.rerender(tree());
    await waitFor(() => expect(h.saveDefault).toHaveBeenCalledWith('gpt-x'));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Continue' })).toBeEnabled());
  });

  it('lets Continue go once it is decided that no default is saved', async () => {
    renderConnected(true);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Continue' })).toBeEnabled());
    expect(h.saveDefault).not.toHaveBeenCalled();
  });

  it('lists the subscription chooser, with keys as a quiet way on under it', async () => {
    renderStep();
    expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Continue with an API key' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/provider');
  });

  it('does not count a sign-in the server holds as one the user connected', async () => {
    h.providers = [{ provider: 'codex-oauth', access_type: 'oauth', masked_key: '(server)', state: 'connected' }];
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: /ChatGPT/ }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/connect');
  });

  it('offers only the ways in the account\'s rules leave open, and says which are off', () => {
    h.policy = { own_model_access: { api_keys: false, subscriptions: true } };
    const view = renderStep();
    expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Continue with an API key' })).toBeNull();
    expect(screen.getByText("Your organization doesn't allow your own API keys.")).toBeInTheDocument();
    view.unmount();
    h.policy = { own_model_access: { api_keys: true, subscriptions: false } };
    renderStep();
    expect(screen.queryByRole('heading', { name: 'Sign in' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Continue with an API key' })).toBeInTheDocument();
  });

  it('takes a sign-in of the user\'s own to its models', async () => {
    h.providers = [{ provider: 'codex-oauth', access_type: 'oauth', masked_key: null, state: 'connected' }];
    renderStep();
    await userEvent.click(screen.getByRole('button', { name: /ChatGPT/ }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/models');
  });
});
