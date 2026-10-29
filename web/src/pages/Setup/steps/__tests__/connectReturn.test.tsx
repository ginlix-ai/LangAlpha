/**
 * A connection made for a page that sent the user into setup goes straight back
 * there, naming the provider by the key it was saved under, instead of walking
 * on through this app's model steps. Without a return the steps are unchanged.
 */
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from '@testing-library/react';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { captureSetupReturn, readSetupReturn } from '@/lib/setupReturn';

const h = vi.hoisted(() => ({
  assign: vi.fn(),
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  updatePreferences: vi.fn(async (_payload: unknown) => ({})),
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  preferences: null as Record<string, unknown> | null,
  catalog: [] as unknown[],
  configured: [] as string[],
  /** Subscriptions stored but needing their sign-in again. */
  reauth: [] as string[],
  submitClaudeCallback: vi.fn(),
  initiateClaudeOAuth: vi.fn(),
  initiateDevice: vi.fn(),
  pollDevice: vi.fn(),
}));

vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: '/account' }));
vi.mock('@/hooks/useApiKeys', () => ({
  useApiKeys: () => ({ apiKeys: null, isLoading: false }),
  useUpdateApiKeys: () => ({ mutateAsync: h.mutateAsync }),
}));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: h.preferences }) }));
vi.mock('@/hooks/useStoredPreferences', () => ({ useReadStoredPreferences: () => async () => h.preferences }));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: h.updatePreferences }),
}));
vi.mock('@/hooks/useModels', () => ({
  useModels: () => ({ models: { provider_catalog: h.catalog }, isLoading: false }),
}));
vi.mock('@/hooks/useConfiguredProviders', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useConfiguredProviders')>()),
  useConfiguredProviders: () => ({
    providers: [
      ...h.configured.map((provider) => ({ provider, state: 'connected' })),
      ...h.reauth.map((provider) => ({ provider, state: 'needs_reauth' })),
    ],
    configuredSet: new Set([...h.configured, ...h.reauth]),
  }),
}));
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({ rawModels: {}, metadata: {}, isLoading: false }),
}));
vi.mock('@/api/client', () => ({ api: { get: h.apiGet, post: h.apiPost, defaults: { baseURL: '' } } }));
vi.mock('@/api/oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/oauth')>()),
  initiateDevice: h.initiateDevice,
  pollDevice: h.pollDevice,
  initiateClaudeOAuth: h.initiateClaudeOAuth,
  submitClaudeCallback: h.submitClaudeCallback,
}));

import { ApiKeyConnect } from '../connectStep/ApiKeyConnect';
import { CustomProviderConnect } from '../connectStep/CustomProviderConnect';
import { SubscriptionConnect } from '../connectStep/SubscriptionConnect';
import ProviderStep from '../ProviderStep';
import ConnectStep from '../ConnectStep';
import type { LocationState } from '../connectStep/shared';

/**
 * The way back is a full page load, which jsdom will not perform. `assign` is an
 * own non-configurable property of Location, so the whole object is swapped
 * rather than the one method spied.
 */
const realLocation = window.location;
Object.defineProperty(window, 'location', {
  configurable: true,
  value: { href: realLocation.href, origin: realLocation.origin, assign: h.assign },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

const returnUrl = () => `${window.location.origin}/account/onboarding/models?step=model`;
const keepReturn = () => captureSetupReturn(`?return=${encodeURIComponent(returnUrl())}`);

/** The step at `route` with its state, and the model step it would move on to. */
function renderStep(element: React.ReactElement, route = '/setup/connect') {
  return renderWithProviders(
    <Routes>
      <Route path={route} element={element} />
      <Route path="/setup/models" element={<p>models step</p>} />
    </Routes>,
    { route },
  );
}

// DashScope as the catalog lists it: China on the entry, international as a
// region of it.
const dashscope = {
  provider: 'dashscope',
  display_name: 'DashScope',
  brand_key: 'dashscope',
  access_type: 'api_key',
  sdk: 'openai',
  base_url: 'https://cn.example.com/v1',
  region: 'cn',
  region_variants: [
    { provider: 'dashscope-intl', display_name: 'DashScope', region: 'intl', base_url: 'https://intl.example.com/v1', sdk: 'openai' },
  ],
};

const apiKeyState: LocationState = { method: 'api_key', provider: 'dashscope' };

afterEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
  h.preferences = null;
  h.catalog = [];
  h.configured = [];
  h.reauth = [];
});

describe('ApiKeyConnect with a return', () => {
  it('goes back naming the key the provider was saved under', async () => {
    keepReturn();
    h.catalog = [dashscope];
    const user = userEvent.setup();
    // The provider step names the region it picked by the key it saves under.
    renderStep(<ApiKeyConnect state={{ method: 'api_key', provider: 'dashscope-intl' }} />);

    expect(screen.getByLabelText(/Base URL/)).toHaveValue('https://intl.example.com/v1');
    await user.type(screen.getByPlaceholderText(/key/i), 'sk-test');
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledTimes(1));
    expect(h.mutateAsync.mock.calls[0][0]).toMatchObject({ api_keys: { 'dashscope-intl': 'sk-test' } });
    expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=dashscope-intl`);
    expect(screen.queryByText('models step')).toBeNull();
  });

  it('without a return, moves on to the model step', async () => {
    h.catalog = [dashscope];
    const user = userEvent.setup();
    renderStep(<ApiKeyConnect state={apiKeyState} />);

    await user.type(screen.getByPlaceholderText(/key/i), 'sk-test');
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    expect(await screen.findByText('models step')).toBeInTheDocument();
    expect(h.mutateAsync.mock.calls[0][0]).toMatchObject({ api_keys: { dashscope: 'sk-test' } });
    expect(h.assign).not.toHaveBeenCalled();
  });

  it('keeps the button usable after a failed save', async () => {
    keepReturn();
    h.catalog = [dashscope];
    h.mutateAsync.mockRejectedValueOnce({ response: { status: 400, data: { detail: 'key rejected' } } });
    const user = userEvent.setup();
    renderStep(<ApiKeyConnect state={apiKeyState} />);

    await user.type(screen.getByPlaceholderText(/key/i), 'sk-test');
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('key rejected');
    expect(screen.getByRole('button', { name: 'Continue' })).toBeEnabled();
    expect(h.assign).not.toHaveBeenCalled();
  });

  it("saves a key for a provider of the user's own under its name", async () => {
    keepReturn();
    const user = userEvent.setup();
    renderStep(<ApiKeyConnect state={{ method: 'api_key', provider: 'my-server', displayName: 'my-server' }} />);

    await user.type(screen.getByPlaceholderText(/key/i), 'sk-test');
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=my-server`));
    expect(h.mutateAsync.mock.calls[0][0]).toMatchObject({ api_keys: { 'my-server': 'sk-test' } });
  });

  it('takes a local server on to its models, keeping the return for the last step', async () => {
    keepReturn();
    h.catalog = [{
      provider: 'lmstudio',
      display_name: 'LM Studio',
      brand_key: 'lmstudio',
      access_type: 'local',
      dynamic_models: true,
      base_url: 'http://localhost:1234/v1',
    }];
    const user = userEvent.setup();
    renderStep(<ApiKeyConnect state={{ method: 'local', provider: 'lmstudio' }} />);

    await user.click(screen.getByRole('button', { name: 'Continue' }));

    expect(await screen.findByText('models step')).toBeInTheDocument();
    expect(h.mutateAsync.mock.calls[0][0]).toMatchObject({
      api_keys: { lmstudio: '' },
      base_urls: { lmstudio: 'http://localhost:1234/v1' },
    });
    expect(h.assign).not.toHaveBeenCalled();
    expect(readSetupReturn()).toBe(returnUrl());
  });
});

describe('CustomProviderConnect with a return', () => {
  async function fillEndpoint(user: ReturnType<typeof userEvent.setup>) {
    await user.type(screen.getByLabelText('Provider name'), 'My Server');
    await user.type(screen.getByLabelText('Base URL'), 'https://llm.example.com/v1');
    await user.type(screen.getByLabelText('API key'), 'sk-test');
  }

  it('goes back naming the new provider by its slug, with the tuned models it serves', async () => {
    keepReturn();
    h.apiPost.mockResolvedValue({
      data: {
        status: 'ok',
        // On another API than its own the model takes no levels, and the entry says so.
        recognized: [{ name: 'gpt-x', model_id: 'openai/gpt-x', display_name: 'GPT X', context: 200000, reasoning: { efforts: [] } }],
        others: [],
      },
    });
    const user = userEvent.setup();
    renderStep(<CustomProviderConnect state={{ method: 'api_key', isCustom: true }} />);

    await fillEndpoint(user);
    // A tuned model the endpoint serves arrives picked.
    expect(await screen.findByRole('checkbox', { name: 'GPT X' }, { timeout: 3000 })).toBeChecked();
    expect(h.apiPost).toHaveBeenCalledWith(
      '/api/v1/providers/custom/models',
      { api_format: 'openai-completions', base_url: 'https://llm.example.com/v1', api_key: 'sk-test' },
      expect.anything(),
    );
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=my-server`));
    expect(h.updatePreferences.mock.calls[0][0]).toMatchObject({
      model_preference: {
        custom_models: [{ name: 'gpt-x', model_id: 'openai/gpt-x', provider: 'my-server', context: 200000, reasoning: { efforts: [] } }],
      },
    });
    expect(screen.queryByText('models step')).toBeNull();
  });

  it('asks the window of a model it does not know, filled from the endpoint list', async () => {
    keepReturn();
    h.apiPost.mockResolvedValue({
      data: { status: 'ok', recognized: [], others: [{ id: 'house-7b', display_name: 'House 7B', context_window: 32768 }] },
    });
    const user = userEvent.setup();
    renderStep(<CustomProviderConnect state={{ method: 'api_key', isCustom: true }} />);

    await fillEndpoint(user);
    await user.type(await screen.findByLabelText('Model ID', {}, { timeout: 3000 }), 'house-7b');
    expect(screen.getByLabelText('Context window')).toHaveValue('32768');
    await user.clear(screen.getByLabelText('Context window'));
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: '128K' }));
    await user.type(screen.getByLabelText('Max output tokens (optional)'), '8K');
    await user.click(screen.getByRole('button', { name: 'Continue' }));

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=my-server`));
    expect(h.updatePreferences.mock.calls[0][0]).toMatchObject({
      model_preference: {
        custom_models: [{ name: 'house-7b', model_id: 'house-7b', provider: 'my-server', context: 128000, parameters: { max_tokens: 8000 } }],
      },
    });
  });
});

describe('CustomProviderConnect warning', () => {
  it('warns of a proxy only once an address is typed, and not for a cloud we recognize', async () => {
    h.apiPost.mockResolvedValue({ data: { status: 'ok', recognized: [], others: [] } });
    const user = userEvent.setup();
    renderStep(<CustomProviderConnect state={{ method: 'api_key', isCustom: true }} />);
    expect(screen.queryByRole('note')).toBeNull();

    await user.type(screen.getByLabelText('Base URL'), 'https://relay.example.com/v1');
    expect(await screen.findByRole('note', {}, { timeout: 3000 })).toHaveTextContent('Not verified by us.');

    await user.clear(screen.getByLabelText('Base URL'));
    expect(screen.queryByRole('note')).toBeNull();
    await user.type(screen.getByLabelText('Base URL'), 'https://bedrock-mantle.us-east-1.api.aws/anthropic');
    await new Promise((r) => setTimeout(r, 800));
    expect(screen.queryByRole('note')).toBeNull();
  });
});

describe('SubscriptionConnect with a return', () => {
  it('goes back naming the sign-in provider', async () => {
    keepReturn();
    const open = vi.spyOn(window, 'open').mockReturnValue(null);
    h.initiateClaudeOAuth.mockResolvedValue({ authorize_url: 'https://auth.example.com/authorize', expires_at: null });
    h.submitClaudeCallback.mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderStep(<SubscriptionConnect state={{ method: 'oauth', provider: 'claude-oauth', displayName: 'Claude' }} />);

    await user.click(screen.getByRole('checkbox'));
    await user.click(screen.getByRole('button', { name: 'Continue' }));
    // A paste that is the code connects by itself, with no click on Connect.
    await user.click(await screen.findByRole('textbox'));
    await user.paste('code#state');

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=claude-oauth`));
    expect(h.submitClaudeCallback).toHaveBeenCalledWith('code#state');
    expect(screen.queryByText('models step')).toBeNull();
    expect(open).not.toHaveBeenCalled();
  });

  it('goes back once the device sign-in completes', async () => {
    keepReturn();
    const open = vi.spyOn(window, 'open').mockReturnValue(null);
    h.initiateDevice.mockResolvedValue({
      user_code: 'ABCD-1234',
      verification_url: 'https://auth.example.com/device',
      interval: 0.001,
      expires_at: null,
    });
    h.pollDevice.mockResolvedValueOnce({ status: 'pending' }).mockResolvedValue({ status: 'success' });
    const user = userEvent.setup();
    renderStep(<SubscriptionConnect state={{ method: 'oauth', provider: 'codex-oauth', displayName: 'Codex' }} />);

    await user.click(screen.getByRole('checkbox'));
    await user.click(screen.getByRole('button', { name: /get my code/i }));

    await vi.waitFor(() => expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=codex-oauth`));
    expect(h.pollDevice).toHaveBeenCalledTimes(2);
    // Getting the code never sends the user away; only their own click on
    // "Open OpenAI" does.
    expect(open).not.toHaveBeenCalled();
    expect(screen.queryByText('models step')).toBeNull();
  });
});

describe('ProviderStep with a return', () => {
  const catalogEntry = {
    provider: 'deepseek',
    display_name: 'DeepSeek',
    brand_key: 'deepseek',
    access_type: 'api_key',
  };

  it('goes back at once for a provider already connected', async () => {
    keepReturn();
    h.catalog = [catalogEntry];
    h.configured = ['deepseek'];
    const user = userEvent.setup();
    renderStep(<ProviderStep />, '/setup/provider');

    await user.click(screen.getByRole('button', { name: /DeepSeek/ }));

    expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=deepseek`);
    expect(screen.queryByText('models step')).toBeNull();
  });

  it('goes back at once for a custom provider already set up', async () => {
    keepReturn();
    h.catalog = [catalogEntry];
    h.configured = ['my-server'];
    h.preferences = { model_preference: { custom_providers: [{ name: 'my-server', parent_provider: 'openai' }] } };
    const user = userEvent.setup();
    renderStep(<ProviderStep />, '/setup/provider');

    await user.click(screen.getByRole('button', { name: /my-server/ }));

    expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=my-server`);
  });

  it('does not report a custom provider without a key as connected', async () => {
    keepReturn();
    h.catalog = [catalogEntry];
    h.preferences = { model_preference: { custom_providers: [{ name: 'my-server', parent_provider: 'openai' }] } };
    const user = userEvent.setup();
    renderStep(<ProviderStep />, '/setup/provider');

    await user.click(screen.getByRole('button', { name: /my-server/ }));

    expect(await screen.findByText('models step')).toBeInTheDocument();
    expect(h.assign).not.toHaveBeenCalled();
    expect(readSetupReturn()).toBe(returnUrl());
  });

  it('without a return, moves on to the model step', async () => {
    h.catalog = [catalogEntry];
    h.configured = ['deepseek'];
    const user = userEvent.setup();
    renderStep(<ProviderStep />, '/setup/provider');

    await user.click(screen.getByRole('button', { name: /DeepSeek/ }));

    expect(await screen.findByText('models step')).toBeInTheDocument();
    expect(h.assign).not.toHaveBeenCalled();
  });

  it('goes back naming the plan region already saved, from the subscriptions', async () => {
    keepReturn();
    // A coding plan is a subscription: listed under the plan's own name, its
    // regions the only choice left.
    h.catalog = [
      { provider: 'z-ai', display_name: 'Zhipu AI', group: 'z-ai', group_display_name: 'Zhipu AI', access_type: 'api_key', region: 'intl', base_url: 'https://intl.example' },
      { provider: 'z-ai-coding', display_name: 'GLM Coding Plan', group: 'z-ai', group_display_name: 'Zhipu AI', access_type: 'coding_plan', region: 'intl', base_url: 'https://plan.example' },
      { provider: 'z-ai-cn-coding', display_name: 'GLM Coding Plan', group: 'z-ai', group_display_name: 'Zhipu AI', access_type: 'coding_plan', region: 'cn', base_url: 'https://cn.example' },
    ];
    h.configured = ['z-ai-cn-coding'];
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <MemoryRouter initialEntries={[{ pathname: '/setup/provider', state: { list: 'oauth' } }]}>
          <Routes>
            <Route path="/setup/provider" element={<ProviderStep />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    // The plan starts on the region its key is saved for, already connected.
    await user.click(screen.getByRole('button', { name: /GLM Coding Plan/ }));

    expect(h.assign).toHaveBeenCalledWith(`${returnUrl()}&connected=z-ai-cn-coding`);
  });

  it('a provider not yet connected still goes to its connect step', async () => {
    keepReturn();
    h.catalog = [catalogEntry];
    const user = userEvent.setup();
    renderWithProviders(
      <Routes>
        <Route path="/setup/provider" element={<ProviderStep />} />
        <Route path="/setup/connect" element={<p>connect step</p>} />
      </Routes>,
      { route: '/setup/provider' },
    );

    await user.click(screen.getByRole('button', { name: /DeepSeek/ }));

    expect(await screen.findByText('connect step')).toBeInTheDocument();
    expect(h.assign).not.toHaveBeenCalled();
  });

  it('takes a subscription that must sign in again through its sign-in, not on to its models', async () => {
    h.reauth = ['codex-oauth'];
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <MemoryRouter initialEntries={[{ pathname: '/setup/provider', state: { list: 'oauth' } }]}>
          <Routes>
            <Route path="/setup/provider" element={<ProviderStep />} />
            <Route path="/setup/connect" element={<p>connect step</p>} />
            <Route path="/setup/models" element={<p>models step</p>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    await user.click(screen.getByRole('button', { name: /ChatGPT/ }));

    expect(await screen.findByText('connect step')).toBeInTheDocument();
    expect(screen.queryByText('models step')).toBeNull();
  });

  it("returns from a coding plan's key form to the subscriptions it was picked from", async () => {
    h.catalog = [
      { provider: 'z-ai', display_name: 'Zhipu AI', group: 'z-ai', group_display_name: 'Zhipu AI', access_type: 'api_key', region: 'intl', base_url: 'https://intl.example' },
      { provider: 'z-ai-coding', display_name: 'GLM Coding Plan', group: 'z-ai', group_display_name: 'Zhipu AI', access_type: 'coding_plan', region: 'intl', base_url: 'https://plan.example' },
    ];
    const user = userEvent.setup();
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <MemoryRouter initialEntries={[{ pathname: '/setup/provider', state: { list: 'oauth' } }]}>
          <Routes>
            <Route path="/setup/provider" element={<ProviderStep />} />
            <Route path="/setup/connect" element={<ConnectStep />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    await user.click(screen.getByRole('button', { name: /GLM Coding Plan/ }));
    // The plan's form is a key's.
    expect(await screen.findByPlaceholderText(/key/i)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Back' }));

    expect(await screen.findByRole('button', { name: /ChatGPT/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /GLM Coding Plan/ })).toBeInTheDocument();
  });
});
