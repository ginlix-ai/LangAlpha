/**
 * The connect pages the answers' model step sends the user to know it from
 * the visit (`answering` in the router state), not from the account's
 * onboarding being unfinished: there the subscriptions leave Claude Code to
 * its own answer, and picking a provider's models goes back to the answers'
 * model step; a visit from Settings goes on to the defaults as before.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';

const h = vi.hoisted(() => ({ save: vi.fn(), policy: null as null | object }));
vi.mock('@/hooks/useOnboardingRoute', () => ({ useOnboardingRoute: () => ({ kind: 'answers' }) }));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], isLoading: false }),
}));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: null }) }));
vi.mock('@/hooks/useAccount', () => ({ useAccount: () => ({ policy: h.policy }) }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], configuredSet: new Set() }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: { state: 'ready', cloud: true }, unread: false, isLoading: false }),
}));
vi.mock('@/hooks/useModels', () => ({ useModels: () => ({ models: null }) }));
vi.mock('@/components/modelAccess/SubscriptionChoices', () => ({
  SubscriptionChoices: ({ claudeCode, signIns }: { claudeCode?: object; signIns?: boolean }) => (
    <p>{claudeCode ? 'Claude Code offered' : 'no Claude Code'}{signIns === false ? ', no sign-ins' : ''}</p>
  ),
}));
vi.mock('@/components/modelAccess/useModelPicker', () => ({
  useModelPicker: () => ({ save: h.save, saving: false, loading: false }),
}));
vi.mock('@/components/modelAccess/ModelPicker', () => ({ ModelPickerList: () => null }));
vi.mock('@/components/modelAccess/SubscriptionSummary', () => ({ SubscriptionSummary: () => null }));

import ProviderStep from '../ProviderStep';
import ModelPickStep from '../ModelPickStep';

function Where() {
  const location = useLocation();
  return <p data-testid="where">{location.pathname} {JSON.stringify(location.state)}</p>;
}

function open(path: string, element: React.ReactElement, state: object) {
  render(
    <MemoryRouter initialEntries={[{ pathname: path, state }]}>
      <Routes>
        <Route path={path} element={element} />
        <Route path="*" element={<Where />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  h.save = vi.fn(async () => true);
  h.policy = null;
});

describe('the subscriptions list', () => {
  it('leaves Claude Code out for the answers', () => {
    open('/setup/provider', <ProviderStep />, { list: 'oauth', answering: true });
    expect(screen.getByText('no Claude Code')).toBeInTheDocument();
  });

  it('keeps the coding plans, which are keys, where only sign-ins are kept off, and leaves where both are', () => {
    h.policy = { own_model_access: { api_keys: true, subscriptions: false } };
    open('/setup/provider', <ProviderStep />, { list: 'oauth', answering: true });
    expect(screen.getByText('no Claude Code, no sign-ins')).toBeInTheDocument();
  });

  it('goes back to the answers\' model step where the list\'s credentials are all kept off', () => {
    h.policy = { own_model_access: { api_keys: false, subscriptions: true } };
    open('/setup/provider', <ProviderStep />, { list: 'api_key', answering: true });
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/model');
  });

  it('offers Claude Code elsewhere', () => {
    open('/setup/provider', <ProviderStep />, { list: 'oauth' });
    expect(screen.getByText('Claude Code offered')).toBeInTheDocument();
  });
});

describe('picking a provider\'s models', () => {
  it('goes back to the answers model step the visit came from', async () => {
    open('/setup/models', <ModelPickStep />, { provider: 'zai-coding', answering: true });
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/model {"connected":"zai-coding"}'));
  });

  it('adds another provider from the answers model step the visit came from', async () => {
    open('/setup/models', <ModelPickStep />, { provider: 'zai-coding', answering: true });
    await userEvent.click(screen.getByRole('button', { name: '+ Add' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/setup/model');
    expect(screen.getByTestId('where')).not.toHaveTextContent('/setup/method');
  });

  it('goes on to the defaults for a visit from elsewhere, onboarding unfinished or not', async () => {
    open('/setup/models', <ModelPickStep />, { provider: 'my-gateway' });
    await userEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/setup/defaults'));
  });
});
