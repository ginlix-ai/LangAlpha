/**
 * The member's own-context switch in the Agent panel: drawn only where the
 * organization's feature is on, on unless the key is an explicit false, and
 * it writes its one key alone (null to turn on, since absent means on).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const h = vi.hoisted(() => ({
  featureOn: true,
  preferences: {} as Record<string, unknown>,
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
}));

vi.mock('@/hooks/useFeatures', () => ({
  useFeatureEnabled: (key: string) => key === 'shared_workspaces_member_context' && h.featureOn,
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: true }),
}));
const mutation = { mutateAsync: h.mutateAsync };
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => mutation }));
vi.mock('@/hooks/useModels', () => ({ useModels: () => ({ models: null }) }));
vi.mock('../components/SearchProviderSelect', () => ({ SearchProviderSelect: () => null }));
vi.mock('@/hooks/useSearchTierAllows', () => ({ useSearchTierAllows: () => () => true }));
vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast: vi.fn() }) }));
vi.mock('../AutoApproveSettings', () => ({ AutoApproveSettings: () => null }));
vi.mock('../OutputFormatRanking', () => ({ OutputFormatRanking: () => null }));

import { AgentPanel } from '../AgentPanel';

const LABEL = 'settings.ownContext';

beforeEach(() => {
  h.featureOn = true;
  h.preferences = {};
  h.mutateAsync.mockReset().mockResolvedValue({});
});

describe('AgentPanel: own context in shared workspaces', () => {
  it('is hidden when the feature is off', () => {
    h.featureOn = false;
    render(<AgentPanel />);
    expect(screen.queryByRole('switch', { name: LABEL })).toBeNull();
  });

  it('reads on when the key is absent', () => {
    render(<AgentPanel />);
    expect(screen.getByRole('switch', { name: LABEL })).toHaveAttribute('aria-checked', 'true');
  });

  it('reads off for an explicit false', () => {
    h.preferences = { other_preference: { own_context_in_shared_workspaces: false } };
    render(<AgentPanel />);
    expect(screen.getByRole('switch', { name: LABEL })).toHaveAttribute('aria-checked', 'false');
  });

  it('turning it off writes false', async () => {
    render(<AgentPanel />);
    fireEvent.click(screen.getByRole('switch', { name: LABEL }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({ other_preference: { own_context_in_shared_workspaces: false } }));
  });

  it('turning it on writes null', async () => {
    h.preferences = { other_preference: { own_context_in_shared_workspaces: false } };
    render(<AgentPanel />);
    fireEvent.click(screen.getByRole('switch', { name: LABEL }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({ other_preference: { own_context_in_shared_workspaces: null } }));
  });
});
