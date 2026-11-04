/**
 * ExperimentsPanel: the API serves each feature's English, and a locale shows
 * its own translation under `settings.featureCatalog.<key>` when it has one.
 * A feature the locale has not translated still renders, in English.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import type { FeatureState } from '@/types/api';

const h = vi.hoisted(() => ({ features: [] as FeatureState[] }));

vi.mock('@/hooks/useFeatures', () => ({
  useFeatures: () => ({ data: h.features, isLoading: false }),
  useSetFeatureOverride: () => ({ mutateAsync: vi.fn() }),
}));

vi.mock('../usePreferenceWrite', () => ({
  useSaveFailureToast: () => vi.fn(),
}));

import { ExperimentsPanel } from '../ExperimentsPanel';

const feature = (overrides: Partial<FeatureState> & { key: string }): FeatureState => ({
  label: overrides.key,
  description: '',
  tradeoffs: null,
  enabled: false,
  gate: 'opt_in',
  min_tier: null,
  user_override: null,
  ...overrides,
});

const WAKEUPS = feature({
  key: 'wakeups',
  label: 'Wakeups',
  description: 'Lets the agent pick a thread back up by itself.',
  tradeoffs: 'A wakeup that fires while the thread is idle starts a turn by itself.',
  gate: 'opt_out',
  enabled: true,
});
const UNTRANSLATED = feature({
  key: 'beta_thing',
  label: 'Beta thing',
  description: 'Something new.',
  tradeoffs: 'It may cost more.',
});

describe('ExperimentsPanel', () => {
  afterEach(async () => {
    await act(() => i18n.changeLanguage('en-US'));
  });

  it('shows the API English in en-US', async () => {
    await act(() => i18n.changeLanguage('en-US'));
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);

    expect(screen.getByText('Wakeups')).toBeInTheDocument();
    expect(screen.getByText(WAKEUPS.description)).toBeInTheDocument();
    expect(screen.getByText(WAKEUPS.tradeoffs!)).toBeInTheDocument();
    expect(screen.getByText('Trade-off:')).toBeInTheDocument();
  });

  it('shows the zh-CN translation of a cataloged feature', async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);

    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.label'))).toBeInTheDocument();
    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.description'))).toBeInTheDocument();
    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.tradeoffs'))).toBeInTheDocument();
    expect(screen.getByText('注意事项：')).toBeInTheDocument();
    expect(screen.queryByText(WAKEUPS.description)).not.toBeInTheDocument();
  });

  it('falls back to the API English for a feature zh-CN has not translated', async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    h.features = [UNTRANSLATED];
    renderWithProviders(<ExperimentsPanel />);

    expect(screen.getByText('Beta thing')).toBeInTheDocument();
    expect(screen.getByText('Something new.')).toBeInTheDocument();
    expect(screen.getByText('It may cost more.')).toBeInTheDocument();
  });
});
