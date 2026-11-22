/**
 * ExperimentsPanel: the API serves each feature's English, and a locale shows
 * its own translation under `settings.featureCatalog.<key>` when it has one.
 * A feature the locale has not translated still renders, in English. Features
 * are listed by category, a feature that needs another under it.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
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
import { experimentGroups } from '../featureGroups';

const feature = (overrides: Partial<FeatureState> & { key: string }): FeatureState => ({
  label: overrides.key,
  description: '',
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
  category: 'autonomy',
  guide: {
    on_if: 'you hand off tasks that wait on something.',
    off_if: 'you want every turn to start with a message from you.',
    cost: 'Each wakeup can start a turn',
    good_to_know: 'A wakeup that fires on an idle thread costs a turn.',
    caution: 'Turning this off cancels every pending wakeup.',
  },
  gate: 'opt_out',
  enabled: true,
});
const ORDER_WAKEUPS = feature({
  key: 'order_wakeups',
  label: 'Order wakeups',
  category: 'autonomy',
  gate: 'opt_out',
  enabled: true,
  requires: ['wakeups'],
});
const PLAN = feature({ key: 'agent_plan', label: 'Show the agent\'s plan', category: 'chat', gate: 'opt_out', enabled: true });
const UNTRANSLATED = feature({
  key: 'beta_thing',
  label: 'Beta thing',
  description: 'Something new.',
  guide: { on_if: 'you want it.', off_if: 'you do not.', cost: 'It may cost more.', good_to_know: null, caution: null },
  category: 'tools',
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
    expect(screen.getByText(WAKEUPS.guide!.cost!)).toBeInTheDocument();
    expect(screen.getByText('On by default')).toBeInTheDocument();
    // What switching it off loses stays on the row, beside the switch that
    // toggles without the dialog.
    expect(screen.getByText(WAKEUPS.guide!.caution!)).toBeInTheDocument();
  });

  it('opens the case for each side from the feature, worded for an opt-out feature', async () => {
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);
    expect(screen.queryByText(WAKEUPS.guide!.on_if)).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Wakeups' }));
    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByText('Keep it on if')).toBeInTheDocument();
    expect(within(dialog).getByText(WAKEUPS.guide!.on_if)).toBeInTheDocument();
    expect(within(dialog).getByText('Turn it off if')).toBeInTheDocument();
    expect(within(dialog).getByText(WAKEUPS.guide!.off_if)).toBeInTheDocument();
    expect(within(dialog).getByText(WAKEUPS.guide!.good_to_know!)).toBeInTheDocument();
    expect(within(dialog).getByRole('switch', { name: 'Wakeups' })).toBeChecked();
  });

  // A keyboard user reaches a feature, opens it, and keeps their place in
  // the list when the dialog closes.
  it('opens a feature from the keyboard and returns focus to it on close', async () => {
    const user = userEvent.setup();
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);
    const opener = screen.getByRole('button', { name: 'Wakeups' });

    await user.tab();
    expect(opener).toHaveFocus();
    await user.keyboard('{Enter}');
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toContainElement(document.activeElement as HTMLElement));

    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(opener).toHaveFocus();
  });

  // Either switch can turn Wakeups off without asking, so each one says what
  // that cancels.
  it('names what switching it off loses on the switch itself', async () => {
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);
    expect(screen.getByRole('switch', { name: 'Wakeups' })).toHaveAccessibleDescription(
      expect.stringContaining(WAKEUPS.guide!.caution!),
    );

    fireEvent.click(screen.getByRole('button', { name: 'Wakeups' }));
    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByRole('switch', { name: 'Wakeups' })).toHaveAccessibleDescription(WAKEUPS.guide!.caution!);
  });

  it('holds a feature off, and says so, while a feature it needs is off', async () => {
    h.features = [{ ...WAKEUPS, enabled: false }, { ...ORDER_WAKEUPS, enabled: false }];
    renderWithProviders(<ExperimentsPanel />);
    expect(screen.getByRole('switch', { name: 'Order wakeups' })).toBeDisabled();
    expect(screen.getByText('Needs Wakeups')).toBeInTheDocument();
    expect(screen.getByRole('switch', { name: 'Wakeups' })).toBeEnabled();
  });

  it('shows the zh-CN translation of a cataloged feature', async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    h.features = [WAKEUPS];
    renderWithProviders(<ExperimentsPanel />);

    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.label'))).toBeInTheDocument();
    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.description'))).toBeInTheDocument();
    expect(screen.getByText(i18n.t('settings.featureCatalog.wakeups.cost'))).toBeInTheDocument();
    expect(screen.getByText('默认开启')).toBeInTheDocument();
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

  // Claude Code is turned on by connecting it under Model access, not here.
  it('leaves out a feature turned on elsewhere', async () => {
    h.features = [WAKEUPS, feature({ key: 'claude_code_runtime', label: 'Claude Code runtime (preview)', home: 'model_access' })];
    renderWithProviders(<ExperimentsPanel />);
    expect(await screen.findByText('Wakeups')).toBeInTheDocument();
    expect(screen.queryByText('Claude Code runtime (preview)')).toBeNull();
  });
});

describe('experimentGroups', () => {
  it('lists the groups in their order, a feature that needs another right under it', () => {
    const UNKNOWN = feature({ key: 'later', category: 'not_yet_known' as never });
    const groups = experimentGroups([ORDER_WAKEUPS, UNKNOWN, WAKEUPS, PLAN]);
    expect(groups.map((g) => g.key)).toEqual(['chat', 'autonomy', 'other']);
    expect(groups[1].rows.map((r) => [r.feature.key, r.nested])).toEqual([
      ['wakeups', false],
      ['order_wakeups', true],
    ]);
  });

  it('leaves out what the user cannot set here, and names the feature holding one off', () => {
    const groups = experimentGroups([
      { ...WAKEUPS, enabled: false },
      ORDER_WAKEUPS,
      feature({ key: 'claude_code_runtime', gate: 'none', category: 'chat' }),
      feature({ key: 'elsewhere', category: 'chat', home: 'model_access' }),
    ]);
    expect(groups.map((g) => g.key)).toEqual(['autonomy']);
    expect(groups[0].rows.map((r) => r.blockedBy?.key ?? null)).toEqual([null, 'wakeups']);
  });
});
